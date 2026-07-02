package storage

import (
	"context"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xkeyideal/mraft/productready/ilogger"
	"github.com/xkeyideal/mraft/productready/storage/store"

	zlog "github.com/xkeyideal/mraft/logger"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/lni/goutils/syncutil"
	"go.uber.org/zap"
)

func initLogger(nodeId uint64, target string) {
	ilogger.Lo.SetNodeId(nodeId)
	ilogger.Lo.SetTarget(target)
}

var (
	unready uint32 = 0
	ready   uint32 = 1
)

type MemberInfo struct {
	ClusterId      uint64
	ConfigChangeId uint64
	Nodes          map[uint64]string
	Observers      map[uint64]string
	LeaderId       uint64
	LeaderValid    bool
}

type Storage struct {
	status  uint32
	cfg     *RaftConfig
	dataDir string

	// 当前节点的地址，若是地址不变则取raftAddr,若采用gossip方式起则是nhid-xxxxx
	target string

	nh      *dragonboat.NodeHost
	log     *zap.Logger
	stopper *syncutil.Stopper

	mu    sync.RWMutex
	smMap map[uint64]*store.Store
	csMap map[uint64]*client.Session

	// 用来记录cluster leader的变化
	leaderc chan raftio.LeaderInfo

	// 用来记录cluster membership的变化
	memberc chan raftio.NodeInfo

	cmu         sync.Mutex
	memberCache map[uint64]*MemberInfo

	wg    sync.WaitGroup
	exitc chan struct{}
}

func NewStorage(cfg *RaftConfig) (*Storage, error) {

	// 存储目录
	if err := mkdir(cfg.StorageDir); err != nil {
		return nil, err
	}

	// 初始化raft内部的日志
	if err := mkdir(cfg.LogDir); err != nil {
		return nil, err
	}

	// raftDir: base/raft_node_nodeId
	// dataDir: base/data_node_nodeId
	raftDir, dataDir, err := initPath(cfg.StorageDir, cfg.NodeId)
	if err != nil {
		return nil, err
	}

	log := zlog.NewLogger(filepath.Join(cfg.LogDir, "raft-storage.log"), zap.WarnLevel, false)

	s := &Storage{
		cfg:         cfg,
		status:      unready,
		dataDir:     dataDir,
		csMap:       make(map[uint64]*client.Session, len(cfg.ClusterIds)),
		smMap:       make(map[uint64]*store.Store, len(cfg.ClusterIds)),
		stopper:     syncutil.NewStopper(),
		log:         log,
		leaderc:     make(chan raftio.LeaderInfo, 24),
		memberc:     make(chan raftio.NodeInfo, 24),
		memberCache: make(map[uint64]*MemberInfo),
		exitc:       make(chan struct{}),
	}

	set := make(map[uint64]struct{})
	for _, jn := range cfg.Join {
		for nodeId := range jn {
			set[nodeId] = struct{}{}
		}
	}

	// dragonboat raft的事件处理
	raftEvent := &raftEvent{s}
	systemEvent := &systemEvent{s}
	var nhc config.NodeHostConfig
	// 根据raft寻址配置，确定采用gossip方式寻址或固定Addr方式寻址
	// 此处的gossip是dragonboat内部的，与上面的gossip不同
	if cfg.Gossip {
		s.target = fmt.Sprintf("nhid-%d", cfg.NodeId)
		// nhc = buildNodeHostConfigByGossip(
		// 	raftDir, cfg.NodeId, cfg.GossipPort,
		// 	cfg.HostIP, cfg.RaftAddr, cfg.GossipSeeds,
		// 	cfg.Metrics, raftEvent, systemEvent,
		// )
		panic("dragonboat raft gossip not implemented")
	} else {
		s.target = cfg.RaftAddr
		nhc = buildNodeHostConfig(raftDir, cfg.RaftAddr, cfg.Metrics, raftEvent, systemEvent)
	}

	initLogger(cfg.NodeId, s.target)

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return nil, err
	}
	s.nh = nh

	// 启动dragonboat的event变化的回调处理
	s.stopper.RunWorker(s.handleEvents)

	// 根据分配好的每个节点归属的clusterIds来初始化实例
	err = s.stateMachine(cfg.Join, dataDir, cfg.ClusterIds)
	if err != nil {
		s.cleanupOnError()
		return nil, err
	}

	return s, nil
}

func (s *Storage) cleanupOnError() {
	if s.nh != nil {
		s.nh.Stop()
		s.nh = nil
	}

	s.mu.Lock()
	for _, st := range s.smMap {
		if st != nil {
			st.Close()
		}
	}
	s.mu.Unlock()

	s.stopper.Close()
}

func (s *Storage) stateMachine(join map[uint64]map[uint64]bool, dataDir string, clusterIds []uint64) error {
	var (
		initialMembers map[uint64]string
		jn             map[uint64]bool
		ok             bool
		nodeJoin       bool
	)

	// 根据分配好的每个节点归属的clusterIds来初始化实例
	for _, clusterId := range clusterIds {
		jn, ok = join[clusterId]
		if !ok {
			return fmt.Errorf("raft clusterId: %d, can't find join config", clusterId)
		}

		nodeJoin, ok = jn[s.cfg.NodeId]
		if !ok {
			return fmt.Errorf("raft clusterId: %d, nodeId: %d, can't find join config", clusterId, s.cfg.NodeId)
		}

		rc := buildRaftConfig(s.cfg.NodeId, clusterId)
		clusterDataPath := filepath.Join(dataDir, strconv.Itoa(int(clusterId)))

		// 如果该cluster的数据目录已经初始化过，说明是重启，join必须传false
		isRestart := store.IsInitialized(clusterDataPath)
		if err := os.MkdirAll(clusterDataPath, os.ModePerm); err != nil {
			return err
		}

		opts := store.PebbleClusterOption{
			Target:    s.target,
			NodeId:    s.cfg.NodeId,
			ClusterId: clusterId,
		}

		// 获取pebbledb的存储目录
		pebbleDBDir, err := store.GetPebbleDBDir(clusterDataPath)
		if err != nil {
			return err
		}

		st, err := store.NewStore(clusterId, clusterDataPath, pebbleDBDir, opts, s.log)
		if err != nil {
			return err
		}

		if isRestart {
			nodeJoin = false
		}

		if !nodeJoin {
			if isRestart {
				initialMembers = make(map[uint64]string)
			} else {
				initialMembers, ok = s.cfg.InitialMembers[clusterId]
				if !ok {
					return fmt.Errorf("raft clusterId: %d, can't find initial members", clusterId)
				}
			}
		} else {
			initialMembers = make(map[uint64]string)
		}

		stateMachine := newStateMachine(s.cfg.RaftAddr, s.target, clusterId, s.cfg.NodeId, st)
		err = s.nh.StartOnDiskCluster(initialMembers, nodeJoin, func(_ uint64, _ uint64) sm.IOnDiskStateMachine {
			return stateMachine
		}, rc)
		if err != nil {
			st.Close()
			return err
		}

		s.mu.Lock()
		s.csMap[clusterId] = s.nh.GetNoOPSession(clusterId)
		s.smMap[clusterId] = st
		s.mu.Unlock()
	}

	return nil
}

func (s *Storage) RaftReady() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	ch := make(chan struct{}, 1)
	go s.nodeReady(ctx, s.cfg.ClusterIds, ch)

	select {
	case <-ch:
	case <-ctx.Done():
		return fmt.Errorf("raft ready timeout: %w", ctx.Err())
	}

	// 集群启动后，先同步一遍membership
	membership := make(map[uint64]*MemberInfo)
	for _, clusterId := range s.cfg.ClusterIds {
		info, err := s.getClusterMembership(clusterId)
		if err != nil {
			log.Println("[WARN] RaftReady get cluster membership:", clusterId, err.Error())
			continue
		}

		membership[clusterId] = info
	}

	// s.gossip.UpdateMembershipMessage(&gossip.RaftMembershipMessage{
	// 	MemberInfos: membership,
	// })

	atomic.StoreUint32(&s.status, ready)
	log.Println("[INFO] raft", s.target, "started", s.nh.ID())

	return nil
}

func (s *Storage) nodeReady(ctx context.Context, clusterIds []uint64, ch chan<- struct{}) {
	wg := sync.WaitGroup{}
	wg.Add(len(clusterIds))
	for _, clusterId := range clusterIds {
		go func(clusterId uint64) {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				_, ready, err := s.nh.GetLeaderID(clusterId)
				if err == nil && ready {
					log.Println("nodeReady", s.target, clusterId, "ready")
					return
				}

				if err != nil {
					log.Println("nodeReady", s.target, clusterId, err.Error())
				}

				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
			}
		}(clusterId)
	}

	wg.Wait()

	select {
	case ch <- struct{}{}:
	default:
	}
}

func (s *Storage) RequestLeaderTransfer(clusterID, nodeId uint64) error {
	nh, err := s.nodeHost()
	if err != nil {
		return err
	}
	return nh.RequestLeaderTransfer(clusterID, nodeId)
}

func (s *Storage) StopRaftNode() error {
	atomic.StoreUint32(&s.status, unready)

	s.mu.Lock()
	if s.nh != nil {
		s.nh.Stop()
		s.nh = nil
	}
	for clusterId, st := range s.smMap {
		if st != nil {
			if err := st.Close(); err != nil {
				s.log.Warn("close pebble store failed", zap.Uint64("clusterId", clusterId), zap.Error(err))
			}
		}
	}
	s.smMap = make(map[uint64]*store.Store)
	s.csMap = make(map[uint64]*client.Session)
	s.mu.Unlock()

	s.log.Sync()
	s.stopper.Close()
	return nil
}

func (s *Storage) GetNodeHost() map[uint64]string {
	nh, err := s.nodeHost()
	if err != nil {
		return map[uint64]string{}
	}

	info := nh.GetNodeHostInfo(dragonboat.NodeHostInfoOption{
		SkipLogInfo: true,
	})

	if len(info.ClusterInfoList) == 0 {
		return map[uint64]string{}
	}

	nodes := make(map[uint64]string)
	for _, ci := range info.ClusterInfoList {
		for nodeId, addr := range ci.Nodes {
			nodes[nodeId] = addr
		}
	}
	return nodes
}

func (s *Storage) GetMembership(ctx context.Context) ([]*MemberInfo, error) {
	nh, err := s.nodeHost()
	if err != nil {
		return nil, err
	}

	memberInfoList := make([]*MemberInfo, 0, len(s.cfg.ClusterIds))
	for _, clusterId := range s.cfg.ClusterIds {
		childCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		membership, err := nh.SyncGetClusterMembership(childCtx, uint64(clusterId))
		cancel()
		if err != nil {
			return nil, err
		}
		info := &MemberInfo{
			ClusterId:      uint64(clusterId),
			ConfigChangeId: membership.ConfigChangeID,
			Nodes:          membership.Nodes,
			Observers:      membership.Observers,
		}
		memberInfoList = append(memberInfoList, info)
		leaderID, valid, err := nh.GetLeaderID(uint64(clusterId))
		if err != nil {
			return nil, err
		}
		info.LeaderId = leaderID
		info.LeaderValid = valid
	}
	return memberInfoList, nil
}

func (s *Storage) GetNodeId() uint64 {
	return s.cfg.NodeId
}

func (s *Storage) GetTarget() string {
	return s.target
}

func (s *Storage) getClusterId(hashKey string) uint64 {
	return uint64(crc32.ChecksumIEEE([]byte(hashKey)) % s.cfg.MultiGroupSize)
}

func (s *Storage) getClusterMembership(clusterId uint64) (*MemberInfo, error) {
	nh, err := s.nodeHost()
	if err != nil {
		return nil, err
	}

	membership, err := s.clusterMembership(clusterId)
	if err != nil {
		return nil, err
	}

	leaderID, valid, err := nh.GetLeaderID(clusterId)
	if err != nil {
		return nil, err
	}

	return &MemberInfo{
		ClusterId:      clusterId,
		ConfigChangeId: membership.ConfigChangeID,
		Nodes:          membership.Nodes,
		Observers:      membership.Observers,
		LeaderId:       leaderID,
		LeaderValid:    valid,
	}, nil
}

func (s *Storage) clusterMembership(clusterId uint64) (*dragonboat.Membership, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.nh.SyncGetClusterMembership(ctx, clusterId)
}

func initPath(path string, nodeId uint64) (string, string, error) {
	raftPath := filepath.Join(path, fmt.Sprintf("raft_node_%d", nodeId))
	dataPath := filepath.Join(path, fmt.Sprintf("data_node_%d", nodeId))
	if err := os.MkdirAll(raftPath, os.ModePerm); err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		return "", "", err
	}
	return raftPath, dataPath, nil
}

func mkdir(dir string) error {
	if !pathIsExist(dir) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}

	return nil
}

func pathIsExist(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	return true
}
