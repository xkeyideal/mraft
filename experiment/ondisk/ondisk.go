package ondisk

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/xkeyideal/mraft/experiment/store"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	sm "github.com/lni/dragonboat/v3/statemachine"
)

type OnDiskRaft struct {
	RaftNodePeers  map[uint64]string // mraft节点地址
	RaftClusterIDs []uint64

	nodehost       *dragonboat.NodeHost
	clusterSession map[uint64]*client.Session
	nodeUsers      map[uint64]dragonboat.INodeUser

	clusterMetrics map[uint64]*ondiskMetrics
	lock           sync.RWMutex
}

func NewOnDiskRaft(peers map[uint64]string, clusterIDs []uint64) *OnDiskRaft {

	dr := &OnDiskRaft{
		RaftNodePeers:  peers,
		RaftClusterIDs: clusterIDs,
		clusterSession: make(map[uint64]*client.Session),
		nodeUsers:      make(map[uint64]dragonboat.INodeUser),

		clusterMetrics: make(map[uint64]*ondiskMetrics),
		lock:           sync.RWMutex{},
	}

	for _, clusterID := range clusterIDs {
		dr.clusterMetrics[clusterID] = newOndiskMetrics()
	}

	return dr
}

func (disk *OnDiskRaft) ClusterReady(clusterId uint64) bool {
	var _, success, err = disk.nodehost.GetLeaderID(clusterId)
	if err != nil {
		return false
	}

	return success
}

func (disk *OnDiskRaft) ClusterAllReady() bool {
	for _, clusterID := range disk.RaftClusterIDs {
		if !disk.ClusterReady(clusterID) {
			return false
		}
	}
	return true
}

func (disk *OnDiskRaft) Start(raftDataDir string, nodeID uint64, nodeAddr string, join bool) error {

	datadir := filepath.Join(raftDataDir, fmt.Sprintf("node%d", nodeID))

	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	logger.GetLogger("dragonboat").SetLevel(logger.WARNING)
	logger.GetLogger("logdb").SetLevel(logger.WARNING)

	raftAddress := disk.RaftNodePeers[nodeID]
	peers := disk.RaftNodePeers
	if join {
		raftAddress = nodeAddr
		peers = make(map[uint64]string)
	}

	nhc := config.NodeHostConfig{
		DeploymentID:   20,
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 100,
		RaftAddress:    raftAddress,
	}

	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		return err
	}

	disk.nodehost = nh

	for _, clusterID := range disk.RaftClusterIDs {
		rc := config.Config{
			NodeID:             nodeID,
			ClusterID:          clusterID,
			ElectionRTT:        10,
			HeartbeatRTT:       1,
			CheckQuorum:        true,
			SnapshotEntries:    1000,
			CompactionOverhead: 100,
		}

		if err := nh.StartOnDiskCluster(peers, join, NewDiskKV, rc); err != nil {
			panic(err)
		}

		disk.clusterSession[clusterID] = disk.nodehost.GetNoOPSession(clusterID)
	}

	for _, clusterID := range disk.RaftClusterIDs {
		nodeuser, err := disk.nodehost.GetNodeUser(clusterID)
		if err != nil {
			panic(err)
		}
		disk.nodeUsers[clusterID] = nodeuser
	}

	return nil
}

func checkRequestState(rs *dragonboat.RequestState) (sm.Result, error) {
	select {
	case r := <-rs.CompletedC:
		if r.Completed() {
			return r.GetResult(), nil
		} else if r.Rejected() {
			return sm.Result{}, dragonboat.ErrRejected
		} else if r.Timeout() {
			return sm.Result{}, dragonboat.ErrTimeout
		} else if r.Terminated() {
			return sm.Result{}, dragonboat.ErrClusterClosed
		} else if r.Dropped() {
			return sm.Result{}, dragonboat.ErrClusterNotReady
		}
	}

	return sm.Result{}, nil
}

func (disk *OnDiskRaft) Write(kv *store.Command) error {
	idx := kv.HashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	cs := disk.clusterSession[clusterID]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	cmdBytes, _ := kv.Marshal()

	_, err := disk.nodehost.SyncPropose(ctx, cs, cmdBytes)

	cancel()
	return err
}

func (disk *OnDiskRaft) AdvanceWrite(kv *store.Command) error {
	idx := kv.HashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	cs := disk.clusterSession[clusterID]

	cmdBytes, _ := kv.Marshal()

	rs, err := disk.nodeUsers[clusterID].Propose(cs, cmdBytes, 3*time.Second)

	defer func() {
		if rs != nil {
			rs.Release()
		}
	}()

	if err != nil {
		return err
	}

	_, err = checkRequestState(rs)

	return err
}

// SyncRead 线性读
func (disk *OnDiskRaft) SyncRead(key string, hashKey uint64) (*store.RaftAttribute, error) {
	idx := hashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	result, err := disk.nodehost.SyncRead(ctx, clusterID, []byte(key))
	cancel()

	if err != nil {
		return nil, err
	}

	return result.(*store.RaftAttribute), nil
}

func (disk *OnDiskRaft) AdvanceSyncRead(key string, hashKey uint64) (*store.RaftAttribute, error) {
	idx := hashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]

	rs, err := disk.nodeUsers[clusterID].ReadIndex(3 * time.Second)
	defer func() {
		if rs != nil {
			rs.Release()
		}
	}()

	if err != nil {
		return nil, err
	}

	_, err = checkRequestState(rs)
	if err != nil {
		return nil, err
	}

	v, err := disk.nodehost.NAReadLocalNode(rs, []byte(key))
	if err != nil {
		return nil, err
	}

	attr := &store.RaftAttribute{}
	err = attr.Unmarshal(v)

	return attr, err
}

// ReadLocal 读本地
func (disk *OnDiskRaft) ReadLocal(key string, hashKey uint64) (*store.RaftAttribute, error) {
	idx := hashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	result, err := disk.nodehost.StaleRead(clusterID, []byte(key))

	if err != nil {
		return nil, err
	}

	return result.(*store.RaftAttribute), nil
}

func (disk *OnDiskRaft) Stop() {
	disk.nodehost.Stop()

	disk.clusterMetrics = make(map[uint64]*ondiskMetrics)
	disk.clusterSession = make(map[uint64]*client.Session)
}

// MetricsInfo 用于做写入次数的统计
func (disk *OnDiskRaft) MetricsInfo() string {
	disk.lock.RLock()
	defer disk.lock.RUnlock()

	type stat struct {
		ClusterID uint64 `json:"clusterID"`
		Total     int64  `json:"total"`
		Err       int64  `json:"err"`
	}

	res := []stat{}

	for clusterID, metrics := range disk.clusterMetrics {
		res = append(res, stat{clusterID, metrics.total.Count(), metrics.err.Count()})
	}

	b, _ := json.Marshal(res)

	return string(b)
}

// Info 查询NodeHostInfo
func (disk *OnDiskRaft) Info() *dragonboat.NodeHostInfo {
	return disk.nodehost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: false})
}

// RaftAddNode 新增一个节点，./example-helloworld -nodeid 4 -addr localhost:63100 -join
func (disk *OnDiskRaft) RaftAddNode(nodeID uint64, nodeAddr string) error {

	if _, ok := disk.RaftNodePeers[nodeID]; ok {
		return fmt.Errorf("<%d> conflict", nodeID)
	}

	for _, clusterID := range disk.RaftClusterIDs {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ms, err := disk.nodehost.SyncGetClusterMembership(ctx, uint64(clusterID))
		if err != nil {
			return err
		}

		ctx2, _ := context.WithTimeout(context.Background(), 5*time.Second)
		err = disk.nodehost.SyncRequestAddNode(ctx2, clusterID, nodeID, nodeAddr, ms.ConfigChangeID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (disk *OnDiskRaft) RaftRemoveNode(nodeId uint64) error {
	for _, clusterID := range disk.RaftClusterIDs {
		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		ms, err := disk.nodehost.SyncGetClusterMembership(ctx, uint64(clusterID))
		if err != nil {
			return err
		}

		ctx2, _ := context.WithTimeout(context.Background(), 5*time.Second)
		err = disk.nodehost.SyncRequestDeleteNode(ctx2, uint64(clusterID), nodeId, ms.ConfigChangeID)
		if err != nil {
			return err
		}
	}
	return nil
}
