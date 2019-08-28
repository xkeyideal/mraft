package ondisk

import (
	"context"
	"encoding/json"
	"fmt"
	"mraft/store"
	"path/filepath"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
)

type OnDiskRaft struct {
	RaftNodePeers  map[uint64]string // mraft节点地址
	RaftClusterIDs []uint64

	nodehost       *dragonboat.NodeHost
	clusterSession map[uint64]*client.Session

	kvs      chan *store.Command
	exitChan chan struct{}
	wg       sync.WaitGroup

	clusterMetrics map[uint64]*ondiskMetrics
	lock           sync.RWMutex
}

func NewOnDiskRaft(peers map[uint64]string, clusterIDs []uint64) *OnDiskRaft {

	dr := &OnDiskRaft{
		RaftNodePeers:  peers,
		RaftClusterIDs: clusterIDs,
		clusterSession: make(map[uint64]*client.Session),
		kvs:            make(chan *store.Command, 10),
		exitChan:       make(chan struct{}),
		wg:             sync.WaitGroup{},

		clusterMetrics: make(map[uint64]*ondiskMetrics),
		lock:           sync.RWMutex{},
	}

	for _, clusterID := range clusterIDs {
		dr.clusterMetrics[clusterID] = newOndiskMetrics()
	}

	return dr
}

func (disk *OnDiskRaft) Start(raftDataDir string, nodeID uint64) error {
	datadir := filepath.Join(raftDataDir, fmt.Sprintf("node%d", nodeID))

	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	logger.GetLogger("dragonboat").SetLevel(logger.WARNING)

	nhc := config.NodeHostConfig{
		DeploymentID:   20,
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 100,
		RaftAddress:    disk.RaftNodePeers[nodeID],
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
			SnapshotEntries:    10,
			CompactionOverhead: 5,
		}
		if err := nh.StartOnDiskCluster(disk.RaftNodePeers, false, NewDiskKV, rc); err != nil {
			panic(err)
		}

		disk.clusterSession[clusterID] = disk.nodehost.GetNoOPSession(clusterID)
	}

	go disk.start()

	return nil
}

func (disk *OnDiskRaft) start() {
	disk.wg.Add(1)
	for {
		select {
		case <-disk.exitChan:
			goto exit
		case kv := <-disk.kvs:
			idx := kv.HashKey % uint64(len(disk.RaftClusterIDs))
			clusterID := disk.RaftClusterIDs[idx]
			cs := disk.clusterSession[clusterID]
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

			cmdBytes, _ := kv.Marshal()
			_, err := disk.nodehost.SyncPropose(ctx, cs, cmdBytes)
			ok := true
			if err != nil {
				ok = false
			}

			disk.lock.Lock()
			metrics := disk.clusterMetrics[clusterID]
			metrics.add(1, ok)
			disk.lock.Unlock()

			cancel()
		}
	}

exit:
	disk.wg.Done()
}

func (disk *OnDiskRaft) Write(kv *store.Command) {
	disk.kvs <- kv
}

func (disk *OnDiskRaft) Read(key string, hashKey uint64) (*store.RaftAttribute, error) {
	idx := hashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	result, err := disk.nodehost.SyncRead(ctx, clusterID, []byte(key))
	cancel()

	return result.(*store.RaftAttribute), err
}

func (disk *OnDiskRaft) Stop() {
	close(disk.exitChan)
	disk.nodehost.Stop()

	disk.clusterMetrics = make(map[uint64]*ondiskMetrics)
}

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

func (disk *OnDiskRaft) Info() *dragonboat.NodeHostInfo {
	return disk.nodehost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: false})
}
