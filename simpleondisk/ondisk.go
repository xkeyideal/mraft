package simpleondisk

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/client"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
)

type SimpleOnDiskRaft struct {
	RaftNodePeers  map[uint64]string // mraft节点地址
	RaftClusterIDs []uint64

	nodehost       *dragonboat.NodeHost
	clusterSession map[uint64]*client.Session
	lock           sync.RWMutex
}

func NewSimpleOnDiskRaft(peers map[uint64]string, clusterIDs []uint64) *SimpleOnDiskRaft {

	dr := &SimpleOnDiskRaft{
		RaftNodePeers:  peers,
		RaftClusterIDs: clusterIDs,
		clusterSession: make(map[uint64]*client.Session),
		lock:           sync.RWMutex{},
	}

	return dr
}

func (disk *SimpleOnDiskRaft) Start(raftDataDir string, nodeID uint64, nodeAddr string, join bool) error {

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

		if err := nh.StartOnDiskCluster(peers, join, NewSimpleDiskKV, rc); err != nil {
			panic(err)
		}

		disk.clusterSession[clusterID] = disk.nodehost.GetNoOPSession(clusterID)
	}

	return nil
}

func (disk *SimpleOnDiskRaft) Write(key string, hashKey uint64, value int) error {
	idx := hashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	cs := disk.clusterSession[clusterID]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	d := kv{key, value}
	b, _ := json.Marshal(d)

	_, err := disk.nodehost.SyncPropose(ctx, cs, b)

	cancel()
	return err
}

// SyncRead 线性读
func (disk *SimpleOnDiskRaft) SyncRead(key string, hashKey uint64) ([]byte, error) {
	idx := hashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	result, err := disk.nodehost.SyncRead(ctx, clusterID, []byte(key))
	cancel()

	if err != nil {
		return nil, err
	}

	return result.([]byte), nil
}

// ReadLocal 读本地
func (disk *SimpleOnDiskRaft) ReadLocal(key string, hashKey uint64) ([]byte, error) {
	idx := hashKey % uint64(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	result, err := disk.nodehost.StaleRead(clusterID, []byte(key))

	if err != nil {
		return nil, err
	}

	return result.([]byte), nil
}

func (disk *SimpleOnDiskRaft) Stop() {
	disk.nodehost.Stop()

	disk.clusterSession = make(map[uint64]*client.Session)
}

// Info 查询NodeHostInfo
func (disk *SimpleOnDiskRaft) Info() *dragonboat.NodeHostInfo {
	return disk.nodehost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: false})
}
