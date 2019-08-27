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
}

func NewOnDiskRaft(peers map[uint64]string, clusterIDs []uint64) *OnDiskRaft {

	return &OnDiskRaft{
		RaftNodePeers:  peers,
		RaftClusterIDs: clusterIDs,
		clusterSession: make(map[uint64]*client.Session),
		kvs:            make(chan *store.Command, 10),
		exitChan:       make(chan struct{}),
		wg:             sync.WaitGroup{},
	}
}

func (disk *OnDiskRaft) Start(nodeID uint64) error {
	datadir := filepath.Join(
		"/Volumes/ST1000/",
		"mraft-ondisk",
		fmt.Sprintf("node%d", nodeID))

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
			idx := fnv32(kv.Key) % uint32(len(disk.RaftClusterIDs))
			clusterID := disk.RaftClusterIDs[idx]
			cs := disk.clusterSession[clusterID]
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

			cmd, err := json.Marshal(kv)
			if err != nil {
				panic(err)
			}

			res, err := disk.nodehost.SyncPropose(ctx, cs, cmd)
			fmt.Println(res, err)

			cancel()
		}
	}

exit:
	disk.wg.Done()
}

func (disk *OnDiskRaft) Write(kv *store.Command) {
	disk.kvs <- kv
}

func (disk *OnDiskRaft) Read(key string) (string, error) {
	idx := fnv32(key) % uint32(len(disk.RaftClusterIDs))
	clusterID := disk.RaftClusterIDs[idx]
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	result, err := disk.nodehost.SyncRead(ctx, clusterID, []byte(key))
	cancel()

	return string(result.([]byte)), err
}

func (disk *OnDiskRaft) Stop() {
	close(disk.exitChan)
	disk.nodehost.Stop()
}

func (disk *OnDiskRaft) Info() *dragonboat.NodeHostInfo {
	return disk.nodehost.GetNodeHostInfo(dragonboat.NodeHostInfoOption{SkipLogInfo: false})
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
