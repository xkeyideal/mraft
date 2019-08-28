package config

type OnDiskRaftConfig struct {
	RaftDataDir string

	RaftNodePeers map[uint64]string

	RaftClusterIDs []uint64
}

func NewOnDiskRaftConfig() *OnDiskRaftConfig {
	return &OnDiskRaftConfig{
		RaftDataDir: "/Volumes/ST1000/mraft-ondisk",
		RaftNodePeers: map[uint64]string{
			10000: "10.101.44.4:54000",
			10001: "10.101.44.4:54100",
			10002: "10.101.44.4:54200",
		},
		RaftClusterIDs: []uint64{254000, 254100, 254200},
	}
}
