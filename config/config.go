package config

type OnDiskRaftConfig struct {
	RaftDataDir string

	RaftNodePeers map[uint64]string

	RaftClusterIDs []uint64
}

func NewOnDiskRaftConfig() *OnDiskRaftConfig {
	return &OnDiskRaftConfig{
		RaftDataDir: "/Users/xkey/test/mraft-ondisk1",
		RaftNodePeers: map[uint64]string{
			10000: "10.181.20.34:11000",
			10001: "10.181.20.34:11100",
			10002: "10.181.20.34:11200",
			//10004: "10.181.20.34:11400",
		},
		RaftClusterIDs: []uint64{14000, 14100, 14200},
	}
}
