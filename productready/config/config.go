package config

type DynamicConfig struct {
	// raft数据存储目录
	RaftDir string `json:"raftDir"`

	// 日志存储目录
	LogDir string `json:"logDir"`

	// 每个raft节点的Id，一旦生成且加入集群，再次启动后，不能变动
	NodeId uint64 `json:"nodeId"`

	// key: clusterId, key:nodeId
	Join map[uint64]map[uint64]bool `json:"join"`

	// key: clusterId, key:nodeId
	// val: 根据dragonboat的启动方式决定
	// gossip方式: NodeHostId
	// 常规方式: raftAddr
	InitialMembers map[uint64]map[uint64]string `json:"initial_members"`

	// 本机的地址
	IP string `json:"ip"`

	// raft port
	RaftPort uint16 `json:"raft_port"`

	// http port
	HttpPort uint16 `json:"http_port"`
}
