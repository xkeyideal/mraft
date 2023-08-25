package storage

import (
	"github.com/lni/dragonboat/v3/config"
	"go.uber.org/zap/zapcore"
)

const deploymentId = 2023082513

type RaftConfig struct {
	LogDir string
	// 数据存储地址
	StorageDir string
	LogLevel   zapcore.Level

	// 本机IP地址
	HostIP string

	// nodeId一旦生成不能变动
	NodeId uint64

	// 该节点被分配的clusterIds
	ClusterIds []uint64

	// raft通信地址
	RaftAddr string

	// 用于moveTo命令时对方raft节点的grpc端口
	// GrpcPort uint16

	// raft cluster的分组个数, 用于hashKey计算clusterId
	MultiGroupSize uint32

	// 是否以join的方式加入raft集群
	// key: clusterId, key:nodeId
	Join map[uint64]map[uint64]bool

	// 此参数需注意的是:
	// 采用gossip方式启动时val是nodehostId, 详细参考dragonboat的文档,
	// 初次可以使用dragonboat id.NewNodeHostID(id uint64)来生成;
	// 若采用raftAddr固定不变的方式启动，val就是raftAddr
	// key: clusterId, key:nodeId
	InitialMembers map[uint64]map[uint64]string

	// 如果raft集群采用gossip可变IP的方式启动需设置
	Gossip      bool
	GossipPort  uint16
	GossipSeeds []string

	// dragonboat 是否开启metrics
	Metrics bool
}

func buildNodeHostConfig(raftDir string, raftAddr string, metrics bool, re *raftEvent, se *systemEvent) config.NodeHostConfig {
	return config.NodeHostConfig{
		// DeploymentID用于确定两个NodeHost实例是否属于同一部署，并因此允许彼此通信。
		// 通过将上下文消息发送到不相关的Raft节点，这有助于防止意外配置错误的NodeHost实例导致数据损坏错误。
		// 对于特定的基于Dragonboat的应用程序，可以在所有生产NodeHost实例上将DeploymentID设置为相同的uint64值，
		// 然后在登台和开发环境中使用不同的DeploymentID值。 对于不同的基于Dragonboat的应用程序，也建议使用不同的DeploymentID值。
		// 如果未设置，则默认值0将用作部署ID，从而允许所有具有部署ID 0的NodeHost实例相互通信。
		DeploymentID: deploymentId,

		// WALDir是用于存储所有Raft日志的WAL的目录,这仅用于存储Raft日志的WAL，它的大小通常很小，
		// 每个NodeHost的64GB通常绰绰有余。如果不设置,则所有内容会存储在NodeHostDir中
		WALDir: raftDir,

		// NodeHostDir存储所有需要存储的信息
		NodeHostDir: raftDir,

		// RTTMillisecond定义了两个NodeHost实例之间的平均往返时间（RTT），以毫秒为单位
		// 这样的RTT间隔在内部用作逻辑时钟滴答，raft的心跳和选举间隔都根据有多少这样的RTT间隔来定义
		// 请注意，RTTMillisecond是两个NodeHost实例之间的组合延迟，包括由网络传输引起的所有延迟，NodeHost排队和处理引起的所有延迟。
		// 例如，在满载时，我们用于基准测试的两个NodeHost实例之间的平均往返时间最多为500微秒，而它们之间的ping时间为100微秒。
		// 当您的环境中的RTTMillisecond小于1百万时，请将其设置为1。
		RTTMillisecond: 200,

		//当前节点对外的IP和端口,其他raft节点需要通过这个信息获得
		RaftAddress: raftAddr,

		// ListenAddress是Raft RPC模块用于侦听Raft消息和快照的IP：端口地址。
		// 如果未设置ListenAddress字段，则Raft RPC模块将使用RaftAddress。
		// 如果将0.0.0.0指定为ListenAddress的IP，则Dragonboat将侦听所有接口上的指定端口。
		// 指定主机名或域名后，它将首先在本地解析为IP地址，而Dragonboat会侦听所有解析的IP地址。
		// 一般不指定这个,和RaftAddress保持一致就好了,收发就用一个端口,没有必要分开
		// ListenAddress: listenAddr,

		//是否使用TLS进行安全认证,整个程序都是部署在内网中,可以认为是安全的,就不打开这个了
		MutualTLS: false,

		//当配置了TLS时,需要指定CA文件的地址
		//当配置了TLS时,需要指定CertFile的地址
		//CertFile string
		//当配置了TLS时,需要指定KeyFile的地址
		//KeyFile string
		//MaxReceiveQueueSize是每个接收队列的最大大小（以字节为单位）。 一旦达到最大大小，将删除更多复制消息以限制内存使用。 设置为0时，表示队列大小不受限制。
		//暂时先设置为128M
		MaxSendQueueSize: 128 * 1024 * 1024,

		// EnableMetrics确定是否应启用Prometheus格式的健康度量。
		EnableMetrics: metrics,

		//MaxSnapshotSendBytesPerSecond定义了NodeHost实例管理的所有Raft群集每秒可发送多少快照数据。默认值0表示没有为快照流设置限制。
		//每秒最多传输256M数据
		MaxSnapshotSendBytesPerSecond: 256 * 1024 * 1024,

		// MaxSnapshotRecvBytesPerSecond定义可以存储多少快照数据由NodeHost实例管理的所有Raft群集每秒收到一次。默认值0表示接收快照数据没有限制。
		//目前不限制接受的大小,由发送端决定
		MaxSnapshotRecvBytesPerSecond: 0,

		// RaftEventListener是暴露给用户空间的Raft事件（例如Raft领导变更）的侦听器。
		//  NodeHost使用一个专用的goroutine来逐个调用所有RaftEventListener方法，
		//  可能导致长时间延迟的CPU密集型或IO相关过程，应将其分流到用户管理的工作程序中。
		RaftEventListener: re,

		//SystemEventsListener允许向用户通知系统事件，例如快照创建，日志压缩和快照流。 它通常用于测试目的或用于其他高级用途，不需要Dragonboat应用程序来显式设置此字段。
		SystemEventListener: se,
	}
}

func buildRaftConfig(nodeId, clusterId uint64) config.Config {
	return config.Config{
		//当前节点的ID
		NodeID: nodeId,

		//当前节点的分片ID,如果当前raft是多组的,那么这个地方是指定当前组的ID
		ClusterID: clusterId,

		//领导节点是否应定期检查非领导者节点的状态，并在其不再具有法定人数时退出成为跟随者节点
		//当有5台机器,挂了3台,法定人数不够,则主节点退出,不再是主节点了,所有的写操作和同步读操作应该都不能执行了
		//各个节点只能读取本地的数据
		CheckQuorum: false,

		// ElectionRTT是两次选举之间的消息RTT的最小数量。 消息RTT由NodeHostConfig.RTTMillisecond定义。
		// Raft论文建议其幅度大于HeartbeatRTT(因为是先发现不健康,才会进行选举)，即两个心跳之间的间隔。
		// 在Raft中，选举之间的实际间隔被随机分配在ElectionRTT和2 * ElectionRTT之间。例如，假设NodeHostConfig.RTTMillisecond为100毫秒，
		// 要将选举间隔设置为1秒，则应该将ElectionRTT设置为10。启用CheckQuorum后，ElectionRTT还将定义检查领导者定额的时间间隔。
		// 这个值是个比例,具体的RTT时间大小是RTTMillisecond*ElectionRTT,当需要选举主节点时,各个节点的随机间隔在ElectionRTT和2 * ElectionRTT,
		// 当CheckQuorum为true,主也会每隔这个时间检查下从机数据是否符合法定人数
		ElectionRTT: 60,

		// HeartbeatRTT是两次心跳之间的消息RTT数。 消息RTT由NodeHostConfig.RTTMillisecond定义。 Raft论文建议心跳间隔应接近节点之间的平均RTT。
		// 例如，假设NodeHostConfig.RTTMillisecond为100毫秒，要将心跳间隔设置为每200毫秒，则应将HeartbeatRTT设置为2。
		HeartbeatRTT: 6,

		// SnapshotEntries定义应自动对状态机进行快照的频率,可以将SnapshotEntries设置为0以禁用此类自动快照。
		// 当SnapshotEntries设置为N时，意味着大约每N条Raft日志创建一个快照。这也意味着向跟踪者发送N个日志条目比发送快照要昂贵。
		// 生成快照后，可以压缩新快照覆盖的Raft日志条目。这涉及两个步骤，冗余日志条目首先被标记为已删除，然后在稍后发布 LogDB 压缩时将其从基础存储中物理删除。
		// 有关在生成快照后实际删除和压缩哪些日志条目的详细信息，请参见CompactionOverhead,通过将SnapshotEntries字段设置为0禁用自动快照后，
		// 用户仍然可以使用NodeHost的RequestSnapshot或SyncRequestSnapshot方法手动请求快照。
		SnapshotEntries: 25 * 10000 * 10,

		// CompactionOverhead定义每次Raft日志压缩后要保留的最新条目数。
		// 假设当前的日志为10000,开始创建快照,那么快照创建完成后,<=10000的日志都会被清理,
		// 如果想获得9000这样的日志,那么就得先完全加载快照,再从快照中读取,如果设置了CompactionOverhead为3000,
		// 那么就算创建了快照,我们仍然能获得10000-7000之间的日志记录,只有小于7000的,才需要重新加载日志获取
		CompactionOverhead: 25 * 10000,

		//确定是否使用ChangeID的顺序强制执行Raft成员资格更改。
		OrderedConfigChange: true,

		// MaxInMemLogSize是允许在每个Raft节点上的Raft日志存储在内存中的目标大小（以字节为单位）。 内存中的筏日志是尚未应用的日志。
		// MaxInMemLogSize是为防止内存无限增长而实现的目标值，并非用于精确限制确切的内存使用量。
		// 当MaxInMemLogSize为0时，目标设置为math.MaxUint64。 设置MaxInMemLogSize并达到目标后，客户端尝试提出新建议时将返回错误。
		// 建议将MaxInMemLogSize大于要使用的最大建议。
		//内存中未应用的日志大小,暂定为256M,超过256M的大小后会返回错误
		MaxInMemLogSize: 256 * 1024 * 1024,

		// SnapshotCompressionType是用于压缩生成的快照数据的压缩类型。 默认情况下不使用压缩。
		// 快照数据本身由rocksdb生成,采用了LZ4压缩,所以这边就不再继续压缩了
		SnapshotCompressionType: config.NoCompression,

		// EntryCompressionType是用于压缩用户日志。 使用Snappy时，允许的最大建议有效负载大致限制为3.42GB。
		EntryCompressionType: config.Snappy,

		// DisableAutoCompactions禁用用于回收Raft条目存储空间的自动压缩。
		// 默认情况下，每次捕获快照时都会执行压缩，这有助于以较高的IO开销为代价，尽快回收磁盘空间。
		// 用户可以禁用此类自动压缩，并在必要时使用NodeHost.RequestCompaction手动请求此类压缩。
		DisableAutoCompactions: false,

		// IsObserver指示当前节点是否是Observer节点,(观察者节点通常用于允许新节点加入群集并追赶其他日志，而不会影响可用性。 还可以引入额外的观察者节点来满足只读请求，而不会影响系统的写吞吐量)
		IsObserver: false,

		// IsWitness指示这是否是没有实际日志复制且没有状态机的见证Raft节点,见证节点支持目前处于试验阶段。
		IsWitness: false,

		//停顿指定在没有群集活动时是否让Raft群集进入停顿模式。 静默模式下的群集不交换心跳消息以最小化带宽消耗。当前处于试验阶段
		Quiesce: false,
	}
}
