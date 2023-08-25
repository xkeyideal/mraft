### 启动方式

程序启动入口: productready/main/app.go httpPort, raftPort

### 启动配置项

```go
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
```

### dragonboat raft的启动方式

该raft框架的启动方式有两种

1. 采用常规的ip:port方式，此方式限制了重启后ip:port不能发生改变
2. 采用gossip方式启动，此方式只需要保证raft config里的
```go
Expert: config.ExpertConfig{
			TestNodeHostID: nodeId,
		},
``` 
TestNodeHostID不变即可，可以实现重启后的ip改动，但数据文件和raft文件不能丢失，本质上仅支持机器换ip

本示例采用的是第一种方式.

raft集群里的每台集群管理的`clusterIds`不一定必须完全一致，即每台机器没必要存储全量的数据，可以写一个管理端程序来管理clusterId的每台机器分配情况。
如果每台机器不存全量的`clusterIds`，那么业务请求的key到来，可能该机器并不存在此key的value，解决办法可以采用该机器找到该key实际存在在哪来机器，
然后帮助业务完成请求并返回。

### 本示例目前无法直接启动

由于需要配置`DynamicConfig` 里的 `Join` 和 `InitialMembers` 字段后才能顺利启动，本人使用的真实生产环境是将启动配置写入服务器文件，每次重启时读取该文件获取上述必备的启动数据；如果不存在本地文件，则等待管理端推送该份配置。

因此想启动，可以先研究一下代码，然后将 `Join` 和 `InitialMembers` 字段写死在`productready/main/app.go`文件的配置里，然后尝试启动

刚开始第一步，将所有的`clusterId`和`nodeId`全部以原始节点启动，目前在`productready/engine.go`里系统是将所有`clusterIds`写死的`clusterIds = []uint64{0, 1, 2}`，真实的生产环境`clusterIds`的个数也是预先配置好的，均存储在管理端程序中。

如果节点是以`join`的方式加入，那么先启动该节点，然后调用集群接口，将此节点加入进集群，然后重新生成 `Join` 和 `InitialMembers` 字段，推送给新节点，等待即可；该份新配置也应该推送给集群中原本以存在的机器，本人真实使用的情况是对此份配置加上版本号来进行控制的。

**根据[dragonboat](https://github.com/lni/dragonboat/blob/master/docs/overview.CHS.md)节点启动的文档，当一个节点重启时，不论该节点是一个初始节点还是后续通过成员变更添加的节点，均无需再次提供初始成员信息，也不再需要设置join参数为true**。

配置解释:

1. raft集群中每个节点ID(NodeID)，采用该节点IP+port生成的48位uint64整型值
2. 若未配置raftPort，则控制中心采用默认的raftPort端口 `13890`启动
3. 样例提供的http端口采用raftPort的整型值加1作为HttpPort，无需用户配置
4. `join`字段，每个clusterId下对应存在哪些nodeId，且这些nodeId是集群的原始节点或后加入的节点
5. `initial_members`字段，告知集群每个clusterId下对应存在哪些nodeId，每个nodeId的raftAddr

### 节点加入raft集群

根据上述配置的说明，当节点主动加入raft集群时节点无需配置上述配置里的参数 `native` 和 `raftPeers`，
以在集群里的节点不能加入，**之前被删除的节点不能再次加入集群**

1. 调用样例加入节点的http接口，通知控制中心的raft集群，有新的节点需要加入集群, 待接口返回加入集群成功
2. 启动待加入集群的节点，此时原raft集群会自动寻址该新节点并同步数据
3. 新节点成功加入集群后，请立即去webapi的控制面板里修改raft集群的节点数据

### NodeHostConfig

```
config.NodeHostConfig{
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
    RaftAddress: addr,

    // ListenAddress是Raft RPC模块用于侦听Raft消息和快照的IP：端口地址。
    // 如果未设置ListenAddress字段，则Raft RPC模块将使用RaftAddress。
    // 如果将0.0.0.0指定为ListenAddress的IP，则Dragonboat将侦听所有接口上的指定端口。
    // 指定主机名或域名后，它将首先在本地解析为IP地址，而Dragonboat会侦听所有解析的IP地址。
    // 一般不指定这个,和RaftAddress保持一致就好了,收发就用一个端口,没有必要分开
    ListenAddress: listenAddr,

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
    EnableMetrics: false,

    //MaxSnapshotSendBytesPerSecond定义了NodeHost实例管理的所有Raft群集每秒可发送多少快照数据。默认值0表示没有为快照流设置限制。
    //每秒最多传输256M数据
    MaxSnapshotSendBytesPerSecond: 256 * 1024 * 1024,

    // MaxSnapshotRecvBytesPerSecond定义可以存储多少快照数据由NodeHost实例管理的所有Raft群集每秒收到一次。默认值0表示接收快照数据没有限制。
    //目前不限制接受的大小,由发送端决定
    MaxSnapshotRecvBytesPerSecond: 0,
}
```

### RaftConfig

```
config.Config{
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
```