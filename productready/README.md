### 启动方式

程序启动入口: productready/cmd/main/main.go

```
# CMD ./main -c ${NODES} -i 192.168.1.1 -p 13890 -d /data/raft init
# CMD ./main  -i 192.168.1.1 -p 13890  -d /data/raft join

# Usage:
#  /main [flags] <init|join>
#
#Flags:
#  -c, --cluster-item stringArray   add cluster node <ip:port> | <ip:port,ip:port...>
#  -d, --data-dir string            set data dir (default "./data")
#  -i, --discover-address string    address exported to others (default "auto")
#  -h, --help                       help
#  -s, --silent                     set log level to error.
#  -t, --try-run                    dump config but not serve.
#  -v, --verbose count              set log level [ 0=warn | 1=info | 2=debug ].
#  -V, --version                    show version and build info.

# Examples:
# CMD /main -c "1.2.3.4:13890,1.2.3.5:13890" -i "1.2.3.4" -p 13890 init
# CMD /main -c "1.2.3.4:13890" -c '1.2.3.5'-i "1.2.3.4" -p 13890 init
# CMD /main -i "1.2.3.6" -p 13890 -u "http://1.2.3.4:13891/raft/join" join
# ./main -c "10.181.20.34:12100,10.181.20.34:12200,10.181.20.34:12300" -i 10.181.20.34 -p 12100 init
# ./main  -i 10.181.20.34 -p 12400 -u "http://10.181.20.34:12301/raft/join" join
```

### 启动配置项

```go
type DynamicConfig struct {
	// raft数据存储目录
	RaftDir string `json:"raftDir"`

	// raft最初的集群地址,IP+Port
	RaftPeers []string `json:"raftPeers"`

	// 该节点是否是raft的最初集群之一
	Native bool `json:"native"`

	// join 节点时，http join的接口地址
	JoinUrl string `json:"joinUrl"`

	// 本机的地址
	IP string `json:"ip"`

	// raft port
	RaftPort uint16 `json:"raftPort"`
}
```

**根据[dragonboat](https://github.com/lni/dragonboat/blob/master/docs/overview.CHS.md)节点启动的文档，当一个节点重启时，不论该节点是一个初始节点还是后续通过成员变更添加的节点，均无需再次提供初始成员信息，也不再需要设置join参数为true**, 因此针对上述配置文件, **raftPeers**和**native**参数在重启时可以不提供

配置解释:

1. raft集群中每个节点ID(NodeID)，采用该节点IP+port生成的48位uint64整型值
2. 若未配置raftPort，则控制中心采用默认的raftPort端口 `13890`启动
3. 样例提供的http端口采用raftPort的整型值加1作为HttpPort，无需用户配置
4. 原生的raft集群的机器启动时需要指定上述配置里两个重要的参数 `native=true` 和 详细的`raftPeers` 
5. 以join的方式主动加入原生raft集群的节点无需配置上述配置里的参数 `native` 和 `raftPeers`，具体的加入方式见下面的说明
6. raftPeers里使用IP+port的方式提供，故此处需注意raftPeers里的本机raftPort需与配置文件里需提供的raftPort一致，否则程序会启动失败

### 节点加入raft集群

根据上述配置的说明，当节点主动加入raft集群时节点无需配置上述配置里的参数 `native` 和 `raftPeers`，
以在集群里的节点不能加入，**之前被删除的节点不能再次加入集群**

1. 调用样例加入节点的http接口，通知控制中心的raft集群，有新的节点需要加入集群, 待接口返回加入集群成功
2. 启动待加入集群的节点，此时原raft集群会自动寻址该新节点并同步数据
3. 新节点成功加入集群后，请立即去webapi的控制面板里修改raft集群的节点数据