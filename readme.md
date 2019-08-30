## dragonboat multi-group raft simple example

multi-group raft的简单使用示例，由于对[dragonboat](https://github.com/lni/dragonboat)的理解有限，可能存在部分错误，还望指出。

### 示例说明

本示例是对[dragonboat-example](https://github.com/lni/dragonboat-example)中ondisk示例的重写，改变其代码结构。

本示例，简单的CURD测试通过，未进行压测。

有兴趣者，可以参考。

### 序列化工具

本示例为了兼容后续项目的需要，业务上只能使用 `thrift` 作为序列化方式，`thrift` 序列化库未采用官方库，使用的是[thrifter](https://github.com/thrift-iterator/go)，压测结果详见[thrifter-benchmark](https://github.com/xkeyideal/mraft/blob/master/benchmark/thrift-serialize/thrift-serialize.md)

<br>

在Raft SaveSnapshot与RecoverFromSnapshot时，采用的是自定义二进制协议，详细见[fsm.go](https://github.com/xkeyideal/mraft/blob/master/ondisk/fsm.go#L233)，压测结果详见[binary-benchmark](https://github.com/xkeyideal/mraft/blob/master/benchmark/binary-serialize/binary-serialize.md)

### TCPServer压测结果

multi-raft的网络协议与数据格式均使用simple-server中相同的方式，压测结果详见[simple-server-benchmark](https://github.com/xkeyideal/mraft/blob/master/benchmark/multi-raft/simple-server-benchmark.md)

### 压测机器说明

机器采用的是开发环境的机器，操作系统macOS High Sierra，`Darwin Kernel Version 18.6.0 root:xnu-4903.261.4~2/RELEASE_X86_64 x86_64 i386 iMac14,2 Darwin`

<br>

CPU：3.29 GHz Intel Core i5

<br>

内存：20 GB 1600 MHz DDR3


### 启动方式

首先需要安装rocksdb，本示例业务的存储使用的是rocksdb。

`CGO_CFLAGS="-I/usr/local/include/rocksdb" CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4" go run app.go 10000 9800`

**10000** 是NodeID，已经在代码里限定了（代码中的NodeID分别是10000，10001，10002），不能修改.
**9800**是HTTP的端口号，随意设定即可

```go
peers := map[uint64]string{
    10000: "10.101.44.4:54000",
    10001: "10.101.44.4:54100",
    10002: "10.101.44.4:54200",
}

clusters := []uint64{254000, 254100, 254200}
```

### HTTP服务

示例的核心入口代码在engine/engine.go中，由于是示例，很多参数直接在代码中写死了。

HTTP服务采用[gin](https://github.com/gin-gonic/gin)

### RequestAddNode 向集群添加节点的注意事项

1. 先在集群中调用添加节点的命令RequestAddNode
2. 启动新增的节点，注意join节点的启动参数， nh.StartOnDiskCluster(map[uint64]string{}, true, NewDiskKV, rc)
3. 新增节点成功后，机器会通过Snapshot将数据同步给join节点
4. 新增节点与集群原有节点的启动顺序不影响集群的工作
5. 若新的集群需要重启，那么不能改变原有的peers(将新节点加入到peers)，否则集群启动不起来，报错如下：


```
join节点的报错

2019-08-30 15:29:09.597258 E | raftpb: restarting previously joined node, member list map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300]
2019-08-30 15:29:09.597454 E | dragonboat: bootstrap validation failed, [54000:10003], map[], true, map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300], false
panic: cluster settings are invalid
```
<br>
```
集群原来节点的报错

2019-08-30 15:29:06.590245 E | raftpb: inconsistent node list, bootstrap map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200], incoming map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300]
2019-08-30 15:29:06.590289 E | dragonboat: bootstrap validation failed, [54000:10002], map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200], false, map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300], false
panic: cluster settings are invalid
```

```
原来的集群节点
map[uint64]string{
    10000: "10.101.44.4:54000",
    10001: "10.101.44.4:54100",
    10002: "10.101.44.4:54200",
}

新增的节点：10003: "10.101.44.4:54300"
```

```
正确join或重启的方式
join := false
nodeAddr := ""
if engine.nodeID == 10003 {
    join = true
    nodeAddr = "10.101.44.4:54300"
}

engine.nh.Start(engine.raftDataDir, engine.nodeID, nodeAddr, join)
```