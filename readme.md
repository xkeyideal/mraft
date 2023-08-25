## dragonboat multi-group raft simple example

multi-group raft的简单使用示例，由于对[dragonboat](https://github.com/lni/dragonboat)的理解有限，可能存在部分错误，还望指出。

### 生产ready的样例

提供生产ready的样例，[productready](https://github.com/xkeyideal/mraft/blob/master/productready/README.md)

1. 提供了完整的采用`pebbledb`作为业务数据存储的状态机代码，此代码已用于生产环境。
2. 提供了支持动态配置的启动方式，提供了`dragonboat`配置需处理节点ID等问题的一个解决思路
3. 程序化的提供了新增raft节点的方案

### 示例说明

本示例是对[dragonboat-example](https://github.com/lni/dragonboat-example)中ondisk示例的重写，改变其代码结构，状态机的数据协议采用自定义的二进制协议，尽可能的提高读写性能。

本示例[dragonboat](https://github.com/lni/dragonboat) 使用的是v3.3.7版本, [pebbledb](https://github.com/cockroachdb/pebble) 使用的是跟随`dragonboat`所使用的版本

### 序列化工具

本示例为了兼容后续项目的需要，业务上只能使用 `thrift` 作为序列化方式，`thrift` 序列化库未采用官方库，使用的是[thrifter](https://github.com/thrift-iterator/go)，压测结果详见[thrifter-benchmark](https://github.com/xkeyideal/mraft/blob/master/benchmark/thrift-serialize/thrift-serialize.md)

<br>

在Raft SaveSnapshot与RecoverFromSnapshot时，采用的是自定义二进制协议，详细见[fsm.go](https://github.com/xkeyideal/mraft/blob/master/ondisk/fsm.go#L233)，压测结果详见[binary-benchmark](https://github.com/xkeyideal/mraft/blob/master/benchmark/binary-serialize/binary-serialize.md)

### TCPServer压测结果

multi-raft的网络协议与数据格式均使用simple-server中相同的方式，压测结果详见[simple-server-benchmark](https://github.com/xkeyideal/mraft/blob/master/benchmark/multi-raft/simple-server-benchmark.md)

### RaftServer压测结果

multi-raft的压测协议与数据格式均使用simple-server中相同的方式，压测结果详见[raft-server-benchmark](https://github.com/xkeyideal/mraft/blob/master/benchmark/multi-raft/raft-server-benchmark.md)

压测数据用例使用的是[代码自动化数据生成工具](https://github.com/xkeyideal/mraft/blob/master/benchmark/generate/generate-data.go)，每条数据的数据量大约在2KB以上，具体未做统计。

### 压测机器说明

机器采用的是开发环境的机器，操作系统macOS High Sierra，`Darwin Kernel Version 18.6.0 root:xnu-4903.261.4~2/RELEASE_X86_64 x86_64 i386 iMac14,2 Darwin`

CPU：3.29 GHz Intel Core i5

内存：20 GB 1600 MHz DDR3

磁盘：256GB Intel SATA SSD

参考了[dragonboat](https://github.com/lni/dragonboat)作者的文章[从共识算法开谈 - 硬盘性能的最大几个误解](https://zhuanlan.zhihu.com/p/55658164)，
特对开发环境的磁盘的fsync()落盘写性能使用**pg_test_fsync**工具进行测试

```
5 seconds per test
Direct I/O is not supported on this platform.

Compare file sync methods using one 8kB write:
(in wal_sync_method preference order, except fdatasync is Linux's default)
        open_datasync                     15293.184 ops/sec      65 usecs/op
        fdatasync                         15042.152 ops/sec      66 usecs/op
        fsync                             15062.644 ops/sec      66 usecs/op
        fsync_writethrough                   87.954 ops/sec   11370 usecs/op
        open_sync                         15060.335 ops/sec      66 usecs/op

Compare file sync methods using two 8kB writes:
(in wal_sync_method preference order, except fdatasync is Linux's default)
        open_datasync                      7342.068 ops/sec     136 usecs/op
        fdatasync                         11375.823 ops/sec      88 usecs/op
        fsync                             11035.212 ops/sec      91 usecs/op
        fsync_writethrough                   87.290 ops/sec   11456 usecs/op
        open_sync                          6943.205 ops/sec     144 usecs/op

Compare open_sync with different write sizes:
(This is designed to compare the cost of writing 16kB in different write
open_sync sizes.)
         1 * 16kB open_sync write         11774.650 ops/sec      85 usecs/op
         2 *  8kB open_sync writes         7335.006 ops/sec     136 usecs/op
         4 *  4kB open_sync writes         4147.836 ops/sec     241 usecs/op
         8 *  2kB open_sync writes         2048.232 ops/sec     488 usecs/op
        16 *  1kB open_sync writes         1015.277 ops/sec     985 usecs/op

Test if fsync on non-write file descriptor is honored:
(If the times are similar, fsync() can sync data written on a different
descriptor.)
        write, fsync, close                9232.970 ops/sec     108 usecs/op
        write, close, fsync               11632.603 ops/sec      86 usecs/op

Non-sync'ed 8kB writes:
        write                             14077.617 ops/sec      71 usecs/op
```

### 启动方式

示例代码已经放弃使用`rocksdb`作为存储，已经是纯`go`实现

`go run app.go 10000 9800`

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

详细的`dragonboat raft` 添加集群节点的示例请参考[productready](https://github.com/xkeyideal/mraft/blob/master/productready/README.md)

1. 先在集群中调用添加节点的命令RequestAddNode
2. 启动新增的节点，注意join节点的启动参数， nh.StartOnDiskCluster(map[uint64]string{}, true, NewDiskKV, rc)
3. 新增节点成功后，机器会通过Snapshot将数据同步给join节点
4. 新增节点与集群原有节点的启动顺序不影响集群的工作
5. 若新的集群需要重启，那么不能改变原有的peers(将新节点加入到peers)，否则集群启动不起来，报错如下：

```json
join节点的报错

2019-08-30 15:29:09.597258 E | raftpb: restarting previously joined node, member list map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300]
2019-08-30 15:29:09.597454 E | dragonboat: bootstrap validation failed, [54000:10003], map[], true, map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300], false
panic: cluster settings are invalid
```

```json
集群原来节点的报错

2019-08-30 15:29:06.590245 E | raftpb: inconsistent node list, bootstrap map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200], incoming map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300]
2019-08-30 15:29:06.590289 E | dragonboat: bootstrap validation failed, [54000:10002], map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200], false, map[10000:10.101.44.4:54000 10001:10.101.44.4:54100 10002:10.101.44.4:54200 10003:10.101.44.4:54300], false
panic: cluster settings are invalid
```

```json
原来的集群节点
map[uint64]string{
    10000: "10.101.44.4:54000",
    10001: "10.101.44.4:54100",
    10002: "10.101.44.4:54200",
}

新增的节点：10003: "10.101.44.4:54300"
```

```json
正确join或重启的方式
join := false
nodeAddr := ""
if engine.nodeID == 10003 {
    join = true
    nodeAddr = "10.101.44.4:54300"
}

engine.nh.Start(engine.raftDataDir, engine.nodeID, nodeAddr, join)
```