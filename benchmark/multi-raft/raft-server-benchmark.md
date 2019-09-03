## multi-raft 压测结果

```json
总次数: 6000, 错误数: 0, 线程数: 6, 每个线程连接数: 1, 请求次数: 1000
最小值: 663us, 最大值: 203700us, 中间值: 6182.6us
75百分位: 3939.0us, 90百分位: 5404.0us, 95百分位: 11294.0us, 99百分位: 93239.6us
```

### 压测程序Server端

[server端主程序](https://github.com/xkeyideal/mraft/blob/master/benchmark/multi-raft/raft_server/raft_server.go)注意修改ip地址
[server端启动程序](https://github.com/xkeyideal/mraft/blob/master/benchmark/multi-raft/raft_server/main/main.go) 注意启动参数，同时注意修改raft的存储目录地址

第一个参数为raft的NodeID，第二个参数为应用程序的TCP端口号

```go
go run main.go 10000 25700
go run main.go 10001 25800
go run main.go 10002 25900
```

server端总计配置了10个cluster

### 压测程序Client端

[client端主程序](https://github.com/xkeyideal/mraft/blob/master/benchmark/multi-raft/raft_client/raft_client.go)
[client端启动程序](https://github.com/xkeyideal/mraft/blob/master/benchmark/multi-raft/raft_client/main/main.go) 注意ip地址和端口号要与server对应

### 压测环境

机器采用的是开发环境的机器，操作系统macOS High Sierra，Darwin Kernel Version 18.6.0 root:xnu-4903.261.4~2/RELEASE_X86_64 x86_64 i386 iMac14,2 Darwin

CPU：3.29 GHz Intel Core i5

内存：20 GB 1600 MHz DDR3

磁盘：256GB SATA SSD