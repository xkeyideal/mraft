## dragonboat multi-group raft simple example

multi-group raft的简单使用示例，由于对[dragonboat](https://github.com/lni/dragonboat)的理解有限，可能存在部分错误，还望指出。

### 示例说明

本示例是对[dragonboat-example](https://github.com/lni/dragonboat-example)中ondisk示例的重写，改变其代码结构。

本示例，简单的CURD测试通过，未进行压测。

有兴趣者，可以参考。

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