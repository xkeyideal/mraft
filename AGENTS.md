# mraft - AI Coding Agent 指南

> 本文件面向不熟悉本项目的 AI 编码助手。所有信息均来自当前代码库与文档，未经验证请勿假设。

## 项目概述

`mraft` 是一个基于 [dragonboat](https://github.com/lni/dragonboat) v3.3.7 的 **multi-group Raft** 学习与示例项目，使用 Go 语言编写。

项目目标：
- 展示如何在单节点上运行多个 Raft 集群（multi-group raft）。
- 提供生产环境可参考的 PebbleDB 状态机实现。
- 对比自定义二进制快照序列化与 Thrift 业务序列化的性能。
- 包含一个基于 `hashicorp/memberlist` 的 gossip 成员发现示例。

> 注意：项目内文档与代码注释主要使用中文。示例代码中大量硬编码了 IP、端口和数据目录，运行前需要根据本地环境修改。

## 技术栈

- **语言**：Go 1.21.0
- **模块路径**：`github.com/xkeyideal/mraft`
- **Raft 引擎**：`github.com/lni/dragonboat/v3 v3.3.7`
- **本地存储**：`github.com/cockroachdb/pebble`（跟随 dragonboat 版本）
- **HTTP 框架**：`github.com/gin-gonic/gin v1.9.1`
- **序列化**：
  - 业务数据：`github.com/thrift-iterator/go`
  - 状态机命令（productready）：`github.com/ugorji/go/codec`（msgpack）
  - 快照：自定义二进制协议 + gzip
- **成员发现示例**：`github.com/hashicorp/memberlist v0.2.2`（gossip 包）
- **日志**：`go.uber.org/zap` + `gopkg.in/natefinch/lumberjack.v2`（轮转）
- **CLI 框架**：`github.com/spf13/cobra`（已声明但未实际使用）
- **Metrics**：`github.com/rcrowley/go-metrics`

## 项目结构

```
mraft/
├── benchmark/              # 性能测试
│   ├── binary-serialize/   # 自定义二进制快照序列化基准
│   ├── thrift-serialize/   # Thrift 序列化基准
│   ├── generate/           # 随机测试数据生成器
│   └── multi-raft/         # multi-raft TCP 与 Raft 压测客户端/服务端
├── config/                 # 顶层示例配置（OnDiskRaftConfig）
├── experiment/             # 学习与示例代码
│   ├── ondisk/             # 完整 ondisk 示例（多集群、HTTP API、状态机）
│   ├── simpleondisk/       # 简化版 ondisk 示例
│   └── store/              # 共享的 Thrift Command/RaftAttribute 模型
├── gossip/                 # memberlist gossip 实现 + Vivaldi 坐标
│   └── coordinate/         # 网络坐标算法实现与测试
├── logger/                 # 共享 zap 日志构造器
├── productready/           # “生产 ready”示例
│   ├── config/             # DynamicConfig 结构定义
│   ├── httpd/              # Gin HTTP 处理器
│   ├── ilogger/            # dragonboat 日志桥接
│   ├── storage/            # Raft 存储封装、状态机、命令
│   │   └── store/          # Pebble Store 抽象与快照流
│   ├── utils/              # 工具函数
│   └── main/               # 入口
├── test/                   # 临时/ scratches 程序，不是正式测试
└── .vscode/                # 遗留 tasks.json（仍引用 RocksDB CGO）
```

## 构建与运行

### 环境要求

- Go 1.21.0 或更高版本
- macOS / Linux（开发环境为 macOS）
- 无需 CGO：当前版本已移除 RocksDB，纯 Go 实现

### 构建命令

```bash
# 编译整个模块
go build ./...

# 运行示例 1：experiment/ondisk
go run experiment/ondisk/main/app.go 10000 9800

# 运行示例 2：experiment/simpleondisk
go run experiment/simpleondisk/main/main.go 10000 9800

# 运行示例 3：productready（需要补充 Join/InitialMembers 配置）
go run productready/main/app.go <httpPort> <raftPort>
```

### 运行说明

#### `experiment/ondisk`
- 入口：`experiment/ondisk/main/app.go`
- 参数：`nodeID` `HTTPPort`
- 固定节点 ID 为 `10000/10001/10002`，对应集群 ID 为 `14000/14100/14200`。
- 节点 `10003/10004/10005` 被硬编码为 join 节点，地址固定为 `10.181.20.34:11300` 等。
- 配置来源：`config/config.go` 中的 `OnDiskRaftConfig`。

#### `experiment/simpleondisk`
- 入口：`experiment/simpleondisk/main/main.go`
- 参数：`nodeID` `HTTPPort`
- 更简单，命令使用 JSON 编码。

#### `productready`
- 入口：`productready/main/app.go`
- 参数：`<httpPort> <raftPort>`（当前代码中两个参数都解析为 `os.Args[1]`，存在明显 bug）。
- **当前无法直接启动**：`DevelopDefaultDynamicConfig` 中 `Join` 与 `InitialMembers` 为空，必须补充后才能启动。
- 集群 ID 硬编码在 `productready/engine.go`：`clusterIds = []uint64{0, 1, 2}`。
- Node ID 规则：由 `utils.Addr2RaftNodeID` 将 IPv4:port 转换为 48 位 uint64。

## 测试

### 测试命令

```bash
# 全部测试（部分测试会失败，见下方说明）
go test ./...

# 可独立运行的测试包
go test ./gossip/coordinate/...
go test ./benchmark/binary-serialize
go test ./benchmark/thrift-serialize
go test ./benchmark/generate

# 运行基准测试
go test -bench=. -benchmem ./benchmark/binary-serialize
go test -bench=. -benchmem ./benchmark/thrift-serialize
go test -bench=. -benchmem ./benchmark/generate
```

### 测试现状

- **有正式测试的目录**：
  - `gossip/coordinate/*_test.go`：Vivaldi 坐标算法测试（可正常运行）。
  - `gossip/gossip_test.go`：gossip 集成测试（**会失败**，硬编码绑定 `10.181.22.31:800x`，当前环境通常无法访问）。
  - `benchmark/binary-serialize/binary-serialize_test.go`
  - `benchmark/thrift-serialize/thrift-serialize_test.go`
  - `benchmark/generate/generate_test.go`

- **没有测试的目录**：`productready/`、`experiment/ondisk`、`experiment/simpleondisk` 等核心示例逻辑均无单元测试。
- `test/` 目录下是临时 `main` 程序，不是 `go test` 测试。

### 已知构建/测试问题

1. `productready/engine.go:74` 存在 `fmt.Sprintf("%s", cfg.HttpPort)` 格式不匹配错误（`uint16` 类型使用了 `%s`），导致 `go test ./...` 中 `productready` 包构建失败。`go build ./...` 不会触发该错误，因为它不编译测试文件。
2. `gossip/gossip_test.go` 使用固定外网 IP 和端口，测试无法在通用环境通过。
3. `.vscode/tasks.json` 仍保留 RocksDB 的 `CGO_CFLAGS/CGO_LDFLAGS`，与当前纯 Go 代码不匹配，已过时。

## 代码风格与约定

- **注释语言**：代码注释主要使用中文，保留业务术语如 `状态机`、`clusterId`、`nodeId`、`Join`、`InitialMembers`。
- **包命名**：与业务语义一致，如 `productready`、`httpd`、`storage`、`store`。
- **错误处理**：示例代码中部分错误被忽略或用 `log.Fatal` 直接退出，生产改造时建议完善。
- **硬编码路径**：数据目录、日志目录、IP、端口大量硬编码（如 `/Users/xkey/test/raftdata`），需要根据部署环境修改。
- **日志**：
  - 通用日志：`logger/zaplog.go`，使用 CST（Asia/Shanghai）时区，支持文件轮转。
  - dragonboat 内部日志：`productready/ilogger/logger.go` 实现 `logger.ILogger` 接口，输出到 `dragonboat-<pkg>.log`。
  - Pebble 事件在 `productready/storage/store/pebbledb.go` 中通过 `pebbleLogger` 记录。
- **并发**：使用 `sync.RWMutex`、`go.uber.org/atomic`、`github.com/lni/goutils/syncutil.Stopper`。

## 主要模块说明

### productready/storage

- `storage.go`：封装 `dragonboat.NodeHost`，管理多 cluster 的生命周期、成员变更、leader 查询。
- `sm.go`：实现 `IOnDiskStateMachine`。
  - `Update`：解码命令，调用 `LocalInvoke` 写入 Pebble，同步 applied index。
  - `Lookup`：执行读命令。
  - `SaveSnapshot/RecoverFromSnapshot`：通过 Pebble snapshot 自定义二进制流式传输。
- `command.go / op.go / get.go / put.go / del.go`：定义 Raft 命令与本地执行逻辑。
- `config.go`：构造 dragonboat `NodeHostConfig` 与 `Config`。

### productready/storage/store

- `store.go`：Pebble `Store` 抽象，提供 `GetBytes`、`Batch`、`Write`、`GetSnapshot`。
- `pebbledb.go`：打开/关闭 Pebble DB，配置 WAL、cache、level、event listener。
- `utils.go`：Pebble 目录版本管理（`pebble.running`、`pebble.updating`）。
- 快照格式：自定义二进制，键值对以 LittleEndian uint64 长度前缀，值使用 gzip 压缩。

### productready/httpd

- 提供 HTTP API：
  - `GET /raft/info`：集群成员信息
  - `GET /raft/key?key=...&sync=true/false`：查询
  - `POST /raft/key?key=...&val=...`：写入
  - `DELETE /raft/key?key=...`：删除
  - `GET /raft/join?addr=...`：添加节点
  - `GET /raft/del?addr=...`：删除节点

### gossip

- 基于 `hashicorp/memberlist` 的成员发现示例。
- 消息使用 msgpack + gzip 编码。
- 包含 Vivaldi 网络坐标实现，用于延迟感知路由。
- **重要**：当前 `productready/storage/storage.go` 中 gossip 集成被注释掉，且 `cfg.Gossip == true` 会触发 `panic`。因此 `gossip/` 目前是独立示例/测试，未接入主流程。

## 配置说明

项目没有 JSON/YAML/TOML 配置文件，所有配置以 Go struct 硬编码：

| 配置位置 | 说明 |
|---------|------|
| `config/config.go` | `OnDiskRaftConfig`，供 `experiment/ondisk` 使用 |
| `productready/config/config.go` | `DynamicConfig` 结构定义（生产示例） |
| `productready/main/app.go` | `DevelopDefaultDynamicConfig`，默认只填了 `RaftDir/LogDir/IP` |
| `productready/engine.go` | `clusterIds = []uint64{0, 1, 2}` |
| `productready/storage/config.go` | dragonboat 详细参数（DeploymentID、RTT、ElectionRTT、HeartbeatRTT 等） |

### productready 启动所需字段

`DynamicConfig` 中必须提供：
- `NodeId`：节点唯一 ID，生成后不可变。
- `Join`：`map[clusterId]map[nodeId]bool`，标记每个 cluster 中哪些节点是后加入的。
- `InitialMembers`：`map[clusterId]map[nodeId]string`，每个 cluster 的初始成员及其 raft 地址。
- `IP/RaftPort/HttpPort`：本机地址与端口。

根据 README，作者在生产环境中将这些配置写入服务器本地文件，或由管理端推送。

## 部署与运维注意事项

- **节点加入流程**：
  1. 调用现有集群 HTTP 接口 `/raft/join?addr=<newNodeRaftAddr>`。
  2. 启动新节点，以 `join=true`、空 `InitialMembers` 启动。
  3. 集群通过 snapshot 自动同步数据。
  4. 更新所有节点的 `Join`/`InitialMembers` 配置（建议加版本号控制）。

- **重启规则**：
  - 已加入集群并生成数据的节点，重启时不再需要 `join=true`，也不需要再次提供初始成员。
  - **不要**将新节点加入原始 `peers` 配置后重启旧节点，否则会导致 `cluster settings are invalid` panic。
  - 被删除的节点不能再次加入集群。

- **两种寻址模式**（概念上存在，当前仅实现第一种）：
  1. 固定 `raftAddr` 模式：重启后 IP:port 不能变。
  2. gossip/`NodeHostID` 模式：IP 可变，但数据文件不能丢失；当前未在 `productready` 中实现。

- **关键 dragonboat 参数**（来自 `productready/storage/config.go`）：
  - `DeploymentID: 2023082513`
  - `RTTMillisecond: 200`
  - `ElectionRTT: 60`, `HeartbeatRTT: 6`
  - `SnapshotEntries: 2500000`
  - `CompactionOverhead: 250000`
  - `MaxInMemLogSize: 256 MB`
  - `EntryCompressionType: Snappy`
  - `MaxSendQueueSize: 128 MB`
  - `MaxSnapshotSendBytesPerSecond: 256 MB/s`

## 安全考虑

- 项目**未启用 TLS**：`MutualTLS: false`，默认部署在内网，认为网络可信。
- 示例 HTTP API 没有认证/鉴权机制。
- 数据目录和日志目录路径在代码中硬编码，生产部署需改为安全路径并设置合适权限。
- 日志轮转由 `lumberjack` 管理，默认最大 512MB、保留 300 个备份、30 天，启用压缩。
- gossip 包中的消息使用 msgpack + gzip，但没有加密或签名。

## 给 AI 助手的提示

1. 修改示例代码时，注意区分 `experiment/`（学习示例）和 `productready/`（生产参考实现）。
2. 若要新增 cluster 或修改节点 ID，需要同步修改硬编码的 `clusterIds`、`OnDiskRaftConfig.RaftClusterIDs`、`RaftNodePeers` 等。
3. 运行测试前，先确认环境是否满足 gossip 测试的固定网络地址要求。
4. 修改 `productready/engine.go` 的 `fmt.Sprintf` 时，注意 `cfg.HttpPort` 是 `uint16`。
5. 新增状态机命令时，需要在 `productready/storage/command.go`、`op.go` 以及 `get.go/put.go/del.go` 中同步扩展。
