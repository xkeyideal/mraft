# productready/storage/store 并发与快照设计说明

本文档说明 `Store` 中 `db` 指针为什么同时使用 `atomic.Pointer` 和 `sync.RWMutex`，以及 `RecoverFromSnapshot` 在 Raft 生命周期中的触发场景。

---

## 1. 为什么 `db` 指针要加 `RWMutex`

### 1.1 `atomic.Pointer` 能保证什么

`Store.db` 使用 `atomic.Pointer[pebble.DB]` 是为了保证 **指针本身的读/写是原子的**。

```go
db := s.db.Load()       // 原子读
s.db.Swap(newdb)        // 原子替换
```

这能保证：

- 任何时刻调用者拿到的一定是“当时最新”的 `*pebble.DB` 值。
- 不会出现读取到撕裂的指针地址。

### 1.2 `atomic.Pointer` 不能保证什么

`atomic.Pointer` **不会**对指针背后的 `*pebble.DB` 对象做引用计数。

也就是说，下面这段代码不是原子的：

```go
// goroutine A
db := s.db.Load()          // 拿到旧 db
val, closer, _ := db.Get(key)
...
closer.Close()

// goroutine B
old := s.db.Swap(newdb)    // 换成新 db
old.Close()                // 关闭旧 db
```

即使 `Load` 和 `Swap` 各自是原子的，A 的“拿到旧 db → 使用旧 db → 用完关闭 closer” 与 B 的“Swap → Close 旧 db” 之间仍然存在竞态窗口。B 完全可能在 A 还在使用旧 db 时把它关掉。

在 `LoadSnapShotFromReader` 里正是这样实现的：

```go
old := s.db.Swap(newdb)
if old != nil {
    old.Close()
}
```

如果不加锁，其他 goroutine 拿到的旧 `*pebble.DB` 可能在 `Close()` 之后继续被访问，导致：

- `pebble.ErrClosed`
- 未定义行为（panic、数据竞争等）

### 1.3 需要防的是哪一种并发

dragonboat 会保证：**对同一个 cluster 的 `Update`、`Lookup`、`SaveSnapshot`、`RecoverFromSnapshot` 是串行执行的**。

所以 dragonboat 内部调用不会出问题。

但 `productready` 提供了一条**绕过 dragonboat 的本地直接读路径**：

```go
// productready/storage/op.go
if linearizable {
    err = cmd.RaftInvoke(ctx, s.nh, clusterId, s.csMap[clusterId])
} else {
    err = cmd.LocalInvoke(s.smMap[clusterId])   // 直接读 Store，不经过 Raft
}
```

当 `linearizable=false` 时，HTTP 请求直接访问 `Store`，不受 dragonboat 的串行化保护。如果此时该 cluster 正在执行 `RecoverFromSnapshot` 并关闭旧 db，直接读就可能访问到已关闭的 db。

因此 `dbMu` 的作用是：

> 把“使用 db 的整个过程”和“swap + close 旧 db 的过程”互斥开。

### 1.4 锁的使用规则

| 操作 | 锁类型 | 说明 |
|---|---|---|
| `GetBytes` / `Batch` / `Write` / `GetIterator` / `GetSnapshot` | `RLock` | 使用 db 期间旧 db 不会被关闭 |
| `LoadSnapShotFromReader` 中 `Swap + old.Close()` | `WLock` | 等所有读/写退出后再换 db |
| `Close` | `WLock` | 等所有读/写退出后再关闭 db |

### 1.5 性能影响

- 正常读写都是 `RLock`，无竞争时开销在纳秒级，远低于 Pebble 的 I/O 延迟。
- `WLock` 只在快照切换和关闭时触发，切换动作本身很快（只是指针替换 + 关闭文件句柄）。
- 每个 cluster 有独立的 `Store` 和 `dbMu`，不同 cluster 之间互不影响。

---

## 2. 什么时候会调用 `RecoverFromSnapshot`

`RecoverFromSnapshot` 在 `productready/storage/sm.go` 中实现，最终调用 `Store.LoadSnapShotFromReader`。

在 dragonboat / Raft 生命周期中，主要有以下几种情况会触发：

### 2.1 新节点加入集群

通过 `SyncRequestAddNode` 把新节点加入 cluster 后，leader 发现新节点没有日志和状态机数据。leader 会生成 snapshot 并发送给新节点，新节点收到后调用 `RecoverFromSnapshot` 用 snapshot 替换本地 Pebble DB。

### 2.2 慢 follower 被日志裁剪甩在后面

dragonboat 会按 `SnapshotEntries` / `CompactionOverhead` 配置做日志压缩。当某个 follower 因网络分区、宕机或处理过慢而落后太多，leader 上已经没有它需要追的日志时，leader 会发送 snapshot，follower 调用 `RecoverFromSnapshot`。

### 2.3 节点重启后从 snapshot 恢复

如果某个节点的 raft log 损坏、被清理，或者 `NodeHost` 数据目录丢失但 snapshot 仍存在，dragonboat 启动时可能会用 snapshot 重建状态机。

### 2.4 手动触发

dragonboat 支持通过 `RequestSnapshot` 等接口主动请求 snapshot，但 `productready` 当前代码中没有显式调用。

---

## 3. 快照恢复期间的读写行为

由于 `RecoverFromSnapshot` 会替换并关闭旧 db，所有经过 `Store` 的读写在这期间都会受 `dbMu` 调度：

- 快照恢复到达 `Swap + Close` 前，读/写可以正常并发执行（`RLock`）。
- 到达 `Swap + Close` 时，`WLock` 会等待所有正在执行的读/写完成。
- 切换完成后，后续读/写拿到新 db。

**线性化读（走 `RaftInvoke`）**最终也会通过 `StateMachine.Lookup` 调用 `Store.GetBytes`，因此同样会获取 `dbMu.RLock`。不过 dragonboat 会先把线性化读调度到状态机上，并保证同一个 cluster 的 `Update/Lookup/SaveSnapshot/RecoverFromSnapshot` 串行执行，所以即使不加 `dbMu`，线性化读本身也不会和 `RecoverFromSnapshot` 并发。

**非线性化本地读**就不一样了：它直接调用 `cmd.LocalInvoke(s.smMap[clusterId])`，绕过 dragonboat 的调度。如果没有 `dbMu`，它可能在 `RecoverFromSnapshot` 关闭旧 db 的过程中访问旧 db，从而拿到 `pebble.ErrClosed` 或更严重的未定义行为。

因此 `dbMu` 的实际作用是：

> 为所有 `Store` 操作提供一层防御性保护，**尤其**是为了堵住“绕过 dragonboat 的直接本地读写”与“快照恢复关闭旧 db”之间的竞态窗口。

---

## 4. 小结

- `atomic.Pointer` 负责指针本身的原子读/写。
- `sync.RWMutex` 负责保护 `*pebble.DB` 对象的生命周期，防止 `LoadSnapShotFromReader` 关闭旧 db 时还有其他 goroutine 在使用它。
- 这把锁**保护所有经过 `Store` 的读写**，但之所以必须加，核心原因是存在**绕过 dragonboat 的直接本地读写**路径。
- `RecoverFromSnapshot` 在新节点加入、慢 follower 追不上、节点灾难恢复等场景下被触发。
