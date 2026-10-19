# 共享内存

`region.bin` 是本机热数据总线。写入进程发布帧，其他进程只读。文件不出这台机器。

`Engine::open` 会删掉已有的 `region.bin` 再按 WAL 重建。只读进程应在写入进程完成打开之后再映射。

## 布局

文件开头是 4096 字节头页。之后是证券槽、快照槽、K 线槽和环形缓冲。`Engine` 使用的开发配置：

| 参数 | 值 |
|---|---|
| 证券槽 | 1024 |
| 环形容量 | 4096 帧 |
| 读者槽 | 64 |
| 帧长 | 256 字节 |
| 容量约束 | 2 的幂 |

头页里的原子计数：

| 偏移 | 字段 |
|---|---|
| 256 | `write_seq` |
| 320 | `durable_seq` |
| 384 | `applied_seq` |
| 448 | `heartbeat_ns` |
| 512 | `epoch` |

魔数必须是 `0x51444231`，ABI 必须是 1，主机必须是小端。

## 发布

```rust
let mut region = ShmRegion::create(path, &RegionConfig::dev())?;
region.publish(&frame)?;
```

槽位是 `seq & (capacity - 1)`。发布前先把槽里的序号写成 0，写完载荷后再发布真实序号。读者看到序号与期望不一致时不会把旧载荷当成新帧。

`Engine` 只把触发写入的那一帧放进环。K 线帧在进程内的 `frames()` 里。

## 读取

```rust
let mut reader = RingReader::open(path)?;
loop {
    match reader.next()? {
        Some(frame) => handle(frame),
        None => break,
    }
}
```

`next` 在没有新帧时返回 `Ok(None)`。下面几种情况返回错误，调用方应停止沿用当前游标：

| 错误 | 何时 |
|---|---|
| `Lapped` | `write_seq - cursor >= capacity`，环已经绕了一圈 |
| `SeqGap` | 槽里的序号不是期望值 |
| `Torn` | 读取过程中槽被覆盖，或帧解不开 |
| `BadCrc` | 帧校验和不匹配 |
| `EpochChanged` | 打开之后头页里的 epoch 变了 |
| `BadMagic` | 魔数或 ABI 不对 |
| `BigEndian` | 主机不是小端 |

容量为 N 时，游标还停在 0 而 `write_seq` 已经到 N，下一次 `next` 就是 `Lapped`。读者要在环写满之前跟上。

## 快照槽

`SnapshotPod` 用 seqlock。`version` 为奇数表示写入中。`read_snapshot` 在版本为奇数或读取前后版本变化时返回 `Busy`，调用方重试。

`write_snapshot` 和 `read_snapshot` 当前只传递 `seq`、`last`、买一价和卖一价。`Engine` 的报价表在进程内存里，不自动写入这个槽。
