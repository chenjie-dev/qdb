# 持久化与恢复

已经能被查询看到的记录，都先到过 WAL。当前每条记录单独 `fsync`，所以一次 `write_trade` 的耗时主要在磁盘同步，而不是内存更新。

## 目录

```text
root/
  wal/wal-1.qwal
  snap/
  region.bin
```

只有 `wal/wal-1.qwal` 参与恢复。`snap/` 会在打开时创建，当前没有检查点内容。`region.bin` 是缓存，打开时删除并重建。

## 记录格式

每条记录是小端的：

```text
crc32:u32 | body_len:u32 | body
body = seq:u64 | msg_type:u16 | flags:u16 | payload
```

`crc32` 覆盖 `body`。类型和载荷：

| 类型 | 载荷 |
|---|---|
| `SymbolUpsert` | `symbol_id:u32`、`exchange_id:u16`、交易所标签 16 字节、代码 32 字节 |
| `TickTrade` | `symbol_id:u32`、`event_ts_ns:i64`、`TickTrade` 32 字节 |
| `TickQuote` | `symbol_id:u32`、`last:i64`、买一 `i64`、卖一 `i64` |
| `ClockTick` | `exchange_id:u16`、`watermark_ns:i64` |

文件末尾不足 `crc + 长度 + body` 时，启动把它当作没写完的尾巴丢掉，`crc_failed` 仍为 false。

CRC 对不上时，重放停在这条之前。已读出的记录会应用，`crc_failed` 变为 true。之后 `write_trade`、`write_quote`、`write_clock` 和新增证券都返回 `wal crc failed`。修复方法是换一份完好的 WAL，或在确认尾巴可以丢弃后截掉坏记录再打开。库不会自动跳过坏记录继续写。

## 启动

`Engine::open`：

1. 读完 WAL。
2. 按记录顺序重建证券表、成交、报价和 K 线。
3. 把 `next_seq` 设为最后已应用序号加 1。
4. 重建共享内存，并把 `durable_seq` 和 `applied_seq` 设成已应用序号。

重放使用 WAL 里保存的交易所编号和标签。内部编号在重启后保持不变。

K 线不单独落盘。它们由成交和时钟重新聚合，桶边界仍是 UTC。

## 还没有的能力

- 按 1 毫秒或 256 条合并 `fsync`
- 把 K 线和报价写入 `snap/` 后从快照加尾部 WAL 恢复
- 写者文件锁。约定上只有一个写者，代码还没有占用 `writer.lock`
- 复制到另一台机器

这些行为以设计文档为准，当前打开和写入路径不提供。
