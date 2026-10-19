# 订阅

`qdb_sub::catch_up` 是进程内的一次拉取。它按证券和可选周期过滤帧，并用客户端已经确认的序号续传。命令行上的持续监控是 [客户端](client.md) 的 `.watch`，它跟随新写入的 WAL。本版本没有 TCP、WebSocket 或 gRPC 推送。

```rust
pub struct SubFilter {
    pub symbol: String,
    pub period: Option<Period>,
    pub from_seq: u64,
}

pub enum SubMsg {
    SnapshotBegin,
    SnapshotEnd { snapshot_seq: u64 },
    Data(Frame),
    Gap { expected: u64 },
}

pub fn catch_up(engine: &Engine, filter: &SubFilter) -> Result<Vec<SubMsg>, SubError>
```

`symbol` 的规则与 `Engine::resolve` 相同。找不到或有歧义时返回 `symbol not found or ambiguous`。

`period` 为 `None` 时，该证券的全部分帧都算匹配，包括成交、报价和各周期 K 线。指定周期后，只保留 `period_id` 相等的帧。

## 从零开始

`from_seq = 0` 表示客户端还没有数据。返回：

1. `SnapshotBegin`
2. `SnapshotEnd { snapshot_seq }`，`snapshot_seq` 是当前 `applied_seq`
3. 匹配且 `seq > snapshot_seq` 的 `Data`

调用发生在写入返回之后时，已应用的帧不会出现在第 3 步。客户端应把 `snapshot_seq` 当作已经对齐的位置，再用 `from_seq = snapshot_seq` 拉取后续帧。当前库没有把快照正文放进这两条消息。

## 续传

`from_seq` 大于 0，且过滤结果里还找得到接得上的帧时，从 `from_seq + 1` 起向后扫描匹配帧：

- `seq` 小于期望值的帧跳过。同一序号上的后续帧因此只会交付第一条匹配帧。
- `seq` 等于期望值时输出 `Data`，期望值加 1。
- `seq` 大于期望值时输出 `Gap { expected }` 并停止。

`Gap` 表示过滤后的序号对不上。客户端应停止把后续帧当成连续更新，重新以快照方式对齐，或处理缺口后再继续。

若客户端给出的 `from_seq` 已经落后到所有匹配帧的序号都大于 `from_seq + 1`，函数改走快照路径，避免把一大段缺口伪装成增量。

`is_ordered` 只检查 `Data` 帧的序号没有下降。

## 帧里有什么

K 线订阅看到的是 `KlineUpdate`（3）和 `KlineClose`（4）。成交订阅在 `period = None` 时看到 `TickTrade`（1）。同一笔成交的 K 线帧与成交帧共享一个 `seq`。

载荷用 `qdb_abi::decode_trade` 或 `decode_kline` 解，两者都按小端读取，不依赖结构体内存布局。
