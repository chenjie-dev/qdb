# 库接口

当前写入、查询和订阅都在进程内完成。在 `Cargo.toml` 里按需依赖：

```toml
qdb-store = { path = "crates/qdb-store" }  # Engine
qdb-sql   = { path = "crates/qdb-sql" }
qdb-sub   = { path = "crates/qdb-sub" }
qdb-abi   = { path = "crates/qdb-abi" }    # 周期、帧、报价结构
qdb-shm   = { path = "crates/qdb-shm" }    # 另进程只读环形缓冲
```

热路径不使用 serde。价格和时间在进入这些函数之前就应是整数。

## 多个库

`Catalog` 管一个实例目录。`open` 保证有库 `default`。`create` 只建目录和空 WAL，不映射共享内存。`open_database` 才是该库的写入者。

```rust
let catalog = qdb_store::Catalog::open(std::path::Path::new("data"))?;
catalog.create("ashare")?;
let mut ashare = catalog.open_database("ashare")?;
```

每个库的 `seq` 从 1 单独递增。只读用 `open_read` 和 `open_database_read`。

`dump` 把库里已经提交的记录写成 `QDBDUMP1` 文件。`restore` 写回一个空库的 WAL，再打开就得到原来的序号和 K 线。`replay_file` 把这些记录按顺序重新写入一个打开的库，并分配新的序号。`playback` 只回放并返回成交和报价，不改调用方的库。

## 打开

```rust
pub fn Engine::open(root: &Path) -> Result<Engine, StoreError>
```

创建 `wal/` 和 `snap/`，重放 `wal/wal-1.qwal`，然后重建 `region.bin`。重放遇到坏 CRC 时，停在最后一条完好记录，并把引擎标成不可再写。

```rust
pub fn Engine::open_read(root: &Path) -> Result<Engine, StoreError>
```

`open_read` 只重放 WAL。目录不存在时返回错误。它不创建文件，不追加日志，也不改 `region.bin`。之后的 `write_trade`、`write_quote`、`write_clock` 返回 `read-only connection`。命令行客户端用的就是这个入口。

```rust
pub fn poll(&mut self) -> Result<Vec<Change>, StoreError>
```

`poll` 只读取打开之后新 `fsync` 的记录，并更新这次打开的内存表。返回的 `Change` 按表区分：`ticks`、`snapshots`、`klines`。没有新记录时返回空向量。坏 CRC 返回 `wal crc failed`。文件尾部不足一条记录时忽略，不视为损坏。

开发配置的共享内存是 1024 个证券槽、4096 帧环形缓冲。这小于 ABI 里的默认值 65536。

## 写入

```rust
pub fn upsert_symbol(&mut self, exchange: &str, code: &str) -> Result<u32, StoreError>
pub fn write_trade(&mut self, exchange: &str, code: &str, event_ts_ns: i64, price: i64, volume: i64) -> Result<u64, StoreError>
pub fn write_quote(&mut self, exchange: &str, code: &str, event_ts_ns: i64, quote: &TickQuote) -> Result<u64, StoreError>
pub fn write_clock(&mut self, exchange: &str, watermark_ns: i64) -> Result<u64, StoreError>
```

返回值：`upsert_symbol` 返回 `symbol_id`，其余返回这条记录的 `seq`。

`write_trade` 和 `write_quote` 会在证券不存在时自动注册。`exchange` 为空返回 `EmptyExchange`。

```rust
let quote = qdb_abi::TickQuote {
    last: 1_050_000,
    volume: 0,
    amount: 0,
    bid_px: [1_049_000, 0, 0, 0, 0],
    bid_qty: [0; 5],
    ask_px: [1_051_000, 0, 0, 0, 0],
    ask_qty: [0; 5],
};
engine.write_quote("SSE", "600000", ts, &quote)?;
```

落盘的报价只有 `last`、`bid_px[0]` 和 `ask_px[0]`。

`write_clock` 不产生成交。水位到达桶结束时间后，该交易所标识下的未闭合 K 线闭合。其他标识的桶不受影响。服务进程不会自动发送时钟，安静的市场要由业务系统自己写水位。

## 读取

```rust
pub fn resolve(&self, text: &str) -> Result<u32, StoreError>
pub fn symbols(&self) -> &[Symbol]
pub fn ticks(&self, symbol_id: u32) -> &[TickRow]
pub fn quote(&self, symbol_id: u32) -> Option<&QuoteRow>
pub fn klines(&self, symbol_id: u32, period: Period) -> Vec<KlineBody>
pub fn frames(&self) -> &[Frame]
pub fn applied_seq(&self) -> u64
pub fn metrics(&self) -> Metrics
pub fn crc_failed(&self) -> bool
```

`resolve("SSE:600000")` 按标签和代码查找。`resolve("600000")` 仅在全库只有一只这个代码时成功。

`klines` 返回已闭合桶，并在末尾附上未闭合桶。没有数据时返回空向量。

`frames` 包含触发帧和派生 K 线帧，订阅用这份列表。`region.bin` 里只有触发帧。

## 指标

`Metrics` 里的计数器是 `Arc<AtomicU64>` 或 `Arc<AtomicBool>`，可以交给别的线程读取。

| 字段 | 含义 |
|---|---|
| `durable_seq` | 已落盘序号 |
| `applied_seq` | 已应用序号 |
| `write_seq` | 已发布序号 |
| `gap_total` | 缺口次数。当前写入路径不增加它 |
| `late_tick_total` | 时间早于当前桶、因而没进入 K 线的成交数 |
| `crc_failed` | WAL 重放发现坏 CRC |
| `heartbeat_ns` | 最近一次写入的 UTC 纳秒 |
| `subscriber_count` | 预留。当前订阅函数不增加它 |

## 直接使用聚合器

`qdb_agg::KlineAggregator` 可以不经 WAL 使用，适合测试和基准。它同样要求序号连续，并按 UTC 分桶。`Engine` 已经在内部调用它，业务写入应走 `Engine`，否则不会进 WAL 和查询表。
