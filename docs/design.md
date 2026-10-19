# QDB 设计

QDB（Quote Database）是单机优先的股市行情内存数据库。同一台机器上，唯一写入进程把行情写入共享内存环形缓冲；数据库、订阅网关和策略进程只读。K 线、SQL 和最新快照由引擎在这份流上维护。崩溃恢复依赖 WAL 和定期快照。共享内存只保存热数据，丢失后可以由 WAL 重建。

分布式按「分区单写 + WAL 复制」预留：共享内存不出节点。第一版只实现单机。

## 1. 目标

第一版要能在单机上完成这条路径：

1. 采集端把逐笔成交写成固定布局的共享内存帧，并带严格递增的 `seq`。
2. 引擎把 Tick 收成多周期 K 线，维护最新盘口快照。
3. 客户端用 SQL 查询历史 K 线和最新价。
4. 订阅端按交易所、代码、周期接收有序增量，断线后用 `from_seq` 续传。
5. 进程被杀死后，从最近快照加 WAL 回到同一 `seq` 和同一组 K 线。

延迟预算是设计目标，不是已测量结果：

- 共享内存发布之后，同机读者在微秒级看见已提交帧。
- 可见性和崩溃不丢数据绑在一起：一组记录先 `fsync` 到 WAL，再发布到共享内存。默认组提交间隔 1ms 或 256 条，先到先刷。

明确不做：

- 把复权结果写进 K 线。查询时复权见 [企业级缺口](2026-10-19-enterprise-broker-md.md)
- 财务指标、交易下单、风控裁决
- 把共享内存当跨机器传输
- 同一只股票多个写入者
- 第一版实现 Raft、WebSocket、gRPC、完整 PostgreSQL 协议

券商全品种、逐笔委托、订单簿、多源、权限和历史库尚未进入本设计，列在 [2026-10-19-enterprise-broker-md.md](2026-10-19-enterprise-broker-md.md)。任务没有点名时不实现。

## 2. 总体结构

三个平面分开：

| 平面 | 职责 | 第一版 |
| --- | --- | --- |
| 数据平面 | Tick 进入、分配 `seq`、写 WAL、更新内存表、发布共享内存 | 实现 |
| 查询平面 | SQL、最新快照、K 线扫描 | 实现基础子集 |
| 控制平面 | 分区领导权、`leader_epoch`、成员 | 只占位字段 |

```mermaid
flowchart LR
  feed[行情源] --> writer[写入线程]
  writer --> wal[WAL]
  wal --> writer
  writer --> mem[内存表]
  writer --> shm[共享内存]
  shm --> engineRead[本机只读进程]
  shm --> gateway[订阅网关]
  mem --> sql[SQL]
  wal --> recovery[启动恢复]
  snap[定期快照] --> recovery
  recovery --> mem
```

单机进程可以是一个二进制 `qdb`，内部仍按写入线程、SQL 线程、订阅线程拆开。共享内存区域照样创建，外部进程可以挂上来只读。这样第一版不必先做多进程部署，ABI 也不用返工。

目标部署再拆成四个角色，仍然只有一个写入者：

| 角色 | 写共享内存 | 说明 |
| --- | --- | --- |
| `qdb-ingest` | 是 | 解码、分配 `seq`、WAL、聚合、发布 |
| `qdb-sql` | 否 | 可以和 ingest 同进程。拆进程后通过只读共享内存读快照和未闭合 K 线，历史段通过本地快照文件 |
| `qdb-sub` | 否 | 从环形缓冲和 WAL 做过滤与推送 |
| 策略进程 | 否 | 只映射共享内存或走订阅 |

每台机器只运行一个 `qdb` 进程。它在打开任何库之前，用排他文件锁占住实例锁（缺省 `/tmp/qdb.instance.lock`）。锁已被占用时，第二个进程直接退出，避免两个进程同时改同一块共享内存。进程退出后锁释放。读者只读映射，不抢这把锁。

一个实例目录里可以有多个库。每个库单独分配 `seq`，单独保存 WAL 和 `region.bin`。库名是字母或下划线开头的标识。已经直接写在实例根目录的 `wal/` 视为库 `default`。新建的库放在 `db/<name>/`。查询和订阅一次只打开其中一个库。

## 3. 共享内存 IPC

共享内存是同机、小端、固定布局的热总线。x86_64 和 aarch64 可用。启动时检查字节序标记，大端机器拒绝启动。

区域内不允许出现指针、`Vec`、`String`、`HashMap` 或带堆分配的结构。引擎私有内存里的索引可以用这些类型，那些结构不映射给别的进程。

### 3.1 区域

区域在创建时定长，一代之内不能扩容。要扩大容量就新建一代，`epoch` 加一，读者重新挂载。

默认容量：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `max_symbols` | 65536 | 符号 ID 从 1 开始，0 表示无效 |
| `frame_size` | 256 | 含帧头 |
| `ring_capacity` | 65536 | 帧数，开发默认约 16MB。生产可调到 2 的幂 |
| `reader_slots` | 64 | 已注册读者，用于水位。未注册读者仍可自检 gap |
| `period_slots` | 8 | 未闭合 K 线表预留，第一版用 6 个周期 |
| 文件权限 | `0640` | 属主可写，同组只读 |

逻辑布局：

```text
[Header 4KB]
[ReaderSlot × 64，每槽独占一条缓存线]
[SymbolRec × max_symbols]
[SnapshotPod × max_symbols]
[KlinePod × max_symbols × period_slots]
[Frame × ring_capacity]
```

头部里的 offset 和 length 是权威值。读者先校验 `magic` 和 `abi_version`，再按 offset 找各段，不在代码里写死绝对地址。

头部前 128 字节是不可变元数据：

| 偏移 | 字段 | 类型 |
| --- | --- | --- |
| 0 | `magic` | `u32`，`0x5144_4231`（`QDB1`） |
| 4 | `abi_version` | `u32`，第一版为 1 |
| 8 | `endian_check` | `u32`，`0x0102_0304` |
| 12 | `frame_size` | `u32` |
| 16 | `capacity` | `u32`，2 的幂 |
| 20 | `capacity_mask` | `u32` |
| 24 | `max_symbols` | `u32` |
| 28 | `period_slots` | `u32` |
| 32 | `snapshot_stride` | `u32` |
| 36 | `flags` | `u32` |
| 48 | `symbol_off` / 各段 offset 与 length | `u64` |
| 120 | `cluster_id`、`node_id`、`partition_id` | 分布式占位，单机为 0 / 1 / 0 |

热原子量各自占一条 64 字节缓存线，避免和读者槽伪共享：

| 缓存线起点 | 字段 | 含义 |
| --- | --- | --- |
| 256 | `write_seq` | 已发布到环形缓冲的最大 `seq` |
| 320 | `durable_seq` | 已 `fsync` 的最大 `seq` |
| 384 | `applied_seq` | 内存表已可见的最大 `seq` |
| 448 | `heartbeat_ns` | 写入者存活时间，单调时钟纳秒 |
| 512 | `epoch` | 区域代际。重建区域时加一 |
| 520 | `symbol_count` | 已提交的符号目录项数 |

`write_seq == 0` 表示还没有发布过数据。有效 `seq` 从 1 开始。

读者槽：

```rust
#[repr(C, align(64))]
struct ReaderSlot {
    active: AtomicU64,       // 0 为空，否则为 reader_id
    cursor_seq: AtomicU64,   // 该读者已消费到的 seq
    heartbeat_ns: AtomicU64,
    pid: u32,
    name: [u8; 16],
}
```

注册用 CAS 抢空槽。心跳超过 3 秒的槽可以回收。写入者扫描这些槽计算最慢读者，用于水位，不根据水位停写。

### 3.2 帧

每条消息都是 256 字节。帧头 48 字节，payload 208 字节。payload 按消息类型使用固定结构，余下字节写 0。`payload_len` 表示有效字节数。

```rust
#[repr(C)]
struct FrameHeader {
    seq: u64,
    event_ts_ns: i64,
    recv_ts_ns: i64,
    payload_len: u32,
    msg_type: u16,
    flags: u16,
    symbol_id: u32,
    exchange_id: u16,
    period_id: u16,
    partition_id: u16,
    reserved: u16,
    crc32: u32,
}
```

`crc32` 覆盖帧头里 crc 字段之前的字节加上 `payload_len` 个 payload 字节。第一版校验 crc。生产若剖析显示开销过大，可以改成抽样校验，但布局保留该字段。

消息类型：

| 值 | 名称 | 含义 |
| --- | --- | --- |
| 1 | `TickTrade` | 逐笔成交 |
| 2 | `TickQuote` | 盘口快照 |
| 3 | `KlineUpdate` | 未闭合 K 线的派生通知 |
| 4 | `KlineClose` | 闭合通知 |
| 5 | `SymbolUpsert` | 新符号 |
| 6 | `ClockTick` | 某个交易所的事件时间水位 |
| 7 | `GapAck` | 写入者确认跳过一段无法补回的 seq |

标志位：

| 位 | 名称 | 含义 |
| --- | --- | --- |
| 0 | `FROM_WAL` | 这条是重放出来的，不是实时流 |
| 1 | `TS_REGRESSION` | 事件时间相对上一条同符号成交回退 |
| 2 | `CORRECTION` | 对已闭合 K 线的新修订版 |

心跳不进入环形缓冲，只更新 `heartbeat_ns`。这样静默行情不会制造假 gap，WAL 也不会被心跳撑满。

### 3.3 序列号

`seq` 是分区内输入事件的全序，由唯一写入者分配，严格加一。输入事件包括成交、报价、符号登记、`ClockTick` 和 `GapAck`。

K 线是这些事件的派生结果。未闭合 K 线携带产生它的那个 `seq`，再用 `revision` 区分同一根 K 线的多次更新。订阅通道上的顺序键是 `(seq, out_idx)`：`out_idx = 0` 是原始 Tick，其后是同一 `seq` 派生出的各周期 K 线通知。

`seq` 不按交易所时间排序。时间回退仍分配更大的 `seq`，并打上 `TS_REGRESSION`。聚合窗口看 `event_ts_ns`，流的顺序看 `seq`。

### 3.4 发布与读取

写入者只有一个线程会碰 WAL、内存表和共享内存。网络接收可以放在前面的 SPSC 队列里，出队之后才分配 `seq`。

一组记录的提交顺序：

1. 写入线程为这组事件分配连续 `seq`。
2. 追加 WAL，并 `fsync`。成功后把 `durable_seq` 设为这组最后的 `seq`（Release）。
3. 应用到内存表：符号目录、Tick 段、K 线、快照。
4. 把 `applied_seq` 设为这组最后的 `seq`（Release）。SQL 只看见小于等于该值的状态。
5. 把帧写入环形缓冲。先把槽位里的 `seq` 写成 0 并写完 payload，再对帧头 `seq` 做 Release 存储。
6. 对 `write_seq` 做 Release 存储。

第 5 步和第 6 步之间崩溃时，读者可能看到新槽位但 `write_seq` 仍是旧值，这会被当成未发布。恢复后 `seq` 从 WAL 继续，不回退。

读者：

1. Acquire 读取 `epoch`。和打开区域时看到的不同就重新挂载。
2. Acquire 读取 `write_seq` 作为 `tail`。
3. 本地 `cursor >= tail` 则没有新数据。
4. `tail - cursor >= capacity` 则被覆盖，进入 gap 路径，禁止继续逐帧猜。
5. 槽位下标是 `(cursor + 1) & capacity_mask`。Acquire 读取帧上的 `seq`。
6. `seq` 必须等于 `cursor + 1`。不相等则重试几次；仍然不相等就认定撕裂或覆盖，进入 gap 路径。
7. 拷贝 256 字节到调用方缓冲区，再次读取 `seq`。若已变化，丢弃这次拷贝并进入 gap 路径。
8. 校验 crc 和 `payload_len`。

拷贝 256 字节是读者的安全边界。调用方拿到的字节块和共享内存布局相同，不再反序列化成 `String` 或动态结构。若调用方保证在被覆盖前处理完，可以在持有槽位期间直接读映射内存；第一版 API 默认拷贝，避免策略线程把悬空引用带出轮询函数。

被覆盖的检测在读者侧完成。写入者发现慢读者只增加水位指标，不阻塞、不覆盖时等待。丢失对慢读者可见：返回 `Lapped { expected, tail, capacity }`，而不是把下一条当成连续数据。

### 3.5 快照与未闭合 K 线的版本

最新价和未闭合 K 线用 seqlock，供任意只读进程零拷贝读取。

```text
version 为奇数：写入中
version 为偶数：稳定
```

写入：`version` 加一变成奇数（Release），写字段，再加一变成偶数（Release）。

读取：Acquire 读到偶数 `v1`，读字段，再 Acquire 读 `v2`。`v1 == v2` 才使用。奇数就自旋，自旋有上限，超过后返回 `Busy`。

`version` 单调增加。快照的 `seq` 是产生该快照的输入 `seq`。读者用 `(version, seq)` 做一致性校验：读前版本和读后版本不同，整次读取作废。

符号目录只追加。写入者写完一条 `SymbolRec` 后，才 Release 增加 `symbol_count`。读者只扫描 `[0, symbol_count)`。符号 ID 不复用。

### 3.6 背压

写入路径对慢读者没有背压停顿。背压信号是显式 gap：

| 情况 | 谁发现 | 结果 |
| --- | --- | --- |
| 读者游标被环形缓冲套圈 | 读者，以及已注册槽的水位 | `Lapped`。若 WAL 仍保留该 `seq`，从 WAL 续传；否则要求先取快照 |
| 帧 `seq` 与期望不一致 | 读者 | 同上 |
| 上游行情本身缺号 | 写入者 | 聚合暂停。不把缺号前后的 Tick 合成一根看起来完整的 K 线 |
| 订阅发送队列满 | 订阅网关 | 断开该订阅者的实时流，标记 `Gap`，等它用 `from_seq` 或快照回来 |

环形缓冲使用率：

```text
usage = (write_seq - min_registered_cursor) / capacity
```

没有已注册读者时，使用率按写入者自己的 `applied_seq` 计算，只说明引擎是否跟上。指标超过 0.7 告警，超过 1 表示至少有一个注册读者已经被套圈。

## 4. 数据模型

价格和金额用 `i64` 定点。每个符号带 `price_scale` 和 `amount_scale`，表示小数位数对应的除数，例如价格 4 位小数则 `price_scale = 10000`。存储和聚合不做 `f64`。SQL 输出价格时按该符号的 scale 格式化。

时间是 UTC 纳秒 `i64`，字段名 `event_ts_ns`。这是交易所事件时间。`recv_ts_ns` 是本机收到时间，只用于延迟指标。

成交量单位是股，不是手。集合竞价成交用 Tick 标志区分，第一版不单做竞价状态机。

复权因子不进 K 线。库内保存交易所原始价。以后若要复权，另加只读因子表，查询时计算。

### 4.1 符号与交易所

代码在帧里只出现为 `symbol_id`。字符串放在符号目录的定长数组中。

```rust
#[repr(C, align(64))]
struct SymbolRec {
    symbol_id: u32,
    exchange_id: u16,
    price_scale: u16,
    amount_scale: u16,
    lot_size: u32,
    status: u16,
    code: [u8; 32],
}
```

交易所代码和符号都是调用方给出的字符串。库第一次见到某个代码时分配内部整数 ID，不维护交易所名录，也不保存时区。事件时间一律是 UTC。

`000001` 在上交所和深交所不是同一标的。SQL 和订阅使用 `EXCHANGE:CODE`，例如 `SZSE:000001`。只有在目录里全局唯一时才允许省略交易所，否则查询报歧义错误。

引擎进程内可以用 `HashMap<(exchange_id, code), symbol_id>` 做解析。这张表由符号目录重建，可以整体替换。它不进入共享内存。

### 4.2 Tick

5 档盘口可以放进单帧 payload（192 字节，小于 208）。第一版深度上限是 5。更深的盘口以后用续帧，不改变现有 Tick 布局。

```rust
#[repr(C)]
struct TickTrade {
    price: i64,
    volume: i64,
    amount: i64,
    trade_id: u64,
}

#[repr(C)]
struct TickQuote {
    last: i64,
    volume: i64,
    amount: i64,
    bid_px: [i64; 5],
    bid_qty: [i64; 5],
    ask_px: [i64; 5],
    ask_qty: [i64; 5],
}
```

Tick 只追加。成交表不更新旧行。报价更新的是快照槽，同时追加一条 `TickQuote` 事件，供订阅和审计使用。

内存里的 Tick 表按 `symbol_id` 分片，每片是按 `seq` 排列的行段：

```text
Segment { symbol_id, start_seq, end_seq, start_ts, end_ts, rows: Vec<TickRow> }
```

这是引擎私有内存。环形缓冲被覆盖之后，SQL 仍能查询保留窗口内的 Tick。默认保留最近 2 小时，可配置。超出保留窗口的 Tick 查询返回 `RetentionExceeded`，不静默截断。更早的数据留在 WAL 保留期内，供恢复和续传，不保证都能被 SQL 扫到。

同一 `(exchange_id, trade_id)` 在去重窗口内出现两次时，第二次不分配新 `seq`。去重窗口默认 65536 条或 60 秒。

### 4.3 K 线

周期：

| `period_id` | 名称 | 对齐 |
| --- | --- | --- |
| 1 | `1s` | 交易所当地时间，按秒取整 |
| 60 | `1m` | 按分钟取整 |
| 300 | `5m` | 按 5 分钟取整 |
| 900 | `15m` | 按 15 分钟取整 |
| 3600 | `1h` | 按小时取整 |
| 14400 | `4h` | 按 4 小时取整。连续交易常用 |
| 65535 | `1d` | 有时段的市场按交易所当地交易日。`BINANCE` 按 UTC 自然日。86400 超出帧头 `u16`，用 `u16::MAX` 表示 |

事件时间是 UTC 纳秒。`1d` 和分钟线都按 UTC 取桶。业务系统若要按某个交易所的本地日期切线，先把时间换算成对应的 UTC 再写入。需要收线时由业务系统写入 `ClockTick`。

```rust
#[repr(C, align(64))]
struct KlinePod {
    version: AtomicU64,
    seq: u64,
    bucket_start_ns: i64,
    symbol_id: u32,
    exchange_id: u16,
    period_id: u16,
    state: u8,       // 0 未闭合，1 已闭合
    origin: u8,      // 0 实时派生，1 导入，2 修订
    revision: u16,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: i64,
    amount: i64,
    tick_count: u32,
    flags: u32,      // bit0: incomplete，中间有 GapAck
}
```

未闭合 K 线放在共享内存的二维表 `[symbol_id][period_index]`，每个周期每只股票最多一根。已闭合 K 线追加到引擎私有的按 `(symbol_id, period_id)` 排序的段里，不在原槽改写。

修订已闭合 K 线时追加一根相同 `bucket_start_ns`、更大 `revision` 的记录。查询取该桶最大 `revision`。旧修订保留，直到快照压缩。`origin = import` 的桶不会被实时 Tick 改写；晚到的 Tick 记为异常。

### 4.4 快照

```rust
#[repr(C, align(64))]
struct SnapshotPod {
    version: AtomicU64,
    seq: u64,
    event_ts_ns: i64,
    recv_ts_ns: i64,
    symbol_id: u32,
    exchange_id: u16,
    depth: u16,
    last: i64,
    volume: i64,
    amount: i64,
    bid_px: [i64; 5],
    bid_qty: [i64; 5],
    ask_px: [i64; 5],
    ask_qty: [i64; 5],
}
```

下标就是 `symbol_id`。`TickTrade` 更新 `last`、累计量和额。`TickQuote` 更新五档和最新价。读取协议见 3.5。

### 4.5 只追加规则

| 数据 | 规则 |
| --- | --- |
| Tick 段 | 只追加。保留期到期整段丢掉 |
| 未闭合 K 线 | 允许在自己的槽里更新，`version` 每次加二（seqlock 的两次加一） |
| 已闭合 K 线 | 不覆盖。修正是新 `revision` |
| 快照 | 每个符号一个槽，允许覆盖，靠 `version` 防止读到半写 |
| 符号 | 只追加。状态变化追加一条新记录并更新引擎侧当前指针；第一版状态只有活跃一种，目录项不可变 |
| WAL | 只追加 |

## 5. SQL

第一版语法是固定子集，手写递归下降解析，不接入通用 SQL 解析器。这样语法边界就是代码边界。

```text
statement   = select
select      = "SELECT" projection "FROM" table
              ["WHERE" expr] ["GROUP BY" group] ["ORDER BY" order] ["LIMIT" integer]
projection  = "*" | item { "," item }
item        = ident | agg "(" ident ")"
agg         = "MAX" | "MIN" | "SUM" | "AVG" | "FIRST" | "LAST" | "COUNT"
table       = "ticks" | "klines" | "snapshots" | "symbols"
expr        = or
or          = and { "OR" and }
and         = pred { "AND" pred }
pred        = ident comp literal
            | ident "BETWEEN" literal "AND" literal
comp        = "=" | "<>" | "<" | "<=" | ">" | ">="
group       = ident { "," ident }
order       = ident ["ASC" | "DESC"] { "," ident ["ASC" | "DESC"] }
```

不支持 `JOIN`、子查询、`HAVING`、窗口函数、`INSERT`、`UPDATE`、`DELETE`。写入只来自行情流和恢复，不来自 SQL。

标识符和字符串用单引号字面量。绑定参数放到后续协议阶段；第一版 SQL 文本里直接写字面量。

### 5.1 表

| 表 | 必要过滤 | 内容 |
| --- | --- | --- |
| `ticks` | `symbol` 可省略；省略时返回全部代码，并用 `LIMIT` 截断 | 保留窗口内的逐笔 |
| `klines` | `symbol` 和 `period` | 已闭合段加一根未闭合 |
| `snapshots` | 可选 `symbol` | 最新价和五档。无 `WHERE` 时必须有 `LIMIT` |
| `symbols` | 无 | 符号目录 |

时间列名是 `ts`，类型为事件时间。也接受 `seq` 过滤。

`klines.period` 取 `1s`、`1m`、`5m`、`15m`、`1h`、`1d`。

示例：

```sql
SELECT ts, open, high, low, close, volume, amount, seq
FROM klines
WHERE symbol = 'SZSE:000001' AND period = '1m' AND ts BETWEEN 1710000000000000000 AND 1710086400000000000
ORDER BY ts ASC
LIMIT 500;

SELECT last, bid_px1, ask_px1, seq
FROM snapshots
WHERE symbol = 'SZSE:000001';
```

`SELECT` 列表里的 `bid_px1` 到 `bid_px5` 对应数组槽位，避免第一版做数组语法。

没有 `LIMIT` 时默认 1000，硬上限 100000。超过硬上限返回错误，要求调用方缩小范围。

### 5.2 执行

1. 解析并校验聚合是否出现在允许的位置。有 `GROUP BY` 时，选择列表里的非聚合列必须出现在分组键中。
2. 解析 `symbol`，得到 `symbol_id`。scale 不同的符号不能出现在同一个 `SUM` / `AVG` 里，除非按 `symbol` 分组。
3. `klines` 对 `(symbol_id, period_id, bucket_start_ns)` 二分。`ticks` 对时间二分。`snapshots` 直接读 seqlock 槽。
4. 查询开始时 Acquire 读取 `applied_seq`。扫描跳过 `seq` 更大的行。未闭合 K 线和快照若 `seq` 更大，或 seqlock 失败，重试最多 3 次，仍失败则返回 `Busy`。
5. `ORDER BY` 若与存储顺序一致则不额外排序。否则只对 `LIMIT` 允许的行排序。
6. `FIRST` / `LAST` 按 `ts` 排序，相同 `ts` 时按 `seq`。
7. `SUM` 用 `i128` 累加，溢出返回错误。`AVG` 返回 `sum` 和 `count` 的有理结果，文本格式为十进制，不使用二进制浮点。

空结果是零行，不是错误。符号不存在是错误。

### 5.3 PostgreSQL 接口

对外目标是 PostgreSQL 线协议的简单查询：启动消息、`Query`（`Q`）、`RowDescription`、`DataRow`、`CommandComplete`、`ErrorResponse`、`ReadyForQuery`。端口默认 `5433`。认证第一版只接受本机 `trust`。

结果里的整数是 `int8`，价格和 `AVG` 是 `numeric` 的文本编码，时间是 `int8` 纳秒。不在第一版实现扩展查询（Parse / Bind / Execute）、COPY、LISTEN/NOTIFY、`information_schema`。

第一版查询路径是进程内 API 加上一个行文本协议，供测试使用。PostgreSQL 编解码放在独立模块 `qdb-pgwire`，避免引擎依赖线协议。功能开关默认关闭。

文本协议仅用于开发和验收：

```text
Q <sql>\n
ROW <column>\t...\n
END <row_count>\n
ERR <code> <message>\n
```

### 5.4 命令行客户端

`qdb-client` 用只读方式打开数据目录。它重放 `wal/wal-1.qwal`，不创建、不删除 `region.bin`，不追加 WAL，因此可以和写入进程同时看已经 `fsync` 的数据。普通查询每条语句重新打开一次。

`.watch` 先打印该表的当前行，再从上次读到的位置跟随新 `fsync` 的记录。`snapshots` 看最新价、买一和卖一，`ticks` 看逐笔，`klines` 还要给周期。这是本机轮询，不是网络推送。

除第 5 节的 `SELECT` 外，客户端接受：

```text
show      = "SHOW" "TABLES"
describe  = "DESCRIBE" table | "DESC" table
```

`SHOW TABLES` 列出四张逻辑表和必要过滤。`DESCRIBE` 列出该表当前查询能返回的列。点命令 `.tables`、`.schema`、`.help`、`.quit` 是上述语句的快捷方式。

没有查询用的网络端口。PostgreSQL 线协议和上面的行文本协议仍不在本版本。

### 5.5 建表

用户表和行情四张表分开。`symbols`、`ticks`、`snapshots`、`klines` 不能被 `CREATE TABLE` 覆盖。

支持的字段类型只有三种，都按小端写入 WAL，不使用浮点：

| 类型 | 含义 |
| --- | --- |
| `INT64` | 有符号 64 位整数。价格、数量、时间和序号用它 |
| `STRING` | UTF-8 文本，最长 1024 字节 |
| `BOOL` | `true` 或 `false` |

一张表至少一列，最多一个 `PRIMARY KEY`。主键重复时插入失败。`INDEX 索引名 (列)` 是二级索引，不要求唯一，只用于等值查找。没有索引的列仍可在 `WHERE` 里过滤，实现会扫描全表。

```text
CREATE TABLE name (
  id INT64 PRIMARY KEY,
  code STRING,
  active BOOL,
  INDEX idx_code (code)
)
INSERT INTO name VALUES (1, 'SSE:600000', true)
SELECT * FROM name WHERE code = 'SSE:600000'
```

`INSERT` 的值顺序与建表列顺序一致。建表和插入各占一个 `seq`，类型号分别是 8 和 9。定义写在 WAL 里，重启后仍在。共享内存帧只发布表名；完整定义以 WAL 为准。

## 6. 订阅

发布模型：写入者把已提交帧放进环形缓冲后，所有跟上的读者都能看见。过滤在订阅网关做。共享内存读者自己过滤。

### 6.1 过滤

订阅键：

```text
exchange_id   可选，0 表示全部
symbol_id     可选，0 表示全部
period_id     可选，0 表示该过滤下的全部消息
msg_mask      Tick、K 线、快照通知的组合
from_seq      希望收到的下一条 seq。0 表示从当前快照开始
```

第一版每个 TCP 连接一个过滤条件。同一连接上的多过滤以后再加。

### 6.2 传输

| 传输 | 阶段 | 行为 |
| --- | --- | --- |
| 共享内存 | 第一版 | 读者自己维护 `cursor`，按 3.4 读帧 |
| TCP 长度前缀帧 | 第一版 | 网关过滤后推送，payload 与共享内存帧相同 |
| WebSocket 二进制 | 后续 | 同一套帧，不另定义业务字段 |
| gRPC server stream | 后续 | 同一套字段的 protobuf 映射 |

TCP 帧：

```text
u32 length | u16 type | u16 flags | body
```

`length` 含 type 和 flags。类型：`Hello`、`Subscribe`、`SnapshotBegin`、`SnapshotRow`、`SnapshotEnd`、`Data`、`Gap`、`Error`。`Data` 的 body 就是 256 字节共享内存帧。

同一连接上，`Data` 按 `(seq, out_idx)` 递增。网关不允许为了合并小包而重排。

### 6.3 断点续传

```mermaid
sequenceDiagram
  participant C as 客户端
  participant G as 网关
  participant W as WAL
  C->>G: Subscribe from_seq=N
  alt N = 0 或落后超过 WAL 保留
    G->>C: SnapshotBegin
    G->>C: 快照与未闭合 K 线
    G->>C: SnapshotEnd snapshot_seq=S
    G->>C: Data seq大于S
  else WAL 中存在 N+1
    G->>C: Data 从 WAL 重放
    G->>C: Data 转入实时环形缓冲
  else 中间有洞
    G->>C: Gap expected=N+1
  end
```

快照消息带 `snapshot_seq`。随后只发送 `seq > snapshot_seq` 的增量。客户端把本连接最后连续 `seq` 作为下次的 `from_seq`。

重放出来的帧带 `FROM_WAL`。实时帧不带。切换点不重复、不跳号。若实时环形缓冲在重放期间把续传点覆盖掉，网关重新从 WAL 定位，不对客户端发出有洞的序列。

客户端若收到 `Gap`，应停止使用这条流上的后续帧做连续计算，直到新的 `SnapshotEnd`。

### 6.4 网关内部

网关一个线程按 `write_seq` 读环形缓冲，匹配订阅者，写入每个订阅者的有界队列。队列默认 4096 帧。队列满时丢弃该订阅者的实时资格并发送 `Gap`，不减慢读取线程。

WAL 重放使用单独的发送循环，补到 `write_seq` 之后再切入实时匹配。一个订阅者在重放完成前不接收实时帧，避免新旧交叉。

## 7. K 线聚合

聚合器跑在写入线程上，输入是已经写入 WAL 的事件。同一 `seq` 的全部周期更新完成后，才增加 `applied_seq` 并发布帧。

### 7.1 状态

每个 `(symbol_id, period_id)` 有：

- 最多一根未闭合 K 线
- `last_applied_seq`
- 已闭合段

另有每个 `exchange_id` 的事件时间水位 `watermark_ns`，只由 `ClockTick` 和该交易所 Tick 推进。一只股票的时间不关闭另一家交易所的 K 线。

### 7.2 算法

对一条 `TickTrade`：

```text
if tick.seq <= last_applied_seq:
    return                    # 重复重放
if tick.seq != last_applied_seq + 1:
    pause until GapAck        # 缺号，不闭合、不修改未闭合 K 线

for period in configured_periods:
    bucket = align(period, tick.event_ts, exchange)
    bar = open_bar(symbol, period)
    if bar exists and tick.event_ts < bar.bucket_start:
        record late_tick      # 已过去的桶，默认丢弃该 Tick 对这个周期的影响
        continue
    if bar exists and bucket != bar.bucket_start:
        seal(bar)
        bar = None
    if bar is None:
        bar = new_bar(tick, bucket)
    else:
        bar.high = max(bar.high, tick.price)
        bar.low = min(bar.low, tick.price)
        bar.close = tick.price
        bar.volume += tick.volume
        bar.amount += tick.amount
        bar.tick_count += 1
        bar.seq = tick.seq
publish updates
```

`seal` 把未闭合槽标成闭合，追加到已闭合段，然后清空该槽的“当前桶”身份。共享内存里可以暂时留着最后一根闭合结果供读取，但 `state = closed` 后聚合器不再改它的 OHLC。下一根未闭合 K 线使用新的 seqlock 写进入槽。

`TickQuote` 不推动 OHLC 成交量。它只更新快照。若报价里的最新价需要推动 K 线，由采集端额外发送 `TickTrade` 或带成交标志的事件。第一版不从报价推断成交。

### 7.3 闭合

K 线在两种日志事件下闭合：

1. 同一周期出现下一个桶的 Tick。
2. 该交易所的 `ClockTick.watermark_ns` 大于等于桶的结束时间。

`ClockTick` 由写入者写入 WAL，默认每 200ms 至多一条，且水位取该交易所已见事件时间减去 `grace`。`grace` 默认 0。跟随者只重放 `ClockTick`，不用自己的墙上时钟闭合 K 线。这样重放和以后的副本结果一致。

迟到 Tick：事件时间落入已经闭合的桶时，默认丢弃其对 K 线的影响，计数 `late_tick_total`，原始 Tick 仍保留在 Tick 表。改为修订闭合 K 线必须由显式修正命令产生新 `revision`，第一版可以没有这条命令，但存储要能放下 `revision`。

### 7.4 多周期与幂等

一条成交在同一次应用里更新全部已配置周期：`1s`、`1m`、`5m`、`15m`、`1h`、`1d`。任一周期的更新失败则整个 `seq` 不算已应用，进程应停止写入并保留 WAL，避免周期之间永久分叉。

幂等来自 `last_applied_seq`：

- 相同 `seq` 再来一次：状态不变。
- 从快照的 `applied_seq` 之后重放 WAL：得到同一未闭合 K 线和同一已闭合序列。
- 转储文件 `QDBDUMP1` 是某一库已提交 WAL 记录的副本。恢复写回空库的 WAL，打开后得到同一 `seq` 和同一组 K 线。回放则按记录顺序重新写入，分配新的 `seq`，用来把这段行情再放一遍。
- 聚合只依赖 WAL 里的事件和快照里的聚合状态，不依赖墙上时钟，不依赖 `HashMap` 的迭代顺序。

缺号时暂停。运维确认无法补数后，写入一条 `GapAck { missing_from, missing_to }`。它占用这些 `seq`，并把受影响的未闭合 K 线标成 `incomplete`。副本重放同一条 `GapAck`，不会各自决定是否跳过。

### 7.5 示例

三笔 `SZSE:000001` 成交，`1s` 与 `1m` 同时聚合：

| seq | 事件时间 | 价格 | 量 |
| --- | --- | --- | --- |
| 1 | 09:30:00.100 | 10 | 100 |
| 2 | 09:30:00.200 | 12 | 50 |
| 3 | 09:30:01.000 | 11 | 10 |

处理完 `seq=2` 时，`1s` 和 `1m` 都是未闭合：`O=10 H=12 L=10 C=12 V=150`。

`seq=3` 落入下一秒，先闭合 `1s` 桶 `[09:30:00, 09:30:01)` 为上述 OHLC，再打开新的 `1s` 桶 `O=H=L=C=11 V=10`。`1m` 桶仍未闭合，变成 `O=10 H=12 L=10 C=11 V=160`。

从空状态重放 `seq` 1 到 3，结果相同。只重放 `seq=2` 时，因 `last_applied_seq` 已是 3，状态不变。若只收到 1 和 3，聚合停在 `seq=1`，不闭合那根 `1s` K 线。

## 8. 并发与一致性

| 规则 | 实现 |
| --- | --- |
| 单写多读 | 文件锁外加设计约束。读路径没有锁等待写入者 |
| 读者不阻塞写入 | 环形缓冲覆盖最慢读者，读者得到 `Lapped` |
| 全序 | 分区内 `seq` 严格加一。单机就是一个分区 |
| 快照一致读 | seqlock 的前后 `version` 必须相同 |
| 跨进程可见 | 提交字用 Release 存储，读者用 Acquire 加载。依赖同机缓存一致性。不把共享内存映射到其他机器 |
| 查询稳定边界 | SQL 使用读取开始时的 `applied_seq` |
| 乱序与回退 | `seq` 回退或跳跃：帧级报 `SeqRegression` / `SeqGap`；事件时间回退只打标志，不打乱 `seq` |

内存顺序只用 Acquire / Release。不在热路径使用 `SeqCst`。

写入线程之外 nobody 修改 K 线槽、快照槽和 WAL。SQL 线程、订阅线程和外部进程只读。

引擎私有的已闭合 K 线段通过「先写元素，再 Release 发布长度」暴露给 SQL。SQL Acquire 读取长度后只扫描已发布前缀。不使用 `RwLock` 保护热路径。

符号映射表替换使用 `Arc` 整体交换。查找先克隆指针再查。这只发生在新符号登记时。

## 9. 持久化与恢复

共享内存可以整段丢掉。权威顺序是：

```text
data/meta.bin          最新 epoch、下一次可分配 seq 的持久提示
data/wal/wal-*.qwal    分段 WAL
data/snap/<seq>/       该 seq 之前的状态快照
```

`meta.bin` 与 WAL 不一致时，以 CRC 完整的 WAL 结尾为准，并回写 `meta.bin`。不允许把 `seq` 拨到 WAL 最大序号之前。

### 9.1 WAL

记录：

```text
crc32 | length | seq | type | payload
```

`type` 与共享内存消息类型相同，外加 `SnapshotMark`（只记录快照文件名和 `seq`，便于恢复时定位）。

分段默认 64MB。每段文件名包含起始 `seq`。段尾不满也不复用已关闭的文件。

组提交默认 1ms 或 256 条。一次 `fsync` 后，这批 `seq` 才对共享内存读者可见。导入和人工修订使用每批 `fsync` 再发布。

WAL 保留默认 7 天或 64 段，取更长者。被快照引用的段不能删除。删除前要保证：该 `seq` 之前的状态已经在一份完整快照里，且不需要这段 WAL 才能从那份快照恢复。

### 9.2 快照

触发：每 60 秒或每 100000 个 `seq`，以及正常关闭时。

快照目录：

```text
data/snap/<applied_seq>/
  manifest.bin
  symbols.bin
  sealed_klines.bin
  open_bars.bin
  snapshots.bin
  tick_segments.bin
```

`manifest.bin` 含 `applied_seq`、对应 WAL 段、各文件 CRC、创建时间。写入使用临时目录，全部 CRC 写完后原子改名。恢复时忽略没有 manifest 或 CRC 失败的目录。

快照内容是那个 `applied_seq` 的内存表，不含环形缓冲。环形缓冲在恢复后重新生成。

### 9.3 启动

```text
读取 meta
选择 CRC 有效且 applied_seq 最大的快照
按快照重建符号、已闭合 K 线、未闭合 K 线、快照表、Tick 段、last_applied_seq
重放 WAL 中 seq > applied_seq 的记录
重放使用与在线相同的聚合器
创建新的共享内存区域，epoch + 1
把快照和未闭合 K 线写入共享内存
write_seq = applied_seq，但环形缓冲里不必塞入历史帧
打开写入锁，接受新行情，下一个 seq = applied_seq + 1
```

环形缓冲不回放全部历史。已经在线的读者看到 `epoch` 变化后重新订阅：先快照，再按 `from_seq` 补 WAL 里还在的增量。

恢复完成后打一条日志：快照 `seq`、重放条数、最终 `seq`、是否遇到 CRC 错误。CRC 错误时停在最后一条好记录之前，不跳过坏记录继续写。需要人工处理。

### 9.4 历史导入

导入不进第一版实现。格式先定下来：

- 输入是已闭合 K 线文件，字段与 `KlinePod` 的业务列相同。
- 每根 K 线写成 WAL 的 `KlineClose`，`origin = import`，由写入者分配新的 `seq`。
- 导入桶必须早于该符号当前未闭合桶。与已有桶冲突时拒绝整批，不覆盖。
- 导入批次在自己的组提交里 `fsync` 后才发布。
- 查询按 `(symbol, period, bucket_start, revision)` 合并导入段和实时段。

导入不是 Tick 重放。没有原始 Tick 时不反推 Tick 表。

## 10. 运维与可观测

第一版提供本机 HTTP：

- `GET /health` 返回 200 或 503
- `GET /metrics` 返回 Prometheus 文本

最小指标：

| 指标 | 含义 |
| --- | --- |
| `qdb_durable_seq` | 已落盘序号 |
| `qdb_applied_seq` | 内存已应用序号 |
| `qdb_write_seq` | 已发布序号 |
| `qdb_group_commit_seconds` | 组提交耗时 |
| `qdb_ingest_lag_seconds` | `recv_ts - event_ts` 的摘要，仅作喂价延迟 |
| `qdb_reader_lag_frames` | 每个已注册读者相对 `write_seq` 的距离 |
| `qdb_ring_usage_ratio` | 第 3.6 节的使用率 |
| `qdb_subscriber_count` | 活跃订阅 |
| `qdb_gap_total` | 套圈、缺号、订阅队列满 |
| `qdb_late_tick_total` | 迟到 Tick |
| `qdb_symbol_silence_seconds` | 交易时段内某符号距上一笔 Tick 的时间。第一版可以先做全局最后事件时间 |

`/health` 在以下情况返回 503：写入者心跳超过 2 秒、存在未确认的 ingest gap、WAL CRC 失败后停住。只读进程的慢只增加指标，不把整个库标成不可用。

空窗：交易时段内 `now - last_event_ts` 超过阈值（默认 5 秒）时增加 `qdb_data_gap_open`。心跳仍在但没有 Tick，表示喂价停了；心跳也停了，表示写入者停了。两者分开告警。A 股时段表第一版可以只做工作日 09:30–11:30 和 13:00–15:00 的静默检测，不参与 K 线闭合。

权限，第一版只做操作系统权限：

- 共享内存文件和 WAL 目录 `0640` / `0750`
- 只有写入进程的用户能打开写入锁
- SQL 和订阅在本机监听，不区分账号

后续再加两种凭据：`reader` 只能查询，`subscriber` 可以查询和订阅。仍然没有远程写入共享内存这一角色。共享内存的写映射只发生在持有写入锁的进程里。

审计，第一版用 `tracing` 结构化日志记录：启动恢复结果、gap、`GapAck`、订阅建立和断开、导入拒绝。查询日志默认关闭，避免逐笔 SQL 拖慢路径。完整审计文件后续再加。

## 11. 分布式

第一版不实现本节，但 ABI 和 WAL 从第一天带上 `cluster_id`、`node_id`、`partition_id`、`leader_epoch`。分区、复制、切换和单机并行的展开见 [分布式、高可用与高并发](2026-10-19-distributed-ha.md)。若细节与本节冲突，以本节的单写、epoch、WAL 复制和 D0/D1/D2 为准。

### 11.1 分区

一只股票在任一时刻只有一个写入者。分区按 `exchange_id` 起步就够用：上交所一个分区，深交所一个分区。符号数量上来之后再按 `symbol_id` 范围切。符号 ID 由元数据分区分配，通过 WAL 复制，全集群不复用。

节点可以领导一部分分区、跟随另一些分区。查询打到持有该分区数据的节点。客户端根据 `EXCHANGE:CODE` 找分区，不在 SQL 里写节点地址。

### 11.2 复制

共享内存仍然只在节点内部。跨机器复制 WAL 记录。

```mermaid
flowchart TB
  raft[控制面: 领导权与 epoch]
  leader[分区 Leader]
  follower[分区 Follower]
  raft --> leader
  raft --> follower
  leader --> walL[本地 WAL]
  walL --> shmL[本地共享内存]
  walL --> walF[复制 WAL]
  walF --> shmF[Follower 本地共享内存]
```

跟随者用和订阅相同的 `from_seq` 拉 WAL，用同一份聚合代码应用。确定性依赖第 7 节：闭合由 `ClockTick` 驱动，跳号由 `GapAck` 驱动。

领导权切换：

1. 新 Leader 必须持有更大的 `leader_epoch`。
2. 它从多数派或外部确认的最大持久 `seq` 之后继续分配。单机文件 `meta.bin` 在分布式里变成复制状态。
3. 旧 Leader 的写入因为 epoch 过期被拒绝。WAL 段头写入 `leader_epoch`，跟随者拒绝更小的 epoch。
4. 已分配但未复制的尾部可以丢失。客户端和订阅者按 `seq` 续传；若新尾部与所见 `seq` 不连续，走快照。

提交级别：

| 级别 | 行为 | 阶段 |
| --- | --- | --- |
| D0 | 本机 `fsync` 后发布 | 第一版 |
| D1 | 本机 `fsync` 后发布，异步复制到跟随者。允许有复制延迟 | 分布式第一实现 |
| D2 | 多数派拿到记录后才对本机订阅者发布 | 需要更低 RPO 时 |

行情默认将来使用 D1。D2 会把组提交延迟变成跨机延迟，不作为默认。

查询带本地 `applied_seq`。调用方可以要求 `min_seq`。跟随者落后时返回 `Stale`，由客户端改打 Leader 或等待。不提供跨分区事务。

共识只用于领导权和 epoch，不用于逐笔成交。逐笔走 WAL 流。控制面可以实现为内置 Raft，也可以先用静态主备配置。静态主备只能用于演练，不能当作脑裂保护。

### 11.3 不会做的分布式形态

- 多主写入同一 `symbol_id`
- 用共享内存或 RDMA 冒充第一版已经完成的跨机总线
- 把 SQL 升级成分布式连接器之后再补 `seq`

## 12. 模块

依赖只能自下而上：

```text
qdb-abi     repr(C) 结构、常量、消息类型。无 unsafe，无 serde
qdb-shm     映射、环形缓冲、seqlock、读者槽。unsafe 限制在本 crate
qdb-wal     分段、组提交、CRC、重放迭代器
qdb-agg     多周期聚合，不含 IO
qdb-store   符号、Tick 段、已闭合 K 线、恢复流程
qdb-sql     解析和执行
qdb-sub     过滤、续传、TCP
qdb-server  写入线程、/health、/metrics、进程入口
qdb-pgwire  后续。简单查询线协议
```

热路径上的帧、Tick、K 线、快照都不经过 serde。快照 manifest 可以用固定二进制，避免为了一个文件引入序列化框架。

异步运行时只用于 TCP、HTTP 和以后的 gRPC。聚合、WAL 和共享内存发布留在专用写入线程。

建议的包和类型名：

| 类型 | 位置 |
| --- | --- |
| `FrameHeader`、`TickTrade`、`KlinePod`、`SnapshotPod` | `qdb_abi` |
| `RingWriter`、`RingReader` | `qdb_shm` |
| `WalAppender`、`WalReader` | `qdb_wal` |
| `KlineAggregator` | `qdb_agg` |
| `MemoryStore` | `qdb_store` |
| `SqlEngine` | `qdb_sql` |
| `Subscription` | `qdb_sub` |

## 13. 里程碑

### M1 共享内存

- 两进程：一个写，一个读
- 固定 256 字节帧，`seq` 连续
- 故意放慢读者，必须返回 `Lapped` 或 `SeqGap`，不能把跳号读成连续 Tick
- 快照槽写入中途，读者看不到奇数版本下的半新半旧字段

### M2 表与聚合

- `SZSE:000001` 的 Tick 生成 `1s` 和 `1m`
- 未闭合 K 线随成交变化
- 闭合后原 OHLC 不再变化
- 同一段 Tick 重放两次，闭合结果相同
- 缺 `seq` 时聚合停住

### M3 WAL 与恢复

- 杀死进程后，`applied_seq`、闭合 K 线和未闭合 K 线与杀死前一致
- 共享内存文件删掉再启动，数据仍在
- 坏 CRC 停在坏记录之前

### M4 SQL

- 第 5.1 节的两条示例可执行
- `MAX` / `MIN` / `SUM` / `AVG` / `FIRST` / `LAST` 作用在一个符号的一个周期上
- 歧义的 `000001` 被拒绝

### M5 订阅

- 按 `SZSE:000001` 和 `1m` 只收到对应帧
- 同一连接 `seq` 不回退
- 断开后用 `from_seq` 补齐
- 落后过多时先 `SnapshotEnd`，再只有更大的 `seq`

M1 到 M5 构成最低可用版本。之后才是 PostgreSQL 简单查询、WebSocket、gRPC、历史导入、账号级别权限、审计文件和 D1 复制。

## 14. 决策

| 议题 | 决定 | 原因 |
| --- | --- | --- |
| 价格表示 | `i64` 加 scale | 聚合和比较可重复 |
| 帧布局 | 256 字节固定帧，5 档盘口 | 零拷贝和槽位计算简单，覆盖第一版行情 |
| 慢读者 | 覆盖并显式 gap | 写入不能被策略进程拖住 |
| 可见性 | 组提交 `fsync` 之后再发布 | 满足重启不丢已发布数据。代价是约 1ms 的发布等待 |
| K 线权威 | 由 WAL 事件派生，闭合记录进快照 | 恢复不必依赖环形缓冲 |
| 已闭合 K 线 | 新 `revision` 追加 | 查询结果不随覆盖漂移 |
| 闭合时钟 | Leader 写入 `ClockTick` | 重放和副本不看本地时钟 |
| 日线 | 交易日 | 与 A 股交易日一致 |
| 复权 | 不入库 | 原始行情和复权因子生命周期不同 |
| 列存 | 第一版 Tick 用按符号行段 | 先保证正确。扫描接口以后可以换列段 |
| 分布式数据面 | WAL 复制 | 逐笔不走共识协议 |
| 分布式控制面 | epoch 防脑裂 | 共享内存解决不了跨机写入权 |

组提交间隔、Tick 保留 2 小时、WAL 保留 7 天、环形缓冲 65536 帧都是默认配置，不是布局的一部分。改配置不需要换 `abi_version`。改帧头、槽位大小或 seqlock 规则需要 `abi_version + 1`。

## 15. 需求对照

| 能力 | 设计位置 | 第一版 |
| --- | --- | --- |
| 单写多读环形缓冲 | 第 3、8 节 | 是 |
| 固定布局 | 第 3.2、4 节 | 是 |
| `seq` 与 `version` | 第 3.3、3.5 节 | 是 |
| 零拷贝读取 | 第 3.4、3.5 节 | 是，默认 API 拷贝出 256 字节定长帧 |
| 背压与丢包检测 | 第 3.6、6.4 节 | 是 |
| Tick / K 线 / 快照 / 符号 | 第 4 节 | 是 |
| 只追加 | 第 4.5 节 | 是 |
| SQL 子集与时间序列查询 | 第 5 节 | 是 |
| `MAX/MIN/SUM/AVG/FIRST/LAST` | 第 5.2 节 | 是 |
| PostgreSQL 线协议 | 第 5.3 节 | 否，只留模块边界 |
| 条件订阅、有序、断点、快照加增量 | 第 6 节 | 是，TCP 与共享内存 |
| WebSocket 与 gRPC | 第 6.2 节 | 否 |
| Tick 转多周期 K 线、未闭合更新、闭合不覆盖、幂等 | 第 7 节 | 是 |
| 原子序号、版本校验、内存屏障、乱序检测 | 第 8 节 | 是 |
| WAL、快照、启动恢复 | 第 9 节 | 是 |
| 历史导入 | 第 9.4 节 | 否 |
| 健康检查、丢包、水位 | 第 10 节 | 是 |
| 权限与审计 | 第 10 节 | 仅文件权限和恢复/gap 日志 |
| 分布式、高可用、高并发 | 第 11 节与 [展开设计](2026-10-19-distributed-ha.md) | 只预留字段，不实现 |
| 券商全品种、逐笔委托、订单簿、授权与历史库 | [企业级缺口](2026-10-19-enterprise-broker-md.md) | 否 |
