---
name: qdb-dev
description: >-
  Implement or change QDB, the Rust in-memory market DB (shared-memory ring, WAL,
  ticks, klines, snapshots, SQL, HTTP /v1, seq resume). Use when editing crates,
  docs/design.md, docs/manual/, shm/WAL/HTTP/subscribe behavior, or when the user
  mentions QDB、共享内存、环形缓冲、WAL、K 线、seq、行情库、/v1/snapshots.
---

> **中文说明**：在 QDB 上实现或修改时使用。先对齐 `docs/design.md` / `docs/manual/` 与 `AGENTS.md`，再改指定 crate；第一版只做到 M1–M5。一轮功能的阶段顺序见 `qdb-pipeline`；收尾见 `qdb-dod`。

# QDB 开发

内核是通用结构化内存库：表、列、主键、追加、最新值、`seq`、订阅。设计第 4、7 节是已经写在代码里的业务表，不是模型。新改动不要再往引擎里加业务名称。

## 开始前（completion：能指出设计/手册里的节和要改的 crate）

1. 读根目录 [`AGENTS.md`](../../../AGENTS.md) 的硬性约束与「生产级迭代」。术语冲突时以 [`CONTEXT.md`](../../../CONTEXT.md) 为准。
2. 按改动加载前备技能：新类型或 crate 用 `rust-program-design`；线程、原子或共享内存用 `rust-concurrency-design`；成交、K 线、序号或订阅用 `market-data-systems`；A 股代码、时段、竞价或复权用 `a-share-market`；币安或 24 小时连续交易用 `binance-market`。
3. 只打开 [`docs/design.md`](../../../docs/design.md) 里这一改动对应的节，加上第 13 节里该里程碑的验收。使用者能看见的行为另开 [`docs/manual/`](../../../docs/manual/index.md) 对应章（`server` / `limits` / `data-model` 等）。
4. 确认落点。没有点名的能力停在设计里，不实现。券商级缺口见 [`docs/2026-10-19-enterprise-broker-md.md`](../../../docs/2026-10-19-enterprise-broker-md.md)，分布式与高可用见 [`docs/2026-10-19-distributed-ha.md`](../../../docs/2026-10-19-distributed-ha.md)。任务没点名就不做。

| 改动意图 | 落点 |
|---|---|
| 帧、行情快照、K 线槽、符号记录的字节布局 | `qdb-abi` |
| 环形缓冲、seqlock、读者槽、映射 | `qdb-shm` |
| 分段日志、组提交、重放 | `qdb-wal` |
| 多周期聚合、`ClockTick`、缺号暂停、late-tick | `qdb-agg` / `qdb-store` |
| 符号表、Tick 段、已闭合 K 线、启动恢复 | `qdb-store` |
| SQL 子集 | `qdb-sql` |
| 过滤、`from_seq`、快照加增量 | `qdb-sub` |
| HTTP `/v1/*`、`/health`、`/metrics`、feed、进程入口 | `qdb-server` |
| 只读命令行查询、表结构 | `qdb-server` 的 `qdb-client` |
| 行为或布局变化 | 先改 `docs/design.md`；使用者可见行为改 `docs/manual/` |

依赖只能自下而上：`abi ← shm / wal / agg ← store ← sql / sub ← server`。热路径上的帧和行情结构不用 serde。

## 实现规则

- **写入顺序**：分配连续 `seq` → WAL `fsync` → 更新内存表 → 发布 `applied_seq` → 写环形缓冲 → 发布 `write_seq`。
- **派生数据**：K 线由 WAL 事件算出。同一 `seq` 的全部周期更新完，才对读者可见。
- **闭合**：只由下一桶的 Tick 或日志里的 `ClockTick` 触发。跟随重放不看本机墙上时钟。
- **late-tick**：早于当前未闭合桶起点的成交进 `ticks`、不进 K 线，计 `late_tick_total`（见手册 `data-model.md`）。禁止改成回填已闭合桶。
- **缺号**：聚合停在洞之前。跳过必须是一条 `GapAck` 日志，而不是读者各自决定。
- **慢读者**：写入者不等待。读者得到 gap 之后停止消费，能续传则走 WAL，否则先取行情快照。
- **SQL**：只读。语法边界以设计第 5 节与手册 `sql.md` 为准。`000001` 在目录里不唯一时必须带交易所。
- **HTTP**：路径与 JSON 形状以手册 `server.md` 为准。`GET /v1/klines` 的 `limit` 上限 5000。破坏 EquityLab 等下游契约时写版本说明。
- **`QDB_FEED`**：默认开；外部采集（EquityLab Compose）用 `QDB_FEED=0`。无写入时 `/health` 可能 503——冒烟须先写入或说明环境。
- **异步**：网络和 HTTP 可以用 Tokio。编号、聚合、落盘、发布留在一个同步写入线程。
- **限制**：容量与语义边界跟 [`docs/manual/limits.md`](../../../docs/manual/limits.md)；改常量必改手册。
- Rust 写法另见 skill `rust-best-practices`。本文件与 `AGENTS.md` 冲突时，以 `AGENTS.md` 为准。

## 验证（completion：该里程碑的验收项有对应测试，并且通过；运行态已重建）

- 布局：`size_of` 与字段偏移断言。
- 环形缓冲：一个写、一个读；故意放慢读者必须观察到 gap。
- 聚合：设计第 7.5 节的三笔成交；同一段重放两次结果相同；缺号时不闭合；late-tick 用例。
- 恢复：进程状态从状态快照加 WAL 重建；删掉共享内存文件后数据仍在；坏 CRC 停在坏记录之前。
- 多库：非法库名和重名失败；两个库的成交互不可见；根目录已有 WAL 时库名是 `default`。
- 实例：第二个 `qdb` 进程在打开库之前失败。
- 转储：空库才能 `restore`；坏魔数或坏 CRC 失败；半截文件不算损坏；恢复后的成交和 K 线与转储前一致；回放不改写源库。
- HTTP（按改动）：重建二进制后 curl `/health`、`/v1/snapshots`、`/v1/klines`、`/v1/docs`；**仅 health 不够**。
- SQL 与订阅：用设计/手册示例和 `SZSE:000001` / `1m` 订阅验收。
- 复制、槽位、可见性、切换时限和组提交性能：按 skill `qdb-test` 的表逐条有测试名。断言用设计里的倍数和时限。

```bash
cargo test --workspace -- --test-threads=1
# 相关 crate 可先：cargo test -p <crate> -- --test-threads=1
cargo build -p qdb-server   # 或重建镜像后再 curl
```

## 注释

公开接口用文档注释，块内说明用普通注释。说明用中文，标识符保持英文。详细程度对齐 Javadoc：读注释就能调用，不必先翻实现。

`//!` 写在 crate 或模块开头，说明这个模块负责什么、不负责什么。

`///` 写在每个公开类型、公开函数和公开字段上。函数按这个顺序写，没有的节就省略：

1. 第一句是摘要，说明它做什么。
2. 空一行后写调用方需要知道的行为：是否 `fsync`、是否映射或删除共享内存、是否只读、序号怎么分配。
3. `# Arguments`：每个参数一条。写合法取值和单位，例如纳秒、定点数。不要只重复参数名。
4. `# Returns`：成功时拿到什么。`()` 可省略。
5. `# Errors`：每种会返回的错误以及触发条件，对应 `@throws`。
6. `# Panics`：只有真的会 panic 时才写。
7. `# Safety`：`unsafe` 函数写调用方必须保证的前提。
8. `# Examples`：只给主要入口。示例要能编译，并放进文档测试太重时就不要包成会运行的代码块。

```rust
/// 写入一笔成交，并更新各周期未闭合 K 线。
///
/// 证券不存在时会先登记。默认每次调用都 `fsync`；组提交打开后要再调用 [`Engine::flush`]。
///
/// # Arguments
///
/// * `exchange_code` - 调用方给出的交易所标签，不能为空。
/// * `code` - 证券代码。
/// * `event_ts_ns` - 事件时间，UTC 纳秒。
/// * `price` - 成交价，定点 `i64`。
/// * `volume` - 成交量。
///
/// # Returns
///
/// 分配到的 `seq`。序号从 1 起。
///
/// # Errors
///
/// * [`StoreError::EmptyExchange`] - 交易所标签为空。
/// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
/// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
/// * [`StoreError::Io`] - 落盘或发布共享内存失败。
```

`//` 只解释代码本身看不出来的原因：不变量、平台差异、故意不用另一种做法。`unsafe` 块前写 `// SAFETY:`，说明这段代码依赖的前提。

注释和 [`docs/manual/`](../../../docs/manual/index.md) 或 [`docs/design.md`](../../../docs/design.md) 冲突时，先改设计和手册，再改注释。

## 收尾

- 使用者能看见的行为变了，就地更新 [`docs/manual/`](../../../docs/manual/index.md) 里对应章节，详细程度按该页「后续增补」。未实现的能力不写进手册。
- 新增或修改的公开函数带上中文 `///`，包含 `# Arguments`、`# Returns`、`# Errors` 中适用的节。`unsafe` 带上 `// SAFETY:`。
- 新术语写进 `CONTEXT.md`（skill `domain-modeling`）。难反转的新决策才写 ADR；已在 `docs/design.md` 第 14 节的决定不另抄一份。
- feature/epic：走 skill **qdb-dod**（或 pipeline 收尾）。简报：`已做 / 未做 / 已知风险 / 复验命令`。
- 用户要求提交时走 skill `commit`。
- 大设计分歧先用 `grill-with-docs`，再改设计正文。
