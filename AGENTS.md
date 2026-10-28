# Agent rules — QDB

本文件是仓库级 **agent 行为规范的唯一权威来源**。Cursor、Codex、Claude Code 及其他编码代理均须遵守。
细则技能见 [`.agents/skills/`](.agents/skills/)；本文件只放跨工具、每次会话都应生效的硬性约定。

写给 agent 看的文档写法见 skill **writing-for-agents**。领域术语见根目录 [`CONTEXT.md`](CONTEXT.md)。

## 项目是什么

QDB 是结构化内存数据库，用 Rust 实现。单机上只有一个写入者：行先组提交进 WAL，再更新内存表，最后发布到本机共享内存。其他进程只读。库不理解业务；行情只是调用方装进表里的一种数据。

```text
写入方 → 写入线程 → WAL → 内存表 → 共享内存 → SQL / 订阅 / HTTP / 外部只读进程
                              ↘ 状态快照 → 启动恢复
```

目录约定：`docs/design.md` 是设计正文，`docs/manual/` 是使用者手册，`crates/*` 是实现（`qdb-abi` … `qdb-server`），`.agents/skills/` 是技能，`.agents/team/` 是角色卡，`.agents/artifacts/` 是阶段交接物。

下游（如 EquityLab）经 HTTP `/v1/*` 与可选内置 feed 使用本库；**契约以本仓手册为准**，破坏性变更须版本说明。

## 开发流水线

功能默认按阶段推进，入口 skill 是 **qdb-pipeline**：

```text
规划 → 架构 → 开发（Engine 与 Query）→ 测试 → 评审 → 发布 → 运维
```

| Agent 角色 | 阶段 | 主要产出 |
|---|---|---|
| Planner | 需求澄清、任务拆解 | PRD、验收标准、任务清单 |
| Architect | 相对设计的增量 | 架构增量、ABI/API、crate 落点 |
| Engine | ABI、共享内存、WAL、聚合、存储 | crate 与测试 |
| Query | SQL、订阅、HTTP、进程入口 | crate 与测试 |
| Test | 自动化测试 | 测试报告、冒烟证据 |
| Reviewer | 质量评审 | Standards / Spec、阻断项 |
| Ops | 发布与运维 | 发布说明、恢复演练、回滚 |

- 流水线说明：[docs/2026-10-19-agent-pipeline.md](docs/2026-10-19-agent-pipeline.md)
- DoD / 契约门禁：[docs/2026-10-26-agent-dod.md](docs/2026-10-26-agent-dod.md)
- 角色卡：[`.agents/team/`](.agents/team/README.md)
- 交接物：`.agents/artifacts/`（模板在 `.agents/templates/`）
- 小修复可跳过规划和架构，不能跳过冒烟和简短评审；**门禁未过不得进入下一阶段**
- **变更分级、DoD、契约面**见下文「生产级迭代」；清单模板 [`.agents/templates/dod.md`](.agents/templates/dod.md)；编排 skill **qdb-pipeline** / **qdb-dod**

## 文档命名

`docs/` 下新建说明文档用 `YYYY-MM-DD-<标题>.md`（创建日）。不要用修改日改写已有前缀；重大改版可新建带新创建日的文件并更新引用。

例外：

- [`docs/design.md`](docs/design.md) 保持此文件名（设计正文）
- [`docs/manual/`](docs/manual/index.md) 章节就地更新，不另建日期副本

## 必读指针（按需加载）

| 何时 | 打开 |
|---|---|
| 改任何实现、ABI、恢复或订阅行为 | [`docs/design.md`](docs/design.md) 里对应的节，不要整篇重读 |
| 改查询、写入、订阅、恢复、HTTP 或服务进程等使用者能看见的行为 | [`docs/manual/`](docs/manual/index.md) 里对应的章，同一变更里改完 |
| HTTP 路径、feed、health、klines 限制 | [`docs/manual/server.md`](docs/manual/server.md)、[`limits.md`](docs/manual/limits.md) |
| late-tick / 数据语义 | [`docs/manual/data-model.md`](docs/manual/data-model.md) |
| 术语不一致 | [`CONTEXT.md`](CONTEXT.md) |
| 在本仓落代码 | skill **qdb-dev** |
| 按阶段做功能 | skill **qdb-pipeline** |
| 完成判据 / 可否发布 | skill **qdb-dod** |
| 写或改 Rust | skill **rust-best-practices**；异步 IO 再加 **rust-async-patterns** |
| 新增 crate、公开类型、错误或所有权 | skill **rust-program-design** |
| 线程、原子量、共享内存、`unsafe` | skill **rust-concurrency-design** |
| 成交/报价、K 线、序号、缺口、快照加增量 | skill **market-data-systems** |
| A 股代码、时段、竞价、涨跌停、复权 | skill **a-share-market** |
| 币安、24 小时连续交易、UTC 日线 | skill **binance-market** |
| 券商全市场等尚未做的能力 | [`docs/2026-10-19-enterprise-broker-md.md`](docs/2026-10-19-enterprise-broker-md.md) |
| 分布式、高可用 | [`docs/2026-10-19-distributed-ha.md`](docs/2026-10-19-distributed-ha.md) |
| 共享内存、WAL、`unsafe` 的差分审查 | skill **differential-review** |

设计第 13 节的 M1–M5 是最低可用版本。验收标准以那一节为准。

## 硬性约束

1. **设计先行**。实现与 [`docs/design.md`](docs/design.md) 冲突时，先改设计再改代码。`abi_version`、帧布局、seqlock、组提交顺序变了，必须同时改设计。
2. **一台机器一个 qdb 实例**。共享内存只允许一个 qdb 进程持有写入映射。第二个进程必须在打开库之前失败并退出。一个实例里可以有多个库；每个库的序号和 WAL 仍然分开。读者只读映射。
3. **共享内存是定长字节**。跨进程类型放在 `qdb-abi`，`repr(C)`、小端、无指针。`Vec`、`String`、`HashMap` 只留在引擎私有内存。
4. **`unsafe` 只出现在 `qdb-shm` 和 `qdb-wal`**。
5. **价格和金额用 `i64` 定点**。存储、聚合、比较不使用 `f64`。库内保存交易所原始价，不存复权价。
6. **先落盘再发布**。读者能看见的 `seq` 已经在 WAL 里 `fsync`。默认组提交 1ms 或 256 条。
7. **已闭合 K 线只追加 `revision`**。未闭合 K 线只更新自己的槽。Tick 段只追加。
8. **gap 必须显式**。`seq` 缺号或读者被环形缓冲套圈时，停止把后续帧当成连续数据。
9. **共享内存不出节点**。跨机器只复制 WAL。第一版不实现复制（设计预留除外，任务未点名不做）。
10. **写入线程保持同步**。Tokio 只用于 TCP、HTTP 和以后的 gRPC。聚合、WAL 和共享内存发布留在专用写入线程。
11. **第一版止于 M1–M5**。任务没有点名时，不做 PostgreSQL 线协议、gRPC、历史导入、账号权限、审计文件、Raft。已实现的 HTTP / WebSocket 以手册为准，不得口头扩范围。
12. **文档命名**。见上文「文档命名」。
13. **标识符用英文，说明用中文**。给人和 agent 的文档、提交说明用中文。
14. **库不做业务裁决**。时钟只认 UTC。交易所代码只是调用方给出的标识，库不解释其含义。开市时间、时区、夏令时、复权、涨跌停由业务系统在写入前处理。
15. **手册随行为更新**。使用者能看见的行为有变化时，同一变更里按 [`docs/manual/index.md`](docs/manual/index.md)「后续增补」把对应章补全：语法、说明、参数或输出、注意、示例，以及需要的兼容性。只加一句话不算完成。手册只写已经实现的用法。设计正文仍是实现约束。未实现的能力留在设计或日期文档里，不写进手册。
16. **注释。** 公开函数的中文 `///` 要写全，结构对齐 Javadoc：第一句说明用途，然后是 `# Arguments`、`# Returns`、`# Errors`。`//` 只写不变量，`unsafe` 写 `// SAFETY:`。细则见 skill **qdb-dev** 的「注释」节。
17. **API 兼容与版本纪律**。改 `/v1/*` 路径、JSON 字段、默认值、错误码，或 `abi_version` / 帧布局时：先改设计或手册；破坏性变更须在手册版本表或发布说明写清，并假定同级 **EquityLab** 等调用方会受影响——不得静默破坏。
18. **限制以手册为准**。批大小、SQL 行数、`GET /v1/klines` 的 `limit` 上限 **5000**、符号/帧容量等以 [`docs/manual/limits.md`](docs/manual/limits.md) 与 `server.md` 为准；改代码必须改手册。
19. **late-tick**。时间早于该证券当前未闭合桶起点的成交进入 `ticks`、不进入 K 线，并计入 `late_tick_total`。改写入/聚合路径时必须测并文档化，禁止 silently 改成「回填已闭合桶」。
20. **`QDB_FEED`**。进程内采集默认开启；EquityLab Compose 等外部采集场景设 `QDB_FEED=0`。排障可关 feed，**不得**把「无 feed + 无写入」当成库健康的默认完成态（`/health` 可能因无心跳 503）。
21. **生产级迭代（DoD / 分级 / 契约面）**：见下一节；未满足不得声称「功能完成」或进入发布。

与任何 skill 冲突时，以本文件和 `docs/design.md` 为准。

## 生产级迭代（强制）

### 变更分级

| 级别 | 典型 | 可跳过 | **不可跳过** |
|---|---|---|---|
| hotfix | 单点 bug、文案、明显回归 | 规划 / 架构 | 冒烟证据 +（建议）快速 Review |
| feature | 新 HTTP 面、聚合语义、订阅、契约微调 | — | 规划摘要或验收条 + 开发 + 测试证据 + Review |
| epic | ABI 升级、跨多层重构、复制/集群类 | — | 架构落盘 + 容量/延迟说明 + 整库回归 + Review 阻断项清零 |

用户未明确说「跳过流水线」时，**禁止**从编码直接跳到「做完了」。epic/feature 结束前加载 skill **qdb-dod** 自检。

### Definition of Done（缺一不可）

声称完成前必须同时具备：

1. **验收**：Planner 验收标准（或 hotfix 自拟 1–3 条）每条有命令输出或测试名。
2. **冒烟**：至少一条与本改动相关的可复现命令（`cargo test` 和/或 curl）。**仅 `/health` 200 不算写入/读路径完成。**
3. **文档**：改 HTTP / env / 限制 / late-tick / ABI → 同步 `docs/manual/`（及必要时 `docs/design.md`）。
4. **诚实边界**：未做完的能力须在手册或设计标明「未提供」；禁止当已实现宣传。
5. **运行态对齐**：改 `qdb-server` 或引擎行为后，必须重建二进制（或容器 recreate）再验；禁止只改源码未重建就报绿。
6. **收尾句式**（对人简报必填）：`已做 / 未做 / 已知风险 / 复验命令`。

清单落盘：`.agents/artifacts/YYYY-MM-DD-<topic>-dod.md`（拷贝 [`.agents/templates/dod.md`](.agents/templates/dod.md)）。

### 契约面不变量（HTTP / 引擎）

与硬性约束 §17–§20 同时生效：

1. **手册 ⊆ 已实现**：手册不写未实现接口；代码有行为则手册有对应章。
2. **读写冒烟顺序**：能写入（或明确用 feed）→ snapshots / klines / docs 读通 → 再谈完成。
3. **limits**：`klines` 的 `limit` ≤ 5000；SQL 默认/最大行数等与 `limits.md` 一致。
4. **late-tick**：写入路径变更必须有测试名或指标断言；行为与 `data-model.md` 一致。
5. **EquityLab 契约**：破坏 `/v1/snapshots|klines|docs|quotes|trades|changes|ws` 的字段或语义时，手册版本说明必填；同级仓客户端是否跟进不在本仓默默假设「没人用」。
6. **可观测**：行为改动至少验证一条 `/metrics` 关键字、日志或 HTTP 计数字段（如 `late_tick_total`、`written`）。

细则：[docs/2026-10-26-agent-dod.md](docs/2026-10-26-agent-dod.md)。

### 评审阻断项（Reviewer 直接 block）

- 破坏 HTTP/ABI 契约却无手册或版本说明
- 改写入语义却未处理/未文档 late-tick
- `f64` 进存储/聚合热路径，或已闭合 K 线被覆盖
- `unsafe` 出现在 `qdb-shm` / `qdb-wal` 以外
- 密钥或 `data/` 运行时目录进提交
- 无验收/冒烟证据的 feature/epic「完成」
- 改限制常量却未改 `limits.md` / `server.md`
- 新的使用者可见行为没有按 [`docs/manual/index.md`](docs/manual/index.md)「后续增补」补进对应章
- 把 stub/未实现能力写进手册当已提供

### 发布 / 回滚（Ops）

- 健康检查 **与** 本功能冒烟同轮通过才可宣称发布。
- 发布说明须含：**回滚 commit/二进制** + 是否需新数据目录（ABI 不兼容 / 脏 WAL）；**默认保留 WAL**。
- 不默认 push；禁止打 Tag（同提交约定）。

## 自动化测试

写完代码、提交之前必须运行：

```bash
cargo test --workspace -- --test-threads=1
```

失败就不提交。`--test-threads=1` 是因为实例锁和 HTTP 端口按进程号分配，并行会让第二个 `qdb` 抢到还没监听的锁。只跑一个 crate 不能代替整库，除非本次改动只在该 crate，并且同一轮里整库已经通过。

已经实现的功能点和边界都要有测试；本轮新写进文档、且代码里已经存在的行为，同一变更里补上测试。未实现的企业级和分布式设计不写成已通过的用例。用例清单见 skill **qdb-test**。

## 本地运行（环境即真相）

```bash
cargo run -p qdb-server --bin qdb -- data
curl -s http://127.0.0.1:9090/health
curl -s 'http://127.0.0.1:9090/v1/snapshots?symbols=SSE:600519'
```

无外呼写入时用 `QDB_FEED=0` 并 POST `/v1/quotes` 或 `/v1/trades`。端口见 `QDB_HTTP_PORT`（缺省 9090）。排障与限制见 [`docs/manual/server.md`](docs/manual/server.md)、[`limits.md`](docs/manual/limits.md)。

## 提交（/commit、提交）

用户说 `/commit`、`提交` 或明确要求 commit 时，遵循 skill **commit**：

- 信息格式：`type: 中文描述`（`feat` / `fix` / `perf` / `refactor` / `chore` / `docs` / `style`）
- **确认后再提交**（用户已说「提交一次」且未改文案时可直接提交）
- **不默认 push**；**禁止打 Tag**。用户明确要求 push 时走 skill **commit** 的推送步骤：经 `https_proxy=http://127.0.0.1:7897` 推送，不把代理写入 git config
- 不提交 `data/` 和密钥

用户没有明确要求提交时，不执行 commit。skill **implement** 末尾的自动提交在本仓不生效。

## 技能索引

完整列表：[`.agents/skills/README.md`](.agents/skills/README.md)。

| 意图 | Skill |
|---|---|
| 在本仓实现或修改 | `qdb-dev` |
| 按流水线推进 | `qdb-pipeline` |
| 完成判据 / 可否发布 | `qdb-dod` |
| 类型、crate、所有权 | `rust-program-design` |
| 并发、共享内存 | `rust-concurrency-design` |
| 行情概念 | `market-data-systems` |
| A 股制度 | `a-share-market` |
| 币安 / 24 小时行情 | `binance-market` |
| 拷问方案并落术语 / ADR | `grill-with-docs` |
| 按设计实现 | `implement`（配 `tdd`） |
| 深模块 / seam | `codebase-design` |
| 对照设计审查 | `code-review`（Spec 来源是 `docs/design.md` + 手册） |
| 提交前测试、补覆盖 | `qdb-test` |
| 难 bug | `diagnosing-bugs` |
| 写 skill / AGENTS | `writing-for-agents` |
