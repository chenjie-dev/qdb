# Agent rules — QDB

本文件是仓库级 **agent 行为规范的唯一权威来源**。Cursor、Codex、Claude Code 及其他编码代理均须遵守。
细则技能见 [`.agents/skills/`](.agents/skills/)；本文件只放跨工具、每次会话都应生效的硬性约定。

写给 agent 看的文档写法见 skill **writing-for-agents**。领域术语见根目录 [`CONTEXT.md`](CONTEXT.md)。

## 项目是什么

QDB 是面向股市行情的内存数据库，用 Rust 实现。单机上只有一个写入者：行情先组提交进 WAL，再更新内存表，最后发布到本机共享内存。其他进程只读。

```text
行情源 → 写入线程 → WAL → 内存表 → 共享内存 → SQL / 订阅 / 外部只读进程
                              ↘ 状态快照 → 启动恢复
```

目录约定：`docs/design.md` 是设计正文，`crates/*` 是实现，`.agents/skills/` 是技能，`.agents/team/` 是角色卡，`.agents/artifacts/` 是阶段交接物。第一版还没有 crate。

## 开发流水线

功能默认按阶段推进，入口 skill 是 **qdb-pipeline**：

```text
规划 → 架构 → 开发（Engine 与 Query）→ 测试 → 评审 → 发布 → 运维
```

说明：[docs/2026-10-19-agent-pipeline.md](docs/2026-10-19-agent-pipeline.md)。角色卡：[`.agents/team/`](.agents/team/README.md)。交接物用 [`.agents/templates/`](.agents/templates/) 写到 `.agents/artifacts/`。门禁未过不得进入下一阶段。小修复可跳过规划和架构，不能跳过冒烟和简短评审。

## 必读指针（按需加载）

| 何时 | 打开 |
|---|---|
| 改任何实现、ABI、恢复或订阅行为 | [`docs/design.md`](docs/design.md) 里对应的节，不要整篇重读 |
| 术语不一致 | [`CONTEXT.md`](CONTEXT.md) |
| 在本仓落代码 | skill **qdb-dev** |
| 按阶段做功能 | skill **qdb-pipeline**，说明见 [`docs/2026-10-19-agent-pipeline.md`](docs/2026-10-19-agent-pipeline.md) |
| 写或改 Rust | skill **rust-best-practices**；异步 IO 再加 **rust-async-patterns** |
| 新增 crate、公开类型、错误或所有权 | skill **rust-program-design** |
| 线程、原子量、共享内存、`unsafe` | skill **rust-concurrency-design** |
| 成交/报价、K 线、序号、缺口、快照加增量 | skill **market-data-systems** |
| A 股代码、时段、竞价、涨跌停、复权 | skill **a-share-market** |
| 共享内存、WAL、`unsafe` 的差分审查 | skill **differential-review** |

设计第 13 节的 M1–M5 是最低可用版本。验收标准以那一节为准。

## 硬性约束

1. **设计先行**。实现与 [`docs/design.md`](docs/design.md) 冲突时，先改设计再改代码。`abi_version`、帧布局、seqlock、组提交顺序变了，必须同时改设计。
2. **一个写入者**。只有持有 `data/writer.lock` 的进程可以分配 `seq`、写 WAL、以可写方式映射共享内存。读者只读映射。
3. **共享内存是定长字节**。跨进程类型放在 `qdb-abi`，`repr(C)`、小端、无指针。`Vec`、`String`、`HashMap` 只留在引擎私有内存。
4. **`unsafe` 只出现在 `qdb-shm` 和 `qdb-wal`**。
5. **价格和金额用 `i64` 定点**。存储、聚合、比较不使用 `f64`。库内保存交易所原始价，不存复权价。
6. **先落盘再发布**。读者能看见的 `seq` 已经在 WAL 里 `fsync`。默认组提交 1ms 或 256 条。
7. **已闭合 K 线只追加 `revision`**。未闭合 K 线只更新自己的槽。Tick 段只追加。
8. **gap 必须显式**。`seq` 缺号或读者被环形缓冲套圈时，停止把后续帧当成连续数据。
9. **共享内存不出节点**。跨机器只复制 WAL。第一版不实现复制。
10. **写入线程保持同步**。Tokio 只用于 TCP、HTTP 和以后的 gRPC。聚合、WAL 和共享内存发布留在专用写入线程。
11. **第一版止于 M1–M5**。任务没有点名时，不做 PostgreSQL 线协议、WebSocket、gRPC、历史导入、账号权限、审计文件、Raft。
12. **文档命名**。`docs/` 下新建文档用 `YYYY-MM-DD-<标题>.md`（创建日）。`docs/design.md` 保持这个文件名，它是设计正文。
13. **标识符用英文，说明用中文**。给人和 agent 的文档、提交说明用中文。

与任何 skill 冲突时，以本文件和 `docs/design.md` 为准。

## 验证

有对应 crate 之后，相关改动至少让 `cargo test -p <crate>` 通过。帧、快照槽、K 线槽的改动要有 `size_of` / 偏移断言。恢复改动要覆盖「删掉共享内存文件后从 WAL 和状态快照重建」。

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
| 类型、crate、所有权 | `rust-program-design` |
| 并发、共享内存 | `rust-concurrency-design` |
| 行情概念 | `market-data-systems` |
| A 股制度 | `a-share-market` |
| 拷问方案并落术语 / ADR | `grill-with-docs` |
| 按设计实现 | `implement`（配 `tdd`） |
| 深模块 / seam | `codebase-design` |
| 对照设计审查 | `code-review`（Spec 来源是 `docs/design.md`） |
| 难 bug | `diagnosing-bugs` |
| 写 skill / AGENTS | `writing-for-agents` |
