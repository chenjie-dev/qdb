# QDB Agent 开发流水线

功能按固定阶段推进：

**规划 → 架构 → 开发 → 测试 → 评审 → 发布 → 运维**

编排入口是 skill **qdb-pipeline**。角色卡在 [`.agents/team/`](../.agents/team/README.md)。硬约束在 [`AGENTS.md`](../AGENTS.md)，设计正文在 [`docs/design.md`](design.md)。

## 1. 总览

```text
Planner ──► Architect ──► Engine + Query
                              │
                              ▼
                            Test
                              │
                              ▼
                          Reviewer
                              │
                              ▼
                            Ops ──► 缺陷回到 Planner
```

| 阶段 | 角色 | 主要产出 | 进入下一阶段 |
|---|---|---|---|
| 规划 | Planner | PRD、验收标准、任务清单 | 验收可测，且没有越过第一版不做的范围 |
| 架构 | Architect | 相对 `docs/design.md` 的增量、crate 落点 | 布局或提交顺序有变化时，设计正文已改 |
| 开发 | Engine、Query | 对应 crate 与测试 | 该里程碑的验收命令能跑 |
| 测试 | Test | 测试报告 | `cargo test --workspace -- --test-threads=1` 通过；`qdb-test` 表里已实现的每行都有测试名 |
| 评审 | Reviewer | Standards / Spec 两轴 | 阻断项清零，或经人确认豁免 |
| 发布 | Ops | 发布说明 | 恢复演练和 `/health` 通过；**DoD pass** |
| 运维 | Ops | 指标、空窗、恢复步骤 | 已知故障写在发布说明里 |

Engine 与 Query 可在架构门禁之后并行。共享的帧布局、`seq` 和符号格式以 Architect 更新后的 `docs/design.md` 为准。

单会话里由当前代理按阶段扮演角色。交接物必须落在 `.agents/artifacts/`。没有落盘文件，不算阶段完成。

小修复可以跳过规划和架构，不能跳过测试冒烟和一次简短评审。feature / epic 在发布前须过 skill **qdb-dod**（见 [agent-dod-与引擎门禁](2026-10-26-agent-dod-与引擎门禁.md)）。

## 2. 角色

| 角色 | 阶段 | 角色卡 |
|---|---|---|
| Planner | 规划 | [01-pm-planner](../.agents/team/01-pm-planner.md) |
| Architect | 架构 | [02-architect](../.agents/team/02-architect.md) |
| Engine | 开发：ABI、共享内存、WAL、聚合、存储 | [03-engine](../.agents/team/03-engine.md) |
| Query | 开发：SQL、订阅、进程入口 | [04-query](../.agents/team/04-query.md) |
| Test | 测试 | [05-test](../.agents/team/05-test.md) |
| Reviewer | 评审 | [06-reviewer](../.agents/team/06-reviewer.md) |
| Ops | 发布与运维 | [07-ops](../.agents/team/07-ops.md) |

## 3. 落点

| 产出 | 路径 |
|---|---|
| PRD、任务、测试报告、评审、发布说明 | `.agents/artifacts/YYYY-MM-DD-<topic>-*.md` |
| 行为或布局变更 | 先改 `docs/design.md` |
| 新的说明文档 | `docs/YYYY-MM-DD-<标题>.md` |
| 帧、快照、K 线槽、符号 | `qdb-abi` |
| 环形缓冲、seqlock | `qdb-shm` |
| WAL、组提交、重放 | `qdb-wal` |
| 多周期聚合 | `qdb-agg` |
| 内存表与启动恢复 | `qdb-store` |
| SQL | `qdb-sql` |
| 订阅与断点续传 | `qdb-sub` |
| 写入线程、`/health`、`/metrics` | `qdb-server` |

模板在 [`.agents/templates/`](../.agents/templates/)。

## 4. 最低可用版本

设计第 13 节的 M1–M5 是第一版范围。一轮流水线可以只做其中一个里程碑，但验收标准仍取那一节，不另写一套。

PostgreSQL 线协议、WebSocket、gRPC、历史导入、账号权限、复制，任务没有点名就不进入开发。

## 5. 启动一轮

```text
按 qdb-pipeline 做：<一句话目标>
从规划开始 / 从架构开始 / 只做 Engine / 只做测试和评审
```

代理打开 skill **qdb-pipeline**，读取当前角色卡，门禁通过后再进入下一阶段。阶段结束时用技能里的固定简报。
