# QDB Agent Skills

仓库级硬约定：[../../AGENTS.md](../../AGENTS.md)。术语：[../../CONTEXT.md](../../CONTEXT.md)。设计正文：[../../docs/design.md](../../docs/design.md)。
锁文件：根目录 [`skills-lock.json`](../../skills-lock.json)（从上游安装的技能记录）。

## 来源说明

1. **流程类**：从 `equitylab/.agents/skills` 复制（该仓源自 deeplexcloud / mattpocock 系），加上本仓 **qdb-dev**。
2. **栈专用**：从 equitylab 已安装的副本取来，只保留 Rust 通用实践和差分审查。来源见锁文件。
3. **本仓编写**：`qdb-dev`、`qdb-pipeline`，加上 Rust 程序设计（`rust-program-design`、`rust-concurrency-design`）和行情前备知识（`market-data-systems`、`a-share-market`）。公开技能库里没有与本设计一致的对应物。

## 未放入（刻意跳过）

与本仓无关：`fastapi`、`langgraph-*`、`langchain-rag`、`rag-architect`、`redis-core`、`supabase-postgres-best-practices`、`qdrant-clients-sdk`、`ai-sdk`、`mcp-builder`、前端与 Tailwind / shadcn、`playwright-cli`、`webapp-testing`、`docker-patterns`、`multi-stage-dockerfile`、`devops-engineer`、`prd-development`、`user-story`、`semgrep`、`typescript-magician`。

角色和阶段按本仓 [qdb-pipeline](./qdb-pipeline/SKILL.md) 与 [`.agents/team/`](../team/README.md)，不照搬 EquityLab 的前端和 RAG 角色。第一版范围仍是 `docs/design.md` 第 13 节的 M1–M5。

## 索引

### 本仓专用

| Skill | 用途 |
|---|---|
| [qdb-dev](./qdb-dev/SKILL.md) | 改 ABI、共享内存、WAL、聚合、SQL、订阅和恢复 |
| [qdb-pipeline](./qdb-pipeline/SKILL.md) | 规划→架构→开发→测试→评审→发布→运维 |
| [qdb-test](./qdb-test/SKILL.md) | 提交前整库测试；分层、边界，以及尚未接入的覆盖率、loom、基准和 fuzz |

### 行情前备知识

| Skill | 用途 |
|---|---|
| [market-data-systems](./market-data-systems/SKILL.md) | 成交与报价、事件时间、K 线、序号、快照加增量 |
| [a-share-market](./a-share-market/SKILL.md) | 交易所代码、时段、竞价、涨跌停、复权 |
| [binance-market](./binance-market/SKILL.md) | 24 小时连续交易、UTC 日线、深度续传 |

### 文档与领域

| Skill | 用途 |
|---|---|
| [writing-for-agents](./writing-for-agents/SKILL.md) | 写 AGENTS.md / skill |
| [domain-modeling](./domain-modeling/SKILL.md) | 术语表 CONTEXT.md、ADR |
| [research](./research/SKILL.md) | 一手来源调研并落盘 |

### 设计与实现

| Skill | 用途 |
|---|---|
| [grilling](./grilling/SKILL.md) / [grill-me](./grill-me/SKILL.md) / [grill-with-docs](./grill-with-docs/SKILL.md) | 拷问式压测方案 |
| [writing-plans](./writing-plans/SKILL.md) / [brainstorming](./brainstorming/SKILL.md) | 计划 / 头脑风暴 |
| [codebase-design](./codebase-design/SKILL.md) | 深模块、seam |
| [improve-codebase-architecture](./improve-codebase-architecture/SKILL.md) | 架构改进评估 |
| [prototype](./prototype/SKILL.md) | 一次性原型 |
| [implement](./implement/SKILL.md) | 按 spec 实现。自动提交以 `AGENTS.md` 为准，本仓不自动提交 |
| [tdd](./tdd/SKILL.md) | 测试驱动 |

### 质量与协作

| Skill | 用途 |
|---|---|
| [diagnosing-bugs](./diagnosing-bugs/SKILL.md) | 硬 bug / 性能诊断 |
| [code-review](./code-review/SKILL.md) | Standards + Spec 双轴审查。Spec 用 `docs/design.md` |
| [differential-review](./differential-review/SKILL.md) | 安全向差分审查 |
| [commit](./commit/SKILL.md) | `type: 中文` 提交。推送经 Clash `127.0.0.1:7897` |
| [resolving-merge-conflicts](./resolving-merge-conflicts/SKILL.md) | 合并冲突 |

### Rust

| Skill | 来源 | 用途 |
|---|---|---|
| [rust-best-practices](./rust-best-practices/SKILL.md) | apollographql/skills | Rust 工程实践 |
| [rust-async-patterns](./rust-async-patterns/SKILL.md) | wshobson/agents | Tokio 异步。写入线程不使用它 |
| [rust-program-design](./rust-program-design/SKILL.md) | 本仓 | 程序设计：边界、类型、错误、所有权 |
| [rust-concurrency-design](./rust-concurrency-design/SKILL.md) | 本仓 | 并发与共享内存设计 |

## 同步

```bash
# 从 equitylab 覆盖某一流程技能或 Rust 技能
cp -R ~/git/equitylab/.agents/skills/<name> .agents/skills/<name>
```

与本仓 `AGENTS.md` 或 `docs/design.md` 冲突时，以 **QDB 的这两份文件**为准。
