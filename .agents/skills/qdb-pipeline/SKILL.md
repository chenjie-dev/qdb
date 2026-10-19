---
name: qdb-pipeline
description: >-
  QDB agent delivery pipeline (plan, architecture, engine, query, test, review,
  release, operate). Use when starting a feature, saying 流水线/规划/架构/评审/发布/运维,
  or when the user wants work run as the QDB agent team.
---

> **中文说明**：按「规划→架构→开发→测试→评审→发布→运维」推进 QDB。一次只主扮演一个角色，门禁通过再进入下一阶段。实现细节仍遵守 `qdb-dev`。

# QDB Pipeline

## 权威文档

- 流水线：[`docs/2026-10-19-agent-pipeline.md`](../../../docs/2026-10-19-agent-pipeline.md)
- 角色卡：[`../../team/README.md`](../../team/README.md)
- 硬约束：[`AGENTS.md`](../../../AGENTS.md)
- 设计正文：[`docs/design.md`](../../../docs/design.md)

## 过程

1. **定阶段**
   - 用户指定起点则听从。
   - 新需求从规划开始。设计里已有契约、任务也明确时从开发开始。修 bug 走开发加测试。
   - 小修复可跳过规划和架构，不可跳过冒烟和简短评审。

2. **加载角色卡**（一次一个主角色；Engine 与 Query 可声明并行）

   | 阶段 | 读取 |
   |---|---|
   | 规划 | `team/01-pm-planner.md` |
   | 架构 | `team/02-architect.md` |
   | 开发-引擎 | `team/03-engine.md` |
   | 开发-查询与订阅 | `team/04-query.md` |
   | 测试 | `team/05-test.md` |
   | 评审 | `team/06-reviewer.md` |
   | 发布 / 运维 | `team/07-ops.md` |

3. **落盘**到 `.agents/artifacts/`，使用 `.agents/templates/`。没有文件不算完成。

4. **门禁**：角色卡「完成判据」全部勾选后才进入下一阶段。

5. **简报**

```text
## Pipeline status
- 阶段：<名>
- 角色：<名>
- 产出：<路径列表>
- 门禁：pass | fail（失败原因）
- 下一步：<下一角色/阶段>
```

## 路由

| 用户说法 | 进入 |
|---|---|
| 规划 / PRD / 拆任务 | Planner |
| 架构 / ABI / 数据模型 | Architect |
| 共享内存 / WAL / 聚合 / 恢复 | Engine |
| SQL / 订阅 / 健康检查 | Query |
| 测试 / 冒烟 | Test |
| 审查 / review | Reviewer |
| 发布 / 运维 / 恢复演练 | Ops |
| 按流水线做完 | 从 Planner 走到 Ops |

## 与其他 skill

- 写代码遵守 `qdb-dev`。
- 新类型用 `rust-program-design`，并发和共享内存用 `rust-concurrency-design`。
- 行情语义用 `market-data-systems` 和 `a-share-market`。
- 评审用 `code-review`；`unsafe` 或共享内存再加 `differential-review`。
- 用户要求提交时用 `commit`。未要求则不提交、不推送。
