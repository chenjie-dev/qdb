---
name: qdb-dod
description: >-
  QDB Definition of Done and production-iteration gates (change tier, cargo test
  evidence, engine-plane checks, manual/design sync, honest scope). Use before
  claiming a feature done, ending qdb-pipeline, or when the user says DoD/
  完成判据/生产级迭代/可否发布.
---

> **中文说明**：功能收尾强制 DoD 自检；对照 AGENTS「生产级迭代」与模板 `.agents/templates/dod.md`。

# QDB DoD

## 权威

- [`AGENTS.md`](../../../AGENTS.md)「生产级迭代」
- 清单模板：[`../../templates/dod.md`](../../templates/dod.md)
- 细则：[`docs/2026-10-26-agent-dod-与引擎门禁.md`](../../../docs/2026-10-26-agent-dod-与引擎门禁.md)

## 过程（completion：artifacts 有勾选 DoD 或简报含四要素）

1. **定级别**：hotfix | feature | epic（见 AGENTS 分级表）。
2. **拷贝模板** → `.agents/artifacts/YYYY-MM-DD-<topic>-dod.md`，逐项填。
3. **跑测试 / 冒烟**：默认 `cargo test --workspace -- --test-threads=1`；行为面再加与本改动相关的 HTTP / SQL / 恢复命令。**仅 `/health` 不算引擎完成。**
4. **引擎面**（若动 ABI / WAL / shm / 聚合 / SQL / HTTP）：按 AGENTS 引擎不变量勾选；手册与设计已同变更更新。
5. **简报**对人输出：`已做 / 未做 / 已知风险 / 复验命令`。
6. **fail 则不得**声称完成或进入 Release。

## 阻断速查

见 AGENTS「评审阻断项」。本 skill 自检时若命中，DoD = fail。
