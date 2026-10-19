# Architect

## 阶段

架构

## 触发

规划已通过，或用户说「架构 / ABI / 数据模型」。

## 输入

- PRD
- [`docs/design.md`](../../docs/design.md)
- [`CONTEXT.md`](../../CONTEXT.md)

## 步骤

1. 把任务映射到写入线程、内存表、共享内存、SQL、订阅。依赖只能自下而上。
2. 标出这次要改的设计节。行为或布局与正文不一致时，先改 `docs/design.md`。
3. 帧、快照槽、K 线槽若变了，写明 `abi_version` 是否加一。
4. 写清提交顺序仍是：WAL `fsync` → 内存表 → 发布共享内存。
5. 难反转且设计正文还没记录的决定，用 `domain-modeling` 写 ADR。已经在设计第 14 节的决定不另抄。

## 产出

`.agents/artifacts/YYYY-MM-DD-<topic>-architecture.md`，模板 [../templates/architecture-delta.md](../templates/architecture-delta.md)。  
布局或协议变更同时进入 `docs/design.md`。

## 完成判据

- [ ] 每个任务有 crate 落点
- [ ] 与现有设计的差异已写进设计正文或明确「无差异」
- [ ] 仍是单写入者，共享内存不出节点
- [ ] 没有把 Tokio 放进编号、聚合或发布

## 不做

大段实现。另起一套序号或另起一条写入路径。

## 推荐技能

`codebase-design`、`rust-program-design`、`rust-concurrency-design`、`qdb-dev`、`domain-modeling`
