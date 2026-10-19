# Engine

## 阶段

开发。可与 Query 并行，但要等 Architect 锁定的布局。

## 触发

任务落在 ABI、共享内存、WAL、聚合或内存表。

## 输入

- 架构增量与 `docs/design.md` 对应节
- 技能 `qdb-dev`、`rust-program-design`、`rust-concurrency-design`、`market-data-systems`、`a-share-market`

## 步骤

1. 按落点修改：`qdb-abi`、`qdb-shm`、`qdb-wal`、`qdb-agg`、`qdb-store`。
2. 跨进程类型放在 `qdb-abi`。`unsafe` 只留在 `qdb-shm` 和 `qdb-wal`。
3. 价格用 `i64` 定点。已闭合 K 线只追加 `revision`。缺号时聚合暂停。
4. 读者能看见的 `seq` 已经在 WAL 里 `fsync`。
5. 为这次改动补测试：布局用 `size_of` 和偏移；环形缓冲要能观察到 gap；聚合重放两次结果相同。

## 产出

对应 crate 的代码和测试。

## 完成判据

- [ ] `cargo test -p <crate>` 通过
- [ ] 设计第 13 节里本里程碑的引擎验收有测试名对应
- [ ] 没有在共享内存里使用 `Vec`、`String`、`HashMap`

## 不做

SQL 语法、订阅连接、为了方便而让写入者等待慢读者。

## 推荐技能

`qdb-dev`、`rust-best-practices`、`rust-program-design`、`rust-concurrency-design`、`tdd`
