# DoD：ALTER TABLE RENAME TO

- 日期：2026-10-29
- 级别：feature
- 关联 PRD / commit / 话题：单机 SQL，表改名
- 作者角色：Query

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：`ALTER TABLE ... RENAME TO` 只改调用方表的名字。行、可见序号、截止时间和索引跟着走。旧名字可以再建空表。
- 未做（须诚实）：`RENAME COLUMN`、`ADD COLUMN`、`DROP COLUMN`、`IF EXISTS`、一次改多张表、`COPY`、子查询、`WITH`、窗口函数。
- 已知风险：不认识这条新记录的旧进程重放时会跳过改名，表仍用旧名字。`abi_version` 未变。
- 复验命令：

```bash
cargo test --workspace --offline -- --test-threads=1
```

## 验收对照

| 验收标准 | 结果 pass/fail | 证据（命令或测试名） |
|---|---|---|
| 行、`seq` 和二级索引跟着新名字 | pass | `rename_moves_rows_seq_and_indexes_to_the_new_name` |
| 旧名字可以再建且是空表 | pass | 同上 |
| 同名、已有表、内置表、缺表失败 | pass | 同上 |
| 重启后仍是新名字，截止时间仍生效 | pass | 同上，重新打开后 `EXPIRE AT 100` |

## 冒烟（非仅 /health）

| 命令 | 期望 | 实际 |
|---|---|---|
| `cargo test -p qdb-sql --offline --test tables -- --test-threads=1 rename_moves_rows_seq_and_indexes_to_the_new_name` | pass | pass |
| `cargo test --workspace --offline -- --test-threads=1` | pass | pass |

## 文档

| 变更类型 | 已更新路径 | 跳过理由 |
|---|---|---|
| design.md 对应节 | `docs/design.md` 第 5 节 | |
| docs/manual/ 对应章 | `docs/manual/sql.md`、`errors.md`、`index.md` | |
| CONTEXT.md（若术语变） | | 未改术语 |

## 引擎面（若涉及 ABI / WAL / 共享内存 / 写入 / 查询）

| 检查 | pass/fail | 证据 |
|---|---|---|
| `cargo test --workspace -- --test-threads=1` | pass | 本轮整库通过 |
| 布局 `size_of` / 偏移（若改帧或槽） | | 未改帧。新消息类型 14，`abi_version` 仍为 1 |
| 恢复：删 shm 后从 WAL 重建（若改恢复） | pass | 用例重新打开后行在新名字下 |
| late-tick / 已闭合 K 线语义与手册一致 | | 未改聚合 |
| HTTP 契约：snapshots / klines / docs / quotes（若改 API） | | 未改 HTTP |
| `limits.md` 数值仍准确（若改容量） | | 未改容量 |
| 单写入者：第二进程失败（若改实例锁） | | 未改实例锁 |

## 运行态

- [x] 用本轮测试二进制验证，非口头「应该可以」
- [x] 未把未跑通的测试当成完成

## 结论

- DoD：pass
- 可否进入 Review / Release：可以进入 Review。未要求发布。
- 阻断原因（若 fail）：
