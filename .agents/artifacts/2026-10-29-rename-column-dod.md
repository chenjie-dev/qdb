# DoD：ALTER TABLE RENAME COLUMN

- 日期：2026-10-29
- 级别：feature
- 关联 PRD / commit / 话题：单机 SQL，列改名
- 作者角色：Query

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：`ALTER TABLE ... RENAME COLUMN` 只改列名。行槽和行 `seq` 不变。引用该列的二级索引跟着改。主键列也可以改名。
- 未做（须诚实）：`ADD COLUMN`、`DROP COLUMN`、`IF EXISTS`、一次改多列、`COPY`、子查询、`WITH`、窗口函数。
- 已知风险：不认识这条新记录的旧进程重放时会跳过改列名。调用方列若取名 `symbol` / `period` / `ts`，等值条件仍会被扫描谓词当成行情过滤（既有行为，不是本改动引入）。
- 复验命令：

```bash
cargo test --workspace --offline -- --test-threads=1
```

## 验收对照

| 验收标准 | 结果 pass/fail | 证据（命令或测试名） |
|---|---|---|
| 列名和二级索引跟着改，行 `seq` 不变 | pass | `rename_column_keeps_row_slots_and_updates_indexes` |
| 主键列可改名 | pass | 同上，`id` → `key` |
| 缺列、重名、内置表、缺表失败 | pass | 同上 |
| 重启后仍用新列名 | pass | 同上 |

## 冒烟（非仅 /health）

| 命令 | 期望 | 实际 |
|---|---|---|
| `cargo test -p qdb-sql --offline --test tables -- --test-threads=1 rename_column_keeps_row_slots_and_updates_indexes` | pass | pass |
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
| `cargo test --workspace -- --test-threads=1` | pass | 本轮整库 |
| 布局 `size_of` / 偏移（若改帧或槽） | | 未改帧。新消息类型 15，`abi_version` 仍为 1 |
| 恢复：删 shm 后从 WAL 重建（若改恢复） | pass | 用例重新打开后列名仍是新名字 |
| late-tick / 已闭合 K 线语义与手册一致 | | 未改聚合 |
| HTTP 契约 | | 未改 HTTP |
| `limits.md` | | 未改容量 |
| 单写入者 | | 未改实例锁 |

## 运行态

- [x] 用本轮测试二进制验证
- [x] 未把未跑通的测试当成完成

## 结论

- DoD：pass
- 可否进入 Review / Release：可以进入 Review。未要求发布。
- 阻断原因（若 fail）：
