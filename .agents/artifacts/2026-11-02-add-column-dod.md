# DoD：ALTER TABLE ADD COLUMN

- 日期：2026-11-02
- 级别：feature
- 关联 PRD / commit / 话题：单机 SQL，加列
- 作者角色：Query

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：`ALTER TABLE ... ADD COLUMN` 在末尾加一列。已有行填类型默认值。行 `seq` 不变。不能加主键。
- 未做（须诚实）：`DROP COLUMN`、`DEFAULT`、加列同时建索引、`IF EXISTS`、一次加多列、`COPY`、子查询、`WITH`、窗口函数。
- 已知风险：不认识这条新记录的旧进程重放时会跳过加列。`abi_version` 未变。
- 复验命令：

```bash
cargo test --workspace --offline -- --test-threads=1
```

## 验收对照

| 验收标准 | 结果 pass/fail | 证据（命令或测试名） |
|---|---|---|
| 已有行填 BOOL/INT64/STRING 默认值，行 `seq` 不变 | pass | `add_column_pads_existing_rows_with_type_defaults` |
| 之后插入必须写满新列 | pass | 同上 |
| 重名、主键、未知类型、内置表、缺表失败 | pass | 同上 |
| 重启后仍在 | pass | 同上 |

## 冒烟（非仅 /health）

| 命令 | 期望 | 实际 |
|---|---|---|
| `cargo test -p qdb-sql --offline --test tables -- --test-threads=1 add_column_pads_existing_rows_with_type_defaults` | pass | pass |
| `cargo test --workspace --offline -- --test-threads=1` | pass | pass |

## 文档

| 变更类型 | 已更新路径 | 跳过理由 |
|---|---|---|
| design.md 对应节 | `docs/design.md` 第 5 节 | |
| docs/manual/ 对应章 | `docs/manual/sql.md`、`errors.md` | |
| CONTEXT.md（若术语变） | | 未改术语 |

## 引擎面（若涉及 ABI / WAL / 共享内存 / 写入 / 查询）

| 检查 | pass/fail | 证据 |
|---|---|---|
| `cargo test --workspace -- --test-threads=1` | pass | 本轮整库 |
| 布局 `size_of` / 偏移（若改帧或槽） | | 未改帧。新消息类型 16，`abi_version` 仍为 1 |
| 恢复：删 shm 后从 WAL 重建（若改恢复） | pass | 用例重新打开后默认值仍在 |
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
