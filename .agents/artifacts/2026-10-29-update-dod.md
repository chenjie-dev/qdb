# DoD：UPDATE 按主键覆盖非主键列

- 日期：2026-10-29
- 级别：feature
- 关联 PRD / commit / 话题：单机 SQL，`UPDATE`
- 作者角色：Query

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：`UPDATE` 只改调用方表的非主键列。`SET` 右边按旧行一起求值。`WHERE` 与 `DELETE` 相同。覆盖写 `RowReplace`，分配新 `seq`，不改截止时间，不擦已落盘的旧行。
- 未做（须诚实）：不带 `WHERE` 的更新、`FROM`、`RETURNING`、改主键、按非主键列选择行、`ALTER`、`COPY`、子查询、`WITH`、窗口函数。
- 已知风险：可见序号为 0 的旧快照行不能覆盖，返回 `syntax: seq conflict`。一批里后一条提交若在 `fsync` 之后失败，前面已经成功的覆盖会留下。
- 复验命令：

```bash
cargo test --workspace --offline -- --test-threads=1
```

## 验收对照

| 验收标准 | 结果 pass/fail | 证据（命令或测试名） |
|---|---|---|
| 非主键列被覆盖，主键不变，`seq` 改变 | pass | `update_replaces_non_key_columns_and_keeps_the_deadline` |
| `SET left_n = right_n, right_n = left_n` 交换 | pass | 同上 |
| 缺键整句不写 | pass | 同上，`syntax: key not found` 后 `n` 仍为 2 |
| 截止时间保留，到期后该行不可见 | pass | 同上，`EXPIRE AT 100` |
| 重启后仍是新值；只读引擎拒绝 | pass | 同上 |

## 冒烟（非仅 /health）

| 命令 | 期望 | 实际 |
|---|---|---|
| `cargo test -p qdb-sql --offline --test tables -- --test-threads=1 update_replaces_non_key_columns_and_keeps_the_deadline` | pass | pass |
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
| 布局 `size_of` / 偏移（若改帧或槽） | | 未改帧。沿用 `RowReplace` |
| 恢复：删 shm 后从 WAL 重建（若改恢复） | pass | 用例重新打开后 `left_n` 仍为 2 |
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
