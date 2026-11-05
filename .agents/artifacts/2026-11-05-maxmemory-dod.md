# DoD：maxmemory / noeviction

- 日期：2026-11-05
- 级别：feature
- 关联：对齐 Redis 内存预算
- 作者角色：Engine

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：`Engine::set_maxmemory` 与 `QDB_MAXMEMORY`（`0` 不限制，支持 kb/mb/gb）。估算覆盖逐笔、K 线、发布帧、调用方表载荷，不含 `region.bin`。到顶拒绝增长写入，删除/DROP/TRUNCATE 仍可执行。`/metrics` 增加 `qdb_used_memory`、`qdb_maxmemory`、`qdb_rejected_oom`。
- 未做：LRU/LFU 淘汰；按分配器 RSS 精确计量；HTTP 单独的 507 状态码（写入失败仍是 400，正文 `maxmemory reached`）。
- 已知风险：估算小于真实 RSS；单条大写入可以先冲过预算。
- 复验命令：

```bash
cargo test --workspace --offline -- --test-threads=1
```

## 验收对照

| 验收标准 | 结果 | 证据 |
|---|---|---|
| 预算等于当前占用时下一笔成交被拒绝，已有行仍在 | pass | `maxmemory_rejects_growth_and_keeps_the_rows_already_stored` |
| `0` 恢复不限制 | pass | 同上，第二段写入成功 |
| 单位解析 | pass | `maxmemory_accepts_bytes_and_binary_units` |

## 文档

| 路径 | |
|---|---|
| docs/design.md | 容量节 |
| docs/manual/limits.md server.md errors.md library.md | 已改 |

## 结论

- DoD：pass
- 可否进入 Review：是
- 整库：`cargo test --workspace --offline -- --test-threads=1` exit 0
