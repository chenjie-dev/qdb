# DoD：内存分配优化

- 日期：2026-11-03
- 级别：feature
- 关联：用户反馈「使用感觉很耗内存」
- 作者角色：Engine / Query

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：区域容量环境变量化（缺省 desk）；不整文件 `fill(0)`；私有 frames 与 `ring_capacity` 对齐；Tick 保留可配置（`set_tick_retention_ns` / `QDB_TICK_RETENTION_SECS`，`0`=不剪）；**撤销**已闭合 K 线条数硬上限。
- 未做：`RetentionExceeded` 查询错误；按机器自动探测物理内存给缺省区域。
- 已知风险：大机器若忘记调大 `QDB_MAX_SYMBOLS` 会先碰到符号槽满；小机器若设成 market 仍会占约 84 MiB 逻辑长度。
- 复验命令：

```bash
cargo test --workspace --offline -- --test-threads=1
cargo test -p qdb-shm -- --test-threads=1 desk_region from_env create_does_not
cargo test -p qdb-store --test boundaries -- --test-threads=1 ticks_older private_frames
```

## 验收对照

| 验收标准 | 结果 | 证据 |
|---|---|---|
| `qdb` 缺省区域逻辑长度 ≪ 原 market ~84 MiB | pass | `desk_region_is_much_smaller_than_market` |
| 非法 ring 容量启动失败 | pass | `from_env_rejects_non_power_of_two_ring` |
| Tick 超过 2h 被丢掉 | pass | `ticks_older_than_retention_window_are_dropped` |
| 私有 frames ≤ ring | pass | `private_frames_stay_within_ring_capacity` |
| 已闭合 K 线无条数硬上限 | pass | 已删 `MAX_SEALED_BARS`；`zero_tick_retention_keeps_old_prints` |

## 冒烟（非仅 /health）

| 命令 | 期望 | 实际 |
|---|---|---|
| 见上 `cargo test` 过滤用例 | 通过 | 本轮先跑过滤；整库见引擎面 |

## 文档

| 变更类型 | 已更新路径 | 跳过理由 |
|---|---|---|
| design.md §3.1 / §4.2–4.3 / 配置句 | `docs/design.md` | |
| limits / server / shared-memory / library / data-model | `docs/manual/*` | |

## 引擎面

| 检查 | 结果 | 证据 |
|---|---|---|
| `cargo test --workspace -- --test-threads=1` | pass | 本轮 exit 0 |
| late-tick | pass | 未改聚合迟到语义；仍计 `late_tick_total` |
| `limits.md` | pass | 已改容量表 |

## 结论

- DoD：pass
- 可否进入 Review：是
