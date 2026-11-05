# DoD：qdb 进程组提交

- 日期：2026-11-05
- 级别：hotfix
- 关联 PRD / commit / 话题：设计里的 256 条或 1ms 组提交接到 `qdb` 进程
- 作者角色：Engine

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：进程打开每个库时按 `QDB_GROUP_RECORDS` / `QDB_GROUP_MS` 设置组提交（缺省 256 或 1ms）。空闲接受循环调用 `flush_if_due`。库接口 `Engine::open` 仍是每条 `fsync`。成交 HTTP 仍在应答前 `flush`。
- 未做（须诚实）：没有 Redis `appendfsync everysec` 那种独立 bio 线程；间隔靠接受循环的 1ms 睡眠触发。没有运行时热改组提交。
- 已知风险：`QDB_GROUP_MS>0` 时空闲循环从 50ms 改为约 1ms 醒一次。报价在间隔内崩溃会丢掉尚未 `fsync` 的那一批；`POST /v1/trades` 不受影响。
- 复验命令：

```bash
cargo test --workspace --offline -- --test-threads=1
```

## 验收对照

| 验收标准 | 结果 pass/fail | 证据（命令或测试名） |
|---|---|---|
| 间隔未到时 `flush_if_due` 不刷，到了刷一次 | pass | `group_commit_fsyncs_once_for_the_batch_and_hides_it_until_flush` |
| 环境变量缺省 256/1ms，`0` 条按 1，非法毫秒拒绝 | pass | `group_commit_defaults_and_honors_env` |
| 整库回归 | pass | `cargo test --workspace --offline -- --test-threads=1` 退出码 0 |

## 冒烟（非仅 /health）

| 命令 | 期望 | 实际 |
|---|---|---|
| `cargo test -p qdb-store --test engine group_commit_fsyncs_once_for_the_batch_and_hides_it_until_flush -- --test-threads=1` | pass | pass（含在整库运行里） |

## 文档

| 变更类型 | 已更新路径 | 跳过理由 |
|---|---|---|
| design.md 对应节 | `docs/design.md` 组提交段 | |
| docs/manual/ 对应章 | `server.md`、`limits.md`、`concepts.md` | |
| CONTEXT.md（若术语变） | | 术语未变 |
