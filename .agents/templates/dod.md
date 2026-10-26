# DoD：<标题>

- 日期：YYYY-MM-DD
- 级别：hotfix | feature | epic
- 关联 PRD / commit / 话题：
- 作者角色：

## 已做 / 未做 / 已知风险 / 复验命令

- 已做：
- 未做（须诚实）：
- 已知风险：
- 复验命令：

```bash
# 粘贴本轮实际跑过的命令
```

## 验收对照

| 验收标准 | 结果 pass/fail | 证据（命令或测试名） |
|---|---|---|

## 冒烟（非仅 /health）

| 命令 | 期望 | 实际 |
|---|---|---|

## 文档

| 变更类型 | 已更新路径 | 跳过理由 |
|---|---|---|
| design.md 对应节 | | |
| docs/manual/ 对应章 | | |
| CONTEXT.md（若术语变） | | |

## 引擎面（若涉及 ABI / WAL / 共享内存 / 写入 / 查询）

| 检查 | pass/fail | 证据 |
|---|---|---|
| `cargo test --workspace -- --test-threads=1` | | |
| 布局 `size_of` / 偏移（若改帧或槽） | | |
| 恢复：删 shm 后从 WAL 重建（若改恢复） | | |
| late-tick / 已闭合 K 线语义与手册一致 | | |
| HTTP 契约：snapshots / klines / docs / quotes（若改 API） | | |
| `limits.md` 数值仍准确（若改容量） | | |
| 单写入者：第二进程失败（若改实例锁） | | |

## 运行态

- [ ] 用本轮二进制 / `cargo run -p qdb-server`（或已重建镜像）验证，非口头「应该可以」
- [ ] 未把未跑通的测试当成完成

## 结论

- DoD：pass | fail
- 可否进入 Review / Release：是 / 否
- 阻断原因（若 fail）：
