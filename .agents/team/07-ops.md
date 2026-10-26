# Ops

## 阶段

发布与运维

## 触发

评审已通过，或用户说「发布 / 运维 / 恢复演练」。

## 输入

- 无阻断的评审结论
- 测试报告
- DoD pass（feature/epic；见 skill `qdb-dod`）
- 设计第 9、10 节

## 步骤

1. 确认 `qdb-server` 暴露 `/health` 和 `/metrics`。写入者心跳停止、存在未确认缺号、WAL CRC 失败时 `/health` 为 503。
2. 做一次恢复演练：写入若干 `seq`，删掉共享内存文件，再启动，核对 `applied_seq` 与闭合 K 线。
3. 写发布说明：本次 `seq` 行为、数据目录、回滚方式。回滚使用上一版二进制，不删除已有 WAL。
4. 记录空窗与环形缓冲水位该看哪些指标。
5. 第一版没有要求容器时，不新增 Dockerfile 或 Compose。
6. 核对 DoD 复验命令仍绿；仅 `/health` 不算发布通过。

## 产出

`.agents/artifacts/YYYY-MM-DD-<topic>-release.md`，模板 [../templates/release-notes.md](../templates/release-notes.md)。

## 完成判据

- [ ] 恢复演练的命令和结果写在发布说明里
- [ ] `/health` 的失败条件与设计第 10 节一致
- [ ] 回归测试仍通过
- [ ] DoD 为 pass（或 hotfix 有等价冒烟证据）

## 不做

业务功能。`git push --force`。把代理密码或数据库文件写进仓库。用户未要求时不推送。

## 推荐技能

`qdb-dev`、`diagnosing-bugs`、`commit`
