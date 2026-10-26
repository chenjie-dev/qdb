# Test

## 阶段

测试。也可以在开发前按 `tdd` 先写失败测试。

## 触发

实现完成，或用户说「测试 / 冒烟 / 回归」。

## 输入

- PRD 的验收标准
- [`docs/design.md`](../../docs/design.md) 第 13 节
- 技能 `qdb-test`、`tdd`、`diagnosing-bugs`

## 步骤

1. 每条验收对应一条可重复命令或一个测试名。
2. 按里程碑选取证据，而不是只看编译通过：
   - M1：写进程与慢读者，读者必须得到 gap
   - M2：设计第 7.5 节三笔成交；重放两次一致；缺号不闭合
   - M3：删掉共享内存文件后，从状态快照和 WAL 恢复；坏 CRC 停在坏记录前
   - M4：`SZSE:000001` 的 K 线查询；歧义代码被拒绝
   - M5：按代码和周期过滤；`from_seq` 补齐；落后时先快照
3. 运行 `cargo test --workspace -- --test-threads=1`。相关 crate 可以先跑 `cargo test -p <crate> -- --test-threads=1`，不能代替整库。还没有目标 crate 时，报告写明阻塞，不把口头描述当成通过。
4. 已实现的复制、槽位、可见性、切换时限和组提交性能，按 skill `qdb-test` 的表逐条指出测试名。分层、订阅、恢复和「尚未接入的门禁」也以该 skill 为准。性能用表里的倍数和时限，不记录某台机器的 `ops/sec`。
5. 失败时给出最小复现，交回 Engine 或 Query。不在测试里改业务结果来让它变绿。

## 产出

`.agents/artifacts/YYYY-MM-DD-<topic>-test.md`，模板 [../templates/test-report.md](../templates/test-report.md)。

## 完成判据

- [ ] 每条验收有证据
- [ ] 相关 `cargo test` 通过，或明确写出还不能跑的原因
- [ ] 缺陷带复现命令

## 不做

扩大产品范围，把未实现的协议写成已通过。

## 推荐技能

`tdd`、`diagnosing-bugs`、`qdb-dev`
