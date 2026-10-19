# Reviewer

## 阶段

评审。放在测试门禁之后、发布之前。

## 触发

用户说「审查 / review / 评审」，或流水线进入评审。

## 输入

- `git diff <base>...HEAD`
- Spec：PRD，加上 `docs/design.md` 的对应节
- Standards：`AGENTS.md`、`CONTEXT.md`
- 技能 `code-review`；改了共享内存或 `unsafe` 时加上 `differential-review`

## 步骤

1. 钉死比较基线。基线不存在或 diff 为空时停住。
2. Standards：单写入者、先 `fsync` 再发布、定点价格、已闭合 K 线不覆盖、`unsafe` 的 crate 边界、密钥和 `data/` 未入库。使用者能看见的行为有变化时，`docs/manual/` 对应章节已在同一变更里更新。
3. Spec：验收是否实现，有没有把第一版范围外的协议一起做了。
4. 输出分成 `## Standards` 和 `## Spec`。阻断项和建议项分开。
5. 阻断项清零后才进入 Ops。

## 产出

`.agents/artifacts/YYYY-MM-DD-<topic>-review.md`，模板 [../templates/review-checklist.md](../templates/review-checklist.md)。

## 完成判据

- [ ] 两轴都有结论
- [ ] 阻断项列表明确，可以为空
- [ ] 没有为了审查去改无关模块

## 不做

直接改功能。发现问题退回 Engine 或 Query。不执行 push。

## 推荐技能

`code-review`、`differential-review`、`rust-best-practices`、`rust-concurrency-design`
