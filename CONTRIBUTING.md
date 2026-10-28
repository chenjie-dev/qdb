# 参与贡献

QDB 用 Rust 实现，采用 MIT 许可证。设计正文是 [docs/design.md](docs/design.md)，使用者手册是 [docs/manual/index.md](docs/manual/index.md)。

## 改动怎么进仓库

1. 行为和设计冲突时，先改 `docs/design.md`，再改代码。
2. 使用者能看见的行为，同一变更里按 [docs/manual/index.md](docs/manual/index.md)「后续增补」更新对应章节。语法、说明、参数或输出、注意和示例都要补上。
3. 提交前运行 `cargo test --workspace`。已经实现的行为要有测试，边界见 [.agents/skills/qdb-test/SKILL.md](.agents/skills/qdb-test/SKILL.md)。
4. 提交说明用中文，格式是 `type: 描述`。`type` 取 `feat`、`fix`、`perf`、`refactor`、`chore`、`docs`、`style`。

一台机器只运行一个 `qdb` 进程。库只保存和查询行情，不解释开市时间、时区和涨跌停。

## 注释

公开接口用文档注释，块内说明用普通注释。说明用中文，标识符保持英文。细则与 [.agents/skills/qdb-dev/SKILL.md](.agents/skills/qdb-dev/SKILL.md) 的「注释」节一致。

- `//!` 放在 crate 或模块开头。公开函数的 `///` 对齐 Javadoc：摘要、行为、`# Arguments`、`# Returns`、`# Errors`。
- `//` 只解释代码看不出来的原因。`unsafe` 块前写 `// SAFETY:`，说明这段代码依赖的前提。
- 注释和手册或设计冲突时，先改手册和设计。
