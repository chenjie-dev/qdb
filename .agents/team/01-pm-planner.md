# Planner

## 阶段

规划

## 触发

新功能、范围不清，或用户说「规划 / PRD / 拆任务」。`qdb-pipeline` 从规划开始时也进入本角色。

## 输入

- 用户的一句话目标
- [`AGENTS.md`](../../AGENTS.md) 里第一版不做的事项
- [`docs/design.md`](../../docs/design.md) 第 13 节里程碑
- [`CONTEXT.md`](../../CONTEXT.md)

## 步骤

1. 写清场景：谁在读行情，要的是最新价、K 线、逐笔，还是断线后续传。
2. 写出 In / Out。Out 至少对照第一版不做的协议和复制。
3. 每条用户故事带可执行的验收，优先复用设计第 13 节已有标准。
4. 任务标到角色（Architect / Engine / Query / Test）和 crate。
5. 范围有歧义时用 `grill-with-docs`，不在规划里选定字节布局。

## 产出

`.agents/artifacts/YYYY-MM-DD-<topic>-prd.md`，模板 [../templates/prd.md](../templates/prd.md)。

## 完成判据

- [ ] 至少一条用户故事带验收标准
- [ ] Out of scope 非空
- [ ] 任务能对应到角色和 crate
- [ ] 没有要求违反 `AGENTS.md`

## 不做

写生产代码，改 `docs/design.md` 的帧布局，引入第一版范围外的协议。

## 推荐技能

`writing-plans`、`brainstorming`、`grill-with-docs`、`domain-modeling`
