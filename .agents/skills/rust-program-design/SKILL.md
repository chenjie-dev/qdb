---
name: rust-program-design
description: >-
  Design Rust crate boundaries, public APIs, newtypes, errors, and ownership
  before writing implementation. Use when adding a crate or module, shaping a
  public type or trait, choosing an error type, or when the user mentions
  程序设计、API 设计、模块划分、newtype、错误类型、所有权边界.
---

> **中文说明**：动手写实现之前，先把 crate 边界、公开类型、错误和所有权定下来。具体协议以 `docs/design.md` 为准。

# Rust 程序设计

完成判据：每个新公开类型能回答「谁拥有它、谁可以改、非法状态怎样构造不出来」。答不出就还在设计，不写函数体。

## 先定边界

1. 一个 crate 只因一种变化而改。IO、字节布局、纯计算、查询分开。
2. 依赖指向更稳定的层。字节布局 crate 不依赖 IO，计算 crate 不依赖服务器。
3. 公开面保持小。crate 根只 re-export 调用方真正要构造的类型。

热路径上的定长结构不用 serde。解析和格式化留在边界。

## 用类型表示状态

- 标识符用 newtype：`Seq`、`SymbolId`、`ExchangeId`。不要让裸 `u32` 在多个含义之间流动。
- 安全 API 里用 enum 表示互斥状态，例如未闭合和已闭合。
- `repr(C)` 载荷里用显式整数判别式。在进入安全 API 的那一层转换成 enum，不把整数判别式传进聚合逻辑。
- 价格和金额在安全 API 里是带 scale 的定点 newtype，底层仍是 `i64`。聚合和比较不经过 `f64`。
- 布尔参数超过一个时，改成 enum 或独立类型。调用处要能读出含义。

同一概念只留一个名字。安全层叫 `SymbolId` 时，不要同时出现 `code: u32` 和 `sid: i64`。

## 公开函数

- 输入借用，结构体拥有。热路径参数用 `&[u8]`、`&TickTrade`，不把 `String` 传进循环。
- trait 只放调用方会替换的那一个行为。例如对齐交易日是一个方法，不是一个十方法的行情 trait。
- 热路径用静态分发。`dyn` 只放在进程边界或真正要运行时更换实现的地方。
- 有第二个实现之前，不抽取 trait。第一个实现直接是具体类型。

## 错误

- 库路径返回 `Result`。`unwrap` / `expect` 只出现在测试。
- 库错误用 `thiserror` 的 enum，按调用方能采取的动作分类：缺号、被覆盖、符号歧义、保留窗口之外、CRC 损坏。
- 二进制入口可以用 `anyhow` 聚合启动失败。库之间不传递 `anyhow::Error`。
- 缺号、覆盖、半写读失败是返回值，不只打日志。

## 所有权

- 有全序的状态只有一个所有者线程。其他线程拿到的是发布之后的副本、`Arc` 交换出去的不可变表，或只读映射。
- 跨线程移交所有权用单消费者队列，在消费者线程上分配序号。
- 小的定长记录按值复制。需要共享且不再改的表用 `Arc` 整体替换。

## 和本仓其他技能

字节布局、原子量和 `unsafe` 边界用 `rust-concurrency-design`。写法细节用 `rust-best-practices`。QDB 的 crate 名单和提交顺序以 `docs/design.md` 与 `AGENTS.md` 为准。
