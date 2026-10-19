---
name: rust-concurrency-design
description: >-
  Design Rust concurrency for a single writer, atomics, seqlock, and shared
  memory without putting locks on the hot path. Use when choosing threads,
  channels, atomics, mmap, unsafe boundaries, or when the user mentions
  并发、原子、内存屏障、共享内存、seqlock、单写多读、unsafe.
---

> **中文说明**：设计线程、原子发布和共享内存时使用。QDB 的帧布局和组提交顺序以 `docs/design.md` 为准，这里只规定设计方法。

# Rust 并发设计

完成判据：能指出唯一写入者、提交字、读者如何发现撕裂或覆盖，以及 `unsafe` 所在的 crate。缺任何一项就不开始写映射代码。

## 选择同步方式

按数据有没有全序来选，而不是按「能不能编译」来选。

| 数据 | 设计 |
|---|---|
| 必须全序的事件和由它派生的状态 | 一个所有者线程。入口用单生产者单消费者队列把解码后的定长记录交进来，序号在这个线程上分配 |
| 已经发布的计数器 | `AtomicU64`，写入 `Release`，读取 `Acquire` |
| 多字段、会被原地更新的槽 | seqlock：版本为奇数表示写入中，读者前后读到的偶数版本必须相同 |
| 很少改、读很多的查找表 | 写侧造一张新表，用 `Arc` 交换出去 |
| 冷路径控制面 | `Mutex`。行情发布循环里不拿这把锁 |

网络运行时只跑收发。编号、聚合、落盘和发布留在所有者线程，不把这块状态放进异步任务。

## 发布协议

1. 先写读者会看到的载荷。
2. 最后用 `Release` 写提交字（序号或版本）。
3. 读者 `Acquire` 读到提交字之后才读载荷，并再读一次提交字。两次不一致就丢弃这次读取。
4. 提交字独占一条缓存线，不和读者频繁写的游标放在一起。

读者被覆盖时返回明确错误。写入者不等待最慢读者。

共享内存只在同一台机器的进程之间成立。跨机器传递复制日志，不映射同一段内存。

## unsafe 的位置

- 除了映射内存和写日志的 crate，其他 crate `forbid(unsafe_code)`。
- `unsafe` 块上面写 `SAFETY`，点明指针有效、对齐、并发写入者和生命周期。
- 安全函数默认把定长记录复制出去。若返回指向映射区的引用，类型上要带着「在被覆盖前有效」的约定，不把这个引用存进别的线程。
- 用文件锁保证只有一个可写映射者。读者以只读方式打开。

## 测试

- 布局用 `size_of` 和字段偏移断言，不靠肉眼看结构体。
- 至少一个写线程加一个故意变慢的读线程，读线程必须观察到缺号或被覆盖。
- 启动时检查小端标记。大端机器拒绝创建区域。
- 原子顺序用 Acquire/Release 表达。热路径不使用 `SeqCst` 来掩盖协议没写清。
