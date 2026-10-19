# Query

## 阶段

开发。可与 Engine 并行。订阅和 SQL 只读，不分配 `seq`。

## 触发

任务落在 SQL、订阅、`/health`、`/metrics` 或进程入口。

## 输入

- 架构增量、设计第 5、6、10 节
- 技能 `qdb-dev`、`market-data-systems`

## 步骤

1. SQL 放在 `qdb-sql`，订阅放在 `qdb-sub`，进程入口放在 `qdb-server`。
2. 语法停在设计第 5 节。歧义的股票代码必须带交易所。
3. 同一条订阅连接按 `(seq, out_idx)` 递增。落后时先快照，再发送更大的 `seq`。
4. Tokio 只用于 TCP 和 HTTP。不在查询线程里写 WAL 或共享内存。
5. 为示例查询和 `SZSE:000001` / `1m` 订阅补测试。

## 产出

`qdb-sql`、`qdb-sub`、`qdb-server` 的代码和测试。

## 完成判据

- [ ] `cargo test -p <crate>` 通过
- [ ] 设计里的示例查询和订阅验收有对应测试
- [ ] 查询不写入共享内存

## 不做

改帧布局，实现 PostgreSQL 线协议、WebSocket、gRPC，除非任务点名。

## 推荐技能

`qdb-dev`、`rust-best-practices`、`rust-async-patterns`、`market-data-systems`、`tdd`
