# QDB

QDB 是面向股市行情的内存数据库。采集进程把 Tick 写入本机共享内存，数据库、订阅和策略进程只读；查询、K 线聚合和恢复以进程内内存表和 WAL 为准。共享内存是热数据总线，不是唯一存储。

当前阶段是设计，还没有实现代码。设计全文在 [docs/design.md](docs/design.md)。

Agent 约定见 [AGENTS.md](AGENTS.md)，开发流水线见 [docs/2026-10-19-agent-pipeline.md](docs/2026-10-19-agent-pipeline.md)，技能索引见 [.agents/skills/README.md](.agents/skills/README.md)。

## 最低可用版本

1. 共享内存单写多读 RingBuffer
2. Tick / K 线 / 快照内存表
3. 基础 SQL 查询
4. 按 symbol / period 订阅推送
5. seq 断点续传
6. WAL + 启动恢复

PostgreSQL 完整协议、WebSocket、gRPC、历史导入、权限审计和分布式复制在设计里预留，不进入第一版实现。
