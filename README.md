# QDB

QDB 是面向股市行情的内存数据库。采集进程把 Tick 写入本机共享内存，数据库、订阅和策略进程只读；查询、K 线聚合和恢复以进程内内存表和 WAL 为准。共享内存是热数据总线，不是唯一存储。

当前是可运行的初版，覆盖设计里的 M1–M5。库只负责写入、聚合、查询和订阅；开市时间和时区偏移由业务系统在写入时给出。设计全文在 [docs/design.md](docs/design.md)。

```bash
cargo test --workspace
cargo run -p qdb-server -- data
```

健康检查默认监听 `127.0.0.1:9090`。

Agent 约定见 [AGENTS.md](AGENTS.md)，开发流水线见 [docs/2026-10-19-agent-pipeline.md](docs/2026-10-19-agent-pipeline.md)，技能索引见 [.agents/skills/README.md](.agents/skills/README.md)。

## 最低可用版本

1. 共享内存单写多读 RingBuffer
2. Tick / K 线 / 快照内存表
3. 基础 SQL 查询
4. 按 symbol / period 订阅推送
5. seq 断点续传
6. WAL + 启动恢复

PostgreSQL 完整协议、WebSocket、gRPC、历史导入、权限审计和分布式复制在设计里预留，不进入第一版实现。券商全市场还缺的能力写在 [docs/2026-10-19-enterprise-broker-md.md](docs/2026-10-19-enterprise-broker-md.md)。分布式、高可用和高并发写在 [docs/2026-10-19-distributed-ha.md](docs/2026-10-19-distributed-ha.md)，后面再做。
