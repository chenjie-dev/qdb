# 服务进程

`qdb` 打开一个数据目录，然后在本机提供健康检查。它不接收 SQL，也不接收写入。写入仍通过链接 `qdb-store` 的程序完成。

## 启动

```bash
cargo run -p qdb-server --bin qdb -- data
```

数据目录是第一个参数，缺省为 `data`。监听地址固定是 `127.0.0.1`。端口由环境变量 `QDB_HTTP_PORT` 决定，缺省 9090。

```bash
QDB_HTTP_PORT=19090 cargo run -p qdb-server --bin qdb -- /var/lib/qdb
```

标准错误输出一行：`qdb listening on 127.0.0.1:9090`。

进程持有打开后的 `Engine`，直到退出。退出不截断 WAL。

## HTTP

请求路径按空格分隔的第二段识别。除 `/metrics` 及其带查询串的形式外，其余路径都按健康检查处理。

### GET /health

正文是 `text/plain`。

| 状态 | 正文 | 条件 |
|---|---|---|
| 200 | `ok` | 未发生 CRC 失败，`gap_total` 为 0，且心跳为 0 或距离现在不足 2 秒 |
| 503 | `unavailable` | 上述任一条件不成立 |

`Engine::open` 会把心跳写成打开时刻。此后 2 秒内没有新的写入，`/health` 返回 503。服务进程没有后台任务去刷新心跳。心跳为 0 只出现在尚未执行打开流程的指标上，这时检查通过。

### GET /metrics

正文是 Prometheus 文本，每行一个计数：

```text
qdb_durable_seq 4
qdb_applied_seq 4
qdb_write_seq 4
qdb_gap_total 0
qdb_late_tick_total 0
qdb_subscriber_count 0
```

## 基准

`qdb-bench` 在本机测量四条路径，不连接交易所。请用 release 构建。

```bash
cargo run -p qdb-server --bin qdb-bench --release
```

输出表头：

```text
path,ops,p50_ns,p99_ns,mean_ns,ops_per_sec
```

| `path` | 测什么 |
|---|---|
| `kline_agg` | 只跑聚合器 |
| `shm_publish_read` | 发布一帧并立刻读回 |
| `engine_write_fsync` | `Engine::write_trade`，含每笔 `fsync` |
| `sql_ohlc_agg` | 对内存中的 1 分钟 K 线做 OHLC 聚合查询 |

`ops_per_sec` 由平均延迟估算。数字随机器和磁盘变化，以当次输出为准。持久化路径通常比纯内存路径慢两个数量级以上，因为每笔都同步磁盘。
