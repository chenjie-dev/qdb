# 服务进程

`qdb` 打开一个实例目录，里面可以有多个库。HTTP 接收成交和报价写入，并提供快照与 K 线读取。查询参数 `db` 选择库，缺省是 `default`。它不接收 SQL。查询用 [客户端](client.md)。进程内采集写入 `default`。EquityLab Compose 设 `QDB_FEED=0`，改由外部采集进程写入。

## 启动

```bash
cargo run -p qdb-server --bin qdb -- data
```

数据目录是第一个参数，缺省为 `data`。监听地址由 `QDB_HTTP_ADDR` 决定，缺省 `127.0.0.1`。端口由 `QDB_HTTP_PORT` 决定，缺省 9090。

采集默认开启，约每秒拉一批公开快照并 `write_quote` / 增量 `write_trade`。`QDB_FEED=0` 关闭。`QDB_FEED_CODES` 是逗号分隔的 6 位代码，缺省为内置种子。`QDB_FEED_MS` 是间隔毫秒数，缺省 1000。价格按 10000 倍写入整数。依赖本机 `curl`。

```bash
QDB_HTTP_PORT=19090 cargo run -p qdb-server --bin qdb -- /var/lib/qdb
```

标准错误输出一行：`qdb listening on 127.0.0.1:9090`。

进程持有打开后的 `Engine`，直到退出。退出不截断 WAL。

## HTTP

请求路径按空格分隔的第二段识别。

### GET /v1/snapshots?symbols=

`symbols` 为逗号分隔的 `交易所:代码`，例如 `SSE:600519,SZSE:000001`。缺省返回当前采集缓存。正文是 JSON：`source=qdb`，`scale=10000`，`data[]` 含名称、现价、开高低、昨收、量额和 `seq`。这里读的是进程内缓存，不执行 SQL。缓存里没有的代码不会回源。

### POST /v1/quotes

JSON：`{"items":[{"exchange":"SSE","code":"600519","ts_ns":1,"last":13250000,"price":1325.0,"open":1322.5,"high":1348.6,"low":1321.0,"pre_close":1329.0,"volume":1,"amount":1,"name":"贵州茅台"}]}`。`last` 是按 10000 倍缩放的整数。成功正文 `{"written":N}`。

### POST /v1/trades

JSON：`{"items":[{"exchange":"SSE","code":"600519","ts_ns":1,"price":13250000,"volume":100}]}`。成交进入 K 线聚合。同一桶内按时间顺序写入开、高、低、收，可以把一根历史 K 线还原进库。

### POST /v1/docs

JSON：`{"items":[{"key":"binance:ticker:BTCUSDT","value":{}}]}`。值原样保存，供采集进程存放盘口、成交和列表。进程重启后清空。

### GET /v1/docs?key=

返回该 key 的 JSON 值。没有则 404。

### GET /v1/klines?symbol=SSE:600519&period=1d

`period` 取 `1s` `1m` `5m` `15m` `1h` `4h` `1d`。`state` 为 0 表示未闭合。价格已按 `scale` 还原成十进制。

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
