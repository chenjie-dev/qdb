# 服务进程

`qdb` 打开一个实例目录，里面可以有多个库。同一台机器只能有一个 `qdb` 进程，因为共享内存只能有一个写入实例。第二个进程会在打开库之前退出。HTTP 接收成交和报价写入，并提供快照与 K 线读取。查询参数 `db` 选择库，缺省是 `default`。一批成交或报价只 `fsync` 一次。`GET /v1/changes?symbol=SSE:600000&from_seq=0` 给多个下游读取：`from_seq=0` 返回当前五档快照和 `snapshot_seq`，之后用这个序号拉增量。它不接收 SQL。查询用 [客户端](client.md)。进程内采集写入 `default`。EquityLab Compose 设 `QDB_FEED=0`，改由外部采集进程写入。

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

收到 SIGINT 或 SIGTERM 后，进程先停止采集，把未落盘的记录写完，卸下共享内存，清掉成交、报价、K 线和订阅缓存，然后退出。WAL 文件保留，下次启动可以恢复。

进程持有打开后的 `Engine`，直到退出。退出不截断 WAL。

## HTTP

请求路径按空格分隔的第二段识别。

### GET /v1/snapshots?symbols=

`symbols` 为逗号分隔的 `交易所:代码`，例如 `SSE:600519,SZSE:000001`。缺省返回当前采集缓存。正文是 JSON：`source=qdb`，`scale=10000`，`data[]` 含名称、现价、开高低、昨收、量额和 `seq`。这里读的是进程内缓存，不执行 SQL。缓存里没有的代码不会回源。

### POST /v1/quotes

JSON：`{"items":[{"exchange":"SSE","code":"600519","ts_ns":1,"last":13250000,"price":1325.0,"open":1322.5,"high":1348.6,"low":1321.0,"pre_close":1329.0,"volume":1,"amount":1,"name":"贵州茅台"}]}`。`last` 是按 10000 倍缩放的整数。成功正文 `{"written":N}`。这次请求默认不 `fsync`，记录随组提交（256 条或 1 毫秒）落盘。`QDB_QUOTE_FLUSH=1` 时，应答前刷盘。

### POST /v1/trades

JSON：`{"items":[{"exchange":"SSE","code":"600519","ts_ns":1,"price":13250000,"volume":100}]}`。成交进入 K 线聚合。同一桶内按时间顺序写入开、高、低、收，可以把一根历史 K 线还原进库。

### POST /v1/docs

JSON：`{"items":[{"key":"binance:ticker:BTCUSDT","value":{}}]}`。值原样保存，供采集进程存放盘口、成交和列表。进程重启后清空。

### GET /v1/docs?key=

返回该 key 的 JSON 值。没有则 404。

### GET /v1/klines?symbol=SSE:600519&period=1d

`period` 取 `1s` `1m` `5m` `15m` `1h` `4h` `1d`。`state` 为 0 表示未闭合。价格已按 `scale` 还原成十进制。

可选查询参数（窗口读，少传则行为与旧版一致）：

| 参数 | 含义 |
|---|---|
| `limit` | 只返回满足条件的**最后** N 根（上限 5000） |
| `endTime` | 毫秒；只保留 `bucket_open_ms <= endTime` 的桶，用于向前翻历史 |

### GET /v1/ws?db=

把一条 `SELECT` 变成推送。查询参数 `db` 缺省 `default`。请求头需要 `Upgrade: websocket` 和 `Sec-WebSocket-Key`。握手成功后，客户端发送一条文本帧，内容是 `SELECT`，末尾的 `SUBSCRIBE` 可写可不写。

服务端先发一条 JSON：

```json
{"type":"snapshot","columns":["price"],"rows":[["10"]]}
```

之后每出现一行新的匹配数据，再发一条：

```json
{"type":"row","values":["20"]}
```

证券还不存在时先发 `{"type":"waiting"}`。语句错误发 `{"type":"error","message":"..."}` 并关闭。这是只读连接，读已经 `fsync` 的 WAL，大约每 50 毫秒看一次。写失败或客户端关闭就结束这条连接，不挡住写入线程。

### GET /v1/replicate?from=&repl_id=&db=

拉取已经 `fsync` 的 WAL。`from` 是副本已有的字节偏移，`repl_id` 是它记下的 32 位十六进制复制 ID。两者对得上就只发增量；缺 ID 或对不上就从 0 全量发送。正文是 `QDBREPL2`：8 字节魔数、1 字节类型（1 增量，2 全量）、小端 `u64` 任期、16 字节复制 ID、小端 `u64` 下一偏移，然后是原始 WAL。还没落盘的尾部不会出现。坏校验返回 503。

跟随进程设置 `QDB_UPSTREAM=http://主节点:9090`。它变成副本，停止接受本机新写入，并大约每 50 毫秒拉取。从第一次连不上开始算，超过 `QDB_FAILOVER_MS`（缺省 200，`0` 关闭）才考虑升主。探测连接最多等 100 毫秒。

大规模时用 `QDB_CLUSTER=地址=起-止,...` 把 16384 个槽分给多台主节点。键的槽号是 CRC16 的低 14 位，`{标签}` 里的文字决定槽，用来把相关键放在一起。写入的键不在本节点时返回 421，正文是 `moved`、`slot` 和 `key`。`QDB_ADVERTISE` 是本节点在这张表里的地址，缺省是监听地址加端口。`GET /v1/cluster` 返回这张表。

多副本时设置 `QDB_SENTINEL` 为其他副本地址，逗号分隔。`QDB_QUORUM` 是确认主节点失联所需的票数，含自己。缺省时没有其他副本就是 1；有其他副本时是过半。本副本的偏移还必须不小于已应答副本。偏移一样时，对外地址字典序较小的副本升主。`QDB_MIN_REPLICAS` 大于 0 时，主节点在 `QDB_REPLICA_LAG_MS`（缺省 10000）内看不到足够的副本应答就拒绝写入。

`QDB_DURABILITY=d2` 时，本机落盘后订阅者和共享内存先看不到这批记录。要等足够多的副本在 `QDB_REPLICA_LAG_MS` 内应答了不小于这批的偏移才发布。需要的副本数是 `QDB_MIN_REPLICAS`；没设时按哨兵列表算多数派，再减去主节点自己。没有其他副本时仍马上发布。缺省 `d1`，本机落盘后就发布。

升主之后向 `QDB_SENTINEL` 里的副本和旧主发送 `POST /v1/follow?upstream=http://新主:端口`。对方确认新主的任期更高，或者任期相同但自己已经被 fence，就停止写入并改从新主拉取。新的上游写在数据目录的 `upstream.url`；内容是 `-` 表示本进程已经升主，重启后不再使用 `QDB_UPSTREAM`。旧主一直连不上时会继续重试。槽位表不会跟着改。

### POST /v1/promote?db=

把本进程从副本提升为主。复制 ID 换成新的，任期加一，之后可以写入。应提升偏移最大的那台副本。

### GET /v1/info?db=

返回 `role`、`epoch`、`repl_id`、`offset`、`upstream`。`role` 为 `master` 或 `replica`。主节点的 `upstream` 为空。

### POST /v1/epoch?op=fence&epoch=&db=

`fence` 让本进程停止分配新序号。`epoch` 小于已经看见的任期时返回 409。旧主在副本提升之前要先 fence。

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
