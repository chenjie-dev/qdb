# 客户端

`qdb-client` 用来查看表结构、查询数据和订阅变更。它是只读进程：重放 `wal/wal-1.qwal`，不创建、不删除 `region.bin`，也不追加 WAL，因此可以在写入进程运行时打开同一目录。

普通查询每次重新打开，读到的是已经 `fsync` 的记录。`.watch` 保持打开，并接着读写入者新同步的记录。还没同步完的尾部会留到下一次读取。

## 启动

```bash
cargo run -p qdb-server --bin qdb-client -- data
```

第一个不是选项的参数是数据目录，缺省为 `data`。进入交互后提示符是 `qdb>`。

一条语句直接执行并退出：

```bash
cargo run -p qdb-server --bin qdb-client -- data -c "SHOW TABLES"
```

`-h` 打印用法。交互里输入 `help` 同样打印用法，`ll` 列出库和当前库的表。按 Tab 补全命令、库名、表名和代码。语句可以带结尾分号。

## 多个库

一个数据目录是一个实例，里面可以有多个库。每个库有自己的成交、报价、K 线和序号。

```text
qdb> CREATE DATABASE ashare
qdb> SHOW DATABASES
qdb> USE ashare
```

不写 `USE` 时打在 `default` 上。实例根目录里已经有的 `wal/` 就是这个 `default`，不会被挪走。新建的库在 `db/<库名>/`。库名以字母或下划线开头，只含字母、数字和下划线，最长 32 字节。

换库之后，查询和 `.watch` 只看当前库。服务进程写行情时用查询参数 `db` 指定库，缺省仍是 `default`。

## 转储和回放

`.dump` 把当前库已经落盘的行情写成文件。`.restore` 把这份文件写回当前库；当前库必须还没有行情。之后打开该库，成交、报价和 K 线与转储时一致。

```text
qdb> .dump market.qdump
qdb> CREATE DATABASE copy
qdb> USE copy
qdb> .restore market.qdump
```

`.replay` 按文件里的顺序把成交和报价再打一遍，不写入当前库。要把这段行情写进正在运行的库，向该库发送 `POST /v1/replay?db=库名`，正文是转储文件的字节。

## 表结构

```text
qdb> .tables
qdb> SHOW TABLES
qdb> .schema
qdb> .schema klines
qdb> DESCRIBE ticks
```

`.tables` 等于 `SHOW TABLES`。`.schema` 不带表名时列出四张表的列，带表名时等于 `DESCRIBE`。

| 表 | 查询时要写的过滤 |
|---|---|
| `symbols` | 无 |
| `ticks` | 无 |
| `snapshots` | `symbol` |
| `klines` | `symbol` 和 `period` |

列的含义以 `DESCRIBE` 的 `note` 为准，与 [SQL](sql.md) 的返回列一致。

## 数据

```text
qdb> SELECT ts, price, volume, seq FROM ticks WHERE symbol = 'SSE:600000' LIMIT 20
qdb> SELECT ts, open, high, low, close, volume FROM klines WHERE symbol = 'SSE:600000' AND period = '1m'
```

结果是对齐的文本表，最后一行是行数，例如 `(1 rows)`。错误打印到标准错误，`-c` 和脚本模式下退出码为 1。

## 订阅

`.watch` 先打印当前一行，再持续打印新变更。`Ctrl-C` 结束。`.sub` 和 `SUBSCRIBE` 是同一条命令。

```text
qdb> .watch SSE:600000
qdb> .watch snapshots SSE:600000
qdb> .watch ticks SSE:600000
qdb> .watch klines SSE:600000 1m
```

不写表名时看 `snapshots`，也就是当前最新价、买一和卖一。成交会更新最新价。

```text
snapshots	SSE:600000	seq=4	last=1050000	bid=1049000	ask=1051000
snapshots	SSE:600000	seq=5	last=1060000	bid=1049000	ask=1051000
```

| 表 | 每行有什么 |
|---|---|
| `snapshots` | `seq`、`last`、`bid`、`ask` |
| `ticks` | `seq`、`ts`、`price`、`volume` |
| `klines` | 周期、桶起点、开高低收和成交量 |

代码要和写入时的交易所标签一致，例如 `SSE:600000`。证券还没出现时先打印 `waiting`，等第一笔匹配的记录。

订阅大约每 50 毫秒看一次 WAL。它不占用共享内存写权限，也没有单独的推送端口。

## 交互

交互模式里，单行失败后仍可继续输入。从管道读入时，第一条失败语句结束进程。

`.help` 打印命令列表。`.quit` 或 `.exit` 结束交互。

## 和写入进程的关系

客户端不把 SQL 发给服务进程。查询和订阅都直接读本机 WAL。写入者正在追加时，不足一条完整记录的尾部会等到写完再读。
