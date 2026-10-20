# SQL

`qdb_sql::SqlEngine` 执行一条只读语句，返回列名和字符串行。没有预编译语句，没有参数绑定。标识符和关键字不区分大小写，字符串用单引号。

本版本没有 `INSERT`、`UPDATE`、`DELETE`、`JOIN`、子查询和函数嵌套。写入走 [库接口](library.md)。

## 建表

用户表使用三种类型：`INT64`、`STRING`、`BOOL`。价格和时间用 `INT64`，不用浮点。`STRING` 最长 1024 字节。一张表最多一个 `PRIMARY KEY`。`INDEX` 是不唯一的二级索引，供 `WHERE 列 = 值` 查找。

```sql
CREATE TABLE note (
  id INT64 PRIMARY KEY,
  code STRING,
  active BOOL,
  INDEX idx_code (code)
);
INSERT INTO note VALUES (1, 'SSE:600000', true);
SELECT * FROM note WHERE code = 'SSE:600000';
```

`INSERT` 的值按建表时的列顺序书写。不能建名为 `symbols`、`ticks`、`snapshots`、`klines` 的表。建表和插入会以写入者身份打开当前库。

## 目录

```sql
SHOW TABLES;
DESCRIBE klines;
DESC ticks;
```

`SHOW TABLES` 返回 `table` 和 `filter`。`filter` 是查询该表时必须写的条件。`DESCRIBE` 和 `DESC` 返回 `column` 和 `note`，列出当前查询实际能返回的列。未知表名是语法错误。

## 语法

```text
SELECT select_list
FROM table_name
[WHERE predicate [AND predicate ...]]
[ORDER BY column]
[LIMIT n]
[SUBSCRIBE]
```

`SUBSCRIBE` 只在客户端生效。它先执行去掉该关键字后的 `SELECT`，再持续打印匹配的新行。库接口里的 `SqlEngine::query` 遇到这个关键字会返回语法错误；跟随用 `split_subscribe` 和 `live_rows`。

`select_list` 是 `*`，或逗号分隔的列名，或聚合 `func(column)`。

`ORDER BY` 和 `GROUP BY` 可以被解析器跳过。结果顺序是存储顺序，不会按 `ORDER BY` 重排。聚合是否生效取决于 `SELECT` 列表里有没有聚合函数，不取决于 `GROUP BY`。

`LIMIT` 缺省 1000，最大 100000。超过上限返回 `limit N exceeds 100000`。截断发生在过滤之后。

`WHERE` 支持：

| 谓词 | 适用 | 说明 |
|---|---|---|
| `symbol = '交易所:代码'` | `snapshots`、`klines` 必须写；`ticks` 可以不写 | 也可用唯一的裸代码。`ticks` 不写时返回全部代码 |
| `period = '1m'` | `klines` | 取值见 [数据模型](data-model.md) |
| `ts BETWEEN a AND b` | `ticks`、`klines` | 闭区间，纳秒整数 |

未知的 `period` 使 `klines` 语句失败。`symbols` 忽略 `symbol` 过滤，返回全部证券。

## 表和列

### symbols

```sql
SELECT * FROM symbols
```

| 列 | 内容 |
|---|---|
| `symbol` | `交易所:代码` |
| `exchange` | 内部 `exchange_id` 的十进制文本 |

### ticks

```sql
SELECT * FROM ticks
```

不写 `symbol` 时，`*` 的列是 `symbol`、`ts`、`price`、`volume`、`seq`，包含库里全部成交，仍受 `LIMIT` 约束。写了 `symbol` 时列是 `ts`、`price`、`volume`、`seq`。指定其他列名不会改变这些列。

```sql
SELECT * FROM ticks
WHERE symbol = 'SSE:600000' AND ts BETWEEN 1704160200000000000 AND 1704160260000000000
```

### snapshots

```sql
SELECT last, bid_px1, ask_px1, seq FROM snapshots WHERE symbol = 'BINANCE:BTCUSDT'
```

`*` 展开为 `last`、`bid_px1`、`ask_px1`、`seq`。没有报价时返回 `symbol not found`。

### klines

```sql
SELECT ts, open, high, low, close, volume, amount
FROM klines
WHERE symbol = 'SSE:600000' AND period = '1m'
```

`*` 展开为 `ts`、`open`、`high`、`low`、`close`、`volume`、`amount`、`seq`。`ts` 是桶起点 `bucket_start_ns`。这张表的 `seq` 列当前总是 `0`。

缺少 `symbol` 或 `period` 时返回语法错误。

## 聚合

聚合只实现在 `klines` 上。多个聚合出现在同一 `SELECT` 列表时，结果是一行。

| 函数 | 行为 |
|---|---|
| `max(column)` | 最大值 |
| `min(column)` | 最小值 |
| `sum(column)` | 和，用 `i128` 累加 |
| `count(column)` | 过滤后的行数，不看列的值 |
| `avg(column)` | 有理数除法，最多 4 位小数，不使用浮点 |
| `first(column)` | 存储顺序的第一行 |
| `last(column)` | 存储顺序的最后一行 |

可用列：`open`、`high`、`low`、`close`、`volume`、`amount`。没有行时聚合结果是空字符串。

```sql
SELECT max(high), min(low), sum(volume), avg(close)
FROM klines
WHERE symbol = 'SSE:600000' AND period = '1m'
  AND ts BETWEEN 1704160200000000000 AND 1704246600000000000
```

`avg(close)` 在两根收盘价 10 和 11 时返回 `10.5`。

## 结果

```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}
```

数值以十进制文本返回，没有货币符号，也没有按 `price_scale` 插入小数点。

## 调用

```rust
let sql = qdb_sql::SqlEngine::new(&engine);
let result = sql.query("SELECT * FROM symbols LIMIT 10")?;
```

`SqlEngine` 只借用 `Engine`。查询期间不要同时通过这个 `&mut Engine` 写入。
