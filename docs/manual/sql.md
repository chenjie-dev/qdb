# SQL

`qdb_sql::SqlEngine` 执行一条只读语句，返回列名和字符串行。没有预编译语句，没有参数绑定。标识符和关键字不区分大小写，字符串用单引号。

本版本没有 `UPDATE`、`DELETE`、子查询和函数嵌套。写入走 [库接口](library.md)，用户表也可以 `INSERT`。表和表用 `JOIN` 连接，规则与 PostgreSQL 的内连接和外连接相同。

## 建表

用户表使用三种类型：`INT64`、`STRING`、`BOOL`。价格和时间用 `INT64`，不用浮点。`STRING` 最长 1024 字节。`PRIMARY KEY` 可以写在多列上，这几列合起来唯一。`INDEX` 是不唯一的二级索引，供 `WHERE 列 = 值` 查找。

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

`SHOW TABLES` 返回 `table` 和 `filter`。`filter` 是查询该表时必须写的条件。`DESCRIBE` 和 `DESC` 返回 `column` 和 `note`。`note` 是 `INT64`、`STRING` 或 `BOOL`，主键列再加 `PRIMARY KEY`，有二级索引再加 `INDEX` 和索引名。行情四表和用户表用同一种字典。未知表名是语法错误。

## 语法

```text
SELECT [DISTINCT] select_list
FROM table_name [join ...]
[WHERE expr]
[GROUP BY column [, column ...]]
[HAVING expr]
[ORDER BY column [ASC | DESC] [, ...]]
[LIMIT n]
[SUBSCRIBE]
```

`SUBSCRIBE` 只在客户端生效。它先执行去掉该关键字后的 `SELECT`，再持续打印匹配的新行。库接口里的 `SqlEngine::query` 遇到这个关键字会返回语法错误；跟随用 `split_subscribe` 和 `live_rows`。

`select_list` 是 `*`，或逗号分隔的列名，或聚合 `func(column)`。

`ORDER BY` 会按列排序后再套 `LIMIT`。整列都能解析成整数时按数值比，否则按文本比。同一键的先后保持存储顺序。`GROUP BY` 每个分组一行。选择列表里不是聚合的列必须出现在分组键中。没有 `GROUP BY` 的聚合仍然是一行。`HAVING` 在分组之后过滤。`DISTINCT` 去掉重复的结果行。`SELECT 列 AS 别名` 可以给结果列改名，`ORDER BY` 和 `HAVING` 能用这个别名。

`WHERE` 在单表和连接之后都生效，支持 `AND`、`OR`、括号、`=`、`<>`、`!=`、`<`、`<=`、`>`、`>=`、`BETWEEN`、`IS NULL`、`IS NOT NULL`。`OR` 的优先级低于 `AND`。比较两边都是整数时按数值比。

`symbols`、`klines` 上的 `symbol` 和 `period` 仍要写在最外层的 `AND` 里，不能只放在 `OR` 中。`klines.seq` 仍是 `0`。

## 与 PostgreSQL 的差异

已经对齐的是：内连接和外连接、`CROSS JOIN`、`ON` 等值、`WHERE` 的与或和比较、`GROUP BY`、`HAVING`、`DISTINCT`、`ORDER BY` 再 `LIMIT`、列别名。

还没有的是：子查询、`UNION`、`WITH`、窗口函数、`CASE`、四则运算、`LIKE`、`IN`、`UPDATE`、`DELETE`、`information_schema`，以及 PostgreSQL 线协议。连接补出来的空值在结果里是空文本，`IS NULL` 能把它和真正的空字符串分开。`SUBSCRIBE` 的增量仍按到达顺序追加，不按 `ORDER BY` 重排整段输出。

`LIMIT` 缺省 1000，最大 100000。超过上限返回 `limit N exceeds 100000`。截断发生在过滤之后。

## 关联

`JOIN` 写在 `FROM` 后面、`WHERE` 前面。可以连多张表，包括 `symbols`、`ticks`、`snapshots`、`klines` 和用户表。

| 写法 | 结果 |
|---|---|
| `JOIN`、`INNER JOIN` | 只留下 `ON` 配得上的行 |
| `LEFT JOIN` | 留下左表全部行。右边没有匹配时，右边的列是空文本 |
| `RIGHT JOIN` | 留下右表全部行。左边没有匹配时，左边的列是空文本 |
| `FULL JOIN` | 两边没有匹配的行都留下，缺的一侧是空文本 |
| `CROSS JOIN` | 两表每一行相乘，不写 `ON` |

`OUTER` 可以写在 `LEFT`、`RIGHT`、`FULL` 后面，也可以不写。`ON` 只接受 `别名.列 = 别名.列`，多个条件用 `AND`。空文本和空文本相等。没有匹配而补出来的空列不再参与后面的等值，和 PostgreSQL 里 `NULL` 不匹配 `NULL` 一样。

表名后面可以写别名：`note AS n` 或 `note n`。同一条语句里别名不能重复。

多表时 `SELECT *` 的列名是 `别名.列名`。点名一列时写成 `n.id`；如果这个列名只在一张表里出现，也可以写 `id`。重名又不加前缀是语法错误。`WHERE` 在连接完成之后过滤。连接上不能写聚合。

```sql
SELECT s.code, n.id
FROM symbols s
JOIN note n ON s.code = n.code
WHERE n.active = true;
```

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
| `symbol_id` | 内部编号，主键 |
| `symbol` | `交易所:代码` |
| `exchange` | 调用方给出的来源标签 |
| `code` | 代码 |
| `exchange_id` | 标签第一次出现时分配的内部编号 |

`SELECT symbol, exchange_id` 仍能只取这两列。`*` 返回上表全部列。

### ticks

```sql
SELECT * FROM ticks
```

`*` 的列是 `seq`、`symbol_id`、`symbol`、`ts`、`price`、`volume`、`amount`。`seq` 是主键。不写 `symbol` 时包含库里全部成交，仍受 `LIMIT` 约束。可以只点名其中几列，未知列名是语法错误。

```sql
SELECT * FROM ticks
WHERE symbol = 'SSE:600000' AND ts BETWEEN 1704160200000000000 AND 1704160260000000000
```

### snapshots

```sql
SELECT last, bid_px1, ask_px1, seq FROM snapshots WHERE symbol = 'BINANCE:BTCUSDT'
```

`*` 展开为 `symbol_id`、`symbol`、`last`、五档 `bid_px` / `bid_qty` / `ask_px` / `ask_qty` 和 `seq`。`symbol_id` 是主键。没有报价时返回 `symbol not found`。可以只点名其中几列。

### klines

```sql
SELECT ts, open, high, low, close, volume, amount
FROM klines
WHERE symbol = 'SSE:600000' AND period = '1m'
```

`*` 展开为 `symbol_id`、`period_id`、`ts`、`symbol`、`period`、`open`、`high`、`low`、`close`、`volume`、`amount`、`seq`。主键是 `symbol_id`、`period_id`、`ts`。`ts` 是桶起点。`seq` 当前总是 `0`。可以只点名其中几列。

缺少 `symbol` 或 `period` 时返回语法错误。

## 聚合

聚合可以写在任意单表上，包括用户表。连接上的聚合是语法错误。没有 `GROUP BY` 时，多个聚合合成一行。

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
