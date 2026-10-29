# SQL

本章是 QDB 当前 SQL 的参考，写法对齐 PostgreSQL 的 SQL 命令参考：每条语句给出语法、说明、参数、输出、注意、示例和兼容性。只记录已经实现的用法。还没有的语句列在文末，调用它们会得到语法错误。

入口是 `qdb_sql::SqlEngine`。一次调用一条语句，返回列名和文本行。没有预编译语句，没有 `$1` 参数绑定，没有 `PREPARE`。

```rust
let sql = qdb_sql::SqlEngine::new(&engine);
let result = sql.query("SELECT close FROM klines WHERE symbol = 'SSE:600000' AND period = '1m' LIMIT 10")?;
```

`query` 只读。`CREATE TABLE`、`INSERT` 和 `DELETE` 用 `SqlEngine::execute`，需要可写引擎。

## 词法

标识符和关键字不区分大小写，存进目录时转成小写。`SSE` 和 `sse` 是同一个标识符。字符串区分大小写。

| 记号 | 写法 | 说明 |
|---|---|---|
| 标识符 | 字母、数字、`_`、`.` | `price`、`n.id`。点号留在标识符里，不单独成记号 |
| 关键字 | 见下表 | 不能靠引号把关键字当成列名 |
| 字符串 | `'文本'` | 单引号。字符串里不能再出现 `'`，没有 `''` 转义 |
| 整数 | `10`、`-3` | 十进制。没有小数、科学计数和 `0x` |
| 布尔 | `true`、`false` | 在值的位置是字面量，不是列名 |
| 空值字面量 | `null` | 在值的位置是空值。`IS NULL` 是谓词，不是这个字面量 |
| 运算符 | `+ - * / = <> != < <= > >=` | 两侧可以有空格。`<=`、`>=`、`<>`、`!=` 各是一个记号 |

没有 `--` 或 `/* */` 注释。`SqlEngine::query` 和 `execute` 会去掉末尾一个分号；语句中间的分号不是分隔符。

关键字包括：`select`、`distinct`、`from`、`where`、`group`、`by`、`having`、`order`、`asc`、`desc`、`limit`、`as`、`and`、`or`、`not`、`in`、`like`、`between`、`is`、`null`、`case`、`when`、`then`、`else`、`end`、`join`、`inner`、`left`、`right`、`full`、`cross`、`outer`、`on`、`union`、`all`、`create`、`table`、`insert`、`into`、`values`、`primary`、`key`、`index`、`show`、`tables`、`describe`、`desc`。

## 数据类型

库内只有三种存储类型。查询结果一律是文本，不返回 PostgreSQL 的二进制 `int8` 或 `numeric`。

| 类型 | 字面量 | 存储 | 比较 |
|---|---|---|---|
| `INT64` | 整数 | `i64` | 两边都能按整数解析时按数值比 |
| `STRING` | `'文本'` | 最多 1024 字节 | 文本顺序 |
| `BOOL` | `true`、`false` | 布尔 | 文本 `true` / `false` |

价格、时间和成交量用 `INT64`。不使用 `f64`。查询不会按 `price_scale` 插入小数点。`000001` 这种代码是字符串；选择它时仍返回 `000001`，不会变成 `1`。和整数比较时，若两边文本都能解析成整数，则按数值相等，所以 `code = 1` 能配上 `000001`。

空值在结果里显示成空字符串。连接补出来的空值和真正的 `''` 不是同一个值，见下文「空值」。

## 值表达式

值可以用在选择列表、`WHERE`、`HAVING`、`GROUP BY`、`CASE`、`IN` 和 `LIKE` 里。

```text
value  = add
add    = mul { ("+" | "-") mul }
mul    = unary { ("*" | "/") unary }
unary  = ["-"] primary
primary = column | literal | "(" expr ")" | case
case   = "CASE" when { when } ["ELSE" value] "END"
when   = "WHEN" expr "THEN" value
```

优先级从低到高：

| 优先级 | 运算符 | 结合 |
|---|---|---|
| 1 | `OR` | 左 |
| 2 | `AND` | 左 |
| 3 | `NOT` | 右 |
| 4 | `=` `<>` `!=` `<` `<=` `>` `>=` `BETWEEN` `IN` `LIKE` `IS NULL` | 左 |
| 5 | `+` `-` | 左 |
| 6 | `*` `/` | 左 |
| 7 | 一元 `-` | 右 |

`id + 2 * 3` 是 `id + (2 * 3)`。除法是整数除法，向零截断：`1 / 2` 得 `0`，`-3 / 2` 得 `-1`。除以 0 返回 `syntax: division by zero`。不是整数的文本做四则运算返回 `syntax: numeric`。结果超出 `i128` 返回 `syntax: overflow`。

任一操作数是空值时，四则运算的结果是空值，不报错。

`CASE` 只接受带 `WHEN` 条件的形式，不接受 `CASE 列 WHEN 值`。按书写顺序取第一个为真的分支。都不是真，有 `ELSE` 就用 `ELSE`，否则结果是空值。`ELSE` 可以省略。

```sql
SELECT CASE WHEN id > 1 THEN 'hi' ELSE 'lo' END AS band
FROM note;
```

选择列表里的表达式可以用 `AS 别名` 改结果列名。不写 `AS` 时，列名就是表达式文本，例如 `id / 2`。别名可以用在 `ORDER BY` 和 `HAVING`，不能用在 `WHERE` 和 `GROUP BY`。

## 谓词

`WHERE` 和 `HAVING` 里的条件求成三值：真、假、未知。只有真留下该行。未知和假都丢掉。

```text
expr = or
or   = and { "OR" and }
and  = not { "AND" not }
not  = ["NOT"] predicate
predicate = value [ comparison value
                  | "BETWEEN" value "AND" value
                  | ["NOT"] "IN" "(" value { "," value } ")"
                  | ["NOT"] "LIKE" value
                  | "IS" ["NOT"] "NULL" ]
          | "(" expr ")"
```

| 写法 | 为真的条件 |
|---|---|
| `=` `<>` `!=` `<` `<=` `>` `>=` | 两边都不是空值，且按上一节的比较规则成立。`!=` 与 `<>` 相同 |
| `value BETWEEN low AND high` | `value` 能解析成整数，且 `low <= value <= high`。两端都包含 |
| `value IN (a, b, ...)` | 列表里有一项与 `value` 相等。列表不能空 |
| `value NOT IN (a, b, ...)` | 列表里没有相等项，且比较里没有未知 |
| `value LIKE pattern` | 按下面的通配规则匹配 |
| `value NOT LIKE pattern` | 不匹配，且两边都不是空值 |
| `value IS NULL` | `value` 是空值 |
| `value IS NOT NULL` | `value` 不是空值 |
| `NOT predicate` | 内部为假。内部是未知时，结果仍是未知 |

单独一个布尔列可以当条件：`WHERE active` 等价于值是 `true`。单独一个整数或普通字符串返回 `syntax: boolean`。

`AND` / `OR` / `NOT`：

| `AND` | 真 | 假 | 未知 |
|---|---|---|---|
| 真 | 真 | 假 | 未知 |
| 假 | 假 | 假 | 假 |
| 未知 | 未知 | 假 | 未知 |

| `OR` | 真 | 假 | 未知 |
|---|---|---|---|
| 真 | 真 | 真 | 真 |
| 假 | 真 | 假 | 未知 |
| 未知 | 真 | 未知 | 未知 |

`NOT` 真为假，`NOT` 假为真，`NOT` 未知仍是未知。所以 `id = 1 OR id = 10 AND active = false` 是 `id = 1 OR (id = 10 AND active = false)`。

`IN` 的列表里若有空值，又没有任何一项相等，结果是未知，行被丢掉。`id IN (3, null)` 在没有 `id = 3` 时得不到行。已经相等则仍为真，后面的空值不再影响。

`LIKE` 区分大小写。`%` 匹配任意长度，包括空。`_` 匹配一个字符。`\` 转义下一个字符，因此 `\%` 匹配百分号本身。任一端是空值时结果是未知。

`symbol = '...'` 和 `period = '...'` 若写在 `WHERE` 最外层的 `AND` 上，会下推到扫描。放进 `OR` 里不会下推。`klines` 因此仍要求最外层能看到这两项，否则返回 `syntax: symbol required` 或 `syntax: period required`。`ts BETWEEN 整数 AND 整数` 同样下推，区间是闭的，单位是 UTC 纳秒。

## 空值

连接没有配上的那一侧是空值。它显示成空字符串，但 `IS NULL` 为真，`= ''` 为假。真正插入的 `''` 则 `IS NULL` 为假，`= ''` 为真。

空值与任何值比较，包括与另一个空值比较，结果都是未知。`NULL = NULL` 不为真。四则运算遇空值得到空值。`count` 不计入空值；其余聚合在组内没有非空值时返回空字符串。

## 查询的执行顺序

一条查询按这个顺序产生结果。这和 PostgreSQL 的逻辑顺序一致，别名因此不能出现在 `WHERE` 里。

1. `FROM` 和 `JOIN`。单表扫描最多 100000 行。
2. `WHERE`。
3. `GROUP BY`。没有分组且选择列表含聚合时，整段输入合成一组。
4. 计算聚合。
5. `HAVING`。
6. 计算选择列表，得到结果列名。
7. `DISTINCT`。
8. `UNION` 或 `UNION ALL`。
9. `ORDER BY`。
10. `LIMIT`。

`FIRST` 和 `LAST` 使用组内的存储顺序，不另按 `ts` 排序。

## SELECT

取出一个表或连接的行。

### 语法

```text
SELECT [DISTINCT] select_list
FROM from_item
[WHERE expr]
[GROUP BY value { "," value }]
[HAVING expr]
[ORDER BY output_name [ASC | DESC] { "," output_name [ASC | DESC] }]
[LIMIT integer]
[SUBSCRIBE]
```

`select_list` 是 `*`，或逗号分隔的值表达式、`聚合(列)`，每一项都可以写 `AS 别名`。`*` 必须单独出现，不能写成 `SELECT *, id`。

### 说明

不写 `DISTINCT` 时保留重复行。`DISTINCT` 在排序之前去掉完全相同的结果行。

`FROM` 是一张表，或后面「表连接」里的连接。表名是 `symbols`、`ticks`、`snapshots`、`klines`，或 `CREATE TABLE` 建出的名字。

`GROUP BY` 的每一项是值表达式。选择列表里每个非聚合表达式都必须按同样的写法出现在分组键里，不能用别名代替。违反时返回 `syntax: column "..." must appear in GROUP BY`。没有 `GROUP BY` 的聚合仍然只出一行；过滤后没有输入行时，这一行的聚合是空字符串。有 `GROUP BY` 且输入为空时，结果是零行。

`ORDER BY` 只能点结果列名或 `AS` 别名，不能写表达式。同名列里若整列都能解析成 `i64`，按数值排序，因此 `10` 排在 `2` 后面而不是前面。否则按文本排序。同一键保持存储里的先后。缺省 `ASC`。

`LIMIT` 缺省 1000，必须是 0 到 100000 的整数。`0` 得到零行。大于 100000 返回 `limit N exceeds 100000`。截断发生在排序之后。

`SUBSCRIBE` 不是服务器语句。客户端把它从末尾剥掉，先做上面的 `SELECT`，再跟随新行。`SqlEngine::query` 看到它返回 `syntax: SUBSCRIBE follows in the client; use split_subscribe`。

### 输出

`columns` 是结果列名，`rows` 是文本行。没有命中时 `rows` 为空，不是错误。证券不存在、`snapshots` 还没有报价，返回 `symbol not found`。

### 注意

下推之后的过滤仍在最多 100000 行的扫描结果上做。比这更大的表，`ORDER BY` 再 `LIMIT` 看不到扫描窗口以外的行。

`klines.seq` 在 SQL 里总是 `0`。这是结果列，不是共享内存帧的序号。

### 示例

```sql
SELECT price
FROM ticks
WHERE symbol = 'SSE:600000' AND price < 12
ORDER BY price DESC
LIMIT 10;
```

```sql
SELECT id / 2 AS bucket, count(id) AS n
FROM note
GROUP BY id / 2
HAVING n = 1
ORDER BY bucket;
```

### 兼容性

没有 `FETCH`、`OFFSET`、`TABLESAMPLE`、`FOR UPDATE`、窗口函数和 `DISTINCT ON`。`LIMIT` 不是 PostgreSQL 标准里的 `FETCH FIRST`，但位置和「先排序再截断」相同。

## 表连接

`JOIN` 写在 `FROM` 后、`WHERE` 前。可以连多张表，包括四张行情表和用户表。

### 语法

```text
from_item = table [alias] { join }
join      = ["INNER"] "JOIN" table [alias] "ON" eq { "AND" eq }
          | "LEFT" ["OUTER"] "JOIN" table [alias] "ON" eq { "AND" eq }
          | "RIGHT" ["OUTER"] "JOIN" table [alias] "ON" eq { "AND" eq }
          | "FULL" ["OUTER"] "JOIN" table [alias] "ON" eq { "AND" eq }
          | "CROSS" "JOIN" table [alias]
alias     = ["AS"] ident
eq        = qual "=" qual
qual      = ident "." ident
```

### 说明

| 写法 | 留下的行 |
|---|---|
| `JOIN`、`INNER JOIN` | 两边 `ON` 都配上的行 |
| `LEFT JOIN`、`LEFT OUTER JOIN` | 左表全部行。右边没有匹配时，右边的列是空值 |
| `RIGHT JOIN`、`RIGHT OUTER JOIN` | 右表全部行。左边没有匹配时，左边的列是空值 |
| `FULL JOIN`、`FULL OUTER JOIN` | 两边没有匹配的行都留下 |
| `CROSS JOIN` | 两表每一行相乘。不能写 `ON` |

`ON` 的每一项必须是 `别名.列 = 别名.列`。多项用 `AND`。不接受 `OR`、不等式和字面量。空字符串和空字符串相等。空值不和任何值相等，包括另一个空值。

结果先按左表顺序，再补上没有配上的右表行。

表名后可以写 `AS 别名` 或直接写别名。同一条语句里别名不能重复，否则 `syntax: table name "..." specified more than once`。

多表时 `SELECT *` 的列名是 `别名.列名`。`n.id` 指向该别名的列。列名只在一张表里出现时可以不写前缀。重名又不加前缀返回 `syntax: column "..." is ambiguous`。

### 注意

连接上不能写聚合，返回 `syntax: aggregate is not supported on join`。每张表最多取 100000 行再做哈希连接。`WHERE` 在连接完成之后过滤，所以 `LEFT JOIN` 后再写 `WHERE 右表.列 = ...` 会丢掉补出来的空行，效果接近内连接。

### 示例

```sql
SELECT s.code, n.id
FROM symbols AS s
JOIN note AS n ON s.symbol = n.code
WHERE n.active = true;
```

```sql
SELECT n.id
FROM note n
LEFT JOIN symbols s ON n.code = s.symbol
WHERE s.symbol IS NULL;
```

### 兼容性

没有 `NATURAL JOIN`、`USING`、`LATERAL` 和非等值 `ON`。

## UNION

把多段 `SELECT` 的行接在一起。

### 语法

```text
query = select { "UNION" ["ALL"] select }
        [ORDER BY ...] [LIMIT integer]
```

### 说明

`UNION` 去掉完全相同的结果行。`UNION ALL` 保留重复。每一段的列数必须相同，否则 `syntax: each UNION query must have the same number of columns`。结果列名用第一段。后面的段不看列名，只按位置对齐。

`A UNION ALL B UNION C` 先保留 A 和 B 的重复，再与 C 一起去重。`A UNION B UNION ALL C` 先对 A 和 B 去重，再把 C 全部接上。

`ORDER BY` 和 `LIMIT` 只能写在整段查询的最后，作用在合并之后的结果上。写在中间一段的后面是语法错误。

### 示例

```sql
SELECT id FROM note WHERE id = 2
UNION
SELECT id FROM note WHERE id = 1
ORDER BY id;
```

### 兼容性

没有 `INTERSECT`、`EXCEPT`，也不能用括号把一段 `SELECT` 单独排序。

## 聚合函数

聚合可以写在任意单表上，包括用户表。参数必须是一个列名，不能是表达式。`sum(close - open)` 和 `count(*)` 都是语法错误。多个聚合放在同一个选择列表里。

没有输入行，或列上全是空值时，结果是空字符串。`sum` 用 `i128` 累加。

### max

```text
max(column)
```

组内的最大整数。

### min

```text
min(column)
```

组内的最小整数。

### sum

```text
sum(column)
```

组内整数之和。

### count

```text
count(column)
```

组内非空值的个数。不看数值是不是 0。

### avg

```text
avg(column)
```

`sum / count` 的十进制文本，不用二进制浮点。除得尽时没有小数点。有余数时最多 4 位小数，末尾的 0 去掉。两根收盘价 10 和 11 的 `avg(close)` 是 `10.5`。

### first

```text
first(column)
```

组内存储顺序的第一行。不是按时间再排的第一行。

### last

```text
last(column)
```

组内存储顺序的最后一行。

### 示例

```sql
SELECT max(close), min(close), sum(volume), count(close), avg(close), first(open), last(close)
FROM klines
WHERE symbol = 'SSE:600000' AND period = '1s';
```

### 兼容性

没有 `count(*)`、`FILTER`、`WITHIN GROUP`、`string_agg` 和嵌套聚合。

## CREATE TABLE

建一张用户表。成功时 `columns` 是 `result`，`rows` 是 `created`。语句写入 WAL。

### 语法

```text
CREATE TABLE name (
  column type [PRIMARY KEY] { "," column type [PRIMARY KEY] }
  { "," INDEX index_name "(" column ")" }
)
```

`type` 是 `INT64`、`STRING` 或 `BOOL`。

### 说明

`PRIMARY KEY` 可以写在多列上。这几列合起来唯一，单独一列可以重复。`INDEX` 是不唯一的二级索引，供 `WHERE 列 = 值` 查找。索引只指向一列。

不能使用 `symbols`、`ticks`、`snapshots`、`klines` 这四个名字。重名返回 `syntax: table already exists`。未知类型返回 `syntax: unknown type ...`。没有列、列名重复或索引指向不存在的列，返回存储层的 schema 文案。

`STRING` 最长 1024 字节。

### 示例

```sql
CREATE TABLE note (
  id INT64 PRIMARY KEY,
  code STRING,
  active BOOL,
  INDEX idx_code (code)
);
```

### 兼容性

没有 `IF NOT EXISTS`、`REFERENCES`、`UNIQUE`、`CHECK`、`DEFAULT` 和表空间。复合主键不是单独一条 `PRIMARY KEY (a, b)` 约束，而是写在列后面。

## INSERT

插入一行或多行。不写列名时按建表顺序。写了列名时按清单顺序填值，再排回建表顺序后写入。成功时 `columns` 是 `result`，`rows` 是 `inserted`。语句写入 WAL。多行时每一行仍有自己的 `seq`。

### 语法

```text
INSERT INTO name ["(" column { "," column } ")"] VALUES tuple { "," tuple }
    ["EXPECT" integer] ["EXPIRE AT" integer]
tuple = "(" value { "," value } ")"
```

### 说明

不写列清单时，值的个数和顺序必须等于建表列，否则 `syntax: column count`。写了列清单时，每个列名恰好一次，并且必须是全部建表列，顺序不限；值的顺序跟清单一致，写入前再排回建表顺序。清单里列名重复返回 `syntax: duplicate column`。点了不存在的列返回 `syntax: unknown column 名称`。少了列，或某一行的值个数和清单不一致，返回 `syntax: column count`。这些失败都不写 WAL。没有默认值，不能省略列。

类型不对返回 `syntax: type mismatch`。主键冲突返回 `syntax: duplicate key`。负数写成 `-10`。布尔值写成 `true` 或 `false`。

`VALUES` 后面可以写多个元组。先检查全部行，再逐行提交。后一行类型不对、主键已经存在，或同一句里主键重复时，前面的行也不写入，返回 `syntax: duplicate key` 或对应的类型错误。没有主键的表则每行都追加。多个元组不能再写 `EXPECT`，否则 `syntax: expect`。

`EXPECT n` 写在值列表后面。`n` 为 0 时，这个主键必须还不存在。`n` 大于 0 时，必须等于 `SELECT` 里该行的 `seq`，然后用新行覆盖它，并得到新的 `seq`。对不上返回 `syntax: seq conflict`，不写日志。没有主键的表不能带 `EXPECT`。

`EXPIRE AT n` 也可以写在值列表后面，和 `EXPECT` 的顺序不限。多个元组时它作用在这一句的每一行上。`n` 是 UTC 纳秒。`0` 表示这行没有截止时间。非 0 时，之后的 `EXPIRE AT` 水位只要大于或等于它，就把这个主键收成墓碑。

调用方用 `CREATE TABLE` 建成的表，查询结果在列的最后多一列 `seq`。它不是 `DESCRIBE` 里的列。`SELECT *` 也会带上它。

### 示例

```sql
INSERT INTO note VALUES (1, 'SSE:600000', true);
INSERT INTO note VALUES (1, 'SSE:600000', false) EXPECT 4;
INSERT INTO note VALUES (2, 'kept', true) EXPIRE AT 1700000000000000000;
INSERT INTO note VALUES (3, 'a', true), (4, 'b', false);
INSERT INTO note (active, id, code) VALUES (true, 5, 'named');
```

### 兼容性

没有 `RETURNING` 和 `ON CONFLICT`。列清单不能只写一部分列。多个元组不能各自带不同的 `EXPECT` 或 `EXPIRE AT`。

## DELETE

按完整主键删掉调用方建成的表里当前可见的行。成功时 `columns` 是 `result`，`rows` 是 `deleted`。每个键写入自己的 WAL 记录，分配新的 `seq`。已经落盘的旧行留在日志里，查询不再返回这些键。同一个键可以再 `INSERT`。

### 语法

```text
DELETE FROM name WHERE column = value { "AND" column = value }
DELETE FROM name WHERE column IN "(" value { "," value } ")"
DELETE FROM name WHERE "(" column { "," column } ")" IN "(" row { "," row } ")"
row = "(" value { "," value } ")"
```

第二种只用于恰好一列主键的表，`column` 必须是那一列。第三种的列必须恰好是全部主键，每个列名一次，顺序不限。每个 `row` 的值个数和顺序跟括号里的列一致。

### 说明

等值形式里，每一列主键恰好出现一次，用 `AND` 连接，顺序不限。复合主键必须写全。`IN` 里的每个值或每一行按写出的顺序各成一条墓碑。列表为空、有重复键，或其中任何一个键当前不可见，整句不写，已经可见的行保持原样。重复键返回 `syntax: duplicate key`。缺键返回 `syntax: key not found`。某一行的值个数不对返回 `syntax: column count`。

没有主键、只写了部分主键、多写了别的列、使用 `OR`，或在多列主键上写 `列 IN (...)`，都返回 `syntax: primary key required`。多列主键用第三种写法一次删多个完整键。示例里的 `pair` 主键是 `a` 和 `b`。

不是 `CREATE TABLE` 建成的表返回 `syntax: reserved table`。表不存在返回 `symbol not found`。

`SELECT` 看不到墓碑之后的这些行。重新打开库之后仍然看不到，直到再次插入。

### 示例

```sql
DELETE FROM note WHERE id = 1;
DELETE FROM note WHERE id IN (2, 3);
DELETE FROM pair WHERE (b, a) IN (('y', 2), ('x', 1));
```

### 兼容性

没有 `DELETE` 不带 `WHERE`、`USING`、`RETURNING`，也不能按非主键列删除。这不是擦掉历史，而是按键追加墓碑。单列 `IN` 只接受那一列主键的字面量。多列 `IN` 只接受完整主键的行，不能混进其他列。

## EXPIRE AT

按调用方给出的 UTC 纳秒水位，把已经到期的主键收成墓碑。成功时 `columns` 是 `result`，`rows` 是 `expired`。没有命中时也返回这一行，并且不写 WAL。

### 语法

```text
EXPIRE AT integer
```

### 说明

水位可以是负数。只处理调用方建成、带主键、并且插入时写过非 0 `EXPIRE AT` 的行。截止时间小于或等于水位的行各追加一条墓碑。截止时间是 0 的行保留。没有主键的表跳过。库不读本机时钟，所以同一条语句重放结果相同。

水位不是整数时返回 `syntax: expire`。

### 示例

```sql
EXPIRE AT 1700000000000000000;
```

### 兼容性

没有按表名限制的 `EXPIRE`，也没有把期限写成一段时间。期限在 `INSERT` 里写成绝对的 UTC 纳秒。

## SHOW

列出可以查询的表。

### 语法

```text
SHOW TABLES
```

### 输出

| 列 | 内容 |
|---|---|
| `table` | 表名 |
| `filter` | 查询时建议写在最外层 `AND` 上的列。没有则为空 |

固定四行是 `symbols`（空）、`ticks`（空）、`snapshots`（`symbol`）、`klines`（`symbol, period`）。用户表接在后面，`filter` 为空。其他 `SHOW` 写法返回 `syntax: show tables`。

## DESCRIBE

列出一张表的列。`DESC` 与 `DESCRIBE` 相同。

### 语法

```text
DESCRIBE table
DESC table
```

### 输出

| 列 | 内容 |
|---|---|
| `column` | 列名 |
| `note` | `INT64`、`STRING` 或 `BOOL`。主键列再加 ` PRIMARY KEY`。该列有二级索引时再加 ` INDEX` 和索引名 |

复合主键的每一列都带 `PRIMARY KEY`，唯一性仍是这几列合起来。未知表返回 `syntax: unknown table 名称`。行情四表和用户表用同一种输出。

### 示例

```sql
DESCRIBE klines;
DESC note;
```

## 内置表

这四张表由写入路径维护，不能 `CREATE` 或 `INSERT`。列类型见 `DESCRIBE`。

### symbols

一行是一只已注册证券。没有必需过滤。写在最外层 `AND` 上的 `symbol = ...` 会被扫描剥掉，并且不再参与过滤，所以它不会把目录收成一只证券。其他谓词仍然生效。

| 列 | 内容 |
|---|---|
| `symbol_id` | 内部编号，从 1 起。主键 |
| `symbol` | `交易所:代码` |
| `exchange` | 调用方给出的来源标签 |
| `code` | 代码 |
| `exchange_id` | 标签第一次出现时分配的内部编号 |

索引 `idx_code` 在 `code` 上。

### ticks

一行是一笔成交，只追加。不写 `symbol` 时返回库里的成交，仍受 `LIMIT` 约束。

| 列 | 内容 |
|---|---|
| `seq` | 这笔成交的序号。主键 |
| `symbol_id` | 证券编号 |
| `symbol` | `交易所:代码` |
| `ts` | 事件时间，UTC 纳秒 |
| `price` | 成交价，定点整数 |
| `volume` | 成交量 |
| `amount` | `price * volume`，溢出时饱和 |

```sql
SELECT ts, price, volume
FROM ticks
WHERE symbol = 'SSE:600000'
  AND ts BETWEEN 1704160200000000000 AND 1704160260000000000
ORDER BY ts;
```

### snapshots

一行是一只证券的最新报价。必须能在最外层 `AND` 上解析出 `symbol`。还没有报价时返回 `symbol not found`，不是空结果。

| 列 | 内容 |
|---|---|
| `symbol_id` | 主键 |
| `symbol` | `交易所:代码` |
| `last` | 最新价 |
| `bid_px1` … `bid_px5` | 买价。当前写入只填第 1 档，其余为 0 |
| `bid_qty1` … `bid_qty5` | 买量 |
| `ask_px1` … `ask_px5` | 卖价 |
| `ask_qty1` … `ask_qty5` | 卖量 |
| `seq` | 产生这条报价的序号 |

```sql
SELECT last, bid_px1, ask_px1, seq
FROM snapshots
WHERE symbol = 'BINANCE:BTCUSDT';
```

### klines

一行是一根 K 线，先是已闭合的桶，再是当前未闭合的最后一根。必须同时有最外层的 `symbol` 和 `period`，否则分别是 `syntax: symbol required` 和 `syntax: period required`。`period` 取 `1s`、`1m`、`5m`、`15m`、`1h`、`4h`、`1d`。写了别的周期时，条件被剥掉且周期为空，语句同样以 `period required` 失败。

| 列 | 内容 |
|---|---|
| `symbol_id`、`period_id`、`ts` | 合起来的主键。`ts` 是桶起点 |
| `symbol` | `交易所:代码` |
| `period` | 周期名 |
| `open`、`high`、`low`、`close` | 定点整数 |
| `volume`、`amount` | 桶内成交的量和额 |
| `seq` | 恒为 `0` |

```sql
SELECT ts, open, high, low, close, volume, amount
FROM klines
WHERE symbol = 'SSE:600000' AND period = '1m'
  AND ts BETWEEN 1704160200000000000 AND 1704246600000000000
ORDER BY ts
LIMIT 500;
```

## 调用

```rust
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}
```

`SqlEngine` 只借用 `Engine`。查询期间不要用同一个 `&mut Engine` 写入。

| 函数 | 作用 |
|---|---|
| `SqlEngine::query` | `SELECT`、`SHOW`、`DESCRIBE`。不写 WAL |
| `SqlEngine::execute` | 另外接受 `CREATE TABLE`、`INSERT` 和按主键的 `DELETE`。需要可写引擎 |
| `split_subscribe` | 若语句以 `SUBSCRIBE` 结尾，返回去掉它之后的 `SELECT` |
| `captured_rows` | 当前结果，最多 100000 行，给订阅记住已经看过的行 |
| `live_rows` | 把一批新变更投影成与 `SELECT` 相同的列 |
| `follows_changes` | 这条 `SELECT` 的增量是否只由变更列表决定 |

WebSocket `GET /v1/ws` 接受同一套 `SELECT`，见 [服务进程](server.md)。命令行的 `.watch` 和 `SUBSCRIBE` 见 [客户端](client.md)。

## 与 PostgreSQL 的差异

已经按 PostgreSQL 的规则实现的是：内连接和外连接、`CROSS JOIN`、`ON` 等值、`WHERE` 的与或和非、整数比较、`BETWEEN`、`IN`、`LIKE`、搜索形式的 `CASE`、`GROUP BY`、`HAVING`、`DISTINCT`、`UNION` / `UNION ALL`、先 `ORDER BY` 再 `LIMIT`、列别名，以及空值不参与等值。

还没有，写出来就是语法错误：

| 类别 | 未实现 |
|---|---|
| 查询 | 子查询、`WITH`、`LATERAL`、`VALUES` 作为 `FROM`、`TABLE` |
| 集合 | `INTERSECT`、`EXCEPT`、带括号的分支排序 |
| 窗口 | `OVER`、`rank`、`lag`、`lead` |
| 表达式 | `CASE 列 WHEN`、`COALESCE`、`NULLIF`、`CAST`、`::`、四则以外的函数、正则 |
| 谓词 | `ILIKE`、`SIMILAR TO`、`ANY` / `ALL`、数组、`IS TRUE` |
| 写入 | `UPDATE`、`TRUNCATE`、`ALTER`、`DROP`、`COPY`。`DELETE` 只按主键追加墓碑 |
| 目录 | `information_schema`、`pg_catalog` |
| 协议 | 预编译语句、参数绑定、PostgreSQL 线协议 |

订阅的增量按到达顺序追加，不按语句里的 `ORDER BY` 重排整段输出。带表达式或 `UNION` 的订阅会重算当前结果，而不是只投影这一条变更。
