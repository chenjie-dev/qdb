# 错误

调用方应匹配错误类型，不要依赖文案做分支。下面的英文句子是 `Display` 输出。

## 写入：`qdb_store::StoreError`

| 变体 | 文案 | 原因 |
|---|---|---|
| `Io` | `io: ...` | 建目录、读写 WAL 或映射共享内存失败 |
| `EmptyExchange` | `exchange label is empty` | 交易所标签是空字符串 |
| `ReadOnly` | `read-only connection` | `open_read` 打开的引擎拒绝写入 |
| `NotFound` | `symbol not found` | `resolve` 没有这只证券，或成交指向的证券不在表里 |
| `Ambiguous` | `ambiguous symbol` | 裸代码对应多只证券 |
| `Crc` | `wal crc failed` | 打开时 WAL 有坏 CRC，引擎拒绝继续写 |
| `Gap` | `gap in wal` | 应用时序号不连续 |
| `Agg` | 聚合器的文案 | K 线聚合返回的缺口 |

坏 CRC 之后，已重放的数据仍可查询。新的写入返回 `Crc`。

## 查询：`qdb_sql::SqlError`

| 变体 | 文案 | 原因 |
|---|---|---|
| `Syntax` | `syntax: ...` | 缺关键字、未知表、`klines` 缺 `symbol` 或 `period`、未知聚合 |
| `NotFound` | `symbol not found` | 证券不存在，或 `snapshots` 还没有报价 |
| `Ambiguous` | `ambiguous symbol` | 裸代码命中多只证券 |
| `Limit` | `limit N exceeds 100000` | `LIMIT` 大于 100000 |
| `Store` | 存储层文案 | 其他存储错误 |

未知表的文案是 `syntax: unknown table 名称`。

常见语法文案：

| 文案 | 何时出现 |
|---|---|
| `syntax: symbol required` | `klines` 或 `snapshots` 的最外层 `AND` 里没有 `symbol = ...` |
| `syntax: period required` | `klines` 没有 `period = ...` |
| `syntax: unknown column 名称` | 选择列表、`WHERE` 或 `ORDER BY` 点了不存在的列 |
| `syntax: column "名称" is ambiguous` | 连接后列名在多张表里出现，又没有表前缀 |
| `syntax: column "名称" must appear in GROUP BY` | 非聚合表达式不在分组键里 |
| `syntax: column "名称" does not exist` | 过滤或排序用了结果里没有的列 |
| `syntax: each UNION query must have the same number of columns` | `UNION` 两侧列数不同 |
| `syntax: cross join cannot have on` | `CROSS JOIN` 写了 `ON` |
| `syntax: aggregate is not supported on join` | 连接查询里写了聚合 |
| `syntax: division by zero` | 整数表达式除以 0 |
| `syntax: numeric` | 四则运算的操作数不是整数 |
| `syntax: boolean` | `WHERE` 里单独写了一个不能当布尔值的表达式 |
| `syntax: duplicate key` | `INSERT` 违反主键 |
| `syntax: table already exists` | `CREATE TABLE` 重名 |
| `syntax: type mismatch` | `INSERT` 的值对不上列类型 |
| `syntax: column count` | `INSERT` 的值个数和列数不同 |
| `syntax: primary key required` | `DELETE` 没有写全主键，或写了主键以外的条件 |
| `syntax: reserved table` | `DELETE` 的对象是 `symbols`、`ticks`、`snapshots` 或 `klines` |
| `syntax: key not found` | `DELETE` 的主键当前没有可见行 |
| `syntax: seq conflict` | `INSERT ... EXPECT` 的序号不是该主键当前可见序号 |
| `syntax: seq` | `EXPECT` 后面不是非负整数 |
| `syntax: limit` | `LIMIT` 后面不是整数，或写了两次 |

## 订阅：`qdb_sub::SubError`

| 变体 | 文案 | 原因 |
|---|---|---|
| `Symbol` | `symbol not found or ambiguous` | 证券不存在，或裸代码有歧义 |

缺口不是错误。`catch_up` 成功返回，消息列表里含 `Gap`。

## 共享内存：`qdb_shm::ShmError`

| 变体 | 文案 | 处理 |
|---|---|---|
| `BadCapacity` | `capacity must be a power of two` | 创建区域时容量不是 2 的幂 |
| `Lapped` | `reader was lapped: ...` | 放弃当前游标，从新的快照位置重新接 |
| `SeqGap` | `sequence gap: ...` | 停止把后续帧当连续数据 |
| `Torn` | `torn or overwritten frame` | 重试或重新打开 |
| `BadCrc` | `bad frame crc` | 不要使用这帧 |
| `Busy` | `snapshot version was odd or changed` | 重试 `read_snapshot` |
| `BadMagic` | `bad magic or abi` | 文件不是当前 ABI |
| `BigEndian` | `big endian host is unsupported` | 换小端主机 |
| `EpochChanged` | `epoch changed` | 区域被换代，重新打开 |

## WAL：`qdb_wal::WalError`

| 变体 | 文案 | 原因 |
|---|---|---|
| `Io` | `io: ...` | 打开或同步文件失败 |
| `Crc` | `crc failed after seq N` | 单条记录的 body 解不开 |

文件级 CRC 不匹配时，`read_path` 不返回这个错误。它返回已经读到的记录，并把 `WalRead.crc_failed` 设为 true。
