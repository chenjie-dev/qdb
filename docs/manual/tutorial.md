# 教程

本页用库接口写入两笔成交，再查出 1 分钟 K 线。服务进程的 HTTP 写入和 WebSocket 见 [服务进程](server.md)。

## 环境

安装 Rust 1.78 或更高版本，进入仓库根目录。

```bash
cargo test --workspace
```

## 数据目录

`Engine::open` 接受一个目录。它会创建：

```text
data/
  wal/wal-1.qwal    追加日志
  snap/             预留目录，当前恢复不读这里
  region.bin        本机共享内存文件，每次打开都会重建
```

删掉 `region.bin` 不影响已提交的数据。下次 `open` 会重放 WAL，再生成新的共享内存文件。

## 写入和查询

价格和时间都由调用方准备好再传入。价格是 `i64`，库不换算小数。时间是 UTC 纳秒，从 Unix 纪元起算。交易所代码是任意非空字符串，例如 `SSE` 或 `BINANCE`。

```rust
use qdb_sql::SqlEngine;
use qdb_store::Engine;

let dir = std::path::Path::new("data");
let mut engine = Engine::open(dir)?;

// 2024-01-02 01:50:00 UTC
let ts = 1_704_160_200_000_000_000;
engine.write_trade("SSE", "600000", ts, 1_050_000, 100)?;
engine.write_trade("SSE", "600000", ts + 30_000_000_000, 1_060_000, 50)?;

let sql = SqlEngine::new(&engine);
let result = sql.query(
    "SELECT ts, open, high, low, close, volume FROM klines \
     WHERE symbol = 'SSE:600000' AND period = '1m'",
)?;
assert_eq!(result.rows.len(), 1);
```

`1_050_000` 表示什么小数位，由业务系统约定。库原样保存，并把 `price * volume` 记成金额。

同一目录同时只能有一个写入进程。其他进程用只读方式打开 `region.bin`，见 [共享内存](shared-memory.md)。

## 看进程是否活着

```bash
cargo run -p qdb-server -- data
```

默认监听 `127.0.0.1:9090`。`GET /health` 返回 `ok`，`GET /metrics` 返回序号计数。写入、K 线和 WebSocket 订阅见 [服务进程](server.md)。SQL 也可以不经进程，直接对上面的 `Engine` 调用。

## 下一步

- 表和周期：[数据模型](data-model.md)
- 查询语法：[SQL](sql.md)
- 报价、时钟和代码注册：[库接口](library.md)
