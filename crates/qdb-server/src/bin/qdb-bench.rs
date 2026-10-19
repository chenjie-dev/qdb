//! 发布构建下的性能测量。打印各路径的吞吐和延迟，不连交易所。

use std::time::Instant;

use qdb_abi::{seal_frame, FrameHeader, MsgType, Period};
use qdb_agg::{KlineAggregator, TradeInput};
use qdb_shm::{RegionConfig, RingReader, ShmRegion};
use qdb_sql::SqlEngine;
use qdb_store::Engine;

struct Summary {
    name: &'static str,
    ops: u64,
    p50_ns: u64,
    p99_ns: u64,
    mean_ns: u64,
}

fn main() {
    let mut rows = Vec::new();
    rows.push(bench_agg(200_000));
    rows.push(bench_shm(100_000));
    rows.push(bench_engine(2_000));
    rows.push(bench_sql(2_000));
    println!("path,ops,p50_ns,p99_ns,mean_ns,ops_per_sec");
    for row in &rows {
        let ops_per_sec = if row.mean_ns == 0 {
            0
        } else {
            1_000_000_000 / row.mean_ns
        };
        println!(
            "{},{},{},{},{},{}",
            row.name, row.ops, row.p50_ns, row.p99_ns, row.mean_ns, ops_per_sec
        );
    }
}

fn bench_agg(n: u64) -> Summary {
    let mut agg = KlineAggregator::default();
    let base = 1_704_153_600_000_000_000_i64;
    let mut samples = Vec::with_capacity(n as usize);
    for i in 1..=n {
        let trade = TradeInput {
            symbol_id: 1,
            exchange_id: 1,
            event_ts_ns: base + i as i64 * 1_000_000,
            price: 100 + (i % 7) as i64,
            volume: 1,
            amount: 100,
        };
        let start = Instant::now();
        agg.apply_trade(i, &trade).unwrap();
        samples.push(start.elapsed().as_nanos() as u64);
    }
    let _ = agg.open_bar(1, Period::M1);
    summary("kline_agg", samples)
}

fn bench_shm(n: u64) -> Summary {
    let dir = std::env::temp_dir().join(format!("qdb-bench-shm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("region.bin");
    let mut region = ShmRegion::create(
        &path,
        &RegionConfig {
            max_symbols: 32,
            ring_capacity: 65_536,
            reader_slots: 4,
        },
    )
    .unwrap();
    let mut reader = RingReader::open(&path).unwrap();
    let mut samples = Vec::with_capacity(n as usize);
    for seq in 1..=n {
        let frame = seal_frame(
            FrameHeader {
                seq,
                event_ts_ns: seq as i64,
                recv_ts_ns: 0,
                payload_len: 0,
                msg_type: MsgType::TickTrade as u16,
                flags: 0,
                symbol_id: 1,
                exchange_id: 1,
                period_id: 0,
                partition_id: 0,
                reserved: 0,
                crc32: 0,
            },
            &seq.to_le_bytes(),
        );
        let start = Instant::now();
        region.publish(&frame).unwrap();
        assert!(reader.next().unwrap().is_some());
        samples.push(start.elapsed().as_nanos() as u64);
    }
    let _ = std::fs::remove_dir_all(&dir);
    summary("shm_publish_read", samples)
}

fn bench_engine(n: u64) -> Summary {
    let (mut engine, _dir) = open_engine("fsync");
    let base = 1_704_153_600_000_000_000_i64;
    let mut samples = Vec::with_capacity(n as usize);
    for i in 0..n {
        let start = Instant::now();
        engine
            .write_trade("BINANCE", "BTCUSDT", base + i as i64 * 1_000_000, 100, 1)
            .unwrap();
        samples.push(start.elapsed().as_nanos() as u64);
    }
    summary("engine_fsync", samples)
}

fn bench_sql(n: u64) -> Summary {
    let (engine, _dir) = {
        let (mut engine, dir) = open_engine("sql");
        let base = 1_704_153_600_000_000_000_i64;
        for i in 0..n {
            engine
                .write_trade(
                    "BINANCE",
                    "BTCUSDT",
                    base + i as i64 * 60_000_000_000,
                    100 + (i % 5) as i64,
                    1,
                )
                .unwrap();
        }
        (engine, dir)
    };
    let sql = SqlEngine::new(&engine);
    let mut samples = Vec::with_capacity(200);
    for _ in 0..200 {
        let start = Instant::now();
        let result = sql
            .query("SELECT max(high), min(low), sum(volume), avg(close), first(open), last(close) FROM klines WHERE symbol = 'BINANCE:BTCUSDT' AND period = '1m'")
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        samples.push(start.elapsed().as_nanos() as u64);
    }
    summary("sql_ohlc_agg", samples)
}

fn open_engine(name: &str) -> (Engine, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("qdb-bench-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let engine = Engine::open(&dir).unwrap();
    (engine, dir)
}

fn summary(name: &'static str, mut samples: Vec<u64>) -> Summary {
    samples.sort_unstable();
    let ops = samples.len() as u64;
    let p50_ns = samples[samples.len() / 2];
    let p99_ns = samples[samples.len() * 99 / 100];
    let mean_ns = samples.iter().sum::<u64>() / ops;
    Summary {
        name,
        ops,
        p50_ns,
        p99_ns,
        mean_ns,
    }
}
