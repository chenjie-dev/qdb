//! 采购评审里开盘逐笔之外还要看的场景：停牌只有报价、时钟只收本交易所、
//! 更多证券同时登记、写入完成后大量查询同时读、进程退出后删掉共享内存再打开。

use std::thread;

use qdb_abi::{Period, TickQuote, STATE_CLOSED, STATE_OPEN};
use qdb_sql::SqlEngine;
use qdb_store::Engine;
use qdb_suite::Fixture;

const OPEN_NS: i64 = 1_704_159_000_000_000_000;

#[test]
fn a_halted_name_updates_the_quote_and_opens_no_bar() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    engine
        .write_quote("SSE", "600519", OPEN_NS, &quote(1_680_0000))
        .unwrap();
    let id = engine.resolve("SSE:600519").unwrap();
    assert!(engine.ticks(id).is_empty());
    assert!(engine.klines(id, Period::S1).is_empty());
    assert_eq!(engine.quote(id).unwrap().last, 1_680_0000);
    assert_eq!(engine.quote(id).unwrap().bid_px[0], 1_680_0000 - 100);
}

#[test]
fn a_clock_seals_only_the_named_exchange() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    engine
        .write_trade("SSE", "600519", OPEN_NS, 1_680_0000, 100)
        .unwrap();
    engine
        .write_trade("SZSE", "000001", OPEN_NS, 120_000, 100)
        .unwrap();
    engine.write_clock("SSE", OPEN_NS + 60_000_000_000).unwrap();
    let sse = engine.resolve("SSE:600519").unwrap();
    let szse = engine.resolve("SZSE:000001").unwrap();
    let sse_bar = engine.klines(sse, Period::S1);
    assert_eq!(sse_bar.len(), 1);
    assert_eq!(sse_bar[0].state, STATE_CLOSED);
    assert_eq!(sse_bar[0].close, 1_680_0000);
    let sz_bar = engine.klines(szse, Period::S1);
    assert_eq!(sz_bar.len(), 1);
    assert_eq!(sz_bar[0].state, STATE_OPEN);
    assert_eq!(sz_bar[0].close, 120_000);
}

#[test]
fn one_hundred_twenty_eight_names_each_keep_their_own_print() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    engine.set_group_commit(256, 0);
    for i in 0..128 {
        let (exchange, code, price) = name(i);
        engine
            .write_trade(exchange, &code, OPEN_NS + i as i64 * 1_000, price, 100)
            .unwrap();
    }
    engine.flush().unwrap();
    assert!(engine.flush_count() < 128);
    for i in 0..128 {
        let (exchange, code, price) = name(i);
        let id = engine.resolve(&format!("{exchange}:{code}")).unwrap();
        assert_eq!(engine.ticks(id).len(), 1, "{exchange}:{code}");
        assert_eq!(engine.ticks(id)[0].price, price);
    }
    let listed = SqlEngine::new(&engine)
        .query("SELECT symbol FROM symbols")
        .unwrap();
    assert_eq!(listed.rows.len(), 128);
}

#[test]
fn sixty_four_queries_see_the_same_close_after_the_writer_finishes() {
    let fix = Fixture::new();
    let mut writer = fix.open().unwrap();
    writer
        .write_trade("SSE", "600519", OPEN_NS, 1_680_0000, 100)
        .unwrap();
    writer
        .write_trade("SSE", "600519", OPEN_NS + 1_000_000_000, 1_680_0700, 100)
        .unwrap();
    let dir = fix.dir().to_path_buf();
    let mut misses = Vec::new();
    thread::scope(|scope| {
        let mut joins = Vec::new();
        for _ in 0..64 {
            let dir = dir.clone();
            joins.push(scope.spawn(move || {
                let reader = Engine::open_read(&dir).unwrap();
                SqlEngine::new(&reader)
                    .query(
                        "SELECT ts, close FROM klines WHERE symbol = 'SSE:600519' AND period = '1s' ORDER BY ts",
                    )
                    .unwrap()
                    .rows
            }));
        }
        for join in joins {
            let rows = join.join().unwrap();
            if rows.last().map(|row| row[1].as_str()) != Some("16800700") {
                misses.push(rows);
            }
        }
    });
    assert!(misses.is_empty(), "{misses:?}");
}

#[test]
fn the_desk_book_reopens_after_shared_memory_is_removed() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    engine
        .write_trade("SSE", "600519", OPEN_NS, 1_680_0000, 200)
        .unwrap();
    engine
        .write_trade("SZSE", "000001", OPEN_NS, 120_000, 300)
        .unwrap();
    engine
        .write_quote("SSE", "510050", OPEN_NS, &quote(2_5000))
        .unwrap();
    drop(engine);
    std::fs::remove_file(fix.dir().join("region.bin")).unwrap();
    let engine = fix.open().unwrap();
    let maotai = engine.resolve("SSE:600519").unwrap();
    let pingan = engine.resolve("SZSE:000001").unwrap();
    let etf = engine.resolve("SSE:510050").unwrap();
    assert_eq!(engine.ticks(maotai)[0].volume, 200);
    assert_eq!(engine.ticks(pingan)[0].price, 120_000);
    assert!(engine.ticks(etf).is_empty());
    assert_eq!(engine.quote(etf).unwrap().last, 2_5000);
    assert_eq!(engine.klines(maotai, Period::S1)[0].close, 1_680_0000);
}

fn name(index: usize) -> (&'static str, String, i64) {
    if index % 2 == 0 {
        (
            "SSE",
            format!("{:06}", 600_000 + index / 2),
            100_000 + index as i64,
        )
    } else {
        (
            "SZSE",
            format!("{:06}", 1 + index / 2),
            200_000 + index as i64,
        )
    }
}

fn quote(last: i64) -> TickQuote {
    TickQuote {
        last,
        volume: 0,
        amount: 0,
        bid_px: [last - 100, 0, 0, 0, 0],
        bid_qty: [100, 0, 0, 0, 0],
        ask_px: [last + 100, 0, 0, 0, 0],
        ask_qty: [100, 0, 0, 0, 0],
    }
}
