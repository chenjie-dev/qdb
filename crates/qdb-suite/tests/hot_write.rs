//! 券商行情席位在连续竞价开头的一小段。
//!
//! 时间已由柜台换成 UTC 纳秒再写入。库不识别 09:30、涨跌停或复权。
//! 逐笔委托、十档和订单簿还不在库里，本场景只写五档报价和成交。
//! 不断言某台机器的每秒笔数。

use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use qdb_abi::{Period, TickQuote};
use qdb_sql::SqlEngine;
use qdb_store::{Engine, StoreError};
use qdb_suite::Fixture;

/// 2024-01-02 09:30:00 上海（无夏令时）对应的 UTC 01:30:00。
const OPEN_NS: i64 = 1_704_159_000_000_000_000;
const LOT: i64 = 100;
const ROUNDS: i64 = 8;

#[derive(Clone, Copy)]
struct Name {
    exchange: &'static str,
    code: &'static str,
    open_px: i64,
}

#[test]
fn a_broker_desk_opening_keeps_venues_apart_and_closed_bars_intact() {
    let book = book();
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    engine.set_group_commit(256, 0);
    let started = Instant::now();
    write_opening(&mut engine);
    let next_second = OPEN_NS + 1_000_000_000;
    engine.flush().unwrap();

    assert!(started.elapsed() < Duration::from_secs(15));
    let writes = book.len() * (ROUNDS as usize) * 2 + book.len() * 2 + 1;
    assert!(engine.flush_count() < writes as u64);
    assert!(engine.flush_count() <= 4, "{}", engine.flush_count());
    assert!(
        engine
            .metrics()
            .late_tick_total
            .load(std::sync::atomic::Ordering::Acquire)
            >= 1
    );
    assert!(matches!(
        engine.resolve("000001"),
        Err(StoreError::Ambiguous)
    ));

    let sse = engine.resolve("SSE:000001").unwrap();
    let szse = engine.resolve("SZSE:000001").unwrap();
    assert_ne!(sse, szse);
    assert_eq!(engine.ticks(sse)[0].price, 99_900);
    assert_eq!(engine.ticks(szse)[0].price, 120_000);
    assert!(SqlEngine::new(&engine)
        .query("SELECT price FROM ticks WHERE symbol = '000001'")
        .is_err());
    let sz_only = SqlEngine::new(&engine)
        .query("SELECT ts, price FROM ticks WHERE symbol = 'SZSE:000001' ORDER BY ts")
        .unwrap();
    assert_eq!(sz_only.rows[0][1], "120000");
    assert!(sz_only.rows.iter().all(|row| row[1] != "99900"));

    let maotai = engine.resolve("SSE:600519").unwrap();
    let first = engine
        .klines(maotai, Period::S1)
        .into_iter()
        .find(|bar| bar.bucket_start_ns == OPEN_NS)
        .unwrap();
    assert_eq!(first.close, 1_680_0000 + (ROUNDS - 1) * 100);
    assert_eq!(first.volume, ROUNDS * LOT);
    let second = engine
        .klines(maotai, Period::S1)
        .into_iter()
        .find(|bar| bar.bucket_start_ns == next_second)
        .unwrap();
    assert_eq!(second.volume, LOT);
    assert_eq!(second.close, 1_680_0000 + ROUNDS * 100);
    assert_eq!(engine.ticks(maotai).len(), ROUNDS as usize + 2);
    assert_eq!(engine.quote(maotai).unwrap().last, 1);

    let etf = engine.resolve("SSE:510050").unwrap();
    assert_eq!(
        engine
            .klines(etf, Period::S1)
            .into_iter()
            .find(|bar| bar.bucket_start_ns == OPEN_NS)
            .unwrap()
            .volume,
        ROUNDS * LOT
    );
    let btc = engine.resolve("BTCUSDT").unwrap();
    assert_eq!(
        engine
            .klines(btc, Period::S1)
            .into_iter()
            .find(|bar| bar.bucket_start_ns == OPEN_NS)
            .unwrap()
            .close,
        42_000_0000 + (ROUNDS - 1) * 100
    );

    drop(engine);
    let reader = Engine::open_read(fix.dir()).unwrap();
    let id = reader.resolve("SSE:600519").unwrap();
    assert_eq!(
        reader
            .klines(id, Period::S1)
            .into_iter()
            .find(|bar| bar.bucket_start_ns == OPEN_NS)
            .unwrap()
            .close,
        1_680_0700
    );
}

fn write_opening(engine: &mut Engine) {
    let book = book();
    for step in 0..ROUNDS {
        let ts = OPEN_NS + step * 100_000_000;
        for name in &book {
            let price = name.open_px + step * 100;
            engine
                .write_quote(name.exchange, name.code, ts, &quote(price))
                .unwrap();
            engine
                .write_trade(name.exchange, name.code, ts + 1_000, price, LOT)
                .unwrap();
        }
    }
    for name in &book {
        let price = name.open_px + (ROUNDS - 1) * 100;
        engine
            .write_quote(
                name.exchange,
                name.code,
                OPEN_NS + 900_000_000,
                &quote(price + 100),
            )
            .unwrap();
    }
    let next_second = OPEN_NS + 1_000_000_000;
    for name in &book {
        engine
            .write_trade(
                name.exchange,
                name.code,
                next_second,
                name.open_px + ROUNDS * 100,
                LOT,
            )
            .unwrap();
    }
    engine
        .write_trade("SSE", "600519", OPEN_NS + 200_000_000, 1, LOT)
        .unwrap();
}

fn expected_ticks(code: &str) -> usize {
    if code == "600519" {
        ROUNDS as usize + 2
    } else {
        ROUNDS as usize + 1
    }
}

#[test]
fn many_desk_subscribers_catch_up_while_the_writer_keeps_going() {
    const SUBSCRIBERS: usize = 256;
    let book = book();
    let fix = Fixture::new();
    let mut writer = fix.open().unwrap();
    writer.set_group_commit(256, 1_000_000);
    let dir = fix.dir().to_path_buf();
    let published = AtomicBool::new(false);

    thread::scope(|scope| {
        for i in 0..SUBSCRIBERS {
            let name = book[i % book.len()];
            let dir = dir.clone();
            scope.spawn(move || {
                let mut reader = Engine::open_read(&dir).unwrap();
                for _ in 0..50_000 {
                    reader.poll().unwrap();
                    let key = format!("{}:{}", name.exchange, name.code);
                    let Ok(id) = reader.resolve(&key) else {
                        thread::yield_now();
                        continue;
                    };
                    if reader.ticks(id).len() >= expected_ticks(name.code) {
                        let prices: Vec<_> =
                            reader.ticks(id).iter().map(|tick| tick.price).collect();
                        if name.code == "000001" && name.exchange == "SZSE" {
                            assert!(prices.iter().all(|price| *price != 99_900));
                        }
                        return;
                    }
                    thread::yield_now();
                }
                panic!(
                    "subscriber {} did not catch {} {}",
                    i, name.exchange, name.code
                );
            });
        }
        let dir = dir.clone();
        let published = &published;
        scope.spawn(move || {
            while !published.load(Ordering::Acquire) {
                thread::yield_now();
            }
            let mut reader = Engine::open_read(&dir).unwrap();
            for _ in 0..100 {
                reader.poll().unwrap();
                if reader.resolve("SSE:600519").is_ok()
                    && reader.ticks(reader.resolve("SSE:600519").unwrap()).len()
                        >= expected_ticks("600519")
                {
                    return;
                }
                thread::yield_now();
            }
            panic!("slow subscriber missed the opening after the writer finished");
        });
        write_opening(&mut writer);
        writer.flush().unwrap();
        published.store(true, Ordering::Release);
    });

    let extra = writer
        .write_trade("SSE", "600519", OPEN_NS + 2_000_000_000, 1_680_1000, LOT)
        .unwrap();
    assert!(extra > 0);
}

fn book() -> Vec<Name> {
    vec![
        Name {
            exchange: "SSE",
            code: "600519",
            open_px: 1_680_0000,
        },
        Name {
            exchange: "SSE",
            code: "601318",
            open_px: 48_0000,
        },
        Name {
            exchange: "SSE",
            code: "600000",
            open_px: 10_0000,
        },
        Name {
            exchange: "SSE",
            code: "510050",
            open_px: 2_5000,
        },
        Name {
            exchange: "SSE",
            code: "000001",
            open_px: 99_900,
        },
        Name {
            exchange: "SZSE",
            code: "000001",
            open_px: 120_000,
        },
        Name {
            exchange: "SZSE",
            code: "000002",
            open_px: 80_000,
        },
        Name {
            exchange: "SZSE",
            code: "300750",
            open_px: 180_0000,
        },
        Name {
            exchange: "BSE",
            code: "830799",
            open_px: 15_0000,
        },
        Name {
            exchange: "HKEX",
            code: "00700",
            open_px: 320_0000,
        },
        Name {
            exchange: "BINANCE",
            code: "BTCUSDT",
            open_px: 42_000_0000,
        },
    ]
}

fn quote(last: i64) -> TickQuote {
    TickQuote {
        last,
        volume: 0,
        amount: 0,
        bid_px: [last - 100, last - 200, last - 300, last - 400, last - 500],
        bid_qty: [LOT, LOT, LOT, LOT, LOT],
        ask_px: [last + 100, last + 200, last + 300, last + 400, last + 500],
        ask_qty: [LOT, LOT, LOT, LOT, LOT],
    }
}
