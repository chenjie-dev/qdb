use qdb_abi::{Period, TickQuote};
use qdb_store::{Engine, StoreError};

#[test]
fn registering_the_same_symbol_twice_does_not_allocate_another_seq() {
    let dir = temp("sym");
    let mut engine = Engine::open(&dir).unwrap();
    let first = engine.upsert_symbol("SZSE", "000001").unwrap();
    let applied = engine.applied_seq();
    let second = engine.upsert_symbol("SZSE", "000001").unwrap();
    assert_eq!(first, second);
    assert_eq!(engine.applied_seq(), applied);
    assert!(matches!(engine.upsert_symbol("", "1").unwrap_err(), StoreError::EmptyExchange));
    assert!(engine.upsert_symbol("NOPE", "1").is_ok());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_unique_bare_code_resolves_and_ticks_keep_price_times_volume() {
    let dir = temp("bare");
    let mut engine = Engine::open(&dir).unwrap();
    let ts = shanghai(9, 30, 0, 0);
    engine.write_trade("SZSE", "000001", ts, 10, 100).unwrap();
    let id = engine.resolve("000001").unwrap();
    let tick = &engine.ticks(id)[0];
    assert_eq!((tick.price, tick.volume, tick.amount), (10, 100, 1000));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn four_venues_accept_symbols_and_tag_the_trade_session() {
    let dir = temp("venues");
    let mut engine = Engine::open(&dir).unwrap();
    let hk = utc(8, 5, 0);
    engine.write_trade("SSE", "600000", hk, 10, 100).unwrap();
    engine.write_trade("SZSE", "000001", hk, 10, 100).unwrap();
    engine.write_trade("BSE", "830000", hk, 10, 100).unwrap();
    engine.write_trade("HKEX", "00700", hk, 300, 100).unwrap();
    engine.write_trade("NYSE", "AAPL", utc(14, 30, 0), 180, 1).unwrap();
    engine.write_trade("NASDAQ", "MSFT", utc(14, 30, 0), 400, 1).unwrap();
    engine.write_trade("BINANCE", "BTCUSDT", hk, 100, 1).unwrap();
    assert!(engine.resolve("HKEX:00700").is_ok());
    assert!(engine.resolve("NYSE:AAPL").is_ok());
    assert!(engine.resolve("NASDAQ:MSFT").is_ok());
    assert!(engine.resolve("BINANCE:BTCUSDT").is_ok());
    assert!(engine.symbols().iter().all(|symbol| symbol.price_scale == 10_000));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn quote_replaces_bid_and_ask_without_adding_volume_to_the_bar() {
    let dir = temp("quote");
    let mut engine = Engine::open(&dir).unwrap();
    let ts = shanghai(9, 30, 0, 0);
    engine.write_trade("BINANCE", "BTCUSDT", ts, 100, 2).unwrap();
    let id = engine.resolve("BINANCE:BTCUSDT").unwrap();
    let volume = engine.klines(id, Period::M1)[0].volume;
    engine
        .write_quote(
            "BINANCE",
            "BTCUSDT",
            ts,
            &TickQuote {
                last: 101,
                volume: 2,
                amount: 0,
                bid_px: [99, 0, 0, 0, 0],
                bid_qty: [1, 0, 0, 0, 0],
                ask_px: [102, 0, 0, 0, 0],
                ask_qty: [1, 0, 0, 0, 0],
            },
        )
        .unwrap();
    let quote = engine.quote(id).unwrap();
    assert_eq!((quote.last, quote.bid_px[0], quote.ask_px[0]), (101, 99, 102));
    assert_eq!(engine.klines(id, Period::M1)[0].volume, volume);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn clock_watermark_survives_deleting_shared_memory() {
    let dir = temp("clock");
    let mut engine = Engine::open(&dir).unwrap();
    let start = utc(0, 0, 10);
    engine.write_trade("BINANCE", "BTCUSDT", start, 100, 1).unwrap();
    engine.write_clock("BINANCE", utc(0, 1, 0)).unwrap();
    let id = engine.resolve("BINANCE:BTCUSDT").unwrap();
    assert_eq!(engine.klines(id, Period::M1).len(), 1);
    assert_eq!(engine.klines(id, Period::M1)[0].state, qdb_abi::STATE_CLOSED);
    drop(engine);
    std::fs::remove_file(dir.join("region.bin")).unwrap();
    let restored = Engine::open(&dir).unwrap();
    let id = restored.resolve("BINANCE:BTCUSDT").unwrap();
    assert_eq!(restored.klines(id, Period::M1)[0].close, 100);
    assert_eq!(restored.klines(id, Period::M1)[0].state, qdb_abi::STATE_CLOSED);
    let _ = std::fs::remove_dir_all(dir);
}

fn temp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("qdb-engine-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn shanghai(h: u32, m: u32, s: u32, nanos: i64) -> i64 {
    let days = days(2024, 1, 2);
    (days * 86_400 + i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(s) - 8 * 3600) * 1_000_000_000 + nanos
}

fn utc(h: u32, m: u32, s: u32) -> i64 {
    let days = days(2024, 1, 2);
    (days * 86_400 + i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(s)) * 1_000_000_000
}

fn days(mut y: i32, m: u32, d: u32) -> i64 {
    y -= i32::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146_097 + doe as i64 - 719_468
}
