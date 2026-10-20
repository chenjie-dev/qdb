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
    assert!(matches!(
        engine.upsert_symbol("", "1").unwrap_err(),
        StoreError::EmptyExchange
    ));
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
    engine
        .write_trade("NYSE", "AAPL", utc(14, 30, 0), 180, 1)
        .unwrap();
    engine
        .write_trade("NASDAQ", "MSFT", utc(14, 30, 0), 400, 1)
        .unwrap();
    engine
        .write_trade("BINANCE", "BTCUSDT", hk, 100, 1)
        .unwrap();
    assert!(engine.resolve("HKEX:00700").is_ok());
    assert!(engine.resolve("NYSE:AAPL").is_ok());
    assert!(engine.resolve("NASDAQ:MSFT").is_ok());
    assert!(engine.resolve("BINANCE:BTCUSDT").is_ok());
    assert!(engine
        .symbols()
        .iter()
        .all(|symbol| symbol.price_scale == 10_000));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn quote_replaces_bid_and_ask_without_adding_volume_to_the_bar() {
    let dir = temp("quote");
    let mut engine = Engine::open(&dir).unwrap();
    let ts = shanghai(9, 30, 0, 0);
    engine
        .write_trade("BINANCE", "BTCUSDT", ts, 100, 2)
        .unwrap();
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
    assert_eq!(
        (quote.last, quote.bid_px[0], quote.ask_px[0]),
        (101, 99, 102)
    );
    assert_eq!(engine.klines(id, Period::M1)[0].volume, volume);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn clock_watermark_survives_deleting_shared_memory() {
    let dir = temp("clock");
    let mut engine = Engine::open(&dir).unwrap();
    let start = utc(0, 0, 10);
    engine
        .write_trade("BINANCE", "BTCUSDT", start, 100, 1)
        .unwrap();
    engine.write_clock("BINANCE", utc(0, 1, 0)).unwrap();
    let id = engine.resolve("BINANCE:BTCUSDT").unwrap();
    assert_eq!(engine.klines(id, Period::M1).len(), 1);
    assert_eq!(
        engine.klines(id, Period::M1)[0].state,
        qdb_abi::STATE_CLOSED
    );
    drop(engine);
    std::fs::remove_file(dir.join("region.bin")).unwrap();
    let restored = Engine::open(&dir).unwrap();
    let id = restored.resolve("BINANCE:BTCUSDT").unwrap();
    assert_eq!(restored.klines(id, Period::M1)[0].close, 100);
    assert_eq!(
        restored.klines(id, Period::M1)[0].state,
        qdb_abi::STATE_CLOSED
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn group_commit_fsyncs_once_for_the_batch_and_hides_it_until_flush() {
    let dir = temp("group");
    let mut engine = Engine::open(&dir).unwrap();
    engine.set_group_commit(100, 0);
    let before = engine.flush_count();
    engine
        .write_trade("SSE", "600000", utc(1, 0, 0), 10, 1)
        .unwrap();
    engine
        .write_trade("SSE", "600000", utc(1, 0, 1), 11, 1)
        .unwrap();
    assert_eq!(engine.flush_count(), before);
    engine.flush().unwrap();
    assert_eq!(engine.flush_count(), before + 1);
    drop(engine);
    let restored = Engine::open_read(&dir).unwrap();
    let id = restored.resolve("SSE:600000").unwrap();
    assert_eq!(restored.ticks(id).len(), 2);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn quote_keeps_five_levels_and_changes_start_from_the_snapshot() {
    let dir = temp("depth");
    let mut engine = Engine::open(&dir).unwrap();
    let quote = TickQuote {
        last: 11,
        volume: 0,
        amount: 0,
        bid_px: [10, 9, 8, 7, 6],
        bid_qty: [1, 2, 3, 4, 5],
        ask_px: [12, 13, 14, 15, 16],
        ask_qty: [6, 7, 8, 9, 10],
    };
    engine
        .write_quote("SSE", "600000", utc(1, 0, 0), &quote)
        .unwrap();
    let id = engine.resolve("SSE:600000").unwrap();
    let stored = engine.quote(id).unwrap();
    assert_eq!(stored.bid_px, [10, 9, 8, 7, 6]);
    assert_eq!(stored.ask_px, [12, 13, 14, 15, 16]);
    assert_eq!(stored.bid_qty[1], 2);
    let (snapshot_seq, rows) = engine.changes_since(Some("SSE:600000"), 0, 10);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].kind, "snapshot");
    assert_eq!(rows[0].bid_px[2], 8);
    let (_seq, next) = engine.changes_since(Some("SSE:600000"), snapshot_seq, 10);
    assert!(next.is_empty());
    engine
        .write_trade("SSE", "600000", utc(1, 0, 1), 12, 3)
        .unwrap();
    let (_seq, next) = engine.changes_since(Some("SSE:600000"), snapshot_seq, 10);
    assert!(next
        .iter()
        .any(|row| row.kind == "trade" && row.price == 12));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn release_clears_memory_and_reopen_reads_the_wal() {
    let dir = temp("rel");
    let mut engine = Engine::open(&dir).unwrap();
    engine
        .write_trade("SSE", "600000", utc(3, 0, 0), 18, 2)
        .unwrap();
    engine.release();
    assert!(engine.symbols().is_empty());
    assert!(engine.resolve("SSE:600000").is_err());
    drop(engine);
    let restored = Engine::open(&dir).unwrap();
    let id = restored.resolve("SSE:600000").unwrap();
    assert_eq!(restored.ticks(id)[0].price, 18);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn checkpoint_survives_a_corrupt_wal_prefix() {
    let dir = temp("ck");
    let mut engine = Engine::open(&dir).unwrap();
    engine
        .write_trade("SSE", "600000", utc(2, 0, 0), 15, 4)
        .unwrap();
    engine.checkpoint().unwrap();
    drop(engine);
    let wal = dir.join("wal/wal-1.qwal");
    let mut raw = std::fs::read(&wal).unwrap();
    raw[20] ^= 0xff;
    std::fs::write(&wal, &raw).unwrap();
    let restored = Engine::open(&dir).unwrap();
    assert!(!restored.crc_failed());
    let id = restored.resolve("SSE:600000").unwrap();
    assert_eq!(restored.ticks(id)[0].price, 15);
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
    (days * 86_400 + i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(s) - 8 * 3600)
        * 1_000_000_000
        + nanos
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
