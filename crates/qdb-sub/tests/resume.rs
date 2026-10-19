use qdb_abi::Period;
use qdb_store::Engine;
use qdb_sub::{catch_up, is_ordered, SubError, SubFilter, SubMsg};

#[test]
fn unknown_and_ambiguous_symbols_fail() {
    let (dir, engine) = sample();
    assert_eq!(
        catch_up(&engine, &filter("NOTEXIST", Some(Period::M1), 0)).unwrap_err(),
        SubError::Symbol
    );
    assert_eq!(
        catch_up(&engine, &filter("000001", Some(Period::M1), 0)).unwrap_err(),
        SubError::Symbol
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_hole_between_matching_frames_is_a_gap() {
    let (dir, mut engine) = sample();
    let id = engine.resolve("BINANCE:BTCUSDT").unwrap();
    let trade_seq = engine.applied_seq();
    let start = engine.klines(id, Period::M1)[0].bucket_start_ns;
    engine.write_clock("BINANCE", start + 1_000_000).unwrap();
    engine.write_trade("BINANCE", "BTCUSDT", start + 2_000_000_000, 120, 1).unwrap();
    let resumed = catch_up(&engine, &filter("BINANCE:BTCUSDT", Some(Period::M1), trade_seq)).unwrap();
    assert!(resumed.iter().any(|msg| matches!(msg, SubMsg::Gap { .. })));
    assert!(is_ordered(&resumed));
    let _ = std::fs::remove_dir_all(dir);
}

fn filter(symbol: &str, period: Option<Period>, from_seq: u64) -> SubFilter {
    SubFilter {
        symbol: symbol.into(),
        period,
        from_seq,
    }
}

fn sample() -> (std::path::PathBuf, Engine) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("qdb-sub-more-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut engine = Engine::open(&dir).unwrap();
    let days = days_from_civil(2024, 1, 2);
    let ts = days * 86_400 * 1_000_000_000;
    engine.write_trade("BINANCE", "BTCUSDT", ts + 10_000_000_000, 100, 1).unwrap();
    engine.write_trade("SSE", "000001", ts, 1, 1).unwrap();
    engine.write_trade("SZSE", "000001", ts, 1, 1).unwrap();
    (dir, engine)
}

fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
    y -= i32::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146_097 + doe as i64 - 719_468
}
