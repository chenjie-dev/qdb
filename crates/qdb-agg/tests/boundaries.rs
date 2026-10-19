use qdb_abi::{Period, KLINE_INCOMPLETE};
use qdb_agg::{KlineAggregator, TradeInput};

#[test]
fn one_trade_opens_every_configured_period() {
    let mut agg = KlineAggregator::default();
    agg.apply_trade(1, &sz(shanghai(9, 30, 0, 0), 10, 1))
        .unwrap();
    for period in [
        Period::S1,
        Period::M1,
        Period::M5,
        Period::M15,
        Period::H1,
        Period::H4,
        Period::D1,
    ] {
        assert!(agg.open_bar(1, period).is_some(), "{}", period.as_str());
    }
}

#[test]
fn a_late_print_does_not_change_the_open_second() {
    let mut agg = KlineAggregator::default();
    agg.apply_trade(1, &sz(shanghai(9, 30, 0, 100_000_000), 10, 1))
        .unwrap();
    agg.apply_trade(2, &sz(shanghai(9, 29, 59, 0), 1, 1))
        .unwrap();
    assert_eq!(agg.open_bar(1, Period::S1).unwrap().body.close, 10);
    assert!(agg.late_tick_total > 0);
}

#[test]
fn gap_ack_marks_incomplete_and_skips_the_hole() {
    let mut agg = KlineAggregator::default();
    agg.apply_trade(1, &sz(shanghai(9, 30, 0, 0), 10, 1))
        .unwrap();
    agg.ack_gap(2, 5).unwrap();
    assert_eq!(agg.last_applied_seq(), 5);
    assert_ne!(
        agg.open_bar(1, Period::M1).unwrap().body.flags & KLINE_INCOMPLETE,
        0
    );
    agg.apply_trade(6, &sz(shanghai(9, 30, 1, 0), 11, 1))
        .unwrap();
    let again = agg
        .apply_trade(6, &sz(shanghai(9, 30, 1, 0), 99, 1))
        .unwrap();
    assert!(matches!(again.0, qdb_agg::ApplyStatus::Duplicate));
    assert_eq!(agg.open_bar(1, Period::S1).unwrap().body.close, 11);
}

#[test]
fn clock_does_not_seal_another_exchange() {
    let mut agg = KlineAggregator::default();
    agg.apply_trade(1, &sz(shanghai(9, 30, 0, 0), 10, 1))
        .unwrap();
    let end = shanghai(9, 31, 0, 0);
    agg.apply_clock(2, 2, end).unwrap();
    assert!(agg.sealed(1, Period::M1).is_empty());
    assert!(agg.open_bar(1, Period::M1).is_some());
}

#[test]
fn four_hour_bucket_changes_on_the_boundary() {
    let mut agg = KlineAggregator::default();
    let first = utc(0, 0, 0);
    let next = utc(4, 0, 0);
    agg.apply_trade(1, &binance(first, 1)).unwrap();
    agg.apply_trade(2, &binance(next, 2)).unwrap();
    assert_eq!(agg.sealed(1, Period::H4).len(), 1);
    assert_eq!(agg.sealed(1, Period::H4)[0].close, 1);
    assert_eq!(agg.open_bar(1, Period::H4).unwrap().body.open, 2);
}

fn sz(ts: i64, price: i64, volume: i64) -> TradeInput {
    TradeInput {
        symbol_id: 1,
        exchange_id: 1,
        event_ts_ns: ts,
        price,
        volume,
        amount: price * volume,
    }
}

fn binance(ts: i64, price: i64) -> TradeInput {
    TradeInput {
        symbol_id: 1,
        exchange_id: 2,
        event_ts_ns: ts,
        price,
        volume: 1,
        amount: price,
    }
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
