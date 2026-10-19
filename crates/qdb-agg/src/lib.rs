//! 多周期 K 线。闭合只由下一桶成交或日志里的 ClockTick 触发。
#![forbid(unsafe_code)]

use std::collections::BTreeMap;

use qdb_abi::{
    align_bucket, exchange_tz, KlineBody, Period, KLINE_INCOMPLETE, ORIGIN_LIVE, PERIODS,
    STATE_CLOSED, STATE_OPEN,
};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AggError {
    #[error("gap at seq {seq}, last applied {last_applied}")]
    Gap { seq: u64, last_applied: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradeInput {
    pub symbol_id: u32,
    pub exchange_id: u16,
    pub event_ts_ns: i64,
    pub price: i64,
    pub volume: i64,
    pub amount: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenBar {
    pub symbol_id: u32,
    pub exchange_id: u16,
    pub period: Period,
    pub seq: u64,
    pub body: KlineBody,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BarEvent {
    Update(OpenBar),
    Seal(OpenBar),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyStatus {
    Applied,
    Duplicate,
}

pub struct KlineAggregator {
    last_applied_seq: u64,
    open: BTreeMap<(u32, u16), OpenBar>,
    sealed: BTreeMap<(u32, u16), Vec<KlineBody>>,
    pub late_tick_total: u64,
}

impl Default for KlineAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl KlineAggregator {
    pub fn new() -> Self {
        Self {
            last_applied_seq: 0,
            open: BTreeMap::new(),
            sealed: BTreeMap::new(),
            late_tick_total: 0,
        }
    }

    pub fn last_applied_seq(&self) -> u64 {
        self.last_applied_seq
    }

    pub fn open_bar(&self, symbol_id: u32, period: Period) -> Option<&OpenBar> {
        self.open.get(&(symbol_id, period.id()))
    }

    pub fn sealed(&self, symbol_id: u32, period: Period) -> &[KlineBody] {
        self.sealed
            .get(&(symbol_id, period.id()))
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn apply_trade(&mut self, seq: u64, trade: &TradeInput) -> Result<(ApplyStatus, Vec<BarEvent>), AggError> {
        if seq <= self.last_applied_seq {
            return Ok((ApplyStatus::Duplicate, Vec::new()));
        }
        if seq != self.last_applied_seq + 1 {
            return Err(AggError::Gap {
                seq,
                last_applied: self.last_applied_seq,
            });
        }
        let mut events = Vec::new();
        for period in PERIODS {
            if let Some(event) = self.apply_period(seq, trade, period) {
                events.push(event);
            }
        }
        self.last_applied_seq = seq;
        Ok((ApplyStatus::Applied, events))
    }

    pub fn apply_clock(&mut self, seq: u64, exchange_id: u16, watermark_ns: i64) -> Result<Vec<BarEvent>, AggError> {
        if seq <= self.last_applied_seq {
            return Ok(Vec::new());
        }
        if seq != self.last_applied_seq + 1 {
            return Err(AggError::Gap {
                seq,
                last_applied: self.last_applied_seq,
            });
        }
        let mut events = Vec::new();
        let keys: Vec<(u32, u16)> = self
            .open
            .iter()
            .filter(|(_, bar)| bar.exchange_id == exchange_id)
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            let bar = self.open.get(&key).unwrap();
            let end = bar.body.bucket_start_ns + Period::from_u16(key.1).unwrap().nanos();
            if watermark_ns >= end {
                events.push(self.seal(key, seq));
            }
        }
        self.last_applied_seq = seq;
        Ok(events)
    }

    pub fn ack_gap(&mut self, seq: u64, through: u64) -> Result<(), AggError> {
        if seq <= self.last_applied_seq {
            return Ok(());
        }
        if seq != self.last_applied_seq + 1 && seq > through {
            return Err(AggError::Gap {
                seq,
                last_applied: self.last_applied_seq,
            });
        }
        for bar in self.open.values_mut() {
            bar.body.flags |= KLINE_INCOMPLETE;
        }
        self.last_applied_seq = through.max(seq);
        Ok(())
    }

    pub fn note_seq(&mut self, seq: u64) -> Result<(), AggError> {
        if seq <= self.last_applied_seq {
            return Ok(());
        }
        if seq != self.last_applied_seq + 1 {
            return Err(AggError::Gap {
                seq,
                last_applied: self.last_applied_seq,
            });
        }
        self.last_applied_seq = seq;
        Ok(())
    }

    fn apply_period(&mut self, seq: u64, trade: &TradeInput, period: Period) -> Option<BarEvent> {
        let bucket = align_bucket(period, trade.event_ts_ns, exchange_tz(trade.exchange_id));
        let key = (trade.symbol_id, period.id());
        if let Some(bar) = self.open.get(&key) {
            if trade.event_ts_ns < bar.body.bucket_start_ns {
                self.late_tick_total += 1;
                return None;
            }
            if bar.body.bucket_start_ns != bucket {
                let sealed = self.seal(key, seq);
                self.insert_open(seq, trade, period, bucket);
                return Some(sealed);
            }
        }
        if self.open.contains_key(&key) {
            let bar = self.open.get_mut(&key).unwrap();
            bar.body.high = bar.body.high.max(trade.price);
            bar.body.low = bar.body.low.min(trade.price);
            bar.body.close = trade.price;
            bar.body.volume += trade.volume;
            bar.body.amount += trade.amount;
            bar.body.tick_count += 1;
            bar.seq = seq;
            Some(BarEvent::Update(bar.clone()))
        } else {
            self.insert_open(seq, trade, period, bucket);
            Some(BarEvent::Update(self.open.get(&key).unwrap().clone()))
        }
    }

    fn insert_open(&mut self, seq: u64, trade: &TradeInput, period: Period, bucket: i64) {
        let body = KlineBody {
            bucket_start_ns: bucket,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume: trade.volume,
            amount: trade.amount,
            tick_count: 1,
            state: STATE_OPEN,
            origin: ORIGIN_LIVE,
            revision: 1,
            flags: 0,
        };
        self.open.insert(
            (trade.symbol_id, period.id()),
            OpenBar {
                symbol_id: trade.symbol_id,
                exchange_id: trade.exchange_id,
                period,
                seq,
                body,
            },
        );
    }

    fn seal(&mut self, key: (u32, u16), seq: u64) -> BarEvent {
        let mut bar = self.open.remove(&key).expect("open bar");
        bar.body.state = STATE_CLOSED;
        bar.seq = seq;
        self.sealed.entry(key).or_default().push(bar.body);
        BarEvent::Seal(bar)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdb_abi::{EXCHANGE_SZSE, Period};

    fn ts(h: u32, m: u32, s: u32, nanos: i64) -> i64 {
        qdb_abi_time(2024, 1, 2, h, m, s, nanos)
    }

    fn qdb_abi_time(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32, nanos: i64) -> i64 {
        let days = days_from_civil(y, m, d);
        let local = days * 86_400 + i64::from(h) * 3600 + i64::from(min) * 60 + i64::from(s);
        (local - 8 * 3600) * 1_000_000_000 + nanos
    }

    fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
        y -= i32::from(m <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146_097 + doe as i64 - 719_468
    }

    fn trade(ts_ns: i64, price: i64, volume: i64) -> TradeInput {
        TradeInput {
            symbol_id: 1,
            exchange_id: EXCHANGE_SZSE,
            event_ts_ns: ts_ns,
            price,
            volume,
            amount: price * volume,
        }
    }

    #[test]
    fn three_prints_build_one_second_and_one_minute_bars() {
        let mut agg = KlineAggregator::default();
        agg.apply_trade(1, &trade(ts(9, 30, 0, 100_000_000), 10, 100)).unwrap();
        agg.apply_trade(2, &trade(ts(9, 30, 0, 200_000_000), 12, 50)).unwrap();
        let sec = agg.open_bar(1, Period::S1).unwrap();
        let min = agg.open_bar(1, Period::M1).unwrap();
        assert_eq!((sec.body.open, sec.body.high, sec.body.low, sec.body.close, sec.body.volume), (10, 12, 10, 12, 150));
        assert_eq!(min.body, sec.body);

        agg.apply_trade(3, &trade(ts(9, 30, 1, 0), 11, 10)).unwrap();
        let sealed = agg.sealed(1, Period::S1);
        assert_eq!(sealed.len(), 1);
        assert_eq!((sealed[0].open, sealed[0].high, sealed[0].low, sealed[0].close, sealed[0].volume), (10, 12, 10, 12, 150));
        let open_sec = agg.open_bar(1, Period::S1).unwrap();
        assert_eq!((open_sec.body.open, open_sec.body.close, open_sec.body.volume), (11, 11, 10));
        let minute = agg.open_bar(1, Period::M1).unwrap();
        assert_eq!((minute.body.open, minute.body.high, minute.body.low, minute.body.close, minute.body.volume), (10, 12, 10, 11, 160));
        assert!(agg.sealed(1, Period::M1).is_empty());

        let frozen = sealed[0];
        agg.apply_trade(3, &trade(ts(9, 30, 1, 0), 99, 1)).unwrap();
        assert_eq!(agg.sealed(1, Period::S1)[0], frozen);
        assert_eq!(agg.open_bar(1, Period::S1).unwrap().body.close, 11);
    }

    #[test]
    fn replay_matches_and_a_hole_does_not_seal() {
        let mut once = KlineAggregator::default();
        let inputs = [
            (1, trade(ts(9, 30, 0, 100_000_000), 10, 100)),
            (2, trade(ts(9, 30, 0, 200_000_000), 12, 50)),
            (3, trade(ts(9, 30, 1, 0), 11, 10)),
        ];
        for (seq, trade) in &inputs {
            once.apply_trade(*seq, trade).unwrap();
        }
        let mut twice = KlineAggregator::default();
        for (seq, trade) in &inputs {
            twice.apply_trade(*seq, trade).unwrap();
        }
        assert_eq!(once.sealed(1, Period::S1), twice.sealed(1, Period::S1));
        assert_eq!(once.open_bar(1, Period::M1), twice.open_bar(1, Period::M1));

        let mut gapped = KlineAggregator::default();
        gapped.apply_trade(1, &inputs[0].1).unwrap();
        let err = gapped.apply_trade(3, &inputs[2].1).unwrap_err();
        assert!(matches!(err, AggError::Gap { .. }));
        assert!(gapped.sealed(1, Period::S1).is_empty());
        assert_eq!(gapped.open_bar(1, Period::S1).unwrap().body.close, 10);
    }

    #[test]
    fn binance_day_rolls_at_utc_midnight_without_a_session_break() {
        let mut agg = KlineAggregator::default();
        let before = utc_ns(2024, 1, 2, 23, 59, 50);
        let after = utc_ns(2024, 1, 3, 0, 0, 10);
        agg.apply_trade(1, &binance(before, 100)).unwrap();
        agg.apply_trade(2, &binance(after, 110)).unwrap();
        assert_eq!(agg.sealed(1, Period::D1).len(), 1);
        assert_eq!(agg.sealed(1, Period::D1)[0].close, 100);
        assert_eq!(agg.open_bar(1, Period::D1).unwrap().body.open, 110);
        assert_eq!(
            agg.sealed(1, Period::D1)[0].bucket_start_ns,
            utc_ns(2024, 1, 2, 0, 0, 0)
        );
    }

    #[test]
    fn clock_tick_seals_a_quiet_minute() {
        let mut agg = KlineAggregator::default();
        let start = utc_ns(2024, 1, 2, 0, 0, 10);
        agg.apply_trade(1, &binance(start, 100)).unwrap();
        let end = utc_ns(2024, 1, 2, 0, 1, 0);
        agg.apply_clock(2, qdb_abi::EXCHANGE_BINANCE, end).unwrap();
        assert_eq!(agg.sealed(1, Period::M1).len(), 1);
        assert_eq!(agg.sealed(1, Period::M1)[0].close, 100);
        assert!(agg.open_bar(1, Period::M1).is_none());
    }

    fn binance(ts_ns: i64, price: i64) -> TradeInput {
        TradeInput {
            symbol_id: 1,
            exchange_id: qdb_abi::EXCHANGE_BINANCE,
            event_ts_ns: ts_ns,
            price,
            volume: 1,
            amount: price,
        }
    }

    fn utc_ns(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> i64 {
        let days = days_from_civil(y, m, d);
        (days * 86_400 + i64::from(h) * 3600 + i64::from(min) * 60 + i64::from(s)) * 1_000_000_000
    }
}
