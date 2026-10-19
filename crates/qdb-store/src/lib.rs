//! 内存表、WAL 应用和启动恢复。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use qdb_abi::{
    code_str, decode_trade, encode_kline, encode_trade, exchange, fixed_code, seal_frame, Frame,
    FrameHeader, KlineBody, MsgType, Period, TickQuote, TickTrade,
};
use qdb_agg::{BarEvent, KlineAggregator, TradeInput};
use qdb_shm::{RegionConfig, ShmRegion};
use qdb_wal::{WalAppender, WalRecord};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(String),
    #[error("unknown exchange {0}")]
    UnknownExchange(String),
    #[error("symbol not found")]
    NotFound,
    #[error("ambiguous symbol")]
    Ambiguous,
    #[error("wal crc failed")]
    Crc,
    #[error("gap in wal")]
    Gap,
    #[error(transparent)]
    Agg(#[from] qdb_agg::AggError),
}

#[derive(Clone, Debug)]
pub struct Symbol {
    pub id: u32,
    pub exchange_id: u16,
    pub code: String,
    pub price_scale: u16,
}

#[derive(Clone, Debug)]
pub struct TickRow {
    pub seq: u64,
    pub ts: i64,
    pub price: i64,
    pub volume: i64,
    pub amount: i64,
}

#[derive(Clone, Debug)]
pub struct QuoteRow {
    pub seq: u64,
    pub last: i64,
    pub bid_px: [i64; 5],
    pub ask_px: [i64; 5],
}

#[derive(Clone, Debug)]
pub struct Metrics {
    pub durable_seq: Arc<AtomicU64>,
    pub applied_seq: Arc<AtomicU64>,
    pub write_seq: Arc<AtomicU64>,
    pub gap_total: Arc<AtomicU64>,
    pub late_tick_total: Arc<AtomicU64>,
    pub crc_failed: Arc<AtomicBool>,
    pub heartbeat_ns: Arc<AtomicU64>,
    pub subscriber_count: Arc<AtomicU64>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            durable_seq: Arc::new(AtomicU64::new(0)),
            applied_seq: Arc::new(AtomicU64::new(0)),
            write_seq: Arc::new(AtomicU64::new(0)),
            gap_total: Arc::new(AtomicU64::new(0)),
            late_tick_total: Arc::new(AtomicU64::new(0)),
            crc_failed: Arc::new(AtomicBool::new(false)),
            heartbeat_ns: Arc::new(AtomicU64::new(0)),
            subscriber_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

pub struct Engine {
    root: PathBuf,
    wal: WalAppender,
    agg: KlineAggregator,
    symbols: Vec<Symbol>,
    by_key: HashMap<(u16, String), u32>,
    ticks: HashMap<u32, Vec<TickRow>>,
    quotes: HashMap<u32, QuoteRow>,
    frames: Vec<Frame>,
    shm: Option<ShmRegion>,
    next_seq: u64,
    metrics: Metrics,
    crc_failed: bool,
}

impl Engine {
    pub fn open(root: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(root.join("wal")).map_err(io)?;
        fs::create_dir_all(root.join("snap")).map_err(io)?;
        let wal = WalAppender::open(&root.join("wal/wal-1.qwal")).map_err(io)?;
        let loaded = wal.read().map_err(io)?;
        let mut engine = Self {
            root: root.to_path_buf(),
            wal,
            agg: KlineAggregator::default(),
            symbols: Vec::new(),
            by_key: HashMap::new(),
            ticks: HashMap::new(),
            quotes: HashMap::new(),
            frames: Vec::new(),
            shm: None,
            next_seq: 1,
            metrics: Metrics::new(),
            crc_failed: loaded.crc_failed,
        };
        for record in &loaded.records {
            engine.apply_record(record)?;
        }
        if loaded.crc_failed {
            engine.crc_failed = true;
            engine.metrics.crc_failed.store(true, Ordering::Release);
        }
        engine.next_seq = engine.agg.last_applied_seq() + 1;
        engine.attach_shm()?;
        engine.sync_metrics();
        Ok(engine)
    }

    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    pub fn crc_failed(&self) -> bool {
        self.crc_failed
    }

    pub fn applied_seq(&self) -> u64 {
        self.agg.last_applied_seq()
    }

    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    pub fn ticks(&self, symbol_id: u32) -> &[TickRow] {
        self.ticks.get(&symbol_id).map(Vec::as_slice).unwrap_or(&[])
    }

    pub fn quote(&self, symbol_id: u32) -> Option<&QuoteRow> {
        self.quotes.get(&symbol_id)
    }

    pub fn klines(&self, symbol_id: u32, period: Period) -> Vec<KlineBody> {
        let mut rows = self.agg.sealed(symbol_id, period).to_vec();
        if let Some(open) = self.agg.open_bar(symbol_id, period) {
            rows.push(open.body);
        }
        rows
    }

    pub fn resolve(&self, text: &str) -> Result<u32, StoreError> {
        if let Some((ex, code)) = text.split_once(':') {
            let (exchange_id, _) = exchange(ex).ok_or_else(|| StoreError::UnknownExchange(ex.to_string()))?;
            return self.by_key.get(&(exchange_id, code.to_string())).copied().ok_or(StoreError::NotFound);
        }
        let hits: Vec<u32> = self.symbols.iter().filter(|symbol| symbol.code == text).map(|symbol| symbol.id).collect();
        match hits.as_slice() {
            [id] => Ok(*id),
            [] => Err(StoreError::NotFound),
            _ => Err(StoreError::Ambiguous),
        }
    }

    pub fn upsert_symbol(&mut self, exchange_code: &str, code: &str) -> Result<u32, StoreError> {
        let (exchange_id, _) = exchange(exchange_code).ok_or_else(|| StoreError::UnknownExchange(exchange_code.to_string()))?;
        if let Some(id) = self.by_key.get(&(exchange_id, code.to_string())) {
            return Ok(*id);
        }
        self.ensure_writable()?;
        let id = self.symbols.len() as u32 + 1;
        let mut payload = Vec::new();
        payload.extend_from_slice(&id.to_le_bytes());
        payload.extend_from_slice(&exchange_id.to_le_bytes());
        payload.extend_from_slice(&fixed_code(code));
        self.commit(MsgType::SymbolUpsert, 0, id, exchange_id, 0, &payload)?;
        Ok(id)
    }

    pub fn write_trade(
        &mut self,
        exchange_code: &str,
        code: &str,
        event_ts_ns: i64,
        price: i64,
        volume: i64,
    ) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        let symbol_id = self.upsert_symbol(exchange_code, code)?;
        let exchange_id = self.symbols.iter().find(|symbol| symbol.id == symbol_id).unwrap().exchange_id;
        let mut payload = Vec::new();
        payload.extend_from_slice(&symbol_id.to_le_bytes());
        payload.extend_from_slice(&event_ts_ns.to_le_bytes());
        payload.extend_from_slice(&encode_trade(&TickTrade {
            price,
            volume,
            amount: price.saturating_mul(volume),
            trade_id: 0,
        }));
        self.commit(MsgType::TickTrade, event_ts_ns, symbol_id, exchange_id, 0, &payload)
    }

    pub fn write_quote(
        &mut self,
        exchange_code: &str,
        code: &str,
        event_ts_ns: i64,
        quote: &TickQuote,
    ) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        let symbol_id = self.upsert_symbol(exchange_code, code)?;
        let exchange_id = self.symbols.iter().find(|symbol| symbol.id == symbol_id).unwrap().exchange_id;
        let mut payload = Vec::new();
        payload.extend_from_slice(&symbol_id.to_le_bytes());
        payload.extend_from_slice(&quote.last.to_le_bytes());
        payload.extend_from_slice(&quote.bid_px[0].to_le_bytes());
        payload.extend_from_slice(&quote.ask_px[0].to_le_bytes());
        self.commit(MsgType::TickQuote, event_ts_ns, symbol_id, exchange_id, 0, &payload)
    }

    /// 连续交易的时间水位。水位越过桶结束时间就闭合 K 线，不需要等待下一笔成交。
    pub fn write_clock(&mut self, exchange_code: &str, watermark_ns: i64) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        let (exchange_id, _) = exchange(exchange_code).ok_or_else(|| StoreError::UnknownExchange(exchange_code.to_string()))?;
        let mut payload = Vec::new();
        payload.extend_from_slice(&exchange_id.to_le_bytes());
        payload.extend_from_slice(&watermark_ns.to_le_bytes());
        self.commit(MsgType::ClockTick, watermark_ns, 0, exchange_id, 0, &payload)
    }

    fn commit(
        &mut self,
        msg_type: MsgType,
        event_ts_ns: i64,
        symbol_id: u32,
        exchange_id: u16,
        period_id: u16,
        payload: &[u8],
    ) -> Result<u64, StoreError> {
        let seq = self.next_seq;
        let record = WalRecord {
            seq,
            msg_type: msg_type as u16,
            flags: 0,
            payload: payload.to_vec(),
        };
        self.wal.append(&record).map_err(io)?;
        self.wal.flush().map_err(io)?;
        if let Some(shm) = self.shm.as_mut() {
            shm.set_durable(seq);
        }
        let events = self.apply_record(&record)?;
        let frame = seal_frame(
            FrameHeader {
                seq,
                event_ts_ns,
                recv_ts_ns: event_ts_ns,
                payload_len: 0,
                msg_type: msg_type as u16,
                flags: 0,
                symbol_id,
                exchange_id,
                period_id,
                partition_id: 0,
                reserved: 0,
                crc32: 0,
            },
            payload,
        );
        self.frames.push(frame.clone());
        for (index, event) in events.iter().enumerate() {
            self.frames.push(bar_frame(seq, event_ts_ns, event, index as u16 + 1));
        }
        let applied = self.applied_seq();
        if let Some(shm) = self.shm.as_mut() {
            shm.set_applied(applied);
            shm.publish(&frame).map_err(io)?;
            shm.set_heartbeat(now_ns());
        }
        self.next_seq = self.applied_seq() + 1;
        self.sync_metrics();
        Ok(seq)
    }

    fn apply_record(&mut self, record: &WalRecord) -> Result<Vec<BarEvent>, StoreError> {
        match MsgType::from_u16(record.msg_type) {
            Some(MsgType::SymbolUpsert) => {
                let id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let exchange_id = u16::from_le_bytes(record.payload[4..6].try_into().unwrap());
                let code = code_str(&record.payload[6..38]);
                if !self.by_key.contains_key(&(exchange_id, code.clone())) {
                    self.by_key.insert((exchange_id, code.clone()), id);
                    self.symbols.push(Symbol {
                        id,
                        exchange_id,
                        code,
                        price_scale: 10_000,
                    });
                }
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::TickTrade) => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let event_ts_ns = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let trade = decode_trade(&record.payload[12..]).ok_or(StoreError::Crc)?;
                let exchange_id = self.symbols.iter().find(|symbol| symbol.id == symbol_id).ok_or(StoreError::NotFound)?.exchange_id;
                let (_status, events) = self.agg.apply_trade(
                    record.seq,
                    &TradeInput {
                        symbol_id,
                        exchange_id,
                        event_ts_ns,
                        price: trade.price,
                        volume: trade.volume,
                        amount: trade.amount,
                    },
                )?;
                self.ticks.entry(symbol_id).or_default().push(TickRow {
                    seq: record.seq,
                    ts: event_ts_ns,
                    price: trade.price,
                    volume: trade.volume,
                    amount: trade.amount,
                });
                let quote = self.quotes.entry(symbol_id).or_insert(QuoteRow {
                    seq: record.seq,
                    last: trade.price,
                    bid_px: [0; 5],
                    ask_px: [0; 5],
                });
                quote.seq = record.seq;
                quote.last = trade.price;
                Ok(events)
            }
            Some(MsgType::TickQuote) => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let last = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let bid = i64::from_le_bytes(record.payload[12..20].try_into().unwrap());
                let ask = i64::from_le_bytes(record.payload[20..28].try_into().unwrap());
                let mut bid_px = [0; 5];
                let mut ask_px = [0; 5];
                bid_px[0] = bid;
                ask_px[0] = ask;
                self.quotes.insert(symbol_id, QuoteRow { seq: record.seq, last, bid_px, ask_px });
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::ClockTick) => {
                let exchange_id = u16::from_le_bytes(record.payload[0..2].try_into().unwrap());
                let watermark = i64::from_le_bytes(record.payload[2..10].try_into().unwrap());
                Ok(self.agg.apply_clock(record.seq, exchange_id, watermark)?)
            }
            _ => {
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
        }
    }

    fn attach_shm(&mut self) -> Result<(), StoreError> {
        let path = self.root.join("region.bin");
        if path.exists() {
            let _ = fs::remove_file(&path);
        }
        let shm = ShmRegion::create(&path, &RegionConfig::dev()).map_err(io)?;
        if self.applied_seq() > 0 {
            shm.set_applied(self.applied_seq());
            shm.set_durable(self.applied_seq());
        }
        self.shm = Some(shm);
        Ok(())
    }

    fn ensure_writable(&self) -> Result<(), StoreError> {
        if self.crc_failed {
            Err(StoreError::Crc)
        } else {
            Ok(())
        }
    }

    fn sync_metrics(&self) {
        let applied = self.applied_seq();
        self.metrics.applied_seq.store(applied, Ordering::Release);
        self.metrics.durable_seq.store(applied, Ordering::Release);
        self.metrics.write_seq.store(applied, Ordering::Release);
        self.metrics.heartbeat_ns.store(now_ns(), Ordering::Release);
        self.metrics.late_tick_total.store(self.agg.late_tick_total, Ordering::Release);
    }
}

fn bar_frame(seq: u64, ts: i64, event: &BarEvent, out_idx: u16) -> Frame {
    let (msg, bar) = match event {
        BarEvent::Update(bar) => (MsgType::KlineUpdate, bar),
        BarEvent::Seal(bar) => (MsgType::KlineClose, bar),
    };
    seal_frame(
        FrameHeader {
            seq,
            event_ts_ns: ts,
            recv_ts_ns: ts,
            payload_len: 0,
            msg_type: msg as u16,
            flags: 0,
            symbol_id: bar.symbol_id,
            exchange_id: bar.exchange_id,
            period_id: bar.period.id(),
            partition_id: 0,
            reserved: out_idx,
            crc32: 0,
        },
        &encode_kline(&bar.body),
    )
}

fn io<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Io(err.to_string())
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("qdb-store-{name}-{}-{}", std::process::id(), name.len()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ts(extra_ns: i64) -> i64 {
        // 2024-01-02 09:30:00 +08，再加上 extra_ns。
        let days = days_from_civil(2024, 1, 2);
        let local = days * 86_400 + 9 * 3600 + 30 * 60;
        (local - 8 * 3600) * 1_000_000_000 + extra_ns
    }

    fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
        y -= i32::from(m <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146_097 + doe as i64 - 719_468
    }

    #[test]
    fn region_file_is_not_the_only_copy() {
        let root = temp_root("recover");
        let mut engine = Engine::open(&root).unwrap();
        engine.write_trade("SZSE", "000001", ts(100_000_000), 10, 100).unwrap();
        engine.write_trade("SZSE", "000001", ts(200_000_000), 12, 50).unwrap();
        engine.write_trade("SZSE", "000001", ts(1_000_000_000), 11, 10).unwrap();
        let applied = engine.applied_seq();
        let id = engine.resolve("SZSE:000001").unwrap();
        let before = engine.klines(id, Period::S1);
        drop(engine);
        fs::remove_file(root.join("region.bin")).unwrap();
        let restored = Engine::open(&root).unwrap();
        assert!(!restored.crc_failed());
        assert_eq!(restored.applied_seq(), applied);
        let id = restored.resolve("SZSE:000001").unwrap();
        assert_eq!(restored.klines(id, Period::S1), before);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_wal_stops_before_the_bad_record() {
        let root = temp_root("crc");
        let mut engine = Engine::open(&root).unwrap();
        engine.write_trade("SZSE", "000001", ts(100_000_000), 10, 100).unwrap();
        let applied = engine.applied_seq();
        drop(engine);
        let wal = root.join("wal/wal-1.qwal");
        let mut raw = fs::read(&wal).unwrap();
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.extend_from_slice(&12u32.to_le_bytes());
        raw.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        fs::write(&wal, raw).unwrap();
        let mut restored = Engine::open(&root).unwrap();
        assert!(restored.crc_failed());
        assert_eq!(restored.applied_seq(), applied);
        assert!(restored.write_trade("SZSE", "000001", ts(200_000_000), 1, 1).is_err());
        let _ = fs::remove_dir_all(root);
    }
}
