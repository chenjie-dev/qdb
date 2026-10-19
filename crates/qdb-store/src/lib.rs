//! 内存表、WAL 应用和启动恢复。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use qdb_abi::{
    code_str, decode_trade, encode_kline, encode_trade, fixed_code, seal_frame, Frame, FrameHeader,
    KlineBody, MsgType, Period, TickQuote, TickTrade,
};
use qdb_agg::{BarEvent, KlineAggregator, TradeInput};
use qdb_shm::{RegionConfig, ShmRegion};
use qdb_wal::{read_from, read_path, WalAppender, WalRead, WalRecord};
use thiserror::Error;

mod catalog;
pub use catalog::{Catalog, CatalogError};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(String),
    #[error("exchange label is empty")]
    EmptyExchange,
    #[error("read-only connection")]
    ReadOnly,
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
    pub exchange: String,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub seq: u64,
    pub table: &'static str,
    pub symbol: String,
    pub ts: i64,
    pub price: i64,
    pub volume: i64,
    pub last: i64,
    pub bid: i64,
    pub ask: i64,
    pub period: &'static str,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
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
    wal: Option<WalAppender>,
    agg: KlineAggregator,
    symbols: Vec<Symbol>,
    by_key: HashMap<(u16, String), u32>,
    exchanges: Vec<String>,
    ticks: HashMap<u32, Vec<TickRow>>,
    quotes: HashMap<u32, QuoteRow>,
    frames: Vec<Frame>,
    shm: Option<ShmRegion>,
    next_seq: u64,
    metrics: Metrics,
    crc_failed: bool,
    readonly: bool,
    wal_pos: u64,
}

impl Engine {
    pub fn open(root: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(root.join("wal")).map_err(io)?;
        fs::create_dir_all(root.join("snap")).map_err(io)?;
        let wal = WalAppender::open(&root.join("wal/wal-1.qwal")).map_err(io)?;
        let loaded = wal.read().map_err(io)?;
        let mut engine = Self::from_replay(root, Some(wal), &loaded)?;
        engine.attach_shm()?;
        engine.sync_metrics();
        Ok(engine)
    }

    /// 重放 WAL 供查询。不创建目录，不追加日志，不重建 `region.bin`。
    pub fn open_read(root: &Path) -> Result<Self, StoreError> {
        if !root.is_dir() {
            return Err(StoreError::Io(format!(
                "data directory not found: {}",
                root.display()
            )));
        }
        let loaded = read_path(&root.join("wal/wal-1.qwal")).map_err(io)?;
        let engine = Self::from_replay(root, None, &loaded)?;
        Ok(engine)
    }

    fn from_replay(
        root: &Path,
        wal: Option<WalAppender>,
        loaded: &WalRead,
    ) -> Result<Self, StoreError> {
        let readonly = wal.is_none();
        let mut engine = Self {
            root: root.to_path_buf(),
            wal,
            agg: KlineAggregator::default(),
            symbols: Vec::new(),
            by_key: HashMap::new(),
            exchanges: Vec::new(),
            ticks: HashMap::new(),
            quotes: HashMap::new(),
            frames: Vec::new(),
            shm: None,
            next_seq: 1,
            metrics: Metrics::new(),
            crc_failed: loaded.crc_failed,
            readonly,
            wal_pos: loaded.consumed,
        };
        for record in &loaded.records {
            engine.apply_record(record)?;
        }
        if loaded.crc_failed {
            engine.crc_failed = true;
            engine.metrics.crc_failed.store(true, Ordering::Release);
        }
        engine.next_seq = engine.agg.last_applied_seq() + 1;
        if engine.readonly {
            engine.sync_metrics();
        }
        Ok(engine)
    }

    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    pub fn crc_failed(&self) -> bool {
        self.crc_failed
    }

    /// 读入写入者新 `fsync` 的记录，并更新本进程里的表。
    pub fn poll(&mut self) -> Result<Vec<Change>, StoreError> {
        if self.crc_failed {
            return Err(StoreError::Crc);
        }
        let appended = read_from(&self.root.join("wal/wal-1.qwal"), self.wal_pos).map_err(io)?;
        self.wal_pos = appended.consumed;
        let mut changes = Vec::new();
        for record in &appended.records {
            let events = self.apply_record(record)?;
            changes.extend(self.changes_for(record, &events));
            self.next_seq = self.applied_seq() + 1;
        }
        if appended.crc_failed {
            self.crc_failed = true;
            self.metrics.crc_failed.store(true, Ordering::Release);
            if changes.is_empty() {
                return Err(StoreError::Crc);
            }
        }
        self.sync_metrics();
        Ok(changes)
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
            let exchange_id = self.exchange_id(ex).ok_or(StoreError::NotFound)?;
            return self
                .by_key
                .get(&(exchange_id, code.to_string()))
                .copied()
                .ok_or(StoreError::NotFound);
        }
        let hits: Vec<u32> = self
            .symbols
            .iter()
            .filter(|symbol| symbol.code == text)
            .map(|symbol| symbol.id)
            .collect();
        match hits.as_slice() {
            [id] => Ok(*id),
            [] => Err(StoreError::NotFound),
            _ => Err(StoreError::Ambiguous),
        }
    }

    pub fn upsert_symbol(&mut self, exchange_code: &str, code: &str) -> Result<u32, StoreError> {
        if exchange_code.is_empty() {
            return Err(StoreError::EmptyExchange);
        }
        let exchange_id = self.intern_exchange(exchange_code);
        if let Some(id) = self.by_key.get(&(exchange_id, code.to_string())) {
            return Ok(*id);
        }
        self.ensure_writable()?;
        let id = self.symbols.len() as u32 + 1;
        let mut payload = Vec::new();
        payload.extend_from_slice(&id.to_le_bytes());
        payload.extend_from_slice(&exchange_id.to_le_bytes());
        payload.extend_from_slice(&fixed_label(exchange_code));
        payload.extend_from_slice(&fixed_code(code));
        self.commit(MsgType::SymbolUpsert, 0, id, exchange_id, 0, &payload, 0)?;
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
        let exchange_id = self
            .symbols
            .iter()
            .find(|symbol| symbol.id == symbol_id)
            .unwrap()
            .exchange_id;
        let mut payload = Vec::new();
        payload.extend_from_slice(&symbol_id.to_le_bytes());
        payload.extend_from_slice(&event_ts_ns.to_le_bytes());
        payload.extend_from_slice(&encode_trade(&TickTrade {
            price,
            volume,
            amount: price.saturating_mul(volume),
            trade_id: 0,
        }));
        self.commit(
            MsgType::TickTrade,
            event_ts_ns,
            symbol_id,
            exchange_id,
            0,
            &payload,
            0,
        )
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
        let exchange_id = self
            .symbols
            .iter()
            .find(|symbol| symbol.id == symbol_id)
            .unwrap()
            .exchange_id;
        let mut payload = Vec::new();
        payload.extend_from_slice(&symbol_id.to_le_bytes());
        payload.extend_from_slice(&quote.last.to_le_bytes());
        payload.extend_from_slice(&quote.bid_px[0].to_le_bytes());
        payload.extend_from_slice(&quote.ask_px[0].to_le_bytes());
        self.commit(
            MsgType::TickQuote,
            event_ts_ns,
            symbol_id,
            exchange_id,
            0,
            &payload,
            0,
        )
    }

    /// 连续交易的时间水位。水位越过桶结束时间就闭合 K 线，不需要等待下一笔成交。
    pub fn write_clock(
        &mut self,
        exchange_code: &str,
        watermark_ns: i64,
    ) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        if exchange_code.is_empty() {
            return Err(StoreError::EmptyExchange);
        }
        let exchange_id = self.intern_exchange(exchange_code);
        let mut payload = Vec::new();
        payload.extend_from_slice(&exchange_id.to_le_bytes());
        payload.extend_from_slice(&watermark_ns.to_le_bytes());
        self.commit(
            MsgType::ClockTick,
            watermark_ns,
            0,
            exchange_id,
            0,
            &payload,
            0,
        )
    }

    fn commit(
        &mut self,
        msg_type: MsgType,
        event_ts_ns: i64,
        symbol_id: u32,
        exchange_id: u16,
        period_id: u16,
        payload: &[u8],
        flags: u16,
    ) -> Result<u64, StoreError> {
        let seq = self.next_seq;
        let record = WalRecord {
            seq,
            msg_type: msg_type as u16,
            flags,
            payload: payload.to_vec(),
        };
        let wal = self.wal.as_mut().ok_or(StoreError::ReadOnly)?;
        wal.append(&record).map_err(io)?;
        wal.flush().map_err(io)?;
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
                flags,
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
            self.frames
                .push(bar_frame(seq, event_ts_ns, event, index as u16 + 1));
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
                let exchange_name = code_str(&record.payload[6..22]);
                let code = code_str(&record.payload[22..54]);
                self.remember_exchange(exchange_id, &exchange_name);
                if !self.by_key.contains_key(&(exchange_id, code.clone())) {
                    self.by_key.insert((exchange_id, code.clone()), id);
                    self.symbols.push(Symbol {
                        id,
                        exchange_id,
                        code,
                        price_scale: 10_000,
                        exchange: exchange_name,
                    });
                }
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::TickTrade) => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let event_ts_ns = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let trade = decode_trade(&record.payload[12..]).ok_or(StoreError::Crc)?;
                let symbol = self
                    .symbols
                    .iter()
                    .find(|symbol| symbol.id == symbol_id)
                    .ok_or(StoreError::NotFound)?;
                let exchange_id = symbol.exchange_id;
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
                self.quotes.insert(
                    symbol_id,
                    QuoteRow {
                        seq: record.seq,
                        last,
                        bid_px,
                        ask_px,
                    },
                );
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

    fn changes_for(&self, record: &WalRecord, events: &[BarEvent]) -> Vec<Change> {
        let mut out = Vec::new();
        match MsgType::from_u16(record.msg_type) {
            Some(MsgType::TickTrade) if record.payload.len() >= 12 => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let ts = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let trade = decode_trade(&record.payload[12..]);
                let (price, volume) = trade
                    .map(|item| (item.price, item.volume))
                    .unwrap_or((0, 0));
                let (bid, ask) = self.bid_ask(symbol_id);
                let symbol = self.symbol_name(symbol_id);
                out.push(Change {
                    seq: record.seq,
                    table: "ticks",
                    symbol: symbol.clone(),
                    ts,
                    price,
                    volume,
                    last: price,
                    bid,
                    ask,
                    period: "",
                    open: 0,
                    high: 0,
                    low: 0,
                    close: price,
                });
                out.push(Change {
                    seq: record.seq,
                    table: "snapshots",
                    symbol,
                    ts,
                    price,
                    volume,
                    last: price,
                    bid,
                    ask,
                    period: "",
                    open: 0,
                    high: 0,
                    low: 0,
                    close: price,
                });
            }
            Some(MsgType::TickQuote) if record.payload.len() >= 28 => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let last = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let bid = i64::from_le_bytes(record.payload[12..20].try_into().unwrap());
                let ask = i64::from_le_bytes(record.payload[20..28].try_into().unwrap());
                out.push(Change {
                    seq: record.seq,
                    table: "snapshots",
                    symbol: self.symbol_name(symbol_id),
                    ts: 0,
                    price: last,
                    volume: 0,
                    last,
                    bid,
                    ask,
                    period: "",
                    open: 0,
                    high: 0,
                    low: 0,
                    close: last,
                });
            }
            _ => {}
        }
        for event in events {
            let bar = match event {
                BarEvent::Update(bar) | BarEvent::Seal(bar) => bar,
            };
            out.push(Change {
                seq: record.seq,
                table: "klines",
                symbol: self.symbol_name(bar.symbol_id),
                ts: bar.body.bucket_start_ns,
                price: bar.body.close,
                volume: bar.body.volume,
                last: bar.body.close,
                bid: 0,
                ask: 0,
                period: bar.period.as_str(),
                open: bar.body.open,
                high: bar.body.high,
                low: bar.body.low,
                close: bar.body.close,
            });
        }
        out
    }

    fn bid_ask(&self, symbol_id: u32) -> (i64, i64) {
        self.quotes
            .get(&symbol_id)
            .map(|quote| (quote.bid_px[0], quote.ask_px[0]))
            .unwrap_or((0, 0))
    }

    fn symbol_name(&self, symbol_id: u32) -> String {
        self.symbols
            .iter()
            .find(|symbol| symbol.id == symbol_id)
            .map(|symbol| format!("{}:{}", symbol.exchange, symbol.code))
            .unwrap_or_default()
    }

    fn ensure_writable(&self) -> Result<(), StoreError> {
        if self.readonly {
            return Err(StoreError::ReadOnly);
        }
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
        self.metrics
            .late_tick_total
            .store(self.agg.late_tick_total, Ordering::Release);
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

impl Engine {
    fn intern_exchange(&mut self, name: &str) -> u16 {
        if let Some(index) = self.exchanges.iter().position(|existing| existing == name) {
            return (index as u16) + 1;
        }
        self.exchanges.push(name.to_string());
        self.exchanges.len() as u16
    }

    fn remember_exchange(&mut self, id: u16, name: &str) {
        let index = (id as usize).saturating_sub(1);
        if self.exchanges.len() <= index {
            self.exchanges.resize(index + 1, String::new());
        }
        if self.exchanges[index].is_empty() {
            self.exchanges[index] = name.to_string();
        }
    }

    fn exchange_id(&self, name: &str) -> Option<u16> {
        self.exchanges
            .iter()
            .position(|existing| existing == name)
            .map(|index| (index as u16) + 1)
    }
}

fn fixed_label(value: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let bytes = value.as_bytes();
    let n = bytes.len().min(16);
    out[..n].copy_from_slice(&bytes[..n]);
    out
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
        let dir = std::env::temp_dir().join(format!(
            "qdb-store-{name}-{}-{}",
            std::process::id(),
            name.len()
        ));
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
        engine
            .write_trade("SZSE", "000001", ts(100_000_000), 10, 100)
            .unwrap();
        engine
            .write_trade("SZSE", "000001", ts(200_000_000), 12, 50)
            .unwrap();
        engine
            .write_trade("SZSE", "000001", ts(1_000_000_000), 11, 10)
            .unwrap();
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
        engine
            .write_trade("SZSE", "000001", ts(100_000_000), 10, 100)
            .unwrap();
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
        assert!(restored
            .write_trade("SZSE", "000001", ts(200_000_000), 1, 1)
            .is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_only_open_replays_wal_without_touching_the_region_file() {
        let root = temp_root("reader");
        let mut engine = Engine::open(&root).unwrap();
        engine
            .write_trade("SSE", "600000", ts(0), 1_050_000, 100)
            .unwrap();
        drop(engine);
        let region = root.join("region.bin");
        let before = fs::read(&region).unwrap();
        let wal_len = fs::metadata(root.join("wal/wal-1.qwal")).unwrap().len();
        let mut reader = Engine::open_read(&root).unwrap();
        assert!(reader
            .quote(reader.resolve("SSE:600000").unwrap())
            .is_some());
        assert!(matches!(
            reader.write_trade("SSE", "600000", ts(1), 1, 1),
            Err(StoreError::ReadOnly)
        ));
        assert_eq!(fs::read(&region).unwrap(), before);
        assert_eq!(
            fs::metadata(root.join("wal/wal-1.qwal")).unwrap().len(),
            wal_len
        );
        let _ = fs::remove_dir_all(&root);
        assert!(Engine::open_read(&root).is_err());
    }

    #[test]
    fn poll_applies_a_trade_appended_while_the_reader_is_open() {
        let root = temp_root("poll");
        let mut writer = Engine::open(&root).unwrap();
        writer.write_trade("SSE", "600000", ts(0), 10, 1).unwrap();
        let mut reader = Engine::open_read(&root).unwrap();
        let id = reader.resolve("SSE:600000").unwrap();
        assert_eq!(reader.quote(id).unwrap().last, 10);
        assert!(reader.poll().unwrap().is_empty());
        writer
            .write_quote(
                "SSE",
                "600000",
                ts(1),
                &qdb_abi::TickQuote {
                    last: 11,
                    volume: 0,
                    amount: 0,
                    bid_px: [10, 0, 0, 0, 0],
                    bid_qty: [1, 0, 0, 0, 0],
                    ask_px: [12, 0, 0, 0, 0],
                    ask_qty: [1, 0, 0, 0, 0],
                },
            )
            .unwrap();
        writer.write_trade("SSE", "600000", ts(2), 13, 2).unwrap();
        let changes = reader.poll().unwrap();
        assert!(changes
            .iter()
            .any(|change| change.table == "snapshots" && change.last == 11 && change.bid == 10));
        assert!(changes
            .iter()
            .any(|change| change.table == "ticks" && change.price == 13 && change.volume == 2));
        assert!(changes
            .iter()
            .any(|change| change.table == "klines" && change.period == "1m" && change.close == 13));
        assert_eq!(reader.quote(id).unwrap().last, 13);
        assert!(reader.poll().unwrap().is_empty());
        let _ = fs::remove_dir_all(root);
    }
}
