//! 把转储文件里的行情按原来的顺序重新写入。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::path::Path;

use qdb_abi::{code_str, decode_trade, MsgType, TickQuote};
use qdb_wal::{read_dump, WalRecord};

use crate::{Change, Engine, StoreError};

pub fn replay_file(engine: &mut Engine, file: &Path) -> Result<Vec<Change>, StoreError> {
    let dump = read_dump(file).map_err(as_io)?;
    if dump.crc_failed {
        return Err(StoreError::Crc);
    }
    replay_records(engine, &dump.records)
}

pub fn replay_bytes(engine: &mut Engine, bytes: &[u8]) -> Result<Vec<Change>, StoreError> {
    if bytes.len() < 8 || &bytes[..8] != qdb_wal::DUMP_MAGIC {
        return Err(StoreError::Io("not a qdb dump".into()));
    }
    let dump = qdb_wal::parse_dump_body(&bytes[8..]).map_err(as_io)?;
    if dump.crc_failed {
        return Err(StoreError::Crc);
    }
    replay_records(engine, &dump.records)
}

pub fn playback(file: &Path) -> Result<Vec<Change>, StoreError> {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let dir = std::env::temp_dir().join(format!("qdb-playback-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut engine = Engine::open(&dir)?;
    let changes = replay_file(&mut engine, file);
    let _ = std::fs::remove_dir_all(&dir);
    changes
}

fn replay_records(engine: &mut Engine, records: &[WalRecord]) -> Result<Vec<Change>, StoreError> {
    let mut symbols: HashMap<u32, (String, String)> = HashMap::new();
    let mut exchanges: HashMap<u16, String> = HashMap::new();
    let mut out = Vec::new();
    for record in records {
        match MsgType::from_u16(record.msg_type) {
            Some(MsgType::SymbolUpsert) if record.payload.len() >= 54 => {
                let id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let exchange_id = u16::from_le_bytes(record.payload[4..6].try_into().unwrap());
                let exchange = code_str(&record.payload[6..22]);
                let code = code_str(&record.payload[22..54]);
                exchanges.insert(exchange_id, exchange.clone());
                symbols.insert(id, (exchange.clone(), code.clone()));
                engine.upsert_symbol(&exchange, &code)?;
            }
            Some(MsgType::TickTrade) if record.payload.len() >= 12 => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let ts = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let (exchange, code) = symbols
                    .get(&symbol_id)
                    .cloned()
                    .ok_or(StoreError::NotFound)?;
                let trade = decode_trade(&record.payload[12..]).ok_or(StoreError::Crc)?;
                let seq = engine.write_trade(&exchange, &code, ts, trade.price, trade.volume)?;
                let symbol = format!("{exchange}:{code}");
                out.push(trade_change(seq, &symbol, ts, trade.price, trade.volume));
                out.push(snapshot_change(seq, &symbol, ts, trade.price, 0, 0));
            }
            Some(MsgType::TickQuote) if record.payload.len() >= 28 => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let last = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let bid = i64::from_le_bytes(record.payload[12..20].try_into().unwrap());
                let ask = i64::from_le_bytes(record.payload[20..28].try_into().unwrap());
                let (exchange, code) = symbols
                    .get(&symbol_id)
                    .cloned()
                    .ok_or(StoreError::NotFound)?;
                let mut quote = TickQuote {
                    last,
                    volume: 0,
                    amount: 0,
                    bid_px: [0; 5],
                    bid_qty: [0; 5],
                    ask_px: [0; 5],
                    ask_qty: [0; 5],
                };
                quote.bid_px[0] = bid;
                quote.ask_px[0] = ask;
                let seq = engine.write_quote(&exchange, &code, 0, &quote)?;
                out.push(snapshot_change(
                    seq,
                    &format!("{exchange}:{code}"),
                    0,
                    last,
                    bid,
                    ask,
                ));
            }
            Some(MsgType::ClockTick) if record.payload.len() >= 10 => {
                let exchange_id = u16::from_le_bytes(record.payload[0..2].try_into().unwrap());
                let watermark = i64::from_le_bytes(record.payload[2..10].try_into().unwrap());
                let exchange = exchanges
                    .get(&exchange_id)
                    .cloned()
                    .ok_or(StoreError::NotFound)?;
                engine.write_clock(&exchange, watermark)?;
            }
            _ => {}
        }
    }
    Ok(out)
}

fn trade_change(seq: u64, symbol: &str, ts: i64, price: i64, volume: i64) -> Change {
    Change {
        seq,
        table: "ticks",
        symbol: symbol.to_string(),
        ts,
        price,
        volume,
        last: price,
        bid: 0,
        ask: 0,
        period: "",
        open: 0,
        high: 0,
        low: 0,
        close: price,
    }
}

fn snapshot_change(seq: u64, symbol: &str, ts: i64, last: i64, bid: i64, ask: i64) -> Change {
    Change {
        seq,
        table: "snapshots",
        symbol: symbol.to_string(),
        ts,
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
    }
}

fn as_io<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Io(err.to_string())
}
