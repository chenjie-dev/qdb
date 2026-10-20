//! 把转储文件里的行情按原来的顺序重新写入。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::path::Path;

use qdb_abi::{code_str, decode_trade, MsgType, TickQuote};
use qdb_wal::{read_dump, WalRecord};

use crate::{Change, Engine, StoreError};

/// 把转储文件里的成交、报价和时钟按顺序重新写入。
///
/// 每条记录分配新的 `seq`，不保留文件里的原序号。建表和插入记录会被跳过。
///
/// # Arguments
///
/// * `engine` - 可写引擎。已有数据会留着，新记录接在后面。
/// * `file` - `QDBDUMP1` 文件。
///
/// # Returns
///
/// 这次回放产生的变更。同一笔成交会出现在 `ticks` 和 `snapshots`。
///
/// # Errors
///
/// * [`StoreError::Io`] - 文件打不开，或文件头不是转储格式。
/// * [`StoreError::Crc`] - 转储文件校验失败。
/// * [`StoreError::ReadOnly`] - 引擎只读。
pub fn replay_file(engine: &mut Engine, file: &Path) -> Result<Vec<Change>, StoreError> {
    let dump = read_dump(file).map_err(as_io)?;
    if dump.crc_failed {
        return Err(StoreError::Crc);
    }
    replay_records(engine, &dump.records)
}

/// 与 [`replay_file`] 相同，输入是已经读入内存的转储字节。
///
/// # Arguments
///
/// * `engine` - 可写引擎。
/// * `bytes` - 含 `QDBDUMP1` 魔数的完整文件内容。
///
/// # Errors
///
/// 魔数不对时返回 [`StoreError::Io`]。校验失败时返回 [`StoreError::Crc`]。
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

/// 在临时库里回放转储文件，只返回变更，不改源库。
///
/// 临时目录在返回前删除。回放使用新的 `seq`。
///
/// # Arguments
///
/// * `file` - `QDBDUMP1` 文件。
///
/// # Errors
///
/// 同 [`replay_file`]。临时目录创建失败时返回 [`StoreError::Io`]。
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
                let (symbol_id, last, bid_px, bid_qty, ask_px, ask_qty) =
                    crate::decode_quote_payload(&record.payload).ok_or(StoreError::Crc)?;
                let (exchange, code) = symbols
                    .get(&symbol_id)
                    .cloned()
                    .ok_or(StoreError::NotFound)?;
                let quote = TickQuote {
                    last,
                    volume: 0,
                    amount: 0,
                    bid_px,
                    bid_qty,
                    ask_px,
                    ask_qty,
                };
                let seq = engine.write_quote(&exchange, &code, 0, &quote)?;
                out.push(snapshot_change(
                    seq,
                    &format!("{exchange}:{code}"),
                    0,
                    last,
                    bid_px[0],
                    ask_px[0],
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
