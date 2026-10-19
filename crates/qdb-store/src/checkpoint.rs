//! 检查点：打开时先装入这里的状态，再只重放之后的 WAL。
#![forbid(unsafe_code)]

use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use qdb_abi::{KlineBody, Period};
use qdb_agg::{AggState, OpenBar};

use crate::{QuoteRow, StoreError, Symbol, TickRow};

const MAGIC: &[u8; 8] = b"QDBCHK01";

pub struct Checkpoint {
    pub wal_pos: u64,
    pub symbols: Vec<Symbol>,
    pub exchanges: Vec<String>,
    pub ticks: Vec<(u32, TickRow)>,
    pub quotes: Vec<(u32, QuoteRow)>,
    pub agg: AggState,
    pub tables: Vec<u8>,
}

pub fn write(root: &Path, checkpoint: &Checkpoint) -> Result<(), StoreError> {
    let path = root.join("snap/checkpoint.qsnap");
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| StoreError::Io(err.to_string()))?;
    }
    let mut file = fs::File::create(&path).map_err(|err| StoreError::Io(err.to_string()))?;
    file.write_all(MAGIC).map_err(io)?;
    file.write_all(&checkpoint.wal_pos.to_le_bytes())
        .map_err(io)?;
    write_u32(&mut file, checkpoint.symbols.len() as u32)?;
    for symbol in &checkpoint.symbols {
        write_u32(&mut file, symbol.id)?;
        file.write_all(&symbol.exchange_id.to_le_bytes())
            .map_err(io)?;
        file.write_all(&symbol.price_scale.to_le_bytes())
            .map_err(io)?;
        write_str(&mut file, &symbol.exchange)?;
        write_str(&mut file, &symbol.code)?;
    }
    write_u32(&mut file, checkpoint.exchanges.len() as u32)?;
    for exchange in &checkpoint.exchanges {
        write_str(&mut file, exchange)?;
    }
    write_u32(&mut file, checkpoint.ticks.len() as u32)?;
    for (symbol_id, tick) in &checkpoint.ticks {
        write_u32(&mut file, *symbol_id)?;
        file.write_all(&tick.seq.to_le_bytes()).map_err(io)?;
        file.write_all(&tick.ts.to_le_bytes()).map_err(io)?;
        file.write_all(&tick.price.to_le_bytes()).map_err(io)?;
        file.write_all(&tick.volume.to_le_bytes()).map_err(io)?;
        file.write_all(&tick.amount.to_le_bytes()).map_err(io)?;
    }
    write_u32(&mut file, checkpoint.quotes.len() as u32)?;
    for (symbol_id, quote) in &checkpoint.quotes {
        write_u32(&mut file, *symbol_id)?;
        file.write_all(&quote.seq.to_le_bytes()).map_err(io)?;
        file.write_all(&quote.last.to_le_bytes()).map_err(io)?;
        write_i64s(&mut file, &quote.bid_px)?;
        write_i64s(&mut file, &quote.bid_qty)?;
        write_i64s(&mut file, &quote.ask_px)?;
        write_i64s(&mut file, &quote.ask_qty)?;
    }
    file.write_all(&checkpoint.agg.last_applied_seq.to_le_bytes())
        .map_err(io)?;
    file.write_all(&checkpoint.agg.late_tick_total.to_le_bytes())
        .map_err(io)?;
    write_u32(&mut file, checkpoint.agg.open.len() as u32)?;
    for bar in &checkpoint.agg.open {
        write_open(&mut file, bar)?;
    }
    write_u32(&mut file, checkpoint.agg.sealed.len() as u32)?;
    for ((symbol_id, period_id), rows) in &checkpoint.agg.sealed {
        write_u32(&mut file, *symbol_id)?;
        file.write_all(&period_id.to_le_bytes()).map_err(io)?;
        write_u32(&mut file, rows.len() as u32)?;
        for row in rows {
            write_kline(&mut file, row)?;
        }
    }
    write_u32(&mut file, checkpoint.tables.len() as u32)?;
    file.write_all(&checkpoint.tables).map_err(io)?;
    file.sync_all().map_err(io)?;
    Ok(())
}

pub fn read(root: &Path) -> Option<Checkpoint> {
    let path = root.join("snap/checkpoint.qsnap");
    let mut file = fs::File::open(path).ok()?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic).ok()?;
    if &magic != MAGIC {
        return None;
    }
    let checkpoint = (|| {
        let wal_pos = read_u64(&mut file)?;
        let n = read_u32(&mut file)? as usize;
        let mut symbols = Vec::with_capacity(n);
        for _ in 0..n {
            let id = read_u32(&mut file)?;
            let exchange_id = read_u16(&mut file)?;
            let price_scale = read_u16(&mut file)?;
            let exchange = read_str(&mut file)?;
            let code = read_str(&mut file)?;
            symbols.push(Symbol {
                id,
                exchange_id,
                code,
                price_scale,
                exchange,
            });
        }
        let n = read_u32(&mut file)? as usize;
        let mut exchanges = Vec::with_capacity(n);
        for _ in 0..n {
            exchanges.push(read_str(&mut file)?);
        }
        let n = read_u32(&mut file)? as usize;
        let mut ticks = Vec::with_capacity(n);
        for _ in 0..n {
            let symbol_id = read_u32(&mut file)?;
            ticks.push((
                symbol_id,
                TickRow {
                    seq: read_u64(&mut file)?,
                    ts: read_i64(&mut file)?,
                    price: read_i64(&mut file)?,
                    volume: read_i64(&mut file)?,
                    amount: read_i64(&mut file)?,
                },
            ));
        }
        let n = read_u32(&mut file)? as usize;
        let mut quotes = Vec::with_capacity(n);
        for _ in 0..n {
            let symbol_id = read_u32(&mut file)?;
            quotes.push((
                symbol_id,
                QuoteRow {
                    seq: read_u64(&mut file)?,
                    last: read_i64(&mut file)?,
                    bid_px: read_i64s(&mut file)?,
                    bid_qty: read_i64s(&mut file)?,
                    ask_px: read_i64s(&mut file)?,
                    ask_qty: read_i64s(&mut file)?,
                },
            ));
        }
        let last_applied_seq = read_u64(&mut file)?;
        let late_tick_total = read_u64(&mut file)?;
        let n = read_u32(&mut file)? as usize;
        let mut open = Vec::with_capacity(n);
        for _ in 0..n {
            open.push(read_open(&mut file)?);
        }
        let n = read_u32(&mut file)? as usize;
        let mut sealed = Vec::with_capacity(n);
        for _ in 0..n {
            let symbol_id = read_u32(&mut file)?;
            let period_id = read_u16(&mut file)?;
            let rows_n = read_u32(&mut file)? as usize;
            let mut rows = Vec::with_capacity(rows_n);
            for _ in 0..rows_n {
                rows.push(read_kline(&mut file)?);
            }
            sealed.push(((symbol_id, period_id), rows));
        }
        let table_len = read_u32(&mut file)? as usize;
        let mut tables = vec![0u8; table_len];
        file.read_exact(&mut tables).ok()?;
        Some(Checkpoint {
            wal_pos,
            symbols,
            exchanges,
            ticks,
            quotes,
            agg: AggState {
                last_applied_seq,
                late_tick_total,
                open,
                sealed,
            },
            tables,
        })
    })();
    checkpoint
}

fn write_open(file: &mut fs::File, bar: &OpenBar) -> Result<(), StoreError> {
    write_u32(file, bar.symbol_id)?;
    file.write_all(&bar.exchange_id.to_le_bytes()).map_err(io)?;
    file.write_all(&bar.period.id().to_le_bytes()).map_err(io)?;
    file.write_all(&bar.seq.to_le_bytes()).map_err(io)?;
    write_kline(file, &bar.body)
}

fn read_open(file: &mut fs::File) -> Option<OpenBar> {
    let symbol_id = read_u32(file)?;
    let exchange_id = read_u16(file)?;
    let period = Period::from_u16(read_u16(file)?)?;
    let seq = read_u64(file)?;
    Some(OpenBar {
        symbol_id,
        exchange_id,
        period,
        seq,
        body: read_kline(file)?,
    })
}

fn write_kline(file: &mut fs::File, body: &KlineBody) -> Result<(), StoreError> {
    for value in [
        body.bucket_start_ns,
        body.open,
        body.high,
        body.low,
        body.close,
        body.volume,
        body.amount,
    ] {
        file.write_all(&value.to_le_bytes()).map_err(io)?;
    }
    file.write_all(&body.tick_count.to_le_bytes()).map_err(io)?;
    file.write_all(&[body.state, body.origin]).map_err(io)?;
    file.write_all(&body.revision.to_le_bytes()).map_err(io)?;
    file.write_all(&body.flags.to_le_bytes()).map_err(io)?;
    Ok(())
}

fn read_kline(file: &mut fs::File) -> Option<KlineBody> {
    Some(KlineBody {
        bucket_start_ns: read_i64(file)?,
        open: read_i64(file)?,
        high: read_i64(file)?,
        low: read_i64(file)?,
        close: read_i64(file)?,
        volume: read_i64(file)?,
        amount: read_i64(file)?,
        tick_count: read_u32(file)?,
        state: read_u8(file)?,
        origin: read_u8(file)?,
        revision: read_u16(file)?,
        flags: read_u32(file)?,
    })
}

fn write_i64s(file: &mut fs::File, values: &[i64; 5]) -> Result<(), StoreError> {
    for value in values {
        file.write_all(&value.to_le_bytes()).map_err(io)?;
    }
    Ok(())
}

fn read_i64s(file: &mut fs::File) -> Option<[i64; 5]> {
    Some([
        read_i64(file)?,
        read_i64(file)?,
        read_i64(file)?,
        read_i64(file)?,
        read_i64(file)?,
    ])
}

fn write_u32(file: &mut fs::File, value: u32) -> Result<(), StoreError> {
    file.write_all(&value.to_le_bytes()).map_err(io)
}

fn write_str(file: &mut fs::File, value: &str) -> Result<(), StoreError> {
    write_u32(file, value.len() as u32)?;
    file.write_all(value.as_bytes()).map_err(io)
}

fn read_u64(file: &mut fs::File) -> Option<u64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).ok()?;
    Some(u64::from_le_bytes(buf))
}

fn read_i64(file: &mut fs::File) -> Option<i64> {
    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).ok()?;
    Some(i64::from_le_bytes(buf))
}

fn read_u32(file: &mut fs::File) -> Option<u32> {
    let mut buf = [0u8; 4];
    file.read_exact(&mut buf).ok()?;
    Some(u32::from_le_bytes(buf))
}

fn read_u16(file: &mut fs::File) -> Option<u16> {
    let mut buf = [0u8; 2];
    file.read_exact(&mut buf).ok()?;
    Some(u16::from_le_bytes(buf))
}

fn read_u8(file: &mut fs::File) -> Option<u8> {
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf).ok()?;
    Some(buf[0])
}

fn read_str(file: &mut fs::File) -> Option<String> {
    let len = read_u32(file)? as usize;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf).ok()?;
    String::from_utf8(buf).ok()
}

fn io(err: std::io::Error) -> StoreError {
    StoreError::Io(err.to_string())
}
