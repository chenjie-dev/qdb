//! 第一版 SQL 子集：SELECT / WHERE / ORDER BY / LIMIT / GROUP BY / BETWEEN。
#![forbid(unsafe_code)]

use qdb_abi::{KlineBody, Period};
use qdb_store::{Engine, StoreError};
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SqlError {
    #[error("syntax: {0}")]
    Syntax(String),
    #[error("symbol not found")]
    NotFound,
    #[error("ambiguous symbol")]
    Ambiguous,
    #[error("limit {0} exceeds 100000")]
    Limit(u64),
    #[error("{0}")]
    Store(String),
}

pub struct SqlEngine<'a> {
    engine: &'a Engine,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl<'a> SqlEngine<'a> {
    pub fn new(engine: &'a Engine) -> Self {
        Self { engine }
    }

    pub fn query(&self, sql: &str) -> Result<QueryResult, SqlError> {
        let stmt = parse(sql)?;
        if stmt.limit > 100_000 {
            return Err(SqlError::Limit(stmt.limit));
        }
        match stmt.table.as_str() {
            "klines" => self.query_klines(&stmt),
            "snapshots" => self.query_snapshots(&stmt),
            "symbols" => self.query_symbols(&stmt),
            "ticks" => self.query_ticks(&stmt),
            other => Err(SqlError::Syntax(format!("unknown table {other}"))),
        }
    }
}

struct Stmt {
    columns: Vec<String>,
    aggregates: Vec<Agg>,
    table: String,
    symbol: Option<String>,
    period: Option<Period>,
    ts_from: Option<i64>,
    ts_to: Option<i64>,
    limit: u64,
}

struct Agg {
    func: String,
    column: String,
}

fn parse(sql: &str) -> Result<Stmt, SqlError> {
    let tokens = tokenize(sql);
    let mut index = 0;
    expect(&tokens, &mut index, "select")?;
    let mut columns = Vec::new();
    let mut aggregates = Vec::new();
    if tokens.get(index).map(|t| t.as_str()) == Some("*") {
        columns.push("*".to_string());
        index += 1;
    } else {
        loop {
            let name = tokens.get(index).ok_or_else(|| SqlError::Syntax("column".into()))?.clone();
            index += 1;
            if tokens.get(index).map(|t| t.as_str()) == Some("(") {
                index += 1;
                let column = tokens.get(index).ok_or_else(|| SqlError::Syntax("agg arg".into()))?.clone();
                index += 1;
                expect(&tokens, &mut index, ")")?;
                aggregates.push(Agg {
                    func: name.to_ascii_lowercase(),
                    column,
                });
                columns.push(format!("{}({})", aggregates.last().unwrap().func, aggregates.last().unwrap().column));
            } else {
                columns.push(name);
            }
            if tokens.get(index).map(|t| t.as_str()) == Some(",") {
                index += 1;
                continue;
            }
            break;
        }
    }
    expect(&tokens, &mut index, "from")?;
    let table = tokens.get(index).ok_or_else(|| SqlError::Syntax("table".into()))?.to_ascii_lowercase();
    index += 1;
    let mut symbol = None;
    let mut period = None;
    let mut ts_from = None;
    let mut ts_to = None;
    if tokens.get(index).map(|t| t.eq_ignore_ascii_case("where")).unwrap_or(false) {
        index += 1;
        while index < tokens.len() && !tokens[index].eq_ignore_ascii_case("order") && !tokens[index].eq_ignore_ascii_case("limit") && !tokens[index].eq_ignore_ascii_case("group") {
            let field = tokens.get(index).ok_or_else(|| SqlError::Syntax("where".into()))?.to_ascii_lowercase();
            index += 1;
            if tokens.get(index).map(|t| t.eq_ignore_ascii_case("between")).unwrap_or(false) {
                index += 1;
                let from = parse_i64(tokens.get(index).ok_or_else(|| SqlError::Syntax("between".into()))?)?;
                index += 1;
                expect(&tokens, &mut index, "and")?;
                let to = parse_i64(tokens.get(index).ok_or_else(|| SqlError::Syntax("between".into()))?)?;
                index += 1;
                if field == "ts" {
                    ts_from = Some(from);
                    ts_to = Some(to);
                }
            } else {
                expect(&tokens, &mut index, "=")?;
                let value = tokens.get(index).ok_or_else(|| SqlError::Syntax("value".into()))?.clone();
                index += 1;
                if field == "symbol" {
                    symbol = Some(unquote(&value));
                } else if field == "period" {
                    period = Period::parse(&unquote(&value));
                }
            }
            if tokens.get(index).map(|t| t.eq_ignore_ascii_case("and")).unwrap_or(false) {
                index += 1;
            }
        }
    }
    let mut limit = 1000;
    if let Some(pos) = tokens.iter().position(|t| t.eq_ignore_ascii_case("limit")) {
        limit = tokens.get(pos + 1).ok_or_else(|| SqlError::Syntax("limit".into()))?.parse().map_err(|_| SqlError::Syntax("limit".into()))?;
    }
    Ok(Stmt {
        columns,
        aggregates,
        table,
        symbol,
        period,
        ts_from,
        ts_to,
        limit,
    })
}

impl<'a> SqlEngine<'a> {
    fn query_klines(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let symbol = stmt.symbol.clone().ok_or_else(|| SqlError::Syntax("symbol required".into()))?;
        let symbol_id = map_resolve(self.engine.resolve(&symbol))?;
        let period = stmt.period.ok_or_else(|| SqlError::Syntax("period required".into()))?;
        let mut rows = self.engine.klines(symbol_id, period);
        if let Some(from) = stmt.ts_from {
            rows.retain(|row| row.bucket_start_ns >= from);
        }
        if let Some(to) = stmt.ts_to {
            rows.retain(|row| row.bucket_start_ns <= to);
        }
        rows.truncate(stmt.limit as usize);
        if stmt.aggregates.is_empty() {
            let columns = if stmt.columns == ["*"] {
                vec!["ts".into(), "open".into(), "high".into(), "low".into(), "close".into(), "volume".into(), "amount".into(), "seq".into()]
            } else {
                stmt.columns.clone()
            };
            let out = rows
                .iter()
                .map(|row| columns.iter().map(|col| kline_value(row, col)).collect())
                .collect();
            Ok(QueryResult { columns, rows: out })
        } else {
            let columns = stmt.aggregates.iter().map(|agg| format!("{}({})", agg.func, agg.column)).collect();
            let values = stmt.aggregates.iter().map(|agg| aggregate(&rows, agg)).collect::<Result<Vec<_>, _>>()?;
            Ok(QueryResult {
                columns,
                rows: vec![values],
            })
        }
    }

    fn query_snapshots(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let symbol = stmt.symbol.clone().ok_or_else(|| SqlError::Syntax("symbol required".into()))?;
        let symbol_id = map_resolve(self.engine.resolve(&symbol))?;
        let quote = self.engine.quote(symbol_id).ok_or(SqlError::NotFound)?;
        let columns = if stmt.columns == ["*"] {
            vec!["last".into(), "bid_px1".into(), "ask_px1".into(), "seq".into()]
        } else {
            stmt.columns.clone()
        };
        let row = columns
            .iter()
            .map(|col| match col.as_str() {
                "last" => quote.last.to_string(),
                "bid_px1" => quote.bid_px[0].to_string(),
                "ask_px1" => quote.ask_px[0].to_string(),
                "seq" => quote.seq.to_string(),
                _ => String::new(),
            })
            .collect();
        Ok(QueryResult { columns, rows: vec![row] })
    }

    fn query_symbols(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let columns = vec!["symbol".into(), "exchange".into()];
        let mut rows = Vec::new();
        for symbol in self.engine.symbols() {
            let name = format!("{}:{}", qdb_abi::exchange_code(symbol.exchange_id).unwrap_or("?"), symbol.code);
            rows.push(vec![name, symbol.exchange_id.to_string()]);
        }
        rows.truncate(stmt.limit as usize);
        Ok(QueryResult { columns, rows })
    }

    fn query_ticks(&self, stmt: &Stmt) -> Result<QueryResult, SqlError> {
        let symbol = stmt.symbol.clone().ok_or_else(|| SqlError::Syntax("symbol required".into()))?;
        let symbol_id = map_resolve(self.engine.resolve(&symbol))?;
        let columns = vec!["ts".into(), "price".into(), "volume".into(), "seq".into()];
        let mut rows = Vec::new();
        for tick in self.engine.ticks(symbol_id) {
            if stmt.ts_from.is_some_and(|from| tick.ts < from) || stmt.ts_to.is_some_and(|to| tick.ts > to) {
                continue;
            }
            rows.push(vec![tick.ts.to_string(), tick.price.to_string(), tick.volume.to_string(), tick.seq.to_string()]);
        }
        rows.truncate(stmt.limit as usize);
        Ok(QueryResult { columns, rows })
    }
}

fn aggregate(rows: &[KlineBody], agg: &Agg) -> Result<String, SqlError> {
    if rows.is_empty() {
        return Ok(String::new());
    }
    let nums: Vec<i128> = rows.iter().map(|row| kline_i128(row, &agg.column)).collect();
    let ordered = rows;
    match agg.func.as_str() {
        "max" => Ok(nums.iter().copied().max().unwrap_or(0).to_string()),
        "min" => Ok(nums.iter().copied().min().unwrap_or(0).to_string()),
        "sum" => Ok(nums.iter().sum::<i128>().to_string()),
        "count" => Ok(rows.len().to_string()),
        "avg" => {
            let sum: i128 = nums.iter().sum();
            let count = nums.len() as i128;
            Ok(format_avg(sum, count))
        }
        "first" => Ok(kline_value(&ordered[0], &agg.column)),
        "last" => Ok(kline_value(ordered.last().unwrap(), &agg.column)),
        other => Err(SqlError::Syntax(format!("aggregate {other}"))),
    }
}

fn format_avg(sum: i128, count: i128) -> String {
    if count == 0 {
        return "0".into();
    }
    let whole = sum / count;
    let rem = (sum % count).abs();
    if rem == 0 {
        return whole.to_string();
    }
    let mut digits = String::new();
    let mut rest = rem;
    for _ in 0..4 {
        rest *= 10;
        digits.push(char::from(b'0' + (rest / count) as u8));
        rest %= count;
        if rest == 0 {
            break;
        }
    }
    format!("{whole}.{digits}")
}

fn kline_value(row: &KlineBody, column: &str) -> String {
    match column {
        "ts" | "bucket_start_ns" => row.bucket_start_ns.to_string(),
        "open" => row.open.to_string(),
        "high" => row.high.to_string(),
        "low" => row.low.to_string(),
        "close" => row.close.to_string(),
        "volume" => row.volume.to_string(),
        "amount" => row.amount.to_string(),
        "seq" => "0".into(),
        _ => String::new(),
    }
}

fn kline_i128(row: &KlineBody, column: &str) -> i128 {
    match column {
        "open" => row.open as i128,
        "high" => row.high as i128,
        "low" => row.low as i128,
        "close" => row.close as i128,
        "volume" => row.volume as i128,
        "amount" => row.amount as i128,
        _ => 0,
    }
}

fn map_resolve(result: Result<u32, StoreError>) -> Result<u32, SqlError> {
    match result {
        Ok(id) => Ok(id),
        Err(StoreError::NotFound) => Err(SqlError::NotFound),
        Err(StoreError::Ambiguous) => Err(SqlError::Ambiguous),
        Err(err) => Err(SqlError::Store(err.to_string())),
    }
}

fn tokenize(sql: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch.is_whitespace() {
            continue;
        }
        if ch == '\'' {
            let mut value = String::from("'");
            for inner in chars.by_ref() {
                value.push(inner);
                if inner == '\'' {
                    break;
                }
            }
            tokens.push(value);
            continue;
        }
        if ch == ',' || ch == '(' || ch == ')' || ch == '=' {
            tokens.push(ch.to_string());
            continue;
        }
        let mut word = String::from(ch);
        while let Some(next) = chars.peek() {
            if next.is_whitespace() || *next == ',' || *next == '(' || *next == ')' || *next == '=' {
                break;
            }
            word.push(chars.next().unwrap());
        }
        tokens.push(word);
    }
    tokens
}

fn expect(tokens: &[String], index: &mut usize, word: &str) -> Result<(), SqlError> {
    let got = tokens.get(*index).ok_or_else(|| SqlError::Syntax(word.into()))?;
    if !got.eq_ignore_ascii_case(word) {
        return Err(SqlError::Syntax(format!("expected {word}, got {got}")));
    }
    *index += 1;
    Ok(())
}

fn unquote(value: &str) -> String {
    value.trim_matches('\'').to_string()
}

fn parse_i64(value: &str) -> Result<i64, SqlError> {
    unquote(value).parse().map_err(|_| SqlError::Syntax(value.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdb_abi::TickQuote;

    fn engine() -> (std::path::PathBuf, Engine) {
        let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let dir = std::env::temp_dir().join(format!("qdb-sql-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut engine = Engine::open(&dir).unwrap();
        let base = ts(100_000_000);
        engine.write_trade("SZSE", "000001", base, 10, 100).unwrap();
        engine.write_trade("SZSE", "000001", base + 100_000_000, 12, 50).unwrap();
        engine.write_trade("SZSE", "000001", base + 900_000_000, 11, 10).unwrap();
        engine.write_trade("SSE", "000001", base, 9, 1).unwrap();
        engine
            .write_quote(
                "SZSE",
                "000001",
                base,
                &TickQuote {
                    last: 11,
                    volume: 160,
                    amount: 0,
                    bid_px: [10, 0, 0, 0, 0],
                    bid_qty: [1, 0, 0, 0, 0],
                    ask_px: [12, 0, 0, 0, 0],
                    ask_qty: [1, 0, 0, 0, 0],
                },
            )
            .unwrap();
        (dir, engine)
    }

    fn ts(extra: i64) -> i64 {
        let days = days(2024, 1, 2);
        (days * 86_400 + 9 * 3600 + 30 * 60 - 8 * 3600) * 1_000_000_000 + extra
    }

    fn days(mut y: i32, m: u32, d: u32) -> i64 {
        y -= i32::from(m <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146_097 + doe as i64 - 719_468
    }

    #[test]
    fn example_queries_and_ambiguous_code() {
        let (_dir, engine) = engine();
        let sql = SqlEngine::new(&engine);
        let bars = sql
            .query("SELECT ts, open, high, low, close, volume, amount, seq FROM klines WHERE symbol = 'SZSE:000001' AND period = '1m' AND ts BETWEEN 0 AND 2000000000000000000 ORDER BY ts ASC LIMIT 500")
            .unwrap();
        assert_eq!(bars.rows.len(), 1);
        assert_eq!(bars.rows[0][1], "10");
        assert_eq!(bars.rows[0][4], "11");
        let snap = sql
            .query("SELECT last, bid_px1, ask_px1, seq FROM snapshots WHERE symbol = 'SZSE:000001'")
            .unwrap();
        assert_eq!(snap.rows[0][0], "11");
        assert_eq!(snap.rows[0][1], "10");
        assert_eq!(snap.rows[0][2], "12");
        let agg = sql
            .query("SELECT max(high), min(low), sum(volume), avg(close), first(open), last(close) FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s'")
            .unwrap();
        assert_eq!(agg.rows[0][0], "12");
        assert_eq!(agg.rows[0][1], "10");
        assert_eq!(agg.rows[0][2], "160");
        assert_eq!(agg.rows[0][4], "10");
        assert_eq!(agg.rows[0][5], "11");
        assert!(sql.query("SELECT last FROM snapshots WHERE symbol = '000001'").is_err());
    }
}
