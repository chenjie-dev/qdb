//! 可选的进程内采集，以及 HTTP 读写用的快照缓存。
//! Compose 里采集在 EquityLab collector，本进程用 `QDB_FEED=0` 只接收写入。

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdb_abi::{Period, TickQuote};
use qdb_store::Engine;
use serde::Deserialize;
use serde_json::Value;

pub const PRICE_SCALE: i64 = 10_000;

#[derive(Clone, Debug)]
pub struct LiveQuote {
    pub symbol: String,
    pub exchange: String,
    pub code: String,
    pub name: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub pre_close: f64,
    pub price: f64,
    pub volume: f64,
    pub amount: f64,
    pub seq: u64,
}

pub fn spawn(
    engine: Arc<Mutex<Engine>>,
    live: Arc<Mutex<HashMap<String, LiveQuote>>>,
) -> Option<thread::JoinHandle<()>> {
    if std::env::var("QDB_FEED").ok().as_deref() == Some("0") {
        eprintln!("qdb feed disabled");
        return None;
    }
    let codes = std::env::var("QDB_FEED_CODES").unwrap_or_else(|_| default_codes());
    let interval_ms: u64 = std::env::var("QDB_FEED_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    Some(thread::spawn(move || {
        let mut last_vol: HashMap<String, i64> = HashMap::new();
        while !qdb_shm::shutdown_requested() {
            if let Err(err) = pull_once(&codes, &engine, &live, &mut last_vol) {
                eprintln!("qdb feed: {err}");
            }
            let mut waited = 0;
            while waited < interval_ms && !qdb_shm::shutdown_requested() {
                thread::sleep(Duration::from_millis(50.min(interval_ms - waited)));
                waited += 50;
            }
        }
    }))
}

fn default_codes() -> String {
    "600519,000001,300750,601318,000858,002594,600036,601012,688981,510300".into()
}

fn pull_once(
    codes: &str,
    engine: &Mutex<Engine>,
    live: &Mutex<HashMap<String, LiveQuote>>,
    last_vol: &mut HashMap<String, i64>,
) -> Result<(), String> {
    let list = sina_list(codes);
    if list.is_empty() {
        return Ok(());
    }
    let body = http_get(&format!("https://hq.sinajs.cn/list={list}"))?;
    let rows = parse_sina_body(&body);
    let ts = now_ns() as i64;
    let mut guard = engine.lock().map_err(|_| "engine lock".to_string())?;
    let mut book = live.lock().map_err(|_| "live lock".to_string())?;
    for row in rows {
        let price_i = scale_price(row.price);
        if price_i <= 0 {
            continue;
        }
        let vol_i = row.volume.round() as i64;
        let prev = last_vol.get(&row.symbol).copied().unwrap_or(0);
        let delta = if vol_i > prev { vol_i - prev } else { 0 };
        let quote = TickQuote {
            last: price_i,
            volume: vol_i.max(0),
            amount: scale_price(row.amount),
            bid_px: [0; 5],
            bid_qty: [0; 5],
            ask_px: [0; 5],
            ask_qty: [0; 5],
        };
        let seq = guard
            .write_quote(&row.exchange, &row.code, ts, &quote)
            .map_err(|e| e.to_string())?;
        if delta > 0 {
            guard
                .write_trade(&row.exchange, &row.code, ts, price_i, delta)
                .map_err(|e| e.to_string())?;
        }
        last_vol.insert(row.symbol.clone(), vol_i.max(prev));
        book.insert(
            row.symbol.clone(),
            LiveQuote {
                symbol: row.symbol,
                exchange: row.exchange,
                code: row.code,
                name: row.name,
                open: row.open,
                high: row.high,
                low: row.low,
                pre_close: row.pre_close,
                price: row.price,
                volume: row.volume,
                amount: row.amount,
                seq,
            },
        );
    }
    guard.flush().map_err(|err| err.to_string())?;
    Ok(())
}

struct Parsed {
    symbol: String,
    exchange: String,
    code: String,
    name: String,
    open: f64,
    pre_close: f64,
    price: f64,
    high: f64,
    low: f64,
    volume: f64,
    amount: f64,
}

fn parse_sina_body(body: &str) -> Vec<Parsed> {
    let mut out = Vec::new();
    for line in body.lines() {
        let Some(rest) = line.split("hq_str_").nth(1) else {
            continue;
        };
        let Some((sym, quoted)) = rest.split_once('=') else {
            continue;
        };
        let sym = sym.trim();
        let (exchange, code) = match sym.get(..2) {
            Some("sh") => ("SSE", &sym[2..]),
            Some("sz") => ("SZSE", &sym[2..]),
            _ => continue,
        };
        if code.len() < 6 {
            continue;
        }
        let code = &code[..6];
        let Some(start) = quoted.find('"') else {
            continue;
        };
        let Some(end) = quoted[start + 1..].find('"') else {
            continue;
        };
        let payload = &quoted[start + 1..start + 1 + end];
        if payload.is_empty() {
            continue;
        }
        let parts: Vec<&str> = payload.split(',').collect();
        if parts.len() < 10 {
            continue;
        }
        out.push(Parsed {
            symbol: format!("{exchange}:{code}"),
            exchange: exchange.to_string(),
            code: code.to_string(),
            name: parts[0].to_string(),
            open: parts[1].parse().unwrap_or(0.0),
            pre_close: parts[2].parse().unwrap_or(0.0),
            price: parts[3].parse().unwrap_or(0.0),
            high: parts[4].parse().unwrap_or(0.0),
            low: parts[5].parse().unwrap_or(0.0),
            volume: parts[8].parse().unwrap_or(0.0),
            amount: parts[9].parse().unwrap_or(0.0),
        });
    }
    out
}

fn sina_list(codes: &str) -> String {
    codes
        .split(|c: char| c == ',' || c.is_whitespace())
        .filter(|c| !c.is_empty())
        .map(|raw| {
            let code = raw
                .chars()
                .filter(|ch| ch.is_ascii_digit())
                .collect::<String>();
            let code = if code.len() >= 6 {
                code[code.len() - 6..].to_string()
            } else {
                return String::new();
            };
            if code.starts_with(['5', '6', '9']) {
                format!("sh{code}")
            } else {
                format!("sz{code}")
            }
        })
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

fn http_get(url: &str) -> Result<String, String> {
    let out = Command::new("curl")
        .args([
            "-fsS",
            "--max-time",
            "8",
            "-H",
            "Referer: https://finance.sina.com.cn",
            "-H",
            "User-Agent: qdb-feed",
            url,
        ])
        .output()
        .map_err(|e| format!("curl: {e}"))?;
    if !out.status.success() {
        return Err(format!("curl status {}", out.status));
    }
    // 新浪行情正文是 GBK。
    let mut child = Command::new("iconv")
        .args(["-f", "GB18030", "-t", "UTF-8"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("iconv: {e}"))?;
    child
        .stdin
        .take()
        .unwrap()
        .write_all(&out.stdout)
        .map_err(|e| e.to_string())?;
    let converted = child.wait_with_output().map_err(|e| e.to_string())?;
    if !converted.status.success() {
        return Err(format!("iconv status {}", converted.status));
    }
    String::from_utf8(converted.stdout).map_err(|e| e.to_string())
}

pub fn scale_price(v: f64) -> i64 {
    (v * PRICE_SCALE as f64).round() as i64
}

pub fn snapshots_json(live: &HashMap<String, LiveQuote>, symbols: &[String]) -> String {
    let rows: Vec<&LiveQuote> = if symbols.is_empty() {
        live.values().collect()
    } else {
        symbols.iter().filter_map(|s| live.get(s)).collect()
    };
    let mut body = String::from("{\"source\":\"qdb\",\"scale\":10000,\"data\":[");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(
            "{{\"symbol\":\"{}\",\"exchange\":\"{}\",\"code\":\"{}\",\"name\":\"{}\",\"price\":{},\"open\":{},\"high\":{},\"low\":{},\"pre_close\":{},\"volume\":{},\"amount\":{},\"seq\":{}}}",
            row.symbol,
            row.exchange,
            row.code,
            json_escape(&row.name),
            num(row.price),
            num(row.open),
            num(row.high),
            num(row.low),
            num(row.pre_close),
            num(row.volume),
            num(row.amount),
            row.seq,
        ));
    }
    body.push_str("]}");
    body
}

fn num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

pub fn query_param(query: &str, key: &str) -> Vec<String> {
    for pair in query.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        if k == key {
            return url_decode(v)
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
    Vec::new()
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub fn write_bytes(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        421 => "Misdirected Request",
        _ => "Service Unavailable",
    };
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(resp.as_bytes())?;
    stream.write_all(body)
}

pub fn write_http(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    write_bytes(stream, status, content_type, body.as_bytes())
}

pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub body: Vec<u8>,
    pub headers: HashMap<String, String>,
}

pub fn read_request(stream: &mut TcpStream) -> std::io::Result<HttpRequest> {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        let n = stream.read(&mut tmp)?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(header_end) = find_header_end(&buf) {
            let header = String::from_utf8_lossy(&buf[..header_end]).into_owned();
            let mut body = buf[header_end + 4..].to_vec();
            let len = content_length(&header);
            while body.len() < len {
                let n = stream.read(&mut tmp)?;
                if n == 0 {
                    break;
                }
                body.extend_from_slice(&tmp[..n]);
            }
            if body.len() > len {
                body.truncate(len);
            }
            let mut lines = header.lines();
            let request_line = lines.next().unwrap_or("");
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("GET").to_string();
            let path = parts.next().unwrap_or("/").to_string();
            return Ok(HttpRequest {
                method,
                path,
                body,
                headers: header_map(&header),
            });
        }
        if buf.len() > 2_000_000 {
            break;
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "incomplete http request",
    ))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

fn header_map(header: &str) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    for line in header.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }
    headers
}

fn content_length(header: &str) -> usize {
    for line in header.lines() {
        let Some((k, v)) = line.split_once(':') else {
            continue;
        };
        if k.eq_ignore_ascii_case("content-length") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}

#[derive(Deserialize)]
struct TradeIn {
    exchange: String,
    code: String,
    ts_ns: i64,
    price: i64,
    volume: i64,
}

#[derive(Deserialize)]
struct TradeBatch {
    items: Vec<TradeIn>,
}

#[derive(Deserialize)]
struct QuoteIn {
    exchange: String,
    code: String,
    ts_ns: i64,
    last: i64,
    #[serde(default)]
    bid_px: i64,
    #[serde(default)]
    ask_px: i64,
    name: Option<String>,
    #[serde(default)]
    open: f64,
    #[serde(default)]
    high: f64,
    #[serde(default)]
    low: f64,
    #[serde(default)]
    pre_close: f64,
    #[serde(default)]
    price: f64,
    #[serde(default)]
    volume: f64,
    #[serde(default)]
    amount: f64,
}

#[derive(Deserialize)]
struct QuoteBatch {
    items: Vec<QuoteIn>,
}

pub fn apply_trades(engine: &Mutex<Engine>, body: &[u8]) -> Result<String, String> {
    let batch: TradeBatch = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let mut guard = engine.lock().map_err(|_| "engine lock".to_string())?;
    let mut n = 0u64;
    for item in &batch.items {
        guard
            .write_trade(
                &item.exchange,
                &item.code,
                item.ts_ns,
                item.price,
                item.volume,
            )
            .map_err(|e| e.to_string())?;
        n += 1;
    }
    guard.flush().map_err(|e| e.to_string())?;
    Ok(format!("{{\"written\":{n}}}"))
}

pub fn apply_quotes(
    engine: &Mutex<Engine>,
    live: &Mutex<HashMap<String, LiveQuote>>,
    body: &[u8],
) -> Result<String, String> {
    let batch: QuoteBatch = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let mut guard = engine.lock().map_err(|_| "engine lock".to_string())?;
    let mut book = live.lock().map_err(|_| "live lock".to_string())?;
    let mut n = 0u64;
    for item in &batch.items {
        let quote = TickQuote {
            last: item.last,
            volume: item.volume.round() as i64,
            amount: scale_price(item.amount),
            bid_px: [item.bid_px, 0, 0, 0, 0],
            bid_qty: [0; 5],
            ask_px: [item.ask_px, 0, 0, 0, 0],
            ask_qty: [0; 5],
        };
        let seq = guard
            .write_quote(&item.exchange, &item.code, item.ts_ns, &quote)
            .map_err(|e| e.to_string())?;
        let symbol = format!("{}:{}", item.exchange, item.code);
        let price = if item.price > 0.0 {
            item.price
        } else {
            item.last as f64 / PRICE_SCALE as f64
        };
        book.insert(
            symbol.clone(),
            LiveQuote {
                symbol,
                exchange: item.exchange.clone(),
                code: item.code.clone(),
                name: item.name.clone().unwrap_or_default(),
                open: item.open,
                high: item.high,
                low: item.low,
                pre_close: item.pre_close,
                price,
                volume: item.volume,
                amount: item.amount,
                seq,
            },
        );
        n += 1;
    }
    guard.flush().map_err(|e| e.to_string())?;
    Ok(format!("{{\"written\":{n}}}"))
}

#[derive(Deserialize)]
struct DocIn {
    key: String,
    value: Value,
}

#[derive(Deserialize)]
struct DocBatch {
    items: Vec<DocIn>,
}

pub fn apply_docs(docs: &Mutex<HashMap<String, Value>>, body: &[u8]) -> Result<String, String> {
    let batch: DocBatch = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let mut guard = docs.lock().map_err(|_| "docs lock".to_string())?;
    let n = batch.items.len();
    for item in batch.items {
        guard.insert(item.key, item.value);
    }
    Ok(format!("{{\"written\":{n}}}"))
}

pub fn doc_json(docs: &HashMap<String, Value>, key: &str) -> Option<String> {
    docs.get(key).map(|v| v.to_string())
}

pub fn klines_json(
    engine: &Mutex<Engine>,
    symbol: &str,
    period_name: &str,
) -> Result<String, String> {
    let period =
        Period::parse(period_name).ok_or_else(|| format!("unknown period {period_name}"))?;
    let guard = engine.lock().map_err(|_| "engine lock".to_string())?;
    let id = guard.resolve(symbol).map_err(|e| e.to_string())?;
    let rows = guard.klines(id, period);
    let mut body = format!(
        "{{\"symbol\":\"{symbol}\",\"period\":\"{}\",\"scale\":{PRICE_SCALE},\"data\":[",
        period.as_str()
    );
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            body.push(',');
        }
        body.push_str(&format!(
            "{{\"ts\":{},\"open\":{},\"high\":{},\"low\":{},\"close\":{},\"volume\":{},\"state\":{}}}",
            row.bucket_start_ns,
            px(row.open),
            px(row.high),
            px(row.low),
            px(row.close),
            row.volume,
            row.state,
        ));
    }
    body.push_str("]}");
    Ok(body)
}

fn px(v: i64) -> String {
    let whole = v / PRICE_SCALE;
    let frac = (v % PRICE_SCALE).abs();
    if frac == 0 {
        format!("{whole}")
    } else {
        format!("{whole}.{frac:04}")
    }
}

fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_sina_snapshot_into_exchange_symbol() {
        let body = "var hq_str_sh600519=\"贵州茅台,1320.00,1310.00,1329.50,1330.00,1315.00,0,0,100,200\";\n";
        let rows = parse_sina_body(body);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "SSE:600519");
        assert_eq!(rows[0].price, 1329.50);
        assert_eq!(scale_price(rows[0].price), 13_295_000);
    }

    #[test]
    fn snapshot_json_filters_requested_symbols() {
        let mut live = HashMap::new();
        live.insert(
            "SSE:600519".into(),
            LiveQuote {
                symbol: "SSE:600519".into(),
                exchange: "SSE".into(),
                code: "600519".into(),
                name: "贵州茅台".into(),
                open: 1.0,
                high: 2.0,
                low: 0.5,
                pre_close: 1.0,
                price: 1.5,
                volume: 10.0,
                amount: 15.0,
                seq: 7,
            },
        );
        let body = snapshots_json(&live, &["SZSE:000001".into()]);
        assert!(body.contains("\"data\":[]"));
        let body = snapshots_json(&live, &["SSE:600519".into()]);
        assert!(body.contains("\"seq\":7"));
        assert!(body.contains("贵州茅台"));
    }

    #[test]
    fn docs_round_trip_returns_stored_json() {
        let docs = Mutex::new(HashMap::new());
        let written = apply_docs(
            &docs,
            br#"{"items":[{"key":"binance:ticker:BTCUSDT","value":{"lastPrice":"1.5"}}]}"#,
        )
        .unwrap();
        assert!(written.contains("\"written\":1"));
        let body = doc_json(&docs.lock().unwrap(), "binance:ticker:BTCUSDT").unwrap();
        assert!(body.contains("\"lastPrice\":\"1.5\""));
        assert!(doc_json(&docs.lock().unwrap(), "missing").is_none());
    }
}
