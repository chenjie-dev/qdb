mod feed;

use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdb_shm::InstanceLock;
use qdb_store::{replay_bytes, Catalog, Engine, Metrics};

use crate::feed::{
    apply_docs, apply_quotes, apply_trades, doc_json, klines_json, query_param, read_request,
    snapshots_json, write_http, LiveQuote,
};
use serde_json::Value;

struct Databases {
    catalog: Catalog,
    engines: Mutex<HashMap<String, Arc<Mutex<Engine>>>>,
}

impl Databases {
    fn get(&self, name: &str) -> Result<Arc<Mutex<Engine>>, String> {
        let mut engines = self
            .engines
            .lock()
            .map_err(|_| "database lock".to_string())?;
        if let Some(engine) = engines.get(name) {
            return Ok(engine.clone());
        }
        if !self.catalog.exists(name) {
            return Err(format!("database not found: {name}"));
        }
        let engine = Arc::new(Mutex::new(
            self.catalog
                .open_database_with(name, &qdb_shm::RegionConfig::market())
                .map_err(|err| err.to_string())?,
        ));
        engines.insert(name.to_string(), engine.clone());
        Ok(engine)
    }
}

fn main() {
    qdb_shm::watch_shutdown();
    let lock_path =
        std::env::var("QDB_INSTANCE_LOCK").unwrap_or_else(|_| "/tmp/qdb.instance.lock".into());
    let _instance = match InstanceLock::try_acquire(std::path::Path::new(&lock_path)) {
        Ok(lock) => lock,
        Err(_) => {
            eprintln!("本机已有一个 qdb 实例，共享内存只允许运行一个");
            std::process::exit(1);
        }
    };
    let root = std::env::args().nth(1).unwrap_or_else(|| "data".into());
    let catalog = Catalog::open(std::path::Path::new(&root)).expect("open qdb");
    let databases = Databases {
        catalog,
        engines: Mutex::new(HashMap::new()),
    };
    let engine = databases.get("default").expect("open default database");
    let metrics = engine.lock().expect("default database").metrics();
    let live: Arc<Mutex<HashMap<String, LiveQuote>>> = Arc::new(Mutex::new(HashMap::new()));
    let docs: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));
    let feed = feed::spawn(engine, live.clone());
    let addr = std::env::var("QDB_HTTP_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("QDB_HTTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);
    let listener = TcpListener::bind((addr.as_str(), port)).expect("bind health port");
    listener.set_nonblocking(true).expect("nonblocking");
    eprintln!("qdb listening on {addr}:{port}");
    loop {
        if qdb_shm::shutdown_requested() {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nonblocking(false);
                let _ = handle(
                    stream,
                    metrics.clone(),
                    &databases,
                    live.clone(),
                    docs.clone(),
                );
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break,
        }
    }
    release_memory(feed, &databases, &live, &docs);
}

fn release_memory(
    feed: Option<JoinHandle<()>>,
    databases: &Databases,
    live: &Mutex<HashMap<String, LiveQuote>>,
    docs: &Mutex<HashMap<String, Value>>,
) {
    if let Some(feed) = feed {
        let _ = feed.join();
    }
    if let Ok(mut engines) = databases.engines.lock() {
        for engine in engines.values() {
            if let Ok(mut guard) = engine.lock() {
                guard.release();
            }
        }
        engines.clear();
        engines.shrink_to_fit();
    }
    if let Ok(mut live) = live.lock() {
        live.clear();
        live.shrink_to_fit();
    }
    if let Ok(mut docs) = docs.lock() {
        docs.clear();
        docs.shrink_to_fit();
    }
    eprintln!("qdb 已释放内存并退出");
}

fn handle(
    mut stream: TcpStream,
    metrics: Metrics,
    databases: &Databases,
    live: Arc<Mutex<HashMap<String, LiveQuote>>>,
    docs: Arc<Mutex<HashMap<String, Value>>>,
) -> std::io::Result<()> {
    let req = read_request(&mut stream)?;
    let (route, query) = req.path.split_once('?').unwrap_or((req.path.as_str(), ""));
    let engine = match database(databases, query) {
        Ok(engine) => engine,
        Err(err) => {
            return write_http(
                &mut stream,
                404,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            );
        }
    };
    if route == "/v1/replay" && req.method == "POST" {
        let mut guard = match engine.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return write_http(
                    &mut stream,
                    400,
                    "application/json",
                    "{\"error\":\"engine lock\"}",
                );
            }
        };
        return match replay_bytes(&mut guard, &req.body) {
            Ok(changes) => write_http(
                &mut stream,
                200,
                "application/json",
                &format!("{{\"played\":{}}}", changes.len()),
            ),
            Err(err) => write_http(
                &mut stream,
                400,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            ),
        };
    }
    if route == "/v1/changes" {
        let symbol = query_param(query, "symbol").into_iter().next();
        let from_seq = query_param(query, "from_seq")
            .into_iter()
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let limit = query_param(query, "limit")
            .into_iter()
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(1000);
        let guard = match engine.lock() {
            Ok(guard) => guard,
            Err(_) => {
                return write_http(
                    &mut stream,
                    400,
                    "application/json",
                    "{\"error\":\"engine lock\"}",
                )
            }
        };
        let (snapshot_seq, rows) = guard.changes_since(symbol.as_deref(), from_seq, limit);
        let mut body =
            format!("{{\"snapshot_seq\":{snapshot_seq},\"from_seq\":{from_seq},\"changes\":[");
        for (index, row) in rows.iter().enumerate() {
            if index > 0 {
                body.push(',');
            }
            body.push_str(&format!(
                "{{\"seq\":{},\"symbol\":\"{}\",\"kind\":\"{}\",\"ts\":{},\"price\":{},\"volume\":{},\"last\":{},\"bid_px\":[{},{},{},{},{}],\"ask_px\":[{},{},{},{},{}]}}",
                row.seq,
                row.symbol,
                row.kind,
                row.ts,
                row.price,
                row.volume,
                row.last,
                row.bid_px[0], row.bid_px[1], row.bid_px[2], row.bid_px[3], row.bid_px[4],
                row.ask_px[0], row.ask_px[1], row.ask_px[2], row.ask_px[3], row.ask_px[4],
            ));
        }
        body.push_str("]}");
        return write_http(&mut stream, 200, "application/json", &body);
    }
    if route == "/v1/trades" && req.method == "POST" {
        return match apply_trades(&engine, &req.body) {
            Ok(body) => write_http(&mut stream, 200, "application/json", &body),
            Err(err) => write_http(
                &mut stream,
                400,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            ),
        };
    }
    if route == "/v1/docs" && req.method == "POST" {
        return match apply_docs(&docs, &req.body) {
            Ok(body) => write_http(&mut stream, 200, "application/json", &body),
            Err(err) => write_http(
                &mut stream,
                400,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            ),
        };
    }
    if route == "/v1/docs" {
        let key = query_param(query, "key")
            .into_iter()
            .next()
            .unwrap_or_default();
        let body = docs.lock().ok().and_then(|guard| doc_json(&guard, &key));
        return match body {
            Some(body) => write_http(&mut stream, 200, "application/json", &body),
            None => write_http(
                &mut stream,
                404,
                "application/json",
                "{\"error\":\"not found\"}",
            ),
        };
    }
    if route == "/v1/quotes" && req.method == "POST" {
        return match apply_quotes(&engine, &live, &req.body) {
            Ok(body) => write_http(&mut stream, 200, "application/json", &body),
            Err(err) => write_http(
                &mut stream,
                400,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            ),
        };
    }
    if route == "/v1/klines" {
        let symbol = query_param(query, "symbol")
            .into_iter()
            .next()
            .unwrap_or_default();
        let period = query_param(query, "period")
            .into_iter()
            .next()
            .unwrap_or_else(|| "1d".into());
        return match klines_json(&engine, &symbol, &period) {
            Ok(body) => write_http(&mut stream, 200, "application/json", &body),
            Err(err) => write_http(
                &mut stream,
                404,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            ),
        };
    }
    if route == "/v1/snapshots" {
        let symbols = query_param(query, "symbols");
        let body = live
            .lock()
            .map(|guard| snapshots_json(&guard, &symbols))
            .unwrap_or_else(|_| "{\"source\":\"qdb\",\"scale\":10000,\"data\":[]}".into());
        return write_http(&mut stream, 200, "application/json", &body);
    }
    if route.starts_with("/metrics") {
        return write_http(&mut stream, 200, "text/plain", &metrics_text(&metrics));
    }
    let ok = health_ok(&metrics);
    let status = if ok { 200 } else { 503 };
    let body = if ok { "ok\n" } else { "unavailable\n" };
    write_http(&mut stream, status, "text/plain", body)
}

fn database(databases: &Databases, query: &str) -> Result<Arc<Mutex<Engine>>, String> {
    let name = query_param(query, "db")
        .into_iter()
        .next()
        .unwrap_or_else(|| "default".into());
    databases.get(&name)
}

fn health_ok(metrics: &Metrics) -> bool {
    if metrics.crc_failed.load(Ordering::Acquire) || metrics.gap_total.load(Ordering::Acquire) > 0 {
        return false;
    }
    let heartbeat = metrics.heartbeat_ns.load(Ordering::Acquire);
    if heartbeat == 0 {
        return true;
    }
    now_ns().saturating_sub(heartbeat) < 2_000_000_000
}

fn metrics_text(metrics: &Metrics) -> String {
    format!(
        "qdb_durable_seq {}\nqdb_applied_seq {}\nqdb_write_seq {}\nqdb_gap_total {}\nqdb_late_tick_total {}\nqdb_subscriber_count {}\n",
        metrics.durable_seq.load(Ordering::Acquire),
        metrics.applied_seq.load(Ordering::Acquire),
        metrics.write_seq.load(Ordering::Acquire),
        metrics.gap_total.load(Ordering::Acquire),
        metrics.late_tick_total.load(Ordering::Acquire),
        metrics.subscriber_count.load(Ordering::Acquire),
    )
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
    use std::sync::atomic::{AtomicBool, AtomicU64};

    fn metrics() -> Metrics {
        Metrics {
            durable_seq: Arc::new(AtomicU64::new(4)),
            applied_seq: Arc::new(AtomicU64::new(4)),
            write_seq: Arc::new(AtomicU64::new(4)),
            gap_total: Arc::new(AtomicU64::new(0)),
            late_tick_total: Arc::new(AtomicU64::new(0)),
            crc_failed: Arc::new(AtomicBool::new(false)),
            heartbeat_ns: Arc::new(AtomicU64::new(0)),
            subscriber_count: Arc::new(AtomicU64::new(1)),
        }
    }

    #[test]
    fn health_fails_on_crc_gap_or_stale_heartbeat() {
        let fresh = metrics();
        assert!(health_ok(&fresh));
        fresh.crc_failed.store(true, Ordering::Release);
        assert!(!health_ok(&fresh));

        let gapped = metrics();
        gapped.gap_total.store(1, Ordering::Release);
        assert!(!health_ok(&gapped));

        let live = metrics();
        live.heartbeat_ns.store(now_ns(), Ordering::Release);
        assert!(health_ok(&live));
        live.heartbeat_ns
            .store(now_ns().saturating_sub(3_000_000_000), Ordering::Release);
        assert!(!health_ok(&live));
    }

    #[test]
    fn metrics_text_lists_the_sequence_gauges() {
        let text = metrics_text(&metrics());
        assert!(text.contains("qdb_durable_seq 4"));
        assert!(text.contains("qdb_applied_seq 4"));
        assert!(text.contains("qdb_subscriber_count 1"));
    }
}
