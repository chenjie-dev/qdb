use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdb_store::{Engine, Metrics};

fn main() {
    let root = std::env::args().nth(1).unwrap_or_else(|| "data".into());
    let engine = Engine::open(std::path::Path::new(&root)).expect("open qdb");
    let metrics = engine.metrics();
    let engine = Arc::new(Mutex::new(engine));
    let clock = Arc::clone(&engine);
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));
        let watermark = now_ns() as i64;
        if let Ok(mut engine) = clock.lock() {
            let _ = engine.write_clock("BINANCE", watermark);
        }
    });
    let port: u16 = std::env::var("QDB_HTTP_PORT").ok().and_then(|v| v.parse().ok()).unwrap_or(9090);
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind health port");
    eprintln!("qdb listening on 127.0.0.1:{port}");
    for conn in listener.incoming() {
        if let Ok(stream) = conn {
            let _ = handle(stream, metrics.clone());
        }
    }
}

fn handle(mut stream: TcpStream, metrics: Metrics) -> std::io::Result<()> {
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf)?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let path = req.split_whitespace().nth(1).unwrap_or("/");
    let (status, body) = if path.starts_with("/metrics") {
        (200, metrics_text(&metrics))
    } else {
        let ok = health_ok(&metrics);
        (if ok { 200 } else { 503 }, if ok { "ok\n" } else { "unavailable\n" }.to_string())
    };
    let resp = format!(
        "HTTP/1.1 {status} {}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        if status == 200 { "OK" } else { "Service Unavailable" },
        body.len()
    );
    stream.write_all(resp.as_bytes())
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
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
