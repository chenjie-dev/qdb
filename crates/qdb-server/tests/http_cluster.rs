//! 本机进程上的复制、槽位和切换时限。不连交易所。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

static NEXT_PORT: AtomicU16 = AtomicU16::new(0);

struct Node {
    child: Child,
    port: u16,
    dir: std::path::PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn port() -> u16 {
    22_000 + (std::process::id() as u16 % 8_000) + NEXT_PORT.fetch_add(1, Ordering::Relaxed)
}

fn start(name: &str, port: u16, extra: &[(&str, &str)]) -> Node {
    let dir = std::env::temp_dir().join(format!("qdb-http-{name}-{}-{port}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let lock = dir.join("instance.lock");
    let mut command = Command::new(env!("CARGO_BIN_EXE_qdb"));
    command
        .arg(&dir)
        .env("QDB_FEED", "0")
        .env("QDB_HTTP_ADDR", "127.0.0.1")
        .env("QDB_HTTP_PORT", port.to_string())
        .env("QDB_INSTANCE_LOCK", &lock)
        .env("QDB_ADVERTISE", format!("127.0.0.1:{port}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in extra {
        command.env(key, value);
    }
    let child = command.spawn().unwrap();
    let node = Node { child, port, dir };
    wait_until_listening(node.port);
    node
}

fn wait_until_listening(port: u16) {
    let addr = format!("127.0.0.1:{port}");
    for _ in 0..100 {
        if TcpStream::connect(&addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("qdb did not listen on {addr}");
}

fn exchange(port: u16, method: &str, path: &str, body: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(head.as_bytes()).unwrap();
    let mut buf = Vec::new();
    let _ = stream.read_to_end(&mut buf);
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

fn info(port: u16) -> serde_json::Value {
    let (status, body) = exchange(port, "GET", "/v1/info", "");
    assert_eq!(status, 200, "{body}");
    serde_json::from_str(&body).unwrap()
}

#[test]
fn cluster_routes_some_keys_away_and_replicate_returns_the_wal_magic() {
    let chosen = port();
    let advertise = format!("127.0.0.1:{chosen}");
    let cluster = format!("{advertise}=0-8191,10.255.255.1:9=8192-16383");
    let node = start(
        "slots",
        chosen,
        &[
            ("QDB_CLUSTER", cluster.as_str()),
            ("QDB_ADVERTISE", advertise.as_str()),
        ],
    );
    // 键是 `交易所:代码`，槽号要落在 8192 之后才返回 421。
    let mut saw_moved = false;
    let mut saw_local = false;
    for code in 0..200 {
        let (status, body) = exchange(
            node.port,
            "POST",
            "/v1/trades",
            &format!(
                r#"{{"items":[{{"exchange":"SSE","code":"{code}","ts_ns":1,"price":1,"volume":1}}]}}"#
            ),
        );
        if status == 421 {
            assert!(body.contains("10.255.255.1:9"), "{body}");
            saw_moved = true;
        } else if status == 200 {
            saw_local = true;
        } else {
            panic!("unexpected {status} {body}");
        }
        if saw_moved && saw_local {
            break;
        }
    }
    assert!(saw_moved, "no key was routed away");
    assert!(saw_local, "no key stayed on this node");
    let (bad, _) = exchange(node.port, "POST", "/v1/trades", "not-json");
    assert_eq!(bad, 400);
    let (status, blob) = exchange(node.port, "GET", "/v1/replicate?from=0", "");
    assert_eq!(status, 200);
    assert!(blob.as_bytes().starts_with(b"QDBREPL2"), "{blob:?}");
    let (status, map) = exchange(node.port, "GET", "/v1/cluster", "");
    assert_eq!(status, 200, "{map}");
    assert!(map.contains("16383"), "{map}");
}

#[test]
fn replica_catches_the_offset_and_promotes_within_two_seconds() {
    let master = start("master", port(), &[]);
    let upstream = format!("http://127.0.0.1:{}", master.port);
    let replica = start(
        "replica",
        port(),
        &[
            ("QDB_UPSTREAM", upstream.as_str()),
            ("QDB_FAILOVER_MS", "200"),
        ],
    );
    let (status, body) = exchange(
        master.port,
        "POST",
        "/v1/trades",
        r#"{"items":[{"exchange":"SSE","code":"600000","ts_ns":1,"price":10,"volume":1}]}"#,
    );
    assert_eq!(status, 200, "{body}");
    let master_offset = info(master.port)["offset"].as_u64().unwrap();
    assert!(master_offset > 0);
    let mut caught = false;
    for _ in 0..50 {
        let replica_offset = info(replica.port)["offset"].as_u64().unwrap_or(0);
        if replica_offset == master_offset {
            caught = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(40));
    }
    assert!(caught, "replica did not reach offset {master_offset}");
    let killed = Instant::now();
    drop(master);
    let mut promoted = None;
    for _ in 0..40 {
        let body = info(replica.port);
        if body["role"] == "master" {
            promoted = Some(body);
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let elapsed = killed.elapsed();
    let body = promoted.expect("replica did not promote");
    assert!(
        elapsed < Duration::from_secs(2),
        "promotion took {elapsed:?}"
    );
    assert!(body["epoch"].as_u64().unwrap() >= 2);
    assert_eq!(body["upstream"], "");
}
