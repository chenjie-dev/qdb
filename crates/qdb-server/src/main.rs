//! 行情进程。一台机器只运行一个实例，退出时卸下共享内存并清掉行情表。

mod cluster;
mod feed;
mod ws;

use std::collections::HashMap;
use std::io::ErrorKind;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use qdb_shm::InstanceLock;
use qdb_store::{replay_bytes, Catalog, Engine, Metrics};

use crate::feed::{
    apply_docs, apply_quotes, apply_trades, doc_json, klines_json, query_param, read_request,
    snapshots_json, write_bytes, write_http, LiveQuote,
};
use serde_json::Value;

struct Databases {
    catalog: Catalog,
    engines: Mutex<HashMap<String, Arc<Mutex<Engine>>>>,
}

impl Databases {
    fn database_path(&self, name: &str) -> Result<std::path::PathBuf, String> {
        self.catalog.path(name).map_err(|err| err.to_string())
    }

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
    let addr = std::env::var("QDB_HTTP_ADDR").unwrap_or_else(|_| "127.0.0.1".into());
    let port: u16 = std::env::var("QDB_HTTP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(9090);
    let listener = TcpListener::bind((addr.as_str(), port)).expect("bind health port");
    listener.set_nonblocking(true).expect("nonblocking");
    eprintln!("qdb listening on {addr}:{port}");
    let advertise = std::env::var("QDB_ADVERTISE").unwrap_or_else(|_| format!("{addr}:{port}"));
    let cluster = match std::env::var("QDB_CLUSTER") {
        Ok(text) => match cluster::parse_cluster(&text, &advertise) {
            Some(map) => Some(map),
            None => {
                eprintln!("QDB_CLUSTER 覆盖不了 16384 个槽，或本节点地址不在表里");
                std::process::exit(1);
            }
        },
        Err(_) => None,
    };
    let acks: Arc<Mutex<HashMap<String, (u64, u64)>>> = Arc::new(Mutex::new(HashMap::new()));
    let follow: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(initial_upstream()));
    if follow.lock().ok().and_then(|slot| slot.clone()).is_some() {
        if let Ok(mut guard) = engine.lock() {
            let epoch = guard.leader_epoch();
            let _ = guard.fence(epoch);
        }
    } else if durability_waits() {
        if let Ok(mut guard) = engine.lock() {
            let _ = guard.arm_visibility_gate();
        }
    }
    let databases = Arc::new(databases);
    let feed = feed::spawn(engine.clone(), live.clone());
    let followed = engine.clone();
    let announced = advertise.clone();
    let followed_slot = follow.clone();
    std::thread::spawn(move || replication_loop(followed_slot, followed, announced));
    loop {
        if qdb_shm::shutdown_requested() {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let _ = stream.set_nonblocking(false);
                let metrics = metrics.clone();
                let databases = databases.clone();
                let live = live.clone();
                let docs = docs.clone();
                let cluster = cluster.clone();
                let acks = acks.clone();
                let follow = follow.clone();
                let advertise = advertise.clone();
                std::thread::spawn(move || {
                    let _ = handle(
                        stream,
                        metrics,
                        databases.as_ref(),
                        live,
                        docs,
                        cluster,
                        acks,
                        follow,
                        advertise,
                    );
                });
            }
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break,
        }
    }
    release_memory(feed, databases.as_ref(), &live, &docs);
}

fn votes_allow_promote(upstream: &str, engine: &Mutex<Engine>, advertise: &str) -> bool {
    let peers = sentinel_peers();
    let quorum = configured_quorum(peers.len());
    let local_offset = engine
        .lock()
        .map(|guard| guard.durable_offset())
        .unwrap_or(0);
    let master = percent_encode(upstream);
    let reports = std::thread::scope(|scope| {
        let mut joins = Vec::new();
        for peer in &peers {
            let master = master.clone();
            joins.push(scope.spawn(move || probe_sentinel(peer, &master)));
        }
        joins
            .into_iter()
            .filter_map(|join| join.join().ok().flatten())
            .collect::<Vec<_>>()
    });
    let down = 1 + reports.iter().filter(|peer| peer.master_down).count() as u32;
    cluster::may_promote(down, quorum, local_offset, advertise, &reports)
}

fn probe_sentinel(peer: &str, master: &str) -> Option<cluster::PeerReport> {
    let body = http_get(peer, &format!("/v1/sentinel?master={master}")).ok()?;
    let value = serde_json::from_slice::<serde_json::Value>(&body).ok()?;
    let master_down = value.get("master_down").and_then(|item| item.as_bool()) == Some(true);
    let offset = value
        .get("offset")
        .and_then(|item| item.as_u64())
        .unwrap_or(0);
    let peer_advertise = value
        .get("advertise")
        .and_then(|item| item.as_str())
        .map(|item| item.to_string())
        .unwrap_or_else(|| host_of(peer));
    Some(cluster::PeerReport {
        advertise: peer_advertise,
        offset,
        master_down,
    })
}

fn sentinel_peers() -> Vec<String> {
    std::env::var("QDB_SENTINEL")
        .unwrap_or_default()
        .split(',')
        .map(|peer| peer.trim().to_string())
        .filter(|peer| !peer.is_empty())
        .collect()
}

fn configured_quorum(peers: usize) -> u32 {
    if let Ok(value) = std::env::var("QDB_QUORUM") {
        if let Ok(quorum) = value.parse() {
            return quorum;
        }
    }
    if peers == 0 {
        1
    } else {
        ((peers as u32) + 1) / 2 + 1
    }
}

fn enough_replicas(acks: &Mutex<HashMap<String, (u64, u64)>>) -> Result<(), String> {
    let min = min_replicas();
    if min == 0 {
        return Ok(());
    }
    let fresh = fresh_ack_offsets(acks).len();
    if fresh < min {
        Err("not enough replicas".into())
    } else {
        Ok(())
    }
}

fn min_replicas() -> usize {
    std::env::var("QDB_MIN_REPLICAS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0)
}

fn replica_lag_ms() -> u64 {
    std::env::var("QDB_REPLICA_LAG_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000)
}

fn durability_waits() -> bool {
    let mode = std::env::var("QDB_DURABILITY").unwrap_or_default();
    mode.eq_ignore_ascii_case("d2") && replica_acks_required() > 0
}

fn replica_acks_required() -> usize {
    let min = min_replicas();
    if min > 0 {
        return min;
    }
    let peers = sentinel_peers().len();
    if peers == 0 {
        return 0;
    }
    let voters = peers + 1;
    voters / 2 + 1 - 1
}

fn fresh_ack_offsets(acks: &Mutex<HashMap<String, (u64, u64)>>) -> Vec<u64> {
    let lag = replica_lag_ms();
    let now = now_ms();
    acks.lock()
        .map(|acks| {
            acks.values()
                .filter(|(_, seen)| now.saturating_sub(*seen) <= lag)
                .map(|(offset, _)| *offset)
                .collect()
        })
        .unwrap_or_default()
}

fn data_root() -> std::path::PathBuf {
    std::path::PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| "data".into()))
}

fn upstream_file() -> std::path::PathBuf {
    data_root().join("upstream.url")
}

/// 数据目录里的 `upstream.url` 优先。文件内容是 `-` 时本进程是主，不再读 `QDB_UPSTREAM`。
fn initial_upstream() -> Option<String> {
    if let Ok(text) = std::fs::read_to_string(upstream_file()) {
        return parse_upstream_file(&text);
    }
    std::env::var("QDB_UPSTREAM")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn parse_upstream_file(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() || text == "-" {
        None
    } else {
        Some(text.to_string())
    }
}

fn save_upstream(upstream: Option<&str>) {
    let path = upstream_file();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let body = match upstream {
        Some(url) => url.to_string(),
        None => "-".into(),
    };
    if let Ok(mut file) = std::fs::File::create(&path) {
        use std::io::Write;
        let _ = file.write_all(body.as_bytes());
        let _ = file.sync_all();
    }
}

fn host_of(url: &str) -> String {
    url.trim_end_matches('/')
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_string()
}

fn percent_encode(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn now_ms() -> u64 {
    now_ns() / 1_000_000
}

fn repl_hex(id: &[u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_repl_id(query: &str) -> Option<[u8; 16]> {
    let text = query_param(query, "repl_id").into_iter().next()?;
    if text.len() != 32 {
        return None;
    }
    let mut id = [0u8; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(id)
}

fn replication_loop(
    follow: Arc<Mutex<Option<String>>>,
    engine: Arc<Mutex<Engine>>,
    advertise: String,
) {
    let limit = failover_limit();
    let mut down_since: Option<std::time::Instant> = None;
    let mut following = String::new();
    let mut promoted_from: Option<String> = None;
    while !qdb_shm::shutdown_requested() {
        if let Some(old) = promoted_from.clone() {
            let redirected = follow.lock().ok().and_then(|slot| slot.clone());
            if redirected.is_some_and(|upstream| !cluster::same_endpoint(&advertise, &upstream)) {
                promoted_from = None;
                down_since = None;
                continue;
            }
            redirect_followers(&old, &advertise);
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        let upstream = follow.lock().ok().and_then(|slot| slot.clone());
        let Some(upstream) = upstream else {
            std::thread::sleep(Duration::from_millis(50));
            continue;
        };
        if upstream != following {
            following = upstream.clone();
            down_since = None;
            eprintln!("qdb following {upstream}");
        }
        let (from, repl_id) = match engine.lock() {
            Ok(guard) => (guard.durable_offset(), guard.repl_id()),
            Err(_) => (0, [0u8; 16]),
        };
        match http_get(
            &upstream,
            &format!("/v1/replicate?from={from}&repl_id={}", repl_hex(&repl_id)),
        ) {
            Ok(body) => {
                down_since = None;
                if let Ok(mut guard) = engine.lock() {
                    if guard.apply_replication(&body).is_ok() {
                        let offset = guard.durable_offset();
                        drop(guard);
                        let _ = http_post(
                            &upstream,
                            &format!(
                                "/v1/ack?offset={offset}&replica={}",
                                percent_encode(&advertise)
                            ),
                        );
                    }
                }
            }
            Err(_) => {
                let started = *down_since.get_or_insert_with(std::time::Instant::now);
                if should_failover(started.elapsed(), limit)
                    && votes_allow_promote(&upstream, &engine, &advertise)
                {
                    let epoch = engine
                        .lock()
                        .map(|guard| guard.leader_epoch().saturating_add(1))
                        .unwrap_or(1);
                    let _ = http_post(&upstream, &format!("/v1/epoch?op=fence&epoch={epoch}"));
                    let promoted = engine.lock().ok().and_then(|mut guard| {
                        guard.promote().ok()?;
                        if durability_waits() {
                            let _ = guard.arm_visibility_gate();
                        }
                        Some(())
                    });
                    if promoted.is_some() {
                        if let Ok(mut slot) = follow.lock() {
                            *slot = None;
                        }
                        save_upstream(None);
                        promoted_from = Some(upstream.clone());
                        eprintln!("qdb promoted to master after upstream failed");
                        redirect_followers(&upstream, &advertise);
                    }
                }
            }
        }
        let pause = if down_since.is_some() { 10 } else { 50 };
        std::thread::sleep(Duration::from_millis(pause));
    }
}

fn redirect_followers(old_upstream: &str, advertise: &str) {
    let me = format!("http://{advertise}");
    let path = format!("/v1/follow?upstream={}", percent_encode(&me));
    let mut targets = sentinel_peers();
    if !old_upstream.is_empty() {
        targets.push(old_upstream.to_string());
    }
    for target in targets {
        if cluster::same_endpoint(advertise, &target) {
            continue;
        }
        if http_post(&target, &path).is_ok() {
            eprintln!("qdb asked {target} to follow {me}");
        }
    }
}

fn accept_follow(
    engine: &Mutex<Engine>,
    follow: &Mutex<Option<String>>,
    advertise: &str,
    upstream: &str,
) -> Result<(), String> {
    if upstream.is_empty() || cluster::same_endpoint(advertise, upstream) {
        return Err("cannot follow self".into());
    }
    let body = http_get(upstream, "/v1/info").map_err(|err| err.to_string())?;
    let value: serde_json::Value = serde_json::from_slice(&body).map_err(|err| err.to_string())?;
    let remote_epoch = value
        .get("epoch")
        .and_then(|item| item.as_u64())
        .unwrap_or(0);
    let remote_master = value.get("role").and_then(|item| item.as_str()) == Some("master");
    let guard = engine.lock().map_err(|_| "engine lock".to_string())?;
    let local_epoch = guard.leader_epoch();
    let local_fenced = guard.is_replica();
    if !cluster::should_follow(
        local_epoch,
        local_fenced,
        remote_epoch,
        remote_master,
        false,
    ) {
        return Err("refusing follow".into());
    }
    drop(guard);
    let mut guard = engine.lock().map_err(|_| "engine lock".to_string())?;
    guard
        .fence(remote_epoch.max(local_epoch))
        .map_err(|err| err.to_string())?;
    drop(guard);
    let mut slot = follow.lock().map_err(|_| "follow lock".to_string())?;
    *slot = Some(upstream.to_string());
    drop(slot);
    save_upstream(Some(upstream));
    Ok(())
}

/// 主节点连续失联达到这个时间就提升自己。0 表示关闭自动切换。
fn failover_limit() -> Duration {
    let millis = std::env::var("QDB_FAILOVER_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(200);
    if millis == 0 {
        Duration::ZERO
    } else {
        Duration::from_millis(millis)
    }
}

fn should_failover(down_for: Duration, limit: Duration) -> bool {
    !limit.is_zero() && down_for >= limit
}

fn http_post(upstream: &str, path: &str) -> std::io::Result<Vec<u8>> {
    http_exchange(upstream, &format!("POST {path} HTTP/1.1\r\nHost: {{host}}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))
}

fn http_get(upstream: &str, path: &str) -> std::io::Result<Vec<u8>> {
    http_exchange(
        upstream,
        &format!("GET {path} HTTP/1.1\r\nHost: {{host}}\r\nConnection: close\r\n\r\n"),
    )
}

fn http_exchange(upstream: &str, template: &str) -> std::io::Result<Vec<u8>> {
    let upstream = upstream.trim_end_matches('/');
    let host = upstream
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let mut stream = connect_host(host)?;
    let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(200)));
    stream.write_all(template.replace("{host}", host).as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let header = buf
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "no http body"))?;
    let head = String::from_utf8_lossy(&buf[..header]);
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);
    if !(200..300).contains(&status) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("upstream status {status}"),
        ));
    }
    Ok(buf[header + 4..].to_vec())
}

fn connect_host(host: &str) -> std::io::Result<TcpStream> {
    let mut last = std::io::Error::new(std::io::ErrorKind::NotFound, "no address");
    let addrs = host.to_socket_addrs()?;
    let mut found = false;
    for addr in addrs {
        found = true;
        match TcpStream::connect_timeout(&addr, Duration::from_millis(100)) {
            Ok(stream) => return Ok(stream),
            Err(err) => last = err,
        }
    }
    if found {
        Err(last)
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no address",
        ))
    }
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
    cluster: Option<cluster::Cluster>,
    acks: Arc<Mutex<HashMap<String, (u64, u64)>>>,
    follow: Arc<Mutex<Option<String>>>,
    advertise: String,
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
    if route == "/v1/cluster" {
        let body = cluster
            .as_ref()
            .map(|map| map.json())
            .unwrap_or_else(|| "{\"slots\":[]}".into());
        return write_http(&mut stream, 200, "application/json", &body);
    }
    if route == "/v1/sentinel" {
        let master = query_param(query, "master")
            .into_iter()
            .next()
            .unwrap_or_default();
        let down = master.is_empty() || http_get(&master, "/v1/info").is_err();
        let guard = match engine.lock() {
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
        return write_http(
            &mut stream,
            200,
            "application/json",
            &format!(
                "{{\"master_down\":{},\"offset\":{},\"epoch\":{},\"advertise\":\"{}\"}}",
                down,
                guard.durable_offset(),
                guard.leader_epoch(),
                advertise
            ),
        );
    }
    if route == "/v1/ack" && req.method == "POST" {
        let offset = query_param(query, "offset")
            .into_iter()
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let replica = query_param(query, "replica")
            .into_iter()
            .next()
            .unwrap_or_else(|| "replica".into());
        if let Ok(mut acks) = acks.lock() {
            acks.insert(replica, (offset, now_ms()));
        }
        if durability_waits() {
            let offsets = fresh_ack_offsets(&acks);
            if let Some(visible) = cluster::acked_offset(replica_acks_required(), &offsets) {
                if let Ok(mut guard) = engine.lock() {
                    let _ = guard.publish_visible(visible);
                }
            }
        }
        return write_http(&mut stream, 200, "application/json", "{\"ok\":true}");
    }
    if req.method == "POST" && (route == "/v1/trades" || route == "/v1/quotes") {
        if let Some(map) = cluster.as_ref() {
            if let Some(moved) = cluster::moved_in_batch(map, &req.body) {
                return write_http(
                    &mut stream,
                    421,
                    "application/json",
                    &format!(
                        "{{\"moved\":\"{}\",\"slot\":{},\"key\":\"{}\"}}",
                        moved.addr, moved.slot, moved.key
                    ),
                );
            }
        }
        if let Err(err) = enough_replicas(&acks) {
            return write_http(
                &mut stream,
                409,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            );
        }
    }
    if route == "/v1/ws" {
        let name = query_param(query, "db")
            .into_iter()
            .next()
            .unwrap_or_else(|| "default".into());
        let path = match databases.database_path(&name) {
            Ok(path) => path,
            Err(err) => {
                return write_http(
                    &mut stream,
                    404,
                    "application/json",
                    &format!("{{\"error\":\"{err}\"}}"),
                );
            }
        };
        std::thread::spawn(move || {
            let _ = ws::accept(stream, &req, &path);
        });
        return Ok(());
    }
    if route == "/v1/replicate" {
        let from = query_param(query, "from")
            .into_iter()
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let repl_id = parse_repl_id(query);
        let blob = {
            let guard = match engine.lock() {
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
            guard.replication_blob(repl_id.as_ref(), from)
        };
        return match blob {
            Ok(bytes) => write_bytes(&mut stream, 200, "application/octet-stream", &bytes),
            Err(err) => {
                let status = if err.to_string().contains("crc") {
                    503
                } else {
                    400
                };
                write_http(
                    &mut stream,
                    status,
                    "application/json",
                    &format!("{{\"error\":\"{err}\"}}"),
                )
            }
        };
    }
    if route == "/v1/promote" && req.method == "POST" {
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
        return match guard.promote() {
            Ok(id) => write_http(
                &mut stream,
                200,
                "application/json",
                &format!(
                    "{{\"role\":\"master\",\"epoch\":{},\"repl_id\":\"{}\"}}",
                    guard.leader_epoch(),
                    repl_hex(&id)
                ),
            ),
            Err(err) => write_http(
                &mut stream,
                400,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            ),
        };
    }
    if route == "/v1/follow" && req.method == "POST" {
        let upstream = query_param(query, "upstream")
            .into_iter()
            .next()
            .unwrap_or_default();
        return match accept_follow(&engine, &follow, &advertise, &upstream) {
            Ok(()) => write_http(&mut stream, 200, "application/json", "{\"ok\":true}"),
            Err(err) => write_http(
                &mut stream,
                409,
                "application/json",
                &format!("{{\"error\":\"{err}\"}}"),
            ),
        };
    }
    if route == "/v1/info" {
        let guard = match engine.lock() {
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
        let role = if guard.is_replica() {
            "replica"
        } else {
            "master"
        };
        let epoch = guard.leader_epoch();
        let repl_id = repl_hex(&guard.repl_id());
        let offset = guard.durable_offset();
        drop(guard);
        let upstream = follow
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_default();
        return write_http(
            &mut stream,
            200,
            "application/json",
            &format!(
                "{{\"role\":\"{role}\",\"epoch\":{epoch},\"repl_id\":\"{repl_id}\",\"offset\":{offset},\"upstream\":\"{upstream}\"}}"
            ),
        );
    }
    if route == "/v1/epoch" && req.method == "POST" {
        let epoch = query_param(query, "epoch")
            .into_iter()
            .next()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let lead = query_param(query, "op")
            .into_iter()
            .next()
            .is_some_and(|op| op == "lead");
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
        let result = if lead {
            guard.lead(epoch)
        } else {
            guard.fence(epoch)
        };
        return match result {
            Ok(()) => write_http(
                &mut stream,
                200,
                "application/json",
                &format!("{{\"epoch\":{}}}", guard.leader_epoch()),
            ),
            Err(err) => {
                let status = if err.to_string().contains("stale epoch") {
                    409
                } else {
                    400
                };
                write_http(
                    &mut stream,
                    status,
                    "application/json",
                    &format!("{{\"error\":\"{err}\"}}"),
                )
            }
        };
    }
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
    fn replica_fails_over_after_the_configured_deadline() {
        assert!(!should_failover(
            Duration::from_millis(100),
            Duration::from_millis(200)
        ));
        assert!(should_failover(
            Duration::from_millis(200),
            Duration::from_millis(200)
        ));
        assert!(!should_failover(Duration::from_secs(5), Duration::ZERO));
    }

    #[test]
    fn upstream_file_dash_means_master_and_a_url_is_kept() {
        assert_eq!(parse_upstream_file("-"), None);
        assert_eq!(parse_upstream_file("  "), None);
        assert_eq!(
            parse_upstream_file("http://qdb-2:9090\n"),
            Some("http://qdb-2:9090".into())
        );
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
