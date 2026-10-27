//! 每个库一个跟随线程，把已经 `fsync` 的变更推给全部 WebSocket。
//!
//! 写入线程不参加。同一批变更里，相同的增量 `SELECT` 只投影一次。连接队列默认 4096 条，满了只关掉那一条。

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use qdb_sql::{captured_rows, live_rows, SqlEngine};
use qdb_store::{Change, Engine};

/// 这条连接要更新的进程计数。
#[derive(Clone)]
pub struct Counters {
    /// [`qdb_store::Metrics::subscriber_count`]。
    pub subscriber_count: Arc<std::sync::atomic::AtomicU64>,
}

/// 一条连接的发送队列。跟随线程只往里放文本，连接线程负责写套接字。
pub struct Outbox {
    queue: Mutex<VecDeque<String>>,
    cap: usize,
    closed: AtomicBool,
}

impl Outbox {
    fn new(cap: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            cap,
            closed: AtomicBool::new(false),
        }
    }

    /// 取出下一条要发送的文本。
    pub fn pop(&self) -> Option<String> {
        self.queue
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .pop_front()
    }

    /// 连接结束。跟随线程看到后不再投递。
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    fn push(&self, text: &str) -> bool {
        if self.is_closed() {
            return false;
        }
        let mut queue = self.queue.lock().unwrap_or_else(|err| err.into_inner());
        if queue.len() >= self.cap {
            queue.clear();
            queue.push_back("{\"type\":\"gap\"}".to_string());
            false
        } else {
            queue.push_back(text.to_string());
            true
        }
    }
}

enum Cmd {
    Join {
        sql: String,
        out: Arc<Outbox>,
        counters: Option<Counters>,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    New,
    Waiting,
    Live,
}

struct Live {
    sql: String,
    out: Arc<Outbox>,
    counters: Option<Counters>,
    seen: Vec<Vec<String>>,
    phase: Phase,
    counted: bool,
    dead: bool,
}

static HUBS: Mutex<Option<HashMap<PathBuf, Sender<Cmd>>>> = Mutex::new(None);

/// 登记一条订阅。返回的队列由连接线程排空。
pub fn subscribe(database: &Path, sql: String, counters: Option<Counters>) -> Arc<Outbox> {
    let out = Arc::new(Outbox::new(queue_cap()));
    let tx = hub(database);
    let _ = tx.send(Cmd::Join {
        sql,
        out: Arc::clone(&out),
        counters,
    });
    out
}

fn queue_cap() -> usize {
    std::env::var("QDB_WS_QUEUE")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|cap| *cap >= 1)
        .unwrap_or(4096)
}

fn hub(path: &Path) -> Sender<Cmd> {
    let mut guard = HUBS.lock().unwrap_or_else(|err| err.into_inner());
    let hubs = guard.get_or_insert_with(HashMap::new);
    if let Some(tx) = hubs.get(path) {
        return tx.clone();
    }
    let (tx, rx) = mpsc::channel();
    let owned = path.to_path_buf();
    thread::Builder::new()
        .name("qdb-ws-fanout".into())
        .spawn(move || run(owned, rx))
        .expect("fanout thread");
    hubs.insert(path.to_path_buf(), tx.clone());
    tx
}

fn run(path: PathBuf, rx: Receiver<Cmd>) {
    let mut engine = None;
    let mut subs: Vec<Live> = Vec::new();
    loop {
        if !intake(&rx, &mut subs) {
            release_all(&mut subs);
            return;
        }
        if subs.is_empty() {
            continue;
        }
        if engine.is_none() {
            match Engine::open_read(&path) {
                Ok(opened) => engine = Some(opened),
                Err(err) => {
                    fail_all(&mut subs, &err.to_string());
                    thread::sleep(Duration::from_millis(50));
                    continue;
                }
            }
        }
        let mut failed = false;
        let idle = if let Some(opened) = engine.as_mut() {
            prime(opened, &mut subs);
            match opened.poll() {
                Ok(changes) => {
                    if !changes.is_empty() {
                        deliver(opened, &mut subs, &changes);
                    }
                    wake_waiting(opened, &mut subs);
                    changes.is_empty()
                }
                Err(err) => {
                    fail_all(&mut subs, &err.to_string());
                    failed = true;
                    true
                }
            }
        } else {
            true
        };
        if failed {
            engine = None;
        }
        subs.retain(|sub| {
            if sub.dead || sub.out.is_closed() {
                release(sub);
                false
            } else {
                true
            }
        });
        if idle && !subs.is_empty() {
            thread::sleep(Duration::from_millis(1));
        }
    }
}

fn intake(rx: &Receiver<Cmd>, subs: &mut Vec<Live>) -> bool {
    loop {
        let cmd = if subs.is_empty() {
            match rx.recv_timeout(Duration::from_millis(200)) {
                Ok(cmd) => cmd,
                Err(mpsc::RecvTimeoutError::Timeout) => return true,
                Err(mpsc::RecvTimeoutError::Disconnected) => return false,
            }
        } else {
            match rx.try_recv() {
                Ok(cmd) => cmd,
                Err(mpsc::TryRecvError::Empty) => return true,
                Err(mpsc::TryRecvError::Disconnected) => return false,
            }
        };
        let Cmd::Join { sql, out, counters } = cmd;
        subs.push(Live {
            sql,
            out,
            counters,
            seen: Vec::new(),
            phase: Phase::New,
            counted: false,
            dead: false,
        });
    }
}

fn prime(engine: &Engine, subs: &mut [Live]) {
    for sub in subs.iter_mut().filter(|sub| sub.phase == Phase::New) {
        if sub.out.is_closed() {
            sub.dead = true;
            continue;
        }
        match SqlEngine::new(engine).query(&sub.sql) {
            Ok(result) => {
                if !sub.out.push(&snapshot_json(&result.columns, &result.rows)) {
                    sub.dead = true;
                    continue;
                }
                sub.seen = captured_rows(engine, &sub.sql).unwrap_or(result.rows);
                sub.phase = Phase::Live;
                note(sub);
            }
            Err(err) if err.to_string().contains("symbol not found") => {
                if !sub.out.push("{\"type\":\"waiting\"}") {
                    sub.dead = true;
                    continue;
                }
                sub.phase = Phase::Waiting;
                note(sub);
            }
            Err(err) => {
                let _ = sub.out.push(&error_json(&err.to_string()));
                sub.dead = true;
            }
        }
    }
}

fn wake_waiting(engine: &Engine, subs: &mut [Live]) {
    for sub in subs.iter_mut().filter(|sub| sub.phase == Phase::Waiting) {
        if sub.out.is_closed() {
            sub.dead = true;
            continue;
        }
        if let Ok(result) = SqlEngine::new(engine).query(&sub.sql) {
            if !sub.out.push(&snapshot_json(&result.columns, &result.rows)) {
                sub.dead = true;
                continue;
            }
            sub.seen = captured_rows(engine, &sub.sql).unwrap_or(result.rows);
            sub.phase = Phase::Live;
        }
    }
}

fn deliver(engine: &Engine, subs: &mut [Live], changes: &[Change]) {
    let mut cache: HashMap<String, Result<Vec<Vec<String>>, String>> = HashMap::new();
    for sub in subs
        .iter_mut()
        .filter(|sub| sub.phase == Phase::Live && !sub.dead)
    {
        let rows = if qdb_sql::follows_changes(&sub.sql).unwrap_or(false) {
            let cached = cache.entry(sub.sql.clone()).or_insert_with(|| {
                live_rows(engine, &sub.sql, changes, &[]).map_err(|err| err.to_string())
            });
            match cached {
                Ok(rows) => rows.clone(),
                Err(err) => {
                    let _ = sub.out.push(&error_json(err));
                    sub.dead = true;
                    continue;
                }
            }
        } else {
            match live_rows(engine, &sub.sql, changes, &sub.seen) {
                Ok(rows) => rows,
                Err(err) => {
                    let _ = sub.out.push(&error_json(&err.to_string()));
                    sub.dead = true;
                    continue;
                }
            }
        };
        for row in rows {
            if !sub.out.push(&row_json(&row)) {
                sub.dead = true;
                break;
            }
            sub.seen.push(row);
        }
    }
}

fn fail_all(subs: &mut Vec<Live>, message: &str) {
    let text = error_json(message);
    for sub in subs.drain(..) {
        let _ = sub.out.push(&text);
        release(&sub);
    }
}

fn release_all(subs: &mut Vec<Live>) {
    for sub in subs.drain(..) {
        release(&sub);
    }
}

fn note(sub: &mut Live) {
    if sub.counted {
        return;
    }
    if let Some(counters) = &sub.counters {
        counters.subscriber_count.fetch_add(1, Ordering::Release);
    }
    sub.counted = true;
}

fn release(sub: &Live) {
    if sub.counted {
        if let Some(counters) = &sub.counters {
            counters.subscriber_count.fetch_sub(1, Ordering::Release);
        }
    }
}

fn error_json(message: &str) -> String {
    format!(
        "{{\"type\":\"error\",\"message\":{}}}",
        json_string(message)
    )
}

fn snapshot_json(columns: &[String], rows: &[Vec<String>]) -> String {
    let mut out = String::from("{\"type\":\"snapshot\",\"columns\":[");
    push_strings(&mut out, columns);
    out.push_str("],\"rows\":[");
    for (index, row) in rows.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push('[');
        push_strings(&mut out, row);
        out.push(']');
    }
    out.push_str("]}");
    out
}

fn row_json(values: &[String]) -> String {
    let mut out = String::from("{\"type\":\"row\",\"values\":[");
    push_strings(&mut out, values);
    out.push_str("]}");
    out
}

fn push_strings(out: &mut String, values: &[String]) {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&json_string(value));
    }
}

fn json_string(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::Outbox;

    #[test]
    fn a_full_queue_keeps_only_the_gap() {
        let out = Outbox::new(1);
        assert!(out.push("{\"type\":\"snapshot\"}"));
        assert!(!out.push("{\"type\":\"row\",\"values\":[\"20\"]}"));
        assert_eq!(out.pop().as_deref(), Some("{\"type\":\"gap\"}"));
        assert!(out.pop().is_none());
    }
}
