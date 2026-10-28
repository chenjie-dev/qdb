//! 生产负载。必须用 `cargo run --release`。
//!
//! 预热不计入结果。同一负载跑三轮。数字随机器和磁盘变化，不能当成别的机器上的合格线。
//!
//! `wal` 是对照：128 个订阅各自读 WAL。`fanout` 仍是一个分区，一个跟随线程读 WAL，128 个策略只看内存计数。
//! `exchange` 把上交所和深交所分成两个分区，各一个写入线程、各一个跟随线程。

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use qdb_abi::TickQuote;
use qdb_store::Engine;

const SYMBOLS: usize = 32;
const SUBSCRIBERS: usize = 128;
const WARMUP: usize = 2_000;
const MEASURE: usize = 20_000;
const RUNS: usize = 3;
const OPEN_NS: i64 = 1_704_159_000_000_000_000;

#[derive(Clone, Copy)]
enum Profile {
    /// 128 个只读引擎各自 `poll` WAL。
    Wal,
    /// 一个跟随线程读 WAL，策略只读内存里的成交计数。
    Fanout,
    /// 上交所、深交所各一个写入者和一份 WAL。
    Exchange,
}

struct Dist {
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
}

struct Run {
    wall: Duration,
    flushes: u64,
    trades: Dist,
    quotes: Dist,
    caught_up: usize,
    followers_ok: bool,
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!(
            "qdb-load 拒绝 debug 构建。请使用 cargo run -p qdb-server --bin qdb-load --release"
        );
        std::process::exit(2);
    }
    println!("qdb-load production profile");
    println!(
        "symbols={SYMBOLS} subscribers={SUBSCRIBERS} warmup={WARMUP} measure={MEASURE} runs={RUNS}"
    );
    println!("group_commit=256 records or 1ms");
    println!("run,wall_ms,flush_count,trade_p50_us,trade_p99_us,trade_p999_us,quote_p50_us,quote_p99_us,quote_p999_us,caught_up");
    let mut failed = false;
    failed |= !run_profile("wal", Profile::Wal);
    failed |= !run_profile("fanout", Profile::Fanout);
    failed |= !run_profile("exchange", Profile::Exchange);
    if failed {
        std::process::exit(1);
    }
}

fn run_profile(name: &str, profile: Profile) -> bool {
    println!("profile={name}");
    let mut runs = Vec::with_capacity(RUNS);
    for index in 1..=RUNS {
        let run = one_run(profile, index);
        println!(
            "{name},{index},{},{},{},{},{},{},{},{},{}/{}",
            run.wall.as_millis(),
            run.flushes,
            us(run.trades.p50_ns),
            us(run.trades.p99_ns),
            us(run.trades.p999_ns),
            us(run.quotes.p50_ns),
            us(run.quotes.p99_ns),
            us(run.quotes.p999_ns),
            run.caught_up,
            SUBSCRIBERS
        );
        runs.push(run);
    }
    runs.sort_by_key(|run| run.trades.p99_ns);
    let mid = &runs[RUNS / 2];
    let ops = MEASURE as u128 * 1_000 / mid.wall.as_millis().max(1);
    println!(
        "median {name} trade_p99_us={} quote_p99_us={} trade_max_us={} wall_ms={} flushes={} ops_per_s={} caught_up={}/{} followers_ok={}",
        us(mid.trades.p99_ns),
        us(mid.quotes.p99_ns),
        us(mid.trades.max_ns),
        mid.wall.as_millis(),
        mid.flushes,
        ops,
        mid.caught_up,
        SUBSCRIBERS,
        mid.followers_ok
    );
    runs.iter()
        .all(|run| run.caught_up == SUBSCRIBERS && run.followers_ok)
}

fn one_run(profile: Profile, index: usize) -> Run {
    match profile {
        Profile::Wal => one_database(index, true),
        Profile::Fanout => one_database(index, false),
        Profile::Exchange => two_exchanges(index),
    }
}

fn one_database(index: usize, each_reads_wal: bool) -> Run {
    let dir = std::env::temp_dir().join(format!(
        "qdb-load-{}-{}",
        if each_reads_wal { "wal" } else { "fanout" },
        std::process::id() + index as u32
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut writer = Engine::open(&dir).unwrap();
    writer.set_group_commit(256, 1_000_000);
    let stop = Arc::new(AtomicBool::new(false));
    let targets: Arc<Vec<AtomicU64>> = Arc::new((0..SYMBOLS).map(|_| AtomicU64::new(0)).collect());
    let seen: Arc<Vec<AtomicU64>> = Arc::new((0..SYMBOLS).map(|_| AtomicU64::new(0)).collect());
    let follower = if each_reads_wal {
        None
    } else {
        let dir = dir.clone();
        let stop = Arc::clone(&stop);
        let seen = Arc::clone(&seen);
        Some(thread::spawn(move || {
            follow_wal(&dir, &stop, &seen, &(0..SYMBOLS).collect::<Vec<_>>())
        }))
    };
    let mut joins = Vec::with_capacity(SUBSCRIBERS);
    for i in 0..SUBSCRIBERS {
        let dir = dir.clone();
        let stop = Arc::clone(&stop);
        let targets = Arc::clone(&targets);
        let seen = Arc::clone(&seen);
        let symbol = i % SYMBOLS;
        joins.push(thread::spawn(move || {
            if each_reads_wal {
                follow_self(&dir, symbol, &stop, &targets)
            } else {
                wait_seen(symbol, &stop, &targets, &seen)
            }
        }));
    }
    drive(
        &mut writer,
        &targets,
        &(0..SYMBOLS).collect::<Vec<_>>(),
        WARMUP,
        false,
    );
    let started = Instant::now();
    let (trades, quotes) = drive(
        &mut writer,
        &targets,
        &(0..SYMBOLS).collect::<Vec<_>>(),
        MEASURE,
        true,
    );
    writer.flush().unwrap();
    let wall = started.elapsed();
    let flushes = writer.flush_count();
    stop.store(true, Ordering::Release);
    let followers_ok = follower
        .map(|handle| handle.join().unwrap_or(false))
        .unwrap_or(true);
    let caught_up = joins
        .into_iter()
        .filter_map(|handle| handle.join().ok())
        .filter(|ok| *ok)
        .count();
    drop(writer);
    let _ = std::fs::remove_dir_all(&dir);
    Run {
        wall,
        flushes,
        trades: dist(trades),
        quotes: dist(quotes),
        caught_up,
        followers_ok,
    }
}

fn two_exchanges(index: usize) -> Run {
    let root = std::env::temp_dir().join(format!("qdb-load-exch-{}-{}", std::process::id(), index));
    let _ = std::fs::remove_dir_all(&root);
    let sse_dir = root.join("sse");
    let szse_dir = root.join("szse");
    std::fs::create_dir_all(&sse_dir).unwrap();
    std::fs::create_dir_all(&szse_dir).unwrap();
    let mut sse_writer = Engine::open(&sse_dir).unwrap();
    let mut szse_writer = Engine::open(&szse_dir).unwrap();
    sse_writer.set_group_commit(256, 1_000_000);
    szse_writer.set_group_commit(256, 1_000_000);
    let sse_symbols: Vec<usize> = (0..SYMBOLS).filter(|symbol| symbol % 2 == 0).collect();
    let szse_symbols: Vec<usize> = (0..SYMBOLS).filter(|symbol| symbol % 2 == 1).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let targets: Arc<Vec<AtomicU64>> = Arc::new((0..SYMBOLS).map(|_| AtomicU64::new(0)).collect());
    let seen: Arc<Vec<AtomicU64>> = Arc::new((0..SYMBOLS).map(|_| AtomicU64::new(0)).collect());
    let gate = Arc::new(Barrier::new(2));
    let mut followers = Vec::new();
    for (dir, symbols) in [
        (&sse_dir, sse_symbols.clone()),
        (&szse_dir, szse_symbols.clone()),
    ] {
        let dir = dir.clone();
        let stop = Arc::clone(&stop);
        let seen = Arc::clone(&seen);
        followers.push(thread::spawn(move || {
            follow_wal(&dir, &stop, &seen, &symbols)
        }));
    }
    let mut strategies = Vec::with_capacity(SUBSCRIBERS);
    for i in 0..SUBSCRIBERS {
        let stop = Arc::clone(&stop);
        let targets = Arc::clone(&targets);
        let seen = Arc::clone(&seen);
        let symbol = i % SYMBOLS;
        strategies.push(thread::spawn(move || {
            wait_seen(symbol, &stop, &targets, &seen)
        }));
    }
    let spawn = |mut writer: Engine, symbols: Vec<usize>| {
        let targets = Arc::clone(&targets);
        let gate = Arc::clone(&gate);
        thread::spawn(move || {
            drive(&mut writer, &targets, &symbols, WARMUP / 2, false);
            gate.wait();
            let started = Instant::now();
            let samples = drive(&mut writer, &targets, &symbols, MEASURE / 2, true);
            writer.flush().unwrap();
            let wall = started.elapsed();
            let flushes = writer.flush_count();
            (wall, flushes, samples.0, samples.1)
        })
    };
    let sse = spawn(sse_writer, sse_symbols);
    let szse = spawn(szse_writer, szse_symbols);
    let sse = sse.join().unwrap();
    let szse = szse.join().unwrap();
    stop.store(true, Ordering::Release);
    let followers_ok = followers
        .into_iter()
        .filter_map(|handle| handle.join().ok())
        .all(|ok| ok);
    let caught_up = strategies
        .into_iter()
        .filter_map(|handle| handle.join().ok())
        .filter(|ok| *ok)
        .count();
    let _ = std::fs::remove_dir_all(&root);
    let mut trades = sse.2;
    trades.extend(szse.2);
    let mut quotes = sse.3;
    quotes.extend(szse.3);
    Run {
        wall: sse.0.max(szse.0),
        flushes: sse.1 + szse.1,
        trades: dist(trades),
        quotes: dist(quotes),
        caught_up,
        followers_ok,
    }
}

fn drive(
    engine: &mut Engine,
    targets: &[AtomicU64],
    symbols: &[usize],
    ops: usize,
    record: bool,
) -> (Vec<u64>, Vec<u64>) {
    let mut trades = Vec::new();
    let mut quotes = Vec::new();
    if record {
        trades.reserve(ops / 2);
        quotes.reserve(ops / 2);
    }
    for step in 0..ops {
        let symbol = symbols[step % symbols.len()];
        let (exchange, code) = symbol_of(symbol);
        let ts = OPEN_NS + step as i64 * 1_000;
        let price = 100_000 + (step % 50) as i64 * 100;
        let start = Instant::now();
        if step % 2 == 0 {
            let seq = engine
                .write_quote(exchange, &code, ts, &quote(price))
                .unwrap();
            black_box(seq);
            if record {
                quotes.push(start.elapsed().as_nanos() as u64);
            }
        } else {
            let seq = engine.write_trade(exchange, &code, ts, price, 100).unwrap();
            black_box(seq);
            targets[symbol].fetch_add(1, Ordering::Release);
            if record {
                trades.push(start.elapsed().as_nanos() as u64);
            }
        }
    }
    (trades, quotes)
}

fn follow_self(
    dir: &std::path::Path,
    symbol: usize,
    stop: &AtomicBool,
    targets: &[AtomicU64],
) -> bool {
    let mut reader = match Engine::open_read(dir) {
        Ok(reader) => reader,
        Err(_) => return false,
    };
    let (exchange, code) = symbol_of(symbol);
    let key = format!("{exchange}:{code}");
    loop {
        if reader.poll().is_err() {
            return false;
        }
        if stop.load(Ordering::Acquire) {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    let need = targets[symbol].load(Ordering::Acquire) as usize;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if reader.poll().is_err() {
            return false;
        }
        if reader
            .resolve(&key)
            .ok()
            .is_some_and(|id| reader.ticks(id).len() >= need)
        {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn follow_wal(
    dir: &std::path::Path,
    stop: &AtomicBool,
    seen: &[AtomicU64],
    symbols: &[usize],
) -> bool {
    let mut reader = match wait_open(dir, stop) {
        Some(reader) => reader,
        None => return false,
    };
    loop {
        if !publish_counts(&mut reader, seen, symbols) {
            return false;
        }
        if stop.load(Ordering::Acquire) {
            return publish_counts(&mut reader, seen, symbols);
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn wait_open(dir: &std::path::Path, stop: &AtomicBool) -> Option<Engine> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(reader) = Engine::open_read(dir) {
            return Some(reader);
        }
        if stop.load(Ordering::Acquire) || Instant::now() > deadline {
            return None;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn publish_counts(reader: &mut Engine, seen: &[AtomicU64], symbols: &[usize]) -> bool {
    if reader.poll().is_err() {
        return false;
    }
    for (place, symbol) in symbols.iter().copied().enumerate() {
        let id = (place as u32) + 1;
        let n = reader.ticks(id).len() as u64;
        seen[symbol].store(n, Ordering::Release);
    }
    true
}

fn wait_seen(symbol: usize, stop: &AtomicBool, targets: &[AtomicU64], seen: &[AtomicU64]) -> bool {
    while !stop.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(1));
    }
    let need = targets[symbol].load(Ordering::Acquire);
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if seen[symbol].load(Ordering::Acquire) >= need {
            return true;
        }
        if Instant::now() > deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn symbol_of(index: usize) -> (&'static str, String) {
    if index % 2 == 0 {
        ("SSE", format!("{:06}", 600_000 + index / 2))
    } else {
        ("SZSE", format!("{:06}", 1 + index / 2))
    }
}

fn quote(last: i64) -> TickQuote {
    TickQuote {
        last,
        volume: 0,
        amount: 0,
        bid_px: [last - 100, last - 200, last - 300, last - 400, last - 500],
        bid_qty: [100, 100, 100, 100, 100],
        ask_px: [last + 100, last + 200, last + 300, last + 400, last + 500],
        ask_qty: [100, 100, 100, 100, 100],
    }
}

fn dist(mut samples: Vec<u64>) -> Dist {
    if samples.is_empty() {
        return Dist {
            p50_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            max_ns: 0,
        };
    }
    samples.sort_unstable();
    let last = samples.len() - 1;
    Dist {
        p50_ns: samples[samples.len() / 2],
        p99_ns: samples[samples.len() * 99 / 100],
        p999_ns: samples[last.min(samples.len() * 999 / 1000)],
        max_ns: samples[last],
    }
}

fn us(ns: u64) -> u64 {
    ns / 1_000
}
