//! 生产负载。必须用 `cargo run --release`。
//!
//! 预热不计入结果。同一负载跑三轮。写入者一个，订阅者只读。
//! 数字随机器和磁盘变化，不能当成别的机器上的合格线。

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
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

struct Dist {
    n: u64,
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
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!("qdb-load 拒绝 debug 构建。请使用 cargo run -p qdb-server --bin qdb-load --release");
        std::process::exit(2);
    }
    println!("qdb-load production profile");
    println!(
        "writer=1 symbols={SYMBOLS} subscribers={SUBSCRIBERS} warmup={WARMUP} measure={MEASURE} runs={RUNS}"
    );
    println!("group_commit=256 records or 1ms");
    println!("run,wall_ms,flush_count,trade_p50_us,trade_p99_us,trade_p999_us,quote_p50_us,quote_p99_us,quote_p999_us,caught_up");
    let mut runs = Vec::with_capacity(RUNS);
    for index in 1..=RUNS {
        let run = one_run(index);
        println!(
            "{index},{},{},{},{},{},{},{},{},{}/{}",
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
    println!(
        "median_run trade_p99_us={} quote_p99_us={} wall_ms={} caught_up={}/{}",
        us(mid.trades.p99_ns),
        us(mid.quotes.p99_ns),
        mid.wall.as_millis(),
        mid.caught_up,
        SUBSCRIBERS
    );
    if runs.iter().any(|run| run.caught_up != SUBSCRIBERS) {
        std::process::exit(1);
    }
}

fn one_run(index: usize) -> Run {
    let dir = std::env::temp_dir().join(format!("qdb-load-{index}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut writer = Engine::open(&dir).unwrap();
    writer.set_group_commit(256, 1_000_000);
    let stop = Arc::new(AtomicBool::new(false));
    let targets: Arc<Vec<AtomicU64>> = Arc::new((0..SYMBOLS).map(|_| AtomicU64::new(0)).collect());
    let mut joins = Vec::with_capacity(SUBSCRIBERS);
    for i in 0..SUBSCRIBERS {
        let dir = dir.clone();
        let stop = Arc::clone(&stop);
        let targets = Arc::clone(&targets);
        let symbol = i % SYMBOLS;
        joins.push(thread::spawn(move || follow(&dir, symbol, &stop, &targets)));
    }
    drive(&mut writer, &targets, WARMUP, false);
    let started = Instant::now();
    let (trades, quotes) = drive(&mut writer, &targets, MEASURE, true);
    writer.flush().unwrap();
    let wall = started.elapsed();
    let flushes = writer.flush_count();
    stop.store(true, Ordering::Release);
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
        trades,
        quotes,
        caught_up,
    }
}

fn drive(
    engine: &mut Engine,
    targets: &[AtomicU64],
    ops: usize,
    record: bool,
) -> (Dist, Dist) {
    let mut trades = Vec::new();
    let mut quotes = Vec::new();
    if record {
        trades.reserve(ops / 2);
        quotes.reserve(ops / 2);
    }
    for step in 0..ops {
        let symbol = step % SYMBOLS;
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
            let seq = engine
                .write_trade(exchange, &code, ts, price, 100)
                .unwrap();
            black_box(seq);
            targets[symbol].fetch_add(1, Ordering::Release);
            if record {
                trades.push(start.elapsed().as_nanos() as u64);
            }
        }
    }
    (dist(trades), dist(quotes))
}

fn follow(dir: &std::path::Path, symbol: usize, stop: &AtomicBool, targets: &[AtomicU64]) -> bool {
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
            n: 0,
            p50_ns: 0,
            p99_ns: 0,
            p999_ns: 0,
            max_ns: 0,
        };
    }
    samples.sort_unstable();
    let last = samples.len() - 1;
    Dist {
        n: samples.len() as u64,
        p50_ns: samples[samples.len() / 2],
        p99_ns: samples[samples.len() * 99 / 100],
        p999_ns: samples[last.min(samples.len() * 999 / 1000)],
        max_ns: samples[last],
    }
}

fn us(ns: u64) -> u64 {
    ns / 1_000
}
