use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use qdb_abi::Period;
use qdb_store::{Cell, ColumnDef, ColumnType, Engine, IndexDef, StoreError, TableSchema};

static TEMP: AtomicU64 = AtomicU64::new(0);

fn temp(name: &str) -> std::path::PathBuf {
    let n = TEMP.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("qdb-bound-{name}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn utc(minute: i64) -> i64 {
    // 2024-01-02 00:00:00 UTC，再加上 minute 分钟。
    let days = days(2024, 1, 2);
    (days * 86_400 + minute * 60) * 1_000_000_000
}

fn days(mut y: i32, m: u32, d: u32) -> i64 {
    y -= i32::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146_097 + doe as i64 - 719_468
}

fn col(name: &str, ty: ColumnType, primary_key: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        primary_key,
    }
}

#[test]
fn user_table_rejects_reserved_names_long_strings_and_duplicate_keys() {
    let dir = temp("table");
    let mut engine = Engine::open(&dir).unwrap();
    let reserved = TableSchema {
        name: "ticks".into(),
        columns: vec![col("n", ColumnType::Int64, true)],
        indexes: Vec::new(),
    };
    assert!(matches!(
        engine.create_table(reserved).unwrap_err(),
        StoreError::Schema(_)
    ));
    let schema = TableSchema {
        name: "notes".into(),
        columns: vec![
            col("id", ColumnType::Int64, true),
            col("body", ColumnType::String, false),
            col("tag", ColumnType::String, false),
        ],
        indexes: vec![IndexDef {
            name: "idx_tag".into(),
            column: "tag".into(),
        }],
    };
    engine.create_table(schema).unwrap();
    assert!(matches!(
        engine
            .create_table(TableSchema {
                name: "notes".into(),
                columns: vec![col("id", ColumnType::Int64, true)],
                indexes: Vec::new(),
            })
            .unwrap_err(),
        StoreError::TableExists
    ));
    let ok = "x".repeat(1024);
    engine
        .insert_row(
            "notes",
            &[
                Cell::Int64(1),
                Cell::String(ok.clone()),
                Cell::String("a".into()),
            ],
        )
        .unwrap();
    assert!(matches!(
        engine
            .insert_row(
                "notes",
                &[
                    Cell::Int64(2),
                    Cell::String(format!("{ok}y")),
                    Cell::String("b".into()),
                ],
            )
            .unwrap_err(),
        StoreError::Schema(_)
    ));
    assert!(matches!(
        engine
            .insert_row(
                "notes",
                &[
                    Cell::Int64(1),
                    Cell::String("z".into()),
                    Cell::String("b".into())
                ],
            )
            .unwrap_err(),
        StoreError::DuplicateKey
    ));
    assert!(matches!(
        engine
            .insert_row(
                "notes",
                &[
                    Cell::String("1".into()),
                    Cell::String("z".into()),
                    Cell::String("b".into())
                ],
            )
            .unwrap_err(),
        StoreError::TypeMismatch
    ));
    engine
        .insert_row(
            "notes",
            &[
                Cell::Int64(2),
                Cell::String("short".into()),
                Cell::String("a".into()),
            ],
        )
        .unwrap();
    let (_cols, rows) = engine
        .scan_table("notes", Some("tag"), Some(&Cell::String("a".into())), 10)
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| row[2] == "a"));
    drop(engine);
    let restored = Engine::open(&dir).unwrap();
    let (_cols, rows) = restored.scan_table("notes", None, None, 10).unwrap();
    assert_eq!(rows.len(), 2);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn negative_and_late_timestamps_stay_in_ticks_without_panicking() {
    let dir = temp("time");
    let mut engine = Engine::open(&dir).unwrap();
    engine.write_trade("SSE", "600000", -1, 10, 1).unwrap();
    engine.write_trade("SSE", "600000", utc(2), 20, 1).unwrap();
    engine.write_trade("SSE", "600000", utc(0), 5, 1).unwrap();
    let id = engine.resolve("SSE:600000").unwrap();
    let ticks = engine.ticks(id);
    assert_eq!(ticks.len(), 3);
    assert_eq!(ticks[0].ts, -1);
    assert_eq!(ticks[2].price, 5);
    assert!(engine
        .klines(id, Period::M1)
        .iter()
        .all(|bar| bar.open != 5));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_fenced_writer_rejects_a_lower_epoch_and_new_trades() {
    let dir = temp("fence");
    let mut engine = Engine::open(&dir).unwrap();
    assert!(matches!(
        engine.fence(0).unwrap_err(),
        StoreError::StaleEpoch
    ));
    engine.fence(engine.leader_epoch()).unwrap();
    assert!(engine.is_replica());
    assert!(matches!(
        engine.write_trade("SSE", "600000", 1, 10, 1).unwrap_err(),
        StoreError::StaleEpoch
    ));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn publishing_an_earlier_offset_hides_the_later_trade() {
    let dir = temp("visible");
    let mut engine = Engine::open(&dir).unwrap();
    engine.arm_visibility_gate().unwrap();
    engine.write_trade("SSE", "600000", 1, 10, 1).unwrap();
    let mid = engine.durable_offset();
    engine.write_trade("SSE", "600000", 2, 20, 1).unwrap();
    assert!(engine.durable_offset() > mid);
    let hidden = Engine::open_read(&dir).unwrap();
    assert!(hidden.symbols().is_empty());
    engine.publish_visible(mid).unwrap();
    let partial = Engine::open_read(&dir).unwrap();
    let id = partial.resolve("SSE:600000").unwrap();
    assert_eq!(
        partial
            .ticks(id)
            .iter()
            .map(|tick| tick.price)
            .collect::<Vec<_>>(),
        vec![10]
    );
    engine.publish_visible(engine.durable_offset()).unwrap();
    let full = Engine::open_read(&dir).unwrap();
    let id = full.resolve("SSE:600000").unwrap();
    assert_eq!(full.ticks(id).len(), 2);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn composite_primary_key_is_the_whole_key_and_bool_roundtrips() {
    let dir = temp("pk");
    let mut engine = Engine::open(&dir).unwrap();
    engine
        .create_table(TableSchema {
            name: "pos".into(),
            columns: vec![
                col("book", ColumnType::Int64, true),
                col("line", ColumnType::Int64, true),
                col("open", ColumnType::Bool, false),
            ],
            indexes: vec![IndexDef {
                name: "missing".into(),
                column: "nope".into(),
            }],
        })
        .unwrap_err();
    engine
        .create_table(TableSchema {
            name: "pos".into(),
            columns: vec![
                col("book", ColumnType::Int64, true),
                col("line", ColumnType::Int64, true),
                col("open", ColumnType::Bool, false),
            ],
            indexes: Vec::new(),
        })
        .unwrap();
    engine
        .insert_row("pos", &[Cell::Int64(1), Cell::Int64(1), Cell::Bool(true)])
        .unwrap();
    engine
        .insert_row("pos", &[Cell::Int64(1), Cell::Int64(2), Cell::Bool(false)])
        .unwrap();
    assert!(matches!(
        engine
            .insert_row("pos", &[Cell::Int64(1), Cell::Int64(1), Cell::Bool(false)],)
            .unwrap_err(),
        StoreError::DuplicateKey
    ));
    let (_cols, rows) = engine.scan_table("pos", None, None, 10).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][2], "true");
    assert_eq!(rows[1][2], "false");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn read_only_rejects_writes_and_a_cursor_past_the_wal_is_rejected() {
    let dir = temp("ro");
    let mut engine = Engine::open(&dir).unwrap();
    engine.write_trade("SSE", "600000", 1, i64::MAX, 3).unwrap();
    let id = engine.resolve("SSE:600000").unwrap();
    assert_eq!(engine.ticks(id)[0].amount, i64::MAX);
    let past = engine.durable_offset() + 1;
    assert!(matches!(
        engine.replication_blob(None, past).unwrap_err(),
        StoreError::Io(_)
    ));
    let (_seq, rows) = engine.changes_since(Some("SSE:600000"), 0, 1);
    assert_eq!(rows.len(), 1);
    drop(engine);
    let mut reader = Engine::open_read(&dir).unwrap();
    assert!(matches!(
        reader.write_trade("SSE", "600000", 2, 1, 1).unwrap_err(),
        StoreError::ReadOnly
    ));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn kline_window_keeps_the_latest_bars_up_to_end_ns() {
    let dir = temp("kwin");
    let mut engine = Engine::open(&dir).unwrap();
    engine.write_trade("SSE", "600000", utc(0), 10, 1).unwrap();
    engine.write_trade("SSE", "600000", utc(1), 11, 1).unwrap();
    engine.write_trade("SSE", "600000", utc(2), 12, 1).unwrap();
    let id = engine.resolve("SSE:600000").unwrap();
    assert_eq!(engine.klines(id, Period::M1).len(), 3);
    let last = engine.klines_window(id, Period::M1, Some(2), None);
    assert_eq!(last.len(), 2);
    assert_eq!(last[0].open, 11);
    assert_eq!(last[1].open, 12);
    let early = engine.klines_window(id, Period::M1, None, Some(utc(1)));
    assert!(early.iter().all(|bar| bar.bucket_start_ns <= utc(1)));
    assert!(early.iter().all(|bar| bar.open != 12));
    assert!(engine
        .klines_window(id, Period::M1, Some(0), None)
        .is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn one_group_commit_beats_one_fsync_per_trade() {
    const N: i64 = 48;
    let single = time_trades(false, N);
    let grouped = time_trades(true, N);
    assert!(
        grouped * 3 < single,
        "grouped {grouped:?} should be at least 3x faster than per-trade fsync {single:?}"
    );
}

#[test]
fn grouped_trades_flush_in_batches_instead_of_once_per_row() {
    let dir = temp("bulk");
    let mut engine = Engine::open(&dir).unwrap();
    engine.set_group_commit(256, 0);
    let start = Instant::now();
    for i in 0..512 {
        engine
            .write_trade("SSE", "600000", i, 10 + (i % 7), 1)
            .unwrap();
    }
    engine.flush().unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(15),
        "grouped writes took {elapsed:?}"
    );
    assert!(
        engine.flush_count() <= 4,
        "flush_count {}",
        engine.flush_count()
    );
    assert!(engine.flush_count() < 512);
    let id = engine.resolve("SSE:600000").unwrap();
    assert_eq!(engine.ticks(id).len(), 512);
    let _ = std::fs::remove_dir_all(dir);
}

fn time_trades(grouped: bool, n: i64) -> Duration {
    let dir = temp(if grouped { "fast" } else { "slow" });
    let mut engine = Engine::open(&dir).unwrap();
    if grouped {
        engine.set_group_commit(n as u64, 0);
    }
    let start = Instant::now();
    for i in 0..n {
        engine.write_trade("SSE", "600000", i, 10, 1).unwrap();
    }
    if grouped {
        engine.flush().unwrap();
    }
    let elapsed = start.elapsed();
    drop(engine);
    let _ = std::fs::remove_dir_all(dir);
    elapsed
}
