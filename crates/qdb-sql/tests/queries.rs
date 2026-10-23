use qdb_sql::{SqlEngine, SqlError};
use qdb_store::Engine;

#[test]
fn between_is_inclusive_and_limit_keeps_the_first_rows() {
    let (dir, engine) = sample();
    let sql = SqlEngine::new(&engine);
    let id_rows = sql
        .query("SELECT ts, close FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s' ORDER BY ts ASC LIMIT 1")
        .unwrap();
    assert_eq!(id_rows.rows.len(), 1);
    assert_eq!(id_rows.rows[0][1], "10");
    let bucket: i64 = id_rows.rows[0][0].parse().unwrap();
    let inside = sql
        .query(&format!(
            "SELECT close FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s' AND ts BETWEEN {bucket} AND {bucket}"
        ))
        .unwrap();
    assert_eq!(inside.rows.len(), 1);
    let outside = sql
        .query(&format!(
            "SELECT close FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s' AND ts BETWEEN 0 AND {}",
            bucket - 1
        ))
        .unwrap();
    assert!(outside.rows.is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rejects_missing_filters_unknown_tables_and_oversized_limits() {
    let (dir, engine) = sample();
    let sql = SqlEngine::new(&engine);
    assert!(matches!(
        sql.query("SELECT close FROM klines").unwrap_err(),
        SqlError::Syntax(_)
    ));
    assert!(matches!(
        sql.query("SELECT close FROM klines WHERE symbol = 'SZSE:000001'")
            .unwrap_err(),
        SqlError::Syntax(_)
    ));
    assert!(matches!(
        sql.query("SELECT close FROM books WHERE symbol = 'SZSE:000001'")
            .unwrap_err(),
        SqlError::Syntax(_)
    ));
    assert!(matches!(
        sql.query(
            "SELECT close FROM klines WHERE symbol = 'SZSE:000001' AND period = '1m' LIMIT 100001"
        )
        .unwrap_err(),
        SqlError::Limit(100001)
    ));
    assert!(matches!(
        sql.query("SELECT last FROM snapshots WHERE symbol = 'NOTEXIST'")
            .unwrap_err(),
        SqlError::NotFound
    ));
    let ticks = sql
        .query("SELECT ts, price, volume, seq FROM ticks WHERE symbol = 'SZSE:000001'")
        .unwrap();
    assert_eq!(ticks.rows.len(), 2);
    assert_eq!(ticks.rows[0][1], "10");
    let names = sql.query("SELECT symbol, exchange FROM symbols").unwrap();
    assert!(names.rows.iter().any(|row| row[0] == "SZSE:000001"));
    let count = sql
        .query("SELECT count(close) FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s'")
        .unwrap();
    assert_eq!(count.rows[0][0], "2");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn order_and_group_keep_storage_order_and_kline_seq_is_zero() {
    let (dir, engine) = sample();
    let sql = SqlEngine::new(&engine);
    let ordered = sql
        .query("SELECT price FROM ticks WHERE symbol = 'SZSE:000001' ORDER BY price DESC")
        .unwrap();
    assert_eq!(
        ordered
            .rows
            .iter()
            .map(|row| row[0].as_str())
            .collect::<Vec<_>>(),
        vec!["10", "11"]
    );
    let grouped = sql
        .query("SELECT price FROM ticks WHERE symbol = 'SZSE:000001' GROUP BY price")
        .unwrap();
    assert_eq!(grouped.rows.len(), 2);
    let seq = sql
        .query("SELECT seq FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s'")
        .unwrap();
    assert!(seq.rows.iter().all(|row| row[0] == "0"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn joins_follow_postgresql_inner_outer_and_cross() {
    let (dir, mut engine) = sample();
    qdb_sql::SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, active BOOL)",
    )
    .unwrap();
    qdb_sql::SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (1, 'SZSE:000001', true)",
    )
    .unwrap();
    qdb_sql::SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (2, 'SSE:600000', false)",
    )
    .unwrap();
    let sql = qdb_sql::SqlEngine::new(&engine);
    let inner = sql
        .query("SELECT n.id, s.symbol FROM symbols s JOIN note n ON s.symbol = n.code")
        .unwrap();
    assert_eq!(inner.columns, vec!["n.id", "s.symbol"]);
    assert_eq!(
        inner.rows,
        vec![vec!["1".to_string(), "SZSE:000001".to_string()]]
    );
    let left = sql
        .query("SELECT n.id, s.code FROM note n LEFT JOIN symbols s ON n.code = s.symbol")
        .unwrap();
    assert_eq!(left.rows.len(), 2);
    assert!(left
        .rows
        .iter()
        .any(|row| row == &vec!["2".to_string(), String::new()]));
    let right = sql
        .query("SELECT n.id FROM note n RIGHT JOIN symbols s ON n.code = s.symbol")
        .unwrap();
    assert_eq!(right.rows.len(), 1);
    assert_eq!(right.rows[0][0], "1");
    let full = sql
        .query(
            "SELECT n.id, s.symbol_id FROM note n FULL OUTER JOIN symbols s ON n.code = s.symbol",
        )
        .unwrap();
    assert_eq!(full.rows.len(), 2);
    assert!(full
        .rows
        .iter()
        .any(|row| row[0].is_empty() || row[1].is_empty()));
    let cross = sql
        .query("SELECT n.id, s.symbol_id FROM note n CROSS JOIN symbols s")
        .unwrap();
    assert_eq!(cross.rows.len(), 2);
    let filtered = sql
        .query("SELECT n.id FROM symbols s INNER JOIN note n ON s.symbol = n.code WHERE n.active = true")
        .unwrap();
    assert_eq!(filtered.rows, vec![vec!["1".to_string()]]);
    assert!(sql
        .query("SELECT code FROM note n JOIN symbols s ON n.code = s.symbol")
        .is_err());
    assert!(sql
        .query("SELECT count(id) FROM note n JOIN symbols s ON n.code = s.symbol")
        .is_err());
    let _ = std::fs::remove_dir_all(dir);
}

fn sample() -> (std::path::PathBuf, Engine) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("qdb-sql-more-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut engine = Engine::open(&dir).unwrap();
    let base = shanghai(9, 30, 0, 100_000_000);
    engine.write_trade("SZSE", "000001", base, 10, 100).unwrap();
    engine
        .write_trade("SZSE", "000001", base + 1_000_000_000, 11, 10)
        .unwrap();
    (dir, engine)
}

fn shanghai(h: u32, m: u32, s: u32, nanos: i64) -> i64 {
    let days = days(2024, 1, 2);
    (days * 86_400 + i64::from(h) * 3600 + i64::from(m) * 60 + i64::from(s) - 8 * 3600)
        * 1_000_000_000
        + nanos
}

fn days(mut y: i32, m: u32, d: u32) -> i64 {
    y -= i32::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146_097 + doe as i64 - 719_468
}
