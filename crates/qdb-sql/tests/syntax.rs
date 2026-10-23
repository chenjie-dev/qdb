//! 当前 SQL 语法和边界。清单见 `.agents/skills/qdb-test` 的「当前 SQL」。

use std::sync::atomic::{AtomicU64, Ordering};

use qdb_sql::{split_subscribe, SqlEngine, SqlError};
use qdb_store::Engine;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn open() -> (std::path::PathBuf, Engine) {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("qdb-syntax-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut engine = Engine::open(&dir).unwrap();
    let base = 1_704_067_200_000_000_000_i64;
    engine.write_trade("SZSE", "000001", base, 10, 100).unwrap();
    engine
        .write_trade("SZSE", "000001", base + 1_000_000_000, 12, 60)
        .unwrap();
    (dir, engine)
}

fn note(engine: &mut Engine) {
    SqlEngine::execute(
        engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, active BOOL)",
    )
    .unwrap();
    SqlEngine::execute(engine, "INSERT INTO note VALUES (1, 'SZSE:000001', true)").unwrap();
    SqlEngine::execute(engine, "INSERT INTO note VALUES (2, '', false)").unwrap();
}

#[test]
fn select_requires_from_and_rejects_unknown_columns_and_other_operators() {
    let (dir, engine) = open();
    let sql = SqlEngine::new(&engine);
    assert!(sql.query("").is_err());
    assert!(sql.query("SELECT *").is_err());
    assert!(sql.query("SELECT nope FROM symbols").is_err());
    assert!(sql.query("UPDATE note SET id = 1").is_err());
    assert!(sql.query("DELETE FROM note").is_err());
    assert!(sql
        .query("SELECT * FROM (SELECT * FROM symbols) t")
        .is_err());
    assert!(sql
        .query("SELECT price FROM ticks WHERE symbol = 'SZSE:000001' AND price < 10")
        .is_err());
    assert!(sql
        .query("SELECT price FROM ticks WHERE symbol = 'SZSE:000001' OR price = 10")
        .is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn keywords_ignore_case_and_a_trailing_semicolon_is_dropped() {
    let (dir, engine) = open();
    let sql = SqlEngine::new(&engine);
    let rows = sql.query("SeLeCt * FrOm symbols;").unwrap();
    assert!(!rows.rows.is_empty());
    assert!(rows.columns.iter().any(|column| column == "symbol"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn limit_uses_1000_by_default_and_stops_at_100000() {
    let (dir, mut engine) = open();
    engine.set_group_commit(2000, 0);
    SqlEngine::execute(&mut engine, "CREATE TABLE n (id INT64 PRIMARY KEY)").unwrap();
    for id in 1..=1001 {
        SqlEngine::execute(&mut engine, &format!("INSERT INTO n VALUES ({id})")).unwrap();
    }
    engine.flush().unwrap();
    let sql = SqlEngine::new(&engine);
    assert_eq!(sql.query("SELECT id FROM n").unwrap().rows.len(), 1000);
    assert!(sql
        .query("SELECT id FROM n LIMIT 0")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        sql.query("SELECT id FROM n LIMIT 100000")
            .unwrap()
            .rows
            .len(),
        1001
    );
    assert!(matches!(
        sql.query("SELECT id FROM n LIMIT 100001").unwrap_err(),
        SqlError::Limit(100001)
    ));
    assert!(sql.query("SELECT id FROM n LIMIT").is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn subscribe_is_stripped_only_from_a_select() {
    assert_eq!(
        split_subscribe("SELECT id FROM n SUBSCRIBE")
            .unwrap()
            .as_deref(),
        Some("SELECT id FROM n")
    );
    assert!(split_subscribe("select id from n subscribe;")
        .unwrap()
        .is_some());
    assert!(split_subscribe("SHOW TABLES SUBSCRIBE").is_err());
    let (dir, engine) = open();
    assert!(SqlEngine::new(&engine)
        .query("SELECT * FROM symbols SUBSCRIBE")
        .is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn show_and_describe_list_known_tables_and_reject_unknown_ones() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let sql = SqlEngine::new(&engine);
    let tables = sql.query("SHOW TABLES").unwrap();
    assert!(tables.rows.iter().any(|row| row[0] == "note"));
    assert!(tables.rows.iter().any(|row| row[0] == "klines"));
    let described = sql.query("DESCRIBE note").unwrap();
    assert!(described
        .rows
        .iter()
        .any(|row| row[0] == "id" && row[1].contains("PRIMARY KEY")));
    let desc = sql.query("DESC ticks").unwrap();
    assert!(desc.rows.iter().any(|row| row[0] == "price"));
    assert!(sql.query("DESCRIBE missing").is_err());
    assert!(sql.query("SHOW").is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn create_and_insert_reject_empty_definitions_bad_types_and_short_rows() {
    let (dir, mut engine) = open();
    assert!(SqlEngine::execute(&mut engine, "CREATE TABLE note ()").is_err());
    assert!(SqlEngine::execute(&mut engine, "CREATE TABLE ticks (id INT64)").is_err());
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, active BOOL)",
    )
    .unwrap();
    assert!(matches!(
        SqlEngine::execute(&mut engine, "INSERT INTO missing VALUES (1)").unwrap_err(),
        SqlError::NotFound
    ));
    assert!(SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1)").is_err());
    assert!(SqlEngine::execute(&mut engine, "INSERT INTO note VALUES ('x', true)").is_err());
    assert!(SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, yes)").is_err());
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, false)").unwrap();
    assert!(SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, true)").is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn filters_require_symbol_and_period_and_between_includes_both_ends() {
    let (dir, engine) = open();
    let sql = SqlEngine::new(&engine);
    assert!(sql.query("SELECT last FROM snapshots").is_err());
    assert!(sql
        .query("SELECT close FROM klines WHERE symbol = 'SZSE:000001'")
        .is_err());
    assert!(sql
        .query("SELECT close FROM klines WHERE symbol = 'SZSE:000001' AND period = '9m'")
        .is_err());
    let symbols = sql
        .query("SELECT symbol FROM symbols WHERE symbol = 'NOPE'")
        .unwrap();
    assert!(symbols.rows.iter().any(|row| row[0] == "SZSE:000001"));
    let base = 1_704_067_200_000_000_000_i64;
    let ticks = sql
        .query(&format!(
            "SELECT price FROM ticks WHERE symbol = 'SZSE:000001' AND ts BETWEEN {base} AND {base}"
        ))
        .unwrap();
    assert_eq!(ticks.rows, vec![vec!["10".to_string()]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn each_aggregate_matches_the_two_one_second_bars() {
    let (dir, engine) = open();
    let sql = SqlEngine::new(&engine);
    let row = sql
        .query("SELECT max(close), min(close), sum(volume), count(close), avg(close), first(open), last(close) FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s'")
        .unwrap();
    assert_eq!(
        row.rows,
        vec![vec![
            "12".to_string(),
            "10".to_string(),
            "160".to_string(),
            "2".to_string(),
            "11".to_string(),
            "10".to_string(),
            "12".to_string(),
        ]]
    );
    assert!(sql
        .query("SELECT nope(close) FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s'")
        .is_err());
    let empty = sql
        .query("SELECT max(close) FROM klines WHERE symbol = 'SZSE:000001' AND period = '1s' AND ts BETWEEN 1 AND 2")
        .unwrap();
    assert_eq!(empty.rows, vec![vec![String::new()]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn join_syntax_covers_outer_and_and_aliases_and_rejects_the_edges() {
    let (dir, mut engine) = open();
    note(&mut engine);
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE tag (id INT64 PRIMARY KEY, label STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO tag VALUES (8, '000001')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO tag VALUES (9, '')").unwrap();
    let sql = SqlEngine::new(&engine);
    let named = sql
        .query("SELECT n.id FROM note AS n LEFT OUTER JOIN symbols AS s ON n.code = s.symbol")
        .unwrap();
    assert_eq!(named.rows.len(), 2);
    let both = sql
        .query("SELECT a.id FROM note a JOIN note b ON a.id = b.id AND a.code = b.code")
        .unwrap();
    assert_eq!(both.rows.len(), 2);
    let padded = sql
        .query("SELECT n.id, t.id FROM note n LEFT JOIN symbols s ON n.code = s.symbol LEFT JOIN tag t ON s.code = t.label")
        .unwrap();
    let unmatched = padded.rows.iter().find(|row| row[0] == "2").unwrap();
    assert_eq!(unmatched[1], "");
    let matched = padded.rows.iter().find(|row| row[0] == "1").unwrap();
    assert_eq!(matched[1], "8");
    assert!(sql.query("SELECT * FROM note n JOIN note m").is_err());
    assert!(sql
        .query("SELECT * FROM note n JOIN note n ON n.id = n.id")
        .is_err());
    assert!(sql
        .query("SELECT * FROM note n CROSS JOIN note m ON n.id = m.id")
        .is_err());
    assert!(sql
        .query("SELECT * FROM note n JOIN symbols s ON n.code = s.symbol WHERE ts BETWEEN 1 AND 2")
        .is_err());
    assert!(sql
        .query("SELECT max(id) FROM note n JOIN symbols s ON n.code = s.symbol")
        .is_err());
    assert!(sql
        .query("SELECT code FROM note n JOIN symbols s ON n.code = s.symbol")
        .is_err());
    let star = sql
        .query("SELECT * FROM note n CROSS JOIN tag t WHERE n.id = 1 AND t.id = 8")
        .unwrap();
    assert!(star.columns.iter().any(|column| column == "n.id"));
    assert!(star.columns.iter().any(|column| column == "t.label"));
    assert_eq!(star.rows.len(), 1);
    let _ = std::fs::remove_dir_all(dir);
}
