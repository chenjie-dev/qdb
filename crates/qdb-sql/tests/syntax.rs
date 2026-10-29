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
    let under = sql
        .query("SELECT price FROM ticks WHERE symbol = 'SZSE:000001' AND price < 12")
        .unwrap();
    assert_eq!(under.rows, vec![vec!["10".to_string()]]);
    let either = sql
        .query(
            "SELECT price FROM ticks WHERE symbol = 'SZSE:000001' AND (price = 10 OR price = 99)",
        )
        .unwrap();
    assert_eq!(either.rows, vec![vec!["10".to_string()]]);
    assert!(sql
        .query("SELECT close FROM klines WHERE symbol = 'SZSE:000001' OR period = '1s'")
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
    {
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
            .query(
                "SELECT * FROM note n JOIN symbols s ON n.code = s.symbol WHERE ts BETWEEN 1 AND 2"
            )
            .is_err());
        let between = sql
            .query(
                "SELECT n.id FROM note n CROSS JOIN tag t WHERE t.id BETWEEN 8 AND 8 AND n.id = 1",
            )
            .unwrap();
        assert_eq!(between.rows, vec![vec!["1".to_string()]]);
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
    }
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (3, 'SZSE:000001', true)",
    )
    .unwrap();
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (10, 'SSE:600000', false)",
    )
    .unwrap();
    let sql = SqlEngine::new(&engine);
    assert!(sql
        .query("SELECT code, active FROM note GROUP BY code")
        .is_err());
    let grouped = sql
        .query("SELECT code, count(id) AS n FROM note GROUP BY code HAVING n = 1 ORDER BY code")
        .unwrap();
    assert_eq!(grouped.columns, vec!["code", "n"]);
    assert_eq!(
        grouped.rows,
        vec![
            vec![String::new(), "1".to_string()],
            vec!["SSE:600000".to_string(), "1".to_string()]
        ]
    );
    let by_id = sql
        .query("SELECT id FROM note WHERE id <> 1 AND id != 2 ORDER BY id DESC")
        .unwrap();
    assert_eq!(
        by_id.rows,
        vec![vec!["10".to_string()], vec!["3".to_string()]]
    );
    let precedence = sql
        .query("SELECT id FROM note WHERE id = 1 OR id = 10 AND active = false ORDER BY id")
        .unwrap();
    assert_eq!(
        precedence.rows,
        vec![vec!["1".to_string()], vec!["10".to_string()]]
    );
    let distinct = sql
        .query("SELECT DISTINCT active FROM note ORDER BY active DESC")
        .unwrap();
    assert_eq!(
        distinct.rows,
        vec![vec!["true".to_string()], vec!["false".to_string()]]
    );
    let nulls = sql
        .query("SELECT n.id FROM note n LEFT JOIN symbols s ON n.code = s.symbol WHERE s.symbol IS NULL")
        .unwrap();
    assert_eq!(
        nulls.rows,
        vec![vec!["2".to_string()], vec!["10".to_string()]]
    );
    let blank = sql
        .query("SELECT t.id FROM note n LEFT JOIN tag t ON n.id = t.id WHERE t.label = ''")
        .unwrap();
    assert!(blank.rows.is_empty());
    let missing = sql
        .query("SELECT n.id FROM note n LEFT JOIN tag t ON n.id = t.id WHERE t.id IS NULL ORDER BY n.id")
        .unwrap();
    assert_eq!(
        missing.rows,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
            vec!["10".to_string()]
        ]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expressions_case_like_in_and_union_follow_integer_rules() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let sql = SqlEngine::new(&engine);
    let notional = sql
        .query("SELECT price * volume AS notional FROM ticks WHERE symbol = 'SZSE:000001' AND price = 10")
        .unwrap();
    assert_eq!(notional.rows, vec![vec!["1000".to_string()]]);
    let added = sql
        .query("SELECT id + 2 * 3 AS n FROM note WHERE id = 1")
        .unwrap();
    assert_eq!(added.rows, vec![vec!["7".to_string()]]);
    let code = sql.query("SELECT code FROM symbols").unwrap();
    assert_eq!(code.rows, vec![vec!["000001".to_string()]]);
    let signed = sql.query("SELECT -id AS n FROM note WHERE id = 1").unwrap();
    assert_eq!(signed.rows, vec![vec!["-1".to_string()]]);
    let half = sql.query("SELECT id / 2 FROM note").unwrap();
    assert_eq!(half.columns, vec!["id / 2"]);
    assert_eq!(
        half.rows,
        vec![vec!["0".to_string()], vec!["1".to_string()]]
    );
    let kept = sql
        .query("SELECT id FROM note WHERE id IN (2, 1) AND NOT id = 2")
        .unwrap();
    assert_eq!(kept.rows, vec![vec!["1".to_string()]]);
    assert!(sql
        .query("SELECT id FROM note WHERE id IN (3, null)")
        .unwrap()
        .rows
        .is_empty());
    let like = sql
        .query("SELECT code FROM note WHERE code LIKE 'SZSE:______'")
        .unwrap();
    assert_eq!(like.rows, vec![vec!["SZSE:000001".to_string()]]);
    let band = sql
        .query("SELECT CASE WHEN id > 1 THEN 'hi' ELSE 'lo' END AS band FROM note")
        .unwrap();
    assert_eq!(band.columns, vec!["band"]);
    assert_eq!(
        band.rows,
        vec![vec!["lo".to_string()], vec!["hi".to_string()]]
    );
    let active = sql.query("SELECT id FROM note WHERE active").unwrap();
    assert_eq!(active.rows, vec![vec!["1".to_string()]]);
    let grouped = sql
        .query("SELECT id / 2 AS bucket, count(id) AS n FROM note GROUP BY id / 2 ORDER BY bucket")
        .unwrap();
    assert_eq!(
        grouped.rows,
        vec![
            vec!["0".to_string(), "1".to_string()],
            vec!["1".to_string(), "1".to_string()]
        ]
    );
    let unioned = sql
        .query(
            "SELECT id FROM note WHERE id = 2 UNION SELECT id FROM note WHERE id = 1 ORDER BY id",
        )
        .unwrap();
    assert_eq!(
        unioned.rows,
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT id FROM note WHERE id = 1 UNION ALL SELECT id FROM note WHERE id = 1")
            .unwrap()
            .rows
            .len(),
        2
    );
    assert_eq!(
        sql.query("SELECT id FROM note WHERE id = 1 UNION SELECT id FROM note WHERE id = 1")
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(sql
        .query("SELECT id, code FROM note UNION SELECT id FROM note")
        .is_err());
    assert!(sql.query("SELECT id / 0 FROM note").is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn delete_hides_the_primary_key_until_it_is_inserted_again() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let deleted = SqlEngine::execute(&mut engine, "DELETE FROM note WHERE id = 1").unwrap();
    assert_eq!(deleted.rows, vec![vec!["deleted".to_string()]]);
    let sql = SqlEngine::new(&engine);
    assert!(sql
        .query("SELECT code FROM note WHERE id = 1")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        sql.query("SELECT id FROM note ORDER BY id").unwrap().rows,
        vec![vec!["2".to_string()]]
    );
    assert!(sql.query("DELETE FROM note WHERE id = 2").is_err());
    drop(sql);
    assert!(SqlEngine::execute(&mut engine, "DELETE FROM note WHERE id = 1").is_err());
    assert!(SqlEngine::execute(&mut engine, "DELETE FROM note WHERE code = 'x'").is_err());
    assert!(SqlEngine::execute(&mut engine, "DELETE FROM ticks WHERE seq = 1").is_err());
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'again', false)").unwrap();
    drop(engine);
    let mut engine = Engine::open(&dir).unwrap();
    let sql = SqlEngine::new(&engine);
    assert_eq!(
        sql.query("SELECT code FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["again".to_string()]]
    );
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE pair (a INT64 PRIMARY KEY, b INT64 PRIMARY KEY, name STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO pair VALUES (1, 2, 'x')").unwrap();
    assert!(SqlEngine::execute(&mut engine, "DELETE FROM pair WHERE a = 1").is_err());
    SqlEngine::execute(&mut engine, "DELETE FROM pair WHERE b = 2 AND a = 1").unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT name FROM pair")
        .unwrap()
        .rows
        .is_empty());
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (3, 'c', true)").unwrap();
    SqlEngine::execute(&mut engine, "DELETE FROM note WHERE id IN (2, 3)").unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id FROM note ORDER BY id")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (4, 'd', true)").unwrap();
    assert!(SqlEngine::execute(&mut engine, "DELETE FROM note WHERE id IN (4, 9)").is_err());
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id FROM note ORDER BY id")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()], vec!["4".to_string()]]
    );
    assert!(SqlEngine::execute(&mut engine, "DELETE FROM note WHERE id IN (4, 4)").is_err());
    assert!(SqlEngine::execute(&mut engine, "DELETE FROM pair WHERE a IN (1)").is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expect_replaces_only_the_matching_seq() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let sql = SqlEngine::new(&engine);
    let seq = sql.query("SELECT seq FROM note WHERE id = 1").unwrap().rows[0][0]
        .parse::<u64>()
        .unwrap();
    assert!(SqlEngine::execute(
        &mut engine,
        &format!(
            "INSERT INTO note VALUES (1, 'no', false) EXPECT {}",
            seq + 1
        )
    )
    .is_err());
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["SZSE:000001".to_string()]]
    );
    SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO note VALUES (1, 'yes', false) EXPECT {seq}"),
    )
    .unwrap();
    let sql = SqlEngine::new(&engine);
    let row = sql
        .query("SELECT code, seq FROM note WHERE id = 1")
        .unwrap();
    assert_eq!(row.rows[0][0], "yes");
    assert_ne!(row.rows[0][1], seq.to_string());
    assert!(SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (9, 'z', true) EXPECT 0"
    )
    .is_ok());
    drop(engine);
    let engine = Engine::open(&dir).unwrap();
    let row = SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 1")
        .unwrap();
    assert_eq!(row.rows[0][0], "yes");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn intersect_binds_tighter_than_union_and_except_counts_copies() {
    let (dir, mut engine) = open();
    note(&mut engine);
    SqlEngine::execute(&mut engine, "CREATE TABLE log (id INT64)").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO log VALUES (1), (1)").unwrap();
    let sql = SqlEngine::new(&engine);
    assert_eq!(
        sql.query(
            "SELECT id FROM note WHERE id = 1 UNION SELECT id FROM note WHERE id = 2 INTERSECT SELECT id FROM note WHERE id = 2 ORDER BY id",
        )
        .unwrap()
        .rows,
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
    assert_eq!(
        sql.query(
            "SELECT id FROM note WHERE id IN (1, 2) EXCEPT SELECT id FROM note WHERE id = 1",
        )
        .unwrap()
        .rows,
        vec![vec!["2".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT id FROM log INTERSECT ALL SELECT id FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT id FROM log EXCEPT ALL SELECT id FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    assert!(sql
        .query("SELECT id FROM log EXCEPT SELECT id FROM note WHERE id = 1")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        sql.query("SELECT id, code FROM note INTERSECT SELECT id FROM note")
            .unwrap_err()
            .to_string(),
        "syntax: each UNION query must have the same number of columns"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn parentheses_group_set_ops_and_limit_the_inner_query() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let sql = SqlEngine::new(&engine);
    assert_eq!(
        sql.query(
            "(SELECT id FROM note WHERE id = 1 UNION SELECT id FROM note WHERE id = 2) INTERSECT SELECT id FROM note WHERE id = 2",
        )
        .unwrap()
        .rows,
        vec![vec!["2".to_string()]]
    );
    assert_eq!(
        sql.query(
            "(SELECT id FROM note WHERE id = 2 UNION SELECT id FROM note WHERE id = 1 ORDER BY id LIMIT 1)",
        )
        .unwrap()
        .rows,
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        sql.query("(SELECT id FROM note ORDER BY id) EXCEPT SELECT id FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["2".to_string()]]
    );
    assert!(sql.query("(SELECT id FROM note").is_err());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn coalesce_skips_null_and_nullif_clears_equal_values() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let sql = SqlEngine::new(&engine);
    assert_eq!(
        sql.query("SELECT coalesce(null, id) FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT coalesce('', 'x') FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![String::new()]]
    );
    assert_eq!(
        sql.query("SELECT coalesce(null, null) FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![String::new()]]
    );
    assert_eq!(
        sql.query("SELECT id FROM note WHERE coalesce(null, null) IS NULL AND id = 1")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    assert!(sql
        .query("SELECT id FROM note WHERE coalesce('', 'x') IS NULL")
        .unwrap()
        .rows
        .is_empty());
    assert!(sql
        .query("SELECT id FROM note WHERE coalesce(null, id) IS NULL")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        sql.query("SELECT nullif(id, 1) FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![String::new()]]
    );
    assert_eq!(
        sql.query("SELECT nullif(id, 2) FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT nullif(null, null) FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![String::new()]]
    );
    assert_eq!(
        sql.query("SELECT coalesce() FROM note")
            .unwrap_err()
            .to_string(),
        "syntax: coalesce"
    );
    assert_eq!(
        sql.query("SELECT nullif(id) FROM note")
            .unwrap_err()
            .to_string(),
        "syntax: nullif"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn simple_case_matches_by_equality_and_null_does_not_match_null() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let sql = SqlEngine::new(&engine);
    assert_eq!(
        sql.query(
            "SELECT CASE id WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM note WHERE id = 1",
        )
        .unwrap()
        .rows,
        vec![vec!["one".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT CASE id WHEN 2 THEN 'two' ELSE 'other' END FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["other".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT CASE null WHEN null THEN 'hit' ELSE 'miss' END FROM note WHERE id = 1",)
            .unwrap()
            .rows,
        vec![vec!["miss".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT CASE id WHEN 1 THEN 'one' END FROM note WHERE id = 2")
            .unwrap()
            .rows,
        vec![vec![String::new()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn cast_converts_int_string_and_bool_and_keeps_null() {
    let (dir, mut engine) = open();
    note(&mut engine);
    let sql = SqlEngine::new(&engine);
    assert_eq!(
        sql.query("SELECT CAST(id AS STRING) FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT id::BOOL FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["true".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT CAST('false' AS INT64) FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["0".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT id FROM note WHERE CAST(null AS INT64) IS NULL AND id = 1")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    assert_eq!(
        sql.query("SELECT CAST('nope' AS INT64) FROM note")
            .unwrap_err()
            .to_string(),
        "syntax: cast"
    );
    assert_eq!(
        sql.query("SELECT id::float FROM note")
            .unwrap_err()
            .to_string(),
        "syntax: unknown type float"
    );
    let _ = std::fs::remove_dir_all(dir);
}
