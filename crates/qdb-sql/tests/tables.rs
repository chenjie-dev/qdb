//! 调用方建成的表。期望来自手册里的建表、插入、更新、删除和 `seq`，不从实现再推一遍。

use std::sync::atomic::{AtomicU64, Ordering};

use qdb_sql::{SqlEngine, SqlError};
use qdb_store::Engine;

static NEXT: AtomicU64 = AtomicU64::new(0);

fn open() -> (std::path::PathBuf, Engine) {
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("qdb-tables-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    (dir.clone(), Engine::open(&dir).unwrap())
}

fn message(err: SqlError) -> String {
    err.to_string()
}

#[test]
fn an_empty_table_select_returns_no_rows() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    let rows = SqlEngine::new(&engine)
        .query("SELECT id FROM note")
        .unwrap();
    assert!(rows.rows.is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_string_of_1024_bytes_is_stored_and_1025_is_rejected() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE blob (id INT64 PRIMARY KEY, body STRING)",
    )
    .unwrap();
    let ok = "x".repeat(1024);
    SqlEngine::execute(&mut engine, &format!("INSERT INTO blob VALUES (1, '{ok}')")).unwrap();
    let stored = SqlEngine::new(&engine)
        .query("SELECT body FROM blob WHERE id = 1")
        .unwrap();
    assert_eq!(stored.rows[0][0].len(), 1024);
    let too_long = format!("{ok}y");
    let err = SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO blob VALUES (2, '{too_long}')"),
    )
    .unwrap_err();
    assert_eq!(message(err), "syntax: string longer than 1024 bytes");
    assert!(SqlEngine::new(&engine)
        .query("SELECT id FROM blob WHERE id = 2")
        .unwrap()
        .rows
        .is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_secondary_index_returns_only_the_matching_row() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, INDEX idx_code (code))",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'alpha')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (2, 'beta')").unwrap();
    let rows = SqlEngine::new(&engine)
        .query("SELECT id FROM note WHERE code = 'alpha'")
        .unwrap();
    assert_eq!(rows.rows, vec![vec!["1".to_string()]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_composite_primary_key_is_the_whole_pair() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE pair (a INT64 PRIMARY KEY, b INT64 PRIMARY KEY, name STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO pair VALUES (1, 1, 'left')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO pair VALUES (1, 2, 'right')").unwrap();
    assert!(SqlEngine::execute(&mut engine, "INSERT INTO pair VALUES (1, 1, 'again')").is_err());
    let err = SqlEngine::execute(&mut engine, "DELETE FROM pair WHERE a = 1").unwrap_err();
    assert_eq!(message(err), "syntax: primary key required");
    SqlEngine::execute(&mut engine, "DELETE FROM pair WHERE b = 1 AND a = 1").unwrap();
    let left = SqlEngine::new(&engine)
        .query("SELECT name FROM pair WHERE a = 1 AND b = 2")
        .unwrap();
    assert_eq!(left.rows, vec![vec!["right".to_string()]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn select_star_appends_seq_and_describe_does_not_list_it() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'alpha')").unwrap();
    let sql = SqlEngine::new(&engine);
    let star = sql.query("SELECT * FROM note").unwrap();
    assert_eq!(
        star.columns,
        vec!["id".to_string(), "code".to_string(), "seq".to_string()]
    );
    assert_eq!(star.rows[0][0], "1");
    assert_eq!(star.rows[0][1], "alpha");
    assert_ne!(star.rows[0][2], "0");
    let described = sql.query("DESCRIBE note").unwrap();
    assert!(described.rows.iter().all(|row| row[0] != "seq"));
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expect_zero_inserts_only_when_the_key_is_absent() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'kept')").unwrap();
    let err = SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'other') EXPECT 0")
        .unwrap_err();
    assert_eq!(message(err), "syntax: seq conflict");
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["kept".to_string()]]
    );
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (2, 'new') EXPECT 0").unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note WHERE id = 2")
            .unwrap()
            .rows,
        vec![vec!["new".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expect_on_a_table_without_a_primary_key_fails() {
    let (dir, mut engine) = open();
    SqlEngine::execute(&mut engine, "CREATE TABLE log (id INT64, code STRING)").unwrap();
    let err =
        SqlEngine::execute(&mut engine, "INSERT INTO log VALUES (1, 'a') EXPECT 0").unwrap_err();
    assert_eq!(message(err), "syntax: primary key required");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_mismatched_expect_leaves_the_visible_seq_unchanged() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'kept')").unwrap();
    let before = SqlEngine::new(&engine)
        .query("SELECT seq FROM note WHERE id = 1")
        .unwrap()
        .rows[0][0]
        .clone();
    let wrong = before.parse::<u64>().unwrap().wrapping_add(9);
    let err = SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO note VALUES (1, 'other') EXPECT {wrong}"),
    )
    .unwrap_err();
    assert_eq!(message(err), "syntax: seq conflict");
    let after = SqlEngine::new(&engine)
        .query("SELECT code, seq FROM note WHERE id = 1")
        .unwrap();
    assert_eq!(after.rows[0][0], "kept");
    assert_eq!(after.rows[0][1], before);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expect_after_reopen_uses_the_stored_seq() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'before')").unwrap();
    let seq = SqlEngine::new(&engine)
        .query("SELECT seq FROM note WHERE id = 1")
        .unwrap()
        .rows[0][0]
        .clone();
    drop(engine);
    let mut engine = Engine::open(&dir).unwrap();
    SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO note VALUES (1, 'after') EXPECT {seq}"),
    )
    .unwrap();
    let row = SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 1")
        .unwrap();
    assert_eq!(row.rows, vec![vec!["after".to_string()]]);
    let err = SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO note VALUES (1, 'stale') EXPECT {seq}"),
    )
    .unwrap_err();
    assert_eq!(message(err), "syntax: seq conflict");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn delete_without_where_and_an_empty_in_list_fail() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'kept')").unwrap();
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "DELETE FROM note").unwrap_err()),
        "syntax: where"
    );
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "DELETE FROM note WHERE id IN ()").unwrap_err()),
        "syntax: primary key required"
    );
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["kept".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn deleting_a_negative_key_hides_only_that_row() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (-5, 'gone')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (5, 'stay')").unwrap();
    SqlEngine::execute(&mut engine, "delete from note where id = -5;").unwrap();
    let rows = SqlEngine::new(&engine)
        .query("SELECT id FROM note ORDER BY id")
        .unwrap();
    assert_eq!(rows.rows, vec![vec!["5".to_string()]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_bool_primary_key_deletes_only_that_value() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE flag (on BOOL PRIMARY KEY, name STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO flag VALUES (true, 'yes')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO flag VALUES (false, 'no')").unwrap();
    SqlEngine::execute(&mut engine, "DELETE FROM flag WHERE on = true").unwrap();
    let rows = SqlEngine::new(&engine)
        .query("SELECT name FROM flag")
        .unwrap();
    assert_eq!(rows.rows, vec![vec!["no".to_string()]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_deleted_row_leaves_the_secondary_index() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, INDEX idx_code (code))",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'same')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (2, 'same')").unwrap();
    SqlEngine::execute(&mut engine, "DELETE FROM note WHERE id = 1").unwrap();
    let rows = SqlEngine::new(&engine)
        .query("SELECT id FROM note WHERE code = 'same'")
        .unwrap();
    assert_eq!(rows.rows, vec![vec!["2".to_string()]]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expect_replace_is_found_under_the_new_index_value() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, INDEX idx_code (code))",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'old')").unwrap();
    let seq = SqlEngine::new(&engine)
        .query("SELECT seq FROM note WHERE id = 1")
        .unwrap()
        .rows[0][0]
        .clone();
    SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO note VALUES (1, 'new') EXPECT {seq}"),
    )
    .unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT id FROM note WHERE code = 'old'")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id FROM note WHERE code = 'new'")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_read_only_engine_rejects_delete_and_expect() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'kept')").unwrap();
    drop(engine);
    let mut reader = Engine::open_read(&dir).unwrap();
    assert!(SqlEngine::execute(&mut reader, "DELETE FROM note WHERE id = 1").is_err());
    assert!(SqlEngine::execute(&mut reader, "INSERT INTO note VALUES (2, 'x') EXPECT 0").is_err());
    drop(reader);
    let engine = Engine::open(&dir).unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["kept".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expire_at_hides_due_rows_and_keeps_the_rest() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "CREATE TABLE log (id INT64, code STRING)").unwrap();
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (1, 'due') EXPIRE AT 100",
    )
    .unwrap();
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (2, 'later') EXPIRE AT 101",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (3, 'forever')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO log VALUES (1, 'append')").unwrap();
    let expired = SqlEngine::execute(&mut engine, "EXPIRE AT 100").unwrap();
    assert_eq!(expired.rows, vec![vec!["expired".to_string()]]);
    let rows = SqlEngine::new(&engine)
        .query("SELECT id FROM note ORDER BY id")
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![vec!["2".to_string()], vec!["3".to_string()]]
    );
    drop(engine);
    let mut engine = Engine::open(&dir).unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 1")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "EXPIRE AT no").unwrap_err()),
        "syntax: expire"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn expect_can_replace_the_deadline() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (1, 'due') EXPIRE AT 50",
    )
    .unwrap();
    let seq = SqlEngine::new(&engine)
        .query("SELECT seq FROM note WHERE id = 1")
        .unwrap()
        .rows[0][0]
        .clone();
    SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO note VALUES (1, 'later') EXPECT {seq} EXPIRE AT 80"),
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "EXPIRE AT 50").unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["later".to_string()]]
    );
    SqlEngine::execute(&mut engine, "EXPIRE AT 80").unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 1")
        .unwrap()
        .rows
        .is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn one_insert_writes_every_tuple_or_none() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "CREATE TABLE log (id INT64, code STRING)").unwrap();
    let inserted = SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (1, 'a'), (2, 'b') EXPIRE AT 100",
    )
    .unwrap();
    assert_eq!(inserted.rows, vec![vec!["inserted".to_string()]]);
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id FROM note ORDER BY id")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
    SqlEngine::execute(&mut engine, "INSERT INTO log VALUES (1, 'x'), (2, 'y')").unwrap();
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "INSERT INTO note VALUES (3, 'c'), (1, 'again')"
            )
            .unwrap_err()
        ),
        "syntax: duplicate key"
    );
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 3")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (4, 'd'), (4, 'e')")
                .unwrap_err()
        ),
        "syntax: duplicate key"
    );
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "INSERT INTO note VALUES (5, 'f'), (6, 'g') EXPECT 0",
            )
            .unwrap_err()
        ),
        "syntax: expect"
    );
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "INSERT INTO note VALUES (7, 'ok'), ('bad', 'x')"
            )
            .unwrap_err()
        ),
        "syntax: type mismatch"
    );
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 7")
        .unwrap()
        .rows
        .is_empty());
    drop(engine);
    let mut engine = Engine::open(&dir).unwrap();
    SqlEngine::execute(&mut engine, "EXPIRE AT 100").unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT id FROM note")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id FROM log ORDER BY id")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_composite_primary_key_can_delete_several_rows() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE pair (a INT64 PRIMARY KEY, b STRING PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO pair VALUES (1, 'x', 'one'), (2, 'y', 'two'), (3, 'z', 'three')",
    )
    .unwrap();
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "DELETE FROM pair WHERE (b, a) IN (('y', 9), ('z', 3))",
            )
            .unwrap_err()
        ),
        "syntax: key not found"
    );
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT a FROM pair ORDER BY a")
            .unwrap()
            .rows,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()]
        ]
    );
    let deleted = SqlEngine::execute(
        &mut engine,
        "DELETE FROM pair WHERE (b, a) IN (('y', 2), ('x', 1))",
    )
    .unwrap();
    assert_eq!(deleted.rows, vec![vec!["deleted".to_string()]]);
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "DELETE FROM pair WHERE (a, b) IN ((3, 'z'), (3, 'z'))",
            )
            .unwrap_err()
        ),
        "syntax: duplicate key"
    );
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "DELETE FROM pair WHERE a IN (3)").unwrap_err()),
        "syntax: primary key required"
    );
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "DELETE FROM pair WHERE (a, b) IN ((3, 'z', 'no'))"
            )
            .unwrap_err()
        ),
        "syntax: column count"
    );
    drop(engine);
    let engine = Engine::open(&dir).unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM pair WHERE a = 3")
            .unwrap()
            .rows,
        vec![vec!["three".to_string()]]
    );
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM pair WHERE a = 1")
        .unwrap()
        .rows
        .is_empty());
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn insert_by_column_list_reorders_values_and_rejects_a_partial_list() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, active BOOL)",
    )
    .unwrap();
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO note (active, id, code) VALUES (true, 1, 'a'), (false, 2, 'b')",
    )
    .unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id, code, active FROM note ORDER BY id")
            .unwrap()
            .rows,
        vec![
            vec!["1".to_string(), "a".to_string(), "true".to_string()],
            vec!["2".to_string(), "b".to_string(), "false".to_string()]
        ]
    );
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "INSERT INTO note (id, code) VALUES (3, 'c')")
                .unwrap_err()
        ),
        "syntax: column count"
    );
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "INSERT INTO note (id, id, code) VALUES (3, 4, 'c')",
            )
            .unwrap_err()
        ),
        "syntax: duplicate column"
    );
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "INSERT INTO note (id, code, missing) VALUES (3, 'c', true)",
            )
            .unwrap_err()
        ),
        "syntax: unknown column missing"
    );
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "INSERT INTO note (code, id, active) VALUES ('nope', 'bad', true)",
            )
            .unwrap_err()
        ),
        "syntax: type mismatch"
    );
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 3")
        .unwrap()
        .rows
        .is_empty());
    let seq = SqlEngine::new(&engine)
        .query("SELECT seq FROM note WHERE id = 1")
        .unwrap()
        .rows[0][0]
        .clone();
    SqlEngine::execute(
        &mut engine,
        &format!("INSERT INTO note (code, active, id) VALUES ('later', false, 1) EXPECT {seq}"),
    )
    .unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code, active FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["later".to_string(), "false".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn drop_table_hides_the_name_until_it_is_created_again() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'old')").unwrap();
    let dropped = SqlEngine::execute(&mut engine, "DROP TABLE note").unwrap();
    assert_eq!(dropped.rows, vec![vec!["dropped".to_string()]]);
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "DROP TABLE note").unwrap_err()),
        "symbol not found"
    );
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "DROP TABLE ticks").unwrap_err()),
        "syntax: reserved table"
    );
    assert!(SqlEngine::new(&engine)
        .query("SHOW TABLES")
        .unwrap()
        .rows
        .iter()
        .all(|row| row[0] != "note"));
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note")
        .unwrap()
        .rows
        .is_empty());
    drop(engine);
    let mut engine = Engine::open(&dir).unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note")
        .unwrap()
        .rows
        .is_empty());
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (2, 'new')").unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note")
            .unwrap()
            .rows,
        vec![vec!["new".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn truncate_keeps_the_table_and_drops_visible_rows() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, INDEX idx_code (code))",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "CREATE TABLE log (id INT64, code STRING)").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'old')").unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO log VALUES (1, 'old'), (2, 'old')").unwrap();
    let truncated = SqlEngine::execute(&mut engine, "TRUNCATE TABLE note").unwrap();
    assert_eq!(truncated.rows, vec![vec!["truncated".to_string()]]);
    SqlEngine::execute(&mut engine, "TRUNCATE TABLE log").unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM note")
        .unwrap()
        .rows
        .is_empty());
    assert!(SqlEngine::new(&engine)
        .query("SELECT code FROM log")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        SqlEngine::new(&engine)
            .query("DESCRIBE note")
            .unwrap()
            .rows
            .first()
            .map(|row| row[0].clone()),
        Some("id".to_string())
    );
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "TRUNCATE TABLE ticks").unwrap_err()),
        "syntax: reserved table"
    );
    assert_eq!(
        message(SqlEngine::execute(&mut engine, "TRUNCATE TABLE missing").unwrap_err()),
        "symbol not found"
    );
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (3, 'new')").unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id FROM note WHERE code = 'new'")
            .unwrap()
            .rows,
        vec![vec!["3".to_string()]]
    );
    drop(engine);
    let engine = Engine::open(&dir).unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note")
            .unwrap()
            .rows,
        vec![vec!["new".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn update_replaces_non_key_columns_and_keeps_the_deadline() {
    let (dir, mut engine) = open();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING, n INT64, active BOOL, INDEX idx_code (code))",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "CREATE TABLE log (id INT64, code STRING)").unwrap();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE swap (id INT64 PRIMARY KEY, left_n INT64, right_n INT64)",
    )
    .unwrap();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE pair (a INT64 PRIMARY KEY, b INT64 PRIMARY KEY, name STRING)",
    )
    .unwrap();
    SqlEngine::execute(
        &mut engine,
        "INSERT INTO note VALUES (1, 'old', 1, true) EXPIRE AT 100",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (2, 'b', 5, true)").unwrap();
    let before = SqlEngine::new(&engine)
        .query("SELECT seq FROM note WHERE id = 1")
        .unwrap();
    let updated = SqlEngine::execute(
        &mut engine,
        "UPDATE note SET code = 'new', n = n + 1, active = false WHERE id = 1",
    )
    .unwrap();
    assert_eq!(updated.rows, vec![vec!["updated".to_string()]]);
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code, n, active FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec![
            "new".to_string(),
            "2".to_string(),
            "false".to_string()
        ]]
    );
    assert!(SqlEngine::new(&engine)
        .query("SELECT id FROM note WHERE code = 'old'")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT id FROM note WHERE code = 'new'")
            .unwrap()
            .rows,
        vec![vec!["1".to_string()]]
    );
    let after = SqlEngine::new(&engine)
        .query("SELECT seq FROM note WHERE id = 1")
        .unwrap();
    assert_ne!(before.rows, after.rows);
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "UPDATE note SET n = 0 WHERE id IN (1, 9)")
                .unwrap_err()
        ),
        "syntax: key not found"
    );
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT n FROM note WHERE id = 1")
            .unwrap()
            .rows,
        vec![vec!["2".to_string()]]
    );
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "UPDATE note SET id = 9 WHERE id = 1").unwrap_err()
        ),
        "syntax: primary key required"
    );
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "UPDATE note SET code = null WHERE id = 1")
                .unwrap_err()
        ),
        "syntax: null"
    );
    assert_eq!(
        message(
            SqlEngine::execute(
                &mut engine,
                "UPDATE note SET code = 'a', code = 'b' WHERE id = 1"
            )
            .unwrap_err()
        ),
        "syntax: duplicate column"
    );
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "UPDATE log SET code = 'x' WHERE id = 1").unwrap_err()
        ),
        "syntax: primary key required"
    );
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "UPDATE ticks SET price = 1 WHERE seq = 1")
                .unwrap_err()
        ),
        "syntax: reserved table"
    );
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "UPDATE missing SET code = 'x' WHERE id = 1")
                .unwrap_err()
        ),
        "symbol not found"
    );
    assert_eq!(
        message(
            SqlEngine::execute(&mut engine, "UPDATE note SET nope = 1 WHERE id = 1").unwrap_err()
        ),
        "syntax: unknown column nope"
    );
    SqlEngine::execute(&mut engine, "INSERT INTO swap VALUES (1, 1, 2)").unwrap();
    SqlEngine::execute(
        &mut engine,
        "UPDATE swap SET left_n = right_n, right_n = left_n WHERE id = 1",
    )
    .unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT left_n, right_n FROM swap")
            .unwrap()
            .rows,
        vec![vec!["2".to_string(), "1".to_string()]]
    );
    SqlEngine::execute(&mut engine, "INSERT INTO pair VALUES (1, 2, 'old')").unwrap();
    SqlEngine::execute(
        &mut engine,
        "UPDATE pair SET name = 'renamed' WHERE (b, a) IN ((2, 1))",
    )
    .unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT name FROM pair")
            .unwrap()
            .rows,
        vec![vec!["renamed".to_string()]]
    );
    SqlEngine::execute(&mut engine, "EXPIRE AT 100").unwrap();
    assert!(SqlEngine::new(&engine)
        .query("SELECT id FROM note WHERE id = 1")
        .unwrap()
        .rows
        .is_empty());
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT code FROM note WHERE id = 2")
            .unwrap()
            .rows,
        vec![vec!["b".to_string()]]
    );
    drop(engine);
    let mut reader = Engine::open_read(&dir).unwrap();
    assert_eq!(
        message(
            SqlEngine::execute(&mut reader, "UPDATE swap SET left_n = 0 WHERE id = 1").unwrap_err()
        ),
        "read-only connection"
    );
    drop(reader);
    let engine = Engine::open(&dir).unwrap();
    assert_eq!(
        SqlEngine::new(&engine)
            .query("SELECT left_n FROM swap")
            .unwrap()
            .rows,
        vec![vec!["2".to_string()]]
    );
    let _ = std::fs::remove_dir_all(dir);
}
