//! 正常、边界和非法输入。期望来自手册，不从被测实现再推一遍。

use qdb_sql::SqlEngine;
use qdb_store::StoreError;
use qdb_suite::{trade, Fixture, EPOCH_NS};

#[test]
fn timestamps_at_zero_and_negative_stay_queryable() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    engine.write_trade("SSE", "600000", 0, 10, 1).unwrap();
    engine.write_trade("SSE", "600000", -1, 11, 1).unwrap();
    let rows = SqlEngine::new(&engine)
        .query("SELECT ts, price FROM ticks WHERE symbol = 'SSE:600000' ORDER BY ts")
        .unwrap();
    assert_eq!(
        rows.rows,
        vec![
            vec!["-1".to_string(), "11".to_string()],
            vec!["0".to_string(), "10".to_string()]
        ]
    );
}

#[test]
fn empty_exchange_and_bad_sql_fail_without_panicking() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    assert!(matches!(
        engine.write_trade("", "600000", EPOCH_NS, 10, 1),
        Err(StoreError::EmptyExchange)
    ));
    trade(&mut engine, "600000", 0, 10, 1).unwrap();
    let sql = SqlEngine::new(&engine);
    let cases = [
        "SELECT nope FROM ticks",
        "SELECT close FROM klines WHERE symbol = 'SSE:600000' OR period = '1s'",
        "UPDATE ticks SET price = 1",
        "SELECT price FROM (SELECT price FROM ticks) t",
    ];
    for sql_text in cases {
        assert!(sql.query(sql_text).is_err(), "{sql_text}");
    }
}

#[test]
fn user_table_rejects_a_duplicate_primary_key_and_keeps_the_first_row() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    SqlEngine::execute(
        &mut engine,
        "CREATE TABLE note (id INT64 PRIMARY KEY, code STRING)",
    )
    .unwrap();
    SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'SSE:600000')").unwrap();
    assert!(SqlEngine::execute(&mut engine, "INSERT INTO note VALUES (1, 'other')").is_err());
    let rows = SqlEngine::new(&engine)
        .query("SELECT code FROM note WHERE id = 1")
        .unwrap();
    assert_eq!(rows.rows, vec![vec!["SSE:600000".to_string()]]);
}
