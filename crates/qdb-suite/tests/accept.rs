//! 写入之后能查到，删掉共享内存后仍能从 WAL 读回。

use qdb_abi::Period;
use qdb_sql::SqlEngine;
use qdb_suite::{trade, Fixture};

#[test]
fn a_trade_is_visible_to_sql_and_survives_deleting_shared_memory() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    trade(&mut engine, "600000", 0, 10, 100).unwrap();
    trade(&mut engine, "600000", 30_000_000_000, 12, 50).unwrap();
    let id = engine.resolve("SSE:600000").unwrap();
    let bars = engine.klines(id, Period::M1);
    assert_eq!(bars.len(), 1);
    assert_eq!((bars[0].open, bars[0].close, bars[0].volume), (10, 12, 150));
    let sql = SqlEngine::new(&engine);
    let row = sql
        .query("SELECT close, volume FROM klines WHERE symbol = 'SSE:600000' AND period = '1m'")
        .unwrap();
    assert_eq!(row.rows, vec![vec!["12".to_string(), "150".to_string()]]);
    drop(engine);

    std::fs::remove_file(fix.dir().join("region.bin")).unwrap();
    let engine = fix.open().unwrap();
    let id = engine.resolve("SSE:600000").unwrap();
    assert_eq!(engine.klines(id, Period::M1)[0].close, 12);
    let again = SqlEngine::new(&engine)
        .query("SELECT price FROM ticks WHERE symbol = 'SSE:600000' ORDER BY price")
        .unwrap();
    assert_eq!(
        again.rows,
        vec![vec!["10".to_string()], vec!["12".to_string()]]
    );
}
