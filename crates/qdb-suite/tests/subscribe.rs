//! 订阅只交付过滤后的帧。别的证券占掉序号时，续传必须显式报缺口。

use qdb_sub::{catch_up, SubFilter, SubMsg};
use qdb_suite::{trade, Fixture};

#[test]
fn a_filter_skips_other_symbols_and_a_hole_is_a_gap() {
    let fix = Fixture::new();
    let mut engine = fix.open().unwrap();
    trade(&mut engine, "600000", 0, 10, 1).unwrap();
    let after_first = engine.applied_seq();
    trade(&mut engine, "600001", 0, 99, 1).unwrap();
    let quiet = catch_up(
        &engine,
        &SubFilter {
            symbol: "SSE:600000".into(),
            period: None,
            from_seq: after_first,
        },
    )
    .unwrap();
    assert!(
        quiet.iter().all(|msg| !matches!(msg, SubMsg::Data(_))),
        "other symbol must not be delivered"
    );

    trade(&mut engine, "600000", 1_000_000_000, 12, 1).unwrap();
    let resumed = catch_up(
        &engine,
        &SubFilter {
            symbol: "SSE:600000".into(),
            period: None,
            from_seq: after_first,
        },
    )
    .unwrap();
    assert!(matches!(resumed.first(), Some(SubMsg::Gap { .. })));

    let caught = catch_up(
        &engine,
        &SubFilter {
            symbol: "SSE:600000".into(),
            period: None,
            from_seq: engine.applied_seq() - 1,
        },
    )
    .unwrap();
    let data: Vec<_> = caught
        .iter()
        .filter_map(|msg| match msg {
            SubMsg::Data(frame) => Some(frame),
            _ => None,
        })
        .collect();
    assert!(!data.is_empty());
    assert!(data.iter().all(|frame| frame.seq() == engine.applied_seq()));
}

#[test]
fn an_unknown_symbol_is_rejected() {
    let fix = Fixture::new();
    let engine = fix.open().unwrap();
    let err = catch_up(
        &engine,
        &SubFilter {
            symbol: "SSE:999999".into(),
            period: None,
            from_seq: 0,
        },
    );
    assert!(err.is_err());
}
