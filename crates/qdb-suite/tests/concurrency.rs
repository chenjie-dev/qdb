//! 一个写入者，多个只读查询。读者变慢也不要求写入者等待。

use std::thread;

use qdb_suite::{trade, Fixture};

#[test]
fn one_writer_and_two_readers_see_every_trade_without_stopping_the_writer() {
    let fix = Fixture::new();
    let mut writer = fix.open().unwrap();
    trade(&mut writer, "600000", 0, 10, 1).unwrap();
    let dir = fix.dir().to_path_buf();

    thread::scope(|scope| {
        for _ in 0..2 {
            let dir = dir.clone();
            scope.spawn(move || {
                let mut reader = qdb_store::Engine::open_read(&dir).unwrap();
                for _ in 0..10_000 {
                    reader.poll().unwrap();
                    let Ok(id) = reader.resolve("SSE:600000") else {
                        thread::yield_now();
                        continue;
                    };
                    if reader.ticks(id).len() >= 20 {
                        return;
                    }
                    thread::yield_now();
                }
                panic!("reader did not see the published trades");
            });
        }
        for i in 1..20 {
            trade(&mut writer, "600000", i * 1_000_000_000, 10 + i, 1).unwrap();
        }
    });

    let extra = trade(&mut writer, "600000", 20 * 1_000_000_000, 40, 1).unwrap();
    assert!(extra > 0);
    let id = writer.resolve("SSE:600000").unwrap();
    assert_eq!(writer.ticks(id).len(), 21);
}
