//! 按代码和周期过滤，并用 from_seq 续传。落后过多时先快照再增量。
#![forbid(unsafe_code)]

use qdb_abi::{Frame, MsgType, Period};
use qdb_store::Engine;
use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SubError {
    #[error("symbol not found or ambiguous")]
    Symbol,
}

#[derive(Clone, Debug)]
pub struct SubFilter {
    pub symbol: String,
    pub period: Option<Period>,
    /// 客户端已经收到的最后一个 seq。0 表示还没有收到数据。
    pub from_seq: u64,
}

#[derive(Clone, Debug)]
pub enum SubMsg {
    SnapshotBegin,
    SnapshotEnd { snapshot_seq: u64 },
    Data(Frame),
    Gap { expected: u64 },
}

pub fn catch_up(engine: &Engine, filter: &SubFilter) -> Result<Vec<SubMsg>, SubError> {
    let symbol_id = engine.resolve(&filter.symbol).map_err(|_| SubError::Symbol)?;
    let matched: Vec<&Frame> = engine
        .frames()
        .iter()
        .filter(|frame| frame_matches(frame, symbol_id, filter.period))
        .collect();
    let max_seq = engine.frames().iter().map(|frame| frame.seq()).max().unwrap_or(0);
    if filter.from_seq == 0 || (filter.from_seq > 0 && matched.iter().all(|frame| frame.seq() > filter.from_seq + 1) && filter.from_seq + 1 < max_seq) {
        let mut out = vec![SubMsg::SnapshotBegin];
        let snapshot_seq = engine.applied_seq();
        out.push(SubMsg::SnapshotEnd { snapshot_seq });
        out.extend(matched.into_iter().filter(|frame| frame.seq() > snapshot_seq).cloned().map(SubMsg::Data));
        return Ok(out);
    }
    let mut expected = filter.from_seq + 1;
    let mut out = Vec::new();
    for frame in matched {
        if frame.seq() < expected {
            continue;
        }
        if frame.seq() > expected {
            out.push(SubMsg::Gap { expected });
            return Ok(out);
        }
        out.push(SubMsg::Data(frame.clone()));
        expected = frame.seq() + 1;
    }
    Ok(out)
}

fn frame_matches(frame: &Frame, symbol_id: u32, period: Option<Period>) -> bool {
    if frame.header.symbol_id != symbol_id {
        return false;
    }
    match period {
        Some(period) => frame.header.period_id == period.id(),
        None => true,
    }
}

pub fn is_ordered(messages: &[SubMsg]) -> bool {
    let mut last = 0;
    for message in messages {
        if let SubMsg::Data(frame) = message {
            if frame.seq() < last {
                return false;
            }
            last = frame.seq();
        }
    }
    true
}

pub fn msg_type(frame: &Frame) -> Option<MsgType> {
    MsgType::from_u16(frame.header.msg_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdb_abi::Period;

    fn sample() -> (std::path::PathBuf, qdb_store::Engine) {
        let n = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let dir = std::env::temp_dir().join(format!("qdb-sub-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut engine = qdb_store::Engine::open(&dir).unwrap();
        let base = ts(100_000_000);
        engine.write_trade("SZSE", "000001", base, 10, 100).unwrap();
        engine.write_trade("SZSE", "000001", base + 100_000_000, 12, 50).unwrap();
        engine.write_trade("SZSE", "000001", base + 900_000_000, 11, 10).unwrap();
        engine.write_trade("SSE", "600000", base, 8, 1).unwrap();
        (dir, engine)
    }

    fn ts(extra: i64) -> i64 {
        let days = days(2024, 1, 2);
        (days * 86_400 + 9 * 3600 + 30 * 60 - 8 * 3600) * 1_000_000_000 + extra
    }

    fn days(mut y: i32, m: u32, d: u32) -> i64 {
        y -= i32::from(m <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146_097 + doe as i64 - 719_468
    }

    #[test]
    fn filters_symbol_and_period_and_resumes_from_seq() {
        let (_dir, engine) = sample();
        let live = catch_up(
            &engine,
            &SubFilter {
                symbol: "SZSE:000001".into(),
                period: Some(Period::M1),
                from_seq: 0,
            },
        )
        .unwrap();
        assert!(matches!(live[0], SubMsg::SnapshotBegin));
        assert!(matches!(live[1], SubMsg::SnapshotEnd { .. }));
        let resumed = catch_up(
            &engine,
            &SubFilter {
                symbol: "SZSE:000001".into(),
                period: Some(Period::M1),
                from_seq: 1,
            },
        )
        .unwrap();
        assert!(is_ordered(&resumed));
        assert!(resumed.iter().all(|msg| match msg {
            SubMsg::Data(frame) => frame.header.period_id == Period::M1.id() && frame.seq() > 1,
            _ => false,
        }));
    }
}
