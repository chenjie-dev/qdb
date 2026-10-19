use qdb_abi::{seal_frame, FrameHeader, MsgType};
use qdb_shm::{RegionConfig, RingReader, ShmError, ShmRegion};

#[test]
fn non_power_of_two_capacity_is_rejected() {
    let path = temp("badcap");
    assert!(matches!(
        ShmRegion::create(&path, &RegionConfig::for_test(3)),
        Err(ShmError::BadCapacity)
    ));
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn a_reader_that_keeps_up_survives_the_ring_wrap() {
    let path = temp("wrap");
    let mut region = ShmRegion::create(&path, &RegionConfig::for_test(4)).unwrap();
    let mut reader = RingReader::open(&path).unwrap();
    for seq in 1..=3 {
        region.publish(&frame(seq)).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().seq(), seq);
    }
    for seq in 4..=6 {
        region.publish(&frame(seq)).unwrap();
    }
    assert_eq!(reader.next().unwrap().unwrap().seq(), 4);
    assert_eq!(reader.next().unwrap().unwrap().seq(), 5);
    assert_eq!(reader.next().unwrap().unwrap().seq(), 6);
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn garbage_header_is_rejected() {
    let path = temp("magic");
    std::fs::write(&path, vec![0u8; 8192]).unwrap();
    assert!(matches!(RingReader::open(&path), Err(ShmError::BadMagic)));
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

fn frame(seq: u64) -> qdb_abi::Frame {
    seal_frame(
        FrameHeader {
            seq,
            event_ts_ns: seq as i64,
            recv_ts_ns: 0,
            payload_len: 0,
            msg_type: MsgType::TickTrade as u16,
            flags: 0,
            symbol_id: 1,
            exchange_id: 2,
            period_id: 0,
            partition_id: 0,
            reserved: 0,
            crc32: 0,
        },
        &seq.to_le_bytes(),
    )
}

fn temp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("qdb-shm-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("region.bin")
}
