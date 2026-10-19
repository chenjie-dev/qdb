use qdb_wal::{read_path, WalAppender, WalRecord};

#[test]
fn missing_file_is_an_empty_log() {
    let path = std::env::temp_dir().join(format!("qdb-wal-missing-{}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let read = read_path(&path).unwrap();
    assert!(read.records.is_empty());
    assert!(!read.crc_failed);
    assert_eq!(read.last_good_seq, 0);
}

#[test]
fn records_roundtrip_in_order() {
    let dir = temp("round");
    let path = dir.join("wal.qwal");
    let mut wal = WalAppender::open(&path).unwrap();
    for seq in 1..=3 {
        wal.append(&WalRecord {
            seq,
            msg_type: 1,
            flags: 0,
            payload: vec![seq as u8],
        })
        .unwrap();
    }
    wal.flush().unwrap();
    let read = wal.read().unwrap();
    assert_eq!(
        read.records
            .iter()
            .map(|record| record.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(read.records[2].payload, vec![3]);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_short_tail_is_not_reported_as_crc_damage() {
    let dir = temp("tail");
    let path = dir.join("wal.qwal");
    let mut wal = WalAppender::open(&path).unwrap();
    wal.append(&WalRecord {
        seq: 1,
        msg_type: 1,
        flags: 0,
        payload: vec![1],
    })
    .unwrap();
    wal.flush().unwrap();
    let mut raw = std::fs::read(&path).unwrap();
    raw.extend_from_slice(&[1, 2, 3, 4]);
    std::fs::write(&path, raw).unwrap();
    let read = read_path(&path).unwrap();
    assert!(!read.crc_failed);
    assert_eq!(read.records.len(), 1);
    assert_eq!(read.last_good_seq, 1);
    let _ = std::fs::remove_dir_all(dir);
}

fn temp(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("qdb-wal-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
