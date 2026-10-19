//! 追加 WAL。坏 CRC 停在最后一条完好记录之前。

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use qdb_abi::MsgType;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum WalError {
    #[error("io: {0}")]
    Io(String),
    #[error("crc failed after seq {last_good_seq}")]
    Crc { last_good_seq: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalRecord {
    pub seq: u64,
    pub msg_type: u16,
    pub flags: u16,
    pub payload: Vec<u8>,
}

pub struct WalAppender {
    file: File,
    path: std::path::PathBuf,
}

pub struct WalRead {
    pub records: Vec<WalRecord>,
    pub crc_failed: bool,
    pub last_good_seq: u64,
    /// 下一次读取的起点。短尾停在不完整记录之前。
    pub consumed: u64,
}

impl WalAppender {
    pub fn open(path: &Path) -> Result<Self, WalError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| WalError::Io(err.to_string()))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .map_err(|err| WalError::Io(err.to_string()))?;
        Ok(Self {
            file,
            path: path.to_path_buf(),
        })
    }

    pub fn append(&mut self, record: &WalRecord) -> Result<(), WalError> {
        let bytes = encode_framed(record);
        self.file
            .write_all(&bytes)
            .map_err(|err| WalError::Io(err.to_string()))?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), WalError> {
        self.file
            .flush()
            .map_err(|err| WalError::Io(err.to_string()))?;
        self.file
            .sync_all()
            .map_err(|err| WalError::Io(err.to_string()))?;
        Ok(())
    }

    pub fn read(&self) -> Result<WalRead, WalError> {
        read_path(&self.path)
    }
}

pub fn read_path(path: &Path) -> Result<WalRead, WalError> {
    read_from(path, 0)
}

/// 从 `from` 读取新写完的记录。文件后部不足一条时不前进 `consumed`。
pub fn read_from(path: &Path, from: u64) -> Result<WalRead, WalError> {
    if !path.exists() {
        return Ok(WalRead {
            records: Vec::new(),
            crc_failed: false,
            last_good_seq: 0,
            consumed: from,
        });
    }
    let mut file = File::open(path).map_err(|err| WalError::Io(err.to_string()))?;
    let len = file
        .metadata()
        .map_err(|err| WalError::Io(err.to_string()))?
        .len();
    if from > len {
        return Err(WalError::Io(format!(
            "wal truncated at {len}, reader was at {from}"
        )));
    }
    file.seek(SeekFrom::Start(from))
        .map_err(|err| WalError::Io(err.to_string()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|err| WalError::Io(err.to_string()))?;
    parse_bytes(&bytes, from)
}

fn parse_bytes(bytes: &[u8], base: u64) -> Result<WalRead, WalError> {
    let mut records = Vec::new();
    let mut offset = 0;
    let mut last_good_seq = 0;
    while offset + 8 <= bytes.len() {
        let crc = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let body_at = offset + 8;
        if body_at + len > bytes.len() {
            break;
        }
        let body = &bytes[body_at..body_at + len];
        if crc32fast::hash(body) != crc {
            return Ok(WalRead {
                records,
                crc_failed: true,
                last_good_seq,
                consumed: base + offset as u64,
            });
        }
        let record = decode_body(body).ok_or_else(|| WalError::Crc { last_good_seq })?;
        last_good_seq = record.seq;
        records.push(record);
        offset = body_at + len;
    }
    Ok(WalRead {
        records,
        crc_failed: false,
        last_good_seq,
        consumed: base + offset as u64,
    })
}

pub const DUMP_MAGIC: &[u8; 8] = b"QDBDUMP1";

pub fn write_dump(path: &Path, records: &[WalRecord]) -> Result<(), WalError> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|err| WalError::Io(err.to_string()))?;
        }
    }
    let mut file = File::create(path).map_err(|err| WalError::Io(err.to_string()))?;
    file.write_all(DUMP_MAGIC)
        .map_err(|err| WalError::Io(err.to_string()))?;
    for record in records {
        file.write_all(&encode_framed(record))
            .map_err(|err| WalError::Io(err.to_string()))?;
    }
    file.sync_all()
        .map_err(|err| WalError::Io(err.to_string()))?;
    Ok(())
}

pub fn parse_dump_body(bytes: &[u8]) -> Result<WalRead, WalError> {
    parse_bytes(bytes, 0)
}

pub fn read_dump(path: &Path) -> Result<WalRead, WalError> {
    let mut file = File::open(path).map_err(|err| WalError::Io(err.to_string()))?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)
        .map_err(|err| WalError::Io(err.to_string()))?;
    if &magic != DUMP_MAGIC {
        return Err(WalError::Io("not a qdb dump".into()));
    }
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|err| WalError::Io(err.to_string()))?;
    parse_bytes(&bytes, 0)
}

/// 用这些记录替换 WAL 文件。调用方要保证没有别的写入者正打开它。
pub fn replace_wal(path: &Path, records: &[WalRecord]) -> Result<(), WalError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| WalError::Io(err.to_string()))?;
    }
    let mut file = File::create(path).map_err(|err| WalError::Io(err.to_string()))?;
    for record in records {
        file.write_all(&encode_framed(record))
            .map_err(|err| WalError::Io(err.to_string()))?;
    }
    file.sync_all()
        .map_err(|err| WalError::Io(err.to_string()))?;
    Ok(())
}

fn encode_framed(record: &WalRecord) -> Vec<u8> {
    let body = encode_body(record);
    let crc = crc32fast::hash(&body);
    let mut out = Vec::with_capacity(8 + body.len());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&body);
    out
}

fn encode_body(record: &WalRecord) -> Vec<u8> {
    let mut body = Vec::with_capacity(12 + record.payload.len());
    body.extend_from_slice(&record.seq.to_le_bytes());
    body.extend_from_slice(&record.msg_type.to_le_bytes());
    body.extend_from_slice(&record.flags.to_le_bytes());
    body.extend_from_slice(&record.payload);
    body
}

fn decode_body(body: &[u8]) -> Option<WalRecord> {
    if body.len() < 12 {
        return None;
    }
    Some(WalRecord {
        seq: u64::from_le_bytes(body[0..8].try_into().ok()?),
        msg_type: u16::from_le_bytes(body[8..10].try_into().ok()?),
        flags: u16::from_le_bytes(body[10..12].try_into().ok()?),
        payload: body[12..].to_vec(),
    })
}

pub fn msg(record: &WalRecord) -> Option<MsgType> {
    MsgType::from_u16(record.msg_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bad_crc_stops_before_the_corrupt_record() {
        let dir = std::env::temp_dir().join(format!("qdb-wal-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("wal.qwal");
        let _ = std::fs::remove_file(&path);
        let mut wal = WalAppender::open(&path).unwrap();
        wal.append(&WalRecord {
            seq: 1,
            msg_type: 1,
            flags: 0,
            payload: vec![1, 2, 3, 4],
        })
        .unwrap();
        wal.append(&WalRecord {
            seq: 2,
            msg_type: 1,
            flags: 0,
            payload: vec![5],
        })
        .unwrap();
        wal.flush().unwrap();
        let mut raw = std::fs::read(&path).unwrap();
        let bogus = encode_body(&WalRecord {
            seq: 3,
            msg_type: 1,
            flags: 0,
            payload: vec![9],
        });
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.extend_from_slice(&(bogus.len() as u32).to_le_bytes());
        raw.extend_from_slice(&bogus);
        std::fs::write(&path, raw).unwrap();
        let read = read_path(&path).unwrap();
        assert!(read.crc_failed);
        assert_eq!(read.records.len(), 2);
        assert_eq!(read.last_good_seq, 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn read_from_returns_only_records_appended_after_the_cursor() {
        let dir = std::env::temp_dir().join(format!("qdb-wal-tail-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
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
        let first = read_path(&path).unwrap();
        assert_eq!(first.records.len(), 1);
        wal.append(&WalRecord {
            seq: 2,
            msg_type: 1,
            flags: 0,
            payload: vec![2],
        })
        .unwrap();
        wal.flush().unwrap();
        let next = read_from(&path, first.consumed).unwrap();
        assert_eq!(next.records.len(), 1);
        assert_eq!(next.records[0].seq, 2);
        assert!(next.consumed > first.consumed);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn dump_rejects_bad_magic_and_ignores_a_short_tail() {
        let dir = std::env::temp_dir().join(format!("qdb-dump-edge-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tape.qdump");
        std::fs::write(&path, b"NOTADUMP").unwrap();
        assert!(read_dump(&path).is_err());

        write_dump(&path, &[]).unwrap();
        let mut raw = std::fs::read(&path).unwrap();
        raw.extend_from_slice(&[1, 2, 3, 4]);
        std::fs::write(&path, &raw).unwrap();
        let read = read_dump(&path).unwrap();
        assert!(!read.crc_failed);
        assert!(read.records.is_empty());

        write_dump(
            &path,
            &[WalRecord {
                seq: 1,
                msg_type: 1,
                flags: 0,
                payload: vec![9, 9, 9, 9],
            }],
        )
        .unwrap();
        let mut raw = std::fs::read(&path).unwrap();
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        std::fs::write(&path, &raw).unwrap();
        let read = read_dump(&path).unwrap();
        assert!(read.crc_failed);
        assert!(read.records.is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
