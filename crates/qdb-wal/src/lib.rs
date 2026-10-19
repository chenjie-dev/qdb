//! 追加 WAL。坏 CRC 停在最后一条完好记录之前。

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
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
        let body = encode_body(record);
        let crc = crc32fast::hash(&body);
        self.file
            .write_all(&crc.to_le_bytes())
            .map_err(|err| WalError::Io(err.to_string()))?;
        self.file
            .write_all(&(body.len() as u32).to_le_bytes())
            .map_err(|err| WalError::Io(err.to_string()))?;
        self.file
            .write_all(&body)
            .map_err(|err| WalError::Io(err.to_string()))?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), WalError> {
        self.file.flush().map_err(|err| WalError::Io(err.to_string()))?;
        self.file.sync_all().map_err(|err| WalError::Io(err.to_string()))?;
        Ok(())
    }

    pub fn read(&self) -> Result<WalRead, WalError> {
        read_path(&self.path)
    }
}

pub fn read_path(path: &Path) -> Result<WalRead, WalError> {
    if !path.exists() {
        return Ok(WalRead {
            records: Vec::new(),
            crc_failed: false,
            last_good_seq: 0,
        });
    }
    let mut file = File::open(path).map_err(|err| WalError::Io(err.to_string()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(|err| WalError::Io(err.to_string()))?;
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
    })
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
}
