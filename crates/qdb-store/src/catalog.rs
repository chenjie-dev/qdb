//! 一个实例目录下的多个库。每个库有自己的 WAL 和序号。
#![forbid(unsafe_code)]

use std::fs;
use std::path::{Path, PathBuf};

use qdb_wal::{read_dump, read_path, replace_wal, write_dump, WalAppender};
use thiserror::Error;

use qdb_shm::RegionConfig;

use crate::{Engine, StoreError};

#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("io: {0}")]
    Io(String),
    #[error("invalid database name")]
    InvalidName,
    #[error("database already exists")]
    Exists,
    #[error("database is not empty")]
    NotEmpty,
    #[error("database not found")]
    NotFound,
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// 一个实例目录下的多个库。每个库有自己的 WAL、序号和共享内存文件。
pub struct Catalog {
    root: PathBuf,
}

impl Catalog {
    /// 打开实例目录。
    ///
    /// 目录不存在时会创建。还没有名为 `default` 的库时，在 `db/default` 建一个空 WAL。
    /// 根目录下已有旧的 `wal/wal-1.qwal` 时，那个文件就是 `default`，不再另建。
    ///
    /// # Arguments
    ///
    /// * `root` - 实例目录，通常是 `data`。
    ///
    /// # Returns
    ///
    /// 目录句柄。这一步不映射共享内存，也不持有写入锁。
    ///
    /// # Errors
    ///
    /// 创建目录或空 WAL 失败时返回 [`CatalogError::Io`]。
    pub fn open(root: &Path) -> Result<Self, CatalogError> {
        fs::create_dir_all(root).map_err(io)?;
        let catalog = Self {
            root: root.to_path_buf(),
        };
        if !catalog.exists("default") {
            catalog.create("default")?;
        }
        Ok(catalog)
    }

    /// 只查看已经存在的实例目录。
    ///
    /// 不创建 `default`，也不建任何文件。
    ///
    /// # Arguments
    ///
    /// * `root` - 实例目录。
    ///
    /// # Errors
    ///
    /// 目录不存在时返回 [`CatalogError::Io`]。
    pub fn open_read(root: &Path) -> Result<Self, CatalogError> {
        if !root.is_dir() {
            return Err(CatalogError::Io(format!(
                "data directory not found: {}",
                root.display()
            )));
        }
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// 列出已有 WAL 的库名。
    ///
    /// # Returns
    ///
    /// 按名字排序、去重后的库名。旧布局的根目录 WAL 记为 `default`。
    ///
    /// # Errors
    ///
    /// 读 `db/` 目录失败时返回 [`CatalogError::Io`]。
    pub fn list(&self) -> Result<Vec<String>, CatalogError> {
        let mut names = Vec::new();
        if self.legacy_default() && !self.named_wal("default") {
            names.push("default".to_string());
        }
        let db_root = self.root.join("db");
        if db_root.is_dir() {
            for entry in fs::read_dir(&db_root).map_err(io)? {
                let entry = entry.map_err(io)?;
                if !entry.file_type().map_err(io)?.is_dir() {
                    continue;
                }
                let name = entry.file_name().to_string_lossy().to_string();
                if valid_name(&name) && self.named_wal(&name) {
                    names.push(name);
                }
            }
        }
        names.sort();
        names.dedup();
        Ok(names)
    }

    /// 该名字是否已经有 WAL 文件。
    ///
    /// 非法名字返回 `false`，不报错。
    ///
    /// # Arguments
    ///
    /// * `name` - 库名。合法形式是字母或 `_` 开头，后接字母、数字或 `_`，最长 32。
    pub fn exists(&self, name: &str) -> bool {
        valid_name(name) && self.wal_file(name).is_file()
    }

    /// 新建一个空库。
    ///
    /// 只创建目录和空的 `wal/wal-1.qwal`。不映射共享内存。打开后才会建 `region.bin`。
    ///
    /// # Arguments
    ///
    /// * `name` - 库名。规则见 [`Catalog::exists`]。
    ///
    /// # Returns
    ///
    /// 新库目录，位于 `db/<name>`。不映射共享内存。
    ///
    /// # Errors
    ///
    /// * [`CatalogError::InvalidName`] - 名字不合法。
    /// * [`CatalogError::Exists`] - 同名库已经有 WAL。
    /// * [`CatalogError::Io`] - 建目录或 WAL 失败。
    pub fn create(&self, name: &str) -> Result<PathBuf, CatalogError> {
        if !valid_name(name) {
            return Err(CatalogError::InvalidName);
        }
        if self.exists(name) {
            return Err(CatalogError::Exists);
        }
        let path = self.stored_path(name);
        fs::create_dir_all(path.join("wal")).map_err(io)?;
        WalAppender::open(&path.join("wal/wal-1.qwal")).map_err(io)?;
        Ok(path)
    }

    /// 已存在库的目录。
    ///
    /// # Arguments
    ///
    /// * `name` - 库名。
    ///
    /// # Returns
    ///
    /// 旧布局的 `default` 返回实例根目录，其余返回 `db/<name>`。
    ///
    /// # Errors
    ///
    /// * [`CatalogError::InvalidName`] - 名字不合法。
    /// * [`CatalogError::NotFound`] - 还没有这个库。
    pub fn path(&self, name: &str) -> Result<PathBuf, CatalogError> {
        if !valid_name(name) {
            return Err(CatalogError::InvalidName);
        }
        if !self.exists(name) {
            return Err(CatalogError::NotFound);
        }
        Ok(self.resolved_path(name))
    }

    /// 以写入者打开库，使用开发规模的共享内存，并且每条写入都 `fsync`。
    ///
    /// 会删除并重建该库的 `region.bin`。同一库不要在 `qdb` 进程持有时再调用。
    ///
    /// # Arguments
    ///
    /// * `name` - 已存在的库名。
    ///
    /// # Errors
    ///
    /// 库不存在时返回 [`CatalogError::NotFound`]。打开失败时返回 [`CatalogError::Store`]。
    pub fn open_database(&self, name: &str) -> Result<Engine, CatalogError> {
        Ok(Engine::open(&self.path(name)?)?)
    }

    /// 以写入者打开库，并打开行情中心用的组提交。
    ///
    /// 共享内存按 `region` 重建。组提交是 256 条或 1 毫秒，内存先更新，`fsync` 和共享内存发布在 [`Engine::flush`]。
    ///
    /// # Arguments
    ///
    /// * `name` - 已存在的库名。
    /// * `region` - 符号槽数和帧容量。`qdb` 进程传入 [`RegionConfig::market`]。
    ///
    /// # Errors
    ///
    /// 同 [`Catalog::open_database`]。
    pub fn open_database_with(
        &self,
        name: &str,
        region: &RegionConfig,
    ) -> Result<Engine, CatalogError> {
        let mut engine = Engine::open_with(&self.path(name)?, region)?;
        engine.set_group_commit(256, 1_000_000);
        Ok(engine)
    }

    /// 只读打开库。不删除 `region.bin`，也不追加 WAL。
    ///
    /// # Arguments
    ///
    /// * `name` - 已存在的库名。
    ///
    /// # Errors
    ///
    /// 同 [`Catalog::open_database`]。
    pub fn open_database_read(&self, name: &str) -> Result<Engine, CatalogError> {
        Ok(Engine::open_read(&self.path(name)?)?)
    }

    /// 把该库已经 `fsync` 的 WAL 记录写成 `QDBDUMP1` 文件。
    ///
    /// 不打开共享内存。文件里的序号与源库相同。
    ///
    /// # Arguments
    ///
    /// * `name` - 已存在的库名。
    /// * `file` - 转储文件路径。已有文件会被覆盖。
    ///
    /// # Returns
    ///
    /// 写入的记录条数。
    ///
    /// # Errors
    ///
    /// * [`CatalogError::NotFound`] - 库不存在。
    /// * [`CatalogError::Store`] - WAL 的 CRC 已经损坏。
    /// * [`CatalogError::Io`] - 读 WAL 或写文件失败。
    pub fn dump(&self, name: &str, file: &Path) -> Result<u64, CatalogError> {
        let loaded = read_path(&self.path(name)?.join("wal/wal-1.qwal")).map_err(io)?;
        if loaded.crc_failed {
            return Err(StoreError::Crc.into());
        }
        write_dump(file, &loaded.records).map_err(io)?;
        Ok(loaded.records.len() as u64)
    }

    /// 把转储文件覆盖进一个空库的 WAL。
    ///
    /// 记录原样写回，序号不变。目标库里已有记录时拒绝覆盖。
    ///
    /// # Arguments
    ///
    /// * `name` - 已存在且 WAL 为空的库。
    /// * `file` - `QDBDUMP1` 文件。
    ///
    /// # Returns
    ///
    /// 写回的记录条数。
    ///
    /// # Errors
    ///
    /// * [`CatalogError::NotEmpty`] - 目标库已经有记录。
    /// * [`CatalogError::NotFound`] - 库不存在。
    /// * [`CatalogError::Store`] - 源 WAL 或转储文件 CRC 损坏。
    /// * [`CatalogError::Io`] - 读写失败，或文件头不是 `QDBDUMP1`。
    pub fn restore(&self, name: &str, file: &Path) -> Result<u64, CatalogError> {
        let wal_path = self.path(name)?.join("wal/wal-1.qwal");
        let existing = read_path(&wal_path).map_err(io)?;
        if existing.crc_failed {
            return Err(StoreError::Crc.into());
        }
        if !existing.records.is_empty() {
            return Err(CatalogError::NotEmpty);
        }
        let dump = read_dump(file).map_err(io)?;
        if dump.crc_failed {
            return Err(StoreError::Crc.into());
        }
        replace_wal(&wal_path, &dump.records).map_err(io)?;
        Ok(dump.records.len() as u64)
    }

    fn resolved_path(&self, name: &str) -> PathBuf {
        if name == "default" && self.legacy_default() && !self.named_wal("default") {
            self.root.clone()
        } else {
            self.stored_path(name)
        }
    }

    fn stored_path(&self, name: &str) -> PathBuf {
        self.root.join("db").join(name)
    }

    fn wal_file(&self, name: &str) -> PathBuf {
        self.resolved_path(name).join("wal/wal-1.qwal")
    }

    fn named_wal(&self, name: &str) -> bool {
        self.stored_path(name).join("wal/wal-1.qwal").is_file()
    }

    fn legacy_default(&self) -> bool {
        self.root.join("wal/wal-1.qwal").is_file()
    }
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => {}
        _ => return false,
    }
    name.len() <= 32 && chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn io<E: std::fmt::Display>(err: E) -> CatalogError {
    CatalogError::Io(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("qdb-catalog-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn two_databases_do_not_share_symbols_or_sequences() {
        let root = temp_root("multi");
        let catalog = Catalog::open(&root).unwrap();
        catalog.create("ashare").unwrap();
        catalog.create("crypto").unwrap();
        let mut ashare = catalog.open_database("ashare").unwrap();
        let mut crypto = catalog.open_database("crypto").unwrap();
        ashare.write_trade("SSE", "600000", 1, 10, 1).unwrap();
        crypto.write_trade("BINANCE", "BTCUSDT", 1, 20, 1).unwrap();
        assert!(ashare.resolve("BINANCE:BTCUSDT").is_err());
        assert!(crypto.resolve("SSE:600000").is_err());
        assert_eq!(ashare.applied_seq(), 2);
        assert_eq!(crypto.applied_seq(), 2);
        drop(ashare);
        drop(crypto);
        let read = Catalog::open_read(&root).unwrap();
        let names = read.list().unwrap();
        assert!(names.contains(&"ashare".to_string()));
        assert!(names.contains(&"crypto".to_string()));
        assert!(names.contains(&"default".to_string()));
        let ashare = read.open_database_read("ashare").unwrap();
        assert!(ashare.resolve("SSE:600000").is_ok());
        assert!(read.create("ashare").is_err());
        assert!(read.create("bad-name").is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_wal_at_the_instance_root_is_the_default_database() {
        let root = temp_root("legacy");
        let mut engine = Engine::open(&root).unwrap();
        engine.write_trade("SSE", "600000", 1, 10, 1).unwrap();
        drop(engine);
        let catalog = Catalog::open_read(&root).unwrap();
        assert_eq!(catalog.list().unwrap(), vec!["default".to_string()]);
        assert_eq!(catalog.path("default").unwrap(), root);
        let read = catalog.open_database_read("default").unwrap();
        assert!(read.resolve("SSE:600000").is_ok());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn dump_restores_the_same_bars_and_replay_repeats_the_prices() {
        let root = temp_root("dump");
        let catalog = Catalog::open(&root).unwrap();
        let mut source = catalog.open_database("default").unwrap();
        source
            .write_trade("SSE", "600000", 1_704_160_200_000_000_000, 10, 2)
            .unwrap();
        source
            .write_trade("SSE", "600000", 1_704_160_230_000_000_000, 12, 3)
            .unwrap();
        let bars = source.klines(source.resolve("SSE:600000").unwrap(), qdb_abi::Period::M1);
        drop(source);
        let file = root.join("market.qdump");
        let n = catalog.dump("default", &file).unwrap();
        assert!(n >= 2);
        catalog.create("copy").unwrap();
        catalog.restore("copy", &file).unwrap();
        let copy = catalog.open_database_read("copy").unwrap();
        let id = copy.resolve("SSE:600000").unwrap();
        assert_eq!(copy.klines(id, qdb_abi::Period::M1), bars);
        assert_eq!(copy.ticks(id).len(), 2);
        assert!(catalog.restore("default", &file).is_err());
        let played = crate::playback(&file).unwrap();
        let prices: Vec<i64> = played
            .iter()
            .filter(|change| change.table == "ticks")
            .map(|change| change.price)
            .collect();
        assert_eq!(prices, vec![10, 12]);
        let source = catalog.open_database_read("default").unwrap();
        assert_eq!(source.ticks(source.resolve("SSE:600000").unwrap()).len(), 2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_invalid_duplicate_and_unknown_database_names() {
        let root = temp_root("names");
        let catalog = Catalog::open(&root).unwrap();
        let long = "a".repeat(33);
        for name in ["", "1share", "bad-name", "a/b", "..", long.as_str()] {
            assert!(
                matches!(catalog.create(name), Err(CatalogError::InvalidName)),
                "{name}"
            );
        }
        assert!(matches!(catalog.create("_ok"), Ok(_)));
        assert!(matches!(catalog.create("_ok"), Err(CatalogError::Exists)));
        assert!(matches!(
            catalog.dump("missing", &root.join("x.qdump")),
            Err(CatalogError::NotFound)
        ));
        assert!(matches!(
            catalog.restore("missing", &root.join("x.qdump")),
            Err(CatalogError::NotFound)
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn restore_rejects_a_bad_dump_and_replay_keeps_quote_prices() {
        let root = temp_root("quote");
        let catalog = Catalog::open(&root).unwrap();
        let mut source = catalog.open_database("default").unwrap();
        source
            .write_quote(
                "SSE",
                "600000",
                1_704_160_200_000_000_000,
                &qdb_abi::TickQuote {
                    last: 11,
                    volume: 0,
                    amount: 0,
                    bid_px: [10, 0, 0, 0, 0],
                    bid_qty: [1, 0, 0, 0, 0],
                    ask_px: [12, 0, 0, 0, 0],
                    ask_qty: [1, 0, 0, 0, 0],
                },
            )
            .unwrap();
        source
            .write_clock("SSE", 1_704_160_260_000_000_000)
            .unwrap();
        drop(source);
        let file = root.join("tape.qdump");
        catalog.dump("default", &file).unwrap();
        std::fs::write(&file, b"NOTADUMP").unwrap();
        catalog.create("copy").unwrap();
        assert!(matches!(
            catalog.restore("copy", &file),
            Err(CatalogError::Io(_))
        ));
        catalog.dump("default", &file).unwrap();
        catalog.restore("copy", &file).unwrap();
        let copy = catalog.open_database_read("copy").unwrap();
        let quote = copy.quote(copy.resolve("SSE:600000").unwrap()).unwrap();
        assert_eq!((quote.last, quote.bid_px[0], quote.ask_px[0]), (11, 10, 12));
        let played = crate::playback(&file).unwrap();
        assert!(played
            .iter()
            .any(|change| change.table == "snapshots" && change.bid == 10 && change.ask == 12));
        let _ = fs::remove_dir_all(root);
    }
}
