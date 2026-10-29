//! 内存表、WAL 应用和启动恢复。
//!
//! [`Engine`] 是一个库的写入或只读视图。先把记录写入 WAL，再更新内存，最后发布共享内存。
//! 使用者说明见 `docs/manual/library.md`。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use qdb_abi::{
    code_str, decode_trade, encode_kline, encode_trade, fixed_code, seal_frame, Frame, FrameHeader,
    KlineBody, MsgType, Period, TickQuote, TickTrade,
};
use qdb_agg::{BarEvent, KlineAggregator, TradeInput};
use qdb_shm::{RegionConfig, ShmRegion};
use qdb_wal::{parse_wal, read_span, replace_wal, WalAppender, WalRead, WalRecord};
use thiserror::Error;

mod catalog;
mod checkpoint;
mod dump;
mod table;
pub use catalog::{Catalog, CatalogError};
pub use dump::{playback, replay_bytes, replay_file};
pub use table::{Cell, ColumnDef, ColumnType, IndexDef, TableSchema};

/// 写入、查询或恢复失败。
///
/// [`StoreError::Io`] 和 [`StoreError::Schema`] 的字符串说明具体原因。其余变体各自对应一种固定失败。
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("io: {0}")]
    Io(String),
    #[error("exchange label is empty")]
    EmptyExchange,
    #[error("read-only connection")]
    ReadOnly,
    #[error("table already exists")]
    TableExists,
    #[error("duplicate key")]
    DuplicateKey,
    #[error("key not found")]
    KeyNotFound,
    #[error("seq conflict")]
    Conflict,
    #[error("type mismatch")]
    TypeMismatch,
    #[error("schema: {0}")]
    Schema(String),
    #[error("symbol not found")]
    NotFound,
    #[error("ambiguous symbol")]
    Ambiguous,
    #[error("wal crc failed")]
    Crc,
    #[error("gap in wal")]
    Gap,
    #[error("stale epoch")]
    StaleEpoch,
    #[error(transparent)]
    Agg(#[from] qdb_agg::AggError),
}

/// 已登记的证券。`id` 从 1 起，在本库内不变。`exchange` 只是调用方标签。
#[derive(Clone, Debug)]
pub struct Symbol {
    pub id: u32,
    pub exchange_id: u16,
    pub code: String,
    pub price_scale: u16,
    pub exchange: String,
}

/// 一笔成交。价格和金额是定点 `i64`，`ts` 是 UTC 纳秒，`amount` 为价格乘成交量。
#[derive(Clone, Debug)]
pub struct TickRow {
    pub seq: u64,
    pub ts: i64,
    pub price: i64,
    pub volume: i64,
    pub amount: i64,
}

/// [`Engine::changes_since`] 返回的一行。`kind` 为 `snapshot`、`trade` 或 `quote`。
#[derive(Clone, Debug)]
pub struct LiveRow {
    pub seq: u64,
    pub symbol: String,
    pub kind: &'static str,
    pub ts: i64,
    pub price: i64,
    pub volume: i64,
    pub last: i64,
    pub bid_px: [i64; 5],
    pub ask_px: [i64; 5],
}

/// 最新报价。数组下标 0 是买一或卖一，未使用的档为 0。
#[derive(Clone, Debug)]
pub struct QuoteRow {
    pub seq: u64,
    pub last: i64,
    pub bid_px: [i64; 5],
    pub bid_qty: [i64; 5],
    pub ask_px: [i64; 5],
    pub ask_qty: [i64; 5],
}

/// [`Engine::poll`] 或回放返回的一条变更。`table` 是 `ticks`、`snapshots` 或 `klines`。
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub seq: u64,
    pub table: &'static str,
    pub symbol: String,
    pub ts: i64,
    pub price: i64,
    pub volume: i64,
    pub last: i64,
    pub bid: i64,
    pub ask: i64,
    pub period: &'static str,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
}

/// 可跨线程读取的计数。原子量与引擎共享，读取不阻塞写入。
#[derive(Clone, Debug)]
pub struct Metrics {
    pub durable_seq: Arc<AtomicU64>,
    pub applied_seq: Arc<AtomicU64>,
    pub write_seq: Arc<AtomicU64>,
    pub gap_total: Arc<AtomicU64>,
    pub late_tick_total: Arc<AtomicU64>,
    pub crc_failed: Arc<AtomicBool>,
    pub heartbeat_ns: Arc<AtomicU64>,
    pub subscriber_count: Arc<AtomicU64>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            durable_seq: Arc::new(AtomicU64::new(0)),
            applied_seq: Arc::new(AtomicU64::new(0)),
            write_seq: Arc::new(AtomicU64::new(0)),
            gap_total: Arc::new(AtomicU64::new(0)),
            late_tick_total: Arc::new(AtomicU64::new(0)),
            crc_failed: Arc::new(AtomicBool::new(false)),
            heartbeat_ns: Arc::new(AtomicU64::new(0)),
            subscriber_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// 一个库的内存表。
///
/// 写入者持有 WAL 追加器和 `region.bin` 映射。只读打开只重放 WAL，不创建、不删除共享内存文件。
/// 同一台机器上只有一个 `qdb` 进程可以成为写入者。
pub struct Engine {
    root: PathBuf,
    wal: Option<WalAppender>,
    agg: KlineAggregator,
    symbols: Vec<Symbol>,
    by_key: HashMap<(u16, String), u32>,
    exchanges: Vec<String>,
    ticks: HashMap<u32, Vec<TickRow>>,
    quotes: HashMap<u32, QuoteRow>,
    frames: Vec<Frame>,
    shm: Option<ShmRegion>,
    next_seq: u64,
    metrics: Metrics,
    crc_failed: bool,
    readonly: bool,
    wal_pos: u64,
    tables: table::TableSet,
    region: RegionConfig,
    group_limit: u64,
    group_interval_ns: u64,
    group_started_ns: u64,
    pending: Vec<Frame>,
    flush_count: u64,
    /// 重放期间先只更新内部结构，结束时再整表重建。
    defer_rows: bool,
    leader_epoch: u64,
    /// 任期被更高的 epoch 取代，或本进程是副本时，不再分配新序号。复制进来的记录仍可应用。
    fenced: bool,
    repl_id: [u8; 16],
    repl_id2: [u8; 16],
    /// 换成新复制 ID 时，旧 ID 仍能接到这个偏移为止的历史。
    offset2: u64,
    /// 本机 `fsync` 之后，要等副本应答才把记录交给订阅者和共享内存。
    wait_majority: bool,
    /// `wal/visible.pos` 里订阅者可以读到的字节偏移。
    visible_pos: u64,
    /// 订阅者能看见的最大序号。还没发布的写入可以已经在内存里。
    published_seq: u64,
    /// 上次发布时的报价。最新报价可能还在等副本。
    published_quotes: HashMap<u32, QuoteRow>,
    /// 已落盘、还没发布的共享内存帧。
    held: Vec<HeldPublish>,
}

struct HeldPublish {
    offset: u64,
    seq: u64,
    frames: Vec<Frame>,
    quotes: HashMap<u32, QuoteRow>,
}

impl Engine {
    /// 用开发规模的共享内存打开库，并成为写入者。
    ///
    /// 等价于 [`Engine::open_with`]，区域是 [`RegionConfig::dev`]：1024 个符号槽、4096 帧。
    ///
    /// # Arguments
    ///
    /// * `root` - 库目录。不存在时会创建 `wal/` 和 `snap/`。
    ///
    /// # Returns
    ///
    /// 可写引擎。默认每条写入都 `fsync` 并发布共享内存。
    ///
    /// # Errors
    ///
    /// 创建目录、读写 WAL 或映射共享内存失败时返回 [`StoreError::Io`]。
    /// 坏 CRC 时仍返回引擎，[`Engine::crc_failed`] 为真，后续写入返回 [`StoreError::Crc`]。
    pub fn open(root: &Path) -> Result<Self, StoreError> {
        Self::open_with(root, &RegionConfig::dev())
    }

    /// 按给定共享内存规模打开库，并成为写入者。
    ///
    /// 会删除已有的 `region.bin` 再按 `region` 重建。有效检查点存在时先装入检查点，再只重放其后的 WAL。
    ///
    /// # Arguments
    ///
    /// * `root` - 库目录。
    /// * `region` - 符号槽数和环形缓冲容量。容量必须是 2 的幂。
    ///
    /// # Returns
    ///
    /// 可写引擎。
    ///
    /// # Errors
    ///
    /// 同 [`Engine::open`]。容量不合法时，错误来自共享内存创建，包在 [`StoreError::Io`] 里。
    pub fn open_with(root: &Path, region: &RegionConfig) -> Result<Self, StoreError> {
        fs::create_dir_all(root.join("wal")).map_err(io)?;
        fs::create_dir_all(root.join("snap")).map_err(io)?;
        let wal_path = root.join("wal/wal-1.qwal");
        let wal = WalAppender::open(&wal_path).map_err(io)?;
        let plan = load_for_open(root, &wal_path, false)?;
        let mut engine = Self::from_replay(root, Some(wal), plan, region)?;
        engine.attach_shm()?;
        engine.sync_metrics();
        Ok(engine)
    }

    /// 只读打开库。
    ///
    /// 重放 WAL 或检查点加尾部记录。不创建目录，不追加日志，不删除 `region.bin`。
    /// 存在 `wal/visible.pos` 时只重放到那个偏移，订阅者先看不见还没被多数副本确认的尾部。
    ///
    /// # Arguments
    ///
    /// * `root` - 已经存在的库目录。
    ///
    /// # Returns
    ///
    /// 只读引擎。写入方法返回 [`StoreError::ReadOnly`]。
    ///
    /// # Errors
    ///
    /// 目录不存在或 WAL 读失败时返回 [`StoreError::Io`]。
    pub fn open_read(root: &Path) -> Result<Self, StoreError> {
        if !root.is_dir() {
            return Err(StoreError::Io(format!(
                "data directory not found: {}",
                root.display()
            )));
        }
        let wal_path = root.join("wal/wal-1.qwal");
        let plan = load_for_open(root, &wal_path, true)?;
        let engine = Self::from_replay(root, None, plan, &RegionConfig::dev())?;
        Ok(engine)
    }

    fn from_replay(
        root: &Path,
        wal: Option<WalAppender>,
        plan: OpenPlan,
        region: &RegionConfig,
    ) -> Result<Self, StoreError> {
        let readonly = wal.is_none();
        let loaded = &plan.loaded;
        let mut engine = Self {
            root: root.to_path_buf(),
            wal,
            agg: KlineAggregator::default(),
            symbols: Vec::new(),
            by_key: HashMap::new(),
            exchanges: Vec::new(),
            ticks: HashMap::new(),
            quotes: HashMap::new(),
            frames: Vec::new(),
            shm: None,
            next_seq: 1,
            metrics: Metrics::new(),
            crc_failed: loaded.crc_failed,
            readonly,
            wal_pos: loaded.consumed,
            tables: table::TableSet::default(),
            region: region.clone(),
            group_limit: 1,
            group_interval_ns: 0,
            group_started_ns: 0,
            pending: Vec::new(),
            flush_count: 0,
            defer_rows: true,
            leader_epoch: 1,
            fenced: false,
            repl_id: [0; 16],
            repl_id2: [0; 16],
            offset2: 0,
            wait_majority: false,
            visible_pos: 0,
            published_seq: 0,
            published_quotes: HashMap::new(),
            held: Vec::new(),
        };
        if let Some(checkpoint) = plan.checkpoint {
            engine.install_checkpoint(checkpoint)?;
        }
        for record in &loaded.records {
            engine.apply_record(record)?;
        }
        engine.defer_rows = false;
        engine.rebuild_business_rows();
        engine.load_or_create_repl();
        if loaded.crc_failed || plan.tail_crc {
            engine.crc_failed = true;
            engine.metrics.crc_failed.store(true, Ordering::Release);
        }
        if plan.gated {
            engine.wait_majority = true;
            engine.visible_pos = plan.visible_pos;
            engine.published_seq = engine.applied_seq();
            engine.published_quotes = engine.quotes.clone();
        }
        for record in &plan.tail {
            engine.apply_record(record)?;
        }
        engine.wal_pos = if plan.gated {
            plan.durable_end
        } else {
            loaded.consumed
        };
        engine.next_seq = engine.agg.last_applied_seq() + 1;
        if engine.readonly {
            engine.sync_metrics();
        }
        Ok(engine)
    }

    /// 复制当前指标句柄。
    ///
    /// 返回的原子量和引擎共享。调用后可以在别的线程读取序号和心跳。
    ///
    /// # Returns
    ///
    /// 克隆后的 [`Metrics`]。不增加落盘次数。
    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    /// 打开或后续重放是否见过坏 CRC。
    ///
    /// # Returns
    ///
    /// 坏 CRC 之后引擎拒绝新的写入。已经重放的记录仍可查询。
    pub fn crc_failed(&self) -> bool {
        self.crc_failed
    }

    /// 列出用户用 `CREATE TABLE` 建的表。
    ///
    /// 不含 `symbols`、`ticks`、`snapshots`、`klines`。
    ///
    /// # Returns
    ///
    /// 按名字排序的表名。
    pub fn table_names(&self) -> Vec<String> {
        self.tables
            .names()
            .into_iter()
            .filter(|name| !table::BUILTIN_TABLES.contains(&name.as_str()))
            .collect()
    }

    /// 读取调用方表里这个主键当前可见的整行。
    ///
    /// 不写 WAL，不分配 `seq`。墓碑之后的键与从未插入过一样，返回 `None`。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成的表。
    /// * `key` - 主键列的值，顺序与建表时的主键列一致。
    ///
    /// # Returns
    ///
    /// 当前可见行，列顺序与建表时一致。没有可见行时为 `None`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 没有主键，或值的个数不对。
    /// * [`StoreError::TypeMismatch`] - 值的类型和主键列不一致。
    /// * [`StoreError::KeyNotFound`] - 主键索引指向的行槽不存在。
    pub fn visible_row(&self, table: &str, key: &[Cell]) -> Result<Option<Vec<Cell>>, StoreError> {
        self.tables.visible_row(table, key)
    }

    /// 读取一张表的列和索引定义。
    ///
    /// `symbols`、`ticks`、`snapshots`、`klines` 是引擎登记的普通表。其余名字来自 `CREATE TABLE`。
    ///
    /// # Arguments
    ///
    /// * `name` - 表名，小写。
    ///
    /// # Returns
    ///
    /// 表不存在时返回 `None`。
    pub fn table_schema(&self, name: &str) -> Option<&TableSchema> {
        table::quote_schema(name).or_else(|| self.tables.schema(name))
    }

    /// 创建一张用户表，并把定义写入 WAL。
    ///
    /// 不能覆盖行情内置表。默认会 `fsync`。组提交打开时，定义要等 [`Engine::flush`] 才落盘。
    ///
    /// # Arguments
    ///
    /// * `schema` - 至少一列。多个 `PRIMARY KEY` 列合成一个键。`STRING` 列的值以后不能超过 1024 字节。
    ///
    /// # Returns
    ///
    /// 这条建表记录的 `seq`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::TableExists`] - 同名表已经存在。
    /// * [`StoreError::Schema`] - 没有列、列名重复、索引指向不存在的列，或表名是 `symbols`、`ticks`、`snapshots`、`klines`。主键可以有多列，合起来是一个键。
    /// * [`StoreError::ReadOnly`] - 只读引擎。
    /// * [`StoreError::Crc`] - WAL 已损坏。
    pub fn create_table(&mut self, schema: TableSchema) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        table::TableSet::validate_new(&schema)?;
        if self.tables.schema(&schema.name).is_some() {
            return Err(StoreError::TableExists);
        }
        let payload = table::TableSet::encode_schema(&schema);
        self.commit(MsgType::TableSchema, 0, 0, 0, 0, &payload, 0)
    }

    /// 从内存里去掉一张调用方建成的表。
    ///
    /// 已经 `fsync` 的建表记录和行留在 WAL 里。这次提交之后，查询、`SHOW TABLES` 和 `DESCRIBE` 都看不到它。同名可以再 `CREATE TABLE`。默认每次调用都 `fsync`。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成的表名。不能是 `symbols`、`ticks`、`snapshots`、`klines`。
    ///
    /// # Returns
    ///
    /// 这条删除记录的 `seq`。它不是表里某一行的序号。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 表名是引擎登记的四张表之一。
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn drop_table(&mut self, table: &str) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        if table::BUILTIN_TABLES.contains(&table) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        if self.tables.schema(table).is_none() {
            return Err(StoreError::NotFound);
        }
        let payload = table::TableSet::encode_drop(table);
        self.commit(MsgType::TableDrop, 0, 0, 0, 0, &payload, 0)
    }

    /// 清掉一张调用方建成的表里当前可见的行。
    ///
    /// 表定义还在。已经 `fsync` 的行留在 WAL 里，查询不再返回它们。之后可以再 `INSERT`。默认每次调用都 `fsync`。空表也写一条记录。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成的表名。不能是 `symbols`、`ticks`、`snapshots`、`klines`。
    ///
    /// # Returns
    ///
    /// 这条清空记录的 `seq`。它不是某一行原来的序号。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 表名是引擎登记的四张表之一。
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn truncate_table(&mut self, table: &str) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        if table::BUILTIN_TABLES.contains(&table) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        if self.tables.schema(table).is_none() {
            return Err(StoreError::NotFound);
        }
        let payload = table::TableSet::encode_truncate(table);
        self.commit(MsgType::TableTruncate, 0, 0, 0, 0, &payload, 0)
    }

    /// 把一张调用方建成的表改名。
    ///
    /// 行、可见序号、截止时间和索引留在内存里，不给每一行分配新序号。这次提交自己有一条 `seq` 并 `fsync`。已经落盘的旧记录仍写着改名前的名字。旧名字之后可以再 `CREATE TABLE`。空表也写一条记录。
    ///
    /// # Arguments
    ///
    /// * `from` - 当前表名。不能是 `symbols`、`ticks`、`snapshots`、`klines`。
    /// * `to` - 新表名。不能是这四张表，也不能已经有一张调用方表。与 `from` 相同视为新名字已占用。
    ///
    /// # Returns
    ///
    /// 这条改名记录的 `seq`。它不是表里某一行的序号。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - `from` 不存在。
    /// * [`StoreError::TableExists`] - `to` 已经有一张调用方表，或与 `from` 相同。
    /// * [`StoreError::Schema`] - `from` 或 `to` 是引擎登记的四张表之一。
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn rename_table(&mut self, from: &str, to: &str) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        if table::BUILTIN_TABLES.contains(&from) || table::BUILTIN_TABLES.contains(&to) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        if self.tables.schema(from).is_none() {
            return Err(StoreError::NotFound);
        }
        if self.tables.schema(to).is_some() {
            return Err(StoreError::TableExists);
        }
        let payload = table::TableSet::encode_rename(from, to);
        self.commit(MsgType::TableRename, 0, 0, 0, 0, &payload, 0)
    }

    /// 按建表时的列顺序插入一行。
    ///
    /// 主键重复时不写入，也不分配 `seq`。成功时这条记录的 `seq` 成为该主键的可见序号。
    /// 默认每次调用都 `fsync`；组提交打开后要再调用 [`Engine::flush`]。
    ///
    /// # Arguments
    ///
    /// * `table` - 已经存在的调用方表名。不能是引擎登记的四张表。
    /// * `values` - 与列顺序、列数、类型一一对应。`STRING` 最长 1024 字节。
    ///
    /// # Returns
    ///
    /// 这条插入记录的 `seq`。序号从 1 起，在本分区内严格加一。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::TypeMismatch`] - 某个值的类型和列定义不一致。
    /// * [`StoreError::DuplicateKey`] - 主键已经有可见行。
    /// * [`StoreError::Schema`] - 列数不对，或字符串超过 1024 字节。
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn insert_row(&mut self, table: &str, values: &[Cell]) -> Result<u64, StoreError> {
        self.insert_timed(table, values, None, None)
    }

    /// 仅当主键当前可见序号等于 `expected` 时写入。
    ///
    /// 比较发生在写 WAL 之前。对不上就返回，磁盘上的记录不变。`expected` 为 0 只表示这个键还必须不存在，不能用来覆盖序号恰好是 0 的旧快照行。大于 0 时在原槽位覆盖这一行，新的 `seq` 成为下次要带上的期望值。默认每次调用都 `fsync`。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方用 `CREATE TABLE` 建成、并且至少有一列主键的表。
    /// * `values` - 与列顺序、列数、类型一一对应。主键列指出要比较的那一行。
    /// * `expected` - 调用方上次读到的可见 `seq`。0 表示键应当还不存在。
    ///
    /// # Returns
    ///
    /// 这次提交的 `seq`。覆盖成功时它大于 `expected`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::Conflict`] - 可见序号不是 `expected`，或 `expected` 为 0 但键已经存在。
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 没有主键、表是引擎登记的四张表之一，或字符串超过 1024 字节。
    /// * [`StoreError::TypeMismatch`] - 某个值的类型和列定义不一致。
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn insert_expect(
        &mut self,
        table: &str,
        values: &[Cell],
        expected: u64,
    ) -> Result<u64, StoreError> {
        self.insert_timed(table, values, Some(expected), None)
    }

    /// 插入或按期望序号覆盖，并可记下截止时间。
    ///
    /// `expire_at` 为 `Some(0)` 或 `None` 且这是新行时，表示没有截止时间。非 0 的截止时间是 UTC 纳秒。`EXPIRE AT` 用调用方给出的水位比较，不读本机时钟。默认每次调用都 `fsync`。条件不符时不写 WAL。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成的表。带 `expected` 或截止时间时必须有主键。
    /// * `values` - 与列顺序、列数、类型一一对应。
    /// * `expected` - `None` 表示普通插入。`Some(0)` 表示键必须不存在。`Some(n)` 覆盖可见序号为 `n` 的行。
    /// * `expire_at` - `None` 表示这次不改已有截止时间。`Some` 写入新的截止时间，`0` 表示取消。
    ///
    /// # Returns
    ///
    /// 这次提交的 `seq`。
    ///
    /// # Errors
    ///
    /// 与 [`Engine::insert_row`] 和 [`Engine::insert_expect`] 相同。
    pub fn insert_timed(
        &mut self,
        table: &str,
        values: &[Cell],
        expected: Option<u64>,
        expire_at: Option<i64>,
    ) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        if expected.is_none() && expire_at.is_none() {
            let payload = self.tables.encode_row(table, values)?;
            return self.commit(MsgType::RowInsert, 0, 0, 0, 0, &payload, 0);
        }
        if table::BUILTIN_TABLES.contains(&table) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        let schema = self
            .tables
            .schema(table)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        let key: Vec<Cell> = values
            .iter()
            .zip(&schema.columns)
            .filter(|(_, column)| column.primary_key)
            .map(|(value, _)| value.clone())
            .collect();
        if key.is_empty() {
            return Err(StoreError::Schema("primary key required".into()));
        }
        if let Some(expected) = expected {
            // 0 只表示键不存在。先比较再写 WAL，冲突不会留下半条覆盖。
            match self.tables.visible_seq(table, &key)? {
                None if expected == 0 => {}
                Some(seq) if seq == expected && expected != 0 => {
                    let payload = self
                        .tables
                        .encode_expect(table, values, expected, expire_at)?;
                    return self.commit(MsgType::RowReplace, 0, 0, 0, 0, &payload, 0);
                }
                _ => return Err(StoreError::Conflict),
            }
        }
        let mut payload = self.tables.encode_row(table, values)?;
        if let Some(expire_at) = expire_at {
            if expire_at != 0 {
                payload.extend_from_slice(&expire_at.to_le_bytes());
            }
        }
        self.commit(MsgType::RowInsert, 0, 0, 0, 0, &payload, 0)
    }

    /// 一次插入多行。先把每一行都编码完，再按顺序提交。
    ///
    /// 后一行主键冲突、类型不对，或同一批里主键重复时，前面的行也不写 WAL。没有主键的表只追加。`expire_at` 非空时每一行带同一个截止时间，表必须有主键。`EXPECT` 不走这里。默认每一条都会 `fsync`，除非组提交还没凑满。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成的表。带截止时间时不能是引擎登记的四张表。
    /// * `rows` - 每行与列顺序、列数、类型一一对应。至少一行。
    /// * `expire_at` - `None` 或 `Some(0)` 表示没有截止时间。非 0 是 UTC 纳秒，写在每一行后面。
    ///
    /// # Returns
    ///
    /// 每行自己的 `seq`，顺序与 `rows` 相同。
    ///
    /// # Errors
    ///
    /// 与 [`Engine::insert_row`] 相同。带截止时间但没有主键时返回 [`StoreError::Schema`]。
    pub fn insert_rows(
        &mut self,
        table: &str,
        rows: &[Vec<Cell>],
        expire_at: Option<i64>,
    ) -> Result<Vec<u64>, StoreError> {
        self.ensure_writable()?;
        if expire_at.is_some() && expire_at != Some(0) {
            if table::BUILTIN_TABLES.contains(&table) {
                return Err(StoreError::Schema("reserved table".into()));
            }
            let schema = self
                .tables
                .schema(table)
                .cloned()
                .ok_or(StoreError::NotFound)?;
            if !schema.columns.iter().any(|column| column.primary_key) {
                return Err(StoreError::Schema("primary key required".into()));
            }
        }
        // 编码只检查键和类型。通过之后再逐条提交，避免后一行失败时前一行已经落盘。
        let mut payloads = self.tables.encode_inserts(table, rows)?;
        if let Some(expire_at) = expire_at {
            if expire_at != 0 {
                for payload in &mut payloads {
                    payload.extend_from_slice(&expire_at.to_le_bytes());
                }
            }
        }
        let mut seqs = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            seqs.push(self.commit(MsgType::RowInsert, 0, 0, 0, 0, payload, 0)?);
        }
        Ok(seqs)
    }

    /// 把截止时间不晚于 `deadline_ns` 的可见行收成墓碑。
    ///
    /// 水位是调用方给出的 UTC 纳秒，不是本机时钟。没有截止时间的行保留。没有主键的表跳过。先收集再提交，没有命中时不写 WAL。每个命中的键一条墓碑。
    ///
    /// # Arguments
    ///
    /// * `deadline_ns` - 截止时间小于或等于这个值的行会被收掉。
    ///
    /// # Returns
    ///
    /// 每条墓碑的 `seq`。没有命中时为空。
    ///
    /// # Errors
    ///
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn expire_at(&mut self, deadline_ns: i64) -> Result<Vec<u64>, StoreError> {
        self.ensure_writable()?;
        let keys = self.tables.keys_expiring_by(deadline_ns);
        let mut seqs = Vec::new();
        let mut index = 0;
        while index < keys.len() {
            let table = keys[index].0.clone();
            let mut batch = Vec::new();
            while index < keys.len() && keys[index].0 == table {
                batch.push(keys[index].1.clone());
                index += 1;
            }
            seqs.extend(self.delete_rows(&table, &batch)?);
        }
        Ok(seqs)
    }

    /// 按主键写一条墓碑。
    ///
    /// 已经 `fsync` 的旧行留在 WAL 里，不改那些字节。这次提交分配新的 `seq` 之后，查询和二级索引都不再返回这个键。同一个键可以再插入。默认每次调用都 `fsync`。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成的表。引擎登记的四张表会失败。
    /// * `key` - 主键列的值，顺序与建表时主键列的顺序一致，个数必须等于主键列数。
    ///
    /// # Returns
    ///
    /// 墓碑记录的 `seq`。它不是被删掉的那一行原来的序号。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 表是引擎登记的四张表之一、没有主键，或值的个数不对。
    /// * [`StoreError::TypeMismatch`] - 值的类型和主键列不一致。
    /// * [`StoreError::KeyNotFound`] - 这个主键当前没有可见行。
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn delete_row(&mut self, table: &str, key: &[Cell]) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        if table::BUILTIN_TABLES.contains(&table) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        let payload = self.tables.encode_delete(table, key)?;
        self.commit(MsgType::RowDelete, 0, 0, 0, 0, &payload, 0)
    }

    /// 按多个主键各写一条墓碑。
    ///
    /// 先检查全部键都可见，并且列表里没有重复键，然后才按顺序提交。任何一个键不可见或重复时，不写 WAL，已经可见的行保持原样。每个键得到自己的 `seq`。默认每次调用里的每一条都会 `fsync`，除非组提交还没凑满。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成的表。
    /// * `keys` - 每个元素是一个主键，列顺序与建表时的主键列一致。至少有一个。
    ///
    /// # Returns
    ///
    /// 每条墓碑的 `seq`，顺序与 `keys` 相同。
    ///
    /// # Errors
    ///
    /// 与 [`Engine::delete_row`] 相同。列表为空返回 [`StoreError::Schema`]，键重复也返回 [`StoreError::Schema`]。
    pub fn delete_rows(&mut self, table: &str, keys: &[Vec<Cell>]) -> Result<Vec<u64>, StoreError> {
        self.ensure_writable()?;
        if table::BUILTIN_TABLES.contains(&table) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        if keys.is_empty() {
            return Err(StoreError::Schema("primary key required".into()));
        }
        for (pos, key) in keys.iter().enumerate() {
            if keys[..pos].iter().any(|earlier| earlier == key) {
                return Err(StoreError::Schema("duplicate key".into()));
            }
        }
        // 编码只检查可见性，不写 WAL。通过之后再逐条提交，避免列表后半段失败时前半段已经删掉。
        let mut payloads = Vec::with_capacity(keys.len());
        for key in keys {
            payloads.push(self.tables.encode_delete(table, key)?);
        }
        let mut seqs = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            seqs.push(self.commit(MsgType::RowDelete, 0, 0, 0, 0, payload, 0)?);
        }
        Ok(seqs)
    }

    /// 按主键覆盖多行已经可见的数据。
    ///
    /// 每一行必须带齐列，主键列仍指向当前那一行。先按各自的可见序号把覆盖编码完，再逐条提交。后一行类型不对、键不可见或主键重复时，前面的行也不写 WAL。每条覆盖分配新的 `seq`，行留在原来的槽位。载荷里不带截止时间，已经记下的期限保持不变。已经落盘的旧字节留在 WAL 里。默认每一条都会 `fsync`，除非组提交还没凑满。可见序号为 0 的旧快照行不能覆盖。
    ///
    /// # Arguments
    ///
    /// * `table` - 调用方建成、并且带主键的表。
    /// * `rows` - 覆盖后的整行，列顺序与建表时一致。至少一行。
    ///
    /// # Returns
    ///
    /// 每条覆盖记录的 `seq`，顺序与 `rows` 相同。这是该主键下一次的可见序号。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 表是引擎登记的四张表之一、没有主键、列数不对、字符串超过 1024 字节，或列表里主键重复。
    /// * [`StoreError::TypeMismatch`] - 某个值的类型和列定义不一致。
    /// * [`StoreError::KeyNotFound`] - 某个主键当前没有可见行。
    /// * [`StoreError::Conflict`] - 可见序号是 0，或编码之后序号已经对不上。
    /// * [`StoreError::ReadOnly`] - 引擎是只读打开的。
    /// * [`StoreError::Crc`] - 打开时 WAL 已损坏，拒绝再写。
    pub fn replace_rows(
        &mut self,
        table: &str,
        rows: &[Vec<Cell>],
    ) -> Result<Vec<u64>, StoreError> {
        self.ensure_writable()?;
        if table::BUILTIN_TABLES.contains(&table) {
            return Err(StoreError::Schema("reserved table".into()));
        }
        if rows.is_empty() {
            return Err(StoreError::Schema("primary key required".into()));
        }
        let schema = self
            .tables
            .schema(table)
            .cloned()
            .ok_or(StoreError::NotFound)?;
        let pk_at: Vec<usize> = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, column)| column.primary_key)
            .map(|(index, _)| index)
            .collect();
        if pk_at.is_empty() {
            return Err(StoreError::Schema("primary key required".into()));
        }
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            if row.len() != schema.columns.len() {
                return Err(StoreError::Schema("column count".into()));
            }
            let key: Vec<Cell> = pk_at.iter().map(|index| row[*index].clone()).collect();
            if keys.iter().any(|earlier| earlier == &key) {
                return Err(StoreError::Schema("duplicate key".into()));
            }
            if self.tables.visible_seq(table, &key)?.is_none() {
                return Err(StoreError::KeyNotFound);
            }
            keys.push(key);
        }
        // 编码只核对可见序号和类型。通过之后再逐条提交，避免后一行失败时前一行已经覆盖。
        let mut payloads = Vec::with_capacity(rows.len());
        for (row, key) in rows.iter().zip(&keys) {
            let expected = self
                .tables
                .visible_seq(table, key)?
                .ok_or(StoreError::KeyNotFound)?;
            payloads.push(self.tables.encode_expect(table, row, expected, None)?);
        }
        let mut seqs = Vec::with_capacity(payloads.len());
        for payload in &payloads {
            seqs.push(self.commit(MsgType::RowReplace, 0, 0, 0, 0, payload, 0)?);
        }
        Ok(seqs)
    }

    /// 扫描一张调用方建成的表。
    ///
    /// 给出列名和期望值时，主键或二级索引走等值查找；没有索引则扫全表。墓碑行不出现在结果里。调用方建成的表在列尾多一列文本 `seq`，值是该行当前可见序号；这不是建表时声明的列。引擎登记的四张表不加这一列。
    ///
    /// # Arguments
    ///
    /// * `table` - 表名。
    /// * `column` - 过滤列。`None` 表示不过滤。
    /// * `expected` - 与 `column` 一起使用的等值。类型必须和列一致。
    /// * `limit` - 最多返回的可见行数。墓碑不占这个名额。
    ///
    /// # Returns
    ///
    /// 列名，以及每行按列顺序排好的文本。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::Schema`] - 过滤列不存在。
    /// * [`StoreError::TypeMismatch`] - 过滤值的类型不对。
    pub fn scan_table(
        &self,
        table: &str,
        column: Option<&str>,
        expected: Option<&Cell>,
        limit: usize,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), StoreError> {
        self.tables.scan(table, column, expected, limit)
    }

    /// 读入打开之后新 `fsync` 的 WAL，并更新本进程里的表。
    ///
    /// 没有新记录时返回空向量。只读引擎用它跟随写入者。
    ///
    /// # Returns
    ///
    /// 新应用到的成交、报价和 K 线变更。同一笔成交会同时出现在 `ticks` 和 `snapshots`。
    ///
    /// # Errors
    ///
    /// WAL 出现坏 CRC 时返回 [`StoreError::Crc`]。短尾留到下一次再读。
    pub fn poll(&mut self) -> Result<Vec<Change>, StoreError> {
        if self.crc_failed {
            return Err(StoreError::Crc);
        }
        let end = if self.readonly {
            read_visible_file(&self.root).unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };
        let appended =
            read_span(&self.root.join("wal/wal-1.qwal"), self.wal_pos, end).map_err(io)?;
        self.wal_pos = appended.consumed;
        let mut changes = Vec::new();
        for record in &appended.records {
            let events = self.apply_record(record)?;
            changes.extend(self.changes_for(record, &events));
            self.next_seq = self.applied_seq() + 1;
        }
        if appended.crc_failed {
            self.crc_failed = true;
            self.metrics.crc_failed.store(true, Ordering::Release);
            if changes.is_empty() {
                return Err(StoreError::Crc);
            }
        }
        self.sync_metrics();
        Ok(changes)
    }

    /// 已经应用到内存表的最后一个序号。
    ///
    /// # Returns
    ///
    /// 还没有记录时为 0。序号从 1 起。
    pub fn applied_seq(&self) -> u64 {
        self.agg.last_applied_seq()
    }

    /// 本进程里按写入顺序保存的帧，含触发帧和派生 K 线帧。
    ///
    /// 共享内存里只有触发帧。组提交尚未 `flush` 的帧也会出现在这里。
    ///
    /// # Returns
    ///
    /// 帧切片。调用方不要假定它跨过下一次写入仍然有效。
    pub fn frames(&self) -> &[Frame] {
        &self.frames
    }

    /// 已登记的证券。
    ///
    /// # Returns
    ///
    /// 按首次出现顺序排列。`id` 从 1 起。
    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    /// 一只证券的成交，按写入顺序。
    ///
    /// # Arguments
    ///
    /// * `symbol_id` - [`Engine::resolve`] 或 [`Engine::upsert_symbol`] 返回的编号。
    ///
    /// # Returns
    ///
    /// 没有成交时返回空切片，不报错。
    pub fn ticks(&self, symbol_id: u32) -> &[TickRow] {
        self.ticks.get(&symbol_id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// 一只证券的最新报价，含五档价格和数量。
    ///
    /// # Arguments
    ///
    /// * `symbol_id` - 证券编号。
    ///
    /// # Returns
    ///
    /// 还没有报价也没有成交时返回 `None`。成交会更新 `last`，不清买卖档。
    pub fn quote(&self, symbol_id: u32) -> Option<&QuoteRow> {
        self.quotes.get(&symbol_id)
    }

    /// 一只证券某个周期的 K 线。
    ///
    /// 已闭合的桶在前，当前未闭合桶在最后。桶起点按 UTC 对齐。
    ///
    /// # Arguments
    ///
    /// * `symbol_id` - 证券编号。
    /// * `period` - `1s`、`1m`、`5m`、`15m`、`1h`、`4h` 或 `1d`。
    ///
    /// # Returns
    ///
    /// 没有数据时返回空向量。
    pub fn klines(&self, symbol_id: u32, period: Period) -> Vec<KlineBody> {
        self.klines_window(symbol_id, period, None, None)
    }

    /// 按时间窗口读取 K 线，只拷贝需要的尾部。
    ///
    /// 先丢掉 `bucket_start_ns` 晚于 `end_ns` 的桶，再取最后 `limit` 根。未闭合桶在窗口内时附在最后。不写 WAL。
    ///
    /// # Arguments
    ///
    /// * `symbol_id` - 证券编号。
    /// * `period` - `1s`、`1m`、`5m`、`15m`、`1h`、`4h` 或 `1d`。
    /// * `limit` - 最多返回多少根。`None` 表示不截断。`Some(0)` 返回空。
    /// * `end_ns` - 桶起点的上限，UTC 纳秒，含该时刻。`None` 表示不截断。
    ///
    /// # Returns
    ///
    /// 按桶起点从早到晚。没有数据时返回空向量。
    pub fn klines_window(
        &self,
        symbol_id: u32,
        period: Period,
        limit: Option<usize>,
        end_ns: Option<i64>,
    ) -> Vec<KlineBody> {
        let sealed = self.agg.sealed(symbol_id, period);
        let end_i = end_ns.unwrap_or(i64::MAX);
        let end_idx = sealed.partition_point(|row| row.bucket_start_ns <= end_i);
        let prefix = &sealed[..end_idx];
        let open = self
            .agg
            .open_bar(symbol_id, period)
            .map(|bar| bar.body)
            .filter(|body| body.bucket_start_ns <= end_i);

        match limit {
            None => {
                let mut rows = prefix.to_vec();
                if let Some(body) = open {
                    if rows
                        .last()
                        .map(|r| r.bucket_start_ns != body.bucket_start_ns)
                        .unwrap_or(true)
                    {
                        rows.push(body);
                    }
                }
                rows
            }
            Some(0) => Vec::new(),
            Some(n) => {
                // 需要含未闭合桶时，先从 sealed 取 n-1，再追加 open
                let take_sealed = if open.is_some() {
                    n.saturating_sub(1)
                } else {
                    n
                };
                let start = prefix.len().saturating_sub(take_sealed);
                let mut rows = prefix[start..].to_vec();
                if let Some(body) = open {
                    if rows
                        .last()
                        .map(|r| r.bucket_start_ns != body.bucket_start_ns)
                        .unwrap_or(true)
                    {
                        rows.push(body);
                    }
                    if rows.len() > n {
                        rows.remove(0);
                    }
                }
                rows
            }
        }
    }

    /// 把 `交易所:代码` 或唯一的裸代码解析成证券编号。
    ///
    /// # Arguments
    ///
    /// * `text` - `SSE:600000` 这种完整键，或全库唯一的代码。
    ///
    /// # Returns
    ///
    /// 从 1 起的 `symbol_id`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 没有这只证券。
    /// * [`StoreError::Ambiguous`] - 裸代码对应多只证券。
    pub fn resolve(&self, text: &str) -> Result<u32, StoreError> {
        if let Some((ex, code)) = text.split_once(':') {
            let exchange_id = self.exchange_id(ex).ok_or(StoreError::NotFound)?;
            return self
                .by_key
                .get(&(exchange_id, code.to_string()))
                .copied()
                .ok_or(StoreError::NotFound);
        }
        let hits: Vec<u32> = self
            .symbols
            .iter()
            .filter(|symbol| symbol.code == text)
            .map(|symbol| symbol.id)
            .collect();
        match hits.as_slice() {
            [id] => Ok(*id),
            [] => Err(StoreError::NotFound),
            _ => Err(StoreError::Ambiguous),
        }
    }

    /// 登记证券。已经存在时返回原来的编号，不再分配 `seq`。
    ///
    /// 新证券会写一条 WAL。交易所标签最长按 16 字节、代码按 32 字节保存。
    ///
    /// # Arguments
    ///
    /// * `exchange_code` - 非空的调用方标签，库不解释其含义。
    /// * `code` - 证券代码。
    ///
    /// # Returns
    ///
    /// `symbol_id`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::EmptyExchange`] - 标签为空。
    /// * [`StoreError::ReadOnly`] - 只读引擎。
    /// * [`StoreError::Crc`] - WAL 已损坏。
    pub fn upsert_symbol(&mut self, exchange_code: &str, code: &str) -> Result<u32, StoreError> {
        if exchange_code.is_empty() {
            return Err(StoreError::EmptyExchange);
        }
        let exchange_id = self.intern_exchange(exchange_code);
        if let Some(id) = self.by_key.get(&(exchange_id, code.to_string())) {
            return Ok(*id);
        }
        self.ensure_writable()?;
        let id = self.symbols.len() as u32 + 1;
        let mut payload = Vec::new();
        payload.extend_from_slice(&id.to_le_bytes());
        payload.extend_from_slice(&exchange_id.to_le_bytes());
        payload.extend_from_slice(&fixed_label(exchange_code));
        payload.extend_from_slice(&fixed_code(code));
        self.commit(MsgType::SymbolUpsert, 0, id, exchange_id, 0, &payload, 0)?;
        Ok(id)
    }

    /// 写入一笔成交，并更新全部周期的未闭合 K 线。
    ///
    /// 证券不存在时先登记。金额是 `price.saturating_mul(volume)`。成交编号固定为 0。
    /// 早于当前桶起点的成交仍进入成交表，但不改 K 线。
    ///
    /// # Arguments
    ///
    /// * `exchange_code` - 非空交易所标签。
    /// * `code` - 证券代码。
    /// * `event_ts_ns` - 事件时间，UTC 纳秒。
    /// * `price` - 定点价格。
    /// * `volume` - 成交量。
    ///
    /// # Returns
    ///
    /// 这笔成交的 `seq`。登记新证券时，登记本身另占一个更小的序号。
    ///
    /// # Errors
    ///
    /// * [`StoreError::EmptyExchange`] - 标签为空。
    /// * [`StoreError::ReadOnly`] - 只读引擎。
    /// * [`StoreError::Crc`] - WAL 已损坏。
    /// * [`StoreError::Io`] - 落盘或发布失败。
    pub fn write_trade(
        &mut self,
        exchange_code: &str,
        code: &str,
        event_ts_ns: i64,
        price: i64,
        volume: i64,
    ) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        let symbol_id = self.upsert_symbol(exchange_code, code)?;
        let exchange_id = self
            .symbols
            .iter()
            .find(|symbol| symbol.id == symbol_id)
            .unwrap()
            .exchange_id;
        let mut payload = Vec::new();
        payload.extend_from_slice(&symbol_id.to_le_bytes());
        payload.extend_from_slice(&event_ts_ns.to_le_bytes());
        payload.extend_from_slice(&encode_trade(&TickTrade {
            price,
            volume,
            amount: price.saturating_mul(volume),
            trade_id: 0,
        }));
        self.commit(
            MsgType::TickTrade,
            event_ts_ns,
            symbol_id,
            exchange_id,
            0,
            &payload,
            0,
        )
    }

    /// 写入五档报价。不改变 K 线的 OHLC 和成交量。
    ///
    /// WAL 保存 `last`、五档买价、五档买量、五档卖价、五档卖量。
    ///
    /// # Arguments
    ///
    /// * `exchange_code` - 非空交易所标签。
    /// * `code` - 证券代码。
    /// * `event_ts_ns` - 事件时间，UTC 纳秒，写在 WAL 记录头。报价载荷只保存价格和数量。
    /// * `quote` - 五档价格和数量。未使用的档填 0。
    ///
    /// # Returns
    ///
    /// 这条报价的 `seq`。
    ///
    /// # Errors
    ///
    /// 同 [`Engine::write_trade`]。
    pub fn write_quote(
        &mut self,
        exchange_code: &str,
        code: &str,
        event_ts_ns: i64,
        quote: &TickQuote,
    ) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        let symbol_id = self.upsert_symbol(exchange_code, code)?;
        let exchange_id = self
            .symbols
            .iter()
            .find(|symbol| symbol.id == symbol_id)
            .unwrap()
            .exchange_id;
        let mut payload = Vec::new();
        payload.extend_from_slice(&symbol_id.to_le_bytes());
        payload.extend_from_slice(&quote.last.to_le_bytes());
        for value in quote.bid_px {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in quote.bid_qty {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in quote.ask_px {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        for value in quote.ask_qty {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        self.commit(
            MsgType::TickQuote,
            event_ts_ns,
            symbol_id,
            exchange_id,
            0,
            &payload,
            0,
        )
    }

    /// 写入某个交易所标签的 UTC 水位，用来闭合已经走完的 K 线。
    ///
    /// 只闭合 `exchange_code` 下仍开着、且桶结束时间不晚于水位的 K 线。不产生成交。
    ///
    /// # Arguments
    ///
    /// * `exchange_code` - 非空标签。还没见过时会分配内部编号。
    /// * `watermark_ns` - UTC 纳秒水位。
    ///
    /// # Returns
    ///
    /// 这条时钟记录的 `seq`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::EmptyExchange`] - 标签为空。
    /// * [`StoreError::ReadOnly`] - 只读引擎。
    /// * [`StoreError::Crc`] - WAL 已损坏。
    pub fn write_clock(
        &mut self,
        exchange_code: &str,
        watermark_ns: i64,
    ) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        if exchange_code.is_empty() {
            return Err(StoreError::EmptyExchange);
        }
        let exchange_id = self.intern_exchange(exchange_code);
        let mut payload = Vec::new();
        payload.extend_from_slice(&exchange_id.to_le_bytes());
        payload.extend_from_slice(&watermark_ns.to_le_bytes());
        self.commit(
            MsgType::ClockTick,
            watermark_ns,
            0,
            exchange_id,
            0,
            &payload,
            0,
        )
    }

    fn commit(
        &mut self,
        msg_type: MsgType,
        event_ts_ns: i64,
        symbol_id: u32,
        exchange_id: u16,
        period_id: u16,
        payload: &[u8],
        flags: u16,
    ) -> Result<u64, StoreError> {
        let seq = self.next_seq;
        let record = WalRecord {
            seq,
            msg_type: msg_type as u16,
            flags,
            payload: payload.to_vec(),
        };
        let wal = self.wal.as_mut().ok_or(StoreError::ReadOnly)?;
        wal.append(&record).map_err(io)?;
        let events = self.apply_record(&record)?;
        let frame = seal_frame(
            FrameHeader {
                seq,
                event_ts_ns,
                recv_ts_ns: event_ts_ns,
                payload_len: 0,
                msg_type: msg_type as u16,
                flags,
                symbol_id,
                exchange_id,
                period_id,
                partition_id: 0,
                reserved: 0,
                crc32: 0,
            },
            payload,
        );
        self.frames.push(frame.clone());
        for (index, event) in events.iter().enumerate() {
            self.frames
                .push(bar_frame(seq, event_ts_ns, event, index as u16 + 1));
        }
        if self.pending.is_empty() {
            self.group_started_ns = now_ns();
        }
        self.pending.push(frame);
        self.next_seq = self.applied_seq() + 1;
        self.maybe_flush()?;
        Ok(seq)
    }

    /// 设置组提交。达到条数或间隔后，下一条写入触发一次 `fsync` 并发布这批帧。
    ///
    /// `qdb` 进程使用 256 条或 1 毫秒。库接口默认是 1 条，也就是每次写入都落盘。
    ///
    /// # Arguments
    ///
    /// * `max_records` - 一批最多多少条。小于 1 时按 1 处理。
    /// * `interval_ns` - 第一批未落盘记录已经等待的纳秒数。0 表示不按时间触发。
    pub fn set_group_commit(&mut self, max_records: u64, interval_ns: u64) {
        self.group_limit = max_records.max(1);
        self.group_interval_ns = interval_ns;
    }

    /// 已经完成的 `fsync` 次数。
    ///
    /// 组提交把多条写入并成一次时，这个数小于写入次数。
    ///
    /// # Returns
    ///
    /// 从这次打开算起的落盘次数。
    pub fn flush_count(&self) -> u64 {
        self.flush_count
    }

    /// 先 [`Engine::flush`]，再把内存状态写到 `snap/checkpoint.qsnap`。
    ///
    /// 下次打开会装入这份状态，并只重放检查点之后的 WAL。文件写坏时打开仍会全量重放。
    ///
    /// # Errors
    ///
    /// 落盘或写检查点文件失败时返回 [`StoreError::Io`]。
    pub fn checkpoint(&mut self) -> Result<(), StoreError> {
        self.flush()?;
        let ticks = self
            .ticks
            .iter()
            .flat_map(|(symbol_id, rows)| rows.iter().map(|row| (*symbol_id, row.clone())))
            .collect();
        let quotes = self
            .quotes
            .iter()
            .map(|(symbol_id, quote)| (*symbol_id, quote.clone()))
            .collect();
        checkpoint::write(
            &self.root,
            &checkpoint::Checkpoint {
                wal_pos: self.wal_pos,
                symbols: self.symbols.clone(),
                exchanges: self.exchanges.clone(),
                ticks,
                quotes,
                agg: self.agg.export_state(),
                tables: self.tables.to_bytes(),
            },
        )
    }

    /// 给一个下游拉取快照或增量。每个调用方自带 `from_seq`，不消耗别人的游标。
    ///
    /// `from_seq` 为 0 时返回当前报价，调用方下次应传入返回的序号。大于 0 时返回序号更大的成交和报价帧。
    ///
    /// # Arguments
    ///
    /// * `symbol` - `交易所:代码`。`None` 表示不按代码过滤。
    /// * `from_seq` - 已经确认过的序号。0 表示还没有接上。
    /// * `limit` - 最多返回多少条。
    ///
    /// # Returns
    ///
    /// 建议作为下次 `from_seq` 的序号，以及这次拿到的行。
    pub fn changes_since(
        &self,
        symbol: Option<&str>,
        from_seq: u64,
        limit: usize,
    ) -> (u64, Vec<LiveRow>) {
        let snapshot_seq = if self.wait_majority {
            self.published_seq
        } else {
            self.applied_seq()
        };
        if from_seq == 0 {
            let quotes = if self.wait_majority {
                &self.published_quotes
            } else {
                &self.quotes
            };
            let quotes = quotes
                .iter()
                .filter_map(|(symbol_id, quote)| {
                    let name = self.symbol_name(*symbol_id);
                    if symbol.is_some_and(|wanted| wanted != name) {
                        return None;
                    }
                    Some(LiveRow {
                        seq: quote.seq,
                        symbol: name,
                        kind: "snapshot",
                        ts: 0,
                        price: quote.last,
                        volume: 0,
                        last: quote.last,
                        bid_px: quote.bid_px,
                        ask_px: quote.ask_px,
                    })
                })
                .take(limit)
                .collect();
            return (snapshot_seq, quotes);
        }
        let mut rows = Vec::new();
        for frame in &self.frames {
            if rows.len() >= limit || frame.seq() <= from_seq || frame.seq() > snapshot_seq {
                continue;
            }
            let kind = match MsgType::from_u16(frame.header.msg_type) {
                Some(MsgType::TickTrade) => "trade",
                Some(MsgType::TickQuote) => "quote",
                _ => continue,
            };
            let name = self.symbol_name(frame.header.symbol_id);
            if symbol.is_some_and(|wanted| wanted != name) {
                continue;
            }
            if kind == "quote" {
                if let Some((_, last, bid_px, _, ask_px, _)) = decode_quote_payload(&frame.payload)
                {
                    rows.push(LiveRow {
                        seq: frame.seq(),
                        symbol: name,
                        kind,
                        ts: frame.header.event_ts_ns,
                        price: last,
                        volume: 0,
                        last,
                        bid_px,
                        ask_px,
                    });
                }
            } else if let Some(trade) = qdb_abi::decode_trade(&frame.payload[12..]) {
                rows.push(LiveRow {
                    seq: frame.seq(),
                    symbol: name,
                    kind,
                    ts: frame.header.event_ts_ns,
                    price: trade.price,
                    volume: trade.volume,
                    last: trade.price,
                    bid_px: [0; 5],
                    ask_px: [0; 5],
                });
            }
        }
        (snapshot_seq, rows)
    }

    fn install_checkpoint(&mut self, saved: checkpoint::Checkpoint) -> Result<(), StoreError> {
        self.symbols = saved.symbols;
        self.by_key = self
            .symbols
            .iter()
            .map(|symbol| ((symbol.exchange_id, symbol.code.clone()), symbol.id))
            .collect();
        self.exchanges = saved.exchanges;
        self.ticks.clear();
        for (symbol_id, tick) in saved.ticks {
            self.ticks.entry(symbol_id).or_default().push(tick);
        }
        self.quotes = saved.quotes.into_iter().collect();
        self.agg.import_state(saved.agg);
        self.tables.from_bytes(&saved.tables)?;
        self.wal_pos = saved.wal_pos;
        Ok(())
    }

    /// 把组提交缓冲区里的记录 `fsync`，并发布到共享内存。
    ///
    /// 打开可见性门闩时只落盘，不发布。副本应答之后由 [`Engine::publish_visible`] 发布。
    /// 缓冲区为空时什么也不做。
    ///
    /// # Errors
    ///
    /// 同步文件或发布帧失败时返回 [`StoreError::Io`]。
    pub fn flush(&mut self) -> Result<(), StoreError> {
        self.flush_pending()
    }

    /// 当前任期。单机从 1 开始。
    ///
    /// # Returns
    ///
    /// 本进程承认的 `leader_epoch`。
    pub fn leader_epoch(&self) -> u64 {
        self.leader_epoch
    }

    /// 已 `fsync` 的 WAL 终点，字节偏移。
    ///
    /// # Returns
    ///
    /// 复制游标。还没落盘时为 0。
    pub fn durable_offset(&self) -> u64 {
        self.wal_pos
    }

    /// 打开可见性门闩：本机落盘后，订阅者要等 [`Engine::publish_visible`]。
    ///
    /// 调用之前已经写进内存的记录仍然可见。之后的 `fsync` 写到 `wal/visible.pos` 后面，订阅者先看不见。
    /// 没有其他副本时不要调用，否则记录会一直停在门闩后面。
    ///
    /// # Errors
    ///
    /// 写 `wal/visible.pos` 失败时返回 [`StoreError::Io`]。
    pub fn arm_visibility_gate(&mut self) -> Result<(), StoreError> {
        if self.wait_majority {
            return Ok(());
        }
        self.wait_majority = true;
        self.published_seq = self.applied_seq();
        self.published_quotes = self.quotes.clone();
        if read_visible_file(&self.root).is_none() {
            self.visible_pos = self.wal_pos;
            self.write_visible()?;
        }
        Ok(())
    }

    /// 把已经落盘、且偏移不超过 `offset` 的记录发布给订阅者和共享内存。
    ///
    /// 没打开门闩、或没有攒下的批次时什么也不做。
    ///
    /// # Arguments
    ///
    /// * `offset` - 已经有足够副本应答的 WAL 字节偏移。大于本机落盘位置时按落盘位置截断。
    ///
    /// # Errors
    ///
    /// 发布共享内存或写 `wal/visible.pos` 失败时返回 [`StoreError::Io`]。
    pub fn publish_visible(&mut self, offset: u64) -> Result<(), StoreError> {
        let offset = offset.min(self.wal_pos);
        let mut changed = false;
        while self
            .held
            .first()
            .is_some_and(|batch| batch.offset <= offset)
        {
            let batch = self.held.remove(0);
            self.published_seq = batch.seq;
            self.published_quotes = batch.quotes;
            self.visible_pos = batch.offset;
            self.publish_frames(&batch.frames)?;
            self.write_visible()?;
            changed = true;
        }
        if changed {
            self.sync_metrics();
        }
        Ok(())
    }

    /// 当前复制 ID。提升为主之后会换成新的。
    ///
    /// # Returns
    ///
    /// 16 字节。副本用它判断还能不能从偏移接着复制。
    pub fn repl_id(&self) -> [u8; 16] {
        self.repl_id
    }

    /// 本进程是不是只收复制、不分配新序号。
    ///
    /// # Returns
    ///
    /// 副本为真。
    pub fn is_replica(&self) -> bool {
        self.fenced
    }

    /// 停止本机分配新序号。
    ///
    /// # Arguments
    ///
    /// * `epoch` - 新的任期。小于当前任期时拒绝。等于当前任期也会停止写入。
    ///
    /// # Errors
    ///
    /// [`StoreError::StaleEpoch`] - `epoch` 比本机已经看见的更小。
    pub fn fence(&mut self, epoch: u64) -> Result<(), StoreError> {
        if epoch < self.leader_epoch {
            return Err(StoreError::StaleEpoch);
        }
        self.leader_epoch = epoch;
        self.fenced = true;
        Ok(())
    }

    /// 以这个任期成为写入者，并从已经应用到的序号往后分配。
    ///
    /// # Arguments
    ///
    /// * `epoch` - 必须不小于本机任期。切换时用比旧写入者更大的值。
    ///
    /// # Errors
    ///
    /// [`StoreError::StaleEpoch`] - `epoch` 比本机已经看见的更小。
    pub fn lead(&mut self, epoch: u64) -> Result<(), StoreError> {
        if epoch < self.leader_epoch {
            return Err(StoreError::StaleEpoch);
        }
        self.leader_epoch = epoch;
        self.fenced = false;
        Ok(())
    }

    /// 按 Redis 的部分同步导出已落盘 WAL。
    ///
    /// 复制 ID 对得上，并且偏移还在文件里，就只发这段之后的字节。对不上就从 0 全量发送。未 `fsync` 的尾部不发送。
    ///
    /// # Arguments
    ///
    /// * `repl_id` - 副本记下的复制 ID。`None` 表示还没同步过，得到全量。
    /// * `from` - 副本已经有的字节偏移。
    ///
    /// # Returns
    ///
    /// `QDBREPL2`。第 9 字节 1 是增量，2 是全量。
    ///
    /// # Errors
    ///
    /// * [`StoreError::Io`] - `from` 超过已落盘位置，或文件读失败。
    /// * [`StoreError::Crc`] - 已落盘区间里有坏校验。
    pub fn replication_blob(
        &self,
        repl_id: Option<&[u8; 16]>,
        from: u64,
    ) -> Result<Vec<u8>, StoreError> {
        let end = self.wal_pos;
        if from > end {
            return Err(StoreError::Io("replication cursor past durable wal".into()));
        }
        let partial = self.can_partial(repl_id, from);
        let start = if partial { from } else { 0 };
        let bytes = self.wal_slice(start, end)?;
        let next = start + bytes.len() as u64;
        let mut out = Vec::with_capacity(41 + bytes.len());
        out.extend_from_slice(b"QDBREPL2");
        out.push(if partial { 1 } else { 2 });
        out.extend_from_slice(&self.leader_epoch.to_le_bytes());
        out.extend_from_slice(&self.repl_id);
        out.extend_from_slice(&next.to_le_bytes());
        out.extend_from_slice(&bytes);
        Ok(out)
    }

    /// 把另一个节点已经落盘的 WAL 按原序号写入本机。
    ///
    /// 比本机任期更小的包被拒绝。更大的任期会记下，并停止本机主动写入，直到 [`Engine::lead`]。
    /// 序号必须接在已应用序号之后。
    ///
    /// # Arguments
    ///
    /// * `blob` - [`Engine::replication_blob`] 的输出。
    ///
    /// # Returns
    ///
    /// 新应用到的记录条数。已经有的序号会跳过。
    ///
    /// # Errors
    ///
    /// * [`StoreError::StaleEpoch`] - 包的任期更旧。
    /// * [`StoreError::Gap`] - 序号接不上。已经连续的前缀仍保留。
    /// * [`StoreError::ReadOnly`] - 没有本地 WAL。
    /// * [`StoreError::Crc`] - 包校验失败。
    pub fn apply_replication(&mut self, blob: &[u8]) -> Result<u64, StoreError> {
        if self.readonly {
            return Err(StoreError::ReadOnly);
        }
        if blob.len() < 41 || &blob[..8] != b"QDBREPL2" {
            return Err(StoreError::Io("not a replication batch".into()));
        }
        let kind = blob[8];
        let epoch = u64::from_le_bytes(blob[9..17].try_into().unwrap());
        let mut repl_id = [0u8; 16];
        repl_id.copy_from_slice(&blob[17..33]);
        if epoch < self.leader_epoch && repl_id != self.repl_id && repl_id != self.repl_id2 {
            return Err(StoreError::StaleEpoch);
        }
        let parsed = parse_wal(&blob[41..]).map_err(io)?;
        if parsed.crc_failed {
            return Err(StoreError::Crc);
        }
        if kind == 2 {
            if !self.fenced && repl_id != self.repl_id {
                return Err(StoreError::StaleEpoch);
            }
            self.reset_from_records(&parsed.records)?;
            self.leader_epoch = epoch.max(self.leader_epoch);
            self.repl_id = repl_id;
            self.fenced = true;
            self.save_repl_meta()?;
            self.catch_up_visible()?;
            return Ok(parsed.records.len() as u64);
        }
        if epoch > self.leader_epoch {
            self.leader_epoch = epoch;
            self.fenced = true;
        }
        self.repl_id = repl_id;
        let applied = self.append_records(&parsed.records)?;
        self.save_repl_meta()?;
        if self.fenced {
            self.catch_up_visible()?;
        }
        Ok(applied)
    }

    /// 把本副本提升为主。复制 ID 换成新的，旧 ID 还能接到提升前的偏移。
    ///
    /// # Returns
    ///
    /// 新的复制 ID。
    ///
    /// # Errors
    ///
    /// 写 `wal/repl.meta` 失败时返回 [`StoreError::Io`]。
    pub fn promote(&mut self) -> Result<[u8; 16], StoreError> {
        self.repl_id2 = self.repl_id;
        self.offset2 = self.wal_pos;
        self.repl_id = new_repl_id();
        self.leader_epoch = self.leader_epoch.saturating_add(1);
        self.fenced = false;
        self.save_repl_meta()?;
        Ok(self.repl_id)
    }

    fn can_partial(&self, repl_id: Option<&[u8; 16]>, from: u64) -> bool {
        let Some(repl_id) = repl_id else {
            return false;
        };
        if *repl_id == self.repl_id {
            return true;
        }
        *repl_id == self.repl_id2 && from <= self.offset2
    }

    fn wal_slice(&self, start: u64, end: u64) -> Result<Vec<u8>, StoreError> {
        if start == end {
            return Ok(Vec::new());
        }
        let path = self.root.join("wal/wal-1.qwal");
        if !path.is_file() {
            return Ok(Vec::new());
        }
        let mut file = File::open(&path).map_err(io)?;
        file.seek(SeekFrom::Start(start)).map_err(io)?;
        let mut buf = vec![0u8; (end - start) as usize];
        file.read_exact(&mut buf).map_err(io)?;
        let parsed = parse_wal(&buf).map_err(io)?;
        if parsed.crc_failed {
            return Err(StoreError::Crc);
        }
        buf.truncate(parsed.consumed as usize);
        Ok(buf)
    }

    fn append_records(&mut self, records: &[WalRecord]) -> Result<u64, StoreError> {
        let mut applied = 0u64;
        for record in records {
            if record.seq <= self.applied_seq() {
                continue;
            }
            if record.seq != self.applied_seq() + 1 {
                self.flush_replicated()?;
                return Err(StoreError::Gap);
            }
            self.wal
                .as_mut()
                .ok_or(StoreError::ReadOnly)?
                .append(record)
                .map_err(io)?;
            self.apply_record(record)?;
            self.next_seq = self.applied_seq() + 1;
            applied += 1;
        }
        self.flush_replicated()?;
        Ok(applied)
    }

    fn reset_from_records(&mut self, records: &[WalRecord]) -> Result<(), StoreError> {
        let path = self.root.join("wal/wal-1.qwal");
        replace_wal(&path, records).map_err(io)?;
        self.symbols.clear();
        self.by_key.clear();
        self.exchanges.clear();
        self.ticks.clear();
        self.quotes.clear();
        self.frames.clear();
        self.pending.clear();
        self.held.clear();
        self.agg = KlineAggregator::default();
        self.tables = table::TableSet::default();
        self.next_seq = 1;
        self.defer_rows = true;
        for record in records {
            self.apply_record(record)?;
            self.next_seq = self.applied_seq() + 1;
        }
        self.defer_rows = false;
        self.rebuild_business_rows();
        self.wal_pos = fs::metadata(&path).map(|meta| meta.len()).unwrap_or(0);
        Ok(())
    }

    fn load_or_create_repl(&mut self) {
        let path = self.root.join("wal/repl.meta");
        if let Ok(bytes) = fs::read(&path) {
            if bytes.len() >= 48 {
                self.repl_id.copy_from_slice(&bytes[0..16]);
                self.repl_id2.copy_from_slice(&bytes[16..32]);
                self.offset2 = u64::from_le_bytes(bytes[32..40].try_into().unwrap());
                let epoch = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
                if epoch > 0 {
                    self.leader_epoch = epoch;
                }
                return;
            }
        }
        self.repl_id = new_repl_id();
        let _ = self.save_repl_meta();
    }

    fn save_repl_meta(&self) -> Result<(), StoreError> {
        let path = self.root.join("wal/repl.meta");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io)?;
        }
        let mut bytes = Vec::with_capacity(48);
        bytes.extend_from_slice(&self.repl_id);
        bytes.extend_from_slice(&self.repl_id2);
        bytes.extend_from_slice(&self.offset2.to_le_bytes());
        bytes.extend_from_slice(&self.leader_epoch.to_le_bytes());
        fs::write(path, bytes).map_err(io)
    }

    fn flush_replicated(&mut self) -> Result<(), StoreError> {
        if let Some(wal) = self.wal.as_mut() {
            wal.flush().map_err(io)?;
        }
        self.wal_pos = fs::metadata(self.root.join("wal/wal-1.qwal"))
            .map(|meta| meta.len())
            .unwrap_or(self.wal_pos);
        Ok(())
    }

    fn maybe_flush(&mut self) -> Result<(), StoreError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let count_due = self.pending.len() as u64 >= self.group_limit;
        let time_due = self.group_interval_ns > 0
            && now_ns().saturating_sub(self.group_started_ns) >= self.group_interval_ns;
        if count_due || time_due {
            self.flush_pending()?;
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> Result<(), StoreError> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if let Some(wal) = self.wal.as_mut() {
            wal.flush().map_err(io)?;
        }
        self.wal_pos = fs::metadata(self.root.join("wal/wal-1.qwal"))
            .map(|meta| meta.len())
            .unwrap_or(self.wal_pos);
        if self.wait_majority {
            let frames = std::mem::take(&mut self.pending);
            self.held.push(HeldPublish {
                offset: self.wal_pos,
                seq: self.applied_seq(),
                frames,
                quotes: self.quotes.clone(),
            });
            self.flush_count += 1;
            self.metrics.heartbeat_ns.store(now_ns(), Ordering::Release);
            return Ok(());
        }
        let last = self.pending.last().map(|frame| frame.seq()).unwrap_or(0);
        let applied = self.applied_seq();
        if let Some(shm) = self.shm.as_mut() {
            shm.set_durable(last);
            shm.set_applied(applied);
            for frame in &self.pending {
                shm.publish(frame).map_err(io)?;
            }
            shm.set_heartbeat(now_ns());
        }
        self.pending.clear();
        self.flush_count += 1;
        self.sync_metrics();
        Ok(())
    }

    fn publish_frames(&mut self, frames: &[Frame]) -> Result<(), StoreError> {
        if frames.is_empty() {
            return Ok(());
        }
        let last = frames.last().map(|frame| frame.seq()).unwrap_or(0);
        let applied = if self.wait_majority {
            self.published_seq
        } else {
            self.applied_seq()
        };
        if let Some(shm) = self.shm.as_mut() {
            shm.set_durable(last);
            shm.set_applied(applied);
            for frame in frames {
                shm.publish(frame).map_err(io)?;
            }
            shm.set_heartbeat(now_ns());
        }
        Ok(())
    }

    fn write_visible(&self) -> Result<(), StoreError> {
        let path = self.root.join("wal/visible.pos");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io)?;
        }
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&self.visible_pos.to_le_bytes());
        bytes[8..].copy_from_slice(&self.published_seq.to_le_bytes());
        let mut file = fs::File::create(&path).map_err(io)?;
        file.write_all(&bytes).map_err(io)?;
        file.sync_all().map_err(io)
    }

    fn catch_up_visible(&mut self) -> Result<(), StoreError> {
        if read_visible_file(&self.root).is_none() {
            return Ok(());
        }
        self.held.clear();
        self.visible_pos = self.wal_pos;
        self.published_seq = self.applied_seq();
        self.published_quotes = self.quotes.clone();
        self.write_visible()
    }

    fn apply_record(&mut self, record: &WalRecord) -> Result<Vec<BarEvent>, StoreError> {
        match MsgType::from_u16(record.msg_type) {
            Some(MsgType::SymbolUpsert) => {
                let id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let exchange_id = u16::from_le_bytes(record.payload[4..6].try_into().unwrap());
                let exchange_name = code_str(&record.payload[6..22]);
                let code = code_str(&record.payload[22..54]);
                self.remember_exchange(exchange_id, &exchange_name);
                if !self.by_key.contains_key(&(exchange_id, code.clone())) {
                    self.by_key.insert((exchange_id, code.clone()), id);
                    self.symbols.push(Symbol {
                        id,
                        exchange_id,
                        code,
                        price_scale: 10_000,
                        exchange: exchange_name,
                    });
                }
                self.agg.note_seq(record.seq)?;
                self.mirror_symbol(id);
                Ok(Vec::new())
            }
            Some(MsgType::TickTrade) => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let event_ts_ns = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let trade = decode_trade(&record.payload[12..]).ok_or(StoreError::Crc)?;
                let symbol = self
                    .symbols
                    .iter()
                    .find(|symbol| symbol.id == symbol_id)
                    .ok_or(StoreError::NotFound)?;
                let exchange_id = symbol.exchange_id;
                let (_status, events) = self.agg.apply_trade(
                    record.seq,
                    &TradeInput {
                        symbol_id,
                        exchange_id,
                        event_ts_ns,
                        price: trade.price,
                        volume: trade.volume,
                        amount: trade.amount,
                    },
                )?;
                self.ticks.entry(symbol_id).or_default().push(TickRow {
                    seq: record.seq,
                    ts: event_ts_ns,
                    price: trade.price,
                    volume: trade.volume,
                    amount: trade.amount,
                });
                let quote = self.quotes.entry(symbol_id).or_insert(QuoteRow {
                    seq: record.seq,
                    last: trade.price,
                    bid_px: [0; 5],
                    bid_qty: [0; 5],
                    ask_px: [0; 5],
                    ask_qty: [0; 5],
                });
                quote.seq = record.seq;
                quote.last = trade.price;
                self.mirror_symbol(symbol_id);
                Ok(events)
            }
            Some(MsgType::TickQuote) => {
                let (symbol_id, last, bid_px, bid_qty, ask_px, ask_qty) =
                    decode_quote_payload(&record.payload).ok_or(StoreError::Crc)?;
                self.quotes.insert(
                    symbol_id,
                    QuoteRow {
                        seq: record.seq,
                        last,
                        bid_px,
                        bid_qty,
                        ask_px,
                        ask_qty,
                    },
                );
                self.agg.note_seq(record.seq)?;
                self.mirror_symbol(symbol_id);
                Ok(Vec::new())
            }
            Some(MsgType::TableSchema) => {
                let schema = table::TableSet::decode_schema(&record.payload)?;
                self.tables.insert_schema(schema)?;
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::RowInsert) => {
                self.tables.insert_encoded(&record.payload, record.seq)?;
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::RowReplace) => {
                self.tables.replace_encoded(&record.payload, record.seq)?;
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::RowDelete) => {
                self.tables.delete_encoded(&record.payload)?;
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::TableDrop) => {
                self.tables.drop_encoded(&record.payload)?;
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::TableTruncate) => {
                self.tables.truncate_encoded(&record.payload)?;
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::TableRename) => {
                self.tables.rename_encoded(&record.payload)?;
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
            Some(MsgType::ClockTick) => {
                let exchange_id = u16::from_le_bytes(record.payload[0..2].try_into().unwrap());
                let watermark = i64::from_le_bytes(record.payload[2..10].try_into().unwrap());
                let events = self.agg.apply_clock(record.seq, exchange_id, watermark)?;
                self.mirror_exchange(exchange_id);
                Ok(events)
            }
            _ => {
                self.agg.note_seq(record.seq)?;
                Ok(Vec::new())
            }
        }
    }

    fn attach_shm(&mut self) -> Result<(), StoreError> {
        let path = self.root.join("region.bin");
        if path.exists() {
            let _ = fs::remove_file(&path);
        }
        let shm = ShmRegion::create(&path, &self.region).map_err(io)?;
        if self.applied_seq() > 0 {
            shm.set_applied(self.applied_seq());
            shm.set_durable(self.applied_seq());
        }
        self.shm = Some(shm);
        Ok(())
    }

    fn changes_for(&self, record: &WalRecord, events: &[BarEvent]) -> Vec<Change> {
        let mut out = Vec::new();
        match MsgType::from_u16(record.msg_type) {
            Some(MsgType::TickTrade) if record.payload.len() >= 12 => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let ts = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let trade = decode_trade(&record.payload[12..]);
                let (price, volume) = trade
                    .map(|item| (item.price, item.volume))
                    .unwrap_or((0, 0));
                let (bid, ask) = self.bid_ask(symbol_id);
                let symbol = self.symbol_name(symbol_id);
                out.push(Change {
                    seq: record.seq,
                    table: "ticks",
                    symbol: symbol.clone(),
                    ts,
                    price,
                    volume,
                    last: price,
                    bid,
                    ask,
                    period: "",
                    open: 0,
                    high: 0,
                    low: 0,
                    close: price,
                });
                out.push(Change {
                    seq: record.seq,
                    table: "snapshots",
                    symbol,
                    ts,
                    price,
                    volume,
                    last: price,
                    bid,
                    ask,
                    period: "",
                    open: 0,
                    high: 0,
                    low: 0,
                    close: price,
                });
            }
            Some(MsgType::TickQuote) if record.payload.len() >= 28 => {
                let symbol_id = u32::from_le_bytes(record.payload[0..4].try_into().unwrap());
                let last = i64::from_le_bytes(record.payload[4..12].try_into().unwrap());
                let bid = i64::from_le_bytes(record.payload[12..20].try_into().unwrap());
                let ask = i64::from_le_bytes(record.payload[20..28].try_into().unwrap());
                out.push(Change {
                    seq: record.seq,
                    table: "snapshots",
                    symbol: self.symbol_name(symbol_id),
                    ts: 0,
                    price: last,
                    volume: 0,
                    last,
                    bid,
                    ask,
                    period: "",
                    open: 0,
                    high: 0,
                    low: 0,
                    close: last,
                });
            }
            _ => {}
        }
        for event in events {
            let bar = match event {
                BarEvent::Update(bar) | BarEvent::Seal(bar) => bar,
            };
            out.push(Change {
                seq: record.seq,
                table: "klines",
                symbol: self.symbol_name(bar.symbol_id),
                ts: bar.body.bucket_start_ns,
                price: bar.body.close,
                volume: bar.body.volume,
                last: bar.body.close,
                bid: 0,
                ask: 0,
                period: bar.period.as_str(),
                open: bar.body.open,
                high: bar.body.high,
                low: bar.body.low,
                close: bar.body.close,
            });
        }
        out
    }

    fn bid_ask(&self, symbol_id: u32) -> (i64, i64) {
        self.quotes
            .get(&symbol_id)
            .map(|quote| (quote.bid_px[0], quote.ask_px[0]))
            .unwrap_or((0, 0))
    }

    fn symbol_name(&self, symbol_id: u32) -> String {
        self.symbols
            .iter()
            .find(|symbol| symbol.id == symbol_id)
            .map(|symbol| format!("{}:{}", symbol.exchange, symbol.code))
            .unwrap_or_default()
    }

    fn ensure_writable(&self) -> Result<(), StoreError> {
        if self.readonly {
            return Err(StoreError::ReadOnly);
        }
        if self.fenced {
            return Err(StoreError::StaleEpoch);
        }
        if self.crc_failed {
            Err(StoreError::Crc)
        } else {
            Ok(())
        }
    }

    fn sync_metrics(&self) {
        let applied = if self.wait_majority {
            self.published_seq
        } else {
            self.applied_seq()
        };
        self.metrics.applied_seq.store(applied, Ordering::Release);
        self.metrics.durable_seq.store(applied, Ordering::Release);
        self.metrics.write_seq.store(applied, Ordering::Release);
        self.metrics.heartbeat_ns.store(now_ns(), Ordering::Release);
        self.metrics
            .late_tick_total
            .store(self.agg.late_tick_total, Ordering::Release);
    }
}

fn bar_frame(seq: u64, ts: i64, event: &BarEvent, out_idx: u16) -> Frame {
    let (msg, bar) = match event {
        BarEvent::Update(bar) => (MsgType::KlineUpdate, bar),
        BarEvent::Seal(bar) => (MsgType::KlineClose, bar),
    };
    seal_frame(
        FrameHeader {
            seq,
            event_ts_ns: ts,
            recv_ts_ns: ts,
            payload_len: 0,
            msg_type: msg as u16,
            flags: 0,
            symbol_id: bar.symbol_id,
            exchange_id: bar.exchange_id,
            period_id: bar.period.id(),
            partition_id: 0,
            reserved: out_idx,
            crc32: 0,
        },
        &encode_kline(&bar.body),
    )
}

impl Engine {
    fn rebuild_business_rows(&mut self) {
        self.ensure_business_tables();
        for name in table::BUILTIN_TABLES {
            self.tables.clear_rows(name);
        }
        let ids: Vec<u32> = self.symbols.iter().map(|symbol| symbol.id).collect();
        let saved = self.defer_rows;
        self.defer_rows = false;
        for id in ids {
            self.mirror_symbol(id);
        }
        self.defer_rows = saved;
    }

    fn ensure_business_tables(&mut self) {
        for name in table::BUILTIN_TABLES {
            if let Some(schema) = table::quote_schema(name) {
                let _ = self.tables.ensure(schema.clone());
            }
        }
    }

    fn mirror_exchange(&mut self, exchange_id: u16) {
        let ids: Vec<u32> = self
            .symbols
            .iter()
            .filter(|symbol| symbol.exchange_id == exchange_id)
            .map(|symbol| symbol.id)
            .collect();
        for id in ids {
            self.mirror_symbol(id);
        }
    }

    fn mirror_symbol(&mut self, symbol_id: u32) {
        if self.defer_rows {
            return;
        }
        self.ensure_business_tables();
        let Some(symbol) = self
            .symbols
            .iter()
            .find(|symbol| symbol.id == symbol_id)
            .cloned()
        else {
            return;
        };
        let label = format!("{}:{}", symbol.exchange, symbol.code);
        let _ = self.tables.upsert(
            "symbols",
            vec![
                Cell::Int64(i64::from(symbol.id)),
                Cell::String(label.clone()),
                Cell::String(symbol.exchange.clone()),
                Cell::String(symbol.code.clone()),
                Cell::Int64(i64::from(symbol.exchange_id)),
            ],
        );
        if let Some(ticks) = self.ticks.get(&symbol_id).cloned() {
            for tick in ticks {
                let _ = self.tables.upsert(
                    "ticks",
                    vec![
                        Cell::Int64(tick.seq as i64),
                        Cell::Int64(i64::from(symbol_id)),
                        Cell::String(label.clone()),
                        Cell::Int64(tick.ts),
                        Cell::Int64(tick.price),
                        Cell::Int64(tick.volume),
                        Cell::Int64(tick.amount),
                    ],
                );
            }
        }
        if let Some(quote) = self.quotes.get(&symbol_id).cloned() {
            let mut values = vec![
                Cell::Int64(i64::from(symbol_id)),
                Cell::String(label.clone()),
                Cell::Int64(quote.last),
            ];
            for level in 0..5 {
                values.push(Cell::Int64(quote.bid_px[level]));
                values.push(Cell::Int64(quote.bid_qty[level]));
                values.push(Cell::Int64(quote.ask_px[level]));
                values.push(Cell::Int64(quote.ask_qty[level]));
            }
            values.push(Cell::Int64(quote.seq as i64));
            let _ = self.tables.upsert("snapshots", values);
        }
        for period in qdb_abi::PERIODS {
            for bar in self.klines(symbol_id, period) {
                let _ = self.tables.upsert(
                    "klines",
                    vec![
                        Cell::Int64(i64::from(symbol_id)),
                        Cell::Int64(i64::from(period.id())),
                        Cell::Int64(bar.bucket_start_ns),
                        Cell::String(label.clone()),
                        Cell::String(period.as_str().to_string()),
                        Cell::Int64(bar.open),
                        Cell::Int64(bar.high),
                        Cell::Int64(bar.low),
                        Cell::Int64(bar.close),
                        Cell::Int64(bar.volume),
                        Cell::Int64(bar.amount),
                        Cell::Int64(0),
                    ],
                );
            }
        }
    }

    fn intern_exchange(&mut self, name: &str) -> u16 {
        if let Some(index) = self.exchanges.iter().position(|existing| existing == name) {
            return (index as u16) + 1;
        }
        self.exchanges.push(name.to_string());
        self.exchanges.len() as u16
    }

    fn remember_exchange(&mut self, id: u16, name: &str) {
        let index = (id as usize).saturating_sub(1);
        if self.exchanges.len() <= index {
            self.exchanges.resize(index + 1, String::new());
        }
        if self.exchanges[index].is_empty() {
            self.exchanges[index] = name.to_string();
        }
    }

    fn exchange_id(&self, name: &str) -> Option<u16> {
        self.exchanges
            .iter()
            .position(|existing| existing == name)
            .map(|index| (index as u16) + 1)
    }
}

fn fixed_label(value: &str) -> [u8; 16] {
    let mut out = [0u8; 16];
    let bytes = value.as_bytes();
    let n = bytes.len().min(16);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

struct OpenPlan {
    loaded: WalRead,
    checkpoint: Option<checkpoint::Checkpoint>,
    tail: Vec<WalRecord>,
    tail_crc: bool,
    durable_end: u64,
    visible_pos: u64,
    gated: bool,
}

fn read_visible_file(root: &Path) -> Option<u64> {
    let bytes = fs::read(root.join("wal/visible.pos")).ok()?;
    if bytes.len() < 8 {
        return None;
    }
    Some(u64::from_le_bytes(bytes[0..8].try_into().ok()?))
}

fn load_for_open(root: &Path, wal_path: &Path, reader: bool) -> Result<OpenPlan, StoreError> {
    let file_len = if wal_path.is_file() {
        fs::metadata(wal_path).map_err(io)?.len()
    } else {
        0
    };
    let visible = read_visible_file(root);
    let gate_end = visible.unwrap_or(file_len).min(file_len);
    if !reader && visible.is_some() && gate_end < file_len {
        let (prefix, checkpoint) = read_up_to(root, wal_path, gate_end)?;
        let tail = if prefix.crc_failed {
            WalRead {
                records: Vec::new(),
                crc_failed: false,
                last_good_seq: 0,
                consumed: prefix.consumed,
            }
        } else {
            read_span(wal_path, prefix.consumed, file_len).map_err(io)?
        };
        return Ok(OpenPlan {
            visible_pos: prefix.consumed,
            durable_end: file_len,
            gated: true,
            tail_crc: tail.crc_failed,
            tail: tail.records,
            loaded: prefix,
            checkpoint,
        });
    }
    let end = if reader { gate_end } else { file_len };
    let (loaded, checkpoint) = read_up_to(root, wal_path, end)?;
    Ok(OpenPlan {
        durable_end: if reader { loaded.consumed } else { file_len },
        visible_pos: visible.unwrap_or(loaded.consumed),
        gated: visible.is_some() && !reader,
        tail: Vec::new(),
        tail_crc: false,
        loaded,
        checkpoint,
    })
}

fn read_up_to(
    root: &Path,
    wal_path: &Path,
    end: u64,
) -> Result<(WalRead, Option<checkpoint::Checkpoint>), StoreError> {
    let file_len = if wal_path.is_file() {
        fs::metadata(wal_path).map_err(io)?.len()
    } else {
        0
    };
    if let Some(saved) = checkpoint::read(root) {
        if saved.wal_pos <= end && saved.wal_pos <= file_len {
            let tail = read_span(wal_path, saved.wal_pos, end).map_err(io)?;
            if !tail.crc_failed {
                return Ok((tail, Some(saved)));
            }
        }
    }
    Ok((read_span(wal_path, 0, end).map_err(io)?, None))
}

pub(crate) fn decode_quote_payload(
    payload: &[u8],
) -> Option<(u32, i64, [i64; 5], [i64; 5], [i64; 5], [i64; 5])> {
    if payload.len() < 28 {
        return None;
    }
    let symbol_id = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let last = i64::from_le_bytes(payload[4..12].try_into().ok()?);
    let mut bid_px = [0; 5];
    let mut bid_qty = [0; 5];
    let mut ask_px = [0; 5];
    let mut ask_qty = [0; 5];
    if payload.len() >= 172 {
        let mut at = 12;
        for slot in &mut bid_px {
            *slot = i64::from_le_bytes(payload[at..at + 8].try_into().ok()?);
            at += 8;
        }
        for slot in &mut bid_qty {
            *slot = i64::from_le_bytes(payload[at..at + 8].try_into().ok()?);
            at += 8;
        }
        for slot in &mut ask_px {
            *slot = i64::from_le_bytes(payload[at..at + 8].try_into().ok()?);
            at += 8;
        }
        for slot in &mut ask_qty {
            *slot = i64::from_le_bytes(payload[at..at + 8].try_into().ok()?);
            at += 8;
        }
    } else {
        bid_px[0] = i64::from_le_bytes(payload[12..20].try_into().ok()?);
        ask_px[0] = i64::from_le_bytes(payload[20..28].try_into().ok()?);
    }
    Some((symbol_id, last, bid_px, bid_qty, ask_px, ask_qty))
}

fn io<E: std::fmt::Display>(err: E) -> StoreError {
    StoreError::Io(err.to_string())
}

impl Engine {
    /// 把未落盘的记录写完并做检查点，然后卸下共享内存、清掉内存里的行情表。
    ///
    /// WAL 文件保留。再次 [`Engine::open`] 可以恢复。调用后本对象上的查询为空。
    pub fn release(&mut self) {
        let _ = self.flush_pending();
        let _ = self.checkpoint();
        self.ticks.clear();
        self.ticks.shrink_to_fit();
        self.quotes.clear();
        self.quotes.shrink_to_fit();
        self.symbols.clear();
        self.symbols.shrink_to_fit();
        self.by_key.clear();
        self.by_key.shrink_to_fit();
        self.exchanges.clear();
        self.exchanges.shrink_to_fit();
        self.frames.clear();
        self.frames.shrink_to_fit();
        self.pending.clear();
        self.pending.shrink_to_fit();
        self.held.clear();
        self.held.shrink_to_fit();
        self.published_quotes.clear();
        self.published_quotes.shrink_to_fit();
        self.tables = table::TableSet::default();
        self.agg = KlineAggregator::new();
        self.shm = None;
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if self.wal.is_some() {
            let _ = self.flush_pending();
        }
        self.shm = None;
    }
}

fn new_repl_id() -> [u8; 16] {
    let nanos = now_ns();
    let mixed = nanos
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(std::process::id() as u64);
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&nanos.to_le_bytes());
    id[8..].copy_from_slice(&mixed.to_le_bytes());
    id
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "qdb-store-{name}-{}-{}",
            std::process::id(),
            name.len()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn ts(extra_ns: i64) -> i64 {
        // 2024-01-02 09:30:00 +08，再加上 extra_ns。
        let days = days_from_civil(2024, 1, 2);
        let local = days * 86_400 + 9 * 3600 + 30 * 60;
        (local - 8 * 3600) * 1_000_000_000 + extra_ns
    }

    fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
        y -= i32::from(m <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146_097 + doe as i64 - 719_468
    }

    #[test]
    fn region_file_is_not_the_only_copy() {
        let root = temp_root("recover");
        let mut engine = Engine::open(&root).unwrap();
        engine
            .write_trade("SZSE", "000001", ts(100_000_000), 10, 100)
            .unwrap();
        engine
            .write_trade("SZSE", "000001", ts(200_000_000), 12, 50)
            .unwrap();
        engine
            .write_trade("SZSE", "000001", ts(1_000_000_000), 11, 10)
            .unwrap();
        let applied = engine.applied_seq();
        let id = engine.resolve("SZSE:000001").unwrap();
        let before = engine.klines(id, Period::S1);
        drop(engine);
        fs::remove_file(root.join("region.bin")).unwrap();
        let restored = Engine::open(&root).unwrap();
        assert!(!restored.crc_failed());
        assert_eq!(restored.applied_seq(), applied);
        let id = restored.resolve("SZSE:000001").unwrap();
        assert_eq!(restored.klines(id, Period::S1), before);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupt_wal_stops_before_the_bad_record() {
        let root = temp_root("crc");
        let mut engine = Engine::open(&root).unwrap();
        engine
            .write_trade("SZSE", "000001", ts(100_000_000), 10, 100)
            .unwrap();
        let applied = engine.applied_seq();
        drop(engine);
        let wal = root.join("wal/wal-1.qwal");
        let mut raw = fs::read(&wal).unwrap();
        raw.extend_from_slice(&0u32.to_le_bytes());
        raw.extend_from_slice(&12u32.to_le_bytes());
        raw.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
        fs::write(&wal, raw).unwrap();
        let mut restored = Engine::open(&root).unwrap();
        assert!(restored.crc_failed());
        assert_eq!(restored.applied_seq(), applied);
        assert!(restored
            .write_trade("SZSE", "000001", ts(200_000_000), 1, 1)
            .is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn read_only_open_replays_wal_without_touching_the_region_file() {
        let root = temp_root("reader");
        let mut engine = Engine::open(&root).unwrap();
        engine
            .write_trade("SSE", "600000", ts(0), 1_050_000, 100)
            .unwrap();
        drop(engine);
        let region = root.join("region.bin");
        let before = fs::read(&region).unwrap();
        let wal_len = fs::metadata(root.join("wal/wal-1.qwal")).unwrap().len();
        let mut reader = Engine::open_read(&root).unwrap();
        assert!(reader
            .quote(reader.resolve("SSE:600000").unwrap())
            .is_some());
        assert!(matches!(
            reader.write_trade("SSE", "600000", ts(1), 1, 1),
            Err(StoreError::ReadOnly)
        ));
        assert_eq!(fs::read(&region).unwrap(), before);
        assert_eq!(
            fs::metadata(root.join("wal/wal-1.qwal")).unwrap().len(),
            wal_len
        );
        let _ = fs::remove_dir_all(&root);
        assert!(Engine::open_read(&root).is_err());
    }

    #[test]
    fn poll_applies_a_trade_appended_while_the_reader_is_open() {
        let root = temp_root("poll");
        let mut writer = Engine::open(&root).unwrap();
        writer.write_trade("SSE", "600000", ts(0), 10, 1).unwrap();
        let mut reader = Engine::open_read(&root).unwrap();
        let id = reader.resolve("SSE:600000").unwrap();
        assert_eq!(reader.quote(id).unwrap().last, 10);
        assert!(reader.poll().unwrap().is_empty());
        writer
            .write_quote(
                "SSE",
                "600000",
                ts(1),
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
        writer.write_trade("SSE", "600000", ts(2), 13, 2).unwrap();
        let changes = reader.poll().unwrap();
        assert!(changes
            .iter()
            .any(|change| change.table == "snapshots" && change.last == 11 && change.bid == 10));
        assert!(changes
            .iter()
            .any(|change| change.table == "ticks" && change.price == 13 && change.volume == 2));
        assert!(changes
            .iter()
            .any(|change| change.table == "klines" && change.period == "1m" && change.close == 13));
        assert_eq!(reader.quote(id).unwrap().last, 13);
        assert!(reader.poll().unwrap().is_empty());
        let _ = fs::remove_dir_all(root);
    }
}
