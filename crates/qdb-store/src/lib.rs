//! 内存表、WAL 应用和启动恢复。
//!
//! [`Engine`] 是一个库的写入或只读视图。先把记录写入 WAL，再更新内存，最后发布共享内存。
//! 使用者说明见 `docs/manual/library.md`。
#![forbid(unsafe_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use qdb_abi::{
    code_str, decode_trade, encode_kline, encode_trade, fixed_code, seal_frame, Frame, FrameHeader,
    KlineBody, MsgType, Period, TickQuote, TickTrade,
};
use qdb_agg::{BarEvent, KlineAggregator, TradeInput};
use qdb_shm::{RegionConfig, ShmRegion};
use qdb_wal::{read_from, read_path, WalAppender, WalRead, WalRecord};
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
        let (loaded, checkpoint) = load_for_open(root, &wal_path)?;
        let mut engine = Self::from_replay(root, Some(wal), &loaded, region, checkpoint)?;
        engine.attach_shm()?;
        engine.sync_metrics();
        Ok(engine)
    }

    /// 只读打开库。
    ///
    /// 重放 WAL 或检查点加尾部记录。不创建目录，不追加日志，不删除 `region.bin`。
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
        let (loaded, checkpoint) = load_for_open(root, &wal_path)?;
        let engine = Self::from_replay(root, None, &loaded, &RegionConfig::dev(), checkpoint)?;
        Ok(engine)
    }

    fn from_replay(
        root: &Path,
        wal: Option<WalAppender>,
        loaded: &WalRead,
        region: &RegionConfig,
        checkpoint: Option<checkpoint::Checkpoint>,
    ) -> Result<Self, StoreError> {
        let readonly = wal.is_none();
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
        };
        if let Some(checkpoint) = checkpoint {
            engine.install_checkpoint(checkpoint)?;
        }
        for record in &loaded.records {
            engine.apply_record(record)?;
        }
        engine.defer_rows = false;
        engine.rebuild_business_rows();
        if loaded.crc_failed {
            engine.crc_failed = true;
            engine.metrics.crc_failed.store(true, Ordering::Release);
        }
        engine.wal_pos = loaded.consumed;
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

    /// 按建表时的列顺序插入一行。
    ///
    /// 主键重复时不写入。二级索引不要求唯一。
    ///
    /// # Arguments
    ///
    /// * `table` - 已存在的用户表名。
    /// * `values` - 与列数、类型一一对应。
    ///
    /// # Returns
    ///
    /// 这条插入记录的 `seq`。
    ///
    /// # Errors
    ///
    /// * [`StoreError::NotFound`] - 表不存在。
    /// * [`StoreError::TypeMismatch`] - 某个值的类型和列定义不一致。
    /// * [`StoreError::DuplicateKey`] - 主键已经有相同的值。
    /// * [`StoreError::Schema`] - 列数不对，或字符串超过 1024 字节。
    /// * [`StoreError::ReadOnly`] - 只读引擎。
    pub fn insert_row(&mut self, table: &str, values: &[Cell]) -> Result<u64, StoreError> {
        self.ensure_writable()?;
        let payload = self.tables.encode_row(table, values)?;
        self.commit(MsgType::RowInsert, 0, 0, 0, 0, &payload, 0)
    }

    /// 扫描一张用户表。
    ///
    /// 给出列名和期望值时，主键或二级索引走等值查找；没有索引则扫全表。
    ///
    /// # Arguments
    ///
    /// * `table` - 用户表名。
    /// * `column` - 过滤列。`None` 表示不过滤。
    /// * `expected` - 与 `column` 一起使用的等值。类型必须和列一致。
    /// * `limit` - 最多返回的行数。
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
        let appended = read_from(&self.root.join("wal/wal-1.qwal"), self.wal_pos).map_err(io)?;
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
        let mut rows = self.agg.sealed(symbol_id, period).to_vec();
        if let Some(open) = self.agg.open_bar(symbol_id, period) {
            rows.push(open.body);
        }
        rows
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
        let snapshot_seq = self.applied_seq();
        if from_seq == 0 {
            let quotes = self
                .quotes
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
            if rows.len() >= limit || frame.seq() <= from_seq {
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
    /// 缓冲区为空时什么也不做。
    ///
    /// # Errors
    ///
    /// 同步文件或发布帧失败时返回 [`StoreError::Io`]。
    pub fn flush(&mut self) -> Result<(), StoreError> {
        self.flush_pending()
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
                self.tables.insert_encoded(&record.payload)?;
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
        if self.crc_failed {
            Err(StoreError::Crc)
        } else {
            Ok(())
        }
    }

    fn sync_metrics(&self) {
        let applied = self.applied_seq();
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

fn load_for_open(
    root: &Path,
    wal_path: &Path,
) -> Result<(WalRead, Option<checkpoint::Checkpoint>), StoreError> {
    let file_len = if wal_path.is_file() {
        fs::metadata(wal_path).map_err(io)?.len()
    } else {
        0
    };
    if let Some(saved) = checkpoint::read(root) {
        if saved.wal_pos <= file_len {
            let tail = read_from(wal_path, saved.wal_pos).map_err(io)?;
            if !tail.crc_failed {
                return Ok((tail, Some(saved)));
            }
        }
    }
    Ok((read_path(wal_path).map_err(io)?, None))
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
