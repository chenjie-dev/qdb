//! 验收测试的夹具。
//!
//! 每个 [`Fixture`] 独占一个临时目录，结束时删掉。事件时间从固定的 UTC 纪元加上偏移，不读墙上时钟。
//! 集成测试在 `tests/`，只调用 `qdb-store`、`qdb-sql` 和 `qdb-sub` 的公开接口。
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use qdb_store::{Engine, StoreError};

/// 2024-01-02 01:50:00 UTC，纳秒。测试里的事件时间都从这里起算。
pub const EPOCH_NS: i64 = 1_704_160_200_000_000_000;

/// 一次验收用的空库目录。
pub struct Fixture {
    dir: PathBuf,
}

impl Fixture {
    /// 建一个空目录。名字带进程号和递增序号，避免并行时撞车。
    ///
    /// # Returns
    ///
    /// 还没有 WAL 的目录。调用 [`Fixture::open`] 后才成为库。
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("qdb-suite-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("create suite dir");
        Self { dir }
    }

    /// 库目录。
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 以写入者打开这个目录。
    ///
    /// # Errors
    ///
    /// 同 [`Engine::open`]。
    pub fn open(&self) -> Result<Engine, StoreError> {
        Engine::open(&self.dir)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

/// 写入一笔上交所成交。`offset_ns` 加在 [`EPOCH_NS`] 上。
///
/// # Errors
///
/// 同 [`Engine::write_trade`]。
pub fn trade(
    engine: &mut Engine,
    code: &str,
    offset_ns: i64,
    price: i64,
    volume: i64,
) -> Result<u64, StoreError> {
    engine.write_trade("SSE", code, EPOCH_NS + offset_ns, price, volume)
}
