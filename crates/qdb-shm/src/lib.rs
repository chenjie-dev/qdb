//! 共享内存区域、环形缓冲、seqlock 和本机实例锁。
//!
//! `unsafe` 只出现在映射、原子量别名和实例文件锁处。每个 `unsafe` 块前有 `// SAFETY:`。

use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::{Mmap, MmapMut};
use qdb_abi::{
    checksum_frame, frame_from_bytes, frame_to_bytes, Frame, SnapshotPod, ABI_VERSION,
    ENDIAN_CHECK, FRAME_SIZE, HEADER_PAGE, MAGIC, OFF_APPLIED_SEQ, OFF_DURABLE_SEQ, OFF_EPOCH,
    OFF_HEARTBEAT_NS, OFF_WRITE_SEQ, PERIOD_SLOTS,
};
use thiserror::Error;

const OFF_SYMBOL: usize = 48;
const OFF_SNAPSHOT: usize = 56;
const OFF_KLINE: usize = 64;
const OFF_RING: usize = 72;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShmError {
    #[error("io: {0}")]
    Io(String),
    #[error("bad magic or abi")]
    BadMagic,
    #[error("big endian host is unsupported")]
    BigEndian,
    #[error("capacity must be a power of two")]
    BadCapacity,
    #[error("reader was lapped: expected {expected}, tail {tail}, capacity {capacity}")]
    Lapped {
        expected: u64,
        tail: u64,
        capacity: u64,
    },
    #[error("sequence gap: expected {expected}, saw {saw}")]
    SeqGap { expected: u64, saw: u64 },
    #[error("torn or overwritten frame")]
    Torn,
    #[error("bad frame crc")]
    BadCrc,
    #[error("epoch changed")]
    EpochChanged,
    #[error("snapshot version was odd or changed")]
    Busy,
    #[error("another qdb instance is already running")]
    InstanceRunning,
}

/// 共享内存区域的容量。`ring_capacity` 必须是 2 的幂，否则 [`ShmRegion::create`] 失败。
#[derive(Clone, Debug)]
pub struct RegionConfig {
    pub max_symbols: u32,
    pub ring_capacity: u32,
    pub reader_slots: u32,
}

impl RegionConfig {
    /// 开发规模：1024 个符号槽、4096 帧、64 个读者槽。
    ///
    /// # Returns
    ///
    /// 库接口默认打开库时用这份配置。不适合全市场。
    pub fn dev() -> Self {
        Self {
            max_symbols: 1024,
            ring_capacity: 4096,
            reader_slots: 64,
        }
    }

    /// 行情中心规模：65536 个符号槽、65536 帧、64 个读者槽。
    ///
    /// # Returns
    ///
    /// `qdb` 进程打开库时使用的配置。映射文件大约数百兆。
    pub fn market() -> Self {
        Self {
            max_symbols: 65_536,
            ring_capacity: 65_536,
            reader_slots: 64,
        }
    }

    /// 测试用的小区域。
    ///
    /// # Arguments
    ///
    /// * `ring_capacity` - 帧槽数，必须是 2 的幂。
    pub fn for_test(ring_capacity: u32) -> Self {
        Self {
            max_symbols: 32,
            ring_capacity,
            reader_slots: 4,
        }
    }
}

pub struct ShmRegion {
    mmap: MmapMut,
    capacity: u32,
}

pub struct RingReader {
    mmap: Mmap,
    cursor: u64,
    epoch: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotView {
    pub seq: u64,
    pub last: i64,
    pub bid_px1: i64,
    pub ask_px1: i64,
    pub version: u64,
}

impl ShmRegion {
    /// 截断并重建区域文件，再以可写方式映射。
    ///
    /// 已有内容会被清掉。调用方必须是本机唯一写入者。
    ///
    /// # Arguments
    ///
    /// * `path` - `region.bin` 路径。父目录不存在时会创建。
    /// * `cfg` - 容量。`ring_capacity` 为 0 或不是 2 的幂时失败。
    ///
    /// # Errors
    ///
    /// * [`ShmError::BadCapacity`] - 帧槽数不合法。
    /// * [`ShmError::Io`] - 建文件或映射失败。
    /// * [`ShmError::BigEndian`] - 本机不是小端。
    pub fn create(path: &Path, cfg: &RegionConfig) -> Result<Self, ShmError> {
        ensure_le()?;
        if cfg.ring_capacity == 0 || !cfg.ring_capacity.is_power_of_two() {
            return Err(ShmError::BadCapacity);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| ShmError::Io(err.to_string()))?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(|err| ShmError::Io(err.to_string()))?;
        let len = region_len(cfg);
        file.set_len(len as u64)
            .map_err(|err| ShmError::Io(err.to_string()))?;
        // SAFETY: 文件长度已设为区域长度，映射在 `ShmRegion` 持有期间有效。
        let mut mmap =
            unsafe { MmapMut::map_mut(&file) }.map_err(|err| ShmError::Io(err.to_string()))?;
        mmap.fill(0);
        write_header(&mut mmap, cfg);
        Ok(Self {
            mmap,
            capacity: cfg.ring_capacity,
        })
    }

    /// 以可写方式打开已有区域，不截断。
    ///
    /// # Arguments
    ///
    /// * `path` - 已经由 [`ShmRegion::create`] 写好的文件。
    ///
    /// # Errors
    ///
    /// 文件不存在、魔数不对或字节序不对时返回 [`ShmError`]。
    pub fn open_write(path: &Path) -> Result<Self, ShmError> {
        ensure_le()?;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(|err| ShmError::Io(err.to_string()))?;
        // SAFETY: 打开的是已有区域文件，长度由创建时写定，映射在 `ShmRegion` 持有期间有效。
        let mmap =
            unsafe { MmapMut::map_mut(&file) }.map_err(|err| ShmError::Io(err.to_string()))?;
        check_header(&mmap)?;
        let capacity = read_u32(&mmap, 16);
        Ok(Self { mmap, capacity })
    }

    /// 把一帧按序号写入环形槽，并推进 `write_seq`。
    ///
    /// 槽先写成 0，载荷写完后再用 Release 存储序号。读者看到非 0 序号时载荷已经可见。
    /// 不检查这帧是否已经 `fsync`。调用方应先落盘再发布。
    ///
    /// # Arguments
    ///
    /// * `frame` - 已封好的 256 字节帧。序号决定槽位。
    ///
    /// # Errors
    ///
    /// 当前实现不返回错误。签名保留 `Result`，以便以后拒绝非法帧。
    pub fn publish(&mut self, frame: &Frame) -> Result<(), ShmError> {
        let seq = frame.seq();
        let index = (seq & u64::from(self.capacity - 1)) as usize;
        let ring = read_u64(&self.mmap, OFF_RING) as usize;
        let slot = ring + index * FRAME_SIZE;
        self.mmap_write_u64(slot, 0);
        let bytes = frame_to_bytes(frame);
        self.mmap[slot + 8..slot + FRAME_SIZE].copy_from_slice(&bytes[8..]);
        atomic_u64(&self.mmap, slot).store(seq, Ordering::Release);
        atomic_u64(&self.mmap, OFF_WRITE_SEQ).store(seq, Ordering::Release);
        Ok(())
    }

    /// 发布已经 `fsync` 的最大序号。读者用它判断能看到的帧是否已落盘。
    ///
    /// # Arguments
    ///
    /// * `seq` - 已落盘的最大序号。
    pub fn set_durable(&self, seq: u64) {
        atomic_u64(&self.mmap, OFF_DURABLE_SEQ).store(seq, Ordering::Release);
    }

    pub fn set_applied(&self, seq: u64) {
        atomic_u64(&self.mmap, OFF_APPLIED_SEQ).store(seq, Ordering::Release);
    }

    pub fn set_heartbeat(&self, ns: u64) {
        atomic_u64(&self.mmap, OFF_HEARTBEAT_NS).store(ns, Ordering::Release);
    }

    pub fn durable_seq(&self) -> u64 {
        atomic_u64(&self.mmap, OFF_DURABLE_SEQ).load(Ordering::Acquire)
    }

    pub fn applied_seq(&self) -> u64 {
        atomic_u64(&self.mmap, OFF_APPLIED_SEQ).load(Ordering::Acquire)
    }

    pub fn write_seq(&self) -> u64 {
        atomic_u64(&self.mmap, OFF_WRITE_SEQ).load(Ordering::Acquire)
    }

    pub fn heartbeat_ns(&self) -> u64 {
        atomic_u64(&self.mmap, OFF_HEARTBEAT_NS).load(Ordering::Acquire)
    }

    fn mmap_write_u64(&self, offset: usize, value: u64) {
        atomic_u64(&self.mmap, offset).store(value, Ordering::Relaxed);
    }
}

impl RingReader {
    /// 只读打开环形缓冲，游标从 0 开始。
    ///
    /// # Arguments
    ///
    /// * `path` - 写入者创建的 `region.bin`。
    ///
    /// # Errors
    ///
    /// 文件不存在或头部不合法时返回 [`ShmError`]。
    pub fn open(path: &Path) -> Result<Self, ShmError> {
        ensure_le()?;
        let file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(|err| ShmError::Io(err.to_string()))?;
        // SAFETY: 只读映射已有区域文件，读者不取得写入权。
        let mmap = unsafe { Mmap::map(&file) }.map_err(|err| ShmError::Io(err.to_string()))?;
        check_header(&mmap)?;
        let epoch = atomic_u64_ro(&mmap, OFF_EPOCH).load(Ordering::Acquire);
        Ok(Self {
            mmap,
            cursor: 0,
            epoch,
        })
    }

    /// 读取下一帧。
    ///
    /// 写入序号还没追上游标时返回 `Ok(None)`。写入者重建区域后返回 [`ShmError::EpochChanged`]。
    /// 读者落后一整圈时返回 [`ShmError::Lapped`]，之后的帧不能再当成连续数据。
    ///
    /// # Errors
    ///
    /// * [`ShmError::EpochChanged`] - 区域被重建。
    /// * [`ShmError::Lapped`] - 环形缓冲已经套圈。
    /// * [`ShmError::Torn`] - 槽里的序号对不上，读到了半帧。
    /// * [`ShmError::BadCrc`] - 帧校验和不对。
    /// * [`ShmError::SeqGap`] - 重试后槽里仍不是期望的序号。
    pub fn next(&mut self) -> Result<Option<Frame>, ShmError> {
        let epoch = atomic_u64_ro(&self.mmap, OFF_EPOCH).load(Ordering::Acquire);
        if epoch != self.epoch {
            return Err(ShmError::EpochChanged);
        }
        let tail = atomic_u64_ro(&self.mmap, OFF_WRITE_SEQ).load(Ordering::Acquire);
        if self.cursor >= tail {
            return Ok(None);
        }
        let capacity = u64::from(read_u32(&self.mmap, 16));
        if tail - self.cursor >= capacity {
            return Err(ShmError::Lapped {
                expected: self.cursor + 1,
                tail,
                capacity,
            });
        }
        let expected = self.cursor + 1;
        let mask = u64::from(read_u32(&self.mmap, 20));
        let index = (expected & mask) as usize;
        let ring = read_u64(&self.mmap, OFF_RING) as usize;
        let slot = ring + index * FRAME_SIZE;
        for _ in 0..3 {
            let seq = atomic_u64_ro(&self.mmap, slot).load(Ordering::Acquire);
            if seq != expected {
                continue;
            }
            let mut copy = [0u8; FRAME_SIZE];
            copy.copy_from_slice(&self.mmap[slot..slot + FRAME_SIZE]);
            let seq2 = atomic_u64_ro(&self.mmap, slot).load(Ordering::Acquire);
            if seq2 != expected {
                return Err(ShmError::Torn);
            }
            let frame = frame_from_bytes(&copy).ok_or(ShmError::Torn)?;
            let expect_crc = checksum_frame(&frame.header, &frame.payload);
            if frame.header.crc32 != expect_crc {
                return Err(ShmError::BadCrc);
            }
            self.cursor = expected;
            return Ok(Some(frame));
        }
        Err(ShmError::SeqGap {
            expected,
            saw: atomic_u64_ro(&self.mmap, slot).load(Ordering::Acquire),
        })
    }
}

pub fn read_snapshot(pod: &SnapshotPod) -> Result<SnapshotView, ShmError> {
    for _ in 0..8 {
        let v1 = pod.version.load(Ordering::Acquire);
        if v1 & 1 == 1 {
            std::thread::yield_now();
            continue;
        }
        let view = SnapshotView {
            seq: pod.seq,
            last: pod.last,
            bid_px1: pod.bid_px[0],
            ask_px1: pod.ask_px[0],
            version: v1,
        };
        let v2 = pod.version.load(Ordering::Acquire);
        if v1 == v2 {
            return Ok(view);
        }
    }
    Err(ShmError::Busy)
}

pub fn write_snapshot(pod: &SnapshotPod, seq: u64, last: i64, bid: i64, ask: i64) {
    pod.version.fetch_add(1, Ordering::Release);
    // SAFETY: 写入者独占这个槽。版本为奇数时读者不会采纳字段。
    unsafe {
        write_at(pod, 8, seq);
        write_at(pod, 40, last);
        write_at(pod, 64, bid);
        write_at(pod, 144, ask);
    }
    pod.version.fetch_add(1, Ordering::Release);
}

/// # Safety
///
/// `offset` 落在 `pod` 内部，并且按 `T` 对齐。调用时该槽的 `version` 为奇数。
unsafe fn write_at<T>(pod: &SnapshotPod, offset: usize, value: T) {
    let ptr = (pod as *const SnapshotPod as *const u8).add(offset) as *mut T;
    std::ptr::write(ptr, value);
}

fn region_len(cfg: &RegionConfig) -> usize {
    let symbols = cfg.max_symbols as usize * std::mem::size_of::<qdb_abi::SymbolRec>();
    let snapshots = cfg.max_symbols as usize * std::mem::size_of::<SnapshotPod>();
    let klines =
        cfg.max_symbols as usize * PERIOD_SLOTS as usize * std::mem::size_of::<qdb_abi::KlinePod>();
    let readers = cfg.reader_slots as usize * 64;
    HEADER_PAGE + readers + symbols + snapshots + klines + cfg.ring_capacity as usize * FRAME_SIZE
}

fn write_header(mmap: &mut MmapMut, cfg: &RegionConfig) {
    write_u32(mmap, 0, MAGIC);
    write_u32(mmap, 4, ABI_VERSION);
    write_u32(mmap, 8, ENDIAN_CHECK);
    write_u32(mmap, 12, FRAME_SIZE as u32);
    write_u32(mmap, 16, cfg.ring_capacity);
    write_u32(mmap, 20, cfg.ring_capacity - 1);
    write_u32(mmap, 24, cfg.max_symbols);
    write_u32(mmap, 28, PERIOD_SLOTS);
    write_u32(mmap, 32, std::mem::size_of::<SnapshotPod>() as u32);
    write_u32(mmap, 40, HEADER_PAGE as u32);
    write_u32(mmap, 44, cfg.reader_slots);
    let readers = cfg.reader_slots as u64 * 64;
    let symbol_off = HEADER_PAGE as u64 + readers;
    let symbol_bytes = cfg.max_symbols as u64 * std::mem::size_of::<qdb_abi::SymbolRec>() as u64;
    let snapshot_off = symbol_off + symbol_bytes;
    let snapshot_bytes = cfg.max_symbols as u64 * std::mem::size_of::<SnapshotPod>() as u64;
    let kline_off = snapshot_off + snapshot_bytes;
    let kline_bytes = cfg.max_symbols as u64
        * PERIOD_SLOTS as u64
        * std::mem::size_of::<qdb_abi::KlinePod>() as u64;
    let ring_off = kline_off + kline_bytes;
    write_u64(mmap, OFF_SYMBOL, symbol_off);
    write_u64(mmap, OFF_SNAPSHOT, snapshot_off);
    write_u64(mmap, OFF_KLINE, kline_off);
    write_u64(mmap, OFF_RING, ring_off);
    write_u64(mmap, 80, symbol_bytes);
    write_u64(mmap, 88, snapshot_bytes);
    write_u64(mmap, 96, kline_bytes);
    write_u64(mmap, 104, cfg.ring_capacity as u64 * FRAME_SIZE as u64);
    atomic_u64(mmap, OFF_EPOCH).store(1, Ordering::Release);
}

fn check_header(mmap: &[u8]) -> Result<(), ShmError> {
    if read_u32(mmap, 0) != MAGIC || read_u32(mmap, 4) != ABI_VERSION {
        return Err(ShmError::BadMagic);
    }
    if read_u32(mmap, 8) != ENDIAN_CHECK {
        return Err(ShmError::BigEndian);
    }
    Ok(())
}

fn ensure_le() -> Result<(), ShmError> {
    if 1u32.to_le_bytes()[0] != 1 {
        return Err(ShmError::BigEndian);
    }
    Ok(())
}

fn write_u32(mmap: &mut [u8], offset: usize, value: u32) {
    mmap[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn write_u64(mmap: &mut [u8], offset: usize, value: u64) {
    mmap[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_u32(mmap: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(mmap[offset..offset + 4].try_into().unwrap())
}

fn read_u64(mmap: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(mmap[offset..offset + 8].try_into().unwrap())
}

fn atomic_u64(mmap: &MmapMut, offset: usize) -> &AtomicU64 {
    // SAFETY: 区域创建时把该偏移对齐到 8 字节，映射在 ShmRegion 存活期间有效，写入者唯一。
    unsafe { &*mmap[offset..].as_ptr().cast::<AtomicU64>() }
}

fn atomic_u64_ro(mmap: &Mmap, offset: usize) -> &AtomicU64 {
    // SAFETY: 只读映射上的原子加载，偏移由头部布局保证对齐。
    unsafe { &*mmap[offset..].as_ptr().cast::<AtomicU64>() }
}

/// 一台机器上的 qdb 进程锁。持有者退出或 Drop 后释放。
pub struct InstanceLock {
    file: File,
}

impl InstanceLock {
    /// 非阻塞地独占这把文件锁。
    ///
    /// 锁在 [`Drop`] 时释放。进程异常退出后由操作系统释放。同一进程里第二次调用不会失败，因为 `flock` 按进程计。
    ///
    /// # Arguments
    ///
    /// * `path` - 锁文件。默认是 `/tmp/qdb.instance.lock`，测试用 `QDB_INSTANCE_LOCK` 换路径。
    ///
    /// # Errors
    ///
    /// * [`ShmError::InstanceRunning`] - 另一个进程已经持有这把锁。
    /// * [`ShmError::Io`] - 建目录或打开文件失败。
    pub fn try_acquire(path: &Path) -> Result<Self, ShmError> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|err| ShmError::Io(err.to_string()))?;
            }
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|err| ShmError::Io(err.to_string()))?;
        // SAFETY: `file` 是刚打开的描述符。锁由 `InstanceLock` 持有，Drop 时解锁。
        let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
        if rc != 0 {
            return Err(ShmError::InstanceRunning);
        }
        Ok(Self { file })
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        // SAFETY: 解锁的是本结构体持有的同一描述符。
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

static SHUTDOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

extern "C" fn on_shutdown(_: i32) {
    SHUTDOWN.store(true, Ordering::Release);
}

/// 把 SIGINT 和 SIGTERM 记到 [`shutdown_requested`]。
///
/// 处理函数只写一个原子量，不分配内存、不拿锁。重复调用会覆盖这两个信号原来的处理函数。
pub fn watch_shutdown() {
    let handler = on_shutdown as *const () as libc::sighandler_t;
    // SAFETY: 处理函数只写原子标记，不分配内存、不拿锁。
    unsafe {
        libc::signal(libc::SIGINT, handler);
        libc::signal(libc::SIGTERM, handler);
    }
}

/// 是否已经收到 SIGINT 或 SIGTERM。
///
/// # Returns
///
/// 还没调用 [`watch_shutdown`]，或信号还没到时，返回 `false`。
pub fn shutdown_requested() -> bool {
    SHUTDOWN.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdb_abi::{seal_frame, FrameHeader, MsgType};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("qdb-{name}-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("region.bin")
    }

    fn frame(seq: u64) -> Frame {
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

    #[test]
    fn slow_reader_observes_lap_instead_of_a_silent_skip() {
        let path = temp_path("lap");
        let mut region = ShmRegion::create(&path, &RegionConfig::for_test(4)).unwrap();
        for seq in 1..=5 {
            region.publish(&frame(seq)).unwrap();
        }
        let mut reader = RingReader::open(&path).unwrap();
        let err = reader.next().unwrap_err();
        assert!(matches!(err, ShmError::Lapped { .. }));
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn reader_keeps_sequence_when_it_keeps_up() {
        let path = temp_path("seq");
        let mut region = ShmRegion::create(&path, &RegionConfig::for_test(8)).unwrap();
        region.publish(&frame(1)).unwrap();
        region.publish(&frame(2)).unwrap();
        let mut reader = RingReader::open(&path).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().seq(), 1);
        assert_eq!(reader.next().unwrap().unwrap().seq(), 2);
        assert!(reader.next().unwrap().is_none());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn odd_snapshot_version_is_not_returned() {
        let pod = SnapshotPod {
            version: AtomicU64::new(0),
            seq: 0,
            event_ts_ns: 0,
            recv_ts_ns: 0,
            symbol_id: 1,
            exchange_id: 2,
            depth: 1,
            last: 0,
            volume: 0,
            amount: 0,
            bid_px: [0; 5],
            bid_qty: [0; 5],
            ask_px: [0; 5],
            ask_qty: [0; 5],
            _pad: [0; 32],
        };
        write_snapshot(&pod, 1, 10, 9, 11);
        let view = read_snapshot(&pod).unwrap();
        assert_eq!((view.last, view.bid_px1, view.ask_px1), (10, 9, 11));
        pod.version.store(3, Ordering::Release);
        assert_eq!(read_snapshot(&pod), Err(ShmError::Busy));
    }

    #[test]
    fn two_processes_share_one_frame() {
        let path = temp_path("proc");
        if std::env::var("QDB_CHILD_PATH").is_ok() {
            let child_path = std::env::var("QDB_CHILD_PATH").unwrap();
            let mut reader = RingReader::open(std::path::Path::new(&child_path)).unwrap();
            for _ in 0..50 {
                if let Some(frame) = reader.next().unwrap() {
                    assert_eq!(frame.seq(), 1);
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            panic!("child saw no frame");
        }
        let mut region = ShmRegion::create(&path, &RegionConfig::for_test(8)).unwrap();
        region.publish(&frame(1)).unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .env("QDB_CHILD_PATH", &path)
            .args([
                "--exact",
                "two_processes_share_one_frame",
                "--test-threads=1",
            ])
            .status()
            .unwrap();
        assert!(status.success());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
