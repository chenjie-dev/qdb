//! 跨进程定长布局。小端、无指针、无 `unsafe`、无 serde。
//!
//! 帧、成交、报价和 K 线槽的字节含义以 `docs/design.md` 为准。价格用 `i64`。
#![forbid(unsafe_code)]

use std::sync::atomic::AtomicU64;

pub const MAGIC: u32 = 0x5144_4231;
pub const ABI_VERSION: u32 = 1;
pub const ENDIAN_CHECK: u32 = 0x0102_0304;
pub const FRAME_SIZE: usize = 256;
pub const FRAME_HEADER_SIZE: usize = 48;
pub const FRAME_PAYLOAD_SIZE: usize = FRAME_SIZE - FRAME_HEADER_SIZE;
pub const HEADER_PAGE: usize = 4096;
pub const CACHE_LINE: usize = 64;
pub const DEFAULT_MAX_SYMBOLS: u32 = 65_536;
pub const DEFAULT_RING_CAPACITY: u32 = 65_536;
pub const DEFAULT_READER_SLOTS: u32 = 64;
pub const PERIOD_SLOTS: u32 = 8;
pub const ACTIVE_PERIODS: usize = 7;

pub const OFF_WRITE_SEQ: usize = 256;
pub const OFF_DURABLE_SEQ: usize = 320;
pub const OFF_APPLIED_SEQ: usize = 384;
pub const OFF_HEARTBEAT_NS: usize = 448;
pub const OFF_EPOCH: usize = 512;
pub const OFF_SYMBOL_COUNT: usize = 520;

pub const FLAG_FROM_WAL: u16 = 1 << 0;
pub const FLAG_TS_REGRESSION: u16 = 1 << 1;
pub const FLAG_CORRECTION: u16 = 1 << 2;
pub const KLINE_INCOMPLETE: u32 = 1;

pub const STATE_OPEN: u8 = 0;
pub const STATE_CLOSED: u8 = 1;
pub const ORIGIN_LIVE: u8 = 0;
pub const ORIGIN_IMPORT: u8 = 1;
pub const ORIGIN_CORRECTION: u8 = 2;

/// 帧和 WAL 记录的消息类型。未知编号用 [`MsgType::from_u16`] 得到 `None`，不当成连续数据。
#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MsgType {
    TickTrade = 1,
    TickQuote = 2,
    KlineUpdate = 3,
    KlineClose = 4,
    SymbolUpsert = 5,
    ClockTick = 6,
    GapAck = 7,
    TableSchema = 8,
    RowInsert = 9,
    /// 用户表按主键写一条墓碑。已落盘的旧行还在 WAL 里，查询不再返回这个键。
    RowDelete = 10,
    /// 调用方表按期望 `seq` 覆盖当前主键。期望不符时不写入。
    RowReplace = 11,
    /// 调用方表从内存里去掉。WAL 里的建表记录还在，同名可以再建。
    TableDrop = 12,
    /// 调用方表的可见行从内存里去掉。表定义还在，已经落盘的行留在 WAL 里。
    TableTruncate = 13,
    /// 调用方表改名。行、序号和索引留在内存里，已经落盘的记录仍用改名前的名字。
    TableRename = 14,
}

impl MsgType {
    /// 把帧头里的 `u16` 还原成消息类型。
    ///
    /// # Arguments
    ///
    /// * `value` - 1 到 14。其余值没有对应类型。
    ///
    /// # Returns
    ///
    /// 未知编号返回 `None`。调用方应停止把后续帧当成连续数据。
    pub fn from_u16(value: u16) -> Option<Self> {
        Some(match value {
            1 => Self::TickTrade,
            2 => Self::TickQuote,
            3 => Self::KlineUpdate,
            4 => Self::KlineClose,
            5 => Self::SymbolUpsert,
            6 => Self::ClockTick,
            7 => Self::GapAck,
            8 => Self::TableSchema,
            9 => Self::RowInsert,
            10 => Self::RowDelete,
            11 => Self::RowReplace,
            12 => Self::TableDrop,
            13 => Self::TableTruncate,
            14 => Self::TableRename,
            _ => return None,
        })
    }
}

/// K 线周期。桶起点按 UTC 对齐，库不做时区换算。
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Period {
    S1,
    M1,
    M5,
    M15,
    H1,
    H4,
    D1,
}

/// 86400 放不进帧头里的 `u16`，日线用 `u16::MAX`。桶按 UTC 对齐。
pub const PERIOD_D1_ID: u16 = u16::MAX;

pub const PERIODS: [Period; ACTIVE_PERIODS] = [
    Period::S1,
    Period::M1,
    Period::M5,
    Period::M15,
    Period::H1,
    Period::H4,
    Period::D1,
];

impl Period {
    /// 写入帧头 `period_id` 的编号。
    ///
    /// 秒和分钟周期的编号就是秒数。日线是 [`PERIOD_D1_ID`]，因为 86400 放不进 `u16`。
    ///
    /// # Returns
    ///
    /// `1s` 为 1，`1m` 为 60，`1d` 为 `u16::MAX`。
    pub fn id(self) -> u16 {
        match self {
            Self::S1 => 1,
            Self::M1 => 60,
            Self::M5 => 300,
            Self::M15 => 900,
            Self::H1 => 3600,
            Self::H4 => 14_400,
            Self::D1 => PERIOD_D1_ID,
        }
    }

    /// 按 [`Period::id`] 还原周期。
    ///
    /// # Arguments
    ///
    /// * `value` - 帧头里的 `period_id`。
    ///
    /// # Returns
    ///
    /// 不是七个周期之一时返回 `None`。
    pub fn from_u16(value: u16) -> Option<Self> {
        PERIODS.into_iter().find(|period| period.id() == value)
    }

    /// 解析 SQL 和订阅里使用的周期名。
    ///
    /// # Arguments
    ///
    /// * `name` - `1s`、`1m`、`5m`、`15m`、`1h`、`4h` 或 `1d`。大小写敏感。
    ///
    /// # Returns
    ///
    /// 名字不在这七个里时返回 `None`。
    pub fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "1s" => Self::S1,
            "1m" => Self::M1,
            "5m" => Self::M5,
            "15m" => Self::M15,
            "1h" => Self::H1,
            "4h" => Self::H4,
            "1d" => Self::D1,
            _ => return None,
        })
    }

    /// 周期名，与 [`Period::parse`] 互逆。
    ///
    /// # Returns
    ///
    /// `1s`、`1m`、`5m`、`15m`、`1h`、`4h` 或 `1d`。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::S1 => "1s",
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::D1 => "1d",
        }
    }

    pub fn index(self) -> usize {
        PERIODS
            .iter()
            .position(|period| *period == self)
            .expect("period")
    }

    /// 一个桶的长度，单位纳秒。日线是 86400 秒，不用 `u16` 编号去乘。
    ///
    /// # Returns
    ///
    /// 正的纳秒数。`1d` 为 `86_400_000_000_000`。
    pub fn nanos(self) -> i64 {
        match self {
            Self::D1 => 86_400 * 1_000_000_000,
            other => i64::from(other.id()) * 1_000_000_000,
        }
    }
}

/// 256 字节帧的前 48 字节。字段小端、无指针，布局变更必须同时改 `docs/design.md`。
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    pub seq: u64,
    pub event_ts_ns: i64,
    pub recv_ts_ns: i64,
    pub payload_len: u32,
    pub msg_type: u16,
    pub flags: u16,
    pub symbol_id: u32,
    pub exchange_id: u16,
    pub period_id: u16,
    pub partition_id: u16,
    pub reserved: u16,
    pub crc32: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: [u8; FRAME_PAYLOAD_SIZE],
}

impl Frame {
    pub fn new(header: FrameHeader, payload: &[u8]) -> Self {
        seal_frame(header, payload)
    }

    pub fn seq(&self) -> u64 {
        self.header.seq
    }
}

/// 成交载荷。价格和金额是定点 `i64`，不使用浮点。
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickTrade {
    pub price: i64,
    pub volume: i64,
    pub amount: i64,
    pub trade_id: u64,
}

/// 五档报价。数组下标 0 是第一档，未使用的档填 0。
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TickQuote {
    pub last: i64,
    pub volume: i64,
    pub amount: i64,
    pub bid_px: [i64; 5],
    pub bid_qty: [i64; 5],
    pub ask_px: [i64; 5],
    pub ask_qty: [i64; 5],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KlineBody {
    pub bucket_start_ns: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
    pub amount: i64,
    pub tick_count: u32,
    pub state: u8,
    pub origin: u8,
    pub revision: u16,
    pub flags: u32,
}

#[repr(C, align(64))]
pub struct KlinePod {
    pub version: AtomicU64,
    pub seq: u64,
    pub bucket_start_ns: i64,
    pub symbol_id: u32,
    pub exchange_id: u16,
    pub period_id: u16,
    pub state: u8,
    pub origin: u8,
    pub revision: u16,
    pub _pad: u32,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
    pub amount: i64,
    pub tick_count: u32,
    pub flags: u32,
}

#[repr(C, align(64))]
pub struct SnapshotPod {
    pub version: AtomicU64,
    pub seq: u64,
    pub event_ts_ns: i64,
    pub recv_ts_ns: i64,
    pub symbol_id: u32,
    pub exchange_id: u16,
    pub depth: u16,
    pub last: i64,
    pub volume: i64,
    pub amount: i64,
    pub bid_px: [i64; 5],
    pub bid_qty: [i64; 5],
    pub ask_px: [i64; 5],
    pub ask_qty: [i64; 5],
    pub _pad: [u8; 32],
}

#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SymbolRec {
    pub symbol_id: u32,
    pub exchange_id: u16,
    pub price_scale: u16,
    pub amount_scale: u16,
    pub _pad0: u16,
    pub lot_size: u32,
    pub status: u16,
    pub _pad1: u16,
    pub code: [u8; 32],
}

pub fn symbol_key(exchange: &str, code: &str) -> String {
    format!("{exchange}:{code}")
}

/// 按 UTC 把事件时间对齐到周期桶起点。
///
/// 不做时区、夏令时或交易时段换算。负时间戳用欧几里得余数，结果仍是不大于事件时间的桶起点。
///
/// # Arguments
///
/// * `period` - 桶长度来自 [`Period::nanos`]。
/// * `event_ts_ns` - 事件时间，UTC 纳秒。可以为负。
///
/// # Returns
///
/// 小于等于 `event_ts_ns` 的桶起点。
pub fn align_bucket(period: Period, event_ts_ns: i64) -> i64 {
    let step = period.nanos();
    event_ts_ns - event_ts_ns.rem_euclid(step)
}

/// 把成交编成 32 字节小端载荷：价格、成交量、金额、成交编号。
///
/// # Arguments
///
/// * `trade` - 定点价格和成交量。
///
/// # Returns
///
/// 可放进 WAL 或帧载荷的 32 字节。
pub fn encode_trade(trade: &TickTrade) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[0..8].copy_from_slice(&trade.price.to_le_bytes());
    out[8..16].copy_from_slice(&trade.volume.to_le_bytes());
    out[16..24].copy_from_slice(&trade.amount.to_le_bytes());
    out[24..32].copy_from_slice(&trade.trade_id.to_le_bytes());
    out
}

/// 还原 [`encode_trade`] 的 32 字节。
///
/// # Arguments
///
/// * `bytes` - 至少 32 字节。更长时只读前 32 字节。
///
/// # Returns
///
/// 长度不足时返回 `None`。
pub fn decode_trade(bytes: &[u8]) -> Option<TickTrade> {
    if bytes.len() < 32 {
        return None;
    }
    Some(TickTrade {
        price: i64::from_le_bytes(bytes[0..8].try_into().ok()?),
        volume: i64::from_le_bytes(bytes[8..16].try_into().ok()?),
        amount: i64::from_le_bytes(bytes[16..24].try_into().ok()?),
        trade_id: u64::from_le_bytes(bytes[24..32].try_into().ok()?),
    })
}

pub fn encode_kline(body: &KlineBody) -> [u8; 72] {
    let mut out = [0u8; 72];
    out[0..8].copy_from_slice(&body.bucket_start_ns.to_le_bytes());
    out[8..16].copy_from_slice(&body.open.to_le_bytes());
    out[16..24].copy_from_slice(&body.high.to_le_bytes());
    out[24..32].copy_from_slice(&body.low.to_le_bytes());
    out[32..40].copy_from_slice(&body.close.to_le_bytes());
    out[40..48].copy_from_slice(&body.volume.to_le_bytes());
    out[48..56].copy_from_slice(&body.amount.to_le_bytes());
    out[56..60].copy_from_slice(&body.tick_count.to_le_bytes());
    out[60] = body.state;
    out[61] = body.origin;
    out[62..64].copy_from_slice(&body.revision.to_le_bytes());
    out[64..68].copy_from_slice(&body.flags.to_le_bytes());
    out
}

pub fn decode_kline(bytes: &[u8]) -> Option<KlineBody> {
    if bytes.len() < 68 {
        return None;
    }
    Some(KlineBody {
        bucket_start_ns: i64::from_le_bytes(bytes[0..8].try_into().ok()?),
        open: i64::from_le_bytes(bytes[8..16].try_into().ok()?),
        high: i64::from_le_bytes(bytes[16..24].try_into().ok()?),
        low: i64::from_le_bytes(bytes[24..32].try_into().ok()?),
        close: i64::from_le_bytes(bytes[32..40].try_into().ok()?),
        volume: i64::from_le_bytes(bytes[40..48].try_into().ok()?),
        amount: i64::from_le_bytes(bytes[48..56].try_into().ok()?),
        tick_count: u32::from_le_bytes(bytes[56..60].try_into().ok()?),
        state: bytes[60],
        origin: bytes[61],
        revision: u16::from_le_bytes(bytes[62..64].try_into().ok()?),
        flags: u32::from_le_bytes(bytes[64..68].try_into().ok()?),
    })
}

pub fn fixed_code(code: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    let bytes = code.as_bytes();
    let n = bytes.len().min(32);
    out[..n].copy_from_slice(&bytes[..n]);
    out
}

pub fn code_str(code: &[u8]) -> String {
    let end = code
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(code.len());
    String::from_utf8_lossy(&code[..end]).into_owned()
}

/// 帧校验和。小端宿主上按字段字节计算，不依赖结构体整体转储。
pub fn checksum_frame(header: &FrameHeader, payload: &[u8]) -> u32 {
    let mut bytes = Vec::with_capacity(FRAME_HEADER_SIZE + payload.len());
    bytes.extend_from_slice(&header.seq.to_le_bytes());
    bytes.extend_from_slice(&header.event_ts_ns.to_le_bytes());
    bytes.extend_from_slice(&header.recv_ts_ns.to_le_bytes());
    bytes.extend_from_slice(&header.payload_len.to_le_bytes());
    bytes.extend_from_slice(&header.msg_type.to_le_bytes());
    bytes.extend_from_slice(&header.flags.to_le_bytes());
    bytes.extend_from_slice(&header.symbol_id.to_le_bytes());
    bytes.extend_from_slice(&header.exchange_id.to_le_bytes());
    bytes.extend_from_slice(&header.period_id.to_le_bytes());
    bytes.extend_from_slice(&header.partition_id.to_le_bytes());
    bytes.extend_from_slice(&header.reserved.to_le_bytes());
    let len = header.payload_len as usize;
    bytes.extend_from_slice(&payload[..len.min(payload.len())]);
    crc32fast::hash(&bytes)
}

pub fn frame_to_bytes(frame: &Frame) -> [u8; FRAME_SIZE] {
    let mut out = [0u8; FRAME_SIZE];
    let header = &frame.header;
    out[0..8].copy_from_slice(&header.seq.to_le_bytes());
    out[8..16].copy_from_slice(&header.event_ts_ns.to_le_bytes());
    out[16..24].copy_from_slice(&header.recv_ts_ns.to_le_bytes());
    out[24..28].copy_from_slice(&header.payload_len.to_le_bytes());
    out[28..30].copy_from_slice(&header.msg_type.to_le_bytes());
    out[30..32].copy_from_slice(&header.flags.to_le_bytes());
    out[32..36].copy_from_slice(&header.symbol_id.to_le_bytes());
    out[36..38].copy_from_slice(&header.exchange_id.to_le_bytes());
    out[38..40].copy_from_slice(&header.period_id.to_le_bytes());
    out[40..42].copy_from_slice(&header.partition_id.to_le_bytes());
    out[42..44].copy_from_slice(&header.reserved.to_le_bytes());
    out[44..48].copy_from_slice(&header.crc32.to_le_bytes());
    out[FRAME_HEADER_SIZE..].copy_from_slice(&frame.payload);
    out
}

pub fn frame_from_bytes(bytes: &[u8]) -> Option<Frame> {
    if bytes.len() < FRAME_SIZE {
        return None;
    }
    let header = FrameHeader {
        seq: u64::from_le_bytes(bytes[0..8].try_into().ok()?),
        event_ts_ns: i64::from_le_bytes(bytes[8..16].try_into().ok()?),
        recv_ts_ns: i64::from_le_bytes(bytes[16..24].try_into().ok()?),
        payload_len: u32::from_le_bytes(bytes[24..28].try_into().ok()?),
        msg_type: u16::from_le_bytes(bytes[28..30].try_into().ok()?),
        flags: u16::from_le_bytes(bytes[30..32].try_into().ok()?),
        symbol_id: u32::from_le_bytes(bytes[32..36].try_into().ok()?),
        exchange_id: u16::from_le_bytes(bytes[36..38].try_into().ok()?),
        period_id: u16::from_le_bytes(bytes[38..40].try_into().ok()?),
        partition_id: u16::from_le_bytes(bytes[40..42].try_into().ok()?),
        reserved: u16::from_le_bytes(bytes[42..44].try_into().ok()?),
        crc32: u32::from_le_bytes(bytes[44..48].try_into().ok()?),
    };
    let mut payload = [0u8; FRAME_PAYLOAD_SIZE];
    payload.copy_from_slice(&bytes[FRAME_HEADER_SIZE..FRAME_SIZE]);
    Some(Frame { header, payload })
}

/// 填上载荷长度和 CRC，得到一帧。
///
/// 载荷超过 [`FRAME_PAYLOAD_SIZE`] 时截断。CRC 覆盖头里 CRC 字段以外的字节和载荷。
///
/// # Arguments
///
/// * `header` - 序号、时间和消息类型已填好。`payload_len` 和 `crc32` 会被改写。
/// * `payload` - 消息体。
///
/// # Returns
///
/// 固定 256 字节的帧。
pub fn seal_frame(mut header: FrameHeader, payload: &[u8]) -> Frame {
    let len = payload.len().min(FRAME_PAYLOAD_SIZE);
    header.payload_len = len as u32;
    header.crc32 = 0;
    header.crc32 = checksum_frame(&header, &payload[..len]);
    let mut frame = Frame {
        header,
        payload: [0; FRAME_PAYLOAD_SIZE],
    };
    frame.payload[..len].copy_from_slice(&payload[..len]);
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, offset_of, size_of};

    #[test]
    fn frame_and_pods_have_fixed_sizes() {
        assert_eq!(size_of::<FrameHeader>(), FRAME_HEADER_SIZE);
        assert_eq!(size_of::<Frame>(), FRAME_SIZE);
        assert_eq!(size_of::<TickTrade>(), 32);
        assert!(size_of::<TickQuote>() <= FRAME_PAYLOAD_SIZE);
        assert_eq!(size_of::<SymbolRec>(), 64);
        assert_eq!(align_of::<SymbolRec>(), 64);
        assert_eq!(size_of::<KlinePod>() % 64, 0);
        assert_eq!(size_of::<SnapshotPod>(), 256);
        assert_eq!(align_of::<SnapshotPod>(), 64);
        assert_eq!(offset_of!(FrameHeader, seq), 0);
        assert_eq!(offset_of!(FrameHeader, payload_len), 24);
        assert_eq!(offset_of!(FrameHeader, crc32), 44);
        assert_eq!(offset_of!(KlinePod, open), 40);
    }

    #[test]
    fn one_second_and_one_minute_buckets_follow_utc() {
        let ts = utc_ns(2024, 1, 2, 1, 30, 0, 100_000_000);
        let next = utc_ns(2024, 1, 2, 1, 30, 1, 0);
        assert_eq!(
            align_bucket(Period::S1, ts),
            utc_ns(2024, 1, 2, 1, 30, 0, 0)
        );
        assert_ne!(align_bucket(Period::S1, ts), align_bucket(Period::S1, next));
        assert_eq!(align_bucket(Period::M1, ts), align_bucket(Period::M1, next));
        assert_eq!(align_bucket(Period::D1, ts), utc_ns(2024, 1, 2, 0, 0, 0, 0));
    }

    fn utc_ns(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32, nanos: i64) -> i64 {
        let days = days_from_civil(y, m, d);
        (days * 86_400 + i64::from(h) * 3600 + i64::from(min) * 60 + i64::from(s)) * 1_000_000_000
            + nanos
    }

    fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
        y -= i32::from(m <= 2);
        let era = if y >= 0 { y } else { y - 399 } / 400;
        let yoe = (y - era * 400) as u64;
        let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era as i64 * 146_097 + doe as i64 - 719_468
    }
}
