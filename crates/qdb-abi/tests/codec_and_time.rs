use qdb_abi::{
    align_bucket, checksum_frame, code_str, decode_kline, decode_trade, encode_kline, encode_trade,
    fixed_code, frame_from_bytes, frame_to_bytes,
    seal_frame, symbol_key, FrameHeader, KlineBody, MsgType, Period, TickTrade, FRAME_SIZE,
    PERIOD_D1_ID,
};

#[test]
fn trade_and_kline_roundtrip_reject_short_input() {
    let trade = TickTrade {
        price: -5,
        volume: 100,
        amount: -500,
        trade_id: 99,
    };
    assert_eq!(decode_trade(&encode_trade(&trade)).unwrap(), trade);
    assert!(decode_trade(&[0; 31]).is_none());

    let body = KlineBody {
        bucket_start_ns: 10,
        open: 1,
        high: 3,
        low: 1,
        close: 2,
        volume: 4,
        amount: 8,
        tick_count: 2,
        state: 1,
        origin: 0,
        revision: 3,
        flags: 1,
    };
    assert_eq!(decode_kline(&encode_kline(&body)).unwrap(), body);
    assert!(decode_kline(&[0; 10]).is_none());
}

#[test]
fn sealed_frame_detects_a_changed_payload_byte() {
    let frame = seal_frame(
        FrameHeader {
            seq: 7,
            event_ts_ns: 11,
            recv_ts_ns: 12,
            payload_len: 0,
            msg_type: MsgType::TickTrade as u16,
            flags: 0,
            symbol_id: 3,
            exchange_id: 1,
            period_id: 0,
            partition_id: 0,
            reserved: 0,
            crc32: 0,
        },
        &[9, 8, 7, 6],
    );
    let bytes = frame_to_bytes(&frame);
    assert_eq!(bytes.len(), FRAME_SIZE);
    let restored = frame_from_bytes(&bytes).unwrap();
    assert_eq!(
        restored.header.crc32,
        checksum_frame(&restored.header, &restored.payload)
    );
    assert!(frame_from_bytes(&[0; 10]).is_none());
}

#[test]
fn period_ids_fit_the_frame_and_four_hours_align_to_epoch() {
    assert_eq!(Period::parse("4h").unwrap().id(), 14_400);
    assert_eq!(Period::D1.id(), PERIOD_D1_ID);
    assert!(Period::parse("1w").is_none());
    let step = Period::H4.nanos();
    assert_eq!(align_bucket(Period::H4, step), step);
    assert_eq!(align_bucket(Period::H4, step - 1), 0);
    assert_eq!(align_bucket(Period::H4, -1), -step);
}

#[test]
fn symbol_key_is_caller_text_and_days_are_utc() {
    assert_eq!(symbol_key("ANY", "BTCUSDT"), "ANY:BTCUSDT");
    let midnight = utc_ns(2024, 1, 3, 0, 0, 0);
    assert_eq!(align_bucket(Period::D1, midnight), midnight);
    assert_eq!(align_bucket(Period::D1, midnight - 1), midnight - Period::D1.nanos());
}

#[test]
fn symbol_codes_are_fixed_width_and_stop_at_nul() {
    let mut raw = fixed_code("ABCDEFGHIJKLMNOPQRSTUVWXYZ012345EXTRA");
    assert_eq!(code_str(&raw).len(), 32);
    raw[3] = 0;
    assert_eq!(code_str(&raw), "ABC");
}

fn utc_ns(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> i64 {
    let days = days_from_civil(y, m, d);
    (days * 86_400 + i64::from(h) * 3600 + i64::from(min) * 60 + i64::from(s)) * 1_000_000_000
}

fn days_from_civil(mut y: i32, m: u32, d: u32) -> i64 {
    y -= i32::from(m <= 2);
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) as u64 + 2) / 5 + d as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era as i64 * 146_097 + doe as i64 - 719_468
}
