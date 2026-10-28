//! WebSocket 文本推送。一条连接执行一条 `SELECT`，先发当前行，再发新落盘的匹配行。
//!
//! 连接只读。同一个库的全部连接共用一个跟随线程，慢连接只关掉自己。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use qdb_sql::split_subscribe;

use crate::fanout::{self, Counters};
use crate::feed::HttpRequest;

const GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// 完成握手并在当前线程上推送，直到客户端关闭或发送失败。
///
/// # Arguments
///
/// * `stream` - 已经读完 HTTP 请求的连接。
/// * `request` - 含 `Upgrade` 和 `Sec-WebSocket-Key` 的请求。
/// * `database` - 库目录，不是实例根目录。
/// * `counters` - 写入进程的订阅计数。测试里可以是 `None`。
///
/// # Errors
///
/// 不是 WebSocket 升级，或握手写失败时返回错误。推送循环里的读写错误只结束连接。
pub fn accept(
    mut stream: TcpStream,
    request: &HttpRequest,
    database: &std::path::Path,
    counters: Option<Counters>,
) -> std::io::Result<()> {
    let key = request
        .headers
        .get("sec-websocket-key")
        .map(String::as_str)
        .unwrap_or("");
    let upgrade = request
        .headers
        .get("upgrade")
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"));
    if !upgrade || key.is_empty() {
        let body = "{\"error\":\"websocket upgrade required\"}";
        let resp = format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(resp.as_bytes())?;
        return Ok(());
    }
    let accept_key = accept_key(key);
    let resp = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept_key}\r\n\r\n"
    );
    stream.write_all(resp.as_bytes())?;
    let _ = stream.set_nodelay(true);
    push(&mut stream, database, counters);
    Ok(())
}

fn push(stream: &mut TcpStream, database: &std::path::Path, counters: Option<Counters>) {
    let Some(sql) = read_subscribe(stream) else {
        return;
    };
    let sql = match split_subscribe(&sql) {
        Ok(Some(sql)) => sql,
        Ok(None) => sql,
        Err(err) => {
            let _ = send_text(
                stream,
                &format!(
                    "{{\"type\":\"error\",\"message\":{}}}",
                    json_string(&err.to_string())
                ),
            );
            return;
        }
    };
    if !sql
        .split_whitespace()
        .next()
        .is_some_and(|word| word.eq_ignore_ascii_case("select"))
    {
        let _ = send_text(
            stream,
            "{\"type\":\"error\",\"message\":\"select required\"}",
        );
        return;
    }
    let out = fanout::subscribe(database, sql, counters);
    let _ = stream.set_read_timeout(Some(Duration::from_millis(1)));
    loop {
        match read_frame(stream) {
            Ok(Frame::Close) => break,
            Ok(Frame::Ping(payload)) => {
                if send_frame(stream, 0xA, &payload).is_err() {
                    break;
                }
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::TimedOut
                    || err.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(_) => break,
            Ok(_) => {}
        }
        if let Some(text) = out.pop() {
            if send_text(stream, &text).is_err() {
                break;
            }
            if text == "{\"type\":\"gap\"}" || text.contains("\"type\":\"error\"") {
                break;
            }
        }
    }
    out.close();
}

fn read_subscribe(stream: &mut TcpStream) -> Option<String> {
    let saved = stream.read_timeout().ok().flatten();
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let text = loop {
        match read_frame(stream) {
            Ok(Frame::Text(text)) => break Some(text),
            Ok(Frame::Close) => break None,
            Ok(Frame::Ping(payload)) => {
                if send_frame(stream, 0xA, &payload).is_err() {
                    break None;
                }
            }
            Ok(_) => {}
            Err(_) => break None,
        }
    };
    let _ = stream.set_read_timeout(saved);
    text
}

enum Frame {
    Text(String),
    Ping(Vec<u8>),
    Close,
    Other,
}

fn read_frame(stream: &mut TcpStream) -> std::io::Result<Frame> {
    let mut header = [0u8; 2];
    stream.read_exact(&mut header)?;
    let opcode = header[0] & 0x0f;
    let masked = header[1] & 0x80 != 0;
    let mut len = (header[1] & 0x7f) as u64;
    if len == 126 {
        let mut ext = [0u8; 2];
        stream.read_exact(&mut ext)?;
        len = u16::from_be_bytes(ext) as u64;
    } else if len == 127 {
        let mut ext = [0u8; 8];
        stream.read_exact(&mut ext)?;
        len = u64::from_be_bytes(ext);
    }
    if len > 1_000_000 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut mask = [0u8; 4];
    if masked {
        stream.read_exact(&mut mask)?;
    }
    let mut payload = vec![0u8; len as usize];
    if len > 0 {
        stream.read_exact(&mut payload)?;
    }
    if masked {
        for (index, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[index % 4];
        }
    }
    Ok(match opcode {
        0x1 => Frame::Text(String::from_utf8_lossy(&payload).into_owned()),
        0x8 => Frame::Close,
        0x9 => Frame::Ping(payload),
        _ => Frame::Other,
    })
}

fn send_text(stream: &mut TcpStream, text: &str) -> std::io::Result<()> {
    send_frame(stream, 0x1, text.as_bytes())
}

fn send_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> std::io::Result<()> {
    let mut header = Vec::with_capacity(10 + payload.len());
    header.push(0x80 | opcode);
    if payload.len() < 126 {
        header.push(payload.len() as u8);
    } else if payload.len() <= u16::MAX as usize {
        header.push(126);
        header.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    } else {
        header.push(127);
        header.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    }
    header.extend_from_slice(payload);
    stream.write_all(&header)
}

fn json_string(value: &str) -> String {
    let mut out = String::from("\"");
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            ch => out.push(ch),
        }
    }
    out.push('"');
    out
}

pub fn accept_key(key: &str) -> String {
    base64(&sha1(format!("{key}{GUID}").as_bytes()))
}

fn base64(bytes: &[u8]) -> String {
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    let mut index = 0;
    while index + 3 <= bytes.len() {
        let n = ((bytes[index] as u32) << 16)
            | ((bytes[index + 1] as u32) << 8)
            | bytes[index + 2] as u32;
        out.push(ALPHA[((n >> 18) & 63) as usize] as char);
        out.push(ALPHA[((n >> 12) & 63) as usize] as char);
        out.push(ALPHA[((n >> 6) & 63) as usize] as char);
        out.push(ALPHA[(n & 63) as usize] as char);
        index += 3;
    }
    if bytes.len() - index == 1 {
        let n = (bytes[index] as u32) << 16;
        out.push(ALPHA[((n >> 18) & 63) as usize] as char);
        out.push(ALPHA[((n >> 12) & 63) as usize] as char);
        out.push_str("==");
    } else if bytes.len() - index == 2 {
        let n = ((bytes[index] as u32) << 16) | ((bytes[index + 1] as u32) << 8);
        out.push(ALPHA[((n >> 18) & 63) as usize] as char);
        out.push(ALPHA[((n >> 12) & 63) as usize] as char);
        out.push(ALPHA[((n >> 6) & 63) as usize] as char);
        out.push('=');
    }
    out
}

fn sha1(message: &[u8]) -> [u8; 20] {
    let ml = (message.len() as u64).wrapping_mul(8);
    let mut data = message.to_vec();
    data.push(0x80);
    while (data.len() % 64) != 56 {
        data.push(0);
    }
    data.extend_from_slice(&ml.to_be_bytes());
    let mut h0: u32 = 0x67452301;
    let mut h1: u32 = 0xEFCDAB89;
    let mut h2: u32 = 0x98BADCFE;
    let mut h3: u32 = 0x10325476;
    let mut h4: u32 = 0xC3D2E1F0;
    for chunk in data.chunks(64) {
        let mut w = [0u32; 80];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(chunk[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h0, h1, h2, h3, h4);
        for i in 0..80 {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(w[i]);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h0 = h0.wrapping_add(a);
        h1 = h1.wrapping_add(b);
        h2 = h2.wrapping_add(c);
        h3 = h3.wrapping_add(d);
        h4 = h4.wrapping_add(e);
    }
    let mut out = [0u8; 20];
    out[0..4].copy_from_slice(&h0.to_be_bytes());
    out[4..8].copy_from_slice(&h1.to_be_bytes());
    out[8..12].copy_from_slice(&h2.to_be_bytes());
    out[12..16].copy_from_slice(&h3.to_be_bytes());
    out[16..20].copy_from_slice(&h4.to_be_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use qdb_store::Engine;
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::thread;

    #[test]
    fn accept_key_matches_the_rfc_sample() {
        assert_eq!(
            sha1(b"abc")
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "a9993e364706816aba3e25717850c26c9cd0d89d"
        );
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn websocket_pushes_the_next_matching_row() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("qdb-ws-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut writer = Engine::open(&dir).unwrap();
        writer.write_trade("SSE", "600000", 1, 10, 1).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_dir = dir.clone();
        let server = thread::spawn(move || {
            let mut stream = listener.accept().unwrap().0;
            let request = crate::feed::read_request(&mut stream).unwrap();
            accept(stream, &request, &server_dir, None).unwrap();
        });
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        client
            .write_all(b"GET /v1/ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
            .unwrap();
        let mut prelude = Vec::new();
        let mut byte = [0u8; 1];
        while !prelude.windows(4).any(|window| window == b"\r\n\r\n") {
            client.read_exact(&mut byte).unwrap();
            prelude.push(byte[0]);
        }
        let response = String::from_utf8_lossy(&prelude);
        assert!(response.contains("101"), "{response}");
        assert!(response.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        write_client_text(
            &mut client,
            "SELECT price FROM ticks WHERE symbol = 'SSE:600000'",
        );
        let snapshot = read_server_text(&mut client).unwrap();
        assert!(snapshot.contains("\"type\":\"snapshot\""), "{snapshot}");
        assert!(snapshot.contains("10"), "{snapshot}");
        writer.write_trade("SSE", "600000", 2, 20, 1).unwrap();
        let row = read_server_text(&mut client).unwrap();
        assert!(row.contains("\"type\":\"row\""), "{row}");
        assert!(row.contains("20"), "{row}");
        write_client_frame(&mut client, 0x8, &[]);
        server.join().unwrap();
        drop(writer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn websocket_fans_one_batch_out_to_two_clients() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("qdb-ws-fan-{n}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut writer = Engine::open(&dir).unwrap();
        writer.write_trade("SSE", "600000", 1, 10, 1).unwrap();
        let count = std::sync::Arc::new(AtomicU64::new(0));
        let counters = Some(crate::fanout::Counters {
            subscriber_count: std::sync::Arc::clone(&count),
        });
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_dir = dir.clone();
        thread::spawn(move || {
            for _ in 0..2 {
                let mut stream = listener.accept().unwrap().0;
                let request = crate::feed::read_request(&mut stream).unwrap();
                let dir = server_dir.clone();
                let counters = counters.clone();
                thread::spawn(move || {
                    accept(stream, &request, &dir, counters).unwrap();
                });
            }
        });
        let mut first = handshake(port);
        let mut second = handshake(port);
        write_client_text(
            &mut first,
            "SELECT price FROM ticks WHERE symbol = 'SSE:600000'",
        );
        write_client_text(
            &mut second,
            "SELECT price FROM ticks WHERE symbol = 'SSE:600000'",
        );
        let left = read_server_text(&mut first).unwrap();
        let right = read_server_text(&mut second).unwrap();
        assert!(left.contains("\"type\":\"snapshot\""), "{left}");
        assert!(right.contains("\"type\":\"snapshot\""), "{right}");
        let started = std::time::Instant::now();
        while count.load(Ordering::Acquire) != 2 && started.elapsed() < Duration::from_secs(1) {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(count.load(Ordering::Acquire), 2);
        writer.write_trade("SSE", "600000", 2, 20, 1).unwrap();
        let left = read_server_text(&mut first).unwrap();
        let right = read_server_text(&mut second).unwrap();
        assert!(left.contains("20"), "{left}");
        assert!(right.contains("20"), "{right}");
        write_client_frame(&mut first, 0x8, &[]);
        write_client_frame(&mut second, 0x8, &[]);
        let started = std::time::Instant::now();
        while count.load(Ordering::Acquire) != 0 && started.elapsed() < Duration::from_secs(1) {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(count.load(Ordering::Acquire), 0);
        drop(writer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn many_websocket_subscribers_each_get_their_own_symbol() {
        const CLIENTS: usize = 128;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("qdb-ws-desk-{n}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut writer = Engine::open(&dir).unwrap();
        writer
            .write_trade("SSE", "600519", 1, 1_680_0000, 100)
            .unwrap();
        writer
            .write_trade("SZSE", "000001", 1, 120_000, 100)
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server_dir = dir.clone();
        thread::spawn(move || {
            for _ in 0..(CLIENTS + 1) {
                let mut stream = listener.accept().unwrap().0;
                let request = crate::feed::read_request(&mut stream).unwrap();
                let dir = server_dir.clone();
                thread::spawn(move || {
                    accept(stream, &request, &dir, None).unwrap();
                });
            }
        });
        let mut maotai: Vec<TcpStream> = (0..CLIENTS).map(|_| handshake(port)).collect();
        let mut ping_an = handshake(port);
        for client in &mut maotai {
            write_client_text(
                client,
                "SELECT price FROM ticks WHERE symbol = 'SSE:600519'",
            );
        }
        write_client_text(
            &mut ping_an,
            "SELECT price FROM ticks WHERE symbol = 'SZSE:000001'",
        );
        for client in &mut maotai {
            let snapshot = read_server_text(client).unwrap();
            assert!(snapshot.contains("16800000"), "{snapshot}");
            assert!(!snapshot.contains("120000"), "{snapshot}");
        }
        let snapshot = read_server_text(&mut ping_an).unwrap();
        assert!(snapshot.contains("120000"), "{snapshot}");
        assert!(!snapshot.contains("16800000"), "{snapshot}");
        writer
            .write_trade("SSE", "600519", 2, 1_680_0700, 100)
            .unwrap();
        writer
            .write_trade("SZSE", "000001", 2, 120_100, 100)
            .unwrap();
        for client in &mut maotai {
            let row = read_server_text(client).unwrap();
            assert!(row.contains("16800700"), "{row}");
            assert!(!row.contains("120100"), "{row}");
        }
        let row = read_server_text(&mut ping_an).unwrap();
        assert!(row.contains("120100"), "{row}");
        assert!(!row.contains("16800700"), "{row}");
        for client in &mut maotai {
            write_client_frame(client, 0x8, &[]);
        }
        write_client_frame(&mut ping_an, 0x8, &[]);
        drop(writer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn handshake(port: u16) -> TcpStream {
        let mut client = TcpStream::connect(("127.0.0.1", port)).unwrap();
        client
            .write_all(b"GET /v1/ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
            .unwrap();
        let mut prelude = Vec::new();
        let mut byte = [0u8; 1];
        while !prelude.windows(4).any(|window| window == b"\r\n\r\n") {
            client.read_exact(&mut byte).unwrap();
            prelude.push(byte[0]);
        }
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        client
    }

    fn write_client_text(stream: &mut TcpStream, text: &str) {
        write_client_frame(stream, 0x1, text.as_bytes());
    }

    fn write_client_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) {
        let mask = [1u8, 2, 3, 4];
        let mut frame = vec![0x80 | opcode, 0x80 | payload.len() as u8];
        frame.extend_from_slice(&mask);
        for (index, byte) in payload.iter().enumerate() {
            frame.push(byte ^ mask[index % 4]);
        }
        stream.write_all(&frame).unwrap();
    }

    fn read_server_text(stream: &mut TcpStream) -> std::io::Result<String> {
        loop {
            let mut header = [0u8; 2];
            stream.read_exact(&mut header)?;
            let opcode = header[0] & 0x0f;
            let mut len = (header[1] & 0x7f) as usize;
            if len == 126 {
                let mut ext = [0u8; 2];
                stream.read_exact(&mut ext)?;
                len = u16::from_be_bytes(ext) as usize;
            }
            let mut payload = vec![0u8; len];
            if len > 0 {
                stream.read_exact(&mut payload)?;
            }
            if opcode == 0x1 {
                return Ok(String::from_utf8_lossy(&payload).into_owned());
            }
        }
    }
}
