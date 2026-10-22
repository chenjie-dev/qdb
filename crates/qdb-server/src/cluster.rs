//! Redis Cluster 的槽位，以及哨兵式的多数确认。
//!
//! 16384 个槽。键的槽号是 CRC16 的低 14 位。花括号里的内容是哈希标签，用来把相关的键放进同一个槽。
//! 本进程只写入自己负责的槽。其他槽返回持有该槽的节点地址。

use serde::Deserialize;

pub const SLOT_COUNT: u16 = 16384;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotOwner {
    pub addr: String,
    pub start: u16,
    pub end: u16,
}

#[derive(Clone, Debug)]
pub struct Cluster {
    pub owners: Vec<SlotOwner>,
    pub local: SlotOwner,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Moved {
    pub slot: u16,
    pub addr: String,
    pub key: String,
}

/// 计算键落在哪个槽。
///
/// `{...}` 中间的文字才参与哈希，和 Redis 的哈希标签一样。没有成对括号时用整个键。
///
/// # Arguments
///
/// * `key` - 调用方给出的键，例如 `SSE:600000`。
///
/// # Returns
///
/// `0..16383`。
pub fn slot_of(key: &str) -> u16 {
    crc16(hash_tag(key).as_bytes()) & (SLOT_COUNT - 1)
}

/// 解析 `主机:端口=起-止` 的逗号列表。
///
/// # Arguments
///
/// * `text` - 例如 `10.0.0.1:9090=0-5460,10.0.0.2:9090=5461-16383`。
/// * `local_addr` - 本进程对外地址，用来标出本地槽。
///
/// # Returns
///
/// 列表为空、区间颠倒或覆盖不到全部槽时返回 `None`。
pub fn parse_cluster(text: &str, local_addr: &str) -> Option<Cluster> {
    let mut owners = Vec::new();
    for part in text.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (addr, range) = part.split_once('=')?;
        let (start, end) = range.split_once('-')?;
        let start: u16 = start.trim().parse().ok()?;
        let end: u16 = end.trim().parse().ok()?;
        if start > end || end >= SLOT_COUNT {
            return None;
        }
        owners.push(SlotOwner {
            addr: addr.trim().to_string(),
            start,
            end,
        });
    }
    if owners.is_empty() {
        return None;
    }
    let mut covered = vec![false; SLOT_COUNT as usize];
    for owner in &owners {
        for slot in owner.start..=owner.end {
            if covered[slot as usize] {
                return None;
            }
            covered[slot as usize] = true;
        }
    }
    if covered.iter().any(|slot| !slot) {
        return None;
    }
    let local = owners
        .iter()
        .find(|owner| owner.addr == local_addr)
        .cloned()?;
    Some(Cluster { owners, local })
}

impl Cluster {
    /// 键不在本节点时，返回应该去的地址。
    ///
    /// # Arguments
    ///
    /// * `key` - 写入或查询用的键。
    ///
    /// # Returns
    ///
    /// 本节点负责该槽时返回 `None`。
    pub fn moved(&self, key: &str) -> Option<Moved> {
        let slot = slot_of(key);
        if slot >= self.local.start && slot <= self.local.end {
            return None;
        }
        let addr = self
            .owners
            .iter()
            .find(|owner| slot >= owner.start && slot <= owner.end)?
            .addr
            .clone();
        Some(Moved {
            slot,
            addr,
            key: key.to_string(),
        })
    }

    pub fn json(&self) -> String {
        let mut out = String::from("{\"slots\":[");
        for (index, owner) in self.owners.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(&format!(
                "{{\"addr\":\"{}\",\"start\":{},\"end\":{}}}",
                owner.addr, owner.start, owner.end
            ));
        }
        out.push_str("]}");
        out
    }
}

/// 一批成交或报价里，第一个不属于本节点的键。
///
/// # Arguments
///
/// * `cluster` - 已启用的槽位表。没有集群时调用方不要进来。
/// * `body` - `{"items":[{"exchange","code",...}]}`。
///
/// # Returns
///
/// 全部落在本节点，或正文不是这批格式时，返回 `None`。
pub fn moved_in_batch(cluster: &Cluster, body: &[u8]) -> Option<Moved> {
    let batch: KeyBatch = serde_json::from_slice(body).ok()?;
    for item in batch.items {
        let key = format!("{}:{}", item.exchange, item.code);
        if let Some(moved) = cluster.moved(&key) {
            return Some(moved);
        }
    }
    None
}

/// 一个副本对主节点的看法。
#[derive(Clone, Debug)]
pub struct PeerReport {
    pub advertise: String,
    pub offset: u64,
    pub master_down: bool,
}

/// 主节点失联的票够不够，本副本不落后，并且偏移相同时由地址较小的副本升主。
///
/// # Arguments
///
/// * `down_votes` - 认为主节点已失联的票数，含本副本自己。
/// * `quorum` - 至少需要的票数。0 表示不自动切换。
/// * `local_offset` - 本副本已落盘偏移。
/// * `local_advertise` - 本副本对外地址，用来在偏移相同时决定谁升主。
/// * `peers` - 其他副本的应答。没应答的不在里面，也不当成偏移 0。
///
/// # Returns
///
/// 可以 `promote` 时为真。
pub fn may_promote(
    down_votes: u32,
    quorum: u32,
    local_offset: u64,
    local_advertise: &str,
    peers: &[PeerReport],
) -> bool {
    if quorum == 0 || down_votes < quorum {
        return false;
    }
    if peers.iter().any(|peer| peer.offset > local_offset) {
        return false;
    }
    let mut names = vec![local_advertise];
    for peer in peers {
        if peer.master_down && peer.offset == local_offset {
            names.push(peer.advertise.as_str());
        }
    }
    names.into_iter().min() == Some(local_advertise)
}

/// 是否改从这台更新的主节点拉取。
///
/// # Arguments
///
/// * `local_epoch` - 本进程任期。
/// * `local_fenced` - 本进程是不是已经停止写入。
/// * `remote_epoch` - 对方任期。
/// * `remote_master` - 对方是不是主节点。
/// * `same_process` - 对方地址是不是自己。
///
/// # Returns
///
/// 对方任期更高，或者任期相同但本进程已经被 fence，并且对方是另一台主节点时为真。
pub fn should_follow(
    local_epoch: u64,
    local_fenced: bool,
    remote_epoch: u64,
    remote_master: bool,
    same_process: bool,
) -> bool {
    if same_process || !remote_master {
        return false;
    }
    remote_epoch > local_epoch || (remote_epoch == local_epoch && local_fenced)
}

/// `http://主机:端口` 和对外地址是不是同一台。
///
/// # Arguments
///
/// * `advertise` - 本进程的 `主机:端口`。
/// * `upstream` - 可能带 `http://` 的上游。
pub fn same_endpoint(advertise: &str, upstream: &str) -> bool {
    let host = upstream
        .trim_end_matches('/')
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    !advertise.is_empty() && host == advertise
}

/// 至少 `required` 个副本都已应答到的最大偏移。
///
/// # Arguments
///
/// * `required` - 需要的副本数。0 表示不必等待。
/// * `offsets` - 还在时限内的副本偏移。
///
/// # Returns
///
/// 不够数时返回 `None`。`required` 为 0 时返回 `Some(u64::MAX)`。
pub fn acked_offset(required: usize, offsets: &[u64]) -> Option<u64> {
    if required == 0 {
        return Some(u64::MAX);
    }
    if offsets.len() < required {
        return None;
    }
    let mut sorted = offsets.to_vec();
    sorted.sort_unstable();
    Some(sorted[sorted.len() - required])
}

#[derive(Deserialize)]
struct KeyBatch {
    items: Vec<KeyItem>,
}

#[derive(Deserialize)]
struct KeyItem {
    exchange: String,
    code: String,
}

fn hash_tag(key: &str) -> &str {
    let Some(start) = key.find('{') else {
        return key;
    };
    let rest = &key[start + 1..];
    let Some(end) = rest.find('}') else {
        return key;
    };
    if end == 0 {
        return key;
    }
    &rest[..end]
}

fn crc16(data: &[u8]) -> u16 {
    let mut crc = 0u16;
    for &byte in data {
        crc ^= u16::from(byte) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slots_match_redis_and_hash_tags_stay_together() {
        assert_eq!(slot_of("foo"), 12182);
        assert_eq!(slot_of("bar"), 5061);
        assert_eq!(slot_of("{user}.a"), slot_of("{user}.b"));
        assert_ne!(slot_of("a"), slot_of("b"));
    }

    #[test]
    fn cluster_rejects_gaps_and_routes_away_from_local_slots() {
        let text = "10.0.0.1:9090=0-8191,10.0.0.2:9090=8192-16383";
        let cluster = parse_cluster(text, "10.0.0.1:9090").unwrap();
        assert!(parse_cluster("10.0.0.1:9090=0-10", "10.0.0.1:9090").is_none());
        assert!(
            parse_cluster("10.0.0.1:9090=10-1,10.0.0.2:9090=2-16383", "10.0.0.1:9090").is_none()
        );
        let key = (0..10_000)
            .map(|n| n.to_string())
            .find(|key| slot_of(key) >= 8192)
            .unwrap();
        let moved = cluster.moved(&key).unwrap();
        assert_eq!(moved.addr, "10.0.0.2:9090");
        assert!(
            cluster.moved("foo").is_none() || cluster.moved("foo").unwrap().addr == "10.0.0.2:9090"
        );
    }

    #[test]
    fn promotion_needs_quorum_and_the_highest_offset() {
        let behind = PeerReport {
            advertise: "b:9090".into(),
            offset: 40,
            master_down: true,
        };
        let tied = PeerReport {
            advertise: "b:9090".into(),
            offset: 100,
            master_down: true,
        };
        let ahead = PeerReport {
            advertise: "b:9090".into(),
            offset: 80,
            master_down: true,
        };
        assert!(may_promote(2, 2, 100, "a:9090", &[behind, tied.clone()]));
        assert!(!may_promote(1, 2, 100, "a:9090", &[]));
        assert!(!may_promote(2, 2, 50, "a:9090", &[ahead]));
        assert!(!may_promote(2, 0, 100, "a:9090", &[]));
        assert!(!may_promote(2, 2, 100, "c:9090", &[tied]));
    }

    #[test]
    fn follow_requires_a_newer_master_and_acks_use_the_nth_offset() {
        assert!(should_follow(1, false, 2, true, false));
        assert!(should_follow(2, true, 2, true, false));
        assert!(!should_follow(2, false, 2, true, false));
        assert!(!should_follow(2, true, 1, true, false));
        assert!(!should_follow(1, false, 2, false, false));
        assert!(!should_follow(1, false, 2, true, true));
        assert!(same_endpoint("qdb-2:9090", "http://qdb-2:9090"));
        assert_eq!(acked_offset(1, &[100, 50]), Some(100));
        assert_eq!(acked_offset(2, &[100, 50]), Some(50));
        assert_eq!(acked_offset(2, &[100]), None);
        assert_eq!(acked_offset(0, &[]), Some(u64::MAX));
    }
}
