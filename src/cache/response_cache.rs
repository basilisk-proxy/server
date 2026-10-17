//! Proxied-response cache with configurable eviction policies.
//!
//! Downstream services opt their responses into gateway caching by returning
//! the `X-Basilisk-Cache` HTTP header formatted as `enabled,TTL,evict-policy`
//! (TTL in seconds). Cached payloads live in the gateway's shared
//! [`GatewayCache`] runtime mechanism so they inherit the configured provider
//! (memory/redis/memcached) and key namespace, while eviction metadata is
//! tracked locally.
//!
//! Supported eviction policies:
//! - `LRU` — least recently used (default when omitted)
//! - `LFU` — least frequently used
//! - `FIFO` — first in, first out
//! - `LIFO` — last in, first out (newest inserted entries evicted first)
//! - `ARC` — adaptive replacement cache (recency/frequency adaptive)
//! - `RANDOM` — random victim selection
//! - `MRU` — most recently used

use super::GatewayCache;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::warn;

/// Response-cache control header emitted by downstream services.
pub const CACHE_CONTROL_HEADER: &str = "x-basilisk-cache";

/// Header added by the gateway reporting `hit`/`miss` per proxied response.
pub const CACHE_STATUS_HEADER: &str = "x-basilisk-cache-status";

const RESPONSE_CACHE_NAMESPACE: &str = "response-cache";

/// Eviction policies supported by the response cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    Lru,
    Lfu,
    Fifo,
    Lifo,
    Arc,
    Random,
    Mru,
}

impl EvictionPolicy {
    pub fn parse(name: &str) -> Option<Self> {
        match name.trim().to_ascii_uppercase().as_str() {
            "LRU" => Some(Self::Lru),
            "LFU" => Some(Self::Lfu),
            "FIFO" => Some(Self::Fifo),
            "LIFO" => Some(Self::Lifo),
            "ARC" => Some(Self::Arc),
            "RANDOM" => Some(Self::Random),
            "MRU" => Some(Self::Mru),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Lru => "LRU",
            Self::Lfu => "LFU",
            Self::Fifo => "FIFO",
            Self::Lifo => "LIFO",
            Self::Arc => "ARC",
            Self::Random => "RANDOM",
            Self::Mru => "MRU",
        }
    }
}

/// Parsed `X-Basilisk-Cache` header directive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheControlDirective {
    pub enabled: bool,
    pub ttl_seconds: u64,
    pub policy: EvictionPolicy,
}

/// Parses the `X-Basilisk-Cache` header value.
///
/// Expects the format `enabled,TTL,evict-policy`, e.g. `enabled,300,LRU`.
/// The disposition token may also be `disabled`, in which case caching is
/// not opted into. `LRU` is assumed when the policy token is omitted.
/// Malformed or unknown tokens produce an error so callers can skip caching.
pub fn parse_cache_control_directive(value: &str) -> anyhow::Result<Option<CacheControlDirective>> {
    let mut parts = value.split(',');
    let disposition = parts
        .next()
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing enabled/disabled disposition"))?;

    match disposition.to_ascii_uppercase().as_str() {
        "DISABLED" => return Ok(None),
        "ENABLED" => {}
        other => {
            return Err(anyhow::anyhow!(
                "unknown cache disposition '{other}'; expected 'enabled' or 'disabled'"
            ));
        }
    }

    let ttl_seconds = match parts.next().map(str::trim) {
        None => 0,
        Some("") => return Err(anyhow::anyhow!("missing TTL value")),
        Some(raw) => raw
            .parse::<u64>()
            .map_err(|_| anyhow::anyhow!("TTL '{raw}' is not a number"))?,
    };

    let policy = match parts.next().map(str::trim) {
        None | Some("") => EvictionPolicy::Lru,
        Some(raw) => EvictionPolicy::parse(raw).ok_or_else(|| {
            anyhow::anyhow!(
                "unknown eviction policy '{raw}'; expected one of LRU, LFU, FIFO, LIFO, ARC, RANDOM, MRU"
            )
        })?,
    };

    Ok(Some(CacheControlDirective {
        enabled: true,
        ttl_seconds,
        policy,
    }))
}

/// A cached proxied response.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CachedResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    body_base64: String,
}

impl CachedResponse {
    pub fn new(status: u16, headers: HashMap<String, String>, body: Vec<u8>) -> Self {
        Self {
            status,
            headers,
            body_base64: STANDARD.encode(body),
        }
    }

    /// Returns the decoded response body bytes.
    pub fn body_bytes(&self) -> Vec<u8> {
        STANDARD
            .decode(self.body_base64.as_bytes())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, Copy)]
struct EntryMeta {
    policy: EvictionPolicy,
    inserted_seq: u64,
    last_used_seq: u64,
    use_count: u64,
}

/// Adaptive Replacement Cache bookkeeping used by `ARC`-policy entries.
///
/// Maintains the standard L1/L2 resident lists (`t1`/`t2`) and ghost lists
/// (`b1`/`b2`) together with the adaptive recency/frequency target `p`.
#[derive(Default)]
struct ArcMetadata {
    t1: VecDeque<String>,
    t2: VecDeque<String>,
    b1: VecDeque<String>,
    b2: VecDeque<String>,
    p: usize,
}

impl ArcMetadata {
    fn push_ghost_bounded(&mut self, in_b1: bool, key: String, capacity: usize) {
        let queue = if in_b1 { &mut self.b1 } else { &mut self.b2 };
        queue.push_back(key);
        while queue.len() > capacity.max(1) {
            queue.pop_front();
        }
    }

    /// Records an access: hits in `t1` move to the `t2` MRU position.
    fn hit(&mut self, key: &str) {
        if let Some(pos) = self.t1.iter().position(|k| k == key) {
            self.t1.remove(pos);
            self.t2.push_back(key.to_string());
        } else if let Some(pos) = self.t2.iter().position(|k| k == key) {
            self.t2.remove(pos);
            self.t2.push_back(key.to_string());
        }
    }

    fn resident_in_t2(&self, key: &str) -> bool {
        self.t2.iter().any(|k| k == key)
    }

    /// Removes the key from the resident lists. Returns `true` when the key
    /// was resident in `t1`.
    fn remove_resident(&mut self, key: &str) -> bool {
        if let Some(pos) = self.t1.iter().position(|k| k == key) {
            self.t1.remove(pos);
            true
        } else if let Some(pos) = self.t2.iter().position(|k| k == key) {
            self.t2.remove(pos);
            false
        } else {
            false
        }
    }

    /// Applies the standard ARC adaptation for an insertion of `key`.
    fn adapt_insert(&mut self, key: &str, capacity: usize) {
        if self.remove_resident(key) {
            // Resident in t1: a second recent reference promotes t1 → t2.
            self.t2.push_back(key.to_string());
        } else if self.resident_in_t2(key) {
            self.t2.push_back(key.to_string());
        } else if self.b1.iter().any(|k| k == key) {
            self.p = self
                .p
                .saturating_add((self.b2.len().saturating_sub(self.b1.len()) / 4).max(1))
                .min(capacity);
            self.b1.retain(|k| k != key);
            self.t2.push_back(key.to_string());
        } else if self.b2.iter().any(|k| k == key) {
            self.p = self
                .p
                .saturating_sub((self.b1.len().saturating_sub(self.b2.len()) / 4).max(1));
            self.b2.retain(|k| k != key);
            self.t2.push_back(key.to_string());
        } else {
            self.t1.push_back(key.to_string());
        }
    }

    /// Evicts one resident ARC entry into the matching ghost list following
    /// the adaptive parameter `p`. Stale keys (expired from the underlying
    /// store) are skipped within a bounded number of passes; returns `None`
    /// when no resident ARC entry exists.
    fn evict_resident(
        &mut self,
        capacity: usize,
        is_resident: impl Fn(&str) -> bool,
    ) -> Option<String> {
        let mut passes = self.t1.len() + self.t2.len();
        while passes > 0 {
            passes -= 1;
            if self.t1.is_empty() && self.t2.is_empty() {
                return None;
            }
            let from_t1 = if self.t2.is_empty() {
                true
            } else if self.t1.is_empty() {
                false
            } else {
                self.t1.len() >= self.p.max(1)
            };
            let key = if from_t1 {
                self.t1.pop_front()?
            } else {
                self.t2.pop_front()?
            };
            self.push_ghost_bounded(from_t1, key.clone(), capacity);
            if is_resident(&key) {
                return Some(key);
            }
        }
        None
    }
}

/// Tracks eviction state for entries stored in the underlying cache.
struct ResponseCacheState {
    entries: DashMap<String, EntryMeta>,
    seq: AtomicU64,
    arc: Mutex<ArcMetadata>,
}

impl ResponseCacheState {
    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    fn note_insert(&self, key: &str, policy: EvictionPolicy, is_reinsert: bool) {
        let seq = self.next_seq();

        // Reinserting the same policy preserves the recency/frequency history
        // of an existing entry; anything else starts a fresh tracking record.
        let existing = self
            .entries
            .get(key)
            .map(|meta| *meta.value())
            .filter(|_| is_reinsert);
        match existing {
            Some(meta) if meta.policy == policy && meta.last_used_seq > 0 => {
                self.entries.insert(
                    key.to_string(),
                    EntryMeta {
                        policy,
                        inserted_seq: meta.inserted_seq,
                        last_used_seq: seq,
                        use_count: meta.use_count.saturating_add(1),
                    },
                );
            }
            _ => {
                self.entries.insert(
                    key.to_string(),
                    EntryMeta {
                        policy,
                        inserted_seq: seq,
                        last_used_seq: seq,
                        use_count: 1,
                    },
                );
            }
        }
    }

    /// Chooses the eviction victim for an insert arriving with `policy`.
    fn pick_victim(&self, policy: EvictionPolicy, capacity: usize) -> Option<String> {
        match policy {
            EvictionPolicy::Arc => {
                let mut arc = self
                    .arc
                    .lock()
                    .map_err(|_| anyhow::anyhow!("response cache ARC lock poisoned"))
                    .ok()?;
                arc.evict_resident(capacity, |key| self.entries.contains_key(key))
            }
            EvictionPolicy::Random => {
                let len = self.entries.len();
                if len == 0 {
                    return None;
                }
                let keys: Vec<String> = self
                    .entries
                    .iter()
                    .map(|entry| entry.key().clone())
                    .collect();
                keys.get(rand::random::<u64>() as usize % len).cloned()
            }
            other => self.pick_by(other),
        }
    }

    fn pick_by(&self, policy: EvictionPolicy) -> Option<String> {
        let mut best: Option<(String, EntryMeta)> = None;
        for entry in self.entries.iter() {
            let candidate = (entry.key().clone(), *entry.value());
            let replace = match &best {
                None => true,
                Some((_, best_meta)) => match policy {
                    EvictionPolicy::Lru => candidate.1.last_used_seq < best_meta.last_used_seq,
                    EvictionPolicy::Mru => candidate.1.last_used_seq > best_meta.last_used_seq,
                    EvictionPolicy::Lfu => {
                        (candidate.1.use_count, candidate.1.last_used_seq)
                            < (best_meta.use_count, best_meta.last_used_seq)
                    }
                    EvictionPolicy::Fifo => candidate.1.inserted_seq < best_meta.inserted_seq,
                    EvictionPolicy::Lifo => candidate.1.inserted_seq > best_meta.inserted_seq,
                    _ => false,
                },
            };
            if replace {
                best = Some(candidate);
            }
        }
        best.map(|(key, _)| key)
    }
}

/// Shared proxied-response cache.
///
/// Stores payloads inside the configured [`GatewayCache`] mechanism under the
/// internal response-cache namespace and keeps eviction metadata locally.
pub struct ResponseCache {
    cache: Arc<GatewayCache>,
    max_entries: usize,
    state: ResponseCacheState,
}

impl ResponseCache {
    /// Creates a response cache storing payloads in the shared gateway cache.
    pub fn new(cache: Arc<GatewayCache>, max_entries: usize) -> Self {
        Self {
            cache,
            max_entries: max_entries.max(1),
            state: ResponseCacheState {
                entries: DashMap::new(),
                seq: AtomicU64::new(0),
                arc: Mutex::new(ArcMetadata::default()),
            },
        }
    }

    /// Maximum number of entries retained by this cache.
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Returns the number of locally tracked entries.
    pub fn len(&self) -> usize {
        self.state.entries.len()
    }

    /// Returns `true` when no entries are tracked locally.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Storage key for a proxied response.
    pub fn response_cache_key(method: &str, path_and_query: &str) -> String {
        format!("{RESPONSE_CACHE_NAMESPACE}:{method}:{path_and_query}")
    }

    /// Fetches a previously cached response, recording the access against the
    /// entry's eviction policy.
    pub fn get(&self, key: &str) -> anyhow::Result<Option<CachedResponse>> {
        let stored = self.cache.internal_get(key)?;
        let existing = self.state.entries.get(key).map(|meta| *meta.value());

        let Some(raw) = stored else {
            // Underlying entry expired or was evicted elsewhere: drop tracking.
            if let Some(meta) = existing {
                self.remove_tracked(key, meta.policy, false);
            }
            return Ok(None);
        };

        if existing.is_some()
            && existing.map(|meta| meta.policy) == Some(EvictionPolicy::Arc)
            && let Ok(mut arc) = self.state.arc.lock()
        {
            arc.hit(key);
        }
        let seq = self.state.next_seq();
        let meta = existing.unwrap_or(EntryMeta {
            policy: EvictionPolicy::Lru,
            inserted_seq: seq,
            last_used_seq: seq,
            use_count: 0,
        });
        let updated = EntryMeta {
            last_used_seq: seq,
            use_count: meta.use_count.saturating_add(1),
            ..meta
        };
        self.state.entries.insert(key.to_string(), updated);

        match serde_json::from_str::<CachedResponse>(&raw) {
            Ok(response) => Ok(Some(response)),
            Err(err) => {
                warn!("failed to decode cached response for {key}: {err}");
                self.remove_tracked(key, meta.policy, true);
                Ok(None)
            }
        }
    }

    /// Stores a proxied response under `key` and enforces the configured
    /// capacity using the supplied eviction policy.
    pub fn put(
        &self,
        key: &str,
        response: &CachedResponse,
        ttl_seconds: u64,
        policy: EvictionPolicy,
    ) -> anyhow::Result<()> {
        let encoded = serde_json::to_string(response)
            .map_err(|err| anyhow::anyhow!("failed to encode cached response: {err}"))?;

        self.cache
            .internal_set_with_ttl(key, encoded, ttl_seconds.max(1))?;

        if policy == EvictionPolicy::Arc
            && let Ok(mut arc) = self.state.arc.lock()
        {
            arc.adapt_insert(key, self.max_entries);
        }
        self.state
            .note_insert(key, policy, self.state.entries.contains_key(key));

        while self.state.entries.len() > self.max_entries {
            let Some(victim) = self.state.pick_victim(policy, self.max_entries) else {
                break;
            };
            if victim == key && self.state.entries.len() <= 1 {
                break;
            }
            let victim_policy = self
                .state
                .entries
                .get(&victim)
                .map(|meta| meta.policy)
                .unwrap_or(policy);
            self.remove_tracked(&victim, victim_policy, true);
        }

        Ok(())
    }

    /// Removes tracking metadata (and optionally the underlying payload) for
    /// an evicted or expired key.
    fn remove_tracked(&self, key: &str, policy: EvictionPolicy, delete: bool) {
        self.state.entries.remove(key);
        if policy == EvictionPolicy::Arc
            && let Ok(mut arc) = self.state.arc.lock()
        {
            arc.remove_resident(key);
            arc.b1.retain(|k| k != key);
            arc.b2.retain(|k| k != key);
        }
        if delete && let Err(err) = self.cache.internal_delete(key) {
            warn!("failed to delete response cache payload for {key}: {err}");
        }
    }

    /// Removes a cached response and its tracking metadata explicitly.
    pub fn invalidate(&self, key: &str) {
        let policy = self
            .state
            .entries
            .get(key)
            .map(|meta| meta.policy)
            .unwrap_or(EvictionPolicy::Lru);
        self.remove_tracked(key, policy, true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cache() -> ResponseCache {
        ResponseCache::new(
            Arc::new(GatewayCache::new("memory").expect("cache init")),
            3,
        )
    }

    fn cached(status: u16, body: &str) -> CachedResponse {
        CachedResponse::new(status, HashMap::new(), body.as_bytes().to_vec())
    }

    #[test]
    fn parses_directives_case_insensitively() {
        let directive = parse_cache_control_directive("Enabled, 300, lru")
            .expect("directive parse")
            .expect("directive enabled");
        assert_eq!(directive.ttl_seconds, 300);
        assert_eq!(directive.policy, EvictionPolicy::Lru);

        assert!(
            parse_cache_control_directive("disabled")
                .expect("disabled")
                .is_none()
        );
        assert!(parse_cache_control_directive("invalid,10,LRU").is_err());
        assert!(parse_cache_control_directive("enabled,abc,LRU").is_err());
        assert!(
            parse_cache_control_directive("enabled,10,BOGUS").is_err(),
            "unknown eviction policy must be rejected"
        );
    }

    #[test]
    fn caches_and_serves_round_trip() {
        let cache = test_cache();
        let key = ResponseCache::response_cache_key("GET", "/a");
        cache
            .put(&key, &cached(200, "hello"), 60, EvictionPolicy::Lru)
            .expect("store");
        let hit = cache.get(&key).expect("read").expect("cache hit");
        assert_eq!(hit.status, 200);
        assert_eq!(hit.body_bytes(), b"hello".as_slice());
    }

    #[test]
    fn expires_tracking_when_underlying_entry_gone() {
        let cache = test_cache();
        let key = "example-key";
        cache
            .put(key, &cached(200, "x"), 60, EvictionPolicy::Lfu)
            .expect("store");
        cache.cache.internal_delete(key).expect("underlying delete");
        assert!(cache.get(key).expect("get").is_none());
        assert!(cache.is_empty(), "tracking must be dropped with payload");
    }

    #[test]
    fn lru_policy_evicts_least_recently_used() {
        let cache = test_cache();
        for (i, body) in ["one", "two", "three"].iter().enumerate() {
            cache
                .put(
                    &format!("k{i}"),
                    &cached(200, body),
                    60,
                    EvictionPolicy::Lru,
                )
                .expect("store");
        }
        cache.get("k0").expect("get");
        cache
            .put("k3", &cached(200, "four"), 60, EvictionPolicy::Lru)
            .expect("store");

        assert!(cache.get("k0").expect("get").is_some());
        assert!(cache.get("k1").expect("get").is_none());
        assert!(cache.get("k2").expect("get").is_some());
        assert!(cache.get("k3").expect("get").is_some());
    }

    #[test]
    fn mru_policy_evicts_most_recently_used() {
        let cache = test_cache();
        for (i, body) in ["one", "two", "three"].iter().enumerate() {
            cache
                .put(
                    &format!("k{i}"),
                    &cached(200, body),
                    60,
                    EvictionPolicy::Mru,
                )
                .expect("store");
        }
        cache
            .put("k3", &cached(200, "four"), 60, EvictionPolicy::Mru)
            .expect("store");

        assert!(cache.get("k0").expect("get").is_some());
        assert!(cache.get("k2").expect("get").is_some());
        assert!(
            cache.get("k3").expect("get").is_none(),
            "most recently inserted entry is the MRU victim"
        );
    }

    #[test]
    fn lfu_policy_evicts_least_frequently_used() {
        let cache = test_cache();
        for (i, body) in ["one", "two", "three"].iter().enumerate() {
            cache
                .put(
                    &format!("k{i}"),
                    &cached(200, body),
                    60,
                    EvictionPolicy::Lfu,
                )
                .expect("store");
        }
        cache.get("k0").expect("get");
        cache.get("k0").expect("get");
        cache.get("k1").expect("get");
        cache
            .put("k3", &cached(200, "four"), 60, EvictionPolicy::Lfu)
            .expect("store");

        assert!(cache.get("k0").expect("get").is_some());
        assert!(cache.get("k1").expect("get").is_some());
        assert!(
            cache.get("k2").expect("get").is_none(),
            "k2 was touched only once and must be the LFU victim"
        );
    }

    #[test]
    fn fifo_policy_evicts_first_inserted() {
        let cache = test_cache();
        for (i, body) in ["one", "two", "three"].iter().enumerate() {
            cache
                .put(
                    &format!("k{i}"),
                    &cached(200, body),
                    60,
                    EvictionPolicy::Fifo,
                )
                .expect("store");
        }
        cache.get("k0").expect("get");
        cache
            .put("k3", &cached(200, "four"), 60, EvictionPolicy::Fifo)
            .expect("store");

        assert!(
            cache.get("k0").expect("get").is_none(),
            "FIFO ignores recency and evicts the oldest insert"
        );
        assert!(cache.get("k1").expect("get").is_some());
    }

    #[test]
    fn lifo_policy_evicts_last_inserted() {
        let cache = test_cache();
        for (i, body) in ["one", "two", "three"].iter().enumerate() {
            cache
                .put(
                    &format!("k{i}"),
                    &cached(200, body),
                    60,
                    EvictionPolicy::Lifo,
                )
                .expect("store");
        }
        cache.get("k2").expect("get");
        cache
            .put("k3", &cached(200, "four"), 60, EvictionPolicy::Lifo)
            .expect("store");

        assert!(
            cache.get("k3").expect("get").is_none(),
            "LIFO evicts the newest insert"
        );
        assert!(cache.get("k0").expect("get").is_some());
        assert!(cache.get("k2").expect("get").is_some());
    }

    #[test]
    fn arc_policy_promotes_recent_reuse_and_adjusts_adaptively() {
        let cache = test_cache();
        for (i, body) in ["one", "two", "three"].iter().enumerate() {
            cache
                .put(
                    &format!("k{i}"),
                    &cached(200, body),
                    60,
                    EvictionPolicy::Arc,
                )
                .expect("store");
        }
        cache.get("k0").expect("get");
        cache
            .put("k3", &cached(200, "four"), 60, EvictionPolicy::Arc)
            .expect("store");

        // k0 was reused (promoted to the frequent t2 list), so the initial
        // recency-family victim (k1) must have been selected.
        assert!(cache.get("k0").expect("get").is_some());
        assert!(cache.get("k1").expect("get").is_none());
        assert!(cache.get("k2").expect("get").is_some());
        assert!(cache.get("k3").expect("get").is_some());
    }

    #[test]
    fn random_policy_keeps_capacity_bounded() {
        let cache = test_cache();
        cache.state.entries.clear();
        for i in 0..10 {
            cache
                .put(
                    &format!("r{i}"),
                    &cached(200, "x"),
                    60,
                    EvictionPolicy::Random,
                )
                .expect("store");
        }
        assert!(
            cache.len() <= cache.max_entries(),
            "random policy must keep the tracked set bounded"
        );
        assert!(!cache.is_empty(), "cache must retain some entries");
    }
}
