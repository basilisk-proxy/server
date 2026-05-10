use super::base::{apply_sort_options, sort_by_score_range, CacheProvider};
use memcache::Client;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{SystemTime, UNIX_EPOCH};

const KEY_INDEX: &str = "__basilisk_cache_keys__";

#[derive(Clone, Serialize, Deserialize)]
enum StoredValue {
    String(String),
    List(VecDeque<String>),
    Set(BTreeSet<String>),
    Hash(BTreeMap<String, String>),
}

#[derive(Clone, Serialize, Deserialize)]
struct StoredEntry {
    value: StoredValue,
    expires_at_unix_ms: Option<u128>,
}

pub(super) struct MemcachedCacheManager {
    client: Option<Client>,
}

impl MemcachedCacheManager {
    pub(super) fn new() -> Self {
        let url = std::env::var("BASILISK_MEMCACHED_URL")
            .unwrap_or("memcache://127.0.0.1:11211".to_string());
        Self {
            client: if memcached_endpoint_reachable(&url) {
                Client::connect(url.as_str()).ok()
            } else {
                None
            },
        }
    }

    fn get_raw(&self, key: &str) -> Option<String> {
        let client = self.client.as_ref()?;
        client.get::<String>(key).unwrap_or(None)
    }

    fn set_raw(&self, key: &str, value: &str, ttl: u32) -> bool {
        let Some(client) = self.client.as_ref() else {
            return false;
        };
        client.set(key, value, ttl).is_ok()
    }

    fn delete_raw(&self, key: &str) -> bool {
        let Some(client) = self.client.as_ref() else {
            return false;
        };
        client.delete(key).is_ok()
    }

    fn read_entry(&self, key: &str) -> Option<StoredEntry> {
        let raw = self.get_raw(key)?;
        let entry: StoredEntry = serde_json::from_str(&raw).ok()?;
        if is_expired(&entry) {
            let _ = self.delete_raw(key);
            self.untrack_key(key);
            return None;
        }
        Some(entry)
    }

    fn write_entry(&self, key: &str, entry: StoredEntry) {
        let ttl = ttl_for_entry(&entry);
        if let Ok(raw) = serde_json::to_string(&entry) {
            if self.set_raw(key, &raw, ttl) {
                self.track_key(key);
            }
        }
    }

    fn track_key(&self, key: &str) {
        if key == KEY_INDEX {
            return;
        }
        let mut keys = self.read_index();
        keys.insert(key.to_string());
        self.write_index(&keys);
    }

    fn untrack_key(&self, key: &str) {
        let mut keys = self.read_index();
        if keys.remove(key) {
            self.write_index(&keys);
        }
    }

    fn read_index(&self) -> BTreeSet<String> {
        self.get_raw(KEY_INDEX)
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default()
    }

    fn write_index(&self, keys: &BTreeSet<String>) {
        if let Ok(raw) = serde_json::to_string(keys) {
            let _ = self.set_raw(KEY_INDEX, &raw, 0);
        }
    }

    fn read_value(&self, key: &str) -> Option<StoredValue> {
        self.read_entry(key).map(|entry| entry.value)
    }

    fn write_value(&self, key: &str, value: StoredValue, ttl: Option<u64>) {
        let entry = StoredEntry {
            value,
            expires_at_unix_ms: ttl.map(|seconds| now_unix_ms() + (seconds as u128 * 1000)),
        };
        self.write_entry(key, entry);
    }

    fn current_ttl(&self, key: &str) -> Option<u64> {
        self.read_entry(key).and_then(|entry| {
            entry.expires_at_unix_ms.map(|expires_at| {
                let now = now_unix_ms();
                if expires_at <= now {
                    0
                } else {
                    ((expires_at - now) as u64).div_ceil(1000)
                }
            })
        })
    }

    fn delete_entry(&self, key: &str) -> bool {
        let deleted = self.delete_raw(key);
        self.untrack_key(key);
        deleted
    }

    fn set_members_internal(&self, key: &str) -> Vec<String> {
        match self.read_value(key) {
            Some(StoredValue::Set(set)) => set.into_iter().collect(),
            _ => Vec::new(),
        }
    }

    fn hash_fields_internal(&self, key: &str) -> Vec<String> {
        match self.read_value(key) {
            Some(StoredValue::Hash(hash)) => hash.keys().cloned().collect(),
            _ => Vec::new(),
        }
    }
}

impl CacheProvider for MemcachedCacheManager {
    fn get(&self, key: &str) -> Option<String> {
        match self.read_value(key) {
            Some(StoredValue::String(value)) => Some(value),
            _ => None,
        }
    }

    fn set_with_ttl(&mut self, key: &str, value: String, ttl: u64) {
        self.write_value(key, StoredValue::String(value), Some(ttl));
    }

    fn set_if_not_exists(&mut self, key: &str, value: String) -> bool {
        if self.read_value(key).is_some() {
            return false;
        }
        self.set(key, value);
        true
    }

    fn set(&mut self, key: &str, value: String) {
        self.write_value(key, StoredValue::String(value), None);
    }

    fn incr(&mut self, key: &str, delta: i64) -> i64 {
        let next = self
            .get(key)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
            + delta;
        self.write_value(
            key,
            StoredValue::String(next.to_string()),
            self.current_ttl(key),
        );
        next
    }

    fn decr(&mut self, key: &str, delta: i64) -> i64 {
        self.incr(key, -delta)
    }

    fn list_append(&mut self, key: &str, value: String) -> String {
        let mut list = match self.read_value(key) {
            Some(StoredValue::List(list)) => list,
            _ => VecDeque::new(),
        };
        list.push_back(value.clone());
        self.write_value(key, StoredValue::List(list), self.current_ttl(key));
        value
    }

    fn list_prepend(&mut self, key: &str, value: String) -> String {
        let mut list = match self.read_value(key) {
            Some(StoredValue::List(list)) => list,
            _ => VecDeque::new(),
        };
        list.push_front(value.clone());
        self.write_value(key, StoredValue::List(list), self.current_ttl(key));
        value
    }

    fn list_pop_left(&mut self, key: &str) -> Option<String> {
        let mut list = match self.read_value(key) {
            Some(StoredValue::List(list)) => list,
            _ => return None,
        };
        let popped = list.pop_front();
        self.write_value(key, StoredValue::List(list), self.current_ttl(key));
        popped
    }

    fn list_pop_right(&mut self, key: &str) -> Option<String> {
        let mut list = match self.read_value(key) {
            Some(StoredValue::List(list)) => list,
            _ => return None,
        };
        let popped = list.pop_back();
        self.write_value(key, StoredValue::List(list), self.current_ttl(key));
        popped
    }

    fn list_length(&self, key: &str) -> usize {
        match self.read_value(key) {
            Some(StoredValue::List(list)) => list.len(),
            _ => 0,
        }
    }

    fn list_index(&self, key: &str, index: usize) -> Option<String> {
        match self.read_value(key) {
            Some(StoredValue::List(list)) => list.get(index).cloned(),
            _ => None,
        }
    }

    fn list_range(&self, key: &str, start: usize, end: usize) -> Vec<String> {
        if end < start {
            return Vec::new();
        }
        match self.read_value(key) {
            Some(StoredValue::List(list)) => list
                .iter()
                .skip(start)
                .take(end - start + 1)
                .cloned()
                .collect(),
            _ => Vec::new(),
        }
    }

    fn set_add(&mut self, key: &str, value: String) -> bool {
        let mut set = match self.read_value(key) {
            Some(StoredValue::Set(set)) => set,
            _ => BTreeSet::new(),
        };
        let inserted = set.insert(value);
        self.write_value(key, StoredValue::Set(set), self.current_ttl(key));
        inserted
    }

    fn set_remove(&mut self, key: &str, value: String) -> bool {
        let mut set = match self.read_value(key) {
            Some(StoredValue::Set(set)) => set,
            _ => return false,
        };
        let removed = set.remove(&value);
        self.write_value(key, StoredValue::Set(set), self.current_ttl(key));
        removed
    }

    fn set_is_member(&self, key: &str, value: String) -> bool {
        match self.read_value(key) {
            Some(StoredValue::Set(set)) => set.contains(&value),
            _ => false,
        }
    }

    fn set_members(&self, key: &str) -> Vec<String> {
        self.set_members_internal(key)
    }

    fn set_random_member(&self, key: &str) -> Option<String> {
        self.set_members_internal(key).into_iter().next()
    }

    fn set_sort(&self, key: &str) -> Vec<String> {
        self.set_members_internal(key)
    }

    fn set_sort_with_options(&self, key: &str, options: String) -> Vec<String> {
        apply_sort_options(self.set_sort(key), &options)
    }

    fn set_sort_by_score(&self, key: &str, min: Option<f64>, max: Option<f64>) -> Vec<String> {
        sort_by_score_range(self.set_members_internal(key), min, max)
    }

    fn set_card(&self, key: &str) -> usize {
        self.set_members_internal(key).len()
    }

    fn hash_get(&self, key: &str, field: &str) -> Option<String> {
        match self.read_value(key) {
            Some(StoredValue::Hash(hash)) => hash.get(field).cloned(),
            _ => None,
        }
    }

    fn hash_set(&mut self, key: &str, field: &str, value: String) {
        let mut hash = match self.read_value(key) {
            Some(StoredValue::Hash(hash)) => hash,
            _ => BTreeMap::new(),
        };
        hash.insert(field.to_string(), value);
        self.write_value(key, StoredValue::Hash(hash), self.current_ttl(key));
    }

    fn hash_delete(&mut self, key: &str, field: &str) -> bool {
        let mut hash = match self.read_value(key) {
            Some(StoredValue::Hash(hash)) => hash,
            _ => return false,
        };
        let removed = hash.remove(field).is_some();
        self.write_value(key, StoredValue::Hash(hash), self.current_ttl(key));
        removed
    }

    fn hash_exists(&self, key: &str, field: &str) -> bool {
        match self.read_value(key) {
            Some(StoredValue::Hash(hash)) => hash.contains_key(field),
            _ => false,
        }
    }

    fn hash_fields(&self, key: &str) -> Vec<String> {
        self.hash_fields_internal(key)
    }

    fn hash_random_field(&self, key: &str) -> Option<String> {
        self.hash_fields_internal(key).into_iter().next()
    }

    fn hash_length(&self, key: &str) -> usize {
        self.hash_fields_internal(key).len()
    }

    fn keys(&self) -> Vec<String> {
        let mut keys = self
            .read_index()
            .into_iter()
            .filter(|key| self.read_value(key).is_some())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    fn exists(&self, key: &str) -> bool {
        self.read_value(key).is_some()
    }

    fn unlink(&mut self, key: &str) -> bool {
        self.delete_entry(key)
    }

    fn expire(&mut self, key: &str, ttl: u64) {
        if let Some(value) = self.read_value(key) {
            self.write_value(key, value, Some(ttl));
        }
    }

    fn delete(&mut self, key: &str) {
        let _ = self.delete_entry(key);
    }
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn is_expired(entry: &StoredEntry) -> bool {
    entry
        .expires_at_unix_ms
        .is_some_and(|expires_at| now_unix_ms() >= expires_at)
}

fn ttl_for_entry(entry: &StoredEntry) -> u32 {
    entry
        .expires_at_unix_ms
        .map(|expires_at| {
            let now = now_unix_ms();
            if expires_at <= now {
                1
            } else {
                ((expires_at - now) as u64).div_ceil(1000).max(1) as u32
            }
        })
        .unwrap_or(0)
}

fn memcached_endpoint_reachable(url: &str) -> bool {
    let Some(address_part) = url.split("://").nth(1) else {
        return false;
    };

    let host_port = address_part.split('/').next().unwrap_or(address_part);
    let socket_addr = match host_port.to_socket_addrs() {
        Ok(mut addrs) => addrs.find(|addr| matches!(addr, SocketAddr::V4(_) | SocketAddr::V6(_))),
        Err(_) => None,
    };

    let Some(socket_addr) = socket_addr else {
        return false;
    };

    std::net::TcpStream::connect_timeout(&socket_addr, std::time::Duration::from_millis(200))
        .is_ok()
}
