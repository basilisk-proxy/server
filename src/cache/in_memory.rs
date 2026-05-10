use super::base::{apply_sort_options, sort_by_score_range, CacheProvider};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct CacheEntry {
    value: CacheValue,
    expires_at: Option<Instant>,
}

#[derive(Clone)]
enum CacheValue {
    String(String),
    List(VecDeque<String>),
    Set(BTreeSet<String>),
    Hash(BTreeMap<String, String>),
}

#[derive(Default)]
pub(super) struct InMemoryCacheManager {
    entries: HashMap<String, CacheEntry>,
}

impl InMemoryCacheManager {
    pub(super) fn new() -> Self {
        Self::default()
    }
}

fn is_expired(entry: &CacheEntry, now: Instant) -> bool {
    entry.expires_at.is_some_and(|expires_at| now >= expires_at)
}

impl InMemoryCacheManager {
    fn get_string_value(&self, key: &str) -> Option<&str> {
        match &self.entry_ref(key)?.value {
            CacheValue::String(value) => Some(value.as_str()),
            _ => None,
        }
    }

    fn entry_ref(&self, key: &str) -> Option<&CacheEntry> {
        let entry = self.entries.get(key)?;
        if is_expired(entry, Instant::now()) {
            return None;
        }
        Some(entry)
    }

    fn purge_if_expired(&mut self, key: &str) {
        let expired = self
            .entries
            .get(key)
            .map(|entry| is_expired(entry, Instant::now()))
            .unwrap_or(false);
        if expired {
            self.entries.remove(key);
        }
    }

    fn ensure_list(&mut self, key: &str) -> &mut VecDeque<String> {
        self.purge_if_expired(key);
        let entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(|| CacheEntry {
                value: CacheValue::List(VecDeque::new()),
                expires_at: None,
            });
        if !matches!(entry.value, CacheValue::List(_)) {
            entry.value = CacheValue::List(VecDeque::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            CacheValue::List(list) => list,
            _ => unreachable!(),
        }
    }

    fn list_ref(&self, key: &str) -> Option<&VecDeque<String>> {
        match &self.entry_ref(key)?.value {
            CacheValue::List(list) => Some(list),
            _ => None,
        }
    }

    fn list_mut(&mut self, key: &str) -> Option<&mut VecDeque<String>> {
        match self.entries.get_mut(key) {
            Some(entry) if matches!(entry.value, CacheValue::List(_)) => match &mut entry.value {
                CacheValue::List(list) => Some(list),
                _ => None,
            },
            _ => None,
        }
    }

    fn ensure_set(&mut self, key: &str) -> &mut BTreeSet<String> {
        self.purge_if_expired(key);
        let entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(|| CacheEntry {
                value: CacheValue::Set(BTreeSet::new()),
                expires_at: None,
            });
        if !matches!(entry.value, CacheValue::Set(_)) {
            entry.value = CacheValue::Set(BTreeSet::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            CacheValue::Set(set) => set,
            _ => unreachable!(),
        }
    }

    fn set_ref(&self, key: &str) -> Option<&BTreeSet<String>> {
        match &self.entry_ref(key)?.value {
            CacheValue::Set(set) => Some(set),
            _ => None,
        }
    }

    fn set_mut(&mut self, key: &str) -> Option<&mut BTreeSet<String>> {
        match self.entries.get_mut(key) {
            Some(entry) if matches!(entry.value, CacheValue::Set(_)) => match &mut entry.value {
                CacheValue::Set(set) => Some(set),
                _ => None,
            },
            _ => None,
        }
    }

    fn ensure_hash(&mut self, key: &str) -> &mut BTreeMap<String, String> {
        self.purge_if_expired(key);
        let entry = self
            .entries
            .entry(key.to_string())
            .or_insert_with(|| CacheEntry {
                value: CacheValue::Hash(BTreeMap::new()),
                expires_at: None,
            });
        if !matches!(entry.value, CacheValue::Hash(_)) {
            entry.value = CacheValue::Hash(BTreeMap::new());
            entry.expires_at = None;
        }
        match &mut entry.value {
            CacheValue::Hash(hash) => hash,
            _ => unreachable!(),
        }
    }

    fn hash_ref(&self, key: &str) -> Option<&BTreeMap<String, String>> {
        match &self.entry_ref(key)?.value {
            CacheValue::Hash(hash) => Some(hash),
            _ => None,
        }
    }

    fn hash_mut(&mut self, key: &str) -> Option<&mut BTreeMap<String, String>> {
        match self.entries.get_mut(key) {
            Some(entry) if matches!(entry.value, CacheValue::Hash(_)) => match &mut entry.value {
                CacheValue::Hash(hash) => Some(hash),
                _ => None,
            },
            _ => None,
        }
    }
}

impl CacheProvider for InMemoryCacheManager {
    fn get(&self, key: &str) -> Option<String> {
        self.get_string_value(key).map(str::to_string)
    }

    fn set_with_ttl(&mut self, key: &str, value: String, ttl: u64) {
        self.entries.insert(
            key.to_string(),
            CacheEntry {
                value: CacheValue::String(value),
                expires_at: Some(Instant::now() + Duration::from_secs(ttl)),
            },
        );
    }

    fn set_if_not_exists(&mut self, key: &str, value: String) -> bool {
        self.purge_if_expired(key);
        if self.entries.contains_key(key) {
            return false;
        }
        self.set(key, value);
        true
    }

    fn set(&mut self, key: &str, value: String) {
        self.entries.insert(
            key.to_string(),
            CacheEntry {
                value: CacheValue::String(value),
                expires_at: None,
            },
        );
    }

    fn incr(&mut self, key: &str, delta: i64) -> i64 {
        let next = self
            .get_string_value(key)
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0)
            + delta;
        self.set(key, next.to_string());
        next
    }

    fn decr(&mut self, key: &str, delta: i64) -> i64 {
        self.incr(key, -delta)
    }

    fn list_append(&mut self, key: &str, value: String) -> String {
        self.ensure_list(key).push_back(value.clone());
        value
    }

    fn list_prepend(&mut self, key: &str, value: String) -> String {
        self.ensure_list(key).push_front(value.clone());
        value
    }

    fn list_pop_left(&mut self, key: &str) -> Option<String> {
        self.purge_if_expired(key);
        self.list_mut(key).and_then(VecDeque::pop_front)
    }

    fn list_pop_right(&mut self, key: &str) -> Option<String> {
        self.purge_if_expired(key);
        self.list_mut(key).and_then(VecDeque::pop_back)
    }

    fn list_length(&self, key: &str) -> usize {
        self.list_ref(key).map_or(0, VecDeque::len)
    }

    fn list_index(&self, key: &str, index: usize) -> Option<String> {
        self.list_ref(key).and_then(|list| list.get(index).cloned())
    }

    fn list_range(&self, key: &str, start: usize, end: usize) -> Vec<String> {
        if end < start {
            return Vec::new();
        }

        self.list_ref(key)
            .map(|list| {
                list.iter()
                    .skip(start)
                    .take(end - start + 1)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn set_add(&mut self, key: &str, value: String) -> bool {
        self.ensure_set(key).insert(value)
    }

    fn set_remove(&mut self, key: &str, value: String) -> bool {
        self.purge_if_expired(key);
        self.set_mut(key).is_some_and(|set| set.remove(&value))
    }

    fn set_is_member(&self, key: &str, value: String) -> bool {
        self.set_ref(key).is_some_and(|set| set.contains(&value))
    }

    fn set_members(&self, key: &str) -> Vec<String> {
        self.set_ref(key)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn set_random_member(&self, key: &str) -> Option<String> {
        self.set_ref(key).and_then(|set| set.iter().next().cloned())
    }

    fn set_sort(&self, key: &str) -> Vec<String> {
        self.set_members(key)
    }

    fn set_sort_with_options(&self, key: &str, options: String) -> Vec<String> {
        apply_sort_options(self.set_members(key), &options)
    }

    fn set_sort_by_score(&self, key: &str, min: Option<f64>, max: Option<f64>) -> Vec<String> {
        sort_by_score_range(self.set_members(key), min, max)
    }

    fn set_card(&self, key: &str) -> usize {
        self.set_ref(key).map_or(0, BTreeSet::len)
    }

    fn hash_get(&self, key: &str, field: &str) -> Option<String> {
        self.hash_ref(key).and_then(|hash| hash.get(field).cloned())
    }

    fn hash_set(&mut self, key: &str, field: &str, value: String) {
        self.ensure_hash(key).insert(field.to_string(), value);
    }

    fn hash_delete(&mut self, key: &str, field: &str) -> bool {
        self.purge_if_expired(key);
        self.hash_mut(key)
            .is_some_and(|hash| hash.remove(field).is_some())
    }

    fn hash_exists(&self, key: &str, field: &str) -> bool {
        self.hash_ref(key)
            .is_some_and(|hash| hash.contains_key(field))
    }

    fn hash_fields(&self, key: &str) -> Vec<String> {
        self.hash_ref(key)
            .map(|hash| hash.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn hash_random_field(&self, key: &str) -> Option<String> {
        self.hash_ref(key)
            .and_then(|hash| hash.keys().next().cloned())
    }

    fn hash_length(&self, key: &str) -> usize {
        self.hash_ref(key).map_or(0, BTreeMap::len)
    }

    fn keys(&self) -> Vec<String> {
        let now = Instant::now();
        let mut keys = self
            .entries
            .iter()
            .filter(|(_, entry)| !is_expired(entry, now))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    fn exists(&self, key: &str) -> bool {
        self.entry_ref(key).is_some()
    }

    fn unlink(&mut self, key: &str) -> bool {
        self.entries.remove(key).is_some()
    }

    fn expire(&mut self, key: &str, ttl: u64) {
        self.purge_if_expired(key);
        if let Some(entry) = self.entries.get_mut(key) {
            entry.expires_at = Some(Instant::now() + Duration::from_secs(ttl));
        }
    }

    fn delete(&mut self, key: &str) {
        self.entries.remove(key);
    }
}
