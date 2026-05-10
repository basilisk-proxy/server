use super::CacheProvider;
use std::sync::Mutex;

const DEFAULT_INTERNAL_NAMESPACE: &str = "basilisk:gateway:runtime";

/// Shared cache handle used by the gateway runtime and Lua cache primitive.
///
/// User-facing primitive operations use the raw keys provided by Lua code.
/// Internal gateway operations always store data under a reserved Basilisk
///  prefix, so proxy metadata never collides with operator-managed keys.
pub struct GatewayCache {
    provider: Mutex<Box<dyn CacheProvider>>,
    internal_namespace: Mutex<String>,
}

impl GatewayCache {
    /// Creates a shared cache backed by the selected provider.
    pub fn new(provider: &str) -> anyhow::Result<Self> {
        Ok(Self {
            provider: Mutex::new(<dyn CacheProvider>::try_new(provider)?),
            internal_namespace: Mutex::new(DEFAULT_INTERNAL_NAMESPACE.to_string()),
        })
    }

    /// Sets the internal key namespace used for Basilisk-owned cache entries.
    pub fn set_internal_namespace(&self, namespace: &str) -> anyhow::Result<()> {
        let normalized = normalize_namespace(namespace);
        let mut guard = self
            .internal_namespace
            .lock()
            .map_err(|_| anyhow::anyhow!("Gateway cache namespace lock poisoned"))?;
        *guard = normalized;
        Ok(())
    }

    /// Replaces the underlying cache provider in-place.
    pub fn reconfigure_provider(&self, provider: &str) -> anyhow::Result<()> {
        let mut guard = self
            .provider
            .lock()
            .map_err(|_| anyhow::anyhow!("Gateway cache lock poisoned"))?;
        *guard = <dyn CacheProvider>::try_new(provider)?;
        Ok(())
    }

    pub fn get(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.with_provider(|provider| provider.get(key))
    }

    pub fn get_or_set(&self, key: &str, value: String) -> anyhow::Result<String> {
        self.with_provider(|provider| provider.get_or_set(key, value))
    }

    pub fn set_with_ttl(&self, key: &str, value: String, ttl: u64) -> anyhow::Result<()> {
        self.with_provider(|provider| provider.set_with_ttl(key, value, ttl))
    }

    pub fn set_if_not_exists(&self, key: &str, value: String) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.set_if_not_exists(key, value))
    }

    pub fn set(&self, key: &str, value: String) -> anyhow::Result<()> {
        self.with_provider(|provider| provider.set(key, value))
    }

    pub fn incr(&self, key: &str, delta: i64) -> anyhow::Result<i64> {
        self.with_provider(|provider| provider.incr(key, delta))
    }

    pub fn decr(&self, key: &str, delta: i64) -> anyhow::Result<i64> {
        self.with_provider(|provider| provider.decr(key, delta))
    }

    pub fn list_append(&self, key: &str, value: String) -> anyhow::Result<String> {
        self.with_provider(|provider| provider.list_append(key, value))
    }

    pub fn list_prepend(&self, key: &str, value: String) -> anyhow::Result<String> {
        self.with_provider(|provider| provider.list_prepend(key, value))
    }

    pub fn list_pop_left(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.with_provider(|provider| provider.list_pop_left(key))
    }

    pub fn list_pop_right(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.with_provider(|provider| provider.list_pop_right(key))
    }

    pub fn list_length(&self, key: &str) -> anyhow::Result<usize> {
        self.with_provider(|provider| provider.list_length(key))
    }

    pub fn list_index(&self, key: &str, index: usize) -> anyhow::Result<Option<String>> {
        self.with_provider(|provider| provider.list_index(key, index))
    }

    pub fn list_range(&self, key: &str, start: usize, end: usize) -> anyhow::Result<Vec<String>> {
        self.with_provider(|provider| provider.list_range(key, start, end))
    }

    pub fn set_add(&self, key: &str, value: String) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.set_add(key, value))
    }

    pub fn set_remove(&self, key: &str, value: String) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.set_remove(key, value))
    }

    pub fn set_is_member(&self, key: &str, value: String) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.set_is_member(key, value))
    }

    pub fn set_members(&self, key: &str) -> anyhow::Result<Vec<String>> {
        self.with_provider(|provider| provider.set_members(key))
    }

    pub fn set_random_member(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.with_provider(|provider| provider.set_random_member(key))
    }

    pub fn set_sort(&self, key: &str) -> anyhow::Result<Vec<String>> {
        self.with_provider(|provider| provider.set_sort(key))
    }

    pub fn set_sort_with_options(&self, key: &str, options: String) -> anyhow::Result<Vec<String>> {
        self.with_provider(|provider| provider.set_sort_with_options(key, options))
    }

    pub fn set_sort_by_score(
        &self,
        key: &str,
        min: Option<f64>,
        max: Option<f64>,
    ) -> anyhow::Result<Vec<String>> {
        self.with_provider(|provider| provider.set_sort_by_score(key, min, max))
    }

    pub fn set_sort_by_score_with_options(
        &self,
        key: &str,
        min: Option<f64>,
        max: Option<f64>,
        options: String,
    ) -> anyhow::Result<Vec<String>> {
        self.with_provider(|provider| {
            provider.set_sort_by_score_with_options(key, min, max, options)
        })
    }

    pub fn set_card(&self, key: &str) -> anyhow::Result<usize> {
        self.with_provider(|provider| provider.set_card(key))
    }

    pub fn hash_get(&self, key: &str, field: &str) -> anyhow::Result<Option<String>> {
        self.with_provider(|provider| provider.hash_get(key, field))
    }

    pub fn hash_set(&self, key: &str, field: &str, value: String) -> anyhow::Result<()> {
        self.with_provider(|provider| provider.hash_set(key, field, value))
    }

    pub fn hash_delete(&self, key: &str, field: &str) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.hash_delete(key, field))
    }

    pub fn hash_exists(&self, key: &str, field: &str) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.hash_exists(key, field))
    }

    pub fn hash_fields(&self, key: &str) -> anyhow::Result<Vec<String>> {
        self.with_provider(|provider| provider.hash_fields(key))
    }

    pub fn hash_random_field(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.with_provider(|provider| provider.hash_random_field(key))
    }

    pub fn hash_length(&self, key: &str) -> anyhow::Result<usize> {
        self.with_provider(|provider| provider.hash_length(key))
    }

    pub fn keys(&self) -> anyhow::Result<Vec<String>> {
        let namespace = self.internal_namespace_prefix()?;
        self.with_provider(|provider| {
            provider
                .keys()
                .into_iter()
                .filter(|key| !key.starts_with(&namespace))
                .collect()
        })
    }

    pub fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.exists(key))
    }

    pub fn unlink(&self, key: &str) -> anyhow::Result<bool> {
        self.with_provider(|provider| provider.unlink(key))
    }

    pub fn expire(&self, key: &str, ttl: u64) -> anyhow::Result<()> {
        self.with_provider(|provider| provider.expire(key, ttl))
    }

    pub fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.with_provider(|provider| provider.delete(key))
    }

    pub fn internal_get(&self, key: &str) -> anyhow::Result<Option<String>> {
        self.get(&self.internal_key(key))
    }

    pub fn internal_incr(&self, key: &str, delta: i64) -> anyhow::Result<i64> {
        self.incr(&self.internal_key(key), delta)
    }

    pub fn internal_hash_get(&self, key: &str, field: &str) -> anyhow::Result<Option<String>> {
        self.hash_get(&self.internal_key(key), field)
    }

    pub fn internal_hash_set_with_ttl(
        &self,
        key: &str,
        field: &str,
        value: String,
        ttl_seconds: u64,
    ) -> anyhow::Result<()> {
        let internal_key = self.internal_key(key);
        self.with_provider(|provider| {
            provider.hash_set(&internal_key, field, value);
            provider.expire(&internal_key, ttl_seconds);
        })
    }

    fn internal_key(&self, key: &str) -> String {
        let namespace = self
            .internal_namespace_prefix()
            .unwrap_or_else(|_| format!("{DEFAULT_INTERNAL_NAMESPACE}:"));
        format!("{namespace}{key}")
    }

    fn internal_namespace_prefix(&self) -> anyhow::Result<String> {
        let guard = self
            .internal_namespace
            .lock()
            .map_err(|_| anyhow::anyhow!("Gateway cache namespace lock poisoned"))?;
        Ok(format!("{}:", guard.as_str()))
    }

    fn with_provider<R>(&self, f: impl FnOnce(&mut dyn CacheProvider) -> R) -> anyhow::Result<R> {
        let mut guard = self
            .provider
            .lock()
            .map_err(|_| anyhow::anyhow!("Gateway cache lock poisoned"))?;
        Ok(f(&mut **guard))
    }
}

fn normalize_namespace(namespace: &str) -> String {
    let trimmed = namespace.trim().trim_matches(':');
    if trimmed.is_empty() {
        DEFAULT_INTERNAL_NAMESPACE.to_string()
    } else {
        trimmed.to_string()
    }
}
