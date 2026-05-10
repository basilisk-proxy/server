use super::in_memory::InMemoryCacheManager;
use super::memcached::MemcachedCacheManager;
use super::redis::RedisCacheManager;
use anyhow::{anyhow, Result};

/// Provider-agnostic cache strategy surface.
///
/// Implementations may back these operations with in-process memory, Redis, or
/// Memcached. The trait intentionally groups several data-structure styles under
/// one strategy interface so callers can switch providers without changing call
/// sites.
pub trait CacheProvider: Send {
    /// Returns a string value for `key` when present and not expired.
    fn get(&self, key: &str) -> Option<String>;

    /// Returns the existing string value for `key`, or stores and returns `value`
    /// when the key is absent.
    fn get_or_set(&mut self, key: &str, value: String) -> String {
        if let Some(existing) = self.get(key) {
            return existing;
        }
        self.set(key, value.clone());
        value
    }

    /// Stores a string value with a time-to-live in seconds.
    fn set_with_ttl(&mut self, key: &str, value: String, ttl: u64);

    /// Stores a string value only when the key does not already exist.
    fn set_if_not_exists(&mut self, key: &str, value: String) -> bool;

    /// Stores a string value without expiration.
    fn set(&mut self, key: &str, value: String);

    /// Increments an integer-like string value by `delta` and returns the result.
    fn incr(&mut self, key: &str, delta: i64) -> i64;

    /// Decrements an integer-like string value by `delta` and returns the result.
    fn decr(&mut self, key: &str, delta: i64) -> i64;

    /// Appends a value to the tail of a cached list.
    fn list_append(&mut self, key: &str, value: String) -> String;

    /// Prepends a value to the head of a cached list.
    fn list_prepend(&mut self, key: &str, value: String) -> String;

    /// Removes and returns the first list item.
    fn list_pop_left(&mut self, key: &str) -> Option<String>;

    /// Removes and returns the last list item.
    fn list_pop_right(&mut self, key: &str) -> Option<String>;

    /// Returns the number of items in the cached list.
    fn list_length(&self, key: &str) -> usize;

    /// Returns the list item at `index` when present.
    fn list_index(&self, key: &str, index: usize) -> Option<String>;

    /// Returns the inclusive `[start, end]` range from the cached list.
    fn list_range(&self, key: &str, start: usize, end: usize) -> Vec<String>;

    /// Adds a member to a cached set.
    fn set_add(&mut self, key: &str, value: String) -> bool;

    /// Removes a member from a cached set.
    fn set_remove(&mut self, key: &str, value: String) -> bool;

    /// Returns whether `value` belongs to the cached set.
    fn set_is_member(&self, key: &str, value: String) -> bool;

    /// Returns all members of the cached set.
    fn set_members(&self, key: &str) -> Vec<String>;

    /// Returns one member from the cached set when available.
    fn set_random_member(&self, key: &str) -> Option<String>;

    /// Returns members sorted lexicographically.
    fn set_sort(&self, key: &str) -> Vec<String>;

    /// Returns members sorted with additional provider-agnostic options.
    fn set_sort_with_options(&self, key: &str, options: String) -> Vec<String>;

    /// Returns numeric set members sorted by score inside the optional range.
    fn set_sort_by_score(&self, key: &str, min: Option<f64>, max: Option<f64>) -> Vec<String>;

    /// Returns numeric set members sorted by score with extra options applied.
    fn set_sort_by_score_with_options(
        &self,
        key: &str,
        min: Option<f64>,
        max: Option<f64>,
        options: String,
    ) -> Vec<String> {
        apply_sort_options(self.set_sort_by_score(key, min, max), &options)
    }

    /// Returns the set cardinality.
    fn set_card(&self, key: &str) -> usize;

    /// Returns the hash field value when present.
    fn hash_get(&self, key: &str, field: &str) -> Option<String>;

    /// Stores a hash field value.
    fn hash_set(&mut self, key: &str, field: &str, value: String);

    /// Deletes a hash field when present.
    fn hash_delete(&mut self, key: &str, field: &str) -> bool;

    /// Returns whether a hash field exists.
    fn hash_exists(&self, key: &str, field: &str) -> bool;

    /// Returns all hash field names.
    fn hash_fields(&self, key: &str) -> Vec<String>;

    /// Returns one hash field name when present.
    fn hash_random_field(&self, key: &str) -> Option<String>;

    /// Returns the number of fields in the hash.
    fn hash_length(&self, key: &str) -> usize;

    /// Returns all visible keys known to the cache implementation.
    fn keys(&self) -> Vec<String>;

    /// Returns whether `key` exists and is not expired.
    fn exists(&self, key: &str) -> bool;

    /// Removes `key` and reports whether it existed.
    fn unlink(&mut self, key: &str) -> bool;

    /// Updates the TTL of an existing key.
    fn expire(&mut self, key: &str, ttl: u64);

    /// Deletes `key` without returning a status.
    fn delete(&mut self, key: &str);
}

impl dyn CacheProvider {
    /// Returns the supported cache provider names.
    pub fn supported_providers() -> &'static [&'static str] {
        &["memory", "redis", "memcached"]
    }

    /// Creates a cache strategy for the named provider.
    pub fn try_new(provider: &str) -> Result<Box<dyn CacheProvider>> {
        match provider {
            "redis" => Ok(Box::new(RedisCacheManager::new())),
            "memcached" => Ok(Box::new(MemcachedCacheManager::new())),
            "memory" => Ok(Box::new(InMemoryCacheManager::new())),
            _ => Err(anyhow!(
                "Unsupported cache provider: {}. Supported providers: {}",
                provider,
                Self::supported_providers().join(", ")
            )),
        }
    }

    /// Creates a cache strategy for the named provider.
    ///
    /// Supported values are `memory`, `redis`, and `memcached`.
    pub fn new(provider: &str) -> Box<dyn CacheProvider> {
        Self::try_new(provider).unwrap_or_else(|err| panic!("{err}"))
    }
}
pub(super) fn apply_sort_options(mut values: Vec<String>, options: &str) -> Vec<String> {
    if options.to_ascii_uppercase().contains("DESC") {
        values.reverse();
    }

    let tokens = options.split_whitespace().collect::<Vec<_>>();
    if let Some(index) = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("LIMIT"))
    {
        if let (Some(offset), Some(count)) = (
            tokens
                .get(index + 1)
                .and_then(|value| value.parse::<usize>().ok()),
            tokens
                .get(index + 2)
                .and_then(|value| value.parse::<usize>().ok()),
        ) {
            values = values.into_iter().skip(offset).take(count).collect();
        }
    }

    values
}

pub(super) fn sort_by_score_range(
    values: Vec<String>,
    min: Option<f64>,
    max: Option<f64>,
) -> Vec<String> {
    let mut scored = values
        .into_iter()
        .filter_map(|item| item.parse::<f64>().ok().map(|score| (item, score)))
        .filter(|(_, score)| min.is_none_or(|m| *score >= m) && max.is_none_or(|m| *score <= m))
        .collect::<Vec<_>>();
    scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().map(|(item, _)| item).collect()
}

#[cfg(test)]
mod tests {
    use super::CacheProvider;
    use std::thread;
    use std::time::Duration;

    fn exercise_cache(cache: &mut Box<dyn CacheProvider>) {
        assert_eq!(cache.get("missing"), None);
        assert_eq!(cache.get_or_set("greeting", "hello".to_string()), "hello");
        assert_eq!(cache.get("greeting").as_deref(), Some("hello"));
        assert!(!cache.set_if_not_exists("greeting", "ignored".to_string()));
        cache.set("counter", "10".to_string());
        assert_eq!(cache.incr("counter", 5), 15);
        assert_eq!(cache.decr("counter", 3), 12);

        cache.set_with_ttl("ttl", "bye".to_string(), 0);
        assert_eq!(cache.get("ttl"), None);
        cache.set("ephemeral", "soon-gone".to_string());
        cache.expire("ephemeral", 0);
        assert!(!cache.exists("ephemeral"));

        cache.list_append("jobs", "b".to_string());
        cache.list_prepend("jobs", "a".to_string());
        cache.list_append("jobs", "c".to_string());
        assert_eq!(cache.list_length("jobs"), 3);
        assert_eq!(cache.list_index("jobs", 1).as_deref(), Some("b"));
        assert_eq!(cache.list_range("jobs", 0, 1), vec!["a", "b"]);
        assert_eq!(cache.list_pop_left("jobs").as_deref(), Some("a"));
        assert_eq!(cache.list_pop_right("jobs").as_deref(), Some("c"));

        assert!(cache.set_add("tags", "3".to_string()));
        assert!(cache.set_add("tags", "1".to_string()));
        assert!(cache.set_add("tags", "2".to_string()));
        assert!(!cache.set_add("tags", "2".to_string()));
        assert!(cache.set_is_member("tags", "1".to_string()));
        assert_eq!(cache.set_members("tags"), vec!["1", "2", "3"]);
        assert_eq!(cache.set_random_member("tags").as_deref(), Some("1"));
        assert_eq!(cache.set_sort("tags"), vec!["1", "2", "3"]);
        assert_eq!(
            cache.set_sort_with_options("tags", "DESC LIMIT 0 2".to_string()),
            vec!["3", "2"]
        );
        assert_eq!(
            cache.set_sort_by_score("tags", Some(2.0), Some(3.0)),
            vec!["2", "3"]
        );
        assert_eq!(
            cache.set_sort_by_score_with_options("tags", None, None, "DESC LIMIT 1 1".to_string()),
            vec!["2"]
        );
        assert_eq!(cache.set_card("tags"), 3);
        assert!(cache.set_remove("tags", "2".to_string()));

        cache.hash_set("user:1", "name", "Basil".to_string());
        cache.hash_set("user:1", "role", "admin".to_string());
        assert_eq!(cache.hash_get("user:1", "name").as_deref(), Some("Basil"));
        assert!(cache.hash_exists("user:1", "role"));
        assert_eq!(cache.hash_fields("user:1"), vec!["name", "role"]);
        assert_eq!(cache.hash_random_field("user:1").as_deref(), Some("name"));
        assert_eq!(cache.hash_length("user:1"), 2);
        assert!(cache.hash_delete("user:1", "role"));

        let keys = cache.keys();
        assert!(keys.contains(&"greeting".to_string()));
        assert!(keys.contains(&"counter".to_string()));
        assert!(cache.exists("greeting"));
        assert!(cache.unlink("greeting"));
        cache.delete("counter");
        assert!(!cache.exists("counter"));
    }

    #[test]
    fn memory_cache_manager_implements_full_trait_surface() {
        let mut cache = <dyn CacheProvider>::new("memory");
        exercise_cache(&mut cache);
    }

    #[test]
    fn cache_factory_supports_all_configured_providers() {
        let _redis = <dyn CacheProvider>::new("redis");
        let _memcached = <dyn CacheProvider>::new("memcached");
        let _memory = <dyn CacheProvider>::new("memory");
    }

    #[test]
    fn ttl_expiration_hides_entries_after_elapsed_time() {
        let mut cache = <dyn CacheProvider>::new("memory");
        cache.set_with_ttl("session", "abc".to_string(), 1);
        assert_eq!(cache.get("session").as_deref(), Some("abc"));
        thread::sleep(Duration::from_millis(1100));
        assert_eq!(cache.get("session"), None);
    }

    #[test]
    fn redis_and_memcached_tests_can_be_enabled_explicitly() {
        if std::env::var("BASILISK_TEST_REDIS_URL").is_ok() {
            let mut redis_cache = <dyn CacheProvider>::new("redis");
            redis_cache.set("basilisk:test:redis", "ok".to_string());
        }

        if std::env::var("BASILISK_TEST_MEMCACHED_URL").is_ok() {
            let mut memcached_cache = <dyn CacheProvider>::new("memcached");
            memcached_cache.set("basilisk:test:memcached", "ok".to_string());
        }
    }
}
