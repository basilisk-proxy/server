use super::base::{CacheProvider, apply_sort_options, sort_by_score_range};
use redis::{Client, Commands};

pub(super) struct RedisCacheManager {
    client: Option<Client>,
}

impl RedisCacheManager {
    pub(super) fn new() -> Self {
        let url =
            std::env::var("BASILISK_REDIS_URL").unwrap_or("redis://127.0.0.1:6379/".to_string());
        Self {
            client: Client::open(url).ok(),
        }
    }

    fn with_connection<T>(
        &self,
        f: impl FnOnce(&mut redis::Connection) -> redis::RedisResult<T>,
    ) -> Option<T> {
        let client = self.client.as_ref()?;
        let mut connection = client.get_connection().ok()?;
        f(&mut connection).ok()
    }

    fn set_members_internal(&self, key: &str) -> Vec<String> {
        self.with_connection(|connection| connection.smembers(key))
            .unwrap_or_default()
    }

    fn hash_fields_internal(&self, key: &str) -> Vec<String> {
        self.with_connection(|connection| connection.hkeys(key))
            .unwrap_or_default()
    }
}

impl CacheProvider for RedisCacheManager {
    fn get(&self, key: &str) -> Option<String> {
        self.with_connection(|connection| connection.get(key))
            .flatten()
    }

    fn set_with_ttl(&mut self, key: &str, value: String, ttl: u64) {
        let _ = self.with_connection(|connection| connection.set_ex::<_, _, ()>(key, value, ttl));
    }

    fn set_if_not_exists(&mut self, key: &str, value: String) -> bool {
        self.with_connection(|connection| connection.set_nx(key, value))
            .unwrap_or(false)
    }

    fn set(&mut self, key: &str, value: String) {
        let _ = self.with_connection(|connection| connection.set::<_, _, ()>(key, value));
    }

    fn incr(&mut self, key: &str, delta: i64) -> i64 {
        self.with_connection(|connection| connection.incr(key, delta))
            .unwrap_or(0)
    }

    fn decr(&mut self, key: &str, delta: i64) -> i64 {
        self.with_connection(|connection| {
            redis::cmd("DECRBY").arg(key).arg(delta).query(connection)
        })
        .unwrap_or(0)
    }

    fn list_append(&mut self, key: &str, value: String) -> String {
        let _ =
            self.with_connection(|connection| connection.rpush::<_, _, usize>(key, value.clone()));
        value
    }

    fn list_prepend(&mut self, key: &str, value: String) -> String {
        let _ =
            self.with_connection(|connection| connection.lpush::<_, _, usize>(key, value.clone()));
        value
    }

    fn list_pop_left(&mut self, key: &str) -> Option<String> {
        self.with_connection(|connection| redis::cmd("LPOP").arg(key).query(connection))
            .flatten()
    }

    fn list_pop_right(&mut self, key: &str) -> Option<String> {
        self.with_connection(|connection| redis::cmd("RPOP").arg(key).query(connection))
            .flatten()
    }

    fn list_length(&self, key: &str) -> usize {
        self.with_connection(|connection| connection.llen(key))
            .unwrap_or(0)
    }

    fn list_index(&self, key: &str, index: usize) -> Option<String> {
        self.with_connection(|connection| connection.lindex(key, index as isize))
            .flatten()
    }

    fn list_range(&self, key: &str, start: usize, end: usize) -> Vec<String> {
        self.with_connection(|connection| connection.lrange(key, start as isize, end as isize))
            .unwrap_or_default()
    }

    fn set_add(&mut self, key: &str, value: String) -> bool {
        self.with_connection(|connection| connection.sadd(key, value))
            .unwrap_or(false)
    }

    fn set_remove(&mut self, key: &str, value: String) -> bool {
        self.with_connection(|connection| connection.srem(key, value))
            .unwrap_or(false)
    }

    fn set_is_member(&self, key: &str, value: String) -> bool {
        self.with_connection(|connection| connection.sismember(key, value))
            .unwrap_or(false)
    }

    fn set_members(&self, key: &str) -> Vec<String> {
        self.set_members_internal(key)
    }

    fn set_random_member(&self, key: &str) -> Option<String> {
        self.with_connection(|connection| redis::cmd("SRANDMEMBER").arg(key).query(connection))
            .flatten()
    }

    fn set_sort(&self, key: &str) -> Vec<String> {
        let mut values = self.set_members_internal(key);
        values.sort();
        values
    }

    fn set_sort_with_options(&self, key: &str, options: String) -> Vec<String> {
        apply_sort_options(self.set_sort(key), &options)
    }

    fn set_sort_by_score(&self, key: &str, min: Option<f64>, max: Option<f64>) -> Vec<String> {
        sort_by_score_range(self.set_members_internal(key), min, max)
    }

    fn set_card(&self, key: &str) -> usize {
        self.with_connection(|connection| connection.scard(key))
            .unwrap_or(0)
    }

    fn hash_get(&self, key: &str, field: &str) -> Option<String> {
        self.with_connection(|connection| connection.hget(key, field))
            .flatten()
    }

    fn hash_set(&mut self, key: &str, field: &str, value: String) {
        let _ =
            self.with_connection(|connection| connection.hset::<_, _, _, usize>(key, field, value));
    }

    fn hash_delete(&mut self, key: &str, field: &str) -> bool {
        self.with_connection(|connection| connection.hdel(key, field))
            .unwrap_or(false)
    }

    fn hash_exists(&self, key: &str, field: &str) -> bool {
        self.with_connection(|connection| connection.hexists(key, field))
            .unwrap_or(false)
    }

    fn hash_fields(&self, key: &str) -> Vec<String> {
        self.hash_fields_internal(key)
    }

    fn hash_random_field(&self, key: &str) -> Option<String> {
        self.hash_fields_internal(key).into_iter().next()
    }

    fn hash_length(&self, key: &str) -> usize {
        self.with_connection(|connection| connection.hlen(key))
            .unwrap_or(0)
    }

    fn keys(&self) -> Vec<String> {
        let mut keys = self
            .with_connection(|connection| connection.keys::<_, Vec<String>>("*"))
            .unwrap_or_default();
        keys.sort();
        keys
    }

    fn exists(&self, key: &str) -> bool {
        self.with_connection(|connection| connection.exists(key))
            .unwrap_or(false)
    }

    fn unlink(&mut self, key: &str) -> bool {
        self.with_connection(|connection| redis::cmd("UNLINK").arg(key).query::<usize>(connection))
            .map(|count| count > 0)
            .unwrap_or(false)
    }

    fn expire(&mut self, key: &str, ttl: u64) {
        let _ = self.with_connection(|connection| connection.expire::<_, bool>(key, ttl as i64));
    }

    fn delete(&mut self, key: &str) {
        let _ = self.with_connection(|connection| connection.del::<_, usize>(key));
    }
}
