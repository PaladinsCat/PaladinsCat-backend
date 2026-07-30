use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use redis::{AsyncCommands, aio::ConnectionManager};
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct RedisCache {
    client: redis::Client,
    manager: Arc<Mutex<Option<ConnectionManager>>>,
    command_timeout: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RateLimitResult {
    pub remaining: u64,
    pub total: u64,
    pub reset_at_ms: u64,
    pub allowed: bool,
    pub backend_available: bool,
}

impl RedisCache {
    pub fn new(redis_url: &str) -> Result<Self, redis::RedisError> {
        Ok(Self {
            client: redis::Client::open(redis_url)?,
            manager: Arc::new(Mutex::new(None)),
            command_timeout: Duration::from_secs(2),
        })
    }

    async fn connection(&self) -> Option<ConnectionManager> {
        if let Some(connection) = self.manager.lock().await.as_ref().cloned() {
            return Some(connection);
        }
        let connection =
            tokio::time::timeout(self.command_timeout, self.client.get_connection_manager())
                .await
                .ok()?
                .ok()?;
        *self.manager.lock().await = Some(connection.clone());
        Some(connection)
    }

    async fn mark_failed(&self) {
        *self.manager.lock().await = None;
    }

    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, self.connection())
            .await
            .ok()
            .flatten()
            .is_some()
    }

    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        let mut connection = self.connection().await?;
        let result: redis::RedisResult<Option<String>> =
            match tokio::time::timeout(self.command_timeout, connection.get(key)).await {
                Ok(result) => result,
                Err(_) => {
                    self.mark_failed().await;
                    return None;
                }
            };
        let value = match result {
            Ok(Some(value)) => value,
            Ok(None) => return None,
            Err(_) => {
                self.mark_failed().await;
                return None;
            }
        };
        let parsed: serde_json::Value = match serde_json::from_str(&value) {
            Ok(value) => value,
            Err(_) => {
                let _ = self.del(key).await;
                return None;
            }
        };
        if parsed.as_object().is_some_and(|object| {
            object.len() == 1 && object.get("v").is_some_and(|value| value.is_null())
        }) {
            return None;
        }
        match serde_json::from_value(parsed) {
            Ok(value) => Some(value),
            Err(_) => {
                let _ = self.del(key).await;
                None
            }
        }
    }

    pub async fn set<T: Serialize>(&self, key: &str, value: &T, ttl_seconds: Option<u64>) {
        let _ = self.set_required(key, value, ttl_seconds).await;
    }

    pub async fn set_required<T: Serialize>(
        &self,
        key: &str,
        value: &T,
        ttl_seconds: Option<u64>,
    ) -> bool {
        let Ok(mut serialized) = serde_json::to_string(value) else {
            return false;
        };
        if serialized == "null" {
            serialized = r#"{"v":null}"#.to_owned();
        }
        let Some(mut connection) = self.connection().await else {
            return false;
        };
        let result: redis::RedisResult<()> = match ttl_seconds {
            Some(ttl) => {
                match tokio::time::timeout(
                    self.command_timeout,
                    connection.set_ex(key, serialized, ttl),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        self.mark_failed().await;
                        return false;
                    }
                }
            }
            None => {
                match tokio::time::timeout(self.command_timeout, connection.set(key, serialized))
                    .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        self.mark_failed().await;
                        return false;
                    }
                }
            }
        };
        if result.is_err() {
            self.mark_failed().await;
            return false;
        }
        true
    }

    /// Acquire a cross-process lease using the same `SET PX NX` contract as
    /// the TypeScript route cache.
    ///
    /// `None` means Redis was unavailable and the caller should fail open.
    /// `Some(false)` means another process owns the lease.
    pub async fn acquire_lease(&self, key: &str, token: &str, ttl_ms: u64) -> Option<bool> {
        let mut connection = self.connection().await?;
        let result: redis::RedisResult<Option<String>> = match tokio::time::timeout(
            self.command_timeout,
            redis::cmd("SET")
                .arg(key)
                .arg(token)
                .arg("PX")
                .arg(ttl_ms)
                .arg("NX")
                .query_async(&mut connection),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.mark_failed().await;
                return None;
            }
        };
        match result {
            Ok(value) => Some(value.as_deref() == Some("OK")),
            Err(_) => {
                self.mark_failed().await;
                None
            }
        }
    }

    pub async fn release_lease(&self, key: &str, token: &str) {
        let Some(mut connection) = self.connection().await else {
            return;
        };
        let result: redis::RedisResult<i64> = match tokio::time::timeout(
            self.command_timeout,
            redis::Script::new(
                "if redis.call('get', KEYS[1]) == ARGV[1] then \
                 return redis.call('del', KEYS[1]) else return 0 end",
            )
            .key(key)
            .arg(token)
            .invoke_async(&mut connection),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.mark_failed().await;
                return;
            }
        };
        if result.is_err() {
            self.mark_failed().await;
        }
    }

    pub async fn del(&self, key: &str) -> u64 {
        let Some(mut connection) = self.connection().await else {
            return 0;
        };
        let result: redis::RedisResult<u64> =
            match tokio::time::timeout(self.command_timeout, connection.del(key)).await {
                Ok(result) => result,
                Err(_) => {
                    self.mark_failed().await;
                    return 0;
                }
            };
        match result {
            Ok(value) => value,
            Err(_) => {
                self.mark_failed().await;
                0
            }
        }
    }

    pub async fn exists(&self, key: &str) -> bool {
        let Some(mut connection) = self.connection().await else {
            return false;
        };
        let result: redis::RedisResult<bool> =
            match tokio::time::timeout(self.command_timeout, connection.exists(key)).await {
                Ok(result) => result,
                Err(_) => {
                    self.mark_failed().await;
                    return false;
                }
            };
        match result {
            Ok(value) => value,
            Err(_) => {
                self.mark_failed().await;
                false
            }
        }
    }

    pub async fn incr(&self, key: &str) -> i64 {
        let Some(mut connection) = self.connection().await else {
            return 0;
        };
        let result: redis::RedisResult<i64> =
            match tokio::time::timeout(self.command_timeout, connection.incr(key, 1)).await {
                Ok(result) => result,
                Err(_) => {
                    self.mark_failed().await;
                    return 0;
                }
            };
        match result {
            Ok(value) => value,
            Err(_) => {
                self.mark_failed().await;
                0
            }
        }
    }

    pub async fn incr_get(&self, key: &str) -> Option<i64> {
        let mut connection = self.connection().await?;
        let result: redis::RedisResult<Option<String>> =
            match tokio::time::timeout(self.command_timeout, connection.get(key)).await {
                Ok(result) => result,
                Err(_) => {
                    self.mark_failed().await;
                    return None;
                }
            };
        match result {
            Ok(Some(value)) => value.parse().ok(),
            Ok(None) => None,
            Err(_) => {
                self.mark_failed().await;
                None
            }
        }
    }

    pub async fn check_rate_limit(
        &self,
        key: &str,
        limit: u64,
        window_ms: u64,
        fail_open: bool,
    ) -> RateLimitResult {
        self.check_rate_limit_at(key, limit, window_ms, fail_open, unix_time_ms())
            .await
    }

    async fn check_rate_limit_at(
        &self,
        key: &str,
        limit: u64,
        window_ms: u64,
        fail_open: bool,
        now_ms: u64,
    ) -> RateLimitResult {
        let unavailable = || RateLimitResult {
            remaining: if fail_open { limit } else { 0 },
            total: limit,
            reset_at_ms: now_ms.saturating_add(window_ms),
            allowed: fail_open,
            backend_available: false,
        };
        if key.trim().is_empty() || limit == 0 || window_ms == 0 {
            return unavailable();
        }

        let Some(mut connection) = self.connection().await else {
            return unavailable();
        };
        let window_key = format!("rl:{key}");
        let ttl_seconds = window_ms.div_ceil(1_000).saturating_add(1);
        let result: redis::RedisResult<(i64, i64)> = match tokio::time::timeout(
            self.command_timeout,
            redis::Script::new(
                "local current = redis.call('INCR', KEYS[1]); \
                 local ttl = redis.call('PTTL', KEYS[1]); \
                 if ttl == -1 then redis.call('EXPIRE', KEYS[1], ARGV[1]); end; \
                 return {current, ttl}",
            )
            .key(window_key)
            .arg(ttl_seconds)
            .invoke_async(&mut connection),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.mark_failed().await;
                return unavailable();
            }
        };
        let (current, current_ttl) = match result {
            Ok(value) => value,
            Err(_) => {
                self.mark_failed().await;
                return unavailable();
            }
        };
        let actual_ttl = if current_ttl > 0 {
            current_ttl as u64
        } else {
            window_ms
        };
        let current = u64::try_from(current).unwrap_or(u64::MAX);
        RateLimitResult {
            remaining: limit.saturating_sub(current),
            total: limit,
            reset_at_ms: now_ms.saturating_add(actual_ttl),
            allowed: current <= limit,
            backend_available: true,
        }
    }

    pub async fn health_check(&self) -> bool {
        let Some(mut connection) = self.connection().await else {
            return false;
        };
        let result: redis::RedisResult<String> =
            match tokio::time::timeout(self.command_timeout, connection.ping()).await {
                Ok(result) => result,
                Err(_) => {
                    self.mark_failed().await;
                    return false;
                }
            };
        matches!(result.as_deref(), Ok("PONG"))
    }

    pub async fn close(&self) {
        *self.manager.lock().await = None;
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unavailable_redis_fails_open_without_blocking() {
        let cache = RedisCache::new("redis://127.0.0.1:9").expect("cache");
        let started = std::time::Instant::now();
        assert_eq!(cache.get::<serde_json::Value>("missing").await, None);
        assert!(!cache.exists("missing").await);
        assert_eq!(cache.del("missing").await, 0);
        assert_eq!(cache.incr("counter").await, 0);
        assert!(!cache.health_check().await);
        assert!(started.elapsed() < Duration::from_secs(11));
    }

    #[tokio::test]
    async fn invalid_rate_limit_contract_obeys_selected_failure_mode() {
        let cache = RedisCache::new("redis://127.0.0.1:9").expect("cache");
        let open = cache.check_rate_limit_at("", 10, 60_000, true, 1_000).await;
        assert_eq!(
            open,
            RateLimitResult {
                remaining: 10,
                total: 10,
                reset_at_ms: 61_000,
                allowed: true,
                backend_available: false,
            }
        );
        let closed = cache
            .check_rate_limit_at("scope", 0, 60_000, false, 1_000)
            .await;
        assert_eq!(
            closed,
            RateLimitResult {
                remaining: 0,
                total: 0,
                reset_at_ms: 61_000,
                allowed: false,
                backend_available: false,
            }
        );
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_REDIS_URL"]
    async fn live_redis_matches_node_cache_contract() {
        let redis_url =
            std::env::var("PALADINSCAT_TEST_REDIS_URL").expect("PALADINSCAT_TEST_REDIS_URL");
        let cache = RedisCache::new(&redis_url).expect("cache");
        assert!(cache.wait_ready(Duration::from_secs(5)).await);

        let prefix = format!("rust-core-test:{}", uuid::Uuid::new_v4());
        let object_key = format!("{prefix}:object");
        let null_key = format!("{prefix}:null");
        let counter_key = format!("{prefix}:counter");
        let corrupt_key = format!("{prefix}:corrupt");
        let limiter_key = format!("{prefix}:limit");

        cache
            .set(
                &object_key,
                &serde_json::json!({"operation": "getmatchdetailsbatch"}),
                Some(60),
            )
            .await;
        assert_eq!(
            cache.get::<serde_json::Value>(&object_key).await,
            Some(serde_json::json!({"operation": "getmatchdetailsbatch"}))
        );
        assert!(cache.exists(&object_key).await);

        cache
            .set(&null_key, &serde_json::Value::Null, Some(60))
            .await;
        assert_eq!(cache.get::<serde_json::Value>(&null_key).await, None);

        assert_eq!(cache.incr(&counter_key).await, 1);
        assert_eq!(cache.incr(&counter_key).await, 2);
        assert_eq!(cache.incr_get(&counter_key).await, Some(2));

        let first = cache
            .check_rate_limit_at(&limiter_key, 2, 60_000, false, 1_000)
            .await;
        let second = cache
            .check_rate_limit_at(&limiter_key, 2, 60_000, false, 1_100)
            .await;
        let blocked = cache
            .check_rate_limit_at(&limiter_key, 2, 60_000, false, 1_200)
            .await;
        assert_eq!(first.remaining, 1);
        assert!(first.allowed);
        assert_eq!(second.remaining, 0);
        assert!(second.allowed);
        assert_eq!(blocked.remaining, 0);
        assert!(!blocked.allowed);

        let mut connection = cache.connection().await.expect("connection");
        let _: () = redis::cmd("SET")
            .arg(&corrupt_key)
            .arg("{not-json")
            .query_async(&mut connection)
            .await
            .expect("seed corrupt JSON");
        assert_eq!(cache.get::<serde_json::Value>(&corrupt_key).await, None);
        assert!(!cache.exists(&corrupt_key).await);

        for key in [&object_key, &null_key, &counter_key, &corrupt_key] {
            let _ = cache.del(key).await;
        }
        let _ = cache.del(&format!("rl:{limiter_key}")).await;
        cache.close().await;
    }
}
