use std::{
    collections::HashSet,
    future::Future,
    io::{Read, Write},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::http::Uri;
use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::HeaderName},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use paladinscat_core::{cache::RedisCache, database::DatabaseError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use url::form_urlencoded;
use uuid::Uuid;

use crate::{error::ApiError, request::RequestId};

const COLD_MISS_WAIT_MS: u64 = 1_500;
const LEASE_TTL_MS: u64 = 30_000;
const COMPRESSION_THRESHOLD_BYTES: usize = 64 * 1_024;
const IGNORED_QUERY_PARAMS: &[&str] = &[
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
];

#[derive(Clone)]
pub struct RouteCache {
    redis: RedisCache,
    refresh_in_flight: Arc<Mutex<HashSet<String>>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CachedRouteResponse {
    pub payload: Value,
    pub fresh_until: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoredRouteResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compressed_payload: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    encoding: Option<String>,
    fresh_until: i64,
}

impl StoredRouteResponse {
    fn encode(payload: Value, fresh_until: i64) -> Option<Self> {
        let text = serde_json::to_vec(&payload).ok()?;
        if text.len() < COMPRESSION_THRESHOLD_BYTES {
            return Some(Self {
                payload: Some(payload),
                compressed_payload: None,
                encoding: None,
                fresh_until,
            });
        }

        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(6));
        encoder.write_all(&text).ok()?;
        let compressed = encoder.finish().ok()?;
        Some(Self {
            payload: None,
            compressed_payload: Some(BASE64.encode(compressed)),
            encoding: Some("gzip-base64".to_owned()),
            fresh_until,
        })
    }

    fn decode(self) -> Option<CachedRouteResponse> {
        let payload = if self.encoding.as_deref() == Some("gzip-base64") {
            let compressed = BASE64.decode(self.compressed_payload?).ok()?;
            let mut decoder = GzDecoder::new(compressed.as_slice());
            let mut text = Vec::new();
            decoder.read_to_end(&mut text).ok()?;
            serde_json::from_slice(&text).ok()?
        } else {
            self.payload?
        };
        Some(CachedRouteResponse {
            payload,
            fresh_until: self.fresh_until,
        })
    }
}

pub enum ColdMissLease {
    Owner { key: String, token: String },
    Unavailable,
    Follower,
}

impl RouteCache {
    pub fn new(redis: RedisCache) -> Self {
        Self {
            redis,
            refresh_in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub async fn get(&self, key: &str) -> Option<CachedRouteResponse> {
        let stored = self.redis.get::<StoredRouteResponse>(key).await?;
        match stored.decode() {
            Some(cached) => Some(cached),
            None => {
                let _ = self.redis.del(key).await;
                None
            }
        }
    }

    pub async fn store(&self, key: &str, payload: Value, fresh_ttl: u64, stale_ttl: u64) {
        let Some(entry) = StoredRouteResponse::encode(
            payload,
            now_millis().saturating_add((fresh_ttl * 1_000) as i64),
        ) else {
            return;
        };
        self.redis
            .set(key, &entry, Some(fresh_ttl.saturating_add(stale_ttl)))
            .await;
    }

    pub async fn acquire_cold_miss(&self, key: &str) -> ColdMissLease {
        let lease_key = format!("{key}:lease");
        let token = Uuid::new_v4().to_string();
        match self
            .redis
            .acquire_lease(&lease_key, &token, LEASE_TTL_MS)
            .await
        {
            Some(true) => ColdMissLease::Owner {
                key: lease_key,
                token,
            },
            Some(false) => ColdMissLease::Follower,
            None => ColdMissLease::Unavailable,
        }
    }

    pub async fn release(&self, lease: &ColdMissLease) {
        if let ColdMissLease::Owner { key, token } = lease {
            self.redis.release_lease(key, token).await;
        }
    }

    pub async fn wait_for_cold_miss(&self, key: &str) -> Option<CachedRouteResponse> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(COLD_MISS_WAIT_MS);
        let mut delay = 25;
        while tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(delay)).await;
            if let Some(cached) = self.get(key).await {
                return Some(cached);
            }
            delay = (delay * 2).min(150);
        }
        None
    }

    pub async fn begin_refresh(&self, key: &str) -> bool {
        self.refresh_in_flight.lock().await.insert(key.to_owned())
    }

    pub async fn finish_refresh(&self, key: &str) {
        self.refresh_in_flight.lock().await.remove(key);
    }
}

pub async fn cached_database_json<F, Fut>(
    cache: RouteCache,
    key: String,
    fresh_ttl_seconds: u64,
    stale_ttl_seconds: u64,
    request_id: &RequestId,
    loader: F,
) -> Result<Response, ApiError>
where
    F: Fn() -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, DatabaseError>> + Send + 'static,
{
    if let Some(cached) = cache.get(&key).await {
        let stale = cached.fresh_until <= now_millis();
        if stale && cache.begin_refresh(&key).await {
            let refresh_cache = cache.clone();
            let refresh_key = key.clone();
            let refresh_loader = loader.clone();
            tokio::spawn(async move {
                let lease = refresh_cache.acquire_cold_miss(&refresh_key).await;
                if matches!(lease, ColdMissLease::Owner { .. })
                    && let Ok(payload) = refresh_loader().await
                {
                    refresh_cache
                        .store(&refresh_key, payload, fresh_ttl_seconds, stale_ttl_seconds)
                        .await;
                }
                refresh_cache.release(&lease).await;
                refresh_cache.finish_refresh(&refresh_key).await;
            });
        }
        return Ok(json_cache_response(
            cached.payload,
            if stale { "STALE" } else { "HIT" },
            cached.fresh_until,
        ));
    }

    let lease = cache.acquire_cold_miss(&key).await;
    if matches!(lease, ColdMissLease::Follower)
        && let Some(cached) = cache.wait_for_cold_miss(&key).await
    {
        return Ok(json_cache_response(
            cached.payload,
            "COALESCED",
            cached.fresh_until,
        ));
    }
    let payload = match loader().await {
        Ok(payload) => payload,
        Err(error) => {
            cache.release(&lease).await;
            return Err(ApiError::database(error, request_id));
        }
    };
    cache
        .store(&key, payload.clone(), fresh_ttl_seconds, stale_ttl_seconds)
        .await;
    cache.release(&lease).await;
    Ok(json_cache_response(
        payload,
        "MISS",
        now_millis().saturating_add((fresh_ttl_seconds * 1_000) as i64),
    ))
}

pub fn json_cache_response(payload: Value, status: &'static str, fresh_until: i64) -> Response {
    let mut response = (StatusCode::OK, Json(payload)).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("x-cache"),
        HeaderValue::from_static(status),
    );
    // Emit a browser/CDN cache lifetime so major public pages (matches,
    // items, tiers, performance, activity) are not re-fetched on every visit.
    // Derived from the server-side fresh window so clients never serve data
    // newer than the origin would consider fresh; stale-while-revalidate lets
    // CDNs keep a copy while the origin refreshes in the background.
    if status == "HIT" || status == "STALE" {
        let fresh_seconds = (fresh_until - now_millis())
            .div_euclid(Duration::from_secs(1).as_millis() as i64)
            .max(0);
        let stale_seconds = fresh_seconds.saturating_mul(3).max(60);
        if let Ok(value) = HeaderValue::from_str(&format!(
            "public, max-age={fresh_seconds}, s-maxage={fresh_seconds}, stale-while-revalidate={stale_seconds}"
        )) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("cache-control"), value);
        }
        let age = now_millis()
            .saturating_sub(fresh_until)
            .div_euclid(Duration::from_secs(1).as_millis() as i64)
            .max(0);
        if let Ok(value) = HeaderValue::from_str(&age.to_string()) {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-cache-age"), value);
        }
    }
    response
}

pub fn canonical_route_cache_url(uri: &Uri) -> String {
    let mut entries: Vec<(String, String)> = uri
        .query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .filter(|(key, _)| {
                    !IGNORED_QUERY_PARAMS.contains(&key.to_ascii_lowercase().as_str())
                })
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect()
        })
        .unwrap_or_default();
    entries.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let query = form_urlencoded::Serializer::new(String::new())
        .extend_pairs(entries)
        .finish();
    if query.is_empty() {
        uri.path().to_owned()
    } else {
        format!("{}?{query}", uri.path())
    }
}

pub fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_url_matches_typescript_sorting_and_tracking_filter() {
        let uri: Uri = "/notifications/?utm_source=x&limit=5&b=2&b=1"
            .parse()
            .expect("uri");
        assert_eq!(
            canonical_route_cache_url(&uri),
            "/notifications/?b=1&b=2&limit=5"
        );
    }

    #[test]
    fn small_entry_uses_the_typescript_payload_shape() {
        let stored =
            StoredRouteResponse::encode(serde_json::json!([{"id": 1}]), 123).expect("encode");
        assert_eq!(
            serde_json::to_value(&stored).expect("serialize"),
            serde_json::json!({
                "payload": [{"id": 1}],
                "freshUntil": 123
            })
        );
        assert_eq!(
            stored.decode().expect("decode").payload,
            serde_json::json!([{"id": 1}])
        );
    }

    #[test]
    fn large_entry_uses_the_typescript_gzip_base64_shape_and_round_trips() {
        let payload = serde_json::json!({"message": "x".repeat(COMPRESSION_THRESHOLD_BYTES)});
        let stored = StoredRouteResponse::encode(payload.clone(), 456).expect("encode");
        assert_eq!(stored.encoding.as_deref(), Some("gzip-base64"));
        assert!(stored.payload.is_none());
        assert!(stored.compressed_payload.is_some());
        assert_eq!(stored.decode().expect("decode").payload, payload);
    }

    #[test]
    fn compressed_typescript_entry_is_readable() {
        let payload = serde_json::json!({"source": "typescript"});
        let stored = StoredRouteResponse {
            payload: None,
            // Produced by Node's gzipSync(JSON.stringify(payload), { level: 6 }).
            compressed_payload: Some(
                "H4sIAAAAAAAACqtWKs4vLUpOVbJSKqksSC1OLsosKFGqBQDiSy9+FwAAAA==".to_owned(),
            ),
            encoding: Some("gzip-base64".to_owned()),
            fresh_until: 789,
        };
        let decoded = stored.decode().expect("decode");
        assert_eq!(decoded.payload, payload);
        assert_eq!(decoded.fresh_until, 789);
    }
}
