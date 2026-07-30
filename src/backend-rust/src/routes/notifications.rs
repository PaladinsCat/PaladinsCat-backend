use std::{collections::HashMap, time::Duration};

use axum::{
    Json, Router,
    extract::{Extension, Query, State},
    http::{HeaderValue, StatusCode, header::HeaderName},
    response::{IntoResponse, Response},
    routing::get,
};
use paladinscat_core::database::Database;
use serde_json::Value;

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::{ColdMissLease, RouteCache, canonical_route_cache_url, now_millis},
};

const FRESH_TTL_SECONDS: u64 = 60;
const STALE_TTL_SECONDS: u64 = 180;

#[derive(Clone)]
struct NotificationState {
    database: Database,
    cache: RouteCache,
}

pub fn router(database: Database, cache: RouteCache) -> Router {
    Router::new()
        .route("/notifications", get(notifications))
        .route("/notifications/", get(notifications))
        .with_state(NotificationState { database, cache })
}

async fn notifications(
    State(state): State<NotificationState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = javascript_limit(query.get("limit").map(String::as_str), 5, 20);
    let key = format!("route:notifications:{}", canonical_route_cache_url(&uri));

    if let Some(cached) = state.cache.get(&key).await {
        let stale = cached.fresh_until <= now_millis();
        if stale && state.cache.begin_refresh(&key).await {
            spawn_refresh(state.clone(), key.clone(), limit);
        }
        return Ok(cache_response(
            cached.payload,
            if stale { "STALE" } else { "HIT" },
            cached.fresh_until,
        ));
    }

    let lease = state.cache.acquire_cold_miss(&key).await;
    if matches!(lease, ColdMissLease::Follower)
        && let Some(cached) = state.cache.wait_for_cold_miss(&key).await
    {
        return Ok(cache_response(
            cached.payload,
            "COALESCED",
            cached.fresh_until,
        ));
    }

    let payload = match load_notifications(&state.database, limit).await {
        Ok(payload) => payload,
        Err(error) => {
            state.cache.release(&lease).await;
            return Err(ApiError::database(error, &request_id));
        }
    };
    state
        .cache
        .store(&key, payload.clone(), FRESH_TTL_SECONDS, STALE_TTL_SECONDS)
        .await;
    state.cache.release(&lease).await;
    Ok(cache_response(
        payload,
        "MISS",
        now_millis().saturating_add((FRESH_TTL_SECONDS * 1_000) as i64),
    ))
}

fn spawn_refresh(state: NotificationState, key: String, limit: i64) {
    tokio::spawn(async move {
        let lease = state.cache.acquire_cold_miss(&key).await;
        if matches!(lease, ColdMissLease::Owner { .. })
            && let Ok(payload) = load_notifications(&state.database, limit).await
        {
            state
                .cache
                .store(&key, payload, FRESH_TTL_SECONDS, STALE_TTL_SECONDS)
                .await;
        }
        state.cache.release(&lease).await;
        state.cache.finish_refresh(&key).await;
    });
}

async fn load_notifications(
    database: &Database,
    limit: i64,
) -> Result<Value, paladinscat_core::database::DatabaseError> {
    let rows = database
        .query_json(
            "SELECT id, timestamp, importance, message FROM notifications ORDER BY importance DESC, timestamp DESC, id DESC LIMIT $1",
            &[&limit],
        )
        .await?;
    Ok(Value::Array(rows))
}

fn cache_response(payload: Value, status: &'static str, fresh_until: i64) -> Response {
    let mut response = (StatusCode::OK, Json(payload)).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("x-cache"),
        HeaderValue::from_static(status),
    );
    if status == "HIT" || status == "STALE" {
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

fn javascript_limit(value: Option<&str>, default: i64, maximum: i64) -> i64 {
    let parsed = value.and_then(paladinscat_core::web_compat::parse_js_integer);
    match parsed {
        None | Some(0) => default,
        Some(value) => value.min(maximum),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_matches_javascript_truthy_and_upper_bound_behavior() {
        assert_eq!(javascript_limit(None, 5, 20), 5);
        assert_eq!(javascript_limit(Some("invalid"), 5, 20), 5);
        assert_eq!(javascript_limit(Some("0"), 5, 20), 5);
        assert_eq!(javascript_limit(Some("999"), 5, 20), 20);
        assert_eq!(javascript_limit(Some("-1"), 5, 20), -1);
    }
}
