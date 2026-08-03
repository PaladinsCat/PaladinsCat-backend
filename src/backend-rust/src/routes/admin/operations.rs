use std::collections::{BTreeMap, HashMap, HashSet};

use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};

use crate::{
    error::ApiError,
    request::RequestId,
    routes::live::{request_identity, vendor_guard},
    workers::{
        maintenance::{
            cleanup_raw_ingest_buffer_retention, process_buffer_batch, refresh_baselines_with_job,
            refresh_derived_projections_with_job,
        },
        requested_match::RequestedMatchStatus,
    },
};

use super::{AdminState, require_auth};

pub(super) async fn batch_fetch(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let supplied = body
        .get("matchIds")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if supplied.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"matchIds is required (array of numbers)"})),
        )
            .into_response());
    }
    let normalized = supplied
        .iter()
        .filter_map(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
        .filter(|id| *id > 0 && *id <= 9_007_199_254_740_991)
        .collect::<Vec<_>>();
    let unique = normalized.iter().copied().collect::<HashSet<_>>();
    if normalized.len() != supplied.len()
        || unique.len() != normalized.len()
        || normalized.len() > 10
    {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"matchIds must contain 1-10 unique positive safe integers"})),
        )
            .into_response());
    }
    let Some(ingestor) = &state.requested_match else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"success":false,"error":"HirezRelay is unavailable"})),
        )
            .into_response());
    };
    vendor_guard(
        &state.redis,
        &request_identity(&headers),
        "admin-batch-fetch",
        normalized
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        30_000,
        10,
    )
    .await?;
    let before = api_log_snapshot(&state, &request_id).await?;
    let mut fetched = 0_u64;
    let mut not_found = Vec::new();
    for match_id in normalized {
        match ingestor.ingest(match_id).await.status {
            RequestedMatchStatus::Ready => fetched += 1,
            RequestedMatchStatus::NotFound => not_found.push(match_id),
            RequestedMatchStatus::RecoveryFailed | RequestedMatchStatus::ProcessingTimeout => {}
        }
    }
    let after = api_log_snapshot(&state, &request_id).await?;
    let mut calls = BTreeMap::new();
    for (key, count) in after {
        let used = count - before.get(&key).copied().unwrap_or_default();
        if used > 0 {
            calls.insert(key, used);
        }
    }
    let total = calls.values().sum::<i64>();
    Ok(Json(json!({
        "success":true,
        "matchesFetched":fetched,
        "notFound":not_found,
        "apiCalls":calls,
        "totalApiCalls":total,
    }))
    .into_response())
}

async fn api_log_snapshot(
    state: &AdminState,
    request_id: &RequestId,
) -> Result<HashMap<String, i64>, ApiError> {
    let rows = state
        .database
        .query_json(
            "SELECT dev_id,endpoint,consumer,call_count FROM api_log \
         WHERE hour>=now()-INTERVAL '10 minutes' ORDER BY dev_id,consumer,endpoint",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(rows
        .into_iter()
        .map(|row| {
            let dev = row
                .get("dev_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let consumer = row
                .get("consumer")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let endpoint = row
                .get("endpoint")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let count = row
                .get("call_count")
                .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
                .unwrap_or_default();
            (format!("{consumer}:{endpoint} (key {dev})"), count)
        })
        .collect())
}

pub(super) async fn delete_hourly_match_count(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path((date, hour, queue_id)): Path<(String, String, String)>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let Some(hour) = hour.parse::<i32>().ok() else {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Invalid hour or queueId parameters"})),
        )
            .into_response());
    };
    let Some(queue_id) = queue_id.parse::<i32>().ok() else {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Invalid hour or queueId parameters"})),
        )
            .into_response());
    };
    state
        .database
        .query_json(
            "DELETE FROM hourly_match_counts WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
            &[&date, &hour, &queue_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(json!({"deleted":true,"date":date,"hour":hour,"queueId":queue_id})).into_response())
}

pub(super) async fn process_buffer(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let batch = body
        .get("batch")
        .and_then(Value::as_u64)
        .unwrap_or(50)
        .clamp(1, 500) as usize;
    let result = process_buffer_batch(&state.database, state.relay.as_ref(), batch)
        .await
        .map_err(|error| {
            tracing::error!(%error, "manual raw-buffer processing failed");
            ApiError::internal(&request_id)
        })?;
    Ok(Json(json!(result)).into_response())
}

pub(super) async fn buffer_retention(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let supplied = body
        .get("reason")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim();
    let reason = if supplied.is_empty() {
        "manual admin endpoint".to_owned()
    } else {
        format!("manual: {supplied}")
    };
    let result = cleanup_raw_ingest_buffer_retention(&state.database, &reason)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(json!(result)).into_response())
}

pub(super) async fn refresh_coplay(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    match state
        .database
        .query_json("REFRESH MATERIALIZED VIEW mv_player_coplay_stats", &[])
        .await
    {
        Ok(_) => {
            Ok(Json(json!({"message":"Materialized view refreshed successfully"})).into_response())
        }
        Err(error) => Ok(super::coded_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "INTERNAL",
            &format!("Failed to refresh materialized view: {error}"),
        )),
    }
}

pub(super) async fn refresh_baselines(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    match refresh_baselines_with_job(&state.database, "manual").await {
        Ok(result) => Ok(Json(json!({"message":"Baselines refreshed successfully","jobId":result.job_id,"baselineRows":result.rows})).into_response()),
        Err(error) => Ok(super::coded_error(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &format!("Failed to refresh baselines: {error}"))),
    }
}

pub(super) async fn refresh_derived_projections(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    match refresh_derived_projections_with_job(&state.database, "manual").await {
        Ok(result) => Ok(Json(json!({"message":"Derived projections refreshed successfully","jobId":result.job_id,"counts":result.counts})).into_response()),
        Err(error) => Ok(super::coded_error(StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL", &format!("Failed to refresh derived projections: {error}"))),
    }
}

pub(super) async fn sync_api_keys(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let keys = if let Some(dev_id) = body.get("devId").and_then(Value::as_str) {
        vec![dev_id.to_owned()]
    } else {
        state
            .database
            .query_json("SELECT dev_id FROM api_keys ORDER BY dev_id", &[])
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
            .into_iter()
            .filter_map(|row| row.get("dev_id").and_then(Value::as_str).map(str::to_owned))
            .collect()
    };
    let Some(relay) = &state.relay else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"HirezRelay is unavailable"})),
        )
            .into_response());
    };
    let mut synced = Vec::new();
    for dev_id in keys {
        vendor_guard(
            &state.redis,
            &request_identity(&headers),
            "admin-api-key-sync",
            &dev_id,
            30_000,
            20,
        )
        .await?;
        relay
            .call_value("syncApiKeyUsage", vec![json!(dev_id)], "rust_admin")
            .await
            .map_err(|error| {
                tracing::error!(%error);
                ApiError::internal(&request_id)
            })?;
        if let Some(row) = state
            .database
            .one_json(
                "SELECT status,total_24h,daily_limit FROM api_keys WHERE dev_id=$1",
                &[&dev_id],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
        {
            let used = number(&row, "total_24h");
            let limit = number(&row, "daily_limit");
            synced.push(json!({"devId":dev_id,"total_24h":used,"daily_limit":limit,"remaining":(limit-used).max(0),"status":row.get("status").cloned().unwrap_or(Value::Null)}));
        }
    }
    Ok(Json(json!({"synced":synced})).into_response())
}

pub(super) async fn reset_api_key_budgets(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_auth(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let keys = if let Some(dev_id) = body.get("devId").and_then(Value::as_str) {
        vec![dev_id.to_owned()]
    } else {
        state
            .database
            .query_json("SELECT dev_id FROM api_keys ORDER BY dev_id", &[])
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
            .into_iter()
            .filter_map(|row| row.get("dev_id").and_then(Value::as_str).map(str::to_owned))
            .collect()
    };
    let Some(relay) = &state.relay else {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"HirezRelay is unavailable"})),
        )
            .into_response());
    };
    let mut results = Vec::new();
    for dev_id in keys {
        let current = state
            .database
            .one_json(
                "SELECT total_24h,daily_limit,status FROM api_keys WHERE dev_id=$1",
                &[&dev_id],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
        let previous = current.as_ref().map(|row| number(row, "total_24h"));
        let attempt = async {
            let _ = vendor_guard(&state.redis, &request_identity(&headers), "admin-api-key-budget-reset", &dev_id, 30_000, 20).await?;
            let response = relay.call_value("getDataUsed", vec![json!(dev_id)], "rust_admin").await
                .map_err(|error| { tracing::error!(%error); ApiError::internal(&request_id) })?;
            let actual = response.get("Total_Requests_Today").and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
                .ok_or_else(|| ApiError::internal(&request_id))?;
            let reported = response.get("Request_Limit_Daily").and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok())).unwrap_or_default();
            let configured = if dev_id == "2116" { 15_000 } else { 7_500 };
            let limit = if reported > 0 { configured.min(reported) } else { configured };
            state.database.query_json(
                "UPDATE api_keys SET total_24h=$1,daily_limit=$2,\
                 status=CASE WHEN ($2-$1)>500 THEN 'healthy' ELSE 'limited' END,\
                 consecutive_failures=CASE WHEN ($2-$1)>500 THEN 0 ELSE consecutive_failures END WHERE dev_id=$3",
                &[&actual, &limit, &dev_id],
            ).await.map_err(|error| ApiError::database(error, &request_id))?;
            state.database.query_json("DELETE FROM api_key_hourly_usage WHERE dev_id=$1", &[&dev_id])
                .await.map_err(|error| ApiError::database(error, &request_id))?;
            Ok::<_, ApiError>((actual, reported, limit))
        }.await;
        match attempt {
            Ok((actual, reported, limit)) => results.push(json!({
                "dev_id":dev_id,"status":"success","previous_total_24h":previous,
                "hi_rez_total_requests_today":actual,"hi_rez_daily_limit":reported,
                "new_total_24h":actual,"remaining":(limit-actual).max(0),"error":Value::Null
            })),
            Err(error) => {
                tracing::warn!(?error, dev_id, "API-key usage reconciliation failed");
                results.push(json!({
                "dev_id":dev_id,"status":"error","previous_total_24h":previous,
                "hi_rez_total_requests_today":Value::Null,"hi_rez_daily_limit":Value::Null,
                "new_total_24h":Value::Null,"remaining":Value::Null,
                "error":"API-key usage reconciliation failed"
                }))
            }
        }
    }
    let _ = relay
        .call_value("reloadApiKeyPool", vec![], "rust_admin")
        .await;
    let successful = results
        .iter()
        .filter(|row| row.get("status").and_then(Value::as_str) == Some("success"))
        .count();
    Ok(Json(json!({"keys":results,"total_keys":results.len(),"successful":successful,"failed":results.len()-successful})).into_response())
}

fn number(row: &Value, key: &str) -> i64 {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}
