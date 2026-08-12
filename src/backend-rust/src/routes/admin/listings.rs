use std::collections::HashMap;

use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
};
use paladinscat_core::database::QueryParam;
use serde_json::Value;

use crate::{error::ApiError, request::RequestId};

use super::{AdminState, require_admin};

fn pagination(query: &HashMap<String, String>) -> (i64, i64) {
    let page = query
        .get("page")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(1)
        .max(1);
    let per_page = query
        .get("perPage")
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(20)
        .clamp(1, 100);
    (per_page, (page - 1) * per_page)
}

fn where_clause(predicates: &[String]) -> String {
    if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    }
}

async fn rows(
    state: &AdminState,
    request_id: &RequestId,
    sql: String,
    params: Vec<QueryParam>,
) -> Result<Response, ApiError> {
    let rows = state
        .database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(Json(Value::Array(rows)).into_response())
}

pub(super) async fn sync_jobs(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let (limit, offset) = pagination(&query);
    let mut predicates = Vec::new();
    let mut params = Vec::new();
    for (query_key, column) in [("type", "job_type"), ("status", "status")] {
        if let Some(value) = query.get(query_key).filter(|value| !value.is_empty()) {
            params.push(QueryParam::Text(value.clone()));
            predicates.push(format!("{column}=${}", params.len()));
        }
    }
    let clause = where_clause(&predicates);
    params.push(QueryParam::Int64(limit));
    let limit_index = params.len();
    params.push(QueryParam::Int64(offset));
    let offset_index = params.len();
    rows(
        &state,
        &request_id,
        format!(
            "SELECT * FROM sync_jobs{clause} ORDER BY created_at DESC LIMIT ${limit_index} OFFSET ${offset_index}"
        ),
        params,
    )
    .await
}

pub(super) async fn sync_jobs_type(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(job_type): Path<String>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    rows(
        &state,
        &request_id,
        "SELECT * FROM sync_jobs WHERE job_type=$1 ORDER BY created_at DESC LIMIT 50".to_owned(),
        vec![QueryParam::Text(job_type)],
    )
    .await
}

pub(super) async fn pull_list(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let (limit, offset) = pagination(&query);
    rows(
        &state,
        &request_id,
        "SELECT * FROM match_pull_list ORDER BY created_at DESC LIMIT $1 OFFSET $2".to_owned(),
        vec![QueryParam::Int64(limit), QueryParam::Int64(offset)],
    )
    .await
}

pub(super) async fn api_log(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let (limit, offset) = pagination(&query);
    let mut predicates = Vec::new();
    let mut params = Vec::new();
    for (query_key, column) in [
        ("devId", "dev_id"),
        ("endpoint", "endpoint"),
        ("consumer", "consumer"),
    ] {
        if let Some(value) = query.get(query_key).filter(|value| !value.is_empty()) {
            params.push(QueryParam::Text(value.clone()));
            predicates.push(format!("{column}=${}", params.len()));
        }
    }
    for (query_key, operator) in [("from", ">="), ("to", "<=")] {
        if let Some(value) = query.get(query_key).filter(|value| !value.is_empty()) {
            params.push(QueryParam::Text(value.clone()));
            predicates.push(format!("hour{operator}${}::TIMESTAMPTZ", params.len()));
        }
    }
    let clause = where_clause(&predicates);
    params.push(QueryParam::Int64(limit));
    let limit_index = params.len();
    params.push(QueryParam::Int64(offset));
    let offset_index = params.len();
    rows(
        &state,
        &request_id,
        format!(
            "SELECT * FROM api_log{clause} ORDER BY hour DESC,dev_id,consumer,endpoint LIMIT ${limit_index} OFFSET ${offset_index}"
        ),
        params,
    )
    .await
}

pub(super) async fn api_log_key(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(dev_id): Path<String>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    rows(
        &state,
        &request_id,
        "SELECT * FROM api_log WHERE dev_id=$1 ORDER BY hour DESC,consumer,endpoint LIMIT 100"
            .to_owned(),
        vec![QueryParam::Text(dev_id)],
    )
    .await
}

pub(super) async fn hourly_usage(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let mut predicates = Vec::new();
    let mut params = Vec::new();
    if let Some(value) = query.get("devId").filter(|value| !value.is_empty()) {
        params.push(QueryParam::Text(value.clone()));
        predicates.push(format!("dev_id=${}", params.len()));
    }
    for (key, operator) in [("from", ">="), ("to", "<=")] {
        if let Some(value) = query.get(key).filter(|value| !value.is_empty()) {
            params.push(QueryParam::Text(value.clone()));
            predicates.push(format!(
                "hour_bucket{operator}${}::TIMESTAMPTZ",
                params.len()
            ));
        }
    }
    let clause = where_clause(&predicates);
    rows(
        &state,
        &request_id,
        format!(
            "SELECT * FROM api_key_hourly_usage{clause} ORDER BY hour_bucket DESC,dev_id LIMIT 100"
        ),
        params,
    )
    .await
}

pub(super) async fn hourly_match_counts(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let (limit, offset) = pagination(&query);
    let mut predicates = Vec::new();
    let mut params = Vec::new();
    if let Some(value) = query.get("date").filter(|value| !value.is_empty()) {
        params.push(QueryParam::Text(value.clone()));
        predicates.push(format!("date=${}::DATE", params.len()));
    }
    for (key, column) in [("hour", "hour"), ("queueId", "queue_id")] {
        if let Some(value) = query
            .get(key)
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<i32>().ok())
        {
            params.push(QueryParam::Int32(value));
            predicates.push(format!("{column}=${}", params.len()));
        }
    }
    let clause = where_clause(&predicates);
    params.push(QueryParam::Int64(limit));
    let limit_index = params.len();
    params.push(QueryParam::Int64(offset));
    let offset_index = params.len();
    rows(
        &state,
        &request_id,
        format!(
            "SELECT * FROM hourly_match_counts{clause} ORDER BY date DESC,hour DESC LIMIT ${limit_index} OFFSET ${offset_index}"
        ),
        params,
    )
    .await
}

pub(super) async fn hourly_match_counts_date(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(date): Path<String>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    rows(
        &state,
        &request_id,
        "SELECT * FROM hourly_match_counts WHERE date=$1::TEXT::DATE ORDER BY hour DESC".to_owned(),
        vec![QueryParam::Text(date)],
    )
    .await
}
