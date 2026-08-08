use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use paladinscat_core::database::{Database, QueryParam};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::foundation::FoundationState;
use crate::security::developer_bearer_token;
use crate::{error::ApiError, request::RequestId, workers::relay::WorkerRelayClient};

pub const ROUTE_COUNT: usize = 7;

pub fn router(database: Database, foundation: &FoundationState) -> Router {
    let relay = WorkerRelayClient::new(&foundation.config).ok();
    Router::new()
        .route("/api/hirez-raw-responses", get(list_hirez_raw_responses))
        .route(
            "/api/hirez-raw-responses/stats",
            get(hirez_raw_response_stats),
        )
        .route(
            "/api/hirez-raw-responses/{id}",
            get(hirez_raw_response_by_id),
        )
        .route("/api/raw-responses", get(list_raw_responses))
        .route("/api/raw-responses/stats", get(raw_response_stats))
        .route(
            "/api/raw-responses/match/{match_id}",
            get(raw_responses_by_match),
        )
        .route("/api/raw-responses/{id}", get(raw_response_by_id))
        .route("/api/raw/call", post(raw_call_endpoint))
        .with_state(database)
        .layer(axum::extract::Extension(relay))
        .layer(axum::extract::Extension(foundation.security.clone()))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawCallRequest {
    method: String,
    #[serde(default)]
    params: Vec<String>,
}

#[derive(serde::Serialize)]
struct RawCallResponse {
    raw: Value,
}

async fn raw_call_endpoint(
    Extension(relay): Extension<Option<WorkerRelayClient>>,
    Extension(security): Extension<Arc<crate::security::SecurityContext>>,
    headers: HeaderMap,
    Extension(_request_id): Extension<RequestId>,
    Json(req): Json<RawCallRequest>,
) -> Result<Response, ApiError> {
    // Operator-only authorization
    let authenticated_dev = security.developer_key_configured()
        && developer_bearer_token(&headers)
            .is_some_and(|token| security.authenticate_developer_key(token));
    if !security.is_operator_request(&headers, authenticated_dev) {
        return Err(ApiError::coded(
            StatusCode::FORBIDDEN,
            "OPERATOR_ONLY",
            "Raw API passthrough requires operator authorization",
        ));
    }

    // Method name validation
    let method = &req.method;
    if method.is_empty()
        || method.len() > 64
        || !method
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.')
    {
        return Err(ApiError::coded(
            StatusCode::BAD_REQUEST,
            "INVALID_METHOD",
            "method must be alphanumeric/dots, max 64 chars",
        ));
    }

    let Some(relay) = relay else {
        return Err(ApiError::coded(
            StatusCode::SERVICE_UNAVAILABLE,
            "RELAY_NOT_CONFIGURED",
            "HirezRelay is not configured",
        ));
    };

    // Build relay args: [method_string, params_array]
    let args = vec![
        Value::String(method.clone()),
        Value::Array(
            req.params
                .iter()
                .map(|s| Value::String(s.clone()))
                .collect(),
        ),
    ];

    let result = relay
        .call_value("callRawEndpoint", args, "raw_api_passthrough")
        .await
        .map_err(|e| ApiError::coded(StatusCode::BAD_GATEWAY, "RELAY_ERROR", e.to_string()))?;

    Ok((StatusCode::OK, Json(RawCallResponse { raw: result })).into_response())
}

async fn list_hirez_raw_responses(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = js_integer(query.get("limit"), 50).min(500);
    let include_text = query
        .get("includeText")
        .is_some_and(|value| value == "true");
    let mut conditions = Vec::new();
    let mut params = Vec::new();
    for (query_name, column_name) in [
        ("endpoint", "endpoint"),
        ("entityType", "entity_type"),
        ("entityId", "entity_id"),
    ] {
        if let Some(value) = query.get(query_name) {
            conditions.push(format!("{column_name} = ${}", params.len() + 1));
            params.push(QueryParam::Text(value.clone()));
        }
    }
    let text_column = if include_text {
        ", raw_response_text"
    } else {
        ""
    };
    let mut sql = format!(
        "SELECT id, endpoint, operation, entity_type, entity_id, params, \
         raw_response{text_column}, response_sha256, response_shape, \
         response_count, status_code, success, error_message, source, created_at \
         FROM hirez_raw_api_responses"
    );
    if !conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&conditions.join(" AND "));
    }
    sql.push_str(&format!(
        " ORDER BY created_at DESC LIMIT ${}",
        params.len() + 1
    ));
    params.push(QueryParam::Int64(limit));
    let rows = database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let count = rows.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "data": rows, "count": count })),
    )
        .into_response())
}

async fn hirez_raw_response_stats(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let rows = database
        .query_json(
            "SELECT endpoint, operation, entity_type, \
             COUNT(*)::INT AS total_requests, \
             COUNT(*) FILTER (WHERE success)::INT AS success_count, \
             COUNT(*) FILTER (WHERE NOT success)::INT AS error_count, \
             SUM(COALESCE(response_count, 0))::INT AS total_response_items, \
             MIN(created_at) AS first_request, MAX(created_at) AS last_request \
             FROM hirez_raw_api_responses \
             GROUP BY endpoint, operation, entity_type \
             ORDER BY total_requests DESC, endpoint ASC",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok((StatusCode::OK, Json(json!({ "data": rows }))).into_response())
}

async fn hirez_raw_response_by_id(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<i64>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let include_text = query
        .get("includeText")
        .is_some_and(|value| value == "true");
    let text_column = if include_text {
        ", raw_response_text"
    } else {
        ""
    };
    let row = database
        .one_json(
            &format!(
                "SELECT id, endpoint, operation, entity_type, entity_id, params, \
                 raw_response{text_column}, response_sha256, response_shape, \
                 response_count, status_code, success, error_message, source, created_at \
                 FROM hirez_raw_api_responses WHERE id = $1"
            ),
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(match row {
        Some(row) => (StatusCode::OK, Json(row)).into_response(),
        None => (StatusCode::NOT_FOUND, Json(json!({ "error": "Not found" }))).into_response(),
    })
}

async fn list_raw_responses(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let mut params = Vec::new();
    let mut sql = "SELECT id, endpoint, params, raw_data as raw_response, status_code, \
                   session_id, response_time_ms, error_message, created_at \
                   FROM raw_ingest_buffer"
        .to_owned();
    if let Some(endpoint) = query.get("endpoint") {
        sql.push_str(" WHERE endpoint = $1");
        params.push(QueryParam::Text(endpoint.clone()));
    }
    sql.push_str(&format!(
        " ORDER BY created_at DESC LIMIT ${}",
        params.len() + 1
    ));
    params.push(QueryParam::Int64(js_integer(query.get("limit"), 50)));
    let rows = database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let count = rows.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "data": rows, "count": count })),
    )
        .into_response())
}

async fn raw_response_by_id(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<i64>,
) -> Result<Response, ApiError> {
    let row = database
        .one_json(
            "SELECT id, endpoint, params, raw_data as raw_response, status_code, \
             session_id, response_time_ms, error_message, created_at \
             FROM raw_ingest_buffer WHERE id = $1",
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(match row {
        Some(row) => (StatusCode::OK, Json(row)).into_response(),
        None => (StatusCode::NOT_FOUND, Json(json!({ "error": "Not found" }))).into_response(),
    })
}

async fn raw_response_stats(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let rows = database
        .query_json(
            "SELECT endpoint, COUNT(*) as total_requests, \
             COUNT(CASE WHEN status_code = 200 THEN 1 END) as success_count, \
             COUNT(CASE WHEN status_code != 200 THEN 1 END) as error_count, \
             AVG(response_time_ms) as avg_response_time_ms, \
             MIN(created_at) as first_request, MAX(created_at) as last_request \
             FROM raw_ingest_buffer GROUP BY endpoint ORDER BY total_requests DESC",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok((StatusCode::OK, Json(json!({ "data": rows }))).into_response())
}

async fn raw_responses_by_match(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(match_id): Path<String>,
) -> Result<Response, ApiError> {
    let pattern = format!("\"{match_id}\"");
    let rows = database
        .query_json(
            "SELECT id, endpoint, params, raw_data as raw_response, status_code, \
             session_id, response_time_ms, error_message, created_at \
             FROM raw_ingest_buffer \
             WHERE endpoint = 'getmatchdetailsbatch' AND params::text LIKE $1 \
             ORDER BY created_at DESC",
            &[&pattern],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let count = rows.len();
    Ok((
        StatusCode::OK,
        Json(json!({ "data": rows, "count": count })),
    )
        .into_response())
}

fn js_integer(value: Option<&String>, fallback: i64) -> i64 {
    value
        .and_then(|value| paladinscat_core::web_compat::parse_js_integer(value))
        .filter(|value| *value != 0)
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_limits_preserve_javascript_parse_int_and_truthiness() {
        assert_eq!(js_integer(None, 50), 50);
        assert_eq!(js_integer(Some(&"invalid".to_owned()), 50), 50);
        assert_eq!(js_integer(Some(&"0".to_owned()), 50), 50);
        assert_eq!(js_integer(Some(&"12rows".to_owned()), 50), 12);
    }
}
