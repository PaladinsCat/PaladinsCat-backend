use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use std::collections::HashMap;

use crate::{error::ApiError, request::RequestId, routes::identity::{parse_id, simple_error}};
use super::{AdminState, require_admin};

pub(super) async fn search_accounts(
    State(state): State<AdminState>, Extension(request_id): Extension<RequestId>, headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await { return Ok(response); }
    let term = query.get("q").map(String::as_str).unwrap_or("").trim();
    if term.len() < 2 { return Ok(simple_error(StatusCode::BAD_REQUEST, "Enter at least two characters")); }
    let pattern = format!("%{term}%");
    let rows = state.database.query_json(
        "SELECT id,username,email,CASE WHEN is_admin THEN 'admin' ELSE role END AS role FROM users \
         WHERE username ILIKE $1 OR email ILIKE $1 ORDER BY username,id LIMIT 50", &[&pattern]
    ).await.map_err(|error| ApiError::database(error, &request_id))?;
    Ok((StatusCode::OK, Json(json!({"data":rows}))).into_response())
}

pub(super) async fn update_role(
    State(state): State<AdminState>, Extension(request_id): Extension<RequestId>, headers: HeaderMap,
    Path(id): Path<String>, Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await { return Ok(response); }
    let Some(id) = parse_id(&id).and_then(|id| i32::try_from(id).ok()) else { return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid account")); };
    let Some(role) = body.get("role").and_then(Value::as_str).map(str::to_ascii_lowercase).filter(|role| matches!(role.as_str(), "user"|"moderator"|"developer"|"admin")) else { return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid role")); };
    let row = state.database.one_json(
        "UPDATE users SET role=$1,is_admin=($1='admin'),updated_at=now() WHERE id=$2 \
         RETURNING id,username,email,role,is_admin", &[&role,&id]
    ).await.map_err(|error| ApiError::database(error, &request_id))?;
    Ok(match row { Some(row) => (StatusCode::OK, Json(row)).into_response(), None => simple_error(StatusCode::NOT_FOUND, "Account not found") })
}
