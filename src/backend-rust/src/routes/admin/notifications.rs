use axum::{
    Json,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{error::ApiError, request::RequestId};

use super::{AdminState, require_admin};

type NormalizedNotification = (Option<String>, Option<i32>, Option<OffsetDateTime>);

fn normalize(body: &Value, partial: bool) -> Result<NormalizedNotification, &'static str> {
    let message = if !partial || body.get("message").is_some() {
        let value = body
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .trim()
            .to_owned();
        if value.is_empty() {
            return Err("message is required");
        }
        if value.chars().count() > 500 {
            return Err("message must be 500 characters or fewer");
        }
        Some(value)
    } else {
        None
    };
    let importance = if !partial || body.get("importance").is_some() {
        let value = body
            .get("importance")
            .and_then(|value| {
                value
                    .as_i64()
                    .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            })
            .and_then(|value| i32::try_from(value).ok())
            .ok_or("importance must be an integer")?;
        Some(value)
    } else {
        None
    };
    let timestamp = if !partial || body.get("timestamp").is_some() {
        match body.get("timestamp").filter(|value| !value.is_null()) {
            Some(Value::String(value)) => Some(
                OffsetDateTime::parse(value, &Rfc3339)
                    .map_err(|_| "timestamp must be a valid date")?,
            ),
            Some(_) => return Err("timestamp must be a valid date"),
            None => Some(OffsetDateTime::now_utc()),
        }
    } else {
        None
    };
    Ok((message, importance, timestamp))
}

pub(super) async fn list(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let rows = state
        .database
        .query_json(
            "SELECT id,timestamp,importance,message FROM notifications \
         ORDER BY importance DESC,timestamp DESC,id DESC LIMIT 100",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(Value::Array(rows)).into_response())
}

pub(super) async fn create(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let (message, importance, timestamp) = match normalize(&body, false) {
        Ok(values) => values,
        Err(message) => {
            return Ok(super::coded_error(
                StatusCode::BAD_REQUEST,
                "VALIDATION",
                message,
            ));
        }
    };
    let row = state
        .database
        .one_json(
            "INSERT INTO notifications(timestamp,importance,message) VALUES($1,$2,$3) \
         RETURNING id,timestamp,importance,message",
            &[&timestamp, &importance, &message],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    state.redis.del("route:notifications").await;
    Ok((StatusCode::CREATED, Json(row.unwrap_or(Value::Null))).into_response())
}

pub(super) async fn update(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let Some(id) = id.parse::<i64>().ok().filter(|id| *id > 0) else {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "Invalid notification id",
        ));
    };
    let (message, importance, timestamp) = match normalize(&body, true) {
        Ok(values) => values,
        Err(message) => {
            return Ok(super::coded_error(
                StatusCode::BAD_REQUEST,
                "VALIDATION",
                message,
            ));
        }
    };
    if message.is_none() && importance.is_none() && timestamp.is_none() {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "No fields to update",
        ));
    }
    let row = state.database.one_json(
        "UPDATE notifications SET \
           message=COALESCE($2,message),importance=COALESCE($3,importance),timestamp=COALESCE($4,timestamp) \
         WHERE id=$1 RETURNING id,timestamp,importance,message",
        &[&id, &message, &importance, &timestamp],
    ).await.map_err(|error| ApiError::database(error, &request_id))?;
    let Some(row) = row else {
        return Ok(super::coded_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "Notification not found",
        ));
    };
    state.redis.del("route:notifications").await;
    Ok(Json(row).into_response())
}

pub(super) async fn delete(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let Some(id) = id.parse::<i64>().ok().filter(|id| *id > 0) else {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "Invalid notification id",
        ));
    };
    let row = state
        .database
        .one_json(
            "DELETE FROM notifications WHERE id=$1::BIGINT RETURNING id",
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    if row.is_none() {
        return Ok(super::coded_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "Notification not found",
        ));
    }
    state.redis.del("route:notifications").await;
    Ok(Json(json!({"deleted":true,"id":id})).into_response())
}
