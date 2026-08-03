use std::time::Duration;

use axum::{
    Json,
    extract::{Extension, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use paladinscat_core::deployment::{DeploymentPhase, DeploymentStateInput};
use serde_json::{Value, json};

use crate::{error::ApiError, request::RequestId};

use super::AdminState;

fn operator_denial(state: &AdminState, headers: &HeaderMap) -> Option<Response> {
    if state
        .foundation
        .security
        .is_operator_request(headers, false)
    {
        None
    } else {
        Some((
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":{"code":"OPERATOR_AUTH_REQUIRED","message":"Operator credentials are required for this endpoint."}})),
        )
            .into_response())
    }
}

fn runtime(state: &AdminState) -> Value {
    json!({
        "acceptingPublicWork": !state.foundation.active_requests.count().eq(&usize::MAX),
        "activeRequests": state.foundation.active_requests.count(),
        "schedulerOwnership": "rust",
    })
}

pub(super) async fn status(
    State(state): State<AdminState>,
    Extension(_request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(response) = operator_denial(&state, &headers) {
        return Ok(response);
    }
    Ok(Json(
        json!({"state":state.foundation.deployment.local_state().await,"runtime":runtime(&state)}),
    )
    .into_response())
}

fn input(body: &Value, phase: DeploymentPhase) -> DeploymentStateInput {
    DeploymentStateInput {
        id: body
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        phase,
        message: body
            .get("message")
            .and_then(Value::as_str)
            .map(str::to_owned),
        ttl_seconds: body.get("ttlSeconds").and_then(Value::as_u64),
    }
}

pub(super) async fn set_state(
    State(state): State<AdminState>,
    Extension(_request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Some(response) = operator_denial(&state, &headers) {
        return Ok(response);
    }
    let Some(phase) = body
        .get("phase")
        .cloned()
        .and_then(|value| serde_json::from_value::<DeploymentPhase>(value).ok())
    else {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"VALIDATION","message":"Invalid deployment phase"}})),
        )
            .into_response());
    };
    match state
        .foundation
        .deployment
        .set_state(input(&body, phase))
        .await
    {
        Ok(value) => Ok(Json(json!(value)).into_response()),
        Err(_) => Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":{"code":"PERSISTENCE_ERROR","message":"Deployment state could not be persisted"}})),
        )
            .into_response()),
    }
}

pub(super) async fn drain(
    State(state): State<AdminState>,
    Extension(_request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Some(response) = operator_denial(&state, &headers) {
        return Ok(response);
    }
    if body
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"VALIDATION","message":"Deployment id is required"}})),
        )
            .into_response());
    }
    let deployment = match state
        .foundation
        .deployment
        .set_state(input(&body, DeploymentPhase::Draining))
        .await
    {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":{"code":"OPERATION_FAILED","message":"Backend drain failed"}})),
            )
                .into_response());
        }
    };
    let seconds = body
        .get("timeoutSeconds")
        .and_then(Value::as_u64)
        .unwrap_or(90)
        .clamp(5, 300);
    let drained = state
        .foundation
        .active_requests
        .wait_for_zero(Duration::from_secs(seconds))
        .await;
    let payload = json!({"state":deployment,"drain":{"drained":drained,"activeRequests":state.foundation.active_requests.count()}});
    Ok((
        if drained {
            StatusCode::OK
        } else {
            StatusCode::CONFLICT
        },
        Json(payload),
    )
        .into_response())
}

pub(super) async fn warm(
    State(state): State<AdminState>,
    Extension(_request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Some(response) = operator_denial(&state, &headers) {
        return Ok(response);
    }
    if body
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":{"code":"VALIDATION","message":"Deployment id is required"}})),
        )
            .into_response());
    }
    let deployment = match state
        .foundation
        .deployment
        .set_state(input(&body, DeploymentPhase::Warming))
        .await
    {
        Ok(value) => value,
        Err(_) => {
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":{"code":"OPERATION_FAILED","message":"Backend warm-up failed"}})),
            )
                .into_response());
        }
    };
    let _ = state.foundation.search.initialize_indices().await;
    let warmer = match crate::workers::cache_warmer::CacheWarmer::new(
        state.database.clone(),
        &state.foundation.config,
    ) {
        Ok(warmer) => warmer,
        Err(_) => {
            return Ok((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error":{"code":"INITIALIZATION_FAILED","message":"Backend cache warmer could not start"}})),
            )
                .into_response());
        }
    };
    if warmer.warm_deployment_critical().await.is_err() {
        return Ok((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":{"code":"OPERATION_FAILED","message":"Deployment-critical cache warm-up failed"}})),
        )
            .into_response());
    }
    Ok(Json(json!({"state":deployment,"runtime":runtime(&state)})).into_response())
}
