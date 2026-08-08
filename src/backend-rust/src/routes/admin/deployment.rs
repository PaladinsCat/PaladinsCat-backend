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

/// Record/backfill a stack_versions stamp.
///
/// The OVH backend container has no git checkout, so the version stamp
/// (git_commit, changelog, release significance, …) cannot be derived
/// server-side — it must be supplied by the caller (the deploy pipeline
/// computes it from the workstation HEAD and POSTs it here). Mirrors the
/// semantics of the legacy Deploy-PaladinsCatVps.ps1 stamping SQL: writes one
/// `stack` row plus one row per declared service plus a `database` row.
pub(super) async fn record_version(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if let Some(response) = operator_denial(&state, &headers) {
        return Ok(response);
    }

    let required = |name: &str| -> Option<String> {
        body.get(name).and_then(Value::as_str).map(str::to_owned)
    };
    let git_commit = required("gitCommit").filter(|v| !v.trim().is_empty());
    let version = required("version").filter(|v| !v.trim().is_empty());
    let environment = required("environment").unwrap_or_else(|| "production".to_owned());
    if git_commit.is_none() {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "gitCommit is required",
        ));
    }
    let git_commit = git_commit.unwrap();
    let version = version.unwrap_or_else(|| "v0.0.0".to_owned());
    let git_short = required("gitCommitShort").unwrap_or_default();
    let git_branch = required("gitBranch").unwrap_or_default();
    let git_dirty = body
        .get("gitDirty")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let build_timestamp = required("buildTimestamp");
    let deployment_id = required("deploymentId").unwrap_or_default();
    let compose_mode = required("composeMode").unwrap_or_else(|| "production".to_owned());
    let remote_project_dir = required("remoteProjectDir").unwrap_or_default();
    let release_type = required("releaseType").unwrap_or_else(|| "patch".to_owned());
    let change_count = body.get("changeCount").and_then(Value::as_u64).unwrap_or(0);
    let changelog = body
        .get("changelog")
        .and_then(Value::as_str)
        .map(str::to_owned);

    let mut components: Vec<String> = body
        .get("services")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(|s| s.trim().to_owned())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    components.push("database".to_owned());

    let metadata_json = json!({
        "composeMode": compose_mode,
        "services": components.join(","),
        "remoteProjectDir": remote_project_dir,
        "deploymentId": deployment_id,
        "changeCount": change_count,
        "releaseType": release_type,
    })
    .to_string();

    let db_schema_version = state
        .database
        .one_json("SELECT MAX(version) AS version FROM schema_migrations", &[])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .and_then(|row| {
            row.get("version")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_else(|| "038_baseline".to_owned());

    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;

    let stack_row = transaction
        .query_one(
            "INSERT INTO stack_versions (component, environment, version, git_commit, git_commit_short, \
                    git_branch, git_dirty, build_timestamp, deployed_at, db_schema_version, source, metadata, changelog) \
             VALUES ('stack', $1, $2, $3, $4, $5, $6, $7, now(), $8, 'deploy-script', $9, $10) \
             RETURNING id",
            &[
                &environment,
                &version,
                &git_commit,
                &git_short,
                &git_branch,
                &git_dirty,
                &build_timestamp.as_deref(),
                &db_schema_version,
                &metadata_json,
                &changelog.as_deref(),
            ],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let stack_id: i64 = stack_row.get(0);

    for component in &components {
        transaction
            .execute(
                "INSERT INTO stack_versions (component, environment, version, git_commit, git_commit_short, \
                        git_branch, git_dirty, build_timestamp, deployed_at, db_schema_version, source, metadata) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), $9, 'deploy-script', \
                         jsonb_build_object('stackVersionId', $10))",
                &[
                    &component,
                    &environment,
                    &version,
                    &git_commit,
                    &git_short,
                    &git_branch,
                    &git_dirty,
                    &build_timestamp.as_deref(),
                    &db_schema_version,
                    &stack_id,
                ],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?;
    }

    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "recorded": true,
            "component": "stack",
            "environment": environment,
            "version": version,
            "gitCommit": git_commit,
            "gitCommitShort": git_short,
            "gitDirty": git_dirty,
            "releaseType": release_type,
            "changeCount": change_count,
            "components": components,
            "stackVersionId": stack_id,
        })),
    )
        .into_response())
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
                Json(
                    json!({"error":{"code":"OPERATION_FAILED","message":"Backend warm-up failed"}}),
                ),
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
