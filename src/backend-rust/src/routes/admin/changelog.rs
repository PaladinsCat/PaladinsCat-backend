use axum::{
    Json,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::{error::ApiError, request::RequestId};

use super::{AdminState, require_admin};

const MAX_CHANGELOG_LENGTH: usize = 12_000;

#[derive(Deserialize)]
pub(super) struct LimitQuery {
    limit: Option<String>,
}

fn significance(metadata: Option<&Value>, changelog: &str) -> (usize, &'static str) {
    let change_count = changelog
        .lines()
        .filter(|line| {
            let line = line.trim_start();
            line.starts_with("- ") || line.starts_with("* ")
        })
        .count();
    let text = format!(
        "{} {}",
        metadata.map(Value::to_string).unwrap_or_default(),
        changelog
    )
    .to_ascii_lowercase();
    let release_type = if text.contains("breaking") {
        "major"
    } else if text.contains("feature") || text.contains("feat:") {
        "minor"
    } else {
        "patch"
    };
    (change_count, release_type)
}

fn map_entry(row: Value) -> Value {
    let changelog = row
        .get("changelog")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let (change_count, release_type) = significance(row.get("metadata"), changelog);
    let commit = row
        .get("git_commit")
        .and_then(Value::as_str)
        .unwrap_or_default();
    json!({
        "id": row.get("id").cloned().unwrap_or(Value::Null),
        "version": row.get("version").cloned().unwrap_or(Value::Null),
        "gitCommit": commit,
        "gitCommitShort": row.get("git_commit_short").and_then(Value::as_str)
            .map(str::to_owned).unwrap_or_else(|| commit.chars().take(7).collect()),
        "gitBranch": row.get("git_branch").and_then(Value::as_str).unwrap_or_default(),
        "deployedAt": row.get("deployed_at").cloned().unwrap_or(Value::Null),
        "source": row.get("source").and_then(Value::as_str).unwrap_or_default(),
        "changelog": changelog,
        "changeCount": change_count,
        "releaseType": release_type,
    })
}

pub(super) async fn list(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<LimitQuery>,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let limit = query
        .limit
        .as_deref()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(100)
        .clamp(1, 100);
    let rows = state.database.query_json(
        "SELECT id,version,git_commit,git_commit_short,git_branch,deployed_at,source,metadata,changelog \
         FROM stack_versions WHERE component='stack' \
         ORDER BY deployed_at DESC,id DESC LIMIT $1",
        &[&limit],
    ).await.map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(Value::Array(rows.into_iter().map(map_entry).collect())).into_response())
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
            "Invalid changelog entry id",
        ));
    };
    let Some(changelog) = body.get("changelog").and_then(Value::as_str) else {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "changelog must be a string",
        ));
    };
    let normalized = changelog
        .replace("\r\n", "\n")
        .replace('\r', "\n")
        .trim()
        .to_owned();
    if normalized.chars().count() > MAX_CHANGELOG_LENGTH {
        return Ok(super::coded_error(
            StatusCode::BAD_REQUEST,
            "VALIDATION",
            "changelog must be 12,000 characters or fewer",
        ));
    }
    let changelog: Option<String> = (!normalized.is_empty()).then_some(normalized);
    let row = state.database.one_json(
        "UPDATE stack_versions SET changelog=$2 WHERE id=$1 AND component='stack' \
         RETURNING id,version,git_commit,git_commit_short,git_branch,deployed_at,source,metadata,changelog",
        &[&id, &changelog],
    ).await.map_err(|error| ApiError::database(error, &request_id))?;
    let Some(row) = row else {
        return Ok(super::coded_error(
            StatusCode::NOT_FOUND,
            "NOT_FOUND",
            "Changelog entry not found",
        ));
    };
    state.redis.del("route:meta:changelog").await;
    Ok(Json(map_entry(row)).into_response())
}

