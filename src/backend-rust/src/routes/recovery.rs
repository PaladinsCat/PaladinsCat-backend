use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    routing::get,
};
use paladinscat_core::{
    database::Database,
    web_compat::{paginate, parse_js_integer},
};
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{error::ApiError, request::RequestId};

#[derive(Clone)]
struct RecoveryState {
    database: Database,
}

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/recovery/broken-skins", get(broken_skins))
        .route(
            "/recovery/broken-skins/{champion_id}",
            get(broken_skins_for_champion),
        )
        .route("/recovery/stats", get(recovery_stats))
        .route("/recovery/stats/{match_id}", get(recovery_stats_for_match))
        .route("/recovery/pending", get(pending_recovery))
        .with_state(RecoveryState { database })
}

async fn broken_skins(
    State(state): State<RecoveryState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<Value>>, ApiError> {
    let champion_id = match query.get("championId").filter(|value| !value.is_empty()) {
        Some(value) => {
            Some(parse_js_integer(value).ok_or_else(|| ApiError::internal(&request_id))?)
        }
        None => None,
    };
    let rows = if let Some(champion_id) = champion_id {
        let champion_id =
            i32::try_from(champion_id).map_err(|_| ApiError::internal(&request_id))?;
        state
            .database
            .query_json(
                "SELECT * FROM broken_skins WHERE champion_id = $1 ORDER BY champion_id, skin_id",
                &[&champion_id],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
    } else {
        state
            .database
            .query_json(
                "SELECT * FROM broken_skins ORDER BY champion_id, skin_id",
                &[],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
    };
    Ok(Json(rows))
}

async fn broken_skins_for_champion(
    State(state): State<RecoveryState>,
    Extension(request_id): Extension<RequestId>,
    Path(champion_id): Path<String>,
) -> Result<Json<Vec<Value>>, ApiError> {
    let champion_id = parse_js_integer(&champion_id)
        .ok_or_else(|| ApiError::validation("Invalid champion ID"))
        .and_then(|value| i32::try_from(value).map_err(|_| ApiError::internal(&request_id)))?;
    let rows = state
        .database
        .query_json(
            "SELECT * FROM broken_skins WHERE champion_id = $1 ORDER BY skin_id",
            &[&champion_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(rows))
}

async fn recovery_stats(
    State(state): State<RecoveryState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<Value>>, ApiError> {
    let pagination = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    let from = query.get("from").filter(|value| !value.is_empty());
    let to = query.get("to").filter(|value| !value.is_empty());
    let rows = match (from, to) {
        (Some(from), Some(to)) => {
            let from = parse_javascript_date(from, &request_id)?;
            let to = parse_javascript_date(to, &request_id)?;
            state
                .database
                .query_json(
                    "SELECT * FROM recovery_stats WHERE created_at >= $1 AND created_at <= $2 ORDER BY created_at DESC LIMIT $3 OFFSET $4",
                    &[&from, &to, &pagination.per_page, &pagination.offset],
                )
                .await
                .map_err(|error| ApiError::database(error, &request_id))?
        }
        (Some(from), None) => {
            let from = parse_javascript_date(from, &request_id)?;
            state
                .database
                .query_json(
                    "SELECT * FROM recovery_stats WHERE created_at >= $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                    &[&from, &pagination.per_page, &pagination.offset],
                )
                .await
                .map_err(|error| ApiError::database(error, &request_id))?
        }
        (None, Some(to)) => {
            let to = parse_javascript_date(to, &request_id)?;
            state
                .database
                .query_json(
                    "SELECT * FROM recovery_stats WHERE created_at <= $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                    &[&to, &pagination.per_page, &pagination.offset],
                )
                .await
                .map_err(|error| ApiError::database(error, &request_id))?
        }
        (None, None) => state
            .database
            .query_json(
                "SELECT * FROM recovery_stats ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                &[&pagination.per_page, &pagination.offset],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?,
    };
    Ok(Json(rows))
}

fn parse_javascript_date(value: &str, request_id: &RequestId) -> Result<OffsetDateTime, ApiError> {
    if value.len() == 10 {
        let midnight = format!("{value}T00:00:00.000Z");
        if let Ok(parsed) = OffsetDateTime::parse(&midnight, &Rfc3339) {
            return Ok(parsed);
        }
    }
    OffsetDateTime::parse(value, &Rfc3339).map_err(|error| {
        tracing::error!(value, error = %error, "date query parameter could not be parsed");
        ApiError::internal(request_id)
    })
}

async fn recovery_stats_for_match(
    State(state): State<RecoveryState>,
    Extension(request_id): Extension<RequestId>,
    Path(match_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let match_id =
        parse_js_integer(&match_id).ok_or_else(|| ApiError::validation("Invalid match ID"))?;
    let row = state
        .database
        .one_json(
            "SELECT * FROM recovery_stats WHERE match_id = $1",
            &[&match_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| {
            ApiError::not_found("Recovery stats not found", json!({ "matchId": match_id }))
        })?;
    Ok(Json(row))
}

async fn pending_recovery(
    State(state): State<RecoveryState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Vec<Value>>, ApiError> {
    let parsed_limit = query.get("limit").and_then(|value| parse_js_integer(value));
    let limit = match parsed_limit {
        None | Some(0) => 50,
        Some(limit) => limit.min(200),
    };
    let rows = state
        .database
        .query_json(
            "SELECT entity_id, entity_type, status, endpoint, created_at FROM raw_ingest_buffer WHERE entity_type = 'match' AND status IN ('pending', 'failed') ORDER BY created_at DESC LIMIT $1",
            &[&limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(rows))
}
