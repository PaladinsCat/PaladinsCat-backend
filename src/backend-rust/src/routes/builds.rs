use std::collections::{HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, post},
};
use paladinscat_core::database::Database;
use serde_json::{Value, json};

use crate::{error::ApiError, request::RequestId};

use super::identity::{as_i64, json_response, parse_id, require_session, simple_error};

#[derive(Clone)]
struct BuildsState {
    database: Database,
}

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/builds", get(list).post(create))
        .route("/builds/", get(list).post(create))
        .route("/builds/{id}", get(detail))
        .route("/builds/{id}/like", post(like))
        .with_state(BuildsState { database })
}

fn select_sql() -> &'static str {
    "SELECT b.id,b.user_id,b.champion_id,COALESCE(c.name,'Champion '||b.champion_id::text) AS champion_name, \
       b.name,b.items,b.cards,b.actives,b.talents,b.notes,b.visibility,b.likes,b.view_count,b.created_at,u.username \
     FROM builds b JOIN users u ON u.id=b.user_id LEFT JOIN champions c ON c.id=b.champion_id"
}

async fn select_build(
    state: &BuildsState,
    id: i64,
    request_id: &RequestId,
) -> Result<Option<Value>, ApiError> {
    state
        .database
        .one_json(&format!("{} WHERE b.id=$1", select_sql()), &[&id])
        .await
        .map_err(|error| ApiError::database(error, request_id))
}

fn int_array(value: Option<&Value>, maximum: usize) -> Option<Vec<i32>> {
    let Some(value) = value else {
        return Some(Vec::new());
    };
    let rows = value.as_array()?;
    if rows.len() > maximum {
        return None;
    }
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for row in rows {
        let id = as_i64(Some(row)).and_then(|value| i32::try_from(value).ok())?;
        if id <= 0 {
            return None;
        }
        if seen.insert(id) {
            output.push(id);
        }
    }
    Some(output)
}

fn build_cards(value: Option<&Value>) -> Option<Value> {
    let Some(value) = value else {
        return Some(json!([]));
    };
    let rows = value.as_array()?;
    if rows.len() > 5 {
        return None;
    }
    let mut seen = HashSet::new();
    let mut cards = Vec::new();
    for row in rows {
        let card_id = as_i64(row.get("card_id").or_else(|| row.get("cardId")))
            .and_then(|value| i32::try_from(value).ok())?;
        let level = as_i64(row.get("level").or_else(|| row.get("card_level")))
            .and_then(|value| i32::try_from(value).ok())?;
        if card_id <= 0 || !(1..=5).contains(&level) || !seen.insert(card_id) {
            return None;
        }
        cards.push(json!({"card_id":card_id,"level":level}));
    }
    Some(Value::Array(cards))
}

async fn list(
    State(state): State<BuildsState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let champion_id = query
        .get("championId")
        .and_then(|value| parse_id(value))
        .and_then(|value| i32::try_from(value).ok());
    let rows = if let Some(champion_id) = champion_id {
        state
            .database
            .query_json(
                &format!(
                    "{} WHERE b.champion_id=$1 AND b.visibility='public' ORDER BY b.likes DESC",
                    select_sql()
                ),
                &[&champion_id],
            )
            .await
    } else {
        state
            .database
            .query_json(
                &format!(
                    "{} WHERE b.visibility='public' ORDER BY b.likes DESC",
                    select_sql()
                ),
                &[],
            )
            .await
    }
    .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(StatusCode::OK, Value::Array(rows)))
}

async fn create(
    State(state): State<BuildsState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let champion_id = as_i64(body.get("champion_id"))
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0);
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    let items = int_array(body.get("items"), 4);
    let cards = build_cards(body.get("cards"));
    let actives = int_array(body.get("actives"), 4);
    let talents = int_array(body.get("talents"), 1);
    let visibility = if body.get("visibility").and_then(Value::as_str) == Some("private") {
        "private"
    } else {
        "public"
    };
    let Some(champion_id) = champion_id else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid champion_id"));
    };
    if name.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Build name is required",
        ));
    }
    let Some(items) = items else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Builds can include up to 4 valid item IDs",
        ));
    };
    let Some(cards) = cards else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Builds can include up to 5 cards with levels from 1 to 5",
        ));
    };
    let Some(actives) = actives else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Invalid legacy active item IDs",
        ));
    };
    let Some(talents) = talents else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Builds can include only 1 valid talent ID",
        ));
    };
    let notes = body
        .get("notes")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let inserted = state
        .database
        .one_json(
            "INSERT INTO builds(user_id,champion_id,name,items,cards,actives,talents,notes,visibility) \
             VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9) RETURNING id",
            &[
                &session.user_id,
                &champion_id,
                &name,
                &items,
                &cards,
                &actives,
                &talents,
                &notes,
                &visibility,
            ],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::internal(&request_id))?;
    let id = as_i64(inserted.get("id")).ok_or_else(|| ApiError::internal(&request_id))?;
    let build = select_build(&state, id, &request_id)
        .await?
        .ok_or_else(|| ApiError::internal(&request_id))?;
    Ok(json_response(StatusCode::OK, build))
}

async fn detail(
    State(state): State<BuildsState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::NOT_FOUND, "Build not found"));
    };
    let Some(build) = select_build(&state, id, &request_id).await? else {
        return Ok(simple_error(StatusCode::NOT_FOUND, "Build not found"));
    };
    state
        .database
        .query_json(
            "UPDATE builds SET view_count=view_count+1 WHERE id=$1 RETURNING id",
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(StatusCode::OK, build))
}

async fn like(
    State(state): State<BuildsState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(StatusCode::NOT_FOUND, "Build not found"));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let existing = transaction
        .query_opt(
            "SELECT 1 FROM user_build_likes WHERE user_id=$1 AND build_id=$2 FOR UPDATE",
            &[&session.user_id, &id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?
        .is_some();
    let row = if existing {
        transaction
            .execute(
                "DELETE FROM user_build_likes WHERE user_id=$1 AND build_id=$2",
                &[&session.user_id, &id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?;
        transaction
            .query_opt(
                "UPDATE builds SET likes=GREATEST(likes-1,0) WHERE id=$1 RETURNING likes",
                &[&id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?
    } else {
        transaction
            .execute(
                "INSERT INTO user_build_likes(user_id,build_id) VALUES($1,$2)",
                &[&session.user_id, &id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?;
        transaction
            .query_opt(
                "UPDATE builds SET likes=likes+1 WHERE id=$1 RETURNING likes",
                &[&id],
            )
            .await
            .map_err(|error| ApiError::database(error.into(), &request_id))?
    };
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let likes = row
        .as_ref()
        .map(|row| row.get::<_, i32>("likes"))
        .unwrap_or_default();
    Ok(json_response(
        StatusCode::OK,
        json!({"liked":!existing,"likes":likes}),
    ))
}
