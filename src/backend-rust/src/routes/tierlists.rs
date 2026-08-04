use std::collections::{HashMap, HashSet};

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::Response,
    routing::get,
};
use paladinscat_core::database::Database;
use serde_json::{Value, json};

use crate::{error::ApiError, request::RequestId};

use super::identity::{as_i64, json_response, parse_id, require_session, simple_error};

pub const ROUTE_COUNT: usize = 3;

const SELECT: &str = "SELECT p.id,p.user_id,p.title,p.content,p.likes,p.view_count,p.created_at, \
  u.username,u.linked_player_id,(SELECT COUNT(*)::int FROM comments cmt WHERE cmt.post_id=p.id) AS comment_count, \
  COALESCE(jsonb_agg(jsonb_build_object('championId',e.champion_id,'championName',c.name,'tier',e.tier,'position',e.position) \
    ORDER BY CASE e.tier WHEN 'S' THEN 0 WHEN 'A' THEN 1 WHEN 'B' THEN 2 WHEN 'C' THEN 3 WHEN 'D' THEN 4 ELSE 5 END,e.position) \
    FILTER(WHERE e.champion_id IS NOT NULL),'[]'::jsonb) AS entries \
  FROM tier_lists tl JOIN posts p ON p.id=tl.post_id JOIN users u ON u.id=p.user_id \
  LEFT JOIN tier_list_entries e ON e.post_id=p.id LEFT JOIN champions c ON c.id=e.champion_id";

#[derive(Clone)]
struct TierListState {
    database: Database,
}

#[derive(Clone)]
struct Entry {
    champion_id: i32,
    tier: String,
    position: i32,
}

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/tierlists", get(list).post(create))
        .route("/tierlists/", get(list).post(create))
        .route("/tierlists/{id}", get(detail).put(update))
        .with_state(TierListState { database })
}

fn entries(value: Option<&Value>) -> Option<Vec<Entry>> {
    let rows = value?.as_array()?;
    rows.iter()
        .map(|row| {
            let champion_id = as_i64(row.get("championId"))
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value > 0)?;
            let tier = row
                .get("tier")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_uppercase();
            let position = as_i64(row.get("position"))
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value >= 0)?;
            matches!(tier.as_str(), "S" | "A" | "B" | "C" | "D" | "F").then_some(Entry {
                champion_id,
                tier,
                position,
            })
        })
        .collect()
}

async fn entry_error(
    state: &TierListState,
    entries: &[Entry],
    request_id: &RequestId,
) -> Result<Option<&'static str>, ApiError> {
    if entries.is_empty() {
        return Ok(Some("Place at least one champion in the tier list"));
    }
    let champion_ids = entries
        .iter()
        .map(|entry| entry.champion_id)
        .collect::<HashSet<_>>();
    if champion_ids.len() != entries.len() {
        return Ok(Some("Each champion can appear only once"));
    }
    let positions = entries
        .iter()
        .map(|entry| format!("{}:{}", entry.tier, entry.position))
        .collect::<HashSet<_>>();
    if positions.len() != entries.len() {
        return Ok(Some("Each tier position can contain only one champion"));
    }
    let known = state
        .database
        .query_json("SELECT id FROM champions ORDER BY id", &[])
        .await
        .map_err(|error| ApiError::database(error, request_id))?
        .into_iter()
        .filter_map(|row| as_i64(row.get("id")).and_then(|value| i32::try_from(value).ok()))
        .collect::<HashSet<_>>();
    Ok(champion_ids
        .iter()
        .any(|id| !known.contains(id))
        .then_some("Tier list contains an unknown champion"))
}

async fn list(
    State(state): State<TierListState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = query
        .get("limit")
        .and_then(|value| parse_id(value))
        .unwrap_or(20)
        .min(100);
    let rows = state
        .database
        .query_json(
            &format!(
                "{SELECT} GROUP BY p.id,u.username,u.linked_player_id ORDER BY p.created_at DESC LIMIT $1"
            ),
            &[&limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let mut response = json_response(StatusCode::OK, Value::Array(rows));
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=30, s-maxage=60"),
    );
    Ok(response)
}

async fn detail(
    State(state): State<TierListState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Invalid tier-list id",
        ));
    };
    let rows = state
        .database
        .query_json(
            &format!("{SELECT} WHERE p.id=$1::BIGINT GROUP BY p.id,u.username,u.linked_player_id"),
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(match rows.into_iter().next() {
        Some(row) => json_response(StatusCode::OK, row),
        None => simple_error(StatusCode::NOT_FOUND, "Tier list not found"),
    })
}

fn text(body: &Value, field: &str) -> String {
    body.get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

fn entry_json(entries: &[Entry]) -> Value {
    Value::Array(
        entries
            .iter()
            .map(|entry| {
                json!({"championId":entry.champion_id,"tier":entry.tier,"position":entry.position})
            })
            .collect(),
    )
}

async fn validate_body(
    state: &TierListState,
    body: &Value,
    request_id: &RequestId,
) -> Result<Result<(String, String, Vec<Entry>), Response>, ApiError> {
    let title = text(body, "title");
    let description = text(body, "description");
    let Some(entries) = entries(body.get("entries")) else {
        return Ok(Err(simple_error(
            StatusCode::BAD_REQUEST,
            "Tier-list entries are invalid",
        )));
    };
    if title.is_empty() || title.chars().count() > 160 {
        return Ok(Err(simple_error(
            StatusCode::BAD_REQUEST,
            "Title is required and must be 160 characters or fewer",
        )));
    }
    if description.chars().count() > 4000 {
        return Ok(Err(simple_error(
            StatusCode::BAD_REQUEST,
            "Description must be 4000 characters or fewer",
        )));
    }
    if let Some(error) = entry_error(state, &entries, request_id).await? {
        return Ok(Err(simple_error(StatusCode::BAD_REQUEST, error)));
    }
    Ok(Ok((title, description, entries)))
}

async fn create(
    State(state): State<TierListState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let (title, description, entries) = match validate_body(&state, &body, &request_id).await? {
        Ok(value) => value,
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
    let post = transaction
        .query_one(
            "INSERT INTO posts(user_id,title,content) VALUES($1,$2,$3) RETURNING id",
            &[&session.user_id, &title, &description],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let post_id = post.get::<_, i64>("id");
    transaction
        .execute(
            "INSERT INTO tier_lists(post_id,user_id) VALUES($1,$2)",
            &[&i32::try_from(post_id).unwrap_or_default(), &session.user_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let serialized = entry_json(&entries);
    transaction
        .execute(
            "INSERT INTO tier_list_entries(post_id,champion_id,tier,position) \
             SELECT $1,entry.\"championId\",entry.tier,entry.position FROM jsonb_to_recordset($2::jsonb) \
             AS entry(\"championId\" integer,tier text,position integer)",
            &[&i32::try_from(post_id).unwrap_or_default(), &serialized],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    Ok(json_response(
        StatusCode::CREATED,
        json!({"postId":post_id}),
    ))
}

async fn update(
    State(state): State<TierListState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Invalid tier-list id",
        ));
    };
    let session = match require_session(&state.database, &headers, &request_id).await {
        Ok(session) => session,
        Err(response) => return Ok(response),
    };
    let existing = state
        .database
        .one_json("SELECT user_id FROM tier_lists WHERE post_id=$1::BIGINT", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let Some(existing) = existing else {
        return Ok(simple_error(StatusCode::NOT_FOUND, "Tier list not found"));
    };
    if as_i64(existing.get("user_id")) != Some(i64::from(session.user_id)) && !session.is_admin {
        return Ok(simple_error(
            StatusCode::FORBIDDEN,
            "Not allowed to edit this tier list",
        ));
    }
    let (title, description, entries) = match validate_body(&state, &body, &request_id).await? {
        Ok(value) => value,
        Err(response) => return Ok(response),
    };
    let serialized = entry_json(&entries);
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute(
            "UPDATE posts SET title=$2,content=$3,updated_at=now() WHERE id=$1::BIGINT",
            &[&id, &title, &description],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute(
            "UPDATE tier_lists SET updated_at=now() WHERE post_id=$1::BIGINT",
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute("DELETE FROM tier_list_entries WHERE post_id=$1::BIGINT", &[&id])
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute(
            "INSERT INTO tier_list_entries(post_id,champion_id,tier,position) \
             SELECT $1,entry.\"championId\",entry.tier,entry.position FROM jsonb_to_recordset($2::jsonb) \
             AS entry(\"championId\" integer,tier text,position integer)",
            &[&i32::try_from(id).unwrap_or_default(), &serialized],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    Ok(json_response(StatusCode::OK, json!({"postId":id})))
}
