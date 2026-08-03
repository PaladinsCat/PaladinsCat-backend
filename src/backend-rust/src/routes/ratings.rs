use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    routing::get,
};
use paladinscat_core::{
    database::{Database, QueryParam},
    web_compat::{paginate, parse_js_integer, sorting},
};
use serde_json::{Value, json};

use crate::{error::ApiError, request::RequestId, sql_compat::DISPLAY_NAME_SQL};

pub const ROUTE_COUNT: usize = 8;

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/ratings/queue/{player_id}", get(queue_ratings))
        .route("/ratings/champion/meta", get(champion_meta))
        .route(
            "/ratings/champion/match-history/{champion_id}",
            get(champion_match_history),
        )
        .route("/ratings/champion/{player_id}", get(champion_ratings))
        .route("/ratings/snapshots/{match_id}", get(match_snapshots))
        .route(
            "/ratings/snapshots/player/{player_id}",
            get(player_snapshots),
        )
        .route("/ratings/distribution", get(distribution))
        .route("/ratings/volatility/{player_id}", get(volatility_history))
        .with_state(database)
}

async fn queue_ratings(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let player_id = positive_player_id(&raw_player_id)?;
    let mut params = vec![QueryParam::Int64(player_id)];
    let mut sql = "SELECT * FROM player_queue_ratings WHERE player_id = $1".to_owned();
    if let Some(raw_queue_id) = truthy_query(&query, "queueId") {
        let queue_id = int32_query(raw_queue_id, &request_id)?;
        params.push(QueryParam::Int32(queue_id));
        sql.push_str(" AND queue_id = $2");
    }
    sql.push_str(
        " AND mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2",
    );
    sql.push_str(&sorting(
        query.get("sort").map(String::as_str),
        query.get("order").map(String::as_str),
        &["mu", "phi", "volatility", "queue_id"],
    ));
    rows(&database, &sql, &params, &request_id).await
}

async fn champion_ratings(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let player_id = positive_player_id(&raw_player_id)?;
    let mut params = vec![QueryParam::Int64(player_id)];
    let mut sql = "SELECT * FROM player_champion_ratings WHERE player_id = $1".to_owned();
    if let Some(raw_champion_id) = truthy_query(&query, "championId") {
        let champion_id = int32_query(raw_champion_id, &request_id)?;
        params.push(QueryParam::Int32(champion_id));
        sql.push_str(" AND champion_id = $2");
    }
    sql.push_str(
        " AND mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2",
    );
    sql.push_str(&sorting(
        query.get("sort").map(String::as_str),
        query.get("order").map(String::as_str),
        &[
            "mu",
            "phi",
            "matches_played",
            "wins",
            "losses",
            "champion_id",
        ],
    ));
    rows(&database, &sql, &params, &request_id).await
}

async fn champion_meta(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Json<Value>, ApiError> {
    rows(
        &database,
        "SELECT * FROM champion_ratings ORDER BY rating DESC LIMIT 100",
        &[],
        &request_id,
    )
    .await
}

async fn champion_match_history(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_champion_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let champion_id = int32_unvalidated_path(&raw_champion_id, &request_id)?;
    let page = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    rows(
        &database,
        "SELECT * FROM champion_match_ratings WHERE champion_id = $1 ORDER BY match_id DESC LIMIT $2 OFFSET $3",
        &[
            QueryParam::Int32(champion_id),
            QueryParam::Int64(page.per_page),
            QueryParam::Int64(page.offset),
        ],
        &request_id,
    )
    .await
}

async fn match_snapshots(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_match_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let match_id =
        parse_js_integer(&raw_match_id).ok_or_else(|| ApiError::validation("Invalid match ID"))?;
    rows(
        &database,
        &format!(
            "SELECT ms.*, {DISPLAY_NAME_SQL} as player_name \
             FROM match_rating_snapshots ms \
             JOIN players p ON p.id = ms.player_id \
             WHERE ms.match_id = $1 \
             ORDER BY {DISPLAY_NAME_SQL}"
        ),
        &[QueryParam::Int64(match_id)],
        &request_id,
    )
    .await
}

async fn player_snapshots(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let player_id = int64_unvalidated_path(&raw_player_id, &request_id)?;
    let page = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    let mut params = vec![QueryParam::Int64(player_id)];
    let mut clauses = vec!["player_id = $1".to_owned()];
    if let Some(raw_from) = truthy_query(&query, "from") {
        let from = raw_from
            .trim()
            .parse::<i64>()
            .map_err(|_| ApiError::internal(&request_id))?;
        params.push(QueryParam::Int64(from));
        clauses.push(format!("match_id >= ${}", params.len()));
    }
    if let Some(raw_queue_id) = truthy_query(&query, "queueId") {
        let queue_id = int32_query(raw_queue_id, &request_id)?;
        params.push(QueryParam::Int32(queue_id));
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM matches m WHERE m.match_id = ms.match_id AND m.queue_id = ${})",
            params.len()
        ));
    }
    params.push(QueryParam::Int64(page.per_page));
    let limit_index = params.len();
    params.push(QueryParam::Int64(page.offset));
    let offset_index = params.len();
    let sql = format!(
        "SELECT ms.*, {DISPLAY_NAME_SQL} as player_name \
         FROM match_rating_snapshots ms \
         JOIN players p ON p.id = ms.player_id \
         WHERE {} \
         ORDER BY ms.match_id DESC LIMIT ${limit_index} OFFSET ${offset_index}",
        clauses.join(" AND ")
    );
    rows(&database, &sql, &params, &request_id).await
}

async fn distribution(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let bin_size = truthy_query(&query, "binSize")
        .and_then(parse_js_integer)
        .filter(|value| *value != 0)
        .unwrap_or(50);
    rows(
        &database,
        "SELECT \
           FLOOR(mu / $1::BIGINT) * $1::BIGINT as bin_start, \
           (FLOOR(mu / $1::BIGINT) + 1) * $1::BIGINT as bin_end, \
           COUNT(*) as player_count, \
           ROUND(AVG(mu)::NUMERIC, 2) as avg_mu, \
           ROUND(AVG(phi)::NUMERIC, 2) as avg_phi \
         FROM player_queue_ratings \
         WHERE mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2 \
         GROUP BY FLOOR(mu / $1::BIGINT) \
         ORDER BY bin_start",
        &[QueryParam::Int64(bin_size)],
        &request_id,
    )
    .await
}

async fn volatility_history(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let player_id = positive_player_id(&raw_player_id)?;
    let values = database
        .query_json_params(
            "SELECT * FROM player_queue_ratings WHERE player_id = $1 \
             AND mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2 \
             ORDER BY queue_id",
            &[QueryParam::Int64(player_id)],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(Value::Array(
        values
            .into_iter()
            .map(|row| {
                json!({
                    "queue_id": row.get("queue_id").cloned().unwrap_or(Value::Null),
                    "volatility": row.get("volatility").cloned().unwrap_or(Value::Null),
                })
            })
            .collect(),
    )))
}

async fn rows(
    database: &Database,
    sql: &str,
    params: &[QueryParam],
    request_id: &RequestId,
) -> Result<Json<Value>, ApiError> {
    let rows = database
        .query_json_params(sql, params)
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(Json(Value::Array(rows)))
}

fn truthy_query<'a>(query: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    query
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

fn positive_player_id(raw: &str) -> Result<i64, ApiError> {
    let value = parse_js_integer(raw).ok_or_else(|| ApiError::validation("Invalid player ID"))?;
    if value <= 0 {
        return Err(ApiError::validation("Invalid player ID"));
    }
    Ok(value)
}

fn int32_query(raw: &str, request_id: &RequestId) -> Result<i32, ApiError> {
    parse_js_integer(raw)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| ApiError::internal(request_id))
}

fn int32_unvalidated_path(raw: &str, request_id: &RequestId) -> Result<i32, ApiError> {
    parse_js_integer(raw)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| ApiError::internal(request_id))
}

fn int64_unvalidated_path(raw: &str, request_id: &RequestId) -> Result<i64, ApiError> {
    parse_js_integer(raw).ok_or_else(|| ApiError::internal(request_id))
}
