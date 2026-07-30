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

use crate::{error::ApiError, request::RequestId, sql_compat::DISPLAY_NAME_SQL};

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/esports/leagues", get(leagues))
        .route("/esports/leagues/{id}", get(league))
        .route("/esports/teams", get(teams))
        .route("/esports/teams/{id}", get(team))
        .route("/esports/teams/{id}/players", get(team_players))
        .route("/esports/search", get(search))
        .with_state(database)
}

async fn leagues(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let page = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    let rows = if let Some(q) = truthy_query(&query, "q") {
        let pattern = format!("%{q}%");
        database
            .query_json(
                "SELECT * FROM esports_leagues WHERE league_name ILIKE $1 ORDER BY league_name LIMIT $2 OFFSET $3",
                &[&pattern, &page.per_page, &page.offset],
            )
            .await
    } else {
        database
            .query_json(
                "SELECT * FROM esports_leagues ORDER BY league_name LIMIT $1 OFFSET $2",
                &[&page.per_page, &page.offset],
            )
            .await
    }
    .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(Value::Array(rows)))
}

async fn league(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = integer_path_parameter(&raw_id, "Invalid league ID", &request_id)?;
    let league = database
        .one_json("SELECT * FROM esports_leagues WHERE league_id = $1", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("League not found", json!({ "id": id })))?;
    let teams = database
        .query_json(
            "SELECT * FROM esports_teams WHERE league_id = $1 ORDER BY team_name",
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(json!({ "league": league, "teams": teams })))
}

async fn teams(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let page = paginate(
        query.get("page").map(String::as_str),
        query.get("perPage").map(String::as_str),
    );
    let league_id = match truthy_query(&query, "leagueId") {
        Some(value) => Some(integer_query_parameter(value, &request_id)?),
        None => None,
    };
    let q = truthy_query(&query, "q").map(|value| format!("%{value}%"));

    let rows = match (league_id, q.as_ref()) {
        (Some(league_id), Some(pattern)) => {
            database
                .query_json(
                    "SELECT * FROM esports_teams WHERE league_id = $1 AND team_name ILIKE $2 ORDER BY team_name LIMIT $3 OFFSET $4",
                    &[&league_id, pattern, &page.per_page, &page.offset],
                )
                .await
        }
        (Some(league_id), None) => {
            database
                .query_json(
                    "SELECT * FROM esports_teams WHERE league_id = $1 ORDER BY team_name LIMIT $2 OFFSET $3",
                    &[&league_id, &page.per_page, &page.offset],
                )
                .await
        }
        (None, Some(pattern)) => {
            database
                .query_json(
                    "SELECT * FROM esports_teams WHERE team_name ILIKE $1 ORDER BY team_name LIMIT $2 OFFSET $3",
                    &[pattern, &page.per_page, &page.offset],
                )
                .await
        }
        (None, None) => {
            database
                .query_json(
                    "SELECT * FROM esports_teams ORDER BY team_name LIMIT $1 OFFSET $2",
                    &[&page.per_page, &page.offset],
                )
                .await
        }
    }
    .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(Value::Array(rows)))
}

async fn team(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = integer_path_parameter(&raw_id, "Invalid team ID", &request_id)?;
    let team = database
        .one_json("SELECT * FROM esports_teams WHERE team_id = $1", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("Team not found", json!({ "id": id })))?;
    let players = load_team_players(&database, id, &request_id).await?;
    Ok(Json(json!({ "team": team, "players": players })))
}

async fn team_players(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let id = integer_path_parameter(&raw_id, "Invalid team ID", &request_id)?;
    Ok(Json(Value::Array(
        load_team_players(&database, id, &request_id).await?,
    )))
}

async fn load_team_players(
    database: &Database,
    id: i32,
    request_id: &RequestId,
) -> Result<Vec<Value>, ApiError> {
    database
        .query_json(
            &format!(
                "SELECT etp.*, {DISPLAY_NAME_SQL} as player_name \
                 FROM esports_team_players etp \
                 JOIN players p ON p.id = etp.player_id \
                 WHERE etp.team_id = $1 \
                 ORDER BY {DISPLAY_NAME_SQL}, etp.player_id"
            ),
            &[&id],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))
}

async fn search(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let q = truthy_query(&query, "q")
        .ok_or_else(|| ApiError::validation("Missing required query param: q"))?;
    let pattern = format!("%{q}%");
    let rows = database
        .query_json(
            "SELECT * FROM esports_teams WHERE team_name ILIKE $1 ORDER BY team_name LIMIT 20",
            &[&pattern],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(Value::Array(rows)))
}

fn truthy_query<'a>(query: &'a HashMap<String, String>, key: &str) -> Option<&'a str> {
    query
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
}

fn integer_path_parameter(
    raw: &str,
    validation_message: &'static str,
    request_id: &RequestId,
) -> Result<i32, ApiError> {
    let parsed = parse_js_integer(raw).ok_or_else(|| ApiError::validation(validation_message))?;
    i32::try_from(parsed).map_err(|_| ApiError::internal(request_id))
}

fn integer_query_parameter(raw: &str, request_id: &RequestId) -> Result<i32, ApiError> {
    parse_js_integer(raw)
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| ApiError::internal(request_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_truthiness_matches_string_query_values() {
        let mut query = HashMap::new();
        query.insert("q".to_owned(), String::new());
        assert_eq!(truthy_query(&query, "q"), None);
        query.insert("q".to_owned(), "0".to_owned());
        assert_eq!(truthy_query(&query, "q"), Some("0"));
    }
}
