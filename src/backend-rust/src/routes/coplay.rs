use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    response::Response,
    routing::get,
};
use paladinscat_core::{
    database::{Database, DatabaseError, QueryParam},
    web_compat::{paginate, parse_js_integer},
};
use serde_json::{Value, json};

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::{
        MAJOR_DIRECTORY_FRESH_SECONDS, MAJOR_DIRECTORY_STALE_SECONDS, RouteCache,
        cached_database_json, canonical_route_cache_url,
    },
};

pub const ROUTE_COUNT: usize = 7;

pub fn router(database: Database, route_cache: RouteCache) -> Router {
    Router::new()
        .route("/coplay/parties", get(parties))
        .route("/coplay/teammates/{player_id}", get(teammates))
        .route("/coplay/opponents/{player_id}", get(opponents))
        .route("/coplay/party/{player_id}", get(party))
        .route("/coplay/pair/{source_id}/{target_id}", get(pair))
        .route("/coplay/stats/{player_id}", get(stats))
        .route("/coplay/top-pairs", get(top_pairs))
        .with_state(database)
        .layer(Extension(route_cache))
}

async fn parties(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Extension(route_cache): Extension<RouteCache>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let kind = query
        .get("kind")
        .map_or("pairs".to_owned(), |value| value.to_lowercase());
    if kind != "pairs" && kind != "stacks" {
        return Err(ApiError::validation("kind must be pairs or stacks"));
    }
    let key = format!(
        "route:coplay:parties:v1:{}",
        canonical_route_cache_url(&uri)
    );
    cached_database_json(
        route_cache,
        key,
        MAJOR_DIRECTORY_FRESH_SECONDS,
        MAJOR_DIRECTORY_STALE_SECONDS,
        &request_id,
        move || {
            let database = database.clone();
            let query = query.clone();
            async move { load_parties(database, &query).await }
        },
    )
    .await
}

async fn load_parties(
    database: Database,
    query: &HashMap<String, String>,
) -> Result<Value, DatabaseError> {
    let page = paginate(
        query.get("page").map(String::as_str),
        query
            .get("perPage")
            .or_else(|| query.get("limit"))
            .map(String::as_str),
    );
    let search = query.get("q").map_or("", String::as_str).trim();
    let kind = query
        .get("kind")
        .map_or("pairs".to_owned(), |value| value.to_lowercase());
    if kind == "stacks" {
        let stack_size = query
            .get("size")
            .and_then(|value| parse_js_integer(value))
            .filter(|value| (2..=5).contains(value))
            .map(|value| value as i32);
        let mut params = Vec::new();
        let mut clauses = Vec::new();
        if !search.is_empty() {
            params.push(QueryParam::Text(format!("%{search}%")));
            clauses.push(format!(
                "EXISTS (\
                   SELECT 1 \
                   FROM unnest(pss.player_ids) AS searched(player_id) \
                   JOIN players searched_player ON searched_player.id = searched.player_id \
                   WHERE searched_player.name ILIKE ${}\
                 )",
                params.len()
            ));
        }
        if let Some(stack_size) = stack_size {
            params.push(QueryParam::Int16(stack_size as i16));
            clauses.push(format!("pss.stack_size = ${}", params.len()));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        };
        params.push(QueryParam::Int64(page.per_page));
        let limit_index = params.len();
        params.push(QueryParam::Int64(page.offset));
        let offset_index = params.len();
        let sql = format!(
            "SELECT \
               pss.group_key, \
               pss.player_ids, \
               members.player_names, \
               pss.stack_size, \
               pss.match_count, \
               pss.first_seen, \
               pss.last_seen, \
               COUNT(*) OVER()::INT AS total_count \
             FROM party_stack_stats pss \
             JOIN LATERAL (\
               SELECT array_agg(\
                 COALESCE(p.name, 'Player ' || member.player_id::TEXT) \
                 ORDER BY member.ordinal\
               ) AS player_names \
               FROM unnest(pss.player_ids) WITH ORDINALITY AS member(player_id, ordinal) \
               LEFT JOIN players p ON p.id = member.player_id\
             ) members ON TRUE \
             {where_clause} \
             ORDER BY pss.stack_size DESC, pss.match_count DESC, \
                      pss.last_seen DESC, pss.group_key \
             LIMIT ${limit_index} OFFSET ${offset_index}"
        );
        return database
            .query_json_params(&sql, &params)
            .await
            .map(Value::Array);
    }

    let mut params = Vec::new();
    let search_clause = if search.is_empty() {
        String::new()
    } else {
        params.push(QueryParam::Text(format!("%{search}%")));
        format!(
            "AND (source.name ILIKE ${0} OR target.name ILIKE ${0})",
            params.len()
        )
    };
    params.push(QueryParam::Int64(page.per_page));
    let limit_index = params.len();
    params.push(QueryParam::Int64(page.offset));
    let offset_index = params.len();
    let sql = format!(
        "SELECT \
           pps.player_low_id AS source_player_id, \
           source.name AS source_player_name, \
           pps.player_high_id AS target_player_id, \
           target.name AS target_player_name, \
           pps.match_count, \
           pps.first_seen, \
           pps.last_seen, \
           COUNT(*) OVER()::INT AS total_count \
         FROM party_pair_stats pps \
         JOIN players source ON source.id = pps.player_low_id \
         JOIN players target ON target.id = pps.player_high_id \
         WHERE TRUE {search_clause} \
         ORDER BY pps.match_count DESC, pps.last_seen DESC, \
                  pps.player_low_id, pps.player_high_id \
         LIMIT ${limit_index} OFFSET ${offset_index}"
    );
    database
        .query_json_params(&sql, &params)
        .await
        .map(Value::Array)
}

async fn teammates(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    relationship_rows(
        &database,
        &request_id,
        &raw_player_id,
        query.get("limit").map(String::as_str),
        true,
    )
    .await
}

async fn opponents(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    relationship_rows(
        &database,
        &request_id,
        &raw_player_id,
        query.get("limit").map(String::as_str),
        false,
    )
    .await
}

async fn relationship_rows(
    database: &Database,
    request_id: &RequestId,
    raw_player_id: &str,
    raw_limit: Option<&str>,
    same_team: bool,
) -> Result<Json<Value>, ApiError> {
    let player_id = positive_player_id(raw_player_id)?;
    let limit = legacy_limit(raw_limit, 20, 100);
    rows(
        database,
        "SELECT \
           $1::BIGINT AS player_id, \
           CASE WHEN pr.source_player_id = $1 \
             THEN pr.target_player_id ELSE pr.source_player_id END AS other_player_id, \
           p2.name AS other_player_name, \
           pr.source_player_id, \
           pr.target_player_id, \
           pr.same_team, \
           pr.same_party, \
           pr.count AS match_count, \
           pr.first_seen, \
           pr.last_seen \
         FROM player_relationships pr \
         JOIN players p2 ON p2.id = CASE WHEN pr.source_player_id = $1 \
           THEN pr.target_player_id ELSE pr.source_player_id END \
         WHERE (pr.source_player_id = $1 OR pr.target_player_id = $1) \
           AND pr.same_team = $3 \
         ORDER BY pr.count DESC, pr.last_seen DESC \
         LIMIT $2",
        &[
            QueryParam::Int64(player_id),
            QueryParam::Int64(limit),
            QueryParam::Bool(same_team),
        ],
        request_id,
    )
    .await
}

async fn party(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let player_id = positive_player_id(&raw_player_id)?;
    let limit = legacy_limit(query.get("limit").map(String::as_str), 20, 100);
    rows(
        &database,
        "SELECT \
           $1::BIGINT AS player_id, \
           CASE WHEN pps.player_low_id = $1 \
             THEN pps.player_high_id ELSE pps.player_low_id END AS other_player_id, \
           p2.name AS other_player_name, \
           pps.player_low_id AS source_player_id, \
           pps.player_high_id AS target_player_id, \
           true AS same_team, \
           true AS same_party, \
           pps.match_count, \
           pps.first_seen, \
           pps.last_seen \
         FROM party_pair_stats pps \
         JOIN players p2 ON p2.id = CASE WHEN pps.player_low_id = $1 \
           THEN pps.player_high_id ELSE pps.player_low_id END \
         WHERE pps.player_low_id = $1 OR pps.player_high_id = $1 \
         ORDER BY pps.match_count DESC, pps.last_seen DESC \
         LIMIT $2",
        &[QueryParam::Int64(player_id), QueryParam::Int64(limit)],
        &request_id,
    )
    .await
}

async fn pair(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path((raw_source_id, raw_target_id)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    let source_id = positive_player_id(&raw_source_id)
        .map_err(|_| ApiError::validation("Invalid player IDs"))?;
    let target_id = positive_player_id(&raw_target_id)
        .map_err(|_| ApiError::validation("Invalid player IDs"))?;
    let pair_source_id = source_id.min(target_id);
    let pair_target_id = source_id.max(target_id);
    let params = [
        QueryParam::Int64(pair_source_id),
        QueryParam::Int64(pair_target_id),
    ];
    let teammate = first_row(
        &database,
        "SELECT * FROM player_relationships \
         WHERE source_player_id = $1 AND target_player_id = $2 AND same_team = true",
        &params,
        &request_id,
    )
    .await?;
    let opponent = first_row(
        &database,
        "SELECT * FROM player_relationships \
         WHERE source_player_id = $1 AND target_player_id = $2 AND same_team = false",
        &params,
        &request_id,
    )
    .await?;
    let party = first_row(
        &database,
        "SELECT match_count, first_seen, last_seen \
         FROM party_pair_stats \
         WHERE player_low_id = $1 AND player_high_id = $2",
        &params,
        &request_id,
    )
    .await?;
    Ok(Json(json!({
        "source_player_id": source_id,
        "target_player_id": target_id,
        "teammate": teammate,
        "opponent": opponent,
        "party": party,
    })))
}

async fn stats(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_player_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let player_id = positive_player_id(&raw_player_id)?;
    rows(
        &database,
        "SELECT \
           $1::BIGINT AS player_id, \
           CASE WHEN mv.source_player_id = $1 \
             THEN mv.target_player_id ELSE mv.source_player_id END AS other_player_id, \
           p2.name AS other_player_name, \
           mv.source_player_id, \
           mv.target_player_id, \
           mv.same_team, \
           mv.times_together, \
           COALESCE(pps.match_count, 0) AS times_in_party, \
           mv.first_seen, \
           mv.last_seen \
         FROM mv_player_coplay_stats mv \
         JOIN players p2 ON p2.id = CASE WHEN mv.source_player_id = $1 \
           THEN mv.target_player_id ELSE mv.source_player_id END \
         LEFT JOIN party_pair_stats pps \
           ON pps.player_low_id = mv.source_player_id \
          AND pps.player_high_id = mv.target_player_id \
         WHERE mv.source_player_id = $1 OR mv.target_player_id = $1 \
         ORDER BY mv.times_together DESC, mv.last_seen DESC",
        &[QueryParam::Int64(player_id)],
        &request_id,
    )
    .await
}

async fn top_pairs(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);
    let where_clause = match query.get("sameTeam").map(String::as_str) {
        Some("true") => " WHERE pr.same_team = true",
        Some("false") => " WHERE pr.same_team = false",
        _ => "",
    };
    let sql = format!(
        "SELECT pr.source_player_id, pr.target_player_id, \
                p1.name as source_player_name, p2.name as target_player_name, \
                pr.count AS match_count, pr.last_seen, pr.same_team \
         FROM player_relationships pr \
         JOIN players p1 ON p1.id = pr.source_player_id \
         JOIN players p2 ON p2.id = pr.target_player_id\
         {where_clause} \
         ORDER BY pr.count DESC, pr.last_seen DESC, \
                  pr.source_player_id, pr.target_player_id, pr.same_team \
         LIMIT $1"
    );
    rows(&database, &sql, &[QueryParam::Int64(limit)], &request_id).await
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

async fn first_row(
    database: &Database,
    sql: &str,
    params: &[QueryParam],
    request_id: &RequestId,
) -> Result<Value, ApiError> {
    database
        .one_json_params(sql, params)
        .await
        .map(|row| row.unwrap_or(Value::Null))
        .map_err(|error| ApiError::database(error, request_id))
}

fn positive_player_id(raw: &str) -> Result<i64, ApiError> {
    let value = parse_js_integer(raw).ok_or_else(|| ApiError::validation("Invalid player ID"))?;
    if value <= 0 {
        return Err(ApiError::validation("Invalid player ID"));
    }
    Ok(value)
}

fn legacy_limit(raw: Option<&str>, default: i64, maximum: i64) -> i64 {
    match raw.and_then(parse_js_integer) {
        None | Some(0) => default,
        Some(value) => value.min(maximum),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn party_directory_uses_the_shared_stale_cache() {
        let source = include_str!("coplay.rs");
        let route = source
            .split_once("async fn parties(")
            .and_then(|(_, rest)| {
                rest.split_once("async fn teammates(")
                    .map(|(route, _)| route)
            })
            .expect("party directory route");
        assert!(route.contains("cached_database_json("));
        assert!(route.contains("canonical_route_cache_url"));
        assert!(route.contains("MAJOR_DIRECTORY_STALE_SECONDS"));
    }

    #[test]
    fn legacy_limits_match_javascript_truthiness_and_upper_bounds() {
        assert_eq!(legacy_limit(None, 20, 100), 20);
        assert_eq!(legacy_limit(Some("invalid"), 20, 100), 20);
        assert_eq!(legacy_limit(Some("0"), 20, 100), 20);
        assert_eq!(legacy_limit(Some("150tail"), 20, 100), 100);
        assert_eq!(legacy_limit(Some("-2"), 20, 100), -2);
    }
}
