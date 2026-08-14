use std::collections::HashSet;

use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Extension, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use paladinscat_core::database::{Database, QueryParam};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{error::ApiError, request::RequestId};

pub const ROUTE_COUNT: usize = 9;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlayerExtQuery {
    page: Option<String>,
    per_page: Option<String>,
    q: Option<String>,
    ids: Option<String>,
    cheater: Option<String>,
    suspicious: Option<String>,
    region: Option<String>,
    platform: Option<String>,
    tier_min: Option<String>,
    tier_max: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct ReportBody {
    #[serde(rename = "type")]
    report_type: Option<String>,
    reason: Option<String>,
}

#[derive(Debug)]
struct UserSession {
    id: i32,
    is_admin: bool,
    is_approved: bool,
}

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/player-ext/name-history/{player_id}", get(name_history))
        .route("/player-ext/merges/{player_id}", get(account_merges))
        .route("/player-ext/status/{player_id}", get(player_status))
        .route(
            "/player-ext/achievements/{player_id}",
            get(player_achievements),
        )
        .route("/player-ext/private", get(private_accounts))
        .route("/player-ext/private/bulk", get(private_accounts_bulk))
        .route(
            "/player-ext/private/{private_id}",
            get(private_account_detail),
        )
        .route(
            "/player-ext/private/{private_id}/report",
            post(report_private_account),
        )
        .route("/player-ext/private-history", get(private_history))
        .route("/player-ext/bulk", get(players_bulk))
        .route("/player-ext/search", get(advanced_player_search))
        .with_state(database)
}

async fn name_history(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(player_id): Path<String>,
    Query(query): Query<PlayerExtQuery>,
) -> Result<Response, ApiError> {
    let player_id = parse_player_id(&player_id)?;
    let (_, per_page, offset) = paginate(&query);
    let rows = database
        .query_json(
            "SELECT * FROM player_name_history \
             WHERE player_id = $1 ORDER BY changed_at DESC LIMIT $2 OFFSET $3",
            &[&player_id, &per_page, &offset],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_array(rows))
}

async fn account_merges(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(player_id): Path<String>,
) -> Result<Response, ApiError> {
    let player_id = parse_player_id(&player_id)?;
    let rows = database
        .query_json(
            "SELECT * FROM player_account_merges \
             WHERE player_id = $1 OR merged_into_player_id = $1 \
             ORDER BY merged_at DESC",
            &[&player_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_array(rows))
}

async fn player_status(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(player_id): Path<String>,
) -> Result<Response, ApiError> {
    let player_id = parse_player_id(&player_id)?;
    let row = database
        .one_json_params(
            "SELECT * FROM player_status WHERE player_id = $1",
            &[integer_query_param(player_id)],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    match row {
        Some(row) => Ok((StatusCode::OK, Json(row)).into_response()),
        None => Err(ApiError::not_found(
            "Player status not found",
            json!({ "playerId": player_id }),
        )),
    }
}

async fn player_achievements(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(player_id): Path<String>,
) -> Result<Response, ApiError> {
    let player_id = parse_player_id(&player_id)?;
    let rows = database
        .query_json_params(
            "SELECT * FROM player_achievements \
             WHERE player_id = $1 ORDER BY achievement_id",
            &[integer_query_param(player_id)],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_array(rows))
}

async fn private_accounts(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerExtQuery>,
) -> Result<Response, ApiError> {
    let (_, per_page, offset) = paginate(&query);
    let search = query.q.as_deref().unwrap_or_default().trim();
    let mut params = Vec::new();
    let mut where_clause = " WHERE is_active".to_owned();
    if !search.is_empty() {
        params.push(QueryParam::Text(format!("%{search}%")));
        where_clause.push_str(&format!(
            " AND (alias ILIKE ${0} OR verified_name ILIKE ${0})",
            params.len()
        ));
    }
    match query.cheater.as_deref() {
        Some("true") => where_clause.push_str(" AND cheater"),
        Some("false") => where_clause.push_str(" AND NOT cheater"),
        _ => {}
    }
    if query.suspicious.as_deref() == Some("true") {
        where_clause.push_str(" AND sus_count > 0");
    }
    let sql = format!(
        "SELECT id, party_id, account_level, mastery_level, league_tier, league_points, \
         first_seen, last_seen, match_count, alias, verified_name, \
         COALESCE(verified_name, alias) AS display_name, \
         identity_status, identity_confidence, tracking_version, \
         cheater, cheater_reason, cheater_marked_at, sus_count, \
         COALESCE(( \
           SELECT jsonb_agg(reason_group ORDER BY reason_group.count DESC, reason_group.reason) \
           FROM ( \
             SELECT vote.reason, COUNT(*)::INT AS count \
             FROM private_account_community_votes vote \
             WHERE vote.private_player_id = players_private.id \
               AND vote.vote_type = 'suspicious' \
             GROUP BY vote.reason \
             ORDER BY COUNT(*) DESC, vote.reason LIMIT 3 \
           ) reason_group \
         ), '[]'::jsonb) AS top_reasons, \
         COUNT(*) OVER()::INT AS total_count \
         FROM players_private{where_clause} \
         ORDER BY last_seen DESC, id DESC \
         LIMIT ${} OFFSET ${}",
        params.len() + 1,
        params.len() + 2
    );
    params.push(QueryParam::Int64(per_page));
    params.push(QueryParam::Int64(offset));
    let rows = database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_array(rows))
}

async fn private_accounts_bulk(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerExtQuery>,
) -> Result<Response, ApiError> {
    let ids = bulk_ids(query.ids.as_deref(), 50);
    if ids.is_empty() {
        return Err(ApiError::validation("Missing or invalid ids parameter"));
    }
    let ids_i32 = ids
        .iter()
        .filter_map(|id| i32::try_from(*id).ok())
        .collect::<Vec<_>>();
    let accounts = database
        .query_json(
            "SELECT id, cheater, cheater_reason, cheater_marked_at, sus_count \
             FROM players_private WHERE id = ANY($1) AND is_active",
            &[&ids_i32],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let found = accounts
        .iter()
        .filter_map(|account| value_i64(account.get("id")))
        .collect::<HashSet<_>>();
    let not_found = ids
        .into_iter()
        .filter(|id| !found.contains(id))
        .collect::<Vec<_>>();
    let count = accounts.len();
    Ok((
        StatusCode::OK,
        Json(json!({
            "accounts": accounts,
            "count": count,
            "notFound": not_found,
        })),
    )
        .into_response())
}

async fn private_account_detail(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(private_id): Path<String>,
) -> Result<Response, ApiError> {
    let requested_private_id = private_id_value(&private_id)?;
    let Some(canonical_id) =
        canonical_private_id(&database, requested_private_id, &request_id).await?
    else {
        return Err(ApiError::not_found_without_details(
            "Private account not found",
        ));
    };
    let account = database
        .one_json(
            "SELECT id, party_id, account_level, mastery_level, league_tier, league_points, \
             first_seen, last_seen, match_count, alias, verified_name, \
             COALESCE(verified_name, alias) AS display_name, \
             identity_status, identity_confidence, tracking_version, \
             cheater, cheater_reason, cheater_marked_at, sus_count \
             FROM players_private WHERE id = $1 AND is_active",
            &[&canonical_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let Some(account) = account else {
        return Err(ApiError::not_found_without_details(
            "Private account not found",
        ));
    };
    let observations = database
        .query_json(
            "WITH ordered AS ( \
               SELECT o.*, \
                      lag(o.league_points) OVER ( \
                        ORDER BY o.entry_datetime, o.match_id, o.private_slot \
                      ) AS previous_league_points, \
                      lag(o.league_tier) OVER ( \
                        ORDER BY o.entry_datetime, o.match_id, o.private_slot \
                      ) AS previous_league_tier \
               FROM private_account_observations o \
               WHERE o.private_player_id = $1 \
             ), timeline AS ( \
               SELECT ordered.*, \
                      CASE WHEN league_tier = previous_league_tier \
                        THEN league_points - previous_league_points ELSE NULL END AS tp_delta \
               FROM ordered \
             ) \
             SELECT o.match_id, o.private_slot, o.entry_datetime, o.account_level, \
                    o.mastery_level, o.league_tier, o.league_points, o.tp_delta, \
                    o.win_status, o.champion_id, c.name AS champion_name, \
                    o.task_force, o.platform, o.source, o.resolution_status, \
                    o.resolution_confidence, o.resolution_reasons, \
                    m.map, m.queue_id, m.region, m.duration_seconds \
             FROM timeline o \
             LEFT JOIN matches m ON m.match_id = o.match_id \
             LEFT JOIN champions c ON c.id = o.champion_id \
             ORDER BY o.entry_datetime DESC, o.match_id DESC, o.private_slot DESC \
             LIMIT 250",
            &[&canonical_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok((
        StatusCode::OK,
        Json(json!({
            "account": account,
            "observations": observations,
            "requested_private_id": requested_private_id,
        })),
    )
        .into_response())
}

async fn report_private_account(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Path(private_id): Path<String>,
    request: Request,
) -> Result<Response, ApiError> {
    let requested_private_id = private_id_value(&private_id)?;
    // The TypeScript handler authenticates before it reads report fields.
    // Reading the raw request keeps a missing/invalid session from being
    // pre-empted by Axum's JSON content-type extractor.
    let session = require_user_session(&database, request.headers(), &request_id).await?;
    let is_json = request
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.to_ascii_lowercase().starts_with("application/json"));
    let body = if is_json {
        let bytes = to_bytes(request.into_body(), 1_048_576)
            .await
            .map_err(|_| ApiError::validation("Invalid request body"))?;
        if bytes.is_empty() {
            ReportBody::default()
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|_| ApiError::validation("Invalid request body"))?
        }
    } else {
        ReportBody::default()
    };
    let report_type = body.report_type.unwrap_or_default();
    let reason = body.reason.unwrap_or_default().trim().to_owned();
    if !matches!(report_type.as_str(), "suspicious" | "cheater") {
        return Err(ApiError::validation(
            "Private accounts support suspicious and cheater reports",
        ));
    }
    if reason.is_empty() {
        return Err(ApiError::validation("A reason is required"));
    }
    // JavaScript String.length counts UTF-16 code units, including surrogate
    // pairs. Preserve that boundary instead of counting Unicode scalar values.
    if reason.encode_utf16().count() > 2_000 {
        return Err(ApiError::validation(
            "reason must be at most 2000 characters",
        ));
    }
    if report_type == "cheater" && !session.is_admin && !session.is_approved {
        return Err(ApiError::coded(
            StatusCode::FORBIDDEN,
            "PERMISSION",
            "Action requires admin or approved status",
        ));
    }
    let Some(canonical_id) =
        canonical_private_id(&database, requested_private_id, &request_id).await?
    else {
        return Err(ApiError::not_found_without_details(
            "Private account not found",
        ));
    };

    if report_type == "suspicious" {
        let result = database
            .one_json(
                "WITH inserted_vote AS ( \
                   INSERT INTO private_account_community_votes ( \
                     private_player_id, user_id, vote_type, reason \
                   ) VALUES ($1, $2, 'suspicious', $3) \
                   ON CONFLICT (private_player_id, user_id, vote_type) DO NOTHING \
                   RETURNING id \
                 ), updated_account AS ( \
                   UPDATE players_private \
                   SET sus_count = sus_count + 1, updated_at = now() \
                   WHERE id = $1 AND EXISTS (SELECT 1 FROM inserted_vote) \
                   RETURNING sus_count AS count \
                 ) \
                 SELECT EXISTS (SELECT 1 FROM inserted_vote) AS created, \
                        (SELECT count FROM updated_account) AS count",
                &[&canonical_id, &session.id, &reason],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
            .unwrap_or_else(|| json!({ "created": false, "count": null }));
        let created = result
            .get("created")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let count = match value_i64(result.get("count")) {
            Some(count) => count,
            None => database
                .one_json(
                    "SELECT sus_count FROM players_private WHERE id = $1",
                    &[&canonical_id],
                )
                .await
                .map_err(|error| ApiError::database(error, &request_id))?
                .as_ref()
                .and_then(|row| value_i64(row.get("sus_count")))
                .unwrap_or(0),
        };
        return Ok((
            StatusCode::OK,
            Json(json!({
                "success": true,
                "message": if created {
                    "Private account reported as Suspicious"
                } else {
                    "You have already reported this private account as Suspicious"
                },
                "already_voted": !created,
                "private_id": canonical_id,
                "sus_count": count,
            })),
        )
            .into_response());
    }

    let result = database
        .one_json(
            "WITH inserted_vote AS ( \
               INSERT INTO private_account_community_votes ( \
                 private_player_id, user_id, vote_type, reason \
               ) VALUES ($1, $2, 'cheater', $3) \
               ON CONFLICT (private_player_id, user_id, vote_type) DO NOTHING \
               RETURNING id \
             ) \
             UPDATE players_private \
             SET cheater = TRUE, cheater_reason = $3, \
                 cheater_marked_at = COALESCE(cheater_marked_at, now()), updated_at = now() \
             WHERE id = $1 \
             RETURNING EXISTS (SELECT 1 FROM inserted_vote) AS created, cheater",
            &[&canonical_id, &session.id, &reason],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .unwrap_or_else(|| json!({ "created": false, "cheater": true }));
    let created = result
        .get("created")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Ok((
        StatusCode::OK,
        Json(json!({
            "success": true,
            "message": if created {
                "Private account confirmed as cheater"
            } else {
                "Cheater report already recorded for this private account"
            },
            "already_voted": !created,
            "private_id": canonical_id,
            "cheater": true,
        })),
    )
        .into_response())
}

async fn private_history(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerExtQuery>,
) -> Result<Response, ApiError> {
    let (_, per_page, offset) = paginate(&query);
    let rows = database
        .query_json(
            "SELECT * FROM players_private_history \
             ORDER BY recorded_at DESC LIMIT $1 OFFSET $2",
            &[&per_page, &offset],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_array(rows))
}

async fn players_bulk(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerExtQuery>,
) -> Result<Response, ApiError> {
    let ids = bulk_ids(query.ids.as_deref(), 50);
    if ids.is_empty() {
        return Err(ApiError::validation("Missing or invalid ids parameter"));
    }
    let rows = database
        .query_json(
            "SELECT id, name, level, region, platform, kbm_tier, kbm_points, \
             cheater, sus_count FROM players WHERE id = ANY($1)",
            &[&ids],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    // Node-postgres returns BIGINT player IDs as strings. The TypeScript route
    // compares those directly with numeric query IDs, so its legacy notFound
    // field retains every requested ID. Preserve that response contract until
    // both runtimes can change together.
    let not_found = ids.clone();
    let count = rows.len();
    let mut payload = json!({ "players": rows, "count": count });
    if !not_found.is_empty() {
        payload
            .as_object_mut()
            .expect("bulk response")
            .insert("notFound".to_owned(), json!(not_found));
    }
    Ok((StatusCode::OK, Json(payload)).into_response())
}

async fn advanced_player_search(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerExtQuery>,
) -> Result<Response, ApiError> {
    let (_, per_page, offset) = paginate(&query);
    let mut params = Vec::new();
    let mut conditions = Vec::new();
    if let Some(q) = query.q.as_deref().filter(|value| !value.is_empty()) {
        params.push(QueryParam::Text(format!("%{q}%")));
        conditions.push(format!("name ILIKE ${}", params.len()));
    }
    if let Some(region) = query.region {
        params.push(QueryParam::Text(region));
        conditions.push(format!("region = ${}", params.len()));
    }
    if let Some(platform) = query.platform {
        params.push(QueryParam::Text(platform));
        conditions.push(format!("platform = ${}", params.len()));
    }
    if let Some(tier_min) = query
        .tier_min
        .as_deref()
        .filter(|value| !value.is_empty())
        .and_then(paladinscat_core::web_compat::parse_js_integer)
    {
        params.push(integer_query_param(tier_min));
        conditions.push(format!("kbm_tier >= ${}", params.len()));
    }
    if let Some(tier_max) = query
        .tier_max
        .as_deref()
        .filter(|value| !value.is_empty())
        .and_then(paladinscat_core::web_compat::parse_js_integer)
    {
        params.push(integer_query_param(tier_max));
        conditions.push(format!("kbm_tier <= ${}", params.len()));
    }
    match query.cheater.as_deref() {
        Some("true") => {
            params.push(QueryParam::Bool(true));
            conditions.push(format!("cheater = ${}", params.len()));
        }
        Some("false") => {
            params.push(QueryParam::Bool(false));
            conditions.push(format!("cheater = ${}", params.len()));
        }
        _ => {}
    }
    let clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    let sql = format!(
        "SELECT id, name, level, region, platform, kbm_tier, kbm_points, \
         cheater, sus_count FROM players{clause} \
         ORDER BY kbm_points DESC LIMIT ${} OFFSET ${}",
        params.len() + 1,
        params.len() + 2
    );
    params.push(QueryParam::Int64(per_page));
    params.push(QueryParam::Int64(offset));
    let rows = database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_array(rows))
}

async fn canonical_private_id(
    database: &Database,
    private_id: i32,
    request_id: &RequestId,
) -> Result<Option<i32>, ApiError> {
    let row = database
        .one_json(
            "WITH RECURSIVE identity_chain AS ( \
               SELECT id, merged_into_id, is_active, 0 AS depth \
               FROM players_private WHERE id = $1 \
               UNION ALL \
               SELECT next.id, next.merged_into_id, next.is_active, chain.depth + 1 \
               FROM players_private next \
               JOIN identity_chain chain ON next.id = chain.merged_into_id \
               WHERE chain.depth < 16 \
             ) \
             SELECT id FROM identity_chain WHERE is_active \
             ORDER BY depth DESC LIMIT 1",
            &[&private_id],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(row
        .as_ref()
        .and_then(|row| value_i64(row.get("id")))
        .and_then(|id| i32::try_from(id).ok()))
}

async fn require_user_session(
    database: &Database,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<UserSession, ApiError> {
    let Some(token) = crate::routes::identity::session_token(headers) else {
        return Err(ApiError::coded(
            StatusCode::UNAUTHORIZED,
            "AUTH",
            "Authentication required",
        ));
    };
    let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    let row = database
        .one_json(
            "SELECT s.user_id, u.username, u.email, u.is_admin, u.is_approved, u.is_active \
             FROM sessions s JOIN users u ON u.id = s.user_id \
             WHERE s.token = $1 AND s.expires_at > now()",
            &[&token_hash],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let Some(row) = row.filter(|row| {
        row.get("is_active")
            .and_then(Value::as_bool)
            .unwrap_or(true)
    }) else {
        return Err(ApiError::coded(
            StatusCode::UNAUTHORIZED,
            "AUTH",
            "Authentication required",
        ));
    };
    let Some(id) = value_i64(row.get("user_id")).and_then(|id| i32::try_from(id).ok()) else {
        return Err(ApiError::coded(
            StatusCode::UNAUTHORIZED,
            "AUTH",
            "Authentication required",
        ));
    };
    Ok(UserSession {
        id,
        is_admin: row
            .get("is_admin")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        is_approved: row
            .get("is_approved")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    })
}

fn parse_player_id(value: &str) -> Result<i64, ApiError> {
    value
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Invalid player ID"))
}

fn private_id_value(value: &str) -> Result<i32, ApiError> {
    value
        .trim()
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Invalid private account ID"))
}

fn paginate(query: &PlayerExtQuery) -> (i64, i64, i64) {
    let page = query
        .page
        .as_deref()
        .and_then(paladinscat_core::web_compat::parse_js_integer)
        .filter(|value| *value != 0)
        .unwrap_or(1)
        .max(1);
    let per_page = query
        .per_page
        .as_deref()
        .and_then(paladinscat_core::web_compat::parse_js_integer)
        .filter(|value| *value != 0)
        .unwrap_or(20)
        .clamp(1, 100);
    (page, per_page, (page - 1).saturating_mul(per_page))
}

fn bulk_ids(value: Option<&str>, maximum: usize) -> Vec<i64> {
    value
        .unwrap_or_default()
        .split(',')
        .filter_map(|value| paladinscat_core::web_compat::parse_js_integer(value.trim()))
        .filter(|value| *value > 0)
        .take(maximum)
        .collect()
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn integer_query_param(value: i64) -> QueryParam {
    i32::try_from(value)
        .map(QueryParam::Int32)
        .unwrap_or(QueryParam::Int64(value))
}

fn json_array(rows: Vec<Value>) -> Response {
    (StatusCode::OK, Json(Value::Array(rows))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pagination_and_bulk_ids_match_javascript_prefix_integer_rules() {
        let query = PlayerExtQuery {
            page: Some("0".to_owned()),
            per_page: Some("500rows".to_owned()),
            ..PlayerExtQuery::default()
        };
        assert_eq!(paginate(&query), (1, 100, 0));
        assert_eq!(bulk_ids(Some("1, 2rows, nope, -4, 5"), 50), vec![1, 2, 5]);
    }
}
