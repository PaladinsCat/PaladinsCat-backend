use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::get,
};
use paladinscat_core::{
    cache::RedisCache,
    config::BackendConfig,
    database::{Database, QueryParam},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    error::ApiError, request::RequestId, security::client_rate_limit_identity,
    workers::relay::WorkerRelayClient,
};

pub const ROUTE_COUNT: usize = 5;

const LIVE_LOOKUP_TTL_SECONDS: u64 = 30;
const LIVE_PENDING_TTL_SECONDS: u64 = 10;

#[derive(Clone)]
struct LiveState {
    database: Database,
    redis: RedisCache,
    relay: Option<WorkerRelayClient>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PageQuery {
    page: Option<String>,
    per_page: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct LimitQuery {
    limit: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LiveLookupCache {
    state: String,
    #[serde(rename = "matchId", skip_serializing_if = "Option::is_none")]
    match_id: Option<i64>,
}

pub fn router(database: Database, redis: RedisCache, config: Arc<BackendConfig>) -> Router {
    let relay = WorkerRelayClient::new(&config).ok();
    Router::new()
        .route("/live/matches", get(live_matches))
        .route("/live/matches/{match_id}", get(live_match))
        .route("/live/players/{player_id}", get(player_live_match))
        .route("/live/drop-hack-suspects", get(drop_hack_suspects))
        .route("/live/ended", get(ended_matches))
        .with_state(LiveState {
            database,
            redis,
            relay,
        })
}

async fn live_matches(
    State(state): State<LiveState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PageQuery>,
) -> Result<Response, ApiError> {
    let page = positive_js_integer(query.page.as_deref(), 1);
    let per_page = positive_js_integer(query.per_page.as_deref(), 20).min(100);
    let offset = (page - 1).saturating_mul(per_page);
    let (sql, params) = match query.status {
        Some(status) => (
            "SELECT * FROM live_matches WHERE status = $1 \
             ORDER BY detected_at DESC LIMIT $2 OFFSET $3",
            vec![
                QueryParam::Text(status),
                QueryParam::Int64(per_page),
                QueryParam::Int64(offset),
            ],
        ),
        None => (
            "SELECT * FROM live_matches \
             ORDER BY detected_at DESC LIMIT $1 OFFSET $2",
            vec![QueryParam::Int64(per_page), QueryParam::Int64(offset)],
        ),
    };
    let rows = state
        .database
        .query_json_params(sql, &params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok((StatusCode::OK, Json(Value::Array(rows))).into_response())
}

async fn live_match(
    State(state): State<LiveState>,
    Extension(request_id): Extension<RequestId>,
    Path(match_id): Path<String>,
) -> Result<Response, ApiError> {
    let Some(match_id) = parse_integer(&match_id) else {
        return Err(ApiError::validation("Invalid match ID"));
    };
    let Some(match_row) = state
        .database
        .one_json(
            "SELECT * FROM live_matches WHERE match_id = $1",
            &[&match_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
    else {
        return Err(ApiError::not_found(
            "Live match not found",
            json!({ "matchId": match_id }),
        ));
    };
    let players = enriched_players(&state.database, match_id, &request_id).await?;
    Ok((
        StatusCode::OK,
        Json(json!({ "match": match_row, "players": players })),
    )
        .into_response())
}

async fn player_live_match(
    State(state): State<LiveState>,
    Extension(request_id): Extension<RequestId>,
    Path(player_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(player_id) = parse_integer(&player_id).filter(|value| *value > 0) else {
        return Err(ApiError::validation("Invalid player ID"));
    };
    let identity = request_identity(&headers);
    let lookup_limit = state
        .redis
        .check_rate_limit(&format!("live-player:{identity}"), 12, 60_000, true)
        .await;
    if !lookup_limit.allowed {
        return Ok((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({
                "error": {
                    "code": "RATE_LIMITED",
                    "message": "Too many live-match lookups. Try again shortly."
                },
                "retryAfter": retry_after_seconds(lookup_limit.reset_at_ms)
            })),
        )
            .into_response());
    }

    let resolution = resolve_player_live_match(
        &state.database,
        &state.redis,
        state.relay.as_ref(),
        player_id,
        &identity,
        &request_id,
        true,
    )
    .await?;
    let mut response = (StatusCode::OK, Json(resolution.payload)).into_response();
    if let Some(headers) = resolution.vendor_headers {
        headers.apply(&mut response);
    }
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    Ok(response)
}

async fn drop_hack_suspects(
    State(state): State<LiveState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<LimitQuery>,
) -> Result<Response, ApiError> {
    let limit = js_truthy_integer(query.limit.as_deref(), 50);
    let rows = state
        .database
        .query_json(
            "SELECT * FROM drop_hack_suspects \
             WHERE incident_count > 1 \
             ORDER BY incident_count DESC LIMIT $1",
            &[&limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok((StatusCode::OK, Json(Value::Array(rows))).into_response())
}

async fn ended_matches(
    State(state): State<LiveState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<LimitQuery>,
) -> Result<Response, ApiError> {
    let limit = js_truthy_integer(query.limit.as_deref(), 20).min(100);
    let rows = state
        .database
        .query_json(
            "SELECT * FROM live_matches WHERE status = 'ended' \
             ORDER BY ended_at DESC LIMIT $1",
            &[&limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok((StatusCode::OK, Json(Value::Array(rows))).into_response())
}

pub(crate) async fn resolve_player_live_match(
    database: &Database,
    redis: &RedisCache,
    relay: Option<&WorkerRelayClient>,
    player_id: i64,
    identity: &str,
    request_id: &RequestId,
    enriched: bool,
) -> Result<LiveResolution, ApiError> {
    let cache_key = format!("live_match_lookup:{player_id}");
    if let Some(cached) = redis.get::<LiveLookupCache>(&cache_key).await {
        match cached.state.as_str() {
            "not_live" => {
                return Ok(LiveResolution::cached(
                    json!({ "match": null, "players": [], "player_id": player_id }),
                ));
            }
            "pending" => {
                return Ok(LiveResolution::cached(json!({
                    "match": null,
                    "players": [],
                    "player_id": player_id,
                    "pending": true,
                    "message": "Live lobby details are not ready yet. Try again shortly."
                })));
            }
            "live" => {
                if let Some(match_id) = cached.match_id
                    && let Some(payload) = stored_player_live_match(
                        database,
                        player_id,
                        Some(match_id),
                        request_id,
                        enriched,
                    )
                    .await?
                {
                    return Ok(LiveResolution::cached(payload));
                }
            }
            _ => {}
        }
    }
    let Some(relay) = relay else {
        return Err(ApiError::internal(request_id));
    };
    let mut rate_headers =
        Some(vendor_guard(redis, identity, "live-player-status", player_id, 30_000, 8).await?);
    let statuses = relay
        .call_value(
            "getPlayerStatus",
            vec![json!(player_id)],
            "rust_live_match_lookup",
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "live player status relay call failed");
            ApiError::internal(request_id)
        })?;
    dump_raw(
        relay,
        "getplayerstatus",
        "player_status",
        player_id,
        statuses.clone(),
    )
    .await;
    let status_rows = value_rows(&statuses);
    let status = status_rows
        .iter()
        .find(|row| text(row, &["ret_msg"]).trim().is_empty())
        .or_else(|| status_rows.first());
    let Some(status) = status.filter(|row| text(row, &["ret_msg"]).trim().is_empty()) else {
        redis
            .set(
                &cache_key,
                &LiveLookupCache {
                    state: "not_live".to_owned(),
                    match_id: None,
                },
                Some(LIVE_LOOKUP_TTL_SECONDS),
            )
            .await;
        return Ok(LiveResolution {
            payload: json!({ "match": null, "players": [], "player_id": player_id }),
            vendor_headers: rate_headers,
        });
    };
    let status_number = number(status, &["status"]).unwrap_or(0);
    let status_string = clean_text(status, &["status_string"]);
    let match_id = number(status, &["Match"]).filter(|value| *value > 0);
    let queue_id = number(status, &["match_queue_id"]).and_then(|value| i32::try_from(value).ok());
    let privacy = text(status, &["privacy_flag"]).eq_ignore_ascii_case("y");
    let personal = clean_text(status, &["personal_status_message"]);
    database
        .query_json(
            "INSERT INTO player_status (\
               player_id, status, status_string, current_match_id, queue_id, \
               privacy_flag, personal_status_message, updated_at\
             ) VALUES ($1::BIGINT,$2,$3,$4,$5,$6,$7,now()) \
             ON CONFLICT (player_id) DO UPDATE SET \
               status = EXCLUDED.status, status_string = EXCLUDED.status_string, \
               current_match_id = EXCLUDED.current_match_id, queue_id = EXCLUDED.queue_id, \
               privacy_flag = EXCLUDED.privacy_flag, \
               personal_status_message = EXCLUDED.personal_status_message, updated_at = now()",
            &[
                &player_id,
                &i32::try_from(status_number).unwrap_or_default(),
                &status_string,
                &match_id,
                &queue_id,
                &privacy,
                &personal,
            ],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let Some(match_id) = match_id else {
        redis
            .set(
                &cache_key,
                &LiveLookupCache {
                    state: "not_live".to_owned(),
                    match_id: None,
                },
                Some(LIVE_LOOKUP_TTL_SECONDS),
            )
            .await;
        return Ok(LiveResolution {
            payload: json!({ "match": null, "players": [], "player_id": player_id }),
            vendor_headers: rate_headers,
        });
    };

    rate_headers =
        Some(vendor_guard(redis, identity, "live-match-players", match_id, 10_000, 8).await?);
    let raw_players = relay
        .call_value(
            "getMatchPlayerDetails",
            vec![json!(match_id)],
            "rust_live_match_lookup",
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "live match players relay call failed");
            ApiError::internal(request_id)
        })?;
    let raw_players: Vec<Value> = value_rows(&raw_players)
        .into_iter()
        .filter(|row| text(row, &["ret_msg"]).trim().is_empty())
        .cloned()
        .collect();
    if raw_players.is_empty() {
        redis
            .set(
                &cache_key,
                &LiveLookupCache {
                    state: "pending".to_owned(),
                    match_id: Some(match_id),
                },
                Some(LIVE_PENDING_TTL_SECONDS),
            )
            .await;
        return Ok(LiveResolution {
            payload: json!({
                "match": null,
                "players": [],
                "player_id": player_id,
                "pending": true,
                "message": "Live lobby details are not ready yet. Try again shortly."
            }),
            vendor_headers: rate_headers,
        });
    }
    dump_raw(
        relay,
        "getmatchplayerdetails",
        "live_match",
        match_id,
        Value::Array(raw_players.clone()),
    )
    .await;
    persist_live_snapshot(
        database,
        match_id,
        player_id,
        queue_id,
        &raw_players,
        request_id,
    )
    .await?;
    redis
        .set(
            &cache_key,
            &LiveLookupCache {
                state: "live".to_owned(),
                match_id: Some(match_id),
            },
            Some(LIVE_LOOKUP_TTL_SECONDS),
        )
        .await;
    let payload =
        stored_player_live_match(database, player_id, Some(match_id), request_id, enriched)
            .await?
            .unwrap_or_else(|| json!({ "match": null, "players": [] }));
    Ok(LiveResolution {
        payload,
        vendor_headers: rate_headers,
    })
}

async fn stored_player_live_match(
    database: &Database,
    player_id: i64,
    match_id: Option<i64>,
    request_id: &RequestId,
    enriched: bool,
) -> Result<Option<Value>, ApiError> {
    let row = match match_id {
        Some(match_id) => {
            database
                .one_json(
                    "SELECT lm.* FROM live_matches lm \
                     JOIN live_match_players lmp ON lmp.match_id = lm.match_id \
                     WHERE lmp.player_id = $1 AND lm.status = 'active' AND lm.match_id = $2 \
                     ORDER BY lm.detected_at DESC LIMIT 1",
                    &[&player_id, &match_id],
                )
                .await
        }
        None => {
            database
                .one_json(
                    "SELECT lm.* FROM live_matches lm \
                     JOIN live_match_players lmp ON lmp.match_id = lm.match_id \
                     WHERE lmp.player_id = $1 AND lm.status = 'active' \
                     ORDER BY lm.detected_at DESC LIMIT 1",
                    &[&player_id],
                )
                .await
        }
    }
    .map_err(|error| ApiError::database(error, request_id))?;
    let Some(match_row) = row else {
        return Ok(None);
    };
    let match_id = match_row
        .get("match_id")
        .and_then(value_i64)
        .ok_or_else(|| ApiError::internal(request_id))?;
    let players = if enriched {
        enriched_players(database, match_id, request_id).await?
    } else {
        database
            .query_json(
                "SELECT * FROM live_match_players WHERE match_id=$1 ORDER BY task_force,id",
                &[&match_id],
            )
            .await
            .map_err(|error| ApiError::database(error, request_id))?
    };
    Ok(Some(json!({ "match": match_row, "players": players })))
}

async fn enriched_players(
    database: &Database,
    match_id: i64,
    request_id: &RequestId,
) -> Result<Vec<Value>, ApiError> {
    database
        .query_json(
            "SELECT lmp.match_id, lmp.player_id, \
             COALESCE(p.name, lmp.player_name) AS player_name, \
             lmp.champion_id, COALESCE(c.name, lmp.champion_name) AS champion_name, \
             lmp.skin_id, lmp.skin_name, lmp.account_level, lmp.mastery_level, \
             lmp.tier AS live_tier, lmp.tier_wins, lmp.tier_losses, lmp.task_force, \
             lmp.platform, (p.id IS NOT NULL) AS has_profile, p.level AS profile_level, \
             p.mastery_level AS profile_mastery_level, p.platform AS profile_platform, \
             p.region AS profile_region, p.hours_played AS profile_hours_played, \
             p.total_xp AS profile_total_xp, p.kbm_tier, COALESCE(lc.rank, p.kbm_rank) AS kbm_rank, \
             p.kbm_points, p.wins AS profile_wins, p.losses AS profile_losses, \
             (COALESCE(p.wins, 0) + COALESCE(p.losses, 0))::INT AS profile_matches, \
             CASE WHEN (COALESCE(p.wins, 0) + COALESCE(p.losses, 0)) > 0 \
               THEN ROUND(100.0 * COALESCE(p.wins, 0)::NUMERIC / \
                 (COALESCE(p.wins, 0) + COALESCE(p.losses, 0)), 1)::DOUBLE PRECISION \
               ELSE NULL END AS profile_win_rate, \
             COALESCE(lc.wins, p.kbm_wins) AS ranked_wins, \
             COALESCE(lc.losses, p.kbm_losses) AS ranked_losses, \
             (COALESCE(lc.wins, p.kbm_wins, 0) + COALESCE(lc.losses, p.kbm_losses, 0))::INT AS ranked_matches, \
             CASE WHEN (COALESCE(lc.wins, p.kbm_wins, 0) + COALESCE(lc.losses, p.kbm_losses, 0)) > 0 \
               THEN ROUND(100.0 * COALESCE(lc.wins, p.kbm_wins, 0)::NUMERIC / \
                 (COALESCE(lc.wins, p.kbm_wins, 0) + COALESCE(lc.losses, p.kbm_losses, 0)), 1)::DOUBLE PRECISION \
               ELSE NULL END AS ranked_win_rate, \
             p.total_matches, p.total_wins, p.total_losses, p.avg_dpm, p.avg_hpm, p.avg_mpm, \
             pqr.mu::DOUBLE PRECISION AS queue_elo, pqr.phi::DOUBLE PRECISION AS queue_phi, \
             pcr.mu::DOUBLE PRECISION AS champion_elo, pcr.phi::DOUBLE PRECISION AS champion_phi \
             FROM live_match_players lmp \
             JOIN live_matches lm ON lm.match_id = lmp.match_id \
             LEFT JOIN players p ON p.id = lmp.player_id \
             LEFT JOIN leaderboard_current lc ON lc.player_id = lmp.player_id \
             LEFT JOIN champions c ON c.id = lmp.champion_id \
             LEFT JOIN player_queue_ratings pqr ON pqr.player_id = lmp.player_id \
               AND pqr.queue_id = lm.queue_id \
               AND pqr.mu BETWEEN 0 AND 3500 AND pqr.phi BETWEEN 1 AND 350 \
               AND pqr.volatility BETWEEN 0.001 AND 0.2 \
             LEFT JOIN player_champion_ratings pcr ON pcr.player_id = lmp.player_id \
               AND pcr.champion_id = lmp.champion_id \
               AND pcr.mu BETWEEN 0 AND 3500 AND pcr.phi BETWEEN 1 AND 350 \
               AND pcr.volatility BETWEEN 0.001 AND 0.2 \
             WHERE lmp.match_id = $1 \
             ORDER BY lmp.task_force, COALESCE(p.name, lmp.player_name), lmp.player_id",
            &[&match_id],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))
}

async fn persist_live_snapshot(
    database: &Database,
    match_id: i64,
    source_player_id: i64,
    status_queue_id: Option<i32>,
    raw_players: &[Value],
    request_id: &RequestId,
) -> Result<(), ApiError> {
    let first = raw_players.first().unwrap_or(&Value::Null);
    let queue_id = status_queue_id
        .map(i64::from)
        .or_else(|| {
            number(
                first,
                &["match_queue_id", "Match_Queue_Id", "Queue", "queue_id"],
            )
        })
        .unwrap_or(0);
    let map = text(first, &["mapGame", "Map_Game", "map"]);
    let region = {
        let value = text(first, &["playerRegion", "Region", "region"]);
        if value.is_empty() {
            "Unknown".to_owned()
        } else {
            value
        }
    };
    let mut client = database
        .connection()
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let transaction = client.transaction().await.map_err(|error| {
        tracing::error!(%error, "live snapshot transaction failed");
        ApiError::internal(request_id)
    })?;
    let queue_id = i32::try_from(queue_id).unwrap_or_default();
    transaction
        .execute(
            "INSERT INTO live_matches (\
               match_id, queue_id, region, map, detected_at, source_player_id, status, ended_at, dropped\
             ) VALUES ($1,$2,$3,$4,now(),$5,'active',NULL,false) \
             ON CONFLICT (match_id) DO UPDATE SET \
               queue_id = EXCLUDED.queue_id, region = EXCLUDED.region, map = EXCLUDED.map, \
               detected_at = now(), source_player_id = EXCLUDED.source_player_id, \
               status = 'active', ended_at = NULL, dropped = false",
            &[&match_id, &queue_id, &region, &map, &source_player_id],
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "live match upsert failed");
            ApiError::internal(request_id)
        })?;
    transaction
        .execute(
            "DELETE FROM live_match_players WHERE match_id = $1",
            &[&match_id],
        )
        .await
        .map_err(|error| {
            tracing::error!(%error, "live player snapshot replacement failed");
            ApiError::internal(request_id)
        })?;
    for (index, raw) in raw_players.iter().enumerate() {
        let resolved_player_id = number(raw, &["playerId", "player_id", "PlayerId"])
            .filter(|value| *value > 0)
            .unwrap_or(-((index as i64) + 1));
        let player_name = {
            let value = text(raw, &["playerName", "player_name", "PlayerName"]);
            if value.is_empty() {
                "Private Account".to_owned()
            } else {
                value
            }
        };
        let champion_id = int32(raw, &["ChampionId", "champion_id"]);
        let champion_name = text(raw, &["ChampionName", "champion_name"]);
        let skin_id = int32(raw, &["SkinId", "skin_id"]);
        let skin_name = text(raw, &["Skin", "skin_name"]);
        let account_level = int32(raw, &["Account_Level", "account_level"]);
        let mastery_level = int32(raw, &["Mastery_Level", "mastery_level"]);
        let tier = int32(raw, &["Tier", "tier"]);
        let tier_wins = int32(raw, &["tierWins", "tier_wins"]);
        let tier_losses = int32(raw, &["tierLosses", "tier_losses"]);
        let task_force = int32(raw, &["taskForce", "task_force", "TaskForce"]);
        let platform = int32(
            raw,
            &["playerPortalId", "player_portal_id", "PlayerPortalId"],
        );
        transaction
            .execute(
                "INSERT INTO live_match_players (\
                   match_id, player_id, player_name, champion_id, champion_name, \
                   skin_id, skin_name, account_level, mastery_level, tier, tier_wins, \
                   tier_losses, task_force, platform\
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
                &[
                    &match_id,
                    &resolved_player_id,
                    &player_name,
                    &champion_id,
                    &champion_name,
                    &skin_id,
                    &skin_name,
                    &account_level,
                    &mastery_level,
                    &tier,
                    &tier_wins,
                    &tier_losses,
                    &task_force,
                    &platform,
                ],
            )
            .await
            .map_err(|error| {
                tracing::error!(%error, "live player insert failed");
                ApiError::internal(request_id)
            })?;
    }
    transaction.commit().await.map_err(|error| {
        tracing::error!(%error, "live snapshot commit failed");
        ApiError::internal(request_id)
    })?;
    Ok(())
}

async fn dump_raw(
    relay: &WorkerRelayClient,
    endpoint: &str,
    entity_type: &str,
    entity_id: i64,
    raw_data: Value,
) {
    let payload = json!([{
        "endpoint": endpoint,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "raw_data": raw_data,
        "source": "player-current-match"
    }]);
    if let Err(error) = relay
        .call_value("dumpRawPayloads", vec![payload], "rust_live_match_lookup")
        .await
    {
        tracing::warn!(%error, "failed to preserve live lookup raw payload");
    }
}

pub(crate) async fn vendor_guard(
    redis: &RedisCache,
    identity: &str,
    scope: &str,
    entity: impl ToString,
    entity_window_ms: u64,
    client_limit: u64,
) -> Result<VendorRateLimitHeaders, ApiError> {
    let client = redis
        .check_rate_limit(
            &format!("vendor-fallback:{scope}:{identity}:public"),
            client_limit,
            60_000,
            false,
        )
        .await;
    if !client.backend_available {
        return Err(ApiError::request_security(
            StatusCode::SERVICE_UNAVAILABLE,
            "PROTECTION_UNAVAILABLE",
            "Live-data protection is temporarily unavailable. Cached database data remains available.",
            retry_after_seconds(client.reset_at_ms),
        ));
    }
    if !client.allowed {
        return Err(ApiError::request_security(
            StatusCode::TOO_MANY_REQUESTS,
            "VENDOR_RATE_LIMITED",
            "Too many live-data fallbacks. Please wait for the database buffer to refresh.",
            retry_after_seconds(client.reset_at_ms),
        ));
    }
    let global = redis
        .check_rate_limit("vendor-fallback:global", 180, 60_000, false)
        .await;
    if !global.backend_available {
        return Err(ApiError::request_security(
            StatusCode::SERVICE_UNAVAILABLE,
            "PROTECTION_UNAVAILABLE",
            "Live-data protection is temporarily unavailable. Cached database data remains available.",
            retry_after_seconds(global.reset_at_ms),
        ));
    }
    if !global.allowed {
        return Err(ApiError::request_security(
            StatusCode::TOO_MANY_REQUESTS,
            "VENDOR_GLOBAL_RATE_LIMITED",
            "The live-data fallback is busy. Cached database data remains available.",
            retry_after_seconds(global.reset_at_ms),
        ));
    }
    let entity_digest = format!(
        "{:x}",
        Sha256::digest(format!("{scope}:{}", entity.to_string()).as_bytes())
    );
    let entity_key = format!("vendor-fallback:entity:{}", &entity_digest[..32]);
    let entity = redis
        .check_rate_limit(&entity_key, 1, entity_window_ms, false)
        .await;
    if !entity.backend_available {
        return Err(ApiError::request_security(
            StatusCode::SERVICE_UNAVAILABLE,
            "PROTECTION_UNAVAILABLE",
            "Live-data protection is temporarily unavailable. Cached database data remains available.",
            retry_after_seconds(entity.reset_at_ms),
        ));
    }
    if !entity.allowed {
        return Err(ApiError::request_security(
            StatusCode::TOO_MANY_REQUESTS,
            "VENDOR_ENTITY_COOLDOWN",
            "A live-data attempt for this record already ran recently. Cached database data remains available.",
            retry_after_seconds(entity.reset_at_ms),
        ));
    }
    Ok(VendorRateLimitHeaders { client, global })
}

pub(crate) struct LiveResolution {
    pub(crate) payload: Value,
    pub(crate) vendor_headers: Option<VendorRateLimitHeaders>,
}

impl LiveResolution {
    fn cached(payload: Value) -> Self {
        Self {
            payload,
            vendor_headers: None,
        }
    }
}

pub(crate) struct VendorRateLimitHeaders {
    client: paladinscat_core::cache::RateLimitResult,
    global: paladinscat_core::cache::RateLimitResult,
}

impl VendorRateLimitHeaders {
    pub(crate) fn apply(&self, response: &mut Response) {
        for (name, value) in [
            ("x-vendor-ratelimit-limit", self.client.total),
            ("x-vendor-ratelimit-remaining", self.client.remaining),
            ("x-vendor-ratelimit-reset", self.client.reset_at_ms),
            ("x-vendor-global-ratelimit-limit", self.global.total),
            ("x-vendor-global-ratelimit-remaining", self.global.remaining),
            ("x-vendor-global-ratelimit-reset", self.global.reset_at_ms),
        ] {
            if let (Ok(name), Ok(value)) = (
                name.parse::<axum::http::HeaderName>(),
                HeaderValue::from_str(&value.to_string()),
            ) {
                response.headers_mut().insert(name, value);
            }
        }
    }
}

pub(crate) fn request_identity(headers: &HeaderMap) -> String {
    let address = headers
        .get("cf-connecting-ip")
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next_back())
        .map(str::trim)
        .unwrap_or("unknown");
    client_rate_limit_identity(address)
}

fn retry_after_seconds(reset_at_ms: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|value| value.as_millis() as u64)
        .unwrap_or_default();
    reset_at_ms.saturating_sub(now).div_ceil(1_000).max(1)
}

fn parse_integer(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok()
}

fn positive_js_integer(value: Option<&str>, fallback: i64) -> i64 {
    js_truthy_integer(value, fallback).max(1)
}

fn js_truthy_integer(value: Option<&str>, fallback: i64) -> i64 {
    value
        .and_then(paladinscat_core::web_compat::parse_js_integer)
        .filter(|value| *value != 0)
        .unwrap_or(fallback)
}

fn value_rows(value: &Value) -> Vec<&Value> {
    value
        .as_array()
        .map(|rows| rows.iter().filter(|row| row.is_object()).collect())
        .unwrap_or_else(|| value.is_object().then_some(value).into_iter().collect())
}

fn text(row: &Value, names: &[&str]) -> String {
    names
        .iter()
        .find_map(|name| row.get(*name))
        .map(|value| match value {
            Value::String(value) => value.clone(),
            Value::Null => String::new(),
            value => value.to_string(),
        })
        .unwrap_or_default()
}

fn clean_text(row: &Value, names: &[&str]) -> Option<String> {
    let value = text(row, names).replace('\0', "");
    (!value.is_empty()).then_some(value)
}

fn number(row: &Value, names: &[&str]) -> Option<i64> {
    names
        .iter()
        .find_map(|name| row.get(*name))
        .and_then(value_i64)
}

fn value_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn int32(row: &Value, names: &[&str]) -> i32 {
    number(row, names)
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_normalization_accepts_every_typescript_alias() {
        let row = json!({
            "PlayerId": "42",
            "ChampionId": 7,
            "playerPortalId": "22",
            "TaskForce": 2
        });
        assert_eq!(
            number(&row, &["playerId", "player_id", "PlayerId"]),
            Some(42)
        );
        assert_eq!(int32(&row, &["ChampionId", "champion_id"]), 7);
        assert_eq!(
            int32(
                &row,
                &["playerPortalId", "player_portal_id", "PlayerPortalId"]
            ),
            22
        );
        assert_eq!(int32(&row, &["taskForce", "task_force", "TaskForce"]), 2);
    }
}
