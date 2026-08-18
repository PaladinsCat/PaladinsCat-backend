use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use paladinscat_core::{
    cache::RedisCache,
    config::BackendConfig,
    database::{Database, QueryParam},
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    error::ApiError,
    raw_hirez_audit::{RawHirezAudit, record_raw_hirez_response},
    request::RequestId,
    route_cache::{RouteCache, cached_database_value, now_millis},
    routes::live::{request_identity, resolve_player_live_match, vendor_guard},
    workers::{
        pipeline::CanonicalIngestPipeline,
        relay::WorkerRelayClient,
        requested_match::{RequestedMatchIngestor, RequestedMatchStatus},
    },
};

const RANKED_QUEUE_ID: i32 = 486;
const CASUAL_HOURLY_SQL: &str = "WITH rows AS ( \
  SELECT date,hour,queue_id,region,match_count::int AS total_matches \
  FROM match_count_discovery_region_hours \
  WHERE date::text IN ($1,$2) AND queue_id<>$3 AND UPPER(BTRIM(COALESCE(region,'')))<>'UNKNOWN' \
  UNION ALL SELECT d.source_date,d.source_hour,d.queue_id, \
  COALESCE(NULLIF(casual.region,''),NULLIF(special.region,''),NULLIF(d.region,''),'Unknown') AS region, \
  COUNT(DISTINCT d.match_id)::int AS total_matches \
  FROM match_count_discoveries d LEFT JOIN casual_matches casual ON casual.match_id=d.match_id \
  LEFT JOIN special_matches special ON special.match_id=d.match_id \
  WHERE d.source_date::text IN ($1,$2) AND d.queue_id<>$3 \
    AND UPPER(BTRIM(COALESCE(d.region,''))) IN ('','UNKNOWN') \
  GROUP BY 1,2,3,4 \
) SELECT date::text AS date,hour,queue_id,CASE LOWER(BTRIM(COALESCE(region,'Unknown'))) \
    WHEN 'north america' THEN 'NA' WHEN 'na' THEN 'NA' WHEN 'europe' THEN 'EU' WHEN 'eu' THEN 'EU' \
    WHEN 'brazil' THEN 'BR' WHEN 'br' THEN 'BR' WHEN 'southeast asia' THEN 'SEA' WHEN 'sea' THEN 'SEA' \
    WHEN 'australia' THEN 'OCE' WHEN 'oceania' THEN 'OCE' WHEN 'oce' THEN 'OCE' \
    WHEN 'japan' THEN 'JPN' WHEN 'jpn' THEN 'JPN' WHEN 'russia' THEN 'RUS' WHEN 'rus' THEN 'RUS' \
    WHEN 'south america' THEN 'SA' WHEN 'sa' THEN 'SA' WHEN 'asia' THEN 'ASIA' \
    ELSE COALESCE(NULLIF(BTRIM(region),''),'Unknown') END AS region, \
  SUM(total_matches)::int AS total_matches FROM rows \
GROUP BY 1,2,3,4 ORDER BY 1,2,3,4";
const MATCH_DETAIL_CACHE_VERSION: i32 = 18;
const ACTIVITY_OVERVIEW_FRESH_TTL_SECONDS: u64 = 600;
const ACTIVITY_OVERVIEW_STALE_TTL_SECONDS: u64 = 900;
const RECENT_MATCHES_FRESH_TTL_SECONDS: u64 = 60;
const RECENT_MATCHES_STALE_TTL_SECONDS: u64 = 15 * 60;

pub const ROUTE_COUNT: usize = 21;

#[derive(Clone)]
struct MatchesState {
    database: Database,
    redis: RedisCache,
    route_cache: RouteCache,
    relay: Option<WorkerRelayClient>,
    requested_match: Option<RequestedMatchIngestor>,
    canonical_ingest: Option<CanonicalIngestPipeline>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PullBody {
    queue_id: Option<Value>,
    from: Option<String>,
    #[allow(dead_code)]
    to: Option<String>,
    #[allow(dead_code)]
    region: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DiscoverBody {
    queue_id: Option<Value>,
    date: Option<Value>,
    hour: Option<Value>,
    #[serde(default)]
    force: bool,
}

#[derive(Debug, Deserialize)]
struct MatchCursor {
    at: String,
    id: i64,
}

pub fn router(
    database: Database,
    redis: RedisCache,
    route_cache: RouteCache,
    config: Arc<BackendConfig>,
) -> Router {
    let relay = WorkerRelayClient::new(&config).ok();
    let canonical_ingest = CanonicalIngestPipeline::new(database.clone(), &config).ok();
    let requested_match = relay.clone().map(|relay| {
        RequestedMatchIngestor::new(
            database.clone(),
            relay,
            Duration::from_millis(config.hirez_relay_timeout_ms),
        )
    });
    Router::new()
        .route("/matches/overview", get(overview))
        .route("/matches/dropped/summary", get(dropped_summary))
        .route("/matches/dropped/nonranked", get(dropped_nonranked))
        .route("/matches/dropped", get(dropped))
        .route("/matches/batch", get(batch))
        .route("/matches/recent", get(recent))
        .route("/matches/queue/{queue_id}", get(queue))
        .route("/matches/pull", post(pull))
        .route("/matches/discover", post(discover))
        .route(
            "/matches/live/drop-hack-suspects",
            get(live_drop_hack_suspects),
        )
        .route("/matches/live/{player_id}", get(live_player))
        .route("/matches/raw/discover", get(raw_discover))
        .route("/matches/raw/matchdetails", get(raw_matchdetails))
        .route("/matches/raw/demo", get(raw_demo))
        .route("/matches/raw/playerbatch", get(raw_playerbatch))
        .route("/matches/search", get(search))
        .route("/matches/bans", get(bans))
        .route("/matches/fact/{match_id}", get(fact))
        .route("/matches/hourly-stats", get(hourly_stats))
        .route("/matches/compositions", get(compositions))
        .route("/matches/{id}", get(match_detail))
        .with_state(MatchesState {
            database,
            redis,
            route_cache,
            relay,
            requested_match,
            canonical_ingest,
        })
}

async fn match_detail(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let match_id =
        positive_i64(Some(&id)).ok_or_else(|| ApiError::validation("Invalid match ID"))?;
    let payload = fetch_matches(&state, &[match_id], &request_id).await?;
    if payload
        .get("count")
        .and_then(Value::as_i64)
        .unwrap_or_default()
        > 0
    {
        return Ok((StatusCode::OK, Json(payload)).into_response());
    }
    let rate_headers = vendor_guard(
        &state.redis,
        &request_identity(&headers),
        "requested-match",
        match_id,
        120_000,
        8,
    )
    .await?;
    let Some(ingestor) = state.requested_match.as_ref() else {
        return Err(ApiError::internal(&request_id));
    };
    let result = ingestor.ingest(match_id).await;
    let mut response = match result.status {
        RequestedMatchStatus::Ready => {
            let payload = fetch_matches(&state, &[match_id], &request_id).await?;
            if payload
                .get("count")
                .and_then(Value::as_i64)
                .unwrap_or_default()
                > 0
            {
                (StatusCode::OK, Json(payload)).into_response()
            } else {
                match_recovery_error(
                    StatusCode::BAD_GATEWAY,
                    "MATCH_RECOVERY_FAILED",
                    format!(
                        "Match {match_id} could not be reconstructed from the Hi-Rez relay response."
                    ),
                    match_id,
                )
            }
        }
        RequestedMatchStatus::NotFound => match_recovery_error(
            StatusCode::NOT_FOUND,
            "MATCH_NOT_FOUND",
            format!("Match {match_id} was not found."),
            match_id,
        ),
        RequestedMatchStatus::ProcessingTimeout => match_recovery_error(
            StatusCode::GATEWAY_TIMEOUT,
            "MATCH_RECOVERY_TIMEOUT",
            format!("Match {match_id} recovery did not reach the durable fact boundary in time."),
            match_id,
        ),
        RequestedMatchStatus::RecoveryFailed => match_recovery_error(
            StatusCode::BAD_GATEWAY,
            "MATCH_RECOVERY_FAILED",
            format!("Match {match_id} could not be reconstructed from the Hi-Rez relay response."),
            match_id,
        ),
    };
    rate_headers.apply(&mut response);
    Ok(response)
}

fn match_recovery_error(
    status: StatusCode,
    code: &str,
    message: String,
    match_id: i64,
) -> Response {
    (
        status,
        Json(json!({
            "error": {
                "code": code,
                "message": message,
                "matchId": match_id
            }
        })),
    )
        .into_response()
}

async fn batch(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let Some(ids) = query.get("ids") else {
        return Err(ApiError::validation("Missing ids query parameter"));
    };
    let match_ids = ids
        .split(',')
        .filter_map(|part| part.trim().parse::<i64>().ok())
        .collect::<Vec<_>>();
    if match_ids.is_empty() {
        return Err(ApiError::validation("Invalid match IDs"));
    }
    if match_ids.len() > 10 {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "Maximum 10 match IDs per request",
                "received": match_ids.len()
            })),
        )
            .into_response());
    }
    Ok((
        StatusCode::OK,
        Json(fetch_matches(&state, &match_ids, &request_id).await?),
    )
        .into_response())
}

async fn fetch_matches(
    state: &MatchesState,
    match_ids: &[i64],
    request_id: &RequestId,
) -> Result<Value, ApiError> {
    let mut unique = match_ids
        .iter()
        .copied()
        .filter(|id| *id > 0)
        .collect::<Vec<_>>();
    unique.sort_unstable();
    unique.dedup();
    let mut matches = Vec::new();
    let mut not_found = Vec::new();
    for match_id in unique {
        match format_match(state, match_id, request_id).await? {
            Some(value) => matches.push(value),
            None => not_found.push(Value::from(match_id)),
        }
    }
    let mut payload = Map::new();
    payload.insert("count".to_owned(), Value::from(matches.len()));
    payload.insert("matches".to_owned(), Value::Array(matches));
    if !not_found.is_empty() {
        payload.insert("notFound".to_owned(), Value::Array(not_found));
    }
    Ok(Value::Object(payload))
}

async fn format_match(
    state: &MatchesState,
    match_id: i64,
    request_id: &RequestId,
) -> Result<Option<Value>, ApiError> {
    let cache_key = format!("match:v{MATCH_DETAIL_CACHE_VERSION}:{match_id}");
    if let Some(cached) = state.redis.get::<Value>(&cache_key).await {
        return Ok(Some(cached));
    }
    let ranked = state
        .database
        .one_json(
            "SELECT m.*,q.queue_name,q.stats_scope,q.participant_model, \
             COALESCE(q.stats_scope='custom' OR q.participant_model='custom',false) AS is_custom \
             FROM matches m LEFT JOIN queue_types q ON q.queue_id=m.queue_id WHERE m.match_id=$1",
            &[&match_id],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let (mut match_row, players, bans) = if let Some(match_row) = ranked {
        let entry_datetime = match_row
            .get("entry_datetime")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let players = state.database.query_json_params(
            "WITH party_groups AS ( \
               SELECT party_id FROM match_players WHERE match_id=$1 AND entry_datetime=$2::TEXT::TIMESTAMPTZ AND party_id>0 \
               GROUP BY party_id HAVING COUNT(*)>1), \
             party_numbered AS (SELECT party_id,ROW_NUMBER() OVER (ORDER BY party_id) AS party_num FROM party_groups) \
             SELECT mp.*,COALESCE(pn.party_num,0::BIGINT) AS party,c.name AS champion_name, \
             COALESCE(NULLIF(mp.private_player_id,0),o.private_player_id) AS private_player_id, \
             pp.alias AS private_account_alias,pp.verified_name AS private_account_verified_name, \
             COALESCE(pp.verified_name,pp.alias) AS private_account_display_name, \
             CASE WHEN mp.player_id>0 THEN jsonb_build_object( \
               'source','players_database','level',COALESCE(NULLIF(p.level,0),NULLIF(mp.account_level,0)), \
               'platform',COALESCE(NULLIF(p.platform,''),NULLIF(mp.platform,'')), \
               'region',COALESCE(NULLIF(NULLIF(p.region,''),'Unknown'),NULLIF(NULLIF(mp.region,''),'Unknown')), \
               'global_wins',p.wins,'global_losses',p.losses,'kbm_tier',COALESCE(p.kbm_tier,NULLIF(mp.league_tier,0)), \
               'kbm_points',COALESCE(p.kbm_points,mp.league_points),'queue_elo',pqr.mu,'champion_elo',pcr.mu,'cheater',COALESCE(p.cheater,false), \
               'sus_count',COALESCE(p.sus_count,0),'verified',EXISTS(SELECT 1 FROM users u WHERE u.linked_player_id=mp.player_id) \
             ) WHEN COALESCE(NULLIF(mp.private_player_id,0),o.private_player_id)>0 THEN jsonb_build_object( \
               'source','private_account_database','level',COALESCE(NULLIF(pp.account_level,0),NULLIF(mp.account_level,0)), \
               'platform',NULLIF(mp.platform,''),'kbm_tier',COALESCE(NULLIF(pp.league_tier,0),NULLIF(mp.league_tier,0)), \
               'kbm_points',COALESCE(pp.league_points,mp.league_points),'cheater',COALESCE(pp.cheater,false), \
               'sus_count',COALESCE(pp.sus_count,0),'verified',false) ELSE NULL END AS profile_snapshot \
             FROM match_players mp LEFT JOIN champions c ON c.id=mp.champion_id \
             LEFT JOIN players p ON p.id=mp.player_id \
             LEFT JOIN player_queue_ratings pqr ON pqr.player_id=mp.player_id AND pqr.queue_id=486 \
             LEFT JOIN player_champion_ratings pcr ON pcr.player_id=mp.player_id AND pcr.champion_id=mp.champion_id \
             LEFT JOIN private_account_observations o ON mp.player_id=0 AND o.match_id=mp.match_id \
               AND (o.private_slot=mp.private_slot OR (mp.private_slot=0 AND o.private_slot=1)) \
             LEFT JOIN players_private pp ON pp.id=COALESCE(NULLIF(mp.private_player_id,0),o.private_player_id) \
             LEFT JOIN party_numbered pn ON pn.party_id=mp.party_id \
             WHERE mp.match_id=$1 AND mp.entry_datetime=$2::TEXT::TIMESTAMPTZ \
             ORDER BY mp.task_force,mp.player_id,mp.private_slot",
            &[QueryParam::Int64(match_id),QueryParam::Text(entry_datetime)],
        ).await.map_err(|error| ApiError::database(error,request_id))?;
        let bans = state
            .database
            .query_json(
                "SELECT mb.match_id,mb.ban_slot,mb.champion_id,c.name AS champion_name \
                 FROM match_bans mb LEFT JOIN champions c ON c.id=mb.champion_id \
                 WHERE mb.match_id=$1 ORDER BY mb.ban_slot",
                &[&match_id],
            )
            .await
            .map_err(|error| ApiError::database(error, request_id))?;
        (match_row, players, bans)
    } else {
        let Some(match_row) = state.database.one_json(
            "SELECT m.match_id,m.entry_datetime,m.queue_id,false AS is_ranked,m.duration_seconds,m.region,m.map, \
             team1_score,team2_score,winning_task_force,false AS has_replay,quality<>'complete' AS broken, \
             false AS recovered,quality<>'complete' AS limited, \
             CASE WHEN quality='complete' THEN NULL ELSE quality END AS limited_reason, \
             source,ingested_at,quality,stats_eligible,q.queue_name,q.stats_scope,q.participant_model, \
             COALESCE(q.stats_scope='custom' OR q.participant_model='custom',false) AS is_custom \
             FROM casual_matches m LEFT JOIN queue_types q ON q.queue_id=m.queue_id WHERE m.match_id=$1 UNION ALL \
             SELECT m.match_id,m.entry_datetime,m.queue_id,false,m.duration_seconds,m.region,m.map,team1_score,team2_score, \
             winning_task_force,false,quality<>'complete',false,quality<>'complete', \
             CASE WHEN quality='complete' THEN NULL ELSE quality END,source,ingested_at,quality,stats_eligible, \
             CASE WHEN m.stats_scope='custom' OR m.participant_model='custom' OR q.stats_scope='custom' OR q.participant_model='custom' THEN 'Custom Match' ELSE q.queue_name END, \
             CASE WHEN q.stats_scope='custom' OR q.participant_model='custom' THEN 'custom' ELSE m.stats_scope END, \
             CASE WHEN q.stats_scope='custom' OR q.participant_model='custom' THEN 'custom' ELSE m.participant_model END, \
             (m.stats_scope='custom' OR m.participant_model='custom' OR q.stats_scope='custom' OR q.participant_model='custom') \
             FROM special_matches m LEFT JOIN queue_types q ON q.queue_id=m.queue_id WHERE m.match_id=$1 LIMIT 1",
            &[&match_id],
        ).await.map_err(|error| ApiError::database(error,request_id))? else {
            return Ok(None);
        };
        let players = state.database.query_json(
            "WITH facts AS ( \
               SELECT p.match_id,p.roster_slot,p.private_slot,p.player_id,p.private_player_id,p.player_name,p.champion_id, \
                 p.champion_name,p.task_force,p.win_status,p.kills,p.deaths,p.assists,p.damage_done_in_hand,p.damage,p.damage_taken, \
                 p.healing,p.mitigation,p.credits,p.objective_time,p.account_level,p.mastery_level,p.party_id,p.portal_id, \
                 p.portal_user_id,p.platform,p.participant_kind,p.source,m.duration_seconds \
               FROM casual_match_players p JOIN casual_matches m USING(match_id) WHERE p.match_id=$1 \
               UNION ALL SELECT p.match_id,p.roster_slot,p.private_slot,p.player_id,p.private_player_id,p.player_name,p.champion_id, \
                 p.champion_name,p.task_force,p.win_status,p.kills,p.deaths,p.assists,p.damage_done_in_hand,p.damage,p.damage_taken, \
                 p.healing,p.mitigation,p.credits,p.objective_time,p.account_level,p.mastery_level,p.party_id,p.portal_id, \
                 p.portal_user_id,p.platform,p.participant_kind,p.source,m.duration_seconds \
               FROM special_match_players p JOIN special_matches m USING(match_id) WHERE p.match_id=$1), \
             party_groups AS (SELECT party_id FROM facts WHERE party_id>0 GROUP BY party_id HAVING COUNT(*)>1), \
             party_numbered AS (SELECT party_id,ROW_NUMBER() OVER (ORDER BY party_id) AS party_num FROM party_groups) \
             SELECT f.match_id,f.player_id,f.private_slot,f.player_name,f.champion_id, \
               COALESCE(c.name,f.champion_name) AS champion_name,f.task_force,f.win_status,f.kills,f.deaths,f.assists, \
               COALESCE(f.damage_done_in_hand,CASE WHEN COALESCE(history.raw_data->>'Damage_Done_In_Hand','') ~ '^[0-9]{1,10}$' \
                 THEN (history.raw_data->>'Damage_Done_In_Hand')::INT END) AS damage_done_in_hand, \
               f.damage AS damage_done_physical,f.damage_taken,f.healing,f.mitigation AS damage_mitigated, \
               f.credits AS gold_earned,f.objective_time AS objective_assists,f.account_level,f.mastery_level, \
               ROUND((f.kills::NUMERIC+f.assists::NUMERIC/2)/GREATEST(f.deaths,1),2)::DOUBLE PRECISION AS kda, \
               CASE WHEN f.duration_seconds>0 THEN ROUND(f.damage::NUMERIC*60/f.duration_seconds,2)::DOUBLE PRECISION ELSE 0 END AS damage_per_minute, \
               CASE WHEN f.duration_seconds>0 THEN ROUND(f.healing::NUMERIC*60/f.duration_seconds,2)::DOUBLE PRECISION ELSE 0 END AS healing_per_minute, \
               CASE WHEN f.duration_seconds>0 THEN ROUND(f.mitigation::NUMERIC*60/f.duration_seconds,2)::DOUBLE PRECISION ELSE 0 END AS mitigation_per_minute, \
               CASE WHEN f.duration_seconds>0 THEN ROUND(f.credits::NUMERIC*60/f.duration_seconds,2)::DOUBLE PRECISION ELSE 0 END AS gold_per_minute, \
               CASE WHEN f.duration_seconds>0 THEN ROUND((f.credits-500)::NUMERIC*60/f.duration_seconds,2)::DOUBLE PRECISION ELSE 0 END AS egpm, \
               f.duration_seconds AS time_in_match,0 AS healing_self,0::DOUBLE PRECISION AS healing_self_per_minute,0::DOUBLE PRECISION AS afk_rate, \
               f.party_id,COALESCE(pn.party_num,0::bigint) AS party,f.portal_id,f.portal_user_id,f.platform,f.participant_kind,f.source, \
               f.private_player_id,pp.alias AS private_account_alias,pp.verified_name AS private_account_verified_name, \
               COALESCE(pp.verified_name,pp.alias) AS private_account_display_name, \
               CASE WHEN f.player_id>0 THEN jsonb_build_object( \
                 'source','players_database','level',COALESCE(NULLIF(p.level,0),NULLIF(f.account_level,0)), \
                 'platform',COALESCE(NULLIF(p.platform,''),NULLIF(f.platform,'')), \
                 'region',NULLIF(NULLIF(p.region,''),'Unknown'),'global_wins',p.wins,'global_losses',p.losses, \
                 'kbm_tier',p.kbm_tier,'kbm_points',p.kbm_points,'cheater',COALESCE(p.cheater,false), \
                 'sus_count',COALESCE(p.sus_count,0), \
                 'verified',EXISTS(SELECT 1 FROM users u WHERE u.linked_player_id=f.player_id)) \
               WHEN f.private_player_id IS NOT NULL THEN jsonb_build_object( \
                 'source','private_account_database','level',COALESCE(NULLIF(pp.account_level,0),NULLIF(f.account_level,0)), \
                 'platform',NULLIF(f.platform,''),'kbm_tier',NULL,'kbm_points',NULL,'queue_elo',NULL, \
                 'champion_elo',NULL,'cheater',COALESCE(pp.cheater,false),'sus_count',COALESCE(pp.sus_count,0), \
                 'verified',false) ELSE NULL END AS profile_snapshot \
             FROM facts f LEFT JOIN champions c ON c.id=f.champion_id \
             LEFT JOIN players p ON p.id=f.player_id \
             LEFT JOIN players_private pp ON pp.id=f.private_player_id \
             LEFT JOIN player_match_history_entries history ON history.match_id=f.match_id AND history.player_id=f.player_id AND f.player_id>0 \
             LEFT JOIN party_numbered pn ON pn.party_id=f.party_id ORDER BY f.task_force,f.roster_slot",
            &[&match_id],
        ).await.map_err(|error| ApiError::database(error,request_id))?;
        (match_row, players, Vec::new())
    };
    normalize_match_queue_metadata(&mut match_row);
    let payload = json!({ "match": match_row, "players": players, "bans": bans });
    state.redis.set(&cache_key, &payload, Some(3_600)).await;
    Ok(Some(payload))
}

fn normalize_match_queue_metadata(match_row: &mut Value) {
    let is_custom = match_row
        .get("is_custom")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || match_row.get("stats_scope").and_then(Value::as_str) == Some("custom")
        || match_row.get("participant_model").and_then(Value::as_str) == Some("custom");
    if let Some(object) = match_row.as_object_mut() {
        object.insert("is_custom".to_owned(), Value::Bool(is_custom));
        if is_custom {
            object.insert("queue_name".to_owned(), Value::from("Custom Match"));
        }
    }
}

async fn recent(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    paged_matches(&state, &request_id, RANKED_QUEUE_ID, &query).await
}

async fn queue(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Path(queue_id): Path<String>,
    Query(mut query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let queue_id = queue_id
        .parse::<i32>()
        .map_err(|_| ApiError::validation("Invalid queue ID"))?;
    query.insert("_queue_shape".to_owned(), "1".to_owned());
    paged_matches(&state, &request_id, queue_id, &query).await
}

async fn paged_matches(
    state: &MatchesState,
    request_id: &RequestId,
    queue_id: i32,
    query: &HashMap<String, String>,
) -> Result<Response, ApiError> {
    let limit = bounded_i64(query.get("limit").map(String::as_str), 20, 1, 100);
    let cursor = match query.get("cursor") {
        Some(value) => {
            Some(parse_cursor(value).ok_or_else(|| ApiError::validation("Invalid cursor"))?)
        }
        None => None,
    };
    let cache_key = paged_matches_cache_key(queue_id, limit, cursor.as_ref());
    let (sql, params) = if let Some(cursor) = cursor {
        (
            "SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.duration_seconds,m.region, \
             m.winning_task_force,(SELECT c.name FROM match_players mp JOIN champions c ON c.id=mp.champion_id \
             WHERE mp.match_id=m.match_id LIMIT 1) AS sample_champion \
             FROM matches m WHERE m.queue_id=$1 AND (m.entry_datetime,m.match_id)<($2::TEXT::TIMESTAMPTZ,$3::bigint) \
             ORDER BY m.entry_datetime DESC,m.match_id DESC LIMIT $4",
            vec![
                QueryParam::Int32(queue_id),
                QueryParam::Text(cursor.at),
                QueryParam::Int64(cursor.id),
                QueryParam::Int64(limit + 1),
            ],
        )
    } else {
        (
            "SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.duration_seconds,m.region, \
             m.winning_task_force,(SELECT c.name FROM match_players mp JOIN champions c ON c.id=mp.champion_id \
             WHERE mp.match_id=m.match_id LIMIT 1) AS sample_champion \
             FROM matches m WHERE m.queue_id=$1 ORDER BY m.entry_datetime DESC,m.match_id DESC LIMIT $2",
            vec![QueryParam::Int32(queue_id), QueryParam::Int64(limit + 1)],
        )
    };
    // Purpose: reuse the shared cold-miss lease and stale-while-revalidate
    // path used by every major cached route. Input: the canonical page key,
    // SQL, and typed parameters. Output: raw look-ahead rows for the common
    // response renderer below; refreshes never block a reader with stale data.
    let database = state.database.clone();
    let loader_params = params.clone();
    let payload = cached_database_value(
        state.route_cache.clone(),
        cache_key.clone(),
        RECENT_MATCHES_FRESH_TTL_SECONDS,
        RECENT_MATCHES_STALE_TTL_SECONDS,
        move || {
            let database = database.clone();
            let params = loader_params.clone();
            async move {
                database
                    .query_json_params(sql, &params)
                    .await
                    .map(Value::Array)
            }
        },
    )
    .await
    .map_err(|error| ApiError::database(error, request_id))?;
    let rows = payload.as_array().cloned().unwrap_or_default();
    let fresh_until = state
        .route_cache
        .get(&cache_key)
        .await
        .map(|cached| cached.fresh_until);
    Ok(paged_matches_response(
        rows,
        limit,
        query.contains_key("_queue_shape"),
        fresh_until,
    ))
}

/// Purpose: render one cached or database-backed recent-match page. Input:
/// raw rows including the look-ahead row, typed limit, response shape, and
/// optional cache freshness. Output: public JSON plus cursor/cache headers.
fn paged_matches_response(
    mut rows: Vec<Value>,
    limit: i64,
    queue_shape: bool,
    fresh_until: Option<i64>,
) -> Response {
    if queue_shape {
        for row in &mut rows {
            if let Some(object) = row.as_object_mut() {
                object.remove("queue_id");
                object.remove("sample_champion");
            }
        }
    }
    let has_more = rows.len() > usize::try_from(limit).unwrap_or(0);
    rows.truncate(usize::try_from(limit).unwrap_or(0));
    let next_cursor = has_more
        .then(|| rows.last().and_then(encode_cursor))
        .flatten();
    let payload = Value::Array(rows);
    let mut response = match fresh_until {
        Some(fresh_until) => crate::route_cache::json_cache_response(payload, "HIT", fresh_until),
        None => cache_miss((StatusCode::OK, Json(payload)).into_response()),
    };
    if has_more
        && let Some(cursor) = next_cursor
        && let Ok(value) = HeaderValue::from_str(&cursor)
    {
        response
            .headers_mut()
            .insert(HeaderName::from_static("x-next-cursor"), value);
    }
    response
}

/// Purpose: key only parameters that alter the recent-match payload. Input:
/// queue ID, bounded limit, and parsed cursor. Output: stable cache key shared
/// by the route and warmer; unrelated query parameters cannot bypass caching.
fn paged_matches_cache_key(queue_id: i32, limit: i64, cursor: Option<&MatchCursor>) -> String {
    match cursor {
        Some(cursor) => format!(
            "route:matches:/matches/paged:queue:{queue_id}:limit:{limit}:at:{}:id:{}",
            cursor.at, cursor.id
        ),
        None => format!("route:matches:/matches/paged:queue:{queue_id}:limit:{limit}:first"),
    }
}

async fn dropped_nonranked(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let date = query.get("date").map(String::as_str).unwrap_or("");
    if !date.is_empty() && !valid_date(date) {
        return Err(ApiError::validation("date must be YYYY-MM-DD"));
    }
    let scope = query
        .get("scope")
        .map(|value| value.trim().to_lowercase())
        .unwrap_or_default();
    let hour = match optional_hour(query.get("hour").map(String::as_str)) {
        Ok(hour) => hour,
        Err(_) => {
            return Ok(plain_error(
                StatusCode::BAD_REQUEST,
                "hour must be an integer from 0 to 23",
            ));
        }
    };
    let limit = bounded_i64(query.get("limit").map(String::as_str), 500, 1, 2_000);
    let offset = bounded_i64(query.get("offset").map(String::as_str), 0, 0, i64::MAX);
    let rows = state.database.query_json_params(
        "SELECT a.match_id::text,a.source_date::text AS date,a.source_hour AS hour,a.queue_id, \
         q.queue_name,a.stats_scope,a.region,a.discovered_entry_datetime,a.quality,a.direct_player_count, \
         a.roster_player_count,a.detail_attempts,a.roster_attempts,a.terminal_reason,a.completed_at \
         FROM nonranked_match_acquisition a JOIN queue_types q ON q.queue_id=a.queue_id \
         WHERE a.status='dropped' AND ($1='' OR a.source_date=$1::date) \
           AND ($2='' OR a.stats_scope=$2) AND ($3<0 OR a.source_hour=$3) \
         ORDER BY a.source_date DESC,a.source_hour DESC,a.match_id DESC LIMIT $4 OFFSET $5",
        &[QueryParam::Text(date.to_owned()),QueryParam::Text(scope.clone()),QueryParam::Int32(hour.unwrap_or(-1)),
          QueryParam::Int64(limit),QueryParam::Int64(offset)],
    ).await.map_err(|error| ApiError::database(error,&request_id))?;
    let summary = state.database.query_json_params(
        "SELECT a.source_date::text AS date,a.source_hour AS hour,a.stats_scope,a.queue_id,q.queue_name, \
         COUNT(*)::int AS dropped FROM nonranked_match_acquisition a JOIN queue_types q ON q.queue_id=a.queue_id \
         WHERE a.status='dropped' AND ($1='' OR a.source_date=$1::date) \
           AND ($2='' OR a.stats_scope=$2) AND ($3<0 OR a.source_hour=$3) \
         GROUP BY a.source_date,a.source_hour,a.stats_scope,a.queue_id,q.queue_name \
         ORDER BY a.source_date DESC,a.source_hour DESC,a.stats_scope,a.queue_id",
        &[QueryParam::Text(date.to_owned()),QueryParam::Text(scope.clone()),QueryParam::Int32(hour.unwrap_or(-1))],
    ).await.map_err(|error| ApiError::database(error,&request_id))?;
    Ok((
        StatusCode::OK,
        Json(json!({
            "date": if date.is_empty(){Value::Null}else{Value::String(date.to_owned())},
            "scope": if scope.is_empty(){Value::Null}else{Value::String(scope)},
            "hour": hour,"count": rows.len(),"summary":summary,"matches":rows
        })),
    )
        .into_response())
}

async fn dropped_summary(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let date = match dropped_date(query.get("date").map(String::as_str)) {
        Ok(date) => date,
        Err(_) => {
            return Ok(plain_error(
                StatusCode::BAD_REQUEST,
                "date must be YYYY-MM-DD or YYYYMMDD",
            ));
        }
    };
    let queue_id = query
        .get("queueId")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(RANKED_QUEUE_ID);
    let summary = dropped_summary_rows(&state, &request_id, &date, queue_id).await?;
    Ok((
        StatusCode::OK,
        Json(json!({
            "date": date, "queue_id": queue_id, "refreshed": 0, "summary": summary
        })),
    )
        .into_response())
}

async fn dropped(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let date = match dropped_date(query.get("date").map(String::as_str)) {
        Ok(date) => date,
        Err(_) => {
            return Ok(plain_error(
                StatusCode::BAD_REQUEST,
                "date must be YYYY-MM-DD or YYYYMMDD",
            ));
        }
    };
    let queue_id = query
        .get("queueId")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(RANKED_QUEUE_ID);
    let status = query
        .get("status")
        .map(String::as_str)
        .unwrap_or("dropped")
        .to_lowercase();
    if !["open", "dropped", "resolved", "all"].contains(&status.as_str()) {
        return Err(ApiError::validation("Invalid dropped match status"));
    }
    let limit = bounded_i64(query.get("limit").map(String::as_str), 500, 1, 2_000);
    let offset = bounded_i64(query.get("offset").map(String::as_str), 0, 0, i64::MAX);
    let category = query
        .get("category")
        .map(|value| value.trim().to_owned())
        .unwrap_or_default();
    let hour = match optional_hour(query.get("hour").map(String::as_str)) {
        Ok(hour) => hour,
        Err(_) => {
            return Ok(plain_error(
                StatusCode::BAD_REQUEST,
                "hour must be an integer from 0 to 23",
            ));
        }
    };
    let rows = state
        .database
        .query_json_params(
            "SELECT * FROM dropped_matches WHERE date=$1::text::date AND queue_id=$2 \
         AND ($3='all' OR ($3='open' AND status<>'complete') \
           OR ($3='dropped' AND drop_category='api_no_data' AND status<>'complete') OR status=$3) \
         AND ($4='' OR drop_category=$4) AND ($5<0 OR hour=$5) \
         ORDER BY hour ASC,status ASC,attempts DESC,match_id ASC LIMIT $6 OFFSET $7",
            &[
                QueryParam::Text(date.clone()),
                QueryParam::Int32(queue_id),
                QueryParam::Text(status.clone()),
                QueryParam::Text(category.clone()),
                QueryParam::Int32(hour.unwrap_or(-1)),
                QueryParam::Int64(limit),
                QueryParam::Int64(offset),
            ],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let summary = dropped_summary_rows(&state, &request_id, &date, queue_id).await?;
    Ok((
        StatusCode::OK,
        Json(json!({
            "date":date,"queue_id":queue_id,"status":status,
            "category":if category.is_empty(){Value::Null}else{Value::String(category)},
            "hour":hour,"refreshed":0,"count":rows.len(),"summary":summary,"matches":rows
        })),
    )
        .into_response())
}

async fn dropped_summary_rows(
    state: &MatchesState,
    request_id: &RequestId,
    date: &str,
    queue_id: i32,
) -> Result<Vec<Value>, ApiError> {
    state
        .database
        .query_json_params(
            "SELECT hour,COUNT(*)::int AS tracked, \
         COUNT(*) FILTER (WHERE status<>'complete')::int AS open, \
         COUNT(*) FILTER (WHERE status='pending')::int AS pending, \
         COUNT(*) FILTER (WHERE status='staged')::int AS staged, \
         COUNT(*) FILTER (WHERE status='complete')::int AS resolved, \
         COUNT(*) FILTER (WHERE drop_category='api_no_data' AND status<>'complete')::int AS dropped, \
         COUNT(*) FILTER (WHERE drop_category='broken_recovery_pending' AND status<>'complete')::int AS broken_recovery_pending, \
         COUNT(*) FILTER (WHERE drop_category='no_authoritative_payload' AND status<>'complete')::int AS no_authoritative_payload, \
         COUNT(*) FILTER (WHERE drop_category='no_history_anchor' AND status<>'complete')::int AS no_history_anchor, \
         COUNT(*) FILTER (WHERE drop_category='partial_history_anchor' AND status<>'complete')::int AS partial_history_anchor, \
         COUNT(*) FILTER (WHERE drop_category='local_ingest_failed' AND status<>'complete')::int AS local_ingest_failed, \
         COUNT(*) FILTER (WHERE drop_category='invalid_payload' AND status<>'complete')::int AS invalid_payload, \
         MIN(next_retry_at) FILTER (WHERE status='pending') AS next_retry_at \
         FROM dropped_matches WHERE date=$1::text::date AND queue_id=$2 GROUP BY hour ORDER BY hour ASC",
            &[
                QueryParam::Text(date.to_owned()),
                QueryParam::Int32(queue_id),
            ],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))
}

async fn bans(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = bounded_i64(query.get("limit").map(String::as_str), 50, 1, 200);
    let champion_id = query
        .get("championId")
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    let sort = if query.get("sort").map(String::as_str) == Some("ban_rate") {
        "ban_rate"
    } else {
        "ban_count"
    };
    let order = if query.get("order").map(String::as_str) == Some("asc") {
        "ASC"
    } else {
        "DESC"
    };
    let sql = format!(
        "WITH champion_bans AS (SELECT sba.champion_id,SUM(sba.bans)::bigint AS ban_count \
         FROM stats_ban_aggregate sba WHERE sba.queue_id=$1 AND ($2=0 OR sba.champion_id=$2) \
         GROUP BY sba.champion_id),totals AS (SELECT SUM(bans)::bigint AS all_bans \
         FROM stats_ban_aggregate WHERE queue_id=$1),match_total AS \
         (SELECT SUM(match_count)::bigint AS total_matches FROM stats_match_aggregate WHERE queue_id=$1) \
         SELECT c.id AS champion_id,c.name AS champion_name,cb.ban_count,cb.ban_count AS total_bans, \
         mt.total_matches,ROUND(100.0*cb.ban_count::numeric/NULLIF(t.all_bans,0),2) AS ban_rate \
         FROM champion_bans cb JOIN champions c ON c.id=cb.champion_id CROSS JOIN totals t \
         CROSS JOIN match_total mt ORDER BY {sort} {order} LIMIT $3"
    );
    let rows = state
        .database
        .query_json_params(
            &sql,
            &[
                QueryParam::Int32(RANKED_QUEUE_ID),
                QueryParam::Int32(champion_id),
                QueryParam::Int64(limit),
            ],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(cache_miss(
        (StatusCode::OK, Json(Value::Array(rows))).into_response(),
    ))
}

async fn compositions(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let valid = [
        "count",
        "winrate",
        "wins",
        "frontline",
        "damage",
        "flank",
        "support",
    ];
    let sort = query
        .get("sortBy")
        .filter(|value| valid.contains(&value.as_str()))
        .map(String::as_str)
        .unwrap_or("count");
    let order = if query.get("order").map(String::as_str) == Some("asc") {
        "ASC"
    } else {
        "DESC"
    };
    let limit = bounded_i64(query.get("limit").map(String::as_str), 50, 1, 200);
    let min_tier = match tier_bound(query.get("tierMin").map(String::as_str)) {
        Ok(value) => value,
        Err(_) => {
            return Ok(plain_error(
                StatusCode::BAD_REQUEST,
                "Tier bounds must be between 1 and 26.",
            ));
        }
    };
    let max_tier = match tier_bound(query.get("tierMax").map(String::as_str)) {
        Ok(value) => value,
        Err(_) => {
            return Ok(plain_error(
                StatusCode::BAD_REQUEST,
                "Tier bounds must be between 1 and 26.",
            ));
        }
    };
    if min_tier.is_some() && max_tier.is_some() && min_tier > max_tier {
        return Ok(plain_error(
            StatusCode::BAD_REQUEST,
            "Tier bounds must be between 1 and 26.",
        ));
    }
    let sql = format!(
        "SELECT sca.comp_id,sca.frontline,sca.damage,sca.flank,sca.support, \
         SUM(sca.uses)::bigint AS count,SUM(sca.wins)::bigint AS wins,SUM(sca.losses)::bigint AS losses, \
         ROUND(100.0*SUM(sca.wins)::numeric/NULLIF((SUM(sca.wins)+SUM(sca.losses))::numeric,0),2) AS winrate \
         FROM stats_composition_aggregate sca WHERE sca.queue_id=$1 \
         AND ($2=0 OR sca.lobby_tier >= $2) AND ($3=0 OR sca.lobby_tier <= $3) \
         GROUP BY sca.comp_id,sca.frontline,sca.damage,sca.flank,sca.support \
         ORDER BY {sort} {order},sca.comp_id LIMIT $4"
    );
    let rows = state
        .database
        .query_json_params(
            &sql,
            &[
                QueryParam::Int32(RANKED_QUEUE_ID),
                QueryParam::Int32(min_tier.unwrap_or(0)),
                QueryParam::Int32(max_tier.unwrap_or(0)),
                QueryParam::Int64(limit),
            ],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(cache_miss(
        (
            StatusCode::OK,
            Json(json!({ "total": rows.len(), "data": rows })),
        )
            .into_response(),
    ))
}

async fn live_drop_hack_suspects(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = bounded_i64(query.get("limit").map(String::as_str), 50, 1, i64::MAX);
    let rows = state
        .database
        .query_json(
            "SELECT * FROM drop_hack_suspects WHERE incident_count>1 ORDER BY incident_count DESC LIMIT $1",
            &[&limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok((StatusCode::OK, Json(Value::Array(rows))).into_response())
}

async fn live_player(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Path(player_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(player_id) = positive_i64(Some(&player_id)) else {
        return Ok((StatusCode::OK, Json(json!({"error":"Invalid player ID"}))).into_response());
    };
    let identity = request_identity(&headers);
    let resolution = resolve_player_live_match(
        &state.database,
        &state.redis,
        state.relay.as_ref(),
        player_id,
        &identity,
        &request_id,
        false,
    )
    .await?;
    let payload = if resolution.payload.get("match").is_some_and(Value::is_null)
        && !resolution
            .payload
            .get("pending")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        json!({"message":"Player not in a live match","player_id":player_id})
    } else {
        resolution.payload
    };
    let mut response = (StatusCode::OK, Json(payload)).into_response();
    if let Some(headers) = resolution.vendor_headers {
        headers.apply(&mut response);
    }
    Ok(response)
}

async fn raw_discover(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(queue_id) = positive_i64(query.get("queueId").map(String::as_str)) else {
        return Ok(plain_error(
            StatusCode::OK,
            "Missing required query params: queueId, date, hour",
        ));
    };
    let date = query.get("date").cloned().unwrap_or_default();
    let hour = query
        .get("hour")
        .and_then(|value| paladinscat_core::web_compat::parse_js_integer(value));
    let Some(hour) = hour else {
        return Ok(plain_error(
            StatusCode::OK,
            "Missing required query params: queueId, date, hour",
        ));
    };
    if date.is_empty() {
        return Ok(plain_error(
            StatusCode::OK,
            "Missing required query params: queueId, date, hour",
        ));
    }
    let entity = format!("{queue_id}:{date}:{hour}");
    let rate_headers = vendor_guard(
        &state.redis,
        &request_identity(&headers),
        "raw-match-discovery",
        &entity,
        120_000,
        60,
    )
    .await?;
    let raw = call_relay(
        &state,
        &request_id,
        "getMatchIdsByQueue",
        vec![json!(queue_id), json!(date), json!(hour)],
        "operator_raw_audit",
    )
    .await?;
    let audit = audit_raw(
        &state,
        &request_id,
        RawHirezAudit {
            endpoint: "getmatchidsbyqueue",
            operation: "getMatchIdsByQueue",
            entity_type: "match_discovery",
            entity_id: entity,
            params: json!({"queueId":queue_id,"date":date,"hour":hour}),
            raw_response: &raw,
            source: "paladinscat-api-raw-pass-through",
        },
    )
    .await?;
    let mut response = (
        StatusCode::OK,
        Json(json!({
            "queue_id": queue_id,
            "date": date,
            "hour": hour,
            "count": raw.as_array().map(Vec::len),
            "audit": audit,
            "data": raw
        })),
    )
        .into_response();
    rate_headers.apply(&mut response);
    Ok(response)
}

async fn raw_matchdetails(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let ids = query
        .get("ids")
        .or_else(|| query.get("matchIds"))
        .map(String::as_str)
        .unwrap_or("");
    let mut match_ids = ids
        .split(',')
        .filter_map(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
        .collect::<Vec<_>>();
    match_ids.sort_unstable();
    match_ids.dedup();
    if match_ids.is_empty() {
        let message = if ids.is_empty() {
            "Missing required query param: ids (comma-separated match IDs)"
        } else {
            "No valid match IDs provided"
        };
        return Ok(plain_error(StatusCode::OK, message));
    }
    if match_ids.len() > 10 {
        return Ok(plain_error(
            StatusCode::BAD_REQUEST,
            "Maximum 10 match IDs per raw request",
        ));
    }
    let entity = match_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let rate_headers = vendor_guard(
        &state.redis,
        &request_identity(&headers),
        "raw-match-details",
        &entity,
        120_000,
        60,
    )
    .await?;
    let raw = call_relay(
        &state,
        &request_id,
        "getMatchDetailsBatchRaw",
        vec![json!(&match_ids)],
        "operator_raw_audit",
    )
    .await?;
    let audit = audit_raw(
        &state,
        &request_id,
        RawHirezAudit {
            endpoint: "getmatchdetailsbatch",
            operation: "getMatchDetailsBatchRaw",
            entity_type: "match_batch",
            entity_id: entity,
            params: json!({"matchIds":match_ids}),
            raw_response: &raw,
            source: "paladinscat-api-raw-pass-through",
        },
    )
    .await?;
    let mut response = (
        StatusCode::OK,
        Json(json!({
            "match_ids": match_ids,
            "count": raw.as_array().map(Vec::len),
            "audit": audit,
            "data": raw
        })),
    )
        .into_response();
    rate_headers.apply(&mut response);
    Ok(response)
}

async fn raw_demo(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(match_id) = positive_i64(
        query
            .get("matchId")
            .or_else(|| query.get("id"))
            .map(String::as_str),
    ) else {
        return Ok(plain_error(StatusCode::OK, "Missing or invalid matchId"));
    };
    let rate_headers = vendor_guard(
        &state.redis,
        &request_identity(&headers),
        "raw-match-demo",
        match_id,
        120_000,
        60,
    )
    .await?;
    let raw = call_relay(
        &state,
        &request_id,
        "getDemoDetails",
        vec![json!(match_id)],
        "operator_raw_audit",
    )
    .await?;
    let audit = audit_raw(
        &state,
        &request_id,
        RawHirezAudit {
            endpoint: "getdemodetails",
            operation: "getDemoDetails",
            entity_type: "match_demo",
            entity_id: match_id.to_string(),
            params: json!({"matchId":match_id}),
            raw_response: &raw,
            source: "paladinscat-api-raw-pass-through",
        },
    )
    .await?;
    let mut response = (
        StatusCode::OK,
        Json(json!({"match_id":match_id,"audit":audit,"data":raw})),
    )
        .into_response();
    rate_headers.apply(&mut response);
    Ok(response)
}

async fn raw_playerbatch(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(match_id) = positive_i64(query.get("matchId").map(String::as_str)) else {
        return Ok(plain_error(
            StatusCode::OK,
            "Missing required query param: matchId",
        ));
    };
    let rate_headers = vendor_guard(
        &state.redis,
        &request_identity(&headers),
        "raw-match-player-batch",
        match_id,
        120_000,
        60,
    )
    .await?;
    let raw = call_relay(
        &state,
        &request_id,
        "getPlayerBatchFromMatch",
        vec![json!(match_id)],
        "operator_raw_audit",
    )
    .await?;
    let audit = audit_raw(
        &state,
        &request_id,
        RawHirezAudit {
            endpoint: "getplayerbatchfrommatch",
            operation: "getPlayerBatchFromMatch",
            entity_type: "match",
            entity_id: match_id.to_string(),
            params: json!({"matchId":match_id}),
            raw_response: &raw,
            source: "paladinscat-api-raw-pass-through",
        },
    )
    .await?;
    let mut response = (
        StatusCode::OK,
        Json(json!({
            "match_id":match_id,
            "count":raw.as_array().map(Vec::len),
            "audit":audit,
            "data":raw
        })),
    )
        .into_response();
    rate_headers.apply(&mut response);
    Ok(response)
}

async fn call_relay(
    state: &MatchesState,
    request_id: &RequestId,
    operation: &str,
    args: Vec<Value>,
    consumer: &str,
) -> Result<Value, ApiError> {
    let relay = state
        .relay
        .as_ref()
        .ok_or_else(|| ApiError::internal(request_id))?;
    let result = relay
        .call_value(operation, args, consumer)
        .await
        .map_err(|error| {
            tracing::error!(%error, operation, "matches relay call failed");
            ApiError::internal(request_id)
        })?;
    Ok(result)
}

async fn audit_raw(
    state: &MatchesState,
    request_id: &RequestId,
    input: RawHirezAudit<'_>,
) -> Result<Value, ApiError> {
    record_raw_hirez_response(&state.database, input)
        .await
        .map_err(|error| ApiError::database(error, request_id))?
        .ok_or_else(|| ApiError::internal(request_id))
}

async fn pull(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Json(body): Json<PullBody>,
) -> Result<Response, ApiError> {
    let queue_id = body
        .queue_id
        .as_ref()
        .and_then(json_i64)
        .filter(|id| *id > 0);
    let from = body.from.unwrap_or_default();
    if queue_id.is_none()
        || from.len() < 13
        || !from.as_bytes().get(10).is_some_and(|byte| *byte == b'T')
    {
        return Err(ApiError::validation("Invalid queueId or from timestamp"));
    }
    let date = from[..10].replace('-', "");
    let hour = from[11..13]
        .parse::<i32>()
        .map_err(|_| ApiError::validation("Invalid queueId or from timestamp"))?;
    discover_window(
        &state,
        &request_id,
        i32::try_from(queue_id.unwrap_or(0)).unwrap_or_default(),
        &date,
        hour,
        false,
    )
    .await
}

/// Purpose: validate an authenticated operator discovery request. Input: route
/// state, request ID, and JSON body. Output: canonical ingest response or error.
async fn discover(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Json(body): Json<DiscoverBody>,
) -> Result<Response, ApiError> {
    let queue_id = body
        .queue_id
        .as_ref()
        .and_then(json_i64)
        .filter(|id| *id > 0)
        .ok_or_else(|| ApiError::validation("Missing required fields: queueId, date, hour"))?;
    let hour = body
        .hour
        .as_ref()
        .and_then(json_i64)
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| (0..=23).contains(value))
        .ok_or_else(|| ApiError::validation("Invalid queueId or hour. hour must be 0-23."))?;
    let date = body
        .date
        .as_ref()
        .map(json_text)
        .unwrap_or_default()
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>();
    if date.len() != 8 {
        return Err(ApiError::validation("Invalid date format. Use YYYYMMDD."));
    }
    let queue_id = i32::try_from(queue_id)
        .map_err(|_| ApiError::validation("queueId exceeds the supported integer range"))?;
    discover_window(&state, &request_id, queue_id, &date, hour, body.force).await
}

/// Purpose: adapt the operator HTTP contract to the shared pipeline object.
/// Input: typed queue/date/hour and compatibility force flag. Output: only
/// after discovery, DB-first filtering, fact finalization, and projection end.
async fn discover_window(
    state: &MatchesState,
    request_id: &RequestId,
    queue_id: i32,
    date: &str,
    hour: i32,
    _force: bool,
) -> Result<Response, ApiError> {
    let db_date = format!("{}-{}-{}", &date[..4], &date[4..6], &date[6..8]);
    let pipeline = state
        .canonical_ingest
        .as_ref()
        .ok_or_else(|| ApiError::internal(request_id))?;
    let result = pipeline
        .discover_hour(queue_id, &db_date, hour, "operator-match-discover")
        .await
        .map_err(|error| {
            tracing::error!(%error, queue_id, date=%db_date, hour, "canonical match discovery failed");
            ApiError::internal(request_id)
        })?;
    Ok((
        StatusCode::OK,
        Json(json!({"message":"Discovery and canonical ingestion completed","result":result})),
    )
        .into_response())
}

async fn fact(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Path(match_id): Path<String>,
) -> Result<Response, ApiError> {
    let Some(match_id) = positive_i64(Some(&match_id)) else {
        return Ok(plain_error(StatusCode::BAD_REQUEST, "Invalid match ID"));
    };
    let storage = state
        .database
        .one_json(
            "SELECT match_id,'ranked'::text AS storage_kind FROM matches WHERE match_id=$1 \
         UNION ALL SELECT match_id,'casual'::text FROM casual_matches WHERE match_id=$1 \
         UNION ALL SELECT match_id,'special'::text FROM special_matches WHERE match_id=$1 LIMIT 1",
            &[&match_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let Some(storage_kind) = storage
        .as_ref()
        .and_then(|row| row.get("storage_kind"))
        .and_then(Value::as_str)
    else {
        return Ok(plain_error(StatusCode::NOT_FOUND, "Match not found"));
    };
    let stored_players = state.database.query_json_params(
        "WITH stored_players AS ( \
           SELECT 'ranked'::text AS storage_kind,mp.player_id,mp.private_slot,mp.player_name,mp.champion_id, \
             NULL::text AS stored_champion_name,mp.task_force,0::int AS roster_slot,NULL::jsonb AS raw_player \
           FROM match_players mp WHERE mp.match_id=$1 \
           UNION ALL SELECT 'casual',cmp.player_id,cmp.private_slot,cmp.player_name,cmp.champion_id, \
             cmp.champion_name,cmp.task_force,cmp.roster_slot,cmp.raw_player \
           FROM casual_match_players cmp WHERE cmp.match_id=$1 \
           UNION ALL SELECT 'special',smp.player_id,smp.private_slot,smp.player_name,smp.champion_id, \
             smp.champion_name,smp.task_force,smp.roster_slot,smp.raw_player \
           FROM special_match_players smp WHERE smp.match_id=$1) \
         SELECT stored.player_id,stored.roster_slot,stored.player_name,stored.champion_id, \
           COALESCE(c.name,stored.stored_champion_name) AS champion_name,stored.raw_player \
         FROM stored_players stored LEFT JOIN champions c ON c.id=stored.champion_id \
         WHERE stored.storage_kind=$2 ORDER BY stored.task_force,stored.roster_slot,stored.player_id,stored.private_slot",
        &[QueryParam::Int64(match_id),QueryParam::Text(storage_kind.to_owned())],
    ).await.map_err(|error| ApiError::database(error,&request_id))?;
    let items = state.database.query_json(
        "WITH facts AS(\
           SELECT 'ranked'::TEXT storage_kind,mpi.player_id,0::SMALLINT roster_slot,mpi.item_id,mpi.slot,mpi.item_level \
           FROM match_player_items mpi WHERE mpi.match_id=$1 \
           UNION ALL SELECT population,n.player_id,n.roster_slot,n.item_id,n.slot,n.item_level \
           FROM nonranked_match_items n WHERE n.match_id=$1)\
         SELECT f.player_id,f.roster_slot,f.item_id,f.slot,f.item_level,i.item_name,i.description,\
           i.item_type,i.cost,i.icon_url db_icon_url FROM facts f LEFT JOIN items i ON i.item_id=f.item_id \
         WHERE f.storage_kind=$2 ORDER BY f.roster_slot,f.player_id,f.slot",
        &[&match_id,&storage_kind],
    ).await.map_err(|error| ApiError::database(error,&request_id))?;
    let cards = state.database.query_json(
        "WITH facts AS(\
           SELECT 'ranked'::TEXT storage_kind,mpc.player_id,0::SMALLINT roster_slot,mpc.card_id,mpc.card_level \
           FROM match_player_cards mpc WHERE mpc.match_id=$1 \
           UNION ALL SELECT population,n.player_id,n.roster_slot,n.card_id,n.card_level \
           FROM nonranked_match_cards n WHERE n.match_id=$1)\
         SELECT f.player_id,f.roster_slot,f.card_id,f.card_level,COALESCE(c.card_name,i.item_name) card_name,\
           COALESCE(c.champion_id,i.champion_id) champion_id,i.description,i.icon_url db_icon_url \
         FROM facts f LEFT JOIN cards c ON c.card_id=f.card_id LEFT JOIN items i ON i.item_id=f.card_id \
         WHERE f.storage_kind=$2 ORDER BY f.roster_slot,f.player_id,f.card_id",
        &[&match_id,&storage_kind],
    ).await.map_err(|error| ApiError::database(error,&request_id))?;
    let talents = state.database.query_json(
        "WITH facts AS(\
           SELECT 'ranked'::TEXT storage_kind,mpt.player_id,0::SMALLINT roster_slot,mpt.talent_id,mp.champion_id \
           FROM match_player_talents mpt JOIN match_players mp ON mp.match_id=mpt.match_id AND mp.player_id=mpt.player_id \
           WHERE mpt.match_id=$1 \
           UNION ALL SELECT population,n.player_id,n.roster_slot,n.talent_id,n.champion_id \
           FROM nonranked_match_talents n WHERE n.match_id=$1)\
         SELECT f.player_id,f.roster_slot,f.talent_id,COALESCE(t.talent_name,i.item_name) talent_name,\
           COALESCE(t.champion_id,i.champion_id,f.champion_id) champion_id,c.name champion_name,i.description,\
           i.icon_url db_icon_url FROM facts f LEFT JOIN talents t ON t.talent_id=f.talent_id \
           LEFT JOIN items i ON i.item_id=f.talent_id LEFT JOIN champions c ON c.id=COALESCE(t.champion_id,i.champion_id,f.champion_id) \
           WHERE f.storage_kind=$2 AND COALESCE(t.champion_id,i.champion_id,f.champion_id)=f.champion_id \
           ORDER BY f.roster_slot,f.player_id,f.talent_id",
        &[&match_id,&storage_kind],
    ).await.map_err(|error| ApiError::database(error,&request_id))?;
    let players = assemble_match_facts(&stored_players, items, cards, talents);
    Ok((
        StatusCode::OK,
        Json(json!({"match_id":match_id,"players":players})),
    )
        .into_response())
}

fn assemble_match_facts(
    stored_players: &[Value],
    items: Vec<Value>,
    cards: Vec<Value>,
    talents: Vec<Value>,
) -> Vec<Value> {
    let mut players = stored_players
        .iter()
        .map(|stored| {
            json!({
                "player_id":field(stored,"player_id"),
                "roster_slot":field(stored,"roster_slot"),
                "player_name":field(stored,"player_name"),
                "champion_id":field(stored,"champion_id"),
                "champion_name":field(stored,"champion_name"),
                "items":[],"cards":[],"talents":[]
            })
        })
        .collect::<Vec<_>>();
    let mut by_player = HashMap::new();
    for (index, player) in players.iter().enumerate() {
        by_player.insert(fact_player_key(player), index);
    }
    for item in items {
        let Some(index) = by_player.get(&fact_player_key(&item)).copied() else {
            continue;
        };
        let name = item.get("item_name").and_then(Value::as_str);
        push_fact(
            &mut players[index],
            "items",
            json!({
                "item_id":field(&item,"item_id"),"slot":field(&item,"slot"),
                "item_level":field(&item,"item_level"),"item_name":field(&item,"item_name"),
                "description":field(&item,"description"),"item_type":field(&item,"item_type"),
                "cost":field(&item,"cost"),"icon_url":database_or_local_icon(&item,item_icon_url(name)),
                "fallback_icon_url":item_fallback_icon_url(name)
            }),
        );
    }
    for card in cards {
        let Some(index) = by_player.get(&fact_player_key(&card)).copied() else {
            continue;
        };
        let name = card.get("card_name").and_then(Value::as_str);
        push_fact(
            &mut players[index],
            "cards",
            json!({
                "card_id":field(&card,"card_id"),"card_level":field(&card,"card_level"),
                "card_name":field(&card,"card_name"),"champion_id":field(&card,"champion_id"),
                "description":field(&card,"description"),"icon_url":database_or_local_icon(&card,card_icon_url(name)),
                "fallback_icon_url":card_fallback_icon_url(name)
            }),
        );
    }
    for talent in talents {
        let Some(index) = by_player.get(&fact_player_key(&talent)).copied() else {
            continue;
        };
        let talent_name = talent.get("talent_name").and_then(Value::as_str);
        let champion_name = talent
            .get("champion_name")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                players[index]
                    .get("champion_name")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            });
        push_fact(
            &mut players[index],
            "talents",
            json!({
                "talent_id":field(&talent,"talent_id"),"talent_name":field(&talent,"talent_name"),
                "champion_id":field(&talent,"champion_id"),
                "champion_name":champion_name.as_deref(),
                "description":field(&talent,"description"),
                "icon_url":database_or_local_icon(&talent,talent_icon_url(champion_name.as_deref(),talent_name)),
                "fallback_icon_url":talent_fallback_icon_url(champion_name.as_deref(),talent_name)
            }),
        );
    }
    for (index, stored) in stored_players.iter().enumerate() {
        let Some(raw) = stored.get("raw_player").and_then(Value::as_object) else {
            continue;
        };
        if fact_list_empty(&players[index], "items") {
            for slot in 1..=4 {
                let Some(item_id) = raw
                    .get(&format!("active_id_{slot}"))
                    .and_then(json_i64)
                    .filter(|id| *id > 0)
                else {
                    continue;
                };
                let name = raw_text(raw, &format!("item_active_{slot}"));
                let raw_level = raw
                    .get(&format!("active_level_{slot}"))
                    .and_then(json_i64)
                    .unwrap_or_default()
                    .max(0);
                let item_level = if raw_level > 2 {
                    raw_level.div_euclid(4)
                } else {
                    raw_level
                };
                push_fact(
                    &mut players[index],
                    "items",
                    json!({
                        "item_id":item_id,"slot":slot,"item_level":item_level,"item_name":name,
                        "description":null,"item_type":null,"cost":null,
                        "icon_url":item_icon_url(name.as_deref()),
                        "fallback_icon_url":item_fallback_icon_url(name.as_deref())
                    }),
                );
            }
        }
        if fact_list_empty(&players[index], "cards") {
            let champion_id = field(&players[index], "champion_id");
            for slot in 1..=5 {
                let Some(card_id) = raw
                    .get(&format!("item_id_{slot}"))
                    .and_then(json_i64)
                    .filter(|id| *id > 0)
                else {
                    continue;
                };
                let name = raw_text(raw, &format!("item_purch_{slot}"));
                let level = raw
                    .get(&format!("item_level_{slot}"))
                    .and_then(json_i64)
                    .unwrap_or_default()
                    .max(0);
                push_fact(
                    &mut players[index],
                    "cards",
                    json!({
                        "card_id":card_id,"card_level":level,"card_name":name,
                        "champion_id":champion_id,"description":null,
                        "icon_url":card_icon_url(name.as_deref()),
                        "fallback_icon_url":card_fallback_icon_url(name.as_deref())
                    }),
                );
            }
        }
        if fact_list_empty(&players[index], "talents")
            && let Some(talent_id) = raw.get("item_id_6").and_then(json_i64).filter(|id| *id > 0)
        {
            let name = raw_text(raw, "item_purch_6");
            let champion_id = field(&players[index], "champion_id");
            let champion_name = players[index]
                .get("champion_name")
                .and_then(Value::as_str)
                .map(str::to_owned);
            push_fact(
                &mut players[index],
                "talents",
                json!({
                    "talent_id":talent_id,"talent_name":name,"champion_id":champion_id,
                    "champion_name":champion_name,"description":null,
                    "icon_url":talent_icon_url(champion_name.as_deref(),name.as_deref()),
                    "fallback_icon_url":talent_fallback_icon_url(champion_name.as_deref(),name.as_deref())
                }),
            );
        }
    }
    players
}

fn field(value: &Value, name: &str) -> Value {
    value.get(name).cloned().unwrap_or(Value::Null)
}

fn json_key(value: Option<&Value>) -> String {
    value.map(json_text).unwrap_or_default()
}

fn fact_player_key(value: &Value) -> String {
    format!(
        "{}:{}",
        json_key(value.get("player_id")),
        json_key(value.get("roster_slot"))
    )
}

fn push_fact(player: &mut Value, name: &str, fact: Value) {
    if let Some(list) = player.get_mut(name).and_then(Value::as_array_mut) {
        list.push(fact);
    }
}

fn fact_list_empty(player: &Value, name: &str) -> bool {
    player
        .get(name)
        .and_then(Value::as_array)
        .is_none_or(Vec::is_empty)
}

fn raw_text(raw: &Map<String, Value>, name: &str) -> Option<String> {
    raw.get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn database_or_local_icon(row: &Value, fallback: Value) -> Value {
    row.get("db_icon_url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Value::from)
        .unwrap_or(fallback)
}

fn image_asset_segment(name: Option<&str>) -> Option<String> {
    let name = name?.trim();
    if name.is_empty() {
        return None;
    }
    Some(
        name.chars()
            .filter(|value| {
                !value.is_control()
                    && !matches!(value, '<' | '>' | ':' | '\"' | '/' | '\\' | '|' | '?' | '*')
            })
            .map(|value| if value.is_whitespace() { '_' } else { value })
            .collect(),
    )
}

fn spaced_asset_segment(name: Option<&str>) -> Option<String> {
    let name = name?.trim();
    (!name.is_empty()).then(|| name.replace(',', ""))
}

fn item_icon_url(name: Option<&str>) -> Value {
    image_asset_segment(name)
        .map(|name| Value::from(format!("/images/items/{name}_Icon.avif")))
        .unwrap_or(Value::Null)
}

fn item_fallback_icon_url(name: Option<&str>) -> Value {
    image_asset_segment(name)
        .map(|name| Value::from(format!("/images/items/{name}_Icon.png")))
        .unwrap_or(Value::Null)
}

fn card_icon_url(name: Option<&str>) -> Value {
    image_asset_segment(name)
        .map(|name| Value::from(format!("/images/cards/Card_{name}.avif")))
        .unwrap_or(Value::Null)
}

fn card_fallback_icon_url(name: Option<&str>) -> Value {
    image_asset_segment(name)
        .map(|name| Value::from(format!("/images/cards/Card_{name}.png")))
        .unwrap_or(Value::Null)
}

fn talent_icon_url(champion: Option<&str>, talent: Option<&str>) -> Value {
    talent_asset_url(champion, talent, "avif")
}

fn talent_fallback_icon_url(champion: Option<&str>, talent: Option<&str>) -> Value {
    talent_asset_url(champion, talent, "png")
}

fn talent_asset_url(champion: Option<&str>, talent: Option<&str>, extension: &str) -> Value {
    let Some(champion) = spaced_asset_segment(champion) else {
        return Value::Null;
    };
    let Some(talent) = spaced_asset_segment(talent) else {
        return Value::Null;
    };
    let name = if champion == "Seris" && talent == "Resuscitate" {
        "Seris Soul Collector".to_owned()
    } else {
        format!("{champion} {talent}")
    };
    Value::from(format!("/images/champions/Talent {name}.{extension}"))
}

async fn search(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let cache_key = matches_query_cache_key("/matches/search", &query);
    if let Some(cached) = state.route_cache.get(&cache_key).await {
        let next_cursor = cached
            .payload
            .get("next_cursor")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut response =
            crate::route_cache::json_cache_response(cached.payload, "HIT", cached.fresh_until);
        if let Some(value) = next_cursor
            .as_deref()
            .and_then(|value| HeaderValue::from_str(value).ok())
        {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-next-cursor"), value);
        }
        return Ok(response);
    }
    let page = bounded_i64(query.get("page").map(String::as_str), 1, 1, i64::MAX);
    let per_page = bounded_i64(query.get("perPage").map(String::as_str), 20, 1, 100);
    let offset = (page - 1).saturating_mul(per_page);
    let cursor = match query.get("cursor") {
        Some(value) if !value.is_empty() => {
            let Some(cursor) = parse_cursor(value) else {
                return Ok(plain_error(StatusCode::BAD_REQUEST, "Invalid cursor"));
            };
            Some(cursor)
        }
        _ => None,
    };
    let cursor_mode = cursor.is_some()
        || query
            .get("cursorMode")
            .is_some_and(|value| matches!(value.to_lowercase().as_str(), "1" | "true"));

    let mut conditions = Vec::new();
    let mut params = Vec::new();
    let mut champion_param = None;
    if let Some(value) = query.get("championId") {
        let Some(champion_id) =
            paladinscat_core::web_compat::parse_js_integer(value).filter(|value| *value > 0)
        else {
            return Ok(plain_error(StatusCode::BAD_REQUEST, "Invalid championId"));
        };
        let champion_id =
            i32::try_from(champion_id).map_err(|_| ApiError::validation("Invalid championId"))?;
        params.push(QueryParam::Int32(champion_id));
        let parameter = params.len();
        champion_param = Some(parameter);
        conditions.push(format!(
            "EXISTS (SELECT 1 FROM match_players mp_filter \
             WHERE mp_filter.match_id=m.match_id AND mp_filter.champion_id=${parameter})"
        ));
    }
    if let Some(value) = query.get("queueId") {
        let Some(queue_id) =
            paladinscat_core::web_compat::parse_js_integer(value).filter(|value| *value > 0)
        else {
            return Ok(plain_error(StatusCode::BAD_REQUEST, "Invalid queueId"));
        };
        let queue_id =
            i32::try_from(queue_id).map_err(|_| ApiError::validation("Invalid queueId"))?;
        params.push(QueryParam::Int32(queue_id));
        conditions.push(format!("m.queue_id=${}", params.len()));
    }
    if let Some(region) = query
        .get("region")
        .map(|value| value.trim())
        .filter(|v| !v.is_empty())
    {
        params.push(QueryParam::Text(region.to_uppercase()));
        conditions.push(format!("m.region=${}", params.len()));
    }

    let date = query.get("date").map(String::as_str).unwrap_or("");
    if !date.is_empty() {
        if !valid_calendar_date(date) {
            return Ok(plain_error(
                StatusCode::BAD_REQUEST,
                "Invalid date; expected YYYY-MM-DD",
            ));
        }
        let requested_hour = match query.get("hour").map(String::as_str) {
            None | Some("") => None,
            Some(value) => match value.parse::<i32>() {
                Ok(hour) if (0..=23).contains(&hour) => Some(hour),
                _ => {
                    return Ok(plain_error(
                        StatusCode::BAD_REQUEST,
                        "Invalid hour; expected 0 through 23",
                    ));
                }
            },
        };
        let time_zone = query.get("timeZone").map(String::as_str).unwrap_or("UTC");
        let valid_zone = state
            .database
            .one_json(
                "SELECT EXISTS(SELECT 1 FROM pg_timezone_names WHERE name=$1) AS valid",
                &[&time_zone],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
            .and_then(|row| row.get("valid").and_then(Value::as_bool))
            .unwrap_or(false);
        if !valid_zone {
            return Ok(plain_error(StatusCode::BAD_REQUEST, "Invalid timeZone"));
        }
        params.push(QueryParam::Text(date.to_owned()));
        let date_param = params.len();
        params.push(QueryParam::Text(time_zone.to_owned()));
        let zone_param = params.len();
        if let Some(hour) = requested_hour {
            params.push(QueryParam::Int32(hour));
            let hour_param = params.len();
            let start = format!(
                "(${date_param}::text::date+make_interval(hours=>${hour_param})) AT TIME ZONE ${zone_param}"
            );
            conditions.push(format!("m.entry_datetime>={start}"));
            conditions.push(format!("m.entry_datetime<({start}+INTERVAL '1 hour')"));
        } else {
            conditions.push(format!(
                "m.entry_datetime>=(${date_param}::text::date AT TIME ZONE ${zone_param})"
            ));
            conditions.push(format!(
                "m.entry_datetime<((${date_param}::text::date+INTERVAL '1 day') AT TIME ZONE ${zone_param})"
            ));
        }
    } else {
        if query.get("hour").is_some_and(|value| !value.is_empty()) {
            return Ok(plain_error(StatusCode::BAD_REQUEST, "hour requires date"));
        }
        for (name, operator, error) in [
            ("from", ">=", "Invalid from date"),
            ("to", "<=", "Invalid to date"),
        ] {
            if let Some(value) = query.get(name).filter(|value| !value.is_empty()) {
                if !valid_iso_datetime(value) {
                    return Ok(plain_error(StatusCode::BAD_REQUEST, error));
                }
                params.push(QueryParam::Text(value.clone()));
                conditions.push(format!(
                    "m.entry_datetime{operator}${}::text::timestamptz",
                    params.len()
                ));
            }
        }
    }
    if let Some(cursor) = cursor {
        params.push(QueryParam::Text(cursor.at));
        let at_param = params.len();
        params.push(QueryParam::Int64(cursor.id));
        let id_param = params.len();
        conditions.push(format!(
            "(m.entry_datetime,m.match_id)<(${at_param}::text::timestamptz,${id_param}::bigint)"
        ));
    }
    let clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };
    let total = if cursor_mode {
        None
    } else {
        Some(
            state
                .database
                .one_json_params(
                    &format!("SELECT COUNT(*) AS total FROM matches m{clause}"),
                    &params,
                )
                .await
                .map_err(|error| ApiError::database(error, &request_id))?
                .and_then(|row| row.get("total").and_then(json_i64))
                .unwrap_or_default(),
        )
    };
    let selected_champion_condition = champion_param
        .map(|parameter| format!("AND mp.champion_id=${parameter}"))
        .unwrap_or_default();
    let limit_param = params.len() + 1;
    let offset_clause = if cursor_mode {
        String::new()
    } else {
        format!(" OFFSET ${}", params.len() + 2)
    };
    let sql = format!(
        "SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.duration_seconds,m.region, \
         COALESCE(mp.champion_id,0) AS champion_id,COALESCE(c.name,'') AS champion_name, \
         COALESCE(mp.win_status,'') AS win_status,COALESCE(mp.kills,0) AS kills, \
         COALESCE(mp.deaths,0) AS deaths,COALESCE(mp.assists,0) AS assists, \
         (SELECT COUNT(*) FROM match_players mp2 WHERE mp2.match_id=m.match_id) AS player_count \
         FROM matches m LEFT JOIN LATERAL (\
           SELECT champion_id,win_status,kills,deaths,assists FROM match_players mp \
           WHERE mp.match_id=m.match_id {selected_champion_condition} \
           ORDER BY mp.entry_datetime DESC,mp.player_id,mp.private_slot LIMIT 1\
         ) mp ON true LEFT JOIN champions c ON c.id=mp.champion_id{clause} \
         ORDER BY m.entry_datetime DESC,m.match_id DESC LIMIT ${limit_param}{offset_clause}"
    );
    let mut data_params = params;
    data_params.push(QueryParam::Int64(if cursor_mode {
        per_page + 1
    } else {
        per_page
    }));
    if !cursor_mode {
        data_params.push(QueryParam::Int64(offset));
    }
    let mut rows = state
        .database
        .query_json_params(&sql, &data_params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    if cursor_mode {
        let has_more = rows.len() > usize::try_from(per_page).unwrap_or_default();
        rows.truncate(usize::try_from(per_page).unwrap_or_default());
        let next_cursor = has_more && !rows.is_empty();
        let next_cursor = next_cursor
            .then(|| rows.last().and_then(encode_cursor))
            .flatten();
        let payload = json!({
            "data":rows,
            "total":null,
            "next_cursor":next_cursor,
            "page":{"current":null,"size":per_page,"totalPages":null}
        });
        state
            .route_cache
            .store(&cache_key, payload.clone(), 60, 180)
            .await;
        let mut response = (StatusCode::OK, Json(payload)).into_response();
        if let Some(value) = next_cursor
            .as_deref()
            .and_then(|value| HeaderValue::from_str(value).ok())
        {
            response
                .headers_mut()
                .insert(HeaderName::from_static("x-next-cursor"), value);
        }
        response.headers_mut().insert(
            HeaderName::from_static("x-cache"),
            HeaderValue::from_static("MISS"),
        );
        return Ok(response);
    }
    let total = total.unwrap_or_default();
    let payload = json!({
        "data":rows,"total":total,"page":{"current":page,"size":per_page,
        "totalPages":if total==0{0}else{(total+per_page-1)/per_page}}
    });
    state
        .route_cache
        .store(&cache_key, payload.clone(), 60, 180)
        .await;
    Ok(cache_miss((StatusCode::OK, Json(payload)).into_response()))
}

async fn hourly_stats(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let Ok((tier_min, tier_max)) = tier_bounds(&query) else {
        return Ok(plain_error(
            StatusCode::BAD_REQUEST,
            "Tier bounds must be between 1 and 26.",
        ));
    };
    let cache_key = matches_query_cache_key("/matches/hourly-stats", &query);
    if let Some(cached) = state.route_cache.get(&cache_key).await {
        return Ok(crate::route_cache::json_cache_response(
            cached.payload,
            "HIT",
            cached.fresh_until,
        ));
    }
    let payload = hourly_stats_payload(&state, &request_id, &query, tier_min, tier_max).await?;
    state
        .route_cache
        .store(&cache_key, payload.clone(), 60, 180)
        .await;
    Ok(cache_miss((StatusCode::OK, Json(payload)).into_response()))
}

async fn hourly_stats_payload(
    state: &MatchesState,
    request_id: &RequestId,
    query: &HashMap<String, String>,
    tier_min: Option<i32>,
    tier_max: Option<i32>,
) -> Result<Value, ApiError> {
    let include_players = query
        .get("includePlayers")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let now = time::OffsetDateTime::now_utc();
    let current_hour = i64::from(now.hour());
    let today = now.date().to_string();
    let yesterday = (now.date() - time::Duration::days(1)).to_string();
    let week_start_date = now.date() - time::Duration::days(6);
    let week_start = week_start_date.to_string();

    let tier_active = tier_min.is_some() || tier_max.is_some();
    let rows = if tier_active {
        let mut predicates = vec!["average_tier IS NOT NULL".to_owned()];
        let mut params = vec![
            QueryParam::Text(yesterday.clone()),
            QueryParam::Text(today.clone()),
            QueryParam::Int32(RANKED_QUEUE_ID),
        ];
        if let Some(minimum) = tier_min {
            params.push(QueryParam::Int32(minimum));
            predicates.push(format!("average_tier>=${}::int", params.len()));
        }
        if let Some(maximum) = tier_max {
            params.push(QueryParam::Int32(maximum + 1));
            predicates.push(format!("average_tier<${}::int", params.len()));
        }
        state.database.query_json_params(
            &format!(
                "WITH ranked_lobbies AS (\
                   SELECT m.match_id,m.entry_datetime,m.region,\
                     AVG(mp.league_tier::numeric) FILTER (\
                       WHERE mp.player_id>0 AND mp.champion_id>0 AND mp.league_tier BETWEEN 1 AND 26\
                     ) AS average_tier \
                   FROM matches m JOIN match_players mp \
                     ON mp.match_id=m.match_id AND mp.entry_datetime=m.entry_datetime \
                   WHERE m.entry_datetime>=$1::text::date \
                     AND m.entry_datetime<($2::text::date+INTERVAL '1 day') AND m.queue_id=$3 \
                   GROUP BY m.match_id,m.entry_datetime,m.region\
                 ), scoped_lobbies AS (\
                   SELECT * FROM ranked_lobbies WHERE {}\
                 ) SELECT \
                   (entry_datetime AT TIME ZONE 'UTC')::date::text AS date,\
                   EXTRACT(HOUR FROM entry_datetime AT TIME ZONE 'UTC')::int AS hour,\
                   COUNT(*) FILTER (WHERE region='NA')::int AS matches_na,\
                   COUNT(*) FILTER (WHERE region='EU')::int AS matches_eu,\
                   COUNT(*) FILTER (WHERE region IN ('ASIA','Asia'))::int AS matches_asia,\
                   COUNT(*) FILTER (WHERE region='SEA')::int AS matches_sea,\
                   COUNT(*) FILTER (WHERE region='JPN')::int AS matches_jpn,\
                   COUNT(*) FILTER (WHERE region='RUS')::int AS matches_rus,\
                   COUNT(*) FILTER (WHERE region='BR')::int AS matches_br,\
                   COUNT(*) FILTER (WHERE region='OCE')::int AS matches_oce,\
                   COUNT(*) FILTER (WHERE region IN ('SA','LATAM'))::int AS matches_sa,\
                   COUNT(*) FILTER (WHERE region IS NULL OR region NOT IN \
                     ('NA','EU','ASIA','Asia','SEA','JPN','RUS','BR','OCE','SA','LATAM'))::int \
                     AS matches_unknown,\
                   COUNT(*)::int AS total_matches \
                 FROM scoped_lobbies GROUP BY 1,2 ORDER BY 1,2",
                predicates.join(" AND ")
            ),
            &params,
        ).await
    } else {
        state.database.query_json(
            "SELECT date::text AS date,hour,matches_na,matches_eu,matches_asia,matches_sea, \
             matches_jpn,matches_rus,matches_br,matches_oce,matches_sa,matches_unknown,total_matches \
             FROM hourly_match_counts WHERE date::text IN ($1,$2) AND queue_id=$3 ORDER BY date,hour",
            &[&today,&yesterday,&RANKED_QUEUE_ID],
        ).await
    }
    .map_err(|error| hourly_database_error(error, request_id, "ranked hourly rows"))?;

    let casual_rows = state
        .database
        .query_json(CASUAL_HOURLY_SQL, &[&today, &yesterday, &RANKED_QUEUE_ID])
        .await
        .map_err(|error| hourly_database_error(error, request_id, "casual hourly rows"))?;
    let queue_rows = state
        .database
        .query_json(
            "SELECT q.queue_id,q.queue_name,q.is_ranked FROM queue_types q \
         WHERE q.stats_enabled=true AND (q.queue_id=$1 OR EXISTS(\
           SELECT 1 FROM match_count_discovery_region_hours h \
           WHERE h.queue_id=q.queue_id AND h.match_count>0)) ORDER BY q.queue_id",
            &[&RANKED_QUEUE_ID],
        )
        .await
        .map_err(|error| hourly_database_error(error, request_id, "queue metadata"))?;
    let ranked_daily = state
        .database
        .query_json_params(
            "SELECT date::text AS date,SUM(total_matches)::int AS total FROM hourly_match_counts \
         WHERE date>=$1::text::date AND date<=$2::text::date AND queue_id=$3 GROUP BY date ORDER BY date",
            &[
                QueryParam::Text(week_start.clone()),
                QueryParam::Text(today.clone()),
                QueryParam::Int32(RANKED_QUEUE_ID),
            ],
        )
        .await
        .map_err(|error| hourly_database_error(error, request_id, "ranked daily rows"))?;
    let nonranked_daily = state
        .database
        .query_json_params(
            "SELECT date::text AS date,queue_id,SUM(match_count)::int AS total \
         FROM match_count_discovery_region_hours \
         WHERE date>=$1::text::date AND date<=$2::text::date AND queue_id<>$3 \
         GROUP BY date,queue_id ORDER BY date,queue_id",
            &[
                QueryParam::Text(week_start.clone()),
                QueryParam::Text(today.clone()),
                QueryParam::Int32(RANKED_QUEUE_ID),
            ],
        )
        .await
        .map_err(|error| hourly_database_error(error, request_id, "nonranked daily rows"))?;
    let daily_players = if include_players {
        let daily_players_cache_key =
            format!("route:matches:/matches/hourly-stats:daily-players:{week_start}:{today}");
        let database = state.database.clone();
        let start = week_start.clone();
        let end = today.clone();
        cached_database_value(
            state.route_cache.clone(),
            daily_players_cache_key,
            600,
            6 * 60 * 60,
            move || {
                let database = database.clone();
                let start = start.clone();
                let end = end.clone();
                async move {
                    database.query_json_params(
            "WITH observations AS MATERIALIZED (\
               SELECT (mp.entry_datetime AT TIME ZONE 'UTC')::date AS activity_date,\
                 $3::int AS queue_id,mp.player_id FROM match_players mp \
               WHERE mp.entry_datetime>=$1::text::date AND mp.entry_datetime<($2::text::date+INTERVAL '1 day') \
                 AND mp.player_id>0 AND COALESCE(mp.source,'direct') IN ('direct','recovered') \
               UNION ALL SELECT (m.entry_datetime AT TIME ZONE 'UTC')::date,m.queue_id,f.player_id \
               FROM casual_matches m JOIN queue_types q ON q.queue_id=m.queue_id AND q.track_presence=true \
               JOIN casual_match_players f ON f.match_id=m.match_id \
               WHERE m.entry_datetime>=$1::text::date AND m.entry_datetime<($2::text::date+INTERVAL '1 day') \
                 AND f.player_id>0 AND f.participant_kind='human' \
               UNION ALL SELECT (m.entry_datetime AT TIME ZONE 'UTC')::date,m.queue_id,f.player_id \
               FROM special_matches m JOIN queue_types q ON q.queue_id=m.queue_id AND q.track_presence=true \
               JOIN special_match_players f ON f.match_id=m.match_id \
               WHERE m.entry_datetime>=$1::text::date AND m.entry_datetime<($2::text::date+INTERVAL '1 day') \
                 AND f.player_id>0 AND f.participant_kind='human'\
             ) SELECT activity_date::text AS date,queue_id,COUNT(DISTINCT player_id)::int AS players \
             FROM observations GROUP BY GROUPING SETS ((activity_date),(activity_date,queue_id)) \
             ORDER BY activity_date,queue_id NULLS FIRST",
                &[QueryParam::Text(start),QueryParam::Text(end),QueryParam::Int32(RANKED_QUEUE_ID)],
                    ).await.map(Value::Array)
                }
            },
        )
        .await
        .map_err(|error| hourly_database_error(error,request_id,"daily player rows"))?
        .as_array()
        .cloned()
        .unwrap_or_default()
    } else {
        Vec::new()
    };

    let row_map = rows
        .iter()
        .map(|row| {
            (
                format!(
                    "{}|{:02}",
                    row.get("date").and_then(Value::as_str).unwrap_or_default(),
                    row.get("hour").and_then(json_i64).unwrap_or_default()
                ),
                row,
            )
        })
        .collect::<HashMap<_, _>>();
    let region_defs = [
        ("matches_na", "NA"),
        ("matches_eu", "EU"),
        ("matches_asia", "Asia"),
        ("matches_br", "BR"),
        ("matches_oce", "OCE"),
        ("matches_sa", "LATAM"),
    ];
    let mut hourly = Vec::new();
    let mut grand_total = 0_i64;
    let mut region_totals = region_defs
        .iter()
        .map(|(_, region)| ((*region).to_owned(), 0_i64))
        .collect::<HashMap<_, _>>();
    for difference in (0..=23).rev() {
        let unwrapped_hour = current_hour - difference;
        let (date, hour) = if unwrapped_hour < 0 {
            (yesterday.as_str(), unwrapped_hour + 24)
        } else {
            (today.as_str(), unwrapped_hour)
        };
        let row = row_map.get(&format!("{date}|{hour:02}")).copied();
        let mut entry = Map::new();
        entry.insert("hour".to_owned(), json!(hour));
        entry.insert("date".to_owned(), json!(date));
        for (column, region) in region_defs {
            let value = row
                .and_then(|row| row.get(column))
                .and_then(json_i64)
                .unwrap_or_default();
            entry.insert(region.to_owned(), json!(value));
            *region_totals.entry(region.to_owned()).or_default() += value;
        }
        let total = row
            .and_then(|row| row.get("total_matches"))
            .and_then(json_i64)
            .unwrap_or_default();
        entry.insert("total".to_owned(), json!(total));
        grand_total += total;
        hourly.push(Value::Object(entry));
    }
    let hours_with_data = i64::try_from(rows.len()).unwrap_or_default().max(1);

    let mut casual_windows: HashMap<(i64, String, i64), HashMap<String, i64>> = HashMap::new();
    for row in &casual_rows {
        let queue_id = row.get("queue_id").and_then(json_i64).unwrap_or_default();
        let date = row
            .get("date")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let hour = row.get("hour").and_then(json_i64).unwrap_or_default();
        let raw_region = row
            .get("region")
            .and_then(Value::as_str)
            .unwrap_or("Unknown");
        let region = if raw_region == "SA" {
            "LATAM"
        } else {
            raw_region
        };
        *casual_windows
            .entry((queue_id, date, hour))
            .or_default()
            .entry(region.to_owned())
            .or_default() += row
            .get("total_matches")
            .and_then(json_i64)
            .unwrap_or_default();
    }
    let detailed_regions = [
        ("matches_na", "NA"),
        ("matches_eu", "EU"),
        ("matches_sea", "SEA"),
        ("matches_jpn", "JPN"),
        ("matches_rus", "RUS"),
        ("matches_br", "BR"),
        ("matches_oce", "OCE"),
        ("matches_sa", "LATAM"),
        ("matches_unknown", "Unknown"),
    ];
    let mut queue_activities = Vec::new();
    for queue in queue_rows {
        let queue_id = queue.get("queue_id").and_then(json_i64).unwrap_or_default();
        let mut totals = detailed_regions
            .iter()
            .map(|(_, region)| ((*region).to_owned(), 0_i64))
            .collect::<HashMap<_, _>>();
        let mut queue_hourly = Vec::new();
        let mut total_24h = 0_i64;
        for difference in (0..=23).rev() {
            let unwrapped_hour = current_hour - difference;
            let (date, hour) = if unwrapped_hour < 0 {
                (yesterday.as_str(), unwrapped_hour + 24)
            } else {
                (today.as_str(), unwrapped_hour)
            };
            let mut counts = Map::new();
            if queue_id == i64::from(RANKED_QUEUE_ID) {
                let ranked = row_map.get(&format!("{date}|{hour:02}")).copied();
                for (column, region) in detailed_regions {
                    let value = ranked
                        .and_then(|row| row.get(column))
                        .and_then(json_i64)
                        .unwrap_or_default();
                    counts.insert(region.to_owned(), json!(value));
                }
                let legacy_asia = ranked
                    .and_then(|row| row.get("matches_asia"))
                    .and_then(json_i64)
                    .unwrap_or_default();
                let sea = counts.get("SEA").and_then(json_i64).unwrap_or_default();
                counts.insert("SEA".to_owned(), json!(sea + legacy_asia));
            } else {
                let observed = casual_windows.get(&(queue_id, date.to_owned(), hour));
                for (_, region) in detailed_regions {
                    counts.insert(
                        region.to_owned(),
                        json!(
                            observed
                                .and_then(|values| values.get(region))
                                .copied()
                                .unwrap_or_default()
                        ),
                    );
                }
            }
            let total = counts.values().filter_map(json_i64).sum::<i64>();
            for (region, value) in &counts {
                *totals.entry(region.clone()).or_default() += json_i64(value).unwrap_or_default();
            }
            total_24h += total;
            queue_hourly.push(json!({"date":date,"hour":hour,"total":total,"regions":counts}));
        }
        let regions = detailed_regions
            .iter()
            .map(|(_, region)| {
                let total = totals.get(*region).copied().unwrap_or_default();
                json!({
                    "region":region,
                    "total24h":total,
                    "matchesPerHour":round_ratio(total,24)
                })
            })
            .collect::<Vec<_>>();
        queue_activities.push(json!({
            "queueId":queue_id,
            "queueName":field(&queue,"queue_name"),
            "ranked":field(&queue,"is_ranked"),
            "total24h":total_24h,
            "regions":regions,
            "hourly":queue_hourly
        }));
    }

    let mut weekly = Vec::new();
    for offset in 0..7 {
        let date = (week_start_date + time::Duration::days(offset)).to_string();
        let ranked = ranked_daily
            .iter()
            .find(|row| row.get("date").and_then(Value::as_str) == Some(date.as_str()))
            .and_then(|row| row.get("total"))
            .and_then(json_i64)
            .unwrap_or_default();
        let mut queues = HashMap::<String, i64>::new();
        if ranked != 0 {
            queues.insert(RANKED_QUEUE_ID.to_string(), ranked);
        }
        for row in nonranked_daily
            .iter()
            .filter(|row| row.get("date").and_then(Value::as_str) == Some(date.as_str()))
        {
            if let (Some(queue_id), Some(total)) = (
                row.get("queue_id").and_then(json_i64),
                row.get("total").and_then(json_i64),
            ) {
                queues.insert(queue_id.to_string(), total);
            }
        }
        let mut players = 0_i64;
        let mut player_queues = HashMap::<String, i64>::new();
        for row in daily_players
            .iter()
            .filter(|row| row.get("date").and_then(Value::as_str) == Some(date.as_str()))
        {
            let count = row.get("players").and_then(json_i64).unwrap_or_default();
            if row.get("queue_id").is_none_or(Value::is_null) {
                players = count;
            } else if let Some(queue_id) = row.get("queue_id").and_then(json_i64) {
                player_queues.insert(queue_id.to_string(), count);
            }
        }
        weekly.push(json!({
            "date":date,
            "total":queues.values().sum::<i64>(),
            "ranked":ranked,
            "queues":queues,
            "players":players,
            "playerQueues":player_queues
        }));
    }
    let regions = region_defs
        .iter()
        .map(|(_, region)| {
            let total = region_totals.get(*region).copied().unwrap_or_default();
            json!({
                "region":region,
                "matchesPerHour":round_ratio(total,hours_with_data),
                "totalToday":total
            })
        })
        .collect::<Vec<_>>();
    let all_queues = queue_activities
        .iter()
        .filter_map(|queue| queue.get("total24h").and_then(json_i64))
        .sum::<i64>();
    Ok(json!({
        "totalToday":grand_total,
        "rankedToday":grand_total,
        "regions":regions,
        "hourly":hourly,
        "currentHour":current_hour,
        "allQueuesTotal24h":all_queues,
        "queues":queue_activities,
        "weekly":weekly
    }))
}

async fn overview(
    State(state): State<MatchesState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    overview_with_cache(state, request_id, query).await
}

/// Purpose: serve the shared match overview cache and refresh stale values in
/// the background. Input: typed route state/query. Output: the public overview
/// response; stale readers never wait for the database under recovery load.
async fn overview_with_cache(
    state: MatchesState,
    request_id: RequestId,
    query: HashMap<String, String>,
) -> Result<Response, ApiError> {
    let Ok((tier_min, tier_max)) = tier_bounds(&query) else {
        return Ok(plain_error(
            StatusCode::BAD_REQUEST,
            "Tier bounds must be between 1 and 26.",
        ));
    };
    let view = query.get("view").map(String::as_str).unwrap_or("");
    let is_activity_view = view == "activity-v3";
    let overview_fresh_ttl = if is_activity_view {
        ACTIVITY_OVERVIEW_FRESH_TTL_SECONDS
    } else {
        60
    };
    let overview_stale_ttl = if is_activity_view {
        ACTIVITY_OVERVIEW_STALE_TTL_SECONDS
    } else {
        900
    };
    let now = now_millis();
    let cache_key = format!(
        "route:matches:/matches/overview:tier-min:{}:tier-max:{}:view:{view}",
        tier_min.map_or_else(|| "all".to_owned(), |value| value.to_string()),
        tier_max.map_or_else(|| "all".to_owned(), |value| value.to_string())
    );
    if let Some(cached) = state.route_cache.get(&cache_key).await {
        let stale = cached.fresh_until <= now;
        if stale && state.route_cache.begin_refresh(&cache_key).await {
            let refresh_state = state.clone();
            let refresh_request_id = request_id.clone();
            let refresh_query = query.clone();
            let refresh_cache = state.route_cache.clone();
            let refresh_key = cache_key.clone();
            let refresh_view = view.to_owned();
            tokio::spawn(async move {
                if let Ok(payload) = overview_payload(
                    &refresh_state,
                    &refresh_request_id,
                    &refresh_query,
                    tier_min,
                    tier_max,
                    &refresh_view,
                    OverviewTtls {
                        fresh: overview_fresh_ttl,
                        stale: overview_stale_ttl,
                    },
                )
                .await
                {
                    refresh_cache
                        .store(
                            &refresh_key,
                            payload,
                            overview_fresh_ttl,
                            overview_stale_ttl,
                        )
                        .await;
                }
                refresh_cache.finish_refresh(&refresh_key).await;
            });
        }
        return Ok(crate::route_cache::json_cache_response(
            cached.payload,
            if stale { "STALE" } else { "HIT" },
            cached.fresh_until,
        ));
    }
    let payload = overview_payload(
        &state,
        &request_id,
        &query,
        tier_min,
        tier_max,
        view,
        OverviewTtls {
            fresh: overview_fresh_ttl,
            stale: overview_stale_ttl,
        },
    )
    .await?;
    state
        .route_cache
        .store(
            &cache_key,
            payload.clone(),
            overview_fresh_ttl,
            overview_stale_ttl,
        )
        .await;
    let mut response = (StatusCode::OK, Json(payload)).into_response();
    response.headers_mut().insert(
        HeaderName::from_static("x-cache"),
        HeaderValue::from_static("MISS"),
    );
    response.headers_mut().insert(
        HeaderName::from_static("cache-control"),
        HeaderValue::from_str(&format!(
            "public, max-age={overview_fresh_ttl}, stale-while-revalidate={overview_stale_ttl}"
        ))
        .expect("static activity cache policy"),
    );
    Ok(response)
}

/// Purpose: pair of cache lifetimes for one overview view so the overview
/// assembly path stays under the clippy argument-count budget.
#[derive(Clone, Copy)]
struct OverviewTtls {
    fresh: u64,
    stale: u64,
}

/// Purpose: build one match-overview payload for both foreground cold misses
/// and the shared background refresh path. Input: route dependencies, typed
/// query bounds/view, and cache lifetimes. Output: the complete JSON payload;
/// no caller owns a second implementation of overview assembly.
async fn overview_payload(
    state: &MatchesState,
    request_id: &RequestId,
    query: &HashMap<String, String>,
    tier_min: Option<i32>,
    tier_max: Option<i32>,
    view: &str,
    ttls: OverviewTtls,
) -> Result<Value, ApiError> {
    let is_activity_view = view == "activity-v3";
    let mut hourly_query = HashMap::new();
    for name in ["tierMin", "tierMax"] {
        if let Some(value) = query.get(name) {
            hourly_query.insert(name.to_owned(), value.clone());
        }
    }
    if is_activity_view {
        hourly_query.insert("includePlayers".to_owned(), "true".to_owned());
    }
    let hourly_cache_key = matches_query_cache_key("/matches/hourly-stats", &hourly_query);
    let hourly = if let Some(cached) = state.route_cache.get(&hourly_cache_key).await
        && (!is_activity_view || cached.fresh_until > now_millis())
    {
        cached.payload
    } else {
        let payload =
            hourly_stats_payload(state, request_id, &hourly_query, tier_min, tier_max).await?;
        state
            .route_cache
            .store(&hourly_cache_key, payload.clone(), ttls.fresh, ttls.stale)
            .await;
        payload
    };
    let recent = state
        .database
        .query_json(
            "SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.duration_seconds,m.region, \
             m.winning_task_force,(SELECT c.name FROM match_players mp JOIN champions c \
               ON c.id=mp.champion_id WHERE mp.match_id=m.match_id LIMIT 1) AS sample_champion \
             FROM matches m WHERE m.queue_id=$1 AND m.entry_datetime>=now()-interval '7 days' \
             ORDER BY m.entry_datetime DESC,m.match_id DESC LIMIT 20",
            &[&RANKED_QUEUE_ID],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    let mut dropped_by_hour = Map::new();
    let mut dropped_ids_by_hour: HashMap<String, Vec<String>> = HashMap::new();
    if tier_min.is_none() && tier_max.is_none() {
        let mut dates = hourly
            .get("hourly")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|entry| entry.get("date").and_then(Value::as_str))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        dates.sort();
        dates.dedup();
        for date in dates {
            let summary = dropped_summary_rows(state, request_id, &date, RANKED_QUEUE_ID).await?;
            for entry in summary {
                let hour = entry.get("hour").and_then(json_i64).unwrap_or_default();
                let dropped = entry.get("dropped").and_then(json_i64).unwrap_or_default();
                dropped_by_hour.insert(format!("{date}|{hour}"), json!(dropped));
            }
            let matches = state
                .database
                .query_json_params(
                    "SELECT match_id,hour FROM dropped_matches WHERE date=$1::text::date AND queue_id=$2 \
                 AND drop_category='api_no_data' AND status<>'complete' \
                 ORDER BY hour ASC,attempts DESC,match_id ASC LIMIT 500",
                    &[
                        QueryParam::Text(date.clone()),
                        QueryParam::Int32(RANKED_QUEUE_ID),
                    ],
                )
                .await
                .map_err(|error| ApiError::database(error, request_id))?;
            for dropped_match in matches {
                let hour = dropped_match
                    .get("hour")
                    .and_then(json_i64)
                    .unwrap_or_default();
                let match_id = dropped_match
                    .get("match_id")
                    .map(json_text)
                    .unwrap_or_default();
                dropped_ids_by_hour
                    .entry(format!("{date}|{hour}"))
                    .or_default()
                    .push(match_id);
            }
        }
    }
    Ok(json!({
        "hourly":hourly,
        "recent":recent,
        "dropped_by_hour":dropped_by_hour,
        "dropped_ids_by_hour":dropped_ids_by_hour
    }))
}

fn positive_i64(value: Option<&str>) -> Option<i64> {
    value?.parse::<i64>().ok().filter(|number| *number > 0)
}

fn bounded_i64(value: Option<&str>, default: i64, min: i64, max: i64) -> i64 {
    value
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(default)
        .clamp(min, max)
}

fn optional_hour(value: Option<&str>) -> Result<Option<i32>, ApiError> {
    match value {
        None | Some("") => Ok(None),
        Some(value) => value
            .parse::<i32>()
            .ok()
            .filter(|hour| (0..=23).contains(hour))
            .map(Some)
            .ok_or_else(|| ApiError::validation("hour must be an integer from 0 to 23")),
    }
}

fn tier_bound(value: Option<&str>) -> Result<Option<i32>, ApiError> {
    match value {
        None | Some("") => Ok(None),
        Some(value) => value
            .parse::<i32>()
            .ok()
            .filter(|tier| (1..=26).contains(tier))
            .map(Some)
            .ok_or_else(|| ApiError::validation("Tier bounds must be between 1 and 26.")),
    }
}

fn tier_bounds(query: &HashMap<String, String>) -> Result<(Option<i32>, Option<i32>), ApiError> {
    let min = tier_bound(query.get("tierMin").map(String::as_str))?;
    let max = tier_bound(query.get("tierMax").map(String::as_str))?;
    if min.is_some() && max.is_some() && min > max {
        return Err(ApiError::validation(
            "Tier bounds must be between 1 and 26.",
        ));
    }
    Ok((min, max))
}

fn valid_date(value: &str) -> bool {
    value.len() == 10
        && value.as_bytes().get(4) == Some(&b'-')
        && value.as_bytes().get(7) == Some(&b'-')
        && value
            .chars()
            .enumerate()
            .all(|(index, value)| index == 4 || index == 7 || value.is_ascii_digit())
}

fn valid_calendar_date(value: &str) -> bool {
    if !valid_date(value) {
        return false;
    }
    let Ok(format) = time::format_description::parse_borrowed::<2>("[year]-[month]-[day]") else {
        return false;
    };
    time::Date::parse(value, &format).is_ok()
}

fn valid_iso_datetime(value: &str) -> bool {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).is_ok()
        || valid_calendar_date(value)
}

fn dropped_date(value: Option<&str>) -> Result<String, ApiError> {
    match value {
        Some(value) if valid_date(value) => Ok(value.to_owned()),
        Some(_) => Err(ApiError::validation("date must be YYYY-MM-DD")),
        None => Ok(time::OffsetDateTime::now_utc().date().to_string()),
    }
}

fn parse_cursor(value: &str) -> Option<MatchCursor> {
    let decoded = URL_SAFE_NO_PAD.decode(value).ok()?;
    let cursor = serde_json::from_slice::<MatchCursor>(&decoded).ok()?;
    (cursor.id > 0 && !cursor.at.is_empty()).then_some(cursor)
}

fn encode_cursor(row: &Value) -> Option<String> {
    let at = row.get("entry_datetime")?.as_str()?;
    let id = row.get("match_id").and_then(json_i64)?;
    Some(URL_SAFE_NO_PAD.encode(json!({"at":at,"id":id}).to_string()))
}

fn json_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn json_text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn round_ratio(numerator: i64, denominator: i64) -> i64 {
    if denominator <= 0 {
        return 0;
    }
    numerator.saturating_add(denominator / 2) / denominator
}

fn hourly_database_error(
    error: paladinscat_core::database::DatabaseError,
    request_id: &RequestId,
    query: &'static str,
) -> ApiError {
    tracing::error!(?error, query, "hourly match statistics query failed");
    ApiError::database(error, request_id)
}

fn matches_query_cache_key(path: &str, query: &HashMap<String, String>) -> String {
    let mut entries = query.iter().collect::<Vec<_>>();
    entries.sort_unstable_by(|left, right| left.0.cmp(right.0).then(left.1.cmp(right.1)));
    let query = entries
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("route:matches:{path}?{query}")
}

fn plain_error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

fn cache_miss(mut response: Response) -> Response {
    response.headers_mut().insert(
        HeaderName::from_static("x-cache"),
        HeaderValue::from_static("MISS"),
    );
    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recent_match_cache_key_uses_only_payload_parameters() {
        let cursor = MatchCursor {
            at: "2026-08-16T20:00:00Z".to_owned(),
            id: 128_000_001,
        };
        assert_eq!(
            paged_matches_cache_key(486, 20, None),
            "route:matches:/matches/paged:queue:486:limit:20:first"
        );
        assert_eq!(
            paged_matches_cache_key(486, 20, Some(&cursor)),
            "route:matches:/matches/paged:queue:486:limit:20:at:2026-08-16T20:00:00Z:id:128000001"
        );
    }

    #[test]
    fn match_overview_serves_stale_while_one_refresh_runs() {
        let source = include_str!("matches.rs");
        let route = source
            .split("async fn overview_with_cache")
            .nth(1)
            .expect("overview cache implementation")
            .split("fn positive_i64")
            .next()
            .expect("overview cache boundary");
        assert!(route.contains("begin_refresh(&cache_key)"));
        assert!(route.contains("if stale { \"STALE\" } else { \"HIT\" }"));
        assert!(route.contains("overview_payload("));
        assert!(route.contains("finish_refresh(&refresh_key)"));
        assert!(route.contains("900"));
    }

    #[test]
    fn casual_hourly_regions_prefer_durable_match_facts_and_canonicalize_aliases() {
        assert!(
            CASUAL_HOURLY_SQL
                .contains("NULLIF(casual.region,''),NULLIF(special.region,''),NULLIF(d.region,'')")
        );
        assert!(CASUAL_HOURLY_SQL.contains("WHEN 'europe' THEN 'EU'"));
        assert!(
            CASUAL_HOURLY_SQL.contains("UPPER(BTRIM(COALESCE(d.region,''))) IN ('','UNKNOWN')")
        );
    }

    #[test]
    fn match_cursor_round_trips_typescript_shape() {
        let row = json!({
            "entry_datetime": "2026-07-30T14:30:45Z",
            "match_id": "1281129406"
        });
        let encoded = encode_cursor(&row).expect("cursor");
        let decoded = parse_cursor(&encoded).expect("decoded cursor");
        assert_eq!(decoded.at, "2026-07-30T14:30:45Z");
        assert_eq!(decoded.id, 1_281_129_406);
    }

    #[test]
    fn match_assets_preserve_valid_punctuation() {
        assert_eq!(
            card_icon_url(Some("Never Surrender!")),
            Value::from("/images/cards/Card_Never_Surrender!.avif")
        );
        assert_eq!(
            talent_icon_url(Some("Seris"), Some("Resuscitate")),
            Value::from("/images/champions/Talent Seris Soul Collector.avif")
        );
    }

    #[test]
    fn custom_match_metadata_uses_stored_scope_without_queue_id_lists() {
        let mut row = json!({
            "queue_id": 99999,
            "queue_name": "Unclassified Queue 99999",
            "stats_scope": "custom",
            "participant_model": "custom"
        });
        normalize_match_queue_metadata(&mut row);
        assert_eq!(row["is_custom"], true);
        assert_eq!(row["queue_name"], "Custom Match");
    }

    #[test]
    fn operator_discovery_uses_the_canonical_pipeline_without_raw_staging() {
        let source = include_str!("matches.rs");
        let route = source
            .split("async fn discover_window")
            .nth(1)
            .expect("operator discovery")
            .split("async fn fact")
            .next()
            .expect("operator discovery body");
        assert!(route.contains(".discover_hour("));
        assert!(!route.contains("raw_ingest_buffer"));
        assert!(!route.contains("getMatchDetailsBatchRaw"));
    }

    #[test]
    fn tiered_activity_bounds_bind_as_postgres_integers() {
        let source = include_str!("matches.rs");
        assert!(source.contains("average_tier>=${}::int"));
        assert!(source.contains("average_tier<${}::int"));
    }

    #[test]
    fn activity_recent_matches_are_bounded_to_recent_hypertable_chunks() {
        let source = include_str!("matches.rs");
        assert!(source.contains("m.entry_datetime>=now()-interval '7 days'"));
    }

    #[test]
    fn casual_raw_facts_keep_items_cards_and_talents_classified() {
        let stored = vec![json!({
            "player_id":"7","player_name":"fixture","champion_id":2491,
            "champion_name":"Furia","raw_player":{
                "active_id_1":2001,"item_active_1":"Chronos","active_level_1":8,
                "item_id_1":3001,"item_purch_1":"Burning Oath","item_level_1":5,
                "item_id_6":4001,"item_purch_6":"Cherish"
            }
        })];
        let players = assemble_match_facts(&stored, vec![], vec![], vec![]);
        assert_eq!(players[0]["items"][0]["item_level"], 2);
        assert_eq!(players[0]["cards"][0]["card_level"], 5);
        assert_eq!(players[0]["talents"][0]["talent_name"], "Cherish");
    }
}
