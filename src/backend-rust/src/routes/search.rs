use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Extension, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::get,
};
use paladinscat_core::{
    config::BackendConfig,
    database::{Database, QueryParam},
    search::SearchIndex,
};
use regex::Regex;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    error::ApiError,
    foundation::AuthenticatedDeveloper,
    raw_hirez_audit::{RawHirezAudit, record_raw_hirez_response},
    request::RequestId,
    security::client_rate_limit_identity,
    workers::{
        relay::WorkerRelayClient,
        requested_match::{RequestedMatchIngestor, RequestedMatchStatus},
    },
};

pub const ROUTE_COUNT: usize = 3;
const MATCH_SEARCH_BY_ID_SQL: &str = r#"
WITH candidates AS (
  SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.region,m.duration_seconds,
    (SELECT COUNT(DISTINCT mp.player_id)::INT FROM match_players mp
      WHERE mp.match_id=m.match_id AND mp.entry_datetime=m.entry_datetime) AS player_count,
    0 AS source_order
  FROM matches m WHERE m.match_id=$1
  UNION ALL
  SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.region,m.duration_seconds,
    (SELECT COUNT(DISTINCT mp.player_id)::INT FROM casual_match_players mp
      WHERE mp.match_id=m.match_id),1
  FROM casual_matches m WHERE m.match_id=$1
  UNION ALL
  SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.region,m.duration_seconds,
    (SELECT COUNT(DISTINCT mp.player_id)::INT FROM special_match_players mp
      WHERE mp.match_id=m.match_id),2
  FROM special_matches m WHERE m.match_id=$1
)
SELECT match_id,entry_datetime,map,queue_id,region,duration_seconds,player_count
FROM candidates ORDER BY source_order,entry_datetime DESC LIMIT 1
"#;

#[derive(Clone)]
struct SearchState {
    database: Database,
    search: SearchIndex,
    relay: Option<WorkerRelayClient>,
    requested_match: Option<RequestedMatchIngestor>,
    redis: paladinscat_core::cache::RedisCache,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchQuery {
    q: Option<String>,
    limit: Option<String>,
    remote: Option<String>,
    remote_target: Option<String>,
    refresh: Option<String>,
    bypass_cache: Option<String>,
}

pub fn router(database: Database, search: SearchIndex, config: Arc<BackendConfig>) -> Router {
    let relay = WorkerRelayClient::new(&config).ok();
    let requested_match = relay.clone().map(|relay| {
        RequestedMatchIngestor::new(
            database.clone(),
            relay,
            Duration::from_millis(config.hirez_relay_timeout_ms),
        )
    });
    let redis = paladinscat_core::cache::RedisCache::new(&config.redis_url)
        .expect("validated Redis configuration");
    Router::new()
        .route("/search/players", get(search_players))
        .route("/search/matches", get(search_matches))
        .route("/search/universal", get(universal_search))
        .with_state(SearchState {
            database,
            search,
            relay,
            requested_match,
            redis,
        })
}

async fn search_players(
    State(state): State<SearchState>,
    Query(query): Query<SearchQuery>,
) -> Response {
    let q = query.q.unwrap_or_default();
    if q.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Missing required query parameter: q" })),
        )
            .into_response();
    }
    let limit = js_truthy_integer(query.limit.as_deref(), 20).min(100);
    let results = state
        .search
        .search(
            "players",
            q.trim(),
            usize::try_from(limit).unwrap_or_default(),
        )
        .await;
    (StatusCode::OK, Json(Value::Array(results))).into_response()
}

async fn search_matches(
    State(state): State<SearchState>,
    Query(query): Query<SearchQuery>,
) -> Response {
    let q = query.q.unwrap_or_default();
    if q.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Missing required query parameter: q" })),
        )
            .into_response();
    }
    let limit = js_truthy_integer(query.limit.as_deref(), 20).min(100);
    let results = state
        .search
        .search(
            "matches",
            q.trim(),
            usize::try_from(limit).unwrap_or_default(),
        )
        .await;
    (StatusCode::OK, Json(Value::Array(results))).into_response()
}

async fn universal_search(
    State(state): State<SearchState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<SearchQuery>,
    headers: HeaderMap,
    developer: Option<Extension<AuthenticatedDeveloper>>,
) -> Result<Response, ApiError> {
    let q = query.q.unwrap_or_default().trim().to_owned();
    if q.is_empty() {
        return Ok((
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "Missing required query parameter: q" })),
        )
            .into_response());
    }
    let limit = positive_bounded_limit(query.limit.as_deref(), 30, 60);
    let per_source_limit = limit.clamp(10, 25);
    let escaped = escape_like(&q);
    let like = format!("%{escaped}%");
    let numeric = is_numeric_id(&q);
    let numeric_value = numeric.then(|| q.parse::<i64>().ok()).flatten();
    let auto_match_lookup = is_likely_match_id(&q) && developer.is_none();
    let explicit_remote = truthy_flag(query.remote.as_deref());
    let wants_remote = explicit_remote || auto_match_lookup;
    let remote_target = if auto_match_lookup {
        Some("match-id".to_owned())
    } else if explicit_remote {
        normalize_remote_target(query.remote_target.as_deref(), &q).map(str::to_owned)
    } else {
        None
    };
    let bypass_remote_cache = explicit_remote
        && remote_target.as_deref() == Some("match-id")
        && (truthy_flag(query.refresh.as_deref()) || truthy_flag(query.bypass_cache.as_deref()));

    let player_params = vec![
        QueryParam::Text(
            numeric_value
                .map(|value| value.to_string())
                .unwrap_or_default(),
        ),
        QueryParam::Text(q.clone()),
        QueryParam::Text(format!("{escaped}%")),
        QueryParam::Text(like.clone()),
        QueryParam::Int64(per_source_limit),
    ];
    // The first expression deliberately casts through NULLIF so one stable
    // parameter type represents TypeScript's nullable numeric value.
    let players = safe_query(
        &state.database,
        "players",
        "SELECT id, name, hz_player_name, hz_gamer_tag, region, platform, \
         kbm_tier, kbm_rank, total_matches, total_wins, \
         CASE \
           WHEN NULLIF($1::TEXT, '')::BIGINT IS NOT NULL \
             AND id = NULLIF($1::TEXT, '')::BIGINT THEN 120 \
           WHEN lower(name) = lower($2) THEN 110 \
           WHEN lower(name) LIKE lower($3) ESCAPE '\\' THEN 92 \
           ELSE 70 \
         END + LEAST(COALESCE(total_matches, 0), 5000) / 500 AS score \
         FROM players \
         WHERE (NULLIF($1::TEXT, '')::BIGINT IS NOT NULL \
                  AND id = NULLIF($1::TEXT, '')::BIGINT) \
            OR name ILIKE $4 ESCAPE '\\' \
            OR hz_player_name ILIKE $4 ESCAPE '\\' \
            OR hz_gamer_tag ILIKE $4 ESCAPE '\\' \
         ORDER BY score DESC, total_matches DESC NULLS LAST, name ASC LIMIT $5",
        &player_params,
    )
    .await;
    let matches = if let Some(match_id) = numeric_value {
        safe_query(
            &state.database,
            "matches",
            MATCH_SEARCH_BY_ID_SQL,
            &[QueryParam::Int64(match_id)],
        )
        .await
    } else {
        Vec::new()
    };
    let champions = safe_query(
        &state.database,
        "champions",
        "SELECT id, name, roles FROM champions \
         WHERE name ILIKE $1 ESCAPE '\\' \
         ORDER BY CASE WHEN lower(name) = lower($2) THEN 0 \
                       WHEN lower(name) LIKE lower($3) ESCAPE '\\' THEN 1 ELSE 2 END, \
                  name ASC LIMIT $4",
        &[
            QueryParam::Text(like.clone()),
            QueryParam::Text(q.clone()),
            QueryParam::Text(format!("{escaped}%")),
            QueryParam::Int64(per_source_limit),
        ],
    )
    .await;
    let items = safe_query(
        &state.database,
        "items",
        "SELECT item_id, item_name, item_type, i.champion_id, c.name AS champion_name \
         FROM items i LEFT JOIN champions c ON c.id = i.champion_id \
         WHERE i.item_name ILIKE $1 ESCAPE '\\' \
         ORDER BY CASE WHEN lower(i.item_name) = lower($2) THEN 0 \
                       WHEN lower(i.item_name) LIKE lower($3) ESCAPE '\\' THEN 1 ELSE 2 END, \
                  i.champion_id NULLS FIRST, i.item_name ASC LIMIT $4",
        &[
            QueryParam::Text(like.clone()),
            QueryParam::Text(q.clone()),
            QueryParam::Text(format!("{escaped}%")),
            QueryParam::Int64(per_source_limit),
        ],
    )
    .await;
    let cards = safe_query(
        &state.database,
        "cards",
        "SELECT card_id, card_name, ca.champion_id, c.name AS champion_name \
         FROM cards ca LEFT JOIN champions c ON c.id = ca.champion_id \
         WHERE ca.card_name ILIKE $1 ESCAPE '\\' \
         ORDER BY CASE WHEN lower(ca.card_name) = lower($2) THEN 0 \
                       WHEN lower(ca.card_name) LIKE lower($3) ESCAPE '\\' THEN 1 ELSE 2 END, \
                  ca.card_name ASC LIMIT $4",
        &[
            QueryParam::Text(like.clone()),
            QueryParam::Text(q.clone()),
            QueryParam::Text(format!("{escaped}%")),
            QueryParam::Int64(per_source_limit),
        ],
    )
    .await;
    let talents = safe_query(
        &state.database,
        "talents",
        "SELECT talent_id, talent_name, t.champion_id, c.name AS champion_name \
         FROM talents t LEFT JOIN champions c ON c.id = t.champion_id \
         WHERE t.talent_name ILIKE $1 ESCAPE '\\' \
         ORDER BY CASE WHEN lower(t.talent_name) = lower($2) THEN 0 \
                       WHEN lower(t.talent_name) LIKE lower($3) ESCAPE '\\' THEN 1 ELSE 2 END, \
                  t.talent_name ASC LIMIT $4",
        &[
            QueryParam::Text(like),
            QueryParam::Text(q.clone()),
            QueryParam::Text(format!("{escaped}%")),
            QueryParam::Int64(per_source_limit),
        ],
    )
    .await;

    let mut results = Vec::new();
    results.extend(players.iter().map(|row| player_result(row, &q, 70.0)));
    results.extend(matches.iter().map(match_result));
    results.extend(champions.iter().map(|row| champion_result(row, &q)));
    results.extend(items.iter().map(|row| item_result(row, &q)));
    results.extend(cards.iter().map(|row| card_result(row, &q)));
    results.extend(talents.iter().map(|row| talent_result(row, &q)));

    let exact_local_player = if numeric {
        players.iter().any(|row| field_text(row, "id") == q)
    } else {
        players.iter().any(|row| {
            ["name", "hz_player_name", "hz_gamer_tag"]
                .iter()
                .any(|field| normalize_text(row.get(*field)) == normalize_text(Some(&json!(q))))
        })
    };
    let exact_local_match = numeric && matches.iter().any(|row| field_text(row, "match_id") == q);
    let mut remote_info = json!({ "attempted": false });

    if wants_remote && remote_target.is_none() {
        remote_info = json!({
            "attempted": false,
            "skipped": true,
            "reason": "remote lookup requires remoteTarget=player-id, player-name, or match-id with an exact safe query"
        });
    } else if let Some(target) = remote_target {
        if matches!(target.as_str(), "player-id" | "player-name") && exact_local_player {
            remote_info = json!({
                "attempted": false,
                "target": target,
                "skipped": true,
                "reason": "player already exists locally"
            });
        } else if target == "match-id" && exact_local_match {
            remote_info = json!({
                "attempted": false,
                "target": target,
                "skipped": true,
                "reason": "match already exists locally"
            });
        } else {
            let identity = request_identity(&headers);
            let remote = run_remote_lookup(
                &state,
                &q,
                &target,
                bypass_remote_cache,
                &identity,
                &request_id,
            )
            .await;
            remote_info = remote.1;
            results.extend(remote.0);
        }
    }

    dedupe_results(&mut results);
    results.sort_by(|left, right| {
        score(right)
            .total_cmp(&score(left))
            .then_with(|| field_text(left, "type").cmp(&field_text(right, "type")))
            .then_with(|| field_text(left, "title").cmp(&field_text(right, "title")))
    });
    results.truncate(usize::try_from(limit).unwrap_or_default());
    let total = results.len();
    let mut response = (
        StatusCode::OK,
        Json(json!({
            "query": q,
            "total": total,
            "data": results,
            "remote": remote_info,
        })),
    )
        .into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static(if wants_remote {
            "private, no-store"
        } else {
            "public, max-age=60"
        }),
    );
    Ok(response)
}

async fn safe_query(
    database: &Database,
    label: &str,
    sql: &str,
    params: &[QueryParam],
) -> Vec<Value> {
    match database.query_json_params(sql, params).await {
        Ok(rows) => rows,
        Err(error) => {
            tracing::error!(label, %error, "universal search source failed");
            Vec::new()
        }
    }
}

async fn run_remote_lookup(
    state: &SearchState,
    q: &str,
    target: &str,
    bypass_cache: bool,
    identity: &str,
    request_id: &RequestId,
) -> (Vec<Value>, Value) {
    let cache_key = format!(
        "universal:{target}:{}",
        if target == "player-name" {
            q.trim().to_lowercase()
        } else {
            q.to_owned()
        }
    );
    if !bypass_cache
        && let Some((results, info)) = read_remote_cache(&state.database, &cache_key).await
    {
        return (results, info);
    }
    let Some(relay) = state.relay.as_ref() else {
        return (
            Vec::new(),
            json!({
                "attempted": true,
                "target": target,
                "cacheHit": false,
                "status": "error",
                "error": "HirezRelay is unavailable"
            }),
        );
    };
    let outcome = async {
        vendor_guard(&state.redis, identity, &format!("search-{target}"), q).await?;
        if target == "match-id" {
            let match_id = q.parse::<i64>().map_err(|error| error.to_string())?;
            let ingestor = state
                .requested_match
                .as_ref()
                .ok_or_else(|| "requested-match ingestion is unavailable".to_owned())?;
            let result = ingestor.ingest(match_id).await;
            match result.status {
                RequestedMatchStatus::Ready => {}
                RequestedMatchStatus::NotFound => return Ok(Vec::new()),
                RequestedMatchStatus::RecoveryFailed | RequestedMatchStatus::ProcessingTimeout => {
                    return Err(result.error.unwrap_or_else(|| {
                        format!("requested match {match_id} did not reach durable facts")
                    }));
                }
            }
            let rows = state
                .database
                .query_json(MATCH_SEARCH_BY_ID_SQL, &[&match_id])
                .await
                .map_err(|error| error.to_string())?;
            return Ok::<Vec<Value>, String>(rows.iter().map(match_result).collect());
        }

        let candidate_ids = if target == "player-id" {
            vec![q.parse::<i64>().map_err(|error| error.to_string())?]
        } else {
            resolve_remote_player_ids(relay, q).await?
        };
        if candidate_ids.is_empty() {
            return Ok(Vec::new());
        }
        let raw = relay
            .call_value(
                "getPlayerBatchLookup",
                vec![json!(candidate_ids)],
                "rust_universal_search",
            )
            .await
            .map_err(|error| error.to_string())?;
        audit_raw_response(
            &state.database,
            "getplayerbatch",
            "getPlayerBatchLookup",
            if target == "player-id" {
                "search_player_id"
            } else {
                "search_player_name_profile"
            },
            q,
            json!({ "playerIds": candidate_ids, "reason": "universal_search_remote_lookup" }),
            &raw,
        )
        .await
        .map_err(|error| error.to_string())?;
        let ids = upsert_remote_profiles(&state.database, &raw)
            .await
            .map_err(|error| error.to_string())?;
        player_results_by_ids(&state.database, &ids, q)
            .await
            .map_err(|error| error.to_string())
    }
    .await;

    match outcome {
        Ok(results) => {
            let status = if results.is_empty() { "miss" } else { "hit" };
            if let Err(error) = write_remote_cache(
                &state.database,
                &cache_key,
                q,
                target,
                status,
                &results,
                None,
            )
            .await
            {
                tracing::warn!(%error, "failed to write universal search cache");
            }
            (
                results,
                json!({
                    "attempted": true,
                    "target": target,
                    "cacheHit": false,
                    "status": status
                }),
            )
        }
        Err(error) => {
            if let Err(cache_error) = write_remote_cache(
                &state.database,
                &cache_key,
                q,
                target,
                "error",
                &[],
                Some(&error),
            )
            .await
            {
                tracing::warn!(%cache_error, "failed to write universal search error cache");
            }
            let _ = request_id;
            (
                Vec::new(),
                json!({
                    "attempted": true,
                    "target": target,
                    "cacheHit": false,
                    "status": "error",
                    "error": error
                }),
            )
        }
    }
}

async fn resolve_remote_player_ids(relay: &WorkerRelayClient, q: &str) -> Result<Vec<i64>, String> {
    let portal = parse_portal_hint(q);
    let (operation, args) = match &portal {
        Some((portal_id, _, value, true)) => (
            "getPlayerIdByPortalUserId",
            vec![json!(portal_id), json!(value)],
        ),
        Some((portal_id, _, value, false)) => (
            "getPlayerIdsByGamerTag",
            vec![json!(portal_id), json!(value)],
        ),
        None => ("getPlayerIdByName", vec![json!(q)]),
    };
    let raw = relay
        .call_value(operation, args, "rust_universal_search")
        .await
        .map_err(|error| error.to_string())?;
    let mut ids = player_ids_from_remote(&raw);
    if ids.is_empty() && portal.is_none() {
        let raw = relay
            .call_value("searchPlayers", vec![json!(q)], "rust_universal_search")
            .await
            .map_err(|error| error.to_string())?;
        let wanted = normalize_lookup_name(q);
        let exact = value_rows(&raw)
            .into_iter()
            .filter(|row| {
                [
                    "Name",
                    "name",
                    "hz_player_name",
                    "hz_gamer_tag",
                    "player_name",
                    "playerName",
                    "gamerTag",
                    "GamerTag",
                ]
                .iter()
                .any(|field| normalize_lookup_name(&field_text(row, field)) == wanted)
            })
            .cloned()
            .collect::<Vec<_>>();
        ids = player_ids_from_remote(&Value::Array(exact));
    }
    Ok(ids)
}

async fn player_results_by_ids(
    database: &Database,
    ids: &[i64],
    q: &str,
) -> Result<Vec<Value>, paladinscat_core::database::DatabaseError> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let rows = database
        .query_json(
            "SELECT id, name, region, platform, kbm_tier, kbm_rank, total_matches, total_wins \
             FROM players WHERE id = ANY($1::bigint[]) \
             ORDER BY total_matches DESC NULLS LAST, name ASC",
            &[&ids],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|row| player_result(row, q, 112.0))
        .collect())
}

async fn upsert_remote_profiles(
    database: &Database,
    raw: &Value,
) -> Result<Vec<i64>, paladinscat_core::database::DatabaseError> {
    let mut ids = Vec::new();
    for row in value_rows(raw) {
        let Some(player_id) = first_i64(row, &["Id", "ActivePlayerId"]).filter(|id| *id > 0) else {
            continue;
        };
        let active_player_id = first_i64(row, &["ActivePlayerId", "Id"]).unwrap_or(player_id);
        let platform_name = nullable_field_text(row, "Name");
        let hz_player_name = nullable_field_text(row, "hz_player_name");
        let hz_gamer_tag = nullable_field_text(row, "hz_gamer_tag");
        let candidates = [
            ("hz_player_name", hz_player_name.as_deref()),
            ("hz_gamer_tag", hz_gamer_tag.as_deref()),
            ("name", platform_name.as_deref()),
        ];
        let selected = candidates
            .iter()
            .find(|(_, value)| value.is_some_and(|value| !synthetic_name(value)));
        let (name_source, name) = selected
            .map(|(source, value)| ((*source).to_owned(), value.unwrap_or_default().to_owned()))
            .unwrap_or_else(|| ("none".to_owned(), format!("Player {player_id}")));
        let api_level = first_i64(row, &["Level", "level"]).unwrap_or(0);
        let total_xp = first_i64(row, &["Total_XP", "total_xp"]).unwrap_or(0);
        let level = resolve_player_level(total_xp, api_level);
        let wins = first_i64(row, &["Wins"]).unwrap_or(0);
        let losses = first_i64(row, &["Losses"]).unwrap_or(0);
        let leaves = first_i64(row, &["Leaves"]).unwrap_or(0);
        let mastery = first_i64(row, &["MasteryLevel"]).unwrap_or(0);
        let hours = first_i64(row, &["HoursPlayed"]).unwrap_or(0);
        let minutes = first_i64(row, &["MinutesPlayed"]).unwrap_or(0);
        let region = normalize_region(&field_text(row, "Region"));
        let platform = nullable_field_text(row, "Platform");
        let privacy = field_text(row, "privacy_flag").eq_ignore_ascii_case("y");
        let mut client = database.connection().await?;
        let transaction = client.transaction().await?;
        transaction
            .execute(
                "INSERT INTO players (\
                   id, active_player_id, name, level, api_level, wins, losses, leaves, \
                   hours_played, minutes_played, mastery_level, region, platform, \
                   total_xp, platform_name, hz_player_name, hz_gamer_tag, name_source, \
                   privacy_flag, first_seen, last_seen, last_updated, hirez_profile_refreshed_at\
                 ) VALUES (\
                   $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,\
                   now(),now(),now(),now()\
                 ) \
                 ON CONFLICT (id) DO UPDATE SET \
                   active_player_id = EXCLUDED.active_player_id, \
                   name = CASE WHEN EXCLUDED.name_source <> 'none' \
                         AND NULLIF(EXCLUDED.name, '') IS NOT NULL THEN EXCLUDED.name ELSE players.name END, \
                   level = EXCLUDED.level, api_level = EXCLUDED.api_level, wins = EXCLUDED.wins, \
                   losses = EXCLUDED.losses, leaves = EXCLUDED.leaves, \
                   hours_played = EXCLUDED.hours_played, minutes_played = EXCLUDED.minutes_played, \
                   mastery_level = EXCLUDED.mastery_level, \
                   region = CASE WHEN EXCLUDED.region <> 'Unknown' THEN EXCLUDED.region ELSE players.region END, \
                   platform = COALESCE(NULLIF(EXCLUDED.platform, ''), players.platform), \
                   total_xp = EXCLUDED.total_xp, platform_name = EXCLUDED.platform_name, \
                   hz_player_name = EXCLUDED.hz_player_name, hz_gamer_tag = EXCLUDED.hz_gamer_tag, \
                   name_source = CASE WHEN EXCLUDED.name_source <> 'none' \
                     THEN EXCLUDED.name_source ELSE players.name_source END, \
                   privacy_flag = EXCLUDED.privacy_flag, hirez_profile_refreshed_at = now(), \
                   last_seen = now(), last_updated = now()",
                &[
                    &player_id,
                    &active_player_id,
                    &name,
                    &i32::try_from(level).unwrap_or_default(),
                    &i32::try_from(api_level).unwrap_or_default(),
                    &i32::try_from(wins).unwrap_or_default(),
                    &i32::try_from(losses).unwrap_or_default(),
                    &i32::try_from(leaves).unwrap_or_default(),
                    &i32::try_from(hours).unwrap_or_default(),
                    &i32::try_from(minutes).unwrap_or_default(),
                    &i32::try_from(mastery).unwrap_or_default(),
                    &region,
                    &platform,
                    &total_xp,
                    &platform_name,
                    &hz_player_name,
                    &hz_gamer_tag,
                    &name_source,
                    &privacy,
                ],
            )
            .await?;
        transaction
            .execute(
                "DELETE FROM player_profile_merged_players WHERE player_id = $1",
                &[&player_id],
            )
            .await?;
        transaction.commit().await?;
        ids.push(player_id);
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

async fn audit_raw_response(
    database: &Database,
    endpoint: &str,
    operation: &str,
    entity_type: &str,
    entity_id: &str,
    params: Value,
    raw: &Value,
) -> Result<(), paladinscat_core::database::DatabaseError> {
    record_raw_hirez_response(
        database,
        RawHirezAudit {
            endpoint,
            operation,
            entity_type,
            entity_id: entity_id.to_owned(),
            params,
            raw_response: raw,
            source: "universal-search-remote-lookup",
        },
    )
    .await?;
    Ok(())
}

async fn ensure_remote_cache(
    database: &Database,
) -> Result<(), paladinscat_core::database::DatabaseError> {
    database
        .query_json(
            "CREATE TABLE IF NOT EXISTS search_remote_lookup_cache (\
               cache_key TEXT PRIMARY KEY, query TEXT NOT NULL, target VARCHAR(30) NOT NULL, \
               status VARCHAR(20) NOT NULL CHECK (status IN ('hit', 'miss', 'error')), \
               result JSONB NOT NULL DEFAULT '[]'::jsonb, error_message TEXT, \
               fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(), expires_at TIMESTAMPTZ NOT NULL\
             ); \
             CREATE INDEX IF NOT EXISTS idx_search_remote_lookup_cache_expires \
               ON search_remote_lookup_cache (expires_at)",
            &[],
        )
        .await?;
    Ok(())
}

async fn read_remote_cache(database: &Database, cache_key: &str) -> Option<(Vec<Value>, Value)> {
    ensure_remote_cache(database).await.ok()?;
    let row = database
        .one_json(
            "SELECT status, result, error_message \
             FROM search_remote_lookup_cache \
             WHERE cache_key = $1 AND expires_at > now() LIMIT 1",
            &[&cache_key],
        )
        .await
        .ok()??;
    let results = row
        .get("result")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut info = json!({
        "attempted": true,
        "target": cache_key.split(':').nth(1).unwrap_or_default(),
        "cacheHit": true,
        "status": field_text(&row, "status")
    });
    if let Some(error) = row.get("error_message").and_then(Value::as_str) {
        info.as_object_mut()
            .expect("remote info")
            .insert("error".to_owned(), json!(error));
    }
    Some((results, info))
}

async fn write_remote_cache(
    database: &Database,
    cache_key: &str,
    query: &str,
    target: &str,
    status: &str,
    results: &[Value],
    error: Option<&str>,
) -> Result<(), paladinscat_core::database::DatabaseError> {
    ensure_remote_cache(database).await?;
    let ttl = if status == "hit" {
        86_400_i32
    } else {
        21_600_i32
    };
    let result = Value::Array(results.to_vec());
    database
        .query_json(
            "INSERT INTO search_remote_lookup_cache (\
               cache_key, query, target, status, result, error_message, fetched_at, expires_at\
             ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,now(),now() + ($7::int * interval '1 second')) \
             ON CONFLICT (cache_key) DO UPDATE SET query = EXCLUDED.query, \
               target = EXCLUDED.target, status = EXCLUDED.status, result = EXCLUDED.result, \
               error_message = EXCLUDED.error_message, fetched_at = EXCLUDED.fetched_at, \
               expires_at = EXCLUDED.expires_at",
            &[&cache_key, &query, &target, &status, &result, &error, &ttl],
        )
        .await?;
    Ok(())
}

async fn vendor_guard(
    redis: &paladinscat_core::cache::RedisCache,
    identity: &str,
    scope: &str,
    entity: &str,
) -> Result<(), String> {
    let client = redis
        .check_rate_limit(
            &format!("vendor-fallback:{scope}:{identity}:public"),
            8,
            60_000,
            false,
        )
        .await;
    if !client.backend_available || !client.allowed {
        return Err(
            "Too many live-data fallbacks. Please wait for the database buffer to refresh."
                .to_owned(),
        );
    }
    let global = redis
        .check_rate_limit("vendor-fallback:global", 180, 60_000, false)
        .await;
    if !global.backend_available || !global.allowed {
        return Err(
            "The live-data fallback is busy. Cached database data remains available.".to_owned(),
        );
    }
    let digest = format!(
        "{:x}",
        Sha256::digest(format!("{scope}:{entity}").as_bytes())
    );
    let entity_limit = redis
        .check_rate_limit(
            &format!("vendor-fallback:entity:{}", &digest[..32]),
            1,
            120_000,
            false,
        )
        .await;
    if !entity_limit.backend_available || !entity_limit.allowed {
        return Err("A live-data attempt for this record already ran recently. Cached database data remains available.".to_owned());
    }
    Ok(())
}

fn player_result(row: &Value, q: &str, base: f64) -> Value {
    let id = field_text(row, "id");
    let name = nonempty_field(row, "name").unwrap_or_else(|| format!("Player {id}"));
    let region = nonempty_field(row, "region").unwrap_or_else(|| "Unknown region".to_owned());
    let platform = nonempty_field(row, "platform").unwrap_or_else(|| "Unknown platform".to_owned());
    let total_matches = value_i64(row.get("total_matches")).unwrap_or(0);
    let tier = row.get("kbm_tier").cloned().unwrap_or(Value::Null);
    let mut subtitle = vec![region, platform];
    if !tier.is_null() {
        subtitle.push(format!("Tier {}", value_display(&tier)));
    }
    subtitle.push(format!("{} matches", thousands(total_matches)));
    json!({
        "type": "player",
        "id": id,
        "title": name,
        "subtitle": subtitle.join(" · "),
        "href": format!("/players/{id}"),
        "score": value_f64(row.get("score")).unwrap_or_else(|| rank_name(&name, q, base)),
        "meta": {
            "region": row.get("region").cloned().unwrap_or(Value::Null),
            "platform": row.get("platform").cloned().unwrap_or(Value::Null),
            "tier": tier,
            "rank": row.get("kbm_rank").cloned().unwrap_or(Value::Null),
            "totalMatches": row.get("total_matches").cloned().unwrap_or(Value::Null),
            "totalWins": row.get("total_wins").cloned().unwrap_or(Value::Null),
        }
    })
}

fn match_result(row: &Value) -> Value {
    let id = field_text(row, "match_id");
    let map = nonempty_field(row, "map").unwrap_or_else(|| "Unknown map".to_owned());
    let region = nonempty_field(row, "region").unwrap_or_else(|| "Unknown region".to_owned());
    let queue_id = value_i64(row.get("queue_id"));
    let player_count = value_i64(row.get("player_count"));
    let mut subtitle = vec![map.clone(), region.clone()];
    if let Some(queue_id) = queue_id.filter(|value| *value != 0) {
        subtitle.push(format!("Queue {queue_id}"));
    }
    if let Some(player_count) = player_count {
        subtitle.push(format!("{player_count}/10 players"));
    }
    json!({
        "type": "match",
        "id": id,
        "title": format!("Match {id}"),
        "subtitle": subtitle.join(" · "),
        "href": format!("/matches/{id}"),
        "score": 118,
        "meta": {
            "entryDatetime": row.get("entry_datetime").cloned().unwrap_or(Value::Null),
            "map": row.get("map").cloned().unwrap_or(Value::Null),
            "queueId": row.get("queue_id").cloned().unwrap_or(Value::Null),
            "region": row.get("region").cloned().unwrap_or(Value::Null),
            "playerCount": row.get("player_count").cloned().unwrap_or(Value::Null),
            "durationSeconds": row.get("duration_seconds").cloned().unwrap_or(Value::Null),
        }
    })
}

fn champion_result(row: &Value, q: &str) -> Value {
    let name = field_text(row, "name");
    let roles = nonempty_field(row, "roles").unwrap_or_else(|| "Champion".to_owned());
    json!({
        "type": "champion",
        "id": field_text(row, "id"),
        "title": name,
        "subtitle": format!("{roles} · stats, talents, cards, leaderboards"),
        "href": format!("/champions/{}", champion_slug(&name)),
        "score": rank_name(&name, q, 88.0),
        "meta": { "role": row.get("roles").cloned().unwrap_or(Value::Null) }
    })
}

fn item_result(row: &Value, q: &str) -> Value {
    let id = field_text(row, "item_id");
    let name = nonempty_field(row, "item_name").unwrap_or_else(|| format!("Item {id}"));
    let item_type = nonempty_field(row, "item_type").unwrap_or_else(|| "Item".to_owned());
    let champion = nonempty_field(row, "champion_name");
    json!({
        "type": "item",
        "id": id,
        "title": name,
        "subtitle": format!("{} · {}", item_type, champion.as_ref().map(|value| format!("{value} reference")).unwrap_or_else(|| "Universal item".to_owned())),
        "href": champion.as_ref().map(|value| format!("/champions/{}", champion_slug(value))).unwrap_or_else(|| "/stats/items".to_owned()),
        "score": rank_name(&name, q, 72.0),
        "meta": {
            "itemType": row.get("item_type").cloned().unwrap_or(Value::Null),
            "championId": row.get("champion_id").cloned().unwrap_or(Value::Null),
            "championName": row.get("champion_name").cloned().unwrap_or(Value::Null),
        }
    })
}

fn card_result(row: &Value, q: &str) -> Value {
    let id = field_text(row, "card_id");
    let name = nonempty_field(row, "card_name").unwrap_or_else(|| format!("Card {id}"));
    let champion = nonempty_field(row, "champion_name");
    json!({
        "type": "card",
        "id": id,
        "title": name,
        "subtitle": champion.as_ref().map(|value| format!("{value} loadout card")).unwrap_or_else(|| "Loadout card".to_owned()),
        "href": champion.as_ref().map(|value| format!("/champions/{}", champion_slug(value))).unwrap_or_else(|| "/stats/loadouts".to_owned()),
        "score": rank_name(&name, q, 76.0),
        "meta": {
            "championId": row.get("champion_id").cloned().unwrap_or(Value::Null),
            "championName": row.get("champion_name").cloned().unwrap_or(Value::Null),
        }
    })
}

fn talent_result(row: &Value, q: &str) -> Value {
    let id = field_text(row, "talent_id");
    let name = nonempty_field(row, "talent_name").unwrap_or_else(|| format!("Talent {id}"));
    let champion = nonempty_field(row, "champion_name");
    json!({
        "type": "talent",
        "id": id,
        "title": name,
        "subtitle": champion.as_ref().map(|value| format!("{value} talent")).unwrap_or_else(|| "Champion talent".to_owned()),
        "href": champion.as_ref().map(|value| format!("/champions/{}", champion_slug(value))).unwrap_or_else(|| "/stats/talents".to_owned()),
        "score": rank_name(&name, q, 78.0),
        "meta": {
            "championId": row.get("champion_id").cloned().unwrap_or(Value::Null),
            "championName": row.get("champion_name").cloned().unwrap_or(Value::Null),
        }
    })
}

fn dedupe_results(results: &mut Vec<Value>) {
    let mut seen = std::collections::HashSet::new();
    results.retain(|result| {
        seen.insert(format!(
            "{}:{}:{}",
            field_text(result, "type"),
            field_text(result, "id"),
            field_text(result, "href")
        ))
    });
}

fn score(value: &Value) -> f64 {
    value_f64(value.get("score")).unwrap_or_default()
}

fn normalize_remote_target<'a>(value: Option<&'a str>, q: &str) -> Option<&'a str> {
    match value.map(str::trim).map(str::to_lowercase).as_deref() {
        Some("player-id") if is_numeric_id(q) => value.map(|_| "player-id"),
        Some("match-id") if is_likely_match_id(q) => value.map(|_| "match-id"),
        Some("player-name") if is_safe_exact_player_name(q) => value.map(|_| "player-name"),
        _ => None,
    }
}

fn parse_portal_hint(value: &str) -> Option<(i64, &'static str, String, bool)> {
    let regex = Regex::new(r"(?i)^(xbox|xbl|psn|ps|playstation|switch|nintendo)[:/](.+)$")
        .expect("portal regex");
    let captures = regex.captures(value.trim())?;
    let prefix = captures.get(1)?.as_str().to_ascii_lowercase();
    let raw = captures.get(2)?.as_str().trim().to_owned();
    if !(2..=64).contains(&raw.len()) {
        return None;
    }
    let (portal_id, label) = match prefix.as_str() {
        "xbox" | "xbl" => (10, "Xbox"),
        "psn" | "ps" | "playstation" => (9, "PlayStation"),
        "switch" | "nintendo" => (22, "Nintendo Switch"),
        _ => return None,
    };
    let portal_user_id = raw.len() >= 6 && raw.bytes().all(|byte| byte.is_ascii_digit());
    Some((portal_id, label, raw, portal_user_id))
}

fn player_ids_from_remote(value: &Value) -> Vec<i64> {
    let mut ids = Vec::new();
    for row in value_rows(value) {
        for field in [
            "player_id",
            "playerId",
            "playerID",
            "PlayerId",
            "Id",
            "id",
            "ActivePlayerId",
            "active_player_id",
        ] {
            if let Some(id) = value_i64(row.get(field)).filter(|id| *id > 0) {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    ids.dedup();
    ids.truncate(20);
    ids
}

fn request_identity(headers: &HeaderMap) -> String {
    let address = headers
        .get("cf-connecting-ip")
        .or_else(|| headers.get("x-forwarded-for"))
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next_back())
        .map(str::trim)
        .unwrap_or("unknown");
    client_rate_limit_identity(address)
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn normalize_text(value: Option<&Value>) -> String {
    value
        .map(value_display)
        .unwrap_or_default()
        .trim()
        .to_lowercase()
}

fn normalize_lookup_name(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn champion_slug(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .filter(|value| value.is_ascii_alphanumeric())
        .collect()
}

fn rank_name(name: &str, q: &str, base: f64) -> f64 {
    let name = name.trim().to_lowercase();
    let q = q.trim().to_lowercase();
    if name.is_empty() || q.is_empty() {
        base
    } else if name == q {
        base + 30.0
    } else if name.starts_with(&q) {
        base + 18.0
    } else if name.contains(&q) {
        base + 8.0
    } else {
        base
    }
}

fn is_numeric_id(value: &str) -> bool {
    let value = value.trim();
    value.len() >= 2 && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn is_likely_match_id(value: &str) -> bool {
    let value = value.trim();
    (10..=13).contains(&value.len())
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && value
            .parse::<i64>()
            .is_ok_and(|value| value >= 1_000_000_000)
}

fn is_safe_exact_player_name(value: &str) -> bool {
    let value = value.trim();
    if parse_portal_hint(value).is_some() {
        return true;
    }
    (3..=32).contains(&value.len())
        && !value.bytes().all(|byte| byte.is_ascii_digit())
        && !value
            .chars()
            .any(|value| [',', '%', '*', '?'].contains(&value))
}

fn truthy_flag(value: Option<&str>) -> bool {
    matches!(value, Some("true" | "1"))
}

fn positive_bounded_limit(value: Option<&str>, fallback: i64, maximum: i64) -> i64 {
    value
        .and_then(paladinscat_core::web_compat::parse_js_integer)
        .filter(|value| *value > 0)
        .unwrap_or(fallback)
        .min(maximum)
}

fn js_truthy_integer(value: Option<&str>, fallback: i64) -> i64 {
    value
        .and_then(paladinscat_core::web_compat::parse_js_integer)
        .filter(|value| *value != 0)
        .unwrap_or(fallback)
}

fn field_text(row: &Value, field: &str) -> String {
    row.get(field).map(value_display).unwrap_or_default()
}

fn nullable_field_text(row: &Value, field: &str) -> Option<String> {
    nonempty_field(row, field)
}

fn nonempty_field(row: &Value, field: &str) -> Option<String> {
    let value = field_text(row, field).replace('\0', "");
    (!value.trim().is_empty()).then_some(value.trim().to_owned())
}

fn value_display(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        value => value.to_string(),
    }
}

fn value_i64(value: Option<&Value>) -> Option<i64> {
    match value? {
        Value::Number(value) => value.as_i64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn first_i64(row: &Value, fields: &[&str]) -> Option<i64> {
    fields.iter().find_map(|field| value_i64(row.get(*field)))
}

fn value_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(value) => value.as_f64(),
        Value::String(value) => value.parse().ok(),
        _ => None,
    }
}

fn value_rows(value: &Value) -> Vec<&Value> {
    value
        .as_array()
        .map(|rows| rows.iter().filter(|row| row.is_object()).collect())
        .unwrap_or_else(|| value.is_object().then_some(value).into_iter().collect())
}

fn synthetic_name(value: &str) -> bool {
    let value = value.trim();
    Regex::new(r"(?i)^[0-9a-f]{20,}User-[0-9a-f]{6,}$")
        .expect("Epic name regex")
        .is_match(value)
        || Regex::new(r"(?i)^DummyPlayer[0-9]+$")
            .expect("dummy name regex")
            .is_match(value)
}

fn normalize_region(value: &str) -> String {
    match value {
        "North America" => "NA",
        "Europe" => "EU",
        "Brazil" => "BR",
        "Australia" => "OCE",
        "Southeast Asia" => "SEA",
        "Japan" => "JPN",
        "Russia" => "RUS",
        "NA" | "EU" | "BR" | "OCE" | "SEA" | "JPN" | "RUS" | "SA" => value,
        _ => "Unknown",
    }
    .to_owned()
}

fn resolve_player_level(total_xp: i64, api_level: i64) -> i64 {
    if total_xp >= 25_480_000 {
        return (total_xp - 25_480_000) / 1_000_000 + 50;
    }
    if total_xp >= 0 {
        let mut threshold = 0_i64;
        for level in 2..=50 {
            threshold += level * 20_000;
            if threshold > total_xp {
                return level - 1;
            }
        }
        return 50;
    }
    api_level.max(0)
}

fn thousands(value: i64) -> String {
    let negative = value < 0;
    let digits = value.unsigned_abs().to_string();
    let mut output = String::new();
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            output.push(',');
        }
        output.push(character);
    }
    if negative {
        output.insert(0, '-');
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn universal_search_shapes_match_typescript_guards() {
        assert!(is_numeric_id("42"));
        assert!(!is_numeric_id("4"));
        assert!(is_likely_match_id("1281115795"));
        assert!(!is_likely_match_id("716515038"));
        assert!(is_safe_exact_player_name("NabiCook"));
        assert!(!is_safe_exact_player_name("12"));
        assert_eq!(champion_slug("Bomb King"), "bombking");
    }

    #[test]
    fn universal_match_search_reads_every_population_table() {
        assert!(MATCH_SEARCH_BY_ID_SQL.contains("FROM matches"));
        assert!(MATCH_SEARCH_BY_ID_SQL.contains("FROM casual_matches"));
        assert!(MATCH_SEARCH_BY_ID_SQL.contains("FROM special_matches"));
        assert!(MATCH_SEARCH_BY_ID_SQL.contains("FROM casual_match_players"));
        assert!(MATCH_SEARCH_BY_ID_SQL.contains("FROM special_match_players"));
    }

    #[test]
    fn player_level_and_localized_match_count_match_typescript() {
        assert_eq!(resolve_player_level(25_480_000, 999), 50);
        assert_eq!(resolve_player_level(26_480_000, 999), 51);
        assert_eq!(thousands(12_345_678), "12,345,678");
    }
}
