use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
};
use paladinscat_core::{
    cache::RedisCache,
    config::BackendConfig,
    database::{Database, DatabaseError, QueryParam},
    web_compat::parse_js_integer,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::{
        RouteCache, cached_database_json, cached_database_value, canonical_route_cache_url,
    },
    workers::relay::WorkerRelayClient,
};

mod moderation;
mod provider;

pub const ROUTE_COUNT: usize = 28;

const RANKED_QUEUE_ID: i32 = 486;
const PLAYERS_OVERVIEW_CACHE_KEY: &str = "route:players:v1:/players/overview";
const PLAYERS_OVERVIEW_FRESH_SECONDS: u64 = 300;
const PLAYERS_OVERVIEW_STALE_SECONDS: u64 = 1_800;
const PLAYER_MATCHES_FRESH_SECONDS: u64 = 300;
const PLAYER_MATCHES_STALE_SECONDS: u64 = 1_800;
const AUTOMATIC_AFK_FRESH_SECONDS: u64 = 300;
const AUTOMATIC_AFK_STALE_SECONDS: u64 = 1_800;
const DISPLAY_NAME_SQL: &str = r#"COALESCE(
  CASE
    WHEN NULLIF(p.hz_player_name, '') IS NOT NULL
      AND p.hz_player_name !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.hz_player_name
  END,
  CASE
    WHEN NULLIF(p.hz_gamer_tag, '') IS NOT NULL
      AND p.hz_gamer_tag !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.hz_gamer_tag
  END,
  CASE
    WHEN NULLIF(p.name, '') IS NOT NULL
      AND p.name !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.name
  END,
  'Player ' || p.id::text
)"#;

#[derive(Clone)]
pub(super) struct PlayersState {
    pub(super) database: Database,
    pub(super) redis: RedisCache,
    pub(super) cache: RouteCache,
    pub(super) relay: Option<WorkerRelayClient>,
}

pub fn router(
    database: Database,
    redis: RedisCache,
    cache: RouteCache,
    config: Arc<BackendConfig>,
) -> Router {
    let relay = WorkerRelayClient::new(&config).ok();
    Router::new()
        .route("/players/overview", get(overview))
        .route("/players/search", get(search))
        .route("/players/discord", get(provider::discord))
        .route(
            "/players/discord/saved-player",
            get(provider::saved_player).put(provider::save_player),
        )
        .route("/players/leaderboard/class", get(class_leaderboard))
        .route(
            "/players/leaderboard/champion-elo",
            get(champion_elo_leaderboard),
        )
        .route(
            "/players/leaderboard/performance",
            get(performance_leaderboard),
        )
        .route("/players/raw/profile", get(provider::raw_profile))
        .route("/players/raw/loadouts", get(provider::raw_loadouts))
        .route("/players/boosted", get(boosted))
        .route("/players/boosted/{id}", get(boosted_detail))
        .route("/players/automatic-afk", get(automatic_afk))
        .route("/players/automatic-afk/{id}", get(automatic_afk_detail))
        .route(
            "/players/alt-account-relations",
            get(moderation::alt_account_relations),
        )
        .route("/players/bulk", get(bulk))
        .route("/players/{id}", get(profile))
        .route("/players/{id}/refresh", post(provider::refresh_profile))
        .route(
            "/players/{id}/alt-account-relations/mine",
            get(moderation::my_alt_account_relations),
        )
        .route(
            "/players/{id}/alt-account-relations",
            post(moderation::create_alt_account_relation),
        )
        .route(
            "/players/{id}/alt-account-relations/{other_id}",
            delete(moderation::delete_alt_account_relation),
        )
        .route("/players/{id}/report", post(moderation::report))
        .route("/players/{id}/clear-tag", post(moderation::clear_tag))
        .route("/players/{id}/matches", get(matches))
        .route("/players/{id}/champions", get(champions))
        .route(
            "/players/{id}/champions/refresh",
            post(provider::refresh_champions),
        )
        .route("/players/{id}/charts", get(charts))
        .route("/players/{id}/loadouts", get(loadouts))
        .route(
            "/players/{id}/loadouts/refresh",
            post(provider::refresh_loadouts),
        )
        .route(
            "/players/{id}/loadouts/decks/{loadout_id}",
            get(loadout_detail),
        )
        .route("/players/{id}/card-winrates", get(card_winrates))
        .with_state(PlayersState {
            database,
            redis,
            cache,
            relay,
        })
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlayerQuery {
    name: Option<String>,
    q: Option<String>,
    region: Option<String>,
    platform: Option<String>,
    tier_min: Option<String>,
    tier_max: Option<String>,
    cheater: Option<String>,
    sus_only: Option<String>,
    weirdo_only: Option<String>,
    hall_of_fame_only: Option<String>,
    dropper_only: Option<String>,
    afk_wintrade_only: Option<String>,
    alt_account_only: Option<String>,
    limit: Option<String>,
    per_page: Option<String>,
    offset: Option<String>,
    page: Option<String>,
    queue_id: Option<String>,
    champion_id: Option<String>,
    win_status: Option<String>,
    from: Option<String>,
    to: Option<String>,
    include: Option<String>,
    role: Option<String>,
    mode: Option<String>,
    metric: Option<String>,
    scope: Option<String>,
    ids: Option<String>,
}

#[derive(Clone, Copy)]
struct Role {
    name: &'static str,
    id: i32,
}

fn role(value: Option<&str>) -> Option<Role> {
    let normalized = value?.to_ascii_lowercase().replace([' ', '_', '-'], "");
    match normalized.as_str() {
        "front" | "frontline" | "frontlinepaladins" => Some(Role {
            name: "Frontline",
            id: 4,
        }),
        "damage" => Some(Role {
            name: "Damage",
            id: 1,
        }),
        "flank" => Some(Role {
            name: "Flank",
            id: 2,
        }),
        "support" => Some(Role {
            name: "Support",
            id: 3,
        }),
        _ => None,
    }
}

fn champion_role_sql(alias: &str) -> String {
    let frontline = "'Ash','Atlas','Azaan','Barik','Fernando','Inara','Khan','Makoa','Nyx','Raum','Ruckus','Terminus','Torvald','Yagorath'";
    let damage = "'Betty La Bomba','Betty la Bomba','Bomb King','Cassie','Dredge','Drogoz','Imani','Kinessa','Lian','Octavia','Omen','Saati','Sha Lin','Strix','Tiberius','Tyra','Viktor','Vivian','Willo'";
    let flank = "'Androxus','Buck','Caspian','Evie','Kasumi','Koga','Lex','Maeve','Skye','Talus','Vatu','VII','Vora','Zhin'";
    let support = "'Corvus','Furia','Grohk','Grover','Io','Jenos','Lillith','Mal Damba','Mal''Damba','Moji','Pip','Rei','Seris','Ying'";
    format!(
        "CASE \
         WHEN {alias}.roles ILIKE '%Frontline%' OR {alias}.roles ILIKE '%Front Line%' OR {alias}.name IN ({frontline}) THEN 'Frontline' \
         WHEN {alias}.roles ILIKE '%Damage%' OR {alias}.name IN ({damage}) THEN 'Damage' \
         WHEN {alias}.roles ILIKE '%Flank%' OR {alias}.name IN ({flank}) THEN 'Flank' \
         WHEN {alias}.roles ILIKE '%Support%' OR {alias}.name IN ({support}) THEN 'Support' \
         ELSE COALESCE(NULLIF({alias}.roles,''),'Unknown') END"
    )
}

fn parsed(value: Option<&str>, fallback: i64) -> i64 {
    value
        .and_then(parse_js_integer)
        .filter(|value| *value != 0)
        .unwrap_or(fallback)
}

fn limit(value: Option<&str>, fallback: i64, maximum: i64) -> i64 {
    parsed(value, fallback).clamp(1, maximum)
}

fn player_id(value: &str) -> Result<i64, ApiError> {
    parse_js_integer(value)
        .filter(|id| *id > 0)
        .ok_or_else(|| ApiError::validation("Invalid player ID"))
}

fn queue_id(value: Option<&str>) -> Result<i32, ApiError> {
    let value = parsed(value, i64::from(RANKED_QUEUE_ID));
    i32::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Invalid queueId."))
}

fn map_database(
    error: paladinscat_core::database::DatabaseError,
    request_id: &RequestId,
) -> ApiError {
    ApiError::database(error, request_id)
}

fn rows_response(rows: Vec<Value>) -> Response {
    (StatusCode::OK, Json(Value::Array(rows))).into_response()
}

fn json_response(value: Value) -> Response {
    (StatusCode::OK, Json(value)).into_response()
}

fn cache(response: &mut Response, value: &'static str) {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static(value));
}

fn value_i64(value: Option<&Value>) -> i64 {
    match value {
        Some(Value::Number(number)) => number.as_i64().unwrap_or_default(),
        Some(Value::String(value)) => value.parse().unwrap_or_default(),
        _ => 0,
    }
}

fn bool_query(value: Option<&String>) -> bool {
    value.is_some_and(|value| value == "true")
}

async fn overview(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        PLAYERS_OVERVIEW_CACHE_KEY.to_owned(),
        PLAYERS_OVERVIEW_FRESH_SECONDS,
        PLAYERS_OVERVIEW_STALE_SECONDS,
        &request_id,
        move || {
            let database = database.clone();
            async move { load_players_overview(database).await }
        },
    )
    .await
}

/// Purpose: build the public player-directory summary from PostgreSQL.
/// Input: typed database handle. Output: complete JSON payload cached only by
/// `overview`; relationship: cache misses, refreshes, and the warmer reuse this
/// one loader and never duplicate the aggregate query.
async fn load_players_overview(database: Database) -> Result<Value, DatabaseError> {
    let counts = database
        .one_json(
            "SELECT \
               COUNT(*) FILTER(WHERE cheater)::BIGINT AS cheaters, \
               COUNT(*) FILTER(WHERE NOT cheater AND sus_count>0)::BIGINT AS suspicious, \
               COUNT(*) FILTER(WHERE weirdo_count>0)::BIGINT AS weirdos, \
               COUNT(*) FILTER(WHERE hall_of_fame_count>0)::BIGINT AS hall_of_fame, \
               COUNT(*) FILTER(WHERE dropper)::BIGINT AS droppers, \
               COUNT(*) FILTER(WHERE afk_wintrade)::BIGINT AS afk_wintrade, \
               COUNT(*) FILTER(WHERE alt_account)::BIGINT AS alt_accounts, \
               (SELECT COUNT(DISTINCT player_id)::BIGINT FROM player_boosted_associations) AS boosted, \
               (SELECT COUNT(*)::BIGINT FROM players_private WHERE is_active) AS private_accounts, \
               (SELECT COUNT(*)::BIGINT FROM party_pair_stats) AS parties \
             FROM players",
            &[],
        )
        .await?
        .unwrap_or_else(|| json!({}));
    Ok(json!({
        "champion_elo":{"data":[]},
        "performance":{},
        "ranked":[],
        "account_elo":{"data":[]},
        "cheaters":[{"total_count":value_i64(counts.get("cheaters"))}],
        "boosted":[{"total_count":value_i64(counts.get("boosted"))}],
        "suspicious":[{"total_count":value_i64(counts.get("suspicious"))}],
        "weirdos":[{"total_count":value_i64(counts.get("weirdos"))}],
        "hall_of_fame":[{"total_count":value_i64(counts.get("hall_of_fame"))}],
        "droppers":[{"total_count":value_i64(counts.get("droppers"))}],
        "afk_wintrade":[{"total_count":value_i64(counts.get("afk_wintrade"))}],
        "alt_accounts":[{"total_count":value_i64(counts.get("alt_accounts"))}],
        "private_accounts":[{"total_count":value_i64(counts.get("private_accounts"))}],
        "party_pairs":[{"total_count":value_i64(counts.get("parties"))}],
        "community_counts":{
          "cheaters":value_i64(counts.get("cheaters")),
          "boosted":value_i64(counts.get("boosted")),
          "suspicious":value_i64(counts.get("suspicious")),
          "weirdos":value_i64(counts.get("weirdos")),
          "hall_of_fame":value_i64(counts.get("hall_of_fame")),
          "droppers":value_i64(counts.get("droppers")),
          "afk_wintrade":value_i64(counts.get("afk_wintrade")),
          "alt_accounts":value_i64(counts.get("alt_accounts"))
        },
        "directory_counts":{
          "private_accounts":value_i64(counts.get("private_accounts")),
          "parties":value_i64(counts.get("parties"))
        }
    }))
}

async fn search(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let name = query.name.as_deref().or(query.q.as_deref());
    let sus_only = bool_query(query.sus_only.as_ref());
    let weirdo_only = bool_query(query.weirdo_only.as_ref());
    let hall_only = bool_query(query.hall_of_fame_only.as_ref());
    let dropper_only = bool_query(query.dropper_only.as_ref());
    let afk_only = bool_query(query.afk_wintrade_only.as_ref());
    let alt_only = bool_query(query.alt_account_only.as_ref());
    let community_count = [
        sus_only,
        weirdo_only,
        hall_only,
        dropper_only,
        afk_only,
        alt_only,
    ]
    .into_iter()
    .filter(|active| *active)
    .count();
    if name.is_none() && query.cheater.is_none() && community_count == 0 {
        return Err(ApiError::validation(
            "Missing required query parameter: name or a player moderation filter",
        ));
    }
    if community_count > 1 {
        return Err(ApiError::validation(
            "Only one community player filter may be used at a time",
        ));
    }

    let mut params = Vec::<QueryParam>::new();
    let mut filters = Vec::<String>::new();
    if let Some(name) = name {
        if name
            .trim()
            .chars()
            .all(|character| character.is_ascii_digit())
        {
            params.push(QueryParam::Int64(name.trim().parse().unwrap_or_default()));
            filters.push(format!("players.id=${}", params.len()));
        } else {
            params.push(QueryParam::Text(format!("%{}%", escape_like(name))));
            filters.push(format!("players.name ILIKE ${} ESCAPE '\\'", params.len()));
        }
    }
    push_text_filter(&mut params, &mut filters, "players.region", query.region);
    push_text_filter(
        &mut params,
        &mut filters,
        "players.platform",
        query.platform,
    );
    push_integer_filter(
        &mut params,
        &mut filters,
        "players.kbm_tier",
        ">=",
        query.tier_min,
    );
    push_integer_filter(
        &mut params,
        &mut filters,
        "players.kbm_tier",
        "<=",
        query.tier_max,
    );
    if query.cheater.as_deref() == Some("true") {
        filters.push("players.cheater=true".to_owned());
    } else if query.cheater.as_deref() == Some("false") {
        filters.push("players.cheater=false".to_owned());
    }
    if sus_only {
        filters.extend([
            "NOT players.cheater".to_owned(),
            "players.sus_count>0".to_owned(),
        ]);
    }
    if weirdo_only {
        filters.push("players.weirdo_count>0".to_owned());
    }
    if hall_only {
        filters.push("players.hall_of_fame_count>0".to_owned());
    }
    if dropper_only {
        filters.push("players.dropper".to_owned());
    }
    if afk_only {
        filters.push("players.afk_wintrade".to_owned());
    }
    if alt_only {
        filters.push("players.alt_account".to_owned());
    }
    let reason_vote_type = if sus_only {
        Some(("suspicious", 3))
    } else if query.cheater.as_deref() == Some("true") {
        Some(("cheater", 1))
    } else {
        None
    };
    let top_reasons = reason_vote_type.map_or_else(
        || "'[]'::jsonb".to_owned(),
        |(vote_type, reason_limit)| {
            format!(
                "COALESCE((SELECT jsonb_agg(jsonb_build_object('reason',reason_counts.reason,'count',reason_counts.reason_count) \
                 ORDER BY reason_counts.reason_count DESC,reason_counts.last_reported_at DESC) \
                 FROM (SELECT btrim(pcv.reason) AS reason,COUNT(*)::INT AS reason_count,MAX(pcv.created_at) AS last_reported_at \
                 FROM player_community_votes pcv WHERE pcv.player_id=players.id AND pcv.vote_type='{vote_type}' \
                 AND btrim(pcv.reason)<>'' GROUP BY btrim(pcv.reason) ORDER BY reason_count DESC,last_reported_at DESC \
                 LIMIT {reason_limit}) reason_counts),'[]'::jsonb)"
            )
        },
    );
    let where_clause = if filters.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", filters.join(" AND "))
    };
    let order = if weirdo_only {
        "weirdo_count"
    } else if hall_only {
        "hall_of_fame_count"
    } else {
        "total_matches"
    };
    params.push(QueryParam::Int64(limit(
        query.limit.as_deref().or(query.per_page.as_deref()),
        20,
        100,
    )));
    let limit_parameter = params.len();
    params.push(QueryParam::Int64(
        parse_js_integer(query.offset.as_deref().unwrap_or("0"))
            .unwrap_or_default()
            .max(0),
    ));
    let rows = state
        .database
        .query_json_params(
            &format!(
                "SELECT id,name,level,wins,losses,kbm_tier,kbm_points,region,platform, \
                 cheater,sus_count,weirdo_count,hall_of_fame_count,dropper,afk_wintrade,alt_account, \
                 EXISTS(SELECT 1 FROM player_boosted_associations association WHERE association.player_id=players.id) AS boosted, \
                 avg_dpm,avg_hpm,avg_egpm,avg_mpm,total_matches,{top_reasons} AS top_reasons, \
                 COUNT(*) OVER() AS total_count, \
                 ROUND(CASE WHEN total_matches>0 THEN total_wins::NUMERIC*100/total_matches \
                   WHEN (wins+losses)>0 THEN wins::NUMERIC*100/(wins+losses) ELSE NULL END,2) AS win_rate \
                 FROM players{where_clause} ORDER BY {order} DESC,name ASC \
                 LIMIT ${limit_parameter} OFFSET ${}",
                params.len()
            ),
            &params,
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let mut response = rows_response(rows);
    cache(&mut response, "public, max-age=60");
    Ok(response)
}

fn escape_like(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn push_text_filter(
    params: &mut Vec<QueryParam>,
    filters: &mut Vec<String>,
    column: &str,
    value: Option<String>,
) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        params.push(QueryParam::Text(value));
        filters.push(format!("{column}=${}", params.len()));
    }
}

fn push_integer_filter(
    params: &mut Vec<QueryParam>,
    filters: &mut Vec<String>,
    column: &str,
    operator: &str,
    value: Option<String>,
) {
    if let Some(value) = value
        .as_deref()
        .and_then(parse_js_integer)
        .and_then(|value| i32::try_from(value).ok())
    {
        params.push(QueryParam::Int32(value));
        filters.push(format!("{column}{operator}${}", params.len()));
    }
}

async fn class_leaderboard(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let role = role(query.role.as_deref()).ok_or_else(|| {
        ApiError::validation("Invalid role. Use Frontline, Damage, Flank, or Support.")
    })?;
    let queue_id = queue_id(query.queue_id.as_deref())?;
    let limit = limit(query.limit.as_deref(), 100, 100);
    let mode = if query.mode.as_deref() == Some("account") {
        "account"
    } else {
        "champion"
    };
    let sql = if mode == "account" {
        format!(
            "SELECT ROW_NUMBER() OVER(ORDER BY pqr.mu DESC,pqr.updated_at DESC,{DISPLAY_NAME_SQL} ASC) AS rank, \
             pqr.player_id,{DISPLAY_NAME_SQL} AS player_name,NULL::TEXT AS champion_name,NULL::INT AS champion_id, \
             pqr.mu::DOUBLE PRECISION AS elo,pqr.mu::DOUBLE PRECISION AS mu,pqr.phi::DOUBLE PRECISION AS phi, \
             COALESCE(rc.total_matches,0)::BIGINT AS total_matches,COALESCE(rc.total_wins,0)::BIGINT AS total_wins, \
             ROUND(CASE WHEN COALESCE(rc.total_matches,0)>0 THEN COALESCE(rc.total_wins,0)::NUMERIC*100/rc.total_matches ELSE NULL END,2) AS win_rate, \
             p.region,COUNT(*) OVER() AS _total FROM player_queue_ratings pqr JOIN players p ON p.id=pqr.player_id \
             JOIN player_queue_rating_summary rc ON rc.player_id=pqr.player_id AND rc.queue_id=pqr.queue_id \
             WHERE pqr.queue_id=$1 AND pqr.mu BETWEEN 0 AND 3500 AND pqr.phi BETWEEN 1 AND 350 \
             AND pqr.volatility BETWEEN 0.001 AND 0.2 AND NOT p.cheater \
             ORDER BY pqr.mu DESC,pqr.updated_at DESC,{DISPLAY_NAME_SQL} ASC LIMIT $2"
        )
    } else {
        format!(
            "SELECT ROW_NUMBER() OVER(ORDER BY best.mu DESC,best.matches_played DESC,best.wins DESC,best.player_id ASC) AS rank, \
             best.player_id,{DISPLAY_NAME_SQL} AS player_name,c.name AS champion_name,best.champion_id, \
             best.mu::DOUBLE PRECISION AS elo,best.mu::DOUBLE PRECISION AS mu,best.phi::DOUBLE PRECISION AS phi, \
             ROUND(CASE WHEN best.matches_played>0 THEN best.wins::NUMERIC*100/best.matches_played ELSE NULL END,2) AS win_rate, \
             best.matches_played AS total_matches,best.wins AS total_wins,p.region,COUNT(*) OVER() AS _total \
             FROM player_best_champion_ratings best JOIN players p ON p.id=best.player_id JOIN champions c ON c.id=best.champion_id \
             WHERE best.role_id=$1 AND best.queue_id=$2 AND NOT p.cheater \
             ORDER BY best.mu DESC,best.matches_played DESC,best.wins DESC,best.player_id ASC LIMIT $3"
        )
    };
    let params = if mode == "account" {
        vec![QueryParam::Int32(queue_id), QueryParam::Int64(limit)]
    } else {
        vec![
            QueryParam::Int32(role.id),
            QueryParam::Int32(queue_id),
            QueryParam::Int64(limit),
        ]
    };
    let mut rows = state
        .database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let total = rows
        .first()
        .map(|row| value_i64(row.get("_total")))
        .unwrap_or_default();
    for row in &mut rows {
        row.as_object_mut().map(|row| row.remove("_total"));
    }
    let mut response = json_response(json!({
        "data":rows,
        "total":total,
        "mode":mode,
        "role":role.name,
        "queue_id":queue_id,
        "page":{"current":1,"size":limit,"totalPages":if total>0 {(total+limit-1)/limit} else {0}}
    }));
    cache(&mut response, "public, max-age=300");
    Ok(response)
}

async fn champion_elo_leaderboard(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let queue_id = queue_id(query.queue_id.as_deref())?;
    let champion_id = query
        .champion_id
        .as_deref()
        .and_then(parse_js_integer)
        .and_then(|value| i32::try_from(value).ok());
    if query.champion_id.is_some() && champion_id.is_none_or(|value| value <= 0) {
        return Err(ApiError::validation("Invalid championId."));
    }
    let limit = limit(query.limit.as_deref(), 100, 200);
    let role = query
        .role
        .as_deref()
        .map(|value| {
            role(Some(value)).ok_or_else(|| {
                ApiError::validation("Invalid role. Use Frontline, Damage, Flank, or Support.")
            })
        })
        .transpose()?;
    let role_sql = champion_role_sql("c");
    let (sql, params) = if let Some(champion_id) = champion_id {
        (
            format!(
                "SELECT ROW_NUMBER() OVER(ORDER BY pcr.mu DESC,pcr.matches_played DESC) AS rank, \
                 pcr.player_id,{DISPLAY_NAME_SQL} AS player_name,pcr.champion_id,c.name AS champion_name,{role_sql} AS class_name, \
                 pcr.mu::DOUBLE PRECISION AS elo,pcr.phi::DOUBLE PRECISION AS phi,pcr.matches_played AS total_matches, \
                 pcr.wins AS total_wins,ROUND(CASE WHEN pcr.matches_played>0 THEN pcr.wins::NUMERIC*100/pcr.matches_played ELSE NULL END,2) AS win_rate, \
                 p.region,COUNT(*) OVER() AS _total FROM player_champion_ratings pcr JOIN champions c ON c.id=pcr.champion_id \
                 JOIN players p ON p.id=pcr.player_id WHERE pcr.champion_id=$1 AND NOT p.cheater AND pcr.matches_played>0 \
                 ORDER BY pcr.mu DESC,pcr.matches_played DESC LIMIT $2"
            ),
            vec![QueryParam::Int32(champion_id), QueryParam::Int64(limit)],
        )
    } else {
        (
            format!(
                "SELECT ROW_NUMBER() OVER(ORDER BY best.mu DESC,best.matches_played DESC,best.wins DESC,best.player_id ASC) AS rank, \
                 best.player_id,{DISPLAY_NAME_SQL} AS player_name,c.name AS champion_name,best.champion_id,{role_sql} AS class_name, \
                 best.mu::DOUBLE PRECISION AS elo,best.phi::DOUBLE PRECISION AS phi, \
                 ROUND(CASE WHEN best.matches_played>0 THEN best.wins::NUMERIC*100/best.matches_played ELSE NULL END,2) AS win_rate, \
                 best.matches_played AS total_matches,best.wins AS total_wins,p.region,COUNT(*) OVER() AS _total \
                 FROM player_best_champion_ratings best JOIN players p ON p.id=best.player_id JOIN champions c ON c.id=best.champion_id \
                 WHERE best.queue_id=$1 AND best.role_id=$2 AND NOT p.cheater \
                 ORDER BY best.mu DESC,best.matches_played DESC,best.wins DESC,best.player_id ASC LIMIT $3"
            ),
            vec![
                QueryParam::Int32(queue_id),
                QueryParam::Int32(role.map_or(0, |role| role.id)),
                QueryParam::Int64(limit),
            ],
        )
    };
    let mut rows = state
        .database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let total = rows
        .first()
        .map(|row| value_i64(row.get("_total")))
        .unwrap_or_default();
    for row in &mut rows {
        row.as_object_mut().map(|row| row.remove("_total"));
    }
    let mut payload = Map::new();
    payload.insert("data".to_owned(), Value::Array(rows));
    payload.insert("total".to_owned(), json!(total));
    payload.insert("queue_id".to_owned(), json!(queue_id));
    if let Some(champion_id) = champion_id {
        payload.insert("champion_id".to_owned(), json!(champion_id));
    } else {
        payload.insert(
            "role".to_owned(),
            json!(role.map_or("Global", |role| role.name)),
        );
    }
    let mut response = json_response(Value::Object(payload));
    cache(&mut response, "public, max-age=300");
    Ok(response)
}

async fn performance_leaderboard(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let (metric, ranked_column, casual_expression) = match query
        .metric
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "dpm" => (
            "dpm",
            "pr.dpm",
            "cmp.damage*60.0/NULLIF(cm.duration_seconds,0)",
        ),
        "hpm" => (
            "hpm",
            "pr.hpm",
            "cmp.healing*60.0/NULLIF(cm.duration_seconds,0)",
        ),
        "gpm" => (
            "gpm",
            "pr.gpm",
            "cmp.credits*60.0/NULLIF(cm.duration_seconds,0)",
        ),
        "mpm" => (
            "mpm",
            "pr.mpm",
            "cmp.mitigation*60.0/NULLIF(cm.duration_seconds,0)",
        ),
        _ => {
            return Err(ApiError::validation(
                "Invalid metric. Use dpm, hpm, gpm, or mpm.",
            ));
        }
    };
    let scope = query
        .scope
        .as_deref()
        .unwrap_or("ranked")
        .trim()
        .to_ascii_lowercase();
    if !matches!(scope.as_str(), "ranked" | "casual") {
        return Err(ApiError::validation("Invalid scope. Use ranked or casual."));
    }
    let role = query
        .role
        .as_deref()
        .map(|value| {
            role(Some(value)).ok_or_else(|| {
                ApiError::validation("Invalid role. Use Frontline, Damage, Flank, or Support.")
            })
        })
        .transpose()?;
    let limit = limit(query.limit.as_deref(), 100, 100);
    let role_sql = champion_role_sql("c");
    let (sql, params, queue_value) = if scope == "casual" {
        let mut filters = vec![
            "cm.stats_eligible=true".to_owned(),
            "cm.quality='complete'".to_owned(),
            "cmp.stats_eligible=true".to_owned(),
            "cmp.participant_kind='human'".to_owned(),
            "cmp.player_id>0".to_owned(),
            "cmp.task_force IN (1,2)".to_owned(),
            "lower(COALESCE(cmp.win_status,'')) IN ('winner','win','loser','loss')".to_owned(),
            "cm.duration_seconds>0".to_owned(),
            format!("({casual_expression})>0"),
            "NOT COALESCE(p.cheater,false)".to_owned(),
        ];
        let mut params = Vec::new();
        if let Some(role) = role {
            params.push(QueryParam::Text(role.name.to_owned()));
            filters.push(format!("{role_sql}=${}", params.len()));
        }
        if let Some(region) = query.region.clone() {
            params.push(QueryParam::Text(region));
            filters.push(format!("cm.region=${}", params.len()));
        }
        params.push(QueryParam::Int64(limit));
        (
            format!(
                "SELECT cmp.match_id,cm.entry_datetime,cmp.player_id, \
                 COALESCE({DISPLAY_NAME_SQL},NULLIF(cmp.player_name,''),'Player '||cmp.player_id::text) AS player_name, \
                 COALESCE(NULLIF(cmp.champion_name,''),c.name) AS champion_name,cmp.champion_id,{role_sql} AS class_name, \
                 ({casual_expression})::DOUBLE PRECISION AS value,cm.region,COALESCE(NULLIF(cmp.platform,''),p.platform) AS platform \
                 FROM casual_match_players cmp JOIN casual_matches cm ON cm.match_id=cmp.match_id \
                 LEFT JOIN players p ON p.id=cmp.player_id LEFT JOIN champions c ON c.id=cmp.champion_id \
                 WHERE {} ORDER BY value DESC,cm.entry_datetime DESC,cmp.match_id DESC,cmp.player_id ASC LIMIT ${}",
                filters.join(" AND "),
                params.len()
            ),
            params,
            None,
        )
    } else {
        let queue_id = queue_id(query.queue_id.as_deref())?;
        if queue_id != RANKED_QUEUE_ID {
            return Ok(json_response(json!({
                "data":[],
                "total":0,
                "metric":metric,
                "scope":scope,
                "queue_id":queue_id,
                "page":{"current":1,"size":limit,"totalPages":0}
            })));
        }
        let mut params = vec![QueryParam::Int32(queue_id)];
        let mut filters = vec![
            "pr.queue_id=$1".to_owned(),
            format!("{ranked_column} IS NOT NULL"),
            format!("{ranked_column}>0"),
            "NOT p.cheater".to_owned(),
        ];
        if let Some(role) = role {
            params.push(QueryParam::Text(role.name.to_owned()));
            filters.push(format!("pr.role_name=${}", params.len()));
        }
        if let Some(region) = query.region.clone() {
            params.push(QueryParam::Text(region));
            filters.push(format!(
                "COALESCE(NULLIF(pr.region,''),p.region)=${}",
                params.len()
            ));
        }
        params.push(QueryParam::Int64(limit));
        (
            format!(
                "SELECT pr.match_id,pr.entry_datetime,pr.player_id,{DISPLAY_NAME_SQL} AS player_name, \
                 pr.champion_name,pr.champion_id,pr.role_name AS class_name,{ranked_column}::DOUBLE PRECISION AS value, \
                 COALESCE(NULLIF(pr.region,''),p.region) AS region,COALESCE(NULLIF(pr.platform,''),p.platform) AS platform \
                 FROM performance_records_ranked pr JOIN players p ON p.id=pr.player_id WHERE {} \
                 ORDER BY {ranked_column} DESC,pr.entry_datetime DESC,pr.match_id DESC,pr.player_id ASC LIMIT ${}",
                filters.join(" AND "),
                params.len()
            ),
            params,
            Some(queue_id),
        )
    };
    let mut rows = state
        .database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| map_database(error, &request_id))?;
    for (index, row) in rows.iter_mut().enumerate() {
        row.as_object_mut()
            .map(|row| row.insert("rank".to_owned(), json!(index + 1)));
    }
    let total = rows.len();
    let mut payload = json!({
        "data":rows,
        "total":total,
        "metric":metric,
        "scope":scope,
        "page":{"current":1,"size":limit,"totalPages":if total>0 {1} else {0}}
    });
    if scope == "casual" {
        payload["queue_ids"] = json!([424, 452]);
    } else {
        payload["queue_id"] = json!(queue_value.unwrap_or(RANKED_QUEUE_ID));
    }
    let mut response = json_response(payload);
    cache(
        &mut response,
        "public, max-age=60, stale-while-revalidate=300",
    );
    Ok(response)
}

async fn boosted(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let limit = limit(
        query.limit.as_deref().or(query.per_page.as_deref()),
        20,
        100,
    );
    let rows = state
        .database
        .query_json(
            "SELECT p.id,p.name,p.platform,p.region,p.kbm_tier,p.kbm_points,p.cheater,p.sus_count, \
             p.weirdo_count,p.hall_of_fame_count,p.total_matches,p.total_wins,p.avg_dpm,p.avg_hpm,p.avg_egpm,p.avg_mpm, \
             SUM(association.match_count)::INT AS party_match_count,MIN(association.first_seen) AS first_seen, \
             MAX(association.last_seen) AS last_seen, \
             jsonb_agg(jsonb_build_object('id',cheater.id,'name',cheater.name,'match_count',association.match_count, \
               'first_seen',association.first_seen,'last_seen',association.last_seen) \
               ORDER BY association.match_count DESC,association.last_seen DESC,cheater.id) AS cheaters, \
             COUNT(*) OVER()::INT AS total_count, \
             ROUND(CASE WHEN p.total_matches>0 THEN p.total_wins::NUMERIC*100/p.total_matches \
               WHEN (p.wins+p.losses)>0 THEN p.wins::NUMERIC*100/(p.wins+p.losses) ELSE NULL END,2) AS win_rate \
             FROM player_boosted_associations association JOIN players p ON p.id=association.player_id \
             JOIN players cheater ON cheater.id=association.cheater_id \
             GROUP BY p.id,p.name,p.platform,p.region,p.kbm_tier,p.kbm_points,p.cheater,p.sus_count,p.weirdo_count, \
               p.hall_of_fame_count,p.total_matches,p.total_wins,p.wins,p.losses,p.avg_dpm,p.avg_hpm,p.avg_egpm,p.avg_mpm \
             ORDER BY party_match_count DESC,last_seen DESC,p.name ASC LIMIT $1",
            &[&limit],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let mut response = rows_response(rows);
    cache(&mut response, "public, max-age=60");
    Ok(response)
}

async fn boosted_detail(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let player = state
        .database
        .one_json(
            "SELECT p.id,p.name,p.platform,p.region,p.kbm_tier,p.kbm_points,p.cheater,p.sus_count, \
             p.weirdo_count,p.hall_of_fame_count,p.total_matches,p.total_wins,p.avg_dpm,p.avg_hpm,p.avg_egpm,p.avg_mpm, \
             SUM(association.match_count)::INT AS party_match_count,MIN(association.first_seen) AS first_seen, \
             MAX(association.last_seen) AS last_seen, \
             jsonb_agg(jsonb_build_object('id',cheater.id,'name',cheater.name,'match_count',association.match_count, \
               'first_seen',association.first_seen,'last_seen',association.last_seen) \
               ORDER BY association.match_count DESC,association.last_seen DESC,cheater.id) AS cheaters, \
             ROUND(CASE WHEN p.total_matches>0 THEN p.total_wins::NUMERIC*100/p.total_matches \
               WHEN (p.wins+p.losses)>0 THEN p.wins::NUMERIC*100/(p.wins+p.losses) ELSE NULL END,2) AS win_rate \
             FROM player_boosted_associations association JOIN players p ON p.id=association.player_id \
             JOIN players cheater ON cheater.id=association.cheater_id WHERE association.player_id=$1 \
             GROUP BY p.id,p.name,p.platform,p.region,p.kbm_tier,p.kbm_points,p.cheater,p.sus_count,p.weirdo_count, \
               p.hall_of_fame_count,p.total_matches,p.total_wins,p.wins,p.losses,p.avg_dpm,p.avg_hpm,p.avg_egpm,p.avg_mpm",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?
        .ok_or_else(|| {
            ApiError::not_found("Boosted player not found", json!({"playerId":player_id}))
        })?;
    let matches = state
        .database
        .query_json(
            "WITH related_matches AS ( \
               SELECT pair.match_id,pair.entry_datetime, \
                 jsonb_agg(DISTINCT jsonb_build_object('id',cheater.id,'name',cheater.name)) AS cheaters \
               FROM player_boosted_associations association JOIN match_party_pairs pair \
                 ON pair.player_low_id=LEAST(association.player_id,association.cheater_id) \
                AND pair.player_high_id=GREATEST(association.player_id,association.cheater_id) \
               JOIN players cheater ON cheater.id=association.cheater_id WHERE association.player_id=$1 \
               GROUP BY pair.match_id,pair.entry_datetime \
             ) SELECT related.match_id,related.entry_datetime,m.map,m.queue_id,COALESCE(m.region,mp.region) AS region, \
               COALESCE(m.duration_seconds,mp.time_in_match,0) AS duration_seconds,m.team1_score,m.team2_score,m.winning_task_force, \
               mp.champion_id,champion.name AS champion_name,mp.win_status,mp.kills,mp.deaths,mp.assists,mp.league_tier, \
               mp.league_points,mp.source,related.cheaters FROM related_matches related \
             JOIN matches m ON m.match_id=related.match_id AND m.entry_datetime=related.entry_datetime \
             JOIN match_players mp ON mp.match_id=related.match_id AND mp.entry_datetime=related.entry_datetime AND mp.player_id=$1 \
             LEFT JOIN champions champion ON champion.id=mp.champion_id \
             ORDER BY related.entry_datetime DESC,related.match_id DESC",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let mut response = json_response(json!({"player":player,"matches":matches}));
    cache(&mut response, "public, max-age=60");
    Ok(response)
}

fn tier_bounds(query: &PlayerQuery) -> Result<(Option<i32>, Option<i32>), ApiError> {
    let minimum = query
        .tier_min
        .as_deref()
        .and_then(parse_js_integer)
        .and_then(|value| i32::try_from(value).ok());
    let maximum = query
        .tier_max
        .as_deref()
        .and_then(parse_js_integer)
        .and_then(|value| i32::try_from(value).ok());
    if minimum.is_some_and(|value| !(1..=26).contains(&value))
        || maximum.is_some_and(|value| !(1..=26).contains(&value))
        || minimum.zip(maximum).is_some_and(|(min, max)| min > max)
    {
        return Err(ApiError::validation(
            "Tier bounds must be between 1 and 26.",
        ));
    }
    Ok((minimum, maximum))
}

fn automatic_afk_filters(
    player_id: Option<i64>,
    bounds: (Option<i32>, Option<i32>),
) -> (Vec<QueryParam>, Vec<String>) {
    let mut params = Vec::new();
    let mut filters = Vec::new();
    if let Some(player_id) = player_id {
        params.push(QueryParam::Int64(player_id));
        filters.push("mp.player_id=$1".to_owned());
    }
    filters.extend(
        [
            "m.queue_id=486",
            "mp.egpm>=0",
            "mp.egpm<70",
            "COALESCE(mis.status,'complete')='complete'",
            "COALESCE(mp.source,'direct') IN ('direct','recovered')",
            "mp.is_ranked=true",
            "mp.player_id>0",
            "mp.champion_id>0",
            "mp.task_force IN (1,2)",
            "LOWER(BTRIM(COALESCE(mp.win_status,''))) IN ('winner','loser','win','loss')",
            "m.duration_seconds>120",
        ]
        .into_iter()
        .map(str::to_owned),
    );
    if bounds.0.is_some() || bounds.1.is_some() {
        params.push(QueryParam::Int32(bounds.0.unwrap_or(1)));
        let minimum = params.len();
        params.push(QueryParam::Int32(bounds.1.unwrap_or(26)));
        filters.push(format!(
            "mlt.lobby_tier BETWEEN ${minimum} AND ${}",
            params.len()
        ));
    }
    (params, filters)
}

async fn automatic_afk(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let bounds = tier_bounds(&query)?;
    let key = format!(
        "route:players:automatic-afk:v1:{}",
        canonical_route_cache_url(&uri)
    );
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        key,
        AUTOMATIC_AFK_FRESH_SECONDS,
        AUTOMATIC_AFK_STALE_SECONDS,
        &request_id,
        move || {
            let database = database.clone();
            let query = query.clone();
            async move { load_automatic_afk(database, &query, bounds).await }
        },
    )
    .await
}

/// Purpose: load the auto-flagged AFK directory rows that back
/// `/players/automatic-afk`. Input: database handle, the parsed query
/// filters, and the resolved lobby-tier bounds. Output: the JSON row array
/// (player aggregates with `automatic_match_count`, ecpm stats, and the
/// paged `total_count`) ordered by activity. Relationship: `automatic_afk`
/// is the only caller and wraps this loader in the shared stale-while-
/// refreshing route cache, so the query text must stay byte-stable.
async fn load_automatic_afk(
    database: Database,
    query: &PlayerQuery,
    bounds: (Option<i32>, Option<i32>),
) -> Result<Value, DatabaseError> {
    let (mut params, filters) = automatic_afk_filters(None, bounds);
    let limit = limit(
        query.limit.as_deref().or(query.per_page.as_deref()),
        20,
        100,
    );
    let offset = parse_js_integer(query.offset.as_deref().unwrap_or("0"))
        .unwrap_or_default()
        .max(0);
    params.push(QueryParam::Int64(limit));
    let limit_parameter = params.len();
    params.push(QueryParam::Int64(offset));
    database
        .query_json_params(
            &format!(
                "SELECT p.id,p.name,p.platform,p.region,p.kbm_tier,p.kbm_points,p.cheater,p.sus_count,p.weirdo_count, \
                 p.hall_of_fame_count,p.dropper,p.afk_wintrade,p.alt_account,p.total_matches,p.total_wins,p.avg_dpm,p.avg_hpm, \
                 p.avg_egpm,p.avg_mpm,EXISTS(SELECT 1 FROM player_boosted_associations association WHERE association.player_id=p.id) AS boosted, \
                 COUNT(*)::INT AS automatic_match_count,MIN(mp.entry_datetime) AS first_seen,MAX(mp.entry_datetime) AS last_seen, \
                 ROUND(MIN(mp.egpm)::NUMERIC,2)::DOUBLE PRECISION AS lowest_ecpm, \
                 ROUND(AVG(mp.egpm)::NUMERIC,2)::DOUBLE PRECISION AS average_ecpm,COUNT(*) OVER()::INT AS total_count, \
                 ROUND(CASE WHEN p.total_matches>0 THEN p.total_wins::NUMERIC*100/p.total_matches \
                   WHEN (p.wins+p.losses)>0 THEN p.wins::NUMERIC*100/(p.wins+p.losses) ELSE NULL END,2) AS win_rate \
                 FROM match_players mp JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime \
                 LEFT JOIN match_ingest_status mis ON mis.match_id=m.match_id \
                 LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime \
                 JOIN players p ON p.id=mp.player_id WHERE {} \
                 GROUP BY p.id,p.name,p.platform,p.region,p.kbm_tier,p.kbm_points,p.cheater,p.sus_count,p.weirdo_count, \
                   p.hall_of_fame_count,p.dropper,p.afk_wintrade,p.alt_account,p.total_matches,p.total_wins,p.wins,p.losses, \
                   p.avg_dpm,p.avg_hpm,p.avg_egpm,p.avg_mpm HAVING COUNT(*)>=10 \
                 ORDER BY automatic_match_count DESC,last_seen DESC,p.name ASC LIMIT ${limit_parameter} OFFSET ${}",
                filters.join(" AND "),
                params.len()
            ),
            &params,
        )
        .await
        .map(Value::Array)
}

/// Purpose: load the player summary and per-match rows that back
/// `/players/automatic-afk/{id}`. Input: database handle, player ID, and
/// the resolved lobby-tier bounds. Output: the `{"player":...,"matches":[...]}`
/// payload. Relationship: `automatic_afk_detail` is the only caller and wraps
/// this loader in the shared stale-while-refreshing route cache, so the query
/// text must stay byte-stable.
async fn load_automatic_afk_detail(
    database: Database,
    player_id: i64,
    bounds: (Option<i32>, Option<i32>),
) -> Result<Value, DatabaseError> {
    let (params, filters) = automatic_afk_filters(Some(player_id), bounds);
    let joins = "FROM match_players mp \
      JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime \
      LEFT JOIN match_ingest_status mis ON mis.match_id=m.match_id \
      LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime";
    let player = database
        .one_json_params(
            &format!(
                "SELECT p.id,p.name,p.platform,p.region,p.afk_wintrade,COUNT(*)::INT AS automatic_match_count, \
                 MIN(mp.entry_datetime) AS first_seen,MAX(mp.entry_datetime) AS last_seen, \
                 ROUND(MIN(mp.egpm)::NUMERIC,2)::DOUBLE PRECISION AS lowest_ecpm, \
                 ROUND(AVG(mp.egpm)::NUMERIC,2)::DOUBLE PRECISION AS average_ecpm {joins} \
                 JOIN players p ON p.id=mp.player_id WHERE {} \
                 GROUP BY p.id,p.name,p.platform,p.region,p.afk_wintrade",
                filters.join(" AND ")
            ),
            &params,
        )
        .await?
        .ok_or_else(|| {
            DatabaseError::NotFound(
                "Automatically flagged player not found".to_owned(),
            )
        })?;
    let matches = database
        .query_json_params(
            &format!(
                "SELECT mp.match_id,mp.entry_datetime,m.map,m.queue_id,COALESCE(m.region,mp.region) AS region,m.duration_seconds, \
                 m.team1_score,m.team2_score,m.winning_task_force,mp.champion_id,champion.name AS champion_name,mp.win_status, \
                 mp.kills,mp.deaths,mp.assists,mp.league_tier,mp.league_points,mp.source, \
                 ROUND(mp.egpm::NUMERIC,2)::DOUBLE PRECISION AS ecpm {joins} \
                 LEFT JOIN champions champion ON champion.id=mp.champion_id WHERE {} \
                 ORDER BY mp.entry_datetime DESC,mp.match_id DESC",
                filters.join(" AND ")
            ),
            &params,
        )
        .await?;
    Ok(json!({"player":player,"matches":matches}))
}

async fn automatic_afk_detail(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(id): Path<String>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let bounds = tier_bounds(&query)?;
    let key = format!(
        "route:players:automatic-afk-detail:v1:{}:{}",
        player_id,
        canonical_route_cache_url(&uri)
    );
    let database = state.database.clone();
    let route_cache = state.cache.clone();
    let payload = cached_database_value(
        route_cache.clone(),
        key.clone(),
        AUTOMATIC_AFK_FRESH_SECONDS,
        AUTOMATIC_AFK_STALE_SECONDS,
        move || {
            let database = database.clone();
            async move { load_automatic_afk_detail(database, player_id, bounds).await }
        },
    )
    .await
    .map_err(|error| match error {
        DatabaseError::NotFound(message) => {
            ApiError::not_found(message, json!({"playerId":player_id}))
        }
        other => map_database(other, &request_id),
    })?;
    // Purpose: mirror the `matches` route so the detail page reports its cache
    // status. Input: the cache key just written by `cached_database_value`.
    // Output: `x-cache: HIT` plus a freshness-derived `cache-control` when the
    // entry is present (every success path stores before returning); fall back
    // to the plain short-lived response otherwise.
    let fresh_until = route_cache.get(&key).await.map(|cached| cached.fresh_until);
    let response = match fresh_until {
        Some(fresh_until) => crate::route_cache::json_cache_response(payload, "HIT", fresh_until),
        None => {
            let mut response = json_response(payload);
            cache(&mut response, "public, max-age=60");
            response
        }
    };
    Ok(response)
}

async fn profile(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let mut player = state
        .database
        .one_json(
            "SELECT p.*, \
               EXISTS(SELECT 1 FROM player_boosted_associations association WHERE association.player_id=p.id) AS boosted, \
               EXISTS(SELECT 1 FROM users verified_user WHERE verified_user.linked_player_id=p.id) AS verified, \
               COALESCE(lc.rank,p.kbm_rank) AS kbm_rank,COALESCE(lc.wins,p.kbm_wins) AS kbm_wins, \
               COALESCE(lc.losses,p.kbm_losses) AS kbm_losses,COALESCE(lc.leaves,p.kbm_leaves) AS kbm_leaves \
             FROM players p LEFT JOIN leaderboard_current lc ON lc.player_id=p.id WHERE p.id=$1",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?
        .ok_or_else(|| {
            ApiError::not_found("Player not found", json!({"playerId":player_id}))
        })?;
    player
        .as_object_mut()
        .map(|player| player.remove("cheater_status"));
    let freshness = provider::profile_freshness(&player, 24 * 60 * 60);
    let global_stats = provider::global_stats(&state, player_id, &request_id).await?;
    let include = query
        .include
        .as_deref()
        .unwrap_or("ratings,champions")
        .split(',')
        .map(str::trim)
        .collect::<Vec<_>>();
    let mut payload = Map::new();
    payload.insert("player".to_owned(), player);
    payload.insert(
        "profileRefresh".to_owned(),
        json!({
            "ttl_seconds":freshness.ttl_seconds,
            "refreshed_at":freshness.refreshed_at,
            "expires_at":freshness.expires_at,
            "remaining_seconds":freshness.remaining_seconds,
            "expired":freshness.expired,
            "was_expired":freshness.expired,
            "attempted":false,
            "refreshed":false,
            "source":if freshness.expired {"stale-database"} else {"database"}
        }),
    );
    payload.insert(
        "globalStats".to_owned(),
        global_stats.unwrap_or(Value::Null),
    );
    if include.contains(&"ratings") {
        let queue_ratings = state
            .database
            .query_json(
                "WITH queue_rating_counts AS ( \
                   SELECT mrs.player_id,m.queue_id,COUNT(DISTINCT mrs.match_id)::INT AS matches_played, \
                     COUNT(DISTINCT mrs.match_id) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::INT AS wins, \
                     COUNT(DISTINCT mrs.match_id) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::INT AS losses \
                   FROM match_rating_snapshots mrs JOIN matches m ON m.match_id=mrs.match_id \
                   LEFT JOIN match_players mp ON mp.match_id=m.match_id AND mp.entry_datetime=m.entry_datetime AND mp.player_id=mrs.player_id \
                   WHERE mrs.player_id=$1 GROUP BY mrs.player_id,m.queue_id \
                 ) SELECT pqr.queue_id,pqr.mu::DOUBLE PRECISION AS mu,pqr.phi::DOUBLE PRECISION AS phi, \
                   pqr.volatility::FLOAT4 AS volatility,COALESCE(qrc.matches_played,0)::INT AS matches_played, \
                   COALESCE(qrc.wins,0)::INT AS wins,COALESCE(qrc.losses,0)::INT AS losses \
                 FROM player_queue_ratings pqr LEFT JOIN queue_rating_counts qrc \
                   ON qrc.player_id=pqr.player_id AND qrc.queue_id=pqr.queue_id \
                 WHERE pqr.player_id=$1 AND pqr.mu BETWEEN 0 AND 3500 AND pqr.phi BETWEEN 1 AND 350 \
                   AND pqr.volatility BETWEEN 0.001 AND 0.2",
                &[&player_id],
            )
            .await
            .map_err(|error| map_database(error, &request_id))?;
        payload.insert("queueRatings".to_owned(), Value::Array(queue_ratings));
        if include.contains(&"champions") {
            let ratings = state
                .database
                .query_json(
                    "SELECT pcr.champion_id,c.name AS champion_name,pcr.mu::DOUBLE PRECISION AS mu, \
                       pcr.phi::DOUBLE PRECISION AS phi,pcr.volatility::FLOAT4 AS volatility, \
                       pcr.matches_played,pcr.wins,pcr.losses FROM player_champion_ratings pcr \
                     JOIN champions c ON c.id=pcr.champion_id WHERE pcr.player_id=$1 \
                       AND pcr.mu BETWEEN 0 AND 3500 AND pcr.phi BETWEEN 1 AND 350 \
                       AND pcr.volatility BETWEEN 0.001 AND 0.2 ORDER BY pcr.mu DESC",
                    &[&player_id],
                )
                .await
                .map_err(|error| map_database(error, &request_id))?;
            payload.insert("championRatings".to_owned(), Value::Array(ratings));
        }
    }
    Ok(json_response(Value::Object(payload)))
}

async fn matches(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(id): Path<String>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let key = format!(
        "route:players:matches:v1:{}",
        canonical_route_cache_url(&uri)
    );
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        key,
        PLAYER_MATCHES_FRESH_SECONDS,
        PLAYER_MATCHES_STALE_SECONDS,
        &request_id,
        move || {
            let database = database.clone();
            let query = query.clone();
            async move { load_player_matches(database, player_id, &query).await }
        },
    )
    .await
}

/// Purpose: load the player match-history rows that back `/players/{id}/matches`.
/// Input: database handle, player id, and the parsed query filters. Output: the
/// JSON row array (ranked + casual + special + history observations) ordered by
/// recency. Relationship: `matches` is the only caller and wraps this loader in
/// the shared stale-while-refreshing route cache, so the query text must stay
/// byte-stable for the `player_history_keeps_special_matches_discoverable` test.
async fn load_player_matches(
    database: Database,
    player_id: i64,
    query: &PlayerQuery,
) -> Result<Value, DatabaseError> {
    let mut params = vec![QueryParam::Int64(player_id)];
    let mut authoritative = vec!["mp.player_id=$1".to_owned()];
    let mut history = vec![
        "h.player_id=$1".to_owned(),
        "(h.expires_at IS NULL OR h.expires_at>now())".to_owned(),
        "NOT EXISTS( \
           SELECT 1 FROM match_players existing WHERE existing.match_id=h.match_id AND existing.player_id=h.player_id \
           UNION ALL SELECT 1 FROM casual_match_players existing WHERE existing.match_id=h.match_id AND existing.player_id=h.player_id \
           UNION ALL SELECT 1 FROM special_match_players existing WHERE existing.match_id=h.match_id AND existing.player_id=h.player_id \
         )"
        .to_owned(),
    ];
    let mut add = |authoritative_sql: &str, history_sql: &str, value: QueryParam| {
        params.push(value);
        let placeholder = format!("${}", params.len());
        authoritative.push(authoritative_sql.replace('?', &placeholder));
        history.push(history_sql.replace('?', &placeholder));
    };
    if let Some(value) = query
        .queue_id
        .as_deref()
        .and_then(parse_js_integer)
        .and_then(|value| i32::try_from(value).ok())
    {
        add(
            "m.queue_id=?",
            "COALESCE(m.queue_id,h.queue_id)=?",
            QueryParam::Int32(value),
        );
    }
    if let Some(value) = query
        .champion_id
        .as_deref()
        .and_then(parse_js_integer)
        .and_then(|value| i32::try_from(value).ok())
    {
        add(
            "mp.champion_id=?",
            "h.champion_id=?",
            QueryParam::Int32(value),
        );
    }
    if let Some(value) = query.win_status.clone() {
        add("mp.win_status=?", "h.win_status=?", QueryParam::Text(value));
    }
    if let Some(value) = query.from.clone() {
        add(
            "m.entry_datetime>=?::timestamptz",
            "COALESCE(m.entry_datetime,h.entry_datetime)>=?::timestamptz",
            QueryParam::Text(value),
        );
    }
    if let Some(value) = query.to.clone() {
        add(
            "m.entry_datetime<=?::timestamptz",
            "COALESCE(m.entry_datetime,h.entry_datetime)<=?::timestamptz",
            QueryParam::Text(value),
        );
    }
    let casual = authoritative
        .iter()
        .map(|filter| filter.replace("mp.", "cmp.").replace("m.", "cm."))
        .collect::<Vec<_>>();
    let special = authoritative
        .iter()
        .map(|filter| filter.replace("mp.", "smp.").replace("m.", "sm."))
        .collect::<Vec<_>>();
    params.push(QueryParam::Int64(limit(query.limit.as_deref(), 20, 100)));
    let limit_parameter = params.len();
    params.push(QueryParam::Int64(
        parse_js_integer(query.offset.as_deref().unwrap_or("0")).unwrap_or_default(),
    ));
    database
        .query_json_params(
            &format!(
                "WITH authoritative AS ( \
                   SELECT m.match_id,m.entry_datetime,m.map,m.queue_id,m.duration_seconds,m.region,mp.champion_id,c.name AS champion_name, \
                     mp.win_status,mp.kills,mp.deaths,mp.assists,mp.damage_done_physical AS damage_done,mp.damage_per_minute, \
                     mp.league_tier,mp.afk_rate AS afk_score,mp.source,true AS authoritative \
                   FROM match_players mp JOIN matches m ON m.match_id=mp.match_id LEFT JOIN champions c ON c.id=mp.champion_id \
                   WHERE {} \
                   UNION ALL \
                   SELECT cm.match_id,cm.entry_datetime,cm.map,cm.queue_id,cm.duration_seconds,cm.region,cmp.champion_id, \
                     COALESCE(c.name,cmp.champion_name),cmp.win_status,cmp.kills,cmp.deaths,cmp.assists,cmp.damage, \
                     CASE WHEN cm.duration_seconds>0 THEN ROUND(cmp.damage::numeric*60/cm.duration_seconds,2)::double precision END, \
                     NULL::int,NULL::double precision,cmp.source,true \
                   FROM casual_match_players cmp JOIN casual_matches cm ON cm.match_id=cmp.match_id \
                   LEFT JOIN champions c ON c.id=cmp.champion_id WHERE {} \
                   UNION ALL \
                   SELECT sm.match_id,sm.entry_datetime,sm.map,sm.queue_id,sm.duration_seconds,sm.region,smp.champion_id, \
                     COALESCE(c.name,smp.champion_name),smp.win_status,smp.kills,smp.deaths,smp.assists,smp.damage, \
                     CASE WHEN sm.duration_seconds>0 THEN ROUND(smp.damage::numeric*60/sm.duration_seconds,2)::double precision END, \
                     NULL::int,NULL::double precision,smp.source,true \
                   FROM special_match_players smp JOIN special_matches sm ON sm.match_id=smp.match_id \
                   LEFT JOIN champions c ON c.id=smp.champion_id WHERE {} \
                 ), history_observations AS ( \
                   SELECT h.match_id,COALESCE(m.entry_datetime,h.entry_datetime),COALESCE(m.map,h.map), \
                     COALESCE(m.queue_id,h.queue_id),COALESCE(m.duration_seconds,h.time_in_match),COALESCE(m.region,h.region), \
                     h.champion_id,COALESCE(c.name,h.champion_name),h.win_status,h.kills,h.deaths,h.assists,h.damage, \
                     CASE WHEN COALESCE(h.time_in_match,0)>0 THEN ROUND((COALESCE(h.damage,0)::NUMERIC/h.time_in_match)*60,2)::DOUBLE PRECISION END, \
                     h.league_tier,NULL::double precision,h.source,false \
                   FROM player_match_history_entries h LEFT JOIN matches m ON m.match_id=h.match_id \
                   LEFT JOIN champions c ON c.id=h.champion_id WHERE {} \
                 ) SELECT * FROM (SELECT * FROM authoritative UNION ALL SELECT * FROM history_observations) combined \
                 ORDER BY entry_datetime DESC NULLS LAST LIMIT ${limit_parameter} OFFSET ${}",
                authoritative.join(" AND "),
                casual.join(" AND "),
                special.join(" AND "),
                history.join(" AND "),
                params.len()
            ),
            &params,
        )
        .await
        .map(Value::Array)
}

async fn champions(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let exists = state
        .database
        .one_json(
            "SELECT 1 AS present FROM players WHERE id=$1",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?
        .is_some();
    if !exists {
        return Err(ApiError::not_found_without_details("Player not found"));
    }
    let role = champion_role_sql("c");
    let rows = state
        .database
        .query_json(
            &format!(
                "SELECT c.id AS champion_id,c.name AS champion_name,{role} AS role,COALESCE(pc.xp,0)::BIGINT AS xp, \
                 COALESCE(pc.ownership_type,'') AS ownership_type,COALESCE(pc.wins,0)::INTEGER AS wins, \
                 COALESCE(pc.losses,0)::INTEGER AS losses,COALESCE(pc.kills,0)::INTEGER AS kills, \
                 COALESCE(pc.deaths,0)::INTEGER AS deaths,COALESCE(pc.assists,0)::INTEGER AS assists, \
                 COALESCE(pc.minutes_played,0)::INTEGER AS minutes_played, \
                 (COALESCE(pc.wins,0)+COALESCE(pc.losses,0))::INTEGER AS matches_played, \
                 CASE WHEN COALESCE(pc.wins,0)+COALESCE(pc.losses,0)>0 \
                   THEN ROUND(COALESCE(pc.wins,0)::NUMERIC*100/(COALESCE(pc.wins,0)+COALESCE(pc.losses,0)),2) END AS win_rate, \
                 pc.last_updated FROM champions c LEFT JOIN player_champions pc ON pc.player_id=$1::BIGINT AND pc.champion_id=c.id \
                 WHERE c.id>0 ORDER BY CASE {role} WHEN 'Frontline' THEN 1 WHEN 'Damage' THEN 2 WHEN 'Flank' THEN 3 \
                   WHEN 'Support' THEN 4 ELSE 5 END,c.name ASC"
            ),
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    Ok(rows_response(rows))
}

async fn charts(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let mut params = vec![QueryParam::Int64(player_id)];
    let mut filters = vec!["mp.player_id=$1".to_owned()];
    if let Some(from) = query.from {
        params.push(QueryParam::Text(from));
        filters.push(format!("mp.entry_datetime>=${}::timestamptz", params.len()));
    }
    if let Some(to) = query.to {
        params.push(QueryParam::Text(to));
        filters.push(format!("mp.entry_datetime<=${}::timestamptz", params.len()));
    }
    params.push(QueryParam::Int64(limit(query.limit.as_deref(), 100, 500)));
    let rows = state
        .database
        .query_json_params(
            &format!(
                "SELECT mp.entry_datetime,mp.champion_id,mp.kills,mp.deaths,mp.assists,mp.damage_per_minute, \
                 mp.gold_earned,mp.win_status,mrs.queue_mu_post::DOUBLE PRECISION AS rating \
                 FROM match_players mp LEFT JOIN match_rating_snapshots mrs ON mrs.match_id=mp.match_id \
                   AND mrs.player_id=mp.player_id AND mrs.champion_id=mp.champion_id \
                 WHERE {} ORDER BY mp.entry_datetime DESC LIMIT ${}",
                filters.join(" AND "),
                params.len()
            ),
            &params,
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    Ok(rows_response(rows))
}

pub(super) async fn loadout_rows(
    state: &PlayersState,
    player_id: i64,
    request_id: &RequestId,
) -> Result<Vec<Value>, ApiError> {
    state
        .database
        .query_json(
            "SELECT pl.id,pl.deck_id,pl.deck_key,pl.champion_id, \
               COALESCE(c.name,'Champion '||pl.champion_id::TEXT) AS champion_name,pl.loadout_name, \
               COALESCE(pl.card_ids,'{}') AS card_ids,COALESCE(pl.card_levels,'{}') AS card_levels, \
               pl.talent_id,pl.fetched_at,pl.updated_at FROM player_loadouts pl \
             LEFT JOIN champions c ON c.id=pl.champion_id WHERE pl.player_id=$1 \
             ORDER BY champion_name ASC,pl.loadout_name ASC,pl.id ASC",
            &[&player_id],
        )
        .await
        .map_err(|error| map_database(error, request_id))
}

async fn loadouts(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let freshness = provider::loadout_freshness(&state, player_id, &request_id).await?;
    Ok(json_response(json!({
        "loadouts":loadout_rows(&state,player_id,&request_id).await?,
        "freshness":freshness,
        "refreshed":false,
        "refresh_error":null
    })))
}

async fn loadout_detail(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path((id, loadout_id)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let loadout_id = parse_js_integer(&loadout_id)
        .filter(|id| *id > 0)
        .ok_or_else(|| ApiError::validation("Invalid player or loadout ID"))?;
    let loadout = state
        .database
        .one_json(
            "SELECT pl.id,pl.deck_id,pl.deck_key,pl.champion_id, \
               COALESCE(c.name,'Champion '||pl.champion_id::TEXT) AS champion_name,pl.loadout_name, \
               COALESCE(pl.card_ids,'{}') AS card_ids,COALESCE(pl.card_levels,'{}') AS card_levels, \
               pl.talent_id,pl.fetched_at,pl.updated_at FROM player_loadouts pl \
             LEFT JOIN champions c ON c.id=pl.champion_id WHERE pl.player_id=$1 AND pl.id=$2",
            &[&player_id, &loadout_id],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found_without_details("Saved loadout not found."))?;
    Ok(json_response(json!({
        "loadout":loadout,
        "freshness":provider::loadout_freshness(&state,player_id,&request_id).await?
    })))
}

async fn card_winrates(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let player_id = player_id(&id)?;
    let champion_id = query
        .champion_id
        .as_deref()
        .and_then(parse_js_integer)
        .filter(|value| *value > 0);
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute(
            "INSERT INTO player_loadout_cards(player_id,champion_id,card_id,card_level,times_used,wins,losses,win_rate,updated_at) \
             SELECT mp.player_id,mp.champion_id,mpc.card_id,mpc.card_level,COUNT(*)::INT, \
               COUNT(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::INT, \
               COUNT(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::INT, \
               ROUND(100.0*COUNT(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::NUMERIC/NULLIF(COUNT(*),0),2),now() \
             FROM match_players mp JOIN match_player_cards mpc ON mpc.match_id=mp.match_id AND mpc.player_id=mp.player_id \
             WHERE mp.player_id=$1 GROUP BY mp.player_id,mp.champion_id,mpc.card_id,mpc.card_level \
             ON CONFLICT(player_id,champion_id,card_id) DO UPDATE SET \
               card_level=EXCLUDED.card_level,times_used=EXCLUDED.times_used,wins=EXCLUDED.wins,losses=EXCLUDED.losses, \
               win_rate=EXCLUDED.win_rate,updated_at=now()",
            &[&player_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    let rows = if let Some(champion_id) = champion_id {
        state
            .database
            .query_json(
                "SELECT champion_id,card_id,card_level,times_used,wins,losses,win_rate,updated_at \
                 FROM player_loadout_cards WHERE player_id=$1 AND champion_id=$2 \
                 ORDER BY times_used DESC,win_rate DESC,card_id,card_level",
                &[&player_id, &champion_id],
            )
            .await
    } else {
        state
            .database
            .query_json(
                "SELECT * FROM player_loadout_cards WHERE player_id=$1 ORDER BY win_rate DESC",
                &[&player_id],
            )
            .await
    }
    .map_err(|error| map_database(error, &request_id))?;
    Ok(rows_response(rows))
}

async fn bulk(
    State(state): State<PlayersState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<PlayerQuery>,
) -> Result<Response, ApiError> {
    let ids = query
        .ids
        .as_deref()
        .unwrap_or_default()
        .split(',')
        .filter_map(|value| parse_js_integer(value.trim()))
        .filter(|value| *value > 0)
        .take(50)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Err(ApiError::validation(
            "Missing or invalid ids parameter. Provide comma-separated player IDs.",
        ));
    }
    let rows = state
        .database
        .query_json(
            "SELECT p.id,p.name,p.level,p.region,p.platform,p.kbm_tier,p.kbm_points,p.cheater,p.sus_count, \
               p.dropper,p.afk_wintrade,p.alt_account, \
               EXISTS(SELECT 1 FROM player_boosted_associations association WHERE association.player_id=p.id) AS boosted, \
               EXISTS(SELECT 1 FROM users u WHERE u.linked_player_id=p.id) AS verified \
             FROM players p WHERE p.id=ANY($1::bigint[])",
            &[&ids],
        )
        .await
        .map_err(|error| map_database(error, &request_id))?;
    let found = rows
        .iter()
        .map(|row| value_i64(row.get("id")))
        .collect::<std::collections::HashSet<_>>();
    let missing = ids
        .iter()
        .filter(|id| !found.contains(id))
        .copied()
        .collect::<Vec<_>>();
    let mut payload = json!({"players":rows,"count":rows.len()});
    if !missing.is_empty() {
        payload["notFound"] = json!(missing);
    }
    Ok(json_response(payload))
}

#[cfg(test)]
mod visibility_tests {
    #[test]
    fn players_overview_uses_the_shared_stale_while_refreshing_cache() {
        let source = include_str!("players.rs");
        let overview = source
            .split_once("async fn overview(")
            .and_then(|(_, rest)| rest.split_once("async fn search(").map(|(route, _)| route))
            .expect("players overview route");
        assert!(overview.contains("cached_database_json("));
        assert!(overview.contains("PLAYERS_OVERVIEW_CACHE_KEY"));
        assert!(overview.contains("load_players_overview"));
    }

    #[test]
    fn automatic_afk_uses_the_shared_stale_while_refreshing_cache() {
        let source = include_str!("players.rs");
        let route = source
            .split_once("async fn automatic_afk(")
            .and_then(|(_, rest)| {
                rest.split_once("async fn automatic_afk_detail(")
                    .map(|(route, _)| route)
            })
            .expect("players automatic-afk route");
        assert!(route.contains("cached_database_json("));
        assert!(route.contains("canonical_route_cache_url"));
        assert!(route.contains("AUTOMATIC_AFK_FRESH_SECONDS"));
        assert!(route.contains("load_automatic_afk"));
    }

    #[test]
    fn automatic_afk_detail_uses_the_shared_stale_while_refreshing_cache() {
        let source = include_str!("players.rs");
        let route = source
            .split_once("async fn automatic_afk_detail(")
            .and_then(|(_, rest)| rest.split_once("async fn profile(").map(|(route, _)| route))
            .expect("players automatic-afk detail route");
        assert!(route.contains("cached_database_value("));
        assert!(route.contains("canonical_route_cache_url"));
        assert!(route.contains("AUTOMATIC_AFK_FRESH_SECONDS"));
        assert!(route.contains("load_automatic_afk_detail"));
        assert!(route.contains("DatabaseError::NotFound"));
    }

    #[test]
    fn player_history_keeps_special_matches_discoverable() {
        let source = include_str!("players.rs");
        let history_route = source
            .split_once("async fn matches(")
            .and_then(|(_, rest)| {
                rest.split_once("async fn champions(")
                    .map(|(route, _)| route)
            })
            .expect("player match-history route");
        assert!(history_route.contains("FROM special_match_players smp JOIN special_matches sm"));
        assert!(!history_route.contains("EXCLUDE_CUSTOM_PLAYER_HISTORY_SQL"));
    }
}
