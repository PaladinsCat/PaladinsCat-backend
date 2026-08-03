use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Extension, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use paladinscat_core::{
    database::{Database, QueryParam},
    web_compat::parse_js_integer,
};
use serde_json::{Value, json};

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::{RouteCache, cached_database_json, canonical_route_cache_url},
};

use super::lobby_tier::{TierBounds, parse_tier_bounds};

mod catalog;
mod maps;
mod performance;
mod presence;
mod summary;

// 12 parent routes + 10 catalog + 4 maps + 2 performance + 4 presence + 7 summary
pub const ROUTE_COUNT: usize = 12 + 10 + 4 + 2 + 4 + 7;

const DEFAULT_FRESH_TTL_SECONDS: u64 = 300;
const TIER_POPULATION_FRESH_TTL_SECONDS: u64 = 900;
const CASUAL_ITEM_SCOPES: &[&str] = &[
    "casual",
    "bot",
    "team_deathmatch",
    "arcade",
    "wave_defense",
    "experiment",
    "newcomer",
    "custom",
    "other",
];
const CHAMPION_ROLE_SQL: &str = r#"CASE
    WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' OR c.name IN ('Ash', 'Atlas', 'Azaan', 'Barik', 'Fernando', 'Inara', 'Khan', 'Makoa', 'Nyx', 'Raum', 'Ruckus', 'Terminus', 'Torvald', 'Yagorath') THEN 'Frontline'
    WHEN c.roles ILIKE '%Damage%' OR c.name IN ('Betty La Bomba', 'Betty la Bomba', 'Bomb King', 'Cassie', 'Dredge', 'Drogoz', 'Imani', 'Kinessa', 'Lian', 'Octavia', 'Omen', 'Saati', 'Sha Lin', 'Strix', 'Tiberius', 'Tyra', 'Viktor', 'Vivian', 'Willo') THEN 'Damage'
    WHEN c.roles ILIKE '%Flank%' OR c.name IN ('Androxus', 'Buck', 'Caspian', 'Evie', 'Kasumi', 'Koga', 'Lex', 'Maeve', 'Skye', 'Talus', 'Vatu', 'VII', 'Vora', 'Zhin') THEN 'Flank'
    WHEN c.roles ILIKE '%Support%' OR c.name IN ('Corvus', 'Furia', 'Grohk', 'Grover', 'Io', 'Jenos', 'Lillith', 'Mal Damba', 'Mal''Damba', 'Moji', 'Pip', 'Rei', 'Seris', 'Ying') THEN 'Support'
    ELSE COALESCE(NULLIF(c.roles, ''), 'Unknown')
  END"#;

#[derive(Clone)]
pub(super) struct StatsState {
    pub(super) database: Database,
    pub(super) cache: RouteCache,
}

pub fn router(database: Database, cache: RouteCache) -> Router {
    Router::new()
        .merge(catalog::router())
        .merge(maps::router())
        .merge(performance::router())
        .merge(presence::router())
        .merge(summary::router())
        .route("/stats/leagues", get(leagues))
        .route("/stats/ranked-leaderboard", get(ranked_leaderboard))
        .route("/stats/leaderboard-log", get(leaderboard_log))
        .route("/stats/tier-population", get(tier_population))
        .route("/stats/champion-leaderboard", get(champion_leaderboard))
        .route("/stats/queues", get(queues))
        .route("/stats/regions", get(regions))
        .route("/stats/platforms", get(platforms))
        .route("/stats/loadouts", get(loadouts))
        .route("/stats/items", get(items))
        .route("/stats/tiers", get(tiers))
        .route("/stats/tiers/summary", get(tiers_summary))
        .with_state(StatsState { database, cache })
}

async fn items(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let mode = query
        .get("mode")
        .map_or_else(|| "ranked".to_owned(), |value| value.to_lowercase());
    if mode != "ranked" && mode != "casual" {
        return Err(ApiError::validation("Mode must be ranked or casual."));
    }
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);

    if mode == "casual" {
        for ranked_only_filter in ["tierMin", "tierMax", "tier", "lobby", "championId", "role"] {
            if query
                .get(ranked_only_filter)
                .is_some_and(|value| !value.is_empty())
            {
                return Err(ApiError::validation(format!(
                    "{ranked_only_filter} is available only for ranked item statistics."
                )));
            }
        }

        let mut params = Vec::new();
        let mut item_where = vec!["1=1".to_owned()];
        let mut population_where = vec!["1=1".to_owned()];
        let requested_scope = query
            .get("scope")
            .map(|value| value.trim().to_lowercase())
            .unwrap_or_default();
        if !requested_scope.is_empty() {
            if !CASUAL_ITEM_SCOPES.contains(&requested_scope.as_str()) {
                return Err(ApiError::validation(format!(
                    "Invalid casual scope. Use {}.",
                    CASUAL_ITEM_SCOPES.join(", ")
                )));
            }
            params.push(QueryParam::Text(requested_scope));
            item_where.push(format!("casual.stats_scope = ${}", params.len()));
            population_where.push(format!("ledger.stats_scope = ${}", params.len()));
        }

        if query.get("queueId").is_some_and(|value| !value.is_empty()) {
            let queue_id = parse_javascript_number_integer(query.get("queueId"))
                .and_then(|value| i32::try_from(value).ok())
                .filter(|value| *value > 0 && *value != 486)
                .ok_or_else(|| {
                    ApiError::validation("queueId must identify a positive non-ranked queue.")
                })?;
            params.push(QueryParam::Int32(queue_id));
            item_where.push(format!("casual.queue_id = ${}", params.len()));
            population_where.push(format!("ledger.queue_id = ${}", params.len()));
        }

        params.push(QueryParam::Int64(limit));
        return cached_rows(
            state,
            uri,
            request_id,
            DEFAULT_FRESH_TTL_SECONDS,
            format!(
                r#"WITH item_rows AS (
                   SELECT
                     casual.item_id,
                     casual.slot,
                     casual.item_level,
                     SUM(casual.count)::BIGINT AS uses,
                     SUM(casual.wins)::BIGINT AS wins,
                     SUM(casual.losses)::BIGINT AS losses
                   FROM item_counts_casual casual
                   WHERE {}
                   GROUP BY casual.item_id, casual.slot, casual.item_level
                 ),
                 player_count AS (
                   SELECT COALESCE(SUM(ledger.eligible_players), 0)::BIGINT AS total
                   FROM item_counts_casual_matches ledger
                   WHERE {}
                 ),
                 item_totals AS (
                   SELECT
                     item_id,
                     SUM(uses)::BIGINT AS total_uses,
                     SUM(wins)::BIGINT AS wins,
                     SUM(losses)::BIGINT AS losses
                   FROM item_rows
                   GROUP BY item_id
                 ),
                 slot_rows AS (
                   SELECT
                     item_id,
                     slot,
                     SUM(uses)::BIGINT AS total_uses,
                     COALESCE(ROUND(
                       100.0 * SUM(wins)::NUMERIC
                       / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0),
                       2
                     ), 0) AS win_rate
                   FROM item_rows
                   GROUP BY item_id, slot
                 ),
                 level_rows AS (
                   SELECT
                     item_id,
                     item_level,
                     SUM(uses)::BIGINT AS total_uses,
                     COALESCE(ROUND(
                       100.0 * SUM(wins)::NUMERIC
                       / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0),
                       2
                     ), 0) AS win_rate
                   FROM item_rows
                   GROUP BY item_id, item_level
                 ),
                 breakdown_rows AS (
                   SELECT
                     item_id,
                     slot,
                     item_level,
                     uses AS total_uses,
                     COALESCE(ROUND(
                       100.0 * wins::NUMERIC
                       / NULLIF((wins + losses)::NUMERIC, 0),
                       2
                     ), 0) AS win_rate,
                     COALESCE(ROUND(
                       100.0 * uses::NUMERIC
                       / NULLIF((SELECT total FROM player_count)::NUMERIC, 0),
                       2
                     ), 0) AS pick_rate
                   FROM item_rows
                 )
                 SELECT
                   totals.item_id,
                   COALESCE(item.item_name, 'Item ' || totals.item_id::TEXT) AS item_name,
                   totals.total_uses,
                   COALESCE(ROUND(
                     100.0 * totals.wins::NUMERIC
                     / NULLIF((totals.wins + totals.losses)::NUMERIC, 0),
                     2
                   ), 0) AS win_rate,
                   COALESCE(ROUND(
                     100.0 * totals.total_uses::NUMERIC
                     / NULLIF((SELECT total FROM player_count)::NUMERIC, 0),
                     2
                   ), 0) AS pick_rate,
                   COALESCE((
                     SELECT jsonb_agg(jsonb_build_object(
                       'slot', slot,
                       'total_uses', total_uses,
                       'win_rate', win_rate
                     ) ORDER BY slot)
                     FROM slot_rows slot_row
                     WHERE slot_row.item_id = totals.item_id
                   ), '[]'::JSONB) AS slots,
                   COALESCE((
                     SELECT jsonb_agg(jsonb_build_object(
                       'item_level', item_level,
                       'total_uses', total_uses,
                       'win_rate', win_rate
                     ) ORDER BY item_level)
                     FROM level_rows level_row
                     WHERE level_row.item_id = totals.item_id
                   ), '[]'::JSONB) AS levels,
                   COALESCE((
                     SELECT jsonb_agg(jsonb_build_object(
                       'slot', slot,
                       'item_level', item_level,
                       'total_uses', total_uses,
                       'win_rate', win_rate,
                       'pick_rate', pick_rate
                     ) ORDER BY slot, item_level)
                     FROM breakdown_rows breakdown
                     WHERE breakdown.item_id = totals.item_id
                   ), '[]'::JSONB) AS breakdown
                 FROM item_totals totals
                 LEFT JOIN items item ON item.item_id = totals.item_id
                 ORDER BY totals.total_uses DESC, item_name ASC
                 LIMIT ${}"#,
                item_where.join(" AND "),
                population_where.join(" AND "),
                params.len(),
            ),
            params,
        )
        .await;
    }

    let bounds = valid_tier_bounds(&query)?;
    let champion_id = if query.contains_key("championId") {
        match query
            .get("championId")
            .and_then(|value| parse_js_integer(value))
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| *value > 0)
        {
            Some(value) => Some(value),
            None => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": "Invalid champion id" })),
                )
                    .into_response());
            }
        }
    } else {
        None
    };
    let role_filter = match query.get("role") {
        Some(value) if !value.is_empty() => Some(normalize_item_role(value).ok_or_else(|| {
            ApiError::validation("Invalid role. Use Frontline, Damage, Flank, or Support.")
        })?),
        _ => None,
    };

    let mut params = vec![QueryParam::Int32(486)];
    let mut champion_where = vec!["1=1".to_owned()];
    if let Some(champion_id) = champion_id {
        params.push(QueryParam::Int32(champion_id));
        champion_where.push(format!("c.id = ${}", params.len()));
    }
    if let Some(role_filter) = role_filter {
        params.push(QueryParam::Text(role_filter.to_owned()));
        champion_where.push(format!("{CHAMPION_ROLE_SQL} = ${}", params.len()));
    }
    let mut item_where = vec!["sia.queue_id = $1".to_owned()];
    let mut player_where = vec!["spa.queue_id = $1".to_owned()];
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        item_where.push(format!("sia.lobby_tier >= ${}", params.len()));
        player_where.push(format!("spa.lobby_tier >= ${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        item_where.push(format!("sia.lobby_tier <= ${}", params.len()));
        player_where.push(format!("spa.lobby_tier <= ${}", params.len()));
    }
    params.push(QueryParam::Int64(limit));

    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        format!(
            r#"WITH eligible_champions AS (
                 SELECT c.id FROM champions c WHERE {}
               ), item_rows AS (
                 SELECT sia.item_id, sia.slot, sia.item_level,
                   SUM(sia.uses)::BIGINT AS uses, SUM(sia.wins)::BIGINT AS wins, SUM(sia.losses)::BIGINT AS losses
                 FROM stats_item_aggregate sia
                 JOIN eligible_champions ec ON ec.id = sia.champion_id
                 WHERE {}
                 GROUP BY sia.item_id, sia.slot, sia.item_level
               ), player_count AS (
                 SELECT COALESCE(SUM(spa.plays), 0)::BIGINT AS total
                 FROM stats_player_aggregate spa
                 JOIN eligible_champions ec ON ec.id = spa.champion_id
                 WHERE {}
               ), item_totals AS (
                 SELECT item_id, SUM(uses)::BIGINT AS total_uses, SUM(wins)::BIGINT AS wins, SUM(losses)::BIGINT AS losses
                 FROM item_rows GROUP BY item_id
               ), slot_rows AS (
                 SELECT item_id, slot, SUM(uses)::BIGINT AS total_uses,
                   COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins)+SUM(losses))::NUMERIC,0),2),0) AS win_rate
                 FROM item_rows GROUP BY item_id,slot
               ), level_rows AS (
                 SELECT item_id,item_level,SUM(uses)::BIGINT AS total_uses,
                   COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins)+SUM(losses))::NUMERIC,0),2),0) AS win_rate
                 FROM item_rows GROUP BY item_id,item_level
               ), breakdown_rows AS (
                 SELECT item_id,slot,item_level,uses AS total_uses,
                   COALESCE(ROUND(100.0*wins::NUMERIC/NULLIF((wins+losses)::NUMERIC,0),2),0) AS win_rate,
                   COALESCE(ROUND(100.0*uses::NUMERIC/NULLIF((SELECT total FROM player_count)::NUMERIC,0),2),0) AS pick_rate
                 FROM item_rows
               )
               SELECT totals.item_id, COALESCE(i.item_name,'Item '||totals.item_id::TEXT) AS item_name,
                 totals.total_uses,
                 COALESCE(ROUND(100.0*totals.wins::NUMERIC/NULLIF((totals.wins+totals.losses)::NUMERIC,0),2),0) AS win_rate,
                 COALESCE(ROUND(100.0*totals.total_uses::NUMERIC/NULLIF((SELECT total FROM player_count)::NUMERIC,0),2),0) AS pick_rate,
                 COALESCE((SELECT jsonb_agg(jsonb_build_object('slot',slot,'total_uses',total_uses,'win_rate',win_rate) ORDER BY slot)
                   FROM slot_rows sr WHERE sr.item_id=totals.item_id),'[]'::JSONB) AS slots,
                 COALESCE((SELECT jsonb_agg(jsonb_build_object('item_level',item_level,'total_uses',total_uses,'win_rate',win_rate) ORDER BY item_level)
                   FROM level_rows lr WHERE lr.item_id=totals.item_id),'[]'::JSONB) AS levels,
                 COALESCE((SELECT jsonb_agg(jsonb_build_object('slot',slot,'item_level',item_level,'total_uses',total_uses,'win_rate',win_rate,'pick_rate',pick_rate) ORDER BY slot,item_level)
                   FROM breakdown_rows br WHERE br.item_id=totals.item_id),'[]'::JSONB) AS breakdown
               FROM item_totals totals LEFT JOIN items i ON i.item_id=totals.item_id
               ORDER BY totals.total_uses DESC,item_name ASC LIMIT ${}"#,
            champion_where.join(" AND "),
            item_where.join(" AND "),
            player_where.join(" AND "),
            params.len(),
        ),
        params,
    )
    .await
}

async fn queues(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = valid_tier_bounds(&query)?;
    let mut params = Vec::new();
    let mut clauses = vec!["m.queue_id = 486".to_owned()];
    append_tier_predicates(bounds, &mut params, &mut clauses, "mlt");
    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        format!(
            "SELECT queue_id, COUNT(*) as total_matches, \
               ROUND(AVG(duration_seconds)::NUMERIC, 2) as avg_duration, \
               ROUND(AVG(CASE \
                 WHEN lower(COALESCE(mp.win_status, '')) IN ('winner', 'win') THEN 1 \
                 WHEN lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss') THEN 0 \
                 ELSE NULL \
               END)::NUMERIC, 4) as win_rate \
             FROM matches m \
             JOIN match_lobby_tiers mlt \
               ON mlt.match_id = m.match_id \
              AND mlt.entry_datetime = m.entry_datetime \
             JOIN match_players mp \
               ON mp.match_id = m.match_id \
              AND mp.entry_datetime = m.entry_datetime \
             WHERE {} \
             GROUP BY queue_id",
            clauses.join(" AND ")
        ),
        params,
    )
    .await
}

async fn regions(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![QueryParam::Int32(486)];
    let mut clauses = vec!["sma.queue_id = $1".to_owned()];
    append_tier_predicates(bounds, &mut params, &mut clauses, "sma");
    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        format!(
            "SELECT sma.region, \
               SUM(sma.match_count)::BIGINT AS total_matches, \
               ROUND( \
                 SUM(sma.duration_sum)::NUMERIC \
                 / NULLIF(SUM(sma.match_count), 0), \
                 2 \
               ) AS avg_duration \
             FROM stats_match_aggregate sma \
             WHERE {} \
             GROUP BY sma.region \
             ORDER BY sma.region",
            clauses.join(" AND ")
        ),
        params,
    )
    .await
}

async fn platforms(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![QueryParam::Int32(486)];
    let mut clauses = vec!["spa.queue_id = $1".to_owned()];
    append_tier_predicates(bounds, &mut params, &mut clauses, "spa");
    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        format!(
            "SELECT \
               spa.platform, \
               spa.champion_id, \
               COALESCE(c.name, 'Champion ' || spa.champion_id::TEXT) \
                 AS champion_name, \
               SUM(spa.plays)::BIGINT AS total_matches, \
               ROUND( \
                 100.0 * SUM(spa.wins)::NUMERIC \
                 / NULLIF((SUM(spa.wins) + SUM(spa.losses))::NUMERIC, 0), \
                 2 \
               )::DOUBLE PRECISION AS win_rate, \
               ROUND( \
                 SUM(spa.dpm_sum)::NUMERIC \
                 / NULLIF(SUM(spa.metric_samples), 0), \
                 2 \
               )::DOUBLE PRECISION AS avg_dpm, \
               ROUND( \
                 SUM(spa.hpm_sum)::NUMERIC \
                 / NULLIF(SUM(spa.metric_samples), 0), \
                 2 \
               )::DOUBLE PRECISION AS avg_hpm \
             FROM stats_player_aggregate spa \
             LEFT JOIN champions c ON c.id = spa.champion_id \
             WHERE {} \
             GROUP BY spa.platform, spa.champion_id, c.name \
             ORDER BY platform ASC, total_matches DESC, win_rate DESC \
             LIMIT 250",
            clauses.join(" AND ")
        ),
        params,
    )
    .await
}

async fn loadouts(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);
    let offset = legacy_default_integer(query.get("offset").map(String::as_str), 0).max(0);
    let minimum_plays = legacy_default_integer(query.get("minPlays").map(String::as_str), 1).max(1);
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![
        int32_query_param(minimum_plays),
        QueryParam::Int64(limit),
        QueryParam::Int64(offset),
    ];
    let mut clauses = vec!["1=1".to_owned()];
    if query
        .get("championId")
        .is_some_and(|value| !value.is_empty())
    {
        let champion_id = query
            .get("championId")
            .and_then(|value| parse_js_integer(value))
            .and_then(|value| i32::try_from(value).ok())
            .filter(|value| *value > 0)
            .ok_or_else(|| ApiError::validation("Invalid championId."))?;
        params.push(QueryParam::Int32(champion_id));
        clauses.push(format!("pl.champion_id = ${}", params.len()));
    }
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int32(i32::from(minimum)));
        clauses.push(format!("p.kbm_tier >= ${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int32(i32::from(maximum)));
        clauses.push(format!("p.kbm_tier <= ${}", params.len()));
    }
    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        format!(
            "WITH loadout_rows AS ( \
               SELECT \
                 md5( \
                   COALESCE(array_to_string(pl.card_ids, ','), '') || ':' || \
                   COALESCE(array_to_string(pl.card_levels, ','), '') \
                 ) AS deck_hash, \
                 pl.champion_id, \
                 COALESCE(c.name, 'Champion ' || pl.champion_id::TEXT) \
                   AS champion_name, \
                 COUNT(*)::INT AS total_uses, \
                 MAX(pl.updated_at) AS last_refreshed \
               FROM player_loadouts pl \
               JOIN players p ON p.id = pl.player_id \
               LEFT JOIN champions c ON c.id = pl.champion_id \
               WHERE {} \
               GROUP BY deck_hash, pl.champion_id, \
                 COALESCE(c.name, 'Champion ' || pl.champion_id::TEXT) \
             ) \
             SELECT \
               deck_hash, champion_id, champion_name, \
               total_uses AS total_matches, total_uses, \
               0::INT AS wins, 0::INT AS losses, \
               0::DOUBLE PRECISION AS win_rate, \
               0::INT AS ranked_wins, \
               0::DOUBLE PRECISION AS ranked_win_rate, \
               0::INT AS high_tier_wins, \
               0::DOUBLE PRECISION AS high_tier_win_rate, \
               0::DOUBLE PRECISION AS avg_kills, \
               0::DOUBLE PRECISION AS avg_deaths, \
               0::DOUBLE PRECISION AS avg_assists, \
               0::DOUBLE PRECISION AS avg_dpm, \
               0::DOUBLE PRECISION AS avg_hpm, \
               NULL::JSONB AS loadout_items, \
               last_refreshed \
             FROM loadout_rows \
             WHERE total_uses >= $1 \
             ORDER BY total_uses DESC, champion_name ASC \
             LIMIT $2 OFFSET $3",
            clauses.join(" AND ")
        ),
        params,
    )
    .await
}

async fn tiers(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if query.get("source").is_some_and(|value| value == "matches") {
        return cached_rows(
            state,
            uri,
            request_id,
            DEFAULT_FRESH_TTL_SECONDS,
            "WITH source_row AS ( \
               SELECT * \
               FROM tier_stats \
               WHERE source = $1 \
               UNION ALL \
               SELECT \
                 $1::VARCHAR(10), \
                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                 now() \
               WHERE NOT EXISTS (SELECT 1 FROM tier_stats WHERE source = $1) \
               LIMIT 1 \
             ), \
             tiers AS ( \
               SELECT * \
               FROM source_row, \
               LATERAL (VALUES \
                 (0, tier_0), (1, tier_1), (2, tier_2), (3, tier_3), \
                 (4, tier_4), (5, tier_5), (6, tier_6), (7, tier_7), \
                 (8, tier_8), (9, tier_9), (10, tier_10), (11, tier_11), \
                 (12, tier_12), (13, tier_13), (14, tier_14), \
                 (15, tier_15), (16, tier_16), (17, tier_17), \
                 (18, tier_18), (19, tier_19), (20, tier_20), \
                 (21, tier_21), (22, tier_22), (23, tier_23), \
                 (24, tier_24), (25, tier_25), (26, tier_26) \
               ) AS unpivoted(tier_sort, total_plays) \
             ) \
             SELECT \
               COALESCE(rt.tier_name, 'Tier ' || tiers.tier_sort::TEXT) AS tier, \
               tiers.tier_sort::INT AS tier_sort, \
               tiers.total_plays::INT AS total_plays, \
               ROUND( \
                 100.0 * tiers.total_plays::NUMERIC \
                 / NULLIF(SUM(tiers.total_plays) OVER (), 0), \
                 2 \
               )::DOUBLE PRECISION AS percentage, \
               NULL::DOUBLE PRECISION AS avg_win_rate \
             FROM tiers \
             LEFT JOIN ranked_tiers rt ON rt.tier_id = tiers.tier_sort \
             WHERE tiers.tier_sort BETWEEN 1 AND 26 \
             ORDER BY tiers.tier_sort"
                .to_owned(),
            vec![QueryParam::Text("matches".to_owned())],
        )
        .await;
    }
    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        "WITH effective_player_tiers AS ( \
           SELECT \
             CASE \
               WHEN p.kbm_tier = 26 AND lc.rank BETWEEN 1 AND 100 THEN 27 \
               ELSE p.kbm_tier \
             END AS tier_sort, \
             COUNT(DISTINCT p.id)::INT AS total_plays \
           FROM players p \
           LEFT JOIN leaderboard_current lc \
             ON lc.player_id = p.id \
            AND lc.tier = 26 \
           WHERE p.kbm_tier BETWEEN 1 AND 26 \
           GROUP BY 1 \
         ), \
         tiers AS (SELECT generate_series(1, 27) AS tier_sort), \
         filled AS ( \
           SELECT \
             tiers.tier_sort, \
             COALESCE(effective_player_tiers.total_plays, 0)::INT AS total_plays \
           FROM tiers \
           LEFT JOIN effective_player_tiers \
             ON effective_player_tiers.tier_sort = tiers.tier_sort \
         ) \
         SELECT \
           CASE \
             WHEN filled.tier_sort = 27 THEN 'Grandmaster' \
             ELSE COALESCE(rt.tier_name, 'Tier ' || filled.tier_sort::TEXT) \
           END AS tier, \
           filled.tier_sort::INT AS tier_sort, \
           filled.total_plays::INT AS total_plays, \
           ROUND( \
             100.0 * filled.total_plays::NUMERIC \
             / NULLIF(SUM(filled.total_plays) OVER (), 0), \
             2 \
           )::DOUBLE PRECISION AS percentage, \
           NULL::DOUBLE PRECISION AS avg_win_rate \
         FROM filled \
         LEFT JOIN ranked_tiers rt ON rt.tier_id = filled.tier_sort \
         ORDER BY filled.tier_sort"
            .to_owned(),
        Vec::new(),
    )
    .await
}

async fn tiers_summary(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
) -> Result<Response, ApiError> {
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        DEFAULT_FRESH_TTL_SECONDS,
        DEFAULT_FRESH_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            async move {
                let row = database
                    .one_json_params(
                        "WITH ranked_player_tiers AS ( \
                           SELECT mp.match_id, mp.player_id, mp.league_tier \
                           FROM match_players mp \
                           JOIN matches m \
                             ON m.match_id = mp.match_id \
                            AND m.entry_datetime = mp.entry_datetime \
                           WHERE m.queue_id = 486 \
                             AND COALESCE(m.limited, false) = false \
                             AND mp.league_tier BETWEEN 1 AND 26 \
                         ), \
                         per_match AS ( \
                           SELECT \
                             match_id, \
                             AVG(league_tier)::NUMERIC AS avg_tier, \
                             COUNT(*)::INT AS player_rows \
                           FROM ranked_player_tiers \
                           GROUP BY match_id \
                         ), \
                         profile_tiers AS ( \
                           SELECT \
                             CASE \
                               WHEN p.kbm_tier = 26 \
                                AND lc.rank BETWEEN 1 AND 100 THEN 27 \
                               ELSE p.kbm_tier \
                             END AS effective_tier \
                           FROM players p \
                           LEFT JOIN leaderboard_current lc \
                             ON lc.player_id = p.id \
                            AND lc.tier = 26 \
                           WHERE p.kbm_tier BETWEEN 1 AND 26 \
                         ) \
                         SELECT \
                           (SELECT COUNT(*)::INT FROM profile_tiers) \
                             AS profile_players, \
                           (SELECT ROUND(AVG(effective_tier)::NUMERIC, 2) \
                              ::DOUBLE PRECISION FROM profile_tiers) \
                             AS avg_profile_tier, \
                           (SELECT COUNT(*)::INT FROM ranked_player_tiers) \
                             AS match_player_rows, \
                           (SELECT COUNT(DISTINCT player_id)::INT \
                              FROM ranked_player_tiers) AS active_players, \
                           (SELECT COUNT(*)::INT FROM per_match) AS ranked_matches, \
                           (SELECT ROUND(AVG(league_tier)::NUMERIC, 2) \
                              ::DOUBLE PRECISION FROM ranked_player_tiers) \
                             AS avg_participation_tier, \
                           (SELECT ROUND(AVG(avg_tier)::NUMERIC, 2) \
                              ::DOUBLE PRECISION FROM per_match) AS avg_match_tier, \
                           (SELECT ROUND((PERCENTILE_CONT(0.50) \
                              WITHIN GROUP (ORDER BY avg_tier))::NUMERIC, 2) \
                              ::DOUBLE PRECISION FROM per_match) \
                             AS median_match_tier",
                        &[],
                    )
                    .await?;
                Ok(row.unwrap_or_else(|| {
                    json!({
                        "profile_players": 0,
                        "avg_profile_tier": null,
                        "match_player_rows": 0,
                        "active_players": 0,
                        "ranked_matches": 0,
                        "avg_participation_tier": null,
                        "avg_match_tier": null,
                        "median_match_tier": null
                    })
                }))
            }
        },
    )
    .await
}

async fn leagues(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
) -> Result<Response, ApiError> {
    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        "SELECT mp.league_tier, COUNT(*) as total_plays, \
                COUNT(DISTINCT mp.player_id) as unique_players, \
                ROUND(100.0 * COUNT(*) FILTER (WHERE \
                  lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::NUMERIC \
                  / NULLIF(COUNT(*) FILTER (WHERE \
                    (lower(COALESCE(mp.win_status, '')) IN ('winner', 'win') \
                     OR lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')))::NUMERIC, 0), 2) \
                  as win_rate \
         FROM match_players mp \
         JOIN matches m ON m.match_id = mp.match_id \
                       AND m.entry_datetime = mp.entry_datetime \
         WHERE m.queue_id = 486 \
           AND COALESCE(m.limited, false) = false \
           AND mp.league_tier > 0 \
         GROUP BY mp.league_tier \
         ORDER BY mp.league_tier"
            .to_owned(),
        Vec::new(),
    )
    .await
}

async fn ranked_leaderboard(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let tier = query
        .get("tier")
        .and_then(|value| parse_js_integer(value))
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| (21..=26).contains(value))
        .ok_or_else(|| {
            ApiError::validation("Invalid tier. Must be 21-26 (Diamond 5 to Master).")
        })?;
    let top = legacy_limit(query.get("top").map(String::as_str), 50, 200);
    cached_rows(
        state,
        uri,
        request_id,
        DEFAULT_FRESH_TTL_SECONDS,
        "SELECT *, \
                CASE WHEN prev_rank IS NULL THEN 0 ELSE prev_rank - rank END AS trend \
         FROM leaderboard_current \
         WHERE tier = $1 \
         ORDER BY points DESC \
         LIMIT $2"
            .to_owned(),
        vec![QueryParam::Int32(tier), QueryParam::Int64(top)],
    )
    .await
}

async fn leaderboard_log(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let page = legacy_default_integer(query.get("page").map(String::as_str), 1);
    let per_page = legacy_default_integer(query.get("perPage").map(String::as_str), 20).min(100);
    let offset = page.saturating_sub(1).saturating_mul(per_page);
    let rows = state
        .database
        .query_json_params(
            "SELECT * FROM leaderboard_update_log \
             ORDER BY updated_at DESC \
             LIMIT $1 OFFSET $2",
            &[QueryParam::Int64(per_page), QueryParam::Int64(offset)],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(Value::Array(rows)))
}

async fn tier_population(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
) -> Result<Response, ApiError> {
    cached_rows(
        state,
        uri,
        request_id,
        TIER_POPULATION_FRESH_TTL_SECONDS,
        "SELECT tier, tier_name, player_count, \
                ROUND(100.0 * player_count / NULLIF(SUM(player_count) OVER(), 0), 2) \
                  as percentage \
         FROM tier_population_stats \
         ORDER BY tier"
            .to_owned(),
        Vec::new(),
    )
    .await
}

async fn champion_leaderboard(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let champion_id = query
        .get("championId")
        .and_then(|value| parse_js_integer(value))
        .and_then(|value| i32::try_from(value).ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Invalid championId."))?;
    let limit = legacy_limit(query.get("limit").map(String::as_str), 25, 100);
    let database = state.database.clone();
    let cache = state.cache.clone();
    let key = stats_cache_key(&uri);
    cached_database_json(
        cache,
        key,
        DEFAULT_FRESH_TTL_SECONDS,
        DEFAULT_FRESH_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            async move {
                let rows = database
                    .query_json(
                        "SELECT \
                           pcr.player_id AS \"playerId\", \
                           COALESCE( \
                             CASE \
                               WHEN NULLIF(p.hz_player_name, '') IS NOT NULL \
                                 AND p.hz_player_name !~* \
                                   '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$' \
                               THEN p.hz_player_name \
                             END, \
                             CASE \
                               WHEN NULLIF(p.hz_gamer_tag, '') IS NOT NULL \
                                 AND p.hz_gamer_tag !~* \
                                   '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$' \
                               THEN p.hz_gamer_tag \
                             END, \
                             CASE \
                               WHEN NULLIF(p.name, '') IS NOT NULL \
                                 AND p.name !~* \
                                   '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$' \
                               THEN p.name \
                             END, \
                             'Player ' || p.id::text \
                           ) AS \"playerName\", \
                           pcr.mu, \
                           pcr.phi, \
                           pcr.matches_played AS \"matchesPlayed\", \
                           pcr.wins, \
                           pcr.losses \
                         FROM player_champion_ratings pcr \
                         JOIN players p ON p.id = pcr.player_id \
                         WHERE pcr.champion_id = $1 \
                           AND NOT p.cheater \
                         ORDER BY pcr.mu DESC \
                         LIMIT $2",
                        &[&champion_id, &limit],
                    )
                    .await?;
                Ok(Value::Array(
                    rows.into_iter()
                        .enumerate()
                        .map(|(index, row)| {
                            let mut object = row.as_object().cloned().unwrap_or_default();
                            object.insert("rank".to_owned(), json!(index + 1));
                            // JavaScript object spread retains SELECT key order only
                            // textually; JSON object comparison is key-order agnostic.
                            Value::Object(object)
                        })
                        .collect(),
                ))
            }
        },
    )
    .await
}

pub(super) async fn cached_rows(
    state: StatsState,
    uri: axum::http::Uri,
    request_id: RequestId,
    fresh_ttl_seconds: u64,
    sql: String,
    params: Vec<QueryParam>,
) -> Result<Response, ApiError> {
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        fresh_ttl_seconds,
        fresh_ttl_seconds * 3,
        &request_id,
        move || {
            let database = database.clone();
            let sql = sql.clone();
            let params = params.clone();
            async move {
                database
                    .query_json_params(&sql, &params)
                    .await
                    .map(Value::Array)
            }
        },
    )
    .await
}

pub(super) fn stats_cache_key(uri: &axum::http::Uri) -> String {
    format!("route:stats:v5:{}", canonical_route_cache_url(uri))
}

pub(super) fn valid_tier_bounds(query: &HashMap<String, String>) -> Result<TierBounds, ApiError> {
    parse_tier_bounds(query)
        .ok_or_else(|| ApiError::validation("Tier bounds must be between 1 and 26."))
}

pub(super) fn append_tier_predicates(
    bounds: TierBounds,
    params: &mut Vec<QueryParam>,
    clauses: &mut Vec<String>,
    alias: &str,
) {
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        clauses.push(format!("{alias}.lobby_tier >= ${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        clauses.push(format!("{alias}.lobby_tier <= ${}", params.len()));
    }
}

pub(super) fn legacy_default_integer(raw: Option<&str>, default: i64) -> i64 {
    match raw.and_then(parse_js_integer) {
        None | Some(0) => default,
        Some(value) => value,
    }
}

pub(super) fn legacy_limit(raw: Option<&str>, default: i64, maximum: i64) -> i64 {
    legacy_default_integer(raw, default).min(maximum)
}

fn parse_javascript_number_integer(raw: Option<&String>) -> Option<i64> {
    let raw = raw?.trim();
    let parsed = if let Some(hex) = raw.strip_prefix("0x").or_else(|| raw.strip_prefix("0X")) {
        i64::from_str_radix(hex, 16).ok()? as f64
    } else if let Some(binary) = raw.strip_prefix("0b").or_else(|| raw.strip_prefix("0B")) {
        i64::from_str_radix(binary, 2).ok()? as f64
    } else if let Some(octal) = raw.strip_prefix("0o").or_else(|| raw.strip_prefix("0O")) {
        i64::from_str_radix(octal, 8).ok()? as f64
    } else {
        raw.parse::<f64>().ok()?
    };
    if !parsed.is_finite() || parsed.fract() != 0.0 {
        return None;
    }
    Some(parsed as i64)
}

pub(super) fn normalize_item_role(raw: &str) -> Option<&'static str> {
    let key = raw
        .chars()
        .filter(|character| !character.is_whitespace() && *character != '_' && *character != '-')
        .flat_map(char::to_lowercase)
        .collect::<String>();
    match key.as_str() {
        "front" | "frontline" => Some("Frontline"),
        "damage" => Some("Damage"),
        "flank" => Some("Flank"),
        "support" => Some("Support"),
        _ => None,
    }
}

fn int32_query_param(value: i64) -> QueryParam {
    i32::try_from(value)
        .map(QueryParam::Int32)
        .unwrap_or_else(|_| QueryParam::Text(value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stats_legacy_limits_preserve_prefix_zero_and_negative_behavior() {
        assert_eq!(legacy_limit(None, 50, 200), 50);
        assert_eq!(legacy_limit(Some("0"), 50, 200), 50);
        assert_eq!(legacy_limit(Some("500tail"), 50, 200), 200);
        assert_eq!(legacy_limit(Some("-1"), 50, 200), -1);
    }

    #[test]
    fn tier_predicates_append_in_parameter_order() {
        let mut params = vec![QueryParam::Int32(486)];
        let mut clauses = vec!["spa.queue_id = $1".to_owned()];
        append_tier_predicates(
            TierBounds {
                minimum: Some(5),
                maximum: Some(20),
            },
            &mut params,
            &mut clauses,
            "spa",
        );
        assert_eq!(
            clauses,
            [
                "spa.queue_id = $1",
                "spa.lobby_tier >= $2",
                "spa.lobby_tier <= $3",
            ]
        );
        assert_eq!(params.len(), 3);
    }

    #[test]
    fn item_query_parsers_match_typescript_number_and_role_rules() {
        assert_eq!(
            parse_javascript_number_integer(Some(&" 424 ".to_owned())),
            Some(424)
        );
        assert_eq!(
            parse_javascript_number_integer(Some(&"4.24e2".to_owned())),
            Some(424)
        );
        assert_eq!(
            parse_javascript_number_integer(Some(&"0x1a8".to_owned())),
            Some(424)
        );
        assert_eq!(
            parse_javascript_number_integer(Some(&"424tail".to_owned())),
            None
        );
        assert_eq!(normalize_item_role("front-line"), Some("Frontline"));
        assert_eq!(normalize_item_role("DAMAGE"), Some("Damage"));
        assert_eq!(normalize_item_role("tank"), None);
    }
}
