use std::collections::HashMap;

use axum::{
    Router,
    extract::{Extension, Path, Query, State},
    response::Response,
    routing::get,
};
use paladinscat_core::database::QueryParam;
use serde_json::{Value, json};

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::cached_database_json,
};

use super::{
    StatsState, append_tier_predicates, cached_rows, legacy_limit, stats_cache_key,
    valid_tier_bounds,
};

const CACHE_TTL_SECONDS: u64 = 300;
const PUBLIC_SCOPES: &[&str] = &[
    "ranked",
    "casual",
    "bot",
    "team_deathmatch",
    "arcade",
    "wave_defense",
    "experiment",
    "newcomer",
];

pub(super) fn router() -> Router<StatsState> {
    Router::new()
        .route("/stats/maps", get(maps))
        .route("/stats/champions/{champion_id}/maps", get(champion_maps))
        .route("/stats/maps/{map_name}/comparison", get(map_comparison))
        .route("/stats/maps/{map_name}", get(map_detail))
}

async fn maps(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = legacy_limit(query.get("limit").map(String::as_str), 25, 100);
    let include_unknown = query
        .get("includeUnknown")
        .is_some_and(|value| value.eq_ignore_ascii_case("true"));
    let scope = query
        .get("scope")
        .map_or("ranked".to_owned(), |value| value.trim().to_lowercase());
    if !PUBLIC_SCOPES.contains(&scope.as_str()) {
        return Err(ApiError::validation("Invalid statistics scope."));
    }
    if scope != "ranked" {
        let mut params = vec![QueryParam::Text(scope)];
        let mut clauses = vec!["stats_scope = $1".to_owned()];
        if !include_unknown {
            clauses.push("map <> 'Unknown'".to_owned());
        }
        if let Some(queue_id) = query.get("queueId") {
            let queue_id = queue_id
                .parse::<i32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| ApiError::validation("queueId must be a positive integer."))?;
            params.push(QueryParam::Int32(queue_id));
            clauses.push(format!("queue_id = ${}", params.len()));
        }
        params.push(QueryParam::Int64(limit));
        return cached_rows(
            state,
            uri,
            request_id,
            CACHE_TTL_SECONDS,
            format!(
                "WITH map_counts AS ( \
                   SELECT map, SUM(matches)::BIGINT AS total_matches, \
                     COALESCE(ROUND(SUM(duration_sum)::NUMERIC \
                       / NULLIF(SUM(matches), 0), 2), 0)::DOUBLE PRECISION \
                       AS avg_duration_seconds \
                   FROM nonranked_map_stats_daily WHERE {} GROUP BY map \
                 ) \
                 SELECT map, total_matches, \
                   COALESCE(ROUND(100.0 * total_matches::NUMERIC \
                     / NULLIF(SUM(total_matches) OVER (), 0), 2), 0)::DOUBLE PRECISION \
                     AS distribution_rate, avg_duration_seconds \
                 FROM map_counts ORDER BY total_matches DESC, map ASC LIMIT ${}",
                clauses.join(" AND "),
                params.len()
            ),
            params,
        )
        .await;
    }
    if query
        .get("queueId")
        .is_some_and(|value| value.parse::<i32>().ok() != Some(486))
    {
        return Err(ApiError::validation(
            "Only ranked queue 486 is available for aggregate statistics.",
        ));
    }
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![QueryParam::Int32(486)];
    let mut clauses = vec!["sma.queue_id = $1".to_owned()];
    if !include_unknown {
        clauses.push("sma.map_name <> 'Unknown'".to_owned());
    }
    append_tier_predicates(bounds, &mut params, &mut clauses, "sma");
    params.push(QueryParam::Int64(limit));
    cached_rows(
        state,
        uri,
        request_id,
        CACHE_TTL_SECONDS,
        format!(
            "WITH map_counts AS ( \
               SELECT sma.map_name AS map, \
                 SUM(sma.match_count)::BIGINT AS total_matches, \
                 COALESCE(ROUND(SUM(sma.duration_sum)::NUMERIC \
                   / NULLIF(SUM(sma.match_count), 0), 2), 0)::DOUBLE PRECISION \
                   AS avg_duration_seconds \
               FROM stats_match_aggregate sma WHERE {} GROUP BY sma.map_name \
             ) \
             SELECT map, total_matches, \
               COALESCE(ROUND(100.0 * total_matches::NUMERIC \
                 / NULLIF(SUM(total_matches) OVER (), 0), 2), 0)::DOUBLE PRECISION \
                 AS distribution_rate, avg_duration_seconds \
             FROM map_counts ORDER BY total_matches DESC, map ASC LIMIT ${}",
            clauses.join(" AND "),
            params.len()
        ),
        params,
    )
    .await
}

async fn champion_maps(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(champion_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let champion_id = champion_id
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Champion id must be a positive integer."))?;
    let scope = query
        .get("scope")
        .map_or("ranked".to_owned(), |value| value.trim().to_lowercase());
    if !PUBLIC_SCOPES.contains(&scope.as_str()) {
        return Err(ApiError::validation("Invalid statistics scope."));
    }
    if scope != "ranked" {
        let mut params = vec![QueryParam::Int32(champion_id), QueryParam::Text(scope)];
        let mut clauses = vec![
            "champion_id = $1".to_owned(),
            "stats_scope = $2".to_owned(),
            "map <> 'Unknown'".to_owned(),
        ];
        if let Some(queue_id) = query.get("queueId") {
            let queue_id = queue_id
                .parse::<i32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| ApiError::validation("queueId must be a positive integer."))?;
            params.push(QueryParam::Int32(queue_id));
            clauses.push(format!("queue_id = ${}", params.len()));
        }
        return cached_rows(
            state,
            uri,
            request_id,
            CACHE_TTL_SECONDS,
            format!(
                "WITH champion_map_counts AS ( \
                   SELECT map, SUM(plays)::BIGINT AS total_plays, \
                     SUM(wins)::BIGINT AS wins, SUM(losses)::BIGINT AS losses \
                   FROM nonranked_champion_stats_daily WHERE {} GROUP BY map \
                 ) \
                 SELECT map, total_plays, wins, losses, \
                   COALESCE(ROUND(100.0 * wins::NUMERIC \
                     / NULLIF((wins + losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION \
                     AS win_rate, \
                   COALESCE(ROUND(100.0 * total_plays::NUMERIC \
                     / NULLIF(SUM(total_plays) OVER (), 0), 2), 0)::DOUBLE PRECISION \
                     AS pick_rate \
                 FROM champion_map_counts ORDER BY total_plays DESC, map ASC",
                clauses.join(" AND ")
            ),
            params,
        )
        .await;
    }
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![QueryParam::Int32(champion_id), QueryParam::Int32(486)];
    let mut clauses = vec![
        "spa.champion_id = $1".to_owned(),
        "spa.queue_id = $2".to_owned(),
        "spa.map_name <> 'Unknown'".to_owned(),
    ];
    append_tier_predicates(bounds, &mut params, &mut clauses, "spa");
    cached_rows(
        state,
        uri,
        request_id,
        CACHE_TTL_SECONDS,
        format!(
            "WITH champion_map_counts AS ( \
               SELECT spa.map_name AS map, SUM(spa.plays)::BIGINT AS total_plays, \
                 SUM(spa.wins)::BIGINT AS wins, SUM(spa.losses)::BIGINT AS losses \
               FROM stats_player_aggregate spa WHERE {} GROUP BY spa.map_name \
             ) \
             SELECT map, total_plays, wins, losses, \
               COALESCE(ROUND(100.0 * wins::NUMERIC \
                 / NULLIF((wins + losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION \
                 AS win_rate, \
               COALESCE(ROUND(100.0 * total_plays::NUMERIC \
                 / NULLIF(SUM(total_plays) OVER (), 0), 2), 0)::DOUBLE PRECISION \
                 AS pick_rate \
             FROM champion_map_counts ORDER BY total_plays DESC, map ASC",
            clauses.join(" AND ")
        ),
        params,
    )
    .await
}

async fn map_detail(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(map_name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let map_name = map_name.trim().to_owned();
    if map_name.is_empty() {
        return Err(ApiError::validation("Map name is required."));
    }
    let scope = query
        .get("scope")
        .map_or("ranked".to_owned(), |value| value.trim().to_lowercase());
    if !PUBLIC_SCOPES.contains(&scope.as_str()) {
        return Err(ApiError::validation("Invalid statistics scope."));
    }
    let bounds = valid_tier_bounds(&query)?;
    if scope != "ranked" && bounds.active() {
        return Err(ApiError::validation(
            "Lobby-tier filters apply only to ranked statistics.",
        ));
    }
    let exists = if scope == "ranked" {
        state
            .database
            .one_json_params(
                "SELECT 1 AS present FROM stats_match_aggregate \
                 WHERE queue_id=486 AND map_name=$1 LIMIT 1",
                &[QueryParam::Text(map_name.clone())],
            )
            .await
    } else {
        state
            .database
            .one_json_params(
                "SELECT 1 AS present FROM nonranked_map_stats_daily \
                 WHERE stats_scope=$1 AND map=$2 LIMIT 1",
                &[
                    QueryParam::Text(scope.clone()),
                    QueryParam::Text(map_name.clone()),
                ],
            )
            .await
    }
    .map_err(|error| ApiError::database(error, &request_id))?;
    if exists.is_none() {
        return Err(ApiError::not_found_without_details(
            "Map statistics not found.",
        ));
    }
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            let map_name = map_name.clone();
            let scope = scope.clone();
            async move {
                if scope != "ranked" {
                    let params = [
                        QueryParam::Text(map_name.clone()),
                        QueryParam::Text(scope.clone()),
                    ];
                    let map = database
                        .one_json_params(
                            "WITH map_counts AS ( \
                               SELECT map, SUM(matches)::BIGINT AS total_matches, \
                                 SUM(duration_sum)::BIGINT AS duration_sum \
                               FROM nonranked_map_stats_daily \
                               WHERE stats_scope = $2 AND map <> 'Unknown' GROUP BY map \
                             ), totals AS ( \
                               SELECT SUM(total_matches)::BIGINT AS total_matches FROM map_counts \
                             ) \
                             SELECT mc.map, mc.total_matches, \
                               COALESCE(ROUND(100.0 * mc.total_matches::NUMERIC \
                                 / NULLIF(t.total_matches, 0), 2), 0) AS distribution_rate, \
                               COALESCE(ROUND(mc.duration_sum::NUMERIC \
                                 / NULLIF(mc.total_matches, 0), 2), 0) AS avg_duration_seconds \
                             FROM map_counts mc CROSS JOIN totals t WHERE mc.map = $1",
                            &params,
                        )
                        .await?;
                    let Some(map) = map else {
                        return Ok(json!({"error": {"code": "NOT_FOUND", "message": "Map statistics not found."}}));
                    };
                    let champions = database
                        .query_json_params(
                            "WITH rows AS ( \
                               SELECT champion_id, SUM(plays)::BIGINT AS total_plays, \
                                 SUM(wins)::BIGINT AS wins, SUM(losses)::BIGINT AS losses \
                               FROM nonranked_champion_stats_daily \
                               WHERE map = $1 AND stats_scope = $2 GROUP BY champion_id \
                             ), totals AS (SELECT SUM(total_plays)::BIGINT AS plays FROM rows) \
                             SELECT r.champion_id, \
                               COALESCE(c.name, 'Champion ' || r.champion_id::TEXT) AS champion_name, \
                               r.total_plays, r.wins, r.losses, NULL::BIGINT AS total_bans, \
                               COALESCE(ROUND(100.0 * r.wins::NUMERIC \
                                 / NULLIF((r.wins + r.losses)::NUMERIC, 0), 2), 0) AS win_rate, \
                               COALESCE(ROUND(100.0 * r.total_plays::NUMERIC \
                                 / NULLIF(t.plays, 0), 2), 0) AS pick_rate, \
                               NULL::NUMERIC AS ban_rate \
                             FROM rows r LEFT JOIN champions c ON c.id = r.champion_id \
                             CROSS JOIN totals t ORDER BY r.total_plays DESC, champion_name",
                            &params,
                        )
                        .await?;
                    return Ok(json!({
                        "map": map,
                        "champions": champions,
                        "talents": [],
                        "items": [],
                        "compositions": [],
                        "stats_scope": scope
                    }));
                }

                let mut params = vec![
                    QueryParam::Text(map_name.clone()),
                    QueryParam::Int32(486),
                ];
                let mut match_where = vec![
                    "sma.queue_id = $2".to_owned(),
                    "sma.map_name = $1".to_owned(),
                ];
                append_tier_predicates(bounds, &mut params, &mut match_where, "sma");
                let tier_parameters = params.clone();
                let tier_predicate = |alias: &str| {
                    let mut clauses = Vec::new();
                    let mut index = 2;
                    if bounds.minimum.is_some() {
                        index += 1;
                        clauses.push(format!("{alias}.lobby_tier >= ${index}"));
                    }
                    if bounds.maximum.is_some() {
                        index += 1;
                        clauses.push(format!("{alias}.lobby_tier <= ${index}"));
                    }
                    if clauses.is_empty() {
                        String::new()
                    } else {
                        format!(" AND {}", clauses.join(" AND "))
                    }
                };
                let map = database
                    .one_json_params(
                        &format!(
                            "WITH map_counts AS ( \
                               SELECT map_name, SUM(match_count)::BIGINT AS total_matches, \
                                 SUM(duration_sum)::BIGINT AS duration_sum \
                               FROM stats_match_aggregate sma \
                               WHERE sma.queue_id = $2 AND sma.map_name <> 'Unknown'{} \
                               GROUP BY map_name \
                             ), all_maps AS (SELECT SUM(total_matches)::BIGINT AS total_matches FROM map_counts) \
                             SELECT mc.map_name AS map, mc.total_matches, \
                               COALESCE(ROUND(100.0 * mc.total_matches::NUMERIC \
                                 / NULLIF(am.total_matches, 0), 2), 0) AS distribution_rate, \
                               COALESCE(ROUND(mc.duration_sum::NUMERIC \
                                 / NULLIF(mc.total_matches, 0), 2), 0) AS avg_duration_seconds \
                             FROM map_counts mc CROSS JOIN all_maps am WHERE mc.map_name = $1",
                            tier_predicate("sma")
                        ),
                        &tier_parameters,
                    )
                    .await?;
                let Some(map) = map else {
                    return Ok(json!({"error": {"code": "NOT_FOUND", "message": "Map statistics not found."}}));
                };
                let champions = database
                    .query_json_params(
                        &format!(
                            "WITH player_rows AS ( \
                               SELECT spa.champion_id, SUM(spa.plays)::BIGINT AS total_plays, \
                                 SUM(spa.wins)::BIGINT AS wins, SUM(spa.losses)::BIGINT AS losses \
                               FROM stats_player_aggregate spa \
                               WHERE spa.queue_id = $2 AND spa.map_name = $1{} GROUP BY spa.champion_id \
                             ), totals AS (SELECT SUM(total_plays)::BIGINT AS plays FROM player_rows), \
                             bans AS ( \
                               SELECT champion_id, SUM(bans)::BIGINT AS total_bans \
                               FROM stats_ban_aggregate sba \
                               WHERE sba.queue_id = $2 AND sba.map_name = $1{} GROUP BY champion_id \
                             ), matches AS ( \
                               SELECT SUM(match_count)::BIGINT AS count FROM stats_match_aggregate sma \
                               WHERE sma.queue_id = $2 AND sma.map_name = $1{} \
                             ) \
                             SELECT pr.champion_id, c.name AS champion_name, pr.total_plays, \
                               pr.wins, pr.losses, COALESCE(b.total_bans, 0)::BIGINT AS total_bans, \
                               COALESCE(ROUND(100.0 * pr.wins::NUMERIC \
                                 / NULLIF((pr.wins + pr.losses)::NUMERIC, 0), 2), 0) AS win_rate, \
                               COALESCE(ROUND(100.0 * pr.total_plays::NUMERIC \
                                 / NULLIF(t.plays, 0), 2), 0) AS pick_rate, \
                               COALESCE(ROUND(100.0 * COALESCE(b.total_bans, 0)::NUMERIC \
                                 / NULLIF(m.count, 0), 2), 0) AS ban_rate \
                             FROM player_rows pr JOIN champions c ON c.id = pr.champion_id \
                             CROSS JOIN totals t CROSS JOIN matches m \
                             LEFT JOIN bans b ON b.champion_id = pr.champion_id \
                             ORDER BY pr.total_plays DESC, c.name",
                            tier_predicate("spa"),
                            tier_predicate("sba"),
                            tier_predicate("sma")
                        ),
                        &tier_parameters,
                    )
                    .await?;
                let talents = database
                    .query_json_params(
                        &format!(
                            "WITH rows AS ( \
                               SELECT sta.talent_id, sta.champion_id, \
                                 SUM(sta.uses)::BIGINT AS total_plays, \
                                 SUM(sta.wins)::BIGINT AS wins, SUM(sta.losses)::BIGINT AS losses \
                               FROM stats_talent_aggregate sta \
                               WHERE sta.queue_id = $2 AND sta.map_name = $1{} GROUP BY 1, 2 \
                             ), champion_totals AS ( \
                               SELECT champion_id, SUM(plays)::BIGINT AS plays \
                               FROM stats_player_aggregate spa \
                               WHERE spa.queue_id = $2 AND spa.map_name = $1{} GROUP BY champion_id \
                             ) \
                             SELECT r.talent_id, t.talent_name, r.champion_id, \
                               c.name AS champion_name, r.total_plays, r.wins, r.losses, \
                               COALESCE(ROUND(100.0 * r.wins::NUMERIC \
                                 / NULLIF((r.wins + r.losses)::NUMERIC, 0), 2), 0) AS win_rate, \
                               COALESCE(ROUND(100.0 * r.total_plays::NUMERIC \
                                 / NULLIF(ct.plays, 0), 2), 0) AS pick_rate \
                             FROM rows r JOIN talents t ON t.talent_id = r.talent_id \
                               AND t.champion_id = r.champion_id \
                             JOIN champions c ON c.id = r.champion_id \
                             JOIN champion_totals ct ON ct.champion_id = r.champion_id \
                             ORDER BY r.total_plays DESC, t.talent_name",
                            tier_predicate("sta"),
                            tier_predicate("spa")
                        ),
                        &tier_parameters,
                    )
                    .await?;
                let items = database
                    .query_json_params(
                        &format!(
                            "WITH rows AS ( \
                               SELECT sia.item_id, SUM(sia.uses)::BIGINT AS total_uses, \
                                 SUM(sia.wins)::BIGINT AS wins, SUM(sia.losses)::BIGINT AS losses \
                               FROM stats_item_aggregate sia \
                               WHERE sia.queue_id = $2 AND sia.map_name = $1{} GROUP BY sia.item_id \
                             ), total AS ( \
                               SELECT SUM(plays)::BIGINT AS plays FROM stats_player_aggregate spa \
                               WHERE spa.queue_id = $2 AND spa.map_name = $1{} \
                             ) \
                             SELECT r.item_id, COALESCE(i.item_name, 'Item ' || r.item_id::TEXT) AS item_name, \
                               r.total_uses, r.wins, r.losses, \
                               COALESCE(ROUND(100.0 * r.wins::NUMERIC \
                                 / NULLIF((r.wins + r.losses)::NUMERIC, 0), 2), 0) AS win_rate, \
                               COALESCE(ROUND(100.0 * r.total_uses::NUMERIC \
                                 / NULLIF(t.plays, 0), 2), 0) AS pick_rate \
                             FROM rows r LEFT JOIN items i ON i.item_id = r.item_id \
                             CROSS JOIN total t ORDER BY r.total_uses DESC, item_name",
                            tier_predicate("sia"),
                            tier_predicate("spa")
                        ),
                        &tier_parameters,
                    )
                    .await?;
                let compositions = database
                    .query_json_params(
                        &format!(
                            "SELECT sca.comp_id, sca.frontline, sca.damage, sca.flank, \
                               sca.support, SUM(sca.uses)::BIGINT AS count, \
                               SUM(sca.wins)::BIGINT AS wins, SUM(sca.losses)::BIGINT AS losses, \
                               COALESCE(ROUND(100.0 * SUM(sca.wins)::NUMERIC \
                                 / NULLIF((SUM(sca.wins) + SUM(sca.losses))::NUMERIC, 0), 2), 0) AS winrate \
                             FROM stats_composition_aggregate sca \
                             WHERE sca.queue_id = $2 AND sca.map_name = $1{} \
                             GROUP BY sca.comp_id, sca.frontline, sca.damage, sca.flank, sca.support \
                             ORDER BY count DESC, sca.comp_id",
                            tier_predicate("sca")
                        ),
                        &tier_parameters,
                    )
                    .await?;
                Ok(json!({
                    "map": map,
                    "champions": champions,
                    "talents": talents,
                    "items": items,
                    "compositions": compositions
                }))
            }
        },
    )
    .await
}

async fn map_comparison(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(map_name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if map_name.trim().is_empty() {
        return Err(ApiError::validation("Map name is required."));
    }
    let section = query
        .get("section")
        .map_or(String::new(), |value| value.to_lowercase());
    if !["champions", "talents", "items", "compositions"].contains(&section.as_str()) {
        return Err(ApiError::validation(
            "section must be champions, talents, items, or compositions.",
        ));
    }
    let bounds = valid_tier_bounds(&query)?;
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<i64>().ok())
        .map(|value| value.clamp(1, 500));
    if query.get("cursor").is_some_and(|value| !value.is_empty()) {
        return Err(ApiError::validation("Invalid comparison cursor."));
    }

    let mut params = vec![QueryParam::Int32(486), QueryParam::Text(map_name)];
    let (source, alias, entity, joins, count_column) = match section.as_str() {
        "champions" => (
            "stats_player_aggregate",
            "spa",
            "spa.champion_id::TEXT",
            "",
            "spa.plays",
        ),
        "talents" => (
            "stats_talent_aggregate",
            "sta",
            "sta.talent_id::TEXT",
            "",
            "sta.uses",
        ),
        "items" => (
            "stats_item_aggregate",
            "sia",
            "sia.item_id::TEXT",
            "",
            "sia.uses",
        ),
        _ => (
            "stats_composition_aggregate",
            "sca",
            "sca.comp_id",
            "",
            "sca.uses",
        ),
    };
    let mut clauses = vec![
        format!("{alias}.queue_id = $1"),
        format!("{alias}.map_name <> $2"),
        format!("{alias}.map_name <> 'Unknown'"),
    ];
    append_tier_predicates(bounds, &mut params, &mut clauses, alias);
    if let Some(limit) = limit {
        params.push(QueryParam::Int64(limit + 1));
    }
    let limit_sql = limit
        .map(|_| format!(" LIMIT ${}", params.len()))
        .unwrap_or_default();
    let sql = format!(
        "SELECT {entity} AS entity_key, {alias}.map_name, \
           SUM({count_column})::BIGINT AS total_count, \
           SUM({alias}.wins)::BIGINT AS wins, SUM({alias}.losses)::BIGINT AS losses, \
           0::BIGINT AS total_bans, \
           COALESCE(ROUND(100.0 * SUM({alias}.wins)::NUMERIC \
             / NULLIF((SUM({alias}.wins) + SUM({alias}.losses))::NUMERIC, 0), 2), 0) AS win_rate, \
           0::NUMERIC AS pick_rate, 0::NUMERIC AS ban_rate \
         FROM {source} {alias} {joins} WHERE {} \
         GROUP BY {entity}, {alias}.map_name \
         ORDER BY {entity}, win_rate DESC, total_count DESC, {alias}.map_name{limit_sql}",
        clauses.join(" AND ")
    );
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            let params = params.clone();
            let section = section.clone();
            let sql = sql.clone();
            async move {
                let mut rows = database.query_json_params(&sql, &params).await?;
                let has_more = limit.is_some_and(|limit| rows.len() > limit as usize);
                if let Some(limit) = limit {
                    rows.truncate(limit as usize);
                }
                Ok(json!({
                    "section": section,
                    "rows": rows,
                    "next_cursor": if has_more { Value::String("pending".to_owned()) } else { Value::Null }
                }))
            }
        },
    )
    .await
}
