use std::{cmp::Ordering, collections::HashMap};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Extension, Path, Query, State},
    http::{HeaderValue, Request, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::get,
};
use paladinscat_core::database::{Database, QueryParam};
use serde_json::{Map, Value, json};
use tower::ServiceExt;

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::{RouteCache, cached_database_json, canonical_route_cache_url},
};

use super::{
    lobby_tier::{TierBounds, parse_tier_bounds},
    stats,
};

const CACHE_TTL_SECONDS: u64 = 300;

#[derive(Clone)]
struct ChampionsState {
    database: Database,
    cache: RouteCache,
}

pub fn router(database: Database, cache: RouteCache) -> Router {
    Router::new()
        .route("/champions", get(catalog))
        .route("/champions/", get(catalog))
        .route("/champions/overview", get(overview))
        .route("/champions/tiers", get(tiers))
        .route("/champions/top-winrate", get(top_winrate))
        .route("/champions/{id}", get(detail))
        .route("/champions/{id}/page-data", get(page_data))
        .route(
            "/champions/{id}/talents/{talent_id}/page-data",
            get(talent_page_data),
        )
        .route("/champions/{id}/patch-history", get(patch_history))
        .route("/champions/{id}/counters", get(counters))
        .with_state(ChampionsState { database, cache })
}

fn cache_key(uri: &axum::http::Uri) -> String {
    format!("route:champions:v3:{}", canonical_route_cache_url(uri))
}

fn public_bundle_cache(mut response: Response) -> Response {
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=300, stale-while-revalidate=900"),
    );
    response
}

fn bounds(query: &HashMap<String, String>) -> Result<TierBounds, ApiError> {
    parse_tier_bounds(query)
        .ok_or_else(|| ApiError::validation("Tier bounds must be between 1 and 26."))
}

fn append_tiers(
    bounds: TierBounds,
    params: &mut Vec<QueryParam>,
    clauses: &mut Vec<String>,
    alias: &str,
) {
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        clauses.push(format!("{alias}.lobby_tier>=${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        clauses.push(format!("{alias}.lobby_tier<=${}", params.len()));
    }
}

async fn resolve_champion_id(
    database: &Database,
    raw: &str,
) -> Result<Option<i32>, paladinscat_core::database::DatabaseError> {
    if let Some(id) = raw.parse::<i32>().ok().filter(|id| *id > 0) {
        return Ok(Some(id));
    }
    let row = database
        .query_json_params(
            "SELECT id FROM champions WHERE \
               REGEXP_REPLACE(LOWER(name),'[^a-z0-9]+','','g')= \
               REGEXP_REPLACE(LOWER($1),'[^a-z0-9]+','','g') LIMIT 1",
            &[QueryParam::Text(raw.trim().to_owned())],
        )
        .await?
        .into_iter()
        .next();
    Ok(row
        .and_then(|row| row.get("id").cloned())
        .and_then(|value| as_i64(Some(&value)))
        .and_then(|value| i32::try_from(value).ok()))
}

async fn catalog(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
) -> Result<Response, ApiError> {
    if uri.path().ends_with('/') {
        let rows = state
            .database
            .query_json(
                "SELECT id,name,title,health,speed,roles FROM champions WHERE id>0 ORDER BY name",
                &[],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
        return Ok(Json(rows).into_response());
    }
    let database = state.database.clone();
    let cache = state.cache.clone();
    cached_database_json(
        cache,
        cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            async move {
                database
                    .query_json(
                        "SELECT id,name,title,health,speed,roles FROM champions WHERE id>0 ORDER BY name",
                        &[],
                    )
                    .await
                    .map(Value::Array)
            }
        },
    )
    .await
}

async fn overview(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = bounds(&query)?;
    let scope = query
        .get("scope")
        .map_or("ranked".to_owned(), |value| value.trim().to_lowercase());
    let database = state.database.clone();
    let cache = state.cache.clone();
    cached_database_json(
        cache,
        cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let state = state.clone();
            let database = database.clone();
            let scope = scope.clone();
            async move {
                let champions = database
                    .query_json(
                        "SELECT id,name,title,health,speed,roles FROM champions WHERE id>0 ORDER BY name",
                        &[],
                    )
                    .await?;
                let mut path = format!(
                    "/stats/champions?limit=200&scope={}",
                    url::form_urlencoded::byte_serialize(scope.as_bytes()).collect::<String>()
                );
                if scope == "ranked" {
                    append_bound_query(&mut path, bounds);
                }
                let stats = internal_stats_get(&state, path)
                    .await
                    .unwrap_or_else(|| json!([]));
                Ok(json!({ "champions": champions, "stats": stats }))
            }
        },
    )
    .await
}

async fn champion_detail_value(
    database: &Database,
    id: i32,
    bounds: TierBounds,
) -> Result<Option<Value>, paladinscat_core::database::DatabaseError> {
    let champion = database
        .query_json_params(
            "SELECT * FROM champions WHERE id=$1",
            &[QueryParam::Int32(id)],
        )
        .await?
        .into_iter()
        .next();
    let Some(champion) = champion else {
        return Ok(None);
    };
    let stats = if bounds.active() {
        let mut params = vec![QueryParam::Int32(id)];
        let mut clauses = vec![
            "mp.champion_id=$1".to_owned(),
            "m.queue_id=486".to_owned(),
            "COALESCE(mp.source,'direct') IN ('direct','recovered')".to_owned(),
        ];
        append_tiers(bounds, &mut params, &mut clauses, "mlt");
        database
            .query_json_params(
                &format!(
                    "SELECT COUNT(*)::INT AS total_matches,COUNT(*)::INT AS total_plays, \
                       COUNT(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::INT AS wins, \
                       COUNT(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::INT AS losses, \
                       ROUND(100.0*COUNT(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::NUMERIC \
                         /NULLIF(COUNT(*) FILTER(WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win','loser','loss'))::NUMERIC,0),2) AS win_rate, \
                       ROUND(AVG(mp.kills)::NUMERIC,2) AS avg_kills,ROUND(AVG(mp.deaths)::NUMERIC,2) AS avg_deaths, \
                       ROUND(AVG(mp.assists)::NUMERIC,2) AS avg_assists,ROUND(AVG(mp.damage_done_physical)::NUMERIC,2) AS avg_damage, \
                       ROUND(AVG(mp.gold_earned)::NUMERIC,2) AS avg_gold, \
                       ROUND(AVG(mp.league_tier) FILTER(WHERE mp.league_tier BETWEEN 1 AND 26)::NUMERIC,2) AS avg_league_tier \
                     FROM match_players mp JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime \
                     JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime \
                     WHERE {}",
                    clauses.join(" AND ")
                ),
                &params,
            )
            .await?
            .into_iter()
            .next()
    } else {
        database
            .query_json_params(
                "SELECT total_matches,total_matches AS total_plays,wins,losses,win_rate, \
                   CASE WHEN total_matches>0 THEN ROUND(sum_kills::NUMERIC/total_matches,2) END AS avg_kills, \
                   CASE WHEN total_matches>0 THEN ROUND(sum_deaths::NUMERIC/total_matches,2) END AS avg_deaths, \
                   CASE WHEN total_matches>0 THEN ROUND(sum_assists::NUMERIC/total_matches,2) END AS avg_assists, \
                   CASE WHEN total_matches>0 THEN ROUND(sum_damage::NUMERIC/total_matches,2) END AS avg_damage, \
                   CASE WHEN total_matches>0 THEN ROUND(sum_gold::NUMERIC/total_matches,2) END AS avg_gold, \
                   CASE WHEN league_tier_count>0 THEN ROUND(sum_league_tier::NUMERIC/league_tier_count,2) END AS avg_league_tier \
                 FROM champion_stats_ranked WHERE champion_id=$1",
                &[QueryParam::Int32(id)],
            )
            .await?
            .into_iter()
            .next()
    };
    let tier_ratings = database
        .query_json_params(
            "SELECT tier,rating,deviation,matches_played FROM champion_tier_ratings \
             WHERE champion_id=$1 ORDER BY tier",
            &[QueryParam::Int32(id)],
        )
        .await?;
    Ok(Some(json!({
        "champion": champion,
        "stats": stats,
        "tierRatings": tier_ratings
    })))
}

async fn detail(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = bounds(&query)?;
    let id = resolve_champion_id(&state.database, &raw_id)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("Champion not found", json!({})))?;
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            async move {
                champion_detail_value(&database, id, bounds)
                    .await
                    .map(|value| value.unwrap_or_else(|| json!({ "error": "Champion not found" })))
            }
        },
    )
    .await
}

async fn internal_stats_get(state: &ChampionsState, path: String) -> Option<Value> {
    let uri = path.parse::<axum::http::Uri>().ok()?;
    let mut request = Request::builder()
        .uri(uri.clone())
        .body(Body::empty())
        .ok()?;
    request
        .extensions_mut()
        .insert(RequestId(format!("rust-internal-{}", uuid::Uuid::new_v4())));
    request.extensions_mut().insert(EffectiveUri(uri));
    let response = stats::router(state.database.clone(), state.cache.clone())
        .oneshot(request)
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let bytes = to_bytes(response.into_body(), 8 * 1024 * 1024).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn append_bound_query(path: &mut String, bounds: TierBounds) {
    if let Some(minimum) = bounds.minimum {
        path.push_str(&format!("&tierMin={minimum}"));
    }
    if let Some(maximum) = bounds.maximum {
        path.push_str(&format!("&tierMax={maximum}"));
    }
}

async fn page_data(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = bounds(&query)?;
    let id = resolve_champion_id(&state.database, &raw_id)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("Champion not found", json!({})))?;
    cached_database_json(
        state.cache.clone(),
        cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let state = state.clone();
            async move {
                let detail = champion_detail_value(&state.database, id, bounds)
                    .await?
                    .unwrap_or(Value::Null);
                let mut talents_path = format!("/stats/talents/{id}?mode=ranked");
                append_bound_query(&mut talents_path, bounds);
                let mut items_path =
                    format!("/stats/items?mode=ranked&championId={id}&limit=200");
                append_bound_query(&mut items_path, bounds);
                let mut maps_path = format!("/stats/champions/{id}/maps");
                append_bound_query(&mut maps_path, bounds);
                let mut performance_path =
                    "/stats/performance-metrics?queueId=486".to_owned();
                append_bound_query(&mut performance_path, bounds);
                let talent_stats = internal_stats_get(&state, talents_path).await;
                let items = internal_stats_get(&state, items_path)
                    .await
                    .unwrap_or_else(|| json!([]));
                let maps = internal_stats_get(&state, maps_path)
                    .await
                    .unwrap_or_else(|| json!([]));
                let performance = internal_stats_get(&state, performance_path)
                    .await
                    .unwrap_or_else(|| json!({}));
                let mut champion_performance = Map::new();
                for metric in ["dpm", "wpm", "apm", "gpm", "hpm", "mpm", "kda"] {
                    let mut path = format!(
                        "/stats/performance-metrics/by-champion?metric={metric}&championId={id}&queueId=486"
                    );
                    append_bound_query(&mut path, bounds);
                    if let Some(row) = internal_stats_get(&state, path)
                        .await
                        .and_then(|value| value.get("data").cloned())
                        .and_then(|value| value.as_array().and_then(|rows| rows.first()).cloned())
                    {
                        champion_performance
                            .insert(metric.to_owned(), normalize_champion_metric(row));
                    }
                }
                Ok(json!({
                    "champion": detail.get("champion").cloned().unwrap_or(Value::Null),
                    "stats": detail.get("stats").cloned().unwrap_or(Value::Null),
                    "talentStats": talent_stats.map(normalize_talent_stats),
                    "items": normalize_items(items),
                    "maps": normalize_maps(maps),
                    "performance": normalize_performance(performance),
                    "championPerformance": champion_performance
                }))
            }
        },
    )
    .await
    .map(public_bundle_cache)
}

async fn talent_page_data(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path((raw_id, raw_talent_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = bounds(&query)?;
    let champion_id = resolve_champion_id(&state.database, &raw_id)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::not_found("Champion not found", json!({})))?;
    let talent_id = raw_talent_id
        .parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation("Invalid talentId."))?;
    let exists = state
        .database
        .query_json_params(
            "SELECT talent_id FROM talents WHERE talent_id=$1 AND champion_id=$2",
            &[QueryParam::Int32(talent_id), QueryParam::Int32(champion_id)],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .into_iter()
        .next()
        .is_some();
    if !exists {
        return Err(ApiError::not_found(
            "Talent not found for champion",
            json!({}),
        ));
    }
    cached_database_json(
        state.cache.clone(),
        cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let state = state.clone();
            async move {
                let mut talents_path = format!("/stats/talents/{champion_id}?mode=ranked");
                append_bound_query(&mut talents_path, bounds);
                let mut cards_path =
                    format!("/stats/cards/{champion_id}?mode=ranked&talentId={talent_id}");
                append_bound_query(&mut cards_path, bounds);
                let talents = normalize_talent_stats(
                    internal_stats_get(&state, talents_path)
                        .await
                        .unwrap_or_else(|| json!({})),
                );
                let cards = normalize_card_stats(
                    internal_stats_get(&state, cards_path)
                        .await
                        .unwrap_or_else(|| json!({})),
                );
                let talent_stat = talents
                    .get("talents")
                    .and_then(Value::as_array)
                    .and_then(|rows| {
                        rows.iter()
                            .find(|row| as_i64(row.get("talentId")) == Some(i64::from(talent_id)))
                    })
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(json!({
                    "championId": champion_id,
                    "talentId": talent_id,
                    "totalMatches": number_value(talents.get("totalMatches")),
                    "talentStat": talent_stat,
                    "cardStats": cards
                }))
            }
        },
    )
    .await
    .map(public_bundle_cache)
}

async fn tiers(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
) -> Result<Response, ApiError> {
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            async move {
                database
                    .query_json(
                        "SELECT * FROM champion_tier_ratings ORDER BY rating DESC LIMIT 50",
                        &[],
                    )
                    .await
                    .map(Value::Array)
            }
        },
    )
    .await
}

async fn patch_history(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
) -> Result<Response, ApiError> {
    let id = raw_id.parse::<i32>().unwrap_or_default();
    let patches = state
        .database
        .query_json("SELECT * FROM patches ORDER BY release_date DESC", &[])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(json!({ "championId": id, "patches": patches })).into_response())
}

async fn counters(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Path(raw_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let id = raw_id.parse::<i32>().unwrap_or_default();
    let bounds = bounds(&query)?;
    let (sql, params) = if bounds.active() {
        let mut params = vec![QueryParam::Int32(id)];
        let mut clauses = vec![
            "mof.player_champion_id=$1".to_owned(),
            "m.queue_id=486".to_owned(),
        ];
        append_tiers(bounds, &mut params, &mut clauses, "mlt");
        (
            format!(
                "SELECT mof.opponent_champion_id,c.name AS opponent_champion_name, \
                   (SUM(mof.wins)+SUM(mof.losses))::INT AS total_matchups, \
                   (SUM(mof.wins)+SUM(mof.losses))::INT AS total_encounters, \
                   SUM(mof.wins)::INT AS wins,SUM(mof.losses)::INT AS losses, \
                   ROUND(100.0*SUM(mof.wins)::NUMERIC/NULLIF((SUM(mof.wins)+SUM(mof.losses))::NUMERIC,0),2) AS win_rate, \
                   NULL::NUMERIC AS avg_kills,NULL::NUMERIC AS avg_deaths,NULL::NUMERIC AS avg_dpm \
                 FROM match_opponent_facts mof JOIN matches m ON m.match_id=mof.match_id \
                 JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime \
                 JOIN champions c ON c.id=mof.opponent_champion_id WHERE {} \
                 GROUP BY mof.opponent_champion_id,c.name ORDER BY win_rate DESC",
                clauses.join(" AND ")
            ),
            params,
        )
    } else {
        (
            "SELECT opponent_champion_id,opponent_champion_name,total_matchups,total_encounters,wins,losses, \
               win_rate,avg_kills,avg_deaths,avg_dpm FROM counter_pick_stats \
             WHERE attacker_champion_id=$1 ORDER BY win_rate DESC"
                .to_owned(),
            vec![QueryParam::Int32(id)],
        )
    };
    let rows = state
        .database
        .query_json_params(&sql, &params)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(Json(rows).into_response())
}

async fn top_winrate(
    State(state): State<ChampionsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = bounds(&query)?;
    let mut path = "/stats/champions?limit=200".to_owned();
    append_bound_query(&mut path, bounds);
    let stats = internal_stats_get(&state, path)
        .await
        .and_then(|value| value.as_array().cloned())
        .unwrap_or_default();
    let catalog = state
        .database
        .query_json("SELECT id,name,roles FROM champions WHERE id>0", &[])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let by_id = stats
        .iter()
        .filter_map(|row| Some((as_i64(row.get("champion_id"))?, row)))
        .collect::<HashMap<_, _>>();
    let mut candidates = catalog
        .into_iter()
        .filter_map(|mut champion| {
            let id = as_i64(champion.get("id"))?;
            let stats = by_id.get(&id);
            let total = stats
                .and_then(|row| as_i64(row.get("total_matches")))
                .unwrap_or_default();
            if total < 50 {
                return None;
            }
            let object = champion.as_object_mut()?;
            object.insert(
                "winRate".to_owned(),
                json!(number_value(stats.and_then(|row| row.get("win_rate")))),
            );
            object.insert("totalPlays".to_owned(), json!(total));
            Some(champion)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        number_value(right.get("winRate"))
            .partial_cmp(&number_value(left.get("winRate")))
            .unwrap_or(Ordering::Equal)
    });
    let mut groups = Vec::<(String, Vec<Value>)>::new();
    for row in candidates {
        let role = normalize_role(row.get("roles").and_then(Value::as_str).unwrap_or(""));
        let index = groups
            .iter()
            .position(|(existing, _)| existing == &role)
            .unwrap_or_else(|| {
                groups.push((role.clone(), Vec::new()));
                groups.len() - 1
            });
        if groups[index].1.len() < 3 {
            groups[index].1.push(row);
        }
    }
    let result = groups
        .into_iter()
        .flat_map(|(_, rows)| rows)
        .collect::<Vec<_>>();
    cached_database_json(
        state.cache,
        cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let result = result.clone();
            async move { Ok(Value::Array(result)) }
        },
    )
    .await
}

fn normalize_role(raw: &str) -> String {
    let lower = raw.to_lowercase();
    let suffix = lower
        .strip_prefix("paladins ")
        .or_else(|| lower.strip_prefix("paladin "))
        .unwrap_or(&lower);
    suffix
        .strip_suffix("er")
        .unwrap_or(suffix)
        .trim()
        .to_owned()
}

fn as_i64(value: Option<&Value>) -> Option<i64> {
    value?.as_i64().or_else(|| {
        value?
            .as_f64()
            .filter(|number| number.fract() == 0.0)
            .map(|number| number as i64)
            .or_else(|| value?.as_str()?.parse().ok())
    })
}

fn normalize_champion_metric(row: Value) -> Value {
    json!({
        "championId": number_value(row.get("champion_id")),
        "championName": row.get("champion_name").and_then(Value::as_str).unwrap_or(""),
        "className": row.get("class").and_then(Value::as_str).unwrap_or(""),
        "min": number_value(row.get("min")),
        "max": number_value(row.get("max")),
        "mean": number_value(row.get("mean")),
        "median": number_value(row.get("median")),
        "mode": number_value(row.get("mode")),
        "p10": number_value(row.get("p10")),
        "p90": number_value(row.get("p90")),
        "avgValue": number_value(row.get("avg_value")),
        "totalMatches": number_value(row.get("total_matches"))
    })
}

fn number_value(value: Option<&Value>) -> f64 {
    value
        .and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_str().and_then(|raw| raw.parse().ok()))
        })
        .unwrap_or_default()
}

fn normalize_talent_stats(raw: Value) -> Value {
    json!({
        "totalMatches": number_value(raw.get("totalMatches")),
        "talentCoveredMatches": number_value(raw.get("talentCoveredMatches")),
        "disconnectedPlayers": number_value(raw.get("disconnectedPlayers")),
        "disconnectedWins": number_value(raw.get("disconnectedWins")),
        "disconnectedLosses": number_value(raw.get("disconnectedLosses")),
        "disconnectedWinRate": raw.get("disconnectedWinRate").cloned().unwrap_or(Value::Null),
        "talentCoverageRate": raw.get("talentCoverageRate").cloned().unwrap_or(Value::Null),
        "talents": raw.get("talents").and_then(Value::as_array).map(|rows| rows.iter().map(|row| json!({
            "talentId": number_value(row.get("talentId")),
            "talentName": row.get("talentName").and_then(Value::as_str).unwrap_or("Unknown"),
            "totalPlays": number_value(row.get("totalPlays")),
            "wins": number_value(row.get("wins")),
            "losses": number_value(row.get("losses")),
            "winRate": number_value(row.get("winRate"))
        })).collect::<Vec<_>>()).unwrap_or_default()
    })
}

fn normalize_card_stats(raw: Value) -> Value {
    json!({
        "totalMatches": number_value(raw.get("totalMatches")),
        "talentId": raw.get("talentId").map_or(Value::Null, |value| json!(number_value(Some(value)))),
        "cards": raw.get("cards").and_then(Value::as_array).map(|rows| rows.iter().map(|row| json!({
            "cardId": number_value(row.get("cardId")),
            "cardName": row.get("cardName").and_then(Value::as_str).unwrap_or("Unknown"),
            "totalPlays": number_value(row.get("totalPlays")),
            "wins": number_value(row.get("wins")),
            "losses": number_value(row.get("losses")),
            "winRate": number_value(row.get("winRate")),
            "levels": row.get("levels").and_then(Value::as_array).map(|levels| levels.iter().map(|level| json!({
                "level": number_value(level.get("level")),
                "plays": number_value(level.get("plays")),
                "wins": number_value(level.get("wins")),
                "losses": number_value(level.get("losses")),
                "winRate": number_value(level.get("winRate"))
            })).collect::<Vec<_>>()).unwrap_or_default()
        })).collect::<Vec<_>>()).unwrap_or_default()
    })
}

fn normalize_items(raw: Value) -> Value {
    Value::Array(
        raw.as_array()
            .map(|rows| rows.iter().map(normalize_item).collect())
            .unwrap_or_default(),
    )
}

fn normalize_item(row: &Value) -> Value {
    let slots = row
        .get("slots")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|slot| {
                    json!({
                        "slot": number_value(slot.get("slot")),
                        "totalUses": number_value(slot.get("total_uses")),
                        "wins": 0,
                        "losses": 0,
                        "winRate": number_value(slot.get("win_rate"))
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let levels = row
        .get("levels")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|level| {
                    json!({
                        "level": number_value(level.get("item_level")),
                        "totalUses": number_value(level.get("total_uses")),
                        "wins": 0,
                        "losses": 0,
                        "winRate": number_value(level.get("win_rate"))
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let breakdown = row
        .get("breakdown")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .map(|entry| {
                    let mut value = json!({
                        "slot": number_value(entry.get("slot")),
                        "level": number_value(entry.get("item_level")),
                        "totalUses": number_value(entry.get("total_uses")),
                        "wins": 0,
                        "losses": 0,
                        "winRate": number_value(entry.get("win_rate"))
                    });
                    if let Some(pick_rate) = entry.get("pick_rate").filter(|value| !value.is_null())
                    {
                        value
                            .as_object_mut()
                            .expect("item breakdown")
                            .insert("pickRate".to_owned(), json!(number_value(Some(pick_rate))));
                    }
                    value
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut value = json!({
        "itemId": number_value(row.get("item_id")),
        "itemName": row.get("item_name").and_then(Value::as_str).unwrap_or(""),
        "totalUsage": number_value(row.get("total_uses").or_else(|| row.get("total_usage"))),
        "winRate": number_value(row.get("win_rate")),
        "slots": slots,
        "levels": levels,
        "breakdown": breakdown
    });
    if let Some(pick_rate) = row.get("pick_rate").filter(|value| !value.is_null()) {
        value
            .as_object_mut()
            .expect("item")
            .insert("pickRate".to_owned(), json!(number_value(Some(pick_rate))));
    }
    value
}

fn normalize_maps(raw: Value) -> Value {
    Value::Array(
        raw.as_array()
            .map(|rows| {
                rows.iter()
                    .map(|row| {
                        json!({
                            "name": row.get("map").and_then(Value::as_str).unwrap_or(""),
                            "totalPlays": number_value(row.get("total_plays")),
                            "wins": number_value(row.get("wins")),
                            "losses": number_value(row.get("losses")),
                            "winRate": number_value(row.get("win_rate")),
                            "pickRate": number_value(row.get("pick_rate"))
                        })
                    })
                    .collect()
            })
            .unwrap_or_default(),
    )
}

fn normalize_performance(raw: Value) -> Value {
    let mut result = Map::new();
    if let Some(metrics) = raw.as_object() {
        for (key, row) in metrics {
            result.insert(
                key.clone(),
                json!({
                    "min": number_value(row.get("min")),
                    "max": number_value(row.get("max")),
                    "mean": number_value(row.get("mean")),
                    "median": number_value(row.get("median")),
                    "mode": number_value(row.get("mode")),
                    "p10": number_value(row.get("p10")),
                    "p25": number_value(row.get("p25")),
                    "p75": number_value(row.get("p75")),
                    "p90": number_value(row.get("p90")),
                    "sampleSize": number_value(row.get("sample_size").or_else(|| row.get("sampleSize")))
                }),
            );
        }
    }
    Value::Object(result)
}
