use std::collections::HashMap;

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Extension, Query, State},
    http::{HeaderValue, Request, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use paladinscat_core::database::{Database, QueryParam};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::cached_database_json,
};

use super::{
    StatsState, append_tier_predicates, cached_rows, legacy_limit, stats_cache_key,
    valid_tier_bounds,
};
use crate::routes::champions;

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
        .route("/stats/overview", get(overview))
        .route("/stats/page-data", get(page_data))
        .route("/stats/leaderboard", get(leaderboard))
        .route("/stats/trends", get(trends))
        .route("/stats/ecpm-candidates", get(ecpm_candidates))
        .route("/stats/charts", get(charts))
        .route("/stats/champions", get(champions))
}

async fn internal_stats_get(state: &StatsState, path: String) -> Option<Value> {
    let uri = path.parse::<axum::http::Uri>().ok()?;
    let mut request = Request::builder()
        .uri(uri.clone())
        .body(Body::empty())
        .ok()?;
    request
        .extensions_mut()
        .insert(RequestId(format!("rust-internal-{}", uuid::Uuid::new_v4())));
    request.extensions_mut().insert(EffectiveUri(uri));
    let response = super::router(state.database.clone(), state.cache.clone())
        .oneshot(request)
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = to_bytes(response.into_body(), 8 * 1024 * 1024).await.ok()?;
    serde_json::from_slice(&body).ok()
}

async fn internal_champions_get(state: &StatsState, path: String) -> Option<Value> {
    let uri = path.parse::<axum::http::Uri>().ok()?;
    let mut request = Request::builder()
        .uri(uri.clone())
        .body(Body::empty())
        .ok()?;
    request
        .extensions_mut()
        .insert(RequestId(format!("rust-internal-{}", uuid::Uuid::new_v4())));
    request.extensions_mut().insert(EffectiveUri(uri));
    let response = champions::router(state.database.clone(), state.cache.clone())
        .oneshot(request)
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    let body = to_bytes(response.into_body(), 8 * 1024 * 1024).await.ok()?;
    serde_json::from_slice(&body).ok()
}

fn public_bundle_cache(mut response: Response) -> Response {
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=300, stale-while-revalidate=900"),
    );
    response
}

fn private_no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    response
}

fn tier_query(query: &HashMap<String, String>) -> String {
    ["tierMin", "tierMax", "lobby"]
        .into_iter()
        .filter_map(|key| {
            query.get(key).map(|value| {
                format!(
                    "{key}={}",
                    url::form_urlencoded::byte_serialize(value.as_bytes()).collect::<String>()
                )
            })
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn scoped(path: &str, tier_query: &str) -> String {
    if tier_query.is_empty() {
        path.to_owned()
    } else {
        format!(
            "{path}{}{tier_query}",
            if path.contains('?') { '&' } else { '?' }
        )
    }
}

#[derive(Clone, Copy)]
struct EcpmBracket {
    name: &'static str,
    minimum: i32,
    maximum: i32,
    automatic: bool,
}

const ECPM_BRACKETS: &[EcpmBracket] = &[
    EcpmBracket {
        name: "possible-disconnect",
        minimum: 110,
        maximum: 120,
        automatic: false,
    },
    EcpmBracket {
        name: "disconnected",
        minimum: 90,
        maximum: 110,
        automatic: false,
    },
    EcpmBracket {
        name: "partial-afk",
        minimum: 70,
        maximum: 90,
        automatic: false,
    },
    EcpmBracket {
        name: "full-afk",
        minimum: 0,
        maximum: 70,
        automatic: true,
    },
];

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct EcpmCursor {
    at: String,
    match_id: String,
    player_id: String,
}

fn decode_ecpm_cursor(raw: &str) -> Option<EcpmCursor> {
    let bytes = URL_SAFE_NO_PAD.decode(raw).ok()?;
    let cursor: EcpmCursor = serde_json::from_slice(&bytes).ok()?;
    cursor.match_id.parse::<i64>().ok()?;
    cursor.player_id.parse::<i64>().ok()?;
    (!cursor.at.is_empty()).then_some(cursor)
}

fn encode_ecpm_cursor(row: &Value) -> Option<String> {
    let cursor = EcpmCursor {
        at: row.get("entry_datetime")?.as_str()?.to_owned(),
        match_id: row
            .get("match_id")?
            .as_str()
            .map(str::to_owned)
            .or_else(|| row.get("match_id")?.as_i64().map(|value| value.to_string()))?,
        player_id: row
            .get("player_id")?
            .as_str()
            .map(str::to_owned)
            .or_else(|| {
                row.get("player_id")?
                    .as_i64()
                    .map(|value| value.to_string())
            })?,
    };
    serde_json::to_vec(&cursor)
        .ok()
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
}

async fn overview(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    valid_tier_bounds(&query)?;
    let cache_key = stats_cache_key(&uri);
    let cache = state.cache.clone();
    cached_database_json(
        cache,
        cache_key,
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let state = state.clone();
            let query = query.clone();
            async move {
                let tier = tier_query(&query);
                let metrics =
                    internal_stats_get(&state, scoped("/stats/performance-metrics", &tier))
                        .await
                        .unwrap_or_else(|| json!({}));
                let champion_overview =
                    internal_champions_get(&state, scoped("/champions/overview", &tier))
                        .await
                        .unwrap_or_else(|| json!({ "champions": [], "stats": [] }));
                let items =
                    internal_stats_get(&state, scoped("/stats/items?mode=ranked&limit=50", &tier))
                        .await
                        .unwrap_or_else(|| json!([]));
                let maps =
                    internal_stats_get(&state, scoped("/stats/maps?queueId=486&limit=25", &tier))
                        .await
                        .unwrap_or_else(|| json!([]));
                let profile_tiers =
                    internal_stats_get(&state, "/stats/tiers?source=profiles".to_owned())
                        .await
                        .unwrap_or_else(|| json!([]));
                let active_tiers =
                    internal_stats_get(&state, "/stats/tiers?source=matches".to_owned())
                        .await
                        .unwrap_or_else(|| json!([]));
                Ok(json!({
                    "metrics": metrics,
                    "champions": champion_overview,
                    "items": items,
                    "maps": maps,
                    "profile_tiers": profile_tiers,
                    "active_tiers": active_tiers
                }))
            }
        },
    )
    .await
    .map(public_bundle_cache)
}

async fn page_data(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    valid_tier_bounds(&query)?;
    let cache = state.cache.clone();
    cached_database_json(
        cache,
        stats_cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let state = state.clone();
            let query = query.clone();
            async move {
                let tier = tier_query(&query);
                let overview = internal_stats_get(&state, scoped("/stats/overview", &tier))
                    .await
                    .unwrap_or_else(|| json!({
                        "metrics": {}, "champions": { "champions": [], "stats": [] },
                        "items": [], "maps": [], "profile_tiers": [], "active_tiers": []
                    }));
                let baselines = internal_stats_get(
                    &state,
                    scoped("/stats/baselines?queueId=486", &tier),
                )
                .await
                .unwrap_or_else(|| json!([]));
                let skins = internal_stats_get(
                    &state,
                    scoped("/stats/skins?limit=5", &tier),
                )
                .await
                .unwrap_or_else(|| json!([]));
                let broken = internal_stats_get(
                    &state,
                    scoped("/stats/broken-skins", &tier),
                )
                .await
                .unwrap_or_else(|| json!([]));
                let compositions = state
                    .database
                    .query_json(
                        "SELECT frontline_count,damage_count,flank_count,support_count, \
                           SUM(count)::BIGINT AS total_matches,SUM(wins)::BIGINT AS wins, \
                           COALESCE(ROUND(100.0*SUM(wins)::NUMERIC/NULLIF(SUM(count),0),2),0) AS win_rate \
                         FROM composition_counts_ranked GROUP BY 1,2,3,4 \
                         ORDER BY total_matches DESC LIMIT 10",
                        &[],
                    )
                    .await
                    .unwrap_or_default();
                Ok(json!({
                    "overview": overview,
                    "baselines": baselines,
                    "skins": skins.get("data").cloned().unwrap_or(skins),
                    "compositions": compositions,
                    "broken_skins": broken.get("data").cloned().unwrap_or(broken)
                }))
            }
        },
    )
    .await
    .map(public_bundle_cache)
}

async fn leaderboard(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = valid_tier_bounds(&query)?;
    let tier = query
        .get("tier")
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| (0..=26).contains(value));
    if let Some(tier) = tier {
        return cached_rows(
            state,
            uri,
            request_id,
            CACHE_TTL_SECONDS,
            format!("SELECT * FROM leaderboard{tier} ORDER BY points DESC LIMIT 100"),
            Vec::new(),
        )
        .await;
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
            let query = query.clone();
            async move {
                let rows = if bounds.active() {
                    champion_rows(&database, &query).await?
                } else {
                    database
                        .query_json(
                            "SELECT champion_id, champion_name, win_rate, \
                               total_matches AS total_plays \
                             FROM champion_stats_ranked \
                             WHERE total_matches >= 50 \
                             ORDER BY win_rate DESC LIMIT 100",
                            &[],
                        )
                        .await?
                };
                Ok(Value::Array(
                    rows.into_iter()
                        .take(100)
                        .enumerate()
                        .map(|(index, row)| {
                            let source = row.as_object().cloned().unwrap_or_default();
                            json!({
                                "rank": index + 1,
                                "championId": source.get("champion_id").cloned().unwrap_or(Value::Null),
                                "championName": source.get("champion_name").cloned().unwrap_or(Value::Null),
                                "winRate": json_number(source.get("win_rate")),
                                "totalPlays": source
                                    .get("total_matches")
                                    .or_else(|| source.get("total_plays"))
                                    .cloned()
                                    .unwrap_or(Value::Null)
                            })
                        })
                        .collect(),
                ))
            }
        },
    )
    .await
}

async fn trends(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = valid_tier_bounds(&query)?;
    if query
        .get("queueId")
        .is_some_and(|value| value.parse::<i32>().ok() != Some(486))
    {
        return Err(ApiError::validation(
            "Only ranked queue 486 is available for aggregate statistics.",
        ));
    }
    let mut params = vec![QueryParam::Int32(486)];
    let mut clauses = vec!["sma.queue_id = $1".to_owned()];
    if let Some(from) = query.get("from") {
        params.push(QueryParam::Text(from.to_owned()));
        clauses.push(format!("sma.stat_date >= ${}::DATE", params.len()));
    } else {
        let days = query
            .get("days")
            .and_then(|value| value.parse::<i32>().ok())
            .filter(|value| *value != 0)
            .unwrap_or(30);
        params.push(QueryParam::Int32(days));
        clauses.push(format!(
            "sma.stat_date >= CURRENT_DATE - ${}::INT",
            params.len()
        ));
    }
    if let Some(to) = query.get("to") {
        params.push(QueryParam::Text(to.to_owned()));
        clauses.push(format!("sma.stat_date <= ${}::DATE", params.len()));
    }
    if let Some(region) = query.get("region") {
        params.push(QueryParam::Text(region.to_owned()));
        clauses.push(format!("sma.region = ${}", params.len()));
    }
    append_tier_predicates(bounds, &mut params, &mut clauses, "sma");
    cached_rows(
        state,
        uri,
        request_id,
        CACHE_TTL_SECONDS,
        format!(
            "SELECT sma.stat_date, sma.queue_id, sma.region, \
               SUM(sma.match_count)::BIGINT AS match_count, \
               ROUND(SUM(sma.duration_sum)::NUMERIC \
                 / NULLIF(SUM(sma.match_count), 0), 2) AS avg_duration \
             FROM stats_match_aggregate sma WHERE {} \
             GROUP BY sma.stat_date, sma.queue_id, sma.region \
             ORDER BY sma.stat_date",
            clauses.join(" AND ")
        ),
        params,
    )
    .await
}

async fn ecpm_candidates(
    State(state): State<StatsState>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bracket_name = query
        .get("bracket")
        .map_or("possible-disconnect", String::as_str);
    let bracket = ECPM_BRACKETS
        .iter()
        .copied()
        .find(|candidate| candidate.name == bracket_name)
        .ok_or_else(|| {
            ApiError::validation(
                "Invalid bracket. Use possible-disconnect, disconnected, partial-afk, or full-afk.",
            )
        })?;
    let queue_id = query
        .get("queueId")
        .map_or(Ok(486), |value| value.parse::<i32>())
        .ok()
        .filter(|value| *value == 486)
        .ok_or_else(|| ApiError::validation("Invalid queueId."))?;
    let bounds = valid_tier_bounds(&query)?;
    let limit = query
        .get("limit")
        .map_or(Ok(20), |value| value.parse::<i32>())
        .ok()
        .filter(|value| *value > 0)
        .map(|value| value.min(50))
        .ok_or_else(|| ApiError::validation("Limit must be a positive integer."))?;
    let cursor = query
        .get("cursor")
        .filter(|value| !value.is_empty())
        .map(|value| {
            decode_ecpm_cursor(value)
                .ok_or_else(|| ApiError::validation("Invalid candidate cursor."))
        })
        .transpose()?;

    let mut params = vec![
        QueryParam::Int32(queue_id),
        QueryParam::Float64(f64::from(bracket.minimum)),
        QueryParam::Float64(f64::from(bracket.maximum)),
    ];
    let mut clauses = vec![
        "m.queue_id=$1".to_owned(),
        "mp.egpm >= $2".to_owned(),
        "mp.egpm < $3".to_owned(),
        "COALESCE(mis.status,'complete')='complete'".to_owned(),
        "COALESCE(mp.source,'direct') IN ('direct','recovered')".to_owned(),
        "mp.is_ranked=true".to_owned(),
        "mp.player_id>0".to_owned(),
        "mp.champion_id>0".to_owned(),
        "mp.task_force IN (1,2)".to_owned(),
        "lower(COALESCE(mp.win_status,'')) IN ('winner','win','loser','loss')".to_owned(),
        "m.duration_seconds>120".to_owned(),
    ];
    append_tier_predicates(bounds, &mut params, &mut clauses, "mlt");
    if let Some(cursor) = cursor {
        params.push(QueryParam::Text(cursor.at));
        let at = params.len();
        params.push(QueryParam::Int64(
            cursor
                .match_id
                .parse()
                .expect("validated eCPM match cursor"),
        ));
        let match_id = params.len();
        params.push(QueryParam::Int64(
            cursor
                .player_id
                .parse()
                .expect("validated eCPM player cursor"),
        ));
        let player_id = params.len();
        clauses.push(format!(
            "(mp.entry_datetime,mp.match_id,mp.player_id)<(${at}::TIMESTAMPTZ,${match_id}::BIGINT,${player_id}::BIGINT)"
        ));
    }
    params.push(QueryParam::Int64(i64::from(limit + 1)));
    let mut data = state
        .database
        .query_json_params(
            &format!(
                "SELECT mp.player_id::text AS player_id, \
                   COALESCE(NULLIF(p.name,''),NULLIF(mp.player_name,''),'Player '||mp.player_id::text) AS player_name, \
                   mp.match_id::text AS match_id,mp.entry_datetime AS entry_datetime, \
                   mp.champion_id,c.name AS champion_name, \
                   CASE WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline' \
                     WHEN c.roles ILIKE '%Damage%' THEN 'Damage' WHEN c.roles ILIKE '%Flank%' THEN 'Flank' \
                     WHEN c.roles ILIKE '%Support%' THEN 'Support' ELSE COALESCE(NULLIF(c.roles,''),'Unknown') END AS class_name, \
                   ROUND(mp.egpm::NUMERIC,2)::DOUBLE PRECISION AS egpm,mp.win_status,m.map,m.region,m.duration_seconds, \
                   COALESCE(m.recovered,false) AS recovered \
                 FROM match_players mp JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime \
                 LEFT JOIN match_ingest_status mis ON mis.match_id=m.match_id \
                 JOIN champions c ON c.id=mp.champion_id LEFT JOIN players p ON p.id=mp.player_id \
                 LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime \
                 WHERE {} ORDER BY mp.entry_datetime DESC,mp.match_id DESC,mp.player_id DESC LIMIT ${}",
                clauses.join(" AND "),
                params.len()
            ),
            &params,
        )
        .await
        .map_err(|error| ApiError::database(error, &RequestId("ecpm-candidates".to_owned())))?;
    let has_more = data.len() > limit as usize;
    data.truncate(limit as usize);
    let next_cursor = if has_more {
        data.last().and_then(encode_ecpm_cursor)
    } else {
        None
    };

    let mut count_params = vec![QueryParam::Int32(queue_id)];
    let (table, alias) = if bounds.active() {
        ("stats_metric_histogram", "hist")
    } else {
        ("performance_metric_histogram", "hist")
    };
    let mut count_clauses = vec![
        "hist.queue_id=$1".to_owned(),
        "hist.role_id=0".to_owned(),
        "hist.metric='egpm'".to_owned(),
    ];
    if bounds.active() {
        append_tier_predicates(bounds, &mut count_params, &mut count_clauses, alias);
    }
    let counts = state
        .database
        .query_json_params(
            &format!(
                "SELECT COALESCE(SUM(hist.sample_count),0)::BIGINT AS total, \
                   COALESCE(SUM(hist.sample_count) FILTER(WHERE hist.value>=110 AND hist.value<120),0)::BIGINT AS possible_disconnect, \
                   COALESCE(SUM(hist.sample_count) FILTER(WHERE hist.value>=90 AND hist.value<110),0)::BIGINT AS disconnected, \
                   COALESCE(SUM(hist.sample_count) FILTER(WHERE hist.value>=70 AND hist.value<90),0)::BIGINT AS partial_afk, \
                   COALESCE(SUM(hist.sample_count) FILTER(WHERE hist.value>=0 AND hist.value<70),0)::BIGINT AS full_afk \
                 FROM {table} hist WHERE {}",
                count_clauses.join(" AND ")
            ),
            &count_params,
        )
        .await
        .map_err(|error| ApiError::database(error, &RequestId("ecpm-counts".to_owned())))?
        .into_iter()
        .next()
        .unwrap_or_else(|| json!({}));
    let total = integer_value(counts.get("total"));
    let count_value = |key: &str| {
        let count = integer_value(counts.get(key));
        json!({
            "count": count,
            "percentage": if total > 0 {
                ((count as f64 / total as f64) * 10_000.0).round() / 100.0
            } else { 0.0 }
        })
    };
    Ok(private_no_store(
        Json(json!({
            "data": data,
            "next_cursor": next_cursor,
            "bracket": bracket.name,
            "range": { "minimum": bracket.minimum, "maximumExclusive": bracket.maximum },
            "automatic_flag": bracket.automatic,
            "sample_size": total,
            "bracket_counts": {
                "possible-disconnect": count_value("possible_disconnect"),
                "disconnected": count_value("disconnected"),
                "partial-afk": count_value("partial_afk"),
                "full-afk": count_value("full_afk")
            }
        }))
        .into_response(),
    ))
}

fn integer_value(value: Option<&Value>) -> i64 {
    value
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}

async fn charts(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let limit = legacy_limit(query.get("limit").map(String::as_str), 90, 365);
    let bounds = valid_tier_bounds(&query)?;
    if bounds.active() {
        let mut params = Vec::new();
        let mut clauses = vec!["m.queue_id = 486".to_owned()];
        if let Some(from) = query.get("from") {
            params.push(QueryParam::Text(from.to_owned()));
            clauses.push(format!(
                "m.entry_datetime >= ${}::TIMESTAMPTZ",
                params.len()
            ));
        }
        if let Some(to) = query.get("to") {
            params.push(QueryParam::Text(to.to_owned()));
            clauses.push(format!(
                "m.entry_datetime <= ${}::TIMESTAMPTZ",
                params.len()
            ));
        }
        append_tier_predicates(bounds, &mut params, &mut clauses, "mlt");
        params.push(QueryParam::Int64(limit));
        return cached_rows(
            state,
            uri,
            request_id,
            CACHE_TTL_SECONDS,
            format!(
                "SELECT m.entry_datetime::DATE AS entry_date, \
                   ROUND(AVG(mp.kills)::NUMERIC, 2) AS avg_kills, \
                   ROUND(AVG(mp.deaths)::NUMERIC, 2) AS avg_deaths, \
                   ROUND(AVG(mp.assists)::NUMERIC, 2) AS avg_assists, \
                   ROUND(AVG(mp.damage_per_minute)::NUMERIC, 2) AS avg_dpm, \
                   ROUND(AVG(mp.healing_per_minute)::NUMERIC, 2) AS avg_hpm, \
                   COUNT(DISTINCT m.match_id)::INT AS total_matches \
                 FROM matches m \
                 JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id \
                   AND mlt.entry_datetime = m.entry_datetime \
                 JOIN match_players mp ON mp.match_id = m.match_id \
                   AND mp.entry_datetime = m.entry_datetime \
                 WHERE {} GROUP BY m.entry_datetime::DATE \
                 ORDER BY entry_date DESC LIMIT ${}",
                clauses.join(" AND "),
                params.len()
            ),
            params,
        )
        .await;
    }

    let mut params = Vec::new();
    let mut clauses = Vec::new();
    if let Some(from) = query.get("from") {
        params.push(QueryParam::Text(from.to_owned()));
        clauses.push(format!("entry_date >= ${}::DATE", params.len()));
    }
    if let Some(to) = query.get("to") {
        params.push(QueryParam::Text(to.to_owned()));
        clauses.push(format!("entry_date <= ${}::DATE", params.len()));
    }
    params.push(QueryParam::Int64(limit));
    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };
    cached_rows(
        state,
        uri,
        request_id,
        CACHE_TTL_SECONDS,
        format!(
            "SELECT entry_date, avg_kills, avg_deaths, avg_assists, \
               avg_dpm, avg_hpm, total_matches \
             FROM global_match_stats{where_clause} \
             ORDER BY entry_date DESC LIMIT ${}",
            params.len()
        ),
        params,
    )
    .await
}

async fn champions(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
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
    let rows = champion_rows(&state.database, &query)
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let rows = rows.clone();
            let _database = database.clone();
            async move { Ok(Value::Array(rows)) }
        },
    )
    .await
}

pub(super) async fn champion_rows(
    database: &Database,
    query: &HashMap<String, String>,
) -> Result<Vec<Value>, paladinscat_core::database::DatabaseError> {
    let sort = match query.get("sort").map(String::as_str) {
        Some("avg_kills") => "avg_kills",
        Some("ban_rate") => "ban_rate",
        Some("kda") => "kda",
        _ => "win_rate",
    };
    let order = if query.get("order").is_some_and(|value| value == "asc") {
        "ASC"
    } else {
        "DESC"
    };
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);
    let scope = query
        .get("scope")
        .map_or("ranked".to_owned(), |value| value.trim().to_lowercase());
    if scope != "ranked" {
        let mut params = vec![QueryParam::Text(scope)];
        let queue_clause = if let Some(queue_id) = query
            .get("queueId")
            .and_then(|value| value.parse::<i32>().ok())
        {
            params.push(QueryParam::Int32(queue_id));
            format!("AND n.queue_id = ${}", params.len())
        } else {
            String::new()
        };
        params.push(QueryParam::Int64(limit));
        return database
            .query_json_params(
                &format!(
                    "WITH rolled AS ( \
                       SELECT n.champion_id, \
                         COALESCE(MAX(c.name), 'Champion ' || n.champion_id::TEXT) AS champion_name, \
                         SUM(n.plays)::BIGINT AS total_matches, \
                         SUM(n.wins)::BIGINT AS wins, SUM(n.losses)::BIGINT AS losses, \
                         SUM(n.kills_sum)::BIGINT AS sum_kills, \
                         SUM(n.deaths_sum)::BIGINT AS sum_deaths, \
                         SUM(n.assists_sum)::BIGINT AS sum_assists, \
                         SUM(n.damage_sum)::BIGINT AS sum_damage, \
                         SUM(n.credits_sum)::BIGINT AS sum_gold, \
                         SUM(n.healing_sum)::BIGINT AS sum_heal, \
                         SUM(n.mitigation_sum)::BIGINT AS sum_mitigation \
                       FROM nonranked_champion_stats_daily n \
                       LEFT JOIN champions c ON c.id = n.champion_id \
                       WHERE n.stats_scope = $1 {queue_clause} GROUP BY n.champion_id \
                     ), rated AS ( \
                       SELECT *, \
                         COALESCE(ROUND(100.0 * wins::NUMERIC \
                           / NULLIF((wins + losses)::NUMERIC, 0), 2), 0) AS win_rate, \
                         COALESCE(ROUND(total_matches::NUMERIC \
                           / NULLIF(SUM(total_matches) OVER (), 0), 4), 0) AS pick_rate, \
                         ROUND((sum_kills + sum_assists / 2.0)::NUMERIC \
                           / GREATEST(sum_deaths, 1), 2) AS kda FROM rolled \
                     ) \
                     SELECT champion_id, champion_name, total_matches, wins, losses, \
                       win_rate, pick_rate, NULL::NUMERIC AS ban_rate, \
                       NULL::BIGINT AS ban_total, kda, \
                       ROUND(sum_kills::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_kills, \
                       ROUND(sum_deaths::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_deaths, \
                       ROUND(sum_assists::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_assists, \
                       ROUND(sum_damage::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_damage, \
                       ROUND(sum_gold::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_gold, \
                       ROUND(sum_heal::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_heal, \
                       ROUND(sum_mitigation::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_mitigation, \
                       NULL::NUMERIC AS avg_league_tier \
                     FROM rated ORDER BY {sort} {order} LIMIT ${}",
                    params.len()
                ),
                &params,
            )
            .await;
    }

    let bounds = valid_tier_bounds(query).expect("handler validates tier bounds");
    if bounds.active() {
        let mut params = Vec::new();
        let mut player_where = vec!["spa.queue_id = 486".to_owned()];
        let mut ban_where = vec!["sba.queue_id = 486".to_owned()];
        if let Some(minimum) = bounds.minimum {
            params.push(QueryParam::Int16(minimum));
            player_where.push(format!("spa.lobby_tier >= ${}", params.len()));
            ban_where.push(format!("sba.lobby_tier >= ${}", params.len()));
        }
        if let Some(maximum) = bounds.maximum {
            params.push(QueryParam::Int16(maximum));
            player_where.push(format!("spa.lobby_tier <= ${}", params.len()));
            ban_where.push(format!("sba.lobby_tier <= ${}", params.len()));
        }
        params.push(QueryParam::Int64(limit));
        return database
            .query_json_params(
                &format!(
                    "WITH player_agg AS ( \
                       SELECT spa.champion_id, MAX(c.name) AS champion_name, \
                         SUM(spa.plays)::BIGINT AS total_matches, \
                         SUM(spa.wins)::BIGINT AS wins, SUM(spa.losses)::BIGINT AS losses, \
                         SUM(spa.kills_sum)::BIGINT AS sum_kills, \
                         SUM(spa.deaths_sum)::BIGINT AS sum_deaths, \
                         SUM(spa.assists_sum)::BIGINT AS sum_assists, \
                         SUM(spa.damage_sum)::BIGINT AS sum_damage, \
                         SUM(spa.gold_sum)::BIGINT AS sum_gold, \
                         SUM(spa.healing_sum)::BIGINT AS sum_heal, \
                         SUM(spa.mitigation_sum)::BIGINT AS sum_mitigation, \
                         SUM(spa.lobby_tier::BIGINT * spa.plays)::BIGINT AS sum_league_tier \
                       FROM stats_player_aggregate spa \
                       JOIN champions c ON c.id = spa.champion_id \
                       WHERE {} GROUP BY spa.champion_id \
                     ), ban_agg AS ( \
                       SELECT champion_id, SUM(bans)::BIGINT AS ban_total \
                       FROM stats_ban_aggregate sba WHERE {} GROUP BY champion_id \
                     ), merged AS ( \
                       SELECT p.*, COALESCE(b.ban_total, 0)::BIGINT AS ban_total \
                       FROM player_agg p LEFT JOIN ban_agg b ON b.champion_id = p.champion_id \
                     ), rated AS ( \
                       SELECT *, ROUND(100.0 * wins::NUMERIC \
                         / NULLIF((wins + losses)::NUMERIC, 0), 2) AS win_rate, \
                         ROUND(total_matches::NUMERIC \
                         / NULLIF(SUM(total_matches) OVER (), 0), 4) AS pick_rate, \
                         ROUND(ban_total::NUMERIC \
                         / NULLIF(SUM(ban_total) OVER (), 0), 4) AS ban_rate, \
                         ROUND((sum_kills + sum_assists / 2.0)::NUMERIC \
                         / GREATEST(sum_deaths, 1), 2) AS kda FROM merged \
                     ) \
                     SELECT champion_id, champion_name, total_matches, wins, losses, \
                       win_rate, pick_rate, ban_rate, ban_total, kda, \
                       ROUND(sum_kills::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_kills, \
                       ROUND(sum_deaths::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_deaths, \
                       ROUND(sum_assists::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_assists, \
                       ROUND(sum_damage::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_damage, \
                       ROUND(sum_gold::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_gold, \
                       ROUND(sum_heal::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_heal, \
                       ROUND(sum_mitigation::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_mitigation, \
                       ROUND(sum_league_tier::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_league_tier \
                     FROM rated ORDER BY {sort} {order} LIMIT ${}",
                    player_where.join(" AND "),
                    ban_where.join(" AND "),
                    params.len()
                ),
                &params,
            )
            .await;
    }

    database
        .query_json_params(
            &format!(
                "SELECT champion_id, champion_name, total_matches, wins, losses, \
                   win_rate, pick_rate, ban_rate, ban_total, kda, \
                   CASE WHEN total_matches > 0 THEN ROUND(sum_kills::NUMERIC / total_matches, 2) END AS avg_kills, \
                   CASE WHEN total_matches > 0 THEN ROUND(sum_deaths::NUMERIC / total_matches, 2) END AS avg_deaths, \
                   CASE WHEN total_matches > 0 THEN ROUND(sum_assists::NUMERIC / total_matches, 2) END AS avg_assists, \
                   CASE WHEN total_matches > 0 THEN ROUND(sum_damage::NUMERIC / total_matches, 2) END AS avg_damage, \
                   CASE WHEN total_matches > 0 THEN ROUND(sum_gold::NUMERIC / total_matches, 2) END AS avg_gold, \
                   CASE WHEN total_matches > 0 THEN ROUND(sum_heal::NUMERIC / total_matches, 2) END AS avg_heal, \
                   CASE WHEN total_matches > 0 THEN ROUND(sum_mitigation::NUMERIC / total_matches, 2) END AS avg_mitigation, \
                   CASE WHEN league_tier_count > 0 THEN ROUND(sum_league_tier::NUMERIC / league_tier_count, 2) END AS avg_league_tier \
                 FROM champion_stats_ranked ORDER BY {sort} {order} LIMIT $1"
            ),
            &[QueryParam::Int64(limit)],
        )
        .await
}

fn json_number(value: Option<&Value>) -> Value {
    match value {
        Some(Value::Number(number)) => Value::Number(number.clone()),
        Some(Value::String(value)) => value
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}
