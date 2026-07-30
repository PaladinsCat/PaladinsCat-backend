use std::collections::HashMap;

use axum::{
    Router,
    extract::{Extension, Query, State},
    http::{HeaderValue, header::CACHE_CONTROL},
    response::Response,
    routing::get,
};
use paladinscat_core::database::{Database, QueryParam};
use serde_json::{Map, Value, json};

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::cached_database_json,
};

use super::{StatsState, append_tier_predicates, stats_cache_key, valid_tier_bounds};

const PERFORMANCE_TTL_SECONDS: u64 = 900;

fn performance_cache(mut response: Response) -> Response {
    if response
        .headers()
        .get("x-cache")
        .is_none_or(|value| value != "HIT")
    {
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=900"),
        );
    }
    response
}
const ROLE_SQL: &str = r#"CASE
    WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' OR c.name IN ('Ash', 'Atlas', 'Azaan', 'Barik', 'Fernando', 'Inara', 'Khan', 'Makoa', 'Nyx', 'Raum', 'Ruckus', 'Terminus', 'Torvald', 'Yagorath') THEN 'Frontline'
    WHEN c.roles ILIKE '%Damage%' OR c.name IN ('Betty La Bomba', 'Betty la Bomba', 'Bomb King', 'Cassie', 'Dredge', 'Drogoz', 'Imani', 'Kinessa', 'Lian', 'Octavia', 'Omen', 'Saati', 'Sha Lin', 'Strix', 'Tiberius', 'Tyra', 'Viktor', 'Vivian', 'Willo') THEN 'Damage'
    WHEN c.roles ILIKE '%Flank%' OR c.name IN ('Androxus', 'Buck', 'Caspian', 'Evie', 'Kasumi', 'Koga', 'Lex', 'Maeve', 'Skye', 'Talus', 'Vatu', 'VII', 'Vora', 'Zhin') THEN 'Flank'
    WHEN c.roles ILIKE '%Support%' OR c.name IN ('Corvus', 'Furia', 'Grohk', 'Grover', 'Io', 'Jenos', 'Lillith', 'Mal Damba', 'Mal''Damba', 'Moji', 'Pip', 'Rei', 'Seris', 'Ying') THEN 'Support'
    ELSE COALESCE(NULLIF(c.roles, ''), 'Unknown')
  END"#;

pub(super) fn router() -> Router<StatsState> {
    Router::new()
        .route("/stats/performance-metrics", get(performance_metrics))
        .route(
            "/stats/performance-metrics/by-champion",
            get(performance_metrics_by_champion),
        )
}

#[derive(Clone, Copy)]
struct Metric {
    key: &'static str,
    casual_expression: Option<&'static str>,
}

const METRICS: &[Metric] = &[
    Metric {
        key: "dpm",
        casual_expression: Some("cmp.damage * 60.0 / NULLIF(cm.duration_seconds, 0)"),
    },
    Metric {
        key: "wpm",
        casual_expression: None,
    },
    Metric {
        key: "apm",
        casual_expression: None,
    },
    Metric {
        key: "hpm",
        casual_expression: Some("cmp.healing * 60.0 / NULLIF(cm.duration_seconds, 0)"),
    },
    Metric {
        key: "gpm",
        casual_expression: Some("cmp.credits * 60.0 / NULLIF(cm.duration_seconds, 0)"),
    },
    Metric {
        key: "egpm",
        casual_expression: None,
    },
    Metric {
        key: "mpm",
        casual_expression: Some("cmp.mitigation * 60.0 / NULLIF(cm.duration_seconds, 0)"),
    },
    Metric {
        key: "kda",
        casual_expression: None,
    },
];

async fn performance_metrics(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let requested = match query.get("metric") {
        Some(value) => Some(metric(value).ok_or_else(|| {
            ApiError::validation("Invalid metric. Use dpm, wpm, apm, hpm, gpm, egpm, mpm, or kda.")
        })?),
        None => None,
    };
    let scope = query
        .get("scope")
        .map_or("ranked".to_owned(), |value| value.trim().to_lowercase());
    if scope != "ranked" && scope != "casual" {
        return Err(ApiError::validation("Invalid scope. Use ranked or casual."));
    }
    let role = match query.get("role") {
        Some(value) if !value.is_empty() => Some(normalize_role(value).ok_or_else(|| {
            ApiError::validation("Invalid role. Use damage, flank, support, or frontline.")
        })?),
        _ => None,
    };
    let include_roles = role.is_none()
        && query
            .get("includeRoles")
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let bounds = valid_tier_bounds(&query)?;
    if scope == "casual" && bounds.active() {
        return Err(ApiError::validation(
            "Lobby-tier filters apply only to ranked performance.",
        ));
    }
    if scope == "casual" && requested.is_some_and(|value| value.casual_expression.is_none()) {
        return Err(ApiError::validation(
            "Casual performance supports dpm, hpm, gpm, and mpm.",
        ));
    }

    let database = state.database.clone();
    let cache = state.cache.clone();
    let cache_key = stats_cache_key(&uri);
    cached_database_json(
        cache,
        cache_key,
        PERFORMANCE_TTL_SECONDS,
        PERFORMANCE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            let query = query.clone();
            let scope = scope.clone();
            async move {
                let selected: Vec<Metric> =
                    requested.map(|value| vec![value]).unwrap_or_else(|| {
                        METRICS
                            .iter()
                            .copied()
                            .filter(|value| scope == "ranked" || value.casual_expression.is_some())
                            .collect()
                    });
                let mut response = Map::new();
                for selected_metric in selected {
                    let summary =
                        metric_summary(&database, selected_metric, &scope, role, &query).await?;
                    response.insert(selected_metric.key.to_owned(), summary);
                }
                if let Some(requested_metric) = requested.filter(|_| include_roles) {
                    let mut roles = Map::new();
                    for role_name in ["Frontline", "Damage", "Flank", "Support"] {
                        roles.insert(
                            role_name.to_owned(),
                            metric_summary(
                                &database,
                                requested_metric,
                                &scope,
                                Some(role_name),
                                &query,
                            )
                            .await?,
                        );
                    }
                    response.insert("roles".to_owned(), Value::Object(roles));
                }
                Ok(Value::Object(response))
            }
        },
    )
    .await
    .map(performance_cache)
}

async fn metric_summary(
    database: &Database,
    selected: Metric,
    scope: &str,
    role: Option<&'static str>,
    query: &HashMap<String, String>,
) -> Result<Value, paladinscat_core::database::DatabaseError> {
    let bounds = valid_tier_bounds(query).expect("validated tier bounds");
    if scope == "ranked" && bounds.active() {
        let mut params = vec![
            QueryParam::Int32(486),
            QueryParam::Int16(role_id(role)),
            QueryParam::Text(selected.key.to_owned()),
        ];
        let mut clauses = vec![
            "hist.queue_id = $1".to_owned(),
            "hist.role_id = $2".to_owned(),
            "hist.metric = $3".to_owned(),
        ];
        append_tier_predicates(bounds, &mut params, &mut clauses, "hist");
        let rows = database
            .query_json_params(
                &format!(
                    "SELECT hist.value, SUM(hist.sample_count)::BIGINT AS sample_count \
                     FROM stats_metric_histogram hist WHERE {} \
                     GROUP BY hist.value ORDER BY hist.value",
                    clauses.join(" AND ")
                ),
                &params,
            )
            .await?;
        return Ok(weighted_summary(&rows, selected.key));
    }

    if scope == "ranked" {
        return Ok(database
            .one_json_params(
                "SELECT min_value AS min, max_value AS max, mean_value AS mean, \
                    median_value AS median, mode_value AS mode, p10_value AS p10, \
                    p25_value AS p25, p75_value AS p75, p90_value AS p90, \
                    sample_size::DOUBLE PRECISION AS sample_size, updated_at \
                 FROM performance_metric_stats \
                 WHERE queue_id = $1 AND role_name = $2 AND metric = $3",
                &[
                    QueryParam::Int32(486),
                    QueryParam::Text(role.unwrap_or("Global").to_owned()),
                    QueryParam::Text(selected.key.to_owned()),
                ],
            )
            .await?
            .unwrap_or_else(empty_summary));
    }

    let mut params = Vec::new();
    let mut clauses = vec![
        "cm.stats_eligible = true".to_owned(),
        "cm.quality = 'complete'".to_owned(),
        "cmp.stats_eligible = true".to_owned(),
        "cmp.participant_kind = 'human'".to_owned(),
        "cmp.player_id > 0".to_owned(),
        "cmp.task_force IN (1, 2)".to_owned(),
        "lower(COALESCE(cmp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')".to_owned(),
        "cm.duration_seconds > 0".to_owned(),
    ];
    if let Some(role) = role {
        params.push(QueryParam::Text(role.to_owned()));
        clauses.push(format!("{ROLE_SQL} = ${}", params.len()));
    }
    let expression = selected
        .casual_expression
        .expect("casual metric validated by handler");
    let sql = format!(
        "WITH metric_values AS ( \
           SELECT ({expression})::DOUBLE PRECISION AS value \
           FROM casual_match_players cmp \
           JOIN casual_matches cm ON cm.match_id = cmp.match_id \
           LEFT JOIN champions c ON c.id = cmp.champion_id \
           WHERE {} \
         ) \
         SELECT \
           COALESCE(ROUND(MIN(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS min, \
           COALESCE(ROUND(MAX(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS max, \
           COALESCE(ROUND(AVG(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS mean, \
           COALESCE(ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS median, \
           COALESCE(ROUND((MODE() WITHIN GROUP (ORDER BY ROUND(value::NUMERIC, 0)))::NUMERIC, 2), 0)::DOUBLE PRECISION AS mode, \
           COALESCE(ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p10, \
           COALESCE(ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p25, \
           COALESCE(ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p75, \
           COALESCE(ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p90, \
           COUNT(*)::INT AS sample_size \
         FROM metric_values WHERE value > 0",
        clauses.join(" AND ")
    );
    Ok(database
        .one_json_params(&sql, &params)
        .await?
        .unwrap_or_else(empty_summary))
}

async fn performance_metrics_by_champion(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let selected = query
        .get("metric")
        .and_then(|value| metric(value))
        .ok_or_else(|| {
            ApiError::validation("Invalid metric. Use dpm, wpm, apm, hpm, gpm, egpm, mpm, or kda.")
        })?;
    if query
        .get("queueId")
        .is_some_and(|value| value.parse::<i32>().ok() != Some(486))
    {
        return Err(ApiError::validation("Invalid queueId."));
    }
    let champion_id = match query.get("championId") {
        Some(value) => Some(
            value
                .parse::<i32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| ApiError::validation("Invalid championId."))?,
        ),
        None => None,
    };
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![
        QueryParam::Int32(486),
        QueryParam::Text(selected.key.to_owned()),
    ];
    let mut clauses = Vec::new();
    if bounds.active() {
        clauses.push("hist.queue_id = $1".to_owned());
        clauses.push("hist.metric = $2".to_owned());
        if let Some(champion_id) = champion_id {
            params.push(QueryParam::Int32(champion_id));
            clauses.push(format!("hist.champion_id = ${}", params.len()));
        }
        append_tier_predicates(bounds, &mut params, &mut clauses, "hist");
    } else {
        clauses.push("cpb.queue_id = $1".to_owned());
        clauses.push("cpb.metric = $2".to_owned());
        if let Some(champion_id) = champion_id {
            params.push(QueryParam::Int32(champion_id));
            clauses.push(format!("cpb.champion_id = ${}", params.len()));
        }
    }

    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        PERFORMANCE_TTL_SECONDS,
        PERFORMANCE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            let params = params.clone();
            let clauses = clauses.clone();
            async move {
                let rows = if bounds.active() {
                    database
                        .query_json_params(
                            &weighted_champion_histogram_sql(&clauses.join(" AND ")),
                            &params,
                        )
                        .await?
                } else {
                    database
                        .query_json_params(
                            &format!(
                                "SELECT cpb.champion_id, c.name AS champion_name, \
                                   {ROLE_SQL} AS class, cpb.min_value AS min, \
                                   cpb.max_value AS max, cpb.mean_value AS mean, \
                                   cpb.median_value AS median, cpb.mode_value AS mode, \
                                   cpb.p10_value AS p10, cpb.p90_value AS p90, \
                                   cpb.mean_value AS avg_value, \
                                   cpb.sample_size AS total_matches \
                                 FROM champion_performance_baselines cpb \
                                 JOIN champions c ON c.id = cpb.champion_id \
                                 WHERE {} \
                                 ORDER BY cpb.mean_value DESC, cpb.sample_size DESC, c.name ASC",
                                clauses.join(" AND ")
                            ),
                            &params,
                        )
                        .await?
                };
                Ok(json!({
                    "data": rows,
                    "total": rows.len(),
                    "metric": selected.key,
                    "queue_id": 486
                }))
            }
        },
    )
    .await
    .map(performance_cache)
}

fn weighted_summary(rows: &[Value], metric: &str) -> Value {
    let ordered = rows
        .iter()
        .filter_map(|row| {
            let value = row
                .get("value")?
                .as_f64()
                .or_else(|| row.get("value")?.as_str()?.parse().ok())?;
            let count = row
                .get("sample_count")?
                .as_i64()
                .or_else(|| row.get("sample_count")?.as_str()?.parse().ok())?;
            (count > 0 && value.is_finite()).then_some((value, count))
        })
        .collect::<Vec<_>>();
    let sample_size = ordered.iter().map(|(_, count)| *count).sum::<i64>();
    if sample_size <= 0 {
        return empty_summary();
    }
    let percentile = |fraction: f64| {
        let position = (sample_size - 1) as f64 * fraction;
        let lower_index = position.floor() as i64;
        let upper_index = position.ceil() as i64;
        let mut cumulative = 0;
        let mut lower = ordered.first().map_or(0.0, |row| row.0);
        let mut upper = lower;
        let mut found_lower = false;
        for (value, count) in &ordered {
            cumulative += count;
            if !found_lower && cumulative > lower_index {
                lower = *value;
                found_lower = true;
            }
            if cumulative > upper_index {
                upper = *value;
                break;
            }
        }
        lower + (upper - lower) * (position - lower_index as f64)
    };
    let round = |value: f64| (value * 100.0).round() / 100.0;
    let mean = ordered
        .iter()
        .map(|(value, count)| value * *count as f64)
        .sum::<f64>()
        / sample_size as f64;
    let mut modes = std::collections::HashMap::<i64, i64>::new();
    for (value, count) in &ordered {
        let key = if metric == "kda" {
            (value * 10.0).round() as i64
        } else {
            value.round() as i64
        };
        *modes.entry(key).or_default() += count;
    }
    let mode_key = modes
        .into_iter()
        .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        .map_or(0, |row| row.0);
    let mode = if metric == "kda" {
        mode_key as f64 / 10.0
    } else {
        mode_key as f64
    };
    json!({
        "min": round(ordered.first().map_or(0.0, |row| row.0)),
        "max": round(ordered.last().map_or(0.0, |row| row.0)),
        "mean": round(mean),
        "median": round(percentile(0.5)),
        "mode": round(mode),
        "p10": round(percentile(0.1)),
        "p25": round(percentile(0.25)),
        "p75": round(percentile(0.75)),
        "p90": round(percentile(0.9)),
        "sample_size": sample_size
    })
}

fn weighted_champion_histogram_sql(predicate: &str) -> String {
    format!(
        "WITH grouped AS ( \
           SELECT hist.champion_id, hist.value::DOUBLE PRECISION AS value, \
             SUM(hist.sample_count)::BIGINT AS weight \
           FROM stats_champion_metric_histogram hist \
           WHERE {predicate} GROUP BY hist.champion_id, hist.value \
         ), ordered AS ( \
           SELECT champion_id, value, weight, \
             SUM(weight) OVER (PARTITION BY champion_id ORDER BY value) AS cumulative, \
             SUM(weight) OVER (PARTITION BY champion_id) AS total \
           FROM grouped \
         ), rolled AS ( \
           SELECT champion_id, MIN(value) AS min, MAX(value) AS max, \
             ROUND((SUM(value * weight) / NULLIF(SUM(weight), 0))::NUMERIC, 2)::DOUBLE PRECISION AS mean, \
             MIN(value) FILTER (WHERE cumulative >= total * 0.50) AS median, \
             (ARRAY_AGG(value ORDER BY weight DESC, value))[1] AS mode, \
             MIN(value) FILTER (WHERE cumulative >= total * 0.10) AS p10, \
             MIN(value) FILTER (WHERE cumulative >= total * 0.90) AS p90, \
             MAX(total)::BIGINT AS sample_size \
           FROM ordered GROUP BY champion_id \
         ) \
         SELECT rolled.champion_id, c.name AS champion_name, {ROLE_SQL} AS class, \
           min, max, mean, median, mode, p10, p90, mean AS avg_value, \
           sample_size AS total_matches \
         FROM rolled JOIN champions c ON c.id = rolled.champion_id \
         ORDER BY mean DESC, sample_size DESC, c.name ASC"
    )
}

fn metric(value: &str) -> Option<Metric> {
    METRICS
        .iter()
        .copied()
        .find(|metric| metric.key.eq_ignore_ascii_case(value))
}

fn normalize_role(value: &str) -> Option<&'static str> {
    let key = value
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

fn role_id(role: Option<&str>) -> i16 {
    match role {
        Some("Damage") => 1,
        Some("Flank") => 2,
        Some("Support") => 3,
        Some("Frontline") => 4,
        _ => 0,
    }
}

fn empty_summary() -> Value {
    json!({
        "min": 0,
        "max": 0,
        "mean": 0,
        "median": 0,
        "mode": 0,
        "p10": 0,
        "p25": 0,
        "p75": 0,
        "p90": 0,
        "sample_size": 0
    })
}
