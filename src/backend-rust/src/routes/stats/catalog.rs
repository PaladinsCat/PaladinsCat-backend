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
    StatsState, append_tier_predicates, cached_rows, legacy_limit, normalize_item_role,
    stats_cache_key, valid_tier_bounds,
};

const CACHE_TTL_SECONDS: u64 = 300;

pub(super) fn router() -> Router<StatsState> {
    Router::new()
        .route("/stats/skins", get(skins))
        .route("/stats/broken-skins", get(broken_skins))
        .route("/stats/items/{item_id}", get(item_detail))
        .route("/stats/hourly-match-counts", get(hourly_match_counts))
        .route("/stats/talents", get(talents))
        .route("/stats/talents/{champion_id}", get(champion_talents))
        .route("/stats/cards", get(cards))
        .route("/stats/cards/{champion_id}", get(champion_cards))
        .route("/stats/cards/{champion_id}/{card_id}", get(card_detail))
        .route("/stats/baselines", get(baselines))
}

fn ranked_mode(query: &HashMap<String, String>) -> Result<(), ApiError> {
    if query
        .get("mode")
        .is_some_and(|mode| !mode.eq_ignore_ascii_case("ranked"))
    {
        return Err(ApiError::validation(
            "Only ranked aggregate statistics are available for this endpoint.",
        ));
    }
    Ok(())
}

fn positive_id(raw: &str, name: &str) -> Result<i32, ApiError> {
    raw.parse::<i32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| ApiError::validation(format!("Invalid {name}.")))
}

async fn skins(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = valid_tier_bounds(&query)?;
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);
    let mut params = Vec::new();
    let mut clauses = Vec::new();
    if let Some(raw) = query.get("championId") {
        params.push(QueryParam::Int32(positive_id(raw, "championId")?));
        clauses.push(format!("scr.champion_id = ${}", params.len()));
    }
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        clauses.push(format!("scr.league_tier >= ${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        clauses.push(format!("scr.league_tier <= ${}", params.len()));
    }
    params.push(QueryParam::Int64(limit));
    let sql = format!(
        "SELECT scr.skin_id, MAX(scr.skin_name) AS skin_name, scr.champion_id, \
               c.name AS champion_name, SUM(scr.count)::INT AS total_plays, \
               SUM(scr.wins)::INT AS wins, SUM(scr.losses)::INT AS losses, \
               COALESCE(ROUND(100.0 * SUM(scr.wins)::NUMERIC \
                 / NULLIF((SUM(scr.wins) + SUM(scr.losses))::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS win_rate \
             FROM skin_counts_ranked scr JOIN champions c ON c.id = scr.champion_id {} \
             GROUP BY scr.skin_id, scr.champion_id, c.name \
             ORDER BY win_rate DESC, total_plays DESC, skin_name ASC LIMIT ${}",
        if clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", clauses.join(" AND "))
        },
        params.len()
    );
    cached_wrapped_rows(
        state,
        uri,
        request_id,
        sql,
        params,
        bounds.minimum,
        bounds.maximum,
    )
    .await
}

async fn broken_skins(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let bounds = valid_tier_bounds(&query)?;
    let mut params = Vec::new();
    let mut clauses = vec!["scr.skin_id > 32767".to_owned()];
    if let Some(raw) = query.get("championId") {
        params.push(QueryParam::Int32(positive_id(raw, "championId")?));
        clauses.push(format!("scr.champion_id = ${}", params.len()));
    }
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        clauses.push(format!("scr.league_tier >= ${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        clauses.push(format!("scr.league_tier <= ${}", params.len()));
    }
    let sql = format!(
        "WITH broken AS ( \
               SELECT scr.skin_id, MAX(scr.skin_name) AS skin_name, scr.champion_id, \
                 SUM(scr.count)::INT AS total_plays, SUM(scr.wins)::INT AS wins, \
                 SUM(scr.losses)::INT AS losses, \
                 COALESCE(ROUND(100.0 * SUM(scr.wins)::NUMERIC \
                   / NULLIF((SUM(scr.wins) + SUM(scr.losses))::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS win_rate \
               FROM skin_counts_ranked scr WHERE {} \
               GROUP BY scr.skin_id, scr.champion_id \
             ), champ_totals AS ( \
               SELECT champion_id, SUM(total_plays) AS champion_total FROM broken GROUP BY champion_id \
             ) SELECT b.skin_id, b.skin_name, b.champion_id, c.name AS champion_name, \
                 b.total_plays, b.wins, b.losses, b.win_rate, \
                 ROUND(b.total_plays::NUMERIC / NULLIF(ct.champion_total, 0) * 100, 1) AS usage_share \
               FROM broken b JOIN champ_totals ct ON ct.champion_id = b.champion_id \
               JOIN champions c ON c.id = b.champion_id \
               ORDER BY b.champion_id, b.total_plays DESC, b.skin_id DESC",
        clauses.join(" AND ")
    );
    cached_wrapped_rows(
        state,
        uri,
        request_id,
        sql,
        params,
        bounds.minimum,
        bounds.maximum,
    )
    .await
}

async fn item_detail(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(item_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    ranked_mode(&query)?;
    let item_id = positive_id(&item_id, "item id")?;
    let bounds = valid_tier_bounds(&query)?;
    let champion_id = query
        .get("championId")
        .map(|raw| positive_id(raw, "champion id"))
        .transpose()?;
    let role = query
        .get("role")
        .map(|raw| {
            normalize_item_role(raw).ok_or_else(|| {
                ApiError::validation("Invalid role. Use Frontline, Damage, Flank, or Support.")
            })
        })
        .transpose()?;

    let mut params = vec![QueryParam::Int32(item_id), QueryParam::Int32(486)];
    let (source, alias, uses_column) = if bounds.active() || champion_id.is_some() || role.is_some()
    {
        ("stats_item_aggregate", "sia", "uses")
    } else {
        ("item_counts_ranked", "sia", "count")
    };
    let mut clauses = vec!["sia.item_id = $1".to_owned()];
    if source == "stats_item_aggregate" {
        clauses.push("sia.queue_id = $2".to_owned());
        if let Some(champion_id) = champion_id {
            params.push(QueryParam::Int32(champion_id));
            clauses.push(format!("sia.champion_id = ${}", params.len()));
        }
        if let Some(role) = role {
            params.push(QueryParam::Text(role.to_owned()));
            clauses.push(format!("{} = ${}", super::CHAMPION_ROLE_SQL, params.len()));
        }
        append_tier_predicates(bounds, &mut params, &mut clauses, alias);
    } else {
        params.pop();
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
            let params = params.clone();
            let clauses = clauses.clone();
            async move {
                let rows = database
                    .query_json_params(
                        &format!(
                            "SELECT sia.slot, sia.item_level, \
                               SUM(sia.{uses_column})::BIGINT AS total_uses, \
                               SUM(sia.wins)::BIGINT AS wins, SUM(sia.losses)::BIGINT AS losses, \
                               COALESCE(ROUND(100.0 * SUM(sia.wins)::NUMERIC \
                                 / NULLIF((SUM(sia.wins) + SUM(sia.losses))::NUMERIC, 0), 2), 0) AS win_rate \
                             FROM {source} sia {} WHERE {} \
                             GROUP BY sia.slot, sia.item_level ORDER BY sia.slot, sia.item_level",
                            if role.is_some() {
                                "LEFT JOIN champions c ON c.id = sia.champion_id"
                            } else {
                                ""
                            },
                            clauses.join(" AND ")
                        ),
                        &params,
                    )
                    .await?;
                if rows.is_empty() {
                    return Ok(json!({ "error": "Item statistics not found" }));
                }
                let reference = database
                    .query_json_params(
                        "SELECT item_id, item_name FROM items WHERE item_id = $1 LIMIT 1",
                        &[QueryParam::Int32(item_id)],
                    )
                    .await?
                    .into_iter()
                    .next();
                Ok(item_payload(
                    item_id,
                    reference,
                    rows,
                    source == "item_counts_ranked",
                ))
            }
        },
    )
    .await
}

fn integer(row: &Value, key: &str) -> i64 {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}

fn item_payload(
    item_id: i32,
    reference: Option<Value>,
    rows: Vec<Value>,
    string_rates: bool,
) -> Value {
    let summarize = |subset: &[&Value]| {
        let uses: i64 = subset.iter().map(|row| integer(row, "total_uses")).sum();
        let wins: i64 = subset.iter().map(|row| integer(row, "wins")).sum();
        let losses: i64 = subset.iter().map(|row| integer(row, "losses")).sum();
        let rate = if wins + losses > 0 {
            (10000.0 * wins as f64 / (wins + losses) as f64).round() / 100.0
        } else {
            0.0
        };
        json!({
            "total_uses": uses,
            "wins": wins,
            "losses": losses,
            "win_rate": if string_rates {
                Value::String(format!("{rate:.2}"))
            } else {
                json!(rate)
            }
        })
    };
    let all = rows.iter().collect::<Vec<_>>();
    let mut slots = rows
        .iter()
        .filter_map(|row| row.get("slot").and_then(Value::as_i64))
        .collect::<Vec<_>>();
    slots.sort_unstable();
    slots.dedup();
    let mut levels = rows
        .iter()
        .filter_map(|row| row.get("item_level").and_then(Value::as_i64))
        .collect::<Vec<_>>();
    levels.sort_unstable();
    levels.dedup();
    let mut payload = summarize(&all);
    let object = payload.as_object_mut().expect("item summary object");
    object.insert("mode".to_owned(), json!("ranked"));
    object.insert("item_id".to_owned(), json!(item_id));
    object.insert(
        "item_name".to_owned(),
        reference
            .as_ref()
            .and_then(|row| row.get("item_name"))
            .cloned()
            .unwrap_or_else(|| json!(format!("Item {item_id}"))),
    );
    object.insert(
        "slots".to_owned(),
        Value::Array(
            slots
                .into_iter()
                .map(|slot| {
                    let subset = rows
                        .iter()
                        .filter(|row| row.get("slot").and_then(Value::as_i64) == Some(slot))
                        .collect::<Vec<_>>();
                    let mut value = summarize(&subset);
                    value
                        .as_object_mut()
                        .expect("slot summary")
                        .insert("slot".to_owned(), json!(slot));
                    value
                })
                .collect(),
        ),
    );
    object.insert(
        "levels".to_owned(),
        Value::Array(
            levels
                .into_iter()
                .map(|level| {
                    let subset = rows
                        .iter()
                        .filter(|row| row.get("item_level").and_then(Value::as_i64) == Some(level))
                        .collect::<Vec<_>>();
                    let mut value = summarize(&subset);
                    value
                        .as_object_mut()
                        .expect("level summary")
                        .insert("item_level".to_owned(), json!(level));
                    value
                })
                .collect(),
        ),
    );
    object.insert(
        "breakdown".to_owned(),
        Value::Array(
            rows.into_iter()
                .map(|mut row| {
                    let integers =
                        ["total_uses", "wins", "losses"].map(|key| (key, integer(&row, key)));
                    if let Some(object) = row.as_object_mut() {
                        for (key, value) in integers {
                            object.insert(key.to_owned(), json!(value));
                        }
                    }
                    row
                })
                .collect(),
        ),
    );
    payload
}

async fn hourly_match_counts(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let date = query
        .get("date")
        .cloned()
        .unwrap_or_else(current_utc_yyyymmdd);
    if date.len() != 8 || !date.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(ApiError::validation("Invalid date. Use YYYYMMDD."));
    }
    if query
        .get("queueId")
        .is_some_and(|value| value.parse::<i32>().ok() != Some(486))
    {
        return Err(ApiError::validation(
            "Only ranked queue 486 is available for aggregate statistics.",
        ));
    }
    let hour = query
        .get("hour")
        .map(|value| {
            value
                .parse::<i16>()
                .ok()
                .filter(|hour| (0..=23).contains(hour))
                .ok_or_else(|| ApiError::validation("Invalid hour. Use 0-23."))
        })
        .transpose()?;
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![QueryParam::Text(date)];
    let mut clauses;
    let sql = if bounds.active() {
        clauses = vec![
            "m.queue_id = 486".to_owned(),
            "m.entry_datetime >= to_date($1, 'YYYYMMDD')".to_owned(),
            "m.entry_datetime < to_date($1, 'YYYYMMDD') + INTERVAL '1 day'".to_owned(),
        ];
        if let Some(hour) = hour {
            params.push(QueryParam::Int16(hour));
            clauses.push(format!(
                "EXTRACT(HOUR FROM m.entry_datetime AT TIME ZONE 'UTC') = ${}",
                params.len()
            ));
        }
        append_tier_predicates(bounds, &mut params, &mut clauses, "mlt");
        format!(
            "SELECT to_char(m.entry_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS date, \
               EXTRACT(HOUR FROM m.entry_datetime AT TIME ZONE 'UTC')::INT AS hour, 486 AS queue_id, \
               COUNT(*) FILTER (WHERE m.region = 'NA')::INT AS matches_na, \
               COUNT(*) FILTER (WHERE m.region = 'EU')::INT AS matches_eu, \
               COUNT(*) FILTER (WHERE m.region = 'Asia')::INT AS matches_asia, \
               COUNT(*) FILTER (WHERE m.region = 'SEA')::INT AS matches_sea, \
               COUNT(*) FILTER (WHERE m.region = 'JPN')::INT AS matches_jpn, \
               COUNT(*) FILTER (WHERE m.region = 'BR')::INT AS matches_br, \
               COUNT(*) FILTER (WHERE m.region = 'OCE')::INT AS matches_oce, \
               COUNT(*) FILTER (WHERE m.region = 'SA')::INT AS matches_sa, \
               COUNT(*) FILTER (WHERE m.region IS NULL OR m.region NOT IN ('NA','EU','Asia','SEA','JPN','BR','OCE','SA'))::INT AS matches_unknown, \
               COUNT(*)::INT AS total_matches, now() AS fetched_at \
             FROM matches m JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id \
               AND mlt.entry_datetime = m.entry_datetime WHERE {} \
             GROUP BY 1, 2 ORDER BY hour DESC",
            clauses.join(" AND ")
        )
    } else {
        params.push(QueryParam::Int32(486));
        clauses = vec![
            "date = to_date($1, 'YYYYMMDD')".to_owned(),
            "queue_id = $2".to_owned(),
        ];
        if let Some(hour) = hour {
            params.push(QueryParam::Int16(hour));
            clauses.push(format!("hour = ${}", params.len()));
        }
        format!(
            "SELECT date::TEXT AS date, hour, queue_id, matches_na, matches_eu, \
               matches_asia, matches_br, matches_oce, matches_sa, matches_unknown, \
               total_matches, fetched_at FROM hourly_match_counts WHERE {} \
             ORDER BY hour DESC, queue_id ASC",
            clauses.join(" AND ")
        )
    };
    cached_rows(state, uri, request_id, 60, sql, params).await
}

fn current_utc_yyyymmdd() -> String {
    let date = time::OffsetDateTime::now_utc().date();
    format!(
        "{:04}{:02}{:02}",
        date.year(),
        u8::from(date.month()),
        date.day()
    )
}

async fn talents(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    ranked_mode(&query)?;
    let bounds = valid_tier_bounds(&query)?;
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);
    let mut params = vec![QueryParam::Int32(486)];
    let mut clauses = vec!["sta.queue_id = $1".to_owned()];
    append_tier_predicates(bounds, &mut params, &mut clauses, "sta");
    params.push(QueryParam::Int64(limit));
    cached_rows(
        state,
        uri,
        request_id,
        CACHE_TTL_SECONDS,
        format!(
            "SELECT sta.talent_id, COALESCE(t.talent_name, 'Talent ' || sta.talent_id::TEXT) AS name, \
               COALESCE(t.talent_name, 'Talent ' || sta.talent_id::TEXT) AS talent_name, \
               sta.champion_id, COALESCE(c.name, 'Unknown') AS champion_name, \
               SUM(sta.uses)::BIGINT AS total_uses, SUM(sta.uses)::BIGINT AS total_plays, \
               COALESCE(ROUND(100.0 * SUM(sta.wins)::NUMERIC \
                 / NULLIF((SUM(sta.wins) + SUM(sta.losses))::NUMERIC, 0), 2), 0) AS win_rate, \
               ROUND(SUM(sta.kills_sum)::NUMERIC / NULLIF(SUM(sta.uses), 0), 2) AS avg_kills, \
               ROUND(SUM(sta.deaths_sum)::NUMERIC / NULLIF(SUM(sta.uses), 0), 2) AS avg_deaths, \
               ROUND(SUM(sta.assists_sum)::NUMERIC / NULLIF(SUM(sta.uses), 0), 2) AS avg_assists \
             FROM stats_talent_aggregate sta \
             JOIN talents t ON t.talent_id = sta.talent_id AND t.champion_id = sta.champion_id \
             LEFT JOIN champions c ON c.id = sta.champion_id WHERE {} \
             GROUP BY sta.talent_id, t.talent_name, sta.champion_id, c.name \
             ORDER BY total_uses DESC, talent_name ASC LIMIT ${}",
            clauses.join(" AND "),
            params.len()
        ),
        params,
    )
    .await
}

async fn champion_talents(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(champion_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    ranked_mode(&query)?;
    let champion_id = positive_id(&champion_id, "championId")?;
    let bounds = valid_tier_bounds(&query)?;
    let mut params = vec![QueryParam::Int32(champion_id), QueryParam::Int32(486)];
    let mut player_where = vec![
        "spa.champion_id = $1".to_owned(),
        "spa.queue_id = $2".to_owned(),
    ];
    let mut talent_where = vec![
        "sta.champion_id = $1".to_owned(),
        "sta.queue_id = $2".to_owned(),
    ];
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        player_where.push(format!("spa.lobby_tier >= ${}", params.len()));
        talent_where.push(format!("sta.lobby_tier >= ${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        player_where.push(format!("spa.lobby_tier <= ${}", params.len()));
        talent_where.push(format!("sta.lobby_tier <= ${}", params.len()));
    }
    let sql = format!(
        "WITH players AS ( \
           SELECT COALESCE(SUM(plays), 0)::BIGINT AS total, COALESCE(SUM(wins), 0)::BIGINT AS wins, \
             COALESCE(SUM(losses), 0)::BIGINT AS losses FROM stats_player_aggregate spa WHERE {} \
         ), covered AS ( \
           SELECT COALESCE(SUM(uses), 0)::BIGINT AS total, COALESCE(SUM(wins), 0)::BIGINT AS wins, \
             COALESCE(SUM(losses), 0)::BIGINT AS losses FROM stats_talent_aggregate sta \
             JOIN talents t ON t.talent_id = sta.talent_id AND t.champion_id = sta.champion_id WHERE {} \
         ), talent_rows AS ( \
           SELECT t.talent_id AS \"talentId\", t.talent_name AS \"talentName\", \
             COALESCE(SUM(sta.uses), 0)::BIGINT AS \"totalPlays\", \
             COALESCE(SUM(sta.wins), 0)::BIGINT AS wins, COALESCE(SUM(sta.losses), 0)::BIGINT AS losses, \
             ROUND(100.0 * COALESCE(SUM(sta.wins), 0)::NUMERIC \
               / NULLIF((COALESCE(SUM(sta.wins), 0) + COALESCE(SUM(sta.losses), 0))::NUMERIC, 0), 2) AS \"winRate\" \
           FROM talents t LEFT JOIN stats_talent_aggregate sta \
             ON sta.talent_id = t.talent_id AND {} WHERE t.champion_id = $1 \
           GROUP BY t.talent_id, t.talent_name \
         ) SELECT jsonb_build_object( \
             'totalMatches', p.total, 'talentCoveredMatches', c.total, \
             'disconnectedPlayers', GREATEST(p.total - c.total, 0), \
             'disconnectedWins', GREATEST(p.wins - c.wins, 0), \
             'disconnectedLosses', GREATEST(p.losses - c.losses, 0), \
             'disconnectedWinRate', CASE WHEN GREATEST(p.wins-c.wins,0)+GREATEST(p.losses-c.losses,0)>0 \
               THEN ROUND(100.0*GREATEST(p.wins-c.wins,0)/ \
                 (GREATEST(p.wins-c.wins,0)+GREATEST(p.losses-c.losses,0)),2) END, \
             'talentCoverageRate', CASE WHEN p.total>0 THEN ROUND(100.0*c.total/p.total,2) END, \
             'talents', (SELECT COALESCE(jsonb_agg(jsonb_build_object( \
               'talentId', tr.\"talentId\", 'talentName', tr.\"talentName\", \
               'totalPlays', tr.\"totalPlays\"::TEXT, 'wins', tr.wins::TEXT, \
               'losses', tr.losses::TEXT, 'winRate', tr.\"winRate\"::TEXT) \
               ORDER BY tr.\"totalPlays\" DESC), '[]'::jsonb) FROM talent_rows tr) \
           ) AS payload FROM players p CROSS JOIN covered c",
        player_where.join(" AND "),
        talent_where.join(" AND "),
        talent_where.join(" AND ")
    );
    cached_payload(state, uri, request_id, sql, params).await
}

async fn cards(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    ranked_mode(&query)?;
    let bounds = valid_tier_bounds(&query)?;
    let limit = legacy_limit(query.get("limit").map(String::as_str), 50, 200);
    let mut params = vec![QueryParam::Int32(486)];
    let mut clauses = vec!["sca.queue_id = $1".to_owned()];
    append_tier_predicates(bounds, &mut params, &mut clauses, "sca");
    params.push(QueryParam::Int64(limit));
    cached_rows(
        state,
        uri,
        request_id,
        CACHE_TTL_SECONDS,
        format!(
            "SELECT sca.card_id, COALESCE(c.card_name, 'Card ' || sca.card_id::TEXT) AS name, \
               SUM(sca.uses)::BIGINT AS total_uses, \
               COALESCE(ROUND(100.0 * SUM(sca.wins)::NUMERIC \
                 / NULLIF((SUM(sca.wins) + SUM(sca.losses))::NUMERIC, 0), 2), 0) AS win_rate, \
               ROUND(SUM(sca.kills_sum)::NUMERIC / NULLIF(SUM(sca.uses), 0), 2) AS avg_kills, \
               ROUND(SUM(sca.deaths_sum)::NUMERIC / NULLIF(SUM(sca.uses), 0), 2) AS avg_deaths, \
               ROUND(SUM(sca.assists_sum)::NUMERIC / NULLIF(SUM(sca.uses), 0), 2) AS avg_assists \
             FROM stats_card_aggregate sca LEFT JOIN cards c ON c.card_id = sca.card_id \
             WHERE {} GROUP BY sca.card_id, c.card_name \
             ORDER BY total_uses DESC, name ASC LIMIT ${}",
            clauses.join(" AND "),
            params.len()
        ),
        params,
    )
    .await
}

async fn champion_cards(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path(champion_id): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    ranked_mode(&query)?;
    let champion_id = positive_id(&champion_id, "championId")?;
    let bounds = valid_tier_bounds(&query)?;
    let talent_id = query
        .get("talentId")
        .map(|raw| positive_id(raw, "talentId"))
        .transpose()?;
    let (table, alias, denominator, denominator_alias, count_column) = if talent_id.is_some() {
        (
            "stats_talent_card_aggregate",
            "stca",
            "stats_talent_aggregate",
            "sta",
            "uses",
        )
    } else {
        (
            "stats_card_aggregate",
            "sca",
            "stats_player_aggregate",
            "spa",
            "plays",
        )
    };
    let mut params = vec![QueryParam::Int32(champion_id), QueryParam::Int32(486)];
    let mut clauses = vec![
        format!("{alias}.champion_id = $1"),
        format!("{alias}.queue_id = $2"),
    ];
    let mut denominator_clauses = vec![
        format!("{denominator_alias}.champion_id = $1"),
        format!("{denominator_alias}.queue_id = $2"),
    ];
    if let Some(talent_id) = talent_id {
        params.push(QueryParam::Int32(talent_id));
        clauses.push(format!("{alias}.talent_id = ${}", params.len()));
        denominator_clauses.push(format!("{denominator_alias}.talent_id = ${}", params.len()));
    }
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        clauses.push(format!("{alias}.lobby_tier >= ${}", params.len()));
        denominator_clauses.push(format!(
            "{denominator_alias}.lobby_tier >= ${}",
            params.len()
        ));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        clauses.push(format!("{alias}.lobby_tier <= ${}", params.len()));
        denominator_clauses.push(format!(
            "{denominator_alias}.lobby_tier <= ${}",
            params.len()
        ));
    }
    let raw_join = clauses
        .iter()
        .map(|condition| condition.replace(&format!("{alias}."), "raw."))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!(
        "WITH denominator AS ( \
           SELECT COALESCE(SUM({count_column}), 0)::BIGINT AS total \
             FROM {denominator} {denominator_alias} WHERE {} \
         ), level_rows AS ( \
           SELECT {alias}.card_id, {alias}.card_level, SUM({alias}.uses)::BIGINT AS plays, \
             SUM({alias}.wins)::BIGINT AS wins, SUM({alias}.losses)::BIGINT AS losses, \
             COALESCE(ROUND(100.0 * SUM({alias}.wins)::NUMERIC \
               / NULLIF((SUM({alias}.wins)+SUM({alias}.losses))::NUMERIC,0),2),0) AS \"winRate\" \
           FROM {table} {alias} WHERE {} GROUP BY 1,2 \
         ), card_rows AS ( \
           SELECT c.card_id AS \"cardId\", c.card_name AS \"cardName\", \
             COALESCE(SUM(raw.uses),0)::BIGINT AS \"totalPlays\", \
             COALESCE(SUM(raw.wins),0)::BIGINT AS wins, COALESCE(SUM(raw.losses),0)::BIGINT AS losses, \
             COALESCE(ROUND(100.0*SUM(raw.wins)::NUMERIC \
               / NULLIF((SUM(raw.wins)+SUM(raw.losses))::NUMERIC,0),2),0) AS \"winRate\", \
             COALESCE((SELECT jsonb_agg(jsonb_build_object( \
               'level', lr.card_level, 'plays', lr.plays::TEXT, 'wins', lr.wins::TEXT, \
               'losses', lr.losses::TEXT, 'winRate', lr.\"winRate\"::TEXT) ORDER BY lr.card_level) \
               FROM level_rows lr WHERE lr.card_id=c.card_id),'[]'::jsonb) AS levels \
           FROM cards c LEFT JOIN {table} raw ON raw.card_id=c.card_id AND {raw_join} \
           WHERE c.champion_id=$1 GROUP BY c.card_id,c.card_name \
         ), deduped AS ( \
           SELECT *, ROW_NUMBER() OVER (PARTITION BY regexp_replace(lower(\"cardName\"),'[^a-z0-9]','','g') \
             ORDER BY \"totalPlays\" DESC, \"cardId\") AS duplicate_rank FROM card_rows \
         ) SELECT jsonb_build_object('totalMatches',d.total::TEXT,'talentId',{}, \
             'cards',(SELECT COALESCE(jsonb_agg(jsonb_build_object( \
               'cardId', x.\"cardId\", 'cardName', x.\"cardName\", \
               'totalPlays', x.\"totalPlays\"::TEXT, 'wins', x.wins::TEXT, \
               'losses', x.losses::TEXT, 'winRate', x.\"winRate\"::TEXT, \
               'levels', x.levels) ORDER BY x.\"totalPlays\" DESC,x.\"cardName\"), \
               '[]'::jsonb) FROM deduped x WHERE duplicate_rank=1)) AS payload \
           FROM denominator d",
        denominator_clauses.join(" AND "),
        clauses.join(" AND "),
        talent_id.map_or_else(|| "NULL".to_owned(), |value| value.to_string()),
    );
    cached_payload(state, uri, request_id, sql, params).await
}

async fn card_detail(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Path((champion_id, card_id)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    ranked_mode(&query)?;
    let champion_id = positive_id(&champion_id, "championId")?;
    let card_id = positive_id(&card_id, "cardId")?;
    let bounds = valid_tier_bounds(&query)?;
    let talent_id = query
        .get("talentId")
        .map(|raw| positive_id(raw, "talentId"))
        .transpose()?;
    if !bounds.active() {
        let database = state.database.clone();
        return cached_database_json(
            state.cache,
            stats_cache_key(&uri),
            CACHE_TTL_SECONDS,
            CACHE_TTL_SECONDS * 3,
            &request_id,
            move || {
                let database = database.clone();
                async move {
                    let card = database
                        .query_json_params(
                            "SELECT c.card_id AS \"cardId\",c.card_name AS \"cardName\", \
                               c.champion_id AS \"championId\",ch.name AS \"championName\" \
                             FROM cards c LEFT JOIN champions ch ON ch.id=c.champion_id \
                             WHERE c.champion_id=$1 AND c.card_id=$2",
                            &[QueryParam::Int32(champion_id), QueryParam::Int32(card_id)],
                        )
                        .await?
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| json!({}));
                    let (table, extra, mut projection_params) = if let Some(talent_id) = talent_id {
                        (
                            "talent_card_counts_ranked",
                            " AND talent_id=$2",
                            vec![QueryParam::Int32(card_id), QueryParam::Int32(talent_id)],
                        )
                    } else {
                        (
                            "card_counts_ranked",
                            "",
                            vec![QueryParam::Int32(card_id)],
                        )
                    };
                    let summary = database
                        .query_json_params(
                            &format!(
                                "SELECT COALESCE(SUM(count),0)::INT AS \"totalPlays\", \
                                   COALESCE(SUM(wins),0)::INT AS wins,COALESCE(SUM(losses),0)::INT AS losses, \
                                   COALESCE(ROUND(100.0*SUM(wins)::NUMERIC/NULLIF((SUM(wins)+SUM(losses))::NUMERIC,0),2),0)::DOUBLE PRECISION AS \"winRate\" \
                                 FROM {table} WHERE card_id=$1{extra}"
                            ),
                            &projection_params,
                        )
                        .await?
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| json!({}));
                    let levels = database
                        .query_json_params(
                            &format!(
                                "WITH level_ref AS (SELECT generate_series(1,5)::SMALLINT AS level),raw AS ( \
                                   SELECT card_level AS level,SUM(count)::INT AS plays,SUM(wins)::INT AS wins,SUM(losses)::INT AS losses, \
                                     COALESCE(ROUND(100.0*SUM(wins)::NUMERIC/NULLIF((SUM(wins)+SUM(losses))::NUMERIC,0),2),0)::DOUBLE PRECISION AS \"winRate\" \
                                   FROM {table} WHERE card_id=$1{extra} GROUP BY card_level) \
                                 SELECT level_ref.level,COALESCE(raw.plays,0)::INT AS plays, \
                                   COALESCE(raw.wins,0)::INT AS wins,COALESCE(raw.losses,0)::INT AS losses, \
                                   COALESCE(raw.\"winRate\",0)::DOUBLE PRECISION AS \"winRate\" \
                                 FROM level_ref LEFT JOIN raw USING(level) ORDER BY level_ref.level"
                            ),
                            &projection_params,
                        )
                        .await?;
                    projection_params.clear();
                    let talents = database
                        .query_json_params(
                            "SELECT t.talent_id AS \"talentId\",t.talent_name AS \"talentName\", \
                               COALESCE(raw.count,0)::INT AS \"totalPlays\",COALESCE(raw.wins,0)::INT AS wins, \
                               COALESCE(raw.losses,0)::INT AS losses, \
                               COALESCE(ROUND(100.0*raw.wins::NUMERIC/NULLIF((raw.wins+raw.losses)::NUMERIC,0),2),0)::DOUBLE PRECISION AS \"winRate\" \
                             FROM talents t LEFT JOIN (SELECT talent_id,SUM(count)::INT AS count, \
                               SUM(wins)::INT AS wins,SUM(losses)::INT AS losses \
                               FROM talent_card_counts_ranked WHERE card_id=$2 GROUP BY talent_id) raw \
                               ON raw.talent_id=t.talent_id WHERE t.champion_id=$1 \
                             ORDER BY \"totalPlays\" DESC,t.talent_name",
                            &[QueryParam::Int32(champion_id), QueryParam::Int32(card_id)],
                        )
                        .await?;
                    let mut payload = card;
                    let object = payload.as_object_mut().expect("card payload");
                    for key in ["totalPlays", "wins", "losses", "winRate"] {
                        object.insert(
                            key.to_owned(),
                            summary.get(key).cloned().unwrap_or_else(|| json!(0)),
                        );
                    }
                    object.insert("mode".to_owned(), json!("ranked"));
                    object.insert("talentId".to_owned(), json!(talent_id));
                    object.insert("levels".to_owned(), Value::Array(levels));
                    object.insert("talents".to_owned(), Value::Array(talents));
                    Ok(payload)
                }
            },
        )
        .await;
    }
    let (table, alias) = if talent_id.is_some() {
        ("stats_talent_card_aggregate", "stca")
    } else {
        ("stats_card_aggregate", "sca")
    };
    let mut params = vec![
        QueryParam::Int32(champion_id),
        QueryParam::Int32(card_id),
        QueryParam::Int32(486),
    ];
    let mut clauses = vec![
        format!("{alias}.champion_id=$1"),
        format!("{alias}.card_id=$2"),
        format!("{alias}.queue_id=$3"),
    ];
    if let Some(talent_id) = talent_id {
        params.push(QueryParam::Int32(talent_id));
        clauses.push(format!("{alias}.talent_id=${}", params.len()));
    }
    if let Some(minimum) = bounds.minimum {
        params.push(QueryParam::Int16(minimum));
        clauses.push(format!("{alias}.lobby_tier>=${}", params.len()));
    }
    if let Some(maximum) = bounds.maximum {
        params.push(QueryParam::Int16(maximum));
        clauses.push(format!("{alias}.lobby_tier<=${}", params.len()));
    }
    let sql = format!(
        "WITH reference AS ( \
           SELECT c.card_id AS \"cardId\",c.card_name AS \"cardName\",c.champion_id AS \"championId\", \
             ch.name AS \"championName\" FROM cards c LEFT JOIN champions ch ON ch.id=c.champion_id \
             WHERE c.champion_id=$1 AND c.card_id=$2 \
         ), summary AS ( \
           SELECT COALESCE(SUM({alias}.uses),0)::BIGINT AS \"totalPlays\", \
             COALESCE(SUM({alias}.wins),0)::BIGINT AS wins,COALESCE(SUM({alias}.losses),0)::BIGINT AS losses, \
             COALESCE(ROUND(100.0*SUM({alias}.wins)::NUMERIC \
               / NULLIF((SUM({alias}.wins)+SUM({alias}.losses))::NUMERIC,0),2),0) AS \"winRate\" \
             FROM {table} {alias} WHERE {} \
         ), levels AS ( \
           SELECT level_ref.level,COALESCE(raw.plays,0)::BIGINT AS plays, \
             COALESCE(raw.wins,0)::BIGINT AS wins,COALESCE(raw.losses,0)::BIGINT AS losses, \
             COALESCE(raw.win_rate,0) AS \"winRate\" FROM generate_series(1,5) level_ref(level) \
           LEFT JOIN (SELECT {alias}.card_level AS level,SUM({alias}.uses)::BIGINT AS plays, \
             SUM({alias}.wins)::BIGINT AS wins,SUM({alias}.losses)::BIGINT AS losses, \
             ROUND(100.0*SUM({alias}.wins)::NUMERIC \
               / NULLIF((SUM({alias}.wins)+SUM({alias}.losses))::NUMERIC,0),2) AS win_rate \
             FROM {table} {alias} WHERE {} GROUP BY {alias}.card_level) raw USING(level) \
         ), talent_rows AS ( \
           SELECT t.talent_id AS \"talentId\",t.talent_name AS \"talentName\", \
             COALESCE(SUM(stca.uses),0)::BIGINT AS \"totalPlays\", \
             COALESCE(SUM(stca.wins),0)::BIGINT AS wins,COALESCE(SUM(stca.losses),0)::BIGINT AS losses, \
             COALESCE(ROUND(100.0*SUM(stca.wins)::NUMERIC \
               / NULLIF((SUM(stca.wins)+SUM(stca.losses))::NUMERIC,0),2),0) AS \"winRate\" \
           FROM talents t LEFT JOIN stats_talent_card_aggregate stca \
             ON stca.talent_id=t.talent_id AND stca.card_id=$2 AND stca.champion_id=$1 AND stca.queue_id=$3 \
           WHERE t.champion_id=$1 GROUP BY t.talent_id,t.talent_name \
         ) SELECT to_jsonb(r)||to_jsonb(s)||jsonb_build_object('mode','ranked','talentId',{}, \
             'levels',(SELECT jsonb_agg(to_jsonb(l) ORDER BY l.level) FROM levels l), \
             'talents',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.\"totalPlays\" DESC,t.\"talentName\") FROM talent_rows t)) AS payload \
           FROM reference r CROSS JOIN summary s",
        clauses.join(" AND "),
        clauses.join(" AND "),
        talent_id.map_or_else(|| "NULL".to_owned(), |value| value.to_string()),
    );
    cached_payload(state, uri, request_id, sql, params).await
}

async fn baselines(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    if query
        .get("queueId")
        .is_some_and(|value| value.parse::<i32>().ok() != Some(486))
    {
        return Err(ApiError::validation(
            "Only ranked queue 486 is available for aggregate statistics.",
        ));
    }
    let role = query
        .get("role")
        .map(|raw| {
            normalize_item_role(raw).ok_or_else(|| {
                ApiError::validation("Invalid role. Use damage, flank, support, or frontline.")
            })
        })
        .transpose()?;
    let bounds = valid_tier_bounds(&query)?;
    if !bounds.active() {
        let mut params = vec![QueryParam::Int32(486)];
        let mut clauses = vec!["b.queue_id=$1".to_owned()];
        if let Some(role) = role {
            params.push(QueryParam::Text(role.to_owned()));
            clauses.push(format!("b.role_name=${}", params.len()));
        }
        return cached_rows(
            state,
            uri,
            request_id,
            CACHE_TTL_SECONDS,
            format!(
                "SELECT b.role_id,b.role_name AS role,b.queue_id, \
                   b.avg_gpm,b.p10_gpm,b.p25_gpm,b.p75_gpm,b.p90_gpm,b.max_gpm, \
                   b.avg_dpm,b.p10_dpm,b.p25_dpm,b.p75_dpm,b.p90_dpm,b.max_dpm, \
                   b.avg_hpm,b.p10_hpm,b.p25_hpm,b.p75_hpm,b.p90_hpm,b.max_hpm, \
                   b.avg_shpm,b.p10_shpm,b.p25_shpm,b.p75_shpm,b.p90_shpm,b.max_shpm, \
                   b.avg_kda,b.p10_kda,b.p25_kda,b.p75_kda,b.p90_kda,b.max_kda, \
                   b.avg_egpm,b.p10_egpm,b.p25_egpm,b.p75_egpm,b.p90_egpm,b.max_egpm, \
                   b.sample_size,b.updated_at FROM baselines b WHERE {} ORDER BY b.queue_id,b.role_id",
                clauses.join(" AND ")
            ),
            params,
        )
        .await;
    }

    let mut params = vec![QueryParam::Int32(486)];
    let mut clauses = vec!["smh.queue_id=$1".to_owned()];
    if let Some(role) = role {
        let role_id = match role {
            "Damage" => 1,
            "Flank" => 2,
            "Support" => 3,
            "Frontline" => 4,
            _ => 0,
        };
        params.push(QueryParam::Int16(role_id));
        clauses.push(format!("smh.role_id=${}", params.len()));
    }
    append_tier_predicates(bounds, &mut params, &mut clauses, "smh");
    cached_rows(
        state,
        uri,
        request_id,
        CACHE_TTL_SECONDS,
        format!(
            "WITH grouped AS ( \
               SELECT smh.role_id,smh.metric,smh.value,SUM(smh.sample_count)::BIGINT AS weight \
                 FROM stats_metric_histogram smh WHERE {} GROUP BY smh.role_id,smh.metric,smh.value \
             ), ranked AS ( \
               SELECT *,SUM(weight) OVER (PARTITION BY role_id,metric ORDER BY value) AS cumulative, \
                 SUM(weight) OVER (PARTITION BY role_id,metric) AS samples \
                 FROM grouped \
             ), metrics AS ( \
               SELECT role_id,metric,SUM(value*weight)/NULLIF(SUM(weight),0) AS mean,MAX(value) AS maximum, \
                 MIN(value) FILTER (WHERE cumulative>=samples*.10) AS p10, \
                 MIN(value) FILTER (WHERE cumulative>=samples*.25) AS p25, \
                 MIN(value) FILTER (WHERE cumulative>=samples*.75) AS p75, \
                 MIN(value) FILTER (WHERE cumulative>=samples*.90) AS p90,MAX(samples) AS sample_size \
                 FROM ranked GROUP BY role_id,metric \
             ) SELECT role_id,CASE role_id WHEN 1 THEN 'Damage' WHEN 2 THEN 'Flank' \
                 WHEN 3 THEN 'Support' WHEN 4 THEN 'Frontline' ELSE 'Global' END AS role,486 AS queue_id, \
                 MAX(mean) FILTER(WHERE metric='gpm') AS avg_gpm,MAX(p10) FILTER(WHERE metric='gpm') AS p10_gpm,MAX(p25) FILTER(WHERE metric='gpm') AS p25_gpm,MAX(p75) FILTER(WHERE metric='gpm') AS p75_gpm,MAX(p90) FILTER(WHERE metric='gpm') AS p90_gpm,MAX(maximum) FILTER(WHERE metric='gpm') AS max_gpm, \
                 MAX(mean) FILTER(WHERE metric='dpm') AS avg_dpm,MAX(p10) FILTER(WHERE metric='dpm') AS p10_dpm,MAX(p25) FILTER(WHERE metric='dpm') AS p25_dpm,MAX(p75) FILTER(WHERE metric='dpm') AS p75_dpm,MAX(p90) FILTER(WHERE metric='dpm') AS p90_dpm,MAX(maximum) FILTER(WHERE metric='dpm') AS max_dpm, \
                 MAX(mean) FILTER(WHERE metric='hpm') AS avg_hpm,MAX(p10) FILTER(WHERE metric='hpm') AS p10_hpm,MAX(p25) FILTER(WHERE metric='hpm') AS p25_hpm,MAX(p75) FILTER(WHERE metric='hpm') AS p75_hpm,MAX(p90) FILTER(WHERE metric='hpm') AS p90_hpm,MAX(maximum) FILTER(WHERE metric='hpm') AS max_hpm, \
                 MAX(mean) FILTER(WHERE metric='mpm') AS avg_shpm,MAX(p10) FILTER(WHERE metric='mpm') AS p10_shpm,MAX(p25) FILTER(WHERE metric='mpm') AS p25_shpm,MAX(p75) FILTER(WHERE metric='mpm') AS p75_shpm,MAX(p90) FILTER(WHERE metric='mpm') AS p90_shpm,MAX(maximum) FILTER(WHERE metric='mpm') AS max_shpm, \
                 MAX(mean) FILTER(WHERE metric='kda') AS avg_kda,MAX(p10) FILTER(WHERE metric='kda') AS p10_kda,MAX(p25) FILTER(WHERE metric='kda') AS p25_kda,MAX(p75) FILTER(WHERE metric='kda') AS p75_kda,MAX(p90) FILTER(WHERE metric='kda') AS p90_kda,MAX(maximum) FILTER(WHERE metric='kda') AS max_kda, \
                 MAX(mean) FILTER(WHERE metric='egpm') AS avg_egpm,MAX(p10) FILTER(WHERE metric='egpm') AS p10_egpm,MAX(p25) FILTER(WHERE metric='egpm') AS p25_egpm,MAX(p75) FILTER(WHERE metric='egpm') AS p75_egpm,MAX(p90) FILTER(WHERE metric='egpm') AS p90_egpm,MAX(maximum) FILTER(WHERE metric='egpm') AS max_egpm, \
                 MAX(sample_size) AS sample_size,now() AS updated_at FROM metrics GROUP BY role_id ORDER BY role_id",
            clauses.join(" AND ")
        ),
        params,
    )
    .await
}

async fn cached_payload(
    state: StatsState,
    uri: axum::http::Uri,
    request_id: RequestId,
    sql: String,
    params: Vec<QueryParam>,
) -> Result<Response, ApiError> {
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            let sql = sql.clone();
            let params = params.clone();
            async move {
                let row = database
                    .query_json_params(&sql, &params)
                    .await?
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| json!({ "payload": null }));
                Ok(row.get("payload").cloned().unwrap_or(Value::Null))
            }
        },
    )
    .await
}

async fn cached_wrapped_rows(
    state: StatsState,
    uri: axum::http::Uri,
    request_id: RequestId,
    sql: String,
    params: Vec<QueryParam>,
    tier_min: Option<i16>,
    tier_max: Option<i16>,
) -> Result<Response, ApiError> {
    let database = state.database.clone();
    cached_database_json(
        state.cache,
        stats_cache_key(&uri),
        CACHE_TTL_SECONDS,
        CACHE_TTL_SECONDS * 3,
        &request_id,
        move || {
            let database = database.clone();
            let sql = sql.clone();
            let params = params.clone();
            async move {
                let rows = database.query_json_params(&sql, &params).await?;
                Ok(json!({
                    "total": rows.len(),
                    "data": rows,
                    "tier_min": tier_min,
                    "tier_max": tier_max
                }))
            }
        },
    )
    .await
}
