use std::collections::HashMap;

use axum::{
    Router,
    extract::{Extension, Query, State},
    response::Response,
    routing::get,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use paladinscat_core::database::{QueryParam, format_json_timestamp};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use url::form_urlencoded;

use crate::{
    error::ApiError,
    request::{EffectiveUri, RequestId},
    route_cache::cached_database_json,
};

use super::{StatsState, stats_cache_key};

const CACHE_TTL_SECONDS: u64 = 60;
const ACTIVITY_STALE_TTL_SECONDS: u64 = 6 * 60 * 60;
const CANONICAL_REGION_SQL: &str = r#"CASE LOWER(BTRIM(COALESCE(region,'')))
  WHEN 'north america' THEN 'NA' WHEN 'na' THEN 'NA'
  WHEN 'europe' THEN 'EU' WHEN 'eu' THEN 'EU'
  WHEN 'brazil' THEN 'BR' WHEN 'br' THEN 'BR'
  WHEN 'south america' THEN 'SA' WHEN 'sa' THEN 'SA'
  WHEN 'southeast asia' THEN 'SEA' WHEN 'sea' THEN 'SEA'
  WHEN 'australia' THEN 'OCE' WHEN 'oceania' THEN 'OCE' WHEN 'oce' THEN 'OCE'
  WHEN 'japan' THEN 'JPN' WHEN 'jpn' THEN 'JPN'
  WHEN 'russia' THEN 'RUS' WHEN 'rus' THEN 'RUS'
  WHEN 'asia' THEN 'ASIA'
  ELSE COALESCE(NULLIF(BTRIM(region),''),'Unknown') END"#;
const EVIDENCE_CTES: &str = r#"
recent_discoveries AS MATERIALIZED (
  SELECT d.match_id,d.queue_id,q.queue_name,q.stats_scope,q.participant_model,d.region AS observed_region,
    (COALESCE(d.entry_datetime AT TIME ZONE 'UTC',
      d.source_date + (d.source_hour * interval '1 hour')) AT TIME ZONE 'UTC') AS observed_at
  FROM match_count_discoveries d JOIN queue_types q ON q.queue_id=d.queue_id
  WHERE d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date
    AND COALESCE(d.entry_datetime AT TIME ZONE 'UTC',
      d.source_date + (d.source_hour * interval '1 hour'))
      >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
    AND q.track_presence=TRUE AND __QUEUE_FILTER__
),
roster_evidence AS MATERIALIZED (
  SELECT mp.player_id,mp.match_id,discovery.queue_id,discovery.queue_name,
    discovery.stats_scope,discovery.observed_at,discovery.observed_region,NULLIF(BTRIM(mp.player_name),'') AS observed_name,
    CASE WHEN mp.player_id>0 THEN 'human'
      WHEN UPPER(COALESCE(mp.player_name,''))='PRIVATEACCOUNT' OR COALESCE(mp.private_slot,0)>0
        THEN 'private' ELSE 'unknown' END AS participant_kind
  FROM recent_discoveries discovery JOIN match_players mp ON mp.match_id=discovery.match_id
  WHERE mp.entry_datetime>=now()-interval '25 hours'
    AND COALESCE(mp.source,'direct') IN ('direct','recovered')
  UNION ALL
  SELECT cmp.player_id,cmp.match_id,discovery.queue_id,discovery.queue_name,
    discovery.stats_scope,discovery.observed_at,discovery.observed_region,NULLIF(BTRIM(cmp.player_name),''),
    COALESCE(cmp.participant_kind,'human')
  FROM recent_discoveries discovery JOIN casual_match_players cmp ON cmp.match_id=discovery.match_id
  UNION ALL
  SELECT smp.player_id,smp.match_id,discovery.queue_id,discovery.queue_name,
    discovery.stats_scope,discovery.observed_at,discovery.observed_region,NULLIF(BTRIM(smp.player_name),''),
    COALESCE(smp.participant_kind,'human')
  FROM recent_discoveries discovery JOIN special_match_players smp ON smp.match_id=discovery.match_id
),
participation AS MATERIALIZED (
  SELECT player_id,match_id,queue_id,queue_name,stats_scope,observed_at,observed_region,observed_name
  FROM roster_evidence WHERE player_id>0 AND participant_kind='human'
),
latest_observed_region AS MATERIALIZED (
  SELECT DISTINCT ON (player_id) player_id,NULLIF(BTRIM(observed_region),'') AS region
  FROM participation WHERE NULLIF(BTRIM(observed_region),'') IS NOT NULL
    AND UPPER(BTRIM(observed_region))<>'UNKNOWN'
  ORDER BY player_id,observed_at DESC,match_id DESC
),
roster_summary AS MATERIALIZED (
  SELECT match_id,
    COUNT(DISTINCT player_id) FILTER(WHERE player_id>0 AND participant_kind='human')::int AS known_public_players,
    COUNT(*) FILTER(WHERE player_id<=0 AND participant_kind<>'bot')::int AS observed_unresolved_slots,
    COUNT(*) FILTER(WHERE participant_kind<>'bot')::int AS observed_human_slots
  FROM roster_evidence GROUP BY match_id
),
durable_fact_candidates AS MATERIALIZED (
  SELECT discovery.match_id,COALESCE(status.status='complete',false) AS facts_complete
  FROM recent_discoveries discovery LEFT JOIN match_ingest_status status ON status.match_id=discovery.match_id
  WHERE discovery.queue_id=486
  UNION ALL
  SELECT discovery.match_id,acquisition.status='complete_direct'
  FROM recent_discoveries discovery JOIN nonranked_match_acquisition acquisition
    ON acquisition.match_id=discovery.match_id WHERE discovery.queue_id<>486
),
fact_completeness AS MATERIALIZED (
  SELECT match_id,BOOL_OR(facts_complete) AS facts_complete FROM durable_fact_candidates GROUP BY match_id
),
match_uncertainty AS MATERIALIZED (
  SELECT discovery.match_id,discovery.queue_id,discovery.queue_name,discovery.stats_scope,
    CASE WHEN discovery.participant_model IN ('bots','pve')
      THEN COALESCE(roster.observed_unresolved_slots,0)
      WHEN COALESCE(completeness.facts_complete,false) AND COALESCE(roster.observed_human_slots,0)>0
      THEN COALESCE(roster.observed_unresolved_slots,0)
      ELSE GREATEST(10-COALESCE(roster.known_public_players,0),
        COALESCE(roster.observed_unresolved_slots,0),0) END::int AS unresolved_slots_upper
  FROM recent_discoveries discovery LEFT JOIN roster_summary roster ON roster.match_id=discovery.match_id
  LEFT JOIN fact_completeness completeness ON completeness.match_id=discovery.match_id
)"#;

pub(super) fn router() -> Router<StatsState> {
    Router::new()
        .route("/stats/presence", get(presence))
        .route("/stats/presence/match-ids", get(match_ids))
        .route("/stats/presence/players", get(players))
        .route("/stats/presence/details", get(details))
}

fn queue_id(query: &HashMap<String, String>) -> Result<Option<i32>, ApiError> {
    query
        .get("queue_id")
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<i32>()
                .ok()
                .filter(|value| *value >= 0)
                .ok_or_else(|| ApiError::validation("queue_id must be a valid PostgreSQL integer."))
        })
        .transpose()
}

fn page(query: &HashMap<String, String>) -> i32 {
    query
        .get("page")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .map_or(1, |value| (value.trunc() as i32).clamp(1, 1_000_000))
}

fn evidence_limit(query: &HashMap<String, String>) -> i32 {
    query
        .get("per_page")
        .or_else(|| query.get("limit"))
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .map_or(250, |value| (value.trunc() as i32).clamp(50, 500))
}

fn detail_limit(query: &HashMap<String, String>) -> i32 {
    query
        .get("limit")
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .map_or(25, |value| (value.trunc() as i32).clamp(10, 50))
}

fn ctes(queue_id: Option<i32>, params: &mut Vec<QueryParam>) -> String {
    let predicate = queue_id.map_or_else(
        || "TRUE".to_owned(),
        |queue_id| {
            params.push(QueryParam::Int32(queue_id));
            format!("d.queue_id=${}", params.len())
        },
    );
    EVIDENCE_CTES.replace("__QUEUE_FILTER__", &predicate)
}

async fn presence(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
) -> Result<Response, ApiError> {
    let sql = format!(
        "WITH {}, public_ids AS MATERIALIZED (SELECT DISTINCT player_id FROM participation), \
         resolved AS MATERIALIZED ( \
           SELECT identity.player_id,resolved_profile.platform, \
             COALESCE(CASE WHEN LOWER(BTRIM(COALESCE(resolved_profile.region,''))) IN \
               ('','unknown','latin america north','latam north','latin america south','latam south') \
               THEN NULL ELSE resolved_profile.region END,observed_profile.region) AS region, \
             resolved_profile.hirez_profile_refreshed_at \
           FROM public_ids identity LEFT JOIN LATERAL ( \
             SELECT candidate.platform,candidate.region,candidate.hirez_profile_refreshed_at \
             FROM (SELECT p.platform,p.region,p.hirez_profile_refreshed_at,0 AS priority \
               FROM players p WHERE p.id=identity.player_id UNION ALL \
               SELECT p.platform,p.region,p.hirez_profile_refreshed_at,1 \
               FROM players p WHERE p.active_player_id=identity.player_id \
                 AND p.active_player_id>0 AND p.id<>identity.player_id) candidate \
             ORDER BY priority,hirez_profile_refreshed_at DESC NULLS LAST LIMIT 1 \
           ) resolved_profile ON TRUE LEFT JOIN latest_observed_region observed_profile \
             ON observed_profile.player_id=identity.player_id \
         ) SELECT jsonb_build_object( \
           'window_hours',24,'observed_at',now(), \
           'public_players',(SELECT COUNT(*) FROM public_ids), \
           'unresolved_player_slots_lower',0, \
           'unresolved_player_slots_upper',(SELECT COALESCE(SUM(unresolved_slots_upper),0) FROM match_uncertainty), \
           'unresolved_matches',(SELECT COUNT(*) FROM match_uncertainty WHERE unresolved_slots_upper>0), \
           'public_players_lower_bound',(SELECT COUNT(*) FROM public_ids), \
           'public_players_upper_bound',(SELECT COUNT(*) FROM public_ids)+(SELECT COALESCE(SUM(unresolved_slots_upper),0) FROM match_uncertainty), \
           'private_players',(SELECT COUNT(*) FROM private_player_presence_24h WHERE last_observed_at>=now()-interval '24 hours'), \
           'unresolved_private_observations',(SELECT COUNT(*) FROM unresolved_private_presence WHERE observed_at>=now()-interval '24 hours'), \
           'public_by_scope',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY stats_scope),'[]'::jsonb) FROM \
             (SELECT stats_scope,COUNT(DISTINCT player_id)::int AS players FROM participation GROUP BY stats_scope)x), \
           'private_by_scope',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY stats_scope),'[]'::jsonb) FROM \
             (SELECT last_stats_scope AS stats_scope,COUNT(*)::int AS players FROM private_player_presence_24h \
               WHERE last_observed_at>=now()-interval '24 hours' GROUP BY last_stats_scope)x), \
           'unresolved_by_scope',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY stats_scope),'[]'::jsonb) FROM \
             (SELECT stats_scope,COUNT(*)::int AS observations FROM unresolved_private_presence \
               WHERE observed_at>=now()-interval '24 hours' GROUP BY stats_scope)x), \
           'public_by_queue',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY players DESC,queue_id),'[]'::jsonb) FROM \
             (SELECT queue_id,MAX(queue_name) AS queue_name,MAX(stats_scope) AS stats_scope, \
               COUNT(DISTINCT player_id)::int AS players FROM participation GROUP BY queue_id)x), \
           'public_by_platform',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY players DESC,platform),'[]'::jsonb) FROM \
             (SELECT COALESCE(NULLIF(BTRIM(platform),''),'Unknown') AS platform,COUNT(*)::int AS players FROM resolved GROUP BY 1)x), \
           'public_by_region',(SELECT COALESCE(jsonb_agg(to_jsonb(x) ORDER BY players DESC,region),'[]'::jsonb) FROM \
             (SELECT {canonical_region} AS region,COUNT(*)::int AS players FROM resolved GROUP BY 1)x), \
           'profile_coverage',jsonb_build_object('total',(SELECT COUNT(*) FROM resolved), \
             'fresh',(SELECT COUNT(*) FROM resolved WHERE hirez_profile_refreshed_at>=now()-interval '24 hours'), \
             'platform_known',(SELECT COUNT(*) FROM resolved WHERE NULLIF(BTRIM(platform),'') IS NOT NULL), \
             'platform_unknown',(SELECT COUNT(*) FROM resolved WHERE NULLIF(BTRIM(platform),'') IS NULL), \
             'last_enrichment_at',(SELECT MAX(last_attempt_at)::text FROM player_activity_profile_refresh)) \
         ) AS payload",
        EVIDENCE_CTES.replace("__QUEUE_FILTER__", "TRUE"),
        canonical_region = CANONICAL_REGION_SQL
    );
    cached_payload(state, uri, request_id, sql, Vec::new()).await
}

async fn match_ids(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let queue_id = queue_id(&query)?;
    let page = page(&query);
    let limit = evidence_limit(&query);
    let offset = (page - 1).saturating_mul(limit);
    let mut params = Vec::new();
    let filter = queue_id.map_or_else(
        || "TRUE".to_owned(),
        |queue_id| {
            params.push(QueryParam::Int32(queue_id));
            format!("d.queue_id=${}", params.len())
        },
    );
    params.push(QueryParam::Int64(i64::from(limit)));
    let limit_param = params.len();
    params.push(QueryParam::Int64(i64::from(offset)));
    let offset_param = params.len();
    let sql = format!(
        "WITH recent AS MATERIALIZED ( \
           SELECT d.match_id,d.queue_id,d.source_date,d.source_hour,q.queue_name \
           FROM match_count_discoveries d JOIN queue_types q ON q.queue_id=d.queue_id \
           WHERE d.source_date>=((now() AT TIME ZONE 'UTC')-interval '25 hours')::date \
             AND COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+d.source_hour*interval '1 hour') \
               >=(now() AT TIME ZONE 'UTC')-interval '24 hours' AND q.track_presence=TRUE AND {filter} \
         ), queue_rows AS ( \
           SELECT d.queue_id,q.queue_name,COUNT(*)::int AS matches FROM match_count_discoveries d \
           JOIN queue_types q ON q.queue_id=d.queue_id \
           WHERE d.source_date>=((now() AT TIME ZONE 'UTC')-interval '25 hours')::date \
             AND COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+d.source_hour*interval '1 hour') \
               >=(now() AT TIME ZONE 'UTC')-interval '24 hours' AND q.track_presence=TRUE \
           GROUP BY d.queue_id,q.queue_name \
         ), page_rows AS (SELECT match_id::text,queue_id FROM recent \
           ORDER BY source_date DESC,source_hour DESC,match_id DESC,queue_id DESC \
           LIMIT ${limit_param} OFFSET ${offset_param}) \
         SELECT jsonb_build_object('window_hours',24,'observed_at',now(), \
           'total_matches',(SELECT COUNT(*) FROM recent),'selected_queue_id',{}, \
           'page',jsonb_build_object('current',{page},'size',{limit}, \
             'total_pages',CEIL((SELECT COUNT(*) FROM recent)::numeric/{limit})), \
           'queues',(SELECT COALESCE(jsonb_agg(to_jsonb(q) ORDER BY matches DESC,queue_id),'[]'::jsonb) FROM queue_rows q), \
           'match_ids',(SELECT COALESCE(jsonb_agg(to_jsonb(p)),'[]'::jsonb) FROM page_rows p)) AS payload",
        queue_id.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
    );
    cached_payload(state, uri, request_id, sql, params).await
}

async fn players(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let queue_id = queue_id(&query)?;
    let page = page(&query);
    let limit = evidence_limit(&query);
    let offset = (page - 1).saturating_mul(limit);
    let sort = if query
        .get("sort")
        .is_some_and(|value| value == "alphabetical")
    {
        "alphabetical"
    } else {
        "matches"
    };
    let order = if sort == "alphabetical" {
        "LOWER(player_name),player_id"
    } else {
        "matches_played DESC,player_id"
    };
    let mut params = Vec::new();
    let evidence = ctes(queue_id, &mut params);
    params.push(QueryParam::Int64(i64::from(limit)));
    let limit_param = params.len();
    params.push(QueryParam::Int64(i64::from(offset)));
    let offset_param = params.len();
    let sql = format!(
        "WITH {evidence}, counts AS MATERIALIZED ( \
           SELECT player_id,COUNT(DISTINCT match_id)::int AS matches_played,MAX(observed_name) AS observed_name \
           FROM participation GROUP BY player_id \
         ), player_rows AS MATERIALIZED ( \
           SELECT counts.player_id::text AS player_id, \
             COALESCE(NULLIF(BTRIM(profile.name),''),counts.observed_name,'Player #'||counts.player_id::text) AS player_name, \
             counts.matches_played FROM counts LEFT JOIN LATERAL ( \
               SELECT candidate.name FROM (SELECT p.name,p.hirez_profile_refreshed_at,0 AS priority \
                 FROM players p WHERE p.id=counts.player_id UNION ALL \
                 SELECT p.name,p.hirez_profile_refreshed_at,1 FROM players p \
                 WHERE p.active_player_id=counts.player_id AND p.active_player_id>0 AND p.id<>counts.player_id) candidate \
               ORDER BY priority,hirez_profile_refreshed_at DESC NULLS LAST LIMIT 1) profile ON TRUE \
         ), paged AS (SELECT * FROM player_rows ORDER BY {order} LIMIT ${limit_param} OFFSET ${offset_param}) \
         SELECT jsonb_build_object('window_hours',24,'observed_at',now(), \
           'total_players',(SELECT COUNT(*) FROM player_rows), \
           'unresolved_player_slots_lower',0, \
           'unresolved_player_slots_upper',(SELECT COALESCE(SUM(unresolved_slots_upper),0) FROM match_uncertainty), \
           'unresolved_matches',(SELECT COUNT(*) FROM match_uncertainty WHERE unresolved_slots_upper>0), \
           'public_players_lower_bound',(SELECT COUNT(*) FROM player_rows), \
           'public_players_upper_bound',(SELECT COUNT(*) FROM player_rows)+(SELECT COALESCE(SUM(unresolved_slots_upper),0) FROM match_uncertainty), \
           'total_matches',(SELECT COUNT(*) FROM recent_discoveries), \
           'represented_matches',(SELECT COUNT(DISTINCT match_id) FROM participation), \
           'unrepresented_matches',GREATEST((SELECT COUNT(*) FROM recent_discoveries)-(SELECT COUNT(DISTINCT match_id) FROM participation),0), \
           'total_participations',(SELECT COALESCE(SUM(matches_played),0) FROM counts), \
           'selected_queue_id',{},'sort','{sort}', \
           'page',jsonb_build_object('current',{page},'size',{limit}, \
             'total_pages',CEIL((SELECT COUNT(*) FROM player_rows)::numeric/{limit})), \
           'players',(SELECT COALESCE(jsonb_agg(to_jsonb(p) ORDER BY {order}),'[]'::jsonb) FROM paged p)) AS payload",
        queue_id.map_or_else(|| "NULL".to_owned(), |value| value.to_string())
    );
    cached_payload(state, uri, request_id, sql, params).await
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Cursor {
    date: String,
    hour: i32,
    match_id: String,
    queue_id: i32,
}

fn decode_cursor(raw: &str) -> Option<Cursor> {
    let bytes = URL_SAFE_NO_PAD.decode(raw).ok()?;
    let cursor: Cursor = serde_json::from_slice(&bytes).ok()?;
    if cursor.date.len() != 10
        || cursor.hour < 0
        || cursor.hour > 23
        || cursor
            .match_id
            .parse::<i64>()
            .ok()
            .filter(|value| *value >= 0)
            .is_none()
        || cursor.queue_id < 0
    {
        return None;
    }
    Some(cursor)
}

fn encode_cursor(row: &Value) -> Option<String> {
    let cursor = Cursor {
        date: row.get("source_date")?.as_str()?.chars().take(10).collect(),
        hour: row.get("source_hour")?.as_i64()? as i32,
        match_id: row.get("match_id")?.as_str()?.to_owned(),
        queue_id: row.get("queue_id")?.as_i64()? as i32,
    };
    serde_json::to_vec(&cursor)
        .ok()
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
}

async fn details(
    State(state): State<StatsState>,
    Extension(request_id): Extension<RequestId>,
    Extension(EffectiveUri(uri)): Extension<EffectiveUri>,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let queue_id = queue_id(&query)?;
    let limit = detail_limit(&query);
    let cursor = query
        .get("cursor")
        .filter(|value| !value.is_empty())
        .map(|value| {
            decode_cursor(value)
                .ok_or_else(|| ApiError::validation("Invalid presence-detail cursor."))
        })
        .transpose()?;
    let mut params = Vec::new();
    let mut predicates = vec![
        "d.source_date>=((now() AT TIME ZONE 'UTC')-interval '25 hours')::date".to_owned(),
        "COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+d.source_hour*interval '1 hour') >= (now() AT TIME ZONE 'UTC')-interval '24 hours'".to_owned(),
        "q.track_presence=TRUE".to_owned(),
    ];
    if let Some(queue_id) = queue_id {
        params.push(QueryParam::Int32(queue_id));
        predicates.push(format!("d.queue_id=${}", params.len()));
    }
    if let Some(cursor) = cursor {
        params.push(QueryParam::Text(cursor.date));
        let date = params.len();
        params.push(QueryParam::Int32(cursor.hour));
        let hour = params.len();
        params.push(QueryParam::Int64(
            cursor.match_id.parse().expect("validated match cursor"),
        ));
        let match_id = params.len();
        params.push(QueryParam::Int32(cursor.queue_id));
        let queue_id = params.len();
        predicates.push(format!(
            "(d.source_date,d.source_hour,d.match_id,d.queue_id)<(((${date}::text)::date),${hour},${match_id},${queue_id})"
        ));
    }
    params.push(QueryParam::Int64(i64::from(limit + 1)));
    let page_limit = params.len();
    let matches_sql = format!(
        "WITH page AS MATERIALIZED ( \
           SELECT d.match_id,d.queue_id,d.region,d.entry_datetime,d.source_date,d.source_hour,q.queue_name,q.stats_scope \
           FROM match_count_discoveries d JOIN queue_types q ON q.queue_id=d.queue_id \
           WHERE {} ORDER BY d.source_date DESC,d.source_hour DESC,d.match_id DESC,d.queue_id DESC LIMIT ${page_limit} \
         ) SELECT page.match_id::text,page.queue_id,page.queue_name,page.stats_scope,page.source_date::text,page.source_hour, \
           COALESCE(page.entry_datetime,ranked.entry_datetime,casual.entry_datetime,special.entry_datetime, \
             page.source_date+page.source_hour*interval '1 hour')::text AS entry_datetime, \
           COALESCE(NULLIF(ranked.region,''),NULLIF(casual.region,''),NULLIF(special.region,''),page.region,'Unknown') AS region, \
           COALESCE(NULLIF(ranked.map,''),NULLIF(casual.map,''),NULLIF(special.map,''),'Unknown') AS map, \
           CASE WHEN page.queue_id=486 AND ranked.match_id IS NULL THEN 'discovered' \
             WHEN page.queue_id=486 AND ranked.limited THEN 'limited' WHEN page.queue_id=486 AND ranked.recovered THEN 'recovered' \
             WHEN page.queue_id=486 AND ranked.broken THEN 'broken' WHEN page.queue_id=486 THEN 'complete' \
             ELSE COALESCE(acquisition.status,'discovered') END AS status, \
           CASE WHEN page.queue_id=486 AND ranked.limited THEN 'limited' \
             WHEN page.queue_id=486 AND ranked.broken AND NOT ranked.recovered THEN 'partial' \
             WHEN page.queue_id=486 AND ranked.match_id IS NOT NULL THEN 'complete' \
             ELSE COALESCE(acquisition.quality,casual.quality,special.quality,'unknown') END AS quality, \
           COALESCE(acquisition.terminal_reason,ranked.limited_reason) AS terminal_reason \
         FROM page LEFT JOIN nonranked_match_acquisition acquisition ON acquisition.match_id=page.match_id \
         LEFT JOIN casual_matches casual ON casual.match_id=page.match_id \
         LEFT JOIN special_matches special ON special.match_id=page.match_id \
         LEFT JOIN LATERAL (SELECT m.* FROM matches m WHERE m.match_id=page.match_id \
           AND m.entry_datetime>=now()-interval '25 hours' ORDER BY m.entry_datetime DESC LIMIT 1) ranked ON TRUE \
         ORDER BY page.source_date DESC,page.source_hour DESC,page.match_id DESC,page.queue_id DESC",
        predicates.join(" AND ")
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
            let matches_sql = matches_sql.clone();
            let params = params.clone();
            async move {
                let total_sql = if queue_id.is_some() {
                    "SELECT COUNT(*)::INT AS total FROM match_count_discoveries d \
                     JOIN queue_types q ON q.queue_id=d.queue_id \
                     WHERE d.source_date>=((now() AT TIME ZONE 'UTC')-interval '25 hours')::date \
                       AND COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+d.source_hour*interval '1 hour') \
                         >=(now() AT TIME ZONE 'UTC')-interval '24 hours' \
                       AND q.track_presence=TRUE AND d.queue_id=$1"
                } else {
                    "SELECT COUNT(*)::INT AS total FROM match_count_discoveries d \
                     JOIN queue_types q ON q.queue_id=d.queue_id \
                     WHERE d.source_date>=((now() AT TIME ZONE 'UTC')-interval '25 hours')::date \
                       AND COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+d.source_hour*interval '1 hour') \
                         >=(now() AT TIME ZONE 'UTC')-interval '24 hours' \
                       AND q.track_presence=TRUE"
                };
                let total_params = queue_id
                    .map(|value| vec![QueryParam::Int32(value)])
                    .unwrap_or_default();
                let total_future = database.one_json_params(total_sql, &total_params);
                let queues_future = database.query_json(
                        "SELECT d.queue_id,q.queue_name,q.stats_scope,COUNT(*)::INT AS matches \
                         FROM match_count_discoveries d JOIN queue_types q ON q.queue_id=d.queue_id \
                         WHERE d.source_date>=((now() AT TIME ZONE 'UTC')-interval '25 hours')::date \
                           AND COALESCE(d.entry_datetime AT TIME ZONE 'UTC',d.source_date+d.source_hour*interval '1 hour') \
                             >=(now() AT TIME ZONE 'UTC')-interval '24 hours' \
                           AND q.track_presence=TRUE \
                         GROUP BY d.queue_id,q.queue_name,q.stats_scope \
                         ORDER BY matches DESC,d.queue_id",
                        &[],
                    );
                let matches_future = database.query_json_params(&matches_sql, &params);
                let (tracked_total_row, queues, mut rows) =
                    tokio::try_join!(total_future, queues_future, matches_future)?;
                let tracked_total = tracked_total_row
                    .and_then(|row| row.get("total").and_then(Value::as_i64))
                    .unwrap_or_default();
                let has_more = rows.len() > limit as usize;
                rows.truncate(limit as usize);
                let match_ids = rows
                    .iter()
                    .filter_map(|row| {
                        row.get("match_id")
                            .and_then(Value::as_str)
                            .and_then(|value| value.parse::<i64>().ok())
                    })
                    .collect::<Vec<_>>();
                let mut players_by_match = HashMap::<String, Vec<Value>>::new();
                if !match_ids.is_empty() {
                    let player_params = match_ids
                        .iter()
                        .copied()
                        .map(QueryParam::Int64)
                        .collect::<Vec<_>>();
                    let placeholders = (1..=player_params.len())
                        .map(|index| format!("${index}"))
                        .collect::<Vec<_>>()
                        .join(",");
                    let player_sql = format!(
                        "WITH player_facts AS ( \
                           SELECT ranked_fact.match_id,ranked_fact.player_id,ranked_fact.player_name, \
                             ranked_fact.platform,ranked_fact.participant_kind,ranked_fact.source \
                           FROM ( \
                             SELECT DISTINCT ON (mp.match_id,mp.player_id,mp.private_slot) \
                               mp.match_id,mp.player_id,mp.player_name,mp.platform, \
                               CASE WHEN mp.player_id>0 THEN 'human' ELSE 'private' END AS participant_kind, \
                               mp.source,mp.private_slot,mp.entry_datetime \
                             FROM match_players mp WHERE mp.match_id IN ({placeholders}) \
                               AND mp.entry_datetime>=now()-interval '25 hours' \
                             ORDER BY mp.match_id,mp.player_id,mp.private_slot,mp.entry_datetime DESC \
                           ) ranked_fact \
                           UNION ALL SELECT cmp.match_id,cmp.player_id,cmp.player_name,cmp.platform, \
                             cmp.participant_kind,cmp.source FROM casual_match_players cmp \
                             WHERE cmp.match_id IN ({placeholders}) \
                           UNION ALL SELECT smp.match_id,smp.player_id,smp.player_name,smp.platform, \
                             smp.participant_kind,smp.source FROM special_match_players smp \
                             WHERE smp.match_id IN ({placeholders}) \
                         ) SELECT fact.match_id::TEXT,fact.player_id::TEXT, \
                           CASE WHEN fact.participant_kind='private' THEN 'Private account' \
                             WHEN NULLIF(BTRIM(fact.player_name),'') IS NOT NULL THEN BTRIM(fact.player_name) \
                             WHEN NULLIF(BTRIM(resolved_profile.name),'') IS NOT NULL THEN BTRIM(resolved_profile.name) \
                             WHEN fact.participant_kind='bot' THEN 'Bot' ELSE 'Unknown player' END AS player_name, \
                           CASE WHEN fact.participant_kind='private' THEN 'Private' \
                             WHEN fact.participant_kind='bot' THEN 'Bot' \
                             ELSE COALESCE(NULLIF(BTRIM(resolved_profile.platform),''), \
                               NULLIF(BTRIM(fact.platform),''),'Unknown') END AS platform, \
                           fact.participant_kind,fact.source \
                         FROM player_facts fact LEFT JOIN LATERAL ( \
                           SELECT candidate.name,candidate.platform FROM ( \
                             SELECT profile.name,profile.platform,profile.hirez_profile_refreshed_at,0 AS identity_priority \
                               FROM players profile WHERE fact.player_id>0 AND profile.id=fact.player_id \
                             UNION ALL \
                             SELECT profile.name,profile.platform,profile.hirez_profile_refreshed_at,1 \
                               FROM players profile WHERE fact.player_id>0 \
                                 AND profile.active_player_id=fact.player_id AND profile.active_player_id>0 \
                                 AND profile.id<>fact.player_id \
                           ) candidate ORDER BY candidate.identity_priority, \
                             candidate.hirez_profile_refreshed_at DESC NULLS LAST LIMIT 1 \
                         ) resolved_profile ON TRUE \
                         ORDER BY fact.match_id DESC,platform,player_name"
                    );
                    for row in database
                        .query_json_params(&player_sql, &player_params)
                        .await?
                    {
                        let Some(match_id) = row.get("match_id").and_then(Value::as_str) else {
                            continue;
                        };
                        players_by_match
                            .entry(match_id.to_owned())
                            .or_default()
                            .push(json!({
                                "player_id": row.get("player_id").and_then(Value::as_str).unwrap_or(""),
                                "player_name": row.get("player_name").and_then(Value::as_str).unwrap_or(""),
                                "platform": row.get("platform").and_then(Value::as_str).unwrap_or(""),
                                "participant_kind": row.get("participant_kind").and_then(Value::as_str).unwrap_or(""),
                                "source": row.get("source").and_then(Value::as_str).unwrap_or("unknown")
                            }));
                    }
                }
                let next_cursor = if has_more {
                    rows.last().and_then(encode_cursor)
                } else {
                    None
                };
                let matches = rows
                    .iter()
                    .map(|row| {
                        let match_id = row
                            .get("match_id")
                            .and_then(Value::as_str)
                            .unwrap_or_default();
                        json!({
                            "match_id": match_id,
                            "queue_id": row.get("queue_id").cloned().unwrap_or(Value::Null),
                            "queue_name": row.get("queue_name").and_then(Value::as_str).unwrap_or(""),
                            "stats_scope": row.get("stats_scope").and_then(Value::as_str).unwrap_or(""),
                            "entry_datetime": row.get("entry_datetime").and_then(Value::as_str).unwrap_or(""),
                            "region": row.get("region").and_then(Value::as_str).unwrap_or(""),
                            "map": row.get("map").and_then(Value::as_str).unwrap_or(""),
                            "status": row.get("status").and_then(Value::as_str).unwrap_or(""),
                            "quality": row.get("quality").and_then(Value::as_str).unwrap_or(""),
                            "terminal_reason": row.get("terminal_reason").cloned().unwrap_or(Value::Null),
                            "players": players_by_match.get(match_id).cloned().unwrap_or_default()
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(json!({
                    "window_hours": 24,
                    "observed_at": format_json_timestamp(time::OffsetDateTime::now_utc()),
                    "total_matches": tracked_total,
                    "selected_queue_id": queue_id,
                    "queues": queues,
                    "matches": matches,
                    "next_cursor": next_cursor
                }))
            }
        },
    )
    .await
}

fn presence_cache_key(uri: &axum::http::Uri) -> String {
    format!("{}:canonical-region-v2", stats_cache_key(uri))
}

async fn cached_payload(
    state: StatsState,
    uri: axum::http::Uri,
    request_id: RequestId,
    sql: String,
    params: Vec<QueryParam>,
) -> Result<Response, ApiError> {
    let database = state.database.clone();
    let stale_ttl_seconds = presence_stale_ttl_seconds(&uri);
    cached_database_json(
        state.cache,
        presence_cache_key(&uri),
        CACHE_TTL_SECONDS,
        stale_ttl_seconds,
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

fn presence_stale_ttl_seconds(uri: &axum::http::Uri) -> u64 {
    if uri.path() == "/stats/presence"
        && uri.query().is_some_and(|query| {
            form_urlencoded::parse(query.as_bytes())
                .find(|(key, _)| key == "view")
                .is_some_and(|(_, value)| value == "activity-v4")
        })
    {
        ACTIVITY_STALE_TTL_SECONDS
    } else {
        CACHE_TTL_SECONDS * 3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn activity_presence_uses_legacy_stale_window() {
        let uri: axum::http::Uri = "/stats/presence?view=activity-v4".parse().unwrap();
        assert_eq!(presence_stale_ttl_seconds(&uri), 6 * 60 * 60);

        let uri: axum::http::Uri = "/stats/presence?view=default&view=activity-v4"
            .parse()
            .unwrap();
        assert_eq!(presence_stale_ttl_seconds(&uri), CACHE_TTL_SECONDS * 3);
    }

    #[test]
    fn presence_cache_is_versioned_for_canonical_regions() {
        let uri: axum::http::Uri = "/stats/presence?view=activity-v4".parse().unwrap();
        assert!(presence_cache_key(&uri).ends_with(":canonical-region-v2"));
    }

    #[test]
    fn presence_regions_canonicalize_aliases_and_use_observed_fallback() {
        assert!(CANONICAL_REGION_SQL.contains("WHEN 'europe' THEN 'EU'"));
        assert!(CANONICAL_REGION_SQL.contains("WHEN 'north america' THEN 'NA'"));
        assert!(EVIDENCE_CTES.contains("d.region AS observed_region"));
        assert!(EVIDENCE_CTES.contains("latest_observed_region AS MATERIALIZED"));
        assert!(include_str!("presence.rs").contains("observed_profile.region"));
    }

    #[test]
    fn detail_cursor_binds_date_through_text() {
        assert!(include_str!("presence.rs").contains("((${date}::text)::date)"));
    }
}
