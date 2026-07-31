use axum::{
    Json,
    extract::{Extension, State},
    http::{HeaderMap, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;

use crate::{error::ApiError, request::RequestId};

use super::{AdminState, require_admin};

pub(super) async fn get(
    State(state): State<AdminState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Err(response) = require_admin(&state.database, &headers, &request_id).await {
        return Ok(response);
    }
    let traffic_summary = state.database.one_json(
        "SELECT COUNT(*) FILTER(WHERE visit_date=(now() AT TIME ZONE 'UTC')::DATE)::INT AS visitors_today,\
         COALESCE(SUM(page_views) FILTER(WHERE visit_date=(now() AT TIME ZONE 'UTC')::DATE),0)::INT AS views_today,\
         COUNT(*) FILTER(WHERE visit_date=(now() AT TIME ZONE 'UTC')::DATE-1)::INT AS visitors_yesterday,\
         COUNT(*) FILTER(WHERE visit_date>=(now() AT TIME ZONE 'UTC')::DATE-6)::INT AS visitor_days_7d,\
         COALESCE(SUM(page_views) FILTER(WHERE visit_date>=(now() AT TIME ZONE 'UTC')::DATE-6),0)::INT AS views_7d \
         FROM site_daily_visitors WHERE visit_date>=(now() AT TIME ZONE 'UTC')::DATE-13",
        &[],
    );
    let active_users = state.database.one_json(
        "SELECT COUNT(*)::INT AS active_users FROM site_live_sessions WHERE last_seen_at>=now()-INTERVAL '5 minutes'",
        &[],
    );
    let daily = state.database.query_json(
        "WITH days AS(SELECT generate_series((now() AT TIME ZONE 'UTC')::DATE-13,(now() AT TIME ZONE 'UTC')::DATE,INTERVAL '1 day')::DATE AS date),\
         visitors AS(SELECT visit_date AS date,COUNT(*)::INT AS visitors,SUM(page_views)::INT AS page_views FROM site_daily_visitors WHERE visit_date>=(now() AT TIME ZONE 'UTC')::DATE-13 GROUP BY visit_date),\
         match_counts AS(SELECT (entry_datetime AT TIME ZONE 'UTC')::DATE AS date,COUNT(*)::INT AS matches FROM matches WHERE entry_datetime>=(now() AT TIME ZONE 'UTC')::DATE-13 GROUP BY 1)\
         SELECT days.date::TEXT,COALESCE(visitors.visitors,0)::INT AS visitors,COALESCE(visitors.page_views,0)::INT AS page_views,COALESCE(match_counts.matches,0)::INT AS matches \
         FROM days LEFT JOIN visitors USING(date) LEFT JOIN match_counts USING(date) ORDER BY days.date",
        &[],
    );
    let top_pages = state.database.query_json(
        "SELECT path,SUM(page_views)::INT AS page_views FROM site_daily_page_views \
         WHERE visit_date>=(now() AT TIME ZONE 'UTC')::DATE-6 GROUP BY path ORDER BY page_views DESC,path LIMIT 12",
        &[],
    );
    let totals = state.database.one_json(
        "SELECT (SELECT COUNT(*)::INT FROM matches) AS matches,\
         (SELECT COUNT(*)::INT FROM matches WHERE queue_id=486) AS ranked_matches,\
         (SELECT COUNT(*)::INT FROM players WHERE id>0) AS players,\
         (SELECT COUNT(*)::INT FROM users) AS registered_users,\
         (SELECT COUNT(*)::INT FROM builds) AS community_builds,\
         pg_database_size(current_database())::BIGINT AS database_bytes",
        &[],
    );
    let pipeline = state.database.one_json(
        "SELECT COUNT(*) FILTER(WHERE rib.status='pending' AND NOT(rib.entity_type='match' AND rib.entity_id~'^[0-9]+$' AND COALESCE(mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[],FALSE)))::INT AS buffer_pending,\
         COUNT(*) FILTER(WHERE rib.status='pending' AND rib.entity_type='match' AND rib.entity_id~'^[0-9]+$' AND COALESCE(mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[],FALSE))::INT AS buffer_projection_pending,\
         COUNT(*) FILTER(WHERE rib.status='processing')::INT AS buffer_processing,\
         COUNT(*) FILTER(WHERE rib.status='failed')::INT AS buffer_failed,\
         COUNT(*) FILTER(WHERE rib.status='processed')::INT AS buffer_processed \
         FROM raw_ingest_buffer rib LEFT JOIN match_ingest_status mis ON rib.entity_type='match' AND mis.match_id=CASE WHEN rib.entity_id~'^[0-9]+$' THEN rib.entity_id::BIGINT ELSE NULL END",
        &[],
    );
    let keys = state.database.query_json(
        "SELECT dev_id,status,COALESCE(total_24h,0)::INT AS used,COALESCE(daily_limit,0)::INT AS daily_limit,\
         GREATEST(COALESCE(daily_limit,0)-COALESCE(total_24h,0),0)::INT AS remaining,\
         COALESCE(calls_total,0)::BIGINT AS calls_total,consecutive_failures,last_used,last_sync_at,last_sync_error \
         FROM api_keys ORDER BY dev_id",
        &[],
    );
    let hourly = state.database.query_json(
        "WITH hours AS(SELECT generate_series(date_trunc('hour',now())-INTERVAL '23 hours',date_trunc('hour',now()),INTERVAL '1 hour') AS hour),\
         usage AS(SELECT date_trunc('hour',hour_bucket) AS hour,SUM(call_count)::INT AS calls FROM api_key_hourly_usage WHERE hour_bucket>=date_trunc('hour',now())-INTERVAL '23 hours' GROUP BY 1)\
         SELECT hours.hour,COALESCE(usage.calls,0)::INT AS calls FROM hours LEFT JOIN usage USING(hour) ORDER BY hours.hour",
        &[],
    );
    let endpoints = state.database.query_json(
        "SELECT consumer,endpoint,SUM(call_count)::INT AS calls,\
         ROUND(SUM(total_response_ms)::NUMERIC/NULLIF(SUM(call_count),0),0)::INT AS avg_response_ms \
         FROM api_log WHERE hour>=now()-INTERVAL '24 hours' GROUP BY consumer,endpoint \
         ORDER BY calls DESC,consumer,endpoint LIMIT 20",
        &[],
    );
    let (
        traffic_summary,
        active_users,
        daily,
        top_pages,
        totals,
        pipeline,
        keys,
        hourly,
        endpoints,
    ) = tokio::join!(
        traffic_summary,
        active_users,
        daily,
        top_pages,
        totals,
        pipeline,
        keys,
        hourly,
        endpoints
    );
    let mut summary = traffic_summary
        .map_err(|error| ApiError::database(error, &request_id))?
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    if let Some(active) = active_users
        .map_err(|error| ApiError::database(error, &request_id))?
        .and_then(|value| value.as_object().cloned())
    {
        summary.extend(active);
    }
    let payload = json!({
        "generated_at": OffsetDateTime::now_utc(),
        "traffic": {
            "summary": Value::Object(summary),
            "daily": daily.map_err(|error| ApiError::database(error, &request_id))?,
            "top_pages": top_pages.map_err(|error| ApiError::database(error, &request_id))?,
        },
        "site": {
            "totals": totals.map_err(|error| ApiError::database(error, &request_id))?.unwrap_or_else(|| Value::Object(Map::new())),
            "pipeline": pipeline.map_err(|error| ApiError::database(error, &request_id))?.unwrap_or_else(|| Value::Object(Map::new())),
        },
        "hirez": {
            "keys": keys.map_err(|error| ApiError::database(error, &request_id))?,
            "hourly": hourly.map_err(|error| ApiError::database(error, &request_id))?,
            "endpoints": endpoints.map_err(|error| ApiError::database(error, &request_id))?,
        }
    });
    let mut response = (StatusCode::OK, Json(payload)).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, "private, no-store".parse().unwrap());
    Ok(response)
}
