use axum::{
    Json, Router,
    extract::{Extension, State},
    http::{HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::get,
};
use paladinscat_core::database::{Database, QueryParam, format_json_timestamp};
use serde_json::{Map, Value, json};
use time::OffsetDateTime;

use crate::{error::ApiError, request::RequestId};

pub const ROUTE_COUNT: usize = 1;

const ACTIVE_USER_WINDOW_SECONDS: i32 = 5 * 60;
const LIVE_SESSION_HEARTBEAT_SECONDS: i32 = 60;
const PUBLIC_CACHE_CONTROL: &str = "public, max-age=30, s-maxage=60, stale-while-revalidate=120";

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/operations/stats", get(public_stats))
        .with_state(database)
}

async fn public_stats(
    State(database): State<Database>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let totals_query = database.one_json(
        "SELECT \
           (SELECT COUNT(*)::INT FROM matches) AS matches, \
           (SELECT COUNT(*)::INT FROM matches WHERE queue_id = 486) AS ranked_matches, \
           (SELECT COUNT(*)::INT FROM matches WHERE queue_id IS DISTINCT FROM 486) AS casual_matches, \
           (SELECT COUNT(*)::INT FROM players WHERE id > 0) AS players, \
           (SELECT COUNT(*)::INT FROM users) AS registered_users, \
           (SELECT COUNT(*)::INT FROM users WHERE linked_player_id IS NOT NULL) AS verified_users, \
           (SELECT COUNT(*)::INT FROM builds WHERE visibility = 'public') AS community_builds, \
           (SELECT COUNT(*)::INT FROM tier_lists) AS tier_lists, \
           (SELECT COUNT(*)::INT FROM posts) AS community_posts, \
           (SELECT COUNT(*)::INT FROM matches WHERE recovered = TRUE) AS recovered_matches, \
           (SELECT COUNT(*)::INT FROM matches WHERE broken = TRUE AND recovered = FALSE) AS incomplete_matches, \
           (SELECT MAX(entry_datetime) FROM matches) AS latest_match_at",
        &[],
    );
    let traffic_query = database.one_json(
        "SELECT \
           COUNT(*) FILTER (WHERE visit_date = (now() AT TIME ZONE 'UTC')::DATE)::INT AS visitors_today, \
           COALESCE(SUM(page_views) FILTER (WHERE visit_date = (now() AT TIME ZONE 'UTC')::DATE), 0)::INT AS views_today, \
           COUNT(*) FILTER (WHERE visit_date >= (now() AT TIME ZONE 'UTC')::DATE - 6)::INT AS visitor_days_7d, \
           COALESCE(SUM(page_views) FILTER (WHERE visit_date >= (now() AT TIME ZONE 'UTC')::DATE - 6), 0)::INT AS views_7d \
         FROM site_daily_visitors \
         WHERE visit_date >= (now() AT TIME ZONE 'UTC')::DATE - 6",
        &[],
    );
    let active_users_query = database.one_json_params(
        "SELECT COUNT(*)::INT AS active_users \
         FROM site_daily_visitors \
         WHERE visit_date = (now() AT TIME ZONE 'UTC')::DATE \
           AND last_seen >= now() - make_interval(secs => $1::INT)",
        &[QueryParam::Int32(ACTIVE_USER_WINDOW_SECONDS)],
    );
    let ingest_coverage_query = database.one_json(
        "SELECT \
           COUNT(*)::INT AS total_matches, \
           COUNT(*) FILTER (WHERE broken IS NOT TRUE AND recovered IS NOT TRUE)::INT AS direct_matches, \
           COUNT(*) FILTER (WHERE recovered IS TRUE)::INT AS recovered_matches, \
           ( \
             SELECT COUNT(*)::INT \
             FROM nonranked_match_acquisition acquisition \
             WHERE acquisition.status IN ('discovered', 'waiting_for_completion', 'fetching') \
               AND acquisition.source_date + acquisition.source_hour * interval '1 hour' \
                     >= (now() AT TIME ZONE 'UTC') - interval '24 hours' \
           ) AS nonranked_open_24h, \
           ( \
             SELECT COUNT(*)::INT \
             FROM nonranked_match_acquisition acquisition \
             WHERE acquisition.status = 'waiting_for_completion' \
               AND acquisition.source_date + acquisition.source_hour * interval '1 hour' \
                     >= (now() AT TIME ZONE 'UTC') - interval '24 hours' \
           ) AS nonranked_waiting_for_completion_24h, \
           ( \
             SELECT COUNT(*)::INT \
             FROM nonranked_match_acquisition acquisition \
             WHERE acquisition.status IN ('discovered', 'waiting_for_completion', 'fetching') \
               AND acquisition.source_date + acquisition.source_hour * interval '1 hour' \
                     < (now() AT TIME ZONE 'UTC') - interval '24 hours' \
           ) AS nonranked_historical_open, \
           ( \
             SELECT MIN(acquisition.source_date + acquisition.source_hour * interval '1 hour') \
             FROM nonranked_match_acquisition acquisition \
             WHERE acquisition.status IN ('discovered', 'waiting_for_completion', 'fetching') \
           ) AS nonranked_oldest_open_hour, \
           ( \
             SELECT MAX(acquisition.completed_at) \
             FROM nonranked_match_acquisition acquisition \
           ) AS nonranked_last_completed_at \
         FROM matches \
         WHERE entry_datetime >= now() - INTERVAL '24 hours'",
        &[],
    );
    let release_query = database.one_json(
        "SELECT version, git_commit_short, deployed_at \
         FROM stack_versions \
         WHERE component = 'stack' \
         ORDER BY deployed_at DESC, id DESC \
         LIMIT 1",
        &[],
    );

    let (totals, traffic, active_users, ingest_coverage, release) = tokio::join!(
        totals_query,
        traffic_query,
        active_users_query,
        ingest_coverage_query,
        release_query,
    );
    let totals = totals.map_err(|error| ApiError::database(error, &request_id))?;
    let traffic = traffic.map_err(|error| ApiError::database(error, &request_id))?;
    let active_users = active_users.map_err(|error| ApiError::database(error, &request_id))?;
    let ingest_coverage =
        ingest_coverage.map_err(|error| ApiError::database(error, &request_id))?;
    let release = release.map_err(|error| ApiError::database(error, &request_id))?;

    let mut traffic_summary = traffic
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    traffic_summary.insert(
        "active_users".to_owned(),
        active_users
            .as_ref()
            .and_then(|value| value.get("active_users"))
            .cloned()
            .unwrap_or_else(|| json!(0)),
    );
    traffic_summary.insert(
        "active_window_seconds".to_owned(),
        json!(ACTIVE_USER_WINDOW_SECONDS),
    );
    traffic_summary.insert(
        "heartbeat_seconds".to_owned(),
        json!(LIVE_SESSION_HEARTBEAT_SECONDS),
    );

    let release = release.unwrap_or_else(release_from_environment);
    let payload = json!({
        "generated_at": format_json_timestamp(OffsetDateTime::now_utc()),
        "release": release,
        "traffic": {
            "summary": Value::Object(traffic_summary),
        },
        "catalog": totals,
        "ingest_coverage": ingest_coverage,
    });
    let mut response = (StatusCode::OK, Json(payload)).into_response();
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static(PUBLIC_CACHE_CONTROL),
    );
    Ok(response)
}

fn release_from_environment() -> Value {
    let mut release = Map::new();
    release.insert(
        "version".to_owned(),
        Value::String(std::env::var("PALADINSCAT_VERSION").unwrap_or_default()),
    );
    release.insert(
        "git_commit_short".to_owned(),
        Value::String(std::env::var("PALADINSCAT_GIT_COMMIT_SHORT").unwrap_or_default()),
    );
    release.insert(
        "deployed_at".to_owned(),
        std::env::var("PALADINSCAT_BUILD_TIMESTAMP")
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    Value::Object(release)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_cache_contract_matches_typescript() {
        assert_eq!(
            PUBLIC_CACHE_CONTROL,
            "public, max-age=30, s-maxage=60, stale-while-revalidate=120"
        );
        assert_eq!(ACTIVE_USER_WINDOW_SECONDS, 300);
        assert_eq!(LIVE_SESSION_HEARTBEAT_SECONDS, 60);
    }
}
