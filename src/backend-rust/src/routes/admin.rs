mod changelog;
mod dashboard;
mod deployment;
mod listings;
mod notifications;
mod operations;
mod private_accounts;

pub const ROUTE_COUNT: usize = 29;

use std::time::Duration;

use axum::{
    Json, Router,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use paladinscat_core::database::Database;
use serde_json::json;

use crate::{
    foundation::FoundationState,
    request::RequestId,
    routes::identity::{require_session, session},
    workers::{relay::WorkerRelayClient, requested_match::RequestedMatchIngestor},
};

#[derive(Clone)]
struct AdminState {
    database: Database,
    redis: paladinscat_core::cache::RedisCache,
    foundation: FoundationState,
    relay: Option<WorkerRelayClient>,
    requested_match: Option<RequestedMatchIngestor>,
}

pub fn router(foundation: FoundationState) -> Router {
    let relay = WorkerRelayClient::new(&foundation.config).ok();
    let requested_match = relay.clone().map(|relay| {
        RequestedMatchIngestor::new(
            foundation.database.clone(),
            relay,
            Duration::from_millis(foundation.config.hirez_relay_timeout_ms),
        )
    });
    Router::new()
        .route("/admin/sync-jobs", get(listings::sync_jobs))
        .route("/admin/sync-jobs/{type}", get(listings::sync_jobs_type))
        .route("/admin/pull-list", get(listings::pull_list))
        .route("/admin/api-log", get(listings::api_log))
        .route("/admin/api-log/{dev_id}", get(listings::api_log_key))
        .route("/admin/hourly-usage", get(listings::hourly_usage))
        .route(
            "/admin/hourly-match-counts",
            get(listings::hourly_match_counts),
        )
        .route(
            "/admin/hourly-match-counts/{date}",
            get(listings::hourly_match_counts_date),
        )
        .route("/admin/batch-fetch", post(operations::batch_fetch))
        .route(
            "/admin/hourly-match-counts/{date}/{hour}/{queue_id}",
            delete(operations::delete_hourly_match_count),
        )
        .route("/admin/buffer/process", post(operations::process_buffer))
        .route(
            "/admin/buffer/retention",
            post(operations::buffer_retention),
        )
        .route("/admin/refresh-coplay", post(operations::refresh_coplay))
        .route(
            "/admin/refresh-baselines",
            post(operations::refresh_baselines),
        )
        .route(
            "/admin/refresh-derived-projections",
            post(operations::refresh_derived_projections),
        )
        .route("/admin/api-keys/sync", post(operations::sync_api_keys))
        .route(
            "/admin/api-keys/reset-budgets",
            post(operations::reset_api_key_budgets),
        )
        .route("/admin/deployment/status", get(deployment::status))
        .route("/admin/deployment/state", post(deployment::set_state))
        .route("/admin/deployment/drain", post(deployment::drain))
        .route("/admin/deployment/warm", post(deployment::warm))
        .route(
            "/admin/private-accounts/reconcile",
            post(private_accounts::reconcile),
        )
        .route(
            "/admin/private-accounts/{private_id}/verify-name",
            post(private_accounts::verify_name),
        )
        .route(
            "/admin/private-accounts/{private_id}/moderation",
            post(private_accounts::moderation),
        )
        .route("/admin/changelog", get(changelog::list))
        .route("/admin/changelog/{id}", put(changelog::update))
        .route("/admin/dashboard", get(dashboard::get))
        .route(
            "/admin/notifications",
            get(notifications::list).post(notifications::create),
        )
        .route(
            "/admin/notifications/{id}",
            put(notifications::update).delete(notifications::delete),
        )
        .with_state(AdminState {
            database: foundation.database.clone(),
            redis: foundation.redis.clone(),
            foundation: foundation.clone(),
            relay,
            requested_match,
        })
}

async fn require_auth(
    database: &Database,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<(), Response> {
    require_session(database, headers, request_id)
        .await
        .map(|_| ())
        .map_err(|_| {
            coded_error(
                StatusCode::UNAUTHORIZED,
                "UNAUTHORIZED",
                "Authentication required",
            )
        })
}

async fn require_admin(
    database: &Database,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<(), Response> {
    match session(database, headers, request_id).await {
        Ok(Some(user)) if user.is_admin => Ok(()),
        _ => Err(coded_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Admin access required",
        )),
    }
}

fn coded_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        Json(json!({"error":{"code":code,"message":message}})),
    )
        .into_response()
}
