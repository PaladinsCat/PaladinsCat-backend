mod changelog;
mod dashboard;
mod deployment;
mod listings;
mod notifications;
mod operations;
mod private_accounts;
mod roles;

pub const ROUTE_COUNT: usize = 35;

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
    routes::identity::{Session, session},
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
            "/admin/deployment/version",
            post(deployment::record_version),
        )
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
        .route("/developer/dashboard", get(dashboard::get_developer))
        .route("/admin/accounts", get(roles::search_accounts))
        .route("/admin/accounts/{id}/role", put(roles::update_role))
        .route(
            "/admin/notifications",
            get(notifications::list).post(notifications::create),
        )
        .route(
            "/admin/notifications/{id}",
            put(notifications::update).delete(notifications::delete),
        )
        .route(
            "/admin/activity-banner",
            get(notifications::activity_banner).put(notifications::update_activity_banner),
        )
        .with_state(AdminState {
            database: foundation.database.clone(),
            redis: foundation.redis.clone(),
            foundation: foundation.clone(),
            relay,
            requested_match,
        })
}

async fn require_admin(
    database: &Database,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<(), Response> {
    match session(database, headers, request_id).await {
        Ok(user) if is_admin_session(user.as_ref()) => Ok(()),
        _ => Err(coded_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Admin access required",
        )),
    }
}

fn is_admin_session(session: Option<&Session>) -> bool {
    session.is_some_and(|user| user.is_admin)
}

pub(crate) fn is_project_staff_session(session: Option<&Session>) -> bool {
    session.is_some_and(|user| user.is_admin || user.is_project_developer)
}

pub(crate) async fn require_project_staff(
    database: &Database,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<(), Response> {
    match session(database, headers, request_id).await {
        Ok(user) if is_project_staff_session(user.as_ref()) => Ok(()),
        _ => Err(coded_error(
            StatusCode::UNAUTHORIZED,
            "UNAUTHORIZED",
            "Operations access required",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admin_access_rejects_authenticated_non_admins() {
        let member = Session {
            user_id: 1,
            username: "member".to_owned(),
            is_admin: false,
            is_project_developer: false,
            linked_player_id: None,
        };
        let admin = Session {
            is_admin: true,
            ..member.clone()
        };
        let developer = Session {
            is_project_developer: true,
            ..member.clone()
        };
        assert!(!is_admin_session(None));
        assert!(!is_admin_session(Some(&member)));
        assert!(is_admin_session(Some(&admin)));
        assert!(!is_project_staff_session(Some(&member)));
        assert!(is_project_staff_session(Some(&developer)));
        assert!(is_project_staff_session(Some(&admin)));
    }
}
