use axum::{
    Json, Router,
    extract::Request,
    http::{
        HeaderValue, StatusCode,
        header::{ACCESS_CONTROL_ALLOW_METHODS, ALLOW, CONTENT_TYPE},
    },
    middleware,
    response::IntoResponse,
    response::Response,
    routing::get,
};
use paladinscat_core::database::format_json_timestamp;
use serde::Serialize;
use serde_json::json;
use time::OffsetDateTime;

use crate::foundation::FoundationState;

pub mod error;
pub mod foundation;
pub mod oidc;
pub mod operators;
mod raw_hirez_audit;
pub mod request;
pub mod route_cache;
pub mod routes;
pub mod security;
pub mod server;
pub mod sql_compat;
pub mod workers;

pub const INVENTORIED_ROUTES: usize = 268;
pub const INVENTORIED_WORKERS: usize = 41;
pub const INVENTORIED_SCHEDULERS: usize = 6;
pub const INVENTORIED_OPERATOR_COMMANDS: usize = 20;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CandidateStatus {
    pub status: &'static str,
    pub admission: &'static str,
    pub routes_migrated: usize,
    pub routes_implemented: usize,
    pub routes_inventoried: usize,
    pub worker_modules_migrated: usize,
    pub worker_modules_implemented: usize,
    pub worker_modules_inventoried: usize,
    pub scheduler_owners_migrated: usize,
    pub scheduler_owners_implemented: usize,
    pub scheduler_owners_inventoried: usize,
    pub operator_commands_migrated: usize,
    pub operator_commands_implemented: usize,
    pub operator_commands_inventoried: usize,
}

pub fn candidate_status() -> CandidateStatus {
    migration_status(false)
}

pub fn runtime_status() -> CandidateStatus {
    migration_status(production_runtime_enabled())
}

pub fn production_runtime_enabled() -> bool {
    std::env::var("PALADINSCAT_RUST_PRODUCTION_ENABLE").as_deref() == Ok("true")
}

fn count_implemented_routes() -> usize {
    routes::count_implemented_routes()
}

fn count_implemented_workers() -> usize {
    workers::count_implemented_workers()
}

fn migration_status(production: bool) -> CandidateStatus {
    let routes_impl = count_implemented_routes();
    let workers_impl = count_implemented_workers();
    CandidateStatus {
        status: if production {
            "production_migrated"
        } else {
            "migration_candidate"
        },
        admission: if production { "active" } else { "quiesced" },
        routes_migrated: if production { INVENTORIED_ROUTES } else { 0 },
        routes_implemented: routes_impl,
        routes_inventoried: INVENTORIED_ROUTES,
        worker_modules_migrated: if production { INVENTORIED_WORKERS } else { 0 },
        worker_modules_implemented: workers_impl,
        worker_modules_inventoried: INVENTORIED_WORKERS,
        scheduler_owners_migrated: if production {
            INVENTORIED_SCHEDULERS
        } else {
            0
        },
        scheduler_owners_implemented: INVENTORIED_SCHEDULERS,
        scheduler_owners_inventoried: INVENTORIED_SCHEDULERS,
        operator_commands_migrated: if production {
            INVENTORIED_OPERATOR_COMMANDS
        } else {
            0
        },
        operator_commands_implemented: INVENTORIED_OPERATOR_COMMANDS,
        operator_commands_inventoried: INVENTORIED_OPERATOR_COMMANDS,
    }
}

pub fn candidate_router(foundation: FoundationState) -> Router {
    let database = foundation.database.clone();
    let redis = foundation.redis.clone();
    let route_cache = route_cache::RouteCache::new(redis.clone());
    let health_state = foundation.clone();
    let deployment_state = foundation.clone();
    let readiness_state = foundation.clone();
    let application_routes = Router::new()
        .route("/health", get(move || health(health_state.clone())))
        .route(
            "/deployment/status",
            get(move || deployment_status(deployment_state.clone())),
        )
        .route(
            "/migration/status",
            get(|| async { Json(runtime_status()) }),
        )
        .route(
            "/migration/readiness",
            get(move || readiness(readiness_state.clone())),
        )
        .merge(routes::recovery::router(database.clone()))
        .merge(routes::coplay::router(database.clone()))
        .merge(routes::meta::router(database.clone(), route_cache.clone()))
        .merge(routes::champions::router(
            database.clone(),
            route_cache.clone(),
        ))
        .merge(routes::stats::router(database.clone(), route_cache.clone()))
        .merge(routes::notifications::router(
            database.clone(),
            route_cache.clone(),
        ))
        .merge(routes::matches::router(
            database.clone(),
            foundation.redis.clone(),
            route_cache.clone(),
            foundation.config.clone(),
        ))
        .merge(routes::players::router(
            database.clone(),
            foundation.redis.clone(),
            route_cache.clone(),
            foundation.config.clone(),
        ))
        .merge(routes::auth::router(
            database.clone(),
            foundation.redis.clone(),
            foundation.config.clone(),
        ))
        .merge(routes::admin::router(foundation.clone()))
        .merge(routes::builds::router(
            database.clone(),
            foundation.redis.clone(),
        ))
        .merge(routes::community::router(
            database.clone(),
            foundation.redis.clone(),
        ))
        .merge(routes::tierlists::router(database.clone()))
        .merge(routes::site_analytics::router(database.clone()))
        .merge(routes::system::router(
            database.clone(),
            foundation.redis.clone(),
            foundation.config.clone(),
        ))
        .merge(routes::player_ext::router(database.clone()))
        .merge(routes::esports::router(database.clone()))
        .merge(routes::ratings::router(database.clone()))
        .merge(routes::search::router(
            database.clone(),
            foundation.search.clone(),
            foundation.config.clone(),
        ))
        .merge(routes::live::router(
            database.clone(),
            foundation.redis.clone(),
            foundation.config.clone(),
        ))
        .merge(routes::reference::router(database.clone(), redis))
        .merge(routes::raw_api_responses::router(
            database.clone(),
            &foundation,
        ))
        .merge(routes::public_operations::router(
            database,
            route_cache.clone(),
        ))
        .fallback(not_found);
    Router::new()
        // Keep the application dispatcher behind one outer service boundary.
        // Axum's per-route MethodRouter otherwise appends an `Allow` header
        // after middleware returns a CORS preflight response, which diverges
        // from Fastify. This boundary also lets `/v1` URL rewriting happen
        // before route selection, matching the TypeScript rewrite hook.
        .fallback_service(application_routes)
        .layer(middleware::from_fn_with_state(
            foundation,
            foundation::application_foundation,
        ))
        .layer(middleware::map_response(match_fastify_response))
}

async fn health(state: FoundationState) -> Json<serde_json::Value> {
    let health = state.dependency_health().await;
    Json(json!({
        "status": if health.healthy() { "healthy" } else { "degraded" },
        "db": health.database,
        "redis": health.redis,
        "meilisearch": health.meilisearch,
        "timestamp": format_json_timestamp(OffsetDateTime::now_utc())
    }))
}

async fn deployment_status(state: FoundationState) -> impl IntoResponse {
    let deployment = state.deployment.local_state().await;
    ([("Cache-Control", "no-store, max-age=0")], Json(deployment))
}

async fn readiness(state: FoundationState) -> Response {
    let health = state.dependency_health().await;
    let deployment = state.deployment.local_state().await;
    let ready = health.healthy() && !deployment.phase.is_blocking();
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "status": if ready { "ready" } else { "not_ready" },
            "admission": if deployment.phase.is_blocking() {
                "blocked"
            } else if production_runtime_enabled() {
                "active"
            } else {
                "quiesced"
            },
            "dependencies": {
                "db": health.database,
                "redis": health.redis,
                "meilisearch": health.meilisearch
            },
            "deployment": deployment,
            "timestamp": format_json_timestamp(OffsetDateTime::now_utc())
        })),
    )
        .into_response()
}

async fn not_found(request: Request) -> impl IntoResponse {
    let uri = request.uri();
    let path_with_query = if let Some(query) = uri.query() {
        format!("{}?{}", uri.path(), query)
    } else {
        uri.path().to_owned()
    };
    (
        StatusCode::NOT_FOUND,
        Json(json!({
            "message": format!(
                "Route {}:{} not found",
                request.method(),
                path_with_query
            ),
            "error": "Not Found",
            "statusCode": 404
        })),
    )
}

async fn match_fastify_response(mut response: Response) -> Response {
    if response
        .headers()
        .get(CONTENT_TYPE)
        .is_some_and(|value| value.as_bytes() == b"application/json")
    {
        response.headers_mut().insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/json; charset=utf-8"),
        );
    }
    if response.status() == StatusCode::NO_CONTENT
        && response
            .headers()
            .contains_key(ACCESS_CONTROL_ALLOW_METHODS)
    {
        response.headers_mut().remove(ALLOW);
    }
    response
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use async_trait::async_trait;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use paladinscat_core::{
        cache::RedisCache,
        config::BackendConfig,
        database::Database,
        deployment::{DeploymentControl, DeploymentPhase, DeploymentState},
    };
    use tower::ServiceExt;

    use super::*;
    use crate::foundation::{DependencyHealth, DependencyHealthBackend};

    struct StaticDependencyHealth(DependencyHealth);

    #[async_trait]
    impl DependencyHealthBackend for StaticDependencyHealth {
        async fn check(&self) -> DependencyHealth {
            self.0
        }
    }

    fn fixture_foundation(health: DependencyHealth) -> FoundationState {
        let values = HashMap::from([
            (
                "DATABASE_URL".to_owned(),
                "postgres://fixture:fixture@127.0.0.1:9/fixture".to_owned(),
            ),
            ("REDIS_URL".to_owned(), "redis://127.0.0.1:9".to_owned()),
            ("NODE_ENV".to_owned(), "production".to_owned()),
        ]);
        let config = BackendConfig::from_lookup(|name| values.get(name).cloned()).expect("config");
        let database = Database::new(&config, "router-foundation-fixture").expect("database");
        let redis = RedisCache::new(&config.redis_url).expect("redis");
        FoundationState::new(config, database, redis)
            .expect("foundation")
            .with_dependency_health_backend(Arc::new(StaticDependencyHealth(health)))
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("JSON")
    }

    #[test]
    fn foundation_cannot_be_mistaken_for_a_migrated_backend() {
        let status = candidate_status();
        assert_eq!(status.admission, "quiesced");
        assert_eq!(status.routes_migrated, 0);
        assert_eq!(status.routes_implemented, count_implemented_routes());
        assert_eq!(status.routes_inventoried, INVENTORIED_ROUTES);
        assert_eq!(status.worker_modules_migrated, 0);
        assert_eq!(
            status.worker_modules_implemented,
            count_implemented_workers()
        );
        assert_eq!(status.scheduler_owners_implemented, INVENTORIED_SCHEDULERS);
        assert_eq!(status.scheduler_owners_migrated, 0);
        assert_eq!(
            status.operator_commands_implemented,
            INVENTORIED_OPERATOR_COMMANDS
        );
        assert_eq!(status.operator_commands_migrated, 0);
    }

    #[test]
    fn production_status_reports_the_complete_fixed_inventory() {
        let status = migration_status(true);
        assert_eq!(status.status, "production_migrated");
        assert_eq!(status.admission, "active");
        assert_eq!(status.routes_migrated, status.routes_inventoried);
        assert_eq!(
            status.worker_modules_migrated,
            status.worker_modules_inventoried
        );
        assert_eq!(
            status.scheduler_owners_migrated,
            status.scheduler_owners_inventoried
        );
        assert_eq!(
            status.operator_commands_migrated,
            status.operator_commands_inventoried
        );
    }

    #[tokio::test]
    async fn health_route_preserves_healthy_and_degraded_typescript_contracts() {
        for (health, expected) in [
            (
                DependencyHealth {
                    database: true,
                    redis: true,
                    meilisearch: true,
                },
                "healthy",
            ),
            (
                DependencyHealth {
                    database: true,
                    redis: false,
                    meilisearch: true,
                },
                "degraded",
            ),
        ] {
            let response = candidate_router(fixture_foundation(health))
                .oneshot(
                    Request::builder()
                        .uri("/health")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers().get("x-ratelimit-limit"), None);
            assert_eq!(
                response.headers().get(CONTENT_TYPE),
                Some(&HeaderValue::from_static("application/json; charset=utf-8"))
            );
            let body = body_json(response).await;
            assert_eq!(body["status"], expected);
            assert_eq!(body["db"], health.database);
            assert_eq!(body["redis"], health.redis);
            assert_eq!(body["meilisearch"], health.meilisearch);
            assert!(
                body["timestamp"]
                    .as_str()
                    .is_some_and(|timestamp| timestamp.ends_with('Z'))
            );
        }
    }

    #[tokio::test]
    async fn deployment_status_reads_local_state_and_never_consumes_quota() {
        let mut foundation = fixture_foundation(DependencyHealth {
            database: true,
            redis: true,
            meilisearch: true,
        });
        foundation.deployment = DeploymentControl::with_local_state(
            foundation.redis.clone(),
            DeploymentState {
                id: "deploy-fixture".to_owned(),
                phase: DeploymentPhase::Draining,
                message: Some("Draining".to_owned()),
                started_at: Some("2099-01-01T00:00:00.000Z".to_owned()),
                updated_at: "2099-01-01T00:00:01.000Z".to_owned(),
                expires_at: Some("2099-01-01T01:00:00.000Z".to_owned()),
            },
        );
        let response = candidate_router(foundation)
            .oneshot(
                Request::builder()
                    .uri("/deployment/status")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("cache-control"),
            Some(&HeaderValue::from_static("no-store, max-age=0"))
        );
        assert_eq!(response.headers().get("x-ratelimit-limit"), None);
        let body = body_json(response).await;
        assert_eq!(body["id"], "deploy-fixture");
        assert_eq!(body["phase"], "draining");
        assert_eq!(body["message"], "Draining");
    }

    #[tokio::test]
    async fn readiness_combines_dependency_and_deployment_admission_state() {
        let mut foundation = fixture_foundation(DependencyHealth {
            database: true,
            redis: true,
            meilisearch: true,
        });
        foundation.deployment = DeploymentControl::with_local_state(
            foundation.redis.clone(),
            DeploymentState {
                id: "deploy-fixture".to_owned(),
                phase: DeploymentPhase::Warming,
                message: None,
                started_at: Some("2099-01-01T00:00:00.000Z".to_owned()),
                updated_at: "2099-01-01T00:00:01.000Z".to_owned(),
                expires_at: Some("2099-01-01T01:00:00.000Z".to_owned()),
            },
        );
        let blocked = candidate_router(foundation)
            .oneshot(
                Request::builder()
                    .uri("/migration/readiness")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(blocked.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(blocked).await;
        assert_eq!(body["status"], "not_ready");
        assert_eq!(body["admission"], "blocked");
        assert_eq!(body["deployment"]["phase"], "warming");

        let degraded = candidate_router(fixture_foundation(DependencyHealth {
            database: true,
            redis: false,
            meilisearch: true,
        }))
        .oneshot(
            Request::builder()
                .uri("/migration/readiness")
                .body(Body::empty())
                .expect("request"),
        )
        .await
        .expect("response");
        assert_eq!(degraded.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_json(degraded).await;
        assert_eq!(body["status"], "not_ready");
        assert_eq!(body["admission"], "quiesced");
        assert_eq!(body["dependencies"]["redis"], false);
    }
}
