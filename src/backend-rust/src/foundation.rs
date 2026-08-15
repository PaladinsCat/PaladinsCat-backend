use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use axum::{
    Json,
    extract::{ConnectInfo, Request, State},
    http::{
        HeaderMap, HeaderName, HeaderValue, Method, StatusCode,
        header::{
            ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS,
            ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_HEADERS,
            ACCESS_CONTROL_REQUEST_METHOD, CACHE_CONTROL, ORIGIN, RETRY_AFTER, VARY,
            WWW_AUTHENTICATE,
        },
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use paladinscat_core::{
    cache::{RateLimitResult, RedisCache},
    config::BackendConfig,
    database::Database,
    deployment::DeploymentControl,
    search::SearchIndex,
};
use serde_json::{Value, json};
use tokio::sync::Notify;

use crate::{
    request::{EffectiveUri, RequestId, next_request_id},
    security::{
        SecurityContext, SecurityContextError, client_rate_limit_identity, developer_bearer_token,
        is_sensitive_operator_route, is_service_only_route, requires_configured_service_route,
        resolve_client_address, resolve_developer_api_route,
    },
};

const ONE_MINUTE_MS: u64 = 60_000;
const OIDC_START_LIMIT: u64 = 30;
const OIDC_START_WINDOW_MS: u64 = 15 * ONE_MINUTE_MS;
const CONTENT_SECURITY_POLICY: &str = "default-src 'self';base-uri 'self';font-src 'self' https: data:;form-action 'self';frame-ancestors 'self';img-src 'self' data:;object-src 'none';script-src 'self';script-src-attr 'none';style-src 'self' https: 'unsafe-inline';upgrade-insecure-requests";

#[derive(Clone)]
pub struct FoundationState {
    pub config: Arc<BackendConfig>,
    pub database: Database,
    pub redis: RedisCache,
    pub deployment: DeploymentControl,
    pub search: SearchIndex,
    pub security: Arc<SecurityContext>,
    pub active_requests: ActiveRequestTracker,
    rate_limits: Arc<dyn RateLimitBackend>,
    dependency_health: Arc<dyn DependencyHealthBackend>,
    developer_active_requests: Arc<AtomicUsize>,
}

impl FoundationState {
    pub fn new(
        config: BackendConfig,
        database: Database,
        redis: RedisCache,
    ) -> Result<Self, FoundationBuildError> {
        let search = SearchIndex::new(&config)?;
        let security = SecurityContext::from_config(&config)?;
        let deployment = DeploymentControl::new(redis.clone());
        let rate_limits = Arc::new(RedisRateLimitBackend {
            redis: redis.clone(),
        });
        let dependency_health = Arc::new(LiveDependencyHealthBackend {
            database: database.clone(),
            redis: redis.clone(),
            search: search.clone(),
        });
        Ok(Self {
            config: Arc::new(config),
            database,
            redis,
            deployment,
            search,
            security: Arc::new(security),
            active_requests: ActiveRequestTracker::default(),
            rate_limits,
            dependency_health,
            developer_active_requests: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn with_rate_limit_backend(mut self, backend: Arc<dyn RateLimitBackend>) -> Self {
        self.rate_limits = backend;
        self
    }

    pub fn with_dependency_health_backend(
        mut self,
        backend: Arc<dyn DependencyHealthBackend>,
    ) -> Self {
        self.dependency_health = backend;
        self
    }

    pub async fn initialize(&self) {
        eprintln!("[init] starting deployment.initialize");
        self.deployment
            .initialize(Duration::from_millis(
                self.config.deployment_redis_startup_timeout_ms,
            ))
            .await;
        eprintln!("[init] deployment.initialize done, starting search.initialize_indices");
        self.search.initialize_indices().await;
        eprintln!("[init] search.initialize_indices done");
    }

    pub async fn dependency_health(&self) -> DependencyHealth {
        self.dependency_health.check().await
    }
}

#[async_trait]
pub trait RateLimitBackend: Send + Sync {
    async fn check(
        &self,
        key: &str,
        limit: u64,
        window_ms: u64,
        fail_open: bool,
    ) -> RateLimitResult;
}

struct RedisRateLimitBackend {
    redis: RedisCache,
}

#[async_trait]
pub trait DependencyHealthBackend: Send + Sync {
    async fn check(&self) -> DependencyHealth;
}

struct LiveDependencyHealthBackend {
    database: Database,
    redis: RedisCache,
    search: SearchIndex,
}

#[async_trait]
impl DependencyHealthBackend for LiveDependencyHealthBackend {
    async fn check(&self) -> DependencyHealth {
        let (database, redis, meilisearch) = tokio::join!(
            self.database.health_check(),
            self.redis.health_check(),
            self.search.health_check(),
        );
        DependencyHealth {
            database,
            redis,
            meilisearch,
        }
    }
}

#[async_trait]
impl RateLimitBackend for RedisRateLimitBackend {
    async fn check(
        &self,
        key: &str,
        limit: u64,
        window_ms: u64,
        fail_open: bool,
    ) -> RateLimitResult {
        self.redis
            .check_rate_limit(key, limit, window_ms, fail_open)
            .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FoundationBuildError {
    #[error(transparent)]
    Security(#[from] SecurityContextError),
    #[error("failed to construct MeiliSearch client: {0}")]
    Search(#[from] reqwest::Error),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DependencyHealth {
    pub database: bool,
    pub redis: bool,
    pub meilisearch: bool,
}

impl DependencyHealth {
    pub fn healthy(self) -> bool {
        self.database && self.redis && self.meilisearch
    }
}

#[derive(Clone, Default)]
pub struct ActiveRequestTracker {
    active: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}

impl ActiveRequestTracker {
    pub fn count(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    pub fn begin(&self) -> ActiveRequestGuard {
        self.active.fetch_add(1, Ordering::AcqRel);
        ActiveRequestGuard {
            tracker: self.clone(),
        }
    }

    pub async fn wait_for_zero(&self, timeout: Duration) -> bool {
        if self.count() == 0 {
            return true;
        }
        tokio::time::timeout(timeout, async {
            while self.count() > 0 {
                self.changed.notified().await;
            }
        })
        .await
        .is_ok()
    }
}

pub struct ActiveRequestGuard {
    tracker: ActiveRequestTracker,
}

impl Drop for ActiveRequestGuard {
    fn drop(&mut self) {
        if self.tracker.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.tracker.changed.notify_waiters();
        }
    }
}

struct DeveloperConcurrencyGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for DeveloperConcurrencyGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AuthenticatedDeveloper;

pub async fn application_foundation(
    State(state): State<FoundationState>,
    mut request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let request_id = next_request_id();
    request.extensions_mut().insert(request_id.clone());
    let origin = request
        .headers()
        .get(ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);

    if has_session_cookie(request.headers())
        && !cookie_request_is_safe(&state, &request, origin.as_deref())
    {
        return finalize_response(
            (
                StatusCode::FORBIDDEN,
                "Cookie authentication requires HTTPS, same-origin and CSRF protection",
            )
                .into_response(),
            &state,
            origin.as_deref(),
            started,
            true,
        );
    }

    if let Some(response) = preflight_response(&state, &request) {
        return finalize_response(response, &state, origin.as_deref(), started, false);
    }

    let raw_url = request.uri().to_string();
    let developer = resolve_developer_api_route(request.method().as_str(), &raw_url);
    let effective_uri = if developer.supported {
        developer
            .target_url
            .parse()
            .unwrap_or_else(|_| request.uri().clone())
    } else {
        request.uri().clone()
    };
    let effective_path = effective_uri.path().to_owned();
    request
        .extensions_mut()
        .insert(EffectiveUri(effective_uri.clone()));
    if developer.supported {
        *request.uri_mut() = effective_uri;
    }
    let effective_path = effective_path.as_str();
    let internal = state.security.is_internal_request(request.headers());

    let bypass_deployment = internal || bypasses_deployment_gate(effective_path);
    let mut public_guard = None;
    if !bypass_deployment {
        let deployment = state.deployment.local_state().await;
        if deployment.phase.is_blocking() {
            let mut response = (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": {
                        "code": "DEPLOYMENT_DRAIN",
                        "message": deployment.message.unwrap_or_else(|| "PaladinsCat is applying an update. Please retry shortly.".to_owned()),
                        "details": {
                            "deploymentId": deployment.id,
                            "phase": deployment.phase,
                            "retryAfterSeconds": 5
                        }
                    }
                })),
            )
                .into_response();
            insert_static(response.headers_mut(), CACHE_CONTROL, "no-store");
            insert_static(response.headers_mut(), RETRY_AFTER, "5");
            return finalize_response(response, &state, origin.as_deref(), started, true);
        }
        public_guard = Some(state.active_requests.begin());
    }

    // v1 routes, including anonymous health/version and public reads, remain
    // subject to both the client IP and global external quotas. Preserve the
    // legacy health/migration bypasses for unversioned/internal surfaces.
    if !internal && (developer.attempted || !bypasses_public_rate_limit(effective_path)) {
        let peer = request
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|connect| connect.0);
        let address = resolve_client_address(
            request.headers(),
            peer,
            state.security.trust_cloudflare_headers,
        );
        let client = state
            .rate_limits
            .check(
                &client_rate_limit_identity(&address),
                state.security.public_api_rate_limit_per_minute,
                ONE_MINUTE_MS,
                true,
            )
            .await;
        if let Some(response) = apply_public_limit("RateLimit", client, request_id.0.as_str()) {
            drop(public_guard);
            return finalize_response(response, &state, origin.as_deref(), started, true);
        }
        let global = state
            .rate_limits
            .check(
                "public-api:global",
                state.security.public_api_global_limit_per_minute,
                ONE_MINUTE_MS,
                true,
            )
            .await;
        if let Some(mut response) =
            apply_public_limit("Global-RateLimit", global, request_id.0.as_str())
        {
            set_limit_headers(response.headers_mut(), "RateLimit", client);
            drop(public_guard);
            return finalize_response(response, &state, origin.as_deref(), started, true);
        }
        request
            .extensions_mut()
            .insert(PublicLimitHeaders { client, global });
    }

    let mut developer_guard = None;
    let mut developer_headers = None;
    let public_headers = request.extensions().get::<PublicLimitHeaders>().copied();
    if developer.attempted {
        if !developer.supported {
            let response = developer_error(
                developer.status_code.unwrap_or(404),
                developer.code.unwrap_or("API_ROUTE_NOT_FOUND"),
                developer
                    .message
                    .unwrap_or("This endpoint is not part of the PaladinsCat v1 API."),
                &request_id,
            );
            drop(public_guard);
            return finalize_developer_response(
                response,
                &state,
                origin.as_deref(),
                started,
                &request_id,
                DeveloperResponseLimits::new(public_headers, None),
                false,
            );
        }
        if !developer.anonymous {
            if !state.security.developer_key_configured() {
                let response = developer_error(
                    503,
                    "DEVELOPER_API_NOT_CONFIGURED",
                    "The PaladinsCat developer API is not configured.",
                    &request_id,
                );
                drop(public_guard);
                return finalize_developer_response(
                    response,
                    &state,
                    origin.as_deref(),
                    started,
                    &request_id,
                    DeveloperResponseLimits::new(public_headers, None),
                    true,
                );
            }
            let Some(candidate) = developer_bearer_token(request.headers()) else {
                let response = developer_auth_error(&request_id, true);
                drop(public_guard);
                return finalize_developer_response(
                    response,
                    &state,
                    origin.as_deref(),
                    started,
                    &request_id,
                    DeveloperResponseLimits::new(public_headers, None),
                    true,
                );
            };
            if !state.security.authenticate_developer_key(candidate) {
                let response = developer_auth_error(&request_id, true);
                drop(public_guard);
                return finalize_developer_response(
                    response,
                    &state,
                    origin.as_deref(),
                    started,
                    &request_id,
                    DeveloperResponseLimits::new(public_headers, None),
                    true,
                );
            }

            let developer_limit = state
                .rate_limits
                .check(
                    &format!("developer-api:{}", state.security.developer_key_identity()),
                    state.security.developer_api_rate_limit_per_minute,
                    ONE_MINUTE_MS,
                    true,
                )
                .await;
            if !developer_limit.allowed {
                let retry_after = retry_after_seconds(developer_limit);
                let mut response = developer_error_with_details(
                    429,
                    "DEVELOPER_RATE_LIMITED",
                    "The developer key has reached its per-minute request limit.",
                    &request_id,
                    json!({
                        "retry_after_seconds": retry_after,
                        "reset_at": developer_limit.reset_at_ms
                    }),
                );
                set_limit_headers(
                    response.headers_mut(),
                    "Developer-RateLimit",
                    developer_limit,
                );
                insert_header(response.headers_mut(), RETRY_AFTER, retry_after.to_string());
                drop(public_guard);
                return finalize_developer_response(
                    response,
                    &state,
                    origin.as_deref(),
                    started,
                    &request_id,
                    DeveloperResponseLimits::new(public_headers, None),
                    true,
                );
            }
            let dev_limit = DeveloperLimitHeaders(developer_limit);
            developer_headers = Some(dev_limit);
            request.extensions_mut().insert(dev_limit);
            if !acquire_developer_slot(
                &state.developer_active_requests,
                state.security.developer_api_concurrency_limit,
            ) {
                let response = developer_error(
                    429,
                    "DEVELOPER_CONCURRENCY_LIMITED",
                    "Too many concurrent requests for this developer key.",
                    &request_id,
                );
                drop(public_guard);
                return finalize_developer_response(
                    response,
                    &state,
                    origin.as_deref(),
                    started,
                    &request_id,
                    DeveloperResponseLimits::new(public_headers, developer_headers),
                    true,
                );
            }
            developer_guard = Some(DeveloperConcurrencyGuard {
                active: state.developer_active_requests.clone(),
            });
            request.extensions_mut().insert(AuthenticatedDeveloper);
        }
    }

    let auth_method = request.method().clone();
    let auth_headers = request.headers().clone();
    let auth_peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|connect| connect.0);
    let authenticated_developer = request
        .extensions()
        .get::<AuthenticatedDeveloper>()
        .is_some();
    let (prehandler_response, auth_limit) = authorize_prehandler(
        &state,
        &auth_method,
        effective_path,
        &auth_headers,
        auth_peer,
        authenticated_developer,
    )
    .await;
    if let Some(mut response) = prehandler_response {
        if let Some(auth_limit) = auth_limit {
            set_limit_headers(response.headers_mut(), "Auth-RateLimit", auth_limit);
        }
        if !developer.attempted
            && let Some(headers) = public_headers
        {
            set_limit_headers(response.headers_mut(), "RateLimit", headers.client);
            set_limit_headers(response.headers_mut(), "Global-RateLimit", headers.global);
        }
        drop(developer_guard);
        drop(public_guard);
        return if developer.attempted {
            finalize_developer_response(
                response,
                &state,
                origin.as_deref(),
                started,
                &request_id,
                DeveloperResponseLimits::new(public_headers, developer_headers),
                !developer.anonymous,
            )
        } else {
            finalize_response(response, &state, origin.as_deref(), started, true)
        };
    }

    let authenticated_developer = request
        .extensions()
        .get::<AuthenticatedDeveloper>()
        .is_some();
    let mut response = next.run(request).await;
    if let Some(headers) = public_headers {
        set_limit_headers(response.headers_mut(), "RateLimit", headers.client);
        set_limit_headers(response.headers_mut(), "Global-RateLimit", headers.global);
    }
    if let Some(headers) = developer_headers {
        set_limit_headers(response.headers_mut(), "Developer-RateLimit", headers.0);
    }
    if let Some(auth_limit) = auth_limit {
        set_limit_headers(response.headers_mut(), "Auth-RateLimit", auth_limit);
    }
    drop(developer_guard);
    drop(public_guard);
    if developer.attempted {
        finalize_developer_response(
            response,
            &state,
            origin.as_deref(),
            started,
            &request_id,
            DeveloperResponseLimits::new(None, None),
            authenticated_developer,
        )
    } else {
        finalize_response(response, &state, origin.as_deref(), started, true)
    }
}

fn has_session_cookie(headers: &HeaderMap) -> bool {
    headers
        .get("cookie")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(';')
                .any(|part| part.trim_start().starts_with("__Host-pc_session="))
        })
}

fn cookie_request_is_safe(
    state: &FoundationState,
    request: &Request,
    origin: Option<&str>,
) -> bool {
    // Caddy overwrites this header at the trusted origin boundary; direct HTTP callers fail closed.
    if request
        .headers()
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        != Some("https")
    {
        return false;
    }
    if matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    ) {
        return true;
    }
    if !origin.is_some_and(|origin| state.security.is_allowed_cors_origin(origin)) {
        return false;
    }
    let csrf_header = request
        .headers()
        .get("x-csrf-token")
        .and_then(|value| value.to_str().ok());
    let csrf_cookie = request
        .headers()
        .get("cookie")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| {
            value
                .split(';')
                .find_map(|part| part.trim().strip_prefix("__Host-pc_csrf="))
        });
    csrf_header
        .zip(csrf_cookie)
        .is_some_and(|(header, cookie)| {
            header.len() >= 32 && crate::security::constant_time_equal_public(header, cookie)
        })
}

#[derive(Clone, Copy)]
struct PublicLimitHeaders {
    client: RateLimitResult,
    global: RateLimitResult,
}

#[derive(Clone, Copy)]
struct DeveloperLimitHeaders(RateLimitResult);

#[derive(Clone, Copy)]
struct DeveloperResponseLimits {
    public: Option<PublicLimitHeaders>,
    developer: Option<DeveloperLimitHeaders>,
}

impl DeveloperResponseLimits {
    fn new(public: Option<PublicLimitHeaders>, developer: Option<DeveloperLimitHeaders>) -> Self {
        Self { public, developer }
    }
}

async fn authorize_prehandler(
    state: &FoundationState,
    method: &Method,
    effective_path: &str,
    headers: &HeaderMap,
    peer: Option<SocketAddr>,
    authenticated_developer: bool,
) -> (Option<Response>, Option<RateLimitResult>) {
    if is_sensitive_operator_route(method.as_str(), effective_path)
        && !state
            .security
            .is_operator_request(headers, authenticated_developer)
    {
        return (
            Some(
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": {
                            "code": "OPERATOR_AUTH_REQUIRED",
                            "message": "Operator credentials are required for this endpoint."
                        }
                    })),
                )
                    .into_response(),
            ),
            None,
        );
    }
    if is_service_only_route(effective_path)
        && (requires_configured_service_route(effective_path)
            || state.security.service_auth_configured())
        && !state.security.is_service_request(headers)
    {
        return (
            Some(
                (
                    StatusCode::UNAUTHORIZED,
                    Json(json!({
                        "error": {
                            "code": "SERVICE_AUTH_REQUIRED",
                            "message": "Service credentials are required for this endpoint."
                        }
                    })),
                )
                    .into_response(),
            ),
            None,
        );
    }
    if method == Method::POST && effective_path == "/auth/oidc/transactions" {
        let address =
            resolve_client_address(headers, peer, state.security.trust_cloudflare_headers);
        let result = state
            .rate_limits
            .check(
                &format!("oidc-start:{}", client_rate_limit_identity(&address)),
                OIDC_START_LIMIT,
                OIDC_START_WINDOW_MS,
                false,
            )
            .await;
        if !result.backend_available {
            return (
                Some(request_security_error(
                    503,
                    "PROTECTION_UNAVAILABLE",
                    "The upstream protection boundary is temporarily unavailable. Cached data remains available.",
                    result,
                )),
                Some(result),
            );
        }
        if !result.allowed {
            return (
                Some(request_security_error(
                    429,
                    "OIDC_START_RATE_LIMITED",
                    "Too many sign-in starts. Please try again later.",
                    result,
                )),
                Some(result),
            );
        }
        return (None, Some(result));
    }
    if method == Method::POST && matches!(effective_path, "/auth/login" | "/auth/account/password")
    {
        let address =
            resolve_client_address(headers, peer, state.security.trust_cloudflare_headers);
        let result = state
            .rate_limits
            .check(
                &format!(
                    "account-auth:{effective_path}:{}",
                    client_rate_limit_identity(&address)
                ),
                state.security.account_auth_attempts_per_window,
                state.security.account_auth_window_ms,
                false,
            )
            .await;
        if !result.backend_available {
            return (
                Some(request_security_error(
                    503,
                    "PROTECTION_UNAVAILABLE",
                    "The upstream protection boundary is temporarily unavailable. Cached data remains available.",
                    result,
                )),
                Some(result),
            );
        }
        if !result.allowed {
            return (
                Some(request_security_error(
                    429,
                    "AUTH_RATE_LIMITED",
                    "Too many account authentication attempts. Please try again later.",
                    result,
                )),
                Some(result),
            );
        }
        return (None, Some(result));
    }
    (None, None)
}

fn preflight_response(state: &FoundationState, request: &Request) -> Option<Response> {
    if request.method() != Method::OPTIONS
        || !request
            .headers()
            .contains_key(ACCESS_CONTROL_REQUEST_METHOD)
    {
        return None;
    }
    let Some(origin) = request
        .headers()
        .get(ORIGIN)
        .and_then(|value| value.to_str().ok())
    else {
        return Some((StatusCode::BAD_REQUEST, "Invalid Preflight Request").into_response());
    };
    if !state.security.is_allowed_cors_origin(origin) {
        return None;
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    append_vary(response.headers_mut(), "Origin");
    append_vary(response.headers_mut(), "Access-Control-Request-Headers");
    insert_header(
        response.headers_mut(),
        ACCESS_CONTROL_ALLOW_ORIGIN,
        origin.to_owned(),
    );
    insert_static(
        response.headers_mut(),
        ACCESS_CONTROL_ALLOW_METHODS,
        "GET,HEAD,PUT,PATCH,POST,DELETE",
    );
    if let Some(requested) = request.headers().get(ACCESS_CONTROL_REQUEST_HEADERS) {
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_HEADERS, requested.clone());
    }
    Some(response)
}

fn apply_public_limit(
    prefix: &str,
    result: RateLimitResult,
    _request_id: &str,
) -> Option<Response> {
    if result.allowed {
        return None;
    }
    let retry_after = retry_after_seconds(result);
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(json!({
            "error": "Too Many Requests",
            "retryAfter": retry_after,
            "resetAt": result.reset_at_ms
        })),
    )
        .into_response();
    set_limit_headers(response.headers_mut(), prefix, result);
    insert_header(response.headers_mut(), RETRY_AFTER, retry_after.to_string());
    Some(response)
}

fn request_security_error(
    status: u16,
    code: &'static str,
    message: &'static str,
    result: RateLimitResult,
) -> Response {
    let retry_after = retry_after_seconds(result);
    let mut response = (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(json!({
            "error": {
                "code": code,
                "message": message,
                "details": {
                    "retry_after_seconds": retry_after
                }
            }
        })),
    )
        .into_response();
    insert_header(response.headers_mut(), RETRY_AFTER, retry_after.to_string());
    response
}

fn developer_auth_error(request_id: &RequestId, authenticate: bool) -> Response {
    let mut response = developer_error(
        401,
        "INVALID_API_KEY",
        "A valid PaladinsCat developer API key is required.",
        request_id,
    );
    if authenticate {
        insert_static(
            response.headers_mut(),
            WWW_AUTHENTICATE,
            r#"Bearer realm="PaladinsCat API", error="invalid_token""#,
        );
    }
    response
}

fn developer_error(
    status: u16,
    code: &'static str,
    message: &'static str,
    request_id: &RequestId,
) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(json!({
            "error": {
                "code": code,
                "message": message,
                "requestId": request_id.0
            }
        })),
    )
        .into_response()
}

fn developer_error_with_details(
    status: u16,
    code: &'static str,
    message: &'static str,
    request_id: &RequestId,
    details: Value,
) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        Json(json!({
            "error": {
                "code": code,
                "message": message,
                "requestId": request_id.0,
                "details": details
            }
        })),
    )
        .into_response()
}

fn acquire_developer_slot(active: &AtomicUsize, limit: usize) -> bool {
    active
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current < limit).then_some(current + 1)
        })
        .is_ok()
}

fn finalize_developer_response(
    mut response: Response,
    state: &FoundationState,
    origin: Option<&str>,
    started: Instant,
    request_id: &RequestId,
    limits: DeveloperResponseLimits,
    private_boundary: bool,
) -> Response {
    if let Some(headers) = limits.public {
        set_limit_headers(response.headers_mut(), "RateLimit", headers.client);
        set_limit_headers(response.headers_mut(), "Global-RateLimit", headers.global);
    }
    if let Some(headers) = limits.developer {
        set_limit_headers(response.headers_mut(), "Developer-RateLimit", headers.0);
    }
    insert_static(
        response.headers_mut(),
        HeaderName::from_static("x-paladinscat-api-version"),
        "v1",
    );
    insert_header(
        response.headers_mut(),
        HeaderName::from_static("x-request-id"),
        request_id.0.clone(),
    );
    if private_boundary {
        insert_static(response.headers_mut(), CACHE_CONTROL, "private, no-store");
    }
    let mut response = finalize_response(response, state, origin, started, true);
    if private_boundary {
        append_vary(response.headers_mut(), "Authorization");
    }
    response
}

fn finalize_response(
    mut response: Response,
    state: &FoundationState,
    origin: Option<&str>,
    started: Instant,
    helmet: bool,
) -> Response {
    insert_header(
        response.headers_mut(),
        HeaderName::from_static("server-timing"),
        format!("app;dur={:.1}", started.elapsed().as_secs_f64() * 1_000.0),
    );
    if helmet {
        apply_helmet_headers(response.headers_mut());
        append_vary(response.headers_mut(), "Origin");
        if let Some(origin) = origin.filter(|origin| state.security.is_allowed_cors_origin(origin))
        {
            insert_header(
                response.headers_mut(),
                ACCESS_CONTROL_ALLOW_ORIGIN,
                origin.to_owned(),
            );
        }
    }
    response
}

fn apply_helmet_headers(headers: &mut HeaderMap) {
    for (name, value) in [
        ("content-security-policy", CONTENT_SECURITY_POLICY),
        ("cross-origin-opener-policy", "same-origin"),
        ("cross-origin-resource-policy", "same-origin"),
        ("origin-agent-cluster", "?1"),
        ("referrer-policy", "no-referrer"),
        (
            "strict-transport-security",
            "max-age=31536000; includeSubDomains",
        ),
        ("x-content-type-options", "nosniff"),
        ("x-dns-prefetch-control", "off"),
        ("x-download-options", "noopen"),
        ("x-frame-options", "SAMEORIGIN"),
        ("x-permitted-cross-domain-policies", "none"),
        ("x-xss-protection", "0"),
    ] {
        insert_static(headers, HeaderName::from_static(name), value);
    }
}

fn set_limit_headers(headers: &mut HeaderMap, prefix: &str, result: RateLimitResult) {
    for (suffix, value) in [
        ("Limit", result.total),
        ("Remaining", result.remaining),
        ("Reset", result.reset_at_ms),
    ] {
        let name = HeaderName::from_bytes(format!("x-{prefix}-{suffix}").as_bytes())
            .expect("rate limit header name");
        insert_header(headers, name, value.to_string());
    }
}

fn retry_after_seconds(result: RateLimitResult) -> u64 {
    result
        .reset_at_ms
        .saturating_sub(unix_time_ms())
        .div_ceil(1_000)
        .max(1)
}

fn bypasses_deployment_gate(path: &str) -> bool {
    path.starts_with("/health")
        || path.starts_with("/migration/")
        || path.starts_with("/schedulers")
        || path.starts_with("/deployment/status")
        || path.starts_with("/admin/deployment")
}

fn bypasses_public_rate_limit(path: &str) -> bool {
    path.starts_with("/health")
        || path.starts_with("/migration/")
        || path.starts_with("/deployment/status")
}

fn append_vary(headers: &mut HeaderMap, value: &str) {
    let mut values = headers
        .get(VARY)
        .and_then(|existing| existing.to_str().ok())
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if !values.iter().any(|item| item.eq_ignore_ascii_case(value)) {
        values.push(value.to_owned());
    }
    insert_header(headers, VARY, values.join(", "));
}

fn insert_static(headers: &mut HeaderMap, name: HeaderName, value: &'static str) {
    headers.insert(name, HeaderValue::from_static(value));
}

fn insert_header(headers: &mut HeaderMap, name: HeaderName, value: String) {
    if let Ok(value) = HeaderValue::from_str(&value) {
        headers.insert(name, value);
    }
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Request as HttpRequest, header::AUTHORIZATION},
        middleware,
        routing::{get, post},
    };
    use paladinscat_core::{
        config::BackendConfig,
        database::Database,
        deployment::{DeploymentControl, DeploymentPhase, DeploymentState},
    };
    use sha2::{Digest, Sha256};
    use tower::ServiceExt;

    use super::*;

    struct AllowingRateLimits;

    #[async_trait]
    impl RateLimitBackend for AllowingRateLimits {
        async fn check(
            &self,
            _key: &str,
            limit: u64,
            window_ms: u64,
            _fail_open: bool,
        ) -> RateLimitResult {
            RateLimitResult {
                remaining: limit.saturating_sub(1),
                total: limit,
                reset_at_ms: unix_time_ms().saturating_add(window_ms),
                allowed: true,
                backend_available: true,
            }
        }
    }

    struct AuthenticationRateLimits {
        auth: RateLimitResult,
        oidc: Option<RateLimitResult>,
    }

    #[async_trait]
    impl RateLimitBackend for AuthenticationRateLimits {
        async fn check(
            &self,
            key: &str,
            limit: u64,
            window_ms: u64,
            _fail_open: bool,
        ) -> RateLimitResult {
            if key.starts_with("account-auth:") {
                return self.auth;
            }
            if key.starts_with("oidc-start:")
                && let Some(result) = self.oidc
            {
                return result;
            }
            RateLimitResult {
                remaining: limit.saturating_sub(1),
                total: limit,
                reset_at_ms: unix_time_ms().saturating_add(window_ms),
                allowed: true,
                backend_available: true,
            }
        }
    }

    struct RecordingPublicRateLimits {
        seen: Arc<Mutex<Vec<String>>>,
        reject_client: bool,
        reject_global: bool,
    }

    #[async_trait]
    impl RateLimitBackend for RecordingPublicRateLimits {
        async fn check(
            &self,
            key: &str,
            limit: u64,
            window_ms: u64,
            _fail_open: bool,
        ) -> RateLimitResult {
            self.seen.lock().expect("rate trace").push(key.to_owned());
            let rejected = (self.reject_client && key.starts_with("client:"))
                || (self.reject_global && key == "public-api:global");
            RateLimitResult {
                remaining: if rejected { 0 } else { limit.saturating_sub(1) },
                total: limit,
                reset_at_ms: unix_time_ms().saturating_add(window_ms),
                allowed: !rejected,
                backend_available: true,
            }
        }
    }

    fn fixture_state(extra: &[(&str, String)]) -> FoundationState {
        let mut values = HashMap::from([
            (
                "DATABASE_URL".to_owned(),
                "postgres://fixture:fixture@127.0.0.1:9/fixture".to_owned(),
            ),
            ("REDIS_URL".to_owned(), "redis://127.0.0.1:9".to_owned()),
            ("NODE_ENV".to_owned(), "production".to_owned()),
        ]);
        values.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone())),
        );
        let config = BackendConfig::from_lookup(|name| values.get(name).cloned()).expect("config");
        let database = Database::new(&config, "foundation-fixture").expect("database");
        let redis = RedisCache::new(&config.redis_url).expect("redis");
        FoundationState::new(config, database, redis)
            .expect("foundation")
            .with_rate_limit_backend(Arc::new(AllowingRateLimits))
    }

    fn fixture_router(state: FoundationState) -> Router {
        let routes = Router::new()
            .route(
                "/health",
                get(|| async { Json(json!({"status": "healthy"})) }),
            )
            .route(
                "/stats/champions",
                get(|| async { Json(json!([{"champion_id": 1}])) }),
            )
            .route(
                "/players/{id}/refresh",
                post(|| async { Json(json!({"refreshed": true})) }),
            )
            .route(
                "/recovery/pending",
                get(|| async { Json(json!({"private": true})) }),
            )
            .route(
                "/auth/oidc/transactions",
                post(|| async { Json(json!({"authenticated": false})) }),
            )
            .route(
                "/auth/login",
                post(|| async { Json(json!({"legacy": true})) }),
            )
            .route(
                "/auth/account/password",
                post(|| async { Json(json!({"legacy": true})) }),
            );
        Router::new()
            .merge(routes.clone())
            .nest("/v1", routes)
            .fallback(|| async { StatusCode::NOT_FOUND })
            .layer(middleware::from_fn_with_state(
                state,
                application_foundation,
            ))
    }

    async fn response_json(response: Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        serde_json::from_slice(&bytes).expect("JSON")
    }

    #[tokio::test]
    async fn active_request_tracker_waits_for_the_last_guard() {
        let tracker = ActiveRequestTracker::default();
        let first = tracker.begin();
        let second = tracker.begin();
        assert_eq!(tracker.count(), 2);
        drop(first);
        assert_eq!(tracker.count(), 1);
        assert!(!tracker.wait_for_zero(Duration::from_millis(1)).await);
        drop(second);
        assert!(tracker.wait_for_zero(Duration::from_millis(1)).await);
    }

    #[tokio::test]
    async fn full_router_drain_waits_for_an_in_flight_public_request() {
        let state = fixture_state(&[]);
        let tracker = state.active_requests.clone();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let route_entered = entered.clone();
        let route_release = release.clone();
        let app = Router::new()
            .route(
                "/slow",
                get(move || {
                    let entered = route_entered.clone();
                    let release = route_release.clone();
                    async move {
                        entered.notify_one();
                        release.notified().await;
                        Json(json!({"done": true}))
                    }
                }),
            )
            .layer(middleware::from_fn_with_state(
                state,
                application_foundation,
            ));
        let request = tokio::spawn(
            app.oneshot(
                HttpRequest::builder()
                    .uri("/slow")
                    .body(Body::empty())
                    .expect("request"),
            ),
        );
        entered.notified().await;
        assert_eq!(tracker.count(), 1);
        assert!(!tracker.wait_for_zero(Duration::from_millis(1)).await);
        release.notify_one();
        assert_eq!(
            request
                .await
                .expect("request task")
                .expect("response")
                .status(),
            StatusCode::OK
        );
        assert!(tracker.wait_for_zero(Duration::from_millis(10)).await);
    }

    #[test]
    fn developer_concurrency_is_bounded_without_oversubscription() {
        let active = AtomicUsize::new(0);
        assert!(acquire_developer_slot(&active, 2));
        assert!(acquire_developer_slot(&active, 2));
        assert!(!acquire_developer_slot(&active, 2));
        assert_eq!(active.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn global_headers_cors_and_preflight_match_fastify_foundation() {
        let app = fixture_router(fixture_state(&[]));
        let response = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/health")
                    .header(ORIGIN, "https://paladinscat.com")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("https://paladinscat.com"))
        );
        assert_eq!(
            response.headers().get("x-content-type-options"),
            Some(&HeaderValue::from_static("nosniff"))
        );
        assert!(
            response
                .headers()
                .get("server-timing")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("app;dur="))
        );
        assert_eq!(response.headers().get("x-ratelimit-limit"), None);

        let preflight = app
            .oneshot(
                HttpRequest::builder()
                    .method(Method::OPTIONS)
                    .uri("/health")
                    .header(ORIGIN, "https://paladinscat.com")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
        assert_eq!(preflight.headers().get("x-content-type-options"), None);
        assert_eq!(
            preflight.headers().get(ACCESS_CONTROL_ALLOW_METHODS),
            Some(&HeaderValue::from_static("GET,HEAD,PUT,PATCH,POST,DELETE"))
        );
        assert!(preflight.headers().contains_key("server-timing"));
    }

    #[tokio::test]
    async fn v1_authentication_allowlist_and_headers_match_typescript() {
        let key = format!("pc_test_{}", "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc");
        let hash = {
            let digest = Sha256::digest(key.as_bytes());
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        let app = fixture_router(fixture_state(&[(
            "PALADINSCAT_DEVELOPER_API_KEY_SHA256",
            hash,
        )]));

        let anonymous_read = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/stats/champions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(anonymous_read.status(), StatusCode::OK);
        assert_eq!(
            anonymous_read.headers().get("x-paladinscat-api-version"),
            Some(&HeaderValue::from_static("v1"))
        );
        assert_eq!(anonymous_read.headers().get(CACHE_CONTROL), None);
        assert_eq!(
            anonymous_read.headers().get("x-ratelimit-limit").unwrap(),
            "300"
        );
        let anonymous_body = response_json(anonymous_read).await;
        let legacy_read = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/stats/champions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(legacy_read.status(), StatusCode::OK);
        assert_eq!(anonymous_body, response_json(legacy_read).await);

        let missing = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/v1/players/716515038/refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(missing.headers().get("x-ratelimit-limit").unwrap(), "300");
        assert_eq!(
            missing.headers().get(CACHE_CONTROL),
            Some(&HeaderValue::from_static("private, no-store"))
        );
        assert_eq!(
            response_json(missing).await["error"]["code"],
            "INVALID_API_KEY"
        );

        let allowed = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/v1/players/716515038/refresh")
                    .header(AUTHORIZATION, format!("Bearer {key}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(allowed.status(), StatusCode::OK);
        assert_eq!(allowed.headers().get("x-ratelimit-limit").unwrap(), "300");
        assert_eq!(
            allowed
                .headers()
                .get("x-developer-ratelimit-limit")
                .unwrap(),
            "120"
        );
        assert_eq!(response_json(allowed).await, json!({"refreshed": true}));

        let forbidden = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/recovery/pending")
                    .header(AUTHORIZATION, format!("Bearer {key}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(forbidden.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response_json(forbidden).await["error"]["code"],
            "API_ROUTE_NOT_FOUND"
        );

        let anonymous = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(anonymous.status(), StatusCode::OK);
        assert_eq!(anonymous.headers().get(CACHE_CONTROL), None);
        assert_eq!(anonymous.headers().get("x-ratelimit-limit").unwrap(), "300");
        assert!(
            !anonymous
                .headers()
                .get(VARY)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.contains("Authorization"))
        );
    }

    #[tokio::test]
    async fn v1_side_by_side_http_e2e_matches_the_legacy_payload() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fixture HTTP server");
        let address = listener.local_addr().expect("fixture address");
        let server = tokio::spawn(async move {
            axum::serve(listener, fixture_router(fixture_state(&[])))
                .await
                .expect("serve fixture router");
        });
        let client = reqwest::Client::new();
        let legacy = client
            .get(format!("http://{address}/stats/champions"))
            .send()
            .await
            .expect("legacy HTTP response");
        let v1 = client
            .get(format!("http://{address}/v1/stats/champions"))
            .send()
            .await
            .expect("v1 HTTP response");

        assert_eq!(legacy.status(), StatusCode::OK);
        assert_eq!(v1.status(), StatusCode::OK);
        assert_eq!(v1.headers()["x-paladinscat-api-version"], "v1");
        assert_eq!(v1.headers()["x-ratelimit-limit"], "300");
        assert_eq!(
            legacy.json::<Value>().await.unwrap(),
            v1.json::<Value>().await.unwrap()
        );

        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn v1_pilot_is_wired_through_the_production_router() {
        let legacy = crate::candidate_router(fixture_state(&[]))
            .oneshot(
                HttpRequest::builder()
                    .uri("/stats/champions")
                    .body(Body::empty())
                    .expect("legacy request"),
            )
            .await
            .expect("legacy response");
        let v1 = crate::candidate_router(fixture_state(&[]))
            .oneshot(
                HttpRequest::builder()
                    .uri("/v1/stats/champions")
                    .body(Body::empty())
                    .expect("v1 request"),
            )
            .await
            .expect("v1 response");

        assert_ne!(legacy.status(), StatusCode::NOT_FOUND);
        assert_eq!(legacy.status(), v1.status());
        assert_eq!(v1.headers()["x-paladinscat-api-version"], "v1");
    }

    #[tokio::test]
    async fn unconfigured_developer_api_fails_before_key_parsing() {
        let response = fixture_router(fixture_state(&[]))
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/v1/players/716515038/refresh")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_json(response).await["error"]["code"],
            "DEVELOPER_API_NOT_CONFIGURED"
        );
    }

    #[tokio::test]
    async fn blocking_deployment_rejects_public_work_before_rate_limits() {
        let mut state = fixture_state(&[]);
        state.deployment = DeploymentControl::with_local_state(
            state.redis.clone(),
            DeploymentState {
                id: "deploy-1".to_owned(),
                phase: DeploymentPhase::Draining,
                message: Some("Updating".to_owned()),
                started_at: Some("2099-01-01T00:00:00.000Z".to_owned()),
                updated_at: "2099-01-01T00:00:00.000Z".to_owned(),
                expires_at: Some("2099-01-01T01:00:00.000Z".to_owned()),
            },
        );
        let response = fixture_router(state)
            .oneshot(
                HttpRequest::builder()
                    .uri("/stats/champions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "5");
        assert_eq!(response.headers().get("x-ratelimit-limit"), None);
        assert_eq!(
            response_json(response).await["error"]["details"]["phase"],
            "draining"
        );
    }

    #[tokio::test]
    async fn sensitive_legacy_routes_require_operator_credentials() {
        let app = fixture_router(fixture_state(&[("ADMIN_SECRET", "operator".to_owned())]));
        let denied = app
            .clone()
            .oneshot(
                HttpRequest::builder()
                    .uri("/recovery/pending")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_json(denied).await["error"]["code"],
            "OPERATOR_AUTH_REQUIRED"
        );
        let allowed = app
            .oneshot(
                HttpRequest::builder()
                    .uri("/recovery/pending")
                    .header(AUTHORIZATION, "Bearer operator")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(allowed.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn authentication_guard_preserves_limit_headers_on_success_and_failure() {
        for path in ["/auth/login", "/auth/account/password"] {
            let allowed = fixture_router(fixture_state(&[]))
                .oneshot(
                    HttpRequest::builder()
                        .method(Method::POST)
                        .uri(path)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(allowed.status(), StatusCode::OK, "{path}");
            assert_eq!(
                allowed.headers().get("x-auth-ratelimit-limit"),
                Some(&HeaderValue::from_static("10")),
                "{path}"
            );
            assert_eq!(
                allowed.headers().get("x-auth-ratelimit-remaining"),
                Some(&HeaderValue::from_static("9")),
                "{path}"
            );
        }

        let oidc = fixture_router(fixture_state(&[]))
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/auth/oidc/transactions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(oidc.status(), StatusCode::OK);
        assert_eq!(
            oidc.headers().get("x-auth-ratelimit-limit"),
            Some(&HeaderValue::from_static("30"))
        );

        for (backend_available, status, code) in [
            (
                false,
                StatusCode::SERVICE_UNAVAILABLE,
                "PROTECTION_UNAVAILABLE",
            ),
            (true, StatusCode::TOO_MANY_REQUESTS, "AUTH_RATE_LIMITED"),
        ] {
            let mut state = fixture_state(&[]);
            state.rate_limits = Arc::new(AuthenticationRateLimits {
                auth: RateLimitResult {
                    remaining: 0,
                    total: 10,
                    reset_at_ms: unix_time_ms().saturating_add(60_000),
                    allowed: false,
                    backend_available,
                },
                oidc: None,
            });
            for path in ["/auth/login", "/auth/account/password"] {
                let response = fixture_router(state.clone())
                    .oneshot(
                        HttpRequest::builder()
                            .method(Method::POST)
                            .uri(path)
                            .body(Body::empty())
                            .expect("request"),
                    )
                    .await
                    .expect("response");
                assert_eq!(response.status(), status, "{path}");
                assert_eq!(
                    response.headers().get("x-auth-ratelimit-limit").unwrap(),
                    "10",
                    "{path}"
                );
                assert_eq!(
                    response_json(response).await["error"]["code"],
                    Value::String(code.to_owned()),
                    "{path}"
                );
            }
        }

        let mut state = fixture_state(&[]);
        state.rate_limits = Arc::new(AuthenticationRateLimits {
            auth: RateLimitResult {
                remaining: 0,
                total: 10,
                reset_at_ms: unix_time_ms().saturating_add(60_000),
                allowed: false,
                backend_available: true,
            },
            oidc: Some(RateLimitResult {
                remaining: 0,
                total: OIDC_START_LIMIT,
                reset_at_ms: unix_time_ms().saturating_add(OIDC_START_WINDOW_MS),
                allowed: false,
                backend_available: true,
            }),
        });
        let oidc = fixture_router(state)
            .oneshot(
                HttpRequest::builder()
                    .method(Method::POST)
                    .uri("/auth/oidc/transactions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(oidc.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response_json(oidc).await["error"]["code"],
            "OIDC_START_RATE_LIMITED"
        );
    }

    #[tokio::test]
    async fn blocked_client_does_not_consume_global_capacity() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut state = fixture_state(&[]);
        state.rate_limits = Arc::new(RecordingPublicRateLimits {
            seen: seen.clone(),
            reject_client: true,
            reject_global: false,
        });
        let response = fixture_router(state)
            .oneshot(
                HttpRequest::builder()
                    .uri("/stats/champions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get("x-ratelimit-limit").unwrap(), "300");
        let seen = seen.lock().expect("rate trace");
        assert_eq!(seen.len(), 1);
        assert!(seen[0].starts_with("client:"));
    }

    #[tokio::test]
    async fn global_rejection_retains_the_successful_client_quota_headers() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut state = fixture_state(&[]);
        state.rate_limits = Arc::new(RecordingPublicRateLimits {
            seen: seen.clone(),
            reject_client: false,
            reject_global: true,
        });
        let response = fixture_router(state)
            .oneshot(
                HttpRequest::builder()
                    .uri("/stats/champions")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get("x-ratelimit-limit").unwrap(), "300");
        assert_eq!(
            response.headers().get("x-global-ratelimit-limit").unwrap(),
            "6000"
        );
        let seen = seen.lock().expect("rate trace");
        assert_eq!(seen.len(), 2);
        assert!(seen[0].starts_with("client:"));
        assert_eq!(seen[1], "public-api:global");
    }
}
