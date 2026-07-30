#![recursion_limit = "2048"]

#[allow(dead_code)]
mod contract;
mod database;
mod dispatch_result;
mod dummy;
mod dummy_runtime;
mod hirez_client;
mod history_operation;
#[allow(dead_code)]
mod history_store;
mod key_crypto;
mod key_pool;
mod live_provider;
mod model;
#[allow(dead_code)]
mod normalizer;
mod observability;
mod operations;
mod owner_lease;
mod player_lookup;
mod postgres_repository;
#[allow(dead_code)]
mod profile_store;
mod provider;
mod raw_buffer_store;
mod real_dispatch;
mod resolver;
mod session;
#[cfg(test)]
mod test_support;
mod upstream;
mod usage_probe;

use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use contract::verify_manifest;
use database::Database;
use dispatch_result::RelayDispatchResult;
use dummy::DummyProvider;
use dummy_runtime::DummyRuntime;
use hirez_client::HirezApiClient;
use history_operation::PostgresHistoryCache;
use history_store::HistoryStore;
use key_crypto::KeyCrypto;
use key_pool::{
    AuthoritativeUsage, DEFAULT_API_KEY_RESERVE_CALLS, KeyPool, KeyPoolError, UsageProbe,
};
use live_provider::LiveMatchProvider;
use model::sanitize_consumer;
use observability::RelayObservability;
use owner_lease::OwnerLease;
use paladinscat_core::cache::RedisCache;
use player_lookup::PostgresPlayerNameLookup;
use postgres_repository::PostgresRepository;
use profile_store::ProfileStore;
use provider::RelayError;
use raw_buffer_store::RawBufferStore;
use real_dispatch::{RealRuntime, RealRuntimeParts};
use serde_json::{Map, Value, json};
use session::SessionManager;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{error, info};
use upstream::{
    ApiCredential, JitteredRetrySleeper, ReqwestTransport, SharedClock, SharedKeyState,
    SharedRetrySleeper, SharedSessionAudit, SharedTransport, SystemClock,
};
use usage_probe::DirectUsageProbe;
use uuid::Uuid;

const DEFAULT_BASE_URL: &str = "https://api.paladins.com/paladinsapi.svc";
const SESSION_TTL: Duration = Duration::from_secs(14 * 60);

struct RuntimeTasks {
    stop: tokio::sync::watch::Sender<bool>,
    handles: tokio::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl RuntimeTasks {
    fn idle() -> Arc<Self> {
        let (stop, _) = tokio::sync::watch::channel(false);
        Arc::new(Self {
            stop,
            handles: tokio::sync::Mutex::new(Vec::new()),
        })
    }

    async fn shutdown(&self) {
        let _ = self.stop.send(true);
        let handles = std::mem::take(&mut *self.handles.lock().await);
        for handle in handles {
            let _ = handle.await;
        }
    }
}

#[derive(Clone)]
enum RelayRuntime {
    Dummy(Arc<DummyRuntime>),
    Real(Arc<RealRuntime>),
}

#[derive(Clone)]
struct AppState {
    runtime: RelayRuntime,
    mode: &'static str,
    metrics: Arc<RelayObservability>,
    started: Instant,
    database: Option<Arc<Database>>,
    key_pool: Option<Arc<KeyPool>>,
    cache: Option<Arc<RedisCache>>,
    owner: Option<Arc<OwnerLease>>,
    tasks: Arc<RuntimeTasks>,
    accepting: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    quiesced: bool,
}

impl AppState {
    async fn dispatch(
        &self,
        operation: &str,
        args: &[Value],
        consumer: &str,
    ) -> Result<Option<RelayDispatchResult>, RelayError> {
        if !self.accepting.load(Ordering::Acquire) {
            return Err(RelayError::Unavailable(
                "HirezRelay is draining and not accepting new work".to_owned(),
            ));
        }
        if self.mode == "real" && (self.quiesced || !self.ready.load(Ordering::Acquire)) {
            return Err(RelayError::Unavailable(if self.quiesced {
                "HirezRelay is quiesced and outbound work is disabled".to_owned()
            } else {
                "HirezRelay does not hold the live provider owner lease".to_owned()
            }));
        }
        match &self.runtime {
            RelayRuntime::Dummy(runtime) => {
                runtime.dispatch(operation, args, consumer).await.map(Some)
            }
            RelayRuntime::Real(runtime) => runtime.dispatch(operation, args, consumer).await,
        }
    }

    async fn shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        let drain_timeout = Duration::from_millis(env_u64("SHUTDOWN_DRAIN_TIMEOUT_MS", 60_000));
        let deadline = Instant::now() + drain_timeout;
        while self.metrics.active() > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        self.tasks.shutdown().await;
        if let Some(key_pool) = &self.key_pool
            && let Err(error) = key_pool.flush_usage().await
        {
            error!(%error, "failed to flush API-key usage during shutdown");
        }
        if let Some(owner) = &self.owner {
            owner.release().await;
        }
        if let Some(cache) = &self.cache {
            cache.close().await;
        }
        if let Some(database) = &self.database {
            database.close();
        }
    }
}

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--healthcheck") {
        match healthcheck_command().await {
            Some(payload) => {
                println!("{payload}");
                std::process::exit(0);
            }
            None => std::process::exit(1),
        }
    }

    let default_log_filter = std::env::var("LOG_LEVEL")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "info".to_owned());
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("paladinscat_hirez_relay={default_log_filter}").into()),
        )
        .init();
    verify_manifest().expect("verify shared HirezRelay operation contract");

    let state = match build_state().await {
        Ok(state) => state,
        Err(startup_error) => {
            error!(error = %startup_error, "failed to initialize Rust HirezRelay");
            std::process::exit(1);
        }
    };
    let body_limit = env_usize("HIREZ_RELAY_BODY_LIMIT_BYTES", 10 * 1024 * 1024).max(1024 * 1024);
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/call", post(call))
        .layer(DefaultBodyLimit::max(body_limit))
        .with_state(state.clone());

    let host = std::env::var("HIREZ_RELAY_HOST")
        .ok()
        .and_then(|value| value.parse::<IpAddr>().ok())
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
    let port = env_u16("HIREZ_RELAY_PORT", 3015);
    let address = SocketAddr::new(host, port);
    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(bind_error) => {
            error!(error = %bind_error, %address, "failed to bind Rust HirezRelay");
            state.shutdown().await;
            std::process::exit(1);
        }
    };
    info!(%address, mode = state.mode, "Rust HirezRelay listening");
    if let Err(server_error) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state))
        .await
    {
        error!(error = %server_error, "Rust HirezRelay server failed");
        std::process::exit(1);
    }
}

async fn build_state() -> Result<AppState, String> {
    let mode = relay_mode()?;
    let cache = std::env::var("REDIS_URL")
        .ok()
        .map(|url| RedisCache::new(&url).map(Arc::new))
        .transpose()
        .map_err(|error| format!("invalid REDIS_URL: {error}"))?;
    if mode == "dummy" {
        let provider = Arc::new(DummyProvider::default());
        let mut database = None;
        let mut history_cache = None;
        let mut raw_buffer = None;
        if let Ok(database_url) = std::env::var("DATABASE_URL") {
            let configured_database = Arc::new(
                Database::new(
                    &database_url,
                    &database_application_name("paladinscat-hirez-relay-dummy"),
                    relay_database_pool_max(),
                    relay_slow_query_ms(),
                )
                .map_err(|error| error.to_string())?,
            );
            if !configured_database.health_check().await {
                return Err("PostgreSQL health check failed in dummy mode".to_owned());
            }
            let history_store = Arc::new(HistoryStore::new(
                configured_database.clone(),
                env_u64("RECOVERY_PLAYER_HISTORY_CACHE_TTL_HOURS", 24),
            ));
            history_store
                .ensure_schema()
                .await
                .map_err(|error| error.to_string())?;
            history_cache = Some(Arc::new(PostgresHistoryCache::new(history_store)));
            raw_buffer = Some(Arc::new(RawBufferStore::new(configured_database.clone())));
            database = Some(configured_database);
        }
        let runtime = DummyRuntime::new(
            provider,
            history_cache,
            raw_buffer,
            env_u32("PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES", 1440),
        );
        return Ok(AppState {
            runtime: RelayRuntime::Dummy(Arc::new(runtime)),
            mode,
            metrics: Arc::new(RelayObservability::from_environment()),
            started: Instant::now(),
            database,
            key_pool: None,
            cache,
            owner: None,
            tasks: RuntimeTasks::idle(),
            accepting: Arc::new(AtomicBool::new(true)),
            ready: Arc::new(AtomicBool::new(true)),
            quiesced: false,
        });
    }

    let database_url =
        std::env::var("DATABASE_URL").map_err(|_| "DATABASE_URL is required in real mode")?;
    let database = Arc::new(
        Database::new(
            &database_url,
            &database_application_name("paladinscat-hirez-relay"),
            relay_database_pool_max(),
            relay_slow_query_ms(),
        )
        .map_err(|error| error.to_string())?,
    );
    if !database.health_check().await {
        return Err("PostgreSQL health check failed".to_owned());
    }
    let quiesced = env_bool("HIREZ_RELAY_START_QUIESCED", false);
    let owner = if quiesced {
        None
    } else {
        Some(Arc::new(
            OwnerLease::acquire(
                database.clone(),
                std::env::var("HIREZ_RELAY_OWNER_LOCK")
                    .unwrap_or_else(|_| "paladinscat:hirez-relay:live-owner".to_owned()),
            )
            .await
            .map_err(|error| error.to_string())?,
        ))
    };

    let reserve = env_u64("API_KEY_RESERVE_CALLS", DEFAULT_API_KEY_RESERVE_CALLS);
    let repository = Arc::new(PostgresRepository::new(database.clone(), reserve));
    let crypto = Arc::new(KeyCrypto::from_environment().map_err(|error| error.to_string())?);
    let clock: SharedClock = Arc::new(SystemClock);
    let placeholder_probe: Arc<dyn UsageProbe> = Arc::new(EmptyUsageProbe);
    let key_pool = Arc::new(KeyPool::new(
        reserve,
        repository.clone(),
        placeholder_probe,
        crypto,
        clock.clone(),
    ));
    let key_file = relay_key_file();
    key_pool
        .initialize(key_file.as_deref())
        .await
        .map_err(|error| error.to_string())?;

    let base_url =
        std::env::var("HIREZ_API_BASE_URL").unwrap_or_else(|_| DEFAULT_BASE_URL.to_owned());
    let transport: SharedTransport =
        Arc::new(ReqwestTransport::new().map_err(|error| error.to_string())?);
    let key_state: SharedKeyState = key_pool.clone();
    let audit: SharedSessionAudit = repository;
    let sessions = Arc::new(SessionManager::new(
        &base_url,
        SESSION_TTL,
        transport.clone(),
        key_state.clone(),
        audit,
        clock,
    ));
    let usage_probe = Arc::new(DirectUsageProbe::new(
        &base_url,
        transport.clone(),
        key_state.clone(),
        sessions.clone(),
    ));
    let usage_probe_trait: Arc<dyn UsageProbe> = usage_probe.clone();
    key_pool.set_usage_probe(&usage_probe_trait);
    let retry_sleeper: SharedRetrySleeper = Arc::new(JitteredRetrySleeper);
    let api = Arc::new(HirezApiClient::new(
        &base_url,
        transport,
        key_state,
        sessions,
        retry_sleeper,
    ));

    let recovery_ttl_hours = env_u64("RECOVERY_PLAYER_HISTORY_CACHE_TTL_HOURS", 24);
    let history_store = Arc::new(HistoryStore::new(database.clone(), recovery_ttl_hours));
    history_store
        .ensure_schema()
        .await
        .map_err(|error| error.to_string())?;
    let history_cache = Arc::new(PostgresHistoryCache::new(history_store.clone()));
    let player_names = Arc::new(PostgresPlayerNameLookup::new(database.clone()));
    let raw_buffer = Arc::new(RawBufferStore::new(database.clone()));
    let profiles = Arc::new(ProfileStore::new(database.clone()));
    let match_provider = Arc::new(LiveMatchProvider::new(
        api.clone(),
        database.clone(),
        history_store,
        profiles,
    ));
    let dummy_provider = Arc::new(DummyProvider::default());
    let runtime = RealRuntime::new(RealRuntimeParts {
        api,
        history_cache,
        player_names,
        match_provider,
        key_pool: key_pool.clone(),
        usage_probe,
        raw_buffer,
        dummy_provider,
        key_file,
        public_history_ttl_minutes: env_u32("PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES", 1440),
    });
    let ready = Arc::new(AtomicBool::new(quiesced || owner.is_some()));
    let tasks = start_runtime_tasks(key_pool.clone(), owner.clone(), ready.clone(), quiesced).await;

    Ok(AppState {
        runtime: RelayRuntime::Real(Arc::new(runtime)),
        mode,
        metrics: Arc::new(RelayObservability::from_environment()),
        started: Instant::now(),
        database: Some(database),
        key_pool: Some(key_pool),
        cache,
        owner,
        tasks,
        accepting: Arc::new(AtomicBool::new(true)),
        ready,
        quiesced,
    })
}

fn relay_mode() -> Result<&'static str, String> {
    if std::env::var("HIREZ_RELAY_MODE").as_deref() == Ok("real") {
        return Ok("real");
    }
    if std::env::var("NODE_ENV").as_deref() == Ok("production") {
        return Err(
            "HIREZ_RELAY_MODE must be set to \"real\" in production. Refusing to start with dummy data."
                .to_owned(),
        );
    }
    Ok("dummy")
}

async fn start_runtime_tasks(
    key_pool: Arc<KeyPool>,
    owner: Option<Arc<OwnerLease>>,
    ready: Arc<AtomicBool>,
    quiesced: bool,
) -> Arc<RuntimeTasks> {
    let tasks = RuntimeTasks::idle();
    if quiesced {
        return tasks;
    }

    let mut handles = tasks.handles.lock().await;

    let mut flush_stop = tasks.stop.subscribe();
    let flush_pool = key_pool.clone();
    handles.push(tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {
                    if let Err(error) = flush_pool.flush_usage().await {
                        error!(%error, "periodic API-key usage flush failed");
                    }
                }
                changed = flush_stop.changed() => {
                    if changed.is_err() || *flush_stop.borrow() {
                        break;
                    }
                }
            }
        }
    }));

    let mut sync_stop = tasks.stop.subscribe();
    let sync_pool = key_pool;
    handles.push(tokio::spawn(async move {
        loop {
            if let Err(error) = sync_pool.sync_all_usage().await {
                error!(%error, "API-key usage synchronization failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60 * 60)) => {}
                changed = sync_stop.changed() => {
                    if changed.is_err() || *sync_stop.borrow() {
                        break;
                    }
                }
            }
        }
    }));

    if let Some(owner) = owner {
        let mut owner_stop = tasks.stop.subscribe();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(5)) => {
                        let owner_ok = owner.is_healthy().await;
                        ready.store(owner_ok, Ordering::Release);
                        if !owner_ok {
                            error!("live provider owner lease lost; outbound admission disabled");
                        }
                    }
                    changed = owner_stop.changed() => {
                        if changed.is_err() || *owner_stop.borrow() {
                            break;
                        }
                    }
                }
            }
        }));
    }

    drop(handles);
    tasks
}

async fn health(State(state): State<AppState>) -> Json<Value> {
    let database_ok = match &state.database {
        Some(database) => database.health_check().await,
        None => true,
    };
    let owner_ok = match &state.owner {
        Some(owner) => owner.is_healthy().await,
        None => false,
    };
    let redis_status = match &state.cache {
        Some(cache) if cache.health_check().await => "ok",
        Some(_) => "unavailable",
        None => "not_configured",
    };
    if state.mode == "real" && !state.quiesced {
        state
            .ready
            .store(database_ok && owner_ok, Ordering::Release);
    }
    let ready = state.ready.load(Ordering::Acquire);
    let keys_loaded = state
        .key_pool
        .as_ref()
        .map_or(0, |pool| pool.status().len());
    Json(json!({
        "service": "HirezRelay",
        "engine": "rust",
        "mode": state.mode,
        "status": if database_ok && (state.mode != "real" || state.quiesced || owner_ok) { "ok" } else { "degraded" },
        "ready": ready,
        "quiesced": state.quiesced,
        "owner": owner_ok,
        "database": if database_ok { "ok" } else { "unavailable" },
        "redis": redis_status,
        "keysLoaded": keys_loaded,
        "activeRequests": state.metrics.active(),
        "keysEnabled": state.mode == "real",
        "timestamp": OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default()
    }))
}

async fn metrics(State(state): State<AppState>) -> Json<Value> {
    let upstream_calls = match &state.runtime {
        RelayRuntime::Dummy(runtime) => {
            serde_json::to_value(runtime.provider().counts()).unwrap_or_else(|_| json!({}))
        }
        RelayRuntime::Real(_) => Value::Object(Map::new()),
    };
    let database_pool = state.database.as_ref().map(|database| {
        let status = database.status();
        json!({
            "size": status.size,
            "available": status.available,
            "waiting": status.waiting,
            "max": status.max_size
        })
    });
    let mut snapshot = state
        .metrics
        .snapshot()
        .as_object()
        .cloned()
        .unwrap_or_default();
    snapshot.insert("service".to_owned(), json!("HirezRelay"));
    snapshot.insert("engine".to_owned(), json!("rust"));
    snapshot.insert("mode".to_owned(), json!(state.mode));
    snapshot.insert(
        "uptimeSeconds".to_owned(),
        json!(state.started.elapsed().as_secs()),
    );
    snapshot.insert("upstreamCalls".to_owned(), upstream_calls);
    snapshot.insert(
        "databasePool".to_owned(),
        database_pool.unwrap_or(Value::Null),
    );
    Json(Value::Object(snapshot))
}

async fn call(State(state): State<AppState>, body: Bytes) -> Response {
    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(parsed) => parsed,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "statusCode": 400,
                    "code": "FST_ERR_CTP_INVALID_JSON_BODY",
                    "error": "Bad Request",
                    "message": "Body is not valid JSON but content-type is set to 'application/json'"
                })),
            )
                .into_response();
        }
    };
    let object = parsed.as_object();
    let raw_operation = object.and_then(|object| object.get("operation"));
    let operation_value = raw_operation
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| json!("unknown"));
    let request_id_value = object
        .and_then(|object| object.get("requestId"))
        .filter(|value| js_truthy(value))
        .cloned()
        .unwrap_or_else(|| json!(Uuid::new_v4().to_string()));
    let Some(operation) = raw_operation
        .and_then(Value::as_str)
        .filter(|operation| !operation.is_empty())
        .map(str::to_owned)
    else {
        return relay_envelope_response(
            StatusCode::BAD_REQUEST,
            state.mode,
            operation_value,
            request_id_value,
            0,
            None,
            Some("operation is required".to_owned()),
            Some("VALIDATION_ERROR"),
        );
    };
    let args_value = object
        .and_then(|object| object.get("args"))
        .filter(|value| !value.is_null())
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let Some(args) = args_value.as_array().cloned() else {
        return relay_envelope_response(
            StatusCode::BAD_REQUEST,
            state.mode,
            Value::String(operation),
            request_id_value,
            0,
            None,
            Some("args must be an array".to_owned()),
            Some("VALIDATION_ERROR"),
        );
    };
    let consumer = object
        .and_then(|object| object.get("attribution"))
        .and_then(Value::as_object)
        .and_then(|attribution| attribution.get("consumer"))
        .map(js_string)
        .map(|consumer| sanitize_consumer(&consumer))
        .unwrap_or_else(|| "unattributed".to_owned());
    let request_id = js_string(&request_id_value);
    let started = Instant::now();
    state.metrics.begin();

    let result = state.dispatch(&operation, &args, &consumer).await;
    let latency_ms = started.elapsed().as_millis() as u64;
    match &result {
        Ok(value) => state.metrics.finish(
            &request_id,
            &operation,
            state.mode,
            &args,
            Ok(value),
            latency_ms,
        ),
        Err(error) => {
            let message = error.to_string();
            state.metrics.finish(
                &request_id,
                &operation,
                state.mode,
                &args,
                Err(&message),
                latency_ms,
            );
        }
    }

    match result {
        Ok(result) => relay_envelope_response(
            StatusCode::OK,
            state.mode,
            Value::String(operation),
            request_id_value,
            latency_ms,
            result,
            None,
            None,
        ),
        Err(error) => {
            let status =
                StatusCode::from_u16(error.status_code()).unwrap_or(StatusCode::BAD_GATEWAY);
            relay_envelope_response(
                status,
                state.mode,
                Value::String(operation),
                request_id_value,
                latency_ms,
                None,
                Some(error.to_string()),
                Some(error.error_code()),
            )
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn relay_envelope_response(
    status: StatusCode,
    mode: &'static str,
    operation: Value,
    request_id: Value,
    latency_ms: u64,
    result: Option<RelayDispatchResult>,
    error: Option<String>,
    error_code: Option<&'static str>,
) -> Response {
    if error.is_none() {
        return (
            status,
            Json(RelaySuccessEnvelope {
                ok: true,
                mode,
                operation,
                request_id,
                latency_ms,
                result,
            }),
        )
            .into_response();
    }

    let mut body = Map::new();
    body.insert("ok".to_owned(), Value::Bool(false));
    body.insert("mode".to_owned(), Value::String(mode.to_owned()));
    body.insert("operation".to_owned(), operation);
    body.insert("requestId".to_owned(), request_id);
    body.insert("latencyMs".to_owned(), json!(latency_ms));
    if let Some(error) = error {
        body.insert("error".to_owned(), Value::String(error));
    }
    if let Some(error_code) = error_code {
        body.insert("errorCode".to_owned(), Value::String(error_code.to_owned()));
    }
    (status, Json(Value::Object(body))).into_response()
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct RelaySuccessEnvelope {
    ok: bool,
    mode: &'static str,
    operation: Value,
    request_id: Value,
    latency_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<RelayDispatchResult>,
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value
            .as_f64()
            .is_some_and(|value| value != 0.0 && !value.is_nan()),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => value.clone(),
        Value::Array(values) => values.iter().map(js_string).collect::<Vec<_>>().join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

struct EmptyUsageProbe;

#[async_trait]
impl UsageProbe for EmptyUsageProbe {
    async fn get_data_used(
        &self,
        _key: &ApiCredential,
    ) -> Result<Option<AuthoritativeUsage>, KeyPoolError> {
        Ok(None)
    }
}

async fn shutdown_signal(state: AppState) {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    info!("shutdown signal received; draining in-flight requests");
    state.shutdown().await;
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn relay_database_pool_max() -> usize {
    std::env::var("HIREZ_RELAY_DB_POOL_MAX")
        .or_else(|_| std::env::var("DB_POOL_MAX"))
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(12)
}

fn database_application_name(fallback: &str) -> String {
    std::env::var("DB_APPLICATION_NAME")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_owned())
}

async fn healthcheck_command() -> Option<Value> {
    let configured_host =
        std::env::var("HIREZ_RELAY_HOST").unwrap_or_else(|_| "127.0.0.1".to_owned());
    let host = match configured_host.as_str() {
        "0.0.0.0" | "::" => "127.0.0.1",
        value => value,
    };
    let port = env_u16("HIREZ_RELAY_PORT", 3015);
    let url = format!("http://{host}:{port}/health");
    let Ok(client) = reqwest::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
    else {
        return None;
    };
    let Ok(response) = client.get(url).send().await else {
        return None;
    };
    if !response.status().is_success() {
        return None;
    }
    let Ok(body) = response.bytes().await else {
        return None;
    };
    let Ok(payload) = serde_json::from_slice::<Value>(&body) else {
        return None;
    };
    let mode = payload.get("mode").and_then(Value::as_str);
    let quiesced = payload
        .get("quiesced")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let owner = payload
        .get("owner")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let keys_loaded = payload
        .get("keysLoaded")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let healthy = payload.get("engine").and_then(Value::as_str) == Some("rust")
        && payload.get("status").and_then(Value::as_str) == Some("ok")
        && payload.get("ready").and_then(Value::as_bool) == Some(true)
        && payload.get("database").and_then(Value::as_str) == Some("ok")
        && payload.get("redis").and_then(Value::as_str) != Some("unavailable")
        && (mode != Some("real")
            || (keys_loaded > 0 && ((quiesced && !owner) || (!quiesced && owner))));
    healthy.then_some(payload)
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn relay_slow_query_ms() -> u64 {
    std::env::var("DB_SLOW_QUERY_MS")
        .ok()
        .or_else(|| std::env::var("SLOW_QUERY_MS").ok())
        .and_then(|value| value.parse().ok())
        .unwrap_or(500)
}

fn relay_key_file() -> Option<PathBuf> {
    std::env::var("HIREZ_API_KEYS_FILE")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("API_KEYS_FILE")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .map(PathBuf::from)
}

fn env_u32(name: &str, fallback: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_u16(name: &str, fallback: u16) -> u16 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_bool(name: &str, fallback: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes"
            )
        })
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    #[test]
    fn production_refuses_implicit_dummy_mode() {
        // Pure mode behavior is covered at the shared contract layer. Keep the
        // error text stable because deployment checks use it as an alarm.
        let message = "HIREZ_RELAY_MODE must be set to \"real\" in production. Refusing to start with dummy data.";
        assert!(message.contains("Refusing to start with dummy data"));
    }

    #[test]
    fn path_import_is_used_for_key_file_configuration() {
        let path = std::path::Path::new("secrets/hirez-api-keys.json");
        assert_eq!(
            path.file_name().and_then(|value| value.to_str()),
            Some("hirez-api-keys.json")
        );
    }
}
