use std::{collections::HashMap, sync::Arc};

use aes_gcm::{
    Aes256Gcm, KeyInit,
    aead::{Aead, AeadCore, OsRng},
};
use axum::{
    Json, Router,
    extract::{Extension, Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use paladinscat_core::{cache::RedisCache, config::BackendConfig, database::Database};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    error::ApiError,
    request::RequestId,
    security::developer_bearer_token,
    workers::{
        coordination::{SCHEDULER_KEYS, WorkerCoordinationRepository},
        scheduler::SCHEDULED_JOBS,
    },
};

use super::identity::{as_i64, json_response, simple_error};

#[derive(Clone)]
struct SystemState {
    database: Database,
    redis: RedisCache,
    config: Arc<BackendConfig>,
}

pub fn router(database: Database, redis: RedisCache, config: Arc<BackendConfig>) -> Router {
    Router::new()
        .route("/status", get(status))
        .route("/system/hirez-status", get(hirez_status))
        .route("/hirez-status", get(hirez_status))
        .route("/schedulers", get(schedulers))
        .route("/database", get(database_status))
        .route("/cache/flush", post(flush_cache))
        .route("/cache/flush/{pattern}", post(flush_pattern))
        .route("/api-keys/encrypt", post(encrypt_key))
        .route("/api-keys/status", get(api_key_status))
        .with_state(SystemState {
            database,
            redis,
            config,
        })
}

async fn status(
    State(state): State<SystemState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let scalar = |row: Option<Value>, field: &str| {
        row.as_ref()
            .and_then(|row| row.get(field))
            .cloned()
            .unwrap_or(Value::Null)
    };
    let matches = state
        .database
        .one_json("SELECT COUNT(*)::bigint AS count FROM matches", &[])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let players = state
        .database
        .one_json("SELECT COUNT(*)::bigint AS count FROM players", &[])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let pulls = state
        .database
        .one_json("SELECT COUNT(*)::bigint AS count FROM match_pull_list", &[])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let last = state
        .database
        .one_json(
            "SELECT entry_datetime FROM matches ORDER BY entry_datetime DESC LIMIT 1",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let buffers = state
        .database
        .query_json(
            "SELECT status,COUNT(*)::bigint AS count FROM raw_ingest_buffer GROUP BY status",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let views = state
        .database
        .query_json(
            "SELECT relname AS mv_name,last_autoanalyze AS last_updated FROM pg_stat_user_tables \
             WHERE relname IN('mv_player_coplay_stats','tier_population_stats','player_rankings') ORDER BY relname",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({
            "matches":scalar(matches,"count"),
            "players":scalar(players,"count"),
            "pendingPulls":scalar(pulls,"count"),
            "lastMatch":scalar(last,"entry_datetime"),
            "bufferStats":buffers,
            "mvFreshness":views,
            "timestamp":OffsetDateTime::now_utc().format(&Rfc3339).ok()
        }),
    ))
}

async fn table_exists(
    database: &Database,
    table: &str,
    request_id: &RequestId,
) -> Result<bool, ApiError> {
    let qualified = format!("public.{table}");
    let row = database
        .one_json(
            "SELECT to_regclass($1) IS NOT NULL AS exists",
            &[&qualified],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(row
        .as_ref()
        .and_then(|row| row.get("exists"))
        .and_then(Value::as_bool)
        .unwrap_or(false))
}

fn classify(
    message: &str,
) -> Option<(
    &'static str,
    &'static str,
    &'static str,
    &'static str,
    &'static str,
)> {
    let lower = message.to_ascii_lowercase();
    if lower.contains("server_regions")
        || lower.contains("##sql_paladins_api")
        || lower.contains("invalid object name")
    {
        return Some((
            "match_detail_server_regions",
            "HIREZ_DETAIL_SERVER_REGIONS",
            "Hi-Rez match detail outage",
            "critical",
            "Hi-Rez match detail endpoints are returning a server-side error. Ranked match backfill is being held to one safe probe until service recovers.",
        ));
    }
    if [
        "maintenance",
        "temporarily unavailable",
        "service unavailable",
        "http 503",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
    {
        return Some((
            "hirez_api_service_unavailable",
            "HIREZ_SERVICE_UNAVAILABLE",
            "Hi-Rez API service degraded",
            "warning",
            "Hi-Rez API is reporting temporary service issues. Live lookups may be delayed while PaladinsCat keeps using local data.",
        ));
    }
    None
}

async fn hirez_status(
    State(state): State<SystemState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let lookback = std::env::var("HIREZ_OUTAGE_SIGNAL_LOOKBACK_MINUTES")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(30)
        .max(5);
    let active = if table_exists(&state.database, "hirez_service_outage_state", &request_id).await?
    {
        state
            .database
            .query_json(
                "SELECT service_key,status,reason,first_detected_at::text,last_detected_at::text, \
                   next_probe_at::text,probe_count,last_success_at::text,updated_at::text \
                 FROM hirez_service_outage_state WHERE status='active' \
                 ORDER BY CASE service_key WHEN 'match_detail_server_regions' THEN 0 ELSE 1 END,updated_at DESC",
                &[],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
    } else {
        Vec::new()
    };
    let mut signal_rows = Vec::new();
    for (table, message_column) in [
        ("hourly_ingest_match_debt", "reason"),
        ("hourly_ingest_state", "error_message"),
    ] {
        if table_exists(&state.database, table, &request_id).await? {
            let mut rows = state
                .database
                .query_json(
                    &format!(
                        "SELECT '{table}' AS source,{message_column} AS message,updated_at::text AS observed_at \
                         FROM {table} WHERE updated_at>=now()-($1::int*interval '1 minute') \
                         AND {message_column} IS NOT NULL ORDER BY updated_at DESC LIMIT 50"
                    ),
                    &[&lookback],
                )
                .await
                .map_err(|error| ApiError::database(error, &request_id))?;
            signal_rows.append(&mut rows);
        }
    }
    let recent = signal_rows
        .into_iter()
        .filter_map(|row| {
            let message = row.get("message").and_then(Value::as_str)?;
            let (service, code, title, severity, public) = classify(message)?;
            Some(json!({
                "source":row.get("source").cloned().unwrap_or(Value::Null),
                "message":message,
                "observedAt":row.get("observed_at").cloned().unwrap_or(Value::Null),
                "code":code,"serviceKey":service,"title":title,"severity":severity,"publicMessage":public
            }))
        })
        .take(10)
        .collect::<Vec<_>>();
    let debt = if table_exists(&state.database, "hourly_ingest_match_debt", &request_id).await? {
        state
            .database
            .one_json(
                "SELECT COUNT(*) FILTER(WHERE status='pending' AND COALESCE(reason,'') ILIKE '%vendor detail service outage%')::bigint AS pending_vendor_debt, \
                   COUNT(*) FILTER(WHERE status='pending' AND COALESCE(reason,'') ILIKE '%vendor detail service outage%' AND(next_retry_at IS NULL OR next_retry_at<=now()))::bigint AS due_vendor_debt, \
                   COUNT(DISTINCT(date::text||':'||hour::text)) FILTER(WHERE status='pending' AND COALESCE(reason,'') ILIKE '%vendor detail service outage%')::bigint AS affected_hours, \
                   (MIN(next_retry_at) FILTER(WHERE status='pending' AND COALESCE(reason,'') ILIKE '%vendor detail service outage%'))::text AS next_retry_at \
                 FROM hourly_ingest_match_debt",
                &[],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
            .unwrap_or_else(|| json!({}))
    } else {
        json!({})
    };
    let pending = as_i64(debt.get("pending_vendor_debt")).unwrap_or(0);
    let due = as_i64(debt.get("due_vendor_debt")).unwrap_or(0);
    let mut outages = active
        .into_iter()
        .map(|row| {
            let service = row
                .get("service_key")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let reason = row.get("reason").and_then(Value::as_str).unwrap_or_default();
            let classification = classify(reason);
            let severity = classification.map(|value| value.3).unwrap_or("critical");
            let title = classification
                .map(|value| value.2)
                .unwrap_or(if service == "match_detail_server_regions" {
                    "Hi-Rez match detail outage"
                } else {
                    "Hi-Rez API outage"
                });
            let message = classification.map(|value| value.4).unwrap_or(
                "Hi-Rez API is currently degraded. PaladinsCat is serving local data where possible.",
            );
            json!({
                "serviceKey":service,"status":"active","title":title,"severity":severity,"message":message,
                "reason":row.get("reason").cloned().unwrap_or(Value::Null),
                "firstDetectedAt":row.get("first_detected_at").cloned().unwrap_or(Value::Null),
                "lastDetectedAt":row.get("last_detected_at").cloned().unwrap_or(Value::Null),
                "nextProbeAt":row.get("next_probe_at").cloned().unwrap_or(Value::Null),
                "probeDue":row.get("next_probe_at").is_none_or(Value::is_null),
                "probeCount":as_i64(row.get("probe_count")).unwrap_or(0),
                "updatedAt":row.get("updated_at").cloned().unwrap_or(Value::Null)
            })
        })
        .collect::<Vec<_>>();
    if pending > 0 && outages.is_empty() {
        outages.push(json!({
            "serviceKey":"match_detail_server_regions","status":"active","title":"Hi-Rez match detail outage",
            "severity":"critical","message":"Hi-Rez match detail endpoints are currently blocked. PaladinsCat is preserving exact match debt and probing safely.",
            "reason":"pending vendor detail service outage debt","firstDetectedAt":null,"lastDetectedAt":null,
            "nextProbeAt":debt.get("next_retry_at").cloned().unwrap_or(Value::Null),"probeDue":due>0,
            "probeCount":0,"updatedAt":null
        }));
    }
    let outage = outages
        .iter()
        .any(|row| row.get("severity").and_then(Value::as_str) == Some("critical"));
    let status = if outage {
        "outage"
    } else if !outages.is_empty() || !recent.is_empty() {
        "degraded"
    } else {
        "ok"
    };
    let message = outages
        .first()
        .and_then(|row| row.get("message"))
        .cloned()
        .or_else(|| {
            recent
                .first()
                .and_then(|row| row.get("publicMessage"))
                .cloned()
        })
        .unwrap_or_else(|| Value::String("Hi-Rez API is operating normally.".to_owned()));
    Ok(json_response(
        StatusCode::OK,
        json!({
            "status":status,"outage":outage,"degraded":status=="degraded","message":message,
            "activeOutages":outages,"recentSignals":recent,"pendingVendorDebt":pending,
            "dueVendorDebt":due,"affectedHours":as_i64(debt.get("affected_hours")).unwrap_or(0),
            "nextDebtRetryAt":debt.get("next_retry_at").cloned().unwrap_or(Value::Null),
            "signalLookbackMinutes":lookback,
            "timestamp":OffsetDateTime::now_utc().format(&Rfc3339).ok()
        }),
    ))
}

async fn schedulers(
    State(state): State<SystemState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let ownership = WorkerCoordinationRepository::new(state.database.clone())
        .scheduler_ownership()
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let recent = state
        .database
        .query_json(
            "SELECT CASE WHEN job_type='afk_tracker' THEN 'baseline_tracker' ELSE job_type END AS job_type, \
               status,created_at,COALESCE(completed_at,started_at,created_at) AS updated_at \
             FROM sync_jobs WHERE job_type=ANY($1::text[]) ORDER BY created_at DESC LIMIT 20",
            &[&vec![
                "ranked_tracker",
                "ranked-tracker",
                "auto_ingester",
                "baseline_tracker",
                "afk_tracker",
                "derived_projection_tracker",
            ]],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let descriptions = HashMap::from([
        (
            "ranked_tracker",
            "Ranked leaderboard snapshots every 4 hours plus startup catch-up",
        ),
        (
            "auto_ingester",
            "Hourly match discovery, 5-minute raw buffer drain, hourly raw/history retention, and hourly MV refresh",
        ),
        ("baseline_tracker", "Daily role/queue baseline rebuild"),
        (
            "derived_projection_tracker",
            "Daily repair rebuild for local derived projection tables",
        ),
        (
            "hourly_gap_checker",
            "Hourly scan for missed ingest windows using hourly_ingest_state",
        ),
        (
            "tier_stats",
            "Hourly tier_stats snapshot refresh from local tables",
        ),
    ]);
    let mut output = Map::new();
    for key in SCHEDULER_KEYS {
        let jobs = recent
            .iter()
            .filter(|row| row.get("job_type").and_then(Value::as_str) == Some(key))
            .collect::<Vec<_>>();
        let enabled = ownership
            .iter()
            .any(|owner| owner.scheduler_key == key && owner.engine == "rust");
        output.insert(
            key.to_owned(),
            json!({
                "enabled":enabled,
                "description":descriptions.get(key).copied().unwrap_or_default(),
                "lastRun":jobs.first().and_then(|row|row.get("updated_at").or_else(||row.get("created_at"))).cloned().unwrap_or(Value::Null),
                "lastStatus":jobs.first().and_then(|row|row.get("status")).cloned().unwrap_or(Value::Null),
                "recentRuns":jobs.len(),
                "jobCount":SCHEDULED_JOBS.iter().filter(|job|job.scheduler_key==key).count()
            }),
        );
    }
    Ok(json_response(StatusCode::OK, Value::Object(output)))
}

async fn database_status(
    State(state): State<SystemState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let tables = state
        .database
        .query_json(
            "SELECT relname AS name,n_live_tup AS row_count FROM pg_stat_user_tables \
             WHERE relname IN('matches','players') OR n_live_tup>0 ORDER BY n_live_tup DESC LIMIT 50",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let server = state
        .database
        .one_json(
            "SELECT current_setting('max_connections')::int AS max_connections, \
               current_setting('superuser_reserved_connections')::int AS superuser_reserved_connections, \
               COUNT(*)::bigint AS total_connections,COUNT(*) FILTER(WHERE state='active')::bigint AS active_connections, \
               COUNT(*) FILTER(WHERE state='idle')::bigint AS idle_connections, \
               COUNT(*) FILTER(WHERE state<>'idle' AND wait_event IS NOT NULL)::bigint AS waiting_connections \
             FROM pg_stat_activity WHERE datname=current_database()",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .unwrap_or_else(|| json!({}));
    let pool = state.database.status();
    Ok(json_response(
        StatusCode::OK,
        json!({
            "tables":tables,"server":server,
            "pool":{"max_connections":pool.max_size,"total_connections":pool.size,
                "idle_connections":pool.available,"waiting_requests":pool.waiting},
            "timestamp":OffsetDateTime::now_utc().format(&Rfc3339).ok()
        }),
    ))
}

fn operator(headers: &HeaderMap, config: &BackendConfig) -> bool {
    let Some(candidate) = developer_bearer_token(headers) else {
        return false;
    };
    let Some(expected) = config.admin_secret.as_deref() else {
        return false;
    };
    let candidate: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
    let expected: [u8; 32] = Sha256::digest(expected.as_bytes()).into();
    bool::from(candidate.ct_eq(&expected))
}

fn operator_error() -> Response {
    json_response(
        StatusCode::UNAUTHORIZED,
        json!({"error":{"code":"UNAUTHORIZED","message":"Authentication required"}}),
    )
}

async fn flush_cache(State(state): State<SystemState>, headers: HeaderMap) -> Response {
    if !operator(&headers, &state.config) {
        return operator_error();
    }
    state.redis.flush_database().await;
    json_response(StatusCode::OK, json!({"message":"Cache flushed"}))
}

async fn flush_pattern(
    State(state): State<SystemState>,
    Path(pattern): Path<String>,
    headers: HeaderMap,
) -> Response {
    if !operator(&headers, &state.config) {
        return operator_error();
    }
    if pattern.is_empty() {
        return simple_error(StatusCode::BAD_REQUEST, "Missing pattern parameter");
    }
    let removed = state.redis.delete_pattern(&pattern).await.unwrap_or(0);
    json_response(
        StatusCode::OK,
        json!({"message":"Cache flushed","pattern":pattern,"keysDeleted":removed}),
    )
}

fn mek() -> Option<[u8; 32]> {
    let raw = std::env::var("MEK")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            let path = std::env::var("MEK_FILE").ok()?;
            let raw = std::fs::read_to_string(path).ok()?;
            Some(
                raw.trim()
                    .strip_prefix("MEK=")
                    .unwrap_or(raw.trim())
                    .trim()
                    .to_owned(),
            )
        })?;
    if raw.len() != 64 || !raw.chars().all(|character| character.is_ascii_hexdigit()) {
        return None;
    }
    let mut key = [0_u8; 32];
    for (index, byte) in key.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&raw[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(key)
}

async fn encrypt_key(
    State(state): State<SystemState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if !operator(&headers, &state.config) {
        return operator_error();
    }
    let plaintext = body
        .get("auth_key_plaintext")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if plaintext.is_empty() {
        return simple_error(StatusCode::BAD_REQUEST, "auth_key_plaintext is required");
    }
    let Some(key) = mek() else {
        return simple_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "MEK is not set or invalid",
        );
    };
    let cipher = Aes256Gcm::new_from_slice(&key).expect("AES-256 key");
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let Ok(mut encrypted) = cipher.encrypt(&nonce, plaintext.as_bytes()) else {
        return simple_error(StatusCode::INTERNAL_SERVER_ERROR, "Encryption failed");
    };
    // RustCrypto returns ciphertext + 16-byte GCM tag, matching Node once the
    // 12-byte IV is prepended.
    let mut output = nonce.to_vec();
    output.append(&mut encrypted);
    json_response(
        StatusCode::OK,
        json!({"auth_key_encrypted":BASE64.encode(output)}),
    )
}

fn effective_limit(dev_id: &str, reported: i64) -> i64 {
    let configured = if dev_id == "2116" { 15_000 } else { 7_500 };
    if reported > 0 {
        reported.min(configured)
    } else {
        configured
    }
}

async fn api_key_status(
    State(state): State<SystemState>,
    Extension(request_id): Extension<RequestId>,
) -> Result<Response, ApiError> {
    let reserve = std::env::var("API_KEY_RESERVE_CALLS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|value| *value >= 0)
        .unwrap_or(100);
    let rows = state
        .database
        .query_json(
            "SELECT dev_id,status,total_24h,daily_limit,calls_total,consecutive_failures,last_used \
             FROM api_keys ORDER BY dev_id",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let rows = rows
        .into_iter()
        .map(|row| {
            let dev = row
                .get("dev_id")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let used = as_i64(row.get("total_24h")).unwrap_or(0);
            let limit = effective_limit(dev, as_i64(row.get("daily_limit")).unwrap_or(0));
            let remaining = (limit - used).max(0);
            json!({
                "devId":dev,"status":row.get("status").cloned().unwrap_or(Value::Null),
                "used_24h":used,"daily_limit":limit,"remaining":remaining,"left_calls":remaining,
                "reserve_threshold":reserve,"turns_off_at_remaining":reserve,
                "usable":remaining>reserve && row.get("status").and_then(Value::as_str)==Some("healthy"),
                "calls_total":as_i64(row.get("calls_total")).unwrap_or(0),
                "consecutive_failures":as_i64(row.get("consecutive_failures")).unwrap_or(0),
                "last_used":row.get("last_used").cloned().unwrap_or(Value::Null)
            })
        })
        .collect::<Vec<_>>();
    Ok(json_response(StatusCode::OK, Value::Array(rows)))
}
