use std::collections::HashMap;

use aes_gcm::aead::{OsRng, rand_core::RngCore};
use axum::{
    Json, Router,
    extract::{Extension, Path, Query, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use paladinscat_core::{config::BackendConfig, database::Database};
use regex::Regex;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{
    error::ApiError,
    oidc::OidcVerifier,
    raw_hirez_audit::{RawHirezAudit, record_raw_hirez_response},
    request::RequestId,
    routes::live::{request_identity, vendor_guard},
    workers::relay::WorkerRelayClient,
};

use super::identity::{as_i64, json_response, parse_id, simple_error};

pub const ROUTE_COUNT: usize = 15;

const SESSION_TTL_HOURS: i64 = 72;
const LINK_COOLDOWN_SECONDS: i32 = 30;
const LINK_MAX_ATTEMPTS: i32 = 5;
const LINK_LOCKOUT_MINUTES: i32 = 10;

#[derive(Clone)]
struct AuthState {
    database: Database,
    redis: paladinscat_core::cache::RedisCache,
    relay: Option<WorkerRelayClient>,
    oidc: Option<OidcVerifier>,
    oidc_bff_service_token: Option<String>,
}

pub fn router(
    database: Database,
    redis: paladinscat_core::cache::RedisCache,
    config: std::sync::Arc<BackendConfig>,
) -> Router {
    Router::new()
        .route("/auth/register", post(register))
        .route("/auth/login", post(login))
        .route("/auth/oidc/exchange", post(oidc_exchange))
        .route("/auth/oidc/transactions", post(oidc_transaction_create))
        .route(
            "/auth/oidc/transactions/consume",
            post(oidc_transaction_consume),
        )
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/profile", put(profile))
        .route("/auth/account", get(account))
        .route("/auth/account/notifications", get(account_notifications))
        .route(
            "/auth/account/notifications/{id}/read",
            post(read_account_notification),
        )
        .route("/auth/account/site-notifications", get(site_notifications))
        .route(
            "/auth/account/site-notifications/{id}/read",
            post(read_site_notification),
        )
        .route(
            "/auth/account/site-notifications/read-all",
            post(read_all_site_notifications),
        )
        .route("/auth/account/player-link", post(player_link))
        .route(
            "/auth/account/player-link/verification",
            get(link_verification)
                .post(create_link_verification)
                .delete(cancel_link_verification),
        )
        .route(
            "/auth/account/player-link/verification/check",
            post(check_link_verification),
        )
        .route("/auth/account/password", post(change_password))
        .with_state(AuthState {
            database,
            redis,
            relay: WorkerRelayClient::new(&config).ok(),
            oidc: config
                .oidc_issuer
                .clone()
                .zip(config.oidc_audience.clone())
                .and_then(|(issuer, audience)| OidcVerifier::new(issuer, audience).ok()),
            oidc_bff_service_token: config.oidc_bff_service_token.clone(),
        })
}

/// BFF sends an access token only.  ID tokens, e-mail and groups are not accepted here.
async fn oidc_exchange(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if !oidc_bff_authorized(&state, &headers) {
        return Ok(simple_error(
            StatusCode::UNAUTHORIZED,
            "OIDC BFF authorization required",
        ));
    }
    let Some(verifier) = state.oidc.as_ref() else {
        return Ok(simple_error(StatusCode::NOT_FOUND, "OIDC is not enabled"));
    };
    let Some(token) = body
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|v| v.len() <= 16_384)
    else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Missing access token",
        ));
    };
    let identity = match verifier.validate(token).await {
        Ok(identity) => identity,
        Err(_) => {
            return Ok(simple_error(
                StatusCode::UNAUTHORIZED,
                "Invalid OIDC access token",
            ));
        }
    };
    let Some(user) = state.database.one_json(
        "SELECT u.id,u.username,u.email,u.avatar_url,u.bio,u.time_zone,u.is_admin,u.is_approved,u.created_at,u.last_login,u.linked_player_id,linked_player.name AS linked_player_name FROM user_identities i JOIN users u ON u.id=i.user_id LEFT JOIN players linked_player ON linked_player.id=u.linked_player_id WHERE i.issuer=$1 AND i.subject=$2 AND i.migration_state='linked' AND u.is_active=true LIMIT 1",
        &[&identity.issuer, &identity.subject]).await.map_err(|e| ApiError::database(e, &request_id))? else { return Ok(simple_error(StatusCode::FORBIDDEN, "OIDC identity is not linked")); };
    let user_id = as_i64(user.get("id"))
        .and_then(|v| i32::try_from(v).ok())
        .ok_or_else(|| ApiError::internal(&request_id))?;
    let (session, expires) = create_session(&state, user_id, &request_id).await?;
    state.database.query_json("UPDATE user_identities SET last_login_at=now() WHERE issuer=$1 AND subject=$2 RETURNING user_id", &[&identity.issuer, &identity.subject]).await.map_err(|e| ApiError::database(e, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"token":session,"expires_at":expires,"user":auth_user(&user)}),
    ))
}

fn oidc_state(body: &Value) -> Option<&str> {
    body.get("state").and_then(Value::as_str).filter(|value| {
        value.len() >= 32
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    })
}

/// Redis-backed, 10-minute one-use BFF transaction. The verifier never reaches a browser cookie.
async fn oidc_transaction_create(
    State(state): State<AuthState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if !oidc_bff_authorized(&state, &headers) {
        return Ok(simple_error(
            StatusCode::UNAUTHORIZED,
            "OIDC BFF authorization required",
        ));
    }
    let Some(state_value) = oidc_state(&body) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid OIDC state"));
    };
    if !body
        .get("nonce")
        .and_then(Value::as_str)
        .is_some_and(|v| v.len() >= 32)
        || !body
            .get("verifier")
            .and_then(Value::as_str)
            .is_some_and(|v| (43..=128).contains(&v.len()))
    {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Invalid OIDC transaction",
        ));
    }
    let key = format!("oidc:transaction:{state_value}");
    if state.redis.exists(&key).await {
        return Ok(simple_error(
            StatusCode::CONFLICT,
            "OIDC state already exists",
        ));
    }
    if !state.redis.set_required(&key, &body, Some(600)).await {
        return Ok(simple_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "OIDC transaction store unavailable",
        ));
    }
    Ok(json_response(
        StatusCode::CREATED,
        json!({"state":state_value,"expires_in":600}),
    ))
}

async fn oidc_transaction_consume(
    State(state): State<AuthState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    if !oidc_bff_authorized(&state, &headers) {
        return Ok(simple_error(
            StatusCode::UNAUTHORIZED,
            "OIDC BFF authorization required",
        ));
    }
    let Some(state_value) = oidc_state(&body) else {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid OIDC state"));
    };
    let key = format!("oidc:transaction:{state_value}");
    let lease = format!("{key}:consume");
    let token = random_hex(16);
    if state.redis.acquire_lease(&lease, &token, 10_000).await != Some(true) {
        return Ok(simple_error(
            StatusCode::CONFLICT,
            "OIDC state already consumed",
        ));
    }
    let transaction: Option<Value> = state.redis.get(&key).await;
    let _ = state.redis.del(&key).await;
    state.redis.release_lease(&lease, &token).await;
    match transaction {
        Some(value) => Ok(json_response(StatusCode::OK, value)),
        None => Ok(simple_error(
            StatusCode::UNAUTHORIZED,
            "OIDC state expired or consumed",
        )),
    }
}

fn oidc_bff_authorized(state: &AuthState, headers: &HeaderMap) -> bool {
    state
        .oidc_bff_service_token
        .as_deref()
        .zip(bearer(headers))
        .is_some_and(|(expected, candidate)| {
            crate::security::constant_time_equal_public(candidate, expected)
        })
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .or_else(|| {
            headers
                .get("cookie")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| {
                    value
                        .split(';')
                        .find_map(|part| part.trim().strip_prefix("__Host-pc_session="))
                        .filter(|value| !value.is_empty())
                })
        })
}

fn sha256(value: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(value.as_ref()))
}

fn password_hash(password: &str, salt: &str) -> String {
    sha256(format!("{password}{salt}"))
}

async fn session_row(
    state: &AuthState,
    headers: &HeaderMap,
    request_id: &RequestId,
    select: &str,
) -> Result<Option<Value>, ApiError> {
    let Some(token) = bearer(headers) else {
        return Ok(None);
    };
    state
        .database
        .one_json(
            &format!(
                "SELECT {select} FROM sessions s JOIN users u ON u.id=s.user_id \
                 LEFT JOIN players linked_player ON linked_player.id=u.linked_player_id \
                 WHERE s.token=$1 AND s.expires_at>now()"
            ),
            &[&sha256(token)],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))
}

async fn require_user_id(
    state: &AuthState,
    headers: &HeaderMap,
    request_id: &RequestId,
    missing: &'static str,
) -> Result<Result<i32, Response>, ApiError> {
    if bearer(headers).is_none() {
        return Ok(Err(simple_error(StatusCode::UNAUTHORIZED, missing)));
    }
    let row = session_row(state, headers, request_id, "s.user_id").await?;
    let Some(id) = row
        .as_ref()
        .and_then(|row| as_i64(row.get("user_id")))
        .and_then(|value| i32::try_from(value).ok())
    else {
        return Ok(Err(simple_error(
            StatusCode::UNAUTHORIZED,
            "Not authenticated",
        )));
    };
    Ok(Ok(id))
}

fn random_hex(bytes: usize) -> String {
    let mut value = vec![0_u8; bytes];
    OsRng.fill_bytes(&mut value);
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn create_session(
    state: &AuthState,
    user_id: i32,
    request_id: &RequestId,
) -> Result<(String, String), ApiError> {
    let token = random_hex(32);
    let hash = sha256(&token);
    let expires = OffsetDateTime::now_utc() + Duration::hours(SESSION_TTL_HOURS);
    let expires_text = expires
        .format(&Rfc3339)
        .map_err(|_| ApiError::internal(request_id))?;
    state
        .database
        .query_json(
            "INSERT INTO sessions(user_id,token,expires_at) VALUES($1,$2,$3::TEXT::TIMESTAMPTZ) RETURNING id",
            &[&user_id, &hash, &expires_text],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok((token, expires_text))
}

fn auth_user(row: &Value) -> Value {
    json!({
        "id":row.get("id").cloned().unwrap_or(Value::Null),
        "username":row.get("username").cloned().unwrap_or(Value::Null),
        "email":row.get("email").cloned().unwrap_or(Value::Null),
        "avatar_url":row.get("avatar_url").cloned().unwrap_or(Value::Null),
        "bio":row.get("bio").cloned().unwrap_or(Value::Null),
        "time_zone":row.get("time_zone").cloned().unwrap_or(Value::Null),
        "is_admin":row.get("is_admin").and_then(Value::as_bool).unwrap_or(false),
        "is_approved":row.get("is_approved").and_then(Value::as_bool).unwrap_or(false),
        "created_at":row.get("created_at").cloned().unwrap_or(Value::Null),
        "last_login":row.get("last_login").cloned().unwrap_or(Value::Null),
        "linked_player_id":row.get("linked_player_id").cloned().unwrap_or(Value::Null),
        "linked_player_name":row.get("linked_player_name").cloned().unwrap_or(Value::Null)
    })
}

fn text(body: &Value, field: &str) -> String {
    body.get(field)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

async fn register(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let username = text(&body, "username");
    let email = text(&body, "email").to_ascii_lowercase();
    let password = body
        .get("password")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if username.is_empty() || email.is_empty() || password.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Missing required fields: username, email, password",
        ));
    }
    let Some(username_re) = Regex::new(r"^[A-Za-z0-9_-]{3,32}$").ok() else {
        return Err(ApiError::internal(&request_id));
    };
    if !username_re.is_match(&username) {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Username must be 3-32 characters and use only letters, numbers, underscore, or dash",
        ));
    }
    let Some(email_re) = Regex::new(r"^[^\s@]+@[^\s@]+\.[^\s@]+$").ok() else {
        return Err(ApiError::internal(&request_id));
    };
    if !email_re.is_match(&email) {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Invalid email address",
        ));
    }
    if password.chars().count() < 6 {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Password must be at least 6 characters",
        ));
    }
    if let Some(existing) = state
        .database
        .one_json(
            "SELECT username,email FROM users WHERE lower(username)=lower($1) OR lower(email)=lower($2) LIMIT 1",
            &[&username, &email],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
    {
        let field = if existing
            .get("username")
            .and_then(Value::as_str)
            .is_some_and(|value| value.eq_ignore_ascii_case(&username))
        {
            "username"
        } else {
            "email"
        };
        return Ok(simple_error(
            StatusCode::CONFLICT,
            format!("That {field} is already registered"),
        ));
    }
    let salt = random_hex(16);
    let hash = password_hash(password, &salt);
    let user = state
        .database
        .one_json(
            "INSERT INTO users(username,email,password_hash,salt,updated_at) VALUES($1,$2,$3,$4,now()) \
             RETURNING id,username,email,avatar_url,bio,time_zone,is_admin,is_approved,created_at,last_login,linked_player_id",
            &[&username, &email, &hash, &salt],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::internal(&request_id))?;
    let user_id = as_i64(user.get("id"))
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| ApiError::internal(&request_id))?;
    let (token, expires) = create_session(&state, user_id, &request_id).await?;
    Ok(json_response(
        StatusCode::OK,
        json!({"message":"User registered","token":token,"expires_at":expires,"user":auth_user(&user)}),
    ))
}

async fn login(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let identifier = text(&body, "username");
    let password = body
        .get("password")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if identifier.is_empty() || password.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Missing required fields: username, password",
        ));
    }
    let user = state
        .database
        .one_json(
            "SELECT u.id,u.username,u.email,u.password_hash,u.salt,u.avatar_url,u.bio,u.time_zone, \
               u.is_admin,u.is_approved,u.created_at,u.last_login,u.linked_player_id,linked_player.name AS linked_player_name \
             FROM users u LEFT JOIN players linked_player ON linked_player.id=u.linked_player_id \
             WHERE lower(u.username)=lower($1) OR lower(u.email)=lower($1) LIMIT 1",
            &[&identifier],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let salt = user
        .as_ref()
        .and_then(|row| row.get("salt"))
        .and_then(Value::as_str)
        .unwrap_or("dummy_salt");
    let stored = user
        .as_ref()
        .and_then(|row| row.get("password_hash"))
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| password_hash("dummy_password", "dummy_salt"));
    let candidate = password_hash(password, salt);
    let candidate_digest: [u8; 32] = Sha256::digest(candidate.as_bytes()).into();
    let stored_digest: [u8; 32] = Sha256::digest(stored.as_bytes()).into();
    if user.is_none() || !bool::from(candidate_digest.ct_eq(&stored_digest)) {
        return Ok(simple_error(
            StatusCode::UNAUTHORIZED,
            "Invalid credentials",
        ));
    }
    let mut user = user.unwrap_or(Value::Null);
    let user_id = as_i64(user.get("id"))
        .and_then(|value| i32::try_from(value).ok())
        .ok_or_else(|| ApiError::internal(&request_id))?;
    let (token, expires) = create_session(&state, user_id, &request_id).await?;
    let last_login = OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .map_err(|_| ApiError::internal(&request_id))?;
    state
        .database
        .query_json(
            "UPDATE users SET last_login=$2::TEXT::TIMESTAMPTZ,updated_at=now() WHERE id=$1 RETURNING id",
            &[&user_id, &last_login],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    user["last_login"] = Value::String(last_login);
    Ok(json_response(
        StatusCode::OK,
        json!({"token":token,"expires_at":expires,"user":auth_user(&user)}),
    ))
}

async fn logout(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if let Some(token) = bearer(&headers) {
        state
            .database
            .query_json(
                "DELETE FROM sessions WHERE token=$1 RETURNING id",
                &[&sha256(token)],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
    }
    Ok(json_response(
        StatusCode::OK,
        json!({"message":"Logged out"}),
    ))
}

async fn me(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if bearer(&headers).is_none() {
        return Ok(simple_error(StatusCode::UNAUTHORIZED, "No token"));
    }
    let row = session_row(
        &state,
        &headers,
        &request_id,
        "s.user_id,s.expires_at,u.username,u.email,u.avatar_url,u.bio,u.time_zone,u.is_admin,u.is_approved,u.linked_player_id,linked_player.name AS linked_player_name",
    )
    .await?;
    Ok(match row {
        Some(row) => json_response(StatusCode::OK, row),
        None => simple_error(StatusCode::UNAUTHORIZED, "Invalid session"),
    })
}

async fn profile(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "Not authenticated").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let has_avatar = body.get("avatar_url").is_some();
    let avatar = body
        .get("avatar_url")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let has_bio = body.get("bio").is_some();
    let bio = body.get("bio").and_then(Value::as_str).map(str::to_owned);
    let timezone = body
        .get("time_zone")
        .map(|value| value.as_str().unwrap_or_default().trim().to_owned());
    if let Some(timezone) = timezone.as_ref()
        && timezone.parse::<chrono_tz::Tz>().is_err()
    {
        return Ok(simple_error(StatusCode::BAD_REQUEST, "Invalid time zone"));
    }
    let has_timezone = timezone.is_some();
    state
        .database
        .query_json(
            "UPDATE users SET avatar_url=CASE WHEN $1 THEN $2 ELSE avatar_url END, \
               bio=CASE WHEN $3 THEN $4 ELSE bio END,time_zone=CASE WHEN $5 THEN $6 ELSE time_zone END,updated_at=now() \
             WHERE id=$7 RETURNING id",
            &[&has_avatar, &avatar, &has_bio, &bio, &has_timezone, &timezone, &user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"message":"Profile updated"}),
    ))
}

async fn account(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    if bearer(&headers).is_none() {
        return Ok(simple_error(StatusCode::UNAUTHORIZED, "No token"));
    }
    let row = session_row(
        &state,
        &headers,
        &request_id,
        "s.user_id,s.expires_at,u.username,u.email,u.avatar_url,u.bio,u.time_zone,u.is_admin,u.is_approved,u.linked_player_id,u.created_at,u.last_login",
    )
    .await?;
    let Some(row) = row else {
        return Ok(simple_error(StatusCode::UNAUTHORIZED, "Invalid session"));
    };
    let linked = if let Some(id) = as_i64(row.get("linked_player_id")) {
        state
            .database
            .one_json(
                "SELECT id,name,platform_name,level,wins,losses,kbm_tier,kbm_points,kbm_player_id,controller_player_id,conquest_player_id FROM players WHERE id=$1",
                &[&id],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?
    } else {
        None
    };
    Ok(json_response(
        StatusCode::OK,
        json!({
            "user":{"id":row.get("user_id").cloned().unwrap_or(Value::Null),
                "username":row.get("username").cloned().unwrap_or(Value::Null),
                "email":row.get("email").cloned().unwrap_or(Value::Null),
                "avatar_url":row.get("avatar_url").cloned().unwrap_or(Value::Null),
                "bio":row.get("bio").cloned().unwrap_or(Value::Null),
                "time_zone":row.get("time_zone").cloned().unwrap_or(Value::Null),
                "is_admin":row.get("is_admin").and_then(Value::as_bool).unwrap_or(false),
                "is_approved":row.get("is_approved").and_then(Value::as_bool).unwrap_or(false),
                "linked_player_id":row.get("linked_player_id").cloned().unwrap_or(Value::Null),
                "created_at":row.get("created_at").cloned().unwrap_or(Value::Null),
                "last_login":row.get("last_login").cloned().unwrap_or(Value::Null)},
            "linkedPlayer":linked
        }),
    ))
}

fn query_limit(query: &HashMap<String, String>, fallback: i64, maximum: i64) -> i64 {
    query
        .get("limit")
        .and_then(|value| paladinscat_core::web_compat::parse_js_integer(value))
        .filter(|value| *value != 0)
        .unwrap_or(fallback)
        .clamp(1, maximum)
}

async fn account_notifications(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let limit = query_limit(&query, 25, 100);
    let rows = state
        .database
        .query_json(
            "SELECT notification.id,notification.type,notification.post_id,notification.comment_id,notification.read_at, \
               notification.created_at,COALESCE(actor.username,'A community member') AS actor_username, \
               post.title AS post_title,comment.content AS comment_content \
             FROM user_notifications notification LEFT JOIN users actor ON actor.id=notification.actor_user_id \
             LEFT JOIN posts post ON post.id=notification.post_id LEFT JOIN comments comment ON comment.id=notification.comment_id \
             WHERE notification.user_id=$1 ORDER BY notification.read_at NULLS FIRST,notification.created_at DESC LIMIT $2",
            &[&user_id, &limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(StatusCode::OK, json!({"data":rows})))
}

async fn read_account_notification(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Invalid notification id",
        ));
    };
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let updated = state
        .database
        .one_json(
            "UPDATE user_notifications SET read_at=COALESCE(read_at,now()) WHERE id=$1 AND user_id=$2 RETURNING id,read_at",
            &[&id, &user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(match updated {
        Some(row) => json_response(StatusCode::OK, row),
        None => simple_error(StatusCode::NOT_FOUND, "Notification not found"),
    })
}

async fn site_notifications(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let limit = query_limit(&query, 8, 20);
    let rows = state
        .database
        .query_json(
            "SELECT notification.id,notification.timestamp,notification.importance,notification.message,notification_read.read_at \
             FROM notifications notification LEFT JOIN site_notification_reads notification_read \
               ON notification_read.notification_id=notification.id AND notification_read.user_id=$1 \
             ORDER BY(notification_read.read_at IS NULL) DESC,notification.importance DESC,notification.timestamp DESC,notification.id DESC LIMIT $2",
            &[&user_id, &limit],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(StatusCode::OK, json!({"data":rows})))
}

async fn read_site_notification(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let Some(id) = parse_id(&id) else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Invalid notification id",
        ));
    };
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    if state
        .database
        .one_json("SELECT id FROM notifications WHERE id=$1::BIGINT", &[&id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .is_none()
    {
        return Ok(simple_error(
            StatusCode::NOT_FOUND,
            "Notification not found",
        ));
    }
    let read = state
        .database
        .one_json(
            "INSERT INTO site_notification_reads(user_id,notification_id) VALUES($1,$2) \
             ON CONFLICT(user_id,notification_id) DO UPDATE SET read_at=site_notification_reads.read_at \
             RETURNING notification_id AS id,read_at",
            &[&user_id, &id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .unwrap_or(Value::Null);
    Ok(json_response(StatusCode::OK, read))
}

async fn read_all_site_notifications(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    state
        .database
        .query_json(
            "INSERT INTO site_notification_reads(user_id,notification_id) SELECT $1,id FROM notifications \
             ON CONFLICT(user_id,notification_id) DO NOTHING RETURNING notification_id",
            &[&user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(StatusCode::OK, json!({"read":true})))
}

async fn player_link(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "Not authenticated").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    if body.get("action").and_then(Value::as_str) == Some("unlink") {
        state
            .database
            .query_json(
                "UPDATE users SET linked_player_id=NULL,updated_at=now() WHERE id=$1 RETURNING id",
                &[&user_id],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
        return Ok(json_response(
            StatusCode::OK,
            json!({"message":"Player link removed"}),
        ));
    }
    Ok(simple_error(
        StatusCode::BAD_REQUEST,
        "Use loadout verification to link a player.",
    ))
}

async fn link_verification(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    state
        .database
        .query_json(
            "DELETE FROM player_link_verifications WHERE expires_at<=now() RETURNING user_id",
            &[],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let row = state
        .database
        .one_json(
            "SELECT verification.player_id,verification.code,verification.expires_at,player.name AS player_name \
             FROM player_link_verifications verification JOIN players player ON player.id=verification.player_id \
             WHERE verification.user_id=$1",
            &[&user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let verification = row.map(|row| {
        json!({"player":{"id":row.get("player_id").cloned().unwrap_or(Value::Null),
            "name":row.get("player_name").cloned().unwrap_or(Value::Null)},
            "code":row.get("code").cloned().unwrap_or(Value::Null),
            "expiresAt":row.get("expires_at").cloned().unwrap_or(Value::Null)})
    });
    Ok(json_response(
        StatusCode::OK,
        json!({"verification":verification}),
    ))
}

fn random_pin() -> String {
    let mut bytes = [0_u8; 4];
    OsRng.fill_bytes(&mut bytes);
    (100_000 + u32::from_le_bytes(bytes) % 900_000).to_string()
}

async fn create_link_verification(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    if let Some(locked) = state
        .database
        .one_json(
            "SELECT locked_until FROM player_link_verifications WHERE user_id=$1 AND locked_until>now()",
            &[&user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
    {
        let retry = locked.get("locked_until").cloned().unwrap_or(Value::Null);
        return Ok(json_response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":format!("Too many verification attempts. Try again after {}.",retry.as_str().unwrap_or_default()),"retry_at":retry}),
        ));
    }
    let player_id = as_i64(body.get("playerId")).filter(|value| *value > 0);
    let Some(player_id) = player_id else {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Valid player ID required",
        ));
    };
    let player = state
        .database
        .one_json("SELECT id,name FROM players WHERE id=$1", &[&player_id])
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let Some(player) = player else {
        return Ok(simple_error(
            StatusCode::NOT_FOUND,
            "Player not found. Search for the player name first.",
        ));
    };
    if state
        .database
        .one_json(
            "SELECT username FROM users WHERE linked_player_id=$1 AND id!=$2",
            &[&player_id, &user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .is_some()
    {
        return Ok(simple_error(
            StatusCode::CONFLICT,
            "This player is already linked to another account.",
        ));
    }
    let code = random_pin();
    let verification = state
        .database
        .one_json(
            "INSERT INTO player_link_verifications(user_id,player_id,code,expires_at) \
             VALUES($1,$2,$3,now()+interval '10 minutes') ON CONFLICT(user_id) DO UPDATE SET \
             player_id=EXCLUDED.player_id,code=EXCLUDED.code,expires_at=EXCLUDED.expires_at,attempt_count=0, \
             last_attempt_at=NULL,next_attempt_at=NULL,locked_until=NULL,created_at=now() RETURNING code,expires_at",
            &[&user_id, &player_id, &code],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .ok_or_else(|| ApiError::internal(&request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"verification":{"player":player,"code":verification.get("code").cloned().unwrap_or(Value::Null),
            "expiresAt":verification.get("expires_at").cloned().unwrap_or(Value::Null)}}),
    ))
}

async fn lock_verification(
    state: &AuthState,
    user_id: i32,
    request_id: &RequestId,
) -> Result<Value, ApiError> {
    Ok(state
        .database
        .one_json(
            "UPDATE player_link_verifications SET locked_until=now()+make_interval(mins=>$2), \
             expires_at=GREATEST(expires_at,now()+make_interval(mins=>$2)) WHERE user_id=$1 RETURNING locked_until",
            &[&user_id, &LINK_LOCKOUT_MINUTES],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?
        .unwrap_or(Value::Null))
}

async fn check_link_verification(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let verification = state
        .database
        .one_json(
            "UPDATE player_link_verifications verification SET attempt_count=attempt_count+1,last_attempt_at=now(), \
               next_attempt_at=now()+make_interval(secs=>$2) FROM players player \
             WHERE verification.user_id=$1 AND player.id=verification.player_id AND verification.expires_at>now() \
               AND verification.attempt_count<$3 AND(verification.next_attempt_at IS NULL OR verification.next_attempt_at<=now()) \
               AND(verification.locked_until IS NULL OR verification.locked_until<=now()) \
             RETURNING verification.player_id,verification.code,verification.expires_at,verification.attempt_count, \
               verification.next_attempt_at,player.name AS player_name",
            &[&user_id, &LINK_COOLDOWN_SECONDS, &LINK_MAX_ATTEMPTS],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let Some(verification) = verification else {
        let row = state
            .database
            .one_json(
                "SELECT expires_at,attempt_count,next_attempt_at,locked_until FROM player_link_verifications WHERE user_id=$1",
                &[&user_id],
            )
            .await
            .map_err(|error| ApiError::database(error, &request_id))?;
        let Some(row) = row else {
            return Ok(simple_error(
                StatusCode::NOT_FOUND,
                "No active player-link verification.",
            ));
        };
        let now = OffsetDateTime::now_utc();
        let timestamp = |field: &str| {
            row.get(field)
                .and_then(Value::as_str)
                .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        };
        if timestamp("expires_at").is_some_and(|value| value <= now) {
            state
                .database
                .query_json(
                    "DELETE FROM player_link_verifications WHERE user_id=$1 RETURNING user_id",
                    &[&user_id],
                )
                .await
                .map_err(|error| ApiError::database(error, &request_id))?;
            return Ok(simple_error(
                StatusCode::GONE,
                "Verification code expired. Generate a new one.",
            ));
        }
        let retry = row
            .get("locked_until")
            .filter(|_| timestamp("locked_until").is_some_and(|value| value > now))
            .or_else(|| {
                row.get("next_attempt_at")
                    .filter(|_| timestamp("next_attempt_at").is_some_and(|value| value > now))
            })
            .cloned();
        return Ok(json_response(
            StatusCode::TOO_MANY_REQUESTS,
            json!({"error":"Please wait before checking again.","retry_at":retry}),
        ));
    };
    let player_id = as_i64(verification.get("player_id")).unwrap_or_default();
    let identity = request_identity(&headers);
    if let Err(error) = vendor_guard(
        &state.redis,
        &identity,
        "player-link-verification",
        player_id,
        u64::try_from(LINK_COOLDOWN_SECONDS).unwrap_or(30) * 1000,
        8,
    )
    .await
    {
        return Ok(error.into_response());
    }
    let relay = state
        .relay
        .as_ref()
        .ok_or_else(|| ApiError::internal(&request_id))?;
    let loadouts = match relay
        .call_value(
            "getPlayerLoadouts",
            vec![json!(player_id)],
            "account_verification",
        )
        .await
    {
        Ok(value) => value,
        Err(_) => {
            if as_i64(verification.get("attempt_count")).unwrap_or(0)
                >= i64::from(LINK_MAX_ATTEMPTS)
            {
                let locked = lock_verification(&state, user_id, &request_id).await?;
                return Ok(json_response(
                    StatusCode::TOO_MANY_REQUESTS,
                    json!({"error":"The verification attempt limit was reached.","retry_at":locked.get("locked_until").cloned().unwrap_or(Value::Null)}),
                ));
            }
            return Ok(simple_error(
                StatusCode::BAD_GATEWAY,
                "Hi-Rez could not refresh this player's loadouts. Please wait for the cooldown and try again.",
            ));
        }
    };
    record_raw_hirez_response(
        &state.database,
        RawHirezAudit {
            endpoint: "getplayerloadouts",
            operation: "player-link-verification",
            entity_type: "player_loadout",
            entity_id: player_id.to_string(),
            params: json!({"playerId":player_id,"reason":"player_link_verification"}),
            raw_response: &loadouts,
            source: "player-link-verification",
        },
    )
    .await
    .map_err(|error| ApiError::database(error, &request_id))?;
    let code = verification
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let verified = loadouts.as_array().is_some_and(|rows| {
        rows.iter().any(|row| {
            row.get("DeckName")
                .or_else(|| row.get("deck_name"))
                .and_then(Value::as_str)
                .is_some_and(|name| name.trim().eq_ignore_ascii_case(code))
        })
    });
    if !verified {
        let attempts = as_i64(verification.get("attempt_count")).unwrap_or(0);
        if attempts >= i64::from(LINK_MAX_ATTEMPTS) {
            let locked = lock_verification(&state, user_id, &request_id).await?;
            return Ok(json_response(
                StatusCode::TOO_MANY_REQUESTS,
                json!({"error":"The verification attempt limit was reached.","retry_at":locked.get("locked_until").cloned().unwrap_or(Value::Null)}),
            ));
        }
        let remaining = i64::from(LINK_MAX_ATTEMPTS) - attempts;
        return Ok(simple_error(
            StatusCode::CONFLICT,
            format!(
                "Code not found in this player's freshly refreshed loadouts. Save the renamed loadout, wait {LINK_COOLDOWN_SECONDS} seconds, then try again. {remaining} attempt{} remaining.",
                if remaining == 1 { "" } else { "s" }
            ),
        ));
    }
    if state
        .database
        .one_json(
            "SELECT username FROM users WHERE linked_player_id=$1 AND id!=$2",
            &[&player_id, &user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?
        .is_some()
    {
        return Ok(simple_error(
            StatusCode::CONFLICT,
            "This player is already linked to another account.",
        ));
    }
    let mut client = state
        .database
        .connection()
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let transaction = client
        .transaction()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute(
            "UPDATE users SET linked_player_id=$1,updated_at=now() WHERE id=$2",
            &[&player_id, &user_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .execute(
            "DELETE FROM player_link_verifications WHERE user_id=$1",
            &[&user_id],
        )
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    transaction
        .commit()
        .await
        .map_err(|error| ApiError::database(error.into(), &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"message":"Player linked","player":{"id":player_id,
            "name":verification.get("player_name").cloned().unwrap_or(Value::Null)}}),
    ))
}

async fn cancel_link_verification(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "No token").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    state
        .database
        .query_json(
            "DELETE FROM player_link_verifications WHERE user_id=$1 RETURNING user_id",
            &[&user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"message":"Verification cancelled"}),
    ))
}

async fn change_password(
    State(state): State<AuthState>,
    Extension(request_id): Extension<RequestId>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, ApiError> {
    let user_id = match require_user_id(&state, &headers, &request_id, "Not authenticated").await? {
        Ok(id) => id,
        Err(response) => return Ok(response),
    };
    let current = body
        .get("currentPassword")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let new = body
        .get("newPassword")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if current.is_empty() || new.is_empty() {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Both current and new password required",
        ));
    }
    if new.chars().count() < 6 {
        return Ok(simple_error(
            StatusCode::BAD_REQUEST,
            "Password must be at least 6 characters",
        ));
    }
    let user = state
        .database
        .one_json(
            "SELECT id,password_hash,salt FROM users WHERE id=$1",
            &[&user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    let valid = user.as_ref().is_some_and(|row| {
        let salt = row.get("salt").and_then(Value::as_str).unwrap_or_default();
        let expected = row
            .get("password_hash")
            .and_then(Value::as_str)
            .unwrap_or_default();
        password_hash(current, salt) == expected
    });
    if !valid {
        return Ok(simple_error(
            StatusCode::FORBIDDEN,
            "Current password is incorrect",
        ));
    }
    let salt = random_hex(16);
    let hash = password_hash(new, &salt);
    state
        .database
        .query_json(
            "UPDATE users SET password_hash=$1,salt=$2,updated_at=now() WHERE id=$3 RETURNING id",
            &[&hash, &salt, &user_id],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(json_response(
        StatusCode::OK,
        json!({"message":"Password changed successfully"}),
    ))
}
