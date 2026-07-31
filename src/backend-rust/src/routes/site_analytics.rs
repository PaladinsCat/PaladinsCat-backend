use std::sync::LazyLock;

use axum::{
    Json, Router,
    extract::{Extension, State},
    http::{HeaderValue, StatusCode, header::CACHE_CONTROL},
    response::{IntoResponse, Response},
    routing::post,
};
use paladinscat_core::database::Database;
use regex::Regex;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};

use crate::{error::ApiError, request::RequestId};

static VISITOR_ID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9_-]{16,128}$").expect("valid visitor regex"));
static DATE_FORMAT: &[FormatItem<'static>] = format_description!("[year]-[month]-[day]");

#[derive(Clone)]
struct AnalyticsState {
    database: Database,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnalyticsBody {
    visitor_id: Option<String>,
    path: Option<String>,
}

pub fn router(database: Database) -> Router {
    Router::new()
        .route("/analytics/visit", post(visit))
        .route("/analytics/heartbeat", post(heartbeat))
        .with_state(AnalyticsState { database })
}

fn normalized_path(value: Option<&str>) -> Option<String> {
    let path = value?.trim().split(['?', '#']).next().unwrap_or_default();
    if !path.starts_with('/')
        || path.len() > 200
        || path == "/admin"
        || path.starts_with("/admin/")
        || path == "/auth"
        || path.starts_with("/auth/")
    {
        return None;
    }
    let mut normalized = String::with_capacity(path.len());
    let mut slash = false;
    for character in path.chars() {
        if character == '/' {
            if !slash {
                normalized.push('/');
            }
            slash = true;
        } else {
            normalized.push(character);
            slash = false;
        }
    }
    let normalized = normalized
        .split('/')
        .map(|segment| {
            if segment.len() >= 4 && segment.chars().all(|character| character.is_ascii_digit()) {
                "[id]"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/");
    Some(
        if normalized.is_empty() {
            "/"
        } else {
            normalized.as_str()
        }
        .chars()
        .take(200)
        .collect(),
    )
}

fn visitor(value: Option<&str>) -> Option<&str> {
    value
        .map(str::trim)
        .filter(|value| VISITOR_ID.is_match(value))
}

async fn touch(
    database: &Database,
    visitor: &str,
    increment: i32,
    request_id: &RequestId,
) -> Result<String, ApiError> {
    let date = OffsetDateTime::now_utc()
        .format(DATE_FORMAT)
        .map_err(|_| ApiError::internal(request_id))?;
    let salt = std::env::var("ANALYTICS_SALT")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| std::env::var("ADMIN_SECRET").ok())
        .unwrap_or_else(|| "paladinscat-anonymous-analytics".to_owned());
    let digest = format!(
        "{:x}",
        Sha256::digest(format!("{date}:{salt}:{visitor}").as_bytes())
    );
    database
        .query_json(
            "INSERT INTO site_daily_visitors(visit_date,visitor_hash,page_views,first_seen,last_seen) \
             VALUES($2::date,$1,$3,now(),now()) ON CONFLICT(visit_date,visitor_hash) DO UPDATE SET \
             page_views=site_daily_visitors.page_views+$3,last_seen=now()",
            &[&digest, &date, &increment],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(date)
}

async fn visit(
    State(state): State<AnalyticsState>,
    Extension(request_id): Extension<RequestId>,
    Json(body): Json<AnalyticsBody>,
) -> Result<Response, ApiError> {
    let Some(visitor) = visitor(body.visitor_id.as_deref()) else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    let Some(path) = normalized_path(body.path.as_deref()) else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    let date = touch(&state.database, visitor, 1, &request_id).await?;
    state
        .database
        .query_json(
            "INSERT INTO site_daily_page_views(visit_date,path,page_views,updated_at) \
             VALUES($2::date,$1,1,now()) ON CONFLICT(visit_date,path) DO UPDATE SET \
             page_views=site_daily_page_views.page_views+1,updated_at=now()",
            &[&path, &date],
        )
        .await
        .map_err(|error| ApiError::database(error, &request_id))?;
    Ok(StatusCode::NO_CONTENT.into_response())
}

async fn heartbeat(
    State(state): State<AnalyticsState>,
    Extension(request_id): Extension<RequestId>,
    Json(body): Json<AnalyticsBody>,
) -> Result<Response, ApiError> {
    let Some(visitor) = visitor(body.visitor_id.as_deref()) else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    touch(&state.database, visitor, 0, &request_id).await?;
    let mut response = StatusCode::NO_CONTENT.into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("private, no-store"));
    Ok(response)
}
