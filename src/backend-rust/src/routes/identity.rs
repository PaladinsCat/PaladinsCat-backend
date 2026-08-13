use axum::{
    Json,
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
};
use paladinscat_core::database::Database;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{error::ApiError, request::RequestId};

#[derive(Clone, Debug)]
pub(crate) struct Session {
    pub user_id: i32,
    pub username: String,
    pub is_admin: bool,
    pub is_project_developer: bool,
    pub linked_player_id: Option<i64>,
}

pub(crate) async fn session(
    database: &Database,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<Option<Session>, ApiError> {
    let Some(token) = session_token(headers) else {
        return Ok(None);
    };
    let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    let row = database
        .one_json(
            "SELECT s.user_id,u.username,u.is_admin,(u.role IN ('developer','admin')) AS is_project_developer,u.linked_player_id \
             FROM sessions s JOIN users u ON u.id=s.user_id \
             WHERE s.token=$1 AND s.expires_at>now()",
            &[&token_hash],
        )
        .await
        .map_err(|error| ApiError::database(error, request_id))?;
    Ok(row.and_then(|row| {
        Some(Session {
            user_id: i32::try_from(as_i64(row.get("user_id"))?).ok()?,
            username: row
                .get("username")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            is_admin: row
                .get("is_admin")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            is_project_developer: row
                .get("is_project_developer")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            linked_player_id: as_i64(row.get("linked_player_id")),
        })
    }))
}

fn session_token(headers: &HeaderMap) -> Option<&str> {
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
                    value.split(';').find_map(|part| {
                        part.trim()
                            .strip_prefix("__Host-pc_session=")
                            .filter(|value| !value.is_empty())
                    })
                })
        })
}

#[cfg(test)]
mod tests {
    use super::session_token;
    use axum::http::{HeaderMap, HeaderValue, header::AUTHORIZATION};

    #[test]
    fn accepts_oidc_cookie_and_prefers_bearer_session() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "cookie",
            HeaderValue::from_static("other=x; __Host-pc_session=cookie-token"),
        );
        assert_eq!(session_token(&headers), Some("cookie-token"));
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer bearer-token"),
        );
        assert_eq!(session_token(&headers), Some("bearer-token"));
    }
}

pub(crate) async fn require_session(
    database: &Database,
    headers: &HeaderMap,
    request_id: &RequestId,
) -> Result<Session, Response> {
    match session(database, headers, request_id).await {
        Ok(Some(session)) => Ok(session),
        Ok(None) => Err(simple_error(StatusCode::UNAUTHORIZED, "Not authenticated")),
        Err(error) => Err(error.into_response()),
    }
}

pub(crate) fn simple_error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error":message.into()}))).into_response()
}

pub(crate) fn json_response(status: StatusCode, value: Value) -> Response {
    (status, Json(value)).into_response()
}

pub(crate) fn parse_id(value: &str) -> Option<i64> {
    paladinscat_core::web_compat::parse_js_integer(value).filter(|value| *value > 0)
}

pub(crate) fn as_i64(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(value)) => value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok())),
        Some(Value::String(value)) => value.parse().ok(),
        _ => None,
    }
}
