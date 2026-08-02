use axum::{
    Json,
    http::{HeaderValue, StatusCode, header::RETRY_AFTER},
    response::{IntoResponse, Response},
};
use paladinscat_core::database::DatabaseError;
use serde_json::{Value, json};

use crate::request::RequestId;

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
    details: Option<Box<Value>>,
    request_id: Option<String>,
    retry_after: Option<u64>,
}

impl ApiError {
    pub fn coded(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            details: None,
            request_id: None,
            retry_after: None,
        }
    }

    pub fn validation(message: impl Into<String>) -> Self {
        Self::coded(StatusCode::BAD_REQUEST, "VALIDATION", message)
    }

    pub fn not_found(message: impl Into<String>, details: Value) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: message.into(),
            details: Some(Box::new(details)),
            request_id: None,
            retry_after: None,
        }
    }

    pub fn not_found_without_details(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: message.into(),
            details: None,
            request_id: None,
            retry_after: None,
        }
    }

    pub fn internal(request_id: &RequestId) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL_ERROR",
            message: "The request could not be completed.".to_owned(),
            details: None,
            request_id: Some(request_id.0.clone()),
            retry_after: None,
        }
    }

    pub fn request_security(
        status: StatusCode,
        code: &'static str,
        message: impl Into<String>,
        retry_after: u64,
    ) -> Self {
        Self {
            status,
            code,
            message: message.into(),
            details: (retry_after > 0)
                .then(|| Box::new(json!({ "retry_after_seconds": retry_after }))),
            request_id: None,
            retry_after: (retry_after > 0).then_some(retry_after),
        }
    }
}

impl ApiError {
    pub fn database(error: DatabaseError, request_id: &RequestId) -> Self {
        tracing::error!(error = ?error, "database request failed");
        Self::internal(request_id)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut error = json!({
            "code": self.code,
            "message": self.message,
        });
        if let Some(details) = self.details {
            error
                .as_object_mut()
                .expect("error object")
                .insert("details".to_owned(), *details);
        }
        if let Some(request_id) = self.request_id {
            error
                .as_object_mut()
                .expect("error object")
                .insert("requestId".to_owned(), Value::String(request_id));
        }
        let mut response = (self.status, Json(json!({ "error": error }))).into_response();
        if let Some(retry_after) = self.retry_after
            && let Ok(value) = HeaderValue::from_str(&retry_after.to_string())
        {
            response.headers_mut().insert(RETRY_AFTER, value);
        }
        response
    }
}
