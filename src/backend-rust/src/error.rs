use axum::{
    Json,
    http::StatusCode,
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
    details: Option<Value>,
    request_id: Option<String>,
}

impl ApiError {
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "VALIDATION",
            message: message.into(),
            details: None,
            request_id: None,
        }
    }

    pub fn not_found(message: impl Into<String>, details: Value) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: message.into(),
            details: Some(details),
            request_id: None,
        }
    }

    pub fn internal(request_id: &RequestId) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL_ERROR",
            message: "The request could not be completed.".to_owned(),
            details: None,
            request_id: Some(request_id.0.clone()),
        }
    }
}

impl ApiError {
    pub fn database(error: DatabaseError, request_id: &RequestId) -> Self {
        tracing::error!(error = %error, "database request failed");
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
                .insert("details".to_owned(), details);
        }
        if let Some(request_id) = self.request_id {
            error
                .as_object_mut()
                .expect("error object")
                .insert("requestId".to_owned(), Value::String(request_id));
        }
        (self.status, Json(json!({ "error": error }))).into_response()
    }
}
