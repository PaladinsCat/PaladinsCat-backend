use std::sync::atomic::{AtomicU64, Ordering};

use axum::{extract::Request, http::Uri, middleware::Next, response::Response};

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct RequestId(pub String);

#[derive(Clone, Debug)]
pub struct EffectiveUri(pub Uri);

pub fn next_request_id() -> RequestId {
    let sequence = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    RequestId(format!("req-{sequence}"))
}

pub async fn assign_request_id(mut request: Request, next: Next) -> Response {
    request.extensions_mut().insert(next_request_id());
    next.run(request).await
}
