use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::Client;
use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiCredential {
    pub dev_id: String,
    pub auth_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpResponse {
    pub status: u16,
    pub reason: String,
    pub body: String,
}

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("request timed out")]
    Timeout,
    #[error("{0}")]
    Network(String),
}

#[async_trait]
pub trait HttpTransport: Send + Sync {
    async fn get(&self, url: &str, timeout: Duration) -> Result<HttpResponse, TransportError>;
}

#[derive(Clone)]
pub struct ReqwestTransport {
    client: Client,
}

impl ReqwestTransport {
    pub fn new() -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder().build()?,
        })
    }
}

#[async_trait]
impl HttpTransport for ReqwestTransport {
    async fn get(&self, url: &str, timeout: Duration) -> Result<HttpResponse, TransportError> {
        let response = self
            .client
            .get(url)
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| {
                if error.is_timeout() {
                    TransportError::Timeout
                } else {
                    TransportError::Network(error.to_string())
                }
            })?;
        let status = response.status();
        let reason = status.canonical_reason().unwrap_or_default().to_owned();
        let body = response.text().await.map_err(|error| {
            if error.is_timeout() {
                TransportError::Timeout
            } else {
                TransportError::Network(error.to_string())
            }
        })?;
        Ok(HttpResponse {
            status: status.as_u16(),
            reason,
            body,
        })
    }
}

#[async_trait]
pub trait ApiKeyState: Send + Sync {
    async fn active_key(&self) -> Result<ApiCredential, UpstreamError>;
    fn increment_usage(&self, dev_id: &str);
    async fn log_endpoint(
        &self,
        dev_id: &str,
        endpoint: &str,
        response_time_ms: u64,
        consumer: &str,
    );
    async fn record_success(&self, dev_id: &str);
    async fn record_failure(&self, dev_id: &str, key_fault: bool);
    async fn sync_usage(&self, dev_id: &str);
}

#[derive(Clone, Debug)]
pub struct SessionAuditRecord {
    pub endpoint: &'static str,
    pub raw_data: Value,
    pub status_code: u16,
    pub session_id: String,
    pub response_time_ms: u64,
}

#[async_trait]
pub trait SessionAudit: Send + Sync {
    async fn save(&self, record: SessionAuditRecord);
}

pub trait Clock: Send + Sync {
    fn unix_millis(&self) -> i64;
}

#[derive(Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn unix_millis(&self) -> i64 {
        time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000
    }
}

#[async_trait]
pub trait RetrySleeper: Send + Sync {
    async fn sleep_before_retry(&self, completed_attempt: usize);
}

#[derive(Default)]
pub struct JitteredRetrySleeper;

#[async_trait]
impl RetrySleeper for JitteredRetrySleeper {
    async fn sleep_before_retry(&self, completed_attempt: usize) {
        let base_ms = completed_attempt.saturating_mul(500) as f64;
        let jitter = rand::random_range(0.9..=1.1);
        let delay_ms = (base_ms * jitter).clamp(100.0, 10_000.0) as u64;
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
}

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("{0}")]
    Configuration(String),
    #[error("{0}")]
    Transport(#[from] TransportError),
    #[error("{0}")]
    Protocol(String),
    #[error("{0}")]
    Application(String),
}

pub type SharedTransport = Arc<dyn HttpTransport>;
pub type SharedKeyState = Arc<dyn ApiKeyState>;
pub type SharedSessionAudit = Arc<dyn SessionAudit>;
pub type SharedClock = Arc<dyn Clock>;
pub type SharedRetrySleeper = Arc<dyn RetrySleeper>;
