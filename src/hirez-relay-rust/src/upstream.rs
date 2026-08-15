use std::{env, sync::Arc, time::Duration};

use async_trait::async_trait;
use reqwest::Client;
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, Error as TlsError, RootCertStore,
    SignatureScheme,
    client::{
        WebPkiServerVerifier,
        danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    },
    pki_types::{CertificateDer, ServerName, UnixTime},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

const PINNED_HIREZ_HOST: &str = "api.paladins.com";
const PIN_SHA256_ENV: &str = "HIREZ_TLS_EXPIRED_CERT_SHA256";
const PIN_UNTIL_ENV: &str = "HIREZ_TLS_PIN_UNTIL_UNIX";

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
    pub fn new() -> Result<Self, String> {
        let mut builder = Client::builder();
        if let Some(pin) = ExpiredCertificatePin::from_env()? {
            let mut roots = RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let standard = WebPkiServerVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|error| format!("failed to build standard TLS verifier: {error}"))?;
            let verifier = Arc::new(PinnedExpiryVerifier { standard, pin });
            let tls = ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(verifier)
                .with_no_client_auth();
            builder = builder.use_preconfigured_tls(tls);
        }
        Ok(Self {
            client: builder
                .build()
                .map_err(|error| format!("failed to build HTTP client: {error}"))?,
        })
    }
}

#[derive(Clone, Debug)]
struct ExpiredCertificatePin {
    certificate_sha256: [u8; 32],
    valid_until_unix: u64,
}

impl ExpiredCertificatePin {
    fn from_env() -> Result<Option<Self>, String> {
        let digest = env::var(PIN_SHA256_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty());
        let until = env::var(PIN_UNTIL_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty());
        match (digest, until) {
            (None, None) => Ok(None),
            (Some(digest), Some(until)) => {
                let bytes = hex::decode(digest.trim()).map_err(|_| {
                    format!("{PIN_SHA256_ENV} must be a 64-character hexadecimal SHA-256 digest")
                })?;
                let certificate_sha256 = bytes.try_into().map_err(|_| {
                    format!("{PIN_SHA256_ENV} must be a 64-character hexadecimal SHA-256 digest")
                })?;
                let valid_until_unix = until
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| format!("{PIN_UNTIL_ENV} must be a Unix timestamp"))?;
                Ok(Some(Self {
                    certificate_sha256,
                    valid_until_unix,
                }))
            }
            _ => Err(format!(
                "{PIN_SHA256_ENV} and {PIN_UNTIL_ENV} must be configured together"
            )),
        }
    }
}

#[derive(Debug)]
struct PinnedExpiryVerifier {
    standard: Arc<WebPkiServerVerifier>,
    pin: ExpiredCertificatePin,
}

impl ServerCertVerifier for PinnedExpiryVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, TlsError> {
        match self.standard.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(verified) => return Ok(verified),
            Err(TlsError::InvalidCertificate(
                CertificateError::Expired | CertificateError::ExpiredContext { .. },
            )) => {}
            Err(error) => return Err(error),
        }

        let expected_host = matches!(
            server_name,
            ServerName::DnsName(name) if name.as_ref() == PINNED_HIREZ_HOST
        );
        let within_window = now.as_secs() <= self.pin.valid_until_unix;
        let actual = Sha256::digest(end_entity.as_ref());
        if expected_host
            && within_window
            && actual.as_slice() == self.pin.certificate_sha256.as_slice()
        {
            return Ok(ServerCertVerified::assertion());
        }
        Err(TlsError::General(
            "expired Hi-Rez certificate did not satisfy the scoped pin".to_owned(),
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.standard.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, TlsError> {
        self.standard.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.standard.supported_verify_schemes()
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
                    TransportError::Network("upstream TLS or network request failed".to_owned())
                }
            })?;
        let status = response.status();
        let reason = status.canonical_reason().unwrap_or_default().to_owned();
        let body = response.text().await.map_err(|error| {
            if error.is_timeout() {
                TransportError::Timeout
            } else {
                TransportError::Network("upstream response body could not be read".to_owned())
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
