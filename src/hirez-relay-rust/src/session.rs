use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use md5::{Digest, Md5};
use serde_json::Value;
use time::{OffsetDateTime, format_description::FormatItem, macros::format_description};
use tokio::sync::{Mutex, RwLock};

use crate::upstream::{
    ApiCredential, SessionAuditRecord, SharedClock, SharedKeyState, SharedSessionAudit,
    SharedTransport, UpstreamError,
};

const SESSION_TIMESTAMP_FORMAT: &[FormatItem<'static>] =
    format_description!("[year][month][day][hour][minute][second]");

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    pub dev_id: String,
    pub session_key: String,
    pub expires_at_ms: i64,
}

pub struct SessionManager {
    base_url: String,
    ttl: Duration,
    transport: SharedTransport,
    key_state: SharedKeyState,
    audit: SharedSessionAudit,
    clock: SharedClock,
    sessions: RwLock<HashMap<String, Session>>,
    key_locks: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

impl SessionManager {
    pub fn new(
        base_url: impl Into<String>,
        ttl: Duration,
        transport: SharedTransport,
        key_state: SharedKeyState,
        audit: SharedSessionAudit,
        clock: SharedClock,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            ttl,
            transport,
            key_state,
            audit,
            clock,
            sessions: RwLock::new(HashMap::new()),
            key_locks: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_session(&self, dev_id: &str) -> Option<Session> {
        let now = self.clock.unix_millis();
        if let Some(session) = self.sessions.read().await.get(dev_id).cloned()
            && session.expires_at_ms > now
        {
            return Some(session);
        }
        self.sessions.write().await.remove(dev_id);
        None
    }

    pub async fn invalidate_session(&self, dev_id: &str) {
        self.sessions.write().await.remove(dev_id);
    }

    pub(crate) fn clock(&self) -> &dyn crate::upstream::Clock {
        self.clock.as_ref()
    }

    pub async fn get_active_session(&self) -> Result<(ApiCredential, Session), UpstreamError> {
        let key = self.key_state.active_key().await?;
        let session = self.acquire_session(&key).await?;
        Ok((key, session))
    }

    pub async fn acquire_session(&self, key: &ApiCredential) -> Result<Session, UpstreamError> {
        if let Some(session) = self.get_session(&key.dev_id).await {
            return Ok(session);
        }

        let key_lock = {
            let mut locks = self.key_locks.lock().await;
            locks
                .entry(key.dev_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = key_lock.lock().await;

        if let Some(session) = self.get_session(&key.dev_id).await {
            return Ok(session);
        }
        self.create_session(key).await
    }

    async fn create_session(&self, key: &ApiCredential) -> Result<Session, UpstreamError> {
        let timestamp = hirez_timestamp(self.clock.as_ref())?;
        let signature = sign("createsession", &key.dev_id, &key.auth_key, &timestamp);
        let url = format!(
            "{}/createsessionJson/{}/{}/{}",
            self.base_url, key.dev_id, signature, timestamp
        );

        self.key_state.increment_usage(&key.dev_id);
        let started = Instant::now();
        let response = match self.transport.get(&url, Duration::from_secs(10)).await {
            Ok(response) => response,
            Err(error) => {
                let elapsed = started.elapsed().as_millis() as u64;
                self.key_state
                    .log_endpoint(&key.dev_id, "createsession", elapsed, "session_management")
                    .await;
                self.key_state.record_failure(&key.dev_id, false).await;
                return Err(error.into());
            }
        };
        let elapsed = started.elapsed().as_millis() as u64;
        self.key_state
            .log_endpoint(&key.dev_id, "createsession", elapsed, "session_management")
            .await;

        if !(200..300).contains(&response.status) {
            self.key_state.record_failure(&key.dev_id, true).await;
            return Err(UpstreamError::Protocol(format!(
                "Session acquisition failed: {}",
                response.reason
            )));
        }

        let data: Value = match parse_json_body(&response.body) {
            Ok(data) => data,
            Err(error) => {
                self.key_state.record_failure(&key.dev_id, true).await;
                return Err(error);
            }
        };
        let approved = data.get("ret_msg").and_then(Value::as_str) == Some("Approved");
        if !approved {
            self.key_state.record_failure(&key.dev_id, true).await;
            let message = data
                .get("ret_msg")
                .and_then(Value::as_str)
                .unwrap_or("Unknown Error");
            return Err(UpstreamError::Application(format!(
                "Hi-Rez Session API Error: {message}"
            )));
        }
        self.key_state.record_success(&key.dev_id).await;

        let session_id = data
            .get("session_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        self.audit
            .save(SessionAuditRecord {
                endpoint: "createsession",
                raw_data: data.clone(),
                status_code: response.status,
                session_id: if session_id.is_empty() {
                    "FAILED".to_owned()
                } else {
                    session_id.to_owned()
                },
                response_time_ms: elapsed,
            })
            .await;
        if session_id.is_empty() {
            return Err(UpstreamError::Protocol(format!(
                "Session acquisition returned no session_id despite Approved: {data}"
            )));
        }

        let session = Session {
            dev_id: key.dev_id.clone(),
            session_key: session_id.to_owned(),
            expires_at_ms: self.clock.unix_millis()
                + i64::try_from(self.ttl.as_millis()).unwrap_or(i64::MAX),
        };
        self.sessions
            .write()
            .await
            .insert(key.dev_id.clone(), session.clone());
        Ok(session)
    }
}

pub fn sign(method: &str, dev_id: &str, auth_key: &str, timestamp: &str) -> String {
    let mut hasher = Md5::new();
    hasher.update(format!("{dev_id}{method}{auth_key}{timestamp}").as_bytes());
    format!("{:x}", hasher.finalize())
}

pub fn hirez_timestamp(clock: &dyn crate::upstream::Clock) -> Result<String, UpstreamError> {
    let millis = clock.unix_millis();
    let datetime = OffsetDateTime::from_unix_timestamp(millis.div_euclid(1_000))
        .map_err(|error| UpstreamError::Configuration(error.to_string()))?;
    datetime
        .format(SESSION_TIMESTAMP_FORMAT)
        .map_err(|error| UpstreamError::Configuration(error.to_string()))
}

pub fn parse_json_body(body: &str) -> Result<Value, UpstreamError> {
    let cleaned = body.strip_prefix('\u{feff}').unwrap_or(body);
    serde_json::from_str(cleaned)
        .map_err(|error| UpstreamError::Protocol(format!("JSON parse error from Hi-Rez: {error}")))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use futures::future::join_all;
    use time::macros::datetime;

    use super::*;
    use crate::test_support::{FakeKeyState, FakeTransport, FixedClock, RecordingAudit};

    fn fixture_clock() -> Arc<FixedClock> {
        Arc::new(FixedClock::new(
            datetime!(2026-07-28 23:59:59 UTC).unix_timestamp() * 1_000,
        ))
    }

    fn fixture_key() -> ApiCredential {
        ApiCredential {
            dev_id: "1234".to_owned(),
            auth_key: "secret".to_owned(),
        }
    }

    #[test]
    fn signature_and_timestamp_match_typescript_vectors() {
        let clock = fixture_clock();
        let timestamp = hirez_timestamp(clock.as_ref()).expect("timestamp");
        assert_eq!(timestamp, "20260728235959");
        assert_eq!(
            sign("getmatchdetailsbatch", "1234", "secret", &timestamp),
            "5ffb551a6f5afbe2a9b5d73a37892999"
        );
    }

    #[test]
    fn json_parser_strips_utf8_bom() {
        assert_eq!(
            parse_json_body("\u{feff}{\"ret_msg\":\"Approved\"}").expect("BOM JSON")["ret_msg"],
            "Approved"
        );
    }

    #[tokio::test]
    async fn concurrent_cache_miss_creates_one_session_per_key() {
        let transport = Arc::new(FakeTransport::new(vec![FakeTransport::json(
            200,
            "OK",
            r#"{"ret_msg":"Approved","session_id":"session-one"}"#,
        )]));
        let key_state = Arc::new(FakeKeyState::one());
        let audit = Arc::new(RecordingAudit::default());
        let manager = Arc::new(SessionManager::new(
            "https://example.invalid/paladinsapi.svc",
            Duration::from_secs(14 * 60),
            transport.clone(),
            key_state.clone(),
            audit.clone(),
            fixture_clock(),
        ));
        let key = fixture_key();

        let sessions = join_all((0..32).map(|_| {
            let manager = manager.clone();
            let key = key.clone();
            async move { manager.acquire_session(&key).await.expect("session") }
        }))
        .await;

        assert!(
            sessions
                .iter()
                .all(|session| session.session_key == "session-one")
        );
        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        assert_eq!(key_state.usage.load(Ordering::Relaxed), 1);
        assert_eq!(key_state.logs.lock().expect("logs").len(), 1);
        assert_eq!(audit.records.lock().expect("records").len(), 1);
    }

    #[tokio::test]
    async fn concurrent_misses_for_different_keys_never_share_identity() {
        let transport = Arc::new(FakeTransport::new(vec![
            FakeTransport::json(
                200,
                "OK",
                r#"{"ret_msg":"Approved","session_id":"session-first"}"#,
            ),
            FakeTransport::json(
                200,
                "OK",
                r#"{"ret_msg":"Approved","session_id":"session-second"}"#,
            ),
        ]));
        let key_state = Arc::new(FakeKeyState::one());
        let manager = Arc::new(SessionManager::new(
            "https://example.invalid/paladinsapi.svc",
            Duration::from_secs(14 * 60),
            transport.clone(),
            key_state,
            Arc::new(RecordingAudit::default()),
            fixture_clock(),
        ));
        let key_a = ApiCredential {
            dev_id: "key-a".to_owned(),
            auth_key: "secret-a".to_owned(),
        };
        let key_b = ApiCredential {
            dev_id: "key-b".to_owned(),
            auth_key: "secret-b".to_owned(),
        };

        let (session_a, session_b) = tokio::join!(
            manager.acquire_session(&key_a),
            manager.acquire_session(&key_b)
        );

        assert_eq!(session_a.expect("session A").dev_id, "key-a");
        assert_eq!(session_b.expect("session B").dev_id, "key-b");
        assert_eq!(transport.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn invalid_session_creation_is_not_cached() {
        let transport = Arc::new(FakeTransport::new(vec![
            FakeTransport::json(
                200,
                "OK",
                r#"{"ret_msg":"Exception while validating developer access"}"#,
            ),
            FakeTransport::json(
                200,
                "OK",
                r#"{"ret_msg":"Approved","session_id":"session-two"}"#,
            ),
        ]));
        let key_state = Arc::new(FakeKeyState::one());
        let manager = SessionManager::new(
            "https://example.invalid/paladinsapi.svc",
            Duration::from_secs(14 * 60),
            transport.clone(),
            key_state.clone(),
            Arc::new(RecordingAudit::default()),
            fixture_clock(),
        );
        let key = fixture_key();

        let error = manager
            .acquire_session(&key)
            .await
            .expect_err("first session must fail");
        assert!(error.to_string().contains("Hi-Rez Session API Error"));
        assert_eq!(
            manager
                .acquire_session(&key)
                .await
                .expect("second session")
                .session_key,
            "session-two"
        );
        assert_eq!(transport.calls.load(Ordering::Relaxed), 2);
        assert_eq!(key_state.usage.load(Ordering::Relaxed), 2);
        assert_eq!(key_state.failures.lock().expect("failures").len(), 1);
    }

    #[tokio::test]
    async fn cached_session_uses_no_additional_quota() {
        let transport = Arc::new(FakeTransport::new(vec![FakeTransport::json(
            200,
            "OK",
            r#"{"ret_msg":"Approved","session_id":"session-one"}"#,
        )]));
        let key_state = Arc::new(FakeKeyState::one());
        let manager = SessionManager::new(
            "https://example.invalid/paladinsapi.svc",
            Duration::from_secs(14 * 60),
            transport.clone(),
            key_state.clone(),
            Arc::new(RecordingAudit::default()),
            fixture_clock(),
        );
        let key = fixture_key();

        manager.acquire_session(&key).await.expect("first");
        manager.acquire_session(&key).await.expect("cached");

        assert_eq!(transport.calls.load(Ordering::Relaxed), 1);
        assert_eq!(key_state.usage.load(Ordering::Relaxed), 1);
    }
}
