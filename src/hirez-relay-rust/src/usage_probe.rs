use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::Value;

use crate::{
    key_pool::{AuthoritativeUsage, KeyPoolError, UsageProbe},
    session::{SessionManager, hirez_timestamp, parse_json_body, sign},
    upstream::{ApiCredential, SharedKeyState, SharedTransport},
};

pub struct DirectUsageProbe {
    base_url: String,
    transport: SharedTransport,
    key_state: SharedKeyState,
    sessions: Arc<SessionManager>,
}

impl DirectUsageProbe {
    pub fn new(
        base_url: impl Into<String>,
        transport: SharedTransport,
        key_state: SharedKeyState,
        sessions: Arc<SessionManager>,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            transport,
            key_state,
            sessions,
        }
    }

    pub async fn get_data_used_raw(&self, key: &ApiCredential) -> Result<Value, KeyPoolError> {
        let data = self.request_data_used(key).await?;
        Ok(match data {
            Value::Array(items) => items.into_iter().next().unwrap_or(Value::Null),
            value => value,
        })
    }

    async fn request_data_used(&self, key: &ApiCredential) -> Result<Value, KeyPoolError> {
        let session = self
            .sessions
            .acquire_session(key)
            .await
            .map_err(|error| KeyPoolError::Probe(error.to_string()))?;
        let timestamp = hirez_timestamp(self.sessions.clock())
            .map_err(|error| KeyPoolError::Probe(error.to_string()))?;
        let signature = sign("getdataused", &key.dev_id, &key.auth_key, &timestamp);
        let url = format!(
            "{}/getdatausedJson/{}/{}/{}/{}",
            self.base_url, key.dev_id, signature, session.session_key, timestamp
        );

        self.key_state.increment_usage(&key.dev_id);
        let started = std::time::Instant::now();
        let response = self.transport.get(&url, Duration::from_secs(15)).await;
        let response_time_ms = started.elapsed().as_millis() as u64;
        self.key_state
            .log_endpoint(&key.dev_id, "getdataused", response_time_ms, "quota_sync")
            .await;

        let response = match response {
            Ok(response) if (200..300).contains(&response.status) => response,
            Ok(response) => {
                self.key_state.record_failure(&key.dev_id, true).await;
                return Err(KeyPoolError::Probe(format!(
                    "HTTP {}: {}",
                    response.status, response.reason
                )));
            }
            Err(error) => {
                self.key_state.record_failure(&key.dev_id, false).await;
                return Err(KeyPoolError::Probe(error.to_string()));
            }
        };
        let data = match parse_json_body(&response.body) {
            Ok(data) => data,
            Err(error) => {
                self.key_state.record_failure(&key.dev_id, true).await;
                return Err(KeyPoolError::Probe(error.to_string()));
            }
        };
        self.key_state.record_success(&key.dev_id).await;
        Ok(data)
    }
}

#[async_trait]
impl UsageProbe for DirectUsageProbe {
    async fn get_data_used(
        &self,
        key: &ApiCredential,
    ) -> Result<Option<AuthoritativeUsage>, KeyPoolError> {
        let data = self.get_data_used_raw(key).await?;
        let Some(used) = number_field(&data, "Total_Requests_Today") else {
            return Ok(None);
        };
        Ok(Some(AuthoritativeUsage {
            used,
            reported_limit: number_field(&data, "Request_Limit_Daily"),
        }))
    }
}

fn number_field(value: &Value, field: &str) -> Option<u64> {
    let value = value.get(field)?;
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|number| u64::try_from(number).ok()))
        .or_else(|| value.as_str().and_then(|number| number.parse().ok()))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use time::macros::datetime;

    use super::*;
    use crate::{
        session::SessionManager,
        test_support::{FakeKeyState, FakeTransport, FixedClock, RecordingAudit},
    };

    fn fixture(
        data_response: Result<crate::upstream::HttpResponse, crate::upstream::TransportError>,
    ) -> (
        DirectUsageProbe,
        Arc<FakeTransport>,
        Arc<FakeKeyState>,
        ApiCredential,
    ) {
        let transport = Arc::new(FakeTransport::new(vec![
            FakeTransport::json(
                200,
                "OK",
                r#"{"ret_msg":"Approved","session_id":"session-one"}"#,
            ),
            data_response,
        ]));
        let keys = Arc::new(FakeKeyState::one());
        let sessions = Arc::new(SessionManager::new(
            "https://example.invalid/paladinsapi.svc",
            Duration::from_secs(14 * 60),
            transport.clone(),
            keys.clone(),
            Arc::new(RecordingAudit::default()),
            Arc::new(FixedClock::new(
                datetime!(2026-07-28 23:59:59 UTC).unix_timestamp() * 1_000,
            )),
        ));
        (
            DirectUsageProbe::new(
                "https://example.invalid/paladinsapi.svc",
                transport.clone(),
                keys.clone(),
                sessions,
            ),
            transport,
            keys,
            ApiCredential {
                dev_id: "1234".to_owned(),
                auth_key: "secret".to_owned(),
            },
        )
    }

    #[tokio::test]
    async fn extracts_array_response_and_counts_session_plus_probe() {
        let (probe, transport, keys, credential) = fixture(FakeTransport::json(
            200,
            "OK",
            r#"[{"Total_Requests_Today":4321,"Request_Limit_Daily":"7500"}]"#,
        ));

        let report = probe
            .get_data_used(&credential)
            .await
            .expect("probe")
            .expect("authoritative");

        assert_eq!(report.used, 4_321);
        assert_eq!(report.reported_limit, Some(7_500));
        assert_eq!(transport.calls.load(Ordering::Relaxed), 2);
        assert_eq!(keys.usage.load(Ordering::Relaxed), 2);
        assert_eq!(
            keys.logs.lock().expect("logs")[1],
            (
                "1234".to_owned(),
                "getdataused".to_owned(),
                "quota_sync".to_owned()
            )
        );
    }

    #[tokio::test]
    async fn empty_or_application_response_has_no_authoritative_usage() {
        let (probe, _, keys, credential) = fixture(FakeTransport::json(
            200,
            "OK",
            r#"[{"ret_msg":"Daily request limit reached"}]"#,
        ));

        assert!(
            probe
                .get_data_used(&credential)
                .await
                .expect("probe")
                .is_none()
        );
        assert_eq!(keys.successes.lock().expect("successes").len(), 2);
    }

    #[tokio::test]
    async fn failed_probe_is_counted_and_penalized_once() {
        let (probe, _, keys, credential) =
            fixture(FakeTransport::json(500, "Internal Server Error", "down"));

        assert!(probe.get_data_used(&credential).await.is_err());
        assert_eq!(keys.usage.load(Ordering::Relaxed), 2);
        assert_eq!(
            keys.failures.lock().expect("failures").as_slice(),
            [("1234".to_owned(), true)]
        );
    }
}
