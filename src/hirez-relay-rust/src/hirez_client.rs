use std::{sync::Arc, time::Duration};

use serde_json::Value;

use crate::{
    session::{SessionManager, hirez_timestamp, parse_json_body, sign},
    upstream::{SharedKeyState, SharedRetrySleeper, SharedTransport, UpstreamError},
};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(15);
const DEFAULT_MAX_RETRIES: usize = 3;

#[derive(Clone, Copy, Debug, Default)]
pub struct ApiRequestOptions {
    pub timeout: Option<Duration>,
    pub max_retries: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RetMsgAction {
    Session,
    Quota,
    Empty,
    Retry,
    Terminal,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct RetMsgClassification {
    action: RetMsgAction,
    code: &'static str,
}

pub struct HirezApiClient {
    base_url: String,
    transport: SharedTransport,
    key_state: SharedKeyState,
    sessions: Arc<SessionManager>,
    retry_sleeper: SharedRetrySleeper,
}

impl HirezApiClient {
    pub fn new(
        base_url: impl Into<String>,
        transport: SharedTransport,
        key_state: SharedKeyState,
        sessions: Arc<SessionManager>,
        retry_sleeper: SharedRetrySleeper,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_owned(),
            transport,
            key_state,
            sessions,
            retry_sleeper,
        }
    }

    pub async fn api_request(
        &self,
        method: &str,
        params: &[String],
        options: ApiRequestOptions,
        consumer: &str,
    ) -> Result<Value, UpstreamError> {
        let timeout = options.timeout.unwrap_or_else(|| endpoint_timeout(method));
        let max_retries = options.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let mut last_error: Option<String> = None;
        let mut last_app_error = String::new();
        let mut last_key_dev_id: Option<String> = None;
        let mut attempts_made = 0;
        let mut transport_failed = false;

        for attempt in 0..=max_retries {
            attempts_made = attempt + 1;
            let (key, session) = match self.sessions.get_active_session().await {
                Ok(active) => active,
                Err(error) => {
                    last_error = Some(error.to_string());
                    if attempt < max_retries {
                        self.retry_sleeper.sleep_before_retry(attempt + 1).await;
                        continue;
                    }
                    break;
                }
            };
            last_key_dev_id = Some(key.dev_id.clone());

            let timestamp = hirez_timestamp(self.sessions_clock())?;
            let signature = sign(method, &key.dev_id, &key.auth_key, &timestamp);
            let mut url = format!(
                "{}/{}Json/{}/{}/{}/{}",
                self.base_url, method, key.dev_id, signature, session.session_key, timestamp
            );
            if !params.is_empty() {
                url.push('/');
                url.push_str(&params.join("/"));
            }

            self.key_state.increment_usage(&key.dev_id);
            let started = std::time::Instant::now();
            let response = self.transport.get(&url, timeout).await;
            let response_time_ms = started.elapsed().as_millis() as u64;
            self.key_state
                .log_endpoint(&key.dev_id, method, response_time_ms, consumer)
                .await;

            let data = match response {
                Ok(response) if (200..300).contains(&response.status) => {
                    match parse_json_body(&response.body) {
                        Ok(data) => data,
                        Err(error) => {
                            last_error = Some(error.to_string());
                            if attempt < max_retries {
                                self.retry_sleeper.sleep_before_retry(attempt + 1).await;
                                continue;
                            }
                            break;
                        }
                    }
                }
                Ok(response) => {
                    let message = format!("HTTP {}: {}", response.status, response.reason);
                    if response.status == 503 {
                        return Err(UpstreamError::Protocol(format!(
                            "API temporarily unavailable (503): {message}"
                        )));
                    }
                    last_error = Some(message);
                    if attempt < max_retries {
                        self.retry_sleeper.sleep_before_retry(attempt + 1).await;
                        continue;
                    }
                    break;
                }
                Err(error) => {
                    transport_failed = true;
                    last_error = Some(error.to_string());
                    if attempt < max_retries {
                        self.retry_sleeper.sleep_before_retry(attempt + 1).await;
                        continue;
                    }
                    break;
                }
            };

            match first_ret_msg(&data) {
                Some(_ret_msg) if should_preserve_broken_skin_batch_response(method, &data) => {
                    self.key_state.record_success(&key.dev_id).await;
                    return Ok(data);
                }
                Some(ret_msg) => {
                    last_app_error = ret_msg.to_owned();
                    let classification = classify_ret_msg(ret_msg);
                    match classification.action {
                        RetMsgAction::Session => {
                            self.sessions.invalidate_session(&key.dev_id).await;
                            continue;
                        }
                        RetMsgAction::Quota => {
                            self.key_state.sync_usage(&key.dev_id).await;
                            continue;
                        }
                        RetMsgAction::Empty => {
                            self.key_state.record_success(&key.dev_id).await;
                            return Ok(Value::Array(Vec::new()));
                        }
                        RetMsgAction::Retry => {
                            last_error = Some(format!("{}: {ret_msg}", classification.code));
                            if attempt < max_retries {
                                self.retry_sleeper.sleep_before_retry(attempt + 1).await;
                                continue;
                            }
                            break;
                        }
                        RetMsgAction::Terminal | RetMsgAction::Unknown => {
                            last_error = Some(format!("{}: {ret_msg}", classification.code));
                            break;
                        }
                    }
                }
                None => {
                    self.key_state.record_success(&key.dev_id).await;
                    return Ok(data);
                }
            }
        }

        let message = last_error
            .filter(|message| !message.is_empty())
            .unwrap_or_else(|| {
                if last_app_error.is_empty() {
                    "Unknown application error".to_owned()
                } else {
                    last_app_error
                }
            });
        let terminal_vendor_return = message.contains("HIREZ_NOT_FOUND_OR_INVALID")
            || message.contains("HIREZ_UNKNOWN_RETURN");
        let is_key_fault = !transport_failed
            && !terminal_vendor_return
            && !message.contains("404")
            && !message.contains("422")
            && !message.contains("ECONNRESET")
            && !message.contains("ETIMEDOUT");
        if let Some(dev_id) = last_key_dev_id {
            self.key_state.record_failure(&dev_id, is_key_fault).await;
        }
        Err(UpstreamError::Application(format!(
            "Request failed after {attempts_made} attempt{}: {message}",
            if attempts_made == 1 { "" } else { "s" }
        )))
    }

    fn sessions_clock(&self) -> &dyn crate::upstream::Clock {
        self.sessions.clock()
    }
}

fn classify_ret_msg(ret_msg: &str) -> RetMsgClassification {
    let message = ret_msg.to_lowercase();
    if message.contains("invalid session") {
        return RetMsgClassification {
            action: RetMsgAction::Session,
            code: "HIREZ_INVALID_SESSION",
        };
    }
    if message.contains("daily request limit") {
        return RetMsgClassification {
            action: RetMsgAction::Quota,
            code: "HIREZ_DAILY_LIMIT",
        };
    }
    if message.contains("no match history") {
        return RetMsgClassification {
            action: RetMsgAction::Empty,
            code: "HIREZ_NO_MATCH_HISTORY",
        };
    }
    if message.contains("privacy flag") || message.contains("private") {
        return RetMsgClassification {
            action: RetMsgAction::Empty,
            code: "HIREZ_PRIVACY_FLAG",
        };
    }
    if message.contains("not found")
        || message.contains("invalid player")
        || message.contains("invalid match")
    {
        return RetMsgClassification {
            action: RetMsgAction::Terminal,
            code: "HIREZ_NOT_FOUND_OR_INVALID",
        };
    }
    if message.contains("exception")
        || message.contains("maintenance")
        || message.contains("temporarily")
        || message.contains("timeout")
    {
        return RetMsgClassification {
            action: RetMsgAction::Retry,
            code: "HIREZ_RETRYABLE_RETURN",
        };
    }
    RetMsgClassification {
        action: RetMsgAction::Unknown,
        code: "HIREZ_UNKNOWN_RETURN",
    }
}

fn first_ret_msg(data: &Value) -> Option<&str> {
    match data {
        Value::Array(items) => items.iter().find_map(|item| {
            item.get("ret_msg")
                .and_then(Value::as_str)
                .filter(|message| !message.is_empty())
        }),
        Value::Object(object) => object
            .get("ret_msg")
            .and_then(Value::as_str)
            .filter(|message| !message.is_empty()),
        _ => None,
    }
}

fn should_preserve_broken_skin_batch_response(method: &str, data: &Value) -> bool {
    if !method.eq_ignore_ascii_case("getmatchdetailsbatch") {
        return false;
    }
    let Value::Array(items) = data else {
        return false;
    };
    let ret_messages = items
        .iter()
        .filter_map(|item| {
            item.get("ret_msg")
                .and_then(Value::as_str)
                .filter(|message| !message.is_empty())
        })
        .collect::<Vec<_>>();
    !ret_messages.is_empty()
        && ret_messages.iter().all(|message| {
            let normalized = message.to_lowercase().replace([' ', '_'], "");
            normalized.contains("int16") && normalized.contains("skinid")
        })
}

fn endpoint_timeout(method: &str) -> Duration {
    match method {
        "getgods"
        | "getitems"
        | "getchampions"
        | "searchplayers"
        | "getqueuestats"
        | "getbountyitems"
        | "getmatchdetails"
        | "getmatchhistory"
        | "getplayeridbyname"
        | "getplayerloadouts"
        | "getplayerstatus"
        | "getmatchplayerdetails"
        | "getplayeridsbygamertag"
        | "getplayeridbyportaluserid" => Duration::from_secs(10),
        "getfriends" | "getplayerbatch" | "getmatchidsbyqueue" | "getmatchdetailsbatch" => {
            Duration::from_secs(20)
        }
        "getchampionskins" => Duration::from_secs(30),
        _ => DEFAULT_TIMEOUT,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::Ordering};

    use time::macros::datetime;

    use super::*;
    use crate::{
        session::SessionManager,
        test_support::{FakeKeyState, FakeTransport, FixedClock, NoopRetrySleeper, RecordingAudit},
        upstream::{ApiCredential, TransportError},
    };

    struct Fixture {
        client: HirezApiClient,
        transport: Arc<FakeTransport>,
        keys: Arc<FakeKeyState>,
        sleeper: Arc<NoopRetrySleeper>,
    }

    fn fixture(
        responses: Vec<Result<crate::upstream::HttpResponse, TransportError>>,
        keys: FakeKeyState,
    ) -> Fixture {
        let transport = Arc::new(FakeTransport::new(responses));
        let keys = Arc::new(keys);
        let clock = Arc::new(FixedClock::new(
            datetime!(2026-07-28 23:59:59 UTC).unix_timestamp() * 1_000,
        ));
        let sessions = Arc::new(SessionManager::new(
            "https://example.invalid/paladinsapi.svc",
            Duration::from_secs(14 * 60),
            transport.clone(),
            keys.clone(),
            Arc::new(RecordingAudit::default()),
            clock,
        ));
        let sleeper = Arc::new(NoopRetrySleeper::default());
        let client = HirezApiClient::new(
            "https://example.invalid/paladinsapi.svc",
            transport.clone(),
            keys.clone(),
            sessions,
            sleeper.clone(),
        );
        Fixture {
            client,
            transport,
            keys,
            sleeper,
        }
    }

    fn approved(session_id: &str) -> Result<crate::upstream::HttpResponse, TransportError> {
        FakeTransport::json(
            200,
            "OK",
            &format!(r#"{{"ret_msg":"Approved","session_id":"{session_id}"}}"#),
        )
    }

    #[tokio::test]
    async fn healthy_calls_reuse_session_and_preserve_endpoint_timeout() {
        let fixture = fixture(
            vec![
                approved("session-one"),
                FakeTransport::json(200, "OK", r#"[{"Match":128000001}]"#),
                FakeTransport::json(200, "OK", r#"[{"Match":128000002}]"#),
            ],
            FakeKeyState::one(),
        );

        for match_id in ["128000001", "128000002"] {
            fixture
                .client
                .api_request(
                    "getmatchdetailsbatch",
                    &[match_id.to_owned()],
                    ApiRequestOptions::default(),
                    "ranked_ingest",
                )
                .await
                .expect("healthy response");
        }

        assert_eq!(fixture.transport.calls.load(Ordering::Relaxed), 3);
        assert_eq!(fixture.keys.usage.load(Ordering::Relaxed), 3);
        let timeouts = fixture.transport.timeouts.lock().expect("timeouts");
        assert_eq!(timeouts[0], Duration::from_secs(10));
        assert_eq!(timeouts[1], Duration::from_secs(20));
        assert_eq!(timeouts[2], Duration::from_secs(20));
        let urls = fixture.transport.urls.lock().expect("urls");
        assert!(urls[0].contains("/createsessionJson/1234/"));
        assert!(urls[1].contains("/getmatchdetailsbatchJson/1234/"));
        assert!(urls[1].ends_with("/128000001"));
    }

    #[tokio::test]
    async fn invalid_session_is_invalidated_and_recreated_without_backoff() {
        let fixture = fixture(
            vec![
                approved("session-one"),
                FakeTransport::json(200, "OK", r#"[{"ret_msg":"Invalid session id."}]"#),
                approved("session-two"),
                FakeTransport::json(200, "OK", r#"[{"Match":128000001}]"#),
            ],
            FakeKeyState::one(),
        );

        let result = fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001".to_owned()],
                ApiRequestOptions::default(),
                "ranked_ingest",
            )
            .await
            .expect("retried response");

        assert_eq!(result[0]["Match"], 128000001);
        assert_eq!(fixture.transport.calls.load(Ordering::Relaxed), 4);
        assert_eq!(fixture.keys.usage.load(Ordering::Relaxed), 4);
        assert_eq!(fixture.sleeper.calls.load(Ordering::Relaxed), 0);
        let urls = fixture.transport.urls.lock().expect("urls");
        assert!(urls[1].contains("/session-one/"));
        assert!(urls[3].contains("/session-two/"));
    }

    #[tokio::test]
    async fn repeated_invalid_sessions_exhaust_the_configured_attempt_budget() {
        let fixture = fixture(
            (1..=4)
                .flat_map(|attempt| {
                    [
                        approved(&format!("session-{attempt}")),
                        FakeTransport::json(200, "OK", r#"[{"ret_msg":"Invalid session id."}]"#),
                    ]
                })
                .collect(),
            FakeKeyState::one(),
        );

        let error = fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001".to_owned()],
                ApiRequestOptions::default(),
                "ranked_ingest",
            )
            .await
            .expect_err("invalid sessions must exhaust");

        assert!(error.to_string().contains("failed after 4 attempts"));
        assert!(error.to_string().contains("Invalid session id."));
        assert_eq!(fixture.transport.calls.load(Ordering::Relaxed), 8);
        assert_eq!(fixture.keys.usage.load(Ordering::Relaxed), 8);
        assert_eq!(fixture.sleeper.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn quota_return_reselects_key_on_next_attempt() {
        let keys = FakeKeyState::new(vec![
            ApiCredential {
                dev_id: "key-a".to_owned(),
                auth_key: "secret-a".to_owned(),
            },
            ApiCredential {
                dev_id: "key-b".to_owned(),
                auth_key: "secret-b".to_owned(),
            },
        ])
        .with_rotation();
        let fixture = fixture(
            vec![
                approved("session-a"),
                FakeTransport::json(200, "OK", r#"[{"ret_msg":"Daily request limit reached"}]"#),
                approved("session-b"),
                FakeTransport::json(200, "OK", r#"[{"Match":128000001}]"#),
            ],
            keys,
        );

        fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001".to_owned()],
                ApiRequestOptions::default(),
                "ranked_ingest",
            )
            .await
            .expect("rotated response");

        assert_eq!(
            fixture.keys.syncs.lock().expect("syncs").as_slice(),
            ["key-a"]
        );
        let urls = fixture.transport.urls.lock().expect("urls");
        assert!(urls[1].contains("/key-a/"));
        assert!(urls[3].contains("/key-b/"));
    }

    #[tokio::test]
    async fn http_404_retries_and_records_non_key_failure() {
        let fixture = fixture(
            vec![
                approved("session-one"),
                FakeTransport::json(404, "Not Found", "missing"),
                FakeTransport::json(404, "Not Found", "missing"),
            ],
            FakeKeyState::one(),
        );

        let error = fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001".to_owned()],
                ApiRequestOptions {
                    max_retries: Some(1),
                    ..ApiRequestOptions::default()
                },
                "ranked_ingest",
            )
            .await
            .expect_err("404 must fail");

        assert!(error.to_string().contains("failed after 2 attempts"));
        assert_eq!(fixture.sleeper.calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            fixture.keys.failures.lock().expect("failures").as_slice(),
            [("1234".to_owned(), false)]
        );
    }

    #[tokio::test]
    async fn http_429_and_500_retry_and_record_key_failures() {
        for (status, reason) in [(429, "Too Many Requests"), (500, "Internal Server Error")] {
            let fixture = fixture(
                vec![
                    approved("session-one"),
                    FakeTransport::json(status, reason, "failed"),
                    FakeTransport::json(status, reason, "failed"),
                ],
                FakeKeyState::one(),
            );

            let error = fixture
                .client
                .api_request(
                    "getmatchdetailsbatch",
                    &["128000001".to_owned()],
                    ApiRequestOptions {
                        max_retries: Some(1),
                        ..ApiRequestOptions::default()
                    },
                    "ranked_ingest",
                )
                .await
                .expect_err("HTTP status must fail");

            assert!(error.to_string().contains("failed after 2 attempts"));
            assert_eq!(fixture.sleeper.calls.load(Ordering::Relaxed), 1);
            assert_eq!(
                fixture.keys.failures.lock().expect("failures").as_slice(),
                [("1234".to_owned(), true)]
            );
        }
    }

    #[tokio::test]
    async fn network_and_timeout_failures_are_counted_before_io() {
        for failure in [
            TransportError::Network("DNS lookup failed".to_owned()),
            TransportError::Timeout,
        ] {
            let fixture = fixture(
                vec![approved("session-one"), Err(failure)],
                FakeKeyState::one(),
            );

            fixture
                .client
                .api_request(
                    "getmatchdetailsbatch",
                    &["128000001".to_owned()],
                    ApiRequestOptions {
                        max_retries: Some(0),
                        ..ApiRequestOptions::default()
                    },
                    "ranked_ingest",
                )
                .await
                .expect_err("transport failure");

            assert_eq!(fixture.transport.calls.load(Ordering::Relaxed), 2);
            assert_eq!(fixture.keys.usage.load(Ordering::Relaxed), 2);
            assert_eq!(fixture.keys.logs.lock().expect("logs").len(), 2);
            assert_eq!(
                fixture.keys.failures.lock().expect("failures").as_slice(),
                [("1234".to_owned(), false)],
                "transport failures must not poison credential health"
            );
        }
    }

    #[tokio::test]
    async fn retryable_http_200_application_return_retries_then_succeeds() {
        let fixture = fixture(
            vec![
                approved("session-one"),
                FakeTransport::json(
                    200,
                    "OK",
                    r#"[{"ret_msg":"Service temporarily under maintenance"}]"#,
                ),
                FakeTransport::json(200, "OK", r#"[{"Match":128000001}]"#),
            ],
            FakeKeyState::one(),
        );

        let result = fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001".to_owned()],
                ApiRequestOptions {
                    max_retries: Some(1),
                    ..ApiRequestOptions::default()
                },
                "ranked_ingest",
            )
            .await
            .expect("retryable return");

        assert_eq!(result[0]["Match"], 128000001);
        assert_eq!(fixture.sleeper.calls.load(Ordering::Relaxed), 1);
        assert_eq!(fixture.keys.usage.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn http_503_is_terminal_without_retry_or_key_failure() {
        let fixture = fixture(
            vec![
                approved("session-one"),
                FakeTransport::json(503, "Service Unavailable", "down"),
            ],
            FakeKeyState::one(),
        );

        let error = fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001".to_owned()],
                ApiRequestOptions::default(),
                "ranked_ingest",
            )
            .await
            .expect_err("503 must fail");

        assert!(error.to_string().contains("temporarily unavailable (503)"));
        assert_eq!(fixture.sleeper.calls.load(Ordering::Relaxed), 0);
        assert!(fixture.keys.failures.lock().expect("failures").is_empty());
    }

    #[tokio::test]
    async fn privacy_and_missing_history_returns_are_successful_empty_arrays() {
        for ret_msg in [
            "No Match History for player",
            "Player has privacy flag enabled",
        ] {
            let fixture = fixture(
                vec![
                    approved("session-one"),
                    FakeTransport::json(200, "OK", &format!(r#"[{{"ret_msg":"{ret_msg}"}}]"#)),
                ],
                FakeKeyState::one(),
            );

            assert_eq!(
                fixture
                    .client
                    .api_request(
                        "getmatchhistory",
                        &["123".to_owned()],
                        ApiRequestOptions::default(),
                        "player_lookup",
                    )
                    .await
                    .expect("empty application return"),
                Value::Array(Vec::new())
            );
        }
    }

    #[tokio::test]
    async fn broken_skin_batch_sentinel_preserves_healthy_prefix() {
        let fixture = fixture(
            vec![
                approved("session-one"),
                FakeTransport::json(
                    200,
                    "OK",
                    r#"[{"Match":128000001},{"Match":128000002,"ret_msg":"Value was too large for an Int16 while reading Skin_Id"}]"#,
                ),
            ],
            FakeKeyState::one(),
        );

        let result = fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001,128000002".to_owned()],
                ApiRequestOptions::default(),
                "ranked_ingest",
            )
            .await
            .expect("partial ordered stream");

        assert_eq!(result.as_array().expect("array").len(), 2);
        assert_eq!(fixture.transport.calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn malformed_json_retries_with_the_same_cached_session() {
        let fixture = fixture(
            vec![
                approved("session-one"),
                FakeTransport::json(200, "OK", "<html>bad</html>"),
                FakeTransport::json(200, "OK", r#"[{"Match":128000001}]"#),
            ],
            FakeKeyState::one(),
        );

        fixture
            .client
            .api_request(
                "getmatchdetailsbatch",
                &["128000001".to_owned()],
                ApiRequestOptions {
                    max_retries: Some(1),
                    ..ApiRequestOptions::default()
                },
                "ranked_ingest",
            )
            .await
            .expect("second JSON response");

        assert_eq!(fixture.transport.calls.load(Ordering::Relaxed), 3);
        assert_eq!(fixture.keys.usage.load(Ordering::Relaxed), 3);
        assert_eq!(fixture.sleeper.calls.load(Ordering::Relaxed), 1);
    }
}
