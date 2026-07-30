use std::time::Duration;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use paladinscat_core::config::BackendConfig;
use reqwest::Client;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use uuid::Uuid;

use super::match_lifecycle::MatchLifecycleAction;

#[derive(Debug, thiserror::Error)]
pub enum WorkerRelayError {
    #[error("failed to construct HirezRelay HTTP client: {0}")]
    Client(#[source] reqwest::Error),
    #[error("HirezRelay transport failed: {0}")]
    Transport(#[source] reqwest::Error),
    #[error("HirezRelay {operation} failed: {message}")]
    Operation { operation: String, message: String },
    #[error("HirezRelay {operation} returned no result")]
    MissingResult { operation: String },
    #[error("getPlayerBatch accepts at most 20 unique positive player IDs; received {count}")]
    InvalidPlayerBatch { count: usize },
}

#[derive(Clone)]
pub struct WorkerRelayClient {
    client: Client,
    base_url: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct RelayCallRequest<'a> {
    operation: &'a str,
    args: Vec<Value>,
    request_id: String,
    attribution: RelayAttribution<'a>,
}

#[derive(Debug, Serialize)]
struct RelayAttribution<'a> {
    consumer: &'a str,
}

#[derive(Debug, Deserialize)]
struct RelayCallResponse<T> {
    ok: bool,
    result: Option<T>,
    error: Option<String>,
}

impl WorkerRelayClient {
    pub fn new(config: &BackendConfig) -> Result<Self, WorkerRelayError> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.hirez_relay_timeout_ms))
            .build()
            .map_err(WorkerRelayError::Client)?;
        Ok(Self {
            client,
            base_url: config.hirez_relay_url.trim_end_matches('/').to_owned(),
        })
    }

    async fn call<T: DeserializeOwned>(
        &self,
        operation: &str,
        args: Vec<Value>,
        consumer: &str,
    ) -> Result<T, WorkerRelayError> {
        let response = self
            .client
            .post(format!("{}/v1/call", self.base_url))
            .json(&RelayCallRequest {
                operation,
                args,
                request_id: Uuid::new_v4().to_string(),
                attribution: RelayAttribution { consumer },
            })
            .send()
            .await
            .map_err(WorkerRelayError::Transport)?;
        let status = response.status();
        let envelope = response
            .json::<RelayCallResponse<T>>()
            .await
            .map_err(WorkerRelayError::Transport)?;
        if !status.is_success() || !envelope.ok {
            return Err(WorkerRelayError::Operation {
                operation: operation.to_owned(),
                message: envelope
                    .error
                    .unwrap_or_else(|| format!("HTTP {}", status.as_u16())),
            });
        }
        envelope
            .result
            .ok_or_else(|| WorkerRelayError::MissingResult {
                operation: operation.to_owned(),
            })
    }
}

#[async_trait]
pub trait MatchLifecycleRelay: Send + Sync {
    async fn fetch_detail(
        &self,
        match_id: i64,
        queue_id: Option<i32>,
    ) -> Result<Value, WorkerRelayError>;
    async fn fetch_roster(&self, match_id: i64) -> Result<Vec<Value>, WorkerRelayError>;
    async fn fetch_history(&self, player_id: i64) -> Result<Vec<Value>, WorkerRelayError>;
    async fn fetch_demo(&self, match_id: i64) -> Result<Value, WorkerRelayError>;
}

#[async_trait]
impl MatchLifecycleRelay for WorkerRelayClient {
    async fn fetch_detail(
        &self,
        match_id: i64,
        queue_id: Option<i32>,
    ) -> Result<Value, WorkerRelayError> {
        let request = match queue_id {
            Some(queue_id) => json!({"matchId": match_id, "queueId": queue_id}),
            None => json!({"matchId": match_id}),
        };
        self.call(
            "getMatchDetailsBatch",
            vec![Value::Array(vec![request])],
            "rust_match_lifecycle",
        )
        .await
    }

    async fn fetch_roster(&self, match_id: i64) -> Result<Vec<Value>, WorkerRelayError> {
        self.call(
            "getPlayerBatchFromMatch",
            vec![json!(match_id)],
            "rust_match_recovery",
        )
        .await
    }

    async fn fetch_history(&self, player_id: i64) -> Result<Vec<Value>, WorkerRelayError> {
        // The relay owns the durable player-history cache. forceRefresh=false
        // is required: a cached target row is always preferred to quota.
        self.call(
            "getMatchHistory",
            vec![json!(player_id), json!(50), json!(false)],
            "rust_match_recovery",
        )
        .await
    }

    async fn fetch_demo(&self, match_id: i64) -> Result<Value, WorkerRelayError> {
        self.call(
            "getDemoDetails",
            vec![json!(match_id)],
            "rust_match_recovery",
        )
        .await
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct MatchLifecycleFetches {
    pub detail: Option<Value>,
    pub roster: Option<Vec<Value>>,
    pub histories: Vec<(i64, Vec<Value>)>,
    pub demo: Option<Value>,
}

pub async fn execute_match_lifecycle_fetches<R: MatchLifecycleRelay>(
    relay: &R,
    match_id: i64,
    queue_id: Option<i32>,
    actions: &[MatchLifecycleAction],
) -> Result<MatchLifecycleFetches, WorkerRelayError> {
    let mut fetched = MatchLifecycleFetches::default();
    for action in actions {
        match action {
            MatchLifecycleAction::FetchDetail => {
                fetched.detail = Some(relay.fetch_detail(match_id, queue_id).await?);
            }
            MatchLifecycleAction::FetchRoster => {
                fetched.roster = Some(relay.fetch_roster(match_id).await?);
            }
            MatchLifecycleAction::FetchHistories(player_ids) => {
                let results = stream::iter(player_ids.iter().copied())
                    .map(|player_id| async move {
                        relay
                            .fetch_history(player_id)
                            .await
                            .map(|history| (player_id, history))
                    })
                    .buffered(5)
                    .collect::<Vec<_>>()
                    .await;
                for result in results {
                    fetched.histories.push(result?);
                }
            }
            MatchLifecycleAction::FetchDemo => {
                fetched.demo = Some(relay.fetch_demo(match_id).await?);
            }
            MatchLifecycleAction::FinalizeFacts
            | MatchLifecycleAction::Project(_)
            | MatchLifecycleAction::Complete => {}
        }
    }
    Ok(fetched)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileBatchRequest {
    pub player_ids: Vec<i64>,
}

impl WorkerRelayClient {
    pub async fn fetch_player_batch(
        &self,
        request: &ProfileBatchRequest,
    ) -> Result<Vec<Value>, WorkerRelayError> {
        let mut player_ids = request
            .player_ids
            .iter()
            .copied()
            .filter(|player_id| *player_id > 0)
            .collect::<Vec<_>>();
        player_ids.sort_unstable();
        player_ids.dedup();
        if player_ids.is_empty() {
            return Ok(Vec::new());
        }
        if player_ids.len() > 20 {
            return Err(WorkerRelayError::InvalidPlayerBatch {
                count: player_ids.len(),
            });
        }
        self.call(
            "getPlayerBatch",
            vec![json!(player_ids)],
            "rust_unknown_identity_enrichment",
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    #[derive(Clone, Default)]
    struct RecordingRelay {
        calls: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingRelay {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls").clone()
        }

        fn record(&self, call: String) {
            self.calls.lock().expect("calls").push(call);
        }
    }

    #[async_trait]
    impl MatchLifecycleRelay for RecordingRelay {
        async fn fetch_detail(
            &self,
            match_id: i64,
            _queue_id: Option<i32>,
        ) -> Result<Value, WorkerRelayError> {
            self.record(format!("detail:{match_id}"));
            Ok(json!([]))
        }

        async fn fetch_roster(&self, match_id: i64) -> Result<Vec<Value>, WorkerRelayError> {
            self.record(format!("roster:{match_id}"));
            Ok(Vec::new())
        }

        async fn fetch_history(&self, player_id: i64) -> Result<Vec<Value>, WorkerRelayError> {
            self.record(format!("history:{player_id}"));
            Ok(Vec::new())
        }

        async fn fetch_demo(&self, match_id: i64) -> Result<Value, WorkerRelayError> {
            self.record(format!("demo:{match_id}"));
            Ok(Value::Null)
        }
    }

    #[tokio::test]
    async fn resumed_recovery_never_replays_detail_or_roster() {
        let relay = RecordingRelay::default();
        let fetched = execute_match_lifecycle_fetches(
            &relay,
            42,
            Some(424),
            &[
                MatchLifecycleAction::FetchHistories(vec![9, 10]),
                MatchLifecycleAction::FetchDemo,
            ],
        )
        .await
        .expect("fetches");

        assert_eq!(relay.calls(), vec!["history:9", "history:10", "demo:42"]);
        assert!(fetched.detail.is_none());
        assert!(fetched.roster.is_none());
        assert_eq!(fetched.histories.len(), 2);
        assert_eq!(fetched.demo, Some(Value::Null));
    }
}
