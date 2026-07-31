use std::collections::{BTreeMap, VecDeque};

use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    casual_mechanics::CasualMechanicsRepository,
    discovery_control::{
        claim_hourly_ingest_hour, mark_hourly_ingest_complete, mark_hourly_ingest_empty,
        mark_hourly_ingest_failed, mark_hourly_ingest_staged, mark_match_debt_complete,
        mark_match_debt_retryable, mark_match_debt_staged_or_complete, record_discovered_matches,
        record_hourly_ingest_quota_wait,
    },
    discovery_store::{
        MatchIdObservation, filter_already_handled_match_ids, record_match_count_discovery_result,
    },
    match_facts::{CanonicalMatchPayload, MatchFactRepository},
    policy::{MATCH_COUNT_QUEUE_DEFINITIONS, api_headroom_snapshot},
    ranked_projection::RankedProjectionRepository,
    relay::{WorkerRelayClient, WorkerRelayError},
};

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Relay(#[from] WorkerRelayError),
    #[error("match fact finalization failed: {0}")]
    Facts(String),
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryResult {
    pub queue_id: i32,
    pub date: String,
    pub hour: i32,
    pub discovered: usize,
    pub skipped: usize,
    pub completed: usize,
    pub retryable: usize,
    pub empty: bool,
}

#[derive(Clone)]
pub struct CanonicalIngestPipeline {
    database: Database,
    relay: WorkerRelayClient,
    facts: MatchFactRepository,
    casual: CasualMechanicsRepository,
    ranked: RankedProjectionRepository,
    reserve_per_key: i32,
}

impl CanonicalIngestPipeline {
    pub fn new(database: Database, config: &BackendConfig) -> Result<Self, WorkerRelayError> {
        Ok(Self {
            facts: MatchFactRepository::new(database.clone()),
            casual: CasualMechanicsRepository::new(database.clone()),
            ranked: RankedProjectionRepository::new(database.clone()),
            database,
            relay: WorkerRelayClient::new(config)?,
            reserve_per_key: std::env::var("API_KEY_RESERVE_CALLS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100),
        })
    }

    pub async fn discover_all_presence_queues(
        &self,
        date: &str,
        hour: i32,
        source: &str,
    ) -> Vec<Result<DiscoveryResult, PipelineError>> {
        let mut results = Vec::new();
        for queue in MATCH_COUNT_QUEUE_DEFINITIONS
            .iter()
            .filter(|queue| queue.track_presence)
        {
            results.push(
                self.discover_hour(queue.queue_id, date, hour, source, false)
                    .await,
            );
        }
        results
    }

    pub async fn discover_hour(
        &self,
        queue_id: i32,
        date: &str,
        hour: i32,
        source: &str,
        force: bool,
    ) -> Result<DiscoveryResult, PipelineError> {
        let mut result = DiscoveryResult {
            queue_id,
            date: date.to_owned(),
            hour,
            ..DiscoveryResult::default()
        };
        let budget = api_headroom_snapshot(&self.database, self.reserve_per_key).await?;
        if !budget.has_usable_keys {
            record_hourly_ingest_quota_wait(
                &self.database,
                date,
                hour,
                queue_id,
                source,
                "all API keys are inside the configured reserve",
            )
            .await?;
            return Ok(result);
        }
        if !claim_hourly_ingest_hour(&self.database, date, hour, queue_id, source, force).await? {
            return Ok(result);
        }
        let api_date = date.replace('-', "");
        let discovery = match self
            .relay
            .call_value(
                "getMatchIdsByQueueDetails",
                vec![json!(queue_id), json!(api_date), json!(hour)],
                "rust_hourly_discovery",
            )
            .await
        {
            Ok(value) => value,
            Err(error) => {
                mark_hourly_ingest_failed(
                    &self.database,
                    date,
                    hour,
                    queue_id,
                    &error.to_string(),
                    None,
                    None,
                )
                .await?;
                return Err(error.into());
            }
        };
        let observations = parse_observations(discovery);
        result.discovered = record_match_count_discovery_result(
            &self.database,
            date,
            hour,
            queue_id,
            &observations,
            source,
        )
        .await?;
        if observations.is_empty() {
            result.empty = true;
            mark_hourly_ingest_empty(&self.database, date, hour, queue_id).await?;
            return Ok(result);
        }
        let ids = observations
            .iter()
            .map(|row| row.match_id)
            .collect::<Vec<_>>();
        record_discovered_matches(
            &self.database,
            date,
            hour,
            queue_id,
            &ids,
            "discovered by hourly ingest",
        )
        .await?;
        let guard = filter_already_handled_match_ids(&self.database, &ids, true, true).await?;
        result.skipped = guard.skipped_ids.len();
        mark_match_debt_staged_or_complete(
            &self.database,
            &guard.skipped_ids,
            "staged or already handled by ingest guard",
        )
        .await?;
        if guard.fetch_ids.is_empty() {
            mark_hourly_ingest_complete(
                &self.database,
                date,
                hour,
                queue_id,
                i32::try_from(ids.len()).unwrap_or(i32::MAX),
            )
            .await?;
            result.completed = ids.len();
            return Ok(result);
        }
        let outcomes = self
            .fetch_completed_continuously(&guard.fetch_ids, queue_id)
            .await?;
        for outcome in outcomes {
            let match_id = extract_match_id(&outcome).unwrap_or_default();
            match CanonicalMatchPayload::from_relay_value(outcome) {
                Ok(payload) => {
                    let finalized = self
                        .facts
                        .finalize(&payload, source)
                        .await
                        .map_err(|error| PipelineError::Facts(error.to_string()))?;
                    if payload.queue_id == 486 {
                        self.ranked
                            .project_match(payload.match_id)
                            .await
                            .map_err(|error| PipelineError::Facts(error.to_string()))?;
                    } else {
                        let _ = self.casual.project_all_for_match(payload.match_id).await;
                    }
                    mark_match_debt_complete(&self.database, finalized.match_id).await?;
                    result.completed += 1;
                }
                Err(error) => {
                    if match_id > 0 {
                        mark_match_debt_retryable(
                            &self.database,
                            match_id,
                            &format!("no authoritative payload: {error}"),
                            None,
                        )
                        .await?;
                    }
                    result.retryable += 1;
                }
            }
        }
        mark_hourly_ingest_staged(
            &self.database,
            date,
            hour,
            queue_id,
            i32::try_from(ids.len()).unwrap_or(i32::MAX),
            i32::try_from(result.completed + result.skipped).unwrap_or(i32::MAX),
        )
        .await?;
        if result.retryable == 0 {
            mark_hourly_ingest_complete(
                &self.database,
                date,
                hour,
                queue_id,
                i32::try_from(ids.len()).unwrap_or(i32::MAX),
            )
            .await?;
        }
        Ok(result)
    }

    /// Preserves the relay's batch-of-ten contract and isolates only an
    /// omitted blocker. Successfully returned matches are never recalled.
    async fn fetch_completed_continuously(
        &self,
        match_ids: &[i64],
        queue_id: i32,
    ) -> Result<Vec<Value>, WorkerRelayError> {
        let mut pending = match_ids
            .iter()
            .copied()
            .filter(|id| *id > 0)
            .collect::<VecDeque<_>>();
        let mut outcomes = Vec::new();
        while !pending.is_empty() {
            let batch = pending.iter().take(10).copied().collect::<Vec<_>>();
            let requests = Value::Array(
                batch
                    .iter()
                    .map(|match_id| json!({"matchId":match_id,"queueId":queue_id}))
                    .collect(),
            );
            let response = self
                .relay
                .call_value("getMatchDetailsBatch", vec![requests], "rust_hourly_ingest")
                .await?;
            let returned = response
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![response]);
            let returned_ids = returned
                .iter()
                .filter_map(extract_match_id)
                .collect::<std::collections::BTreeSet<_>>();
            outcomes.extend(returned);
            for id in batch.iter().filter(|id| returned_ids.contains(id)) {
                if let Some(position) = pending.iter().position(|candidate| candidate == id) {
                    pending.remove(position);
                }
            }
            if let Some(blocker) = batch.iter().find(|id| !returned_ids.contains(id)).copied() {
                let singleton = self
                    .relay
                    .call_value(
                        "getMatchDetailsBatch",
                        vec![json!([{"matchId":blocker,"queueId":queue_id}])],
                        "rust_hourly_ingest",
                    )
                    .await?;
                outcomes.extend(
                    singleton
                        .as_array()
                        .cloned()
                        .unwrap_or_else(|| vec![singleton]),
                );
                if let Some(position) = pending.iter().position(|candidate| *candidate == blocker) {
                    pending.remove(position);
                }
            } else if returned_ids.is_empty() {
                break;
            }
        }
        Ok(outcomes)
    }
}

fn parse_observations(value: Value) -> Vec<MatchIdObservation> {
    let rows = value.as_array().cloned().unwrap_or_default();
    let mut observations = BTreeMap::new();
    for row in rows {
        let match_id = extract_match_id(&row).unwrap_or_default();
        if match_id <= 0 {
            continue;
        }
        observations.insert(
            match_id,
            MatchIdObservation {
                match_id,
                entry_datetime: text(&row, &["Entry_Datetime", "entry_datetime"]),
                region: text(&row, &["Region", "region"]),
                active_flag: boolean(&row, &["Active_Flag", "active_flag"]),
            },
        );
    }
    observations.into_values().collect()
}

fn extract_match_id(value: &Value) -> Option<i64> {
    for candidate in [Some(value), value.get("match"), value.get("payload")]
        .into_iter()
        .flatten()
    {
        for key in ["Match", "match_id", "matchId", "id"] {
            if let Some(id) = candidate
                .get(key)
                .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
                .filter(|id| *id > 0)
            {
                return Some(id);
            }
        }
    }
    None
}

fn text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key)?.as_str().map(str::to_owned))
}

fn boolean(value: &Value, keys: &[&str]) -> bool {
    keys.iter()
        .find_map(|key| value.get(*key)?.as_bool())
        .unwrap_or(false)
}
