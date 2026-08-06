use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde::Serialize;
use serde_json::{Map, Value, json};

use super::{
    discovery_control::{
        claim_hourly_ingest_hour, due_match_debt_ids, mark_hourly_ingest_complete,
        mark_hourly_ingest_empty, mark_hourly_ingest_failed, mark_hourly_ingest_staged,
        mark_match_debt_retryable, mark_match_debt_staged_or_complete, record_discovered_matches,
        record_hourly_ingest_quota_wait,
    },
    discovery_store::{
        MatchIdObservation, filter_already_handled_match_ids, record_match_count_discovery_result,
    },
    match_facts::CanonicalMatchPayload,
    nonranked_acquisition::{NonrankedAcquisitionClaim, NonrankedAcquisitionRepository},
    outage::{
        MATCH_DETAIL_SERVICE_OUTAGE_KEY, classify_hirez_service_outage_message,
        mark_hirez_service_recovered, record_hirez_service_outage,
    },
    policy::{
        MATCH_COUNT_QUEUE_DEFINITIONS, api_headroom_snapshot, calculate_background_match_allowance,
        ranked_priority_reserve_snapshot,
    },
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

#[derive(Debug, Default)]
struct RankedFailureContext {
    claimed_raw_match_count: Option<i32>,
    checkpointed_ids: HashSet<i64>,
}

impl RankedFailureContext {
    fn checkpointed_count(&self) -> Option<i32> {
        (!self.checkpointed_ids.is_empty())
            .then(|| i32::try_from(self.checkpointed_ids.len()).unwrap_or(i32::MAX))
    }
}

const CLAIM_INCOMPLETE_NONRANKED_SQL: &str = r#"
WITH due AS (
  SELECT match_id, queue_id
  FROM nonranked_match_acquisition
  WHERE (status = 'discovered' OR (status = 'waiting_for_completion'
         AND last_observed_at <= now() - ($2::int * interval '1 minute')))
    AND (lease_until IS NULL OR lease_until <= now())
  ORDER BY
    CASE WHEN source_date + (source_hour * interval '1 hour') >= (now() AT TIME ZONE 'UTC') - interval '24 hours' THEN 0 ELSE 1 END,
    CASE WHEN source_date + (source_hour * interval '1 hour') >= (now() AT TIME ZONE 'UTC') - interval '24 hours' THEN source_date + (source_hour * interval '1 hour') END DESC,
    CASE WHEN source_date + (source_hour * interval '1 hour') < (now() AT TIME ZONE 'UTC') - interval '24 hours' THEN source_date + (source_hour * interval '1 hour') END ASC,
    match_id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE nonranked_match_acquisition acquisition
SET status = 'fetching', detail_attempts = detail_attempts + 1,
    last_attempt_at = now(), lease_until = now() + interval '30 minutes',
    error_message = NULL, updated_at = now()
FROM due
WHERE acquisition.match_id = due.match_id
RETURNING acquisition.match_id, acquisition.queue_id, acquisition.source_date::text,
          acquisition.source_hour, acquisition.region,
          acquisition.discovered_entry_datetime::text, acquisition.active_flag
"#;

#[derive(Clone)]
pub struct CanonicalIngestPipeline {
    database: Database,
    relay: WorkerRelayClient,
    nonranked: NonrankedAcquisitionRepository,
    reserve_per_key: i32,
}

impl CanonicalIngestPipeline {
    pub fn new(database: Database, config: &BackendConfig) -> Result<Self, WorkerRelayError> {
        Ok(Self {
            nonranked: NonrankedAcquisitionRepository::new(database.clone()),
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
        // TS takes one snapshot for the complete queue pass. If no key is
        // usable, it returns before claiming or writing any queue-hour state.
        let budget = match api_headroom_snapshot(&self.database, self.reserve_per_key).await {
            Ok(budget) => budget,
            Err(error) => return vec![Err(error.into())],
        };
        if !budget.has_usable_keys {
            tracing::warn!(
                "presence discovery has no usable Hi-Rez key headroom; leaving queue-hours untouched"
            );
            return Vec::new();
        }
        let mut results = Vec::new();
        for queue in MATCH_COUNT_QUEUE_DEFINITIONS
            .iter()
            .filter(|queue| queue.track_presence && !queue.ranked)
        {
            results.push(
                self.discover_presence_hour(queue.queue_id, date, hour, source)
                    .await,
            );
        }
        results
    }

    /// Presence discovery has exactly one vendor call: getmatchidsbyqueue.
    /// It records counts and the non-ranked acquisition ledger, then finalizes
    /// the hour. Detail/roster acquisition is deliberately a separate pass.
    pub async fn discover_presence_hour(
        &self,
        queue_id: i32,
        date: &str,
        hour: i32,
        source: &str,
    ) -> Result<DiscoveryResult, PipelineError> {
        let result = self
            .discover_presence_hour_inner(queue_id, date, hour, source)
            .await;
        if let Err(error) = &result {
            let _ = mark_hourly_ingest_failed(
                &self.database,
                date,
                hour,
                queue_id,
                &pipeline_state_error_message(error),
                None,
                None,
            )
            .await;
        }
        result
    }

    async fn discover_presence_hour_inner(
        &self,
        queue_id: i32,
        date: &str,
        hour: i32,
        source: &str,
    ) -> Result<DiscoveryResult, PipelineError> {
        let mut result = DiscoveryResult {
            queue_id,
            date: date.to_owned(),
            hour,
            ..DiscoveryResult::default()
        };
        let Some(definition) = MATCH_COUNT_QUEUE_DEFINITIONS
            .iter()
            .find(|definition| definition.queue_id == queue_id)
        else {
            return Err(PipelineError::Facts(format!(
                "queue {queue_id} is not configured for presence discovery"
            )));
        };
        if definition.ranked || !definition.track_presence {
            return Err(PipelineError::Facts(format!(
                "queue {queue_id} is not a non-ranked presence queue"
            )));
        }
        if !claim_hourly_ingest_hour(&self.database, date, hour, queue_id, source, false).await? {
            return Ok(result);
        }
        let discovery = self
            .relay
            .call_value(
                "getMatchIdsByQueueDetails",
                vec![json!(queue_id), json!(date.replace('-', "")), json!(hour)],
                "presence_discovery",
            )
            .await?;
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
        result.empty = observations.is_empty();
        // TS presence discovery treats a successful empty response as final;
        // only an absent or failed cron window is eligible for backfill.
        mark_hourly_ingest_complete(
            &self.database,
            date,
            hour,
            queue_id,
            i32::try_from(result.discovered).unwrap_or(i32::MAX),
        )
        .await?;
        result.completed = result.discovered;
        Ok(result)
    }

    /// Scheduler-facing spelling: this is deliberately discovery-only and
    /// cannot acquire completed-match detail.
    pub async fn discover_presence_only(
        &self,
        queue_id: i32,
        date: &str,
        hour: i32,
        source: &str,
    ) -> Result<DiscoveryResult, PipelineError> {
        self.discover_presence_hour(queue_id, date, hour, source)
            .await
    }

    /// Ranked discovery is the only hourly path that can acquire detail. In
    /// debt-only mode it starts from durable exact IDs and never rediscoveries
    /// the hour through getmatchidsbyqueue.
    pub async fn discover_ranked_hour(
        &self,
        date: &str,
        hour: i32,
        source: &str,
        debt_only: bool,
    ) -> Result<DiscoveryResult, PipelineError> {
        let mut failure = RankedFailureContext::default();
        let result = self
            .discover_ranked_hour_inner(date, hour, source, debt_only, &mut failure)
            .await;
        if let Err(error) = &result {
            mark_ranked_discovery_failure(&self.database, date, hour, error, &failure).await?;
        }
        result
    }

    async fn discover_ranked_hour_inner(
        &self,
        date: &str,
        hour: i32,
        source: &str,
        debt_only: bool,
        failure: &mut RankedFailureContext,
    ) -> Result<DiscoveryResult, PipelineError> {
        let queue_id = 486;
        let mut result = DiscoveryResult {
            queue_id,
            date: date.to_owned(),
            hour,
            ..DiscoveryResult::default()
        };
        // cleanupFetchedPlayersCache is an awaited relay signal in TS. Its
        // failure is warning-only; discovery and its headroom check continue.
        if let Err(error) = self
            .relay
            .call_value(
                "cleanupFetchedPlayersCache",
                Vec::new(),
                "backend_unattributed",
            )
            .await
        {
            tracing::warn!(date, hour, %error, "failed to clear relay recovery cache before ranked discovery");
        }
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
        // Normal ranked discovery merges fresh IDs with due durable debt too;
        // debt-only changes only whether getmatchidsbyqueue is called.
        let due_debt = due_match_debt_ids(&self.database, date, hour, queue_id, 250, false).await?;
        if debt_only && due_debt.is_empty() {
            return Ok(result);
        }
        if !claim_hourly_ingest_hour(&self.database, date, hour, queue_id, source, debt_only)
            .await?
        {
            return Ok(result);
        }
        let observations = if debt_only {
            Vec::new()
        } else {
            let discovery = match self
                .relay
                .call_value(
                    "getMatchIdsByQueueDetails",
                    vec![json!(queue_id), json!(date.replace('-', "")), json!(hour)],
                    "ranked_discovery",
                )
                .await
            {
                Ok(value) => value,
                Err(error) => return Err(error.into()),
            };
            parse_observations(discovery)
        };
        if !debt_only {
            result.discovered = observations.len();
            match record_match_count_discovery_result(
                &self.database,
                date,
                hour,
                queue_id,
                &observations,
                source,
            )
            .await
            {
                Ok(discovered) => result.discovered = discovered,
                Err(error) => {
                    tracing::warn!(%error, "ranked match-count mirror failed without blocking acquisition")
                }
            }
        }
        if !debt_only && observations.is_empty() && due_debt.is_empty() {
            result.empty = true;
            mark_hourly_ingest_empty(&self.database, date, hour, queue_id).await?;
            return Ok(result);
        }
        let discovered_ids = observations
            .iter()
            .map(|row| row.match_id)
            .collect::<Vec<_>>();
        let ids = stable_merge_ids(&discovered_ids, &due_debt);
        record_discovered_matches(
            &self.database,
            date,
            hour,
            queue_id,
            &ids,
            if due_debt.is_empty() {
                "discovered by hourly ingest"
            } else {
                "hourly discovery plus unresolved debt retry"
            },
        )
        .await?;
        let guard =
            filter_already_handled_match_ids(&self.database, &ids, queue_id, true, true).await?;
        result.skipped = guard.skipped_ids.len();
        mark_match_debt_staged_or_complete(
            &self.database,
            &guard.skipped_ids,
            "staged or already handled by ingest guard",
        )
        .await?;
        if guard.fetch_ids.is_empty() {
            if guard.skipped.raw_buffer > 0 || guard.skipped.pull_list > 0 {
                mark_hourly_ingest_staged(
                    &self.database,
                    date,
                    hour,
                    queue_id,
                    i32::try_from(ids.len()).unwrap_or(i32::MAX),
                    0,
                )
                .await?;
            } else {
                mark_hourly_ingest_complete(
                    &self.database,
                    date,
                    hour,
                    queue_id,
                    i32::try_from(ids.len()).unwrap_or(i32::MAX),
                )
                .await?;
            }
            result.completed = ids.len();
            return Ok(result);
        }
        failure.claimed_raw_match_count = Some(i32::try_from(ids.len()).unwrap_or(i32::MAX));
        let counts = self
            .fetch_ranked_completed_continuously(&guard.fetch_ids, source, failure)
            .await?;
        result.completed += counts.0;
        result.retryable += counts.1;
        if result.retryable > 0 {
            mark_hourly_ingest_failed(
                &self.database,
                date,
                hour,
                queue_id,
                &format!("{} unresolved ranked match debt ID(s)", result.retryable),
                Some(i32::try_from(ids.len()).unwrap_or(i32::MAX)),
                Some(i32::try_from(result.completed + result.skipped).unwrap_or(i32::MAX)),
            )
            .await?;
        } else {
            mark_hourly_ingest_staged(
                &self.database,
                date,
                hour,
                queue_id,
                i32::try_from(ids.len()).unwrap_or(i32::MAX),
                i32::try_from(result.completed + result.skipped).unwrap_or(i32::MAX),
            )
            .await?;
        }
        Ok(result)
    }

    /// Transitional compatibility for existing operator call sites. It
    /// dispatches to the exact TS-equivalent path; it never turns a presence
    /// queue into completed-match acquisition.
    pub async fn discover_hour(
        &self,
        queue_id: i32,
        date: &str,
        hour: i32,
        source: &str,
        debt_only: bool,
    ) -> Result<DiscoveryResult, PipelineError> {
        if queue_id == 486 {
            self.discover_ranked_hour(date, hour, source, debt_only)
                .await
        } else {
            self.discover_presence_hour(queue_id, date, hour, source)
                .await
        }
    }

    /// The ranked continuous loop mirrors completed-match-batching.ts: each
    /// returned outcome becomes durable before another vendor window opens.
    async fn fetch_ranked_completed_continuously(
        &self,
        match_ids: &[i64],
        _source: &str,
        failure: &mut RankedFailureContext,
    ) -> Result<(usize, usize), PipelineError> {
        let mut pending = match_ids
            .iter()
            .copied()
            .filter(|id| *id > 0)
            .collect::<VecDeque<_>>();
        let mut emitted = BTreeSet::new();
        let mut completed = 0;
        let mut retryable = 0;
        let mut forced_windows = VecDeque::<Vec<i64>>::new();
        while !pending.is_empty() {
            let batch = forced_windows
                .pop_front()
                .unwrap_or_else(|| pending.iter().take(10).copied().collect::<Vec<_>>());
            let response = match self
                .relay
                .call_value(
                    "getMatchDetailsBatch",
                    vec![json!(
                        batch
                            .iter()
                            .map(|match_id| json!({"matchId":match_id,"queueId":486}))
                            .collect::<Vec<_>>()
                    )],
                    "ranked_recovery",
                )
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    if let Some((left, right)) =
                        recoverable_ranked_bisection(&batch, &error.to_string())
                    {
                        forced_windows.push_front(right);
                        forced_windows.push_front(left);
                        continue;
                    }
                    self.record_ranked_detail_outage(&error).await?;
                    return Err(error.into());
                }
            };
            let returned = response
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![response]);
            let validated = validate_requested_outcomes(&batch, returned)?;
            let mut by_id = validated.into_iter().collect::<HashMap<_, _>>();
            let returned_ids = by_id.keys().copied().collect::<HashSet<_>>();
            for match_id in &batch {
                let Some(outcome) = by_id.remove(match_id) else {
                    continue;
                };
                let match_id = *match_id;
                if !emitted.insert(match_id) {
                    return Err(PipelineError::Facts(format!(
                        "canonical relay returned duplicate outcome for match {match_id}"
                    )));
                }
                if self
                    .checkpoint_ranked_outcome_with_outage(outcome, failure)
                    .await?
                {
                    completed += 1;
                } else {
                    retryable += 1;
                }
            }
            for id in batch.iter().filter(|id| returned_ids.contains(id)) {
                if let Some(position) = pending.iter().position(|candidate| candidate == id) {
                    pending.remove(position);
                }
            }
            let Some(blocker) = batch.iter().find(|id| !returned_ids.contains(id)).copied() else {
                continue;
            };
            let singleton = match self
                .relay
                .call_value(
                    "getMatchDetailsBatch",
                    vec![json!([{"matchId":blocker,"queueId":486}])],
                    "ranked_recovery",
                )
                .await
            {
                Ok(singleton) => singleton,
                Err(error) => {
                    self.record_ranked_detail_outage(&error).await?;
                    return Err(error.into());
                }
            };
            let singleton_outcomes = singleton
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![singleton]);
            let singleton_outcomes = validate_requested_outcomes(&[blocker], singleton_outcomes)?;
            let mut resolved = false;
            for (match_id, outcome) in singleton_outcomes {
                if !emitted.insert(match_id) {
                    return Err(PipelineError::Facts(format!(
                        "canonical relay returned duplicate outcome for match {match_id}"
                    )));
                }
                resolved = true;
                if self
                    .checkpoint_ranked_outcome_with_outage(outcome, failure)
                    .await?
                {
                    completed += 1;
                } else {
                    retryable += 1;
                }
            }
            if !resolved {
                mark_match_debt_retryable(
                    &self.database,
                    blocker,
                    "canonical singleton returned no outcome",
                    None,
                )
                .await?;
                retryable += 1;
            }
            if let Some(position) = pending.iter().position(|candidate| *candidate == blocker) {
                pending.remove(position);
            }
        }
        Ok((completed, retryable))
    }

    async fn record_ranked_detail_outage(
        &self,
        error: &WorkerRelayError,
    ) -> Result<(), PipelineError> {
        let Some(classification) = classify_hirez_service_outage_message(error.to_string()) else {
            return Ok(());
        };
        if classification.service_key != MATCH_DETAIL_SERVICE_OUTAGE_KEY {
            return Ok(());
        }
        record_hirez_service_outage(
            &self.database,
            MATCH_DETAIL_SERVICE_OUTAGE_KEY,
            "Hi-Rez match detail service outage: Server_Regions temp-table failure",
            None,
        )
        .await?;
        Ok(())
    }

    async fn checkpoint_ranked_outcome_with_outage(
        &self,
        outcome: Value,
        failure: &mut RankedFailureContext,
    ) -> Result<bool, PipelineError> {
        if authoritative_ranked_outcome(&outcome) {
            mark_hirez_service_recovered(
                &self.database,
                MATCH_DETAIL_SERVICE_OUTAGE_KEY,
                Some("canonical completed-match batch returned authoritative rows"),
            )
            .await?;
        }
        self.checkpoint_ranked_outcome(outcome, failure).await
    }

    /// Ranked discovery owns only durable buffer staging. Fact normalization
    /// and projections remain under the common raw-buffer processor exactly as
    /// in the TS pipeline.
    async fn checkpoint_ranked_outcome(
        &self,
        outcome: Value,
        failure: &mut RankedFailureContext,
    ) -> Result<bool, PipelineError> {
        let match_id = extract_match_id(&outcome).unwrap_or_default();
        let canonical = match CanonicalMatchPayload::from_relay_value(outcome.clone()) {
            Ok(payload) => payload,
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
                return Ok(false);
            }
        };
        let guard = filter_already_handled_match_ids(
            &self.database,
            &[canonical.match_id],
            canonical.queue_id,
            true,
            true,
        )
        .await?;
        if should_dump_ranked_checkpoint(&guard.fetch_ids, canonical.match_id) {
            let raw_payload = build_ranked_raw_payload(&outcome, &canonical)?;
            self.relay
                .call_value(
                    "dumpRawPayloads",
                    vec![Value::Array(vec![raw_payload])],
                    "backend_unattributed",
                )
                .await?;
        }
        // TS records guard-resolved/dumped IDs for outer failure accounting
        // before the debt-state write, because the raw payload is already
        // durable even if that following state update fails.
        failure.checkpointed_ids.insert(canonical.match_id);
        mark_match_debt_staged_or_complete(
            &self.database,
            &[canonical.match_id],
            "checkpointed canonical ranked payload to raw_ingest_buffer",
        )
        .await?;
        Ok(true)
    }

    /// The separate non-ranked acquisition pass claims only ledger rows
    /// created by presence discovery. It never calls getmatchidsbyqueue.
    pub async fn run_nonranked_acquisition(
        &self,
        limit: usize,
        _lookback_hours: i32,
    ) -> Result<usize, PipelineError> {
        if limit == 0 {
            return Ok(0);
        }
        self.terminalize_interrupted_nonranked_claims().await?;
        let started = Instant::now();
        let max_run = Duration::from_millis(
            env_u64("NONRANKED_ACQUISITION_MAX_RUN_MS", 50_000).clamp(30_000, 1_200_000),
        );
        let page_size = env_usize("NONRANKED_ACQUISITION_CLAIM_LIMIT", 500).clamp(10, 1_000);
        let concurrency = env_usize("NONRANKED_ACQUISITION_FETCH_CONCURRENCY", 8).clamp(1, 8);
        let active_grace =
            i32::try_from(env_u64("NONRANKED_ACTIVE_MATCH_GRACE_MINUTES", 30).clamp(10, 360))
                .unwrap_or(30);
        let mut claimed = 0;
        let mut completed = 0;
        while claimed < limit && started.elapsed() < max_run {
            let budget = api_headroom_snapshot(&self.database, self.reserve_per_key).await?;
            if !budget.has_usable_keys {
                break;
            }
            let reserve = ranked_priority_reserve_snapshot(&self.database).await?;
            let remaining = limit - claimed;
            let mut claim_limit = page_size.min(remaining);
            if budget.total_keys > 0 {
                claim_limit = claim_limit.min(
                    usize::try_from(calculate_background_match_allowance(
                        budget.total_usable_before_reserve,
                        reserve.reserved_calls,
                        2,
                    ))
                    .unwrap_or(usize::MAX),
                );
            }
            if claim_limit == 0 {
                break;
            }
            let claim_limit_i64 = i64::try_from(claim_limit).unwrap_or(i64::MAX);
            let rows = self
                .database
                .query_json(
                    CLAIM_INCOMPLETE_NONRANKED_SQL,
                    &[&claim_limit_i64, &active_grace],
                )
                .await?;
            let requests = rows
                .into_iter()
                .filter_map(|row| {
                    Some(NonrankedAcquisitionClaim {
                        match_id: row.get("match_id")?.as_i64().filter(|id| *id > 0)?,
                        queue_id: row
                            .get("queue_id")?
                            .as_i64()
                            .and_then(|id| i32::try_from(id).ok())?,
                        source_date: row.get("source_date")?.as_str()?.to_owned(),
                        source_hour: row
                            .get("source_hour")?
                            .as_i64()
                            .and_then(|hour| i32::try_from(hour).ok())?,
                        region: row.get("region").and_then(Value::as_str).map(str::to_owned),
                        discovered_entry_datetime: row
                            .get("discovered_entry_datetime")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    })
                })
                .collect::<Vec<_>>();
            if requests.is_empty() {
                break;
            }
            claimed += requests.len();
            let lanes = build_continuous_fetch_lanes(&requests, concurrency);
            let mut joins = tokio::task::JoinSet::new();
            for lane in lanes {
                let pipeline = self.clone();
                joins.spawn(async move {
                    let result = pipeline.fetch_nonranked_completed_continuously(&lane).await;
                    (lane, result)
                });
            }
            while let Some(joined) = joins.join_next().await {
                match joined {
                    Ok((_lane, Ok(count))) => completed += count,
                    Ok((lane, Err(error))) => {
                        self.terminalize_nonranked_claims(&lane, &error.to_string())
                            .await?;
                        tracing::warn!(%error, dropped=lane.len(), "non-ranked acquisition lane failed; unpersisted claims terminalized");
                    }
                    Err(error) => {
                        joins.abort_all();
                        self.terminalize_nonranked_claims(
                            &requests,
                            &format!("non-ranked acquisition lane join failed: {error}"),
                        )
                        .await?;
                        return Err(PipelineError::Facts(format!(
                            "non-ranked acquisition lane join failed: {error}"
                        )));
                    }
                }
            }
            if requests.len() < claim_limit {
                break;
            }
        }
        Ok(completed)
    }

    /// Non-ranked acquisition is one-pass terminal work. A worker/persistence
    /// failure is recorded as dropped, never re-claimed for another vendor
    /// attempt on the following gap scan.
    async fn mark_nonranked_terminal(
        &self,
        match_id: i64,
        status: &str,
        error: &str,
    ) -> Result<(), DatabaseError> {
        self.database.query_json(
            "UPDATE nonranked_match_acquisition SET status=$2,quality='unavailable',lease_until=NULL,\
             terminal_reason=COALESCE(terminal_reason,'single_pass_worker_failure'),error_message=$3,\
             completed_at=COALESCE(completed_at,now()),updated_at=now() WHERE match_id=$1 AND status='fetching'",
            &[&match_id, &status, &error],
        ).await?;
        Ok(())
    }

    async fn terminalize_nonranked_claims(
        &self,
        claims: &[NonrankedAcquisitionClaim],
        error: &str,
    ) -> Result<(), DatabaseError> {
        let ids = claims
            .iter()
            .map(|claim| claim.match_id)
            .filter(|id| *id > 0)
            .collect::<Vec<_>>();
        if ids.is_empty() {
            return Ok(());
        }
        // A whole acquisition lane failing is almost always a TRANSIENT infra
        // event (relay lease lost, DB unavailable, quota exhausted) — not evidence
        // of a bad payload. Permanently dropping the lane starved the roster/player
        // tables and under-reported presence (the 38k `single_pass_worker_failure`
        // incident). Reset lane-failed claims back to `discovered` so the next pass
        // re-claims and persists them, bounded by an attempt fuse that parks only
        // matches which repeatedly fail rather than churning forever.
        let attempt_cap = std::env::var("NONRANKED_ACQUISITION_INTERRUPT_MAX_ATTEMPTS")
            .ok()
            .and_then(|raw| raw.parse::<i32>().ok())
            .unwrap_or(6)
            .max(1);
        self.database.query_json(
            "UPDATE nonranked_match_acquisition SET status='discovered',quality='unknown',\
             lease_until=NULL,terminal_reason=NULL,error_message=$2,updated_at=now()\
             WHERE match_id=ANY($1::bigint[]) AND status='fetching'\
               AND (detail_attempts IS NULL OR detail_attempts < $3::int)",
            &[&ids, &error, &attempt_cap],
        )
        .await?;
        // Failed too many times: park as dropped to avoid infinite churn.
        self.database.query_json(
            "UPDATE nonranked_match_acquisition SET status='dropped',quality='unavailable',\
             lease_until=NULL,terminal_reason='worker_failure_attempt_fuse_exceeded',\
             error_message=$2,completed_at=COALESCE(completed_at,now()),updated_at=now()\
             WHERE match_id=ANY($1::bigint[]) AND status='fetching'\
               AND detail_attempts >= $3::int",
            &[&ids, &error, &attempt_cap],
        )
        .await?;
        Ok(())
    }

    async fn terminalize_interrupted_nonranked_claims(&self) -> Result<(), DatabaseError> {
        // An interrupted claim is NOT evidence of a bad payload: it usually means
        // the worker exited (restart/crash) before persisting an in-flight batch.
        // Permanently dropping it starved the roster/player tables and under-reported
        // presence. Reset expired in-flight claims back to `discovered` so the next
        // acquisition pass re-claims and persists them. A bounded attempt fuse still
        // parks genuinely-unavailable matches instead of churning forever.
        let attempt_cap = std::env::var("NONRANKED_ACQUISITION_INTERRUPT_MAX_ATTEMPTS")
            .ok()
            .and_then(|raw| raw.parse::<i32>().ok())
            .unwrap_or(6)
            .max(1);
        self.database.query_json(
            "UPDATE nonranked_match_acquisition SET status='discovered',quality='unknown',\
             lease_until=NULL,terminal_reason=NULL,error_message=NULL,\
             updated_at=now()\
             WHERE status IN('fetching','service_deferred')\
               AND (lease_until IS NULL OR lease_until<=now())\
               AND (detail_attempts IS NULL OR detail_attempts < $1::int)",
            &[&attempt_cap],
        )
        .await?;
        // Anything still stuck in-flight past the fuse is permanently parked.
        self.database.query_json(
            "UPDATE nonranked_match_acquisition SET status='dropped',quality='unavailable',\
             lease_until=NULL,terminal_reason='worker_interrupted_attempt_fuse_exceeded',\
             error_message='Repeatedly interrupted before persistence; parked to avoid churn',\
             completed_at=COALESCE(completed_at,now()),updated_at=now()\
             WHERE status IN('fetching','service_deferred')\
               AND (lease_until IS NULL OR lease_until<=now())\
               AND detail_attempts >= $1::int",
            &[&attempt_cap],
        )
        .await?;
        Ok(())
    }

    /// Matches the TS nonranked-acquisition-batching checkpoint contract: an
    /// outcome is persisted before the next ordered vendor window is opened.
    async fn fetch_nonranked_completed_continuously(
        &self,
        requests: &[NonrankedAcquisitionClaim],
    ) -> Result<usize, PipelineError> {
        let mut pending = requests
            .iter()
            .filter(|claim| claim.match_id > 0 && claim.queue_id > 0)
            .cloned()
            .collect::<VecDeque<_>>();
        let mut emitted = BTreeSet::new();
        let mut completed = 0;
        let mut forced_windows = VecDeque::<Vec<NonrankedAcquisitionClaim>>::new();
        while !pending.is_empty() {
            let batch = forced_windows
                .pop_front()
                .unwrap_or_else(|| pending.iter().take(10).cloned().collect::<Vec<_>>());
            let batch_ids = batch.iter().map(|claim| claim.match_id).collect::<Vec<_>>();
            let response = match self
                .relay
                .call_value(
                    "getMatchDetailsBatch",
                    vec![json!(
                        batch
                            .iter()
                            .map(|claim| json!({"matchId":claim.match_id,"queueId":claim.queue_id}))
                            .collect::<Vec<_>>()
                    )],
                    "presence_acquisition",
                )
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    if batch.len() > 1 && is_recoverable_completed_batch_error(&error.to_string()) {
                        let midpoint = batch.len().div_ceil(2);
                        forced_windows.push_front(batch[midpoint..].to_vec());
                        forced_windows.push_front(batch[..midpoint].to_vec());
                        continue;
                    }
                    for claim in pending.drain(..) {
                        self.mark_nonranked_terminal(claim.match_id, "dropped", &error.to_string())
                            .await?;
                    }
                    return Ok(completed);
                }
            };
            let returned = response
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![response]);
            let validated = match validate_requested_outcomes(&batch_ids, returned) {
                Ok(validated) => validated,
                Err(error) => {
                    for claim in pending.drain(..) {
                        self.mark_nonranked_terminal(claim.match_id, "dropped", &error.to_string())
                            .await?;
                    }
                    return Ok(completed);
                }
            };
            let mut by_id = validated.into_iter().collect::<HashMap<_, _>>();
            let returned_ids = by_id.keys().copied().collect::<HashSet<_>>();
            for claim in &batch {
                let Some(outcome) = by_id.remove(&claim.match_id) else {
                    continue;
                };
                let match_id = claim.match_id;
                if !emitted.insert(match_id) {
                    return Err(PipelineError::Facts(format!(
                        "canonical relay returned duplicate outcome for match {match_id}"
                    )));
                }
                match self.persist_nonranked_outcome(claim, outcome).await {
                    Ok(true) => completed += 1,
                    Ok(false) => {}
                    Err(error) => {
                        for pending_claim in pending.drain(..) {
                            self.mark_nonranked_terminal(
                                pending_claim.match_id,
                                "dropped",
                                &error.to_string(),
                            )
                            .await?;
                        }
                        return Ok(completed);
                    }
                }
            }
            for id in batch_ids.iter().filter(|id| returned_ids.contains(id)) {
                if let Some(position) = pending
                    .iter()
                    .position(|candidate| candidate.match_id == *id)
                {
                    pending.remove(position);
                }
            }
            let Some(blocker) = batch
                .iter()
                .find(|claim| !returned_ids.contains(&claim.match_id))
                .cloned()
            else {
                continue;
            };
            let singleton = match self
                .relay
                .call_value(
                    "getMatchDetailsBatch",
                    vec![json!([{"matchId":blocker.match_id,"queueId":blocker.queue_id}])],
                    "presence_acquisition",
                )
                .await
            {
                Ok(value) => value,
                Err(error) => {
                    for pending_claim in pending.drain(..) {
                        self.mark_nonranked_terminal(
                            pending_claim.match_id,
                            "dropped",
                            &error.to_string(),
                        )
                        .await?;
                    }
                    return Ok(completed);
                }
            };
            let mut resolved = false;
            let singleton = singleton
                .as_array()
                .cloned()
                .unwrap_or_else(|| vec![singleton]);
            let singleton = match validate_requested_outcomes(&[blocker.match_id], singleton) {
                Ok(singleton) => singleton,
                Err(error) => {
                    for pending_claim in pending.drain(..) {
                        self.mark_nonranked_terminal(
                            pending_claim.match_id,
                            "dropped",
                            &error.to_string(),
                        )
                        .await?;
                    }
                    return Ok(completed);
                }
            };
            for (match_id, outcome) in singleton {
                if !emitted.insert(match_id) {
                    return Err(PipelineError::Facts(format!(
                        "canonical relay returned duplicate outcome for match {match_id}"
                    )));
                }
                resolved = true;
                match self.persist_nonranked_outcome(&blocker, outcome).await {
                    Ok(true) => completed += 1,
                    Ok(false) => {}
                    Err(error) => {
                        for pending_claim in pending.drain(..) {
                            self.mark_nonranked_terminal(
                                pending_claim.match_id,
                                "dropped",
                                &error.to_string(),
                            )
                            .await?;
                        }
                        return Ok(completed);
                    }
                }
            }
            if !resolved {
                for pending_claim in pending.drain(..) {
                    self.mark_nonranked_terminal(
                        pending_claim.match_id,
                        "dropped",
                        "canonical singleton returned no outcome",
                    )
                    .await?;
                }
                return Ok(completed);
            }
            if let Some(position) = pending
                .iter()
                .position(|candidate| candidate.match_id == blocker.match_id)
            {
                pending.remove(position);
            }
        }
        Ok(completed)
    }

    async fn persist_nonranked_outcome(
        &self,
        claim: &NonrankedAcquisitionClaim,
        outcome: Value,
    ) -> Result<bool, PipelineError> {
        let match_id = extract_match_id(&outcome).unwrap_or_default();
        if match_id != claim.match_id {
            return Err(PipelineError::Facts(format!(
                "non-ranked outcome {} does not match claim {}",
                match_id, claim.match_id
            )));
        }
        match self.nonranked.persist(claim, outcome).await {
            Ok(()) => Ok(true),
            Err(error) => {
                self.mark_nonranked_terminal(claim.match_id, "dropped", &error.to_string())
                    .await?;
                Err(PipelineError::Facts(format!(
                    "non-ranked persistence failed for match {}: {error}",
                    claim.match_id
                )))
            }
        }
    }

    /// Compatibility name retained for the gap checker while it is being
    /// replaced by the TS-equivalent hourly acquisition call.
    pub async fn replay_incomplete_nonranked(
        &self,
        limit: usize,
        lookback_hours: i32,
    ) -> Result<usize, PipelineError> {
        self.run_nonranked_acquisition(limit, lookback_hours).await
    }
}

fn parse_observations(value: Value) -> Vec<MatchIdObservation> {
    let rows = value.as_array().cloned().unwrap_or_default();
    let mut observations = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        let match_id = extract_match_id(&row).unwrap_or_default();
        if match_id <= 0 || !seen.insert(match_id) {
            continue;
        }
        observations.push(MatchIdObservation {
            match_id,
            entry_datetime: text(&row, &["Entry_Datetime", "entry_datetime"]),
            region: text(&row, &["Region", "region"]),
            active_flag: boolean(&row, &["Active_Flag", "active_flag"]),
        });
    }
    observations
}

fn build_ranked_raw_payload(
    outcome: &Value,
    canonical: &CanonicalMatchPayload,
) -> Result<Value, PipelineError> {
    let raw_match = outcome.get("match").unwrap_or(outcome);
    let players = raw_match
        .get("players")
        .and_then(Value::as_array)
        .ok_or_else(|| PipelineError::Facts("ranked relay match has no player array".to_owned()))?;
    let score_observations = raw_match
        .get("direct_score_observations")
        .and_then(Value::as_array);
    let bans = (1..=8)
        .map(|slot| {
            let keys = [
                format!("ban_id_{slot}"),
                format!("BanId{slot}"),
                format!("Ban_{slot}"),
            ];
            let value = std::iter::once(raw_match)
                .chain(players.iter())
                .find_map(|source| {
                    keys.iter()
                        .find_map(|key| source.get(key).and_then(positive_i64))
                })
                .unwrap_or_default();
            (format!("ban_id_{slot}"), json!(value))
        })
        .collect::<Vec<_>>();
    let mut rows = Vec::with_capacity(players.len());
    for (index, player) in players.iter().enumerate() {
        let mut row = player.as_object().cloned().unwrap_or_default();
        row.insert("Match".to_owned(), json!(canonical.match_id));
        row.insert("Entry_Datetime".to_owned(), json!(canonical.entry_datetime));
        row.insert("Map_Game".to_owned(), json!(canonical.map));
        row.insert("match_queue_id".to_owned(), json!(canonical.queue_id));
        row.insert(
            "Match_Duration".to_owned(),
            json!(canonical.duration_seconds),
        );
        if let Some(minutes) = raw_match.get("minutes") {
            row.insert("Minutes".to_owned(), minutes.clone());
        }
        row.insert("Region".to_owned(), json!(canonical.region));
        let score = score_observations.and_then(|values| values.get(index));
        insert_optional_value(
            &mut row,
            "Team1Score",
            score
                .and_then(|value| value.get("team1"))
                .cloned()
                .or_else(|| canonical.team1_score.map(|value| json!(value))),
        );
        insert_optional_value(
            &mut row,
            "Team2Score",
            score
                .and_then(|value| value.get("team2"))
                .cloned()
                .or_else(|| canonical.team2_score.map(|value| json!(value))),
        );
        insert_optional_value(
            &mut row,
            "Winning_TaskForce",
            score
                .and_then(|value| value.get("winner"))
                .cloned()
                .or_else(|| canonical.winning_task_force.map(|value| json!(value))),
        );
        row.insert(
            "hasReplay".to_owned(),
            json!(if canonical.has_replay.unwrap_or(false) {
                "y"
            } else {
                "n"
            }),
        );
        for key in ["recovery_source", "recovery_api_calls"] {
            if let Some(value) = raw_match.get(key) {
                row.insert(key.to_owned(), value.clone());
            }
        }
        row.insert(
            "recovery_attempted".to_owned(),
            json!(raw_match.get("recovery_attempted").and_then(Value::as_bool) == Some(true)),
        );
        row.insert(
            "recovery_terminal".to_owned(),
            json!(raw_match.get("recovery_terminal").and_then(Value::as_bool) == Some(true)),
        );
        row.insert(
            "limited".to_owned(),
            json!(raw_match.get("limited").and_then(Value::as_bool) == Some(true)),
        );
        for (key, value) in &bans {
            row.insert(key.clone(), value.clone());
        }
        rows.push(sanitize_json_strings(Value::Object(row)));
    }
    Ok(json!({
        "endpoint":"getmatchdetailsbatch",
        "entity_type":"match",
        "entity_id":canonical.match_id,
        "raw_data":rows,
        "source":"batch"
    }))
}

fn should_dump_ranked_checkpoint(fetch_ids_after_fetch: &[i64], match_id: i64) -> bool {
    fetch_ids_after_fetch.contains(&match_id)
}

fn insert_optional_value(target: &mut Map<String, Value>, key: &str, value: Option<Value>) {
    if let Some(value) = value {
        target.insert(key.to_owned(), value);
    }
}

fn positive_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse().ok())
        .filter(|value| *value > 0)
}

fn sanitize_json_strings(value: Value) -> Value {
    match value {
        Value::String(value) => Value::String(value.replace('\0', "").replace("\\u0000", "")),
        Value::Array(values) => {
            Value::Array(values.into_iter().map(sanitize_json_strings).collect())
        }
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, sanitize_json_strings(value)))
                .collect(),
        ),
        value => value,
    }
}

fn stable_merge_ids(primary: &[i64], secondary: &[i64]) -> Vec<i64> {
    let mut seen = HashSet::new();
    primary
        .iter()
        .chain(secondary)
        .copied()
        .filter(|id| *id > 0 && seen.insert(*id))
        .collect()
}

async fn mark_ranked_discovery_failure(
    database: &Database,
    date: &str,
    hour: i32,
    error: &PipelineError,
    failure: &RankedFailureContext,
) -> Result<(), DatabaseError> {
    mark_hourly_ingest_failed(
        database,
        date,
        hour,
        486,
        &pipeline_state_error_message(error),
        failure.claimed_raw_match_count,
        failure.checkpointed_count(),
    )
    .await
}

fn pipeline_state_error_message(error: &PipelineError) -> Cow<'_, str> {
    match error {
        PipelineError::Relay(WorkerRelayError::Operation { message, .. }) => Cow::Borrowed(message),
        _ => Cow::Owned(error.to_string()),
    }
}

fn build_continuous_fetch_lanes(
    requests: &[NonrankedAcquisitionClaim],
    concurrency: usize,
) -> Vec<Vec<NonrankedAcquisitionClaim>> {
    let mut seen = HashSet::new();
    let requests = requests
        .iter()
        .filter(|claim| claim.match_id > 0 && claim.queue_id > 0 && seen.insert(claim.match_id))
        .cloned()
        .collect::<Vec<_>>();
    if requests.is_empty() {
        return Vec::new();
    }
    let lane_count = concurrency.max(1).min(requests.len().div_ceil(10));
    let mut lanes = vec![Vec::new(); lane_count];
    for (window_index, window) in requests.chunks(10).enumerate() {
        lanes[window_index % lane_count].extend_from_slice(window);
    }
    lanes.into_iter().filter(|lane| !lane.is_empty()).collect()
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

fn is_recoverable_completed_batch_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    [
        "hirez_unknown_return",
        "int16",
        "skin_id",
        "skin id",
        "skinid",
        "too large",
        "too small",
    ]
    .iter()
    .any(|needle| error.contains(needle))
}

fn recoverable_ranked_bisection(batch: &[i64], error: &str) -> Option<(Vec<i64>, Vec<i64>)> {
    if batch.len() <= 1 || !is_recoverable_completed_batch_error(error) {
        return None;
    }
    let midpoint = batch.len().div_ceil(2);
    Some((batch[..midpoint].to_vec(), batch[midpoint..].to_vec()))
}

fn authoritative_ranked_outcome(outcome: &Value) -> bool {
    matches!(
        outcome.get("status").and_then(Value::as_str),
        Some("complete_direct" | "complete_recovered")
    ) && outcome.get("match").is_some_and(Value::is_object)
}

fn validate_requested_outcomes(
    requested: &[i64],
    outcomes: Vec<Value>,
) -> Result<Vec<(i64, Value)>, PipelineError> {
    let requested = requested.iter().copied().collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut validated = Vec::with_capacity(outcomes.len());
    for outcome in outcomes {
        let match_id = extract_match_id(&outcome).ok_or_else(|| {
            PipelineError::Facts(
                "canonical relay returned an outcome without a match ID".to_owned(),
            )
        })?;
        if !requested.contains(&match_id) {
            return Err(PipelineError::Facts(format!(
                "canonical relay returned unrequested match {match_id}"
            )));
        }
        if !seen.insert(match_id) {
            return Err(PipelineError::Facts(format!(
                "canonical relay returned duplicate outcome for match {match_id}"
            )));
        }
        validated.push((match_id, outcome));
    }
    Ok(validated)
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

#[cfg(test)]
mod tests {
    use super::*;
    use paladinscat_core::config::BackendConfig;

    #[test]
    fn relay_operation_state_error_matches_typescript_raw_message() {
        let error = PipelineError::Relay(WorkerRelayError::Operation {
            operation: "getMatchIdsByQueueDetails".to_owned(),
            message: "fixture outage".to_owned(),
        });
        assert_eq!(pipeline_state_error_message(&error), "fixture outage");
        assert_eq!(
            error.to_string(),
            "HirezRelay getMatchIdsByQueueDetails failed: fixture outage"
        );
    }

    #[test]
    fn incomplete_nonranked_replay_claim_is_oldest_first_and_bounded() {
        assert!(
            CLAIM_INCOMPLETE_NONRANKED_SQL
                .contains("status = 'discovered' OR (status = 'waiting_for_completion'")
        );
        assert!(
            !CLAIM_INCOMPLETE_NONRANKED_SQL
                .contains("status IN ('discovered', 'waiting_for_completion', 'fetching')")
        );
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("interval '30 minutes'"));
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("(now() AT TIME ZONE 'UTC')"));
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("$2::int * interval '1 minute'"));
        assert!(!CLAIM_INCOMPLETE_NONRANKED_SQL.contains(">= (now() AT TIME ZONE 'UTC') - ($2"));
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("THEN 0 ELSE 1 END"));
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("END DESC"));
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("END ASC"));
        assert!(
            CLAIM_INCOMPLETE_NONRANKED_SQL.contains("lease_until = now() + interval '30 minutes'")
        );
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("LIMIT $1"));
        assert!(CLAIM_INCOMPLETE_NONRANKED_SQL.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn ranked_discovery_then_debt_merge_is_stable() {
        assert_eq!(
            stable_merge_ids(&[30, 10, 30, 20], &[20, 40, 10, 50]),
            vec![30, 10, 20, 40, 50]
        );
    }

    #[test]
    fn match_count_observations_preserve_provider_order() {
        let observations = parse_observations(json!([
            {"Match":30,"Region":"NA"},
            {"Match":10,"Region":"EU"},
            {"Match":30,"Region":"BR"},
            {"Match":20,"Region":"SEA"}
        ]));
        assert_eq!(
            observations
                .into_iter()
                .map(|row| row.match_id)
                .collect::<Vec<_>>(),
            vec![30, 10, 20]
        );
    }

    #[test]
    fn canonical_outcomes_reject_unrequested_duplicate_and_missing_ids() {
        assert!(validate_requested_outcomes(&[1], vec![json!({"matchId":2})]).is_err());
        assert!(
            validate_requested_outcomes(&[1], vec![json!({"matchId":1}), json!({"matchId":1})])
                .is_err()
        );
        assert!(validate_requested_outcomes(&[1], vec![json!({"status":"dropped"})]).is_err());
        let valid =
            validate_requested_outcomes(&[1, 2], vec![json!({"matchId":2}), json!({"matchId":1})])
                .expect("valid outcomes");
        assert_eq!(
            valid.into_iter().map(|(id, _)| id).collect::<Vec<_>>(),
            vec![2, 1]
        );
    }

    #[test]
    fn nonranked_lanes_distribute_ordered_windows_like_typescript() {
        let requests = (1..=25)
            .map(|id| NonrankedAcquisitionClaim {
                match_id: id,
                queue_id: 424,
                source_date: "2026-08-02".to_owned(),
                source_hour: 0,
                region: None,
                discovered_entry_datetime: None,
            })
            .collect::<Vec<_>>();
        let lanes = build_continuous_fetch_lanes(&requests, 2);
        assert_eq!(
            lanes[0]
                .iter()
                .map(|claim| claim.match_id)
                .collect::<Vec<_>>(),
            [(1..=10).collect::<Vec<_>>(), (21..=25).collect::<Vec<_>>()].concat()
        );
        assert_eq!(
            lanes[1]
                .iter()
                .map(|claim| claim.match_id)
                .collect::<Vec<_>>(),
            (11..=20).collect::<Vec<_>>()
        );
    }

    #[test]
    fn nonranked_bisection_is_limited_to_typescript_recoverable_errors() {
        assert!(is_recoverable_completed_batch_error(
            "Int16 skin_id too large"
        ));
        assert!(is_recoverable_completed_batch_error("HIREZ_UNKNOWN_RETURN"));
        assert!(!is_recoverable_completed_batch_error("quota exhausted"));
        assert!(!is_recoverable_completed_batch_error("transport timeout"));
    }

    #[test]
    fn ranked_recoverable_bisection_preserves_typescript_left_then_right_sequence() {
        let (left, right) = recoverable_ranked_bisection(
            &(1..=10).collect::<Vec<_>>(),
            "HIREZ_UNKNOWN_RETURN Int16 skin_id",
        )
        .expect("recoverable ten-ID window");
        assert_eq!(left, vec![1, 2, 3, 4, 5]);
        assert_eq!(right, vec![6, 7, 8, 9, 10]);
        let (left_left, left_right) =
            recoverable_ranked_bisection(&left, "too large").expect("recursive left split");
        assert_eq!(left_left, vec![1, 2, 3]);
        assert_eq!(left_right, vec![4, 5]);
        assert!(recoverable_ranked_bisection(&[1], "Int16").is_none());
        assert!(recoverable_ranked_bisection(&[1, 2], "quota exhausted").is_none());
    }

    #[test]
    fn ranked_outage_latch_transitions_match_authoritative_ts_states() {
        assert!(authoritative_ranked_outcome(&json!({
            "status":"complete_direct","match":{}
        })));
        assert!(authoritative_ranked_outcome(&json!({
            "status":"complete_recovered","match":{}
        })));
        assert!(!authoritative_ranked_outcome(&json!({
            "status":"limited","match":{}
        })));
        assert!(!authoritative_ranked_outcome(&json!({
            "status":"complete_direct"
        })));
        let classified = classify_hirez_service_outage_message(
            "Invalid object name 'Server_Regions' in ##sql_paladins_api",
        )
        .expect("detail outage");
        assert_eq!(classified.service_key, MATCH_DETAIL_SERVICE_OUTAGE_KEY);

        let source = include_str!("pipeline.rs");
        let clear = source
            .split("async fn checkpoint_ranked_outcome_with_outage")
            .nth(1)
            .expect("outage recovery handler")
            .split("/// Ranked discovery owns only durable buffer staging")
            .next()
            .expect("handler body");
        assert!(
            clear.find("mark_hirez_service_recovered").unwrap()
                < clear
                    .find("checkpoint_ranked_outcome(outcome, failure)")
                    .unwrap()
        );
        let outage = source
            .split("async fn record_ranked_detail_outage")
            .nth(1)
            .expect("outage recorder")
            .split("async fn checkpoint_ranked_outcome_with_outage")
            .next()
            .expect("outage recorder body");
        assert!(outage.contains("record_hirez_service_outage"));
        assert!(!outage.contains("mark_match_debt_retryable"));
    }

    #[test]
    fn ranked_checkpoint_uses_raw_buffer_not_direct_fact_finalization() {
        let source = include_str!("pipeline.rs");
        let checkpoint = source
            .split("async fn checkpoint_ranked_outcome(\n")
            .nth(1)
            .expect("checkpoint function")
            .split("pub async fn run_nonranked_acquisition")
            .next()
            .expect("checkpoint body");
        assert!(checkpoint.contains("build_ranked_raw_payload"));
        assert!(checkpoint.contains("\"dumpRawPayloads\""));
        assert!(checkpoint.contains("\"backend_unattributed\""));
        assert!(
            checkpoint.find("filter_already_handled_match_ids").unwrap()
                < checkpoint.find("\"dumpRawPayloads\"").unwrap()
        );
        assert!(checkpoint.contains("mark_match_debt_staged_or_complete"));
        assert!(!checkpoint.contains("INSERT INTO raw_ingest_buffer"));
        assert!(!checkpoint.contains("facts.finalize"));
        assert!(!checkpoint.contains("project_match"));
    }

    #[test]
    fn ranked_raw_payload_matches_historical_array_contract() {
        let outcome = json!({
            "matchId":77,"status":"complete_direct",
            "match":{
                "match_id":77,"queue_id":486,"entry_datetime":"2026-08-02T01:00:00Z",
                "map":"Frog Isle","duration_seconds":600,"minutes":10,"region":"NA",
                "team1_score":4,"team2_score":2,"winning_task_force":1,"has_replay":true,
                "direct_score_observations":[{"team1":3,"team2":1,"winner":1}],
                "players":[{"player_id":9,"player_name":format!("A{}B", '\0'),"BanId1":101}]
            }
        });
        let canonical = CanonicalMatchPayload::from_relay_value(outcome.clone()).unwrap();
        let payload = build_ranked_raw_payload(&outcome, &canonical).unwrap();
        assert_eq!(payload["endpoint"], "getmatchdetailsbatch");
        assert_eq!(payload["entity_type"], "match");
        assert_eq!(payload["entity_id"], 77);
        assert_eq!(payload["source"], "batch");
        let rows = payload["raw_data"].as_array().expect("historical rows");
        assert_eq!(rows[0]["Match"], 77);
        assert_eq!(rows[0]["Team1Score"], 3);
        assert_eq!(rows[0]["ban_id_1"], 101);
        assert_eq!(rows[0]["player_name"], "AB");
    }

    #[test]
    fn ranked_post_fetch_race_guard_suppresses_completed_match_dump() {
        assert!(should_dump_ranked_checkpoint(&[77], 77));
        // A match completing while getMatchDetailsBatch is in flight is absent
        // from the second guard's fetchIds and must never be dumped again.
        assert!(!should_dump_ranked_checkpoint(&[], 77));
    }

    #[test]
    fn ranked_source_keeps_normal_due_debt_and_failed_or_staged_hour_ownership() {
        let source = include_str!("pipeline.rs");
        let ranked = source
            .split("async fn discover_ranked_hour_inner")
            .nth(1)
            .expect("ranked discovery")
            .split("pub async fn discover_hour")
            .next()
            .expect("ranked body");
        assert!(ranked.contains("due_match_debt_ids"));
        assert!(ranked.contains("stable_merge_ids(&discovered_ids, &due_debt)"));
        assert!(ranked.contains("guard.skipped.raw_buffer > 0 || guard.skipped.pull_list > 0"));
        assert!(ranked.contains("mark_hourly_ingest_failed"));
        assert!(!ranked.contains("if result.retryable == 0"));
    }

    #[test]
    fn ranked_cleanup_signal_is_awaited_before_headroom_and_is_warning_only() {
        let source = include_str!("pipeline.rs");
        let ranked = source
            .split("async fn discover_ranked_hour_inner")
            .nth(1)
            .expect("ranked discovery")
            .split("pub async fn discover_hour")
            .next()
            .expect("ranked body");
        let cleanup = ranked
            .find("\"cleanupFetchedPlayersCache\"")
            .expect("cleanup relay signal");
        let headroom = ranked
            .find("api_headroom_snapshot")
            .expect("headroom snapshot");
        assert!(cleanup < headroom);
        assert!(ranked[..headroom].contains("\"backend_unattributed\""));
        assert!(ranked[..headroom].contains("if let Err(error)"));
        assert!(ranked[..headroom].contains("tracing::warn!"));
    }

    #[test]
    fn presence_pass_has_one_preloop_budget_gate_and_no_queue_state_on_exhaustion() {
        let source = include_str!("pipeline.rs");
        let pass = source
            .split("pub async fn discover_all_presence_queues")
            .nth(1)
            .expect("presence pass")
            .split("/// Presence discovery has exactly one vendor call")
            .next()
            .expect("presence pass body");
        assert_eq!(pass.matches("api_headroom_snapshot").count(), 1);
        assert!(pass.find("api_headroom_snapshot").unwrap() < pass.find("for queue").unwrap());
        let exhausted = pass
            .split("if !budget.has_usable_keys")
            .nth(1)
            .expect("no-headroom branch")
            .split("let mut results")
            .next()
            .expect("no-headroom body");
        assert!(exhausted.contains("return Vec::new()"));
        assert!(!exhausted.contains("mark_hourly_ingest"));
        assert!(!exhausted.contains("record_hourly_ingest_quota_wait"));

        let queue = source
            .split("async fn discover_presence_hour_inner")
            .nth(1)
            .expect("presence queue")
            .split("/// Scheduler-facing spelling")
            .next()
            .expect("presence queue body");
        assert!(!queue.contains("api_headroom_snapshot"));
        assert!(!queue.contains("record_hourly_ingest_quota_wait"));
    }

    #[test]
    fn nonranked_lane_failures_terminalize_claims_before_join_error_propagates() {
        let source = include_str!("pipeline.rs");
        let run = source
            .split("pub async fn run_nonranked_acquisition")
            .nth(1)
            .expect("nonranked runner")
            .split("/// Non-ranked acquisition is one-pass terminal work")
            .next()
            .expect("runner body");
        let duplicate_or_contract = run
            .find("terminalize_nonranked_claims(&lane")
            .expect("lane cleanup");
        let dropped_task = run
            .find("terminalize_nonranked_claims(\n                            &requests")
            .expect("dropped task cleanup");
        let propagate = run
            .find("return Err(PipelineError::Facts")
            .expect("join propagation");
        assert!(duplicate_or_contract < propagate);
        assert!(dropped_task < propagate);
    }

    #[test]
    fn interrupted_claim_reset_never_nulls_quality_or_drops_permanently() {
        let source = include_str!("pipeline.rs");
        let reset = source
            .split("async fn terminalize_interrupted_nonranked_claims")
            .nth(1)
            .expect("interrupted terminalize fn")
            .split("async fn fetch_nonranked_completed_continuously")
            .next()
            .expect("fn body");
        // The reset branch must set a valid quality (column is NOT NULL) and must
        // NOT permanently drop a merely-interrupted claim, or roster recovery stalls.
        let reset_stmt = reset
            .split("Anything still stuck in-flight past the fuse is permanently parked")
            .next()
            .expect("reset branch only");
        assert!(
            reset_stmt.contains("SET status='discovered',quality='unknown'"),
            "interrupted-claim reset must set quality='unknown' (NOT NULL column), got:\n{reset_stmt}"
        );
        assert!(
            reset_stmt.contains("status IN('fetching','service_deferred')"),
            "reset targets in-flight claims"
        );
        assert!(
            !reset_stmt.contains("status='dropped'"),
            "interrupted-claim reset must NOT permanently drop; the bounded fuse branch is separate"
        );
        // The bounded fuse still parks genuinely-unavailable matches (churn guard).
        assert!(
            reset.contains("status='dropped',quality='unavailable'"),
            "fuse branch parks over-attempted claims"
        );
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with hourly ingest tables"]
    async fn forced_ranked_outage_latches_then_fails_hour_with_ts_run_counts() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "ranked-failure-state-integration").unwrap();
        let date = "2099-12-29";
        let hour = 23;
        let client = database.connection().await.unwrap();
        client
            .execute(
                "DELETE FROM hourly_ingest_state WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=486",
                &[&date, &hour],
            )
            .await
            .unwrap();
        drop(client);
        assert!(
            claim_hourly_ingest_hour(&database, date, hour, 486, "integration", false)
                .await
                .unwrap()
        );
        record_hirez_service_outage(
            &database,
            MATCH_DETAIL_SERVICE_OUTAGE_KEY,
            "Hi-Rez match detail service outage: Server_Regions temp-table failure",
            None,
        )
        .await
        .unwrap();
        let mut failure = RankedFailureContext {
            claimed_raw_match_count: Some(7),
            ..RankedFailureContext::default()
        };
        failure.checkpointed_ids.extend([7001, 7002]);
        mark_ranked_discovery_failure(
            &database,
            date,
            hour,
            &PipelineError::Facts("forced ranked relay outage".to_owned()),
            &failure,
        )
        .await
        .unwrap();
        let row = database
            .one_json(
                "SELECT status,raw_match_count,staged_match_count,fetched,fetch_succeeded,lease_until::text,next_retry_at::text,error_message FROM hourly_ingest_state WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=486",
                &[&date, &hour],
            )
            .await
            .unwrap()
            .expect("failed hour");
        assert_eq!(row["status"], "failed");
        assert_eq!(row["raw_match_count"], 7);
        assert_eq!(row["staged_match_count"], 2);
        assert_eq!(row["fetched"], true);
        assert_eq!(row["fetch_succeeded"], false);
        assert!(row["lease_until"].is_null());
        assert!(row["next_retry_at"].is_string());
        assert!(
            row["error_message"]
                .as_str()
                .unwrap()
                .contains("forced ranked relay outage")
        );
        assert!(
            crate::workers::outage::active_hirez_service_outage(
                &database,
                MATCH_DETAIL_SERVICE_OUTAGE_KEY,
            )
            .await
            .unwrap()
            .is_some()
        );
        let client = database.connection().await.unwrap();
        client
            .execute(
                "DELETE FROM hourly_ingest_state WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=486",
                &[&date, &hour],
            )
            .await
            .unwrap();
        client
            .execute(
                "DELETE FROM hirez_service_outage_state WHERE service_key=$1",
                &[&MATCH_DETAIL_SERVICE_OUTAGE_KEY],
            )
            .await
            .unwrap();
    }
}
