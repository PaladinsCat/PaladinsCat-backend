use std::{
    borrow::Cow,
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    time::Duration,
};

use futures::future::join_all;
use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde::Serialize;
use serde_json::{Value, json};

use super::{
    casual_mechanics::CasualMechanicsRepository,
    discovery_control::{
        claim_hourly_ingest_hour, claim_hourly_ingest_search, mark_hourly_ingest_complete,
        mark_hourly_ingest_empty, mark_hourly_ingest_failed, refresh_hourly_ingest_lease,
    },
    discovery_store::{MatchIdObservation, record_match_count_discovery_result},
    match_facts::{CanonicalMatchPayload, MatchFactError, MatchFactRepository},
    match_lifecycle::{MatchDiscoverySource, MatchPopulation, TERMINAL_NO_COMPLETED_MATCH_REASON},
    outage::{
        MATCH_DETAIL_SERVICE_OUTAGE_KEY, classify_hirez_service_outage_message,
        mark_hirez_service_recovered, record_hirez_service_outage,
    },
    policy::MATCH_COUNT_QUEUE_DEFINITIONS,
    ranked_projection::RankedProjectionRepository,
    relay::{WorkerRelayClient, WorkerRelayError},
    requested_match::{MatchIngestRequest, RequestedMatchIngestor, RequestedMatchStatus},
};

const MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL: &str = "SELECT requested.match_id, lifecycle.match_id IS NOT NULL AS resume_locally FROM unnest($1::BIGINT[]) WITH ORDINALITY requested(match_id,ordinality) \
     LEFT JOIN match_ingest_status lifecycle ON lifecycle.match_id=requested.match_id \
     WHERE requested.match_id>0 AND NOT COALESCE((\
       lifecycle.status='complete' OR (lifecycle.status='limited' AND (\
         lifecycle.acquisition_state<>'unavailable' \
         OR lifecycle.error_message IS NOT DISTINCT FROM $2\
       ))\
     ),FALSE) ORDER BY requested.ordinality";
const MATCH_DETAIL_PROTOCOL_BATCH_SIZE: usize = 10;

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Relay(#[from] WorkerRelayError),
    #[error("match fact finalization failed: {0}")]
    Facts(String),
    #[error("match {match_id} is terminally unavailable: {reason}")]
    TerminalFacts { match_id: i64, reason: String },
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
    pub empty: bool,
}

#[derive(Debug, Default)]
struct MatchDrainProgress {
    claimed_raw_match_count: Option<i32>,
    checkpointed_ids: HashSet<i64>,
}

#[derive(Debug, Default)]
struct CanonicalWorkSelection {
    batch_ids: Vec<i64>,
    resume_ids: Vec<i64>,
}

impl MatchDrainProgress {
    /// Purpose: report facts completed before a queue-hour error. Input: owned
    /// checkpoint set. Output: optional saturated `i32` count for state audit.
    fn checkpointed_count(&self) -> Option<i32> {
        (!self.checkpointed_ids.is_empty())
            .then(|| i32::try_from(self.checkpointed_ids.len()).unwrap_or(i32::MAX))
    }
}

#[derive(Clone)]
/// Purpose: own the one queue-neutral discovery -> facts -> projection object.
/// Input dependencies: typed database and relay configuration. Output methods:
/// `DiscoveryResult` per queue-hour, never an intermediate work identifier.
/// Relationship: every queue uses `discover_hour`,
/// `fetch_discovered_completed_continuously`, and
/// `checkpoint_canonical_outcome`; only the projector varies by stored facts.
pub struct CanonicalIngestPipeline {
    database: Database,
    relay: WorkerRelayClient,
    facts: MatchFactRepository,
    casual: CasualMechanicsRepository,
    ranked: RankedProjectionRepository,
    lifecycle: RequestedMatchIngestor,
}

impl CanonicalIngestPipeline {
    /// Purpose: construct all shared lifecycle/fact/projector dependencies.
    /// Input: `Database` and `BackendConfig`. Output: ready pipeline or relay
    /// configuration error; no database or vendor work occurs here.
    pub fn new(database: Database, config: &BackendConfig) -> Result<Self, WorkerRelayError> {
        let relay = WorkerRelayClient::new(config)?;
        Ok(Self {
            facts: MatchFactRepository::new(database.clone()),
            casual: CasualMechanicsRepository::new(database.clone()),
            ranked: RankedProjectionRepository::new(database.clone()),
            lifecycle: RequestedMatchIngestor::new(
                database.clone(),
                relay.clone(),
                Duration::from_secs(30 * 60),
            ),
            database,
            relay,
        })
    }

    /// Purpose: process every configured discovery queue uniformly.
    /// Input: UTC date (`&str`), hour (`i32`), audit source (`&str`). Output:
    /// one result per queue after that queue's complete discovered set drains.
    /// Relationship: scheduler discovery calls only this collection method;
    /// ranked and casual queues never have separate orchestration paths.
    pub async fn discover_configured_queues(
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
            results.push(self.discover_hour(queue.queue_id, date, hour, source).await);
        }
        results
    }

    /// Purpose: discover and immediately ingest one queue-hour of any type.
    /// Input: positive queue ID, UTC date/hour, audit source. Output: a result
    /// only after every returned match is durable in the same call.
    pub async fn discover_hour(
        &self,
        queue_id: i32,
        date: &str,
        hour: i32,
        source: &str,
    ) -> Result<DiscoveryResult, PipelineError> {
        let mut progress = MatchDrainProgress::default();
        let result = self
            .discover_hour_inner(queue_id, date, hour, source, &mut progress)
            .await;
        if let Err(error) = &result {
            mark_match_drain_failure(&self.database, queue_id, date, hour, error, &progress)
                .await?;
        }
        result
    }

    /// Purpose: implement every queue-hour using the shared drain.
    /// Input/output types match `discover_hour`; errors are returned
    /// to its state-recording wrapper and never converted into pending work.
    async fn discover_hour_inner(
        &self,
        queue_id: i32,
        date: &str,
        hour: i32,
        source: &str,
        progress: &mut MatchDrainProgress,
    ) -> Result<DiscoveryResult, PipelineError> {
        let mut result = DiscoveryResult {
            queue_id,
            date: date.to_owned(),
            hour,
            ..DiscoveryResult::default()
        };
        if queue_id <= 0 {
            return Err(PipelineError::Facts("queue ID must be positive".to_owned()));
        }
        if !claim_hourly_ingest_hour(&self.database, date, hour, queue_id, source).await? {
            return Ok(result);
        }
        let known_ids = self.discovered_match_ids(date, hour, queue_id).await?;
        let ids = if known_ids.is_empty()
            && claim_hourly_ingest_search(&self.database, date, hour, queue_id).await?
        {
            let discovery = self
                .relay
                .call_value(
                    "getMatchIdsByQueueDetails",
                    vec![json!(queue_id), json!(date.replace('-', "")), json!(hour)],
                    "hourly_match_discovery",
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
            observations
                .into_iter()
                .map(|observation| observation.match_id)
                .collect::<Vec<_>>()
        } else if !known_ids.is_empty() {
            result.discovered = known_ids.len();
            known_ids
        } else {
            return Err(PipelineError::Facts(
                "queue-hour discovery was already attempted without durable IDs".to_owned(),
            ));
        };
        result.empty = ids.is_empty();
        if result.empty {
            mark_hourly_ingest_empty(&self.database, date, hour, queue_id).await?;
            return Ok(result);
        }
        let work = self.match_ids_requiring_canonical_work(&ids).await?;
        result.skipped = ids
            .len()
            .saturating_sub(work.batch_ids.len() + work.resume_ids.len());
        progress.claimed_raw_match_count = Some(i32::try_from(ids.len()).unwrap_or(i32::MAX));
        let batch_result = self
            .fetch_discovered_completed_continuously(
                &work.batch_ids,
                queue_id,
                date,
                hour,
                MatchDiscoverySource::HourlyDiscovery,
                progress,
            )
            .await;
        let resume_result = self
            .resume_discovered_continuously(
                &work.resume_ids,
                queue_id,
                date,
                hour,
                MatchDiscoverySource::HourlyDiscovery,
                progress,
            )
            .await;
        let completed = batch_result.as_ref().copied().unwrap_or_default()
            + resume_result.as_ref().copied().unwrap_or_default();
        result.completed = result.skipped + completed;
        match (batch_result, resume_result) {
            (Ok(_), Ok(_)) => {}
            (Err(error), Ok(_)) => return Err(error),
            (Ok(_), Err(error)) => return Err(error),
            (Err(error), Err(resume_error)) => {
                return Err(PipelineError::Facts(format!(
                    "new acquisition failed: {error}; lifecycle resume failed: {resume_error}"
                )));
            }
        }
        mark_hourly_ingest_complete(
            &self.database,
            date,
            hour,
            queue_id,
            i32::try_from(ids.len()).unwrap_or(i32::MAX),
        )
        .await?;
        Ok(result)
    }

    /// Purpose: load the complete locally known ID set when discovery already
    /// happened. Input: queue-hour key. Output: ordered unique positive IDs;
    /// this replaces debt/pending tables as the source of recovery work.
    async fn discovered_match_ids(
        &self,
        date: &str,
        hour: i32,
        queue_id: i32,
    ) -> Result<Vec<i64>, DatabaseError> {
        let rows = self
            .database
            .query_json(
                "SELECT DISTINCT match_id FROM match_count_discoveries \
                 WHERE source_date=$1::TEXT::DATE AND source_hour=$2 AND queue_id=$3 \
                 AND match_id>0 ORDER BY match_id",
                &[&date, &hour, &queue_id],
            )
            .await?;
        Ok(rows
            .into_iter()
            // PostgreSQL BIGINT values intentionally use node-postgres-compatible
            // JSON strings. Reuse the canonical ID decoder so a known hour is
            // drained from PostgreSQL instead of being rediscovered from Hi-Rez.
            .filter_map(|row| extract_match_id(&row))
            .collect())
    }

    /// Purpose: select canonical work from the indexed lifecycle authority.
    /// Input: discovered `i64` match IDs. Output: only IDs that are neither
    /// complete nor explicitly terminal, partitioned by lifecycle presence.
    /// New IDs use batch acquisition; existing IDs resume their saved evidence
    /// without replaying detail calls or projections.
    async fn match_ids_requiring_canonical_work(
        &self,
        match_ids: &[i64],
    ) -> Result<CanonicalWorkSelection, PipelineError> {
        if match_ids.is_empty() {
            return Ok(CanonicalWorkSelection::default());
        }
        let rows = self
            .database
            .query_json(
                MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL,
                &[&match_ids, &TERMINAL_NO_COMPLETED_MATCH_REASON],
            )
            .await?;
        let mut selection = CanonicalWorkSelection::default();
        for row in rows {
            // This is the same BIGINT boundary as `discovered_match_ids` above.
            // Missing facts must remain in one of the shared drains; dropping
            // the string form falsely classifies every match as durable.
            let Some(match_id) = extract_match_id(&row) else {
                continue;
            };
            if row
                .get("resume_locally")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                selection.resume_ids.push(match_id);
            } else {
                selection.batch_ids.push(match_id);
            }
        }
        Ok(selection)
    }

    /// Purpose: resume every existing lifecycle row without replaying batch
    /// detail acquisition. Input: typed match/queue/hour/source and progress.
    /// Output: durable count after all IDs are attempted in protocol-sized
    /// concurrent windows; no per-run cap, retry ledger, or deferred work.
    async fn resume_discovered_continuously(
        &self,
        match_ids: &[i64],
        queue_id: i32,
        date: &str,
        hour: i32,
        source: MatchDiscoverySource,
        progress: &mut MatchDrainProgress,
    ) -> Result<usize, PipelineError> {
        // Ranked cumulative aggregates operate on the same input set. Complete
        // all locally eligible IDs through the shared set-based projector before
        // individual lifecycle resume; the selector below then removes them.
        let mut completed = self
            .ranked
            .complete_cumulative_stages_for_matches(match_ids)
            .await
            .map_err(|error| PipelineError::Facts(error.to_string()))?;
        let remaining = self.match_ids_requiring_canonical_work(match_ids).await?;
        let match_ids = remaining
            .batch_ids
            .into_iter()
            .chain(remaining.resume_ids)
            .collect::<Vec<_>>();
        let mut failures = Vec::new();
        for window in match_ids.chunks(MATCH_DETAIL_PROTOCOL_BATCH_SIZE) {
            refresh_hourly_ingest_lease(&self.database, date, hour, queue_id).await?;
            let outcomes = join_all(
                window
                    .iter()
                    .copied()
                    .map(|match_id| self.recover_discovered_match(match_id, queue_id, source)),
            )
            .await;
            for (match_id, outcome) in window.iter().copied().zip(outcomes) {
                match outcome {
                    Ok(()) => {
                        progress.checkpointed_ids.insert(match_id);
                        completed += 1;
                    }
                    Err(error) => {
                        let message = error.to_string();
                        if self
                            .record_terminal_outcome(error, queue_id, source, progress)
                            .await?
                        {
                            completed += 1;
                        } else {
                            failures.push(format!("match {match_id}: {message}"));
                        }
                    }
                }
            }
        }
        if failures.is_empty() {
            Ok(completed)
        } else {
            Err(PipelineError::Facts(format!(
                "{} lifecycle match(es) failed after the full queue-hour drain; first: {}",
                failures.len(),
                failures[0]
            )))
        }
    }

    /// Purpose: drain every discovered match through one shared batch pipeline.
    /// Input: all positive `i64` IDs for one `i32` queue, UTC date/hour lease
    /// key, and typed source.
    /// Output: durable match count; the function exhausts the input
    /// in vendor-protocol batches of ten and never applies a per-run work cap.
    async fn fetch_discovered_completed_continuously(
        &self,
        match_ids: &[i64],
        queue_id: i32,
        date: &str,
        hour: i32,
        source: MatchDiscoverySource,
        progress: &mut MatchDrainProgress,
    ) -> Result<usize, PipelineError> {
        let mut remaining = match_ids
            .iter()
            .copied()
            .filter(|id| *id > 0)
            .collect::<VecDeque<_>>();
        let mut emitted = BTreeSet::new();
        let mut completed = 0;
        let mut failures = Vec::new();
        let mut batch_windows = VecDeque::<Vec<i64>>::new();
        while !remaining.is_empty() {
            refresh_hourly_ingest_lease(&self.database, date, hour, queue_id).await?;
            let batch = batch_windows.pop_front().unwrap_or_else(|| {
                remaining
                    .iter()
                    .take(MATCH_DETAIL_PROTOCOL_BATCH_SIZE)
                    .copied()
                    .collect::<Vec<_>>()
            });
            let response = match self
                .relay
                .call_value(
                    "getMatchDetailsBatch",
                    vec![json!(
                        batch
                            .iter()
                            .map(|match_id| json!({"matchId":match_id,"queueId":queue_id}))
                            .collect::<Vec<_>>()
                    )],
                    "canonical_match_recovery",
                )
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    if let Some((left, right)) =
                        recoverable_batch_bisection(&batch, &error.to_string())
                    {
                        batch_windows.push_front(right);
                        batch_windows.push_front(left);
                        continue;
                    }
                    self.record_match_detail_outage(&error).await?;
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
                match self
                    .checkpoint_canonical_outcome(outcome, queue_id, source, progress)
                    .await
                {
                    Ok(()) => completed += 1,
                    Err(error) => {
                        let message = error.to_string();
                        if self
                            .record_terminal_outcome(error, queue_id, source, progress)
                            .await?
                        {
                            completed += 1;
                        } else {
                            failures.push(format!("match {match_id}: {message}"));
                        }
                    }
                }
            }
            for id in batch.iter().filter(|id| returned_ids.contains(id)) {
                if let Some(position) = remaining.iter().position(|candidate| candidate == id) {
                    remaining.remove(position);
                }
            }
            let Some(blocker) = batch.iter().find(|id| !returned_ids.contains(id)).copied() else {
                continue;
            };
            let singleton = match self
                .relay
                .call_value(
                    "getMatchDetailsBatch",
                    vec![json!([{"matchId":blocker,"queueId":queue_id}])],
                    "canonical_match_recovery",
                )
                .await
            {
                Ok(singleton) => singleton,
                Err(error) => {
                    self.record_match_detail_outage(&error).await?;
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
                match self
                    .checkpoint_canonical_outcome(outcome, queue_id, source, progress)
                    .await
                {
                    Ok(()) => completed += 1,
                    Err(error) => {
                        let message = error.to_string();
                        if self
                            .record_terminal_outcome(error, queue_id, source, progress)
                            .await?
                        {
                            completed += 1;
                        } else {
                            failures.push(format!("match {match_id}: {message}"));
                        }
                    }
                }
            }
            if !resolved {
                match self
                    .recover_discovered_match(blocker, queue_id, source)
                    .await
                {
                    Ok(()) => {
                        progress.checkpointed_ids.insert(blocker);
                        completed += 1;
                    }
                    Err(error) => {
                        let message = error.to_string();
                        if self
                            .record_terminal_outcome(error, queue_id, source, progress)
                            .await?
                        {
                            completed += 1;
                        } else {
                            failures.push(format!("match {blocker}: {message}"));
                        }
                    }
                }
            }
            if let Some(position) = remaining.iter().position(|candidate| *candidate == blocker) {
                remaining.remove(position);
            }
        }
        if failures.is_empty() {
            Ok(completed)
        } else {
            Err(PipelineError::Facts(format!(
                "{} match(es) failed after the full queue-hour drain; first: {}",
                failures.len(),
                failures[0]
            )))
        }
    }

    /// Purpose: isolate a terminal provider payload from the remaining batch.
    /// Input: typed per-match pipeline error plus queue/source. Output: `true`
    /// only when the match was durably marked unavailable; transient database,
    /// projection, transport, and ownership errors remain retryable.
    async fn record_terminal_outcome(
        &self,
        error: PipelineError,
        queue_id: i32,
        source: MatchDiscoverySource,
        progress: &mut MatchDrainProgress,
    ) -> Result<bool, PipelineError> {
        let PipelineError::TerminalFacts { match_id, reason } = error else {
            return Ok(false);
        };
        self.lifecycle
            .mark_terminal_unavailable(match_id, queue_id, source, &reason)
            .await?;
        progress.checkpointed_ids.insert(match_id);
        Ok(true)
    }

    /// Purpose: persist a confirmed provider match-detail outage as telemetry.
    /// Input: typed relay error. Output: database telemetry update or no-op;
    /// relationship: it never gates or defers the canonical drain.
    async fn record_match_detail_outage(
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

    /// Purpose: normalize, finalize, and project one batch outcome uniformly.
    /// Input: relay JSON plus expected queue/source. Output: `true` only when
    /// canonical facts and the correct ranked/casual/special projection exist.
    async fn checkpoint_canonical_outcome(
        &self,
        outcome: Value,
        queue_id: i32,
        source: MatchDiscoverySource,
        progress: &mut MatchDrainProgress,
    ) -> Result<(), PipelineError> {
        if authoritative_match_outcome(&outcome) {
            mark_hirez_service_recovered(
                &self.database,
                MATCH_DETAIL_SERVICE_OUTAGE_KEY,
                Some("canonical completed-match batch returned authoritative rows"),
            )
            .await?;
        }
        let match_id = extract_match_id(&outcome).unwrap_or_default();
        let canonical = match CanonicalMatchPayload::from_relay_value(outcome.clone()) {
            Ok(payload) => payload,
            Err(error) => {
                if match_id <= 0 {
                    return Err(PipelineError::Facts(format!(
                        "canonical relay outcome has no match ID: {error}"
                    )));
                }
                return self
                    .recover_discovered_match(match_id, queue_id, source)
                    .await;
            }
        };
        if canonical.queue_id != queue_id {
            return Err(PipelineError::Facts(format!(
                "match {} discovered in queue {queue_id} returned queue {}",
                canonical.match_id, canonical.queue_id
            )));
        }
        let finalized = match self.facts.finalize(&canonical, source.as_database()).await {
            Ok(finalized) => finalized,
            Err(MatchFactError::InvalidPayload(_)) => {
                return self
                    .recover_discovered_match(canonical.match_id, queue_id, source)
                    .await;
            }
            Err(error) => return Err(PipelineError::Facts(error.to_string())),
        };
        match finalized.population {
            MatchPopulation::Ranked => self
                .ranked
                .project_match(canonical.match_id)
                .await
                .map(|_| ())
                .map_err(|error| PipelineError::Facts(format!("{error:?}")))?,
            MatchPopulation::Casual | MatchPopulation::Special => self
                .casual
                .project_all_for_match(canonical.match_id)
                .await
                .map(|_| ())
                .map_err(|error| PipelineError::Facts(format!("{error:?}")))?,
            MatchPopulation::Unknown => {
                return Err(PipelineError::Facts(format!(
                    "match {} remained unclassified after finalization",
                    canonical.match_id
                )));
            }
        }
        progress.checkpointed_ids.insert(canonical.match_id);
        Ok(())
    }

    /// Purpose: finish a non-authoritative/missing batch result immediately via
    /// the same DB-first lifecycle object used by manual lookup.
    /// Input: match/queue IDs and source. Output: durable success or terminal
    /// failure; no retry ledger, buffer, sleep, or deferred claimant is created.
    async fn recover_discovered_match(
        &self,
        match_id: i64,
        queue_id: i32,
        source: MatchDiscoverySource,
    ) -> Result<(), PipelineError> {
        let result = self
            .lifecycle
            .ingest_discovery(MatchIngestRequest {
                match_id,
                queue_id: Some(queue_id),
                source,
            })
            .await;
        match result.status {
            RequestedMatchStatus::Ready => Ok(()),
            RequestedMatchStatus::NotFound => Err(PipelineError::TerminalFacts {
                match_id,
                reason: TERMINAL_NO_COMPLETED_MATCH_REASON.to_owned(),
            }),
            _ => {
                let reason = result.error.unwrap_or_else(|| {
                    format!("match {match_id} did not reach the durable fact boundary")
                });
                Err(PipelineError::Facts(reason))
            }
        }
    }
}

/// Purpose: convert raw discovery rows to typed unique observations. Input:
/// relay JSON. Output: provider-ordered positive-ID observations.
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

/// Purpose: record the progress boundary when any canonical drain aborts.
/// Input: queue ID, queue-hour error and typed progress. Output: reclaimable
/// failed hour; relationship delegates to `mark_hourly_ingest_failed`.
async fn mark_match_drain_failure(
    database: &Database,
    queue_id: i32,
    date: &str,
    hour: i32,
    error: &PipelineError,
    progress: &MatchDrainProgress,
) -> Result<(), DatabaseError> {
    mark_hourly_ingest_failed(
        database,
        date,
        hour,
        queue_id,
        &pipeline_state_error_message(error),
        progress.claimed_raw_match_count,
        progress.checkpointed_count(),
    )
    .await
}

/// Purpose: preserve the useful relay message in hourly state. Input: typed
/// pipeline error. Output: borrowed or owned display text.
fn pipeline_state_error_message(error: &PipelineError) -> Cow<'_, str> {
    match error {
        PipelineError::Relay(WorkerRelayError::Operation { message, .. }) => Cow::Borrowed(message),
        _ => Cow::Owned(error.to_string()),
    }
}

/// Purpose: decide whether a batch payload error can be isolated by bisection.
/// Input: error text. Output: boolean; quota and transport failures stay fatal.
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

/// Purpose: split only a multi-ID payload-failure batch. Input: IDs and error.
/// Output: ordered left/right typed ID windows or `None` for a fatal error.
fn recoverable_batch_bisection(batch: &[i64], error: &str) -> Option<(Vec<i64>, Vec<i64>)> {
    if batch.len() <= 1 || !is_recoverable_completed_batch_error(error) {
        return None;
    }
    let midpoint = batch.len().div_ceil(2);
    Some((batch[..midpoint].to_vec(), batch[midpoint..].to_vec()))
}

/// Purpose: recognize a complete authoritative relay wrapper. Input: outcome
/// JSON. Output: boolean used only to clear outage telemetry.
fn authoritative_match_outcome(outcome: &Value) -> bool {
    matches!(
        outcome.get("status").and_then(Value::as_str),
        Some("complete_direct" | "complete_recovered")
    ) && outcome.get("match").is_some_and(Value::is_object)
}

/// Purpose: enforce exact batch response ownership. Input: requested IDs and
/// relay outcomes. Output: typed `(match_id, outcome)` pairs or a hard error for
/// malformed IDs, duplicates, or outcomes belonging to another request;
/// omitted requested IDs continue to the immediate singleton recovery path.
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

/// Purpose: extract one positive match identifier from supported relay shapes.
/// Input: JSON value. Output: optional `i64` without vendor work.
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

/// Purpose: read the first non-empty text alias from a vendor row. Input: JSON
/// and alias slice. Output: owned optional text.
fn text(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key)?.as_str().map(str::to_owned))
}

/// Purpose: normalize supported vendor boolean encodings. Input: JSON and
/// aliases. Output: deterministic boolean.
fn boolean(value: &Value, keys: &[&str]) -> bool {
    keys.iter()
        .find_map(|key| value.get(*key)?.as_bool())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn postgres_bigint_match_ids_use_the_shared_decoder() {
        assert_eq!(
            extract_match_id(&json!({"match_id":"1281512949"})),
            Some(1_281_512_949)
        );
        assert_eq!(
            extract_match_id(&json!({"match_id":1281512949_i64})),
            Some(1_281_512_949)
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
    fn only_explicit_provider_no_data_is_terminal() {
        assert_eq!(
            TERMINAL_NO_COMPLETED_MATCH_REASON,
            "provider returned no completed match"
        );
        assert_ne!(
            TERMINAL_NO_COMPLETED_MATCH_REASON,
            "invalid canonical match payload"
        );
    }

    #[test]
    fn batch_bisection_is_limited_to_recoverable_payload_errors() {
        assert!(is_recoverable_completed_batch_error(
            "Int16 skin_id too large"
        ));
        assert!(is_recoverable_completed_batch_error("HIREZ_UNKNOWN_RETURN"));
        assert!(!is_recoverable_completed_batch_error("quota exhausted"));
        assert!(!is_recoverable_completed_batch_error("transport timeout"));
    }

    #[test]
    fn recoverable_batch_bisection_preserves_typescript_left_then_right_sequence() {
        let (left, right) = recoverable_batch_bisection(
            &(1..=10).collect::<Vec<_>>(),
            "HIREZ_UNKNOWN_RETURN Int16 skin_id",
        )
        .expect("recoverable ten-ID window");
        assert_eq!(left, vec![1, 2, 3, 4, 5]);
        assert_eq!(right, vec![6, 7, 8, 9, 10]);
        let (left_left, left_right) =
            recoverable_batch_bisection(&left, "too large").expect("recursive left split");
        assert_eq!(left_left, vec![1, 2, 3]);
        assert_eq!(left_right, vec![4, 5]);
        assert!(recoverable_batch_bisection(&[1], "Int16").is_none());
        assert!(recoverable_batch_bisection(&[1, 2], "quota exhausted").is_none());
    }

    #[test]
    fn every_queue_uses_one_immediate_canonical_pipeline_without_debt_or_caps() {
        let source = include_str!("pipeline.rs");
        let discovery = source
            .split("async fn discover_hour_inner")
            .nth(1)
            .expect("queue-neutral discovery")
            .split("async fn discovered_match_ids")
            .next()
            .expect("queue-neutral discovery body");
        assert!(discovery.contains("fetch_discovered_completed_continuously"));
        assert!(discovery.contains("mark_hourly_ingest_complete"));
        assert!(!discovery.contains("hourly_ingest_match_debt"));
        assert!(!discovery.contains("raw_ingest_buffer"));
        assert!(!discovery.contains("MATCH_COUNT_QUEUE_DEFINITIONS"));
        assert!(!discovery.contains("queue_id =="));
        let drain = source
            .split("async fn fetch_discovered_completed_continuously")
            .nth(1)
            .expect("canonical drain")
            .split("async fn record_match_detail_outage")
            .next()
            .expect("drain body");
        assert!(drain.contains("while !remaining.is_empty()"));
        assert!(drain.contains(".take(MATCH_DETAIL_PROTOCOL_BATCH_SIZE)"));
        assert!(!drain.contains("LIMIT"));
    }

    #[test]
    fn provider_search_is_claimed_once_before_the_queue_hour_relay_call() {
        let source = include_str!("pipeline.rs");
        let discovery = source
            .split("async fn discover_hour_inner")
            .nth(1)
            .expect("queue-neutral discovery")
            .split("async fn discovered_match_ids")
            .next()
            .expect("queue-neutral discovery body");
        let claim = discovery
            .find("claim_hourly_ingest_search")
            .expect("durable provider-search claim");
        let relay = discovery
            .find("getMatchIdsByQueueDetails")
            .expect("provider discovery call");
        assert!(claim < relay);
    }

    #[test]
    fn canonical_work_selection_uses_only_the_indexed_lifecycle_authority() {
        assert!(MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL.contains("LEFT JOIN match_ingest_status"));
        assert!(MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL.contains("AS resume_locally"));
        assert!(MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL.contains("NOT COALESCE"));
        assert!(MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL.contains("status='complete'"));
        assert!(!MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL.contains("FROM matches"));
        assert!(!MATCH_IDS_REQUIRING_CANONICAL_WORK_SQL.contains("FROM match_players"));
    }
}
