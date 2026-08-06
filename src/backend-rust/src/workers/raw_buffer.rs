use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{StreamExt, stream};
use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

use super::{
    casual_mechanics::CasualMechanicsRepository,
    match_facts::{CanonicalMatchPayload, MatchFactError, MatchFactRepository},
    policy::api_headroom_snapshot,
    ranked_projection::RankedProjectionRepository,
    relay::WorkerRelayClient,
};

const MAX_RETRIES: i32 = 3;
const DEFAULT_FACT_CONCURRENCY: usize = 8;
const PERFORMANCE_STATS_REFRESH_MIN_SECONDS: i64 = 5 * 60;
static LAST_PERFORMANCE_STATS_REFRESH_AT: AtomicI64 = AtomicI64::new(0);

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct RawBufferBatchResult {
    pub processed: i32,
    pub failed: i32,
    pub deferred: i32,
}

#[derive(Debug, thiserror::Error)]
pub enum RawBufferError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Query(#[from] tokio_postgres::Error),
    #[error(transparent)]
    MatchFacts(#[from] MatchFactError),
    #[error("raw buffer row {row_id} has unsupported entity type {entity_type} ({endpoint})")]
    Unsupported {
        row_id: i64,
        entity_type: String,
        endpoint: String,
    },
    #[error("raw buffer row {row_id} has invalid payload: {message}")]
    Invalid { row_id: i64, message: String },
}

#[derive(Clone, Debug)]
struct ClaimedRow {
    id: i64,
    raw_data: Value,
    endpoint: String,
    entity_type: String,
    entity_id: String,
    retry_count: i32,
    priority: i32,
}

pub async fn process_raw_buffer_batch(
    database: &Database,
    batch_size: usize,
) -> Result<RawBufferBatchResult, RawBufferError> {
    process_raw_buffer_batch_inner(database, None, batch_size, None).await
}

pub async fn process_raw_buffer_batch_with_relay(
    database: &Database,
    relay: &WorkerRelayClient,
    batch_size: usize,
) -> Result<RawBufferBatchResult, RawBufferError> {
    process_raw_buffer_batch_inner(database, Some(relay), batch_size, None).await
}

pub async fn process_raw_buffer_batch_until(
    database: &Database,
    batch_size: usize,
    should_stop: Option<&AtomicBool>,
) -> Result<RawBufferBatchResult, RawBufferError> {
    process_raw_buffer_batch_inner(database, None, batch_size, should_stop).await
}

pub async fn process_raw_buffer_batch_until_with_relay(
    database: &Database,
    relay: &WorkerRelayClient,
    batch_size: usize,
    should_stop: Option<&AtomicBool>,
) -> Result<RawBufferBatchResult, RawBufferError> {
    process_raw_buffer_batch_inner(database, Some(relay), batch_size, should_stop).await
}

async fn process_raw_buffer_batch_inner(
    database: &Database,
    relay: Option<&WorkerRelayClient>,
    batch_size: usize,
    should_stop: Option<&AtomicBool>,
) -> Result<RawBufferBatchResult, RawBufferError> {
    if should_stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
        return Ok(RawBufferBatchResult::default());
    }
    let reserve = std::env::var("API_KEY_RESERVE_CALLS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);
    let headroom = api_headroom_snapshot(database, reserve).await?;
    if !headroom.has_usable_keys {
        requeue_recent_quota_paused_rows(database).await?;
    }
    if let Some(relay) = relay {
        if let Err(error) = relay
            .call_value(
                "cleanupFetchedPlayersCache",
                Vec::new(),
                "backend_unattributed",
            )
            .await
        {
            tracing::warn!(%error, "relay cache cleanup failed before raw-buffer batch");
        }
    } else {
        tracing::warn!("relay cache cleanup unavailable before raw-buffer batch");
    }
    recover_stale_leases(database).await?;
    let rows = claim_rows(database, batch_size).await?;
    if rows.is_empty() {
        return Ok(RawBufferBatchResult::default());
    }
    if should_stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
        return_claims_to_pending(database, &rows).await?;
        return Ok(RawBufferBatchResult::default());
    }
    let mut result = RawBufferBatchResult::default();
    let (fact_rows, derived_rows): (Vec<_>, Vec<_>) = rows
        .into_iter()
        .partition(|row| row.entity_type.eq_ignore_ascii_case("match") && row.priority <= 1);
    let concurrency = std::env::var("MATCH_FACT_PROCESSING_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_FACT_CONCURRENCY);
    let fact_results = stream::iter(fact_rows.into_iter().map(|row| {
        let database = database.clone();
        async move {
            if should_stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
                return_claims_to_pending(&database, &[row]).await?;
                return Ok(RawBufferBatchResult::default());
            }
            match process_claimed_row(&database, row.clone(), headroom.has_usable_keys, true).await
            {
                Ok(result) => Ok(result),
                Err(error) => {
                    let _ = return_claims_to_pending(&database, &[row]).await;
                    Err(error)
                }
            }
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    for outcome in fact_results {
        match outcome {
            Ok(outcome) => merge_batch_result(&mut result, outcome),
            Err(error) => {
                tracing::error!(%error, "fact lane failed outside row retry handling");
                result.failed = result.failed.saturating_add(1);
            }
        }
    }
    let _cumulative_projection_fallbacks = match apply_adaptive_ranked_projection_batches(
        database,
        &derived_rows,
    )
    .await
    {
        Ok(fallbacks) => fallbacks,
        Err(error) => {
            tracing::warn!(error=%error,"projection batch setup failed; using ordinary per-match fallback");
            derived_rows
                .iter()
                .filter(|row| row.entity_type.eq_ignore_ascii_case("match"))
                .filter_map(|row| row.entity_id.parse::<i64>().ok())
                .collect()
        }
    };
    for (index, row) in derived_rows.iter().enumerate() {
        if should_stop.is_some_and(|stop| stop.load(Ordering::Relaxed))
            || (row.priority > 1 && has_pending_match_facts(database).await?)
        {
            return_claims_to_pending(database, &derived_rows[index..]).await?;
            break;
        }
        match process_claimed_row(database, row.clone(), headroom.has_usable_keys, false).await {
            Ok(outcome) => merge_batch_result(&mut result, outcome),
            Err(error) => {
                return_claims_to_pending(database, &derived_rows[index..]).await?;
                return Err(error);
            }
        }
    }
    if should_stop.is_some_and(|stop| stop.load(Ordering::Relaxed)) {
        return Ok(result);
    }
    super::maintenance::cleanup_raw_ingest_buffer_retention(database, "post-batch").await?;
    if result.processed > 0
        && performance_stats_refresh_due()
        && let Err(error) = super::projections::refresh_performance_metric_stats(database).await
    {
        tracing::error!(%error, "post-ingest performance summary refresh failed");
    }
    Ok(result)
}

fn performance_stats_refresh_due() -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        .unwrap_or_default();
    let mut previous = LAST_PERFORMANCE_STATS_REFRESH_AT.load(Ordering::Relaxed);
    loop {
        if now.saturating_sub(previous) < PERFORMANCE_STATS_REFRESH_MIN_SECONDS {
            return false;
        }
        match LAST_PERFORMANCE_STATS_REFRESH_AT.compare_exchange_weak(
            previous,
            now,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(current) => previous = current,
        }
    }
}

fn merge_batch_result(target: &mut RawBufferBatchResult, source: RawBufferBatchResult) {
    target.processed = target.processed.saturating_add(source.processed);
    target.failed = target.failed.saturating_add(source.failed);
    target.deferred = target.deferred.saturating_add(source.deferred);
}

async fn apply_adaptive_ranked_projection_batches(
    database: &Database,
    rows: &[ClaimedRow],
) -> Result<BTreeSet<i64>, RawBufferError> {
    renew_claimed_rows(database, rows).await?;
    let ids = rows
        .iter()
        .filter(|row| row.entity_type.eq_ignore_ascii_case("match"))
        .filter_map(|row| row.entity_id.parse::<i64>().ok())
        .collect::<BTreeSet<_>>();
    if ids.len() < 2 {
        return Ok(BTreeSet::new());
    }
    let ids = ids.into_iter().collect::<Vec<_>>();
    let performance = database
        .query_json(
            "SELECT mis.match_id::TEXT FROM match_ingest_status mis JOIN matches m ON m.match_id=mis.match_id WHERE mis.match_id=ANY($1::BIGINT[]) AND COALESCE(m.is_ranked,m.queue_id=486) AND mis.completed_stages@>ARRAY['ranked_stats']::TEXT[] AND NOT mis.completed_stages@>ARRAY['performance_projections']::TEXT[] ORDER BY mis.match_id",
            &[&ids],
        )
        .await?
        .into_iter()
        .filter_map(|row| integer(&row, "match_id"))
        .collect::<Vec<_>>();
    let mut fallback = apply_adaptive_projection_stage(
        database,
        rows,
        &performance,
        CumulativeProjectionStage::Performance,
    )
    .await?;
    let scalable = database
        .query_json(
            "SELECT mis.match_id::TEXT FROM match_ingest_status mis JOIN matches m ON m.match_id=mis.match_id WHERE mis.match_id=ANY($1::BIGINT[]) AND COALESCE(m.is_ranked,m.queue_id=486) AND mis.completed_stages@>ARRAY['performance_projections']::TEXT[] AND NOT mis.completed_stages@>ARRAY['scalable_stats']::TEXT[] ORDER BY mis.match_id",
            &[&ids],
        )
        .await?
        .into_iter()
        .filter_map(|row| integer(&row, "match_id"))
        .collect::<Vec<_>>();
    renew_claimed_rows(database, rows).await?;
    fallback.extend(
        apply_adaptive_projection_stage(
            database,
            rows,
            &scalable,
            CumulativeProjectionStage::Scalable,
        )
        .await?,
    );
    Ok(fallback)
}

#[derive(Clone, Copy)]
enum CumulativeProjectionStage {
    Performance,
    Scalable,
}

async fn apply_adaptive_projection_stage(
    database: &Database,
    claimed_rows: &[ClaimedRow],
    candidates: &[i64],
    stage: CumulativeProjectionStage,
) -> Result<BTreeSet<i64>, RawBufferError> {
    let batch_size = std::env::var("CUMULATIVE_PROJECTION_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8)
        .min(25);
    let mut pending = candidates
        .chunks(batch_size)
        .map(<[i64]>::to_vec)
        .rev()
        .collect::<Vec<_>>();
    let mut fallback = BTreeSet::new();
    while let Some(chunk) = pending.pop() {
        renew_claimed_matches(database, claimed_rows, &chunk).await?;
        let repository = RankedProjectionRepository::new(database.clone());
        let error = match stage {
            CumulativeProjectionStage::Performance => repository
                .project_performance_batch(&chunk)
                .await
                .err()
                .map(|error| error.to_string()),
            CumulativeProjectionStage::Scalable => repository
                .project_scalable_batch(&chunk)
                .await
                .err()
                .map(|error| error.to_string()),
        };
        if error.is_none() {
        } else if chunk.len() > 1 {
            let midpoint = chunk.len().div_ceil(2);
            pending.push(chunk[midpoint..].to_vec());
            pending.push(chunk[..midpoint].to_vec());
        } else {
            fallback.insert(chunk[0]);
            tracing::warn!(match_id=chunk[0],error=%error.unwrap_or_default(),"ranked projection batch singleton retained for ordinary retry");
        }
    }
    Ok(fallback)
}

async fn process_claimed_row(
    database: &Database,
    row: ClaimedRow,
    has_api_headroom: bool,
    facts_only: bool,
) -> Result<RawBufferBatchResult, RawBufferError> {
    let mut result = RawBufferBatchResult::default();
    if !renew_claimed_row(database, row.id).await? {
        return Ok(result);
    }
    if !has_api_headroom
        && row.entity_type.eq_ignore_ascii_case("match")
        && match_payload_requires_recovery(&row.raw_data)
    {
        database.query_json(
            "UPDATE raw_ingest_buffer SET status='pending',error_message='quota pause: recovery-required match retained pending',processed_at=NULL WHERE id=$1",
            &[&row.id],
        ).await?;
        result.deferred = 1;
        return Ok(result);
    }
    match process_row(database, &row, facts_only).await {
        Ok(()) if facts_only => {
            let limited = database.one_json(
                "SELECT status='limited' limited FROM match_ingest_status WHERE match_id=$1::TEXT::BIGINT",
                &[&row.entity_id],
            ).await?.and_then(|value| value.get("limited").and_then(Value::as_bool)).unwrap_or(false);
            if limited {
                mark_processed(database, row.id).await?;
                result.processed = 1;
            } else {
                database.query_json(
                    "UPDATE raw_ingest_buffer SET status='pending',error_message='match facts durable; background projections pending',processed_at=NULL WHERE id=$1",
                    &[&row.id],
                ).await?;
                result.deferred = 1;
            }
        }
        Ok(()) => {
            mark_processed(database, row.id).await?;
            result.processed = 1;
        }
        Err(error) => {
            let retry = row.retry_count.saturating_add(1);
            let facts_durable = row.entity_type.eq_ignore_ascii_case("match")
                && match_facts_are_durable(database, &row.entity_id).await?;
            let terminal = !facts_durable
                && (retry >= MAX_RETRIES || matches!(error, RawBufferError::Unsupported { .. }));
            mark_failed_or_deferred(
                database,
                row.id,
                retry,
                terminal,
                facts_durable,
                &error.to_string(),
            )
            .await?;
            if terminal {
                result.failed = 1;
            } else {
                result.deferred = 1;
            }
        }
    }
    Ok(result)
}

async fn renew_claimed_row(database: &Database, row_id: i64) -> Result<bool, DatabaseError> {
    Ok(database
        .one_json(
            "UPDATE raw_ingest_buffer SET processed_at=now() WHERE id=$1 AND status='processing' RETURNING id",
            &[&row_id],
        )
        .await?
        .is_some())
}

async fn renew_claimed_rows(database: &Database, rows: &[ClaimedRow]) -> Result<(), DatabaseError> {
    let ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
    if !ids.is_empty() {
        database
            .query_json(
                "UPDATE raw_ingest_buffer SET processed_at=now() WHERE id=ANY($1::BIGINT[]) AND status='processing'",
                &[&ids],
            )
            .await?;
    }
    Ok(())
}

async fn renew_claimed_matches(
    database: &Database,
    rows: &[ClaimedRow],
    match_ids: &[i64],
) -> Result<(), DatabaseError> {
    let match_ids = match_ids.iter().copied().collect::<BTreeSet<_>>();
    let rows = rows
        .iter()
        .filter(|row| {
            row.entity_id
                .parse::<i64>()
                .ok()
                .is_some_and(|id| match_ids.contains(&id))
        })
        .cloned()
        .collect::<Vec<_>>();
    renew_claimed_rows(database, &rows).await
}

async fn recover_stale_leases(database: &Database) -> Result<(), DatabaseError> {
    recover_stale_leases_for(database, None).await
}

async fn recover_stale_leases_for(
    database: &Database,
    row_id: Option<i64>,
) -> Result<(), DatabaseError> {
    database
        .query_json(
            "UPDATE raw_ingest_buffer SET \
               status=CASE WHEN EXISTS(SELECT 1 FROM match_ingest_status mis WHERE raw_ingest_buffer.entity_type='match' AND raw_ingest_buffer.entity_id~'^[0-9]+$' AND mis.match_id=raw_ingest_buffer.entity_id::BIGINT AND mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[]) THEN 'pending' WHEN retry_count >= $1 THEN 'failed' ELSE 'pending' END,\
               retry_count=retry_count+1,\
               error_message=concat_ws(' | ',nullif(error_message,''),'stale processing lease reset'),\
               processed_at=NULL,\
               available_at=CASE WHEN EXISTS(SELECT 1 FROM match_ingest_status mis WHERE raw_ingest_buffer.entity_type='match' AND raw_ingest_buffer.entity_id~'^[0-9]+$' AND mis.match_id=raw_ingest_buffer.entity_id::BIGINT AND mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[]) THEN now()+INTERVAL '5 minutes' ELSE available_at END \
             WHERE status='processing' \
               AND ($2::BIGINT IS NULL OR id=$2) \
               AND ((processed_at IS NOT NULL AND processed_at<now()-INTERVAL '15 minutes') OR (processed_at IS NULL AND created_at<now()-INTERVAL '30 minutes'))",
            &[&(MAX_RETRIES - 1), &row_id],
        )
        .await?;
    Ok(())
}

async fn claim_rows(
    database: &Database,
    batch_size: usize,
) -> Result<Vec<ClaimedRow>, RawBufferError> {
    let limit = i64::try_from(batch_size.max(1)).unwrap_or(i64::MAX);
    let has_pending_match_facts = database
        .one_json(
            "SELECT EXISTS(SELECT 1 FROM raw_ingest_buffer rib LEFT JOIN match_ingest_status mis ON mis.match_id=CASE WHEN rib.entity_id~'^[0-9]+$' THEN rib.entity_id::BIGINT ELSE NULL END WHERE rib.entity_type='match' AND rib.status='pending' AND NOT COALESCE(mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[],FALSE)) has_match_facts",
            &[],
        )
        .await?
        .and_then(|row| row.get("has_match_facts").and_then(Value::as_bool))
        .unwrap_or(false);
    let rows = database
        .query_json(
            "WITH eligible AS MATERIALIZED(\
               SELECT rib.id,rib.created_at,CASE \
                 WHEN rib.entity_type='match' AND NOT COALESCE(mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[],FALSE) AND CASE WHEN jsonb_typeof(rib.raw_data)='array' THEN jsonb_array_length(rib.raw_data)<10 OR EXISTS(SELECT 1 FROM jsonb_array_elements(rib.raw_data) player WHERE btrim(COALESCE(player->>'ret_msg',''))<>'' OR lower(COALESCE(player->>'source',''))='recovered') ELSE FALSE END THEN 0 \
                 WHEN rib.entity_type='match' AND NOT COALESCE(mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[],FALSE) THEN 1 \
                 WHEN rib.entity_type='match' THEN 3 \
                 WHEN rib.endpoint IN('getmatchhistory','getplayermatchhistory','getplayermatchhistoryafterdatetime') OR rib.entity_type IN('match_history','prefetch_match') THEN 4 \
                 ELSE 2 END priority,CASE WHEN rib.entity_type='match' THEN 'match:'||COALESCE(rib.entity_id,rib.id::TEXT) ELSE 'row:'||rib.id::TEXT END entity_claim_key \
               FROM raw_ingest_buffer rib LEFT JOIN match_ingest_status mis ON mis.match_id=CASE WHEN rib.entity_id~'^[0-9]+$' THEN rib.entity_id::BIGINT ELSE NULL END \
               WHERE rib.status='pending' AND rib.available_at<=now() \
                 AND (NOT $2::BOOLEAN OR (rib.entity_type='match' AND NOT COALESCE(mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[],FALSE))) \
                 AND (rib.entity_type<>'match' OR NOT EXISTS(SELECT 1 FROM raw_ingest_buffer in_flight WHERE in_flight.entity_type='match' AND in_flight.entity_id IS NOT DISTINCT FROM rib.entity_id AND in_flight.status='processing' AND in_flight.id<>rib.id))\
             ),deduplicated AS MATERIALIZED(\
               SELECT DISTINCT ON(entity_claim_key) id,created_at,priority FROM eligible ORDER BY entity_claim_key,priority,created_at,id\
             ),candidates AS(\
               SELECT rib.id,rib.created_at,deduplicated.priority FROM deduplicated JOIN raw_ingest_buffer rib ON rib.id=deduplicated.id ORDER BY deduplicated.priority,rib.created_at,rib.id LIMIT $1 FOR UPDATE OF rib SKIP LOCKED\
             ),claimed AS(\
               UPDATE raw_ingest_buffer rib SET status='processing',processed_at=now() \
               FROM candidates WHERE rib.id=candidates.id \
               RETURNING rib.id,rib.raw_data,rib.endpoint,rib.entity_type,rib.entity_id,rib.retry_count,candidates.priority\
             ) SELECT * FROM claimed ORDER BY priority,id",
            &[&limit, &has_pending_match_facts],
        )
        .await?;
    rows.into_iter().map(decode_claimed_row).collect()
}

fn decode_claimed_row(value: Value) -> Result<ClaimedRow, RawBufferError> {
    let id = integer(&value, "id").unwrap_or_default();
    let raw_data = value.get("raw_data").cloned().unwrap_or(Value::Null);
    if id <= 0 || raw_data.is_null() {
        return Err(RawBufferError::Invalid {
            row_id: id,
            message: "claim did not return id/raw_data".to_owned(),
        });
    }
    Ok(ClaimedRow {
        id,
        raw_data,
        endpoint: text(&value, "endpoint"),
        entity_type: text(&value, "entity_type"),
        entity_id: text(&value, "entity_id"),
        retry_count: integer(&value, "retry_count")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        priority: integer(&value, "priority")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(i32::MAX),
    })
}

async fn process_row(
    database: &Database,
    row: &ClaimedRow,
    facts_only: bool,
) -> Result<(), RawBufferError> {
    let endpoint = row.endpoint.to_ascii_lowercase();
    let entity_type = row.entity_type.to_ascii_lowercase();
    if is_history_contract(&endpoint, &entity_type, &row.raw_data) {
        return persist_match_history(database, row).await;
    }
    match entity_type.as_str() {
        "match" => persist_match(database, row, facts_only).await,
        "loadout" => persist_loadouts(database, row).await,
        "player" => persist_players(database, row).await,
        "champion" => persist_champions(database, row).await,
        "item" => persist_items(database, row).await,
        "player_status" => persist_player_status(database, row).await,
        "player_champions" => persist_player_champions(database, row).await,
        "player_achievements" => persist_player_achievements(database, row).await,
        "champion_skins" => persist_champion_skins(database, row).await,
        "live_match" => persist_live_match(database, row).await,
        "leaderboard" | "league_leaderboard" => persist_leaderboard(database, row).await,
        "esports" | "esports_team" => persist_esports(database, row).await,
        "bounty_items" => persist_bounty_items(database, row).await,
        _ => Err(RawBufferError::Unsupported {
            row_id: row.id,
            entity_type: row.entity_type.clone(),
            endpoint: row.endpoint.clone(),
        }),
    }
}

fn is_history_contract(endpoint: &str, entity_type: &str, raw: &Value) -> bool {
    matches!(entity_type, "match_history" | "prefetch_match")
        || matches!(
            endpoint,
            "getmatchhistory" | "getplayermatchhistory" | "getplayermatchhistoryafterdatetime"
        )
        || raw.as_array().is_some_and(|rows| {
            !rows.is_empty()
                && rows.iter().all(|row| {
                    matches!(
                        text(row, "source").to_ascii_lowercase().as_str(),
                        "prefetch" | "match_history" | "history_observation" | "legacy_prefetch"
                    )
                })
        })
}

async fn persist_match(
    database: &Database,
    row: &ClaimedRow,
    facts_only: bool,
) -> Result<(), RawBufferError> {
    let payload = CanonicalMatchPayload::from_buffer_rows(row.raw_data.clone())?;
    if !row.entity_id.is_empty() && row.entity_id.parse::<i64>().ok() != Some(payload.match_id) {
        return Err(RawBufferError::Invalid {
            row_id: row.id,
            message: "entity_id does not match payload match_id".to_owned(),
        });
    }
    let finalized = MatchFactRepository::new(database.clone())
        .finalize(&payload, "rust_raw_buffer")
        .await?;
    if facts_only || finalized.population == super::match_lifecycle::MatchPopulation::Unknown {
        return Ok(());
    }
    if finalized.population == super::match_lifecycle::MatchPopulation::Ranked {
        RankedProjectionRepository::new(database.clone())
            .project_match(payload.match_id)
            .await
            .map_err(|error| RawBufferError::Invalid {
                row_id: row.id,
                message: error.to_string(),
            })?;
    } else {
        CasualMechanicsRepository::new(database.clone())
            .project_all_for_match(payload.match_id)
            .await
            .map_err(|error| RawBufferError::Invalid {
                row_id: row.id,
                message: error.to_string(),
            })?;
    }
    Ok(())
}

async fn persist_match_history(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    let raw = clean_json_text(&row.raw_data)?;
    database
        .query_json(
            r#"
            WITH payload AS (
              SELECT value AS raw
              FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof($1::text::jsonb)='array'
                  THEN $1::text::jsonb ELSE jsonb_build_array($1::text::jsonb) END
              )
              WHERE btrim(COALESCE(value->>'ret_msg',''))=''
            ), normalized AS (
              SELECT
                COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint) match_id,
                COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint,
                         NULLIF(raw->>'playerIdActive','')::bigint) player_id,
                COALESCE(NULLIF(raw->>'entry_datetime',''),NULLIF(raw->>'Match_Time',''),
                         NULLIF(raw->>'Entry_Datetime','')) entry_datetime,
                COALESCE(NULLIF(raw->>'queue_id','')::int,NULLIF(raw->>'match_queue_id','')::int,
                         NULLIF(raw->>'Match_Queue_Id','')::int) queue_id,
                COALESCE(NULLIF(raw->>'region',''),NULLIF(raw->>'Region','')) region,
                COALESCE(NULLIF(raw->>'map',''),NULLIF(raw->>'Map_Game','')) map,
                COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int) champion_id,
                COALESCE(NULLIF(raw->>'champion_name',''),NULLIF(raw->>'Champion','')) champion_name,
                COALESCE(NULLIF(raw->>'skin_id','')::int,NULLIF(raw->>'SkinId','')::int) skin_id,
                COALESCE(NULLIF(raw->>'skin_name',''),NULLIF(raw->>'Skin','')) skin_name,
                COALESCE(NULLIF(raw->>'win_status',''),NULLIF(raw->>'Win_Status','')) win_status,
                COALESCE(NULLIF(raw->>'kills','')::int,NULLIF(raw->>'Kills','')::int,0) kills,
                COALESCE(NULLIF(raw->>'deaths','')::int,NULLIF(raw->>'Deaths','')::int,0) deaths,
                COALESCE(NULLIF(raw->>'assists','')::int,NULLIF(raw->>'Assists','')::int,0) assists,
                COALESCE(NULLIF(raw->>'damage','')::int,NULLIF(raw->>'damage_done_physical','')::int,
                         NULLIF(raw->>'Damage','')::int,0) damage,
                COALESCE(NULLIF(raw->>'healing','')::int,NULLIF(raw->>'Healing','')::int,0) healing,
                COALESCE(NULLIF(raw->>'gold_earned','')::int,NULLIF(raw->>'Gold_Earned','')::int,0) gold_earned,
                COALESCE(NULLIF(raw->>'time_in_match','')::int,NULLIF(raw->>'match_duration','')::int,
                         NULLIF(raw->>'Time_In_Match_Seconds','')::int,0) time_in_match,
                COALESCE(NULLIF(raw->>'task_force','')::smallint,NULLIF(raw->>'TaskForce','')::smallint,0) task_force,
                COALESCE(NULLIF(raw->>'league_tier','')::smallint,NULLIF(raw->>'League_Tier','')::smallint,0) league_tier,
                raw
              FROM payload
            )
            INSERT INTO player_match_history_entries(
              match_id,player_id,fetched_player_id,entry_datetime,queue_id,region,map,
              champion_id,champion_name,skin_id,skin_name,win_status,kills,deaths,assists,
              damage,healing,gold_earned,time_in_match,task_force,league_tier,source,
              raw_data,normalized_data,observed_at,expires_at
            )
            SELECT match_id,player_id,player_id,NULLIF(entry_datetime,'')::timestamptz,queue_id,
              region,map,champion_id,champion_name,skin_id,skin_name,win_status,kills,deaths,
              assists,damage,healing,gold_earned,time_in_match,task_force,league_tier,
              COALESCE(NULLIF($2,''),'getmatchhistory'),raw,raw||jsonb_build_object('source','match_history'),
              now(),now()+INTERVAL '6 hours'
            FROM normalized WHERE match_id>0 AND player_id>0
            ON CONFLICT(match_id,player_id) DO UPDATE SET
              fetched_player_id=COALESCE(EXCLUDED.fetched_player_id,player_match_history_entries.fetched_player_id),
              entry_datetime=COALESCE(EXCLUDED.entry_datetime,player_match_history_entries.entry_datetime),
              queue_id=COALESCE(EXCLUDED.queue_id,player_match_history_entries.queue_id),
              region=COALESCE(EXCLUDED.region,player_match_history_entries.region),
              map=COALESCE(NULLIF(EXCLUDED.map,''),player_match_history_entries.map),
              champion_id=COALESCE(EXCLUDED.champion_id,player_match_history_entries.champion_id),
              champion_name=COALESCE(NULLIF(EXCLUDED.champion_name,''),player_match_history_entries.champion_name),
              skin_id=COALESCE(EXCLUDED.skin_id,player_match_history_entries.skin_id),
              skin_name=COALESCE(NULLIF(EXCLUDED.skin_name,''),player_match_history_entries.skin_name),
              win_status=COALESCE(NULLIF(EXCLUDED.win_status,''),player_match_history_entries.win_status),
              kills=EXCLUDED.kills,deaths=EXCLUDED.deaths,assists=EXCLUDED.assists,
              damage=EXCLUDED.damage,healing=EXCLUDED.healing,gold_earned=EXCLUDED.gold_earned,
              time_in_match=EXCLUDED.time_in_match,task_force=EXCLUDED.task_force,
              league_tier=EXCLUDED.league_tier,source=EXCLUDED.source,raw_data=EXCLUDED.raw_data,
              normalized_data=EXCLUDED.normalized_data,observed_at=now(),expires_at=EXCLUDED.expires_at
            "#,
            &[&raw, &row.endpoint],
        )
        .await?;
    Ok(())
}

async fn persist_loadouts(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    let raw = clean_json_text(&row.raw_data)?;
    database
        .query_json(
            r#"
            WITH rows AS(
              SELECT value raw FROM jsonb_array_elements($1::text::jsonb)
              WHERE btrim(COALESCE(value->>'ret_msg',''))=''
            ), decks AS(
              SELECT
                COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint) player_id,
                COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int) champion_id,
                COALESCE(NULLIF(raw->>'deck_id','')::bigint,NULLIF(raw->>'DeckId','')::bigint) deck_id,
                COALESCE(NULLIF(raw->>'deck_name',''),NULLIF(raw->>'DeckName',''),
                         NULLIF(raw->>'loadout_name','')) deck_name,
                raw
              FROM rows
            ), upserted AS(
              INSERT INTO player_loadouts(
                player_id,champion_id,deck_id,deck_key,loadout_name,card_ids,card_levels,talent_id,fetched_at,updated_at
              )
              SELECT player_id,champion_id,deck_id,
                CASE WHEN COALESCE(deck_id,0)>0 THEN 'id:'||deck_id::text
                  ELSE 'legacy:'||champion_id::text||':'||left(lower(regexp_replace(deck_name,'\s+',' ','g')),80) END,
                deck_name,
                ARRAY(SELECT COALESCE(NULLIF(card->>'item_id','')::int,NULLIF(card->>'ItemId','')::int)
                      FROM jsonb_array_elements(COALESCE(raw->'cards',raw->'LoadoutItems','[]'::jsonb)) card),
                ARRAY(SELECT COALESCE(NULLIF(card->>'points','')::int,NULLIF(card->>'Points','')::int,0)
                      FROM jsonb_array_elements(COALESCE(raw->'cards',raw->'LoadoutItems','[]'::jsonb)) card),
                NULL,now(),now()
              FROM decks WHERE player_id>0 AND champion_id>0 AND COALESCE(deck_name,'')<>''
              ON CONFLICT(player_id,deck_key) DO UPDATE SET champion_id=EXCLUDED.champion_id,
                deck_id=EXCLUDED.deck_id,loadout_name=EXCLUDED.loadout_name,
                card_ids=EXCLUDED.card_ids,card_levels=EXCLUDED.card_levels,fetched_at=now(),updated_at=now()
              RETURNING player_id
            )
            INSERT INTO player_loadout_fetches(player_id,fetched_at)
            SELECT DISTINCT player_id,now() FROM upserted
            ON CONFLICT(player_id) DO UPDATE SET fetched_at=now()
            "#,
            &[&raw],
        )
        .await?;
    Ok(())
}

async fn persist_players(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    let profiles = row
        .raw_data
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(&row.raw_data));
    for profile in profiles {
        if !text(profile, "ret_msg").trim().is_empty() {
            continue;
        }
        super::profile_enrichment::persist_player_profile(database, profile)
            .await
            .map_err(|error| RawBufferError::Invalid {
                row_id: row.id,
                message: error.to_string(),
            })?;
    }
    Ok(())
}

async fn persist_champions(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO champions(id,name,title,roles,lore,health,speed,icon_url,last_updated)
        SELECT COALESCE(NULLIF(raw->>'id','')::int,NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int),
          COALESCE(NULLIF(raw->>'name',''),NULLIF(raw->>'Name','')),
          COALESCE(raw->>'title',raw->>'Title'),COALESCE(raw->>'roles',raw->>'Roles'),
          COALESCE(raw->>'lore',raw->>'Lore'),
          COALESCE(NULLIF(raw->>'health','')::int,NULLIF(raw->>'Health','')::int,0),
          COALESCE(NULLIF(raw->>'speed','')::int,NULLIF(raw->>'Speed','')::int,0),
          COALESCE(raw->>'icon_url',raw->>'ChampionIcon_URL'),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'id','')::int,NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int)>0
        ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,title=EXCLUDED.title,roles=EXCLUDED.roles,
          lore=EXCLUDED.lore,health=EXCLUDED.health,speed=EXCLUDED.speed,icon_url=EXCLUDED.icon_url,last_updated=now()"#,
    )
    .await
}

async fn persist_items(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO items(item_id,item_name,description,price,icon_url,item_type,last_updated)
        SELECT COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'ItemId','')::int),
          COALESCE(NULLIF(raw->>'item_name',''),NULLIF(raw->>'DeviceName','')),
          COALESCE(raw->>'description',raw->>'Description'),
          COALESCE(NULLIF(raw->>'price','')::int,NULLIF(raw->>'Price','')::int,0),
          COALESCE(raw->>'icon_url',raw->>'itemIcon_URL'),
          COALESCE(raw->>'item_type',raw->>'Type'),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'ItemId','')::int)>0
        ON CONFLICT(item_id) DO UPDATE SET item_name=EXCLUDED.item_name,description=EXCLUDED.description,
          price=EXCLUDED.price,icon_url=EXCLUDED.icon_url,item_type=EXCLUDED.item_type,last_updated=now()"#,
    )
    .await
}

async fn persist_player_status(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO player_status(player_id,status,status_string,current_match_id,queue_id,updated_at)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint),
          COALESCE(NULLIF(raw->>'status','')::int,NULLIF(raw->>'status_id','')::int,0),
          COALESCE(raw->>'status_string',raw->>'status'),
          COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint),
          COALESCE(NULLIF(raw->>'queue_id','')::int,NULLIF(raw->>'match_queue_id','')::int),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint)>0
        ON CONFLICT(player_id) DO UPDATE SET status=EXCLUDED.status,status_string=EXCLUDED.status_string,
          current_match_id=EXCLUDED.current_match_id,queue_id=EXCLUDED.queue_id,updated_at=now()"#,
    )
    .await
}

async fn persist_player_champions(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO player_champions(player_id,champion_id,champion_name,xp,ownership_type,wins,losses,kills,deaths,assists,minutes_played,stats_populated,last_updated)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::int,NULLIF(raw->>'playerId','')::int),
          COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'champion_id','')::int),
          COALESCE(raw->>'champion_name',raw->>'champion'),
          COALESCE(NULLIF(raw->>'xp','')::bigint,NULLIF(raw->>'Worshippers','')::bigint,0),
          COALESCE(raw->>'ownership_type',raw->>'Ownership'),COALESCE(NULLIF(raw->>'wins','')::int,0),
          COALESCE(NULLIF(raw->>'losses','')::int,0),COALESCE(NULLIF(raw->>'kills','')::int,0),
          COALESCE(NULLIF(raw->>'deaths','')::int,0),COALESCE(NULLIF(raw->>'assists','')::int,0),
          COALESCE(NULLIF(raw->>'minutes_played','')::int,0),true,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::int,NULLIF(raw->>'playerId','')::int)>0
        ON CONFLICT(player_id,champion_id) DO UPDATE SET champion_name=EXCLUDED.champion_name,xp=EXCLUDED.xp,
          ownership_type=EXCLUDED.ownership_type,wins=EXCLUDED.wins,losses=EXCLUDED.losses,kills=EXCLUDED.kills,
          deaths=EXCLUDED.deaths,assists=EXCLUDED.assists,minutes_played=EXCLUDED.minutes_played,
          stats_populated=true,last_updated=now()"#,
    )
    .await
}

async fn persist_player_achievements(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO player_achievements(player_id,achievements,updated_at)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF($2,'')::bigint),raw,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF($2,'')::bigint)>0
        ON CONFLICT(player_id) DO UPDATE SET achievements=EXCLUDED.achievements,updated_at=now()"#,
    )
    .await
}

async fn persist_champion_skins(
    database: &Database,
    row: &ClaimedRow,
) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO skins(skin_id,skin_name,champion_id,champion_name,rarity,icon_url,last_updated)
        SELECT COALESCE(NULLIF(raw->>'skin_id','')::int,NULLIF(raw->>'skin_id1','')::int),
          COALESCE(raw->>'skin_name',raw->>'skin_name1'),
          COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'champion_id1','')::int),
          COALESCE(raw->>'champion_name',raw->>'champion_name1'),COALESCE(raw->>'rarity',raw->>'rarity'),
          COALESCE(raw->>'icon_url',raw->>'skinIcon_URL'),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'skin_id','')::int,NULLIF(raw->>'skin_id1','')::int)>0
        ON CONFLICT(skin_id) DO UPDATE SET skin_name=EXCLUDED.skin_name,champion_id=EXCLUDED.champion_id,
          champion_name=EXCLUDED.champion_name,rarity=EXCLUDED.rarity,icon_url=EXCLUDED.icon_url,last_updated=now()"#,
    )
    .await
}

async fn persist_live_match(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO live_match_players(match_id,player_id,player_name,champion_id,champion_name,skin_id,skin_name,account_level,mastery_level,tier,tier_wins,tier_losses,task_force,platform)
        SELECT COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint),
          COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint),
          COALESCE(raw->>'player_name',raw->>'playerName'),
          COALESCE(NULLIF(raw->>'champion_id','')::int,NULLIF(raw->>'ChampionId','')::int),
          COALESCE(raw->>'champion_name',raw->>'ChampionName'),
          COALESCE(NULLIF(raw->>'skin_id','')::int,NULLIF(raw->>'SkinId','')::int,0),
          COALESCE(raw->>'skin_name',raw->>'Skin',''),
          COALESCE(NULLIF(raw->>'account_level','')::int,NULLIF(raw->>'Account_Level','')::int,0),
          COALESCE(NULLIF(raw->>'mastery_level','')::int,NULLIF(raw->>'Mastery_Level','')::int,0),
          COALESCE(NULLIF(raw->>'tier','')::int,NULLIF(raw->>'Tier','')::int,0),
          COALESCE(NULLIF(raw->>'tier_wins','')::int,NULLIF(raw->>'tierWins','')::int,0),
          COALESCE(NULLIF(raw->>'tier_losses','')::int,NULLIF(raw->>'tierLosses','')::int,0),
          COALESCE(NULLIF(raw->>'task_force','')::int,NULLIF(raw->>'taskForce','')::int,NULLIF(raw->>'team','')::int,0),
          COALESCE(NULLIF(raw->>'platform','')::int,NULLIF(raw->>'Platform','')::int,NULLIF(raw->>'playerPortalId','')::int,0)
        FROM rows WHERE COALESCE(NULLIF(raw->>'match_id','')::bigint,NULLIF(raw->>'Match','')::bigint)>0
        ON CONFLICT(match_id,player_id) DO UPDATE SET player_name=EXCLUDED.player_name,
          champion_id=EXCLUDED.champion_id,champion_name=EXCLUDED.champion_name,skin_id=EXCLUDED.skin_id,
          skin_name=EXCLUDED.skin_name,account_level=EXCLUDED.account_level,mastery_level=EXCLUDED.mastery_level,
          tier=EXCLUDED.tier,tier_wins=EXCLUDED.tier_wins,tier_losses=EXCLUDED.tier_losses,
          task_force=EXCLUDED.task_force,platform=EXCLUDED.platform"#,
    )
    .await
}

async fn persist_leaderboard(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO leaderboard_entries(player_id,player_name,rank,ranked_points,wins,losses,queue_id,updated_at)
        SELECT COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint),
          COALESCE(raw->>'player_name',raw->>'Name'),
          COALESCE(NULLIF(raw->>'rank','')::int,NULLIF(raw->>'Rank','')::int,0),
          COALESCE(NULLIF(raw->>'player_ranking','')::int,NULLIF(raw->>'Rank_Stat','')::int,0),
          COALESCE(NULLIF(raw->>'wins','')::int,NULLIF(raw->>'Wins','')::int,0),
          COALESCE(NULLIF(raw->>'losses','')::int,NULLIF(raw->>'Losses','')::int,0),
          COALESCE(NULLIF(raw->>'queue_id','')::int,486),now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'player_id','')::bigint,NULLIF(raw->>'playerId','')::bigint)>0
        ON CONFLICT(player_id,queue_id) DO UPDATE SET player_name=EXCLUDED.player_name,rank=EXCLUDED.rank,
          ranked_points=EXCLUDED.ranked_points,wins=EXCLUDED.wins,losses=EXCLUDED.losses,updated_at=now()"#,
    )
    .await
}

async fn persist_esports(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO esports_leagues(league_id,league_name,league_description,league_image_url,league_start_date,league_end_date,updated_at)
        SELECT COALESCE(NULLIF(raw->>'league_id','')::int,NULLIF(raw->>'LeagueId','')::int),
          COALESCE(raw->>'league_name',raw->>'Name'),COALESCE(raw->>'league_description',raw->>'Description'),
          COALESCE(raw->>'league_image_url',raw->>'Image_URL'),
          NULLIF(COALESCE(raw->>'league_start_date',raw->>'StartDate'),'')::timestamptz,
          NULLIF(COALESCE(raw->>'league_end_date',raw->>'EndDate'),'')::timestamptz,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'league_id','')::int,NULLIF(raw->>'LeagueId','')::int)>0
        ON CONFLICT(league_id) DO UPDATE SET league_name=EXCLUDED.league_name,
          league_description=EXCLUDED.league_description,league_image_url=EXCLUDED.league_image_url,
          league_start_date=EXCLUDED.league_start_date,league_end_date=EXCLUDED.league_end_date,updated_at=now()"#,
    )
    .await
}

async fn persist_bounty_items(database: &Database, row: &ClaimedRow) -> Result<(), RawBufferError> {
    run_json_upsert(
        database,
        row,
        r#"WITH rows AS(SELECT value raw FROM jsonb_array_elements($1::text::jsonb))
        INSERT INTO bounty_items(item_id,item_name,champion_name,price,active,raw_data,updated_at)
        SELECT COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'bounty_item_id','')::int),
          COALESCE(raw->>'item_name',raw->>'Name'),COALESCE(raw->>'champion_name',raw->>'ChampionName'),
          COALESCE(NULLIF(raw->>'price','')::int,0),COALESCE(NULLIF(raw->>'active','')::boolean,true),raw,now()
        FROM rows WHERE COALESCE(NULLIF(raw->>'item_id','')::int,NULLIF(raw->>'bounty_item_id','')::int)>0
        ON CONFLICT(item_id) DO UPDATE SET item_name=EXCLUDED.item_name,champion_name=EXCLUDED.champion_name,
          price=EXCLUDED.price,active=EXCLUDED.active,raw_data=EXCLUDED.raw_data,updated_at=now()"#,
    )
    .await
}

async fn run_json_upsert(
    database: &Database,
    row: &ClaimedRow,
    sql: &str,
) -> Result<(), RawBufferError> {
    let raw = clean_json_text(&row.raw_data)?;
    database.query_json(sql, &[&raw, &row.entity_id]).await?;
    Ok(())
}

fn clean_json_text(value: &Value) -> Result<String, RawBufferError> {
    serde_json::to_string(value)
        .map(|value| value.replace('\0', ""))
        .map_err(|error| RawBufferError::Invalid {
            row_id: 0,
            message: error.to_string(),
        })
}

async fn mark_processed(database: &Database, row_id: i64) -> Result<(), DatabaseError> {
    database
        .query_json(
            "UPDATE raw_ingest_buffer SET status='processed',processed_at=now(),error_message=NULL WHERE id=$1",
            &[&row_id],
        )
        .await?;
    Ok(())
}

async fn mark_failed_or_deferred(
    database: &Database,
    row_id: i64,
    retry: i32,
    terminal: bool,
    facts_durable: bool,
    message: &str,
) -> Result<(), DatabaseError> {
    let retry_delay = 2_i32
        .pow(u32::try_from(retry.min(8)).unwrap_or_default())
        .min(300);
    database
        .query_json(
            "UPDATE raw_ingest_buffer SET status=CASE WHEN $3 THEN 'failed' ELSE 'pending' END,\
               retry_count=$2,error_message=left($4,4000),processed_at=CASE WHEN $3 THEN now() ELSE NULL END,\
               available_at=CASE WHEN $3 THEN available_at WHEN $5 THEN now()+($6::INT*INTERVAL '1 second') ELSE available_at END \
             WHERE id=$1",
            &[&row_id, &retry, &terminal, &message, &facts_durable, &retry_delay],
        )
        .await?;
    Ok(())
}

fn match_payload_requires_recovery(raw: &Value) -> bool {
    raw.as_array().is_some_and(|rows| {
        rows.len() < 10
            || rows
                .iter()
                .any(|row| !text(row, "ret_msg").trim().is_empty())
            || rows
                .iter()
                .any(|row| text(row, "source").eq_ignore_ascii_case("recovered"))
    })
}

async fn match_facts_are_durable(
    database: &Database,
    entity_id: &str,
) -> Result<bool, DatabaseError> {
    if entity_id.parse::<i64>().is_err() {
        return Ok(false);
    }
    Ok(database.one_json(
        "SELECT completed_stages@>ARRAY['player_facts','match_bans']::TEXT[] facts_durable FROM match_ingest_status WHERE match_id=$1::TEXT::BIGINT",
        &[&entity_id],
    ).await?.and_then(|row| row.get("facts_durable").and_then(Value::as_bool)).unwrap_or(false))
}

async fn has_pending_match_facts(database: &Database) -> Result<bool, DatabaseError> {
    Ok(database.one_json(
        "SELECT EXISTS(SELECT 1 FROM raw_ingest_buffer rib LEFT JOIN match_ingest_status mis ON mis.match_id=CASE WHEN rib.entity_id~'^[0-9]+$' THEN rib.entity_id::BIGINT ELSE NULL END WHERE rib.entity_type='match' AND rib.status='pending' AND NOT COALESCE(mis.completed_stages@>ARRAY['player_facts','match_bans']::TEXT[],FALSE)) has_match_facts",
        &[],
    ).await?.and_then(|row| row.get("has_match_facts").and_then(Value::as_bool)).unwrap_or(false))
}

async fn return_claims_to_pending(
    database: &Database,
    rows: &[ClaimedRow],
) -> Result<(), DatabaseError> {
    let ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
    if !ids.is_empty() {
        database.query_json(
            "UPDATE raw_ingest_buffer SET status='pending',processed_at=NULL WHERE id=ANY($1::BIGINT[]) AND status='processing'",
            &[&ids],
        ).await?;
    }
    Ok(())
}

async fn requeue_recent_quota_paused_rows(database: &Database) -> Result<(), DatabaseError> {
    database.query_json(
        "UPDATE raw_ingest_buffer rib SET status='pending',retry_count=0,error_message='quota pause: recovery-required match retained pending',processed_at=NULL WHERE rib.status='failed' AND rib.entity_type='match' AND rib.endpoint='getmatchdetailsbatch' AND rib.created_at>=now()-INTERVAL '6 hours' AND jsonb_typeof(rib.raw_data)='array' AND (EXISTS(SELECT 1 FROM jsonb_array_elements(rib.raw_data) player WHERE btrim(COALESCE(player->>'ret_msg',''))<>'') OR (SELECT count(*) FROM jsonb_array_elements(rib.raw_data) player WHERE btrim(COALESCE(player->>'ret_msg',''))='')<10) AND EXISTS(SELECT 1 FROM hourly_ingest_match_debt debt WHERE debt.match_id::TEXT=rib.entity_id AND debt.status IN('pending','staged'))",
        &[],
    ).await?;
    Ok(())
}

fn integer(value: &Value, key: &str) -> Option<i64> {
    value
        .get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

fn text(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use paladinscat_core::config::BackendConfig;
    use serde_json::json;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn recovery_gate_matches_typescript_payload_shapes() {
        let full = Value::Array((0..10).map(|id| json!({"player_id":id + 1})).collect());
        assert!(!match_payload_requires_recovery(&full));
        assert!(match_payload_requires_recovery(&json!([{"player_id":1}])));
        assert!(match_payload_requires_recovery(&json!([
            {"player_id":1,"ret_msg":"broken skin"}
        ])));
        let recovered = Value::Array(
            (0..10)
                .map(|id| json!({"player_id":id + 1,"source":"recovered"}))
                .collect(),
        );
        assert!(match_payload_requires_recovery(&recovered));
    }

    #[test]
    fn facts_lane_default_matches_typescript() {
        assert_eq!(DEFAULT_FACT_CONCURRENCY, 8);
        assert_eq!(MAX_RETRIES, 3);
    }

    #[test]
    fn performance_summary_refresh_is_immediate_then_throttled_for_five_minutes() {
        LAST_PERFORMANCE_STATS_REFRESH_AT.store(0, Ordering::Relaxed);
        assert!(performance_stats_refresh_due());
        assert!(!performance_stats_refresh_due());
        let now = LAST_PERFORMANCE_STATS_REFRESH_AT.load(Ordering::Relaxed);
        LAST_PERFORMANCE_STATS_REFRESH_AT.store(
            now - PERFORMANCE_STATS_REFRESH_MIN_SECONDS,
            Ordering::Relaxed,
        );
        assert!(performance_stats_refresh_due());
    }

    #[test]
    fn post_batch_order_is_quiesce_then_retention_then_histogram_summary() {
        let source = include_str!("raw_buffer.rs");
        let body = source
            .split_once("async fn process_raw_buffer_batch_inner")
            .unwrap()
            .1
            .split_once("fn performance_stats_refresh_due")
            .unwrap()
            .0;
        let final_quiesce = body.rfind("should_stop.is_some_and").unwrap();
        let retention = body.find("cleanup_raw_ingest_buffer_retention").unwrap();
        let summary = body.find("refresh_performance_metric_stats").unwrap();
        assert!(final_quiesce < retention && retention < summary);
    }

    #[test]
    fn relay_cleanup_is_awaited_before_stale_recovery_and_claim() {
        let source = include_str!("raw_buffer.rs");
        let body = source
            .split_once("async fn process_raw_buffer_batch_inner")
            .expect("batch implementation")
            .1
            .split_once("fn merge_batch_result")
            .expect("batch implementation end")
            .0;
        let headroom = body.find("api_headroom_snapshot").expect("headroom");
        let cleanup = body
            .find("cleanupFetchedPlayersCache")
            .expect("relay cleanup");
        let stale = body.find("recover_stale_leases").expect("stale recovery");
        let claim = body.find("claim_rows").expect("claim");
        assert!(headroom < cleanup && cleanup < stale && stale < claim);
        assert!(body.contains("let Err(error)"));
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with raw_ingest_buffer"]
    async fn active_lease_renewal_wins_concurrent_stale_recovery() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "raw-buffer-lease-race-test").expect("database");
        let entity_id = format!("lease-race-{}", uuid::Uuid::new_v4());
        let row_id = database
            .one_json(
                "INSERT INTO raw_ingest_buffer(raw_data,status,entity_type,entity_id,processed_at) VALUES('{}'::JSONB,'processing','player',$1,now()-INTERVAL '1 hour') RETURNING id",
                &[&entity_id],
            )
            .await
            .expect("insert lease row")
            .and_then(|row| integer(&row, "id"))
            .expect("row id");

        let mut renewal_client = database.connection().await.expect("renewal connection");
        let renewal = renewal_client
            .transaction()
            .await
            .expect("renewal transaction");
        renewal
            .execute(
                "UPDATE raw_ingest_buffer SET processed_at=now() WHERE id=$1 AND status='processing'",
                &[&row_id],
            )
            .await
            .expect("renew active lease");

        let recovery_database = database.clone();
        let mut recovery = tokio::spawn(async move {
            recover_stale_leases_for(&recovery_database, Some(row_id)).await
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), &mut recovery)
                .await
                .is_err(),
            "stale recovery must wait for the in-flight renewal row lock"
        );
        renewal.commit().await.expect("commit renewal");
        recovery
            .await
            .expect("recovery task")
            .expect("stale recovery");

        let state = database
            .one_json(
                "SELECT status,retry_count,processed_at IS NOT NULL renewed FROM raw_ingest_buffer WHERE id=$1",
                &[&row_id],
            )
            .await
            .expect("lease state")
            .expect("lease row");
        assert_eq!(text(&state, "status"), "processing");
        assert_eq!(integer(&state, "retry_count"), Some(0));
        assert_eq!(state.get("renewed").and_then(Value::as_bool), Some(true));
        database
            .query_json("DELETE FROM raw_ingest_buffer WHERE id=$1", &[&row_id])
            .await
            .expect("cleanup lease row");
    }

    #[tokio::test]
    async fn quiesce_stops_before_claiming_database_rows() {
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some("postgres://127.0.0.1:9/paladinscat".to_owned()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "raw-buffer-quiesce-test").expect("database");
        let stop = AtomicBool::new(true);
        let result = process_raw_buffer_batch_until(&database, 50, Some(&stop))
            .await
            .expect("quiesced batch");
        assert_eq!(result.processed, 0);
        assert_eq!(result.failed, 0);
        assert_eq!(result.deferred, 0);
    }
}
