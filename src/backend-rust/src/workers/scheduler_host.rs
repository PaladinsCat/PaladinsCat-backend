use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    net::IpAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::future::join_all;
use paladinscat_core::{
    cache::RedisCache,
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde_json::{Value, json};
use time::{Month, OffsetDateTime, Weekday, format_description::well_known::Rfc3339};
use url::{Host, Url};

use super::{
    coordination::WorkerCoordinationRepository,
    discovery_control::reopenable_hour_predicate,
    history_retention::cleanup_player_history_retention,
    live_tracker::detect_dropped_matches,
    maintenance::{
        BufferBatchResult, cleanup_raw_ingest_buffer_retention, process_buffer_batch_until,
        refresh_baselines_with_job, refresh_derived_projections_with_job,
    },
    match_lifecycle::TERMINAL_NO_COMPLETED_MATCH_REASON,
    pipeline::CanonicalIngestPipeline,
    policy::{ApiHeadroomSnapshot, MATCH_COUNT_QUEUE_DEFINITIONS, api_headroom_snapshot},
    profile_enrichment::{ProfileEnrichmentRepository, ProfileEnrichmentResult},
    projections,
    ranked_tracker::RankedTracker,
    relay::WorkerRelayClient,
    scheduler::{ScheduledJob, StartupPolicy, scheduled_jobs_for},
    scheduler_runtime::SchedulerRuntimeExit,
    tier_stats::TierStatsRepository,
};

const OWNERSHIP_LEASE: Duration = Duration::from_secs(60);
const STARTUP_OWNERSHIP_LEASE: Duration = Duration::from_secs(5 * 60);
const OWNERSHIP_HEARTBEAT: Duration = Duration::from_secs(15);
const OWNERSHIP_HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(15);
const JOB_LEASE: Duration = Duration::from_secs(60 * 60);
const GAP_CHECKER_MIN_DATE: &str = "2026-05-31";

#[derive(Clone)]
struct SchedulerServices {
    database: Database,
    config: Arc<BackendConfig>,
    should_stop: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct BufferDrainTotals {
    processed: i32,
    failed: i32,
    deferred: i32,
    batches: usize,
}

async fn drain_buffer_continuously<E, F, Fut>(
    should_stop: &AtomicBool,
    max_batches: Option<usize>,
    mut next_batch: F,
) -> Result<BufferDrainTotals, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<BufferBatchResult, E>>,
{
    let mut totals = BufferDrainTotals::default();
    loop {
        if should_stop.load(Ordering::Relaxed) {
            break;
        }
        if max_batches.is_some_and(|max| totals.batches >= max) {
            break;
        }
        let result = next_batch().await?;
        if result.processed + result.failed + result.deferred == 0 {
            break;
        }
        totals.processed = totals.processed.saturating_add(result.processed);
        totals.failed = totals.failed.saturating_add(result.failed);
        totals.deferred = totals.deferred.saturating_add(result.deferred);
        totals.batches += 1;
        if should_stop.load(Ordering::Relaxed) {
            break;
        }
    }
    Ok(totals)
}

fn startup_delay_seconds(job: ScheduledJob) -> Option<u64> {
    match job.startup {
        StartupPolicy::None => None,
        StartupPolicy::DurableCatchup { delay_seconds }
        | StartupPolicy::Always { delay_seconds } => Some(delay_seconds),
    }
}

fn due_inactive_jobs(
    schedules: &[ScheduledJob],
    now: OffsetDateTime,
    active_job_keys: &BTreeSet<&'static str>,
) -> Vec<ScheduledJob> {
    schedules
        .iter()
        .filter(|job| scheduler_job_is_due(**job, now) && !active_job_keys.contains(job.job_key))
        .copied()
        .collect()
}

fn scheduler_job_is_due(job: ScheduledJob, now: OffsetDateTime) -> bool {
    scheduler_job_is_due_with(job, now, |name| std::env::var(name).ok())
}

fn scheduler_job_is_due_with(
    job: ScheduledJob,
    now: OffsetDateTime,
    lookup: impl Fn(&str) -> Option<String>,
) -> bool {
    let expression = match job.job_key {
        "hourly-gap-checker:scan" => lookup("GAP_CHECKER_CRON_EXPRESSION"),
        "auto-ingester:profile-enrichment" => lookup("PLAYER_ACTIVITY_PROFILE_CRON"),
        _ => None,
    };
    cron_matches(expression.as_deref().unwrap_or(job.cron_expression), now)
}

fn cron_matches(expression: &str, now: OffsetDateTime) -> bool {
    let fields = expression.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return false;
    }
    let weekday = match now.weekday() {
        Weekday::Sunday => 0,
        Weekday::Monday => 1,
        Weekday::Tuesday => 2,
        Weekday::Wednesday => 3,
        Weekday::Thursday => 4,
        Weekday::Friday => 5,
        Weekday::Saturday => 6,
    };
    cron_field_matches(fields[0], now.minute(), 0, 59)
        && cron_field_matches(fields[1], now.hour(), 0, 23)
        && cron_field_matches(fields[2], now.day(), 1, 31)
        && cron_field_matches(fields[3], now.month() as u8, 1, 12)
        && (cron_field_matches(fields[4], weekday, 0, 7)
            || (weekday == 0 && cron_field_matches(fields[4], 7, 0, 7)))
}

fn cron_field_matches(field: &str, value: u8, min: u8, max: u8) -> bool {
    field.split(',').any(|term| {
        let (base, step) = term.split_once('/').map_or((term, 1), |(base, step)| {
            (base, step.parse::<u8>().unwrap_or_default())
        });
        if step == 0 {
            return false;
        }
        let (start, end) = if base == "*" {
            (min, max)
        } else if let Some((start, end)) = base.split_once('-') {
            let (Ok(start), Ok(end)) = (start.parse::<u8>(), end.parse::<u8>()) else {
                return false;
            };
            (start, end)
        } else {
            let Ok(start) = base.parse::<u8>() else {
                return false;
            };
            (start, if term.contains('/') { max } else { start })
        };
        start >= min
            && end <= max
            && start <= end
            && value >= start
            && value <= end
            && (value - start).is_multiple_of(step)
    })
}

/// Cap on buffer batches drained per scheduler invocation. Bounding the drain
/// (rather than draining until the inbox is empty) prevents a large or
/// permanently non-empty raw inbox from pinning the scheduler job slot open
/// for hours, which stalled discovery by keeping its post-discovery inline
/// drain alive forever.
fn bounded_buffer_drain_max_batches(value: Option<usize>) -> usize {
    // Projection debt is durable in raw_ingest_buffer.  On the production
    // 1-vCPU database, allowing a single scheduler invocation to drain five
    // or more 50-row batches keeps expensive ranked projections continuously
    // CPU-bound.  Process one bounded batch and resume on the next schedule.
    value.unwrap_or(1).clamp(1, 1)
}

fn buffer_drain_max_batches_per_run() -> usize {
    // Prefer the new operator knob; fall back to the legacy
    // RUST_BUFFER_DRAIN_MAX_BATCHES used by prior deployment configs, then the
    // built-in one-batch production budget.
    bounded_buffer_drain_max_batches(
        std::env::var("BUFFER_DRAIN_MAX_BATCHES_PER_RUN")
            .ok()
            .and_then(|value| value.parse().ok())
            .or_else(|| {
                std::env::var("RUST_BUFFER_DRAIN_MAX_BATCHES")
                    .ok()
                    .and_then(|value| value.parse().ok())
            }),
    )
}

fn buffer_drain_batch_size() -> usize {
    std::env::var("BUFFER_DRAIN_BATCH_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8)
        .clamp(1, 8)
}

fn auto_ingester_presence_source(trigger: &str) -> &'static str {
    match trigger {
        "cron" => "auto-ingester-cron",
        "startup" => "auto-ingester-startup",
        // Capture runs retain `capture-once` in worker_job_run_log as proof of
        // the guarded entrypoint, but their worker-visible source must match
        // the production cron path for a behavioral parity comparison.
        "capture-once" => "auto-ingester-cron",
        _ => "auto-ingester",
    }
}

/// Purpose: return the full governed recovery boundary, never a rolling window.
/// Input: none. Output: the earliest supported UTC date as an owned string.
fn configured_gap_min_date() -> String {
    GAP_CHECKER_MIN_DATE.to_owned()
}

fn url_is_loopback(value: &str) -> bool {
    Url::parse(value)
        .ok()
        .and_then(|url| url.host().map(|host| host.to_owned()))
        .is_some_and(|host| match host {
            Host::Domain(domain) => {
                domain.eq_ignore_ascii_case("localhost")
                    || domain
                        .parse::<IpAddr>()
                        .is_ok_and(|address| address.is_loopback())
            }
            Host::Ipv4(address) => address.is_loopback(),
            Host::Ipv6(address) => address.is_loopback(),
        })
}

async fn scheduler_capture_fixture_allowed(database: &Database, config: &BackendConfig) -> bool {
    let Some(marker) = std::env::var("PALADINSCAT_SCHEDULER_CAPTURE_MARKER")
        .ok()
        .filter(|value| !value.trim().is_empty())
    else {
        return false;
    };
    if !url_is_loopback(&config.database_url) || !url_is_loopback(&config.hirez_relay_url) {
        return false;
    }
    database
        .one_json(
            "SELECT EXISTS(SELECT 1 FROM scheduler_capture_marker WHERE marker=$1) allowed",
            &[&marker],
        )
        .await
        .ok()
        .flatten()
        .and_then(|row| row.get("allowed").and_then(Value::as_bool))
        .unwrap_or(false)
}

pub async fn run_scheduler_domain(
    database: Database,
    config: Arc<BackendConfig>,
    scheduler_key: String,
    owner_id: String,
) -> Result<SchedulerRuntimeExit, DatabaseError> {
    let coordination = WorkerCoordinationRepository::new(database.clone());
    if !coordination
        .acquire_scheduler_owner(&scheduler_key, &owner_id, STARTUP_OWNERSHIP_LEASE)
        .await?
    {
        return Ok(SchedulerRuntimeExit::OwnershipUnavailable);
    }
    let services = SchedulerServices {
        database,
        config,
        should_stop: Arc::new(AtomicBool::new(false)),
    };
    let schedules = scheduled_jobs_for(&scheduler_key)
        .copied()
        .collect::<Vec<_>>();
    let startup_jobs = schedules
        .iter()
        .filter(|job| startup_delay_seconds(**job).is_some())
        .copied()
        .collect::<Vec<_>>();
    let mut startup_handles = Vec::with_capacity(startup_jobs.len());
    for job in startup_jobs {
        let delay = startup_delay_seconds(job).unwrap_or_default();
        let coordination = coordination.clone();
        let services = services.clone();
        let owner_id = owner_id.clone();
        let startup_stop = services.should_stop.clone();
        startup_handles.push(tokio::spawn(async move {
            if !wait_for_startup_delay(delay, startup_stop).await {
                return;
            }
            execute_scheduled_job(coordination, services, job, owner_id, "startup").await;
        }));
    }
    let mut clock = tokio::time::interval(Duration::from_secs(1));
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(OWNERSHIP_HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut last_started_minute = None;
    let mut active = BTreeMap::<&'static str, tokio::task::JoinHandle<()>>::new();
    let mut shutdown = Box::pin(shutdown_signal());
    let exit = loop {
        tokio::select! {
            _ = &mut shutdown => break SchedulerRuntimeExit::Shutdown,
            _ = heartbeat.tick() => {
                let retained = match wait_with_timeout(
                    OWNERSHIP_HEARTBEAT_TIMEOUT,
                    coordination.heartbeat_scheduler_owner(
                        &scheduler_key,
                        &owner_id,
                        OWNERSHIP_LEASE,
                    ),
                ).await {
                    Some(Ok(true)) => true,
                    Some(Ok(false)) => {
                        tracing::error!(scheduler=%scheduler_key, "scheduler ownership lease was lost");
                        false
                    }
                    Some(Err(error)) => {
                        tracing::error!(scheduler=%scheduler_key,error=?error,"scheduler ownership heartbeat failed");
                        false
                    }
                    None => {
                        tracing::error!(scheduler=%scheduler_key,"scheduler ownership heartbeat timed out");
                        false
                    }
                };
                if !retained {
                    break SchedulerRuntimeExit::OwnershipLost;
                }
            }
            _ = clock.tick() => {
                let finished = active.iter()
                    .filter(|(_, handle)| handle.is_finished())
                    .map(|(job_key, _)| *job_key)
                    .collect::<Vec<_>>();
                for job_key in finished {
                    if let Some(done) = active.remove(job_key) {
                        let _ = done.await;
                    }
                }
                let now = OffsetDateTime::now_utc();
                let slot = now.unix_timestamp()/60;
                if last_started_minute != Some(slot) {
                    last_started_minute = Some(slot);
                    let active_job_keys = active.keys().copied().collect::<BTreeSet<_>>();
                    for job in due_inactive_jobs(&schedules, now, &active_job_keys) {
                        active.insert(job.job_key, tokio::spawn(execute_scheduled_job(
                            coordination.clone(), services.clone(), job, owner_id.clone(), "cron",
                        )));
                    }
                }
            }
        }
    };
    services.should_stop.store(true, Ordering::Relaxed);
    if exit == SchedulerRuntimeExit::OwnershipLost {
        abort_scheduler_work(&startup_handles, &active);
    }
    for handle in startup_handles {
        let _ = handle.await;
    }
    for (_, active) in active {
        let _ = active.await;
    }
    let _ = coordination
        .release_scheduler_owner(&scheduler_key, &owner_id)
        .await;
    Ok(exit)
}

async fn wait_with_timeout<F, T>(duration: Duration, future: F) -> Option<T>
where
    F: Future<Output = T>,
{
    tokio::time::timeout(duration, future).await.ok()
}

fn abort_scheduler_work(
    startup_handles: &[tokio::task::JoinHandle<()>],
    active: &BTreeMap<&'static str, tokio::task::JoinHandle<()>>,
) {
    for handle in startup_handles {
        handle.abort();
    }
    for handle in active.values() {
        handle.abort();
    }
}

async fn wait_for_startup_delay(delay_seconds: u64, should_stop: Arc<AtomicBool>) -> bool {
    if delay_seconds == 0 {
        return !should_stop.load(Ordering::Relaxed);
    }
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(delay_seconds)) => true,
        _ = wait_for_stop(should_stop) => false,
    }
}

async fn wait_for_stop(should_stop: Arc<AtomicBool>) {
    while !should_stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Explicit one-shot entrypoint for the local scheduler parity capture.
/// It retains the production ownership, job lease, run ledger, dispatch,
/// finish, and release sequence without starting the scheduler clock loop.
pub async fn run_scheduler_job_once(
    database: Database,
    config: Arc<BackendConfig>,
    scheduler_key: String,
    job_key: String,
    owner_id: String,
) -> Result<bool, DatabaseError> {
    if !scheduler_capture_fixture_allowed(&database, &config).await {
        return Ok(false);
    }
    let coordination = WorkerCoordinationRepository::new(database.clone());
    if !coordination
        .acquire_scheduler_owner(&scheduler_key, &owner_id, STARTUP_OWNERSHIP_LEASE)
        .await?
    {
        return Ok(false);
    }
    let Some(job) = scheduled_jobs_for(&scheduler_key)
        .find(|candidate| candidate.job_key == job_key)
        .copied()
    else {
        let _ = coordination
            .release_scheduler_owner(&scheduler_key, &owner_id)
            .await;
        return Ok(false);
    };
    let services = SchedulerServices {
        database,
        config,
        should_stop: Arc::new(AtomicBool::new(false)),
    };
    let executed = execute_scheduled_sequence(job, "capture-once", |step, step_trigger| {
        execute_scheduled_job_once(
            coordination.clone(),
            services.clone(),
            step,
            owner_id.clone(),
            step_trigger,
        )
    })
    .await;
    let _ = coordination
        .release_scheduler_owner(&scheduler_key, &owner_id)
        .await;
    Ok(executed)
}

async fn execute_scheduled_job(
    coordination: WorkerCoordinationRepository,
    services: SchedulerServices,
    job: ScheduledJob,
    owner_id: String,
    trigger: &'static str,
) {
    execute_scheduled_sequence(job, trigger, |step, step_trigger| {
        execute_scheduled_job_once(
            coordination.clone(),
            services.clone(),
            step,
            owner_id.clone(),
            step_trigger,
        )
    })
    .await;
}

async fn execute_scheduled_sequence<F, Fut>(
    job: ScheduledJob,
    trigger: &'static str,
    mut execute: F,
) -> bool
where
    F: FnMut(ScheduledJob, &'static str) -> Fut,
    Fut: Future<Output = bool>,
{
    let executed = execute(job, trigger).await;
    if executed
        && job.job_key == "auto-ingester:discovery"
        && let Some(buffer_drain) = scheduled_jobs_for(job.scheduler_key)
            .find(|candidate| candidate.job_key == "auto-ingester:buffer-drain")
            .copied()
    {
        let _ = execute(buffer_drain, "post-discovery").await;
    }
    executed
}

async fn execute_scheduled_job_once(
    coordination: WorkerCoordinationRepository,
    services: SchedulerServices,
    job: ScheduledJob,
    owner_id: String,
    trigger: &'static str,
) -> bool {
    let lease = match coordination
        .acquire_job(job.job_key, job.scheduler_key, &owner_id, JOB_LEASE)
        .await
    {
        Ok(Some(lease)) => lease,
        _ => return false,
    };
    let run_id = match coordination.start_run(&lease, trigger).await {
        Ok(id) => id,
        Err(_) => {
            let _ = coordination.release_job(&lease).await;
            return false;
        }
    };
    let execution = dispatch(&services, job.job_key, trigger).await;
    let (status, result, error) = match execution {
        Ok(value) => ("completed", Some(value), None),
        Err(error) => {
            tracing::error!(job=job.job_key,error=?error,"scheduled Rust job failed");
            ("failed", None, Some(error))
        }
    };
    let _ = coordination
        .finish_run(run_id, status, result.as_ref(), error.as_deref())
        .await;
    let _ = coordination.release_job(&lease).await;
    true
}

async fn dispatch(
    services: &SchedulerServices,
    job_key: &str,
    trigger: &'static str,
) -> Result<Value, String> {
    match job_key {
        "ranked-tracker:leaderboard" => {
            let worker = RankedTracker::new(services.database.clone(), &services.config)
                .map_err(|error| error.to_string())?;
            serde_json::to_value(worker.track().await.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())
        }
        "auto-ingester:discovery" => {
            let now = scheduler_dispatch_now(trigger)?;
            let (date, hour) = completed_discovery_window(now);
            let worker = CanonicalIngestPipeline::new(services.database.clone(), &services.config)
                .map_err(|error| error.to_string())?;
            let discovery_source = auto_ingester_presence_source(trigger);
            // Every configured queue enters the same collection method and
            // shared queue-hour drain; there is no ranked/casual split.
            let queues = worker
                .discover_configured_queues(&date, hour, discovery_source)
                .await;
            let completed = queues.iter().filter(|result| result.is_ok()).count();
            let failed = queues.len() - completed;
            Ok(json!({
                "date":date,
                "hour":hour,
                "queues":queues.len(),
                "completed":completed,
                "failed":failed
            }))
        }
        "auto-ingester:profile-enrichment" => {
            let repository = ProfileEnrichmentRepository::new(services.database.clone());
            if trigger == "startup" {
                let refreshed = repository
                    .replay_audited_profiles()
                    .await
                    .map_err(|error| error.to_string())?;
                return Ok(profile_enrichment_result_json(ProfileEnrichmentResult {
                    refreshed,
                    ..Default::default()
                }));
            }
            let max_calls = profile_enrichment_allowed_calls(
                profile_enrichment_max_calls(),
                api_headroom_snapshot(&services.database, api_key_reserve_calls())
                    .await
                    .map_err(|error| error.to_string())?,
            );
            if max_calls == 0 {
                return Ok(profile_enrichment_result_json(
                    ProfileEnrichmentResult::default(),
                ));
            }
            let enrichment = repository
                .run(&services.config, max_calls, "cron")
                .await
                .map_err(|error| error.to_string())?;
            Ok(profile_enrichment_result_json(enrichment))
        }
        "auto-ingester:buffer-drain" => {
            let relay =
                WorkerRelayClient::new(&services.config).map_err(|error| error.to_string())?;
            let totals = drain_buffer_continuously(
                &services.should_stop,
                Some(buffer_drain_max_batches_per_run()),
                || {
                    process_buffer_batch_until(
                        &services.database,
                        Some(&relay),
                        buffer_drain_batch_size(),
                        &services.should_stop,
                    )
                },
            )
            .await
            .map_err(|error| error.to_string())?;
            Ok(json!({
                "processed":totals.processed,
                "failed":totals.failed,
                "deferred":totals.deferred,
                "batches":totals.batches,
            }))
        }
        "auto-ingester:raw-buffer-retention" => {
            let result = cleanup_raw_ingest_buffer_retention(&services.database, "scheduler")
                .await
                .map_err(|error| error.to_string())?;
            serde_json::to_value(result).map_err(|error| error.to_string())
        }
        "auto-ingester:player-history-retention" => {
            let result = cleanup_player_history_retention(&services.database, "scheduler")
                .await
                .map_err(|error| error.to_string())?;
            serde_json::to_value(result).map_err(|error| error.to_string())
        }
        "auto-ingester:materialized-view-refresh" => {
            for view in ["mv_player_coplay_stats"] {
                let exists = services
                    .database
                    .one_json("SELECT to_regclass($1) IS NOT NULL AS exists", &[&view])
                    .await
                    .map_err(|error| error.to_string())?
                    .and_then(|row| row.get("exists").and_then(Value::as_bool))
                    .unwrap_or(false);
                if exists {
                    services
                        .database
                        .query_json(&format!("REFRESH MATERIALIZED VIEW {view}"), &[])
                        .await
                        .map_err(|error| error.to_string())?;
                }
            }
            let performance_metric_stats =
                super::projections::refresh_performance_metric_stats(&services.database)
                    .await
                    .map_err(|error| error.to_string())?;
            Ok(json!({
                "refreshed":true,
                "performanceMetricStats":performance_metric_stats,
            }))
        }
        "auto-ingester:drop-detection" => {
            let result = detect_dropped_matches(&services.database)
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!(result))
        }
        "auto-ingester:view-count-drain" => {
            let result = drain_view_counts(&services.database, &services.config)
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!(result))
        }
        "baseline-tracker:refresh" => {
            let result = refresh_baselines_with_job(&services.database, "scheduler")
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"jobId":result.job_id,"baselineRows":result.rows}))
        }
        "derived-projections:refresh" => {
            let result = refresh_derived_projections_with_job(&services.database, "scheduler")
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"jobId":result.job_id,"counts":result.counts}))
        }
        "ranked-projection:repair" => {
            let page = std::env::var("PROJECTION_REPAIR_PAGE_SIZE")
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap_or(250);
            let projected = projections::repair_ranked_projection_gaps(&services.database, page)
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({"projected": projected, "pageSize": page}))
        }
        "hourly-gap-checker:scan" => run_gap_check(services).await,
        "tier-stats:refresh" => serde_json::to_value(
            TierStatsRepository::new(services.database.clone())
                .refresh()
                .await,
        )
        .map_err(|error| error.to_string()),
        _ => Err(format!("unknown scheduled job {job_key}")),
    }
}

async fn drain_view_counts(database: &Database, config: &BackendConfig) -> Result<Value, String> {
    let redis = RedisCache::new(&config.redis_url).map_err(|error| error.to_string())?;
    let mut posts = 0_u64;
    let mut builds = 0_u64;
    if let Some(keys) = redis.scan_keys("viewcount:posts:*").await {
        for key in keys {
            let Some(id) = key
                .strip_prefix("viewcount:posts:")
                .and_then(|s| s.parse::<i64>().ok())
            else {
                continue;
            };
            let Some(count) = redis.incr_get(&key).await else {
                continue;
            };
            if count > 0 {
                // view_count is an INT4 column; bind the increment as i32 to satisfy
                // tokio-postgres type-safety (an i64 would raise WrongType Int4/i64).
                let incr = i32::try_from(count).unwrap_or(i32::MAX);
                let _ = database
                    .query_json(
                        "UPDATE posts SET view_count=view_count+$2 WHERE id=$1::BIGINT",
                        &[&id, &incr],
                    )
                    .await;
                posts = posts.saturating_add(count as u64);
            }
            let _ = redis.del(&key).await;
        }
    }
    if let Some(keys) = redis.scan_keys("viewcount:builds:*").await {
        for key in keys {
            let Some(id) = key
                .strip_prefix("viewcount:builds:")
                .and_then(|s| s.parse::<i64>().ok())
            else {
                continue;
            };
            let Some(count) = redis.incr_get(&key).await else {
                continue;
            };
            if count > 0 {
                // view_count is an INT4 column; bind the increment as i32 to satisfy
                // tokio-postgres type-safety (an i64 would raise WrongType Int4/i64).
                let incr = i32::try_from(count).unwrap_or(i32::MAX);
                let _ = database
                    .query_json(
                        "UPDATE builds SET view_count=view_count+$2 WHERE id=$1::BIGINT",
                        &[&id, &incr],
                    )
                    .await;
                builds = builds.saturating_add(count as u64);
            }
            let _ = redis.del(&key).await;
        }
    }
    Ok(json!({ "postsIncremented": posts, "buildsIncremented": builds }))
}

/// Purpose: discover every missing hour and drain every locally known ID.
/// Input: shared scheduler services. Output: JSON counts for all attempted
/// queue-hours; ranked/casual candidates share one newest-first ordering.
async fn run_gap_check(services: &SchedulerServices) -> Result<Value, String> {
    let now = OffsetDateTime::now_utc();
    let min_date = configured_gap_min_date();
    let queue_ids = MATCH_COUNT_QUEUE_DEFINITIONS
        .iter()
        .filter(|queue| queue.track_presence)
        .map(|queue| queue.queue_id)
        .collect::<Vec<_>>();
    let mut candidates = queue_hour_gap_candidates(&services.database, now, &min_date, &queue_ids)
        .await
        .map_err(|error| error.to_string())?;
    // Hi-Rez history exposes only the newest 50 matches. Every queue uses one
    // newest-first candidate set; no population owns a separate recovery lane.
    candidates.sort_by(|left, right| {
        right
            .date
            .cmp(&left.date)
            .then_with(|| right.hour.cmp(&left.hour))
            .then_with(|| left.queue_id.cmp(&right.queue_id))
    });
    if candidates.is_empty() {
        return Ok(json!({"candidates":0,"attempted":0,"completed":0}));
    }
    // Drain newest hour first, with every configured queue in that hour
    // executing together. The hour boundary is the recovery priority; there
    // is no arbitrary concurrency cap or population-specific lane.
    let mut candidates_by_hour = BTreeMap::<(String, i32), Vec<GapCandidate>>::new();
    for candidate in candidates.iter().cloned() {
        candidates_by_hour
            .entry((candidate.date.clone(), candidate.hour))
            .or_default()
            .push(candidate);
    }
    let pipeline = CanonicalIngestPipeline::new(services.database.clone(), &services.config)
        .map_err(|error| error.to_string())?;
    let mut completed = 0;
    let mut infrastructure_retried = 0;
    for ((date, hour), hour_candidates) in candidates_by_hour.into_iter().rev() {
        let hour_queue_ids = hour_candidates
            .iter()
            .map(|candidate| candidate.queue_id)
            .collect::<Vec<_>>();
        let claim_versions_before =
            queue_hour_claim_versions(&services.database, &date, hour, &hour_queue_ids)
                .await
                .map_err(|error| error.to_string())?;
        let outcomes = join_all(hour_candidates.iter().map(|candidate| {
            pipeline.discover_hour(
                candidate.queue_id,
                &candidate.date,
                candidate.hour,
                "gap-checker",
            )
        }))
        .await;
        completed += outcomes.iter().filter(|result| result.is_ok()).count();
        // Purpose: distinguish a recorded pipeline failure from infrastructure
        // contention before the queue-hour claim. Input: this hour's candidates
        // and outcomes. Output: only unclaimed queues are retried; recorded
        // provider/fact failures never replay the same vendor work in this run.
        let failed_queue_ids = hour_candidates
            .iter()
            .zip(&outcomes)
            .filter(|(_, outcome)| outcome.is_err())
            .map(|(candidate, _)| candidate.queue_id)
            .collect::<Vec<_>>();
        let claim_versions_after =
            queue_hour_claim_versions(&services.database, &date, hour, &failed_queue_ids)
                .await
                .map_err(|error| error.to_string())?;
        for candidate in hour_candidates.iter().filter(|candidate| {
            failed_queue_ids.contains(&candidate.queue_id)
                && claim_versions_after
                    .get(&candidate.queue_id)
                    .copied()
                    .unwrap_or_default()
                    <= claim_versions_before
                        .get(&candidate.queue_id)
                        .copied()
                        .unwrap_or_default()
        }) {
            infrastructure_retried += 1;
            pipeline
                .discover_hour(
                    candidate.queue_id,
                    &candidate.date,
                    candidate.hour,
                    "gap-checker",
                )
                .await
                .map_err(|error| {
                    format!(
                        "queue-hour claim failed twice for {} {} {:02}: {error}",
                        candidate.queue_id, candidate.date, candidate.hour
                    )
                })?;
            completed += 1;
        }
    }
    Ok(
        json!({"candidates":candidates.len(),"attempted":candidates.len(),"completed":completed,"infrastructureRetried":infrastructure_retried}),
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GapCandidate {
    queue_id: i32,
    date: String,
    hour: i32,
}

impl GapCandidate {
    /// Purpose: represent one missing queue-hour. Input: configured queue ID,
    /// UTC date and hour. Output: typed candidate consumed by `discover_hour`.
    fn new(queue_id: i32, date: String, hour: i32) -> Self {
        Self {
            queue_id,
            date,
            hour,
        }
    }
}

/// Purpose: identify whether this pass advanced the durable queue-hour claim.
/// Input: UTC date/hour and configured queue IDs. Output: queue-to-attempt
/// versions; `run_gap_check` retries only unchanged claims and never mistakes
/// an older failed row for work recorded by the current scan.
async fn queue_hour_claim_versions(
    database: &Database,
    date: &str,
    hour: i32,
    queue_ids: &[i32],
) -> Result<BTreeMap<i32, i32>, DatabaseError> {
    if queue_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let queue_ids = queue_ids.to_vec();
    let rows = database
        .query_json(
            "SELECT queue_id,attempts FROM hourly_ingest_state \
             WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=ANY($3::INT[])",
            &[&date, &hour, &queue_ids],
        )
        .await?;
    Ok(rows
        .into_iter()
        .filter_map(|row| {
            let queue_id = row
                .get("queue_id")
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())?;
            let attempts = row
                .get("attempts")
                .and_then(Value::as_i64)
                .and_then(|value| i32::try_from(value).ok())?;
            Some((queue_id, attempts))
        })
        .collect())
}

fn queue_hour_states_sql() -> String {
    let reopenable = reopenable_hour_predicate("$4");
    format!(
        "SELECT state.date::TEXT,state.hour,state.queue_id,state.status, \
         state.lease_until<=now() lease_due, \
         CASE WHEN state.status='complete' THEN {reopenable} ELSE FALSE END reopenable \
         FROM hourly_ingest_state state WHERE state.queue_id=ANY($1::INT[]) \
         AND state.date>=$2::TEXT::DATE AND state.date<=$3::TEXT::DATE"
    )
}

/// Purpose: calculate all missing/unfinished queue-hours through one DB query
/// and one state predicate. Input: database, UTC clock, governed lower date,
/// configured queue IDs. Output: candidates with no population-specific lane.
/// Relationship: every result is consumed by `CanonicalIngestPipeline::discover_hour`.
async fn queue_hour_gap_candidates(
    database: &Database,
    now: OffsetDateTime,
    min_date: &str,
    queue_ids: &[i32],
) -> Result<Vec<GapCandidate>, DatabaseError> {
    let Some((max_date, _)) = expected_elapsed_discovery_hours(now, min_date)
        .last()
        .cloned()
    else {
        return Ok(Vec::new());
    };
    let states_sql = queue_hour_states_sql();
    let rows = database
        .query_json(
            &states_sql,
            &[
                &queue_ids,
                &min_date,
                &max_date,
                &TERMINAL_NO_COMPLETED_MATCH_REASON,
            ],
        )
        .await?;
    let mut states = BTreeMap::new();
    for row in rows {
        let Some((date, hour)) = row_key(&row) else {
            continue;
        };
        let Some(queue_id) = row
            .get("queue_id")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
        else {
            continue;
        };
        states.insert((queue_id, date, hour), row);
    }
    let mut candidates = BTreeMap::new();
    for queue_id in queue_ids {
        for (date, hour) in expected_elapsed_discovery_hours(now, min_date) {
            let key = (*queue_id, date.clone(), hour);
            if states.get(&key).is_none_or(queue_hour_needs_recovery) {
                candidates.insert(key, GapCandidate::new(*queue_id, date, hour));
            }
        }
    }
    for ((queue_id, date, hour), row) in &states {
        if queue_hour_needs_recovery(row) {
            candidates.insert(
                (*queue_id, date.clone(), *hour),
                GapCandidate::new(*queue_id, date.clone(), *hour),
            );
        }
    }
    Ok(candidates.into_values().collect())
}

/// Purpose: determine whether a queue-hour executes now. Input: state row.
/// Output: false only for terminal facts or a live execution lease.
fn queue_hour_needs_recovery(row: &Value) -> bool {
    match row.get("status").and_then(Value::as_str) {
        Some("complete") => row
            .get("reopenable")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        Some("empty") => false,
        Some("fetching") => row
            .get("lease_due")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        _ => true,
    }
}

fn row_key(row: &Value) -> Option<(String, i32)> {
    Some((
        row.get("date")?.as_str()?.to_owned(),
        row.get("hour")?
            .as_i64()
            .and_then(|value| i32::try_from(value).ok())?,
    ))
}

fn profile_enrichment_max_calls() -> usize {
    std::env::var("PLAYER_ACTIVITY_PROFILE_MAX_CALLS_PER_RUN")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(100)
        .clamp(1, 500)
}

fn api_key_reserve_calls() -> i32 {
    std::env::var("API_KEY_RESERVE_CALLS")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &i32| *value >= 0)
        .unwrap_or(100)
}

fn profile_enrichment_allowed_calls(max_calls: usize, budget: ApiHeadroomSnapshot) -> usize {
    if !budget.has_usable_keys {
        return 0;
    }
    if budget.total_keys == 0 {
        return max_calls;
    }
    max_calls.min(usize::try_from(budget.total_usable_before_reserve).unwrap_or(usize::MAX))
}

fn profile_enrichment_result_json(result: ProfileEnrichmentResult) -> Value {
    json!({
        "claimed":result.claimed,
        "calls":result.calls,
        "refreshed":result.refreshed,
        "unavailable":result.unavailable,
        "skippedRecent":result.skipped_recent,
        "failed":result.failed
    })
}

fn expected_elapsed_discovery_hours(now: OffsetDateTime, min_date: &str) -> Vec<(String, i32)> {
    let latest_fetch_tick_hour = if now.minute() >= 30 {
        i32::from(now.hour())
    } else {
        i32::from(now.hour()) - 1
    };
    if latest_fetch_tick_hour < 0 {
        return Vec::new();
    }
    let today = now.date();
    let earliest = parse_date_boundary(min_date).unwrap_or(today);
    let mut out = Vec::new();
    let mut date = earliest;
    while date <= today {
        let max_hour = if date == today {
            (latest_fetch_tick_hour - 1).max(0)
        } else {
            23
        };
        for hour in 0..=max_hour {
            out.push((date.to_string(), hour));
        }
        let Some(next) = date.next_day() else {
            break;
        };
        date = next;
        if date > today {
            break;
        }
    }
    out
}

fn parse_date_boundary(value: &str) -> Option<time::Date> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse::<i32>().ok()?;
    let month = parts.next()?.parse::<u8>().ok()?;
    let day = parts.next()?.parse::<u8>().ok()?;
    time::Date::from_calendar_date(year, Month::try_from(month).ok()?, day).ok()
}

fn completed_discovery_window(now: OffsetDateTime) -> (String, i32) {
    let hours_behind = if now.minute() < 30 { 2 } else { 1 };
    let completed = now - time::Duration::hours(hours_behind);
    (completed.date().to_string(), i32::from(completed.hour()))
}

fn scheduler_dispatch_now(trigger: &str) -> Result<OffsetDateTime, String> {
    if trigger != "capture-once" {
        return Ok(OffsetDateTime::now_utc());
    }
    let value = std::env::var("PALADINSCAT_SCHEDULER_CAPTURE_NOW")
        .map_err(|_| "scheduler capture clock is required".to_owned())?;
    OffsetDateTime::parse(&value, &Rfc3339)
        .map_err(|error| format!("scheduler capture clock is invalid: {error}"))
}

#[cfg(unix)]
async fn shutdown_signal() {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");
    tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=terminate.recv()=>{} }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use time::{Date, Month, Time, UtcOffset};

    use super::*;
    use crate::workers::policy::RANKED_STATS_QUEUE_ID;

    fn at(hour: u8, minute: u8) -> OffsetDateTime {
        Date::from_calendar_date(2026, Month::August, 2)
            .expect("date")
            .with_time(Time::from_hms(hour, minute, 0).expect("time"))
            .assume_offset(UtcOffset::UTC)
    }

    #[test]
    fn co_due_jobs_are_all_selected() {
        let schedules = scheduled_jobs_for("auto_ingester")
            .copied()
            .collect::<Vec<_>>();
        let due = due_inactive_jobs(&schedules, at(12, 5), &BTreeSet::new())
            .into_iter()
            .map(|job| job.job_key)
            .collect::<BTreeSet<_>>();

        assert_eq!(
            due,
            BTreeSet::from([
                "auto-ingester:buffer-drain",
                "auto-ingester:materialized-view-refresh",
            ])
        );
    }

    #[test]
    fn scheduler_env_crons_override_typescript_defaults() {
        let gap = scheduled_jobs_for("hourly_gap_checker")
            .next()
            .copied()
            .expect("gap job");
        let profile = scheduled_jobs_for("auto_ingester")
            .find(|job| job.job_key == "auto-ingester:profile-enrichment")
            .copied()
            .expect("profile job");
        let lookup = |name: &str| match name {
            "GAP_CHECKER_CRON_EXPRESSION" => Some("7,17 * * * *".to_owned()),
            "PLAYER_ACTIVITY_PROFILE_CRON" => Some("*/13 * * * *".to_owned()),
            _ => None,
        };

        assert!(scheduler_job_is_due_with(gap, at(12, 7), lookup));
        assert!(!scheduler_job_is_due_with(gap, at(12, 5), lookup));
        assert!(scheduler_job_is_due_with(profile, at(12, 26), lookup));
        assert!(!scheduler_job_is_due_with(profile, at(12, 50), lookup));
    }

    #[test]
    fn cron_parser_matches_node_cron_field_forms() {
        assert!(cron_matches("5-25/10 12 * * 0,7", at(12, 15)));
        assert!(!cron_matches("5-25/10 12 * * 1-5", at(12, 15)));
        assert!(!cron_matches("invalid", at(12, 15)));
    }

    #[test]
    fn presence_source_matches_scheduler_trigger() {
        assert_eq!(auto_ingester_presence_source("cron"), "auto-ingester-cron");
        assert_eq!(
            auto_ingester_presence_source("capture-once"),
            "auto-ingester-cron"
        );
    }

    #[tokio::test]
    async fn buffer_drain_continues_until_backlog_is_empty() {
        let stop = AtomicBool::new(false);
        let mut remaining = 14_usize;
        let totals = drain_buffer_continuously(&stop, None, || {
            let result = if remaining == 0 {
                BufferBatchResult::default()
            } else {
                remaining -= 1;
                BufferBatchResult {
                    processed: 50,
                    failed: 0,
                    deferred: 0,
                }
            };
            std::future::ready(Ok::<_, ()>(result))
        })
        .await
        .expect("drain");

        assert_eq!(totals.batches, 14);
        assert_eq!(totals.processed, 700);
    }

    #[tokio::test]
    async fn buffer_drain_stops_at_batch_cap() {
        let stop = AtomicBool::new(false);
        let mut remaining = 50_usize;
        let totals = drain_buffer_continuously(&stop, Some(5), || {
            let result = BufferBatchResult {
                processed: 50,
                failed: 0,
                deferred: 0,
            };
            remaining -= 1;
            std::future::ready(Ok::<_, ()>(result))
        })
        .await
        .expect("drain should succeed");

        assert_eq!(totals.batches, 5);
        assert_eq!(totals.processed, 250);
        assert_eq!(remaining, 45);
    }

    #[test]
    fn gap_scan_uses_full_governed_history_boundary() {
        assert_eq!(configured_gap_min_date(), GAP_CHECKER_MIN_DATE);
    }

    #[test]
    fn capture_urls_must_be_parseable_loopback_hosts() {
        assert!(url_is_loopback(
            "postgres://user:pass@127.0.0.1:5432/fixture"
        ));
        assert!(url_is_loopback("http://[::1]:3015"));
        assert!(!url_is_loopback("postgres://db:5432/fixture"));
        assert!(!url_is_loopback("not-a-url"));
    }

    #[test]
    fn startup_delays_are_independent_not_cumulative() {
        let delays = scheduled_jobs_for("auto_ingester")
            .filter_map(|job| startup_delay_seconds(*job))
            .collect::<Vec<_>>();

        assert_eq!(delays, vec![10, 15, 20, 25, 30]);
        assert_eq!(delays.last().copied(), Some(30));
    }

    #[tokio::test]
    async fn shutdown_cancels_pending_startup_timer_but_not_started_work() {
        let pending_stop = Arc::new(AtomicBool::new(false));
        let pending = tokio::spawn(wait_for_startup_delay(60, pending_stop.clone()));
        pending_stop.store(true, Ordering::Relaxed);
        assert!(
            !tokio::time::timeout(Duration::from_secs(1), pending)
                .await
                .expect("pending startup timer should observe shutdown")
                .expect("pending startup task")
        );

        let running_stop = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let running = {
            let running_stop = running_stop.clone();
            let started = started.clone();
            let finished = finished.clone();
            tokio::spawn(async move {
                if wait_for_startup_delay(0, running_stop).await {
                    started.store(true, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    finished.store(true, Ordering::Relaxed);
                }
            })
        };
        while !started.load(Ordering::Relaxed) {
            tokio::task::yield_now().await;
        }
        running_stop.store(true, Ordering::Relaxed);
        running.await.expect("running startup task");
        assert!(finished.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn ownership_heartbeat_wait_is_bounded() {
        let result =
            wait_with_timeout(Duration::from_millis(10), std::future::pending::<()>()).await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn ownership_loss_aborts_pending_scheduler_work() {
        let startup_handles = vec![tokio::spawn(std::future::pending::<()>())];
        let mut active = BTreeMap::new();
        active.insert(
            "auto-ingester:discovery",
            tokio::spawn(std::future::pending::<()>()),
        );

        abort_scheduler_work(&startup_handles, &active);

        for handle in startup_handles {
            assert!(
                handle
                    .await
                    .expect_err("startup task should be aborted")
                    .is_cancelled()
            );
        }
        for (_, handle) in active {
            assert!(
                handle
                    .await
                    .expect_err("active task should be aborted")
                    .is_cancelled()
            );
        }
    }

    #[tokio::test]
    async fn discovery_executes_post_drain_in_order() {
        let discovery = scheduled_jobs_for("auto_ingester")
            .find(|job| job.job_key == "auto-ingester:discovery")
            .copied()
            .expect("discovery job");
        let mut executed = Vec::new();

        execute_scheduled_sequence(discovery, "cron", |job, trigger| {
            executed.push((job.job_key, trigger));
            std::future::ready(true)
        })
        .await;

        assert_eq!(
            executed,
            vec![
                ("auto-ingester:discovery", "cron"),
                ("auto-ingester:buffer-drain", "post-discovery"),
            ]
        );
    }

    #[test]
    fn gap_candidate_queries_preserve_sql_token_boundaries() {
        let sql = queue_hour_states_sql();
        assert!(sql.contains("hourly_ingest_state"));
        assert!(!sql.contains("hourly_ingest_match_debt"));
        assert!(sql.contains("reopenable"));
        assert!(sql.contains("ingest.match_id IS NULL"));
        assert!(sql.contains("error_message IS DISTINCT FROM $4"));
    }

    #[test]
    fn every_failed_queue_hour_requires_immediate_recovery() {
        let failed = json!({
            "date":"2026-08-02",
            "hour":2,
            "status":"failed",
            "raw_match_count":10,
            "lease_due":true
        });
        assert!(queue_hour_needs_recovery(&failed));
    }

    #[test]
    fn complete_hour_with_reopenable_invalid_match_requires_recovery() {
        assert!(queue_hour_needs_recovery(&json!({
            "status":"complete",
            "reopenable":true
        })));
        assert!(!queue_hour_needs_recovery(&json!({
            "status":"complete",
            "reopenable":false
        })));
    }

    #[test]
    fn gap_candidates_merge_populations_newest_first() {
        let queues = [424, 425, 452, 453, 10297, 10332, 10348, 10362, 10367, 10369];
        let mut candidates = [1, 2, 3]
            .into_iter()
            .flat_map(|hour| {
                queues
                    .into_iter()
                    .map(move |queue| GapCandidate::new(queue, "2026-08-02".to_owned(), hour))
            })
            .chain([GapCandidate::new(
                RANKED_STATS_QUEUE_ID,
                "2026-08-01".to_owned(),
                23,
            )])
            .collect::<Vec<_>>();

        candidates.sort_by(|left, right| {
            right
                .date
                .cmp(&left.date)
                .then_with(|| right.hour.cmp(&left.hour))
                .then_with(|| left.queue_id.cmp(&right.queue_id))
        });

        assert_eq!(candidates[0].hour, 3);
        assert_eq!(candidates[0].queue_id, 424);
        assert_eq!(
            candidates.last().map(|candidate| candidate.queue_id),
            Some(RANKED_STATS_QUEUE_ID)
        );
    }

    #[test]
    fn gap_recovery_groups_all_queues_by_hour_without_a_work_cap() {
        let source = include_str!("scheduler_host.rs");
        let recovery = source
            .split_once("async fn run_gap_check")
            .expect("gap recovery")
            .1
            .split_once("struct GapCandidate")
            .expect("candidate model")
            .0;
        assert!(recovery.contains("candidates_by_hour"));
        assert!(recovery.contains("join_all"));
        assert!(recovery.contains("queue_hour_claim_versions"));
        assert!(recovery.contains("queue-hour claim failed twice"));
        assert!(!recovery.contains("take("));
        assert!(!recovery.contains("truncate("));
    }

    #[test]
    fn expected_hours_enumerate_prior_days_back_to_min_date() {
        let now = Date::from_calendar_date(2026, Month::August, 2)
            .expect("date")
            .with_time(Time::from_hms(5, 15, 0).expect("time"))
            .assume_offset(UtcOffset::UTC);
        // min_date 08-01 => enumerate prior day fully (0..=23) + today up to
        // latest elapsed tick (hour < 4, since minute 15 < 30 => tick 4, so
        // today hours 0..=3 are elapsed).
        assert_eq!(
            expected_elapsed_discovery_hours(now, "2026-08-01"),
            vec![
                ("2026-08-01".to_owned(), 0),
                ("2026-08-01".to_owned(), 1),
                ("2026-08-01".to_owned(), 2),
                ("2026-08-01".to_owned(), 3),
                ("2026-08-01".to_owned(), 4),
                ("2026-08-01".to_owned(), 5),
                ("2026-08-01".to_owned(), 6),
                ("2026-08-01".to_owned(), 7),
                ("2026-08-01".to_owned(), 8),
                ("2026-08-01".to_owned(), 9),
                ("2026-08-01".to_owned(), 10),
                ("2026-08-01".to_owned(), 11),
                ("2026-08-01".to_owned(), 12),
                ("2026-08-01".to_owned(), 13),
                ("2026-08-01".to_owned(), 14),
                ("2026-08-01".to_owned(), 15),
                ("2026-08-01".to_owned(), 16),
                ("2026-08-01".to_owned(), 17),
                ("2026-08-01".to_owned(), 18),
                ("2026-08-01".to_owned(), 19),
                ("2026-08-01".to_owned(), 20),
                ("2026-08-01".to_owned(), 21),
                ("2026-08-01".to_owned(), 22),
                ("2026-08-01".to_owned(), 23),
                ("2026-08-02".to_owned(), 0),
                ("2026-08-02".to_owned(), 1),
                ("2026-08-02".to_owned(), 2),
                ("2026-08-02".to_owned(), 3),
            ]
        );
        // min_date == today => only today's elapsed hours are enumerated.
        assert_eq!(
            expected_elapsed_discovery_hours(now, "2026-08-02"),
            vec![
                ("2026-08-02".to_owned(), 0),
                ("2026-08-02".to_owned(), 1),
                ("2026-08-02".to_owned(), 2),
                ("2026-08-02".to_owned(), 3),
            ]
        );
        // invalid min_date falls back gracefully (only today), never panics.
        assert!(!expected_elapsed_discovery_hours(now, "not-a-date").is_empty());
    }

    #[test]
    fn auto_discovery_window_matches_typescript_completion_grace() {
        assert_eq!(
            completed_discovery_window(at(0, 5)),
            ("2026-08-01".to_owned(), 22)
        );
        assert_eq!(
            completed_discovery_window(at(5, 15)),
            ("2026-08-02".to_owned(), 3)
        );
        assert_eq!(
            completed_discovery_window(at(9, 30)),
            ("2026-08-02".to_owned(), 8)
        );
    }

    #[test]
    fn profile_enrichment_respects_api_headroom() {
        assert_eq!(
            profile_enrichment_allowed_calls(
                100,
                ApiHeadroomSnapshot {
                    total_keys: 2,
                    usable_keys: 0,
                    total_usable_before_reserve: 0,
                    has_usable_keys: false,
                },
            ),
            0
        );
        assert_eq!(
            profile_enrichment_allowed_calls(
                100,
                ApiHeadroomSnapshot {
                    total_keys: 2,
                    usable_keys: 1,
                    total_usable_before_reserve: 17,
                    has_usable_keys: true,
                },
            ),
            17
        );
        assert_eq!(
            profile_enrichment_allowed_calls(
                100,
                ApiHeadroomSnapshot {
                    total_keys: 0,
                    usable_keys: 0,
                    total_usable_before_reserve: 0,
                    has_usable_keys: true,
                },
            ),
            100
        );
    }

    #[test]
    fn profile_enrichment_result_matches_typescript_contract() {
        let value = profile_enrichment_result_json(ProfileEnrichmentResult {
            claimed: 6,
            calls: 1,
            refreshed: 2,
            unavailable: 3,
            skipped_recent: 1,
            failed: 0,
        });
        assert_eq!(
            value,
            json!({
                "claimed":6,
                "calls":1,
                "refreshed":2,
                "unavailable":3,
                "skippedRecent":1,
                "failed":0
            })
        );
        assert_eq!(
            profile_enrichment_result_json(ProfileEnrichmentResult::default()),
            json!({
                "claimed":0,
                "calls":0,
                "refreshed":0,
                "unavailable":0,
                "skippedRecent":0,
                "failed":0
            })
        );
    }
}
