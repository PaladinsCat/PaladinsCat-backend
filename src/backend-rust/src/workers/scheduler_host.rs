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

use paladinscat_core::{
    cache::RedisCache,
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde_json::{Value, json};
use time::{OffsetDateTime, Weekday, format_description::well_known::Rfc3339};
use url::{Host, Url};

use super::{
    coordination::WorkerCoordinationRepository,
    discovery_control::{
        due_debt_hours, record_hourly_ingest_quota_wait, revive_fresh_no_authority_debt,
    },
    history_retention::cleanup_player_history_retention,
    live_tracker::detect_dropped_matches,
    maintenance::{
        BufferBatchResult, cleanup_raw_ingest_buffer_retention, process_buffer_batch_until,
        refresh_baselines_with_job, refresh_derived_projections_with_job,
    },
    outage::{
        MATCH_DETAIL_SERVICE_OUTAGE_KEY, active_hirez_service_outage,
        is_hirez_service_outage_probe_due,
    },
    pipeline::CanonicalIngestPipeline,
    policy::{ApiHeadroomSnapshot, MATCH_COUNT_QUEUE_DEFINITIONS, api_headroom_snapshot},
    profile_enrichment::{ProfileEnrichmentRepository, ProfileEnrichmentResult},
    ranked_tracker::RankedTracker,
    relay::WorkerRelayClient,
    scheduler::{ScheduledJob, StartupPolicy, scheduled_jobs_for},
    scheduler_runtime::SchedulerRuntimeExit,
    tier_stats::TierStatsRepository,
};

const OWNERSHIP_LEASE: Duration = Duration::from_secs(60);
const STARTUP_OWNERSHIP_LEASE: Duration = Duration::from_secs(5 * 60);
const OWNERSHIP_HEARTBEAT: Duration = Duration::from_secs(15);
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

fn nonranked_acquisition_max_matches_per_run() -> usize {
    bounded_nonranked_acquisition_max_matches(
        std::env::var("NONRANKED_ACQUISITION_MAX_MATCHES_PER_RUN")
            .ok()
            .and_then(|value| value.parse().ok()),
    )
}

fn bounded_nonranked_acquisition_max_matches(value: Option<usize>) -> usize {
    value.unwrap_or(2_000).clamp(1, 20_000)
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

fn bounded_gap_retry_lookback_days(value: Option<i64>) -> i64 {
    value.unwrap_or(2).max(1)
}

fn configured_gap_min_date(now: OffsetDateTime) -> String {
    let lookback = bounded_gap_retry_lookback_days(
        std::env::var("GAP_CHECKER_RETRY_STATE_LOOKBACK_DAYS")
            .ok()
            .and_then(|value| value.parse().ok()),
    );
    gap_min_date(now, lookback)
}

fn gap_min_date(now: OffsetDateTime, lookback_days: i64) -> String {
    let calculated = (now - time::Duration::days(lookback_days))
        .date()
        .to_string();
    if calculated.as_str() < GAP_CHECKER_MIN_DATE {
        GAP_CHECKER_MIN_DATE.to_owned()
    } else {
        calculated
    }
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
                if !coordination.heartbeat_scheduler_owner(&scheduler_key, &owner_id, OWNERSHIP_LEASE).await.unwrap_or(false) {
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
            let presence_source = auto_ingester_presence_source(trigger);
            // Keep the TS auto-ingester order: ranked discovery, presence-only
            // discovery, then one ledger-backed non-ranked acquisition pass.
            let ranked = worker
                .discover_ranked_hour(&date, hour, "auto-ingester", false)
                .await;
            let presence = worker
                .discover_all_presence_queues(&date, hour, presence_source)
                .await;
            let presence_complete = presence.iter().filter(|result| result.is_ok()).count();
            let presence_failed = presence.len() - presence_complete;
            let nonranked = worker
                .run_nonranked_acquisition(nonranked_acquisition_max_matches_per_run(), 48)
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({
                "date":date,
                "hour":hour,
                "ranked_complete":ranked.is_ok(),
                "presence_complete":presence_complete,
                "presence_failed":presence_failed,
                "nonranked_acquired":nonranked
            }))
        }
        "auto-ingester:profile-enrichment" => {
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
            let enrichment = ProfileEnrichmentRepository::new(services.database.clone())
                .run(&services.config, max_calls, "cron")
                .await
                .map_err(|error| error.to_string())?;
            Ok(profile_enrichment_result_json(enrichment))
        }
        "auto-ingester:buffer-drain" => {
            let relay =
                WorkerRelayClient::new(&services.config).map_err(|error| error.to_string())?;
            let totals = drain_buffer_continuously(&services.should_stop, || {
                process_buffer_batch_until(
                    &services.database,
                    Some(&relay),
                    50,
                    &services.should_stop,
                )
            })
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
            Ok(json!({"refreshed":true}))
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

async fn drain_view_counts(
    database: &Database,
    config: &BackendConfig,
) -> Result<Value, String> {
    let redis = RedisCache::new(&config.redis_url).map_err(|error| error.to_string())?;
    let mut posts = 0_u64;
    let mut builds = 0_u64;
    if let Some(keys) = redis.scan_keys("viewcount:posts:*").await {
        for key in keys {
            let Some(id) = key.strip_prefix("viewcount:posts:").and_then(|s| s.parse::<i64>().ok()) else {
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
            let Some(id) = key.strip_prefix("viewcount:builds:").and_then(|s| s.parse::<i64>().ok()) else {
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

async fn run_gap_check(services: &SchedulerServices) -> Result<Value, String> {
    let now = OffsetDateTime::now_utc();
    let min_date = configured_gap_min_date(now);
    let max_date = now.date().to_string();
    let _ = revive_fresh_no_authority_debt(&services.database, 486, &min_date, &max_date)
        .await
        .map_err(|error| error.to_string())?;
    let due = due_debt_hours(&services.database, 486, &min_date, &max_date)
        .await
        .map_err(|error| error.to_string())?;
    let limit = std::env::var("GAP_CHECKER_MAX_BACKFILL_PER_RUN")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(8_usize)
        .max(1);
    let ranked = ranked_gap_candidates(&services.database, now, &min_date, due)
        .await
        .map_err(|error| error.to_string())?;
    let presence = presence_gap_candidates(&services.database, now, &min_date)
        .await
        .map_err(|error| error.to_string())?;
    let mut candidates = ranked.clone();
    candidates.extend(presence.iter().cloned());
    if candidates.is_empty() {
        return Ok(json!({"candidates":0,"attempted":0,"completed":0}));
    }
    let headroom = api_headroom_snapshot(&services.database, api_key_reserve_calls())
        .await
        .map_err(|error| error.to_string())?;
    if !headroom.has_usable_keys {
        for candidate in &candidates {
            record_hourly_ingest_quota_wait(
                &services.database,
                &candidate.date,
                candidate.hour,
                candidate.queue_id,
                "gap-checker-quota-wait",
                "no usable Hi-Rez key headroom",
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        return Ok(
            json!({"candidates":candidates.len(),"attempted":0,"completed":0,"skipped":"no_api_headroom"}),
        );
    }
    let active_outage =
        active_hirez_service_outage(&services.database, MATCH_DETAIL_SERVICE_OUTAGE_KEY)
            .await
            .map_err(|error| error.to_string())?;
    let probe_due = is_hirez_service_outage_probe_due(active_outage.as_ref(), now);
    let mut selected = if active_outage.is_some() {
        let mut eligible = presence.clone();
        if probe_due
            && let Some(probe) = ranked
                .iter()
                .find(|candidate| candidate.debt_only)
                .or(ranked.first())
        {
            eligible.push(probe.clone());
        }
        eligible
    } else {
        let presence_budget = presence.len().min(limit.div_ceil(2));
        let mut eligible = presence
            .into_iter()
            .take(presence_budget)
            .collect::<Vec<_>>();
        eligible.extend(ranked.into_iter().take(limit - eligible.len()));
        eligible
    };
    selected.truncate(limit);
    let pipeline = CanonicalIngestPipeline::new(services.database.clone(), &services.config)
        .map_err(|error| error.to_string())?;
    let mut completed = 0;
    for candidate in &selected {
        let result = if candidate.presence_only {
            pipeline
                .discover_presence_hour(
                    candidate.queue_id,
                    &candidate.date,
                    candidate.hour,
                    "gap-checker-presence-backfill",
                )
                .await
        } else {
            pipeline
                .discover_ranked_hour(
                    &candidate.date,
                    candidate.hour,
                    "gap-checker",
                    candidate.debt_only,
                )
                .await
        };
        if result.is_ok() {
            completed += 1;
        }
    }
    Ok(json!({"candidates":candidates.len(),"attempted":selected.len(),"completed":completed}))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GapCandidate {
    queue_id: i32,
    date: String,
    hour: i32,
    debt_only: bool,
    presence_only: bool,
}

impl GapCandidate {
    fn ranked(date: String, hour: i32, debt_only: bool) -> Self {
        Self {
            queue_id: 486,
            date,
            hour,
            debt_only,
            presence_only: false,
        }
    }

    fn presence(queue_id: i32, date: String, hour: i32) -> Self {
        Self {
            queue_id,
            date,
            hour,
            debt_only: false,
            presence_only: true,
        }
    }
}

const RANKED_GAP_STATES_SQL: &str = r#"SELECT state.date::TEXT,state.hour,state.status,state.raw_match_count,
         state.next_retry_at<=now() retry_due,state.lease_until<=now() lease_due,
         COALESCE(counts.total_matches,0) total_matches,COALESCE(debt.open_count,0) open_count,
         COALESCE(debt.terminal_count,0) terminal_count
         FROM hourly_ingest_state state
         LEFT JOIN hourly_match_counts counts ON counts.date=state.date AND counts.hour=state.hour AND counts.queue_id=state.queue_id
         LEFT JOIN (SELECT date,hour,COUNT(*) FILTER(WHERE status='unrecoverable')::INT terminal_count,
          COUNT(*) FILTER(WHERE status IN('pending','staged'))::INT open_count FROM hourly_ingest_match_debt
          WHERE queue_id=486 AND date>=$1::TEXT::DATE AND date<=$2::TEXT::DATE GROUP BY date,hour) debt
          ON debt.date=state.date AND debt.hour=state.hour
         WHERE state.queue_id=486 AND state.date>=$1::TEXT::DATE AND state.date<=$2::TEXT::DATE"#;

const RANKED_GAP_COUNTS_SQL: &str = r#"SELECT date::TEXT,hour,total_matches FROM hourly_match_counts
         WHERE queue_id=486 AND date>=$1::TEXT::DATE AND date<=$2::TEXT::DATE"#;

const PRESENCE_GAP_STATES_SQL: &str = r#"SELECT date::TEXT,hour,queue_id,status,next_retry_at<=now() retry_due,lease_until<=now() lease_due
         FROM hourly_ingest_state WHERE queue_id=ANY($1::INT[])
         AND date>=$2::TEXT::DATE AND date<=$3::TEXT::DATE"#;

async fn ranked_gap_candidates(
    database: &Database,
    now: OffsetDateTime,
    min_date: &str,
    due_debt: Vec<(String, i32)>,
) -> Result<Vec<GapCandidate>, DatabaseError> {
    let max_date = now.date().to_string();
    let states = database
        .query_json(RANKED_GAP_STATES_SQL, &[&min_date, &max_date])
        .await?;
    let counts = database
        .query_json(RANKED_GAP_COUNTS_SQL, &[&min_date, &max_date])
        .await?;
    let mut states_by_key = BTreeMap::new();
    for row in states {
        let Some((date, hour)) = row_key(&row) else {
            continue;
        };
        states_by_key.insert((date, hour), row);
    }
    let mut counts_by_key = BTreeMap::new();
    for row in counts {
        let Some((date, hour)) = row_key(&row) else {
            continue;
        };
        counts_by_key.insert((date, hour), row_i32(&row, "total_matches"));
    }
    let mut candidates = BTreeMap::new();
    for (date, hour) in expected_elapsed_discovery_hours(now) {
        let key = (date.clone(), hour);
        let handled = states_by_key.get(&key).map_or_else(
            || counts_by_key.get(&key).copied().unwrap_or_default() > 0,
            ranked_state_handled,
        );
        if !handled {
            candidates.insert(key, GapCandidate::ranked(date, hour, false));
        }
    }
    for ((date, hour), row) in &states_by_key {
        if !ranked_state_handled(row) {
            candidates.insert(
                (date.clone(), *hour),
                GapCandidate::ranked(date.clone(), *hour, false),
            );
        }
    }
    for (date, hour) in due_debt {
        candidates.insert((date.clone(), hour), GapCandidate::ranked(date, hour, true));
    }
    Ok(candidates.into_values().collect())
}

async fn presence_gap_candidates(
    database: &Database,
    now: OffsetDateTime,
    min_date: &str,
) -> Result<Vec<GapCandidate>, DatabaseError> {
    let Some((max_date, _)) = expected_elapsed_discovery_hours(now).last().cloned() else {
        return Ok(Vec::new());
    };
    let queue_ids = MATCH_COUNT_QUEUE_DEFINITIONS
        .iter()
        .filter(|queue| queue.track_presence && !queue.ranked)
        .map(|queue| queue.queue_id)
        .collect::<Vec<_>>();
    let rows = database
        .query_json(
            PRESENCE_GAP_STATES_SQL,
            &[&queue_ids.as_slice(), &min_date, &max_date],
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
    for queue_id in &queue_ids {
        for (date, hour) in expected_elapsed_discovery_hours(now) {
            let key = (*queue_id, date.clone(), hour);
            if states.get(&key).is_none_or(presence_state_retryable) {
                candidates.insert(key, GapCandidate::presence(*queue_id, date, hour));
            }
        }
    }
    for ((queue_id, date, hour), row) in &states {
        if presence_state_retryable(row) {
            candidates.insert(
                (*queue_id, date.clone(), *hour),
                GapCandidate::presence(*queue_id, date.clone(), *hour),
            );
        }
    }
    for (queue_id, date, hour) in bracketed_missing_presence_hours(database, now, min_date).await? {
        candidates.insert(
            (queue_id, date.clone(), hour),
            GapCandidate::presence(queue_id, date, hour),
        );
    }
    Ok(candidates.into_values().collect())
}

fn ranked_state_handled(row: &Value) -> bool {
    let raw = row_i32(row, "raw_match_count");
    let count = row_i32(row, "total_matches");
    let open = row_i32(row, "open_count");
    let terminal = row_i32(row, "terminal_count");
    if row.get("status").and_then(Value::as_str) == Some("complete")
        || (raw > 0 && count >= raw)
        || (raw > 0 && open == 0 && count + terminal >= raw)
    {
        return true;
    }
    match row.get("status").and_then(Value::as_str) {
        Some("empty") | Some("fetching") | Some("staged") => !row
            .get(
                if row.get("status").and_then(Value::as_str) == Some("empty") {
                    "retry_due"
                } else {
                    "lease_due"
                },
            )
            .and_then(Value::as_bool)
            .unwrap_or(true),
        Some("failed") | Some("pending") => !row
            .get("retry_due")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        _ => false,
    }
}

fn presence_state_retryable(row: &Value) -> bool {
    match row.get("status").and_then(Value::as_str) {
        Some("complete") | Some("empty") => false,
        Some("fetching") | Some("staged") => row
            .get("lease_due")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        _ => row
            .get("retry_due")
            .and_then(Value::as_bool)
            .unwrap_or(true),
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

fn row_i32(row: &Value, key: &str) -> i32 {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or_default()
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

const BRACKETED_MISSING_PRESENCE_HOURS_SQL: &str = "WITH hours AS(SELECT tick FROM generate_series($2::TEXT::DATE+INTERVAL '1 hour',$3::TEXT::DATE+($4::TEXT::INT*INTERVAL '1 hour')-INTERVAL '1 hour',INTERVAL '1 hour') tick) \
             SELECT queue.queue_id,to_char(hours.tick,'YYYY-MM-DD') date,EXTRACT(HOUR FROM hours.tick)::INT AS \"hour\" \
             FROM unnest($1::INT[]) queue(queue_id) CROSS JOIN hours \
             JOIN hourly_ingest_state previous ON previous.queue_id=queue.queue_id AND previous.date=(hours.tick-INTERVAL '1 hour')::DATE AND previous.hour=EXTRACT(HOUR FROM hours.tick-INTERVAL '1 hour')::INT \
             LEFT JOIN hourly_ingest_state missing ON missing.queue_id=queue.queue_id AND missing.date=hours.tick::DATE AND missing.hour=EXTRACT(HOUR FROM hours.tick)::INT \
             JOIN hourly_ingest_state next ON next.queue_id=queue.queue_id AND next.date=(hours.tick+INTERVAL '1 hour')::DATE AND next.hour=EXTRACT(HOUR FROM hours.tick+INTERVAL '1 hour')::INT \
             WHERE missing.queue_id IS NULL ORDER BY date,\"hour\",queue.queue_id";

async fn bracketed_missing_presence_hours(
    database: &Database,
    now: OffsetDateTime,
    min_date: &str,
) -> Result<Vec<(i32, String, i32)>, DatabaseError> {
    let queue_ids = MATCH_COUNT_QUEUE_DEFINITIONS
        .iter()
        .filter(|queue| queue.track_presence && !queue.ranked)
        .map(|queue| queue.queue_id)
        .collect::<Vec<_>>();
    let Some((max_date, max_hour)) = expected_elapsed_discovery_hours(now).last().cloned() else {
        return Ok(Vec::new());
    };
    let max_hour = max_hour.to_string();
    Ok(database
        .query_json(
            BRACKETED_MISSING_PRESENCE_HOURS_SQL,
            &[&queue_ids.as_slice(), &min_date, &max_date, &max_hour],
        )
        .await?
        .into_iter()
        .filter_map(|row| {
            Some((
                row.get("queue_id")?
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())?,
                row.get("date")?.as_str()?.to_owned(),
                row.get("hour")?
                    .as_i64()
                    .and_then(|value| i32::try_from(value).ok())?,
            ))
        })
        .collect())
}

fn expected_elapsed_discovery_hours(now: OffsetDateTime) -> Vec<(String, i32)> {
    let latest_fetch_tick_hour = if now.minute() >= 30 {
        i32::from(now.hour())
    } else {
        i32::from(now.hour()) - 1
    };
    if latest_fetch_tick_hour < 0 {
        return Vec::new();
    }
    let day_start = now
        .replace_hour(0)
        .and_then(|value| value.replace_minute(30))
        .and_then(|value| value.replace_second(0))
        .and_then(|value| value.replace_nanosecond(0))
        .unwrap_or(now);
    (0..=latest_fetch_tick_hour)
        .map(|fetch_hour| {
            let target = day_start + time::Duration::hours(i64::from(fetch_hour) - 1);
            (target.date().to_string(), i32::from(target.hour()))
        })
        .filter(|(date, _)| date.as_str() >= GAP_CHECKER_MIN_DATE)
        .collect()
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
    fn acquisition_limit_and_presence_source_match_typescript() {
        assert_eq!(bounded_nonranked_acquisition_max_matches(None), 2_000);
        assert_eq!(bounded_nonranked_acquisition_max_matches(Some(0)), 1);
        assert_eq!(
            bounded_nonranked_acquisition_max_matches(Some(50_000)),
            20_000
        );
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
        let totals = drain_buffer_continuously(&stop, || {
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

    #[test]
    fn gap_lookback_matches_typescript_range_and_minimum_date() {
        assert_eq!(bounded_gap_retry_lookback_days(None), 2);
        assert_eq!(bounded_gap_retry_lookback_days(Some(0)), 1);
        assert_eq!(bounded_gap_retry_lookback_days(Some(35)), 35);
        let near_deployment = Date::from_calendar_date(2026, Month::June, 2)
            .expect("date")
            .with_time(Time::MIDNIGHT)
            .assume_offset(UtcOffset::UTC);
        assert_eq!(gap_min_date(near_deployment, 35), GAP_CHECKER_MIN_DATE);
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

        assert_eq!(delays, vec![10, 15, 20, 25]);
        assert_eq!(delays.last().copied(), Some(25));
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
    fn bracketed_gap_scan_binds_string_dates_as_text() {
        assert!(BRACKETED_MISSING_PRESENCE_HOURS_SQL.contains("$2::TEXT::DATE"));
        assert!(BRACKETED_MISSING_PRESENCE_HOURS_SQL.contains("$3::TEXT::DATE"));
    }

    #[test]
    fn gap_candidate_queries_preserve_sql_token_boundaries() {
        assert!(RANKED_GAP_STATES_SQL.contains("terminal_count\n         FROM"));
        assert!(RANKED_GAP_STATES_SQL.contains("hourly_ingest_match_debt\n          WHERE"));
        assert!(RANKED_GAP_STATES_SQL.contains(") debt\n          ON"));
        assert!(RANKED_GAP_COUNTS_SQL.contains("hourly_match_counts\n         WHERE"));
        assert!(PRESENCE_GAP_STATES_SQL.contains("lease_due\n         FROM"));
    }

    #[test]
    fn gap_candidates_sort_like_typescript_by_date_hour_then_queue() {
        let queues = [424, 425, 452, 453, 10297, 10332, 10348, 10362, 10367, 10369];
        let mut candidates = [1, 2, 3]
            .into_iter()
            .flat_map(|hour| {
                queues
                    .into_iter()
                    .map(move |queue| GapCandidate::presence(queue, "2026-08-02".to_owned(), hour))
            })
            .chain([GapCandidate::ranked("2026-08-01".to_owned(), 23, true)])
            .collect::<Vec<_>>();

        candidates.reverse();
        candidates.sort_by(|left, right| {
            left.date
                .cmp(&right.date)
                .then(left.hour.cmp(&right.hour))
                .then(left.queue_id.cmp(&right.queue_id))
        });

        assert!(candidates[0].debt_only);
        assert_eq!(candidates[1].hour, 1);
        assert_eq!(candidates[1].queue_id, 424);
    }

    #[test]
    fn expected_hours_match_typescript_elapsed_tick_grid() {
        let now = Date::from_calendar_date(2026, Month::August, 2)
            .expect("date")
            .with_time(Time::from_hms(5, 15, 0).expect("time"))
            .assume_offset(UtcOffset::UTC);
        assert_eq!(
            expected_elapsed_discovery_hours(now),
            vec![
                ("2026-08-01".to_owned(), 23),
                ("2026-08-02".to_owned(), 0),
                ("2026-08-02".to_owned(), 1),
                ("2026-08-02".to_owned(), 2),
                ("2026-08-02".to_owned(), 3),
            ]
        );
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
