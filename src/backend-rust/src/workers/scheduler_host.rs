use std::{sync::Arc, time::Duration};

use paladinscat_core::{
    config::BackendConfig,
    database::{Database, DatabaseError},
};
use serde_json::{Value, json};
use time::OffsetDateTime;

use super::{
    coordination::WorkerCoordinationRepository,
    discovery_control::{due_debt_hours, revive_fresh_no_authority_debt},
    history_retention::cleanup_player_history_retention,
    live_tracker::detect_dropped_matches,
    maintenance::{
        cleanup_raw_ingest_buffer_retention, process_buffer_batch, refresh_baselines_with_job,
        refresh_derived_projections_with_job,
    },
    pipeline::CanonicalIngestPipeline,
    policy::MATCH_COUNT_QUEUE_DEFINITIONS,
    profile_enrichment::ProfileEnrichmentRepository,
    ranked_tracker::RankedTracker,
    scheduler::{ScheduledJob, StartupPolicy, scheduled_jobs_for},
    scheduler_runtime::SchedulerRuntimeExit,
    tier_stats::TierStatsRepository,
};

const OWNERSHIP_LEASE: Duration = Duration::from_secs(60);
const OWNERSHIP_HEARTBEAT: Duration = Duration::from_secs(15);
const JOB_LEASE: Duration = Duration::from_secs(60 * 60);

#[derive(Clone)]
struct SchedulerServices {
    database: Database,
    config: Arc<BackendConfig>,
}

pub async fn run_scheduler_domain(
    database: Database,
    config: Arc<BackendConfig>,
    scheduler_key: String,
    owner_id: String,
) -> Result<SchedulerRuntimeExit, DatabaseError> {
    let coordination = WorkerCoordinationRepository::new(database.clone());
    if !coordination
        .acquire_scheduler_owner(&scheduler_key, &owner_id, OWNERSHIP_LEASE)
        .await?
    {
        return Ok(SchedulerRuntimeExit::OwnershipUnavailable);
    }
    let services = SchedulerServices { database, config };
    let schedules = scheduled_jobs_for(&scheduler_key)
        .copied()
        .collect::<Vec<_>>();
    let mut startup_jobs = schedules
        .iter()
        .filter(|job| !matches!(job.startup, StartupPolicy::None))
        .copied()
        .collect::<Vec<_>>();
    startup_jobs.sort_by_key(|job| match job.startup {
        StartupPolicy::None => u64::MAX,
        StartupPolicy::DurableCatchup { delay_seconds }
        | StartupPolicy::Always { delay_seconds } => delay_seconds,
    });
    for job in startup_jobs {
        let delay = match job.startup {
            StartupPolicy::None => 0,
            StartupPolicy::DurableCatchup { delay_seconds }
            | StartupPolicy::Always { delay_seconds } => delay_seconds,
        };
        if delay > 0 {
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
        execute_scheduled_job(
            coordination.clone(),
            services.clone(),
            job,
            owner_id.clone(),
            "startup",
        )
        .await;
    }
    let mut clock = tokio::time::interval(Duration::from_secs(1));
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(OWNERSHIP_HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut last_started_minute = None;
    let mut active: Option<tokio::task::JoinHandle<()>> = None;
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
                if active.as_ref().is_some_and(tokio::task::JoinHandle::is_finished)
                    && let Some(done) = active.take()
                {
                    let _ = done.await;
                }
                let now = OffsetDateTime::now_utc();
                let slot = now.unix_timestamp()/60;
                if active.is_none() && last_started_minute != Some(slot)
                    && let Some(job) = schedules.iter().find(|job| job.is_due(now)).copied()
                {
                    last_started_minute = Some(slot);
                    active = Some(tokio::spawn(execute_scheduled_job(
                        coordination.clone(), services.clone(), job, owner_id.clone(), "cron",
                    )));
                }
            }
        }
    };
    if let Some(active) = active {
        let _ = active.await;
    }
    let _ = coordination
        .release_scheduler_owner(&scheduler_key, &owner_id)
        .await;
    Ok(exit)
}

async fn execute_scheduled_job(
    coordination: WorkerCoordinationRepository,
    services: SchedulerServices,
    job: ScheduledJob,
    owner_id: String,
    trigger: &'static str,
) {
    let lease = match coordination
        .acquire_job(job.job_key, job.scheduler_key, &owner_id, JOB_LEASE)
        .await
    {
        Ok(Some(lease)) => lease,
        _ => return,
    };
    let run_id = match coordination.start_run(&lease, trigger).await {
        Ok(id) => id,
        Err(_) => {
            let _ = coordination.release_job(&lease).await;
            return;
        }
    };
    let execution = dispatch(&services, job.job_key).await;
    let (status, result, error) = match execution {
        Ok(value) => ("completed", Some(value), None),
        Err(error) => {
            tracing::error!(job=job.job_key,%error,"scheduled Rust job failed");
            ("failed", None, Some(error))
        }
    };
    let _ = coordination
        .finish_run(run_id, status, result.as_ref(), error.as_deref())
        .await;
    let _ = coordination.release_job(&lease).await;
}

async fn dispatch(services: &SchedulerServices, job_key: &str) -> Result<Value, String> {
    match job_key {
        "ranked-tracker:leaderboard" => {
            let worker = RankedTracker::new(services.database.clone(), &services.config)
                .map_err(|error| error.to_string())?;
            serde_json::to_value(worker.track().await.map_err(|error| error.to_string())?)
                .map_err(|error| error.to_string())
        }
        "auto-ingester:discovery" => {
            let target = OffsetDateTime::now_utc() - time::Duration::hours(1);
            let date = target.date().to_string();
            let worker = CanonicalIngestPipeline::new(services.database.clone(), &services.config)
                .map_err(|error| error.to_string())?;
            let results = worker
                .discover_all_presence_queues(&date, i32::from(target.hour()), "scheduler")
                .await;
            let complete = results.iter().filter(|result| result.is_ok()).count();
            let failed = results.len() - complete;
            let enrichment = ProfileEnrichmentRepository::new(services.database.clone())
                .run(&services.config, 10, "rust-auto-ingester")
                .await
                .map_err(|error| error.to_string())?;
            Ok(json!({
                "date":date,
                "hour":target.hour(),
                "complete":complete,
                "failed":failed,
                "profileEnrichment":{
                    "calls":enrichment.calls,
                    "claimed":enrichment.claimed,
                    "updated":enrichment.updated,
                    "unavailable":enrichment.unavailable,
                    "failed":enrichment.failed
                }
            }))
        }
        "auto-ingester:buffer-drain" => {
            let result = process_buffer_batch(&services.database, 50)
                .await
                .map_err(|error| error.to_string())?;
            serde_json::to_value(result).map_err(|error| error.to_string())
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

async fn run_gap_check(services: &SchedulerServices) -> Result<Value, String> {
    let now = OffsetDateTime::now_utc();
    let min_date = (now - time::Duration::days(30)).date().to_string();
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
    let pipeline = CanonicalIngestPipeline::new(services.database.clone(), &services.config)
        .map_err(|error| error.to_string())?;
    let mut candidates = due
        .into_iter()
        .map(|(date, hour)| (486, date, hour, true))
        .collect::<Vec<_>>();
    for (date, hour) in expected_elapsed_discovery_hours(now) {
        for queue in MATCH_COUNT_QUEUE_DEFINITIONS
            .iter()
            .filter(|queue| queue.track_presence)
        {
            let state = services
                .database
                .one_json(
                    "SELECT status,next_retry_at<=now() retry_due,lease_until<=now() lease_due \
                     FROM hourly_ingest_state WHERE date=$1::DATE AND hour=$2 AND queue_id=$3",
                    &[&date, &hour, &queue.queue_id],
                )
                .await
                .map_err(|error| error.to_string())?;
            let retryable = state.as_ref().is_none_or(|row| {
                match row
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("pending")
                {
                    "complete" => false,
                    "empty" | "pending" | "failed" => row
                        .get("retry_due")
                        .and_then(Value::as_bool)
                        .unwrap_or(true),
                    "fetching" | "staged" => row
                        .get("lease_due")
                        .and_then(Value::as_bool)
                        .unwrap_or(true),
                    _ => true,
                }
            });
            if retryable
                && !candidates
                    .iter()
                    .any(|(candidate_queue, candidate_date, candidate_hour, _)| {
                        *candidate_queue == queue.queue_id
                            && candidate_date == &date
                            && *candidate_hour == hour
                    })
            {
                candidates.push((queue.queue_id, date.clone(), hour, false));
            }
        }
    }
    candidates.sort_by(|left, right| {
        left.1
            .cmp(&right.1)
            .then(left.2.cmp(&right.2))
            .then(left.0.cmp(&right.0))
    });
    let mut completed = 0;
    for (queue_id, date, hour, debt_only) in candidates.iter().take(limit) {
        if pipeline
            .discover_hour(*queue_id, date, *hour, "gap-checker", *debt_only)
            .await
            .is_ok()
        {
            completed += 1;
        }
    }
    Ok(
        json!({"candidates":candidates.len(),"attempted":candidates.len().min(limit),"completed":completed}),
    )
}

fn expected_elapsed_discovery_hours(now: OffsetDateTime) -> Vec<(String, i32)> {
    let latest_tick_hour = if now.minute() >= 30 {
        i32::from(now.hour())
    } else {
        i32::from(now.hour()) - 1
    };
    if latest_tick_hour < 0 {
        return Vec::new();
    }
    (0..=latest_tick_hour)
        .map(|fetch_hour| {
            let target = now
                .replace_hour(u8::try_from(fetch_hour).unwrap_or_default())
                .and_then(|value| value.replace_minute(30))
                .and_then(|value| value.replace_second(0))
                .and_then(|value| value.replace_nanosecond(0))
                .unwrap_or(now)
                - time::Duration::hours(1);
            (target.date().to_string(), i32::from(target.hour()))
        })
        .collect()
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
