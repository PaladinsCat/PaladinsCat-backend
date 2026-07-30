use std::time::Duration;

use paladinscat_core::database::DatabaseError;
use serde::Serialize;
use time::OffsetDateTime;
use tokio::task::JoinHandle;

use super::{
    coordination::WorkerCoordinationRepository, scheduler::scheduled_jobs_for,
    tier_stats::TierStatsRepository,
};

const OWNERSHIP_LEASE: Duration = Duration::from_secs(60);
const OWNERSHIP_HEARTBEAT: Duration = Duration::from_secs(15);
const JOB_LEASE: Duration = Duration::from_secs(60 * 60);
const TIER_STATS_SCHEDULER: &str = "tier_stats";
const TIER_STATS_JOB: &str = "tier-stats:refresh";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerRuntimeExit {
    Shutdown,
    OwnershipUnavailable,
    OwnershipLost,
}

pub async fn run_tier_stats_scheduler(
    coordination: WorkerCoordinationRepository,
    tier_stats: TierStatsRepository,
    owner_id: String,
) -> Result<SchedulerRuntimeExit, DatabaseError> {
    if !coordination
        .acquire_scheduler_owner(TIER_STATS_SCHEDULER, &owner_id, OWNERSHIP_LEASE)
        .await?
    {
        return Ok(SchedulerRuntimeExit::OwnershipUnavailable);
    }

    let schedule = scheduled_jobs_for(TIER_STATS_SCHEDULER)
        .find(|job| job.job_key == TIER_STATS_JOB)
        .expect("tier-stats schedule is fixed by the native inventory");
    let mut clock = tokio::time::interval(Duration::from_secs(1));
    clock.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat = tokio::time::interval(OWNERSHIP_HEARTBEAT);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;
    let mut shutdown = Box::pin(shutdown_signal());
    let mut last_started_minute = None;
    let mut active_job: Option<JoinHandle<()>> = None;
    let exit = loop {
        tokio::select! {
            _ = &mut shutdown => break SchedulerRuntimeExit::Shutdown,
            _ = heartbeat.tick() => {
                let retained = coordination
                    .heartbeat_scheduler_owner(
                        TIER_STATS_SCHEDULER,
                        &owner_id,
                        OWNERSHIP_LEASE,
                    )
                    .await
                    .unwrap_or(false);
                if !retained {
                    break SchedulerRuntimeExit::OwnershipLost;
                }
            }
            _ = clock.tick() => {
        if active_job.as_ref().is_some_and(JoinHandle::is_finished)
            && let Some(completed) = active_job.take()
        {
            let _ = completed.await;
        }
                let now = OffsetDateTime::now_utc();
                let minute_slot = now.unix_timestamp() / 60;
                if active_job.is_none()
                    && last_started_minute != Some(minute_slot)
                    && schedule.is_due(now)
                {
                    last_started_minute = Some(minute_slot);
                    let job_coordination = coordination.clone();
                    let job_repository = tier_stats.clone();
                    let job_owner = owner_id.clone();
                    active_job = Some(tokio::spawn(async move {
                        execute_tier_stats_job(
                            job_coordination,
                            job_repository,
                            job_owner,
                        )
                        .await;
                    }));
                }
            }
        }
    };

    if let Some(active) = active_job {
        let _ = active.await;
    }
    let _ = coordination
        .release_scheduler_owner(TIER_STATS_SCHEDULER, &owner_id)
        .await;
    Ok(exit)
}

async fn execute_tier_stats_job(
    coordination: WorkerCoordinationRepository,
    tier_stats: TierStatsRepository,
    owner_id: String,
) {
    let lease = match coordination
        .acquire_job(TIER_STATS_JOB, TIER_STATS_SCHEDULER, &owner_id, JOB_LEASE)
        .await
    {
        Ok(Some(lease)) => lease,
        Ok(None) => return,
        Err(error) => {
            tracing::error!(job = TIER_STATS_JOB, %error, "job lease failed");
            return;
        }
    };
    let run_id = match coordination.start_run(&lease, "cron").await {
        Ok(run_id) => run_id,
        Err(error) => {
            tracing::error!(job = TIER_STATS_JOB, %error, "job run record failed");
            let _ = coordination.release_job(&lease).await;
            return;
        }
    };
    let summary = tier_stats.refresh().await;
    let status = if summary.errors.is_empty() {
        "completed"
    } else {
        "failed"
    };
    let result = serde_json::to_value(&summary).ok();
    let error_message = (!summary.errors.is_empty()).then(|| summary.errors.join("; "));
    if let Err(error) = coordination
        .finish_run(run_id, status, result.as_ref(), error_message.as_deref())
        .await
    {
        tracing::error!(job = TIER_STATS_JOB, %error, "job completion record failed");
    }
    let _ = coordination.release_job(&lease).await;
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {},
        _ = terminate.recv() => {},
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
