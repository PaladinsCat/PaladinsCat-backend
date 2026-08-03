
use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;

use super::baseline_tracker;

#[derive(Debug, thiserror::Error)]
pub enum BaselineSchedulerError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Baseline(#[from] baseline_tracker::BaselineTrackerError),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BaselineSchedulerTickResult {
    pub executed: bool,
    pub result: Option<baseline_tracker::BaselineRefreshResult>,
}

pub async fn schedule_baseline_refresh(
    database: &Database,
) -> Result<BaselineSchedulerTickResult, BaselineSchedulerError> {
    let last_run = database
        .one_json(
            "SELECT COALESCE(MAX(updated_at),'1970-01-01'::timestamptz) FROM scheduler_state WHERE job_key='baseline_tracker:refresh'",
            &[],
        )
        .await?;
    let last_run = last_run
        .and_then(|value| value.as_str().map(str::to_owned))
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let interval = env_u64("BASELINE_SCHEDULER_INTERVAL_SECS", 86400);
    let executed = (now - last_run) >= interval;
    let result = if executed {
        let result = baseline_tracker::recalculate_champion_baselines(database).await?;
        database
            .query_json(
                "INSERT INTO scheduler_state(job_key, last_run_at) VALUES('baseline_tracker:refresh', now()) \
                 ON CONFLICT(job_key) DO UPDATE SET last_run_at = now()",
                &[],
            )
            .await?;
        Some(result)
    } else {
        None
    };
    Ok(BaselineSchedulerTickResult { executed, result })
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
