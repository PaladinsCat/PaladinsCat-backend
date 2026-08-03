use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum DerivedProjectionSchedulerError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionTickResult {
    pub executed: bool,
    pub projections_updated: usize,
    pub lookback_days: u64,
}

pub async fn tick_derived_projections(
    database: &Database,
) -> Result<ProjectionTickResult, DerivedProjectionSchedulerError> {
    let last_run = database
        .one_json(
            "SELECT COALESCE(MAX(last_run_at),'1970-01-01'::timestamptz) AS last_run_at FROM scheduler_state WHERE job_key='derived_projections:refresh'",
            &[],
        )
        .await?;
    let last_run_secs = last_run
        .and_then(|value| value.get("last_run_at").and_then(Value::as_str).map(|s| s.parse::<u64>().unwrap_or(0)))
        .unwrap_or(0);
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let interval = 86400;
    let executed = (now_secs - last_run_secs) >= interval;
    let lookback = env_u64("DERIVED_PROJECTION_LOOKBACK_DAYS", 90);
    let projections_updated = if executed {
        let rows = database
            .query_json(
                "UPDATE derived_projections SET \
                 winrate=(SELECT COALESCE(AVG(winrate),0) FROM match_facts WHERE champion_id=derived_projections.champion_id AND entry_datetime >= (current_date - ($1::INT * INTERVAL '1 day'))),\
                 avg_score=(SELECT COALESCE(AVG(score),0) FROM match_facts WHERE champion_id=derived_projections.champion_id AND entry_datetime >= (current_date - ($1::INT * INTERVAL '1 day')),\
                 updated_at=now()",
                &[&i32::try_from(lookback).unwrap_or(90)],
            )
            .await?;
        database
            .query_json(
                "INSERT INTO scheduler_state(job_key, last_run_at) VALUES('derived_projections:refresh', now()) \
                 ON CONFLICT(job_key) DO UPDATE SET last_run_at = now()",
                &[],
            )
            .await?;
        rows.len()
    } else {
        0
    };
    Ok(ProjectionTickResult {
        executed,
        projections_updated,
        lookback_days: lookback,
    })
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
