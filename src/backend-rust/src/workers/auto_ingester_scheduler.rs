
use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum AutoIngesterSchedulerError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoIngesterTick {
    pub job_key: String,
    pub executed: bool,
    pub next_run_at: Option<String>,
}

pub async fn tick_auto_ingester(
    database: &Database,
    job_key: &str,
) -> Result<AutoIngesterTick, AutoIngesterSchedulerError> {
    let last_run = database
        .one_json(
            "SELECT COALESCE(MAX(last_run_at),'1970-01-01'::timestamptz) AS last_run_at FROM scheduler_state WHERE job_key=$1",
            &[&job_key],
        )
        .await?;
    let last_run_str = last_run
        .and_then(|value| value.get("last_run_at").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default();
    let last_run_secs = last_run_str
        .parse::<u64>()
        .unwrap_or(0);
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let interval = match job_key {
        "auto-ingester:discovery" => 3600,
        "auto-ingester:buffer-drain" => 300,
        "auto-ingester:raw-buffer-retention" => 3600,
        "auto-ingester:player-history-retention" => 3600,
        "auto-ingester:materialized-view-refresh" => 3600,
        "auto-ingester:drop-detection" => 900,
        "auto-ingester:profile-enrichment" => 3600,
        _ => 3600,
    };
    let executed = (now_secs - last_run_secs) >= interval;
    let next_run_at = if executed {
        database
            .query_json(
                "INSERT INTO scheduler_state(job_key, last_run_at) VALUES($1, now()) \
                 ON CONFLICT(job_key) DO UPDATE SET last_run_at = now()",
                &[&job_key],
            )
            .await?;
        Some(std::format!("{}", now_secs + interval))
    } else {
        None
    };
    Ok(AutoIngesterTick {
        job_key: job_key.to_owned(),
        executed,
        next_run_at,
    })
}

pub async fn get_auto_ingester_jobs(
    database: &Database,
) -> Result<Vec<String>, AutoIngesterSchedulerError> {
    let rows = database
        .query_json(
            "SELECT DISTINCT job_key FROM scheduler_state WHERE job_key LIKE 'auto-ingester:%' ORDER BY job_key",
            &[],
        )
        .await?;
    Ok(rows.into_iter()
        .filter_map(|row| row.get("job_key").and_then(Value::as_str).map(str::to_owned))
        .collect())
}
