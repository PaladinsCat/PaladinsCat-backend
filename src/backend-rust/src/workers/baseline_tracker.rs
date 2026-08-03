use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum BaselineTrackerError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error("baseline refresh failed: {0}")]
    Refresh(String),
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BaselineRefreshResult {
    pub champions_updated: usize,
    pub lookback_days: u64,
    pub queue_id: i32,
}

pub async fn refresh_baselines(
    database: &Database,
    queue_id: i32,
) -> Result<BaselineRefreshResult, BaselineTrackerError> {
    let lookback = env_u64("BASELINE_REFRESH_LOOKBACK_DAYS", 90);
    let rows = database
        .query_json(
            "UPDATE champion_baselines b SET \
             avg_score=(SELECT AVG(m.score) FROM match_facts m WHERE m.champion_id=b.champion_id AND m.queue_id=$1 AND m.entry_datetime >= (current_date - ($2::INT * INTERVAL '1 day'))),\
             avg_winrate=(SELECT AVG(m.winrate) FROM match_facts m WHERE m.champion_id=b.champion_id AND m.queue_id=$1 AND m.entry_datetime >= (current_date - ($2::INT * INTERVAL '1 day'))),\
             match_count=(SELECT COUNT(*) FROM match_facts m WHERE m.champion_id=b.champion_id AND m.queue_id=$1 AND m.entry_datetime >= (current_date - ($2::INT * INTERVAL '1 day'))),\
             updated_at=now()\
             WHERE b.queue_id=$1",
            &[&queue_id, &i32::try_from(lookback).unwrap_or(90)],
        )
        .await?;
    Ok(BaselineRefreshResult {
        champions_updated: rows.len(),
        lookback_days: lookback,
        queue_id,
    })
}

pub async fn get_baselines(
    database: &Database,
    queue_id: i32,
) -> Result<Vec<Value>, BaselineTrackerError> {
    let rows = database
        .query_json(
            "SELECT * FROM champion_baselines WHERE queue_id=$1 ORDER BY champion_id",
            &[&queue_id],
        )
        .await?;
    Ok(rows)
}

pub async fn recalculate_champion_baselines(
    database: &Database,
) -> Result<BaselineRefreshResult, BaselineTrackerError> {
    refresh_baselines(database, 486).await
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
