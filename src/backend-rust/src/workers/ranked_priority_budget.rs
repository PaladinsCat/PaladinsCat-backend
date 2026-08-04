use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum RankedPriorityBudgetError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RankedPriorityBudget {
    pub due_ranked_matches: u64,
    pub configured_hourly_floor: u64,
    pub observed_hourly_peak: u64,
    pub protected_ranked_matches: u64,
    pub calls_per_match: u64,
    pub discovery_calls: u64,
    pub reserved_calls: u64,
}

pub async fn calculate_priority_budget(
    database: &Database,
) -> Result<RankedPriorityBudget, RankedPriorityBudgetError> {
    let lookback = env_u64("RANKED_PRIORITY_PEAK_LOOKBACK_DAYS", 30);
    let row = database
        .one_json(
            "SELECT (SELECT COUNT(*) FROM hourly_ingest_match_debt WHERE queue_id=486 AND status='pending') AS pending_ranked_matches,\
             (SELECT COALESCE(MAX(raw_match_count),0) FROM hourly_ingest_state WHERE queue_id=486 \
               AND date>=current_date-($1::INT*INTERVAL '1 day')) AS observed_hourly_peak",
            &[&i32::try_from(lookback).unwrap_or(30)],
        )
        .await?
        .unwrap_or(Value::Null);
    let due = value_u64(row.get("pending_ranked_matches"));
    let observed_peak = value_u64(row.get("observed_hourly_peak"));
    let floor = env_u64("RANKED_PRIORITY_MAX_MATCHES_PER_HOUR", 75);
    let calls_per_match = env_u64("RANKED_PRIORITY_CALLS_PER_MATCH", 13);
    let discovery_calls = env_u64("RANKED_PRIORITY_DISCOVERY_CALLS", 1);
    let floor = floor.max(1);
    let calls = calls_per_match.max(1);
    let discovery = discovery_calls.max(1);
    let protected = floor.max(observed_peak).max(due);
    Ok(RankedPriorityBudget {
        due_ranked_matches: due,
        configured_hourly_floor: floor,
        observed_hourly_peak: observed_peak,
        protected_ranked_matches: protected,
        calls_per_match: calls,
        discovery_calls: discovery,
        reserved_calls: discovery.saturating_add(protected.saturating_mul(calls)),
    })
}

pub async fn get_background_allowance(
    database: &Database,
    worst_case_calls_per_match: u64,
) -> Result<u64, RankedPriorityBudgetError> {
    let reserve = std::env::var("API_KEY_RESERVE_CALLS")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(100)
        .max(0);
    let budget = calculate_priority_budget(database).await?;
    let row = database
        .one_json(
            "SELECT COALESCE(SUM(GREATEST(daily_limit-total_24h-$1,0)),0) AS total_usable FROM api_keys \
             WHERE status NOT IN('limited','unhealthy','exhausted')",
            &[&reserve],
        )
        .await?
        .unwrap_or(Value::Null);
    let usable = value_u64(row.get("total_usable"));
    Ok(usable.saturating_sub(budget.reserved_calls) / worst_case_calls_per_match.max(1))
}

fn value_u64(value: Option<&Value>) -> u64 {
    value
        .and_then(|value| value.as_u64().or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}

fn env_u64(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
