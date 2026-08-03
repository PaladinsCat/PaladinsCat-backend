use std::collections::BTreeSet;

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum HourlyIngestMatchDebtError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchDebtSummary {
    pub pending: usize,
    pub staged: usize,
    pub complete: usize,
    pub unrecoverable: usize,
    pub total: usize,
}

pub async fn get_match_debt_summary(
    database: &Database,
    queue_id: i32,
) -> Result<MatchDebtSummary, HourlyIngestMatchDebtError> {
    let row = database
        .one_json(
            "SELECT \
             COUNT(*) FILTER(WHERE status='pending') AS pending,\
             COUNT(*) FILTER(WHERE status='staged') AS staged,\
             COUNT(*) FILTER(WHERE status='complete') AS complete,\
             COUNT(*) FILTER(WHERE status='unrecoverable') AS unrecoverable,\
             COUNT(*) AS total \
             FROM hourly_ingest_match_debt WHERE queue_id=$1",
            &[&queue_id],
        )
        .await?
        .unwrap_or(Value::Null);
    Ok(MatchDebtSummary {
        pending: value_usize(row.get("pending")),
        staged: value_usize(row.get("staged")),
        complete: value_usize(row.get("complete")),
        unrecoverable: value_usize(row.get("unrecoverable")),
        total: value_usize(row.get("total")),
    })
}

pub async fn record_match_debt(
    database: &Database,
    match_ids: &[i64],
    date: &str,
    hour: i32,
    queue_id: i32,
    reason: &str,
) -> Result<usize, HourlyIngestMatchDebtError> {
    let mut seen = BTreeSet::new();
    let mut count = 0;
    for match_id in match_ids {
        if *match_id > 0 && seen.insert(*match_id) {
            database
                .query_json(
                    "INSERT INTO hourly_ingest_match_debt(match_id, date, hour, queue_id, status, reason, updated_at)\
                     VALUES($1::BIGINT, $2::TEXT::DATE, $3, $4, 'pending', $5, now())\
                     ON CONFLICT(match_id) DO NOTHING",
                    &[match_id, &date, &hour, &queue_id, &reason],
                )
                .await?;
            count += 1;
        }
    }
    Ok(count)
}

pub async fn get_pending_debt_ids(
    database: &Database,
    queue_id: i32,
    limit: i32,
) -> Result<Vec<i64>, HourlyIngestMatchDebtError> {
    let rows = database
        .query_json(
            "SELECT match_id FROM hourly_ingest_match_debt \
             WHERE status='pending' AND queue_id=$1 ORDER BY first_seen_at ASC LIMIT $2 \
             FOR UPDATE SKIP LOCKED",
            &[&queue_id, &limit],
        )
        .await?;
    Ok(rows.into_iter()
        .filter_map(|row| row.get("match_id").and_then(Value::as_i64).filter(|id| *id > 0))
        .collect())
}

pub async fn mark_debt_complete(
    database: &Database,
    match_ids: &[i64],
) -> Result<usize, HourlyIngestMatchDebtError> {
    if match_ids.is_empty() {
        return Ok(0);
    }
    let ids_vec: Vec<i64> = match_ids.to_vec();
    let rows = database
        .query_json(
            "UPDATE hourly_ingest_match_debt SET status='complete', completed_at=now(), updated_at=now() \
             WHERE match_id=ANY($1::BIGINT[]) AND status='pending'",
            &[&ids_vec],
        )
        .await?;
    Ok(rows.len())
}

fn value_usize(value: Option<&Value>) -> usize {
    value
        .and_then(|value| value.as_u64().map(|v| v as usize).or_else(|| value.as_str()?.parse().ok()))
        .unwrap_or_default()
}
