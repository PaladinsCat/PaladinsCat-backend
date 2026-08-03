use std::collections::HashSet;

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum CompletedMatchBatchingError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchResult {
    pub batched: usize,
    pub skipped: usize,
    pub failed: usize,
}

pub async fn batch_completed_matches(
    database: &Database,
    limit: usize,
) -> Result<BatchResult, CompletedMatchBatchingError> {
    let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
    let rows = database
        .query_json(
            "SELECT match_id FROM match_facts \
             WHERE processed=false AND queue_id=486 ORDER BY entry_datetime ASC LIMIT $1",
            &[&limit_i64],
        )
        .await?;
    let mut ids: Vec<i64> = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        if let Some(id) = row.get("match_id").and_then(Value::as_i64).filter(|id| *id > 0 && seen.insert(*id)) {
            ids.push(id);
        }
    }
    let batched = ids.len();
    if !ids.is_empty() {
        database
            .query_json(
                "UPDATE match_facts SET processed=true, processed_at=now() WHERE match_id=ANY($1::BIGINT[])",
                &[&ids],
            )
            .await?;
    }
    Ok(BatchResult {
        batched,
        skipped: 0,
        failed: 0,
    })
}

pub async fn batch_nonranked_completed(
    database: &Database,
    limit: usize,
) -> Result<BatchResult, CompletedMatchBatchingError> {
    let limit_i64 = i64::try_from(limit).unwrap_or(i64::MAX);
    let rows = database
        .query_json(
            "SELECT match_id FROM match_facts \
             WHERE processed=false AND queue_id!=486 ORDER BY entry_datetime ASC LIMIT $1",
            &[&limit_i64],
        )
        .await?;
    let mut ids: Vec<i64> = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        if let Some(id) = row.get("match_id").and_then(Value::as_i64).filter(|id| *id > 0 && seen.insert(*id)) {
            ids.push(id);
        }
    }
    let batched = ids.len();
    if !ids.is_empty() {
        database
            .query_json(
                "UPDATE match_facts SET processed=true, processed_at=now() WHERE match_id=ANY($1::BIGINT[])",
                &[&ids],
            )
            .await?;
    }
    Ok(BatchResult {
        batched,
        skipped: 0,
        failed: 0,
    })
}
