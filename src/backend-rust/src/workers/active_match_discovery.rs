use std::collections::HashSet;

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum ActiveMatchDiscoveryError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveMatch {
    pub match_id: i64,
    pub entry_datetime: String,
    pub region: String,
    pub queue_id: i32,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActiveDiscoveryResult {
    pub active_matches: Vec<ActiveMatch>,
    pub total_discovered: usize,
}

pub async fn discover_active_matches(
    database: &Database,
    queue_id: i32,
) -> Result<ActiveDiscoveryResult, ActiveMatchDiscoveryError> {
    let rows = database
        .query_json(
            "SELECT match_id, entry_datetime::text, region, queue_id \
             FROM match_presence WHERE active_flag=true AND queue_id=$1 ORDER BY entry_datetime DESC",
            &[&queue_id],
        )
        .await?;
    let mut matches = Vec::new();
    for row in rows {
        let match_id = row.get("match_id").and_then(|v| v.as_i64()).unwrap_or(0);
        if match_id <= 0 {
            continue;
        }
        matches.push(ActiveMatch {
            match_id,
            entry_datetime: row
                .get("entry_datetime")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_default(),
            region: row
                .get("region")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .unwrap_or_default(),
            queue_id: row
                .get("queue_id")
                .and_then(|v| v.as_i64().and_then(|v| i32::try_from(v).ok()))
                .unwrap_or(queue_id),
        });
    }
    let total = matches.len();
    Ok(ActiveDiscoveryResult {
        active_matches: matches,
        total_discovered: total,
    })
}

pub async fn record_active_matches(
    database: &Database,
    matches: &[ActiveMatch],
) -> Result<usize, ActiveMatchDiscoveryError> {
    let mut seen = HashSet::new();
    let mut count = 0;
    for match_data in matches {
        let id = match_data.match_id;
        if id > 0 && seen.insert(id) {
            count += 1;
            database
                .query_json(
                    "INSERT INTO match_presence(match_id, entry_datetime, region, queue_id, active_flag) \
                     VALUES($1::BIGINT, $2::TEXT::TIMESTAMPTZ, $3, $4, true) \
                     ON CONFLICT(match_id) DO UPDATE SET active_flag=true, updated_at=now()",
                    &[
                        &id,
                        &match_data.entry_datetime,
                        &match_data.region,
                        &match_data.queue_id,
                    ],
                )
                .await?;
        }
    }
    Ok(count)
}

pub async fn clear_inactive_matches(
    database: &Database,
    grace_minutes: i32,
) -> Result<usize, ActiveMatchDiscoveryError> {
    let rows = database
        .query_json(
            "DELETE FROM match_presence WHERE active_flag=true \
             AND entry_datetime <= now() - ($1::INT * INTERVAL '1 minute') \
             RETURNING match_id",
            &[&grace_minutes],
        )
        .await?;
    Ok(rows.len())
}
