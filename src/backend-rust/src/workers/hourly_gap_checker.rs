use std::collections::HashSet;

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum HourlyGapCheckerError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GapScanResult {
    pub gaps_found: usize,
    pub gaps: Vec<GapInfo>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GapInfo {
    pub date: String,
    pub hour: i32,
    pub queue_id: i32,
}

pub async fn scan_hourly_gaps(
    database: &Database,
    lookback_hours: i32,
) -> Result<GapScanResult, HourlyGapCheckerError> {
    // Align with policy::MATCH_COUNT_QUEUE_DEFINITIONS (all 12 queues) so this
    // path would not silently drop presence/ranked queues if it were wired up.
    let queue_ids: Vec<i32> = vec![424, 425, 452, 453, 469, 486, 10297, 10332, 10348, 10362, 10367, 10369];
    let params: Box<[&(dyn tokio_postgres::types::ToSql + Sync)]> = Box::new([&lookback_hours, &queue_ids]);
    let rows = database
        .query_json(
            "SELECT g.date::text, g.hour, q.queue_id \
             FROM generate_series(\
               date_trunc('hour', now() - ($1::INT * INTERVAL '1 hour')),\
               date_trunc('hour', now()), \
               INTERVAL '1 hour'\
             ) AS g(dt) \
             CROSS JOIN LATERAL (SELECT unnest($2::INT[]) AS queue_id) AS q \
             WHERE NOT EXISTS (\
               SELECT 1 FROM hourly_ingest_state h \
               WHERE h.date=g.dt::DATE AND h.hour=EXTRACT(HOUR FROM g.dt)::INT AND h.queue_id=q.queue_id \
               AND h.status IN('complete','staged','empty')\
             )",
            &params,
        )
        .await?;
    let mut gaps = Vec::new();
    let mut seen = HashSet::new();
    for row in rows {
        let date = row.get("date").and_then(Value::as_str).map(str::to_owned).unwrap_or_default();
        let hour = row.get("hour").and_then(|v| v.as_i64().and_then(|v| i32::try_from(v).ok())).unwrap_or(-1);
        let queue_id = row.get("queue_id").and_then(|v| v.as_i64().and_then(|v| i32::try_from(v).ok())).unwrap_or(0);
        let key = (date.clone(), hour, queue_id);
        if seen.insert(key) {
            gaps.push(GapInfo { date, hour, queue_id });
        }
    }
    Ok(GapScanResult {
        gaps_found: gaps.len(),
        gaps,
    })
}

pub async fn fill_gaps(
    database: &Database,
    gaps: &[GapInfo],
    source: &str,
) -> Result<usize, HourlyGapCheckerError> {
    let mut filled = 0;
    for gap in gaps {
        let claimed = database
            .query_json(
                "INSERT INTO hourly_ingest_state(date,hour,queue_id,status,source,updated_at)\
                 VALUES($1::TEXT::DATE,$2,$3,'pending',$4,now())\
                 ON CONFLICT(date,hour,queue_id) DO UPDATE SET status='pending',source=EXCLUDED.source,error_message=NULL,updated_at=now()\
                 WHERE hourly_ingest_state.status IN('pending','failed') RETURNING date",
                &[&gap.date, &gap.hour, &gap.queue_id, &source],
            )
            .await?;
        if !claimed.is_empty() {
            filled += 1;
        }
    }
    Ok(filled)
}
