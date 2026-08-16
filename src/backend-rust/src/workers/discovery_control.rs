use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

use super::match_lifecycle::TERMINAL_NO_COMPLETED_MATCH_REASON;

const FETCH_LEASE_MINUTES: i32 = 5;

/// Purpose: define the single database-first rule for reopening a queue-hour.
/// Input: the PostgreSQL parameter containing the explicit terminal reason.
/// Output: an `EXISTS` predicate against the caller's `state` alias; both gap
/// selection and atomic claim reuse it so missing lifecycle rows cannot be
/// classified differently at the two boundaries.
pub fn reopenable_hour_predicate(terminal_reason_parameter: &str) -> String {
    format!(
        "EXISTS(SELECT 1 FROM match_count_discoveries discovery \
         LEFT JOIN match_ingest_status ingest ON ingest.match_id=discovery.match_id \
         WHERE discovery.source_date=state.date AND discovery.source_hour=state.hour \
           AND discovery.queue_id=state.queue_id AND (ingest.match_id IS NULL \
             OR ingest.status NOT IN('complete','limited') \
             OR (ingest.status='limited' AND (ingest.acquisition_state IS DISTINCT FROM 'unavailable' \
               OR ingest.error_message IS DISTINCT FROM {terminal_reason_parameter}))))"
    )
}

#[derive(Clone, Debug, Serialize)]
pub struct HourlyIngestState {
    pub date: String,
    pub hour: i32,
    pub queue_id: i32,
    pub status: String,
    pub attempts: i32,
    pub raw_match_count: i32,
    pub staged_match_count: i32,
    pub fetched: bool,
    pub fetch_succeeded: bool,
    pub source: Option<String>,
    pub error_message: Option<String>,
    pub last_attempt_at: Option<String>,
    pub next_retry_at: Option<String>,
    pub lease_until: Option<String>,
    pub completed_at: Option<String>,
}

/// Purpose: ensure the single queue-hour ownership ledger exists. Input:
/// database handle. Output: ready schema or typed database error; no work item.
pub async fn ensure_hourly_ingest_tables(database: &Database) -> Result<(), DatabaseError> {
    database.query_json(
        "CREATE TABLE IF NOT EXISTS hourly_ingest_state(\
          date DATE NOT NULL,hour INT NOT NULL CHECK(hour>=0 AND hour<=23),queue_id INT NOT NULL,\
          status VARCHAR(20) NOT NULL DEFAULT 'fetching' CHECK(status IN('fetching','empty','complete','failed')),\
          attempts INT NOT NULL DEFAULT 0,raw_match_count INT NOT NULL DEFAULT 0,staged_match_count INT NOT NULL DEFAULT 0,\
          fetched BOOLEAN NOT NULL DEFAULT FALSE,fetch_succeeded BOOLEAN NOT NULL DEFAULT FALSE,source VARCHAR(50),\
          error_message TEXT,last_attempt_at TIMESTAMPTZ,next_retry_at TIMESTAMPTZ,lease_until TIMESTAMPTZ,\
          completed_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),\
          PRIMARY KEY(date,hour,queue_id))",
        &[],
    ).await?;
    database.query_json("CREATE INDEX IF NOT EXISTS idx_his_status_retry ON hourly_ingest_state(status,next_retry_at,lease_until)", &[]).await?;
    database.query_json("CREATE INDEX IF NOT EXISTS idx_his_queue_window ON hourly_ingest_state(queue_id,date,hour)", &[]).await?;
    Ok(())
}

/// Purpose: acquire exclusive execution for one queue-hour. Input: database,
/// UTC date/hour, queue ID, audit source. Output: `true` only for the owner that
/// must execute now; failed hours have no cooldown and completed hours stay final.
pub async fn claim_hourly_ingest_hour(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    source: &str,
) -> Result<bool, DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    let reopenable = reopenable_hour_predicate("$6");
    let claim_sql = format!(
        "INSERT INTO hourly_ingest_state AS state(date,hour,queue_id,status,attempts,fetched,fetch_succeeded,source,error_message,last_attempt_at,next_retry_at,lease_until,updated_at)\
         VALUES($1::TEXT::DATE,$2,$3,'fetching',1,FALSE,FALSE,$4,NULL,now(),NULL,now()+($5::INT*INTERVAL '1 minute'),now())\
         ON CONFLICT(date,hour,queue_id) DO UPDATE SET status='fetching',attempts=state.attempts+1,\
         fetched=FALSE,fetch_succeeded=FALSE,source=EXCLUDED.source,error_message=NULL,last_attempt_at=now(),next_retry_at=NULL,\
         lease_until=now()+($5::INT*INTERVAL '1 minute'),updated_at=now() WHERE \
         state.status='failed' OR (state.status='complete' AND {reopenable}) OR \
         (state.status='fetching' AND (state.lease_until IS NULL OR state.lease_until<=now())) RETURNING date"
    );
    let rows = database
        .query_json(
            &claim_sql,
            &[
                &date,
                &hour,
                &queue_id,
                &source,
                &FETCH_LEASE_MINUTES,
                &TERMINAL_NO_COMPLETED_MATCH_REASON,
            ],
        )
        .await?;
    Ok(!rows.is_empty())
}

/// Purpose: preserve exclusive queue-hour ownership while its complete ID set
/// is actively draining. Input: database and queue-hour key. Output: updated
/// lease row count; this creates no pending state, retry delay, or new work.
pub async fn refresh_hourly_ingest_lease(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
) -> Result<(), DatabaseError> {
    database
        .query_json(
            "UPDATE hourly_ingest_state SET lease_until=now()+($4::INT*INTERVAL '1 minute'),updated_at=now() \
             WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3 AND status='fetching'",
            &[&date, &hour, &queue_id, &FETCH_LEASE_MINUTES],
        )
        .await?;
    Ok(())
}

/// Purpose: close an authoritative discovery that returned zero IDs. Input:
/// database and queue-hour key. Output: durable empty completion, never a retry.
pub async fn mark_hourly_ingest_empty(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
) -> Result<(), DatabaseError> {
    database.query_json(
        "UPDATE hourly_ingest_state SET status='empty',raw_match_count=0,staged_match_count=0,fetched=TRUE,\
         fetch_succeeded=TRUE,error_message=NULL,lease_until=NULL,next_retry_at=NULL,completed_at=now(),updated_at=now()\
         WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
        &[&date, &hour, &queue_id],
    ).await?;
    database.query_json(
        "INSERT INTO hourly_match_counts(date,hour,queue_id,total_matches,fetched_at) VALUES($1::TEXT::DATE,$2,$3,0,now())\
         ON CONFLICT(date,hour,queue_id) DO NOTHING",
        &[&date, &hour, &queue_id],
    ).await?;
    Ok(())
}

/// Purpose: close an hour after every discovered ID is durable. Input: database,
/// queue-hour key and total ID count. Output: completed state with no handoff.
pub async fn mark_hourly_ingest_complete(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    total_matches: i32,
) -> Result<(), DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    database.query_json(
        "UPDATE hourly_ingest_state SET status='complete',raw_match_count=GREATEST(raw_match_count,$4),\
         staged_match_count=GREATEST(staged_match_count,$4),fetched=TRUE,fetch_succeeded=TRUE,\
         error_message=NULL,lease_until=NULL,next_retry_at=NULL,completed_at=now(),updated_at=now()\
         WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
        &[&date, &hour, &queue_id, &total_matches],
    ).await?;
    Ok(())
}

/// Purpose: expose an immediate pipeline failure for the same hour to reclaim.
/// Input: queue-hour key, message, optional observed/durable counts. Output:
/// failed state with no cooldown timestamp or separate match-ID ledger.
pub async fn mark_hourly_ingest_failed(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    message: &str,
    raw_count: Option<i32>,
    staged_count: Option<i32>,
) -> Result<(), DatabaseError> {
    database.query_json(
        "UPDATE hourly_ingest_state SET status='failed',raw_match_count=GREATEST(raw_match_count,COALESCE($5,raw_match_count)),\
         staged_match_count=GREATEST(staged_match_count,COALESCE($6,staged_match_count)),fetched=TRUE,fetch_succeeded=FALSE,\
         error_message=$4,lease_until=NULL,next_retry_at=NULL,updated_at=now()\
         WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
        &[&date, &hour, &queue_id, &message, &raw_count, &staged_count],
    ).await?;
    Ok(())
}

/// Purpose: read queue-hour state for operations and gap calculation. Input:
/// queue and inclusive date range. Output: typed state rows from PostgreSQL.
pub async fn hourly_ingest_states(
    database: &Database,
    queue_id: i32,
    min_date: &str,
    max_date: &str,
) -> Result<Vec<HourlyIngestState>, DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    Ok(database.query_json(
        "SELECT date::TEXT,hour,queue_id,status,attempts,raw_match_count,staged_match_count,fetched,fetch_succeeded,\
         source,error_message,last_attempt_at::TEXT,next_retry_at::TEXT,lease_until::TEXT,completed_at::TEXT \
         FROM hourly_ingest_state WHERE queue_id=$1 AND date>=$2::TEXT::DATE AND date<=$3::TEXT::DATE",
        &[&queue_id, &min_date, &max_date],
    ).await?.into_iter().map(map_state).collect())
}

/// Purpose: convert one database JSON row to the typed state model. Input:
/// owned JSON value. Output: `HourlyIngestState` with bounded numeric fields.
fn map_state(row: Value) -> HourlyIngestState {
    HourlyIngestState {
        date: text(&row, "date").unwrap_or_default().to_owned(),
        hour: integer(&row, "hour")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        queue_id: integer(&row, "queue_id")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        status: text(&row, "status").unwrap_or_default().to_owned(),
        attempts: integer(&row, "attempts")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        raw_match_count: integer(&row, "raw_match_count")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        staged_match_count: integer(&row, "staged_match_count")
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or_default(),
        fetched: row.get("fetched").and_then(Value::as_bool).unwrap_or(false),
        fetch_succeeded: row
            .get("fetch_succeeded")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        source: text(&row, "source").map(str::to_owned),
        error_message: text(&row, "error_message").map(str::to_owned),
        last_attempt_at: text(&row, "last_attempt_at").map(str::to_owned),
        next_retry_at: text(&row, "next_retry_at").map(str::to_owned),
        lease_until: text(&row, "lease_until").map(str::to_owned),
        completed_at: text(&row, "completed_at").map(str::to_owned),
    }
}

/// Purpose: read one optional string field. Input: JSON row/key. Output: borrow.
fn text<'a>(row: &'a Value, key: &str) -> Option<&'a str> {
    row.get(key).and_then(Value::as_str)
}
/// Purpose: normalize one JSON integer representation. Input: row/key. Output:
/// optional `i64` whether PostgreSQL emitted a JSON number or numeric string.
fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
#[cfg(test)]
mod tests {
    use super::reopenable_hour_predicate;

    #[test]
    fn failed_hours_have_no_cooldown_or_match_debt_gate() {
        let source = include_str!("discovery_control.rs")
            .split_once("#[cfg(test)]")
            .expect("worker source has tests")
            .0;
        let reopenable = reopenable_hour_predicate("$6");
        assert!(source.contains("state.status='failed'"));
        assert!(source.contains("state.status='complete' AND {reopenable}"));
        assert!(reopenable.contains("ingest.match_id IS NULL"));
        assert!(reopenable.contains("ingest.status NOT IN('complete','limited')"));
        assert!(reopenable.contains("ingest.error_message IS DISTINCT FROM $6"));
        assert!(!source.contains("status='pending'"));
        assert!(!source.contains("status='staged'"));
        assert!(!source.contains("hourly_ingest_match_debt"));
        assert!(!source.contains("HOURLY_INGEST_MATCH_DEBT_RETRY_MINUTES"));
        assert!(!source.contains("HOURLY_INGEST_BUDGET_RETRY_MINUTES"));
    }

    #[test]
    fn hour_completion_is_direct_after_the_canonical_drain() {
        let source = include_str!("discovery_control.rs");
        let completion = source
            .split_once("pub async fn mark_hourly_ingest_complete")
            .expect("completion function")
            .1
            .split_once("pub async fn mark_hourly_ingest_failed")
            .expect("next function")
            .0;
        assert!(completion.contains("status='complete'"));
        assert!(!completion.contains("pending"));
        assert!(!completion.contains("debt"));
    }
}
