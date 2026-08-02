use std::collections::BTreeSet;

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

const FETCH_LEASE_MINUTES: i32 = 30;
const STAGED_LEASE_MINUTES: i32 = 60;
const FAILED_RETRY_MINUTES: i32 = 30;

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

pub async fn ensure_hourly_ingest_tables(database: &Database) -> Result<(), DatabaseError> {
    database.query_json(
        "CREATE TABLE IF NOT EXISTS hourly_ingest_state(\
          date DATE NOT NULL,hour INT NOT NULL CHECK(hour>=0 AND hour<=23),queue_id INT NOT NULL,\
          status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK(status IN('pending','fetching','staged','empty','complete','failed')),\
          attempts INT NOT NULL DEFAULT 0,raw_match_count INT NOT NULL DEFAULT 0,staged_match_count INT NOT NULL DEFAULT 0,\
          fetched BOOLEAN NOT NULL DEFAULT FALSE,fetch_succeeded BOOLEAN NOT NULL DEFAULT FALSE,source VARCHAR(50),\
          error_message TEXT,last_attempt_at TIMESTAMPTZ,next_retry_at TIMESTAMPTZ,lease_until TIMESTAMPTZ,\
          completed_at TIMESTAMPTZ,created_at TIMESTAMPTZ NOT NULL DEFAULT now(),updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),\
          PRIMARY KEY(date,hour,queue_id))",
        &[],
    ).await?;
    database.query_json("CREATE INDEX IF NOT EXISTS idx_his_status_retry ON hourly_ingest_state(status,next_retry_at,lease_until)", &[]).await?;
    database.query_json("CREATE INDEX IF NOT EXISTS idx_his_queue_window ON hourly_ingest_state(queue_id,date,hour)", &[]).await?;
    database.query_json(
        "CREATE TABLE IF NOT EXISTS hourly_ingest_match_debt(\
          match_id BIGINT PRIMARY KEY,date DATE NOT NULL,hour INT NOT NULL CHECK(hour>=0 AND hour<=23),queue_id INT NOT NULL,\
          status VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK(status IN('pending','staged','complete','unrecoverable')),\
          reason TEXT,attempts INT NOT NULL DEFAULT 0,first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),last_attempt_at TIMESTAMPTZ,\
          next_retry_at TIMESTAMPTZ,staged_at TIMESTAMPTZ,completed_at TIMESTAMPTZ,updated_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        &[],
    ).await?;
    database.query_json("CREATE INDEX IF NOT EXISTS idx_himd_queue_window_status ON hourly_ingest_match_debt(queue_id,date,hour,status)", &[]).await?;
    database.query_json("CREATE INDEX IF NOT EXISTS idx_himd_pending_retry ON hourly_ingest_match_debt(status,next_retry_at,updated_at) WHERE status='pending'", &[]).await?;
    Ok(())
}

pub async fn record_hourly_ingest_quota_wait(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    source: &str,
    reason: &str,
) -> Result<(), DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    let retry = env_i32("HOURLY_INGEST_QUOTA_WAIT_RETRY_MINUTES", 15).max(5);
    database.query_json(
        "INSERT INTO hourly_ingest_state(date,hour,queue_id,status,attempts,raw_match_count,staged_match_count,\
         fetched,fetch_succeeded,source,error_message,last_attempt_at,next_retry_at,lease_until,updated_at)\
         VALUES($1::TEXT::DATE,$2,$3,'pending',0,0,0,FALSE,FALSE,$4,$5,NULL,now()+($6*INTERVAL '1 minute'),NULL,now())\
         ON CONFLICT(date,hour,queue_id) DO NOTHING",
        &[&date, &hour, &queue_id, &source, &reason, &retry],
    ).await?;
    Ok(())
}

pub async fn claim_hourly_ingest_hour(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    source: &str,
    allow_due_debt_retry: bool,
) -> Result<bool, DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    let rows = database.query_json(
        "INSERT INTO hourly_ingest_state(date,hour,queue_id,status,attempts,fetched,fetch_succeeded,source,error_message,last_attempt_at,next_retry_at,lease_until,updated_at)\
         VALUES($1::TEXT::DATE,$2,$3,'fetching',1,FALSE,FALSE,$4,NULL,now(),NULL,now()+($5*INTERVAL '1 minute'),now())\
         ON CONFLICT(date,hour,queue_id) DO UPDATE SET status='fetching',attempts=hourly_ingest_state.attempts+1,\
         fetched=FALSE,fetch_succeeded=FALSE,source=EXCLUDED.source,error_message=NULL,last_attempt_at=now(),next_retry_at=NULL,\
         lease_until=now()+($5*INTERVAL '1 minute'),updated_at=now() WHERE \
         (hourly_ingest_state.status IN('pending','failed') AND (hourly_ingest_state.next_retry_at IS NULL OR hourly_ingest_state.next_retry_at<=now())) OR \
         (hourly_ingest_state.status IN('fetching','staged') AND (hourly_ingest_state.lease_until IS NULL OR hourly_ingest_state.lease_until<=now())) OR \
         (hourly_ingest_state.status='empty' AND (hourly_ingest_state.next_retry_at IS NULL OR hourly_ingest_state.next_retry_at<=now())) OR \
         ($6 AND hourly_ingest_state.status IN('pending','failed','staged','complete')) RETURNING date",
        &[&date, &hour, &queue_id, &source, &FETCH_LEASE_MINUTES, &allow_due_debt_retry],
    ).await?;
    Ok(!rows.is_empty())
}

pub async fn mark_hourly_ingest_empty(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
) -> Result<(), DatabaseError> {
    let fast = 360_i32;
    let slow = 1_440_i32;
    database.query_json(
        "UPDATE hourly_ingest_state SET status='empty',raw_match_count=0,staged_match_count=0,fetched=TRUE,\
         fetch_succeeded=TRUE,error_message=NULL,lease_until=NULL,next_retry_at=now()+(CASE WHEN attempts>=3 THEN $4 ELSE $5 END*INTERVAL '1 minute'),updated_at=now()\
         WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
        &[&date, &hour, &queue_id, &slow, &fast],
    ).await?;
    database.query_json(
        "INSERT INTO hourly_match_counts(date,hour,queue_id,total_matches,fetched_at) VALUES($1::TEXT::DATE,$2,$3,0,now())\
         ON CONFLICT(date,hour,queue_id) DO NOTHING",
        &[&date, &hour, &queue_id],
    ).await?;
    Ok(())
}

pub async fn mark_hourly_ingest_staged(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    raw_count: i32,
    staged_count: i32,
) -> Result<(), DatabaseError> {
    database.query_json(
        "UPDATE hourly_ingest_state SET status='staged',raw_match_count=GREATEST(raw_match_count,$4),\
         staged_match_count=GREATEST(staged_match_count,$5),fetched=TRUE,fetch_succeeded=TRUE,error_message=NULL,\
         lease_until=now()+($6*INTERVAL '1 minute'),next_retry_at=NULL,updated_at=now() \
         WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
        &[&date, &hour, &queue_id, &raw_count, &staged_count, &STAGED_LEASE_MINUTES],
    ).await?;
    Ok(())
}

pub async fn mark_hourly_ingest_complete(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    total_matches: i32,
) -> Result<(), DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    database.query_json(
        "WITH terminal_debt AS(SELECT count(*)::INT terminal_count FROM hourly_ingest_match_debt \
         WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3 AND status='unrecoverable')\
         UPDATE hourly_ingest_state SET status='complete',raw_match_count=GREATEST(raw_match_count,$4),\
         staged_match_count=GREATEST(staged_match_count,LEAST(GREATEST(raw_match_count,$4),$4+terminal_debt.terminal_count)),\
         fetched=TRUE,fetch_succeeded=TRUE,error_message=NULL,lease_until=NULL,next_retry_at=NULL,completed_at=now(),updated_at=now()\
         FROM terminal_debt WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3 \
         AND (raw_match_count=0 OR $4>=raw_match_count OR $4+terminal_debt.terminal_count>=raw_match_count OR status='complete')\
         AND NOT EXISTS(SELECT 1 FROM hourly_ingest_match_debt debt WHERE debt.date=$1::TEXT::DATE AND debt.hour=$2 \
           AND debt.queue_id=$3 AND debt.status IN('pending','staged'))",
        &[&date, &hour, &queue_id, &total_matches],
    ).await?;
    Ok(())
}

pub async fn mark_hourly_ingest_failed(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    message: &str,
    raw_count: Option<i32>,
    staged_count: Option<i32>,
) -> Result<(), DatabaseError> {
    let budget_retry = env_i32("HOURLY_INGEST_BUDGET_RETRY_MINUTES", 60).max(FAILED_RETRY_MINUTES);
    let retry = if RegexLike::budget(message) {
        budget_retry
    } else {
        FAILED_RETRY_MINUTES
    };
    database.query_json(
        "UPDATE hourly_ingest_state SET status='failed',raw_match_count=GREATEST(raw_match_count,COALESCE($6,raw_match_count)),\
         staged_match_count=GREATEST(staged_match_count,COALESCE($7,staged_match_count)),fetched=TRUE,fetch_succeeded=FALSE,\
         error_message=$4,lease_until=NULL,next_retry_at=now()+($5*INTERVAL '1 minute'),updated_at=now()\
         WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3",
        &[&date, &hour, &queue_id, &message, &retry, &raw_count, &staged_count],
    ).await?;
    Ok(())
}

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

pub async fn record_discovered_matches(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    match_ids: &[i64],
    reason: &str,
) -> Result<(), DatabaseError> {
    let ids = normalized_ids(match_ids);
    if ids.is_empty() {
        return Ok(());
    }
    ensure_hourly_ingest_tables(database).await?;
    let retry = env_i32("HOURLY_INGEST_MATCH_DEBT_RETRY_MINUTES", 10).max(5);
    database.query_json(
        "INSERT INTO hourly_ingest_match_debt(match_id,date,hour,queue_id,status,reason,attempts,first_seen_at,last_attempt_at,next_retry_at,updated_at)\
         SELECT id,$1::TEXT::DATE,$2,$3,'pending',$5,1,now(),now(),now()+($6*INTERVAL '1 minute'),now() FROM unnest($4::BIGINT[]) ids(id)\
         ON CONFLICT(match_id) DO UPDATE SET date=EXCLUDED.date,hour=EXCLUDED.hour,queue_id=EXCLUDED.queue_id,\
         status=CASE WHEN hourly_ingest_match_debt.status IN('complete','unrecoverable') THEN hourly_ingest_match_debt.status ELSE 'pending' END,\
         reason=CASE WHEN hourly_ingest_match_debt.status IN('complete','unrecoverable') THEN hourly_ingest_match_debt.reason ELSE EXCLUDED.reason END,\
         attempts=CASE WHEN hourly_ingest_match_debt.status IN('complete','unrecoverable') THEN hourly_ingest_match_debt.attempts ELSE hourly_ingest_match_debt.attempts+1 END,\
         last_attempt_at=CASE WHEN hourly_ingest_match_debt.status IN('complete','unrecoverable') THEN hourly_ingest_match_debt.last_attempt_at ELSE now() END,\
         next_retry_at=CASE WHEN hourly_ingest_match_debt.status IN('complete','unrecoverable') THEN hourly_ingest_match_debt.next_retry_at ELSE now()+($6*INTERVAL '1 minute') END,updated_at=now()",
        &[&date, &hour, &queue_id, &ids, &reason, &retry],
    ).await?;
    Ok(())
}

pub async fn mark_match_debt_staged_or_complete(
    database: &Database,
    match_ids: &[i64],
    reason: &str,
) -> Result<(), DatabaseError> {
    let ids = normalized_ids(match_ids);
    if ids.is_empty() {
        return Ok(());
    }
    ensure_hourly_ingest_tables(database).await?;
    database
        .query_json(
            "WITH ids AS(SELECT id::BIGINT match_id FROM unnest($1::BIGINT[]) ids(id)),\
             complete_ids AS(\
               SELECT mis.match_id FROM match_ingest_status mis JOIN ids USING(match_id) \
               WHERE mis.status IN('complete','limited') \
               UNION SELECT m.match_id FROM matches m JOIN ids USING(match_id) \
               LEFT JOIN match_ingest_status mis USING(match_id) \
               JOIN(SELECT match_id FROM match_players GROUP BY match_id HAVING count(*)>=10) mp USING(match_id) \
               WHERE mis.status IS NULL OR mis.status IN('complete','limited')\
             ) UPDATE hourly_ingest_match_debt debt SET \
               status=CASE WHEN complete_ids.match_id IS NULL THEN 'staged' ELSE 'complete' END,\
               reason=$2,staged_at=CASE WHEN complete_ids.match_id IS NULL THEN COALESCE(debt.staged_at,now()) ELSE debt.staged_at END,\
               completed_at=CASE WHEN complete_ids.match_id IS NOT NULL THEN COALESCE(debt.completed_at,now()) ELSE debt.completed_at END,\
               next_retry_at=NULL,updated_at=now() FROM ids LEFT JOIN complete_ids USING(match_id) \
             WHERE debt.match_id=ids.match_id AND debt.status NOT IN('complete','unrecoverable')",
            &[&ids, &reason],
        )
        .await?;
    Ok(())
}

pub async fn mark_match_debt_complete(
    database: &Database,
    match_id: i64,
) -> Result<(), DatabaseError> {
    if match_id <= 0 {
        return Ok(());
    }
    ensure_hourly_ingest_tables(database).await?;
    database.query_json(
        "UPDATE hourly_ingest_match_debt SET status='complete',reason='match facts durable and readable',\
         completed_at=COALESCE(completed_at,now()),next_retry_at=NULL,updated_at=now() WHERE match_id=$1",
        &[&match_id],
    ).await?;
    Ok(())
}

pub async fn mark_match_debt_retryable(
    database: &Database,
    match_id: i64,
    reason: &str,
    retry_minutes: Option<i32>,
) -> Result<(), DatabaseError> {
    if match_id <= 0 {
        return Ok(());
    }
    let retry = retry_minutes
        .unwrap_or_else(|| env_i32("HOURLY_INGEST_MATCH_DEBT_RETRY_MINUTES", 10).max(5));
    database.query_json(
        "UPDATE hourly_ingest_match_debt SET status=CASE WHEN status='complete' THEN status ELSE 'pending' END,\
         reason=CASE WHEN status='complete' THEN reason ELSE $2 END,next_retry_at=CASE WHEN status='complete' THEN next_retry_at \
         ELSE now()+($3*INTERVAL '1 minute') END,updated_at=now() WHERE match_id=$1 AND status<>'unrecoverable'",
        &[&match_id, &reason, &retry],
    ).await?;
    Ok(())
}

pub async fn mark_match_debt_unrecoverable(
    database: &Database,
    match_ids: &[i64],
    reason: &str,
) -> Result<(), DatabaseError> {
    let ids = normalized_ids(match_ids);
    if ids.is_empty() {
        return Ok(());
    }
    database.query_json(
        "UPDATE hourly_ingest_match_debt SET status=CASE WHEN status='complete' THEN status ELSE 'unrecoverable' END,\
         reason=CASE WHEN status='complete' THEN reason ELSE $2 END,next_retry_at=NULL,updated_at=now() \
         WHERE match_id=ANY($1::BIGINT[]) AND status<>'complete'",
        &[&ids, &reason],
    ).await?;
    Ok(())
}

pub async fn revive_retryable_match_debt(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
) -> Result<usize, DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    let rows = database
        .query_json(
            "UPDATE hourly_ingest_match_debt SET status='pending',\
             reason='retryable revival: previous terminal classification did not prove api_no_data; '||COALESCE(reason,''),\
             next_retry_at=now(),updated_at=now() WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3 \
             AND status='unrecoverable' AND COALESCE(reason,'') NOT ILIKE 'api_no_data:%' RETURNING match_id",
            &[&date, &hour, &queue_id],
        )
        .await?;
    Ok(rows.len())
}

pub async fn revive_fresh_no_authority_debt(
    database: &Database,
    queue_id: i32,
    min_date: &str,
    max_date: &str,
) -> Result<Vec<(String, i32, i32)>, DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    let fresh_hours = env_i32("NO_AUTH_PAYLOAD_FRESH_WINDOW_HOURS", 24).max(1);
    Ok(database
        .query_json(
            "WITH revived AS(UPDATE hourly_ingest_match_debt SET status='pending',\
             reason='fresh retryable revival: previous terminal classification did not prove api_no_data; '||COALESCE(reason,''),\
             next_retry_at=now(),updated_at=now() WHERE queue_id=$1 AND date BETWEEN $2::TEXT::DATE AND $3::TEXT::DATE \
             AND status='unrecoverable' AND first_seen_at>=now()-($4::INT*INTERVAL '1 hour') \
             AND COALESCE(reason,'') NOT ILIKE 'api_no_data:%' \
             AND (COALESCE(reason,'') ILIKE '%no authoritative payload%' OR COALESCE(reason,'') ILIKE 'dropped/corrupt:%') \
             RETURNING date,hour) SELECT date::TEXT,hour,count(*)::INT revived FROM revived GROUP BY date,hour ORDER BY date,hour",
            &[&queue_id, &min_date, &max_date, &fresh_hours],
        )
        .await?
        .into_iter()
        .filter_map(|row| {
            Some((
                text(&row, "date")?.to_owned(),
                i32::try_from(integer(&row, "hour")?).ok()?,
                i32::try_from(integer(&row, "revived")?).ok()?,
            ))
        })
        .collect())
}

pub async fn due_match_debt_ids(
    database: &Database,
    date: &str,
    hour: i32,
    queue_id: i32,
    limit: i64,
    ignore_cooldown: bool,
) -> Result<Vec<i64>, DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    Ok(database.query_json(
        "SELECT match_id::TEXT FROM hourly_ingest_match_debt WHERE date=$1::TEXT::DATE AND hour=$2 AND queue_id=$3 \
         AND status='pending' AND ($5 OR next_retry_at IS NULL OR next_retry_at<=now()) \
         ORDER BY attempts,first_seen_at LIMIT $4",
        &[&date, &hour, &queue_id, &limit, &ignore_cooldown],
    ).await?.into_iter().filter_map(|row| text(&row, "match_id")?.parse().ok()).collect())
}

pub async fn due_debt_hours(
    database: &Database,
    queue_id: i32,
    min_date: &str,
    max_date: &str,
) -> Result<Vec<(String, i32)>, DatabaseError> {
    ensure_hourly_ingest_tables(database).await?;
    Ok(database.query_json(
        "SELECT DISTINCT date::TEXT,hour FROM hourly_ingest_match_debt WHERE queue_id=$1 AND date>=$2::TEXT::DATE \
         AND date<=$3::TEXT::DATE AND status='pending' AND (next_retry_at IS NULL OR next_retry_at<=now()) ORDER BY date,hour",
        &[&queue_id, &min_date, &max_date],
    ).await?.into_iter().filter_map(|row| Some((text(&row, "date")?.to_owned(), i32::try_from(integer(&row, "hour")?).ok()?))).collect())
}

fn normalized_ids(ids: &[i64]) -> Vec<i64> {
    ids.iter()
        .copied()
        .filter(|id| *id > 0)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

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

fn text<'a>(row: &'a Value, key: &str) -> Option<&'a str> {
    row.get(key).and_then(Value::as_str)
}
fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}
fn env_i32(name: &str, fallback: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}
struct RegexLike;
impl RegexLike {
    fn budget(value: &str) -> bool {
        let value = value.to_ascii_lowercase();
        value.contains("budget exhausted") || value.contains("massive drop")
    }
}
