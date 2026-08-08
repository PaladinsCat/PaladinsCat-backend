use std::time::{Duration, Instant};

use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, thiserror::Error)]
pub enum WorkerLockError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error("lock not available: {0}")]
    NotAvailable(String),
    #[error("lock timeout after {0}ms")]
    Timeout(u64),
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LockStatus {
    pub locked: bool,
    pub lock_owner: Option<String>,
    pub expires_at: Option<String>,
}

pub async fn ensure_lock_table(database: &Database) -> Result<(), DatabaseError> {
    let _ = database
        .query_json(
            "CREATE TABLE IF NOT EXISTS worker_locks(\
              lock_key VARCHAR(100) PRIMARY KEY,\
              owner VARCHAR(100) NOT NULL,\
              acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),\
              expires_at TIMESTAMPTZ NOT NULL)",
            &[],
        )
        .await?;
    Ok(())
}

pub async fn try_acquire_lock(
    database: &Database,
    lock_key: &str,
    owner: &str,
    ttl_seconds: u64,
) -> Result<bool, WorkerLockError> {
    ensure_lock_table(database).await?;
    let rows = database
        .query_json(
            "INSERT INTO worker_locks(lock_key, owner, expires_at)\
             VALUES($1, $2, now() + ($3::INT * INTERVAL '1 second')\
             ) ON CONFLICT(lock_key) DO UPDATE SET owner=EXCLUDED.owner, expires_at=EXCLUDED.expires_at\
             WHERE worker_locks.expires_at <= now() RETURNING lock_key",
            &[&lock_key, &owner, &i32::try_from(ttl_seconds).unwrap_or(300)],
        )
        .await?;
    Ok(!rows.is_empty())
}

pub async fn release_lock(
    database: &Database,
    lock_key: &str,
    owner: &str,
) -> Result<bool, WorkerLockError> {
    let rows = database
        .query_json(
            "DELETE FROM worker_locks WHERE lock_key=$1 AND owner=$2",
            &[&lock_key, &owner],
        )
        .await?;
    Ok(!rows.is_empty())
}

pub async fn get_lock_status(
    database: &Database,
    lock_key: &str,
) -> Result<LockStatus, WorkerLockError> {
    ensure_lock_table(database).await?;
    let row = database
        .one_json(
            "SELECT owner, expires_at FROM worker_locks WHERE lock_key=$1",
            &[&lock_key],
        )
        .await?;
    Ok(LockStatus {
        locked: row.is_some(),
        lock_owner: row
            .as_ref()
            .and_then(|r| r.get("owner").and_then(Value::as_str).map(str::to_owned)),
        expires_at: row.as_ref().and_then(|r| {
            r.get("expires_at")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }),
    })
}

pub async fn wait_for_lock(
    database: &Database,
    lock_key: &str,
    owner: &str,
    ttl_seconds: u64,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> Result<bool, WorkerLockError> {
    let started = Instant::now();
    let timeout = Duration::from_millis(timeout_ms);
    let interval = Duration::from_millis(poll_interval_ms);
    loop {
        if started.elapsed() > timeout {
            return Err(WorkerLockError::Timeout(timeout_ms));
        }
        if try_acquire_lock(database, lock_key, owner, ttl_seconds).await? {
            return Ok(true);
        }
        tokio::time::sleep(interval).await;
    }
}
