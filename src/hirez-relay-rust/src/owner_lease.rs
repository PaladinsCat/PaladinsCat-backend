use std::sync::Arc;

use deadpool_postgres::Object;
use thiserror::Error;
use tokio::sync::Mutex;

use crate::database::Database;

#[derive(Debug, Error)]
pub enum OwnerLeaseError {
    #[error("PostgreSQL owner lease failed: {0}")]
    Database(String),
    #[error("another HirezRelay process already owns the live provider lease")]
    AlreadyOwned,
}

pub struct OwnerLease {
    database: Arc<Database>,
    connection: Mutex<Option<Object>>,
    lock_name: String,
}

impl OwnerLease {
    pub async fn acquire(
        database: Arc<Database>,
        lock_name: impl Into<String>,
    ) -> Result<Self, OwnerLeaseError> {
        let lock_name = lock_name.into();
        let connection = database
            .connection()
            .await
            .map_err(|error| OwnerLeaseError::Database(error.to_string()))?;
        let row = connection
            .query_one(
                "SELECT pg_try_advisory_lock(hashtext($1)) AS locked",
                &[&lock_name],
            )
            .await
            .map_err(|error| OwnerLeaseError::Database(error.to_string()))?;
        if !row.get::<_, bool>("locked") {
            return Err(OwnerLeaseError::AlreadyOwned);
        }
        Ok(Self {
            database,
            connection: Mutex::new(Some(connection)),
            lock_name,
        })
    }

    pub async fn is_healthy(&self) -> bool {
        let connection = self.connection.lock().await;
        let Some(connection) = connection.as_ref() else {
            return false;
        };
        connection.simple_query("SELECT 1").await.is_ok()
    }

    /// Keep the session-scoped advisory lock alive and reacquire it after a
    /// broken PostgreSQL connection. A dropped session releases its advisory
    /// locks, so retaining a dead connection without reacquisition leaves the
    /// relay permanently unable to serve provider calls.
    pub async fn ensure_healthy(&self) -> Result<bool, OwnerLeaseError> {
        let mut guard = self.connection.lock().await;
        if let Some(connection) = guard.as_ref()
            && connection.simple_query("SELECT 1").await.is_ok()
        {
            return Ok(true);
        }

        // Drop the broken session before trying to acquire a replacement. If
        // PostgreSQL has not observed the disconnect yet, pg_try_advisory_lock
        // safely returns false and the next health cycle retries.
        guard.take();
        let connection = self
            .database
            .connection()
            .await
            .map_err(|error| OwnerLeaseError::Database(error.to_string()))?;
        let row = connection
            .query_one(
                "SELECT pg_try_advisory_lock(hashtext($1)) AS locked",
                &[&self.lock_name],
            )
            .await
            .map_err(|error| OwnerLeaseError::Database(error.to_string()))?;
        if !row.get::<_, bool>("locked") {
            return Ok(false);
        }

        *guard = Some(connection);
        Ok(true)
    }

    pub async fn release(&self) {
        let mut guard = self.connection.lock().await;
        let Some(connection) = guard.take() else {
            return;
        };
        let _ = connection
            .query_one(
                "SELECT pg_advisory_unlock(hashtext($1)) AS unlocked",
                &[&self.lock_name],
            )
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL"]
    async fn live_owner_lease_is_exclusive_and_releasable() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("PALADINSCAT_TEST_DATABASE_URL");
        let database = Arc::new(Database::new(&database_url, "owner-lease-test", 3, 500).unwrap());
        let lock_name = format!("paladinscat:relay-owner-test:{}", std::process::id());

        let first = OwnerLease::acquire(database.clone(), &lock_name)
            .await
            .expect("first owner");
        assert!(first.is_healthy().await);
        assert!(matches!(
            OwnerLease::acquire(database.clone(), &lock_name).await,
            Err(OwnerLeaseError::AlreadyOwned)
        ));

        let backend_pid = {
            let guard = first.connection.lock().await;
            let connection = guard.as_ref().expect("lease connection");
            connection
                .query_one("SELECT pg_backend_pid() AS pid", &[])
                .await
                .expect("lease backend pid")
                .get::<_, i32>("pid")
        };
        let terminator = database.connection().await.expect("terminator connection");
        let terminated = terminator
            .query_one(
                "SELECT pg_terminate_backend($1) AS terminated",
                &[&backend_pid],
            )
            .await
            .expect("terminate lease backend")
            .get::<_, bool>("terminated");
        assert!(terminated);
        drop(terminator);

        let mut recovered = false;
        for _ in 0..10 {
            if first.ensure_healthy().await.unwrap_or(false) {
                recovered = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(
            recovered,
            "lease should recover after its session is killed"
        );
        assert!(matches!(
            OwnerLease::acquire(database.clone(), &lock_name).await,
            Err(OwnerLeaseError::AlreadyOwned)
        ));

        first.release().await;
        let second = OwnerLease::acquire(database.clone(), &lock_name)
            .await
            .expect("owner after release");
        assert!(second.is_healthy().await);
        second.release().await;
    }
}
