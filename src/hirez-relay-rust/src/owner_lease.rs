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

        first.release().await;
        let second = OwnerLease::acquire(database.clone(), &lock_name)
            .await
            .expect("owner after release");
        assert!(second.is_healthy().await);
        second.release().await;
    }
}
