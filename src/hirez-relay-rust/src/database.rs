use std::{str::FromStr, time::Duration};

use deadpool_postgres::{Manager, ManagerConfig, Object, Pool, RecyclingMethod, Runtime};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::{Config as PostgresConfig, NoTls};

#[derive(Clone)]
pub struct Database {
    pool: Pool,
    slow_query_ms: u64,
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("Invalid DATABASE_URL: {0}")]
    InvalidUrl(String),
    #[error("Failed to construct PostgreSQL pool: {0}")]
    PoolBuild(String),
    #[error("PostgreSQL pool error: {0}")]
    Pool(String),
}

impl Database {
    pub fn new(
        database_url: &str,
        application_name: &str,
        max_connections: usize,
        slow_query_ms: u64,
    ) -> Result<Self, DatabaseError> {
        let mut config = PostgresConfig::from_str(database_url)
            .map_err(|error| DatabaseError::InvalidUrl(error.to_string()))?;
        config.application_name(application_name);
        config.connect_timeout(Duration::from_secs(10));
        config.options("-c statement_timeout=30000");
        let manager = Manager::from_config(
            config,
            NoTls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );
        let pool = Pool::builder(manager)
            .max_size(max_connections.clamp(1, 50))
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_secs(10)))
            .create_timeout(Some(Duration::from_secs(10)))
            .recycle_timeout(Some(Duration::from_secs(10)))
            .build()
            .map_err(|error| DatabaseError::PoolBuild(error.to_string()))?;
        Ok(Self {
            pool,
            slow_query_ms: slow_query_ms.max(50),
        })
    }

    pub async fn connection(&self) -> Result<Object, DatabaseError> {
        self.pool
            .get()
            .await
            .map_err(|error| DatabaseError::Pool(error.to_string()))
    }

    pub async fn health_check(&self) -> bool {
        let Ok(client) = self.connection().await else {
            return false;
        };
        client.simple_query("SELECT 1").await.is_ok()
    }

    pub fn observe_query(
        &self,
        statement: &str,
        started: std::time::Instant,
        row_count: u64,
        failed: bool,
    ) {
        let duration_ms = started.elapsed().as_secs_f64() * 1_000.0;
        if duration_ms < self.slow_query_ms as f64 && !failed {
            return;
        }
        let normalized = statement.split_whitespace().collect::<Vec<_>>().join(" ");
        let fingerprint = hex::encode(Sha256::digest(normalized.as_bytes()));
        let status = self.pool.status();
        tracing::warn!(
            fingerprint = &fingerprint[..12],
            duration_ms,
            row_count,
            failed,
            pool_size = status.size,
            pool_available = status.available,
            pool_waiting = status.waiting,
            pool_max = status.max_size,
            "database-query"
        );
    }

    pub fn status(&self) -> deadpool_postgres::Status {
        self.pool.status()
    }

    pub fn close(&self) {
        self.pool.close();
    }
}
