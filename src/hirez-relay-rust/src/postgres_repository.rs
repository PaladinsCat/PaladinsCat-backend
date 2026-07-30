use std::{sync::Arc, time::Instant};

use async_trait::async_trait;

use crate::{
    database::{Database, DatabaseError},
    key_pool::{
        BootstrapKey, EncryptedKeyRow, KeyPoolError, KeyPoolRepository, KeyStatus, UsageBatch,
    },
    upstream::{SessionAudit, SessionAuditRecord},
};

#[derive(Clone)]
pub struct PostgresRepository {
    database: Arc<Database>,
    reserve: u64,
}

impl PostgresRepository {
    pub fn new(database: Arc<Database>, reserve: u64) -> Self {
        Self { database, reserve }
    }

    fn repository_error(error: impl std::fmt::Display + std::fmt::Debug) -> KeyPoolError {
        KeyPoolError::Repository(format!("{error}: {error:?}"))
    }
}

#[async_trait]
impl KeyPoolRepository for PostgresRepository {
    async fn ensure_schema(&self) -> Result<(), KeyPoolError> {
        const SQL: &str = r#"
            ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS total_24h INT DEFAULT 0;
            ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS daily_limit INT DEFAULT 7500;
            ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS consecutive_failures INT DEFAULT 0;
            ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMPTZ;
            ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS last_sync_error TEXT;
            ALTER TABLE api_log ADD COLUMN IF NOT EXISTS consumer VARCHAR(80) NOT NULL DEFAULT 'legacy';
            CREATE TABLE IF NOT EXISTS api_key_hourly_usage (
                dev_id TEXT NOT NULL,
                hour_bucket TIMESTAMPTZ NOT NULL,
                call_count INT NOT NULL DEFAULT 0,
                PRIMARY KEY (dev_id, hour_bucket)
            );
            CREATE INDEX IF NOT EXISTS idx_api_key_hourly_usage_hour
                ON api_key_hourly_usage (hour_bucket DESC);
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let started = Instant::now();
        let result = client.batch_execute(SQL).await;
        self.database
            .observe_query(SQL, started, 0, result.is_err());
        result.map_err(Self::repository_error)
    }

    async fn load_keys(&self) -> Result<Vec<EncryptedKeyRow>, KeyPoolError> {
        const SQL: &str = r#"
            SELECT dev_id, auth_key, status, total_24h, daily_limit,
                   calls_total, consecutive_failures
            FROM api_keys
            ORDER BY dev_id ASC
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let started = Instant::now();
        let rows = client.query(SQL, &[]).await;
        self.database.observe_query(
            SQL,
            started,
            rows.as_ref().map_or(0, |rows| rows.len() as u64),
            rows.is_err(),
        );
        rows.map_err(Self::repository_error)?
            .into_iter()
            .map(|row| {
                Ok(EncryptedKeyRow {
                    dev_id: row.try_get("dev_id").map_err(Self::repository_error)?,
                    encrypted_auth_key: row.try_get("auth_key").map_err(Self::repository_error)?,
                    status: row
                        .try_get::<_, Option<String>>("status")
                        .map_err(Self::repository_error)?
                        .unwrap_or_else(|| "healthy".to_owned()),
                    total_24h: numeric_u64(&row, "total_24h")?,
                    daily_limit: optional_numeric_u64(&row, "daily_limit")?,
                    calls_total: numeric_u64(&row, "calls_total")?,
                    consecutive_failures: u32::try_from(numeric_u64(&row, "consecutive_failures")?)
                        .unwrap_or(u32::MAX),
                })
            })
            .collect()
    }

    async fn bootstrap_keys(&self, keys: &[BootstrapKey]) -> Result<usize, KeyPoolError> {
        const SQL: &str = r#"
            INSERT INTO api_keys (
                dev_id, auth_key, source, status, calls_today, calls_total,
                total_24h, daily_limit, consecutive_failures
            )
            VALUES ($1, $2, 'file-bootstrap', 'healthy', 0, 0, 0, $3, 0)
            ON CONFLICT (dev_id) DO NOTHING
        "#;
        if keys.is_empty() {
            return Ok(0);
        }
        let mut client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let transaction = client.transaction().await.map_err(Self::repository_error)?;
        let started = Instant::now();
        let mut inserted = 0_u64;
        for key in keys {
            let daily_limit = i32::try_from(key.daily_limit).unwrap_or(i32::MAX);
            inserted += transaction
                .execute(SQL, &[&key.dev_id, &key.encrypted_auth_key, &daily_limit])
                .await
                .map_err(Self::repository_error)?;
        }
        let result = transaction.commit().await;
        self.database
            .observe_query(SQL, started, inserted, result.is_err());
        result.map_err(Self::repository_error)?;
        Ok(inserted as usize)
    }

    async fn flush_usage(&self, batches: &[UsageBatch]) -> Result<(), KeyPoolError> {
        const HOURLY_SQL: &str = r#"
            INSERT INTO api_key_hourly_usage (dev_id, hour_bucket, call_count)
            VALUES ($1, date_trunc('hour', now()), $2)
            ON CONFLICT (dev_id, hour_bucket) DO UPDATE
            SET call_count = api_key_hourly_usage.call_count + EXCLUDED.call_count
        "#;
        const KEY_SQL: &str = r#"
            UPDATE api_keys
            SET total_24h = total_24h + $1,
                calls_total = calls_total + $1,
                last_used = NOW(),
                status = CASE
                    WHEN (daily_limit - (total_24h + $1)) <= $3 THEN 'limited'
                    ELSE status
                END
            WHERE dev_id = $2
        "#;
        if batches.is_empty() {
            return Ok(());
        }
        let mut client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let transaction = client.transaction().await.map_err(Self::repository_error)?;
        let started = Instant::now();
        for batch in batches {
            let calls = i32::try_from(batch.calls).unwrap_or(i32::MAX);
            let reserve = i32::try_from(self.reserve).unwrap_or(i32::MAX);
            transaction
                .execute(HOURLY_SQL, &[&batch.dev_id, &calls])
                .await
                .map_err(Self::repository_error)?;
            transaction
                .execute(KEY_SQL, &[&calls, &batch.dev_id, &reserve])
                .await
                .map_err(Self::repository_error)?;
        }
        let result = transaction.commit().await;
        self.database.observe_query(
            "flush api key usage",
            started,
            batches.len() as u64,
            result.is_err(),
        );
        result.map_err(Self::repository_error)
    }

    async fn update_status(
        &self,
        dev_id: &str,
        status: KeyStatus,
        consecutive_failures: u32,
    ) -> Result<(), KeyPoolError> {
        const SQL: &str = r#"
            UPDATE api_keys
            SET status = $1, consecutive_failures = $2
            WHERE dev_id = $3
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let failures = i32::try_from(consecutive_failures).unwrap_or(i32::MAX);
        client
            .execute(SQL, &[&status_text(status), &failures, &dev_id])
            .await
            .map_err(Self::repository_error)?;
        Ok(())
    }

    async fn log_endpoint(
        &self,
        dev_id: &str,
        endpoint: &str,
        response_time_ms: u64,
        consumer: &str,
    ) -> Result<(), KeyPoolError> {
        const SQL: &str = r#"
            INSERT INTO api_log (
                dev_id, endpoint, consumer, hour, call_count, total_response_ms
            )
            VALUES ($1, $2, $3, date_trunc('hour', now()), 1, $4)
            ON CONFLICT (dev_id, endpoint, consumer, hour) DO UPDATE
            SET call_count = api_log.call_count + 1,
                total_response_ms = api_log.total_response_ms + EXCLUDED.total_response_ms
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let response_time_ms = i32::try_from(response_time_ms).unwrap_or(i32::MAX);
        client
            .execute(SQL, &[&dev_id, &endpoint, &consumer, &response_time_ms])
            .await
            .map_err(Self::repository_error)?;
        Ok(())
    }

    async fn save_authoritative_usage(
        &self,
        dev_id: &str,
        used: u64,
        limit: u64,
        status: KeyStatus,
    ) -> Result<(), KeyPoolError> {
        const SQL: &str = r#"
            UPDATE api_keys
            SET total_24h = $1,
                daily_limit = $2,
                status = $3::varchar,
                consecutive_failures = CASE
                    WHEN $3::varchar = 'healthy' THEN 0
                    ELSE consecutive_failures
                END,
                last_sync_at = now(),
                last_sync_error = NULL
            WHERE dev_id = $4
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let used = i32::try_from(used).unwrap_or(i32::MAX);
        let limit = i32::try_from(limit).unwrap_or(i32::MAX);
        client
            .execute(SQL, &[&used, &limit, &status_text(status), &dev_id])
            .await
            .map_err(Self::repository_error)?;
        Ok(())
    }

    async fn local_usage_estimate(&self, dev_id: &str) -> Result<u64, KeyPoolError> {
        const SQL: &str = r#"
            SELECT COALESCE(SUM(call_count), 0)::bigint AS total
            FROM api_key_hourly_usage
            WHERE dev_id = $1
              AND hour_bucket >= date_trunc('hour', now()) - interval '23 hours'
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let row = client
            .query_one(SQL, &[&dev_id])
            .await
            .map_err(Self::repository_error)?;
        Ok(u64::try_from(row.get::<_, i64>("total")).unwrap_or(0))
    }

    async fn record_sync_error(&self, dev_id: &str, error: &str) -> Result<(), KeyPoolError> {
        const SQL: &str = r#"
            UPDATE api_keys
            SET last_sync_at = now(), last_sync_error = $1
            WHERE dev_id = $2
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        let error = truncate_utf8(error, 2_000);
        client
            .execute(SQL, &[&error, &dev_id])
            .await
            .map_err(Self::repository_error)?;
        Ok(())
    }

    async fn cleanup_rolling_usage(&self) -> Result<(), KeyPoolError> {
        const SQL: &str = r#"
            DELETE FROM api_log
            WHERE hour < date_trunc('hour', now()) - interval '23 hours';
            DELETE FROM api_key_hourly_usage
            WHERE hour_bucket < date_trunc('hour', now()) - interval '23 hours'
        "#;
        let client = self
            .database
            .connection()
            .await
            .map_err(Self::repository_error)?;
        client
            .batch_execute(SQL)
            .await
            .map_err(Self::repository_error)?;
        Ok(())
    }
}

#[async_trait]
impl SessionAudit for PostgresRepository {
    async fn save(&self, record: SessionAuditRecord) {
        const SQL: &str = r#"
            INSERT INTO raw_ingest_buffer (
                endpoint, params, raw_data, status_code, session_id,
                response_time_ms, status, entity_type
            )
            VALUES ($1, '[]'::jsonb, $2, $3, $4, $5, 'processed', 'audit')
        "#;
        let Ok(client) = self.database.connection().await else {
            return;
        };
        let status_code = i32::from(record.status_code);
        let response_time_ms = i32::try_from(record.response_time_ms).unwrap_or(i32::MAX);
        let _ = client
            .execute(
                SQL,
                &[
                    &record.endpoint,
                    &record.raw_data,
                    &status_code,
                    &record.session_id,
                    &response_time_ms,
                ],
            )
            .await;
    }
}

fn numeric_u64(row: &tokio_postgres::Row, column: &str) -> Result<u64, KeyPoolError> {
    optional_numeric_u64(row, column).map(|value| value.unwrap_or(0))
}

fn optional_numeric_u64(
    row: &tokio_postgres::Row,
    column: &str,
) -> Result<Option<u64>, KeyPoolError> {
    if let Ok(value) = row.try_get::<_, Option<i64>>(column) {
        return Ok(value.and_then(|value| u64::try_from(value).ok()));
    }
    if let Ok(value) = row.try_get::<_, Option<i32>>(column) {
        return Ok(value.and_then(|value| u64::try_from(value).ok()));
    }
    Err(KeyPoolError::Repository(format!(
        "Column {column} is not a supported integer type"
    )))
}

fn status_text(status: KeyStatus) -> &'static str {
    match status {
        KeyStatus::Healthy => "healthy",
        KeyStatus::Limited => "limited",
        KeyStatus::Unhealthy => "unhealthy",
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> &str {
    if value.len() <= max_bytes {
        return value;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

impl From<DatabaseError> for KeyPoolError {
    fn from(error: DatabaseError) -> Self {
        KeyPoolError::Repository(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        key_pool::{KeyPoolRepository, UsageBatch},
        upstream::SessionAuditRecord,
    };

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL"]
    async fn live_postgres_repository_preserves_key_pool_side_effects() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("PALADINSCAT_TEST_DATABASE_URL");
        let database = Arc::new(
            Database::new(&database_url, "rust-relay-integration-test", 4, 50).expect("db"),
        );
        assert!(database.health_check().await);
        let client = database.connection().await.expect("connection");
        client
            .batch_execute(
                r#"
                DROP TABLE IF EXISTS api_key_hourly_usage;
                DROP TABLE IF EXISTS api_log;
                DROP TABLE IF EXISTS raw_ingest_buffer;
                DROP TABLE IF EXISTS api_keys;
                CREATE TABLE api_keys (
                    id SERIAL PRIMARY KEY,
                    dev_id VARCHAR NOT NULL UNIQUE,
                    auth_key VARCHAR NOT NULL,
                    source VARCHAR,
                    status VARCHAR DEFAULT 'healthy',
                    calls_today INT DEFAULT 0,
                    total_24h INT DEFAULT 0,
                    daily_limit INT DEFAULT 7500,
                    calls_total INT DEFAULT 0,
                    consecutive_failures INT DEFAULT 0,
                    last_health_check TIMESTAMPTZ,
                    last_used TIMESTAMPTZ,
                    last_sync_at TIMESTAMPTZ,
                    last_sync_error TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE api_log (
                    dev_id VARCHAR NOT NULL,
                    endpoint VARCHAR NOT NULL,
                    consumer VARCHAR(80) NOT NULL DEFAULT 'legacy',
                    hour TIMESTAMPTZ NOT NULL,
                    call_count INT NOT NULL DEFAULT 0,
                    total_response_ms INT NOT NULL DEFAULT 0,
                    PRIMARY KEY (dev_id, endpoint, consumer, hour)
                );
                CREATE TABLE raw_ingest_buffer (
                    id BIGSERIAL PRIMARY KEY,
                    endpoint VARCHAR NOT NULL,
                    params JSONB,
                    raw_data JSONB,
                    status_code INT,
                    session_id VARCHAR,
                    response_time_ms INT,
                    status VARCHAR,
                    entity_type VARCHAR,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                "#,
            )
            .await
            .expect("minimal schema");
        drop(client);

        let repository = PostgresRepository::new(database.clone(), 100);
        repository.ensure_schema().await.expect("ensure schema");
        assert_eq!(
            repository
                .bootstrap_keys(&[BootstrapKey {
                    dev_id: "2116".to_owned(),
                    encrypted_auth_key: "ciphertext".to_owned(),
                    daily_limit: 15_000,
                }])
                .await
                .expect("bootstrap"),
            1
        );
        assert_eq!(repository.load_keys().await.expect("load").len(), 1);

        repository
            .flush_usage(&[UsageBatch {
                dev_id: "2116".to_owned(),
                calls: 3,
            }])
            .await
            .expect("usage");
        repository
            .log_endpoint("2116", "getmatchdetailsbatch", 125, "integration")
            .await
            .expect("endpoint log");
        repository
            .update_status("2116", KeyStatus::Unhealthy, 5)
            .await
            .expect("status");
        repository
            .save_authoritative_usage("2116", 42, 15_000, KeyStatus::Healthy)
            .await
            .expect("authoritative usage");
        repository
            .record_sync_error("2116", "synthetic sync failure")
            .await
            .expect("sync error");
        assert_eq!(
            repository
                .local_usage_estimate("2116")
                .await
                .expect("estimate"),
            3
        );

        repository
            .save(SessionAuditRecord {
                endpoint: "createsession",
                raw_data: serde_json::json!({"ret_msg": "Approved"}),
                status_code: 200,
                session_id: "session-id".to_owned(),
                response_time_ms: 25,
            })
            .await;

        let client = database.connection().await.expect("verify connection");
        let key = client
            .query_one(
                "SELECT total_24h, daily_limit, status, consecutive_failures, last_sync_error FROM api_keys WHERE dev_id = '2116'",
                &[],
            )
            .await
            .expect("key row");
        assert_eq!(key.get::<_, i32>("total_24h"), 42);
        assert_eq!(key.get::<_, i32>("daily_limit"), 15_000);
        assert_eq!(key.get::<_, String>("status"), "healthy");
        assert_eq!(key.get::<_, i32>("consecutive_failures"), 0);
        assert_eq!(
            key.get::<_, Option<String>>("last_sync_error").as_deref(),
            Some("synthetic sync failure")
        );
        let endpoint_log: i64 = client
            .query_one(
                "SELECT COUNT(*)::bigint AS count FROM api_log WHERE consumer = 'integration'",
                &[],
            )
            .await
            .expect("api log")
            .get("count");
        assert_eq!(endpoint_log, 1);
        let audits: i64 = client
            .query_one(
                "SELECT COUNT(*)::bigint AS count FROM raw_ingest_buffer WHERE entity_type = 'audit'",
                &[],
            )
            .await
            .expect("audit")
            .get("count");
        assert_eq!(audits, 1);
    }
}
