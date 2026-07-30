use std::{collections::HashSet, sync::Arc, time::Instant};

use serde_json::Value;
use thiserror::Error;

use crate::database::Database;

const DEDUPE_ENTITY_TYPES: [&str; 3] = ["match", "match_history", "prefetch_match"];

#[derive(Clone, Debug)]
pub struct RawPayload {
    pub endpoint: String,
    pub entity_type: String,
    pub entity_id: Option<String>,
    pub raw_data: Value,
}

#[derive(Debug, Error)]
pub enum RawBufferError {
    #[error("PostgreSQL operation failed: {0}")]
    Database(String),
}

#[derive(Clone)]
pub struct RawBufferStore {
    database: Arc<Database>,
}

impl RawBufferStore {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    pub async fn dump_raw_payloads(
        &self,
        payloads: &[RawPayload],
    ) -> Result<usize, RawBufferError> {
        if payloads.is_empty() {
            return Ok(0);
        }

        let keyed: Vec<_> = payloads
            .iter()
            .filter(|payload| {
                is_deduped_type(&payload.entity_type)
                    && payload
                        .entity_id
                        .as_deref()
                        .is_some_and(|entity_id| !entity_id.is_empty())
            })
            .collect();
        let mut existing = HashSet::new();
        let client = self.database.connection().await.map_err(database_error)?;

        if !keyed.is_empty() {
            let entity_types = unique_strings(keyed.iter().map(|payload| &payload.entity_type));
            let entity_ids = unique_strings(
                keyed
                    .iter()
                    .filter_map(|payload| payload.entity_id.as_ref()),
            );
            const EXISTING_BUFFER_SQL: &str = r#"
                SELECT DISTINCT entity_type, entity_id
                FROM raw_ingest_buffer
                WHERE entity_type = ANY($1::text[])
                  AND entity_id = ANY($2::text[])
                  AND status IN ('pending', 'processing')
            "#;
            let started = Instant::now();
            let rows = client
                .query(EXISTING_BUFFER_SQL, &[&entity_types, &entity_ids])
                .await
                .map_err(database_error)?;
            self.database
                .observe_query(EXISTING_BUFFER_SQL, started, rows.len() as u64, false);
            for row in rows {
                let entity_type: String = row.get("entity_type");
                let entity_id: String = row.get("entity_id");
                existing.insert(dedupe_key(&entity_type, &entity_id));
            }

            let match_ids: Vec<i64> = keyed
                .iter()
                .filter(|payload| payload.entity_type == "match")
                .filter_map(|payload| payload.entity_id.as_deref())
                .filter_map(|entity_id| entity_id.parse::<i64>().ok())
                .filter(|match_id| *match_id > 0)
                .collect::<HashSet<_>>()
                .into_iter()
                .collect();
            if !match_ids.is_empty() {
                for match_id in self
                    .existing_completed_or_limited_matches(&client, &match_ids)
                    .await?
                {
                    existing.insert(dedupe_key("match", &match_id.to_string()));
                }
            }
        }

        let mut seen = HashSet::new();
        let to_insert: Vec<_> = payloads
            .iter()
            .filter(|payload| {
                let Some(entity_id) = payload.entity_id.as_deref() else {
                    return true;
                };
                if entity_id.is_empty() || !is_deduped_type(&payload.entity_type) {
                    return true;
                }
                let key = dedupe_key(&payload.entity_type, entity_id);
                !existing.contains(&key) && seen.insert(key)
            })
            .collect();
        if to_insert.is_empty() {
            return Ok(0);
        }

        const INSERT_SQL: &str = r#"
            INSERT INTO raw_ingest_buffer (
                raw_data, status, endpoint, entity_type, entity_id
            )
            VALUES ($1::jsonb, 'pending', $2, $3, $4)
        "#;
        drop(client);
        let mut client = self.database.connection().await.map_err(database_error)?;
        let transaction = client.transaction().await.map_err(database_error)?;
        let started = Instant::now();
        for payload in &to_insert {
            let raw_data = sanitize_json_value(&payload.raw_data);
            transaction
                .execute(
                    INSERT_SQL,
                    &[
                        &raw_data,
                        &payload.endpoint,
                        &payload.entity_type,
                        &payload.entity_id.clone().unwrap_or_default(),
                    ],
                )
                .await
                .map_err(database_error)?;
        }
        transaction.commit().await.map_err(database_error)?;
        self.database
            .observe_query(INSERT_SQL, started, to_insert.len() as u64, false);
        Ok(to_insert.len())
    }

    async fn existing_completed_or_limited_matches(
        &self,
        client: &deadpool_postgres::Object,
        match_ids: &[i64],
    ) -> Result<Vec<i64>, RawBufferError> {
        const STATUS_SQL: &str = r#"
            SELECT m.match_id
            FROM matches m
            LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
            WHERE m.match_id = ANY($1::bigint[])
              AND (mis.status IN ('complete', 'limited') OR mis.status IS NULL)
            UNION
            SELECT mp.match_id
            FROM match_players mp
            LEFT JOIN match_ingest_status mis ON mis.match_id = mp.match_id
            WHERE mp.match_id = ANY($1::bigint[])
              AND (mis.status IN ('complete', 'limited') OR mis.status IS NULL)
            GROUP BY mp.match_id
            HAVING count(*) >= 10
        "#;
        const LEGACY_SQL: &str = r#"
            SELECT match_id FROM matches WHERE match_id = ANY($1::bigint[])
            UNION
            SELECT match_id
            FROM match_players
            WHERE match_id = ANY($1::bigint[])
            GROUP BY match_id
            HAVING count(*) >= 10
        "#;

        let rows = match client.query(STATUS_SQL, &[&match_ids]).await {
            Ok(rows) => rows,
            Err(_) => client
                .query(LEGACY_SQL, &[&match_ids])
                .await
                .map_err(database_error)?,
        };
        Ok(rows
            .into_iter()
            .map(|row| row.get::<_, i64>("match_id"))
            .collect())
    }
}

fn is_deduped_type(entity_type: &str) -> bool {
    DEDUPE_ENTITY_TYPES.contains(&entity_type)
}

fn dedupe_key(entity_type: &str, entity_id: &str) -> String {
    format!("{entity_type}|{entity_id}")
}

fn unique_strings<'a>(values: impl Iterator<Item = &'a String>) -> Vec<String> {
    values
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

fn sanitize_json_value(value: &Value) -> Value {
    fn sanitize(value: &Value) -> Value {
        match value {
            Value::String(value) => Value::String(value.replace('\0', "").replace("\\u0000", "")),
            Value::Array(values) => Value::Array(values.iter().map(sanitize).collect()),
            Value::Object(values) => Value::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), sanitize(value)))
                    .collect(),
            ),
            value => value.clone(),
        }
    }
    sanitize(value)
}

fn database_error(error: impl std::fmt::Display + std::fmt::Debug) -> RawBufferError {
    RawBufferError::Database(format!("{error}: {error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_sanitizer_removes_actual_and_escaped_nuls_recursively() {
        let value = serde_json::json!({
            "actual": "a\u{0000}b",
            "escaped": "c\\u0000d",
            "nested": ["e\u{0000}f"]
        });
        assert_eq!(
            sanitize_json_value(&value),
            serde_json::json!({
                "actual": "ab",
                "escaped": "cd",
                "nested": ["ef"]
            })
        );
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL"]
    async fn live_postgres_dump_deduplicates_pending_and_durable_matches() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("PALADINSCAT_TEST_DATABASE_URL");
        let database =
            Arc::new(Database::new(&database_url, "rust-raw-buffer-test", 4, 50).expect("db"));
        let client = database.connection().await.expect("connection");
        client
            .batch_execute(
                r#"
                DROP TABLE IF EXISTS match_ingest_status;
                DROP TABLE IF EXISTS match_players;
                DROP TABLE IF EXISTS matches;
                DROP TABLE IF EXISTS raw_ingest_buffer;
                CREATE TABLE raw_ingest_buffer (
                    id BIGSERIAL PRIMARY KEY,
                    raw_data JSONB NOT NULL,
                    status VARCHAR NOT NULL,
                    endpoint VARCHAR NOT NULL,
                    entity_type VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                CREATE TABLE matches (match_id BIGINT PRIMARY KEY);
                CREATE TABLE match_players (
                    match_id BIGINT NOT NULL,
                    player_id BIGINT NOT NULL,
                    PRIMARY KEY (match_id, player_id)
                );
                CREATE TABLE match_ingest_status (
                    match_id BIGINT PRIMARY KEY,
                    status VARCHAR NOT NULL
                );
                INSERT INTO raw_ingest_buffer (raw_data, status, endpoint, entity_type, entity_id)
                VALUES ('{}', 'pending', 'getmatchdetailsbatch', 'match', '10');
                INSERT INTO matches (match_id) VALUES (20), (30);
                INSERT INTO match_ingest_status (match_id, status)
                VALUES (20, 'complete'), (30, 'partial');
                "#,
            )
            .await
            .expect("schema");
        drop(client);

        let store = RawBufferStore::new(database.clone());
        let payload = |id: &str| RawPayload {
            endpoint: "getmatchdetailsbatch".to_owned(),
            entity_type: "match".to_owned(),
            entity_id: Some(id.to_owned()),
            raw_data: serde_json::json!({"match": id, "name": "a\u{0000}b"}),
        };
        let inserted = store
            .dump_raw_payloads(&[
                payload("10"),
                payload("20"),
                payload("30"),
                payload("30"),
                RawPayload {
                    endpoint: "getplayerbatch".to_owned(),
                    entity_type: "player".to_owned(),
                    entity_id: Some("7".to_owned()),
                    raw_data: serde_json::json!([]),
                },
            ])
            .await
            .expect("dump");
        assert_eq!(inserted, 2);

        let client = database.connection().await.expect("verify");
        let rows = client
            .query(
                "SELECT entity_type, entity_id, raw_data FROM raw_ingest_buffer ORDER BY id",
                &[],
            )
            .await
            .expect("rows");
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[1].get::<_, String>("entity_id"), "30");
        assert_eq!(
            rows[1].get::<_, Value>("raw_data")["name"],
            Value::String("ab".to_owned())
        );
        assert_eq!(rows[2].get::<_, String>("entity_type"), "player");
    }
}
