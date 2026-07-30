use std::{collections::HashSet, sync::Arc, time::Instant};

use serde_json::Value;
use thiserror::Error;
use time::OffsetDateTime;

use crate::database::Database;

pub const PLAYER_HISTORY_CACHE_SOURCE: &str = "getmatchhistory-v2";

#[derive(Clone, Debug)]
pub struct HistoryEntry {
    pub match_id: i64,
    pub player_id: i64,
    pub fetched_player_id: i64,
    pub entry_datetime: Option<OffsetDateTime>,
    pub queue_id: Option<i32>,
    pub region: Option<String>,
    pub map: Option<String>,
    pub champion_id: Option<i32>,
    pub champion_name: Option<String>,
    pub skin_id: Option<i32>,
    pub skin_name: Option<String>,
    pub win_status: Option<String>,
    pub kills: i32,
    pub deaths: i32,
    pub assists: i32,
    pub damage: i32,
    pub healing: i32,
    pub gold_earned: i32,
    pub time_in_match: i32,
    pub task_force: i16,
    pub league_tier: i32,
    pub source: String,
    pub raw_data: Value,
    pub normalized_data: Value,
}

#[derive(Debug, Error)]
pub enum HistoryStoreError {
    #[error("PostgreSQL history operation failed: {0}")]
    Database(String),
}

#[derive(Clone)]
pub struct HistoryStore {
    database: Arc<Database>,
    ttl_hours: i32,
}

impl HistoryStore {
    pub fn new(database: Arc<Database>, ttl_hours: u64) -> Self {
        Self {
            database,
            ttl_hours: i32::try_from(ttl_hours.max(1)).unwrap_or(i32::MAX),
        }
    }

    pub async fn ensure_schema(&self) -> Result<(), HistoryStoreError> {
        const SQL: &str = r#"
            CREATE TABLE IF NOT EXISTS player_match_history_cache (
              player_id BIGINT PRIMARY KEY,
              raw_data JSONB NOT NULL,
              match_ids BIGINT[] NOT NULL DEFAULT '{}',
              fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              expires_at TIMESTAMPTZ NOT NULL,
              source VARCHAR(30) NOT NULL DEFAULT 'getmatchhistory'
            );
            CREATE INDEX IF NOT EXISTS idx_player_match_history_cache_expires
              ON player_match_history_cache (expires_at);
            CREATE INDEX IF NOT EXISTS idx_player_match_history_cache_match_ids
              ON player_match_history_cache USING GIN (match_ids);
            CREATE INDEX IF NOT EXISTS idx_rib_match_history_entity_status
              ON raw_ingest_buffer (entity_id, status)
              WHERE entity_type = 'match_history' AND COALESCE(entity_id, '') <> '';
            CREATE TABLE IF NOT EXISTS player_match_history_entries (
              match_id BIGINT NOT NULL,
              player_id BIGINT NOT NULL,
              fetched_player_id BIGINT,
              entry_datetime TIMESTAMPTZ,
              queue_id INT,
              region VARCHAR(50),
              map VARCHAR(200),
              champion_id INT,
              champion_name VARCHAR(100),
              skin_id INT,
              skin_name VARCHAR(100),
              win_status VARCHAR(20),
              kills INT DEFAULT 0,
              deaths INT DEFAULT 0,
              assists INT DEFAULT 0,
              damage INT DEFAULT 0,
              healing INT DEFAULT 0,
              gold_earned INT DEFAULT 0,
              time_in_match INT DEFAULT 0,
              task_force SMALLINT DEFAULT 0,
              league_tier INT DEFAULT 0,
              source VARCHAR(30) NOT NULL DEFAULT 'getmatchhistory',
              raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
              normalized_data JSONB NOT NULL DEFAULT '{}'::jsonb,
              observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
              expires_at TIMESTAMPTZ,
              PRIMARY KEY (match_id, player_id)
            );
            CREATE INDEX IF NOT EXISTS idx_pmhe_player_time
              ON player_match_history_entries (player_id, entry_datetime DESC);
            CREATE INDEX IF NOT EXISTS idx_pmhe_fetched_player_expires
              ON player_match_history_entries (fetched_player_id, expires_at DESC);
            CREATE INDEX IF NOT EXISTS idx_pmhe_match
              ON player_match_history_entries (match_id);
            CREATE INDEX IF NOT EXISTS idx_pmhe_queue_time
              ON player_match_history_entries (queue_id, entry_datetime DESC);
        "#;
        let client = self.database.connection().await.map_err(database_error)?;
        let started = Instant::now();
        let result = client.batch_execute(SQL).await;
        self.database
            .observe_query(SQL, started, 0, result.is_err());
        result.map_err(database_error)
    }

    pub async fn read_recovery_cache(
        &self,
        player_id: i64,
        target_match_id: i64,
    ) -> Result<Option<Vec<Value>>, HistoryStoreError> {
        const SQL: &str = r#"
            SELECT raw_data, match_ids
            FROM player_match_history_cache
            WHERE player_id = $1
              AND expires_at > now()
        "#;
        let client = self.database.connection().await.map_err(database_error)?;
        let rows = client
            .query(SQL, &[&player_id])
            .await
            .map_err(database_error)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let match_ids: Vec<i64> = row.get("match_ids");
        if !match_ids.contains(&target_match_id) {
            return Ok(None);
        }
        Ok(Some(history_values(row.get("raw_data"))))
    }

    pub async fn read_fresh_public_cache(
        &self,
        player_id: i64,
        ttl_minutes: u32,
    ) -> Result<Option<Vec<Value>>, HistoryStoreError> {
        const SQL: &str = r#"
            SELECT raw_data, source
            FROM player_match_history_cache
            WHERE player_id = $1
              AND fetched_at >= now() - ($2::int * interval '1 minute')
              AND expires_at > now()
        "#;
        let client = self.database.connection().await.map_err(database_error)?;
        let ttl_minutes = i32::try_from(ttl_minutes.max(1)).unwrap_or(i32::MAX);
        let rows = client
            .query(SQL, &[&player_id, &ttl_minutes])
            .await
            .map_err(database_error)?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let matches = history_values(row.get("raw_data"));
        let source: String = row.get("source");
        if matches.is_empty() && source != PLAYER_HISTORY_CACHE_SOURCE {
            return Ok(None);
        }
        Ok(Some(matches))
    }

    pub async fn write_history(
        &self,
        player_id: i64,
        matches: &[Value],
        entries: &[HistoryEntry],
    ) -> Result<usize, HistoryStoreError> {
        const CACHE_SQL: &str = r#"
            INSERT INTO player_match_history_cache (
              player_id, raw_data, match_ids, fetched_at, expires_at, source
            )
            VALUES (
              $1, $2::jsonb, $3::bigint[], now(),
              now() + ($4::int * interval '1 hour'), $5
            )
            ON CONFLICT (player_id) DO UPDATE SET
              raw_data = EXCLUDED.raw_data,
              match_ids = EXCLUDED.match_ids,
              fetched_at = EXCLUDED.fetched_at,
              expires_at = EXCLUDED.expires_at,
              source = EXCLUDED.source
        "#;
        const ENTRY_SQL: &str = r#"
            INSERT INTO player_match_history_entries (
              match_id, player_id, fetched_player_id, entry_datetime, queue_id,
              region, map, champion_id, champion_name, skin_id, skin_name,
              win_status, kills, deaths, assists, damage, healing, gold_earned,
              time_in_match, task_force, league_tier, source, raw_data,
              normalized_data, observed_at, expires_at
            )
            VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
              $18,$19,$20,$21,$22,$23::jsonb,$24::jsonb,now(),
              now() + ($25::int * interval '1 hour')
            )
            ON CONFLICT (match_id, player_id) DO UPDATE SET
              fetched_player_id = EXCLUDED.fetched_player_id,
              entry_datetime = COALESCE(
                EXCLUDED.entry_datetime,
                player_match_history_entries.entry_datetime
              ),
              queue_id = COALESCE(EXCLUDED.queue_id, player_match_history_entries.queue_id),
              region = COALESCE(EXCLUDED.region, player_match_history_entries.region),
              map = COALESCE(NULLIF(EXCLUDED.map, ''), player_match_history_entries.map),
              champion_id = COALESCE(
                EXCLUDED.champion_id,
                player_match_history_entries.champion_id
              ),
              champion_name = COALESCE(
                NULLIF(EXCLUDED.champion_name, ''),
                player_match_history_entries.champion_name
              ),
              skin_id = COALESCE(EXCLUDED.skin_id, player_match_history_entries.skin_id),
              skin_name = COALESCE(
                NULLIF(EXCLUDED.skin_name, ''),
                player_match_history_entries.skin_name
              ),
              win_status = COALESCE(
                NULLIF(EXCLUDED.win_status, ''),
                player_match_history_entries.win_status
              ),
              kills = EXCLUDED.kills,
              deaths = EXCLUDED.deaths,
              assists = EXCLUDED.assists,
              damage = EXCLUDED.damage,
              healing = EXCLUDED.healing,
              gold_earned = EXCLUDED.gold_earned,
              time_in_match = EXCLUDED.time_in_match,
              task_force = EXCLUDED.task_force,
              league_tier = EXCLUDED.league_tier,
              source = EXCLUDED.source,
              raw_data = EXCLUDED.raw_data,
              normalized_data = EXCLUDED.normalized_data,
              observed_at = EXCLUDED.observed_at,
              expires_at = EXCLUDED.expires_at
        "#;

        let mut seen_match_ids = HashSet::new();
        let match_ids: Vec<i64> = matches
            .iter()
            .filter_map(history_match_id)
            .filter(|match_id| seen_match_ids.insert(*match_id))
            .collect();
        let raw_data = sanitize_json(&Value::Array(matches.to_vec()));
        let mut client = self.database.connection().await.map_err(database_error)?;
        let transaction = client.transaction().await.map_err(database_error)?;
        transaction
            .execute(
                CACHE_SQL,
                &[
                    &player_id,
                    &raw_data,
                    &match_ids,
                    &self.ttl_hours,
                    &PLAYER_HISTORY_CACHE_SOURCE,
                ],
            )
            .await
            .map_err(database_error)?;

        for entry in entries {
            let raw_data = sanitize_json(&entry.raw_data);
            let normalized_data = sanitize_json(&entry.normalized_data);
            transaction
                .execute(
                    ENTRY_SQL,
                    &[
                        &entry.match_id,
                        &entry.player_id,
                        &entry.fetched_player_id,
                        &entry.entry_datetime,
                        &entry.queue_id,
                        &sanitize_optional(&entry.region),
                        &sanitize_optional(&entry.map),
                        &entry.champion_id,
                        &sanitize_optional(&entry.champion_name),
                        &entry.skin_id,
                        &sanitize_optional(&entry.skin_name),
                        &sanitize_optional(&entry.win_status),
                        &entry.kills,
                        &entry.deaths,
                        &entry.assists,
                        &entry.damage,
                        &entry.healing,
                        &entry.gold_earned,
                        &entry.time_in_match,
                        &entry.task_force,
                        &entry.league_tier,
                        &entry.source,
                        &raw_data,
                        &normalized_data,
                        &self.ttl_hours,
                    ],
                )
                .await
                .map_err(database_error)?;
        }
        transaction.commit().await.map_err(database_error)?;
        Ok(entries.len())
    }

    pub async fn read_match_entries(
        &self,
        match_id: i64,
        player_ids: &[i64],
    ) -> Result<Vec<HistoryEntry>, HistoryStoreError> {
        let client = self.database.connection().await.map_err(database_error)?;
        let rows = if player_ids.is_empty() {
            client
                .query(
                    r#"
                    SELECT * FROM player_match_history_entries
                    WHERE match_id = $1
                      AND (expires_at IS NULL OR expires_at > now())
                    ORDER BY observed_at DESC
                    "#,
                    &[&match_id],
                )
                .await
        } else {
            client
                .query(
                    r#"
                    SELECT * FROM player_match_history_entries
                    WHERE match_id = $1
                      AND player_id = ANY($2::bigint[])
                      AND (expires_at IS NULL OR expires_at > now())
                    ORDER BY observed_at DESC
                    "#,
                    &[&match_id, &player_ids],
                )
                .await
        }
        .map_err(database_error)?;
        rows.into_iter().map(row_to_entry).collect()
    }
}

fn row_to_entry(row: tokio_postgres::Row) -> Result<HistoryEntry, HistoryStoreError> {
    Ok(HistoryEntry {
        match_id: row.get("match_id"),
        player_id: row.get("player_id"),
        fetched_player_id: row.get("fetched_player_id"),
        entry_datetime: row.get("entry_datetime"),
        queue_id: row.get("queue_id"),
        region: row.get("region"),
        map: row.get("map"),
        champion_id: row.get("champion_id"),
        champion_name: row.get("champion_name"),
        skin_id: row.get("skin_id"),
        skin_name: row.get("skin_name"),
        win_status: row.get("win_status"),
        kills: row.get("kills"),
        deaths: row.get("deaths"),
        assists: row.get("assists"),
        damage: row.get("damage"),
        healing: row.get("healing"),
        gold_earned: row.get("gold_earned"),
        time_in_match: row.get("time_in_match"),
        task_force: row.get("task_force"),
        league_tier: row.get("league_tier"),
        source: row.get("source"),
        raw_data: row.get("raw_data"),
        normalized_data: row.get("normalized_data"),
    })
}

fn history_values(value: Value) -> Vec<Value> {
    match value {
        Value::Array(values) => values,
        Value::Object(mut object) => object
            .remove("matches")
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn history_match_id(value: &Value) -> Option<i64> {
    ["Match", "match_id", "MatchId"]
        .into_iter()
        .find_map(|key| value.get(key))
        .and_then(|value| match value {
            Value::Number(value) => value.as_i64(),
            Value::String(value) => value.parse().ok(),
            _ => None,
        })
        .filter(|match_id| *match_id > 0)
}

fn sanitize_optional(value: &Option<String>) -> Option<String> {
    value
        .as_ref()
        .map(|value| value.replace('\0', "").replace("\\u0000", ""))
}

fn sanitize_json(value: &Value) -> Value {
    match value {
        Value::String(value) => Value::String(value.replace('\0', "").replace("\\u0000", "")),
        Value::Array(values) => Value::Array(values.iter().map(sanitize_json).collect()),
        Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), sanitize_json(value)))
                .collect(),
        ),
        value => value.clone(),
    }
}

fn database_error(error: impl std::fmt::Display + std::fmt::Debug) -> HistoryStoreError {
    HistoryStoreError::Database(format!("{error}: {error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(match_id: i64, player_id: i64, kills: i32) -> HistoryEntry {
        HistoryEntry {
            match_id,
            player_id,
            fetched_player_id: player_id,
            entry_datetime: None,
            queue_id: Some(486),
            region: Some("North America".to_owned()),
            map: Some("Stone Keep".to_owned()),
            champion_id: Some(1),
            champion_name: Some("Fernando".to_owned()),
            skin_id: Some(2),
            skin_name: Some("Default".to_owned()),
            win_status: Some("Winner".to_owned()),
            kills,
            deaths: 2,
            assists: 3,
            damage: 4,
            healing: 5,
            gold_earned: 6,
            time_in_match: 7,
            task_force: 1,
            league_tier: 8,
            source: "getmatchhistory".to_owned(),
            raw_data: serde_json::json!({"Match": match_id, "ActivePlayerId": player_id}),
            normalized_data: serde_json::json!({"match_id": match_id, "player_id": player_id}),
        }
    }

    #[test]
    fn extracts_numeric_and_string_match_ids() {
        assert_eq!(
            history_match_id(&serde_json::json!({"Match": 10})),
            Some(10)
        );
        assert_eq!(
            history_match_id(&serde_json::json!({"match_id": "11"})),
            Some(11)
        );
        assert_eq!(history_match_id(&serde_json::json!({"Match": 0})), None);
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL"]
    async fn live_history_cache_preserves_positive_and_negative_semantics() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("PALADINSCAT_TEST_DATABASE_URL");
        let database =
            Arc::new(Database::new(&database_url, "rust-history-test", 4, 50).expect("db"));
        let client = database.connection().await.expect("connection");
        client
            .batch_execute(
                r#"
                DROP TABLE IF EXISTS player_match_history_entries;
                DROP TABLE IF EXISTS player_match_history_cache;
                DROP TABLE IF EXISTS raw_ingest_buffer;
                CREATE TABLE raw_ingest_buffer (
                    id BIGSERIAL PRIMARY KEY,
                    raw_data JSONB NOT NULL DEFAULT '{}',
                    status VARCHAR NOT NULL,
                    endpoint VARCHAR NOT NULL,
                    entity_type VARCHAR NOT NULL,
                    entity_id VARCHAR NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                "#,
            )
            .await
            .expect("base schema");
        drop(client);

        let store = HistoryStore::new(database.clone(), 24);
        store.ensure_schema().await.expect("history schema");
        let matches = vec![
            serde_json::json!({"Match": 100, "ActivePlayerId": 7}),
            serde_json::json!({"Match": "101", "ActivePlayerId": 7}),
        ];
        assert_eq!(
            store
                .write_history(7, &matches, &[entry(100, 7, 1)])
                .await
                .expect("write"),
            1
        );
        assert_eq!(
            store
                .read_recovery_cache(7, 100)
                .await
                .expect("positive")
                .expect("cache")
                .len(),
            2
        );
        assert!(
            store
                .read_recovery_cache(7, 999)
                .await
                .expect("negative")
                .is_none()
        );
        assert_eq!(
            store
                .read_fresh_public_cache(7, 10)
                .await
                .expect("public")
                .expect("cache")
                .len(),
            2
        );
        assert_eq!(
            store.read_match_entries(100, &[7]).await.expect("entries")[0].kills,
            1
        );

        store
            .write_history(8, &[], &[])
            .await
            .expect("negative cache");
        assert_eq!(
            store
                .read_fresh_public_cache(8, 10)
                .await
                .expect("cached empty"),
            Some(Vec::new())
        );
        let client = database.connection().await.expect("poison source");
        client
            .execute(
                "UPDATE player_match_history_cache SET source = 'getmatchhistory' WHERE player_id = 8",
                &[],
            )
            .await
            .expect("legacy source");
        assert!(
            store
                .read_fresh_public_cache(8, 10)
                .await
                .expect("legacy empty")
                .is_none()
        );

        store
            .write_history(7, &matches, &[entry(100, 7, 9)])
            .await
            .expect("upsert");
        assert_eq!(
            store.read_match_entries(100, &[]).await.expect("updated")[0].kills,
            9
        );
    }
}
