use std::{collections::HashMap, sync::Arc};

use futures::future::join_all;
use serde_json::Value;

use crate::{
    database::Database,
    hirez_client::{ApiRequestOptions, HirezApiClient},
    history_operation::history_entry,
    history_store::HistoryStore,
    model::MatchDetails,
    normalizer::{
        normalize_flat_match_detail_rows, normalize_match_history_player, normalize_player_profile,
    },
    operations::{ApiCaller, OperationService},
    profile_store::ProfileStore,
    provider::{CompletedMatchProvider, RelayError},
};

pub struct LiveMatchProvider {
    api: Arc<HirezApiClient>,
    operations: OperationService<HirezApiClient>,
    database: Arc<Database>,
    history: Arc<HistoryStore>,
    profiles: Arc<ProfileStore>,
    consumer: Option<String>,
}

impl LiveMatchProvider {
    pub fn new(
        api: Arc<HirezApiClient>,
        database: Arc<Database>,
        history: Arc<HistoryStore>,
        profiles: Arc<ProfileStore>,
    ) -> Self {
        Self {
            operations: OperationService::new(api.clone()),
            api,
            database,
            history,
            profiles,
            consumer: None,
        }
    }

    pub fn attributed(&self, consumer: &str) -> Self {
        Self {
            api: self.api.clone(),
            operations: OperationService::new(self.api.clone()),
            database: self.database.clone(),
            history: self.history.clone(),
            profiles: self.profiles.clone(),
            consumer: Some(consumer.to_owned()),
        }
    }

    fn consumer<'a>(&'a self, fallback: &'a str) -> &'a str {
        self.consumer.as_deref().unwrap_or(fallback)
    }

    async fn local_recovered_player(
        &self,
        player_id: i64,
        match_id: i64,
    ) -> Result<Option<Value>, RelayError> {
        let client = self
            .database
            .connection()
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        let rows = client
            .query(
                r#"
                SELECT to_jsonb(mp) AS player
                FROM match_players mp
                WHERE mp.match_id = $1
                  AND mp.player_id = $2
                  AND mp.source = 'recovered'
                LIMIT 1
                "#,
                &[&match_id, &player_id],
            )
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        Ok(rows
            .first()
            .and_then(|row| row.try_get::<_, Value>("player").ok())
            .map(mark_recovered))
    }

    async fn local_history_entry(
        &self,
        player_id: i64,
        match_id: i64,
    ) -> Result<Option<Value>, RelayError> {
        let entries = self
            .history
            .read_match_entries(match_id, &[player_id])
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        Ok(entries
            .into_iter()
            .find(|entry| entry.player_id == player_id)
            .map(|entry| mark_recovered(entry.normalized_data)))
    }

    async fn persist_profiles(&self, roster: &[Value]) {
        let writes = roster.iter().filter_map(|raw| {
            if raw
                .get("ret_msg")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
            {
                return None;
            }
            let profile = normalize_player_profile(raw);
            (profile.player_id > 0).then(|| {
                let profiles = self.profiles.clone();
                async move { profiles.upsert(&profile).await }
            })
        });
        for result in join_all(writes).await {
            if let Err(error) = result {
                tracing::warn!(%error, "profile persistence failed during match recovery");
            }
        }
    }

    async fn fetch_and_store_history(&self, player_id: i64) -> Result<Vec<Value>, RelayError> {
        let data = self
            .api
            .call(
                "getmatchhistory",
                &[player_id.to_string()],
                ApiRequestOptions {
                    max_retries: Some(0),
                    ..ApiRequestOptions::default()
                },
                self.consumer("match_recovery"),
            )
            .await?;
        let matches = history_values(data);
        let normalized: Vec<_> = matches.iter().map(normalize_history_for_recovery).collect();
        let entries: Vec<_> = matches
            .iter()
            .zip(&normalized)
            .filter_map(|(raw, normalized)| {
                history_entry(player_id, raw, normalized, "getmatchhistory")
            })
            .collect();
        self.history
            .write_history(player_id, &matches, &entries)
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        Ok(normalized)
    }

    async fn lookup_match_history(
        &self,
        player_id: u64,
        match_id: u64,
    ) -> Result<(Vec<Value>, u32), RelayError> {
        let player_id = i64::try_from(player_id)
            .map_err(|_| RelayError::Validation("player ID exceeds BIGINT".to_owned()))?;
        let match_id = i64::try_from(match_id)
            .map_err(|_| RelayError::Validation("match ID exceeds BIGINT".to_owned()))?;

        if let Some(player) = self.local_recovered_player(player_id, match_id).await? {
            return Ok((vec![player], 0));
        }
        if let Some(player) = self.local_history_entry(player_id, match_id).await? {
            return Ok((vec![player], 0));
        }
        if let Some(matches) = self
            .history
            .read_recovery_cache(player_id, match_id)
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?
        {
            return Ok((
                matches
                    .iter()
                    .map(normalize_history_for_recovery)
                    .map(mark_recovered)
                    .collect(),
                0,
            ));
        }
        Ok((
            self.fetch_and_store_history(player_id)
                .await?
                .into_iter()
                .map(mark_recovered)
                .collect(),
            1,
        ))
    }
}

impl CompletedMatchProvider for LiveMatchProvider {
    async fn get_match_details_batch(
        &self,
        match_ids: &[u64],
    ) -> Result<Vec<MatchDetails>, RelayError> {
        let match_ids: Vec<_> = match_ids.iter().map(|match_id| *match_id as f64).collect();
        let raw = self
            .operations
            .get_match_details_batch_once(&match_ids, self.consumer("match_ingestion"))
            .await?;
        Ok(normalize_flat_match_detail_rows(&raw))
    }

    async fn get_player_batch_from_match(&self, match_id: u64) -> Result<Vec<Value>, RelayError> {
        let roster = self
            .operations
            .get_player_batch_from_match(match_id as f64, self.consumer("match_recovery"))
            .await?;
        self.persist_profiles(&roster).await;
        Ok(roster)
    }

    async fn get_match_history(
        &self,
        player_id: u64,
        match_id: u64,
    ) -> Result<Vec<Value>, RelayError> {
        self.lookup_match_history(player_id, match_id)
            .await
            .map(|(rows, _)| rows)
    }

    async fn get_match_history_with_usage(
        &self,
        player_id: u64,
        match_id: u64,
    ) -> Result<(Vec<Value>, u32), RelayError> {
        self.lookup_match_history(player_id, match_id).await
    }

    async fn get_demo_details(&self, match_id: u64) -> Result<Value, RelayError> {
        self.operations
            .get_demo_details_once(match_id as f64, self.consumer("match_recovery"))
            .await
    }

    async fn get_local_recovery_players(&self, match_id: u64) -> Result<Vec<Value>, RelayError> {
        let match_id = i64::try_from(match_id)
            .map_err(|_| RelayError::Validation("match ID exceeds BIGINT".to_owned()))?;
        let client = self
            .database
            .connection()
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;

        let recovered_rows = client
            .query(
                r#"
                SELECT to_jsonb(mp) AS player
                FROM match_players mp
                WHERE mp.match_id = $1
                  AND mp.player_id > 0
                  AND mp.source = 'recovered'
                ORDER BY mp.entry_datetime DESC
                "#,
                &[&match_id],
            )
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        let history_entries = self
            .history
            .read_match_entries(match_id, &[])
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        let buffered_rows = client
            .query(
                r#"
                SELECT entity_id, raw_data
                FROM raw_ingest_buffer
                WHERE status IN ('pending', 'processing')
                  AND endpoint IN (
                    'getmatchhistory',
                    'getplayermatchhistory',
                    'getplayermatchhistoryafterdatetime'
                  )
                  AND entity_type IN ('match_history', 'prefetch_match', 'match')
                  AND (entity_id = $1 OR entity_id LIKE $2)
                ORDER BY created_at ASC
                "#,
                &[&match_id.to_string(), &format!("{match_id}:%")],
            )
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;

        let mut by_player = HashMap::<u64, Value>::new();
        for row in recovered_rows {
            if let Ok(value) = row.try_get::<_, Value>("player") {
                insert_recovery_player(&mut by_player, value, match_id as u64, None);
            }
        }
        for entry in history_entries {
            insert_recovery_player(
                &mut by_player,
                entry.normalized_data,
                match_id as u64,
                Some(entry.player_id as u64),
            );
        }
        for row in buffered_rows {
            let fallback_player_id = row
                .try_get::<_, String>("entity_id")
                .ok()
                .and_then(|entity_id| entity_id.split_once(':').map(|(_, value)| value.to_owned()))
                .and_then(|value| value.parse::<u64>().ok());
            let raw = row.try_get::<_, Value>("raw_data").unwrap_or(Value::Null);
            for value in buffer_values(raw) {
                insert_recovery_player(&mut by_player, value, match_id as u64, fallback_player_id);
            }
        }
        Ok(by_player.into_values().collect())
    }
}

fn normalize_history_for_recovery(raw: &Value) -> Value {
    let mut normalized = normalize_match_history_player(raw);
    if let Some(object) = normalized.as_object_mut() {
        let map = raw
            .get("Map_Game")
            .or_else(|| raw.get("Map"))
            .or_else(|| raw.get("map"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        object.insert("map".to_owned(), Value::String(map.to_owned()));
    }
    normalized
}

fn mark_recovered(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert("source".to_owned(), Value::String("recovered".to_owned()));
        object.insert("has_ret_msg".to_owned(), Value::Bool(false));
    }
    value
}

fn history_values(data: Value) -> Vec<Value> {
    match data {
        Value::Array(values) => values,
        Value::Object(mut object) => object
            .remove("matches")
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn buffer_values(data: Value) -> Vec<Value> {
    match data {
        Value::Array(values) => values,
        value => vec![value],
    }
}

fn insert_recovery_player(
    players: &mut HashMap<u64, Value>,
    raw: Value,
    match_id: u64,
    fallback_player_id: Option<u64>,
) {
    let raw_match_id = crate::model::player_number(&raw, &["Match", "match_id", "MatchId"]);
    if raw_match_id != 0 && raw_match_id != match_id {
        return;
    }
    let source = raw
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let already_normalized = raw.get("match_id").is_some()
        && matches!(
            source.as_str(),
            "prefetch" | "recovered" | "match_history" | "history_observation" | "legacy_prefetch"
        );
    let mut player = if already_normalized {
        raw
    } else {
        normalize_match_history_player(&raw)
    };
    let normalized_player_id =
        crate::model::player_number(&player, &["player_id", "playerId", "ActivePlayerId", "Id"]);
    let player_id = if normalized_player_id > 0 {
        normalized_player_id
    } else {
        fallback_player_id.unwrap_or_default()
    };
    if player_id == 0 {
        return;
    }
    if let Some(object) = player.as_object_mut() {
        object.insert("match_id".to_owned(), Value::from(match_id));
        object.insert("player_id".to_owned(), Value::from(player_id));
    }
    players
        .entry(player_id)
        .or_insert_with(|| mark_recovered(player));
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn recovery_player_prefers_payload_identity_over_storage_fallback() {
        let mut players = HashMap::new();
        insert_recovery_player(
            &mut players,
            json!({
                "match_id": 123,
                "player_id": 456,
                "source": "match_history"
            }),
            123,
            Some(999),
        );

        assert!(players.contains_key(&456));
        assert!(!players.contains_key(&999));
    }

    #[test]
    fn recovery_player_uses_storage_identity_only_when_payload_has_none() {
        let mut players = HashMap::new();
        insert_recovery_player(
            &mut players,
            json!({
                "match_id": 123,
                "player_id": 0,
                "source": "match_history"
            }),
            123,
            Some(999),
        );

        assert_eq!(players[&999]["player_id"], json!(999));
    }

    #[test]
    fn recovery_history_carries_map_without_changing_public_history_normalization() {
        let normalized = normalize_history_for_recovery(&json!({
            "Match": 123,
            "playerId": 456,
            "Map_Game": "LIVE Jaguar Falls"
        }));
        assert_eq!(normalized["map"], "LIVE Jaguar Falls");
    }
}
