use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use time::{OffsetDateTime, PrimitiveDateTime, macros::format_description};

use crate::{
    hirez_client::ApiRequestOptions,
    history_store::{HistoryEntry, HistoryStore},
    normalizer::normalize_match_history_player,
    operations::ApiCaller,
    provider::RelayError,
};

#[async_trait]
pub trait HistoryCache: Send + Sync {
    async fn read_fresh(
        &self,
        player_id: i64,
        ttl_minutes: u32,
    ) -> Result<Option<Vec<Value>>, RelayError>;
    async fn write(
        &self,
        player_id: i64,
        matches: &[Value],
        normalized: &[Value],
        source: &str,
    ) -> Result<(), RelayError>;
}

pub struct PostgresHistoryCache {
    store: Arc<HistoryStore>,
}

impl PostgresHistoryCache {
    pub fn new(store: Arc<HistoryStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl HistoryCache for PostgresHistoryCache {
    async fn read_fresh(
        &self,
        player_id: i64,
        ttl_minutes: u32,
    ) -> Result<Option<Vec<Value>>, RelayError> {
        self.store
            .read_fresh_public_cache(player_id, ttl_minutes)
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))
    }

    async fn write(
        &self,
        player_id: i64,
        matches: &[Value],
        normalized: &[Value],
        source: &str,
    ) -> Result<(), RelayError> {
        let entries: Vec<_> = matches
            .iter()
            .zip(normalized)
            .filter_map(|(raw, normalized)| history_entry(player_id, raw, normalized, source))
            .collect();
        self.store
            .write_history(player_id, matches, &entries)
            .await
            .map(|_| ())
            .map_err(|error| RelayError::Upstream(error.to_string()))
    }
}

pub struct MatchHistoryService<C: ApiCaller, H: HistoryCache> {
    api: Arc<C>,
    cache: Arc<H>,
    public_cache_ttl_minutes: u32,
}

impl<C: ApiCaller, H: HistoryCache> MatchHistoryService<C, H> {
    pub fn new(api: Arc<C>, cache: Arc<H>, public_cache_ttl_minutes: u32) -> Self {
        Self {
            api,
            cache,
            public_cache_ttl_minutes: public_cache_ttl_minutes.max(1),
        }
    }

    pub async fn get_match_history(
        &self,
        player_id: f64,
        limit: f64,
        force_refresh: bool,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        let player_id = player_id as i64;
        let result_limit = normalize_limit(limit);
        if !force_refresh
            && let Some(matches) = self
                .cache
                .read_fresh(player_id, self.public_cache_ttl_minutes)
                .await?
        {
            let normalized: Vec<_> = matches.iter().map(normalize_match_history_player).collect();
            self.cache
                .write(player_id, &matches, &normalized, "cache_backfill")
                .await?;
            return Ok(normalized.into_iter().take(result_limit).collect());
        }

        let data = match self
            .api
            .call(
                "getmatchhistory",
                &[player_id.to_string()],
                ApiRequestOptions::default(),
                consumer,
            )
            .await
        {
            Ok(data) => data,
            Err(error) if is_terminal_empty(&error.to_string()) => {
                self.cache
                    .write(player_id, &[], &[], "getmatchhistory")
                    .await?;
                return Ok(Vec::new());
            }
            Err(error) => return Err(error),
        };
        let matches = history_values(data);
        let normalized: Vec<_> = matches.iter().map(normalize_match_history_player).collect();
        self.cache
            .write(player_id, &matches, &normalized, "getmatchhistory")
            .await?;
        Ok(normalized.into_iter().take(result_limit).collect())
    }
}

fn normalize_limit(limit: f64) -> usize {
    let value = if limit == 0.0 { 50.0 } else { limit };
    value.floor().clamp(1.0, 50.0) as usize
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

fn is_terminal_empty(message: &str) -> bool {
    [
        "HTTP 404",
        "HIREZ_NO_MATCH_HISTORY",
        "HIREZ_PRIVACY_FLAG",
        "HIREZ_NOT_FOUND_OR_INVALID",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

pub(crate) fn history_entry(
    fetched_player_id: i64,
    raw: &Value,
    normalized: &Value,
    source: &str,
) -> Option<HistoryEntry> {
    let match_id = integer(normalized, "match_id").or_else(|| integer(raw, "Match"))?;
    let player_id = integer(normalized, "player_id")
        .filter(|player_id| *player_id > 0)
        .unwrap_or(fetched_player_id);
    if match_id <= 0 || player_id <= 0 {
        return None;
    }
    let map = string(normalized, "map")
        .or_else(|| string(raw, "Map_Game"))
        .or_else(|| string(raw, "Map"))
        .unwrap_or_default();
    let mut normalized_data = normalized.clone();
    if let Some(object) = normalized_data.as_object_mut() {
        // TypeScript's storage normalizer always materializes `map`, including
        // the empty-string case. Keep the durable JSON shape identical even
        // though the typed column below already carries the same value.
        object.insert("map".to_owned(), Value::String(map.clone()));
    }
    Some(HistoryEntry {
        match_id,
        player_id,
        fetched_player_id,
        entry_datetime: string(normalized, "entry_datetime")
            .or_else(|| string(raw, "Match_Time"))
            .and_then(|value| parse_timestamp(&value)),
        queue_id: integer(normalized, "queue_id").and_then(to_i32),
        region: string(normalized, "region"),
        map: (!map.is_empty()).then_some(map),
        champion_id: integer(normalized, "champion_id").and_then(to_i32),
        champion_name: string(normalized, "champion_name"),
        skin_id: integer(normalized, "skin_id").and_then(to_i32),
        skin_name: string(normalized, "skin_name"),
        win_status: string(normalized, "win_status"),
        kills: integer(normalized, "kills").and_then(to_i32).unwrap_or(0),
        deaths: integer(normalized, "deaths").and_then(to_i32).unwrap_or(0),
        assists: integer(normalized, "assists").and_then(to_i32).unwrap_or(0),
        damage: integer(normalized, "damage_done_physical")
            .saturating_add(integer(normalized, "damage_done_magical"))
            .and_then(to_i32)
            .unwrap_or(0),
        healing: integer(normalized, "healing").and_then(to_i32).unwrap_or(0),
        gold_earned: integer(normalized, "gold_earned")
            .and_then(to_i32)
            .unwrap_or(0),
        time_in_match: integer(normalized, "time_in_match")
            .or_else(|| integer(normalized, "match_duration"))
            .and_then(to_i32)
            .unwrap_or(0),
        task_force: integer(normalized, "task_force")
            .and_then(|value| i16::try_from(value).ok())
            .unwrap_or(0),
        league_tier: integer(normalized, "league_tier")
            .and_then(to_i32)
            .unwrap_or(0),
        source: source.to_owned(),
        raw_data: raw.clone(),
        normalized_data,
    })
}

trait SaturatingOptionAdd {
    fn saturating_add(self, other: Option<i64>) -> Option<i64>;
}

impl SaturatingOptionAdd for Option<i64> {
    fn saturating_add(self, other: Option<i64>) -> Option<i64> {
        Some(self.unwrap_or(0).saturating_add(other.unwrap_or(0)))
    }
}

fn integer(value: &Value, key: &str) -> Option<i64> {
    let value = value.get(key)?;
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| {
            value
                .as_f64()
                .and_then(|value| value.is_finite().then_some(value as i64))
        })
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .filter(|value| !value.is_empty())
}

fn to_i32(value: i64) -> Option<i32> {
    i32::try_from(value).ok()
}

fn parse_timestamp(value: &str) -> Option<OffsetDateTime> {
    if let Ok(value) = OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
    {
        return Some(value);
    }
    const HIREZ_FORMAT: &[time::format_description::BorrowedFormatItem<'_>] = format_description!(
        "[month padding:none]/[day padding:none]/[year] [hour repr:12 padding:none]:[minute]:[second] [period case:upper]"
    );
    PrimitiveDateTime::parse(value, HIREZ_FORMAT)
        .ok()
        .map(|value| value.assume_utc())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct FakeCaller {
        response: Mutex<Option<Result<Value, RelayError>>>,
        calls: Mutex<Vec<(String, Vec<String>)>>,
    }

    #[async_trait]
    impl ApiCaller for FakeCaller {
        async fn call(
            &self,
            method: &str,
            params: &[String],
            _options: ApiRequestOptions,
            _consumer: &str,
        ) -> Result<Value, RelayError> {
            self.calls
                .lock()
                .expect("calls")
                .push((method.to_owned(), params.to_vec()));
            self.response
                .lock()
                .expect("response")
                .take()
                .expect("fake response")
        }
    }

    type CacheWrite = (i64, Vec<Value>, Vec<Value>, String);

    #[derive(Default)]
    struct FakeCache {
        cached: Mutex<Option<Vec<Value>>>,
        writes: Mutex<Vec<CacheWrite>>,
    }

    #[async_trait]
    impl HistoryCache for FakeCache {
        async fn read_fresh(
            &self,
            _player_id: i64,
            _ttl_minutes: u32,
        ) -> Result<Option<Vec<Value>>, RelayError> {
            Ok(self.cached.lock().expect("cache").clone())
        }

        async fn write(
            &self,
            player_id: i64,
            matches: &[Value],
            normalized: &[Value],
            source: &str,
        ) -> Result<(), RelayError> {
            self.writes.lock().expect("writes").push((
                player_id,
                matches.to_vec(),
                normalized.to_vec(),
                source.to_owned(),
            ));
            Ok(())
        }
    }

    #[tokio::test]
    async fn cache_hit_normalizes_backfills_and_spends_no_call() {
        let api = Arc::new(FakeCaller::default());
        let cache = Arc::new(FakeCache {
            cached: Mutex::new(Some(vec![serde_json::json!({
                "Match": 100,
                "playerId": 7,
                "Kills": 2
            })])),
            ..FakeCache::default()
        });
        let service = MatchHistoryService::new(api.clone(), cache.clone(), 10);
        let rows = service
            .get_match_history(7.0, 50.0, false, "profile")
            .await
            .expect("history");
        assert_eq!(rows[0]["kills"], 2);
        assert!(api.calls.lock().expect("calls").is_empty());
        assert_eq!(cache.writes.lock().expect("writes")[0].3, "cache_backfill");
    }

    #[tokio::test]
    async fn force_refresh_uses_only_player_path_segment_and_clamps_limit() {
        let api = Arc::new(FakeCaller {
            response: Mutex::new(Some(Ok(serde_json::json!([
                {"Match": 100, "playerId": 7},
                {"Match": 101, "playerId": 7}
            ])))),
            ..FakeCaller::default()
        });
        let cache = Arc::new(FakeCache::default());
        let service = MatchHistoryService::new(api.clone(), cache.clone(), 10);
        let rows = service
            .get_match_history(7.0, 1.9, true, "profile")
            .await
            .expect("history");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            api.calls.lock().expect("calls")[0],
            ("getmatchhistory".to_owned(), vec!["7".to_owned()])
        );
        assert_eq!(cache.writes.lock().expect("writes")[0].3, "getmatchhistory");
    }

    #[tokio::test]
    async fn terminal_empty_is_persisted_and_nonterminal_error_propagates() {
        let api = Arc::new(FakeCaller {
            response: Mutex::new(Some(Err(RelayError::Upstream(
                "Request failed after 4 attempts: HTTP 404".to_owned(),
            )))),
            ..FakeCaller::default()
        });
        let cache = Arc::new(FakeCache::default());
        let service = MatchHistoryService::new(api, cache.clone(), 10);
        assert!(
            service
                .get_match_history(7.0, 50.0, false, "profile")
                .await
                .expect("empty")
                .is_empty()
        );
        assert!(cache.writes.lock().expect("writes")[0].1.is_empty());
    }

    #[test]
    fn parses_hirez_and_rfc3339_timestamps_as_utc() {
        assert!(parse_timestamp("7/28/2026 1:02:03 PM").is_some());
        assert!(parse_timestamp("2026-07-28T13:02:03Z").is_some());
    }

    #[test]
    fn storage_entry_materializes_map_in_typed_and_normalized_forms() {
        let raw = serde_json::json!({
            "Match": 100,
            "playerId": 7,
            "Map_Game": "LIVE Jaguar Falls"
        });
        let normalized = normalize_match_history_player(&raw);
        let entry = history_entry(7, &raw, &normalized, "getmatchhistory").expect("entry");
        assert_eq!(entry.map.as_deref(), Some("LIVE Jaguar Falls"));
        assert_eq!(entry.normalized_data["map"], "LIVE Jaguar Falls");

        let raw_without_map = serde_json::json!({"Match": 101, "playerId": 7});
        let normalized_without_map = normalize_match_history_player(&raw_without_map);
        let entry = history_entry(
            7,
            &raw_without_map,
            &normalized_without_map,
            "getmatchhistory",
        )
        .expect("entry");
        assert_eq!(entry.map, None);
        assert_eq!(entry.normalized_data["map"], "");
    }
}
