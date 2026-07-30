use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::{
    database::Database,
    operations::{ApiCaller, OperationService},
    provider::RelayError,
};

#[async_trait]
pub trait PlayerNameLookup: Send + Sync {
    async fn name(&self, player_id: i64) -> Result<Option<String>, RelayError>;
}

pub struct PostgresPlayerNameLookup {
    database: Arc<Database>,
}

impl PostgresPlayerNameLookup {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
}

#[async_trait]
impl PlayerNameLookup for PostgresPlayerNameLookup {
    async fn name(&self, player_id: i64) -> Result<Option<String>, RelayError> {
        let client = self
            .database
            .connection()
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        let rows = client
            .query("SELECT name FROM players WHERE id = $1", &[&player_id])
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))?;
        Ok(rows
            .first()
            .and_then(|row| row.try_get::<_, Option<String>>("name").ok())
            .flatten())
    }
}

pub struct PlayerBatchLookupService<C: ApiCaller, N: PlayerNameLookup> {
    operations: OperationService<C>,
    names: Arc<N>,
}

impl<C: ApiCaller, N: PlayerNameLookup> PlayerBatchLookupService<C, N> {
    pub fn new(api: Arc<C>, names: Arc<N>) -> Self {
        Self {
            operations: OperationService::new(api),
            names,
        }
    }

    pub async fn get_player_batch_lookup(
        &self,
        player_ids: &[u64],
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        let mut results = self
            .operations
            .get_player_batch(
                &player_ids
                    .iter()
                    .map(|player_id| *player_id as f64)
                    .collect::<Vec<_>>(),
                true,
                consumer,
            )
            .await?;
        let found: HashSet<_> = results
            .iter()
            .filter_map(|value| parsed_player_id(value, &["Id", "player_id"]))
            .collect();

        for player_id in player_ids
            .iter()
            .copied()
            .filter(|player_id| !found.contains(player_id))
        {
            let Ok(player_id_i64) = i64::try_from(player_id) else {
                continue;
            };
            let Some(name) = self.names.name(player_id_i64).await? else {
                continue;
            };
            let name_results = match self.operations.get_player_id_by_name(&name, consumer).await {
                Ok(results) => results,
                Err(_) => continue,
            };
            let is_private = name_results.iter().any(|value| {
                parsed_player_id(value, &["player_id", "Id"]) == Some(player_id)
                    && value
                        .get("privacy_flag")
                        .and_then(Value::as_str)
                        .is_some_and(|flag| flag.eq_ignore_ascii_case("y"))
            });
            if is_private {
                results.push(json!({
                    "Id": player_id,
                    "ActivePlayerId": player_id,
                    "Name": name,
                    "ret_msg": "Player Privacy Flag set",
                    "privacy_flag": "y"
                }));
            }
        }
        Ok(results)
    }
}

fn parsed_player_id(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        let value = value.get(*key)?;
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
            .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
            .filter(|value| *value > 0)
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use crate::{hirez_client::ApiRequestOptions, operations::ApiCaller};

    use super::*;

    struct FakeCaller {
        responses: Mutex<Vec<Value>>,
        calls: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ApiCaller for FakeCaller {
        async fn call(
            &self,
            method: &str,
            _params: &[String],
            _options: ApiRequestOptions,
            _consumer: &str,
        ) -> Result<Value, RelayError> {
            self.calls.lock().expect("calls").push(method.to_owned());
            Ok(self.responses.lock().expect("responses").remove(0))
        }
    }

    struct FakeNames(HashMap<i64, String>);

    #[async_trait]
    impl PlayerNameLookup for FakeNames {
        async fn name(&self, player_id: i64) -> Result<Option<String>, RelayError> {
            Ok(self.0.get(&player_id).cloned())
        }
    }

    #[tokio::test]
    async fn missing_known_private_id_gets_compatible_marker() {
        let api = Arc::new(FakeCaller {
            responses: Mutex::new(vec![
                serde_json::json!([{"Id": 1, "Name": "Public"}]),
                serde_json::json!([{"player_id": 2, "privacy_flag": "Y"}]),
            ]),
            calls: Mutex::new(Vec::new()),
        });
        let names = Arc::new(FakeNames(HashMap::from([(2, "Private".to_owned())])));
        let service = PlayerBatchLookupService::new(api.clone(), names);
        let results = service
            .get_player_batch_lookup(&[1, 2, 3], "search")
            .await
            .expect("lookup");
        assert_eq!(results.len(), 2);
        assert_eq!(results[1]["Id"], 2);
        assert_eq!(results[1]["Name"], "Private");
        assert_eq!(results[1]["privacy_flag"], "y");
        assert_eq!(
            api.calls.lock().expect("calls").as_slice(),
            ["getplayerbatch", "getplayeridbyname"]
        );
    }
}
