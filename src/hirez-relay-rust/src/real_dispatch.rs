use std::{path::PathBuf, sync::Arc};

use serde::Serialize;
use serde_json::{Map, Value};

use crate::{
    contract::validate_operation,
    dispatch_result::RelayDispatchResult,
    dummy::DummyProvider,
    hirez_client::HirezApiClient,
    history_operation::{MatchHistoryService, PostgresHistoryCache},
    key_pool::KeyPool,
    live_provider::LiveMatchProvider,
    operations::OperationService,
    player_lookup::{PlayerBatchLookupService, PostgresPlayerNameLookup},
    provider::RelayError,
    raw_buffer_store::{RawBufferStore, RawPayload},
    resolver::{get_match_details_batch, resume_match_recovery},
    usage_probe::DirectUsageProbe,
};

pub struct RealRuntime {
    operations: OperationService<HirezApiClient>,
    history: MatchHistoryService<HirezApiClient, PostgresHistoryCache>,
    player_lookup: PlayerBatchLookupService<HirezApiClient, PostgresPlayerNameLookup>,
    match_provider: Arc<LiveMatchProvider>,
    key_pool: Arc<KeyPool>,
    usage_probe: Arc<DirectUsageProbe>,
    raw_buffer: Arc<RawBufferStore>,
    dummy_provider: Arc<DummyProvider>,
    key_file: Option<PathBuf>,
    raw_api: Arc<HirezApiClient>,
}

pub struct RealRuntimeParts {
    pub api: Arc<HirezApiClient>,
    pub history_cache: Arc<PostgresHistoryCache>,
    pub player_names: Arc<PostgresPlayerNameLookup>,
    pub match_provider: Arc<LiveMatchProvider>,
    pub key_pool: Arc<KeyPool>,
    pub usage_probe: Arc<DirectUsageProbe>,
    pub raw_buffer: Arc<RawBufferStore>,
    pub dummy_provider: Arc<DummyProvider>,
    pub key_file: Option<PathBuf>,
    pub public_history_ttl_minutes: u32,
}

impl RealRuntime {
    pub fn new(parts: RealRuntimeParts) -> Self {
        Self {
            operations: OperationService::new(parts.api.clone()),
            history: MatchHistoryService::new(
                parts.api.clone(),
                parts.history_cache,
                parts.public_history_ttl_minutes,
            ),
            raw_api: parts.api.clone(),
            player_lookup: PlayerBatchLookupService::new(parts.api, parts.player_names),
            match_provider: parts.match_provider,
            key_pool: parts.key_pool,
            usage_probe: parts.usage_probe,
            raw_buffer: parts.raw_buffer,
            dummy_provider: parts.dummy_provider,
            key_file: parts.key_file,
        }
    }

    pub async fn dispatch(
        &self,
        operation: &str,
        args: &[Value],
        consumer: &str,
    ) -> Result<Option<RelayDispatchResult>, RelayError> {
        validate_operation(operation, args, "real")?;
        let result = match operation {
            "getMatchDetailsBatch" => {
                let requests =
                    crate::contract::parse_completed_match_requests(args.first().ok_or_else(
                        || RelayError::Validation("requests are required".to_owned()),
                    )?)?;
                let provider = self.match_provider.attributed(consumer);
                return Ok(Some(RelayDispatchResult::CompletedMatches(
                    get_match_details_batch(&provider, &requests).await?,
                )));
            }
            "resumeMatchRecovery" => {
                let requests =
                    crate::contract::parse_completed_match_requests(args.first().ok_or_else(
                        || RelayError::Validation("requests are required".to_owned()),
                    )?)?;
                let request = requests.first().ok_or_else(|| {
                    RelayError::Validation("exactly one recovery request is required".to_owned())
                })?;
                let provider = self.match_provider.attributed(consumer);
                return Ok(Some(RelayDispatchResult::CompletedMatches(vec![
                    resume_match_recovery(&provider, request).await?,
                ])));
            }
            "getDataUsed" => {
                let dev_id = text_arg(args, 0)?;
                match self.key_pool.monitoring_credential(dev_id) {
                    Some(credential) => self
                        .usage_probe
                        .get_data_used_raw(&credential)
                        .await
                        .unwrap_or_else(|_| Value::Object(Map::new())),
                    None => Value::Object(Map::new()),
                }
            }
            "syncApiKeyUsage" => {
                self.key_pool.sync_usage(text_arg(args, 0)?).await;
                Value::Bool(true)
            }
            "getMatchIdsByQueue" => to_value(
                self.operations
                    .get_match_ids_by_queue(
                        number_arg(args, 0)?,
                        text_arg(args, 1)?,
                        number_arg(args, 2)?,
                        consumer,
                    )
                    .await?,
            )?,
            "getMatchIdsByQueueDetails" => to_value(
                self.operations
                    .get_match_ids_by_queue_details(
                        number_arg(args, 0)?,
                        text_arg(args, 1)?,
                        number_arg(args, 2)?,
                        consumer,
                    )
                    .await?,
            )?,
            "getMatchDetailsBatchRaw" => to_value(
                self.operations
                    .get_match_details_batch_raw(&number_array_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getMatchDetailsRaw" => to_value(
                self.operations
                    .get_match_details_raw(number_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getPlayerChampions" => {
                self.operations
                    .get_player_champions(number_arg(args, 0)?, consumer)
                    .await?
            }
            "getChampionRanks" => {
                self.operations
                    .get_champion_ranks(number_arg(args, 0)?, consumer)
                    .await?
            }
            "getChampions" => to_value(self.operations.get_champions(consumer).await?)?,
            "getItems" => to_value(self.operations.get_items(consumer).await?)?,
            "getEsportsProLeagueDetails" => to_value(
                self.operations
                    .get_esports_pro_league_details(consumer)
                    .await?,
            )?,
            "getPlayerLoadouts" => {
                self.operations
                    .get_player_loadouts(number_arg(args, 0)?, consumer)
                    .await?
            }
            "getPlayerStatus" => to_value(
                self.operations
                    .get_player_status(number_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getMatchPlayerDetails" => to_value(
                self.operations
                    .get_match_player_details(number_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getLeagueLeaderboard" => {
                self.operations
                    .get_league_leaderboard(
                        number_arg(args, 0)?,
                        number_arg(args, 1)?,
                        number_arg(args, 2)?,
                        consumer,
                    )
                    .await?
            }
            "getLeagueSeasons" => {
                self.operations
                    .get_league_seasons(number_arg(args, 0)?, consumer)
                    .await?
            }
            "getPlayerBatchFromMatch" => to_value(
                self.operations
                    .get_player_batch_from_match(number_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getDemoDetails" => {
                self.operations
                    .get_demo_details(number_arg(args, 0)?, consumer)
                    .await?
            }
            "getPlayerBatch" => to_value(
                self.operations
                    .get_player_batch(&number_array_arg(args, 0)?, false, consumer)
                    .await?,
            )?,
            "getPlayerBatchLookup" => to_value(
                self.player_lookup
                    .get_player_batch_lookup(&positive_integer_array_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getMatchHistory" => to_value(
                self.history
                    .get_match_history(
                        number_arg(args, 0)?,
                        optional_number_arg(args, 1).unwrap_or(50.0),
                        optional_bool_arg(args, 2).unwrap_or(false),
                        consumer,
                    )
                    .await?,
            )?,
            "getPlayers" => to_value(
                self.operations
                    .get_players(&text_array_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getPlayerIdByName" => to_value(
                self.operations
                    .get_player_id_by_name(text_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "searchPlayers" => to_value(
                self.operations
                    .search_players(text_arg(args, 0)?, consumer)
                    .await?,
            )?,
            "getPlayerIdsByGamerTag" => to_value(
                self.operations
                    .get_player_ids_by_gamer_tag(number_arg(args, 0)?, text_arg(args, 1)?, consumer)
                    .await?,
            )?,
            "getPlayerIdByPortalUserId" => to_value(
                self.operations
                    .get_player_id_by_portal_user_id(
                        number_arg(args, 0)?,
                        text_arg(args, 1)?,
                        consumer,
                    )
                    .await?,
            )?,
            "getMatchLeaderboard" => to_value(
                self.operations
                    .get_match_leaderboard(number_arg(args, 0)?, number_arg(args, 1)?, consumer)
                    .await?,
            )?,
            "dumpRawPayloads" => {
                let payloads = raw_payloads_arg(args, 0)?;
                to_value(
                    self.raw_buffer
                        .dump_raw_payloads(&payloads)
                        .await
                        .map_err(|error| RelayError::Upstream(error.to_string()))?,
                )?
            }
            "resetDummyApiCallCounts" => {
                self.dummy_provider.reset_counts();
                to_value(self.dummy_provider.counts())?
            }
            "getDummyApiCallCounts" => to_value(self.dummy_provider.counts())?,
            "reloadApiKeyPool" => {
                self.key_pool
                    .reload(self.key_file.as_deref())
                    .await
                    .map_err(|error| RelayError::Upstream(error.to_string()))?;
                Value::Bool(true)
            }
            "callRawEndpoint" => {
                // Raw passthrough — no validation, no transformation.
                // User supplies method + params verbatim; Hi-Rez response returns verbatim.
                let method = text_arg(args, 0)?;
                let params: Vec<String> = args
                    .get(1)
                    .and_then(|v| v.as_array())
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                self.raw_api
                    .api_request(
                        method,
                        &params,
                        crate::hirez_client::ApiRequestOptions::default(),
                        consumer,
                    )
                    .await
                    .map_err(|e| RelayError::Upstream(e.to_string()))?
            }
            "getApiKeyStatus" => to_value(self.key_pool.status())?,
            "cleanupFetchedPlayersCache" | "clearMatchHistoryCache" => {
                return Ok(Some(RelayDispatchResult::Json(Value::Bool(true))));
            }
            _ => {
                return Err(RelayError::Unsupported(format!(
                    "Unsupported HirezRelay operation: {operation}"
                )));
            }
        };
        Ok(Some(RelayDispatchResult::Json(sanitize_json_strings(
            result,
        ))))
    }
}

fn sanitize_json_strings(value: Value) -> Value {
    match value {
        Value::String(value) => Value::String(value.replace('\0', "").replace("\\u0000", "")),
        Value::Array(values) => {
            Value::Array(values.into_iter().map(sanitize_json_strings).collect())
        }
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    (
                        key.replace('\0', "").replace("\\u0000", ""),
                        sanitize_json_strings(value),
                    )
                })
                .collect(),
        ),
        value => value,
    }
}

fn to_value(value: impl Serialize) -> Result<Value, RelayError> {
    serde_json::to_value(value).map_err(|error| RelayError::Upstream(error.to_string()))
}

fn number_arg(args: &[Value], index: usize) -> Result<f64, RelayError> {
    js_number(args.get(index))
        .ok_or_else(|| RelayError::Validation(format!("argument {index} must be a finite number")))
}

fn optional_number_arg(args: &[Value], index: usize) -> Option<f64> {
    args.get(index).and_then(|value| js_number(Some(value)))
}

fn optional_bool_arg(args: &[Value], index: usize) -> Option<bool> {
    args.get(index).and_then(Value::as_bool)
}

fn text_arg(args: &[Value], index: usize) -> Result<&str, RelayError> {
    args.get(index)
        .and_then(Value::as_str)
        .ok_or_else(|| RelayError::Validation(format!("argument {index} must be a string")))
}

fn number_array_arg(args: &[Value], index: usize) -> Result<Vec<f64>, RelayError> {
    args.get(index)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| js_number(Some(value)))
                .collect()
        })
        .ok_or_else(|| RelayError::Validation(format!("argument {index} must be an array")))
}

fn positive_integer_array_arg(args: &[Value], index: usize) -> Result<Vec<u64>, RelayError> {
    number_array_arg(args, index)?
        .into_iter()
        .map(|value| {
            if value > 0.0 && value.fract() == 0.0 && value <= u64::MAX as f64 {
                Ok(value as u64)
            } else {
                Err(RelayError::Validation(format!(
                    "argument {index} must contain positive integers"
                )))
            }
        })
        .collect()
}

fn text_array_arg(args: &[Value], index: usize) -> Result<Vec<String>, RelayError> {
    args.get(index)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .ok_or_else(|| RelayError::Validation(format!("argument {index} must be an array")))
}

pub(crate) fn raw_payloads_arg(
    args: &[Value],
    index: usize,
) -> Result<Vec<RawPayload>, RelayError> {
    args.get(index)
        .and_then(Value::as_array)
        .ok_or_else(|| RelayError::Validation(format!("argument {index} must be an array")))?
        .iter()
        .map(|payload| {
            let object = payload.as_object().ok_or_else(|| {
                RelayError::Validation("raw payload must be an object".to_owned())
            })?;
            Ok(RawPayload {
                endpoint: object
                    .get("endpoint")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                entity_type: object
                    .get("entity_type")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                entity_id: object.get("entity_id").and_then(js_entity_id),
                raw_data: object
                    .get("raw_data")
                    .cloned()
                    .unwrap_or_else(|| Value::Object(Map::new())),
            })
        })
        .collect()
}

fn js_entity_id(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::Bool(false) => None,
        Value::Bool(true) => Some("true".to_owned()),
        Value::Number(value) if value.as_f64() == Some(0.0) => None,
        Value::Number(value) => Some(value.to_string()),
        Value::String(value) if value.is_empty() => None,
        Value::String(value) => Some(value.clone()),
        Value::Array(values) if values.is_empty() => None,
        Value::Array(values) => Some(
            values
                .iter()
                .map(|value| js_entity_id(value).unwrap_or_default())
                .collect::<Vec<_>>()
                .join(","),
        ),
        Value::Object(_) => Some("[object Object]".to_owned()),
    }
}

fn js_number(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Null => Some(0.0),
        Value::Bool(value) => Some(if *value { 1.0 } else { 0.0 }),
        Value::Number(value) => value.as_f64().filter(|number| number.is_finite()),
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Some(0.0)
            } else {
                trimmed
                    .parse::<f64>()
                    .ok()
                    .filter(|number| number.is_finite())
            }
        }
        Value::Array(values) if values.is_empty() => Some(0.0),
        Value::Array(values) if values.len() == 1 => js_number(values.first()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;

    #[test]
    fn real_dispatch_arms_cover_every_manifest_real_operation_exactly() {
        let source = include_str!("real_dispatch.rs");
        let handler = source
            .split_once("let result = match operation {")
            .map(|(_, suffix)| suffix)
            .and_then(|suffix| suffix.split_once("\n            _ =>"))
            .map(|(handler, _)| handler)
            .expect("real operation dispatch match");
        let implemented = handler
            .lines()
            .filter_map(|line| {
                let trimmed = line.trim();
                (trimmed.starts_with('"') && trimmed.contains("=>"))
                    .then(|| trimmed.split_once("=>").expect("operation arm").0)
            })
            .flat_map(|arm| arm.split('|'))
            .map(|name| name.trim().trim_matches('"').to_owned())
            .collect::<BTreeSet<_>>();
        let declared = crate::contract::manifest()
            .operations
            .iter()
            .filter(|operation| operation.modes.iter().any(|mode| mode == "real"))
            .map(|operation| operation.name.clone())
            .collect::<BTreeSet<_>>();

        assert_eq!(implemented, declared);
    }

    #[test]
    fn raw_payload_entity_ids_match_javascript_truthy_string_coercion() {
        let args = vec![json!([
            {"endpoint":"x","entity_type":"player","entity_id":0,"raw_data":[]},
            {"endpoint":"x","entity_type":"player","entity_id":12,"raw_data":[]},
            {"endpoint":"x","entity_type":"player","entity_id":"abc","raw_data":[]}
        ])];
        let payloads = raw_payloads_arg(&args, 0).expect("payloads");
        assert_eq!(payloads[0].entity_id, None);
        assert_eq!(payloads[1].entity_id.as_deref(), Some("12"));
        assert_eq!(payloads[2].entity_id.as_deref(), Some("abc"));
    }

    #[test]
    fn api_key_status_uses_the_mixed_case_typescript_wire_shape() {
        let status = crate::key_pool::ApiKeyStatus {
            dev_id: "2116".to_owned(),
            auth_key: "secret".to_owned(),
            status: crate::key_pool::KeyStatus::Healthy,
            daily_limit: 15_000,
            used_24h: 10,
            remaining: 14_990,
            reserve_threshold: 100,
            calls_total: 20,
            consecutive_failures: 0,
            last_used: String::new(),
        };
        let value = serde_json::to_value(status).expect("serialize");
        assert_eq!(value["devId"], "2116");
        assert_eq!(value["authKey"], "secret");
        assert_eq!(value["daily_limit"], 15_000);
        assert!(value.get("dailyLimit").is_none());
    }

    #[test]
    fn sanitizes_postgres_incompatible_nulls_from_upstream_json() {
        let value = sanitize_json_strings(json!({
            "name": "lowelo\0player",
            "nested": ["escaped\\u0000value"],
        }));
        assert_eq!(value["name"], "loweloplayer");
        assert_eq!(value["nested"][0], "escapedvalue");
    }

    #[test]
    fn cleanup_operations_return_an_acknowledgement_payload() {
        let source = include_str!("real_dispatch.rs");
        assert!(source.contains("return Ok(Some(RelayDispatchResult::Json(Value::Bool(true))))"));
    }
}
