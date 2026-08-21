use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;

use crate::{
    hirez_client::{ApiRequestOptions, HirezApiClient},
    provider::RelayError,
};

const MATCH_BATCH_SIZE: usize = 10;
const PLAYER_BATCH_SIZE: usize = 20;

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct MatchIdObservation {
    pub match_id: u64,
    pub entry_datetime: Option<String>,
    pub region: String,
    pub active_flag: bool,
}

#[async_trait]
pub trait ApiCaller: Send + Sync {
    async fn call(
        &self,
        method: &str,
        params: &[String],
        options: ApiRequestOptions,
        consumer: &str,
    ) -> Result<Value, RelayError>;
}

#[async_trait]
impl ApiCaller for HirezApiClient {
    async fn call(
        &self,
        method: &str,
        params: &[String],
        options: ApiRequestOptions,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.api_request(method, params, options, consumer)
            .await
            .map_err(|error| RelayError::Upstream(error.to_string()))
    }
}

#[derive(Clone)]
pub struct OperationService<C: ApiCaller> {
    api: Arc<C>,
}

impl<C: ApiCaller> OperationService<C> {
    pub fn new(api: Arc<C>) -> Self {
        Self { api }
    }

    pub async fn get_match_ids_by_queue_details(
        &self,
        queue_id: f64,
        date: &str,
        hour: f64,
        consumer: &str,
    ) -> Result<Vec<MatchIdObservation>, RelayError> {
        let data = self
            .api
            .call(
                "getmatchidsbyqueue",
                &[
                    js_number_string(queue_id),
                    date.to_owned(),
                    js_number_string(hour),
                ],
                single_attempt(),
                consumer,
            )
            .await?;
        Ok(queue_observations(&data))
    }

    pub async fn get_match_ids_by_queue(
        &self,
        queue_id: f64,
        date: &str,
        hour: f64,
        consumer: &str,
    ) -> Result<Vec<u64>, RelayError> {
        Ok(self
            .get_match_ids_by_queue_details(queue_id, date, hour, consumer)
            .await?
            .into_iter()
            .map(|observation| observation.match_id)
            .collect())
    }

    pub async fn get_match_details_batch_raw(
        &self,
        match_ids: &[f64],
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.get_match_details_batch(match_ids, consumer, ApiRequestOptions::default())
            .await
    }

    /// Canonical ingestion owns its retry budget at the queue-hour boundary.
    /// One vendor batch must therefore translate to one physical API attempt.
    pub async fn get_match_details_batch_once(
        &self,
        match_ids: &[f64],
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.get_match_details_batch(match_ids, consumer, single_attempt())
            .await
    }

    async fn get_match_details_batch(
        &self,
        match_ids: &[f64],
        consumer: &str,
        options: ApiRequestOptions,
    ) -> Result<Vec<Value>, RelayError> {
        let mut results = Vec::new();
        for chunk in match_ids.chunks(MATCH_BATCH_SIZE) {
            let params = vec![
                chunk
                    .iter()
                    .map(|match_id| js_number_string(*match_id))
                    .collect::<Vec<_>>()
                    .join(","),
            ];
            let data = self
                .api
                .call("getmatchdetailsbatch", &params, options, consumer)
                .await?;
            if let Value::Array(values) = data {
                results.extend(values);
            }
        }
        Ok(results)
    }

    pub async fn get_match_details_raw(
        &self,
        match_id: f64,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        let data = self
            .api
            .call(
                "getmatchdetails",
                &[js_number_string(match_id)],
                ApiRequestOptions::default(),
                consumer,
            )
            .await?;
        Ok(value_array(data))
    }

    pub async fn get_player_batch_from_match(
        &self,
        match_id: f64,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        let data = self
            .api
            .call(
                "getplayerbatchfrommatch",
                &[js_number_string(match_id)],
                single_attempt(),
                consumer,
            )
            .await?;
        Ok(value_array(data))
    }

    pub async fn get_demo_details(
        &self,
        match_id: f64,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.get_demo_details_with_options(match_id, consumer, ApiRequestOptions::default())
            .await
    }

    pub async fn get_demo_details_once(
        &self,
        match_id: f64,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.get_demo_details_with_options(match_id, consumer, single_attempt())
            .await
    }

    async fn get_demo_details_with_options(
        &self,
        match_id: f64,
        consumer: &str,
        options: ApiRequestOptions,
    ) -> Result<Value, RelayError> {
        let data = self
            .api
            .call(
                "getdemodetails",
                &[js_number_string(match_id)],
                options,
                consumer,
            )
            .await?;
        Ok(if data.is_object() || data.is_array() {
            data
        } else {
            Value::Object(Default::default())
        })
    }

    pub async fn get_player_champions(
        &self,
        player_id: f64,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.raw_or_empty(
            "getplayerchampions",
            &[js_number_string(player_id)],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_champion_ranks(
        &self,
        player_id: f64,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.raw_or_empty(
            "getchampionranks",
            &[js_number_string(player_id)],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_champions(&self, consumer: &str) -> Result<Vec<Value>, RelayError> {
        self.array_call(
            "getchampions",
            &["1".to_owned()],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_items(&self, consumer: &str) -> Result<Vec<Value>, RelayError> {
        self.array_call(
            "getitems",
            &["1".to_owned()],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_esports_pro_league_details(
        &self,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_call(
            "getesportsproleaguedetails",
            &[],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_player_loadouts(
        &self,
        player_id: f64,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.raw_or_empty(
            "getplayerloadouts",
            &[js_number_string(player_id), "1".to_owned()],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_player_status(
        &self,
        player_id: f64,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_call(
            "getplayerstatus",
            &[js_number_string(player_id)],
            single_attempt(),
            consumer,
        )
        .await
    }

    pub async fn get_match_player_details(
        &self,
        match_id: f64,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_call(
            "getmatchplayerdetails",
            &[js_number_string(match_id)],
            single_attempt(),
            consumer,
        )
        .await
    }

    pub async fn get_league_leaderboard(
        &self,
        queue_id: f64,
        tier: f64,
        season: f64,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.raw_or_empty(
            "getleagueleaderboard",
            &[
                js_number_string(queue_id),
                js_number_string(tier),
                js_number_string(season),
            ],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_league_seasons(
        &self,
        queue_id: f64,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        self.raw_or_empty(
            "getleagueseasons",
            &[js_number_string(queue_id)],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    pub async fn get_player_batch(
        &self,
        player_ids: &[f64],
        single_attempt_only: bool,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        let options = if single_attempt_only {
            single_attempt()
        } else {
            ApiRequestOptions::default()
        };
        let mut results = Vec::new();
        for chunk in player_ids.chunks(PLAYER_BATCH_SIZE) {
            let data = self
                .api
                .call(
                    "getplayerbatch",
                    &[chunk
                        .iter()
                        .map(|player_id| js_number_string(*player_id))
                        .collect::<Vec<_>>()
                        .join(",")],
                    options,
                    consumer,
                )
                .await?;
            if let Value::Array(values) = data {
                results.extend(values);
            }
        }
        Ok(results)
    }

    pub async fn get_players(
        &self,
        names: &[String],
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        let mut results = Vec::new();
        for chunk in names.chunks(MATCH_BATCH_SIZE) {
            let data = self
                .api
                .call(
                    "getplayers",
                    &[chunk.join(",")],
                    ApiRequestOptions::default(),
                    consumer,
                )
                .await?;
            if let Value::Array(values) = data {
                results.extend(values);
            }
        }
        Ok(results)
    }

    pub async fn get_player_id_by_name(
        &self,
        player_name: &str,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_or_wrapped_call("getplayeridbyname", &[player_name.to_owned()], consumer)
            .await
    }

    pub async fn search_players(
        &self,
        search_player: &str,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_or_wrapped_call("searchplayers", &[search_player.to_owned()], consumer)
            .await
    }

    pub async fn get_player_ids_by_gamer_tag(
        &self,
        portal_id: f64,
        gamer_tag: &str,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_or_wrapped_call(
            "getplayeridsbygamertag",
            &[js_number_string(portal_id), gamer_tag.to_owned()],
            consumer,
        )
        .await
    }

    pub async fn get_player_id_by_portal_user_id(
        &self,
        portal_id: f64,
        portal_user_id: &str,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_or_wrapped_call(
            "getplayeridbyportaluserid",
            &[js_number_string(portal_id), portal_user_id.to_owned()],
            consumer,
        )
        .await
    }

    pub async fn get_match_leaderboard(
        &self,
        tier: f64,
        season: f64,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        self.array_call(
            "getmatchleaderboard",
            &[js_number_string(tier), js_number_string(season)],
            ApiRequestOptions::default(),
            consumer,
        )
        .await
    }

    async fn raw_or_empty(
        &self,
        method: &str,
        params: &[String],
        options: ApiRequestOptions,
        consumer: &str,
    ) -> Result<Value, RelayError> {
        let data = self.api.call(method, params, options, consumer).await?;
        Ok(if js_truthy(&data) {
            data
        } else {
            Value::Array(Vec::new())
        })
    }

    async fn array_call(
        &self,
        method: &str,
        params: &[String],
        options: ApiRequestOptions,
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        Ok(value_array(
            self.api.call(method, params, options, consumer).await?,
        ))
    }

    async fn array_or_wrapped_call(
        &self,
        method: &str,
        params: &[String],
        consumer: &str,
    ) -> Result<Vec<Value>, RelayError> {
        let data = self
            .api
            .call(method, params, single_attempt(), consumer)
            .await?;
        Ok(match data {
            Value::Array(values) => values,
            value if js_truthy(&value) => vec![value],
            _ => Vec::new(),
        })
    }
}

fn queue_observations(data: &Value) -> Vec<MatchIdObservation> {
    if let Some(values) = data.as_array() {
        return values.iter().filter_map(queue_observation).collect();
    }
    data.get("match_ids")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(positive_u64)
                .map(|match_id| MatchIdObservation {
                    match_id,
                    entry_datetime: None,
                    region: "Unknown".to_owned(),
                    active_flag: false,
                })
                .collect()
        })
        .unwrap_or_default()
}

fn queue_observation(value: &Value) -> Option<MatchIdObservation> {
    if !value.is_object() {
        return positive_u64(value).map(|match_id| MatchIdObservation {
            match_id,
            entry_datetime: None,
            region: "Unknown".to_owned(),
            active_flag: false,
        });
    }
    let match_id = ["Match", "match_id"]
        .into_iter()
        .find_map(|key| value.get(key))
        .and_then(positive_u64)?;
    let entry_datetime = ["Entry_Datetime", "entry_datetime"]
        .into_iter()
        .find_map(|key| value.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned);
    let region = ["Region", "region"]
        .into_iter()
        .find_map(|key| value.get(key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Unknown")
        .to_owned();
    let active_value = ["Active_Flag", "active_flag"]
        .into_iter()
        .find_map(|key| value.get(key));
    let active_flag = active_value.is_some_and(|value| {
        value.as_bool().unwrap_or(false)
            || value
                .as_str()
                .is_some_and(|value| value.eq_ignore_ascii_case("y"))
    });
    Some(MatchIdObservation {
        match_id,
        entry_datetime,
        region,
        active_flag,
    })
}

fn positive_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .filter(|value| *value > 0)
}

fn value_array(value: Value) -> Vec<Value> {
    value.as_array().cloned().unwrap_or_default()
}

fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|value| value != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn js_number_string(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn single_attempt() -> ApiRequestOptions {
    ApiRequestOptions {
        max_retries: Some(0),
        ..ApiRequestOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    #[derive(Clone, Debug)]
    struct RecordedCall {
        method: String,
        params: Vec<String>,
        max_retries: Option<usize>,
        consumer: String,
    }

    #[derive(Default)]
    struct FakeCaller {
        responses: Mutex<Vec<Value>>,
        calls: Mutex<Vec<RecordedCall>>,
    }

    impl FakeCaller {
        fn with_responses(responses: Vec<Value>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().rev().collect()),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn calls(&self) -> Vec<RecordedCall> {
            self.calls.lock().expect("calls").clone()
        }
    }

    #[async_trait]
    impl ApiCaller for FakeCaller {
        async fn call(
            &self,
            method: &str,
            params: &[String],
            options: ApiRequestOptions,
            consumer: &str,
        ) -> Result<Value, RelayError> {
            self.calls.lock().expect("calls").push(RecordedCall {
                method: method.to_owned(),
                params: params.to_vec(),
                max_retries: options.max_retries,
                consumer: consumer.to_owned(),
            });
            self.responses
                .lock()
                .expect("responses")
                .pop()
                .ok_or_else(|| RelayError::Upstream("missing fake response".to_owned()))
        }
    }

    #[tokio::test]
    async fn static_catalog_operations_preserve_hirez_method_and_language_contract() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!([{"LeagueId": 3}]),
            serde_json::json!([{"ItemId": 2}]),
            serde_json::json!([{"id": 1}]),
        ]));
        let service = OperationService::new(caller.clone());

        service
            .get_champions("operator_static_ingest")
            .await
            .expect("champions");
        service
            .get_items("operator_static_ingest")
            .await
            .expect("items");
        service
            .get_esports_pro_league_details("operator_static_ingest")
            .await
            .expect("esports");

        let calls = caller.calls();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0].method, "getchampions");
        assert_eq!(calls[0].params, vec!["1"]);
        assert_eq!(calls[1].method, "getitems");
        assert_eq!(calls[1].params, vec!["1"]);
        assert_eq!(calls[2].method, "getesportsproleaguedetails");
        assert!(calls[2].params.is_empty());
        assert!(
            calls
                .iter()
                .all(|call| call.consumer == "operator_static_ingest")
        );
    }

    #[tokio::test]
    async fn queue_details_are_single_attempt_for_every_canonical_consumer() {
        let caller = Arc::new(FakeCaller::with_responses(vec![serde_json::json!([
            {
                "Match": "100",
                "Entry_Datetime": " 7/28/2026 1:00:00 PM ",
                "Region": " North America ",
                "Active_Flag": "Y"
            },
            {"match_id": 101, "active_flag": true},
            {"Match": 0}
        ])]));
        let service = OperationService::new(caller.clone());
        assert_eq!(
            service
                .get_match_ids_by_queue_details(486.0, "20260728", 12.0, "hourly_match_discovery")
                .await
                .expect("queue"),
            vec![
                MatchIdObservation {
                    match_id: 100,
                    entry_datetime: Some("7/28/2026 1:00:00 PM".to_owned()),
                    region: "North America".to_owned(),
                    active_flag: true,
                },
                MatchIdObservation {
                    match_id: 101,
                    entry_datetime: None,
                    region: "Unknown".to_owned(),
                    active_flag: true,
                }
            ]
        );
        let calls = caller.calls();
        assert_eq!(calls[0].method, "getmatchidsbyqueue");
        assert_eq!(calls[0].params, ["486", "20260728", "12"]);
        assert_eq!(calls[0].max_retries, Some(0));
        assert_eq!(calls[0].consumer, "hourly_match_discovery");
    }

    #[tokio::test]
    async fn queue_id_fallback_accepts_match_ids_array() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!({"match_ids": ["10", 11, 0, "bad"]}),
        ]));
        let service = OperationService::new(caller);
        assert_eq!(
            service
                .get_match_ids_by_queue(486.0, "20260728", 1.0, "test")
                .await
                .expect("queue IDs"),
            [10, 11]
        );
    }

    #[tokio::test]
    async fn raw_batch_chunks_at_ten_and_flattens_only_arrays() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!([{"Match": 1}]),
            serde_json::json!([{"Match": 11}]),
        ]));
        let service = OperationService::new(caller.clone());
        let ids: Vec<f64> = (1..=11).map(f64::from).collect();
        assert_eq!(
            service
                .get_match_details_batch_raw(&ids, "buffer")
                .await
                .expect("raw")
                .len(),
            2
        );
        let calls = caller.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].params, ["1,2,3,4,5,6,7,8,9,10"]);
        assert_eq!(calls[1].params, ["11"]);
    }

    #[tokio::test]
    async fn canonical_raw_batch_is_single_attempt() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!([{"Match": 1}]),
        ]));
        OperationService::new(caller.clone())
            .get_match_details_batch_once(&[1.0], "match_ingestion")
            .await
            .expect("batch");
        assert_eq!(caller.calls()[0].max_retries, Some(0));
    }

    #[tokio::test]
    async fn empty_raw_batch_spends_no_call() {
        let caller = Arc::new(FakeCaller::default());
        let service = OperationService::new(caller.clone());
        assert!(
            service
                .get_match_details_batch_raw(&[], "buffer")
                .await
                .expect("empty")
                .is_empty()
        );
        assert!(caller.calls().is_empty());
    }

    #[tokio::test]
    async fn roster_is_single_attempt_and_non_array_is_empty() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!({"ret_msg": "private"}),
        ]));
        let service = OperationService::new(caller.clone());
        assert!(
            service
                .get_player_batch_from_match(100.0, "recovery")
                .await
                .expect("roster")
                .is_empty()
        );
        assert_eq!(caller.calls()[0].max_retries, Some(0));
    }

    #[tokio::test]
    async fn player_loadouts_include_required_language_segment() {
        let caller = Arc::new(FakeCaller::with_responses(vec![serde_json::json!([])]));
        let service = OperationService::new(caller.clone());
        service
            .get_player_loadouts(7.0, "profile")
            .await
            .expect("loadouts");
        assert_eq!(caller.calls()[0].params, ["7", "1"]);
    }

    #[tokio::test]
    async fn player_batch_chunks_at_twenty_and_lookup_is_single_attempt() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!([{"Id": 1}]),
            serde_json::json!([{"Id": 21}]),
        ]));
        let service = OperationService::new(caller.clone());
        let ids: Vec<f64> = (1..=21).map(f64::from).collect();
        assert_eq!(
            service
                .get_player_batch(&ids, true, "search")
                .await
                .expect("batch")
                .len(),
            2
        );
        let calls = caller.calls();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].max_retries, Some(0));
        assert_eq!(
            calls[0].params,
            ["1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20"]
        );
        assert_eq!(calls[1].params, ["21"]);
    }

    #[tokio::test]
    async fn exact_name_lookup_wraps_object_and_does_not_retry() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!({"player_id": 7}),
        ]));
        let service = OperationService::new(caller.clone());
        assert_eq!(
            service
                .get_player_id_by_name("NabiCook", "search")
                .await
                .expect("lookup"),
            [serde_json::json!({"player_id": 7})]
        );
        let call = &caller.calls()[0];
        assert_eq!(call.method, "getplayeridbyname");
        assert_eq!(call.max_retries, Some(0));
    }

    #[tokio::test]
    async fn raw_or_empty_preserves_truthy_object_like_typescript() {
        let caller = Arc::new(FakeCaller::with_responses(vec![
            serde_json::json!({"ret_msg": "maintenance"}),
        ]));
        let service = OperationService::new(caller);
        assert_eq!(
            service
                .get_player_champions(7.0, "profile")
                .await
                .expect("champions"),
            serde_json::json!({"ret_msg": "maintenance"})
        );
    }
}
