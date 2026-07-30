use std::sync::Arc;

use serde_json::Value;

use crate::{
    contract::validate_operation,
    dispatch_result::RelayDispatchResult,
    dummy::{DummyProvider, dispatch_dummy_operation},
    history_operation::{MatchHistoryService, PostgresHistoryCache},
    provider::RelayError,
    raw_buffer_store::RawBufferStore,
    real_dispatch::raw_payloads_arg,
};

pub struct DummyRuntime {
    provider: Arc<DummyProvider>,
    history: Option<MatchHistoryService<DummyProvider, PostgresHistoryCache>>,
    raw_buffer: Option<Arc<RawBufferStore>>,
}

impl DummyRuntime {
    pub fn new(
        provider: Arc<DummyProvider>,
        history_cache: Option<Arc<PostgresHistoryCache>>,
        raw_buffer: Option<Arc<RawBufferStore>>,
        public_history_ttl_minutes: u32,
    ) -> Self {
        let history = history_cache.map(|cache| {
            MatchHistoryService::new(provider.clone(), cache, public_history_ttl_minutes)
        });
        Self {
            provider,
            history,
            raw_buffer,
        }
    }

    pub fn provider(&self) -> &DummyProvider {
        self.provider.as_ref()
    }

    pub async fn dispatch(
        &self,
        operation: &str,
        args: &[Value],
        consumer: &str,
    ) -> Result<RelayDispatchResult, RelayError> {
        validate_operation(operation, args, "dummy")?;
        match operation {
            "getMatchDetailsBatch" => {
                let requests =
                    crate::contract::parse_completed_match_requests(args.first().ok_or_else(
                        || RelayError::Validation("requests are required".to_owned()),
                    )?)?;
                Ok(RelayDispatchResult::CompletedMatches(
                    crate::resolver::get_match_details_batch(self.provider.as_ref(), &requests)
                        .await?,
                ))
            }
            "resumeMatchRecovery" => {
                let requests =
                    crate::contract::parse_completed_match_requests(args.first().ok_or_else(
                        || RelayError::Validation("requests are required".to_owned()),
                    )?)?;
                let request = requests.first().ok_or_else(|| {
                    RelayError::Validation("exactly one recovery request is required".to_owned())
                })?;
                Ok(RelayDispatchResult::CompletedMatches(vec![
                    crate::resolver::resume_match_recovery(self.provider.as_ref(), request).await?,
                ]))
            }
            "getMatchHistory" if self.history.is_some() => {
                let history = self.history.as_ref().expect("checked history service");
                let player_id = number_arg(args, 0)?;
                let limit = optional_number_arg(args, 1).unwrap_or(50.0);
                let force_refresh = args.get(2).and_then(Value::as_bool).unwrap_or(false);
                serde_json::to_value(
                    history
                        .get_match_history(player_id, limit, force_refresh, consumer)
                        .await?,
                )
                .map(RelayDispatchResult::Json)
                .map_err(|error| RelayError::Upstream(error.to_string()))
            }
            "dumpRawPayloads" if self.raw_buffer.is_some() => {
                let payloads = raw_payloads_arg(args, 0)?;
                serde_json::to_value(
                    self.raw_buffer
                        .as_ref()
                        .expect("checked raw buffer")
                        .dump_raw_payloads(&payloads)
                        .await
                        .map_err(|error| RelayError::Upstream(error.to_string()))?,
                )
                .map(RelayDispatchResult::Json)
                .map_err(|error| RelayError::Upstream(error.to_string()))
            }
            _ => dispatch_dummy_operation(self.provider.as_ref(), operation, args)
                .await
                .map(RelayDispatchResult::Json),
        }
    }
}

fn number_arg(args: &[Value], index: usize) -> Result<f64, RelayError> {
    args.get(index)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .ok_or_else(|| RelayError::Validation(format!("argument {index} must be a finite number")))
}

fn optional_number_arg(args: &[Value], index: usize) -> Option<f64> {
    args.get(index)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
}
