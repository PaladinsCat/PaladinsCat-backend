use serde::Serialize;
use serde_json::{Value, json};

use crate::model::CompletedMatchResolution;

/// Keeps the canonical completed-match response typed until Axum serializes the
/// final wire envelope. Converting the full response to `serde_json::Value`
/// creates a second tree containing every match and player field, which is a
/// measurable CPU and allocation penalty under the VPS CPU cap.
#[derive(Serialize)]
#[serde(untagged)]
pub enum RelayDispatchResult {
    Json(Value),
    CompletedMatches(Vec<CompletedMatchResolution>),
}

impl RelayDispatchResult {
    pub fn trace_summary(&self, sample_limit: usize) -> Value {
        match self {
            Self::Json(value) => summarize(value, 0, sample_limit),
            Self::CompletedMatches(matches) => {
                let sample = matches
                    .iter()
                    .take(sample_limit)
                    .filter_map(|value| serde_json::to_value(value).ok())
                    .map(|value| summarize(&value, 1, sample_limit))
                    .collect::<Vec<_>>();
                json!({
                    "type": "array",
                    "count": matches.len(),
                    "sample": sample
                })
            }
        }
    }
}

fn summarize(value: &Value, depth: usize, sample_limit: usize) -> Value {
    match value {
        Value::String(value) if value.chars().count() > 500 => Value::String(format!(
            "{}...<truncated>",
            value.chars().take(500).collect::<String>()
        )),
        Value::Array(values) => {
            let sample = values
                .iter()
                .take(sample_limit)
                .map(|value| summarize(value, depth + 1, sample_limit))
                .collect::<Vec<_>>();
            json!({"type":"array", "count":values.len(), "sample":sample})
        }
        Value::Object(values) if depth >= 2 => {
            json!({"type":"object", "keys":values.keys().collect::<Vec<_>>()})
        }
        Value::Object(values) => {
            let mut summary = serde_json::Map::new();
            summary.insert("type".to_owned(), Value::String("object".to_owned()));
            summary.insert(
                "keys".to_owned(),
                Value::Array(values.keys().cloned().map(Value::String).collect()),
            );
            for key in [
                "operation",
                "requestId",
                "endpoint",
                "entity_type",
                "entity_id",
                "match_id",
                "player_id",
                "queue_id",
                "date",
                "hour",
                "error",
                "errorCode",
                "ok",
                "mode",
                "latencyMs",
            ] {
                if let Some(value) = values.get(key) {
                    summary.insert(key.to_owned(), summarize(value, depth + 1, sample_limit));
                }
            }
            for key in [
                "args",
                "raw_data",
                "players",
                "recovered",
                "payloads",
                "result",
            ] {
                if let Some(value) = values.get(key) {
                    summary.insert(key.to_owned(), summarize(value, depth + 1, sample_limit));
                }
            }
            Value::Object(summary)
        }
        value => value.clone(),
    }
}
