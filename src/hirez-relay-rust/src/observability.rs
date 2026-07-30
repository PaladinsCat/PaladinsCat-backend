use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use serde::Serialize;
use serde_json::{Map, Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{debug, warn};

use crate::dispatch_result::RelayDispatchResult;

#[derive(Default)]
struct OperationMetric {
    count: u64,
    errors: u64,
    total_latency_ms: u64,
    last_called_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OperationMetricSnapshot {
    operation: String,
    count: u64,
    errors: u64,
    avg_latency_ms: u64,
    last_called_at: String,
}

pub struct RelayObservability {
    requests: AtomicU64,
    successes: AtomicU64,
    failures: AtomicU64,
    active: AtomicU64,
    total_latency_ms: AtomicU64,
    operations: Mutex<BTreeMap<String, OperationMetric>>,
    traces: Mutex<VecDeque<Value>>,
    trace_limit: usize,
    sample_limit: usize,
}

impl RelayObservability {
    pub fn from_environment() -> Self {
        Self {
            requests: AtomicU64::new(0),
            successes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            active: AtomicU64::new(0),
            total_latency_ms: AtomicU64::new(0),
            operations: Mutex::new(BTreeMap::new()),
            traces: Mutex::new(VecDeque::new()),
            trace_limit: env_usize("HIREZ_RELAY_TRACE_LIMIT", 250),
            sample_limit: env_usize("HIREZ_RELAY_TRACE_ARRAY_SAMPLE_LIMIT", 3),
        }
    }

    pub fn begin(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Release);
    }

    pub fn finish(
        &self,
        request_id: &str,
        operation: &str,
        mode: &str,
        args: &[Value],
        result: Result<&Option<RelayDispatchResult>, &str>,
        latency_ms: u64,
    ) {
        self.active.fetch_sub(1, Ordering::Release);
        self.total_latency_ms
            .fetch_add(latency_ms, Ordering::Relaxed);
        let failed = result.is_err();
        if failed {
            self.failures.fetch_add(1, Ordering::Relaxed);
        } else {
            self.successes.fetch_add(1, Ordering::Relaxed);
        }

        let timestamp = timestamp();
        {
            let mut operations = self.operations.lock().expect("operation metrics");
            let metric = operations.entry(operation.to_owned()).or_default();
            metric.count += 1;
            metric.errors += u64::from(failed);
            metric.total_latency_ms = metric.total_latency_ms.saturating_add(latency_ms);
            metric.last_called_at.clone_from(&timestamp);
        }

        let mut trace = Map::new();
        trace.insert("requestId".to_owned(), Value::String(request_id.to_owned()));
        trace.insert("operation".to_owned(), Value::String(operation.to_owned()));
        trace.insert("mode".to_owned(), Value::String(mode.to_owned()));
        trace.insert("timestamp".to_owned(), Value::String(timestamp));
        trace.insert(
            "payload".to_owned(),
            summarize(&Value::Array(args.to_vec()), 0, self.sample_limit),
        );
        trace.insert("latencyMs".to_owned(), Value::from(latency_ms));
        match result {
            Ok(response) => {
                trace.insert(
                    "response".to_owned(),
                    response
                        .as_ref()
                        .map(|response| response.trace_summary(self.sample_limit))
                        .unwrap_or(Value::Null),
                );
                debug!(operation, request_id, latency_ms, "relay request completed");
            }
            Err(error) => {
                trace.insert("error".to_owned(), Value::String(error.to_owned()));
                warn!(
                    operation,
                    request_id, latency_ms, error, "relay request failed"
                );
            }
        }
        let mut traces = self.traces.lock().expect("relay traces");
        traces.push_back(Value::Object(trace));
        while traces.len() > self.trace_limit {
            traces.pop_front();
        }
    }

    pub fn active(&self) -> u64 {
        self.active.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> Value {
        let requests = self.requests.load(Ordering::Relaxed);
        let metrics: Vec<_> = self
            .operations
            .lock()
            .expect("operation metrics")
            .iter()
            .map(|(operation, metric)| OperationMetricSnapshot {
                operation: operation.clone(),
                count: metric.count,
                errors: metric.errors,
                avg_latency_ms: if metric.count > 0 {
                    ((metric.total_latency_ms as f64) / (metric.count as f64)).round() as u64
                } else {
                    0
                },
                last_called_at: metric.last_called_at.clone(),
            })
            .collect();
        let traces: Vec<_> = self
            .traces
            .lock()
            .expect("relay traces")
            .iter()
            .rev()
            .take(50)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        json!({
            "requests": requests,
            "successes": self.successes.load(Ordering::Relaxed),
            "failures": self.failures.load(Ordering::Relaxed),
            "active": self.active(),
            "averageLatencyMs": if requests > 0 {
                self.total_latency_ms.load(Ordering::Relaxed) as f64 / requests as f64
            } else {
                0.0
            },
            "metrics": metrics,
            "traces": traces,
        })
    }
}

fn summarize(value: &Value, depth: usize, sample_limit: usize) -> Value {
    match value {
        Value::String(value) if value.chars().count() > 500 => Value::String(format!(
            "{}...<truncated>",
            value.chars().take(500).collect::<String>()
        )),
        Value::Array(values) => {
            let sample: Vec<_> = values
                .iter()
                .take(sample_limit)
                .map(|value| summarize(value, depth + 1, sample_limit))
                .collect();
            json!({"type":"array", "count":values.len(), "sample":sample})
        }
        Value::Object(values) if depth >= 2 => {
            json!({"type":"object", "keys":values.keys().collect::<Vec<_>>()})
        }
        Value::Object(values) => {
            let mut summary = Map::new();
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

fn timestamp() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn env_usize(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(fallback)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_payloads_are_bounded_and_operation_metrics_are_exact() {
        let observability = RelayObservability {
            trace_limit: 1,
            sample_limit: 1,
            ..RelayObservability::from_environment()
        };
        observability.begin();
        observability.finish(
            "one",
            "getMatchHistory",
            "dummy",
            &[json!([1, 2, 3])],
            Ok(&Some(RelayDispatchResult::Json(
                json!({"players":[1, 2, 3]}),
            ))),
            10,
        );
        observability.begin();
        observability.finish("two", "getMatchHistory", "dummy", &[], Err("failure"), 20);
        let snapshot = observability.snapshot();
        assert_eq!(snapshot["requests"], json!(2));
        assert_eq!(snapshot["failures"], json!(1));
        assert_eq!(snapshot["metrics"][0]["count"], json!(2));
        assert_eq!(snapshot["metrics"][0]["avgLatencyMs"], json!(15));
        assert_eq!(snapshot["traces"].as_array().unwrap().len(), 1);
        assert_eq!(snapshot["traces"][0]["requestId"], json!("two"));
    }
}
