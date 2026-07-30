use std::time::{Duration, Instant};

use paladinscat_core::database::Database;
use serde_json::Value;
use tokio::time::sleep;
use uuid::Uuid;

use super::{
    match_facts::{CanonicalMatchPayload, MatchFactRepository},
    match_lifecycle::{MatchDiscovery, MatchDiscoverySource, MatchLifecycleRepository},
    relay::{MatchLifecycleRelay, WorkerRelayClient},
};

const COMPLETION_POLL_INTERVAL: Duration = Duration::from_millis(200);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RequestedMatchStatus {
    Ready,
    NotFound,
    RecoveryFailed,
    ProcessingTimeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestedMatchResult {
    pub match_id: i64,
    pub status: RequestedMatchStatus,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct RequestedMatchIngestor {
    database: Database,
    lifecycle: MatchLifecycleRepository,
    facts: MatchFactRepository,
    relay: WorkerRelayClient,
    completion_timeout: Duration,
}

impl RequestedMatchIngestor {
    pub fn new(database: Database, relay: WorkerRelayClient, completion_timeout: Duration) -> Self {
        Self {
            lifecycle: MatchLifecycleRepository::new(database.clone()),
            facts: MatchFactRepository::new(database.clone()),
            database,
            relay,
            completion_timeout,
        }
    }

    pub async fn ingest(&self, match_id: i64) -> RequestedMatchResult {
        if match_id <= 0 {
            return failed(match_id, "match ID must be positive");
        }
        match self.facts_are_durable(match_id).await {
            Ok(true) => return ready(match_id),
            Ok(false) => {}
            Err(error) => return failed(match_id, error),
        }

        let registration = match self
            .lifecycle
            .register_discovery(&MatchDiscovery {
                match_id,
                queue_id: None,
                source: MatchDiscoverySource::DirectLookup,
            })
            .await
        {
            Ok(registration) => registration,
            Err(error) => return failed(match_id, error),
        };
        if !registration.needs_work {
            return ready(match_id);
        }

        let owner = format!("requested-match-{}", Uuid::new_v4());
        let evidence = match self
            .lifecycle
            .claim(match_id, "direct_lookup", &owner, self.completion_timeout)
            .await
        {
            Ok(Some(evidence)) => evidence,
            Ok(None) => return self.wait_for_existing_owner(match_id).await,
            Err(error) => return failed(match_id, error),
        };
        if evidence.facts_durable() {
            let _ = self.lifecycle.release(match_id, &owner).await;
            return ready(match_id);
        }

        // The Rust HirezRelay owns direct lookup and the complete ranked or
        // non-ranked recovery decision. This backend never signs or sends a
        // vendor call itself, and only the normalized terminal match crosses
        // into the canonical fact transaction.
        let relay_value = match self.relay.fetch_detail(match_id, evidence.queue_id).await {
            Ok(value) => value,
            Err(error) => {
                let _ = self.lifecycle.release(match_id, &owner).await;
                return failed(match_id, error);
            }
        };
        let terminal = terminal_status(&relay_value);
        if terminal.as_deref() == Some("dropped") {
            let _ = self.lifecycle.release(match_id, &owner).await;
            return RequestedMatchResult {
                match_id,
                status: RequestedMatchStatus::NotFound,
                error: None,
            };
        }
        if terminal.as_deref() == Some("recovery_pending") {
            let _ = self.lifecycle.release(match_id, &owner).await;
            return failed(
                match_id,
                "HirezRelay recovery remains pending for the requested match",
            );
        }
        let payload = match CanonicalMatchPayload::from_relay_value(relay_value) {
            Ok(payload) => payload,
            Err(error) => {
                let _ = self.lifecycle.release(match_id, &owner).await;
                return if terminal.as_deref() == Some("dropped") {
                    RequestedMatchResult {
                        match_id,
                        status: RequestedMatchStatus::NotFound,
                        error: None,
                    }
                } else {
                    failed(match_id, error)
                };
            }
        };
        if payload.match_id != match_id {
            let _ = self.lifecycle.release(match_id, &owner).await;
            return failed(
                match_id,
                format!(
                    "HirezRelay returned match {} for requested match {match_id}",
                    payload.match_id
                ),
            );
        }
        if let Err(error) = self.facts.finalize(&payload, "direct_lookup").await {
            let _ = self.lifecycle.release(match_id, &owner).await;
            return failed(match_id, error);
        }
        let _ = self.lifecycle.release(match_id, &owner).await;
        match self.facts_are_durable(match_id).await {
            Ok(true) => ready(match_id),
            Ok(false) => failed(
                match_id,
                "canonical finalizer returned before the durable fact boundary",
            ),
            Err(error) => failed(match_id, error),
        }
    }

    async fn wait_for_existing_owner(&self, match_id: i64) -> RequestedMatchResult {
        let deadline = Instant::now() + self.completion_timeout;
        while Instant::now() < deadline {
            match self.facts_are_durable(match_id).await {
                Ok(true) => return ready(match_id),
                Ok(false) => sleep(COMPLETION_POLL_INTERVAL).await,
                Err(error) => return failed(match_id, error),
            }
        }
        RequestedMatchResult {
            match_id,
            status: RequestedMatchStatus::ProcessingTimeout,
            error: None,
        }
    }

    async fn facts_are_durable(&self, match_id: i64) -> Result<bool, String> {
        self.database
            .one_json(
                r#"
                SELECT
                  EXISTS(SELECT 1 FROM matches WHERE match_id=$1) AS match_exists,
                  COALESCE(
                    (SELECT completed_stages @> ARRAY['player_facts','match_bans']::text[]
                     FROM match_ingest_status WHERE match_id=$1),
                    FALSE
                  ) AS facts_durable
                "#,
                &[&match_id],
            )
            .await
            .map(|row| {
                row.is_some_and(|row| {
                    row.get("match_exists").and_then(Value::as_bool) == Some(true)
                        && row.get("facts_durable").and_then(Value::as_bool) == Some(true)
                })
            })
            .map_err(|error| error.to_string())
    }
}

fn terminal_status(value: &Value) -> Option<String> {
    match value {
        Value::Array(values) => values.iter().find_map(terminal_status),
        Value::Object(object) => object
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        _ => None,
    }
}

fn ready(match_id: i64) -> RequestedMatchResult {
    RequestedMatchResult {
        match_id,
        status: RequestedMatchStatus::Ready,
        error: None,
    }
}

fn failed(match_id: i64, error: impl ToString) -> RequestedMatchResult {
    RequestedMatchResult {
        match_id,
        status: RequestedMatchStatus::RecoveryFailed,
        error: Some(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workers::match_lifecycle::MatchPopulation;
    use axum::{Json, Router, routing::post};
    use paladinscat_core::config::BackendConfig;
    use serde_json::json;

    #[test]
    fn terminal_status_accepts_completed_match_resolution_arrays() {
        assert_eq!(
            terminal_status(&json!([{"status":"complete_recovered","match":{}}])),
            Some("complete_recovered".to_owned())
        );
        assert_eq!(
            terminal_status(&json!([{"status":"dropped","reason":"missing"}])),
            Some("dropped".to_owned())
        );
    }

    #[test]
    fn statuses_do_not_claim_projection_completion() {
        let result = ready(42);
        assert_eq!(result.status, RequestedMatchStatus::Ready);
        assert_ne!(
            MatchPopulation::Ranked.as_database(),
            MatchPopulation::Casual.as_database()
        );
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL and a disposable empty database"]
    async fn live_requested_lookup_crosses_relay_and_returns_only_after_facts() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let players = (1..=10)
            .map(|id| {
                json!({
                    "player_id":1000+id,
                    "player_name":format!("Requested {id}"),
                    "champion_id":2000+id,
                    "champion_name":format!("Champion {id}"),
                    "task_force":if id<=5 {1} else {2},
                    "win_status":if id<=5 {"Winner"} else {"Loser"},
                    "source":"direct",
                    "damage_done_physical":10_000+id,
                    "objective_assists":id
                })
            })
            .collect::<Vec<_>>();
        let relay_result = json!([{
            "match_id":125,
            "entry_datetime":"2026-07-30T14:00:00Z",
            "map":"Requested Stone Keep",
            "queue_id":486,
            "duration_seconds":600,
            "region":"NA",
            "team1_score":4,
            "team2_score":1,
            "winning_task_force":1,
            "has_replay":false,
            "players":players
        }]);
        let relay = Router::new().route(
            "/v1/call",
            post(move || {
                let relay_result = relay_result.clone();
                async move {
                    Json(json!({
                        "ok":true,
                        "result":relay_result,
                        "error":null
                    }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("relay listener");
        let relay_address = listener.local_addr().expect("relay address");
        let relay_task = tokio::spawn(async move {
            axum::serve(listener, relay).await.expect("relay server");
        });
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "HIREZ_RELAY_URL" => Some(format!("http://{relay_address}")),
            _ => None,
        })
        .expect("backend config");
        let database = Database::new(&config, "requested-match-test").expect("database");
        database
            .connection()
            .await
            .expect("connection")
            .batch_execute(include_str!(
                "../../../../dev/compat/backend-rust/package-c-match-facts-seed.sql"
            ))
            .await
            .expect("seed schema");
        let ingestor = RequestedMatchIngestor::new(
            database.clone(),
            WorkerRelayClient::new(&config).expect("relay"),
            Duration::from_secs(5),
        );
        let result = ingestor.ingest(125).await;
        relay_task.abort();
        assert_eq!(result.status, RequestedMatchStatus::Ready, "{result:?}");

        let boundary = database
            .one_json(
                r#"
                SELECT
                  (SELECT count(*)::int FROM matches WHERE match_id=125) AS matches,
                  (SELECT count(*)::int FROM match_players WHERE match_id=125) AS players,
                  (SELECT completed_stages @> ARRAY['player_facts','match_bans']::text[]
                   FROM match_ingest_status WHERE match_id=125) AS facts_durable
                "#,
                &[],
            )
            .await
            .expect("boundary")
            .expect("boundary row");
        assert_eq!(
            boundary,
            json!({"matches":1,"players":10,"facts_durable":true})
        );

        // A later hourly pass joins the same durable row and spends no relay
        // call or duplicate fact write.
        let second = ingestor.ingest(125).await;
        assert_eq!(second.status, RequestedMatchStatus::Ready);
        assert_eq!(
            database
                .one_json(
                    "SELECT count(*)::int AS count FROM match_players WHERE match_id=125",
                    &[],
                )
                .await
                .expect("count")
                .expect("count row")["count"],
            10
        );
    }
}
