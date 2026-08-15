use std::time::{Duration, Instant};

use paladinscat_core::database::Database;
use serde_json::Value;
use tokio::time::sleep;
use uuid::Uuid;

use super::{
    casual_mechanics::CasualMechanicsRepository,
    match_facts::{CanonicalMatchPayload, MatchFactRepository},
    match_lifecycle::{
        MatchDiscovery, MatchDiscoverySource, MatchLifecycleAction, MatchLifecycleRepository,
        plan_match_lifecycle,
    },
    ranked_projection::RankedProjectionRepository,
    relay::{MatchLifecycleRelay, WorkerRelayClient, execute_match_lifecycle_fetches},
};

const COMPLETION_POLL_INTERVAL: Duration = Duration::from_millis(200);
const FACTS_DURABLE_SQL: &str = r#"
SELECT
  (
    EXISTS(SELECT 1 FROM matches WHERE match_id=$1)
    OR EXISTS(SELECT 1 FROM casual_matches WHERE match_id=$1)
    OR EXISTS(SELECT 1 FROM special_matches WHERE match_id=$1)
  ) AS match_exists,
  COALESCE(
    (SELECT completed_stages @> ARRAY['player_facts','match_bans']::text[]
     FROM match_ingest_status WHERE match_id=$1),
    FALSE
  ) AS facts_durable
"#;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
    casual: CasualMechanicsRepository,
    ranked: RankedProjectionRepository,
    relay: WorkerRelayClient,
    completion_timeout: Duration,
}

impl RequestedMatchIngestor {
    pub fn new(database: Database, relay: WorkerRelayClient, completion_timeout: Duration) -> Self {
        Self {
            lifecycle: MatchLifecycleRepository::new(database.clone()),
            facts: MatchFactRepository::new(database.clone()),
            casual: CasualMechanicsRepository::new(database.clone()),
            ranked: RankedProjectionRepository::new(database.clone()),
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
            Ok(true) => {
                return match self.ensure_projected(match_id).await {
                    Ok(()) => ready(match_id),
                    Err(error) => failed(match_id, error),
                };
            }
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
            return match self.ensure_projected(match_id).await {
                Ok(()) => ready(match_id),
                Err(error) => failed(match_id, error),
            };
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
            return match self.ensure_projected(match_id).await {
                Ok(()) => ready(match_id),
                Err(error) => failed(match_id, error),
            };
        }

        let actions = plan_match_lifecycle(&evidence);
        // A half-constructed match that already owns durable roster anchors
        // resumes in the middle. Only missing histories are requested here;
        // they are DB-first inside HirezRelay. resumeMatchRecovery then builds
        // the shell from local history plus one demo call, without replaying
        // match detail or getplayerbatchfrommatch.
        let relay_value = match if can_resume_without_detail_or_roster(&actions) {
            let history_actions = actions
                .iter()
                .filter(|action| matches!(action, MatchLifecycleAction::FetchHistories(_)))
                .cloned()
                .collect::<Vec<_>>();
            if let Err(error) = execute_match_lifecycle_fetches(
                &self.relay,
                match_id,
                evidence.queue_id,
                &history_actions,
            )
            .await
            {
                Err(error)
            } else {
                self.relay
                    .resume_recovery(match_id, evidence.queue_id)
                    .await
            }
        } else {
            // First observation and pre-roster states remain owned by the
            // canonical relay resolver. The backend never signs a vendor call.
            self.relay.fetch_detail(match_id, evidence.queue_id).await
        } {
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
        let finalized = match self.facts.finalize(&payload, "direct_lookup").await {
            Ok(finalized) => finalized,
            Err(error) => {
                let _ = self.lifecycle.release(match_id, &owner).await;
                return failed(match_id, error);
            }
        };
        let projection = match finalized.population {
            super::match_lifecycle::MatchPopulation::Ranked => self
                .ranked
                .project_match(match_id)
                .await
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            super::match_lifecycle::MatchPopulation::Casual
            | super::match_lifecycle::MatchPopulation::Special => self
                .casual
                .project_all_for_match(match_id)
                .await
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            super::match_lifecycle::MatchPopulation::Unknown => {
                Err("match population remained unknown after canonical finalization".to_owned())
            }
        };
        if let Err(error) = projection {
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

    async fn ensure_projected(&self, match_id: i64) -> Result<(), String> {
        let row = self
            .database
            .one_json(
                "SELECT population,completed_stages FROM match_ingest_status WHERE match_id=$1",
                &[&match_id],
            )
            .await
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "match ingest status is missing".to_owned())?;
        let population = row
            .get("population")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let stages = row
            .get("completed_stages")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let has_stage = |stage: &str| {
            stages
                .iter()
                .any(|value| value.as_str().is_some_and(|value| value == stage))
        };
        match population {
            "ranked" if !has_stage("ranked_stats") => self
                .ranked
                .project_match(match_id)
                .await
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            "casual" if !has_stage("casual_mechanics_stats") => self
                .casual
                .project_all_for_match(match_id)
                .await
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            "special" if !has_stage("special_mechanics_stats") => self
                .casual
                .project_all_for_match(match_id)
                .await
                .map(|_| ())
                .map_err(|error| format!("{error:?}")),
            "ranked" | "casual" | "special" => Ok(()),
            _ => Err("match population is unknown".to_owned()),
        }
    }

    async fn wait_for_existing_owner(&self, match_id: i64) -> RequestedMatchResult {
        let deadline = Instant::now() + self.completion_timeout;
        while Instant::now() < deadline {
            match self.facts_are_durable(match_id).await {
                Ok(true) => {
                    return match self.ensure_projected(match_id).await {
                        Ok(()) => ready(match_id),
                        Err(error) => failed(match_id, error),
                    };
                }
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
            .one_json(FACTS_DURABLE_SQL, &[&match_id])
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

fn can_resume_without_detail_or_roster(actions: &[MatchLifecycleAction]) -> bool {
    !actions.is_empty()
        && !actions.iter().any(|action| {
            matches!(
                action,
                MatchLifecycleAction::FetchDetail | MatchLifecycleAction::FetchRoster
            )
        })
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
    use axum::{Json, Router, extract::State, routing::post};
    use paladinscat_core::config::BackendConfig;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

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

    #[test]
    fn durable_requested_matches_accept_every_population_table() {
        assert!(FACTS_DURABLE_SQL.contains("FROM matches"));
        assert!(FACTS_DURABLE_SQL.contains("FROM casual_matches"));
        assert!(FACTS_DURABLE_SQL.contains("FROM special_matches"));
    }

    #[test]
    fn partial_roster_recovery_uses_resume_path() {
        assert!(can_resume_without_detail_or_roster(&[
            MatchLifecycleAction::FetchHistories(vec![9, 10]),
            MatchLifecycleAction::FetchDemo,
        ]));
        assert!(!can_resume_without_detail_or_roster(&[
            MatchLifecycleAction::FetchRoster,
        ]));
        assert!(!can_resume_without_detail_or_roster(&[
            MatchLifecycleAction::FetchDetail,
        ]));
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
        let client = database.connection().await.expect("connection");
        let schema_exists = client
            .query_one("SELECT to_regclass('public.queue_types') IS NOT NULL", &[])
            .await
            .expect("schema probe")
            .get::<_, bool>(0);
        if !schema_exists {
            client
                .batch_execute(include_str!(
                    "../../../../legacy/compat-backend-rust/package-c-match-facts-seed.sql"
                ))
                .await
                .expect("seed schema");
        }
        drop(client);
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

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL and a disposable empty database"]
    async fn live_partial_recovery_skips_detail_and_roster_operations() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let operations = Arc::new(Mutex::new(Vec::<String>::new()));
        let players = (1..=10)
            .map(|id| {
                json!({
                    "player_id":3000+id,
                    "player_name":format!("Resumed {id}"),
                    "champion_id":4000+id,
                    "champion_name":format!("Champion {id}"),
                    "task_force":if id<=5 {1} else {2},
                    "win_status":if id<=5 {"Winner"} else {"Loser"},
                    "source":"recovered",
                    "damage_done_physical":20_000+id,
                    "objective_assists":id
                })
            })
            .collect::<Vec<_>>();
        let relay_players = players.clone();
        let relay = Router::new()
            .route(
                "/v1/call",
                post(
                    |State(operations): State<Arc<Mutex<Vec<String>>>>,
                     Json(request): Json<Value>| async move {
                        let operation =
                            request["operation"].as_str().unwrap_or_default().to_owned();
                        operations
                            .lock()
                            .expect("operations")
                            .push(operation.clone());
                        let result = if operation == "getMatchHistory" {
                            json!([])
                        } else if operation == "resumeMatchRecovery" {
                            json!([{
                                "match_id":126,
                                "queue_id":486,
                                "status":"complete_recovered",
                                "match":{
                                    "match_id":126,
                                    "entry_datetime":"2026-07-30T15:00:00Z",
                                    "map":"Resumed Stone Keep",
                                    "queue_id":486,
                                    "duration_seconds":600,
                                    "region":"NA",
                                    "team1_score":4,
                                    "team2_score":1,
                                    "winning_task_force":1,
                                    "has_replay":false,
                                    "recovery_source":"local_resume",
                                    "recovery_api_calls":1,
                                    "players":relay_players
                                }
                            }])
                        } else {
                            json!([])
                        };
                        Json(json!({"ok":true,"result":result,"error":null}))
                    },
                ),
            )
            .with_state(operations.clone());
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
        let database = Database::new(&config, "requested-match-resume-test").expect("database");
        let client = database.connection().await.expect("connection");
        let schema_exists = client
            .query_one("SELECT to_regclass('public.queue_types') IS NOT NULL", &[])
            .await
            .expect("schema probe")
            .get::<_, bool>(0);
        if !schema_exists {
            client
                .batch_execute(include_str!(
                    "../../../../legacy/compat-backend-rust/package-c-match-facts-seed.sql"
                ))
                .await
                .expect("seed schema");
        }
        client
            .execute(
                r#"
                INSERT INTO match_ingest_status (
                  match_id,status,source,attempts,queue_id,population,
                  acquisition_state,detail_attempted_at,roster_resolved_at,
                  direct_player_count,roster_player_count
                )
                VALUES (
                  126,'partial','nonranked_hourly',1,486,'ranked',
                  'recovery_pending',now(),now(),2,10
                )
                "#,
                &[],
            )
            .await
            .expect("partial lifecycle");
        for slot in 1_i16..=10 {
            client
                .execute(
                    r#"
                    INSERT INTO match_ingest_participants (
                      match_id,roster_slot,player_id,participant_kind,source
                    ) VALUES (126,$1,$2,'human','roster')
                    "#,
                    &[&slot, &(3_000_i64 + i64::from(slot))],
                )
                .await
                .expect("participant");
            if slot <= 8 {
                client
                    .execute(
                        "INSERT INTO player_match_history_entries (match_id,player_id) VALUES (126,$1)",
                        &[&(3_000_i64 + i64::from(slot))],
                    )
                    .await
                    .expect("history");
            }
        }

        let ingestor = RequestedMatchIngestor::new(
            database.clone(),
            WorkerRelayClient::new(&config).expect("relay"),
            Duration::from_secs(5),
        );
        let result = ingestor.ingest(126).await;
        relay_task.abort();
        assert_eq!(result.status, RequestedMatchStatus::Ready, "{result:?}");
        assert_eq!(
            operations.lock().expect("operations").as_slice(),
            ["getMatchHistory", "getMatchHistory", "resumeMatchRecovery"]
        );
    }
}
