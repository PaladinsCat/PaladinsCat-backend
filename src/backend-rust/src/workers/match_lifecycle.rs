use std::collections::BTreeSet;
use std::time::Duration;

use paladinscat_core::{
    database::{Database, DatabaseError},
    queue::roster_evidence_is_complete,
};
use serde::Serialize;
use tokio_postgres::Row;

pub(crate) const TERMINAL_NO_COMPLETED_MATCH_REASON: &str = "provider returned no completed match";

const CLAIM_SQL: &str = r#"
INSERT INTO match_ingest_status (
  match_id, status, source, attempts, acquisition_state,
  lease_owner, lease_until, started_at, updated_at
)
VALUES (
  $1, 'processing', $2, 1, 'discovered',
  $3, now() + ($4::int * interval '1 second'), now(), now()
)
ON CONFLICT (match_id) DO UPDATE SET
  status = CASE
    WHEN match_ingest_status.status = 'failed' THEN 'processing'
    ELSE match_ingest_status.status
  END,
  source = COALESCE(match_ingest_status.source, EXCLUDED.source),
  attempts = match_ingest_status.attempts + 1,
  lease_owner = EXCLUDED.lease_owner,
  lease_until = EXCLUDED.lease_until,
  updated_at = now()
WHERE match_ingest_status.status NOT IN ('complete', 'limited')
  AND (
    match_ingest_status.lease_until IS NULL
    OR match_ingest_status.lease_until <= now()
    OR match_ingest_status.lease_owner = EXCLUDED.lease_owner
  )
RETURNING
  match_id, status, completed_stages, queue_id, population,
  acquisition_state, detail_attempted_at IS NOT NULL AS detail_attempted,
  demo_resolved_at IS NOT NULL AS demo_resolved,
  direct_player_count, roster_player_count, unresolved_player_ids
"#;

const PARTICIPANT_IDS_SQL: &str = r#"
SELECT player_id,source FROM (
  SELECT player_id,source,roster_slot FROM match_ingest_participants WHERE match_id=$1
  UNION
  SELECT player_id,COALESCE(source,'direct'),0 FROM match_players WHERE match_id=$1
  UNION
  SELECT player_id,COALESCE(source,'direct'),0 FROM casual_match_players WHERE match_id=$1
  UNION
  SELECT player_id,COALESCE(source,'direct'),0 FROM special_match_players WHERE match_id=$1
) participants
WHERE player_id>0
ORDER BY roster_slot,player_id
"#;

const HISTORY_IDS_SQL: &str = r#"
SELECT player_id
FROM player_match_history_entries
WHERE match_id = $1
  AND player_id > 0
ORDER BY player_id
"#;

const CHECKPOINT_SQL: &str = r#"
UPDATE match_ingest_status
SET queue_id = COALESCE($3, queue_id),
    population = CASE
      WHEN population = 'unknown' THEN $4
      ELSE population
    END,
    acquisition_state = $5,
    detail_attempted_at = CASE WHEN $6 THEN COALESCE(detail_attempted_at, now()) ELSE detail_attempted_at END,
    roster_resolved_at = CASE WHEN $7 THEN COALESCE(roster_resolved_at, now()) ELSE roster_resolved_at END,
    demo_resolved_at = CASE WHEN $8 THEN COALESCE(demo_resolved_at, now()) ELSE demo_resolved_at END,
    direct_player_count = $9,
    roster_player_count = $10,
    unresolved_player_ids = $11,
    updated_at = now()
WHERE match_id = $1
  AND lease_owner = $2
  AND lease_until > now()
"#;

const RELEASE_SQL: &str = r#"
UPDATE match_ingest_status
SET lease_owner = NULL,
    lease_until = NULL,
    updated_at = now()
WHERE match_id = $1
  AND lease_owner = $2
"#;

const COMPLETE_SQL: &str = r#"
UPDATE match_ingest_status
SET status = 'complete',
    acquisition_state = 'complete',
    lease_owner = NULL,
    lease_until = NULL,
    completed_at = COALESCE(completed_at, now()),
    error_message = NULL,
    updated_at = now()
WHERE match_id = $1
  AND lease_owner = $2
  AND lease_until > now()
  AND completed_stages @> ARRAY['player_facts', 'match_bans']::text[]
  AND (
    (population = 'ranked' AND completed_stages @> ARRAY['ranked_stats']::text[])
    OR (
      population = 'casual'
      AND completed_stages @> ARRAY['casual_mechanics_stats']::text[]
    )
    OR (
      population = 'special'
      AND completed_stages @> ARRAY['special_mechanics_stats']::text[]
    )
  )
"#;

const MARK_TERMINAL_UNAVAILABLE_SQL: &str = r#"
INSERT INTO match_ingest_status (
  match_id, status, source, attempts, error_message, queue_id, population,
  acquisition_state, completed_at, updated_at, lease_owner, lease_until
)
VALUES (
  $1, 'limited', $3, 1, $4, $2, 'unknown',
  'unavailable', now(), now(), NULL, NULL
)
ON CONFLICT (match_id) DO UPDATE SET
  status = 'limited',
  source = EXCLUDED.source,
  attempts = match_ingest_status.attempts + 1,
  error_message = EXCLUDED.error_message,
  queue_id = COALESCE(match_ingest_status.queue_id, EXCLUDED.queue_id),
  acquisition_state = 'unavailable',
  completed_at = COALESCE(match_ingest_status.completed_at, now()),
  updated_at = now(),
  lease_owner = NULL,
  lease_until = NULL
WHERE match_ingest_status.status <> 'complete'
"#;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchDiscoverySource {
    HourlyDiscovery,
    ProfileHistory,
    DirectLookup,
    Recovery,
}

impl MatchDiscoverySource {
    /// Purpose: provide the single canonical database/audit spelling shared by
    /// discovery, lifecycle claims, finalization, and projections.
    /// Input: the typed discovery source. Output: a static database string.
    pub(crate) fn as_database(self) -> &'static str {
        match self {
            Self::HourlyDiscovery => "hourly_discovery",
            Self::ProfileHistory => "profile_history",
            Self::DirectLookup => "direct_lookup",
            Self::Recovery => "recovery",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchPopulation {
    Ranked,
    Casual,
    Special,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchDiscovery {
    pub match_id: i64,
    pub queue_id: Option<i32>,
    pub source: MatchDiscoverySource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchDiscoveryRegistration {
    pub match_id: i64,
    pub population: MatchPopulation,
    pub needs_work: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct NonrankedAdoptionSummary {
    pub adopted: usize,
    pub durable_facts: usize,
    pub recovery_pending: usize,
    pub existing_core_projections: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum MatchDiscoveryError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error("PostgreSQL discovery transaction failed: {0}")]
    Query(#[from] tokio_postgres::Error),
    #[error(
        "match {match_id} was already classified as {existing}; refusing conflicting {incoming} discovery"
    )]
    PopulationConflict {
        match_id: i64,
        existing: &'static str,
        incoming: &'static str,
    },
    #[error(
        "match {match_id} was already assigned queue {existing}; refusing conflicting queue {incoming}"
    )]
    QueueConflict {
        match_id: i64,
        existing: i32,
        incoming: i32,
    },
}

impl MatchPopulation {
    fn from_database(value: &str) -> Self {
        match value {
            "ranked" => Self::Ranked,
            "casual" => Self::Casual,
            "special" => Self::Special,
            _ => Self::Unknown,
        }
    }

    pub fn as_database(self) -> &'static str {
        match self {
            Self::Ranked => "ranked",
            Self::Casual => "casual",
            Self::Special => "special",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchEvidence {
    pub match_id: i64,
    pub status: String,
    pub completed_stages: BTreeSet<String>,
    pub queue_id: Option<i32>,
    pub population: MatchPopulation,
    pub acquisition_state: String,
    pub detail_attempted: bool,
    pub roster_resolved: bool,
    pub demo_resolved: bool,
    pub direct_player_ids: BTreeSet<i64>,
    pub roster_player_ids: BTreeSet<i64>,
    pub history_player_ids: BTreeSet<i64>,
    pub unresolved_player_ids: BTreeSet<i64>,
}

impl MatchEvidence {
    pub fn facts_durable(&self) -> bool {
        self.completed_stages.contains("player_facts")
            && self.completed_stages.contains("match_bans")
    }

    pub fn projection_complete(&self) -> bool {
        match self.population {
            MatchPopulation::Ranked => self.completed_stages.contains("ranked_stats"),
            MatchPopulation::Casual => self.completed_stages.contains("casual_mechanics_stats"),
            MatchPopulation::Special => self.completed_stages.contains("special_mechanics_stats"),
            MatchPopulation::Unknown => false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatchLifecycleAction {
    FetchDetail,
    FetchRoster,
    FetchHistories(Vec<i64>),
    FetchDemo,
    FinalizeFacts,
    Project(MatchPopulation),
    Complete,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParticipantAnchorSource {
    Direct,
    Roster,
    History,
    Local,
}

impl ParticipantAnchorSource {
    fn as_database(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Roster => "roster",
            Self::History => "history",
            Self::Local => "local",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParticipantAnchor {
    pub roster_slot: i16,
    pub player_id: i64,
    pub participant_kind: String,
    pub source: ParticipantAnchorSource,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchCheckpoint {
    pub queue_id: Option<i32>,
    pub population: MatchPopulation,
    pub acquisition_state: String,
    pub detail_attempted: bool,
    pub roster_resolved: bool,
    pub demo_resolved: bool,
    pub direct_player_count: i16,
    pub roster_player_count: i16,
    pub unresolved_player_ids: Vec<i64>,
}

pub fn plan_match_lifecycle(evidence: &MatchEvidence) -> Vec<MatchLifecycleAction> {
    if evidence.facts_durable() {
        if evidence.projection_complete() {
            return vec![MatchLifecycleAction::Complete];
        }
        return match evidence.population {
            MatchPopulation::Unknown => vec![MatchLifecycleAction::FinalizeFacts],
            population => vec![MatchLifecycleAction::Project(population)],
        };
    }

    if !evidence.detail_attempted {
        return vec![MatchLifecycleAction::FetchDetail];
    }

    if !evidence.roster_resolved {
        return vec![MatchLifecycleAction::FetchRoster];
    }

    let known_player_ids: BTreeSet<_> = evidence
        .direct_player_ids
        .union(&evidence.history_player_ids)
        .copied()
        .collect();
    let mut missing_history_ids: Vec<_> = evidence
        .roster_player_ids
        .difference(&known_player_ids)
        .copied()
        .collect();
    missing_history_ids.extend(
        evidence
            .unresolved_player_ids
            .difference(&known_player_ids)
            .copied(),
    );
    missing_history_ids.sort_unstable();
    missing_history_ids.dedup();

    let mut actions = Vec::new();
    if !missing_history_ids.is_empty() {
        actions.push(MatchLifecycleAction::FetchHistories(missing_history_ids));
    }
    if !evidence.demo_resolved {
        actions.push(MatchLifecycleAction::FetchDemo);
    }
    if actions.is_empty() {
        actions.push(MatchLifecycleAction::FinalizeFacts);
    }
    actions
}

/// Purpose: derive roster completion from persisted cardinality rather than a
/// stale timestamp. Input: optional queue ID, recorded count, and locally
/// observed public participant count. Output: shared policy decision used by
/// `load_evidence_from_claim` before the lifecycle plan is constructed.
fn persisted_roster_is_complete(
    queue_id: Option<i32>,
    recorded_count: i16,
    observed_public_count: usize,
) -> bool {
    let recorded_count = usize::try_from(recorded_count.max(0)).unwrap_or_default();
    let player_count = recorded_count.max(observed_public_count);
    queue_id.is_some_and(|queue_id| roster_evidence_is_complete(queue_id, player_count))
}

#[derive(Clone)]
pub struct MatchLifecycleRepository {
    database: Database,
}

impl MatchLifecycleRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Registers any discovery source in the same durable ledger.
    ///
    /// Queue taxonomy comes from PostgreSQL rather than a second hard-coded
    /// worker list. A terminal row is returned with `needs_work=false`, which
    /// is how a later hourly pass skips a match already completed by a profile
    /// or direct lookup.
    pub async fn register_discovery(
        &self,
        discovery: &MatchDiscovery,
    ) -> Result<MatchDiscoveryRegistration, MatchDiscoveryError> {
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let incoming_population = match discovery.queue_id {
            Some(queue_id) => transaction
                .query_opt(
                    r#"
                    SELECT is_ranked, stats_scope
                    FROM queue_types
                    WHERE queue_id = $1
                    "#,
                    &[&queue_id],
                )
                .await?
                .map(|row| {
                    let is_ranked = row.get::<_, bool>("is_ranked");
                    let scope = row.get::<_, String>("stats_scope");
                    if is_ranked || scope == "ranked" {
                        MatchPopulation::Ranked
                    } else if scope == "casual" {
                        MatchPopulation::Casual
                    } else {
                        MatchPopulation::Special
                    }
                })
                .unwrap_or(MatchPopulation::Unknown),
            None => MatchPopulation::Unknown,
        };

        let existing = transaction
            .query_opt(
                r#"
                SELECT queue_id, population
                FROM match_ingest_status
                WHERE match_id = $1
                FOR UPDATE
                "#,
                &[&discovery.match_id],
            )
            .await?;
        if let Some(existing) = &existing {
            let existing_population =
                MatchPopulation::from_database(existing.get::<_, String>("population").as_str());
            if existing_population != MatchPopulation::Unknown
                && incoming_population != MatchPopulation::Unknown
                && existing_population != incoming_population
            {
                return Err(MatchDiscoveryError::PopulationConflict {
                    match_id: discovery.match_id,
                    existing: existing_population.as_database(),
                    incoming: incoming_population.as_database(),
                });
            }
            let existing_queue = existing.get::<_, Option<i32>>("queue_id");
            if let (Some(existing), Some(incoming)) = (existing_queue, discovery.queue_id)
                && existing != incoming
            {
                return Err(MatchDiscoveryError::QueueConflict {
                    match_id: discovery.match_id,
                    existing,
                    incoming,
                });
            }
        }

        let row = transaction
            .query_one(
                r#"
                INSERT INTO match_ingest_status (
                  match_id, status, source, queue_id, population,
                  acquisition_state, updated_at
                )
                VALUES ($1, 'processing', $2, $3, $4, 'discovered', now())
                ON CONFLICT (match_id) DO UPDATE SET
                  status = CASE
                    WHEN match_ingest_status.status = 'limited'
                      AND match_ingest_status.acquisition_state = 'unavailable'
                      AND match_ingest_status.error_message IS DISTINCT FROM $5
                    THEN 'processing'
                    ELSE match_ingest_status.status
                  END,
                  source = COALESCE(match_ingest_status.source, EXCLUDED.source),
                  queue_id = COALESCE(match_ingest_status.queue_id, EXCLUDED.queue_id),
                  population = CASE
                    WHEN match_ingest_status.population = 'unknown'
                      THEN EXCLUDED.population
                    ELSE match_ingest_status.population
                  END,
                  acquisition_state = CASE
                    WHEN match_ingest_status.status = 'limited'
                      AND match_ingest_status.acquisition_state = 'unavailable'
                      AND match_ingest_status.error_message IS DISTINCT FROM $5
                    THEN 'discovered'
                    ELSE match_ingest_status.acquisition_state
                  END,
                  error_message = CASE
                    WHEN match_ingest_status.status = 'limited'
                      AND match_ingest_status.acquisition_state = 'unavailable'
                      AND match_ingest_status.error_message IS DISTINCT FROM $5
                    THEN NULL
                    ELSE match_ingest_status.error_message
                  END,
                  completed_at = CASE
                    WHEN match_ingest_status.status = 'limited'
                      AND match_ingest_status.acquisition_state = 'unavailable'
                      AND match_ingest_status.error_message IS DISTINCT FROM $5
                    THEN NULL
                    ELSE match_ingest_status.completed_at
                  END,
                  updated_at = CASE
                    WHEN match_ingest_status.status = 'complete'
                      OR (
                        match_ingest_status.status = 'limited'
                        AND NOT (
                          match_ingest_status.acquisition_state = 'unavailable'
                          AND match_ingest_status.error_message IS DISTINCT FROM $5
                        )
                      )
                      THEN match_ingest_status.updated_at
                    ELSE now()
                  END
                RETURNING status, population
                "#,
                &[
                    &discovery.match_id,
                    &discovery.source.as_database(),
                    &discovery.queue_id,
                    &incoming_population.as_database(),
                    &TERMINAL_NO_COMPLETED_MATCH_REASON,
                ],
            )
            .await?;
        transaction.commit().await?;
        let status = row.get::<_, String>("status");
        Ok(MatchDiscoveryRegistration {
            match_id: discovery.match_id,
            population: MatchPopulation::from_database(row.get::<_, String>("population").as_str()),
            needs_work: !matches!(status.as_str(), "complete" | "limited"),
        })
    }

    /// Adopts the old non-ranked ledger in bounded, DB-only pages.
    ///
    /// This method deliberately has no relay dependency. Existing match/player
    /// rows become durable lifecycle evidence, partial rows become sparse
    /// participant anchors, and the legacy stats timestamp becomes a separate
    /// core-projection stage so map/champion counters are not incremented
    /// again when item/card/talent/composition projection is added.
    pub async fn adopt_nonranked_batch(
        &self,
        limit: usize,
    ) -> Result<NonrankedAdoptionSummary, DatabaseError> {
        if limit == 0 {
            return Ok(NonrankedAdoptionSummary::default());
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let rows = transaction
            .query(
                r#"
                SELECT
                  match_id, queue_id, stats_scope, status, quality,
                  direct_player_count, roster_player_count, detail_attempts,
                  stats_projected_at IS NOT NULL AS core_stats_projected
                FROM nonranked_match_acquisition
                WHERE canonical_adopted_at IS NULL
                ORDER BY match_id
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                "#,
                &[&limit],
            )
            .await?;
        let mut summary = NonrankedAdoptionSummary::default();

        for row in rows {
            let match_id = row.get::<_, i64>("match_id");
            let queue_id = row.get::<_, i32>("queue_id");
            let stats_scope = row.get::<_, String>("stats_scope");
            let population = if stats_scope == "casual" {
                MatchPopulation::Casual
            } else {
                MatchPopulation::Special
            };
            let (match_table, player_table) = match population {
                MatchPopulation::Casual => ("casual_matches", "casual_match_players"),
                MatchPopulation::Special => ("special_matches", "special_match_players"),
                MatchPopulation::Ranked | MatchPopulation::Unknown => unreachable!(),
            };
            let parent = transaction
                .query_opt(
                    &format!(
                        r#"
                        SELECT
                          parent.quality,
                          parent.stats_eligible,
                          COUNT(player.*)::smallint AS player_count,
                          COUNT(player.*) FILTER (
                            WHERE LOWER(COALESCE(player.source, '')) IN (
                              'direct', 'recovered'
                            )
                          )::smallint AS direct_count
                        FROM {match_table} parent
                        LEFT JOIN {player_table} player
                          ON player.match_id = parent.match_id
                        WHERE parent.match_id = $1
                        GROUP BY parent.match_id
                        "#
                    ),
                    &[&match_id],
                )
                .await?;
            let parent_quality = parent
                .as_ref()
                .map(|parent| parent.get::<_, String>("quality"));
            let parent_player_count = parent
                .as_ref()
                .map(|parent| parent.get::<_, i16>("player_count"))
                .unwrap_or_default();
            let parent_direct_count = parent
                .as_ref()
                .map(|parent| parent.get::<_, i16>("direct_count"))
                .unwrap_or_default();
            let ledger_direct_count = row.get::<_, i16>("direct_player_count");
            let ledger_roster_count = row.get::<_, i16>("roster_player_count");
            let direct_count = parent_direct_count.max(ledger_direct_count);
            let roster_count = parent_player_count.max(ledger_roster_count);
            let durable_facts = parent_quality
                .as_deref()
                .is_some_and(|quality| matches!(quality, "complete" | "limited"))
                && roster_count > 0;
            let core_stats_projected = row.get::<_, bool>("core_stats_projected");
            let detail_attempted = row.get::<_, i16>("detail_attempts") > 0 || parent.is_some();
            let roster_resolved = roster_count > 0;
            let mut completed_stages: Vec<&str> = Vec::new();
            if durable_facts {
                completed_stages.extend(["player_facts", "match_bans"]);
            }
            if core_stats_projected {
                completed_stages.push("nonranked_core_stats");
            }
            let acquisition_state = if durable_facts {
                "facts_ready"
            } else if roster_resolved {
                "recovery_pending"
            } else if detail_attempted {
                "detail_pending"
            } else {
                "discovered"
            };

            transaction
                .execute(
                    r#"
                    INSERT INTO match_ingest_status (
                      match_id, status, completed_stages, source, attempts,
                      queue_id, population, acquisition_state,
                      detail_attempted_at, roster_resolved_at,
                      direct_player_count, roster_player_count, updated_at
                    )
                    VALUES (
                      $1, 'processing', $2, 'legacy_nonranked_adoption', 0,
                      $3, $4, $5,
                      CASE WHEN $6 THEN now() ELSE NULL END,
                      CASE WHEN $7 THEN now() ELSE NULL END,
                      $8, $9, now()
                    )
                    ON CONFLICT (match_id) DO UPDATE SET
                      completed_stages = (
                        SELECT ARRAY(
                          SELECT DISTINCT stage_name
                          FROM unnest(
                            match_ingest_status.completed_stages
                            || EXCLUDED.completed_stages
                          ) stage_name
                        )
                      ),
                      queue_id = COALESCE(
                        match_ingest_status.queue_id,
                        EXCLUDED.queue_id
                      ),
                      population = CASE
                        WHEN match_ingest_status.population = 'unknown'
                          THEN EXCLUDED.population
                        ELSE match_ingest_status.population
                      END,
                      acquisition_state = CASE
                        WHEN match_ingest_status.acquisition_state = 'complete'
                          THEN match_ingest_status.acquisition_state
                        ELSE EXCLUDED.acquisition_state
                      END,
                      detail_attempted_at = CASE
                        WHEN $6 THEN COALESCE(
                          match_ingest_status.detail_attempted_at,
                          now()
                        )
                        ELSE match_ingest_status.detail_attempted_at
                      END,
                      roster_resolved_at = CASE
                        WHEN $7 THEN COALESCE(
                          match_ingest_status.roster_resolved_at,
                          now()
                        )
                        ELSE match_ingest_status.roster_resolved_at
                      END,
                      direct_player_count = GREATEST(
                        match_ingest_status.direct_player_count,
                        EXCLUDED.direct_player_count
                      ),
                      roster_player_count = GREATEST(
                        match_ingest_status.roster_player_count,
                        EXCLUDED.roster_player_count
                      ),
                      updated_at = now()
                    "#,
                    &[
                        &match_id,
                        &completed_stages,
                        &queue_id,
                        &population.as_database(),
                        &acquisition_state,
                        &detail_attempted,
                        &roster_resolved,
                        &direct_count,
                        &roster_count,
                    ],
                )
                .await?;

            if !durable_facts && parent.is_some() {
                transaction
                    .execute(
                        "DELETE FROM match_ingest_participants WHERE match_id = $1",
                        &[&match_id],
                    )
                    .await?;
                transaction
                    .execute(
                        &format!(
                            r#"
                            INSERT INTO match_ingest_participants (
                              match_id, roster_slot, player_id,
                              participant_kind, source
                            )
                            SELECT
                              match_id, roster_slot, player_id,
                              participant_kind,
                              CASE
                                WHEN LOWER(COALESCE(source, '')) IN (
                                  'direct', 'recovered'
                                ) THEN 'direct'
                                WHEN LOWER(COALESCE(source, '')) = 'roster'
                                  THEN 'roster'
                                ELSE 'local'
                              END
                            FROM {player_table}
                            WHERE match_id = $1
                            ON CONFLICT (match_id, roster_slot) DO UPDATE SET
                              player_id = EXCLUDED.player_id,
                              participant_kind = EXCLUDED.participant_kind,
                              source = EXCLUDED.source,
                              observed_at = now()
                            "#
                        ),
                        &[&match_id],
                    )
                    .await?;
            }
            transaction
                .execute(
                    r#"
                    UPDATE nonranked_match_acquisition
                    SET canonical_adopted_at = now(),
                        updated_at = now()
                    WHERE match_id = $1
                    "#,
                    &[&match_id],
                )
                .await?;

            summary.adopted += 1;
            summary.durable_facts += usize::from(durable_facts);
            summary.recovery_pending += usize::from(!durable_facts && roster_resolved);
            summary.existing_core_projections += usize::from(core_stats_projected);
        }
        transaction.commit().await?;
        Ok(summary)
    }

    pub async fn preview_nonranked_adoption(
        &self,
        limit: usize,
    ) -> Result<NonrankedAdoptionSummary, DatabaseError> {
        if limit == 0 {
            return Ok(NonrankedAdoptionSummary::default());
        }
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let client = self.database.connection().await?;
        let row = client
            .query_one(
                r#"
                WITH candidates AS MATERIALIZED (
                  SELECT
                    acquisition.match_id,
                    acquisition.stats_scope,
                    acquisition.roster_player_count,
                    acquisition.stats_projected_at IS NOT NULL
                      AS core_stats_projected
                  FROM nonranked_match_acquisition acquisition
                  WHERE acquisition.canonical_adopted_at IS NULL
                  ORDER BY acquisition.match_id
                  LIMIT $1
                ),
                classified AS (
                  SELECT
                    candidate.match_id,
                    COALESCE(casual.quality, special.quality) AS quality,
                    GREATEST(
                      candidate.roster_player_count,
                      COALESCE(casual.player_count, 0),
                      COALESCE(special.player_count, 0)
                    ) AS roster_count,
                    candidate.core_stats_projected
                  FROM candidates candidate
                  LEFT JOIN casual_matches casual
                    ON candidate.stats_scope = 'casual'
                   AND casual.match_id = candidate.match_id
                  LEFT JOIN special_matches special
                    ON candidate.stats_scope <> 'casual'
                   AND special.match_id = candidate.match_id
                )
                SELECT
                  COUNT(*)::bigint AS adopted,
                  COUNT(*) FILTER (
                    WHERE quality IN ('complete', 'limited')
                      AND roster_count > 0
                  )::bigint AS durable_facts,
                  COUNT(*) FILTER (
                    WHERE (
                      quality IS NULL
                      OR quality NOT IN ('complete', 'limited')
                    )
                    AND roster_count > 0
                  )::bigint AS recovery_pending,
                  COUNT(*) FILTER (
                    WHERE core_stats_projected
                  )::bigint AS existing_core_projections
                FROM classified
                "#,
                &[&limit],
            )
            .await?;
        Ok(NonrankedAdoptionSummary {
            adopted: usize::try_from(row.get::<_, i64>("adopted")).unwrap_or(usize::MAX),
            durable_facts: usize::try_from(row.get::<_, i64>("durable_facts"))
                .unwrap_or(usize::MAX),
            recovery_pending: usize::try_from(row.get::<_, i64>("recovery_pending"))
                .unwrap_or(usize::MAX),
            existing_core_projections: usize::try_from(
                row.get::<_, i64>("existing_core_projections"),
            )
            .unwrap_or(usize::MAX),
        })
    }

    pub async fn claim(
        &self,
        match_id: i64,
        source: &str,
        owner: &str,
        lease: Duration,
    ) -> Result<Option<MatchEvidence>, DatabaseError> {
        let client = self.database.connection().await?;
        let lease_seconds = i32::try_from(lease.as_secs()).unwrap_or(i32::MAX);
        let row = client
            .query_opt(CLAIM_SQL, &[&match_id, &source, &owner, &lease_seconds])
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        self.load_evidence_from_claim(&client, row).await.map(Some)
    }

    pub async fn checkpoint(
        &self,
        match_id: i64,
        owner: &str,
        checkpoint: &MatchCheckpoint,
    ) -> Result<bool, DatabaseError> {
        let client = self.database.connection().await?;
        let updated = client
            .execute(
                CHECKPOINT_SQL,
                &[
                    &match_id,
                    &owner,
                    &checkpoint.queue_id,
                    &checkpoint.population.as_database(),
                    &checkpoint.acquisition_state,
                    &checkpoint.detail_attempted,
                    &checkpoint.roster_resolved,
                    &checkpoint.demo_resolved,
                    &checkpoint.direct_player_count,
                    &checkpoint.roster_player_count,
                    &checkpoint.unresolved_player_ids,
                ],
            )
            .await?;
        Ok(updated == 1)
    }

    pub async fn replace_participant_anchors(
        &self,
        match_id: i64,
        owner: &str,
        anchors: &[ParticipantAnchor],
    ) -> Result<bool, DatabaseError> {
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let owns_match = transaction
            .query_opt(
                r#"
                SELECT 1
                FROM match_ingest_status
                WHERE match_id = $1
                  AND lease_owner = $2
                  AND lease_until > now()
                FOR UPDATE
                "#,
                &[&match_id, &owner],
            )
            .await?
            .is_some();
        if !owns_match {
            transaction.rollback().await?;
            return Ok(false);
        }

        transaction
            .execute(
                "DELETE FROM match_ingest_participants WHERE match_id = $1",
                &[&match_id],
            )
            .await?;
        for anchor in anchors {
            transaction
                .execute(
                    r#"
                    INSERT INTO match_ingest_participants (
                      match_id, roster_slot, player_id, participant_kind, source
                    )
                    VALUES ($1, $2, $3, $4, $5)
                    "#,
                    &[
                        &match_id,
                        &anchor.roster_slot,
                        &anchor.player_id,
                        &anchor.participant_kind,
                        &anchor.source.as_database(),
                    ],
                )
                .await?;
        }
        transaction.commit().await?;
        Ok(true)
    }

    pub async fn release(&self, match_id: i64, owner: &str) -> Result<bool, DatabaseError> {
        let client = self.database.connection().await?;
        Ok(client.execute(RELEASE_SQL, &[&match_id, &owner]).await? == 1)
    }

    pub async fn complete_if_durable(
        &self,
        match_id: i64,
        owner: &str,
    ) -> Result<bool, DatabaseError> {
        let client = self.database.connection().await?;
        Ok(client.execute(COMPLETE_SQL, &[&match_id, &owner]).await? == 1)
    }

    /// Purpose: close one immutable provider-unavailable match without
    /// blocking sibling matches in the shared queue-hour drain.
    /// Input: positive match/queue IDs, typed discovery source, stable reason.
    /// Output: durable terminal ledger completion, including an existing
    /// concurrent `complete` row;
    /// relationship: every discovery caller reuses this lifecycle boundary.
    pub async fn mark_terminal_unavailable(
        &self,
        match_id: i64,
        queue_id: i32,
        source: MatchDiscoverySource,
        reason: &str,
    ) -> Result<(), DatabaseError> {
        let client = self.database.connection().await?;
        client
            .execute(
                MARK_TERMINAL_UNAVAILABLE_SQL,
                &[&match_id, &queue_id, &source.as_database(), &reason],
            )
            .await?;
        Ok(())
    }

    async fn load_evidence_from_claim(
        &self,
        client: &deadpool_postgres::Object,
        row: Row,
    ) -> Result<MatchEvidence, DatabaseError> {
        let match_id = row.get::<_, i64>("match_id");
        let participant_rows = client.query(PARTICIPANT_IDS_SQL, &[&match_id]).await?;
        let history_rows = client.query(HISTORY_IDS_SQL, &[&match_id]).await?;

        let mut direct_player_ids = BTreeSet::new();
        let mut roster_player_ids = BTreeSet::new();
        for participant in participant_rows {
            let player_id = participant.get::<_, i64>("player_id");
            match participant.get::<_, String>("source").as_str() {
                "direct" => {
                    direct_player_ids.insert(player_id);
                    roster_player_ids.insert(player_id);
                }
                _ => {
                    roster_player_ids.insert(player_id);
                }
            }
        }
        let history_player_ids = history_rows
            .into_iter()
            .map(|history| history.get::<_, i64>("player_id"))
            .collect();

        let queue_id = row.get::<_, Option<i32>>("queue_id");
        let roster_resolved = persisted_roster_is_complete(
            queue_id,
            row.get("roster_player_count"),
            roster_player_ids.len(),
        );
        Ok(MatchEvidence {
            match_id,
            status: row.get("status"),
            completed_stages: row
                .get::<_, Vec<String>>("completed_stages")
                .into_iter()
                .collect(),
            queue_id,
            population: MatchPopulation::from_database(row.get::<_, String>("population").as_str()),
            acquisition_state: row.get("acquisition_state"),
            detail_attempted: row.get("detail_attempted"),
            // The shared cardinality policy is the authority. A timestamp or
            // one partial participant must not suppress the missing roster
            // fetch for fixed 5v5 queues; variable bot/PvE rosters remain valid.
            roster_resolved,
            demo_resolved: row.get("demo_resolved"),
            direct_player_ids,
            roster_player_ids,
            history_player_ids,
            unresolved_player_ids: row
                .get::<_, Vec<i64>>("unresolved_player_ids")
                .into_iter()
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use paladinscat_core::config::BackendConfig;

    fn evidence() -> MatchEvidence {
        MatchEvidence {
            match_id: 42,
            status: "processing".to_owned(),
            completed_stages: BTreeSet::new(),
            queue_id: Some(424),
            population: MatchPopulation::Casual,
            acquisition_state: "recovery_pending".to_owned(),
            detail_attempted: true,
            roster_resolved: true,
            demo_resolved: false,
            direct_player_ids: BTreeSet::from([1, 2]),
            roster_player_ids: (1..=10).collect(),
            history_player_ids: BTreeSet::from([3, 4, 5, 6, 7, 8]),
            unresolved_player_ids: BTreeSet::from([9, 10]),
        }
    }

    #[test]
    fn every_stored_population_is_authoritative_roster_evidence() {
        for table in [
            "FROM match_players",
            "FROM casual_match_players",
            "FROM special_match_players",
        ] {
            assert!(PARTICIPANT_IDS_SQL.contains(table));
        }
    }

    #[test]
    fn undiscovered_match_fetches_detail_first() {
        let mut value = evidence();
        value.detail_attempted = false;
        value.roster_resolved = false;
        assert_eq!(
            plan_match_lifecycle(&value),
            vec![MatchLifecycleAction::FetchDetail]
        );
    }

    #[test]
    fn attempted_broken_match_without_roster_fetches_only_roster() {
        let mut value = evidence();
        value.roster_resolved = false;
        assert_eq!(
            plan_match_lifecycle(&value),
            vec![MatchLifecycleAction::FetchRoster]
        );
    }

    #[test]
    fn partial_fixed_roster_never_suppresses_roster_fetch() {
        assert!(!persisted_roster_is_complete(Some(424), 1, 1));
        assert!(persisted_roster_is_complete(Some(424), 10, 8));
        assert!(persisted_roster_is_complete(Some(425), 1, 1));
    }

    #[test]
    fn saved_roster_and_eight_local_players_skip_detail_and_roster() {
        assert_eq!(
            plan_match_lifecycle(&evidence()),
            vec![
                MatchLifecycleAction::FetchHistories(vec![9, 10]),
                MatchLifecycleAction::FetchDemo,
            ]
        );
    }

    #[test]
    fn local_histories_and_demo_resume_at_fact_finalization() {
        let mut value = evidence();
        value.history_player_ids.extend([9, 10]);
        value.unresolved_player_ids.clear();
        value.demo_resolved = true;
        assert_eq!(
            plan_match_lifecycle(&value),
            vec![MatchLifecycleAction::FinalizeFacts]
        );
    }

    #[test]
    fn durable_casual_facts_never_select_ranked_projection() {
        let mut value = evidence();
        value.completed_stages =
            BTreeSet::from(["player_facts".to_owned(), "match_bans".to_owned()]);
        assert_eq!(
            plan_match_lifecycle(&value),
            vec![MatchLifecycleAction::Project(MatchPopulation::Casual)]
        );
    }

    #[test]
    fn completed_projection_spends_no_more_work() {
        let mut value = evidence();
        value.population = MatchPopulation::Ranked;
        value.completed_stages = BTreeSet::from([
            "player_facts".to_owned(),
            "match_bans".to_owned(),
            "ranked_stats".to_owned(),
        ]);
        assert_eq!(
            plan_match_lifecycle(&value),
            vec![MatchLifecycleAction::Complete]
        );
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with migration 108"]
    async fn live_repository_claims_once_and_resumes_saved_evidence() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "match-lifecycle-integration").expect("database");
        let repository = MatchLifecycleRepository::new(database.clone());
        let match_id = 9_880_000_001_i64;
        let ranked_lookup_id = match_id + 1;
        let casual_lookup_id = match_id + 2;
        let client = database.connection().await.expect("connection");
        client
            .execute(
                "DELETE FROM match_ingest_status WHERE match_id = ANY($1::bigint[])",
                &[&vec![match_id, ranked_lookup_id, casual_lookup_id]],
            )
            .await
            .expect("clean fixture");
        drop(client);

        let ranked_lookup = repository
            .register_discovery(&MatchDiscovery {
                match_id: ranked_lookup_id,
                queue_id: Some(486),
                source: MatchDiscoverySource::ProfileHistory,
            })
            .await
            .expect("ranked profile discovery");
        assert_eq!(ranked_lookup.population, MatchPopulation::Ranked);
        assert!(ranked_lookup.needs_work);
        repository
            .claim(
                ranked_lookup_id,
                "profile_history",
                "ranked-lookup-worker",
                Duration::from_secs(60),
            )
            .await
            .expect("ranked lookup claim")
            .expect("ranked lookup owned");
        let client = database.connection().await.expect("connection");
        client
            .execute(
                r#"
                UPDATE match_ingest_status
                SET completed_stages = ARRAY[
                  'player_facts', 'match_bans', 'ranked_stats'
                ]
                WHERE match_id = $1
                "#,
                &[&ranked_lookup_id],
            )
            .await
            .expect("ranked stages");
        drop(client);
        assert!(
            repository
                .complete_if_durable(ranked_lookup_id, "ranked-lookup-worker")
                .await
                .expect("complete ranked lookup")
        );
        let ranked_hourly = repository
            .register_discovery(&MatchDiscovery {
                match_id: ranked_lookup_id,
                queue_id: Some(486),
                source: MatchDiscoverySource::HourlyDiscovery,
            })
            .await
            .expect("ranked hourly rediscovery");
        assert!(!ranked_hourly.needs_work);
        assert!(
            repository
                .claim(
                    ranked_lookup_id,
                    "ranked_hourly",
                    "ranked-hourly-worker",
                    Duration::from_secs(60),
                )
                .await
                .expect("ranked hourly terminal claim")
                .is_none()
        );

        let casual_lookup = repository
            .register_discovery(&MatchDiscovery {
                match_id: casual_lookup_id,
                queue_id: Some(424),
                source: MatchDiscoverySource::ProfileHistory,
            })
            .await
            .expect("casual profile discovery");
        assert_eq!(casual_lookup.population, MatchPopulation::Casual);
        assert!(casual_lookup.needs_work);
        repository
            .claim(
                casual_lookup_id,
                "profile_history",
                "casual-lookup-worker",
                Duration::from_secs(60),
            )
            .await
            .expect("casual lookup claim")
            .expect("casual lookup owned");
        let client = database.connection().await.expect("connection");
        client
            .execute(
                r#"
                UPDATE match_ingest_status
                SET completed_stages = ARRAY[
                  'player_facts', 'match_bans', 'casual_mechanics_stats'
                ]
                WHERE match_id = $1
                "#,
                &[&casual_lookup_id],
            )
            .await
            .expect("casual stages");
        drop(client);
        assert!(
            repository
                .complete_if_durable(casual_lookup_id, "casual-lookup-worker")
                .await
                .expect("complete casual lookup")
        );
        let casual_hourly = repository
            .register_discovery(&MatchDiscovery {
                match_id: casual_lookup_id,
                queue_id: Some(424),
                source: MatchDiscoverySource::HourlyDiscovery,
            })
            .await
            .expect("casual hourly rediscovery");
        assert!(!casual_hourly.needs_work);
        assert_eq!(casual_hourly.population, MatchPopulation::Casual);

        let claimed = repository
            .claim(match_id, "integration", "worker-a", Duration::from_secs(60))
            .await
            .expect("claim")
            .expect("owned claim");
        assert_eq!(
            plan_match_lifecycle(&claimed),
            vec![MatchLifecycleAction::FetchDetail]
        );
        assert!(
            repository
                .claim(match_id, "integration", "worker-b", Duration::from_secs(60))
                .await
                .expect("contended claim")
                .is_none()
        );

        let anchors: Vec<_> = (1_i64..=10)
            .map(|player_id| ParticipantAnchor {
                roster_slot: i16::try_from(player_id).expect("slot"),
                player_id,
                participant_kind: "human".to_owned(),
                source: if player_id <= 2 {
                    ParticipantAnchorSource::Direct
                } else {
                    ParticipantAnchorSource::Roster
                },
            })
            .collect();
        assert!(
            repository
                .replace_participant_anchors(match_id, "worker-a", &anchors)
                .await
                .expect("anchors")
        );
        assert!(
            repository
                .checkpoint(
                    match_id,
                    "worker-a",
                    &MatchCheckpoint {
                        queue_id: Some(424),
                        population: MatchPopulation::Casual,
                        acquisition_state: "recovery_pending".to_owned(),
                        detail_attempted: true,
                        roster_resolved: true,
                        demo_resolved: false,
                        direct_player_count: 2,
                        roster_player_count: 10,
                        unresolved_player_ids: vec![9, 10],
                    },
                )
                .await
                .expect("checkpoint")
        );
        let client = database.connection().await.expect("connection");
        for player_id in 3_i64..=8 {
            client
                .execute(
                    r#"
                    INSERT INTO player_match_history_entries (match_id, player_id)
                    VALUES ($1, $2)
                    ON CONFLICT (match_id, player_id) DO NOTHING
                    "#,
                    &[&match_id, &player_id],
                )
                .await
                .expect("history fixture");
        }
        drop(client);

        let resumed = repository
            .claim(match_id, "integration", "worker-a", Duration::from_secs(60))
            .await
            .expect("resume")
            .expect("same owner");
        assert_eq!(
            plan_match_lifecycle(&resumed),
            vec![
                MatchLifecycleAction::FetchHistories(vec![9, 10]),
                MatchLifecycleAction::FetchDemo,
            ]
        );
        assert!(
            repository
                .release(match_id, "worker-a")
                .await
                .expect("release")
        );
        assert!(
            repository
                .claim(match_id, "integration", "worker-b", Duration::from_secs(60))
                .await
                .expect("post-release claim")
                .is_some()
        );

        let client = database.connection().await.expect("connection");
        client
            .execute(
                "DELETE FROM match_ingest_status WHERE match_id = ANY($1::bigint[])",
                &[&vec![match_id, ranked_lookup_id, casual_lookup_id]],
            )
            .await
            .expect("cleanup fixture");
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL with migration 108"]
    async fn live_adoption_resumes_partial_nonranked_rows_without_provider_work() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| match name {
            "DATABASE_URL" => Some(database_url.clone()),
            "REDIS_URL" => Some("redis://127.0.0.1:9".to_owned()),
            _ => None,
        })
        .expect("config");
        let database = Database::new(&config, "nonranked-adoption-integration").expect("database");
        let repository = MatchLifecycleRepository::new(database.clone());
        let match_id = 9_883_000_001_i64;
        let client = database.connection().await.expect("connection");
        client
            .execute(
                "DELETE FROM player_match_history_entries WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("history cleanup");
        client
            .execute(
                "DELETE FROM match_ingest_status WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("status cleanup");
        client
            .execute(
                "DELETE FROM casual_matches WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("match cleanup");
        client
            .execute(
                "DELETE FROM nonranked_match_acquisition WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("ledger cleanup");
        client
            .execute(
                r#"
                INSERT INTO nonranked_match_acquisition (
                  match_id, queue_id, stats_scope, source_date, source_hour,
                  region, status, quality, direct_player_count,
                  roster_player_count, detail_attempts, roster_attempts,
                  canonical_adopted_at
                )
                VALUES (
                  $1, 424, 'casual', CURRENT_DATE, 0,
                  'North America', 'partial_roster', 'partial',
                  1, 2, 1, 1, NULL
                )
                "#,
                &[&match_id],
            )
            .await
            .expect("ledger fixture");
        client
            .execute(
                r#"
                INSERT INTO casual_matches (
                  match_id, queue_id, entry_datetime, region, map,
                  quality, stats_eligible, player_count, source
                )
                VALUES (
                  $1, 424, now(), 'North America', 'Adoption Map',
                  'partial', false, 2, 'recovery'
                )
                "#,
                &[&match_id],
            )
            .await
            .expect("partial match fixture");
        for (slot, player_id, source) in [(1_i16, 101_i64, "direct"), (2, 102, "roster")] {
            client
                .execute(
                    r#"
                    INSERT INTO casual_match_players (
                      match_id, roster_slot, player_id, participant_kind,
                      source, stats_eligible
                    )
                    VALUES ($1, $2, $3, 'human', $4, false)
                    "#,
                    &[&match_id, &slot, &player_id, &source],
                )
                .await
                .expect("partial player fixture");
        }
        drop(client);

        assert_eq!(
            repository
                .preview_nonranked_adoption(10)
                .await
                .expect("preview nonranked adoption"),
            NonrankedAdoptionSummary {
                adopted: 1,
                durable_facts: 0,
                recovery_pending: 1,
                existing_core_projections: 0,
            }
        );
        let summary = repository
            .adopt_nonranked_batch(10)
            .await
            .expect("adopt nonranked");
        assert_eq!(
            summary,
            NonrankedAdoptionSummary {
                adopted: 1,
                durable_facts: 0,
                recovery_pending: 1,
                existing_core_projections: 0,
            }
        );
        let evidence = repository
            .claim(
                match_id,
                "legacy_nonranked_adoption",
                "adoption-worker",
                Duration::from_secs(60),
            )
            .await
            .expect("claim adopted")
            .expect("adopted work");
        assert!(evidence.detail_attempted);
        assert!(evidence.roster_resolved);
        assert_eq!(
            plan_match_lifecycle(&evidence),
            vec![
                MatchLifecycleAction::FetchHistories(vec![102]),
                MatchLifecycleAction::FetchDemo,
            ]
        );
        assert!(
            repository
                .release(match_id, "adoption-worker")
                .await
                .expect("release adoption")
        );
        assert_eq!(
            repository
                .adopt_nonranked_batch(10)
                .await
                .expect("idempotent adoption"),
            NonrankedAdoptionSummary::default()
        );
        assert_eq!(
            repository
                .preview_nonranked_adoption(10)
                .await
                .expect("empty adoption preview"),
            NonrankedAdoptionSummary::default()
        );

        let client = database.connection().await.expect("connection");
        client
            .execute(
                "DELETE FROM match_ingest_status WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("status cleanup");
        client
            .execute(
                "DELETE FROM casual_matches WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("match cleanup");
        client
            .execute(
                "DELETE FROM nonranked_match_acquisition WHERE match_id = $1",
                &[&match_id],
            )
            .await
            .expect("ledger cleanup");
    }
}
