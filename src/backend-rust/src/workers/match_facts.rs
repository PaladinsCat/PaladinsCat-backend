use std::collections::{BTreeMap, BTreeSet};

use paladinscat_core::database::{Database, DatabaseError};
use serde::Deserialize;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_postgres::Transaction;

use super::match_lifecycle::MatchPopulation;
use super::private_identity::persist_and_resolve_private_identities;

const FACT_STAGES: [&str; 3] = ["core", "player_facts", "match_bans"];
const PRIVATE_ACCOUNT_NAME: &str = "PRIVATEACCOUNT";

#[derive(Debug, thiserror::Error)]
pub enum MatchFactError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Query(#[from] tokio_postgres::Error),
    #[error("invalid canonical match payload: {0}")]
    InvalidPayload(String),
    #[error(
        "match {match_id} was already classified as {existing}; refusing fact population {incoming}"
    )]
    PopulationConflict {
        match_id: i64,
        existing: String,
        incoming: &'static str,
    },
    #[error(
        "match {match_id} was already assigned queue {existing}; refusing fact queue {incoming}"
    )]
    QueueConflict {
        match_id: i64,
        existing: i32,
        incoming: i32,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatchFactFinalization {
    pub match_id: i64,
    pub population: MatchPopulation,
    pub player_count: usize,
    pub facts_written: bool,
    pub completed_stages: BTreeSet<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CanonicalMatchPayload {
    pub match_id: i64,
    #[serde(default)]
    pub entry_datetime: String,
    #[serde(default)]
    pub map: String,
    #[serde(default)]
    pub queue_id: i32,
    #[serde(default)]
    pub duration_seconds: i32,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub team1_score: Option<i32>,
    #[serde(default)]
    pub team2_score: Option<i32>,
    #[serde(default)]
    pub winning_task_force: Option<i32>,
    #[serde(default)]
    pub has_replay: Option<bool>,
    #[serde(default)]
    pub recovery_source: Option<String>,
    #[serde(default)]
    pub recovery_api_calls: Option<i32>,
    #[serde(default)]
    pub limited: Option<bool>,
    #[serde(default)]
    pub players: Vec<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl CanonicalMatchPayload {
    /// Accepts the canonical relay shapes used by direct lookup and lifecycle
    /// recovery: a MatchDetails object, a CompletedMatchResolution object, or
    /// a one-element array containing either shape.
    pub fn from_relay_value(value: Value) -> Result<Self, MatchFactError> {
        let candidate = unwrap_relay_match(value).ok_or_else(|| {
            MatchFactError::InvalidPayload(
                "relay response did not contain a completed match".to_owned(),
            )
        })?;
        let payload = serde_json::from_value::<Self>(candidate).map_err(|error| {
            MatchFactError::InvalidPayload(format!("match contract decode failed: {error}"))
        })?;
        payload.validate()?;
        Ok(payload)
    }

    fn validate(&self) -> Result<(), MatchFactError> {
        if self.match_id <= 0 {
            return Err(MatchFactError::InvalidPayload(
                "match_id must be positive".to_owned(),
            ));
        }
        if self.queue_id <= 0 {
            return Err(MatchFactError::InvalidPayload(
                "queue_id must be positive".to_owned(),
            ));
        }
        if self.duration_seconds <= 0 {
            return Err(MatchFactError::InvalidPayload(
                "duration_seconds must be positive".to_owned(),
            ));
        }
        if self.map.trim().is_empty() {
            return Err(MatchFactError::InvalidPayload(
                "map must be present".to_owned(),
            ));
        }
        OffsetDateTime::parse(&self.entry_datetime, &Rfc3339).map_err(|error| {
            MatchFactError::InvalidPayload(format!("entry_datetime must be RFC3339: {error}"))
        })?;
        if !valid_completed_score(self.team1_score, self.team2_score, self.winning_task_force) {
            return Err(MatchFactError::InvalidPayload(
                "completed match score is missing or contradictory".to_owned(),
            ));
        }
        if self.players.is_empty() {
            return Err(MatchFactError::InvalidPayload(
                "players must not be empty".to_owned(),
            ));
        }
        if self.players.iter().any(|player| !player.is_object()) {
            return Err(MatchFactError::InvalidPayload(
                "every player fact must be an object".to_owned(),
            ));
        }
        Ok(())
    }

    fn normalized_players(&self) -> Result<Value, MatchFactError> {
        let mut players = self.players.clone();
        assign_private_slots(&mut players);
        validate_authoritative_players(&players, self.limited == Some(true))?;
        Ok(Value::Array(players))
    }

    fn ban_entries(&self) -> Value {
        let mut entries = Vec::new();
        for slot in 1..=8 {
            let names = [
                format!("ban_id_{slot}"),
                format!("BanId{slot}"),
                format!("Ban_{slot}"),
            ];
            let champion_id = names
                .iter()
                .find_map(|name| positive_i64(self.extra.get(name)))
                .or_else(|| {
                    self.players.iter().find_map(|player| {
                        names.iter().find_map(|name| positive_i64(player.get(name)))
                    })
                })
                .unwrap_or_default();
            if champion_id > 0 {
                entries.push(json!({"slot": slot, "champion_id": champion_id}));
            }
        }
        Value::Array(entries)
    }

    fn quality_flags(&self) -> MatchQualityFlags {
        let has_recovered = self
            .players
            .iter()
            .any(|player| player_text(player, "source").eq_ignore_ascii_case("recovered"));
        let has_private = self.players.iter().any(is_private_player);
        let has_minimal_private = self
            .players
            .iter()
            .any(|player| is_private_player(player) && player_i64(player, "champion_id") <= 0);
        let limited = self.limited == Some(true);
        let recovered = !limited
            && has_recovered
            && !has_minimal_private
            && self.players.iter().all(is_detailed_player);
        MatchQualityFlags {
            broken: limited || has_recovered || has_minimal_private,
            recovered,
            private: has_private,
            limited,
            source: if has_minimal_private {
                "minimal"
            } else if recovered {
                "recovery"
            } else {
                "direct"
            },
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct MatchQualityFlags {
    broken: bool,
    recovered: bool,
    private: bool,
    limited: bool,
    source: &'static str,
}

#[derive(Clone)]
pub struct MatchFactRepository {
    database: Database,
}

impl MatchFactRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Persists the public match-detail boundary in one transaction.
    ///
    /// The canonical fact tables are shared for every mechanics population.
    /// Population is recorded on match_ingest_status and is consumed only when
    /// the physically isolated ranked/casual/special projectors run later.
    pub async fn finalize(
        &self,
        payload: &CanonicalMatchPayload,
        source: &str,
    ) -> Result<MatchFactFinalization, MatchFactError> {
        payload.validate()?;
        let players = payload.normalized_players()?;
        let bans = payload.ban_entries();
        let quality = payload.quality_flags();
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        let population = classify_queue(&transaction, payload.queue_id).await?;
        let mut stages = lock_ingest_status(&transaction, payload, population, source).await?;
        let already_durable = FACT_STAGES.iter().all(|stage| stages.contains(*stage));

        if !stages.contains("core") {
            persist_match(&transaction, payload, population, quality).await?;
            add_stage(&transaction, payload.match_id, "core").await?;
            stages.insert("core".to_owned());
        }
        if !stages.contains("player_facts") {
            persist_player_facts(&transaction, payload, population, &players).await?;
            add_stage(&transaction, payload.match_id, "player_facts").await?;
            stages.insert("player_facts".to_owned());
        }
        // Private-account observations and their inferred identity links share
        // the same transaction as canonical player facts. Replays are safe:
        // the immutable match/slot observation upsert preserves resolved links.
        persist_and_resolve_private_identities(&transaction, payload, &players, quality.limited)
            .await?;
        if !stages.contains("match_bans") {
            persist_bans(&transaction, payload.match_id, &bans).await?;
            add_stage(&transaction, payload.match_id, "match_bans").await?;
            stages.insert("match_bans".to_owned());
        }

        let player_count = players.as_array().map_or(0, Vec::len);
        let direct_player_count = players.as_array().map_or(0, |rows| {
            rows.iter()
                .filter(|player| {
                    matches!(
                        player_text(player, "source").to_ascii_lowercase().as_str(),
                        "direct" | "recovered"
                    )
                })
                .count()
        });
        let player_count = i16::try_from(player_count).unwrap_or(i16::MAX);
        let direct_player_count = i16::try_from(direct_player_count).unwrap_or(i16::MAX);
        transaction
            .execute(
                r#"
                UPDATE match_ingest_status
                SET status = CASE
                      WHEN status IN ('complete', 'limited') THEN status
                      WHEN $4 THEN 'limited'
                      ELSE 'partial'
                    END,
                    acquisition_state = CASE WHEN $4 THEN 'limited' ELSE 'facts_ready' END,
                    direct_player_count = $2,
                    roster_player_count = $3,
                    unresolved_player_ids = '{}',
                    error_message = NULL,
                    completed_at = CASE WHEN $4 THEN COALESCE(completed_at, now()) ELSE completed_at END,
                    updated_at = now()
                WHERE match_id = $1
                "#,
                &[
                    &payload.match_id,
                    &direct_player_count,
                    &player_count,
                    &quality.limited,
                ],
            )
            .await?;
        close_hourly_match_debt(&transaction, payload.match_id).await?;
        transaction.commit().await?;

        Ok(MatchFactFinalization {
            match_id: payload.match_id,
            population,
            player_count: usize::try_from(player_count).unwrap_or_default(),
            facts_written: !already_durable,
            completed_stages: stages,
        })
    }
}

async fn classify_queue(
    transaction: &Transaction<'_>,
    queue_id: i32,
) -> Result<MatchPopulation, MatchFactError> {
    let row = transaction
        .query_opt(
            "SELECT is_ranked, stats_scope FROM queue_types WHERE queue_id=$1",
            &[&queue_id],
        )
        .await?
        .ok_or_else(|| {
            MatchFactError::InvalidPayload(format!(
                "queue_id {queue_id} is absent from the canonical queue taxonomy"
            ))
        })?;
    let is_ranked = row.get::<_, bool>("is_ranked");
    let stats_scope = row.get::<_, String>("stats_scope");
    Ok(if is_ranked || stats_scope == "ranked" {
        MatchPopulation::Ranked
    } else if stats_scope == "casual" {
        MatchPopulation::Casual
    } else {
        MatchPopulation::Special
    })
}

async fn lock_ingest_status(
    transaction: &Transaction<'_>,
    payload: &CanonicalMatchPayload,
    population: MatchPopulation,
    source: &str,
) -> Result<BTreeSet<String>, MatchFactError> {
    let existing = transaction
        .query_opt(
            "SELECT queue_id,population FROM match_ingest_status WHERE match_id=$1 FOR UPDATE",
            &[&payload.match_id],
        )
        .await?;
    if let Some(existing) = &existing {
        let existing_queue = existing.get::<_, Option<i32>>("queue_id");
        if let Some(existing_queue) = existing_queue
            && existing_queue != payload.queue_id
        {
            return Err(MatchFactError::QueueConflict {
                match_id: payload.match_id,
                existing: existing_queue,
                incoming: payload.queue_id,
            });
        }
        let existing_population = existing.get::<_, String>("population");
        if existing_population != "unknown" && existing_population != population.as_database() {
            return Err(MatchFactError::PopulationConflict {
                match_id: payload.match_id,
                existing: existing_population,
                incoming: population.as_database(),
            });
        }
    }
    let row = transaction
        .query_one(
            r#"
            INSERT INTO match_ingest_status (
              match_id,status,source,attempts,queue_id,population,
              acquisition_state,detail_attempted_at,started_at,updated_at
            )
            VALUES ($1,'processing',$2,1,$3,$4,'detail_complete',now(),now(),now())
            ON CONFLICT (match_id) DO UPDATE SET
              status = CASE
                WHEN match_ingest_status.status IN ('complete','limited')
                  THEN match_ingest_status.status
                ELSE 'processing'
              END,
              source = EXCLUDED.source,
              attempts = match_ingest_status.attempts + 1,
              queue_id = COALESCE(match_ingest_status.queue_id,EXCLUDED.queue_id),
              population = CASE
                WHEN match_ingest_status.population='unknown' THEN EXCLUDED.population
                ELSE match_ingest_status.population
              END,
              acquisition_state = CASE
                WHEN match_ingest_status.acquisition_state IN ('complete','limited')
                  THEN match_ingest_status.acquisition_state
                ELSE 'detail_complete'
              END,
              detail_attempted_at = COALESCE(match_ingest_status.detail_attempted_at,now()),
              error_message = NULL,
              updated_at = now()
            RETURNING completed_stages
            "#,
            &[
                &payload.match_id,
                &source,
                &payload.queue_id,
                &population.as_database(),
            ],
        )
        .await?;
    Ok(row
        .get::<_, Vec<String>>("completed_stages")
        .into_iter()
        .collect())
}

async fn persist_match(
    transaction: &Transaction<'_>,
    payload: &CanonicalMatchPayload,
    population: MatchPopulation,
    quality: MatchQualityFlags,
) -> Result<(), MatchFactError> {
    let surrendered = payload
        .players
        .iter()
        .any(|player| player_bool(player, "surrendered"));
    let match_level = payload
        .players
        .iter()
        .map(|player| player_i64(player, "final_match_level"))
        .max()
        .unwrap_or_default() as i32;
    transaction
        .execute(
            r#"
            INSERT INTO matches (
              match_id,entry_datetime,map,queue_id,duration_seconds,region,
              team1_score,team2_score,winning_task_force,has_replay,is_ranked,
              recovered,broken,private,limited,limited_reason,surrendered,
              match_level,source,ingested_at
            )
            VALUES (
              $1,$2::text::timestamptz,$3,$4,$5,$6,$7,$8,$9,$10,$11,
              $12,$13,$14,$15,$16,$17,$18,$19,now()
            )
            ON CONFLICT (match_id,entry_datetime) DO UPDATE SET
              duration_seconds=CASE
                WHEN COALESCE(matches.duration_seconds,0)<=0 AND EXCLUDED.duration_seconds>0
                  THEN EXCLUDED.duration_seconds
                ELSE matches.duration_seconds
              END,
              broken=EXCLUDED.broken,
              recovered=EXCLUDED.recovered,
              private=EXCLUDED.private,
              limited=EXCLUDED.limited,
              limited_reason=EXCLUDED.limited_reason,
              source=EXCLUDED.source,
              team1_score=EXCLUDED.team1_score,
              team2_score=EXCLUDED.team2_score,
              winning_task_force=EXCLUDED.winning_task_force,
              ingested_at=now()
            "#,
            &[
                &payload.match_id,
                &payload.entry_datetime,
                &payload.map,
                &payload.queue_id,
                &payload.duration_seconds,
                &normalized_region(&payload.region),
                &payload.team1_score,
                &payload.team2_score,
                &payload.winning_task_force,
                &payload.has_replay.unwrap_or(false),
                &(population == MatchPopulation::Ranked),
                &quality.recovered,
                &quality.broken,
                &quality.private,
                &quality.limited,
                &quality.limited.then_some("authoritative_roster_incomplete"),
                &surrendered,
                &match_level,
                &quality.source,
            ],
        )
        .await?;
    Ok(())
}

async fn persist_player_facts(
    transaction: &Transaction<'_>,
    payload: &CanonicalMatchPayload,
    population: MatchPopulation,
    players: &Value,
) -> Result<(), MatchFactError> {
    ensure_fact_references(transaction, players).await?;
    transaction
        .execute(
            r#"
            INSERT INTO players (
              id,name,level,wins,losses,mastery_level,region,platform,
              portal_id,portal_user_id,kbm_tier,kbm_points,
              first_seen,last_seen,last_updated,name_source
            )
            SELECT
              (p->>'player_id')::bigint,
              COALESCE(NULLIF(p->>'player_name',''),'Unknown'),
              COALESCE(NULLIF(p->>'account_level','')::int,0),
              0,0,
              COALESCE(NULLIF(p->>'mastery_level','')::int,0),
              COALESCE(NULLIF(p->>'region',''),'Unknown'),
              NULLIF(p->>'platform',''),
              NULLIF(p->>'portal_id','')::smallint,
              NULLIF(p->>'portal_user_id',''),
              CASE
                WHEN $2 AND COALESCE(NULLIF(p->>'league_tier','')::int,0) BETWEEN 1 AND 26
                  THEN (p->>'league_tier')::int
                ELSE 0
              END,
              CASE
                WHEN $2 AND COALESCE(NULLIF(p->>'league_tier','')::int,0) BETWEEN 1 AND 26
                  THEN COALESCE(NULLIF(p->>'league_points','')::int,0)
                ELSE 0
              END,
              now(),now(),now(),'match_player'
            FROM jsonb_array_elements($1::jsonb) AS facts(p)
            WHERE COALESCE(NULLIF(p->>'player_id','')::bigint,0)>0
            ON CONFLICT (id) DO UPDATE SET
              name=EXCLUDED.name,
              name_source='match_player',
              kbm_tier=CASE
                WHEN COALESCE(players.kbm_tier,0)=0 AND EXCLUDED.kbm_tier BETWEEN 1 AND 26
                  THEN EXCLUDED.kbm_tier
                ELSE players.kbm_tier
              END,
              kbm_points=CASE
                WHEN COALESCE(players.kbm_tier,0)=0 AND EXCLUDED.kbm_tier BETWEEN 1 AND 26
                  THEN EXCLUDED.kbm_points
                ELSE players.kbm_points
              END,
              last_seen=now()
            "#,
            &[players, &(population == MatchPopulation::Ranked)],
        )
        .await?;

    transaction
        .execute(
            r#"
            WITH facts AS (
              SELECT
                p,
                COALESCE(NULLIF(p->>'player_id','')::bigint,0) AS player_id,
                COALESCE(NULLIF(p->>'_private_slot','')::smallint,0) AS private_slot,
                CASE LOWER(COALESCE(NULLIF(p->>'source',''),'direct'))
                  WHEN 'direct' THEN 'direct'
                  WHEN 'recovered' THEN 'recovered'
                  ELSE 'minimal'
                END AS fact_source
              FROM jsonb_array_elements($4::jsonb) AS payload(p)
            )
            INSERT INTO match_players (
              match_id,player_id,private_slot,player_name,region,champion_id,
              skin_id,skin_name,kills,deaths,assists,damage_done_in_hand,
              damage_done_physical,damage_done_magical,damage_taken,
              damage_taken_physical,damage_taken_magical,damage_mitigated,
              healing,healing_self,healing_bot,healing_player_self,gold_earned,
              objective_assists,camps_cleared,structure_damage,wards_placed,
              towers_destroyed,distance_traveled,multi_kill_max,killing_spree,
              kills_first_blood,kills_double,kills_triple,kills_quadra,kills_penta,
              kills_fire_giant,kills_gold_fury,kills_phoenix,kills_siege_jugg,
              kills_wild_jugg,win_status,task_force,league_tier,league_points,
              league_wins,league_losses,account_level,mastery_level,party_id,kda,
              time_in_match,entry_datetime,source,portal_id,is_ranked,
              private_player_id,portal_user_id,kills_player,created_at,platform,
              damage_bot,kills_single,kills_bot,final_match_level,rank_stat_league,
              team_id,surrendered,has_ret_msg
            )
            SELECT
              $1,f.player_id,f.private_slot,NULLIF(f.p->>'player_name',''),
              COALESCE(NULLIF(f.p->>'region',''),'Unknown'),
              NULLIF(f.p->>'champion_id','')::int,
              NULLIF(f.p->>'skin_id','')::int,NULLIF(f.p->>'skin_name',''),
              COALESCE(NULLIF(f.p->>'kills','')::int,0),
              COALESCE(NULLIF(f.p->>'deaths','')::int,0),
              COALESCE(NULLIF(f.p->>'assists','')::int,0),
              COALESCE(NULLIF(f.p->>'damage_done_in_hand','')::int,0),
              COALESCE(NULLIF(f.p->>'damage_done_physical','')::int,0),
              COALESCE(NULLIF(f.p->>'damage_done_magical','')::int,0),
              COALESCE(NULLIF(f.p->>'damage_taken','')::int,0),
              COALESCE(NULLIF(f.p->>'damage_taken_physical','')::int,0),
              COALESCE(NULLIF(f.p->>'damage_taken_magical','')::int,0),
              COALESCE(NULLIF(f.p->>'damage_mitigated','')::int,0),
              COALESCE(NULLIF(f.p->>'healing','')::int,0),
              COALESCE(NULLIF(f.p->>'healing_self','')::int,0),
              COALESCE(NULLIF(f.p->>'healing_bot','')::int,0),
              COALESCE(NULLIF(f.p->>'healing_player_self','')::int,0),
              COALESCE(NULLIF(f.p->>'gold_earned','')::int,0),
              COALESCE(NULLIF(f.p->>'objective_assists','')::int,0),
              COALESCE(NULLIF(f.p->>'camps_cleared','')::int,0),
              COALESCE(NULLIF(f.p->>'structure_damage','')::int,0),
              COALESCE(NULLIF(f.p->>'wards_placed','')::int,0),
              COALESCE(NULLIF(f.p->>'towers_destroyed','')::int,0),
              COALESCE(NULLIF(f.p->>'distance_traveled','')::int,0),
              COALESCE(NULLIF(f.p->>'multi_kill_max','')::int,0),
              COALESCE(NULLIF(f.p->>'killing_spree','')::int,0),
              LOWER(COALESCE(f.p->>'kills_first_blood','false')) IN ('true','1'),
              COALESCE(NULLIF(f.p->>'kills_double','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_triple','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_quadra','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_penta','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_fire_giant','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_gold_fury','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_phoenix','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_siege_jugg','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_wild_jugg','')::int,0),
              NULLIF(f.p->>'win_status',''),
              NULLIF(f.p->>'task_force','')::smallint,
              COALESCE(NULLIF(f.p->>'league_tier','')::int,0),
              COALESCE(NULLIF(f.p->>'league_points','')::int,0),
              COALESCE(NULLIF(f.p->>'league_wins','')::int,0),
              COALESCE(NULLIF(f.p->>'league_losses','')::int,0),
              COALESCE(NULLIF(f.p->>'account_level','')::int,0),
              COALESCE(NULLIF(f.p->>'mastery_level','')::int,0),
              COALESCE(NULLIF(f.p->>'party_id','')::int,0),
              ROUND((
                COALESCE(NULLIF(f.p->>'kills','')::numeric,0)
                + COALESCE(NULLIF(f.p->>'assists','')::numeric,0)/2
              ) / GREATEST(COALESCE(NULLIF(f.p->>'deaths','')::numeric,0),1),2)::double precision,
              COALESCE(NULLIF(f.p->>'time_in_match','')::int,0),
              $2::text::timestamptz,f.fact_source,
              NULLIF(f.p->>'portal_id','')::smallint,$3,NULL,
              NULLIF(f.p->>'portal_user_id',''),
              COALESCE(NULLIF(f.p->>'kills_player','')::int,0),now(),
              NULLIF(f.p->>'platform',''),
              COALESCE(NULLIF(f.p->>'damage_bot','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_single','')::int,0),
              COALESCE(NULLIF(f.p->>'kills_bot','')::int,0),
              COALESCE(NULLIF(f.p->>'final_match_level','')::int,0),
              COALESCE(NULLIF(f.p->>'rank_stat_league','')::int,0),
              NULLIF(f.p->>'team_id','')::int,
              LOWER(COALESCE(f.p->>'surrendered','false')) IN ('true','1'),
              LOWER(COALESCE(f.p->>'has_ret_msg','false')) IN ('true','1')
            FROM facts f
            ON CONFLICT (match_id,player_id,private_slot,entry_datetime) DO UPDATE SET
              player_name=EXCLUDED.player_name,region=EXCLUDED.region,
              champion_id=EXCLUDED.champion_id,skin_id=EXCLUDED.skin_id,
              skin_name=EXCLUDED.skin_name,kills=EXCLUDED.kills,
              deaths=EXCLUDED.deaths,assists=EXCLUDED.assists,
              damage_done_in_hand=EXCLUDED.damage_done_in_hand,
              damage_done_physical=EXCLUDED.damage_done_physical,
              damage_done_magical=EXCLUDED.damage_done_magical,
              damage_taken=EXCLUDED.damage_taken,
              damage_taken_physical=EXCLUDED.damage_taken_physical,
              damage_taken_magical=EXCLUDED.damage_taken_magical,
              damage_mitigated=EXCLUDED.damage_mitigated,healing=EXCLUDED.healing,
              healing_self=EXCLUDED.healing_self,healing_bot=EXCLUDED.healing_bot,
              healing_player_self=EXCLUDED.healing_player_self,
              gold_earned=EXCLUDED.gold_earned,
              objective_assists=EXCLUDED.objective_assists,
              win_status=EXCLUDED.win_status,task_force=EXCLUDED.task_force,
              league_tier=EXCLUDED.league_tier,league_points=EXCLUDED.league_points,
              account_level=EXCLUDED.account_level,mastery_level=EXCLUDED.mastery_level,
              party_id=EXCLUDED.party_id,kda=EXCLUDED.kda,
              time_in_match=EXCLUDED.time_in_match,source=EXCLUDED.source,
              portal_id=EXCLUDED.portal_id,portal_user_id=EXCLUDED.portal_user_id,
              platform=EXCLUDED.platform,created_at=EXCLUDED.created_at
            WHERE
              CASE EXCLUDED.source WHEN 'direct' THEN 4 WHEN 'recovered' THEN 3 ELSE 1 END
              >= CASE match_players.source WHEN 'direct' THEN 4 WHEN 'recovered' THEN 3 ELSE 1 END
            "#,
            &[
                &payload.match_id,
                &payload.entry_datetime,
                &(population == MatchPopulation::Ranked),
                players,
            ],
        )
        .await?;

    transaction
        .execute(
            r#"
            WITH incoming AS (
              SELECT
                COALESCE(NULLIF(p->>'player_id','')::bigint,0) AS player_id,
                COALESCE(NULLIF(p->>'_private_slot','')::smallint,0) AS private_slot,
                CASE LOWER(COALESCE(NULLIF(p->>'source',''),'direct'))
                  WHEN 'direct' THEN 4 WHEN 'recovered' THEN 3 ELSE 1
                END AS priority
              FROM jsonb_array_elements($3::jsonb) AS payload(p)
            )
            DELETE FROM match_players existing
            USING incoming
            WHERE existing.match_id=$1
              AND existing.player_id=incoming.player_id
              AND existing.private_slot=incoming.private_slot
              AND existing.entry_datetime IS DISTINCT FROM $2::text::timestamptz
              AND CASE existing.source
                    WHEN 'direct' THEN 4 WHEN 'recovered' THEN 3 ELSE 1
                  END <= incoming.priority
            "#,
            &[&payload.match_id, &payload.entry_datetime, players],
        )
        .await?;
    persist_equipment(transaction, payload.match_id, players).await?;
    persist_account_merges(transaction, players).await?;
    Ok(())
}

async fn ensure_fact_references(
    transaction: &Transaction<'_>,
    players: &Value,
) -> Result<(), MatchFactError> {
    transaction
        .execute(
            r#"
            INSERT INTO champions (id,name,title,health,speed,roles)
            SELECT DISTINCT
              (p->>'champion_id')::int,
              COALESCE(NULLIF(p->>'champion_name',''),'Champion '||(p->>'champion_id')),
              'Reference placeholder from match ingest',0,0,'Unknown'
            FROM jsonb_array_elements($1::jsonb) AS facts(p)
            WHERE COALESCE(NULLIF(p->>'champion_id','')::int,0)>0
            ON CONFLICT (id) DO UPDATE SET
              name=CASE
                WHEN champions.title='Reference placeholder from match ingest'
                  AND EXCLUDED.name NOT LIKE 'Champion %'
                THEN EXCLUDED.name ELSE champions.name
              END
            "#,
            &[players],
        )
        .await?;
    transaction
        .execute(
            r#"
            WITH item_facts AS (
              SELECT
                NULLIF(p->>('active_id_'||slot),'')::int AS item_id,
                NULLIF(p->>('item_active_'||slot),'') AS item_name
              FROM jsonb_array_elements($1::jsonb) AS facts(p)
              CROSS JOIN generate_series(1,4) AS slots(slot)
            )
            INSERT INTO items (item_id,item_name,description,item_type,cost)
            SELECT DISTINCT
              item_id,COALESCE(item_name,'Item '||item_id),
              'Reference placeholder from match ingest','Match item placeholder',0
            FROM item_facts WHERE item_id>0
            ON CONFLICT (item_id) DO UPDATE SET
              item_name=CASE
                WHEN items.description='Reference placeholder from match ingest'
                  AND EXCLUDED.item_name NOT LIKE 'Item %'
                THEN EXCLUDED.item_name ELSE items.item_name
              END
            "#,
            &[players],
        )
        .await?;
    transaction
        .execute(
            r#"
            INSERT INTO talents (talent_id,talent_name,champion_id)
            SELECT DISTINCT
              (p->>'item_id_6')::int,
              COALESCE(NULLIF(p->>'item_purch_6',''),'Talent '||(p->>'item_id_6')),
              NULLIF(p->>'champion_id','')::int
            FROM jsonb_array_elements($1::jsonb) AS facts(p)
            WHERE COALESCE(NULLIF(p->>'item_id_6','')::int,0)>0
            ON CONFLICT (talent_id) DO UPDATE SET
              talent_name=CASE
                WHEN talents.talent_name LIKE 'Talent %'
                  AND EXCLUDED.talent_name NOT LIKE 'Talent %'
                THEN EXCLUDED.talent_name ELSE talents.talent_name
              END,
              champion_id=COALESCE(talents.champion_id,EXCLUDED.champion_id)
            "#,
            &[players],
        )
        .await?;
    Ok(())
}

async fn persist_equipment(
    transaction: &Transaction<'_>,
    match_id: i64,
    players: &Value,
) -> Result<(), MatchFactError> {
    transaction
        .execute(
            r#"
            INSERT INTO match_player_items (match_id,player_id,item_id,slot,item_level)
            SELECT
              $1,(p->>'player_id')::bigint,(p->>('active_id_'||slot))::int,slot,
              CASE
                WHEN COALESCE(NULLIF(p->>('active_level_'||slot),'')::int,0)>2
                  THEN FLOOR((p->>('active_level_'||slot))::numeric/4)::smallint
                ELSE COALESCE(NULLIF(p->>('active_level_'||slot),'')::smallint,0)
              END
            FROM jsonb_array_elements($2::jsonb) AS facts(p)
            CROSS JOIN generate_series(1,4) AS slots(slot)
            WHERE COALESCE(NULLIF(p->>'player_id','')::bigint,0)>0
              AND COALESCE(NULLIF(p->>('active_id_'||slot),'')::int,0)>0
            ON CONFLICT (match_id,player_id,item_id) DO NOTHING
            "#,
            &[&match_id, players],
        )
        .await?;
    transaction
        .execute(
            r#"
            INSERT INTO match_player_cards (match_id,player_id,card_id,card_level)
            SELECT
              $1,(p->>'player_id')::bigint,(p->>('item_id_'||slot))::int,
              COALESCE(NULLIF(p->>('item_level_'||slot),'')::smallint,0)
            FROM jsonb_array_elements($2::jsonb) AS facts(p)
            CROSS JOIN generate_series(1,5) AS slots(slot)
            WHERE COALESCE(NULLIF(p->>'player_id','')::bigint,0)>0
              AND COALESCE(NULLIF(p->>('item_id_'||slot),'')::int,0)>0
            ON CONFLICT (match_id,player_id,card_id) DO NOTHING
            "#,
            &[&match_id, players],
        )
        .await?;
    transaction
        .execute(
            r#"
            INSERT INTO match_player_talents (match_id,player_id,talent_id)
            SELECT
              $1,(p->>'player_id')::bigint,(p->>'item_id_6')::int
            FROM jsonb_array_elements($2::jsonb) AS facts(p)
            JOIN talents talent
              ON talent.talent_id=(p->>'item_id_6')::int
             AND talent.champion_id=NULLIF(p->>'champion_id','')::int
            WHERE COALESCE(NULLIF(p->>'player_id','')::bigint,0)>0
              AND COALESCE(NULLIF(p->>'item_id_6','')::int,0)>0
            ON CONFLICT (match_id,player_id,talent_id) DO NOTHING
            "#,
            &[&match_id, players],
        )
        .await?;
    Ok(())
}

async fn persist_account_merges(
    transaction: &Transaction<'_>,
    players: &Value,
) -> Result<(), MatchFactError> {
    transaction
        .execute(
            r#"
            INSERT INTO player_account_merges (
              player_id,merged_from_id,merged_from_portal,merge_datetime
            )
            SELECT
              (p->>'player_id')::bigint,
              COALESCE(
                NULLIF(merged->>'player_id','')::bigint,
                NULLIF(merged->>'playerId','')::bigint
              ),
              COALESCE(
                NULLIF(merged->>'portal_id','')::smallint,
                NULLIF(merged->>'portalId','')::smallint
              ),
              COALESCE(
                NULLIF(merged->>'merge_datetime','')::timestamptz,
                NULLIF(merged->>'mergeDatetime','')::timestamptz,
                now()
              )
            FROM jsonb_array_elements($1::jsonb) AS facts(p)
            CROSS JOIN LATERAL jsonb_array_elements(
              CASE
                WHEN jsonb_typeof(p->'merged_players')='array' THEN p->'merged_players'
                ELSE '[]'::jsonb
              END
            ) AS merges(merged)
            WHERE COALESCE(NULLIF(p->>'player_id','')::bigint,0)>0
              AND COALESCE(
                NULLIF(merged->>'player_id','')::bigint,
                NULLIF(merged->>'playerId','')::bigint,
                0
              )>0
            ON CONFLICT (player_id,merged_from_id) DO NOTHING
            "#,
            &[players],
        )
        .await?;
    Ok(())
}

async fn persist_bans(
    transaction: &Transaction<'_>,
    match_id: i64,
    bans: &Value,
) -> Result<(), MatchFactError> {
    transaction
        .execute(
            r#"
            INSERT INTO champions (id,name,title,health,speed,roles)
            SELECT DISTINCT
              (ban->>'champion_id')::int,
              'Champion '||(ban->>'champion_id'),
              'Reference placeholder from match ingest',0,0,'Unknown'
            FROM jsonb_array_elements($1::jsonb) AS entries(ban)
            ON CONFLICT (id) DO NOTHING
            "#,
            &[bans],
        )
        .await?;
    transaction
        .execute(
            r#"
            INSERT INTO match_bans (match_id,ban_slot,champion_id)
            SELECT
              $1,(ban->>'slot')::smallint,(ban->>'champion_id')::int
            FROM jsonb_array_elements($2::jsonb) AS entries(ban)
            ON CONFLICT (match_id,ban_slot) DO UPDATE
              SET champion_id=EXCLUDED.champion_id
            "#,
            &[&match_id, bans],
        )
        .await?;
    Ok(())
}

async fn add_stage(
    transaction: &Transaction<'_>,
    match_id: i64,
    stage: &str,
) -> Result<(), MatchFactError> {
    transaction
        .execute(
            r#"
            UPDATE match_ingest_status
            SET completed_stages=(
                  SELECT array_agg(DISTINCT stage_name ORDER BY stage_name)
                  FROM unnest(completed_stages||ARRAY[$2]::text[]) AS stages(stage_name)
                ),
                updated_at=now()
            WHERE match_id=$1
            "#,
            &[&match_id, &stage],
        )
        .await?;
    Ok(())
}

async fn close_hourly_match_debt(
    transaction: &Transaction<'_>,
    match_id: i64,
) -> Result<(), MatchFactError> {
    transaction
        .execute(
            r#"
            UPDATE hourly_ingest_match_debt
            SET status='complete',
                reason='canonical match facts durable',
                completed_at=COALESCE(completed_at,now()),
                next_retry_at=NULL,
                updated_at=now()
            WHERE match_id=$1
              AND status<>'unrecoverable'
            "#,
            &[&match_id],
        )
        .await?;
    Ok(())
}

fn unwrap_relay_match(value: Value) -> Option<Value> {
    match value {
        Value::Array(values) => values.into_iter().find_map(unwrap_relay_match),
        Value::Object(mut object) => {
            if let Some(nested) = object.remove("match")
                && nested.is_object()
            {
                return Some(nested);
            }
            object
                .contains_key("match_id")
                .then_some(Value::Object(object))
        }
        _ => None,
    }
}

fn valid_completed_score(team1: Option<i32>, team2: Option<i32>, winner: Option<i32>) -> bool {
    match (team1, team2, winner) {
        (Some(team1), Some(team2), Some(1)) => team1 >= 0 && team2 >= 0 && team1 > team2,
        (Some(team1), Some(team2), Some(2)) => team1 >= 0 && team2 >= 0 && team2 > team1,
        _ => false,
    }
}

fn validate_authoritative_players(players: &[Value], limited: bool) -> Result<(), MatchFactError> {
    if limited {
        return Ok(());
    }
    if players.len() != 10 {
        return Err(MatchFactError::InvalidPayload(format!(
            "complete match requires 10 logical players; received {}",
            players.len()
        )));
    }
    let team_one = players
        .iter()
        .filter(|player| player_i64(player, "task_force") == 1)
        .count();
    let team_two = players
        .iter()
        .filter(|player| player_i64(player, "task_force") == 2)
        .count();
    if team_one != 5 || team_two != 5 {
        return Err(MatchFactError::InvalidPayload(format!(
            "complete match requires a 5v5 roster; received {team_one}v{team_two}"
        )));
    }
    if players
        .iter()
        .filter(|player| !is_private_player(player))
        .any(|player| !is_detailed_player(player))
    {
        return Err(MatchFactError::InvalidPayload(
            "public player facts must be direct or recovered authoritative rows".to_owned(),
        ));
    }
    Ok(())
}

fn assign_private_slots(players: &mut [Value]) {
    let mut private = players
        .iter()
        .enumerate()
        .filter(|(_, player)| is_private_player(player))
        .map(|(index, player)| (private_sort_key(player), index))
        .collect::<Vec<_>>();
    private.sort_by(|left, right| left.0.cmp(&right.0));
    for (slot, (_, index)) in private.into_iter().enumerate() {
        if let Some(object) = players[index].as_object_mut() {
            object.insert("_private_slot".to_owned(), json!(slot + 1));
        }
    }
}

fn private_sort_key(player: &Value) -> String {
    [
        "task_force",
        "party_id",
        "champion_id",
        "account_level",
        "mastery_level",
        "league_tier",
        "league_points",
    ]
    .into_iter()
    .map(|name| player_i64(player, name).to_string())
    .chain([player_text(player, "portal_user_id")])
    .chain([
        player_i64(player, "kills").to_string(),
        player_i64(player, "deaths").to_string(),
        player_i64(player, "assists").to_string(),
        player_text(player, "source"),
    ])
    .collect::<Vec<_>>()
    .join(":")
}

fn is_private_player(player: &Value) -> bool {
    player_i64(player, "player_id") == 0
        && player_text(player, "player_name").eq_ignore_ascii_case(PRIVATE_ACCOUNT_NAME)
}

fn is_detailed_player(player: &Value) -> bool {
    let source = player_text(player, "source").to_ascii_lowercase();
    player_i64(player, "champion_id") > 0
        && matches!(source.as_str(), "direct" | "recovered")
        && matches!(player_i64(player, "task_force"), 1 | 2)
        && matches!(
            player_text(player, "win_status")
                .to_ascii_lowercase()
                .as_str(),
            "winner" | "win" | "loser" | "loss"
        )
}

fn player_i64(player: &Value, name: &str) -> i64 {
    positive_or_signed_i64(player.get(name)).unwrap_or_default()
}

fn positive_i64(value: Option<&Value>) -> Option<i64> {
    positive_or_signed_i64(value).filter(|number| *number > 0)
}

fn positive_or_signed_i64(value: Option<&Value>) -> Option<i64> {
    value.and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
            .or_else(|| value.as_str().and_then(|text| text.parse::<i64>().ok()))
    })
}

fn player_text(player: &Value, name: &str) -> String {
    player
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

fn player_bool(player: &Value, name: &str) -> bool {
    player.get(name).is_some_and(|value| {
        value.as_bool() == Some(true)
            || value.as_i64().is_some_and(|number| number != 0)
            || value.as_str().is_some_and(|text| {
                matches!(text.to_ascii_lowercase().as_str(), "true" | "1" | "y")
            })
    })
}

fn normalized_region(region: &str) -> String {
    let trimmed = region.trim();
    if trimmed.is_empty() {
        "Unknown".to_owned()
    } else {
        trimmed.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn player(id: i64, team: i64, source: &str) -> Value {
        json!({
            "player_id":id,
            "player_name":format!("Player {id}"),
            "champion_id":id+100,
            "task_force":team,
            "win_status":if team==1 {"Winner"} else {"Loser"},
            "source":source
        })
    }

    fn complete_match() -> Value {
        let players = (1..=10)
            .map(|id| player(id, if id <= 5 { 1 } else { 2 }, "direct"))
            .collect::<Vec<_>>();
        json!({
            "match_id":123,
            "entry_datetime":"2026-07-30T12:00:00Z",
            "map":"Stone Keep",
            "queue_id":486,
            "duration_seconds":900,
            "region":"NA",
            "team1_score":4,
            "team2_score":2,
            "winning_task_force":1,
            "players":players
        })
    }

    #[test]
    fn unwraps_direct_and_completed_resolution_contracts() {
        let direct =
            CanonicalMatchPayload::from_relay_value(complete_match()).expect("direct match");
        assert_eq!(direct.match_id, 123);
        let resolution = json!([{
            "matchId":123,
            "queueId":486,
            "status":"complete_direct",
            "match":complete_match()
        }]);
        let nested = CanonicalMatchPayload::from_relay_value(resolution).expect("resolution match");
        assert_eq!(nested.players.len(), 10);
    }

    #[test]
    fn rejects_incomplete_or_contradictory_public_facts() {
        let mut missing = complete_match();
        missing["players"] = json!([player(1, 1, "direct")]);
        let payload = CanonicalMatchPayload::from_relay_value(missing).expect("shell");
        assert!(payload.normalized_players().is_err());

        let mut bad_score = complete_match();
        bad_score["winning_task_force"] = json!(2);
        assert!(CanonicalMatchPayload::from_relay_value(bad_score).is_err());
    }

    #[test]
    fn private_slots_are_stable_and_do_not_collapse_zero_identities() {
        let mut players = vec![
            json!({"player_id":0,"player_name":"PRIVATEACCOUNT","task_force":2,"champion_id":22,"source":"direct"}),
            json!({"player_id":0,"player_name":"PRIVATEACCOUNT","task_force":1,"champion_id":11,"source":"direct"}),
        ];
        assign_private_slots(&mut players);
        assert_eq!(players[1]["_private_slot"], 1);
        assert_eq!(players[0]["_private_slot"], 2);
    }

    #[test]
    fn bans_use_first_positive_value_for_each_slot() {
        let mut payload = CanonicalMatchPayload::from_relay_value(complete_match()).expect("match");
        payload.extra.insert("BanId1".to_owned(), json!(17));
        payload.players[0]["ban_id_2"] = json!(29);
        assert_eq!(
            payload.ban_entries(),
            json!([
                {"slot":1,"champion_id":17},
                {"slot":2,"champion_id":29}
            ])
        );
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL and a disposable empty database"]
    async fn live_finalizer_is_idempotent_and_keeps_populations_isolated() {
        use paladinscat_core::config::BackendConfig;

        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("test database URL");
        let config = BackendConfig::from_lookup(|name| {
            (name == "DATABASE_URL").then(|| database_url.clone())
        })
        .expect("backend config");
        let database = Database::new(&config, "match-facts-test").expect("database");
        database
            .connection()
            .await
            .expect("connection")
            .batch_execute(include_str!(
                "../../../../dev/compat/backend-rust/package-c-match-facts-seed.sql"
            ))
            .await
            .expect("seed schema");
        let repository = MatchFactRepository::new(database.clone());

        let mut ranked = complete_match();
        ranked["ban_id_1"] = json!(777);
        for (index, player) in ranked["players"]
            .as_array_mut()
            .expect("players")
            .iter_mut()
            .enumerate()
        {
            player["damage_done_physical"] = json!(15_000 + index);
            player["damage_done_magical"] = Value::Null;
            player["gold_earned"] = json!(2_000);
            player["objective_assists"] = json!(20 + index);
            player["active_id_1"] = json!(2_001);
            player["active_level_1"] = json!(8);
            player["item_active_1"] = json!("Chronos");
            player["item_id_1"] = json!(3_001);
            player["item_level_1"] = json!(5);
            player["item_id_6"] = json!(4_001 + index);
            player["item_purch_6"] = json!("Talent");
        }
        let ranked = CanonicalMatchPayload::from_relay_value(ranked).expect("ranked payload");
        let first = repository
            .finalize(&ranked, "direct_lookup")
            .await
            .expect("first finalize");
        let second = repository
            .finalize(&ranked, "ranked_hourly")
            .await
            .expect("idempotent finalize");
        assert!(first.facts_written);
        assert!(!second.facts_written);
        assert_eq!(first.population, MatchPopulation::Ranked);

        let mut casual = complete_match();
        casual["match_id"] = json!(124);
        casual["queue_id"] = json!(424);
        for player in casual["players"].as_array_mut().expect("players") {
            player["active_id_1"] = json!(2_001);
            player["active_level_1"] = json!(4);
            player["item_active_1"] = json!("Chronos");
        }
        let casual = CanonicalMatchPayload::from_relay_value(casual).expect("casual payload");
        let casual_result = repository
            .finalize(&casual, "profile_history")
            .await
            .expect("casual finalize");
        assert_eq!(casual_result.population, MatchPopulation::Casual);

        let rows = database
            .query_json(
                r#"
                SELECT
                  (SELECT count(*)::int FROM matches) AS matches,
                  (SELECT count(*)::int FROM match_players) AS players,
                  (SELECT count(*)::int FROM match_player_items) AS items,
                  (SELECT count(*)::int FROM match_player_cards) AS cards,
                  (SELECT count(*)::int FROM match_player_talents) AS talents,
                  (SELECT count(*)::int FROM match_bans) AS bans,
                  (SELECT count(*)::int FROM item_counts_ranked) AS ranked_aggregates,
                  (SELECT count(*)::int FROM item_counts_casual) AS casual_aggregates,
                  (SELECT damage_per_minute FROM match_players
                    WHERE match_id=123 ORDER BY player_id LIMIT 1) AS dpm,
                  (SELECT objective_assists FROM match_players
                    WHERE match_id=123 ORDER BY player_id LIMIT 1) AS objective_time
                "#,
                &[],
            )
            .await
            .expect("fact summary");
        assert_eq!(
            rows,
            vec![json!({
                "matches":2,
                "players":20,
                "items":20,
                "cards":10,
                "talents":10,
                "bans":1,
                "ranked_aggregates":0,
                "casual_aggregates":0,
                "dpm":1000.0,
                "objective_time":20
            })]
        );
        let populations = database
            .query_json(
                "SELECT match_id::text,population,completed_stages FROM match_ingest_status ORDER BY match_id",
                &[],
            )
            .await
            .expect("populations");
        assert_eq!(populations[0]["population"], "ranked");
        assert_eq!(populations[1]["population"], "casual");
        for row in populations {
            let stages = row["completed_stages"].as_array().expect("stages");
            assert!(stages.iter().any(|stage| stage == "core"));
            assert!(stages.iter().any(|stage| stage == "player_facts"));
            assert!(stages.iter().any(|stage| stage == "match_bans"));
        }

        let private_ranked = |match_id: i64, entry_datetime: &str| {
            let mut value = complete_match();
            value["match_id"] = json!(match_id);
            value["entry_datetime"] = json!(entry_datetime);
            let players = value["players"].as_array_mut().expect("players");
            players[0]["party_id"] = json!(77);
            players[4] = json!({
                "player_id":0,
                "player_name":"PRIVATEACCOUNT",
                "champion_id":2205,
                "task_force":1,
                "win_status":"Winner",
                "source":"direct",
                "party_id":77,
                "account_level":320,
                "mastery_level":42,
                "league_tier":20,
                "league_points":65,
                "portal_id":1,
                "platform":"steam"
            });
            CanonicalMatchPayload::from_relay_value(value).expect("private ranked payload")
        };
        let first_private = private_ranked(130, "2026-07-30T13:00:00Z");
        repository
            .finalize(&first_private, "direct_lookup")
            .await
            .expect("create private identity");
        let second_private = private_ranked(131, "2026-07-30T14:00:00Z");
        repository
            .finalize(&second_private, "ranked_hourly")
            .await
            .expect("link private identity");
        repository
            .finalize(&second_private, "ranked_hourly_replay")
            .await
            .expect("replay linked identity");

        let casual_private = |match_id: i64, entry_datetime: &str| {
            let mut value = complete_match();
            value["match_id"] = json!(match_id);
            value["queue_id"] = json!(424);
            value["entry_datetime"] = json!(entry_datetime);
            value["players"].as_array_mut().expect("players")[4] = json!({
                "player_id":0,
                "player_name":"PRIVATEACCOUNT",
                "champion_id":9999,
                "task_force":1,
                "win_status":"Winner",
                "source":"direct",
                "party_id":909
            });
            CanonicalMatchPayload::from_relay_value(value).expect("private casual payload")
        };
        let first_casual_private = casual_private(140, "2026-07-30T15:00:00Z");
        repository
            .finalize(&first_casual_private, "profile_history")
            .await
            .expect("seed casual private identity");
        let second_casual_private = casual_private(141, "2026-07-30T16:00:00Z");
        repository
            .finalize(&second_casual_private, "casual_hourly")
            .await
            .expect("retain ambiguous casual observation");
        repository
            .finalize(&second_casual_private, "casual_hourly_replay")
            .await
            .expect("replay ambiguous casual observation");

        let private_summary = database
            .query_json(
                r#"
                SELECT
                  (SELECT count(*)::int FROM private_account_observations)
                    AS observations,
                  (SELECT count(*)::int FROM players_private) AS identities,
                  (SELECT count(*)::int FROM players_private_history) AS history,
                  (SELECT count(*)::int FROM private_player_presence_24h)
                    AS resolved_presence,
                  (SELECT count(*)::int FROM unresolved_private_presence)
                    AS unresolved_presence,
                  (SELECT count(DISTINCT private_player_id)::int
                    FROM match_players
                    WHERE match_id IN (130,131)
                      AND private_player_id IS NOT NULL) AS ranked_identities,
                  (SELECT resolution_status
                    FROM private_account_observations
                    WHERE match_id=141 AND private_slot=1) AS casual_resolution,
                  (SELECT private_player_id
                    FROM match_players
                    WHERE match_id=141 AND player_id=0 AND private_slot=1)
                    AS ambiguous_link
                "#,
                &[],
            )
            .await
            .expect("private identity summary");
        assert_eq!(
            private_summary,
            vec![json!({
                "observations":4,
                "identities":2,
                "history":3,
                "resolved_presence":2,
                "unresolved_presence":1,
                "ranked_identities":1,
                "casual_resolution":"ambiguous",
                "ambiguous_link":null
            })]
        );
    }
}
