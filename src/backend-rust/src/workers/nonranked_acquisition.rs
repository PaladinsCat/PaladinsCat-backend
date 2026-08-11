use std::collections::{BTreeMap, HashMap, HashSet};

use paladinscat_core::database::{Database, DatabaseError};
use serde_json::{Map, Value, json};
use time::{
    Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset,
    format_description::well_known::Rfc3339,
};
use tokio_postgres::Transaction;

use super::{
    match_facts::CanonicalMatchPayload,
    policy::{
        MatchCountQueueDefinition, MatchParticipantModel, MatchStatScope,
        get_match_queue_definition,
    },
    private_identity::{persist_and_resolve_private_identities, resolve_existing_private_match},
    profile_enrichment::{ProfileEnrichmentError, persist_player_profile_in_transaction},
};

const PRIVATE_NAME: &str = "PRIVATEACCOUNT";

#[derive(Clone, Debug)]
pub(super) struct NonrankedAcquisitionClaim {
    pub match_id: i64,
    pub queue_id: i32,
    pub source_date: String,
    pub source_hour: i32,
    pub region: Option<String>,
    pub discovered_entry_datetime: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HybridState {
    CompleteDirect,
    PartialRoster,
    RosterOnly,
    Dropped,
}

impl HybridState {
    fn database(self) -> &'static str {
        match self {
            Self::CompleteDirect => "complete_direct",
            Self::PartialRoster => "partial_roster",
            Self::RosterOnly => "roster_only",
            Self::Dropped => "dropped",
        }
    }
}

#[derive(Clone, Debug)]
struct HybridResult {
    match_id: i64,
    detail: Option<Value>,
    roster: Vec<Value>,
    state: HybridState,
    terminal_reason: Option<String>,
}

#[derive(Clone, Debug)]
struct PlayerFact {
    player_id: i64,
    player_name: Option<String>,
    champion_id: i32,
    champion_name: Option<String>,
    task_force: i32,
    win_status: Option<String>,
    kills: i32,
    deaths: i32,
    assists: i32,
    damage_done_in_hand: Option<i32>,
    damage: i32,
    damage_taken: i32,
    healing: i32,
    mitigation: i32,
    credits: i32,
    objective_time: i32,
    account_level: i32,
    mastery_level: i32,
    party_id: i64,
    portal_id: i32,
    portal_user_id: Option<String>,
    platform: Option<String>,
    kind: &'static str,
    source: String,
}

#[derive(Clone)]
pub(super) struct NonrankedAcquisitionRepository {
    database: Database,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum NonrankedPersistenceError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Query(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Profile(#[from] ProfileEnrichmentError),
}

impl NonrankedAcquisitionRepository {
    pub(super) fn new(database: Database) -> Self {
        Self { database }
    }

    pub(super) async fn persist(
        &self,
        claim: &NonrankedAcquisitionClaim,
        outcome: Value,
    ) -> Result<(), NonrankedPersistenceError> {
        let result = hybrid_result(outcome);
        let definition = get_match_queue_definition(claim.queue_id);
        let complete = result.state == HybridState::CompleteDirect
            && is_complete_nonranked(result.detail.as_ref(), definition.participant_model);
        let players = merged_players(result.detail.as_ref(), &result.roster);
        let entry = match_entry(result.detail.as_ref(), claim);
        let region = text_alias(result.detail.as_ref(), &["region"])
            .or_else(|| claim.region.clone())
            .unwrap_or_else(|| "Unknown".to_owned());
        let map =
            text_alias(result.detail.as_ref(), &["map"]).unwrap_or_else(|| "Unknown".to_owned());
        let duration = int_alias(result.detail.as_ref(), &["duration_seconds"]).max(0) as i32;
        let quality = if complete {
            "complete"
        } else if !players.is_empty() {
            if result.detail.is_some() {
                "partial"
            } else {
                "limited"
            }
        } else {
            "unavailable"
        };
        let stats_eligible = complete && definition.stats_enabled;
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;

        persist_new_roster_profiles(&transaction, &result.roster).await?;
        if !players.is_empty() {
            write_match(
                &transaction,
                claim,
                &result,
                definition,
                &entry,
                &region,
                &map,
                duration,
                quality,
                stats_eligible,
                players.len(),
                complete,
            )
            .await?;
            write_player_facts(
                &transaction,
                result.match_id,
                &players,
                definition,
                complete,
            )
            .await?;
            write_presence(&transaction, result.match_id, &entry, definition, &players).await?;
            if stats_eligible {
                replace_projection(
                    &transaction,
                    result.match_id,
                    definition,
                    &entry,
                    &region,
                    &map,
                    duration,
                    &players,
                )
                .await?;
            }
        }
        let direct_count =
            i32::try_from(usable_players(result.detail.as_ref()).len()).unwrap_or(i32::MAX);
        let roster_count = i32::try_from(result.roster.len()).unwrap_or(i32::MAX);
        let attempted_roster = result.state != HybridState::CompleteDirect;
        transaction.execute(
            "UPDATE nonranked_match_acquisition SET status=$2,quality=$3,direct_player_count=$4,\
             roster_player_count=$5,roster_attempts=CASE WHEN $6 THEN roster_attempts+1 ELSE roster_attempts END,\
             terminal_reason=$7,error_message=NULL,lease_until=NULL,completed_at=now(),updated_at=now() WHERE match_id=$1",
            &[&result.match_id, &result.state.database(), &quality, &(direct_count as i16), &(roster_count as i16),
              &attempted_roster, &result.terminal_reason],
        ).await?;
        transaction.commit().await?;
        if let Err(error) = self
            .persist_private_identities_post_commit(
                claim, &result, definition, &entry, &map, duration, quality, &players,
            )
            .await
        {
            tracing::warn!(match_id=result.match_id, %error, "non-ranked private identity enrichment failed after durable acquisition");
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn persist_private_identities_post_commit(
        &self,
        claim: &NonrankedAcquisitionClaim,
        result: &HybridResult,
        definition: MatchCountQueueDefinition,
        entry: &str,
        map: &str,
        duration: i32,
        quality: &str,
        players: &[Value],
    ) -> Result<(), DatabaseError> {
        let mut private_slot = 0i64;
        let observations = players.iter().filter_map(|raw| {
            let direct = !text_alias(Some(raw), &["source"]).is_some_and(|source| source.eq_ignore_ascii_case("roster"));
            let fact = player_fact(raw, definition, direct);
            if fact.kind != "private" { return None; }
            private_slot += 1;
            Some(json!({
                "player_id": fact.player_id, "player_name": fact.player_name,
                "party_id": fact.party_id, "account_level": fact.account_level,
                "mastery_level": fact.mastery_level,
                "league_tier": int_alias(Some(raw), &["league_tier","League_Tier","Tier"]),
                "league_points": int_alias(Some(raw), &["league_points","League_Points","Points"]),
                "champion_id": fact.champion_id, "task_force": fact.task_force,
                "win_status": fact.win_status, "portal_id": fact.portal_id,
                "portal_user_id": fact.portal_user_id, "platform": fact.platform,
                "source": fact.source, "_private_slot": private_slot,
            }))
        }).collect::<Vec<_>>();
        if observations.is_empty() {
            return Ok(());
        }
        let payload = CanonicalMatchPayload {
            match_id: result.match_id,
            entry_datetime: entry.to_owned(),
            map: map.to_owned(),
            queue_id: claim.queue_id,
            duration_seconds: duration,
            region: claim.region.clone().unwrap_or_else(|| "Unknown".to_owned()),
            team1_score: None,
            team2_score: None,
            winning_task_force: None,
            has_replay: None,
            recovery_source: None,
            recovery_api_calls: None,
            limited: Some(quality != "complete"),
            players: observations.clone(),
            extra: BTreeMap::new(),
        };
        let mut client = self.database.connection().await?;
        let transaction = client.transaction().await?;
        persist_and_resolve_private_identities(
            &transaction,
            &payload,
            &Value::Array(observations),
            quality != "complete",
        )
        .await?;
        transaction.commit().await?;
        resolve_existing_private_match(&self.database, result.match_id).await?;
        Ok(())
    }
}

fn hybrid_result(outcome: Value) -> HybridResult {
    let match_id = int_alias(Some(&outcome), &["matchId", "match_id"]);
    let detail = outcome
        .get("match")
        .filter(|value| value.is_object())
        .cloned();
    let roster = outcome
        .get("roster")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let has_detail = !usable_players(detail.as_ref()).is_empty();
    let has_roster = !roster.is_empty();
    let status = outcome
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or("dropped");
    let state = match status {
        "complete_direct" | "complete_recovered" => HybridState::CompleteDirect,
        "limited" if has_detail => HybridState::PartialRoster,
        "limited" if has_roster => HybridState::RosterOnly,
        "roster_only" if has_roster => HybridState::RosterOnly,
        _ => HybridState::Dropped,
    };
    let terminal_reason = if state == HybridState::CompleteDirect {
        None
    } else {
        outcome
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_owned)
            .or_else(|| {
                Some(
                    if state == HybridState::Dropped {
                        "single_pass_no_match_facts"
                    } else {
                        "single_pass_presence_only"
                    }
                    .to_owned(),
                )
            })
    };
    HybridResult {
        match_id,
        detail,
        roster,
        state,
        terminal_reason,
    }
}

fn usable_players(detail: Option<&Value>) -> Vec<Value> {
    detail
        .and_then(|value| value.get("players"))
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter(|row| {
                    !row.get("has_ret_msg")
                        .and_then(Value::as_bool)
                        .unwrap_or(false)
                        && text_alias(Some(row), &["ret_msg"]).is_none()
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

fn is_complete_nonranked(detail: Option<&Value>, model: MatchParticipantModel) -> bool {
    let players = usable_players(detail);
    if matches!(
        model,
        MatchParticipantModel::Pve | MatchParticipantModel::Bots
    ) {
        return !players.is_empty();
    }
    detail.is_some() && players.len() == 10
}

fn merged_players(detail: Option<&Value>, roster: &[Value]) -> Vec<Value> {
    let mut merged = usable_players(detail);
    let mut ids = HashSet::new();
    let mut names = HashSet::new();
    for player in &merged {
        let id = int_alias(
            Some(player),
            &["player_id", "playerId", "Id", "ActivePlayerId"],
        );
        if id > 0 {
            ids.insert(id);
        }
        if let Some(name) = text_alias(
            Some(player),
            &[
                "player_name",
                "playerName",
                "Name",
                "hz_player_name",
                "hz_gamer_tag",
            ],
        ) {
            names.insert(name.to_ascii_lowercase());
        }
    }
    for profile in roster {
        let id = int_alias(
            Some(profile),
            &["player_id", "playerId", "Id", "ActivePlayerId"],
        );
        let name = text_alias(
            Some(profile),
            &[
                "player_name",
                "playerName",
                "Name",
                "hz_player_name",
                "hz_gamer_tag",
            ],
        );
        if (id > 0 && ids.contains(&id))
            || name
                .as_ref()
                .is_some_and(|name| names.contains(&name.to_ascii_lowercase()))
        {
            continue;
        }
        let mut profile = profile.clone();
        if let Some(object) = profile.as_object_mut() {
            object.insert("source".to_owned(), Value::String("roster".to_owned()));
        }
        merged.push(profile);
    }
    merged
}

fn match_entry(detail: Option<&Value>, claim: &NonrankedAcquisitionClaim) -> String {
    let fallback = format!("{}T{:02}:00:00.000Z", claim.source_date, claim.source_hour);
    let candidate = text_alias(detail, &["entry_datetime"])
        .or_else(|| claim.discovered_entry_datetime.clone())
        .unwrap_or_else(|| fallback.clone());
    normalize_provider_datetime(&candidate).unwrap_or(fallback)
}

fn normalize_provider_datetime(value: &str) -> Option<String> {
    if let Ok(parsed) = OffsetDateTime::parse(value, &Rfc3339) {
        return parsed.format(&Rfc3339).ok();
    }
    let mut fields = value.split_whitespace();
    let date = fields.next()?.split('/').collect::<Vec<_>>();
    let clock = fields.next()?.split(':').collect::<Vec<_>>();
    let period = fields.next()?.to_ascii_uppercase();
    if date.len() != 3 || clock.len() != 3 || fields.next().is_some() {
        return None;
    }
    let month = Month::try_from(date[0].parse::<u8>().ok()?).ok()?;
    let day = date[1].parse().ok()?;
    let year = date[2].parse().ok()?;
    let mut hour = clock[0].parse::<u8>().ok()?;
    if hour == 12 {
        hour = 0;
    }
    if period == "PM" {
        hour = hour.checked_add(12)?;
    } else if period != "AM" {
        return None;
    }
    let minute = clock[1].parse().ok()?;
    let second = clock[2].parse().ok()?;
    PrimitiveDateTime::new(
        Date::from_calendar_date(year, month, day).ok()?,
        Time::from_hms(hour, minute, second).ok()?,
    )
    .assume_offset(UtcOffset::UTC)
    .format(&Rfc3339)
    .ok()
}

fn player_fact(player: &Value, definition: MatchCountQueueDefinition, direct: bool) -> PlayerFact {
    let player_id = int_alias(
        Some(player),
        &["player_id", "playerId", "Id", "ActivePlayerId"],
    );
    let player_name = text_alias(
        Some(player),
        &[
            "player_name",
            "playerName",
            "Name",
            "hz_player_name",
            "hz_gamer_tag",
        ],
    );
    let kind = if player_name
        .as_deref()
        .is_some_and(|name| name.eq_ignore_ascii_case(PRIVATE_NAME))
    {
        "private"
    } else if player_id > 0 {
        "human"
    } else if definition.participant_model == MatchParticipantModel::Bots
        || player_name
            .as_deref()
            .is_some_and(|name| name.to_ascii_lowercase().contains("bot"))
    {
        "bot"
    } else {
        "unknown"
    };
    let i32_field =
        |keys: &[&str]| i32::try_from(int_alias(Some(player), keys)).unwrap_or_default();
    PlayerFact {
        player_id,
        player_name,
        champion_id: i32_field(&["champion_id", "ChampionId", "Champion_ID"]),
        champion_name: text_alias(
            Some(player),
            &["champion_name", "Reference_Name", "ChampionName"],
        ),
        task_force: i32_field(&["task_force", "TaskForce"]),
        win_status: text_alias(Some(player), &["win_status", "Win_Status"]),
        kills: i32_field(&["kills", "Kills_Player", "Kills"]),
        deaths: i32_field(&["deaths", "Deaths"]),
        assists: i32_field(&["assists", "Assists"]),
        damage_done_in_hand: optional_i32(
            Some(player),
            &["damage_done_in_hand", "Damage_Done_In_Hand"],
        ),
        damage: i32_field(&[
            "Damage_Player",
            "damage_done_physical",
            "Damage",
            "Damage_Done_Physical",
        ]),
        damage_taken: i32_field(&["damage_taken", "Damage_Taken"]),
        healing: i32_field(&["healing", "Healing"]),
        mitigation: i32_field(&["damage_mitigated", "Damage_Mitigated"]),
        credits: i32_field(&["gold_earned", "Gold_Earned", "Gold"]),
        objective_time: i32_field(&[
            "objective_time",
            "objective_assists",
            "Objective_Assists",
            "Objective_Time",
        ]),
        account_level: i32_field(&["account_level", "Account_Level", "Level"]),
        mastery_level: i32_field(&["mastery_level", "Mastery_Level"]),
        party_id: int_alias(Some(player), &["party_id", "PartyId"]),
        portal_id: i32_field(&["portal_id", "PortalId", "Portal_ID"]),
        portal_user_id: text_alias(
            Some(player),
            &["portal_user_id", "PortalUserId", "hz_player_name"],
        ),
        platform: text_alias(Some(player), &["platform", "Platform"]),
        kind,
        source: if direct {
            text_alias(Some(player), &["source"]).unwrap_or_else(|| "direct".to_owned())
        } else {
            "roster".to_owned()
        },
    }
}

async fn persist_new_roster_profiles(
    transaction: &Transaction<'_>,
    roster: &[Value],
) -> Result<(), ProfileEnrichmentError> {
    if roster.is_empty() {
        return Ok(());
    }
    let ids = roster
        .iter()
        .flat_map(|row| {
            [
                int_alias(Some(row), &["Id", "player_id"]),
                int_alias(Some(row), &["ActivePlayerId"]),
            ]
        })
        .filter(|id| *id > 0)
        .collect::<Vec<_>>();
    if ids.is_empty() {
        return Ok(());
    }
    let existing = transaction.query("SELECT id FROM players WHERE id=ANY($1::bigint[]) OR active_player_id=ANY($1::bigint[])", &[&ids]).await?;
    let mut known = existing
        .into_iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<HashSet<_>>();
    for profile in roster {
        let id = int_alias(Some(profile), &["Id", "player_id"]);
        let active = int_alias(Some(profile), &["ActivePlayerId", "Id"]);
        if id <= 0 || known.contains(&id) || (active > 0 && known.contains(&active)) {
            continue;
        }
        persist_player_profile_in_transaction(transaction, profile).await?;
        known.insert(id);
        known.insert(active.max(id));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_match(
    transaction: &Transaction<'_>,
    claim: &NonrankedAcquisitionClaim,
    result: &HybridResult,
    definition: MatchCountQueueDefinition,
    entry: &str,
    region: &str,
    map: &str,
    duration: i32,
    quality: &str,
    stats_eligible: bool,
    player_count: usize,
    complete: bool,
) -> Result<(), tokio_postgres::Error> {
    let team1 = optional_i32(result.detail.as_ref(), &["team1_score"]);
    let team2 = optional_i32(result.detail.as_ref(), &["team2_score"]);
    let winning = optional_i32(result.detail.as_ref(), &["winning_task_force"]);
    let source = if complete {
        if result
            .detail
            .as_ref()
            .and_then(|v| v.get("recovery_attempted"))
            .and_then(Value::as_bool)
            == Some(true)
        {
            "relay_recovered"
        } else {
            "direct"
        }
    } else {
        result.state.database()
    };
    let count = i16::try_from(player_count).unwrap_or(i16::MAX);
    let winning_task_force = winning.map(|v| v as i16);
    if definition.scope == MatchStatScope::Casual {
        transaction.execute("INSERT INTO casual_matches(match_id,queue_id,entry_datetime,region,map,duration_seconds,team1_score,team2_score,winning_task_force,quality,stats_eligible,player_count,source,raw_match)\
          VALUES($1,$2,$3::text::timestamptz,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NULL) ON CONFLICT(match_id) DO UPDATE SET entry_datetime=EXCLUDED.entry_datetime,region=EXCLUDED.region,map=EXCLUDED.map,duration_seconds=EXCLUDED.duration_seconds,team1_score=EXCLUDED.team1_score,team2_score=EXCLUDED.team2_score,winning_task_force=EXCLUDED.winning_task_force,quality=EXCLUDED.quality,stats_eligible=EXCLUDED.stats_eligible,player_count=EXCLUDED.player_count,source=EXCLUDED.source,raw_match=NULL,updated_at=now()",
          &[&result.match_id,&claim.queue_id,&entry,&region,&map,&duration,&team1,&team2,&winning_task_force,&quality,&stats_eligible,&count,&source]).await?;
    } else {
        let scope = scope_name(definition.scope);
        let model = model_name(definition.participant_model);
        transaction.execute("INSERT INTO special_matches(match_id,queue_id,stats_scope,participant_model,entry_datetime,region,map,duration_seconds,team1_score,team2_score,winning_task_force,quality,stats_eligible,player_count,source,raw_match)\
          VALUES($1,$2,$3,$4,$5::text::timestamptz,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NULL) ON CONFLICT(match_id) DO UPDATE SET queue_id=EXCLUDED.queue_id,stats_scope=EXCLUDED.stats_scope,participant_model=EXCLUDED.participant_model,entry_datetime=EXCLUDED.entry_datetime,region=EXCLUDED.region,map=EXCLUDED.map,duration_seconds=EXCLUDED.duration_seconds,team1_score=EXCLUDED.team1_score,team2_score=EXCLUDED.team2_score,winning_task_force=EXCLUDED.winning_task_force,quality=EXCLUDED.quality,stats_eligible=EXCLUDED.stats_eligible,player_count=EXCLUDED.player_count,source=EXCLUDED.source,raw_match=NULL,updated_at=now()",
          &[&result.match_id,&claim.queue_id,&scope,&model,&entry,&region,&map,&duration,&team1,&team2,&winning_task_force,&quality,&stats_eligible,&count,&source]).await?;
    }
    Ok(())
}

async fn write_player_facts(
    transaction: &Transaction<'_>,
    match_id: i64,
    players: &[Value],
    definition: MatchCountQueueDefinition,
    complete: bool,
) -> Result<(), tokio_postgres::Error> {
    let table = if definition.scope == MatchStatScope::Casual {
        "casual_match_players"
    } else {
        "special_match_players"
    };
    transaction
        .execute(
            &format!("DELETE FROM {table} WHERE match_id=$1"),
            &[&match_id],
        )
        .await?;
    let mut private_slot = 0i16;
    for (index, raw) in players.iter().enumerate() {
        let direct = !text_alias(Some(raw), &["source"])
            .is_some_and(|value| value.eq_ignore_ascii_case("roster"));
        let fact = player_fact(raw, definition, direct);
        if fact.kind == "private" {
            private_slot += 1;
        }
        let roster_slot = i16::try_from(index + 1).unwrap_or(i16::MAX);
        let private = if fact.kind == "private" {
            private_slot
        } else {
            0
        };
        let champion = (fact.champion_id > 0).then_some(fact.champion_id);
        let eligible = complete && fact.kind == "human" && fact.champion_id > 0;
        let raw_player = compact_raw_player(raw);
        transaction.execute(&format!("INSERT INTO {table}(match_id,roster_slot,private_slot,player_id,player_name,champion_id,champion_name,task_force,win_status,kills,deaths,assists,damage_done_in_hand,damage,damage_taken,healing,mitigation,credits,objective_time,account_level,mastery_level,party_id,portal_id,portal_user_id,platform,participant_kind,source,stats_eligible,raw_player) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29::jsonb)"),
          &[&match_id,&roster_slot,&private,&fact.player_id,&fact.player_name,&champion,&fact.champion_name,&(fact.task_force as i16),&fact.win_status,&fact.kills,&fact.deaths,&fact.assists,&fact.damage_done_in_hand,&fact.damage,&fact.damage_taken,&fact.healing,&fact.mitigation,&fact.credits,&fact.objective_time,&fact.account_level,&fact.mastery_level,&(fact.party_id as i32),&fact.portal_id,&fact.portal_user_id,&fact.platform,&fact.kind,&fact.source,&eligible,&raw_player]).await?;
    }
    Ok(())
}

async fn write_presence(
    transaction: &Transaction<'_>,
    match_id: i64,
    entry: &str,
    definition: MatchCountQueueDefinition,
    players: &[Value],
) -> Result<(), tokio_postgres::Error> {
    if !definition.track_presence {
        return Ok(());
    }
    let mut facts = HashMap::new();
    for raw in players {
        let fact = player_fact(raw, definition, true);
        if fact.kind == "human" && fact.player_id > 0 {
            facts.entry(fact.player_id).or_insert(fact);
        }
    }
    let mut ids = facts.keys().copied().collect::<Vec<_>>();
    ids.sort_unstable();
    let scope = scope_name(definition.scope);
    for id in ids {
        transaction.execute("INSERT INTO player_presence_24h(player_id,first_observed_at,last_observed_at,last_match_id,last_queue_id,last_stats_scope) VALUES($1,$2::text::timestamptz,$2::text::timestamptz,$3,$4,$5) ON CONFLICT(player_id) DO UPDATE SET first_observed_at=LEAST(player_presence_24h.first_observed_at,EXCLUDED.first_observed_at),last_observed_at=GREATEST(player_presence_24h.last_observed_at,EXCLUDED.last_observed_at),last_match_id=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_match_id ELSE player_presence_24h.last_match_id END,last_queue_id=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_queue_id ELSE player_presence_24h.last_queue_id END,last_stats_scope=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_stats_scope ELSE player_presence_24h.last_stats_scope END,updated_at=now()", &[&id,&entry,&match_id,&definition.queue_id,&scope]).await?;
        transaction.execute("INSERT INTO player_queue_presence_24h(player_id,queue_id,stats_scope,first_observed_at,last_observed_at,last_match_id) VALUES($1,$2,$3,$4::text::timestamptz,$4::text::timestamptz,$5) ON CONFLICT(player_id,queue_id) DO UPDATE SET first_observed_at=LEAST(player_queue_presence_24h.first_observed_at,EXCLUDED.first_observed_at),last_observed_at=GREATEST(player_queue_presence_24h.last_observed_at,EXCLUDED.last_observed_at),last_match_id=CASE WHEN EXCLUDED.last_observed_at>=player_queue_presence_24h.last_observed_at THEN EXCLUDED.last_match_id ELSE player_queue_presence_24h.last_match_id END,stats_scope=EXCLUDED.stats_scope,updated_at=now()", &[&id,&definition.queue_id,&scope,&entry,&match_id]).await?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn replace_projection(
    transaction: &Transaction<'_>,
    match_id: i64,
    definition: MatchCountQueueDefinition,
    entry: &str,
    region: &str,
    map: &str,
    duration: i32,
    players: &[Value],
) -> Result<(), tokio_postgres::Error> {
    let ledger = transaction.query_one("SELECT stats_projected_at::text FROM nonranked_match_acquisition WHERE match_id=$1 FOR UPDATE", &[&match_id]).await?;
    if ledger.get::<_, Option<String>>(0).is_some() {
        return Ok(());
    }
    let scope = scope_name(definition.scope);
    transaction.execute("INSERT INTO nonranked_map_stats_daily(stats_date,stats_scope,queue_id,region,map,matches,duration_sum) VALUES($1::text::timestamptz::date,$2,$3,$4,$5,1,$6) ON CONFLICT(stats_date,stats_scope,queue_id,region,map) DO UPDATE SET matches=nonranked_map_stats_daily.matches+1,duration_sum=nonranked_map_stats_daily.duration_sum+EXCLUDED.duration_sum,updated_at=now()", &[&entry,&scope,&definition.queue_id,&region,&map,&(duration as i64)]).await?;
    for raw in players {
        let fact = player_fact(raw, definition, true);
        if fact.kind != "human" || fact.champion_id <= 0 {
            continue;
        }
        let win = fact
            .win_status
            .as_deref()
            .is_some_and(|v| v.eq_ignore_ascii_case("winner") || v.eq_ignore_ascii_case("win"));
        let loss = fact
            .win_status
            .as_deref()
            .is_some_and(|v| v.eq_ignore_ascii_case("loser") || v.eq_ignore_ascii_case("loss"));
        transaction.execute("INSERT INTO nonranked_champion_stats_daily(stats_date,stats_scope,queue_id,region,map,champion_id,plays,wins,losses,kills_sum,deaths_sum,assists_sum,damage_sum,healing_sum,mitigation_sum,credits_sum,duration_sum) VALUES($1::text::timestamptz::date,$2,$3,$4,$5,$6,1,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT(stats_date,stats_scope,queue_id,region,map,champion_id) DO UPDATE SET plays=nonranked_champion_stats_daily.plays+1,wins=nonranked_champion_stats_daily.wins+EXCLUDED.wins,losses=nonranked_champion_stats_daily.losses+EXCLUDED.losses,kills_sum=nonranked_champion_stats_daily.kills_sum+EXCLUDED.kills_sum,deaths_sum=nonranked_champion_stats_daily.deaths_sum+EXCLUDED.deaths_sum,assists_sum=nonranked_champion_stats_daily.assists_sum+EXCLUDED.assists_sum,damage_sum=nonranked_champion_stats_daily.damage_sum+EXCLUDED.damage_sum,healing_sum=nonranked_champion_stats_daily.healing_sum+EXCLUDED.healing_sum,mitigation_sum=nonranked_champion_stats_daily.mitigation_sum+EXCLUDED.mitigation_sum,credits_sum=nonranked_champion_stats_daily.credits_sum+EXCLUDED.credits_sum,duration_sum=nonranked_champion_stats_daily.duration_sum+EXCLUDED.duration_sum,updated_at=now()", &[&entry,&scope,&definition.queue_id,&region,&map,&fact.champion_id,&i64::from(i32::from(win)),&i64::from(i32::from(loss)),&(fact.kills as i64),&(fact.deaths as i64),&(fact.assists as i64),&(fact.damage as i64),&(fact.healing as i64),&(fact.mitigation as i64),&(fact.credits as i64),&(duration as i64)]).await?;
    }
    transaction.execute("UPDATE nonranked_match_acquisition SET stats_projected_at=now(),updated_at=now() WHERE match_id=$1", &[&match_id]).await?;
    Ok(())
}

fn compact_raw_player(raw: &Value) -> Value {
    const KEYS: &[&str] = &[
        "active_id_1",
        "item_active_1",
        "active_level_1",
        "active_id_2",
        "item_active_2",
        "active_level_2",
        "active_id_3",
        "item_active_3",
        "active_level_3",
        "active_id_4",
        "item_active_4",
        "active_level_4",
        "item_id_1",
        "item_purch_1",
        "item_level_1",
        "item_id_2",
        "item_purch_2",
        "item_level_2",
        "item_id_3",
        "item_purch_3",
        "item_level_3",
        "item_id_4",
        "item_purch_4",
        "item_level_4",
        "item_id_5",
        "item_purch_5",
        "item_level_5",
        "item_id_6",
        "item_purch_6",
    ];
    let mut compact = Map::new();
    compact.insert(
        "_storage".to_owned(),
        Value::String("compact-equipment-v1".to_owned()),
    );
    if let Some(source) = raw.as_object() {
        for key in KEYS {
            if let Some(value) = source.get(*key).filter(|v| !v.is_null()) {
                compact.insert((*key).to_owned(), value.clone());
            }
        }
    }
    Value::Object(compact)
}

fn text_alias(value: Option<&Value>, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value?.get(*key))
        .and_then(|value| match value {
            Value::String(v) => Some(v.replace('\0', "").replace("\\u0000", "").trim().to_owned()),
            Value::Null => None,
            other => Some(other.to_string()),
        })
        .filter(|v| !v.is_empty())
}
fn int_alias(value: Option<&Value>, keys: &[&str]) -> i64 {
    keys.iter()
        .find_map(|key| value?.get(*key))
        .and_then(finite_int)
        .unwrap_or_default()
}
fn finite_int(value: &Value) -> Option<i64> {
    if let Some(value) = value.as_i64() {
        return Some(value);
    }
    if let Some(value) = value.as_u64() {
        return i64::try_from(value).ok();
    }
    let number = value
        .as_f64()
        .or_else(|| value.as_str().and_then(|value| value.trim().parse().ok()))
        .or_else(|| value.as_bool().map(|value| if value { 1.0 } else { 0.0 }))?;
    (number.is_finite() && number >= i64::MIN as f64 && number <= i64::MAX as f64)
        .then_some(number.trunc() as i64)
}
fn optional_i32(value: Option<&Value>, keys: &[&str]) -> Option<i32> {
    keys.iter()
        .find_map(|key| value?.get(*key))
        .and_then(finite_int)
        .and_then(|value| i32::try_from(value).ok())
}
fn scope_name(scope: MatchStatScope) -> &'static str {
    match scope {
        MatchStatScope::Ranked => "ranked",
        MatchStatScope::Casual => "casual",
        MatchStatScope::Bot => "bot",
        MatchStatScope::TeamDeathmatch => "team_deathmatch",
        MatchStatScope::Arcade => "arcade",
        MatchStatScope::WaveDefense => "wave_defense",
        MatchStatScope::Experiment => "experiment",
        MatchStatScope::Newcomer => "newcomer",
        MatchStatScope::Custom => "custom",
        MatchStatScope::Other => "other",
    }
}
fn model_name(model: MatchParticipantModel) -> &'static str {
    match model {
        MatchParticipantModel::Pvp => "pvp",
        MatchParticipantModel::Pve => "pve",
        MatchParticipantModel::Bots => "bots",
        MatchParticipantModel::Custom => "custom",
        MatchParticipantModel::Unknown => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn maps_every_ts_terminal_outcome_class() {
        let cases = [
            ("complete_direct", true, true, HybridState::CompleteDirect),
            ("limited", true, true, HybridState::PartialRoster),
            ("limited", false, true, HybridState::RosterOnly),
            ("roster_only", false, true, HybridState::RosterOnly),
            ("dropped", false, false, HybridState::Dropped),
        ];
        for (status, detail, roster, expected) in cases {
            let value = json!({"matchId":1,"status":status,"match":detail.then(||json!({"players":[{"player_id":1}]})),"roster":roster.then(||vec![json!({"Id":2})])});
            assert_eq!(hybrid_result(value).state, expected);
        }
    }

    #[test]
    fn roster_merge_and_presence_are_identity_deduplicated() {
        let detail = json!({"players":[{"player_id":2,"player_name":"A"}]});
        let roster = vec![json!({"Id":2,"Name":"A"}), json!({"Id":3,"Name":"B"})];
        let merged = merged_players(Some(&detail), &roster);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[1]["source"], "roster");
    }

    #[test]
    fn pve_and_bots_do_not_require_ten_vendor_rows() {
        let detail = json!({"players":[{"player_id":2}]});
        assert!(is_complete_nonranked(
            Some(&detail),
            MatchParticipantModel::Pve
        ));
        assert!(is_complete_nonranked(
            Some(&detail),
            MatchParticipantModel::Bots
        ));
        assert!(!is_complete_nonranked(
            Some(&detail),
            MatchParticipantModel::Pvp
        ));
    }

    #[test]
    fn typescript_numeric_and_timestamp_edges_are_preserved() {
        assert_eq!(int_alias(Some(&json!({"v":"12.9"})), &["v"]), 12);
        assert_eq!(optional_i32(Some(&json!({"v":0})), &["v"]), Some(0));
        assert!(normalize_provider_datetime("not-a-date").is_none());
        assert_eq!(
            normalize_provider_datetime("8/2/2026 1:02:03 AM").as_deref(),
            Some("2026-08-02T01:02:03Z")
        );
    }

    #[test]
    fn direct_nonranked_facts_preserve_weapon_damage_for_per_minute_metrics() {
        let fact = player_fact(
            &json!({
                "player_id": 7,
                "Damage_Done_In_Hand": 50_022,
                "Damage_Player": 62_086
            }),
            get_match_queue_definition(424),
            true,
        );
        assert_eq!(fact.damage_done_in_hand, Some(50_022));
        assert_eq!(fact.damage, 62_086);
    }
}
