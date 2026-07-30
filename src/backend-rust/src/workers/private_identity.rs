use std::collections::BTreeMap;

use serde_json::Value;
use time::OffsetDateTime;
use tokio_postgres::{Row, Transaction};

use super::match_facts::CanonicalMatchPayload;

pub const PRIVATE_IDENTITY_VERSION: i16 = 3;
pub const PRIVATE_IDENTITY_LINK_THRESHOLD: i16 = 68;
pub const PRIVATE_IDENTITY_MARGIN: i16 = 12;
const PRIVATE_RESOLVER_LOCK: i64 = 812_240_583;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrivateObservation {
    pub match_id: i64,
    pub private_slot: i16,
    pub entry_datetime: OffsetDateTime,
    pub party_id: i32,
    pub account_level: i32,
    pub mastery_level: i32,
    pub league_tier: i32,
    pub league_points: i32,
    pub champion_id: i32,
    pub task_force: i16,
    pub win_status: String,
    pub portal_id: i16,
    pub portal_user_id: String,
    pub platform: String,
    pub source: String,
    pub party_member_ids: Vec<i64>,
    pub queue_id: Option<i32>,
    pub stats_scope: String,
    pub map: String,
    pub match_end_datetime: Option<OffsetDateTime>,
    pub observation_quality: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityScore {
    pub score: i16,
    pub reasons: Vec<String>,
    pub hard_conflict: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrivateIdentityResolution {
    pub observations: usize,
    pub linked: usize,
    pub created: usize,
    pub ambiguous: usize,
    pub minimal: usize,
}

#[derive(Clone, Debug)]
struct ObservationRow {
    observation: PrivateObservation,
    private_player_id: Option<i32>,
}

pub fn has_private_identity_evidence(observation: &PrivateObservation) -> bool {
    observation.account_level > 0
        || observation.mastery_level > 0
        || observation.league_tier > 0
        || observation.league_points > 0
        || observation.party_id > 0
        || observation.champion_id > 0
        || observation.portal_id > 0
        || !observation.portal_user_id.is_empty()
        || !observation.party_member_ids.is_empty()
}

pub fn score_private_identity(
    incoming: &PrivateObservation,
    existing: &PrivateObservation,
) -> IdentityScore {
    let mut reasons = Vec::new();
    let mut score = 0_i16;
    let mut hard_conflict = false;
    if incoming.match_id == existing.match_id {
        return IdentityScore {
            score: 0,
            reasons: vec!["same_match_conflict".to_owned()],
            hard_conflict: true,
        };
    }

    let (earlier, later) = if incoming.entry_datetime >= existing.entry_datetime {
        (existing, incoming)
    } else {
        (incoming, existing)
    };
    let hours = (incoming.entry_datetime - existing.entry_datetime)
        .whole_seconds()
        .unsigned_abs() as f64
        / 3_600.0;

    if !incoming.portal_user_id.is_empty() && !existing.portal_user_id.is_empty() {
        if incoming.portal_user_id != existing.portal_user_id {
            reasons.push("portal_user_conflict".to_owned());
            hard_conflict = true;
        } else if incoming.portal_id == 0
            || existing.portal_id == 0
            || incoming.portal_id == existing.portal_id
        {
            score += 100;
            reasons.push("portal_user_exact".to_owned());
        }
    }
    if incoming.portal_id > 0 && existing.portal_id > 0 && incoming.portal_id != existing.portal_id
    {
        reasons.push("portal_conflict".to_owned());
        hard_conflict = true;
    }

    if earlier.account_level > 0 && later.account_level > 0 {
        let regression = earlier.account_level - later.account_level;
        if regression > 1 {
            reasons.push("account_level_regression".to_owned());
            hard_conflict = true;
        } else if incoming.account_level == existing.account_level {
            score += if incoming.account_level >= 999 { 8 } else { 18 };
            reasons.push(
                if incoming.account_level >= 999 {
                    "account_level_cap_exact"
                } else {
                    "account_level_exact"
                }
                .to_owned(),
            );
        } else if later.account_level >= earlier.account_level
            && later.account_level - earlier.account_level <= 2
        {
            score += 12;
            reasons.push("account_level_progression".to_owned());
        }
    }

    let same_champion = incoming.champion_id > 0 && incoming.champion_id == existing.champion_id;
    if same_champion {
        score += 3;
        reasons.push("champion_exact".to_owned());
        if earlier.mastery_level > 0 && later.mastery_level > 0 {
            let regression = earlier.mastery_level - later.mastery_level;
            if regression > 2 {
                reasons.push("mastery_regression".to_owned());
                hard_conflict = true;
            } else if incoming.mastery_level == existing.mastery_level {
                score += 14;
                reasons.push("mastery_exact".to_owned());
            } else if later.mastery_level >= earlier.mastery_level
                && later.mastery_level - earlier.mastery_level <= 2
            {
                score += 8;
                reasons.push("mastery_progression".to_owned());
            }
        }
    }

    if incoming.league_tier > 0 && incoming.league_tier == existing.league_tier {
        score += 10;
        reasons.push("league_tier_exact".to_owned());
    }
    let mut tp_progression_compatible = false;
    if incoming.league_tier > 0
        && incoming.league_points >= 0
        && existing.league_points >= 0
        && incoming.league_tier == existing.league_tier
    {
        let point_difference = (incoming.league_points - existing.league_points).abs();
        let point_delta = later.league_points - earlier.league_points;
        let earlier_outcome = normalized_outcome(&earlier.win_status);
        let direction_matches = match earlier_outcome {
            Some("win") => point_delta >= 0,
            Some("loss") => point_delta <= 0,
            _ => false,
        };
        if point_difference == 0 {
            score += 5;
            tp_progression_compatible = true;
            reasons.push("tp_stable".to_owned());
        } else if direction_matches && point_difference <= 25 {
            score += 10;
            tp_progression_compatible = true;
            reasons.push(format!(
                "tp_{}_progression",
                earlier_outcome.unwrap_or_default()
            ));
        } else if direction_matches && point_difference <= 50 {
            score += 7;
            tp_progression_compatible = true;
            reasons.push(format!(
                "tp_{}_extended_progression",
                earlier_outcome.unwrap_or_default()
            ));
        } else if direction_matches {
            score += 4;
            tp_progression_compatible = true;
            reasons.push(format!(
                "tp_{}_large_progression",
                earlier_outcome.unwrap_or_default()
            ));
        } else if earlier_outcome.is_none() && point_difference <= 25 {
            score += 3;
            tp_progression_compatible = true;
            reasons.push("tp_near_without_outcome".to_owned());
        } else {
            reasons.push("tp_progression_uncertain".to_owned());
        }
    }
    if !incoming.platform.is_empty() && incoming.platform == existing.platform {
        score += 4;
        reasons.push("platform_exact".to_owned());
    }

    let companions = incoming
        .party_member_ids
        .iter()
        .filter(|player_id| existing.party_member_ids.contains(player_id))
        .count();
    if companions > 0 {
        score += 50
            + i16::try_from((companions - 1) * 6)
                .unwrap_or(i16::MAX)
                .min(12);
        reasons.push(format!("party_companion_overlap:{companions}"));
    }

    if incoming.party_id > 0 && incoming.party_id == existing.party_id {
        if hours <= 12.0 {
            score += 20;
            reasons.push("party_session_exact".to_owned());
            if tp_progression_compatible
                && incoming.account_level > 0
                && incoming.account_level == existing.account_level
                && incoming.league_tier > 0
                && incoming.league_tier == existing.league_tier
            {
                score += 8;
                reasons.push("ranked_session_progression".to_owned());
            }
        } else {
            score += 3;
            reasons.push("party_id_context_only".to_owned());
        }
    }
    if hours <= 12.0 {
        score += 5;
        reasons.push("time_within_12h".to_owned());
    } else if hours <= 24.0 * 7.0 {
        score += 2;
        reasons.push("time_within_7d".to_owned());
    }

    if hours <= 12.0
        && incoming.account_level > 0
        && incoming.account_level < 999
        && incoming.account_level == existing.account_level
        && same_champion
        && incoming.mastery_level > 0
        && incoming.mastery_level == existing.mastery_level
        && !incoming.platform.is_empty()
        && incoming.platform == existing.platform
        && incoming.stats_scope != "ranked"
        && existing.stats_scope != "ranked"
    {
        score += 25;
        reasons.push("casual_progression_bundle".to_owned());
    }

    IdentityScore {
        score: if hard_conflict { 0 } else { score.min(100) },
        reasons,
        hard_conflict,
    }
}

fn normalized_outcome(value: &str) -> Option<&'static str> {
    match value.trim().to_ascii_lowercase().as_str() {
        "winner" | "win" => Some("win"),
        "loser" | "loss" => Some("loss"),
        _ => None,
    }
}

fn score_candidate(
    incoming: &PrivateObservation,
    observations: &[PrivateObservation],
) -> IdentityScore {
    let before = observations
        .iter()
        .rev()
        .find(|observation| observation.entry_datetime <= incoming.entry_datetime);
    let after = observations
        .iter()
        .find(|observation| observation.entry_datetime > incoming.entry_datetime);
    for neighbor in [before, after].into_iter().flatten() {
        let compatibility = score_private_identity(incoming, neighbor);
        if compatibility.hard_conflict {
            return compatibility;
        }
    }
    observations
        .iter()
        .map(|observation| score_private_identity(incoming, observation))
        .filter(|score| !score.hard_conflict)
        .max_by_key(|score| score.score)
        .unwrap_or(IdentityScore {
            score: 0,
            reasons: Vec::new(),
            hard_conflict: false,
        })
}

pub async fn persist_and_resolve_private_identities(
    transaction: &Transaction<'_>,
    payload: &CanonicalMatchPayload,
    players: &Value,
    limited: bool,
) -> Result<PrivateIdentityResolution, tokio_postgres::Error> {
    let queue = transaction
        .query_one(
            "SELECT stats_scope FROM queue_types WHERE queue_id=$1",
            &[&payload.queue_id],
        )
        .await?;
    let stats_scope = queue.get::<_, String>("stats_scope");
    let observation_quality = if limited { "limited" } else { "complete" };
    transaction
        .execute(
            r#"
            INSERT INTO private_account_observations (
              match_id,private_slot,entry_datetime,party_id,account_level,
              mastery_level,league_tier,league_points,champion_id,task_force,
              win_status,portal_id,portal_user_id,platform,source,source_priority,
              resolution_status,queue_id,stats_scope,map,match_end_datetime,
              observation_quality,updated_at
            )
            SELECT
              $1,COALESCE(NULLIF(p->>'_private_slot','')::smallint,0),
              $2::text::timestamptz,
              COALESCE(NULLIF(p->>'party_id','')::int,0),
              COALESCE(NULLIF(p->>'account_level','')::int,0),
              COALESCE(NULLIF(p->>'mastery_level','')::int,0),
              COALESCE(NULLIF(p->>'league_tier','')::int,0),
              COALESCE(NULLIF(p->>'league_points','')::int,0),
              NULLIF(NULLIF(p->>'champion_id','')::int,0),
              NULLIF(NULLIF(p->>'task_force','')::smallint,0),
              NULLIF(LOWER(COALESCE(p->>'win_status','')),''),
              NULLIF(NULLIF(p->>'portal_id','')::smallint,0),
              NULLIF(p->>'portal_user_id',''),
              NULLIF(LOWER(COALESCE(p->>'platform','')),''),
              LOWER(COALESCE(NULLIF(p->>'source',''),'direct')),
              CASE LOWER(COALESCE(NULLIF(p->>'source',''),'direct'))
                WHEN 'direct' THEN 30 WHEN 'recovered' THEN 20 ELSE 10
              END,
              CASE WHEN
                COALESCE(NULLIF(p->>'party_id','')::int,0)>0
                OR COALESCE(NULLIF(p->>'account_level','')::int,0)>0
                OR COALESCE(NULLIF(p->>'mastery_level','')::int,0)>0
                OR COALESCE(NULLIF(p->>'league_tier','')::int,0)>0
                OR COALESCE(NULLIF(p->>'league_points','')::int,0)>0
                OR COALESCE(NULLIF(p->>'champion_id','')::int,0)>0
                OR COALESCE(NULLIF(p->>'portal_id','')::int,0)>0
                OR COALESCE(NULLIF(p->>'portal_user_id',''),'')<>''
              THEN 'unresolved' ELSE 'minimal' END,
              $3,$4,$5,
              $2::text::timestamptz+($6::int*interval '1 second'),
              $7,now()
            FROM jsonb_array_elements($8::jsonb) AS facts(p)
            WHERE COALESCE(NULLIF(p->>'player_id','')::bigint,0)=0
              AND UPPER(COALESCE(p->>'player_name',''))='PRIVATEACCOUNT'
              AND COALESCE(NULLIF(p->>'_private_slot','')::smallint,0)>0
            ON CONFLICT (match_id,private_slot) DO UPDATE SET
              entry_datetime=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.entry_datetime ELSE private_account_observations.entry_datetime END,
              party_id=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.party_id ELSE private_account_observations.party_id END,
              account_level=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.account_level ELSE private_account_observations.account_level END,
              mastery_level=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.mastery_level ELSE private_account_observations.mastery_level END,
              league_tier=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.league_tier ELSE private_account_observations.league_tier END,
              league_points=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.league_points ELSE private_account_observations.league_points END,
              champion_id=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.champion_id ELSE private_account_observations.champion_id END,
              task_force=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.task_force ELSE private_account_observations.task_force END,
              win_status=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.win_status ELSE private_account_observations.win_status END,
              portal_id=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.portal_id ELSE private_account_observations.portal_id END,
              portal_user_id=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.portal_user_id ELSE private_account_observations.portal_user_id END,
              platform=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.platform ELSE private_account_observations.platform END,
              source=CASE WHEN EXCLUDED.source_priority>=private_account_observations.source_priority THEN EXCLUDED.source ELSE private_account_observations.source END,
              source_priority=GREATEST(EXCLUDED.source_priority,private_account_observations.source_priority),
              resolution_status=CASE
                WHEN private_account_observations.private_player_id IS NOT NULL
                  THEN private_account_observations.resolution_status
                ELSE EXCLUDED.resolution_status
              END,
              queue_id=COALESCE(EXCLUDED.queue_id,private_account_observations.queue_id),
              stats_scope=COALESCE(NULLIF(EXCLUDED.stats_scope,''),private_account_observations.stats_scope),
              map=COALESCE(NULLIF(EXCLUDED.map,''),private_account_observations.map),
              match_end_datetime=COALESCE(EXCLUDED.match_end_datetime,private_account_observations.match_end_datetime),
              observation_quality=EXCLUDED.observation_quality,
              updated_at=now()
            "#,
            &[
                &payload.match_id,
                &payload.entry_datetime,
                &payload.queue_id,
                &stats_scope,
                &payload.map,
                &payload.duration_seconds,
                &observation_quality,
                players,
            ],
        )
        .await?;

    let observation_count = transaction
        .query_one(
            "SELECT count(*)::bigint AS count FROM private_account_observations WHERE match_id=$1",
            &[&payload.match_id],
        )
        .await?
        .get::<_, i64>("count") as usize;
    if observation_count == 0 || limited {
        return Ok(PrivateIdentityResolution {
            observations: observation_count,
            ..PrivateIdentityResolution::default()
        });
    }

    transaction
        .query_one(
            "SELECT pg_advisory_xact_lock($1)",
            &[&PRIVATE_RESOLVER_LOCK],
        )
        .await?;
    transaction
        .execute(
            r#"
            UPDATE private_account_observations observation
            SET party_member_ids=COALESCE((
                  SELECT array_agg(DISTINCT player.player_id ORDER BY player.player_id)
                  FROM match_players player
                  WHERE player.match_id=observation.match_id
                    AND player.player_id>0
                    AND observation.party_id>0
                    AND player.party_id=observation.party_id
                ),'{}'::bigint[]),
                updated_at=now()
            WHERE observation.match_id=$1
            "#,
            &[&payload.match_id],
        )
        .await?;

    let rows = transaction
        .query(
            r#"
            SELECT *
            FROM private_account_observations
            WHERE match_id=$1
            ORDER BY private_slot
            FOR UPDATE
            "#,
            &[&payload.match_id],
        )
        .await?;
    let mut summary = PrivateIdentityResolution {
        observations: rows.len(),
        ..PrivateIdentityResolution::default()
    };
    for row in rows {
        let observation_row = observation_row(&row);
        let outcome = resolve_observation(transaction, &observation_row).await?;
        match outcome {
            ResolutionOutcome::Linked => summary.linked += 1,
            ResolutionOutcome::Created => summary.created += 1,
            ResolutionOutcome::Ambiguous => summary.ambiguous += 1,
            ResolutionOutcome::Minimal => summary.minimal += 1,
        }
    }
    Ok(summary)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolutionOutcome {
    Linked,
    Created,
    Ambiguous,
    Minimal,
}

async fn resolve_observation(
    transaction: &Transaction<'_>,
    row: &ObservationRow,
) -> Result<ResolutionOutcome, tokio_postgres::Error> {
    let observation = &row.observation;
    if !has_private_identity_evidence(observation) {
        transaction
            .execute(
                r#"
                UPDATE private_account_observations
                SET resolution_status='minimal',resolution_confidence=0,
                    resolution_reasons='["no_identity_evidence"]'::jsonb,
                    updated_at=now()
                WHERE match_id=$1 AND private_slot=$2
                "#,
                &[&observation.match_id, &observation.private_slot],
            )
            .await?;
        upsert_unresolved_presence(transaction, observation, "no_identity_evidence").await?;
        return Ok(ResolutionOutcome::Minimal);
    }

    if let Some(private_player_id) = row.private_player_id {
        link_match_player(
            transaction,
            observation.match_id,
            observation.private_slot,
            private_player_id,
        )
        .await?;
        refresh_identity(transaction, private_player_id).await?;
        return Ok(ResolutionOutcome::Linked);
    }

    let grouped = candidate_observations(transaction, observation).await?;
    let mut ranked = grouped
        .iter()
        .map(|(id, observations)| (*id, score_candidate(observation, observations)))
        .filter(|(_, result)| !result.hard_conflict)
        .collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        right
            .1
            .score
            .cmp(&left.1.score)
            .then_with(|| left.0.cmp(&right.0))
    });
    let best = ranked.first();
    let runner_up = ranked.get(1);
    let has_threshold =
        best.is_some_and(|(_, result)| result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
    let has_margin = best.is_some_and(|(_, result)| {
        runner_up.is_none_or(|(_, runner)| result.score - runner.score >= PRIVATE_IDENTITY_MARGIN)
    });
    if has_threshold && has_margin {
        let (private_player_id, result) = best.expect("checked best candidate");
        link_observation(
            transaction,
            observation,
            *private_player_id,
            "linked",
            result.score,
            &result.reasons,
        )
        .await?;
        return Ok(ResolutionOutcome::Linked);
    }

    if !observation.stats_scope.is_empty()
        && observation.stats_scope != "ranked"
        && let Some((private_player_id, result)) = best
    {
        let category = if result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD {
            "ambiguous_candidate_margin"
        } else {
            "candidate_below_threshold"
        };
        let reasons = vec![
            category.to_owned(),
            format!("best_candidate:{private_player_id}:{}", result.score),
        ];
        transaction
            .execute(
                r#"
                UPDATE private_account_observations
                SET resolution_status='ambiguous',resolution_confidence=$3,
                    resolution_reasons=$4::text::jsonb,resolved_at=NULL,updated_at=now()
                WHERE match_id=$1 AND private_slot=$2
                "#,
                &[
                    &observation.match_id,
                    &observation.private_slot,
                    &result.score,
                    &serde_json::to_string(&reasons).expect("string reasons serialize"),
                ],
            )
            .await?;
        upsert_unresolved_presence(transaction, observation, "ambiguous_identity").await?;
        return Ok(ResolutionOutcome::Ambiguous);
    }

    let private_player_id = create_identity(transaction, observation).await?;
    let reasons = best.map_or_else(
        || vec!["no_candidate".to_owned()],
        |(candidate_id, result)| {
            vec![
                if result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD {
                    "ambiguous_candidate_margin".to_owned()
                } else {
                    "candidate_below_threshold".to_owned()
                },
                format!("best_candidate:{candidate_id}:{}", result.score),
            ]
        },
    );
    link_observation(
        transaction,
        observation,
        private_player_id,
        "new_identity",
        100,
        &reasons,
    )
    .await?;
    Ok(ResolutionOutcome::Created)
}

fn observation_row(row: &Row) -> ObservationRow {
    ObservationRow {
        observation: PrivateObservation {
            match_id: row.get("match_id"),
            private_slot: row.get("private_slot"),
            entry_datetime: row.get("entry_datetime"),
            party_id: row.get("party_id"),
            account_level: row.get("account_level"),
            mastery_level: row.get("mastery_level"),
            league_tier: row.get("league_tier"),
            league_points: row.get("league_points"),
            champion_id: row.get::<_, Option<i32>>("champion_id").unwrap_or_default(),
            task_force: row.get::<_, Option<i16>>("task_force").unwrap_or_default(),
            win_status: row
                .get::<_, Option<String>>("win_status")
                .unwrap_or_default(),
            portal_id: row.get::<_, Option<i16>>("portal_id").unwrap_or_default(),
            portal_user_id: row
                .get::<_, Option<String>>("portal_user_id")
                .unwrap_or_default(),
            platform: row.get::<_, Option<String>>("platform").unwrap_or_default(),
            source: row.get("source"),
            party_member_ids: row.get("party_member_ids"),
            queue_id: row.get("queue_id"),
            stats_scope: row
                .get::<_, Option<String>>("stats_scope")
                .unwrap_or_default(),
            map: row.get::<_, Option<String>>("map").unwrap_or_default(),
            match_end_datetime: row.get("match_end_datetime"),
            observation_quality: row.get("observation_quality"),
        },
        private_player_id: row.get("private_player_id"),
    }
}

async fn candidate_observations(
    transaction: &Transaction<'_>,
    observation: &PrivateObservation,
) -> Result<BTreeMap<i32, Vec<PrivateObservation>>, tokio_postgres::Error> {
    let rows = transaction
        .query(
            r#"
            SELECT candidate.*
            FROM private_account_observations candidate
            JOIN players_private identity ON identity.id=candidate.private_player_id
            WHERE candidate.private_player_id IS NOT NULL
              AND identity.tracking_version=$2
              AND identity.is_active
              AND candidate.match_id<>$1
              AND NOT EXISTS (
                SELECT 1
                FROM private_account_observations used
                WHERE used.match_id=$1
                  AND used.private_player_id=candidate.private_player_id
              )
              AND (
                ($3<>'' AND candidate.portal_user_id=$3)
                OR (
                  cardinality($4::bigint[])>0
                  AND candidate.party_member_ids && $4::bigint[]
                )
                OR (
                  $5<>0 AND candidate.party_id=$5
                  AND abs(extract(epoch FROM (
                    candidate.entry_datetime-$6::timestamptz
                  )))<=43200
                )
                OR (
                  $7>0
                  AND candidate.account_level BETWEEN GREATEST(1,$7-2) AND $7+2
                  AND $8>0 AND candidate.champion_id=$8
                  AND $9>0
                  AND candidate.mastery_level BETWEEN GREATEST(1,$9-2) AND $9+2
                )
              )
            ORDER BY candidate.private_player_id,candidate.entry_datetime
            "#,
            &[
                &observation.match_id,
                &PRIVATE_IDENTITY_VERSION,
                &observation.portal_user_id,
                &observation.party_member_ids,
                &observation.party_id,
                &observation.entry_datetime,
                &observation.account_level,
                &observation.champion_id,
                &observation.mastery_level,
            ],
        )
        .await?;
    let mut grouped = BTreeMap::<i32, Vec<PrivateObservation>>::new();
    for row in rows {
        let identity_id = row
            .get::<_, Option<i32>>("private_player_id")
            .expect("candidate query requires a linked identity");
        grouped
            .entry(identity_id)
            .or_default()
            .push(observation_row(&row).observation);
    }
    Ok(grouped)
}

async fn create_identity(
    transaction: &Transaction<'_>,
    observation: &PrivateObservation,
) -> Result<i32, tokio_postgres::Error> {
    let id = transaction
        .query_one(
            r#"
            INSERT INTO players_private (
              party_id,account_level,mastery_level,league_tier,league_points,
              last_known_level,last_known_mastery,last_known_league_tier,
              last_known_league_points,first_seen,last_seen,state_observed_at,
              match_count,tracking_version,identity_status,identity_confidence,
              is_active
            ) VALUES (
              $1,$2,$3,$4,$5,$2,$3,$4,$5,$6,$6,$6,0,$7,'inferred',0,TRUE
            )
            RETURNING id
            "#,
            &[
                &observation.party_id,
                &observation.account_level,
                &observation.mastery_level,
                &observation.league_tier,
                &observation.league_points,
                &observation.entry_datetime,
                &PRIVATE_IDENTITY_VERSION,
            ],
        )
        .await?
        .get::<_, i32>("id");
    transaction
        .execute(
            r#"
            UPDATE players_private
            SET alias=COALESCE(NULLIF(btrim(alias),''),'P-'||lpad(id::text,6,'0'))
            WHERE id=$1
            "#,
            &[&id],
        )
        .await?;
    Ok(id)
}

async fn link_observation(
    transaction: &Transaction<'_>,
    observation: &PrivateObservation,
    private_player_id: i32,
    status: &str,
    confidence: i16,
    reasons: &[String],
) -> Result<(), tokio_postgres::Error> {
    let reasons_json = serde_json::to_string(reasons).expect("string reasons serialize");
    transaction
        .execute(
            r#"
            UPDATE private_account_observations
            SET private_player_id=$3,resolution_status=$4,
                resolution_confidence=$5,resolution_reasons=$6::text::jsonb,
                resolved_at=now(),updated_at=now()
            WHERE match_id=$1 AND private_slot=$2
            "#,
            &[
                &observation.match_id,
                &observation.private_slot,
                &private_player_id,
                &status,
                &confidence,
                &reasons_json,
            ],
        )
        .await?;
    link_match_player(
        transaction,
        observation.match_id,
        observation.private_slot,
        private_player_id,
    )
    .await?;
    transaction
        .execute(
            r#"
            INSERT INTO players_private_history (
              player_private_id,party_id,account_level,mastery_level,
              league_tier,league_points,match_id,private_slot,recorded_at,
              resolution_confidence,resolution_reasons
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::text::jsonb)
            ON CONFLICT (match_id,private_slot) WHERE match_id IS NOT NULL
            DO UPDATE SET
              player_private_id=EXCLUDED.player_private_id,
              party_id=EXCLUDED.party_id,
              account_level=EXCLUDED.account_level,
              mastery_level=EXCLUDED.mastery_level,
              league_tier=EXCLUDED.league_tier,
              league_points=EXCLUDED.league_points,
              recorded_at=EXCLUDED.recorded_at,
              resolution_confidence=EXCLUDED.resolution_confidence,
              resolution_reasons=EXCLUDED.resolution_reasons
            "#,
            &[
                &private_player_id,
                &observation.party_id,
                &observation.account_level,
                &observation.mastery_level,
                &observation.league_tier,
                &observation.league_points,
                &observation.match_id,
                &observation.private_slot,
                &observation.entry_datetime,
                &confidence,
                &reasons_json,
            ],
        )
        .await?;
    refresh_identity(transaction, private_player_id).await?;
    upsert_private_presence(transaction, observation, private_player_id, confidence).await?;
    Ok(())
}

async fn link_match_player(
    transaction: &Transaction<'_>,
    match_id: i64,
    private_slot: i16,
    private_player_id: i32,
) -> Result<(), tokio_postgres::Error> {
    transaction
        .execute(
            r#"
            UPDATE match_players
            SET private_player_id=$3
            WHERE match_id=$1 AND player_id=0 AND private_slot=$2
            "#,
            &[&match_id, &private_slot, &private_player_id],
        )
        .await?;
    Ok(())
}

async fn refresh_identity(
    transaction: &Transaction<'_>,
    private_player_id: i32,
) -> Result<(), tokio_postgres::Error> {
    transaction
        .execute(
            r#"
            WITH aggregate AS (
              SELECT
                min(entry_datetime) AS first_seen,
                max(entry_datetime) AS last_seen,
                count(*)::int AS match_count,
                min(
                  CASE WHEN resolution_status='new_identity'
                    THEN 100 ELSE resolution_confidence END
                )::smallint AS confidence
              FROM private_account_observations
              WHERE private_player_id=$1
            ), latest AS (
              SELECT
                party_id,account_level,mastery_level,league_tier,
                league_points,entry_datetime
              FROM private_account_observations
              WHERE private_player_id=$1
              ORDER BY entry_datetime DESC,match_id DESC,private_slot DESC
              LIMIT 1
            )
            UPDATE players_private identity SET
              party_id=latest.party_id,
              account_level=latest.account_level,
              mastery_level=latest.mastery_level,
              league_tier=latest.league_tier,
              league_points=latest.league_points,
              last_known_level=latest.account_level,
              last_known_mastery=latest.mastery_level,
              last_known_league_tier=latest.league_tier,
              last_known_league_points=latest.league_points,
              first_seen=aggregate.first_seen,
              last_seen=aggregate.last_seen,
              state_observed_at=latest.entry_datetime,
              match_count=aggregate.match_count,
              identity_confidence=CASE
                WHEN aggregate.match_count<=1 THEN 0
                ELSE COALESCE(aggregate.confidence,0)
              END,
              identity_status=CASE
                WHEN identity.verified_name IS NOT NULL THEN 'verified'
                ELSE 'inferred'
              END,
              updated_at=now()
            FROM aggregate,latest
            WHERE identity.id=$1
            "#,
            &[&private_player_id],
        )
        .await?;
    Ok(())
}

async fn upsert_private_presence(
    transaction: &Transaction<'_>,
    observation: &PrivateObservation,
    private_player_id: i32,
    confidence: i16,
) -> Result<(), tokio_postgres::Error> {
    if let Some(queue_id) = observation.queue_id
        && !observation.stats_scope.is_empty()
    {
        transaction
            .execute(
                r#"
                INSERT INTO private_player_presence_24h (
                  private_player_id,first_observed_at,last_observed_at,
                  last_match_id,last_queue_id,last_stats_scope,
                  identity_confidence
                ) VALUES ($1,$2,$2,$3,$4,$5,$6)
                ON CONFLICT (private_player_id) DO UPDATE SET
                  first_observed_at=LEAST(
                    private_player_presence_24h.first_observed_at,
                    EXCLUDED.first_observed_at
                  ),
                  last_observed_at=GREATEST(
                    private_player_presence_24h.last_observed_at,
                    EXCLUDED.last_observed_at
                  ),
                  last_match_id=CASE
                    WHEN EXCLUDED.last_observed_at>=
                      private_player_presence_24h.last_observed_at
                    THEN EXCLUDED.last_match_id
                    ELSE private_player_presence_24h.last_match_id
                  END,
                  last_queue_id=CASE
                    WHEN EXCLUDED.last_observed_at>=
                      private_player_presence_24h.last_observed_at
                    THEN EXCLUDED.last_queue_id
                    ELSE private_player_presence_24h.last_queue_id
                  END,
                  last_stats_scope=CASE
                    WHEN EXCLUDED.last_observed_at>=
                      private_player_presence_24h.last_observed_at
                    THEN EXCLUDED.last_stats_scope
                    ELSE private_player_presence_24h.last_stats_scope
                  END,
                  identity_confidence=GREATEST(
                    private_player_presence_24h.identity_confidence,
                    EXCLUDED.identity_confidence
                  ),
                  updated_at=now()
                "#,
                &[
                    &private_player_id,
                    &observation.entry_datetime,
                    &observation.match_id,
                    &queue_id,
                    &observation.stats_scope,
                    &confidence,
                ],
            )
            .await?;
        transaction
            .execute(
                "DELETE FROM unresolved_private_presence WHERE match_id=$1 AND private_slot=$2",
                &[&observation.match_id, &observation.private_slot],
            )
            .await?;
    }
    Ok(())
}

async fn upsert_unresolved_presence(
    transaction: &Transaction<'_>,
    observation: &PrivateObservation,
    reason: &str,
) -> Result<(), tokio_postgres::Error> {
    if let Some(queue_id) = observation.queue_id
        && !observation.stats_scope.is_empty()
    {
        transaction
            .execute(
                r#"
                INSERT INTO unresolved_private_presence (
                  match_id,private_slot,observed_at,queue_id,stats_scope,reason
                ) VALUES ($1,$2,$3,$4,$5,$6)
                ON CONFLICT (match_id,private_slot) DO UPDATE SET
                  observed_at=EXCLUDED.observed_at,
                  queue_id=EXCLUDED.queue_id,
                  stats_scope=EXCLUDED.stats_scope,
                  reason=EXCLUDED.reason,
                  updated_at=now()
                "#,
                &[
                    &observation.match_id,
                    &observation.private_slot,
                    &observation.entry_datetime,
                    &queue_id,
                    &observation.stats_scope,
                    &reason,
                ],
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;

    fn parsed_time(value: &str) -> OffsetDateTime {
        OffsetDateTime::parse(value, &Rfc3339).expect("test timestamp")
    }

    fn observation(match_id: i64, at: OffsetDateTime) -> PrivateObservation {
        PrivateObservation {
            match_id,
            private_slot: 1,
            entry_datetime: at,
            party_id: 44,
            account_level: 100,
            mastery_level: 20,
            league_tier: 15,
            league_points: 42,
            champion_id: 2205,
            task_force: 1,
            win_status: "winner".to_owned(),
            portal_id: 1,
            portal_user_id: String::new(),
            platform: "steam".to_owned(),
            source: "direct".to_owned(),
            party_member_ids: Vec::new(),
            queue_id: Some(486),
            stats_scope: "ranked".to_owned(),
            map: "Stone Keep".to_owned(),
            match_end_datetime: None,
            observation_quality: "complete".to_owned(),
        }
    }

    #[test]
    fn exact_portal_identity_saturates_score() {
        let mut existing = observation(10, parsed_time("2026-07-30T12:00:00Z"));
        existing.portal_user_id = "stable-portal-user".to_owned();
        let mut incoming = observation(11, parsed_time("2026-07-30T13:00:00Z"));
        incoming.portal_user_id = "stable-portal-user".to_owned();

        let result = score_private_identity(&incoming, &existing);
        assert_eq!(result.score, 100);
        assert!(!result.hard_conflict);
        assert!(
            result
                .reasons
                .iter()
                .any(|reason| reason == "portal_user_exact")
        );
    }

    #[test]
    fn portal_and_chronology_conflicts_never_link() {
        let mut existing = observation(10, parsed_time("2026-07-30T12:00:00Z"));
        existing.portal_user_id = "one".to_owned();
        let mut incoming = observation(11, parsed_time("2026-07-30T13:00:00Z"));
        incoming.portal_user_id = "two".to_owned();

        let result = score_private_identity(&incoming, &existing);
        assert_eq!(result.score, 0);
        assert!(result.hard_conflict);

        incoming.match_id = existing.match_id;
        let same_match = score_private_identity(&incoming, &existing);
        assert_eq!(same_match.reasons, vec!["same_match_conflict"]);
        assert!(same_match.hard_conflict);
    }

    #[test]
    fn casual_progression_bundle_crosses_link_threshold() {
        let mut existing = observation(10, parsed_time("2026-07-30T12:00:00Z"));
        existing.stats_scope = "casual".to_owned();
        existing.league_tier = 0;
        existing.league_points = 0;
        let mut incoming = existing.clone();
        incoming.match_id = 11;
        incoming.entry_datetime = parsed_time("2026-07-30T13:00:00Z");

        let result = score_private_identity(&incoming, &existing);
        assert!(result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
        assert!(
            result
                .reasons
                .iter()
                .any(|reason| reason == "casual_progression_bundle")
        );
    }

    #[test]
    fn uncertain_tp_direction_is_evidence_not_a_conflict() {
        let existing = observation(10, parsed_time("2026-07-30T12:00:00Z"));
        let mut incoming = observation(11, parsed_time("2026-07-30T13:00:00Z"));
        incoming.league_points = 10;

        let result = score_private_identity(&incoming, &existing);
        assert!(!result.hard_conflict);
        assert!(
            result
                .reasons
                .iter()
                .any(|reason| reason == "tp_progression_uncertain")
        );
    }

    #[test]
    fn party_id_alone_stays_below_link_threshold() {
        let mut existing = observation(10, parsed_time("2026-07-30T12:00:00Z"));
        existing.account_level = 0;
        existing.mastery_level = 0;
        existing.league_tier = 0;
        existing.league_points = 0;
        existing.champion_id = 0;
        existing.platform.clear();
        let mut incoming = existing.clone();
        incoming.match_id = 11;
        incoming.entry_datetime = parsed_time("2026-07-30T13:00:00Z");

        let result = score_private_identity(&incoming, &existing);
        assert!(!result.hard_conflict);
        assert!(result.score < PRIVATE_IDENTITY_LINK_THRESHOLD);
        assert!(
            result
                .reasons
                .iter()
                .any(|reason| reason == "party_session_exact")
        );
    }

    #[test]
    fn public_party_companion_is_strong_identity_evidence() {
        let mut existing = observation(10, parsed_time("2026-07-29T12:00:00Z"));
        existing.party_id = 90_002;
        existing.party_member_ids = vec![8_717_218];
        let mut incoming = observation(11, parsed_time("2026-07-30T12:00:00Z"));
        incoming.party_id = 70_001;
        incoming.party_member_ids = vec![8_717_218];

        let result = score_private_identity(&incoming, &existing);
        assert!(!result.hard_conflict);
        assert!(result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
        assert!(
            result
                .reasons
                .iter()
                .any(|reason| reason == "party_companion_overlap:1")
        );
    }

    #[test]
    fn capped_level_does_not_trigger_casual_progression_bundle() {
        let mut existing = observation(10, parsed_time("2026-07-30T12:00:00Z"));
        existing.account_level = 999;
        existing.league_tier = 0;
        existing.league_points = 0;
        existing.party_id = 0;
        existing.stats_scope = "casual".to_owned();
        let mut incoming = existing.clone();
        incoming.match_id = 11;
        incoming.entry_datetime = parsed_time("2026-07-30T13:00:00Z");

        let result = score_private_identity(&incoming, &existing);
        assert!(
            !result
                .reasons
                .iter()
                .any(|reason| reason == "casual_progression_bundle")
        );
    }

    #[test]
    fn account_level_regression_matches_typescript_tolerance() {
        let existing = observation(10, parsed_time("2026-07-29T12:00:00Z"));
        let mut one_level = observation(11, parsed_time("2026-07-30T12:00:00Z"));
        one_level.account_level = existing.account_level - 1;
        assert!(!score_private_identity(&one_level, &existing).hard_conflict);

        let mut material = one_level;
        material.account_level = existing.account_level - 2;
        let result = score_private_identity(&material, &existing);
        assert!(result.hard_conflict);
        assert!(
            result
                .reasons
                .iter()
                .any(|reason| reason == "account_level_regression")
        );
    }
}
