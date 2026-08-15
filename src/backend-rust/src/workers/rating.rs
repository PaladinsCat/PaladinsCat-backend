use paladinscat_core::database::{Database, DatabaseError};
use serde::Serialize;
use serde_json::Value;
use tokio_postgres::{Row, Transaction};

const DEFAULT_MU: f64 = 1_500.0;
const DEFAULT_PHI: f64 = 350.0;
const DEFAULT_SIGMA: f64 = 0.06;
const SCALE: f64 = 173.7178;
const TAU: f64 = 0.5;
const RATING_LOCK: i64 = 4_860_001;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct GlickoState {
    pub rating: f64,
    pub deviation: f64,
    pub volatility: f64,
}

#[derive(Clone, Copy, Debug)]
pub struct GlickoOpponent {
    pub rating: f64,
    pub deviation: f64,
    pub weight: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RatingApplicationResult {
    Applied,
    Skipped,
    Deferred,
    Busy,
}

#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RatingReingestReport {
    pub matches_processed: i32,
    pub broken_matches: Vec<BrokenRatingMatch>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokenRatingMatch {
    pub match_id: i64,
    pub reason: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RatingError {
    #[error(transparent)]
    Database(#[from] DatabaseError),
    #[error(transparent)]
    Query(#[from] tokio_postgres::Error),
    #[error("Glicko-2: {0}")]
    InvalidState(String),
}

#[derive(Clone, Debug)]
struct Player {
    player_id: i64,
    champion_id: i32,
    win_status: String,
    task_force: i32,
    source: String,
    queue_id: i32,
    winning_task_force: i32,
    roster_count: i32,
    private_count: i32,
    team_one: i32,
    team_two: i32,
    queue: GlickoState,
    champion: GlickoState,
}

#[derive(Clone, Debug, Serialize)]
struct Change {
    player_id: i64,
    champion_id: i32,
    match_id: i64,
    queue_id: i32,
    queue_mu_pre: f64,
    queue_phi_pre: f64,
    queue_sigma_pre: f64,
    queue_mu_post: f64,
    queue_phi_post: f64,
    queue_sigma_post: f64,
    champ_mu_pre: f64,
    champ_phi_pre: f64,
    champ_sigma_pre: f64,
    champ_mu_post: f64,
    champ_phi_post: f64,
    champ_sigma_post: f64,
    is_winner: bool,
}

#[derive(Clone)]
pub struct RatingRepository {
    database: Database,
}

impl RatingRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    pub async fn apply_match(
        &self,
        match_id: i64,
        rebuilding: bool,
    ) -> Result<RatingApplicationResult, RatingError> {
        let mut client = self.database.connection().await?;
        let tx = client.transaction().await?;
        let acquired = tx
            .query_one(
                "SELECT pg_try_advisory_xact_lock($1) acquired",
                &[&RATING_LOCK],
            )
            .await?
            .get::<_, bool>("acquired");
        if !acquired {
            tx.rollback().await?;
            return Ok(RatingApplicationResult::Busy);
        }
        let rebuild_pending = tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM rating_rebuild_requests WHERE request_key='global')",
                &[],
            )
            .await?
            .get::<_, bool>(0);
        if should_defer_incremental(rebuilding, rebuild_pending) {
            tx.rollback().await?;
            return Ok(RatingApplicationResult::Deferred);
        }
        if !rebuilding {
            record_late_arrival(&tx, match_id).await?;
        }
        let changes = match calculate_changes(&tx, match_id).await {
            Ok(changes) => changes,
            Err(RatingError::InvalidState(reason)) => {
                queue_rebuild(&tx, match_id, &format!("Glicko-2: {reason}")).await?;
                tx.commit().await?;
                return Ok(RatingApplicationResult::Deferred);
            }
            Err(error) => return Err(error),
        };
        if changes.is_empty() || !apply_changes(&tx, &changes).await? {
            mark_stage(&tx, match_id).await?;
            tx.commit().await?;
            return Ok(RatingApplicationResult::Skipped);
        }
        let players = changes
            .iter()
            .map(|change| change.player_id)
            .collect::<Vec<_>>();
        let queue_id = changes[0].queue_id;
        tx.execute(
            "INSERT INTO rating_player_cursors(queue_id,player_id,last_match_id,last_entry_datetime,updated_at) \
             SELECT $2,player_id,$1,m.entry_datetime,now() FROM unnest($3::BIGINT[]) changed(player_id) \
             CROSS JOIN LATERAL(SELECT entry_datetime FROM matches WHERE match_id=$1 LIMIT 1)m \
             ON CONFLICT(queue_id,player_id) DO UPDATE SET last_match_id=EXCLUDED.last_match_id, \
             last_entry_datetime=GREATEST(rating_player_cursors.last_entry_datetime,EXCLUDED.last_entry_datetime),updated_at=now()",
            &[&match_id, &queue_id, &players],
        ).await?;
        mark_stage(&tx, match_id).await?;
        tx.commit().await?;
        Ok(RatingApplicationResult::Applied)
    }

    pub async fn reingest(&self) -> Result<RatingReingestReport, RatingError> {
        self.database.query_json(
            "TRUNCATE TABLE player_queue_ratings,player_champion_ratings,match_rating_snapshots, \
             rating_player_cursors RESTART IDENTITY CASCADE",
            &[],
        ).await?;
        let matches = self.database.query_json(
            "SELECT DISTINCT m.match_id,m.entry_datetime FROM matches m LEFT JOIN match_ingest_status mis ON mis.match_id=m.match_id \
             WHERE m.queue_id=486 AND COALESCE(m.is_ranked,m.queue_id=486)=TRUE \
             AND COALESCE(mis.status,'complete')='complete' ORDER BY m.entry_datetime,m.match_id",
            &[],
        ).await?;
        let mut report = RatingReingestReport::default();
        let mut deferred = false;
        for row in matches {
            let Some(match_id) = integer(&row, "match_id") else {
                continue;
            };
            match self.apply_match(match_id, true).await {
                Ok(RatingApplicationResult::Applied) => {
                    report.matches_processed = report.matches_processed.saturating_add(1);
                }
                Ok(result) => {
                    deferred |= matches!(
                        result,
                        RatingApplicationResult::Deferred | RatingApplicationResult::Busy
                    );
                    report.broken_matches.push(BrokenRatingMatch {
                        match_id,
                        reason: match result {
                            RatingApplicationResult::Deferred => {
                                "Rating update deferred for a chronological rebuild"
                            }
                            RatingApplicationResult::Busy => {
                                "Rating stream was busy; retry the rebuild"
                            }
                            _ => "Rating eligibility rejected match",
                        }
                        .to_owned(),
                    });
                }
                Err(error) => {
                    deferred = true;
                    report.broken_matches.push(BrokenRatingMatch {
                        match_id,
                        reason: format!("Error: {error}"),
                    });
                }
            }
        }
        if !deferred {
            self.database
                .query_json("DELETE FROM rating_rebuild_requests", &[])
                .await?;
        }
        super::projections::rebuild_best_champion_ratings(&self.database).await?;
        Ok(report)
    }
}

fn should_defer_incremental(rebuilding: bool, rebuild_pending: bool) -> bool {
    !rebuilding && rebuild_pending
}

pub fn is_valid_state(state: GlickoState) -> bool {
    state.rating.is_finite()
        && (0.0..=3_500.0).contains(&state.rating)
        && state.deviation.is_finite()
        && (1.0..=350.0).contains(&state.deviation)
        && state.volatility.is_finite()
        && (0.001..=0.2).contains(&state.volatility)
}

pub fn glicko_update(
    current: GlickoState,
    opponents: &[GlickoOpponent],
    score: f64,
) -> Result<GlickoState, RatingError> {
    if !is_valid_state(current) {
        return Err(RatingError::InvalidState(
            "input state is outside safe bounds".to_owned(),
        ));
    }
    if !score.is_finite() || !(0.0..=1.0).contains(&score) {
        return Err(RatingError::InvalidState(format!(
            "invalid outcome score {score}"
        )));
    }
    let mu = (current.rating - DEFAULT_MU) / SCALE;
    let phi = current.deviation / SCALE;
    let sigma = current.volatility;
    let opponents = opponents
        .iter()
        .filter(|opponent| {
            opponent.rating.is_finite()
                && opponent.deviation.is_finite()
                && (1.0..=350.0).contains(&opponent.deviation)
                && opponent.weight.is_finite()
                && opponent.weight > 0.0
        })
        .map(|opponent| {
            (
                (opponent.rating - DEFAULT_MU) / SCALE,
                opponent.deviation / SCALE,
                opponent.weight,
            )
        })
        .collect::<Vec<_>>();
    if opponents.is_empty() {
        return Ok(GlickoState {
            rating: current.rating,
            deviation: round(SCALE * (phi.powi(2) + sigma.powi(2)).sqrt(), 2),
            volatility: current.volatility,
        });
    }
    let g = |value: f64| 1.0 / (1.0 + 3.0 * value.powi(2) / std::f64::consts::PI.powi(2)).sqrt();
    let expected =
        |rating: f64, deviation: f64| 1.0 / (1.0 + (-g(deviation) * (mu - rating)).exp());
    let inverse = opponents
        .iter()
        .map(|(rating, deviation, weight)| {
            let estimate = expected(*rating, *deviation);
            weight * g(*deviation).powi(2) * estimate * (1.0 - estimate)
        })
        .sum::<f64>();
    let variance = if inverse > 0.0 { 1.0 / inverse } else { 0.0 };
    let sum = opponents
        .iter()
        .map(|(rating, deviation, weight)| {
            weight * g(*deviation) * (score - expected(*rating, *deviation))
        })
        .sum::<f64>();
    let delta = variance * sum;
    let alpha = sigma.powi(2).ln();
    let objective = |value: f64| {
        let exponential = value.exp();
        exponential * (delta.powi(2) - phi.powi(2) - variance - exponential)
            / (2.0 * (phi.powi(2) + variance + exponential).powi(2))
            - (value - alpha) / TAU.powi(2)
    };
    let mut lower = alpha;
    let mut upper = if delta.powi(2) > phi.powi(2) + variance {
        (delta.powi(2) - phi.powi(2) - variance).ln()
    } else {
        let mut step = 1;
        while objective(alpha - f64::from(step) * TAU) < 0.0 && step < 100 {
            step += 1;
        }
        alpha - f64::from(step) * TAU
    };
    let mut low_value = objective(lower);
    let mut high_value = objective(upper);
    let mut iterations = 0;
    while (upper - lower).abs() > 0.000_001 && iterations < 100 {
        iterations += 1;
        if (high_value - low_value).abs() < f64::EPSILON {
            break;
        }
        let candidate = lower + (lower - upper) * low_value / (high_value - low_value);
        let value = objective(candidate);
        if value * high_value <= 0.0 {
            lower = upper;
            low_value = high_value;
        } else {
            low_value /= 2.0;
        }
        upper = candidate;
        high_value = value;
    }
    let new_sigma = (lower / 2.0).exp();
    let phi_star = phi.powi(2) + new_sigma.powi(2);
    let new_phi = if variance > 0.0 {
        1.0 / (1.0 / phi_star + 1.0 / variance).sqrt()
    } else {
        phi_star.sqrt()
    };
    let result = GlickoState {
        rating: round(SCALE * (mu + new_phi.powi(2) * sum) + DEFAULT_MU, 2),
        deviation: round(SCALE * new_phi, 2),
        volatility: round(new_sigma, 4),
    };
    if !is_valid_state(result) {
        return Err(RatingError::InvalidState(
            "output state is outside safe bounds".to_owned(),
        ));
    }
    Ok(result)
}

async fn calculate_changes(
    tx: &Transaction<'_>,
    match_id: i64,
) -> Result<Vec<Change>, RatingError> {
    let rows = tx
        .query(
            r#"
            SELECT
              mp.player_id,
              mp.champion_id,
              CASE
                WHEN mp.win_status IN ('Winner','Win') THEN 'Winner'
                WHEN mp.win_status IN ('Loser','Loss') THEN 'Loser'
                ELSE mp.win_status
              END AS win_status,
              mp.task_force::int AS task_force,
              COALESCE(mp.source,'direct') AS source,
              m.queue_id,
              m.winning_task_force,
              (
                SELECT count(*)::int
                FROM match_players roster
                WHERE roster.match_id=m.match_id
                  AND roster.entry_datetime=m.entry_datetime
              ) AS roster_count,
              (
                SELECT count(*)::int
                FROM match_players roster
                WHERE roster.match_id=m.match_id
                  AND roster.entry_datetime=m.entry_datetime
                  AND roster.player_id=0
                  AND upper(COALESCE(roster.player_name,''))='PRIVATEACCOUNT'
              ) AS private_count,
              (
                SELECT count(*)::int
                FROM match_players roster
                WHERE roster.match_id=m.match_id
                  AND roster.entry_datetime=m.entry_datetime
                  AND roster.task_force=1
              ) AS team_one,
              (
                SELECT count(*)::int
                FROM match_players roster
                WHERE roster.match_id=m.match_id
                  AND roster.entry_datetime=m.entry_datetime
                  AND roster.task_force=2
              ) AS team_two,
              pqr.mu::double precision AS queue_mu,
              pqr.phi::double precision AS queue_phi,
              pqr.volatility::double precision AS queue_sigma,
              pcr.mu::double precision AS champ_mu,
              pcr.phi::double precision AS champ_phi,
              pcr.volatility::double precision AS champ_sigma
            FROM matches m
            JOIN match_players mp
              ON mp.match_id=m.match_id
             AND mp.entry_datetime=m.entry_datetime
            LEFT JOIN match_ingest_status mis ON mis.match_id=m.match_id
            LEFT JOIN player_queue_ratings pqr
              ON pqr.player_id=mp.player_id
             AND pqr.queue_id=m.queue_id
            LEFT JOIN player_champion_ratings pcr
              ON pcr.player_id=mp.player_id
             AND pcr.champion_id=mp.champion_id
            WHERE m.match_id=$1
              AND m.queue_id=486
              AND NOT COALESCE(m.limited,FALSE)
              AND COALESCE(m.is_ranked,m.queue_id=486)=TRUE
              AND m.winning_task_force IN (1,2)
              AND COALESCE(mis.status,'complete') IN ('processing','partial','complete')
              AND COALESCE(mp.source,'direct') IN ('direct','recovered')
              AND mp.player_id>0
              AND mp.champion_id>0
              AND mp.task_force IN (1,2)
              AND mp.win_status IN ('Winner','Loser','Win','Loss')
            ORDER BY mp.task_force,mp.player_id
            "#,
            &[&match_id],
        )
        .await?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let players = rows.iter().map(player).collect::<Result<Vec<_>, _>>()?;
    if !valid_roster(&players) {
        return Ok(Vec::new());
    }
    let mut changes = Vec::with_capacity(players.len());
    for current in &players {
        let opponents = players
            .iter()
            .filter(|row| row.task_force != current.task_force)
            .collect::<Vec<_>>();
        if opponents.is_empty() || opponents.len() > 5 {
            return Ok(Vec::new());
        }
        let weight = 1.0 / opponents.len() as f64;
        let queue = opponents
            .iter()
            .map(|row| GlickoOpponent {
                rating: row.queue.rating,
                deviation: row.queue.deviation,
                weight,
            })
            .collect::<Vec<_>>();
        let champion = opponents
            .iter()
            .map(|row| GlickoOpponent {
                rating: row.champion.rating,
                deviation: row.champion.deviation,
                weight,
            })
            .collect::<Vec<_>>();
        let winner = current.win_status == "Winner";
        let queue_post = glicko_update(current.queue, &queue, if winner { 1.0 } else { 0.0 })?;
        let champion_post =
            glicko_update(current.champion, &champion, if winner { 1.0 } else { 0.0 })?;
        changes.push(Change {
            player_id: current.player_id,
            champion_id: current.champion_id,
            match_id,
            queue_id: current.queue_id,
            queue_mu_pre: current.queue.rating,
            queue_phi_pre: current.queue.deviation,
            queue_sigma_pre: current.queue.volatility,
            queue_mu_post: queue_post.rating,
            queue_phi_post: queue_post.deviation,
            queue_sigma_post: queue_post.volatility,
            champ_mu_pre: current.champion.rating,
            champ_phi_pre: current.champion.deviation,
            champ_sigma_pre: current.champion.volatility,
            champ_mu_post: champion_post.rating,
            champ_phi_post: champion_post.deviation,
            champ_sigma_post: champion_post.volatility,
            is_winner: winner,
        });
    }
    Ok(changes)
}

fn player(row: &Row) -> Result<Player, RatingError> {
    Ok(Player {
        player_id: row.get("player_id"),
        champion_id: row.get("champion_id"),
        win_status: row.get("win_status"),
        task_force: row.get("task_force"),
        source: row.get("source"),
        queue_id: row.get("queue_id"),
        winning_task_force: row.get("winning_task_force"),
        roster_count: row.get("roster_count"),
        private_count: row.get("private_count"),
        team_one: row.get("team_one"),
        team_two: row.get("team_two"),
        queue: state(
            row.get("queue_mu"),
            row.get("queue_phi"),
            row.get("queue_sigma"),
        )?,
        champion: state(
            row.get("champ_mu"),
            row.get("champ_phi"),
            row.get("champ_sigma"),
        )?,
    })
}

fn state(
    mu: Option<f64>,
    phi: Option<f64>,
    sigma: Option<f64>,
) -> Result<GlickoState, RatingError> {
    let state = GlickoState {
        rating: mu.unwrap_or(DEFAULT_MU),
        deviation: phi.unwrap_or(DEFAULT_PHI),
        volatility: sigma.unwrap_or(DEFAULT_SIGMA),
    };
    is_valid_state(state)
        .then_some(state)
        .ok_or_else(|| RatingError::InvalidState("stored state is outside safe bounds".to_owned()))
}

fn valid_roster(players: &[Player]) -> bool {
    let first = &players[0];
    let ids = players
        .iter()
        .map(|row| row.player_id)
        .collect::<std::collections::BTreeSet<_>>();
    players.len() <= 10
        && ids.len() == players.len()
        && first.roster_count == 10
        && first.private_count == 10 - i32::try_from(players.len()).unwrap_or_default()
        && first.team_one == 5
        && first.team_two == 5
        && matches!(first.winning_task_force, 1 | 2)
        && players.iter().all(|row| {
            matches!(row.win_status.as_str(), "Winner" | "Loser")
                && matches!(row.task_force, 1 | 2)
                && row.champion_id > 0
                && matches!(row.source.as_str(), "direct" | "recovered")
                && row.queue_id == 486
                && (row.win_status == "Winner") == (row.task_force == first.winning_task_force)
        })
}

async fn apply_changes(
    tx: &Transaction<'_>,
    changes: &[Change],
) -> Result<bool, tokio_postgres::Error> {
    let count = tx
        .query_one(
            "SELECT count(*)::int count FROM match_rating_snapshots WHERE match_id=$1",
            &[&changes[0].match_id],
        )
        .await?
        .get::<_, i32>("count");
    if count > 0 {
        return Ok(false);
    }
    let delta = serde_json::to_value(changes).expect("serialize rating changes");
    tx.execute(
        "INSERT INTO player_queue_ratings(player_id,queue_id,mu,phi,volatility,updated_at) \
         SELECT player_id,queue_id,queue_mu_post,queue_phi_post,queue_sigma_post,now() FROM jsonb_to_recordset($1::jsonb) \
         change(player_id BIGINT,queue_id INT,queue_mu_post DOUBLE PRECISION,queue_phi_post DOUBLE PRECISION,queue_sigma_post DOUBLE PRECISION) \
         ON CONFLICT(player_id,queue_id) DO UPDATE SET mu=EXCLUDED.mu,phi=EXCLUDED.phi,volatility=EXCLUDED.volatility,updated_at=now()",
        &[&delta],
    ).await?;
    tx.execute(
        "INSERT INTO player_champion_ratings(player_id,champion_id,mu,phi,volatility,matches_played,wins,losses,updated_at) \
         SELECT player_id,champion_id,champ_mu_post,champ_phi_post,champ_sigma_post,1,is_winner::int,(NOT is_winner)::int,now() \
         FROM jsonb_to_recordset($1::jsonb) change(player_id BIGINT,champion_id INT,champ_mu_post DOUBLE PRECISION, \
         champ_phi_post DOUBLE PRECISION,champ_sigma_post DOUBLE PRECISION,is_winner BOOLEAN) \
         ON CONFLICT(player_id,champion_id) DO UPDATE SET mu=EXCLUDED.mu,phi=EXCLUDED.phi,volatility=EXCLUDED.volatility, \
         matches_played=player_champion_ratings.matches_played+1,wins=player_champion_ratings.wins+EXCLUDED.wins, \
         losses=player_champion_ratings.losses+EXCLUDED.losses,updated_at=now()",
        &[&delta],
    ).await?;
    tx.execute(
        "INSERT INTO match_rating_snapshots(match_id,player_id,champion_id,queue_mu_pre,queue_phi_pre,queue_mu_post,queue_phi_post, \
         champ_mu_pre,champ_phi_pre,champ_mu_post,champ_phi_post,queue_volatility_pre,queue_volatility_post, \
         champ_volatility_pre,champ_volatility_post,created_at) \
         SELECT match_id,player_id,champion_id,queue_mu_pre,queue_phi_pre,queue_mu_post,queue_phi_post,champ_mu_pre,champ_phi_pre, \
         champ_mu_post,champ_phi_post,queue_sigma_pre,queue_sigma_post,champ_sigma_pre,champ_sigma_post,now() \
         FROM jsonb_to_recordset($1::jsonb) change(match_id BIGINT,player_id BIGINT,champion_id INT,queue_mu_pre DOUBLE PRECISION, \
         queue_phi_pre DOUBLE PRECISION,queue_mu_post DOUBLE PRECISION,queue_phi_post DOUBLE PRECISION,champ_mu_pre DOUBLE PRECISION, \
         champ_phi_pre DOUBLE PRECISION,champ_mu_post DOUBLE PRECISION,champ_phi_post DOUBLE PRECISION,queue_sigma_pre DOUBLE PRECISION, \
         queue_sigma_post DOUBLE PRECISION,champ_sigma_pre DOUBLE PRECISION,champ_sigma_post DOUBLE PRECISION)",
        &[&delta],
    ).await?;
    Ok(true)
}

async fn record_late_arrival(
    tx: &Transaction<'_>,
    match_id: i64,
) -> Result<(), tokio_postgres::Error> {
    tx.execute(
        "INSERT INTO rating_late_match_applications(match_id,entry_datetime,latest_player_cursor_at,policy,created_at) \
         SELECT m.match_id,m.entry_datetime,max(c.last_entry_datetime),'arrival_order_delta',now() FROM matches m \
         JOIN match_players p ON p.match_id=m.match_id AND p.entry_datetime=m.entry_datetime \
         JOIN rating_player_cursors c ON c.queue_id=m.queue_id AND c.player_id=p.player_id \
         WHERE m.match_id=$1 AND c.last_entry_datetime>m.entry_datetime GROUP BY m.match_id,m.entry_datetime \
         ON CONFLICT(match_id) DO NOTHING",
        &[&match_id],
    ).await?;
    Ok(())
}

async fn queue_rebuild(
    tx: &Transaction<'_>,
    match_id: i64,
    reason: &str,
) -> Result<(), tokio_postgres::Error> {
    tx.execute(
        "INSERT INTO rating_rebuild_requests(request_key,earliest_entry_datetime,reason,requested_at) \
         SELECT 'global',entry_datetime,$2,now() FROM matches WHERE match_id=$1 ON CONFLICT(request_key) DO UPDATE SET \
         earliest_entry_datetime=LEAST(rating_rebuild_requests.earliest_entry_datetime,EXCLUDED.earliest_entry_datetime), \
         reason=EXCLUDED.reason,requested_at=now()",
        &[&match_id, &reason],
    ).await?;
    Ok(())
}

async fn mark_stage(tx: &Transaction<'_>, match_id: i64) -> Result<(), tokio_postgres::Error> {
    tx.execute(
        "UPDATE match_ingest_status SET completed_stages=(SELECT array_agg(DISTINCT stage ORDER BY stage) \
         FROM unnest(completed_stages||ARRAY['ratings']::TEXT[])stage),updated_at=now() \
         WHERE match_id=$1 AND population='ranked'",
        &[&match_id],
    ).await?;
    Ok(())
}

fn round(value: f64, places: i32) -> f64 {
    let scale = 10_f64.powi(places);
    (value * scale).round() / scale
}

fn integer(row: &Value, key: &str) -> Option<i64> {
    row.get(key)
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_global_rebuild_freezes_only_incremental_updates() {
        assert!(should_defer_incremental(false, true));
        assert!(!should_defer_incremental(true, true));
        assert!(!should_defer_incremental(false, false));
    }

    #[test]
    fn weighted_team_is_one_event() {
        let state = GlickoState {
            rating: 1_500.0,
            deviation: 350.0,
            volatility: 0.06,
        };
        let one = glicko_update(
            state,
            &[GlickoOpponent {
                rating: 1_500.0,
                deviation: 350.0,
                weight: 1.0,
            }],
            1.0,
        )
        .unwrap();
        let five = glicko_update(
            state,
            &[GlickoOpponent {
                rating: 1_500.0,
                deviation: 350.0,
                weight: 0.2,
            }; 5],
            1.0,
        )
        .unwrap();
        assert_eq!(five, one);
    }
}
