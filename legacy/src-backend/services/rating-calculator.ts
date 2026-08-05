/**
 * =====================================================================
 * rating-calculator.ts — Glicko-2 Rating Engine (Production)
 * =====================================================================
 * Purpose: The actual rating calculation engine used in production.
 * Computes queue-level and champion-level Glicko-2 ratings for every
 * player in a match, then persists results to PostgreSQL tables.
 * This is the file that processes live match data — NOT glicko2.ts
 * (which is the standalone reference implementation, fixed 2026-05-27).
 *
 * Core concepts:
 * - Queue rating: Player's skill for a specific queue (e.g., 5v5, Control).
 *   Full impact per match. Stored in player_queue_ratings.
 * - Champion rating: Player's skill for a specific champion. It uses the same
 *   Glicko-2 update helper as queue rating, but stores state independently in
 *   player_champion_ratings and tracks per-champion matches/wins/losses.
 * - Rating snapshot: Every match's pre/post ratings saved to
 *   match_rating_snapshots for auditing and historical queries.
 *
 * Architecture:
 * - calculateRatingChanges(): Fetches match data from DB, computes all
 *   rating changes using glicko2Update(). Returns array of RatingChange.
 * - applyRatingChanges(): Persists changes to 3 tables (queue, champion,
 *   snapshots) using upsert (ON CONFLICT DO UPDATE). Atomic per player.
 * - reingestRatings(): Re-processes all historical matches. Validates
 *   each match (10 players, 5 winners, no null champions) before processing.
 * - glicko2Update(): Internal Glicko-2 math now lives in glicko2.ts; this
 *   service guards DB-derived inputs before delegating to that helper.
 *
 * Called by:
 * - workers/rating-ingestion.ts — processes new matches via calculate+apply.
 * - scripts/run-pipeline.ts — re-ingests historical matches.
 * - routes/ratings.ts — /api/ratings endpoint uses calculateRatingChanges.
 *
 * Fixed 2026-05-27:
 * - applyRatingChanges now uses computed sigma (queueSigmaPost, champSigmaPost)
 *   instead of hardcoded DEFAULT_SIGMA (0.06). Volatility now updates correctly.
 * - Champion ON CONFLICT updates matches_played, wins, losses (was missing).
 *
 * Fixed 2026-05-30:
 * - Removed 100+ lines of duplicated Glicko-2 math (missing G², fabricated
 *   volatility, no scale conversion). Now imports glickoUpdate() from glicko2.ts
 *   which is the spec-compliant, verified implementation.
 * - Opponent arrays now use { rating, deviation } to match glicko2.ts interface.
 *
 * Fixed 2026-05-30 (final):
 * - reingestRatings(): Added TRUNCATE TABLE before the chronological replay loop.
 *   Without this, LEFT JOIN pulls current (2026) ratings for historical (2024) matches,
 *   calculating wrong deltas and permanently corrupting the timeline.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import { PoolClient } from 'pg';
import { query, transaction } from '../config/db';
import { glickoUpdate, GlickoState, isValidGlickoState, RATING_LIMITS } from './glicko2';
import { rebuildBestChampionRatingProjection } from './performance-projections';

// ──────────────────────────────────────────────
// Defaults: starting values for new players/champions.
// mu=1500 (neutral skill), phi=350 (high uncertainty), sigma=0.06 (low volatility).
// G converts rating difference to log10 scale for probability calculation.
// ──────────────────────────────────────────────
const DEFAULT_MU = 1500;
const DEFAULT_PHI = 350;
const DEFAULT_SIGMA = 0.06;
const G = Math.log(10) / 400; // ≈ 0.005756

/**
 * RatingChange: Complete before/after rating data for one player in one match.
 * Contains queue and champion ratings (pre/post), winner status, and match info.
 * Used by applyRatingChanges() to persist to PostgreSQL.
 */
export interface RatingChange {
  playerId: number;
  championId: number;
  matchId: number;
  queueId: number;

  queueMuPre: number;
  queuePhiPre: number;
  queueSigmaPre: number;
  queueMuPost: number;
  queuePhiPost: number;
  queueSigmaPost: number;

  champMuPre: number;
  champPhiPre: number;
  champSigmaPre: number;
  champMuPost: number;
  champPhiPost: number;
  champSigmaPost: number;

  isWinner: boolean;
}

// ──────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────

/**
 * Safe fallback — treats null, undefined, NaN, and Infinity as invalid.
 * PostgreSQL returns NUMERIC as strings, so we coerce with Number() first.
 */
function stateValue(val: any, fallback: number, label: string): number {
  if (val === null || val === undefined) {
    return fallback;
  }
  const num = Number(val);
  if (!Number.isFinite(num)) {
    throw new Error(`Glicko-2: stored ${label} must be finite, got ${val}`);
  }
  return num;
}

function ratingStateFromRow(row: any, prefix: 'queue' | 'champ'): GlickoState {
  const state: GlickoState = {
    rating: stateValue(row[`${prefix}_mu`], DEFAULT_MU, `${prefix} mu`),
    deviation: stateValue(row[`${prefix}_phi`], DEFAULT_PHI, `${prefix} phi`),
    volatility: stateValue(row[`${prefix}_sigma`], DEFAULT_SIGMA, `${prefix} volatility`),
  };
  if (!isValidGlickoState(state)) {
    throw new Error(
      `Glicko-2: stored ${prefix} state is outside safe bounds `
      + `(mu=${state.rating}, phi=${state.deviation}, sigma=${state.volatility}; `
      + `limits mu=${RATING_LIMITS.minRating}-${RATING_LIMITS.maxRating}, `
      + `phi=${RATING_LIMITS.minDeviation}-${RATING_LIMITS.maxDeviation}, `
      + `sigma=${RATING_LIMITS.minVolatility}-${RATING_LIMITS.maxVolatility})`,
    );
  }
  return state;
}

type QueryRows = (text: string, params?: any[]) => Promise<any[]>;

async function clientQueryRows(client: PoolClient, text: string, params?: any[]): Promise<any[]> {
  const result = await client.query(text, params);
  return result.rows;
}

function validateRatingRows(matchId: number, players: any[]): string | null {
  // A complete ranked roster can expose fewer than ten rateable identities when
  // one or more accounts are private. Private rows (detailed or placeholder)
  // are filtered by the query; every identified player still receives W/L.
  if (players.length < 1 || players.length > 10) {
    return `expected 1 to 10 authoritative identified players, got ${players.length}`;
  }
  const logicalRosterCount = Number(players[0]?.logical_roster_count || 0);
  const privateParticipantCount = Number(players[0]?.private_participant_count || 0);
  if (logicalRosterCount !== 10 || privateParticipantCount !== 10 - players.length) {
    return `expected a 10-row logical roster with ${10 - players.length} private participant(s), got roster=${logicalRosterCount}, private=${privateParticipantCount}`;
  }
  const logicalTeamOne = Number(players[0]?.logical_team_one || 0);
  const logicalTeamTwo = Number(players[0]?.logical_team_two || 0);
  if (logicalTeamOne !== 5 || logicalTeamTwo !== 5) {
    return `expected a logical 5v5 roster, got ${logicalTeamOne}v${logicalTeamTwo}`;
  }

  const uniquePlayers = new Set(players.map((p: any) => Number(p.player_id)));
  if (uniquePlayers.size !== players.length) {
    return `expected ${players.length} unique players, got ${uniquePlayers.size}`;
  }

  const winners = players.filter((p: any) => p.win_status === 'Winner').length;
  const losers = players.filter((p: any) => p.win_status === 'Loser').length;
  if (winners + losers !== players.length || winners > 5 || losers > 5) {
    return `invalid identified-player outcome split winners=${winners}, losers=${losers}`;
  }

  const teamOne = players.filter((p: any) => Number(p.task_force) === 1).length;
  const teamTwo = players.filter((p: any) => Number(p.task_force) === 2).length;
  if (teamOne + teamTwo !== players.length || teamOne > 5 || teamTwo > 5) {
    return `invalid identified players per task_force team1=${teamOne}, team2=${teamTwo}`;
  }

  const winningTaskForce = Number(players[0]?.winning_task_force || 0);
  if (winningTaskForce !== 1 && winningTaskForce !== 2) {
    return `match ${matchId} has invalid winning_task_force ${winningTaskForce}`;
  }
  const inconsistentOutcome = players.find((player: any) => (
    (player.win_status === 'Winner') !== (Number(player.task_force) === winningTaskForce)
  ));
  if (inconsistentOutcome) {
    return `player ${inconsistentOutcome.player_id} outcome conflicts with winning_task_force ${winningTaskForce}`;
  }

  const invalidChampion = players.find((p: any) => !Number.isFinite(Number(p.champion_id)) || Number(p.champion_id) <= 0);
  if (invalidChampion) {
    return `player ${invalidChampion.player_id} has invalid champion_id ${invalidChampion.champion_id}`;
  }

  const invalidSource = players.find((p: any) => !['direct', 'recovered'].includes(String(p.source || 'direct')));
  if (invalidSource) {
    return `player ${invalidSource.player_id} has non-authoritative source ${invalidSource.source}`;
  }

  const invalidQueue = players.find((p: any) => Number(p.queue_id) !== 486);
  if (invalidQueue) {
    return `match ${matchId} is not ranked queue 486`;
  }

  return null;
}

// ──────────────────────────────────────────────
// Glicko-2 math — imported from verified glicko2.ts
// ──────────────────────────────────────────────
// The spec-compliant Glicko-2 algorithm lives in glicko2.ts.
// We import it here to avoid duplicating math. The old duplicated
// implementation (missing G², fabricated volatility, no scale conversion)
// was mathematically corrupt and caused incorrect rating updates.
// glicko2.ts handles internal scale conversion internally.
// Source: Audit 2026-05-30 — "Rogue Math Duplication"
// ──────────────────────────────────────────────

// ──────────────────────────────────────────────
// Main functions
// ──────────────────────────────────────────────

async function calculateRatingChangesWithQuery(matchId: number, queryRows: QueryRows): Promise<RatingChange[]> {
  const players = await queryRows(
    `SELECT
       mp.player_id,
       mp.champion_id,
       -- Direct getmatchdetailsbatch rows usually store Winner/Loser, while
       -- recovered getmatchhistory rows can store Win/Loss. Ratings should not
       -- care which Hi-Rez endpoint supplied the authoritative row, so normalize
       -- the outcome at the query boundary before validation and Glicko math.
       CASE
         WHEN mp.win_status IN ('Winner', 'Win') THEN 'Winner'
         WHEN mp.win_status IN ('Loser', 'Loss') THEN 'Loser'
         ELSE mp.win_status
       END AS win_status,
       mp.task_force,
       COALESCE(mp.source, 'direct') AS source,
       m.queue_id,
       m.winning_task_force,
       (SELECT COUNT(*)::INT
        FROM match_players roster
        WHERE roster.match_id = m.match_id
          AND roster.entry_datetime = m.entry_datetime) AS logical_roster_count,
       (SELECT COUNT(*)::INT
        FROM match_players roster
        WHERE roster.match_id = m.match_id
          AND roster.entry_datetime = m.entry_datetime
          AND roster.player_id = 0
          AND upper(COALESCE(roster.player_name, '')) = 'PRIVATEACCOUNT') AS private_participant_count,
       (SELECT COUNT(*)::INT
        FROM match_players roster
        WHERE roster.match_id = m.match_id
          AND roster.entry_datetime = m.entry_datetime
          AND roster.task_force = 1) AS logical_team_one,
       (SELECT COUNT(*)::INT
        FROM match_players roster
        WHERE roster.match_id = m.match_id
          AND roster.entry_datetime = m.entry_datetime
          AND roster.task_force = 2) AS logical_team_two,
      pqr.mu as queue_mu, pqr.phi as queue_phi, pqr.volatility as queue_sigma,
      pcr.mu as champ_mu, pcr.phi as champ_phi, pcr.volatility as champ_sigma
     FROM matches m
     JOIN match_players mp ON mp.match_id = m.match_id AND mp.entry_datetime = m.entry_datetime
     LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
     LEFT JOIN player_queue_ratings pqr ON pqr.player_id = mp.player_id AND pqr.queue_id = m.queue_id
     LEFT JOIN player_champion_ratings pcr ON pcr.player_id = mp.player_id AND pcr.champion_id = mp.champion_id
     WHERE m.match_id = $1
       AND m.queue_id = 486
       AND COALESCE(m.limited, false) = false
       AND COALESCE(m.is_ranked, m.queue_id = 486) = true
       -- The facts-first worker marks a match partial immediately after the
       -- normalized roster and bans become durable, before derived stages such
       -- as ratings run. Treat that state as eligible here; the full-roster,
       -- source, queue, outcome, and limited-match guards below still prevent
       -- incomplete evidence from changing rating state.
       AND COALESCE(mis.status, 'complete') IN ('processing', 'partial', 'complete')
       AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
       AND mp.player_id > 0
       AND mp.champion_id > 0
       AND mp.task_force IN (1, 2)
       AND mp.win_status IN ('Winner', 'Loser', 'Win', 'Loss')
     ORDER BY mp.task_force, mp.player_id`,
    [matchId]
  );

  if (players.length === 0) return [];

  // Rating eligibility is intentionally checked inside the rating service, not
  // only in the buffer worker. During the 2026-06 recovery incident, history
  // observations and partial broken-match rows could masquerade as match facts.
  // The buffer worker now blocks those rows, but manual replays and future
  // callers still need this boundary: Glicko-2 should only see one complete,
  // authoritative ranked match with a balanced logical roster. Any number of
  // private participants may occupy the remaining slots; only identified rows
  // enter rating state.
  const validationIssue = validateRatingRows(matchId, players);
  if (validationIssue) {
    console.warn(`[rating-calc] Match ${matchId} is not rating-eligible: ${validationIssue}`);
    return [];
  }

  const changes: RatingChange[] = [];
  const queueId = players[0]?.queue_id || 486;

  for (const p of players) {
    const isWinner = p.win_status === 'Winner';

    // CRITICAL: Validate task_force. Paladins uses task_force 1 or 2. A value
    // of 0 means the field is missing/corrupted. If all players have task_force=0,
    // every player sees task_force=0 for themselves and all opponents, the filter
    // on line 161 excludes everyone, opponents[] is empty, and glickoUpdate returns
    // inactivity expansion only (phi increases, rating doesn't change). This silently
    // corrupts the match — 10 players get no rating change because team data is missing.
    // Fix: skip players with invalid task_force. Log a warning so the issue is visible.
    // Source: Debug 2026-05-31 — "task_force=0 trap"
    const myTeam = p.task_force || 0;
    if (myTeam !== 1 && myTeam !== 2) {
      console.warn(`[rating-calc] Skipping player ${p.player_id} in match ${matchId}: invalid task_force ${myTeam}`);
      continue;
    }

    const queueState = ratingStateFromRow(p, 'queue');
    const champState = ratingStateFromRow(p, 'champ');
    const queueMuPre = queueState.rating;
    const queuePhiPre = queueState.deviation;
    const queueSigmaPre = queueState.volatility;
    const champMuPre = champState.rating;
    const champPhiPre = champState.deviation;
    const champSigmaPre = champState.volatility;

    // Opponents — filter by task_force. Only include players from the opposing team.
    const opponents = players.filter((pp: any) => (pp.task_force || 0) !== myTeam);
    if (opponents.length < 1 || opponents.length > 5) {
      console.warn(`[rating-calc] Skipping match ${matchId}: player ${p.player_id} sees ${opponents.length} opponents`);
      return [];
    }

    // glickoUpdate expects opponents as { rating, deviation }[], not { mu, phi }.
    // A team result is one rating event, not five independent 1v1 games.
    // Split that event evenly across each known opponent so expected-score and
    // variance sums retain a total weight of one.
    const opponentWeight = 1 / opponents.length;
    const queueOpponents = opponents.map((o: any) => ({
      rating: ratingStateFromRow(o, 'queue').rating,
      deviation: ratingStateFromRow(o, 'queue').deviation,
      weight: opponentWeight,
    }));

    const champOpponents = opponents.map((o: any) => ({
      rating: ratingStateFromRow(o, 'champ').rating,
      deviation: ratingStateFromRow(o, 'champ').deviation,
      weight: opponentWeight,
    }));

    // Queue Glicko-2 — use imported spec-compliant implementation.
    // glickoUpdate expects GlickoState { rating, deviation, volatility } and
    // opponents { rating, deviation }[]. Outcome is 'win' or 'loss'.
    // Source: Audit 2026-05-30 — "Rogue Math Duplication"
    const queueResult = glickoUpdate(queueState, queueOpponents, isWinner ? 'win' : 'loss');

    // Champion Glicko-2 — same update for now.
    // NOTE: The old code used epsilonScale=0.5 for half impact. glicko2.ts
    // doesn't yet support epsilonScale. If champion ratings need half-speed
    // updates, add the parameter to glicko2.ts later.
    const champResult = glickoUpdate(champState, champOpponents, isWinner ? 'win' : 'loss');

    changes.push({
      playerId: p.player_id,
      championId: p.champion_id,
      matchId,
      queueId,
      queueMuPre,
      queuePhiPre,
      queueSigmaPre,
      queueMuPost: Math.round(queueResult.rating * 100) / 100,
      queuePhiPost: Math.round(queueResult.deviation * 100) / 100,
      queueSigmaPost: Math.round(queueResult.volatility * 10000) / 10000,
      champMuPre,
      champPhiPre,
      champSigmaPre,
      champMuPost: Math.round(champResult.rating * 100) / 100,
      champPhiPost: Math.round(champResult.deviation * 100) / 100,
      champSigmaPost: Math.round(champResult.volatility * 10000) / 10000,
      isWinner,
    });
  }

  if (changes.length !== players.length) {
    console.warn(`[rating-calc] Match ${matchId} produced ${changes.length} changes for ${players.length} eligible players; skipping rating apply`);
    return [];
  }

  return changes;
}

export async function calculateRatingChanges(matchId: number): Promise<RatingChange[]> {
  return calculateRatingChangesWithQuery(matchId, query);
}

async function applyRatingChangesInTransaction(client: PoolClient, changes: RatingChange[]): Promise<boolean> {
  if (changes.length === 0) return false;

  // CRITICAL: Wrap all rating updates in a single transaction. Without this,
  // if the function fails mid-way (e.g., on player 5 of 10), players 1-4 have
  // updated post-match ratings while players 5-10 still have pre-match ratings.
  // The match is permanently in an inconsistent state — future matches use
  // wrong baselines for some players but not others. The snapshot table is
  // also partially written, making historical queries unreliable.
  // Fix: single transaction ensures all-or-nothing semantics. On any failure,
  // ROLLBACK reverts everything and the error propagates to the caller.
  // Source: Debug 2026-05-31 — "No transaction in applyRatingChanges"
  const matchId = changes[0].matchId;
  const existingSnapshots = await client.query(
      `SELECT COUNT(*)::int AS count FROM match_rating_snapshots WHERE match_id = $1`,
      [matchId],
    );
  if (Number(existingSnapshots.rows[0]?.count || 0) > 0) {
      // Rating snapshots are the immutable per-match audit trail. If snapshots
      // already exist, this match has already affected player/champion rating
      // state. Reapplying would overwrite the snapshots with equivalent values
      // but also increment champion `matches_played/wins/losses` again through
      // the upsert below. The buffer worker also tracks a rating stage, but this
      // guard lives inside the rating service so manual replays and future
      // callers get the same idempotency protection.
      console.warn(`[rating-calc] Match ${matchId} already has rating snapshots; skipping duplicate apply`);
    return false;
  }

  const delta = JSON.stringify(changes.map((change) => ({
    player_id: change.playerId,
    champion_id: change.championId,
    match_id: change.matchId,
    queue_id: change.queueId,
    queue_mu_pre: change.queueMuPre,
    queue_phi_pre: change.queuePhiPre,
    queue_sigma_pre: change.queueSigmaPre,
    queue_mu_post: change.queueMuPost,
    queue_phi_post: change.queuePhiPost,
    queue_sigma_post: change.queueSigmaPost,
    champ_mu_pre: change.champMuPre,
    champ_phi_pre: change.champPhiPre,
    champ_sigma_pre: change.champSigmaPre,
    champ_mu_post: change.champMuPost,
    champ_phi_post: change.champPhiPost,
    champ_sigma_post: change.champSigmaPost,
    is_winner: change.isWinner,
  })));

  // The roster is one rating event. Persist it as three set-based statements
  // instead of 3*N network round trips while retaining the transaction and
  // immutable-snapshot guard above.
  await client.query(`
    INSERT INTO player_queue_ratings (player_id,queue_id,mu,phi,volatility,updated_at)
    SELECT player_id,queue_id,queue_mu_post,queue_phi_post,queue_sigma_post,now()
    FROM jsonb_to_recordset($1::jsonb) AS change(
      player_id BIGINT,queue_id INT,queue_mu_post DOUBLE PRECISION,
      queue_phi_post DOUBLE PRECISION,queue_sigma_post DOUBLE PRECISION
    )
    ON CONFLICT (player_id,queue_id) DO UPDATE SET
      mu=EXCLUDED.mu,phi=EXCLUDED.phi,volatility=EXCLUDED.volatility,updated_at=now()
  `, [delta]);

  await client.query(`
    INSERT INTO player_champion_ratings (
      player_id,champion_id,mu,phi,volatility,matches_played,wins,losses,updated_at
    )
    SELECT player_id,champion_id,champ_mu_post,champ_phi_post,champ_sigma_post,
      1,CASE WHEN is_winner THEN 1 ELSE 0 END,CASE WHEN is_winner THEN 0 ELSE 1 END,now()
    FROM jsonb_to_recordset($1::jsonb) AS change(
      player_id BIGINT,champion_id INT,champ_mu_post DOUBLE PRECISION,
      champ_phi_post DOUBLE PRECISION,champ_sigma_post DOUBLE PRECISION,is_winner BOOLEAN
    )
    ON CONFLICT (player_id,champion_id) DO UPDATE SET
      mu=EXCLUDED.mu,phi=EXCLUDED.phi,volatility=EXCLUDED.volatility,
      matches_played=player_champion_ratings.matches_played+1,
      wins=player_champion_ratings.wins+EXCLUDED.wins,
      losses=player_champion_ratings.losses+EXCLUDED.losses,updated_at=now()
  `, [delta]);

  await client.query(`
    INSERT INTO match_rating_snapshots (
      match_id,player_id,champion_id,
      queue_mu_pre,queue_phi_pre,queue_mu_post,queue_phi_post,
      champ_mu_pre,champ_phi_pre,champ_mu_post,champ_phi_post,
      queue_volatility_pre,queue_volatility_post,
      champ_volatility_pre,champ_volatility_post,created_at
    )
    SELECT match_id,player_id,champion_id,
      queue_mu_pre,queue_phi_pre,queue_mu_post,queue_phi_post,
      champ_mu_pre,champ_phi_pre,champ_mu_post,champ_phi_post,
      queue_sigma_pre,queue_sigma_post,champ_sigma_pre,champ_sigma_post,now()
    FROM jsonb_to_recordset($1::jsonb) AS change(
      match_id BIGINT,player_id BIGINT,champion_id INT,
      queue_mu_pre DOUBLE PRECISION,queue_phi_pre DOUBLE PRECISION,
      queue_mu_post DOUBLE PRECISION,queue_phi_post DOUBLE PRECISION,
      champ_mu_pre DOUBLE PRECISION,champ_phi_pre DOUBLE PRECISION,
      champ_mu_post DOUBLE PRECISION,champ_phi_post DOUBLE PRECISION,
      queue_sigma_pre DOUBLE PRECISION,queue_sigma_post DOUBLE PRECISION,
      champ_sigma_pre DOUBLE PRECISION,champ_sigma_post DOUBLE PRECISION
    )
  `, [delta]);
  return true;
}

export async function applyRatingChanges(changes: RatingChange[]): Promise<void> {
  console.log(`applyRatingChanges: ${changes.length} changes to apply`);
  if (changes.length === 0) return;
  await transaction(async (client) => {
    await client.query(`SELECT pg_advisory_xact_lock(4860001)`);
    await applyRatingChangesInTransaction(client, changes);
  });
}

export type RatingApplicationResult = 'applied' | 'skipped' | 'deferred' | 'busy';

async function queueRatingRebuild(client: PoolClient, matchId: number, reason: string): Promise<void> {
  await client.query(`
    INSERT INTO rating_rebuild_requests (request_key, earliest_entry_datetime, reason, requested_at)
    SELECT 'global', m.entry_datetime, $2, now()
    FROM matches m
    WHERE m.match_id = $1
    ON CONFLICT (request_key) DO UPDATE SET
      earliest_entry_datetime = LEAST(rating_rebuild_requests.earliest_entry_datetime, EXCLUDED.earliest_entry_datetime),
      reason = EXCLUDED.reason,
      requested_at = now()
  `, [matchId, reason]);
}

/**
 * Rate a match under a transaction-scoped advisory lock. Calculation must occur
 * after the lock is acquired; otherwise two workers can calculate from the
 * same old state and the later commit silently overwrites the earlier one.
 */
export async function calculateAndApplyRatingChanges(
  matchId: number,
  options: { rebuilding?: boolean } = {},
): Promise<RatingApplicationResult> {
  return transaction(async (client) => {
    // Match facts are processed in parallel, while rating state is one ordered
    // cumulative stream. A blocking advisory lock made every competing fact
    // lane wait behind the current rating transaction until PostgreSQL's
    // 30-second statement timeout. Use a non-blocking lease instead: the
    // caller retains this match as durable projection debt and retries it
    // without occupying a pool connection or aborting the transaction.
    const ratingLease = await client.query<{ acquired: boolean }>(
      `SELECT pg_try_advisory_xact_lock(4860001) AS acquired`,
    );
    if (ratingLease.rows[0]?.acquired !== true) return 'busy';

    if (!options.rebuilding) {
      // Lateness is a ten-row indexed cursor lookup, not a join across the
      // complete snapshot history. A late match records repair debt for itself;
      // it must not freeze timely rating work for every later match.
      const ordering = await client.query(`
        SELECT EXISTS (
          SELECT 1
          FROM matches current_match
          JOIN match_players current_player
            ON current_player.match_id = current_match.match_id
           AND current_player.entry_datetime = current_match.entry_datetime
          JOIN rating_player_cursors cursor
            ON cursor.queue_id = current_match.queue_id
           AND cursor.player_id = current_player.player_id
          WHERE current_match.match_id = $1
            AND cursor.last_entry_datetime > current_match.entry_datetime
        ) AS is_late
      `, [matchId]);
      if (ordering.rows[0]?.is_late) {
        // Ratings are an online cumulative stream. Replaying the entire rating
        // history for one delayed payload makes every timely match wait and is
        // not operationally bounded. Apply the delayed event against current
        // state, while retaining an explicit audit row for offline analysis.
        await client.query(`
          INSERT INTO rating_late_match_applications (match_id,entry_datetime,latest_player_cursor_at,policy,created_at)
          SELECT current_match.match_id,current_match.entry_datetime,
            MAX(cursor.last_entry_datetime),'arrival_order_delta',now()
          FROM matches current_match
          JOIN match_players current_player
            ON current_player.match_id=current_match.match_id
           AND current_player.entry_datetime=current_match.entry_datetime
          JOIN rating_player_cursors cursor
            ON cursor.queue_id=current_match.queue_id
           AND cursor.player_id=current_player.player_id
          WHERE current_match.match_id=$1
          GROUP BY current_match.match_id,current_match.entry_datetime
          ON CONFLICT (match_id) DO NOTHING
        `, [matchId]);
        console.warn(`[rating-calc] Applying late match ${matchId} as an audited arrival-order rating delta`);
      }
    }

    try {
      const changes = await calculateRatingChangesWithQuery(
        matchId,
        (text, params) => clientQueryRows(client, text, params),
      );
      if (changes.length === 0) return 'skipped';
      const applied = await applyRatingChangesInTransaction(client, changes);
      if (!applied) return 'skipped';

      await client.query(`
        INSERT INTO rating_player_cursors (queue_id,player_id,last_match_id,last_entry_datetime,updated_at)
        SELECT $2::INT, player_id, $1::BIGINT, m.entry_datetime, now()
        FROM unnest($3::bigint[]) AS changed(player_id)
        CROSS JOIN LATERAL (
          SELECT entry_datetime FROM matches WHERE match_id = $1 LIMIT 1
        ) m
        ON CONFLICT (queue_id,player_id) DO UPDATE SET
          last_match_id = EXCLUDED.last_match_id,
          last_entry_datetime = GREATEST(
            rating_player_cursors.last_entry_datetime,
            EXCLUDED.last_entry_datetime
          ),
          updated_at = now()
      `, [matchId, changes[0].queueId, changes.map((change) => change.playerId)]);
      return 'applied';
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      if (!reason.startsWith('Glicko-2:')) throw error;
      await queueRatingRebuild(client, matchId, reason);
      return 'deferred';
    }
  });
}

// ──────────────────────────────────────────────
// Re-ingest: recalculate ratings from existing matches
// ──────────────────────────────────────────────

export async function reingestRatings(): Promise<{
  matchesProcessed: number;
  brokenMatches: Array<{ matchId: number; reason: string }>;
}> {
  const brokenMatches: Array<{ matchId: number; reason: string }> = [];

  // CRITICAL: Wipe current ratings before chronologically replaying matches.
  // Without this, calculateRatingChanges() LEFT JOINs player_queue_ratings and
  // player_champion_ratings — which hold the player's CURRENT (2026) ratings.
  // A 2024 match would pull 2026 baselines, calculate wrong deltas, and overwrite.
  // Historical data is permanently corrupted. TRUNCATE resets to 1500/350 defaults.
  // RESTART IDENTITY resets serial sequences to avoid PK conflicts on re-insert.
  // CASCADE ensures match_rating_snapshots is also wiped (it FK-references the rating tables).
  // Source: Audit 2026-05-30 — "Time Travel Data Corruption"
  console.log('[REINGEST] Truncating rating tables to prevent chronological corruption...');
  await query('TRUNCATE TABLE player_queue_ratings, player_champion_ratings, match_rating_snapshots, rating_player_cursors RESTART IDENTITY CASCADE');

  // CRITICAL: Only process ranked, complete match facts. Private-placeholder
  // matches remain eligible for every identified player; zero identities are
  // filtered inside calculateRatingChanges(). This is
  // deliberately stricter than "there is a row in matches": getmatchhistory
  // observations, partial broken-match recovery rows, and unrecovered broken
  // rows must not affect rating state. calculateRatingChanges() repeats the
  // same checks before doing math so manual callers cannot bypass the boundary.
  const matches = await query(
    `SELECT DISTINCT m.match_id, m.entry_datetime
     FROM matches m
     LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
     WHERE m.queue_id = 486
       AND COALESCE(m.is_ranked, m.queue_id = 486) = true
       AND COALESCE(mis.status, 'complete') = 'complete'
     ORDER BY m.entry_datetime ASC, m.match_id ASC`
  );

  let processed = 0;
  let rebuildDeferred = false;
  for (const m of matches) {
    const matchId = m.match_id;

    try {
      const result = await calculateAndApplyRatingChanges(matchId, { rebuilding: true });
      if (result !== 'applied') {
        rebuildDeferred ||= result === 'deferred' || result === 'busy';
        brokenMatches.push({
          matchId,
          reason: result === 'deferred'
            ? 'Rating update deferred for a chronological rebuild'
            : result === 'busy'
              ? 'Rating stream was busy; retry the rebuild'
              : 'Rating eligibility rejected match',
        });
        continue;
      }
      processed++;
    } catch (err) {
      rebuildDeferred = true;
      brokenMatches.push({
        matchId,
        reason: `Error: ${(err as Error).message}`,
      });
    }
  }

  // A replay can safely clear a deferred-rebuild request only when every
  // calculator error was resolved. Ordinary ineligible matches are reported in
  // brokenMatches but do not themselves require another chronological replay.
  if (!rebuildDeferred) {
    await query('DELETE FROM rating_rebuild_requests');
  }

  await rebuildBestChampionRatingProjection();

  return { matchesProcessed: processed, brokenMatches };
}
