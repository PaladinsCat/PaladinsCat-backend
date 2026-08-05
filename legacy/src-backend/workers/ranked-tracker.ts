import { query, one } from '../config/db';
import { getLeagueLeaderboard, getLeagueSeasons } from '../services/hirez';

/**
 * Ranked Tracker Worker
 *
 * Every 4 hours: fetch league leaderboard for tiers 21-26 (Diamond 5 to Master),
 * record player positions, compute trend and tier_change from prev_rank/prev_tier.
 *
 * Tier mapping: 21=D5, 22=D4, 23=D3, 24=D2, 25=D1, 26=Master
 *
 * Uses leaderboard_current (UNIQUE player_id) — one row per player.
 * On tier change, prev_tier is set so we can track promotions/demotions.
 */

/**
 * Strip null bytes from strings (Hi-Rez API sometimes includes them)
 */
type TierFetchResult = {
  tier: number;
  count: number;
  failed: boolean;
  error?: string;
};

function sanitize(s: string): string {
  return s.replace(/\x00/g, '');
}

/**
 * Get current season for a queue
 */
async function getCurrentSeason(queueId: number): Promise<number> {
  try {
    const data = await getLeagueSeasons(queueId, 'ranked_leaderboard_tracker');
    if (Array.isArray(data) && data.length > 0) {
      return Number(data[0].Season || data[0].season || data[0].Id || data[0].id || 0);
    }
    return 0;
  } catch (err) {
    console.error(`[RANKED-TRACKER] Failed to get season for queue ${queueId}: ${err}`);
    return 0;
  }
}

/**
 * Fetch and store leaderboard for a single tier
 */
async function fetchTier(queueId: number, tier: number, season: number): Promise<TierFetchResult> {
  try {
    const data = await getLeagueLeaderboard(queueId, tier, season, 'ranked_leaderboard_tracker');

    if (!Array.isArray(data) || data.length === 0) {
      console.log(`[RANKED-TRACKER] No data for tier ${tier}, season ${season}`);
      return { tier, count: 0, failed: false };
    }

    // ----------------------------------------------------------------
    // Batch optimization: pre-fetch ALL previous ranks in ONE query,
    // then parallelize upserts via Promise.all.
    // Old code: sequential SELECT + INSERT per player = 1,000+ round-trips
    // for ~500 players (6 tiers × 500 players = 6,000 queries every 4h).
    // This completely locked up the worker and stalled other cron jobs.
    // New: 1 SELECT for all previous ranks + batched INSERTs.
    // Source: User report 2026-06-01 — "N+1 query choke in fetchTier():
    //   1,000 sequential queries per tier locks up the worker."
    // ----------------------------------------------------------------

    // Step 1: Extract all player IDs from this tier's API response.
    const playerIds = data.map((e: any) => Number(e.ActivePlayerId || e.playerId || e.player_id || 0)).filter((id: number) => id > 0);

    // Step 2: Pre-fetch previous ranks for ALL players in a single query.
    const prevResults = playerIds.length > 0
      ? await query(`SELECT player_id, rank, tier FROM leaderboard_current WHERE player_id = ANY($1)`, [playerIds])
      : [];
    const prevMap = new Map(prevResults.map((r: any) => [Number(r.player_id), r]));

    let count = 0;
    const upsertJobs: Array<() => Promise<void | null>> = [];

    for (let i = 0; i < data.length; i++) {
      const entry = data[i];
      const player_id = Number(entry.ActivePlayerId || entry.playerId || entry.player_id || 0);
      const player_name = sanitize(entry.Name || entry.name || '');
      const points = Number(entry.Points || entry.points || 0);
      const wins = Number(entry.Wins || entry.wins || 0);
      const losses = Number(entry.Losses || entry.losses || 0);
      const leaves = Number(entry.Leaves || entry.leaves || 0);

      if (!player_id) continue;

      const rank = i + 1;
      const prev = prevMap.get(player_id);
      const prev_rank = prev?.rank ?? 0;
      const prev_tier = prev?.tier ?? null;
      const trend = prev_rank > 0 ? prev_rank - rank : 0;
      const tier_change = prev_tier != null ? tier - prev_tier : 0;

      // Queue an upsert job. We intentionally do not fire every upsert at
      // once: leaderboard responses can contain hundreds of players, and an
      // unbounded Promise.all can occupy the entire shared pg pool while ingest
      // workers are trying to drain raw_ingest_buffer. The bounded flush below
      // keeps this background job polite.
      upsertJobs.push(() => one(`
        INSERT INTO leaderboard_current (player_id, name, tier, points, rank, prev_rank, prev_tier, trend, tier_change, wins, losses, leaves, season, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, now())
        ON CONFLICT (player_id)
        DO UPDATE SET
          name = $2,
          tier = $3,
          points = $4,
          rank = $5,
          prev_rank = $6,
          prev_tier = $7,
          trend = $8,
          tier_change = $9,
          wins = $10,
          losses = $11,
          leaves = $12,
          season = $13,
          updated_at = now()
      `, [player_id, player_name, tier, points, rank, prev_rank, prev_tier, trend, tier_change, wins, losses, leaves, season]));

      count++;
    }

    const UPSERT_CONCURRENCY = 10;
    for (let i = 0; i < upsertJobs.length; i += UPSERT_CONCURRENCY) {
      await Promise.all(upsertJobs.slice(i, i + UPSERT_CONCURRENCY).map(job => job()));
    }

    console.log(`[RANKED-TRACKER] Tier ${tier}: ${count} players recorded (season ${season})`);
    return { tier, count, failed: false };
  } catch (err) {
    console.error(`[RANKED-TRACKER] Failed to fetch tier ${tier}: ${err}`);
    return { tier, count: 0, failed: true, error: err instanceof Error ? err.message : String(err) };
  }
}

/**
 * Main: fetch all tiers 21-26
 */
export async function track(): Promise<number> {
  const jobName = 'ranked_tracker';
  const job = await one<{ id: number }>(`
    INSERT INTO sync_jobs (job_type, status, started_at)
    VALUES ($1, 'running', now())
    RETURNING id
  `, [jobName]);

  let total = 0;
  let error: string | null = null;
  try {
    const season = await getCurrentSeason(486);
    if (!season) {
      error = 'Could not determine current season';
      console.log(`[RANKED-TRACKER] ${error}`);
    } else {
      const failedTiers: TierFetchResult[] = [];
      for (const tier of [21, 22, 23, 24, 25, 26]) {
        const result = await fetchTier(486, tier, season);
        total += result.count;
        if (result.failed) failedTiers.push(result);
      }

      if (failedTiers.length > 0) {
        error = `Failed tiers: ${failedTiers.map(result => `${result.tier}${result.error ? ` (${result.error})` : ''}`).join(', ')}`;
      } else if (total === 0) {
        // A completely empty high-tier leaderboard is more likely an upstream
        // API problem than a legitimate result. Marking the job failed keeps
        // startup catch-up eligible instead of treating an outage as a healthy
        // completed sync.
        error = 'No leaderboard players returned across tiers 21-26';
      }

      // Log tier changes
      const changes = await query(`
        SELECT name, prev_tier, tier, tier_change
        FROM leaderboard_current
        WHERE tier_change != 0
        ORDER BY tier_change DESC
      `);
      if (changes.length > 0) {
        console.log(`[RANKED-TRACKER] Tier changes: ${changes.length} players`);
        for (const c of changes) {
          const dir = c.tier_change > 0 ? 'promoted' : 'demoted';
          console.log(`  ${c.name}: tier ${c.prev_tier} → ${c.tier} (${dir})`);
        }
      }

      console.log(`[RANKED-TRACKER] Complete. Total: ${total} players across 6 tiers (season ${season})`);
    }
  } catch (err) {
    error = String(err);
    console.error(`[RANKED-TRACKER] Fatal error: ${error}`);
  } finally {
    // CRITICAL: Wrap the sync_jobs INSERT in try-catch. If this query fails
    // (DB disconnect, schema change), the error swallows the original error
    // from the try block. The original error was the real failure; losing it
    // makes debugging impossible. Now we log the original error regardless.
    // Source: Fault #12 — "Error swallowed in finally block"
    try {
      if (job?.id) {
        await query(`
          UPDATE sync_jobs
          SET status = $2,
              completed_at = now(),
              players_processed = $3,
              error_message = $4
          WHERE id = $1
        `, [job.id, error ? 'failed' : 'completed', total, error]);
      } else {
        await query(`
          INSERT INTO sync_jobs (job_type, status, started_at, completed_at, players_processed, error_message)
          VALUES ($1, $2, now(), now(), $3, $4)
        `, [jobName, error ? 'failed' : 'completed', total, error]);
      }
    } catch (syncErr) {
      console.error(`[RANKED-TRACKER] Failed to log sync job: ${syncErr}`);
    }
  }
  return total;
}

/**
 * Get top N players for a tier with trend
 */
export async function getTopPlayers(tier: number, limit = 50): Promise<any[]> {
  const result = await query(`
    SELECT *,
      CASE
        WHEN prev_rank IS NULL THEN 0
        ELSE prev_rank - rank
      END AS trend
    FROM leaderboard_current
    WHERE tier = $1
    ORDER BY points DESC
    LIMIT $2
  `, [tier, limit]);

  return result;
}

/**
 * Get all tracked players sorted by tier then points
 */
export async function getAllPlayers(limit = 1000): Promise<any[]> {
  const result = await query(`
    SELECT *
    FROM leaderboard_current
    ORDER BY tier ASC, points DESC
    LIMIT $1
  `, [limit]);

  return result;
}
