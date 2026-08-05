import { query, one } from '../config/db';
import { dumpRawPayloads, getPlayerLoadouts } from '../services/hirez';

/**
 * Fetch and store player loadouts (beta).
 */
export async function fetchPlayerLoadouts(playerId: number): Promise<any[]> {
  try {
    const loadouts = await getPlayerLoadouts(playerId, 'loadout_tracker');
    if (!Array.isArray(loadouts) || loadouts.length === 0) return [];
    // Stage through the same relay facade used by match discovery instead of
    // inserting into raw_ingest_buffer ad hoc. That keeps request/response
    // observability in one place and lets future relay-side staging dedupe rules
    // cover loadouts without every worker reinventing buffer writes.
    await dumpRawPayloads([{
      endpoint: 'getplayerloadouts',
      entity_type: 'loadout',
      entity_id: playerId,
      raw_data: loadouts,
    }]);
    return loadouts;
  } catch (err) {
    console.error(`[LOADOUT] Failed to fetch loadouts for player ${playerId}: ${err}`);
    return [];
  }
}

/**
 * Compute per-card win rates from match_player_cards (main feature).
 */
export async function computeCardWinRates(playerId: number): Promise<number> {
  const rows = await query(`
    SELECT mp.player_id, mp.champion_id, mpc.card_id, mpc.card_level,
      COUNT(*) as times_used,
      -- Match facts can be stored as Winner/Loser from direct details or
      -- Win/Loss from recovery/history rows. Per-player loadout win rates use
      -- the outcome denominator rather than times_used so no-outcome edge rows
      -- cannot silently count as losses.
      COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')) as wins,
      COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')) as losses
    FROM match_player_cards mpc
    JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
    JOIN matches m ON m.match_id = mpc.match_id AND m.entry_datetime = mp.entry_datetime
    WHERE mp.player_id = $1
      AND m.queue_id = 486
    GROUP BY mp.player_id, mp.champion_id, mpc.card_id, mpc.card_level
  `, [playerId]);
  for (const row of rows) {
    const outcomeCount = Number(row.wins || 0) + Number(row.losses || 0);
    const winRate = outcomeCount > 0
      ? Math.round((Number(row.wins || 0) / outcomeCount) * 10000) / 100
      : 0;
    await one(`
      INSERT INTO player_loadout_cards (player_id, champion_id, card_id, card_level, times_used, wins, losses, win_rate, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
      ON CONFLICT (player_id, champion_id, card_id) DO UPDATE SET
        card_level = $4, times_used = $5, wins = $6, losses = $7, win_rate = $8, updated_at = now()
    `, [row.player_id, row.champion_id, row.card_id, row.card_level,
      row.times_used, row.wins, row.losses, winRate]);
  }
  console.log(`[LOADOUT] Computed card win rates for player ${playerId}: ${rows.length} cards`);
  return rows.length;
}

/**
 * Get top loadout builds for a player+champion based on highest win rates.
 */
export async function getTopBuilds(playerId: number, championId: number, limit = 5): Promise<any[]> {
  return await query(`
    SELECT * FROM player_loadout_cards
    WHERE player_id = $1 AND champion_id = $2
    ORDER BY win_rate DESC, times_used DESC
    LIMIT $3
  `, [playerId, championId, limit]);
}

/**
 * Recompute card win rates for all players (scheduled task).
 */
export async function recomputeAllCardWinRates(): Promise<number> {
  const players = await query(`SELECT DISTINCT mpc.player_id
    FROM match_player_cards mpc
    JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
    JOIN matches m ON m.match_id = mpc.match_id AND m.entry_datetime = mp.entry_datetime
    WHERE m.queue_id = 486`);
  let total = 0;
  for (const p of players) {
    const count = await computeCardWinRates(p.player_id);
    total += count;
  }
  console.log(`[LOADOUT] Recomputed all card win rates: ${total} cards`);
  return total;
}
