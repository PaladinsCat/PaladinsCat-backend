import { PoolClient } from 'pg';
import { query, transaction } from '../config/db';

type Queryable = Pick<PoolClient, 'query'>;

function normalizedIds(values: Array<number | string | null | undefined>): number[] {
  return [...new Set(
    values
      .map((id) => Number(id))
      .filter((id) => Number.isFinite(id) && id > 0)
      .map((id) => Math.trunc(id)),
  )];
}

/**
 * player-performance-rollups.ts
 *
 * Purpose:
 * Maintain the denormalized rolling performance columns on `public.players`:
 *   - avg_egpm
 *   - avg_dpm
 *   - avg_hpm
 *   - avg_shpm
 *   - avg_mpm
 *
 * Source of truth:
 * These values are rebuilt only from authoritative match facts in
 * `match_players` + `matches`. `player_match_history_entries` is deliberately
 * excluded because it is a one-player getmatchhistory observation store. It can
 * help the UI show a player history row and can help recovery fill a broken
 * match, but it is not a complete match fact and must never change leaderboard
 * averages or rating-like metrics by itself.
 *
 * Safety contract:
 * A match contributes to player averages when it is ranked and complete with a
 * valid logical 5v5 roster. The roster may contain multiple private rows;
 * identity-based averages exclude all player_id=0 rows while the remaining
 * authoritative players continue to contribute. Outcome checks normalize both Winner/Loser and
 * Win/Loss so recovered rows are not accidentally excluded.
 * Rows with source='prefetch', source='match_history', and similar
 * history-only sources are excluded. This is the same boundary we want around
 * ratings: API recovery/cache observations are useful input, but only completed
 * match facts are allowed to become public performance metrics.
 */

const PLAYER_AVERAGE_COLUMNS = 'avg_egpm = NULL, avg_dpm = NULL, avg_hpm = NULL, avg_shpm = NULL, avg_mpm = NULL';

function eligibleMatchStatusPredicate(allowProcessingMatchIdsParam?: number): string {
  if (!allowProcessingMatchIdsParam) {
    return `COALESCE(mis.status, 'complete') = 'complete'`;
  }

  return `(
    COALESCE(mis.status, 'complete') = 'complete'
    OR (
      m.match_id = ANY($${allowProcessingMatchIdsParam}::bigint[])
      AND mis.status IN ('processing', 'partial')
    )
  )`;
}

function playerAverageRollupSql(targetPlayerParam: number, allowProcessingMatchIdsParam?: number): string {
  return `
    WITH target_players AS (
      SELECT DISTINCT unnest($${targetPlayerParam}::bigint[]) AS player_id
    ),
    candidate_matches AS (
      SELECT DISTINCT m.match_id, m.entry_datetime, m.duration_seconds
      FROM target_players tp
      JOIN match_players target_mp ON target_mp.player_id = tp.player_id
      JOIN matches m
        ON m.match_id = target_mp.match_id
       AND m.entry_datetime = target_mp.entry_datetime
      LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
      WHERE m.queue_id = 486
        AND COALESCE(m.limited, false) = false
        AND COALESCE(m.is_ranked, m.queue_id = 486) = true
        AND ${eligibleMatchStatusPredicate(allowProcessingMatchIdsParam)}
    ),
    eligible_matches AS (
      SELECT cm.match_id, cm.entry_datetime, cm.duration_seconds
      FROM candidate_matches cm
      JOIN match_players mp_check
        ON mp_check.match_id = cm.match_id
       AND mp_check.entry_datetime = cm.entry_datetime
      GROUP BY cm.match_id, cm.entry_datetime, cm.duration_seconds
      HAVING COUNT(*) = 10
        AND COUNT(*) FILTER (WHERE mp_check.task_force = 1) = 5
        AND COUNT(*) FILTER (WHERE mp_check.task_force = 2) = 5
        AND COUNT(*) FILTER (
          WHERE lower(COALESCE(mp_check.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
        ) = 10
        AND (
          COUNT(*) FILTER (
            WHERE COALESCE(mp_check.source, 'direct') IN ('direct', 'recovered')
              AND mp_check.champion_id > 0
              AND mp_check.task_force IN (1, 2)
              AND lower(COALESCE(mp_check.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
          )
          + COUNT(*) FILTER (
            WHERE mp_check.player_id = 0
              AND COALESCE(mp_check.champion_id, 0) = 0
              AND upper(COALESCE(mp_check.player_name, '')) = 'PRIVATEACCOUNT'
              AND COALESCE(mp_check.source, 'minimal') = 'minimal'
          )
        ) = 10
    ),
    rollups AS (
      SELECT
        mp.player_id,
        ROUND(AVG(mp.egpm)::numeric, 2)::double precision AS avg_egpm,
        ROUND(AVG(mp.damage_per_minute)::numeric, 2)::double precision AS avg_dpm,
        ROUND(AVG(mp.healing_per_minute)::numeric, 2)::double precision AS avg_hpm,
        ROUND(AVG(mp.healing_self_per_minute)::numeric, 2)::double precision AS avg_shpm,
        ROUND(AVG(mp.mitigation_per_minute)::numeric, 2)::double precision AS avg_mpm
      FROM match_players mp
      JOIN eligible_matches em ON em.match_id = mp.match_id AND em.entry_datetime = mp.entry_datetime
      JOIN target_players tp ON tp.player_id = mp.player_id
      WHERE COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
        AND mp.player_id > 0
        AND mp.champion_id > 0
        AND mp.task_force IN (1, 2)
        AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
        AND em.duration_seconds > 120
      GROUP BY mp.player_id
    )
    UPDATE players p
    SET
      avg_egpm = r.avg_egpm,
      avg_dpm = r.avg_dpm,
      avg_hpm = r.avg_hpm,
      avg_shpm = r.avg_shpm,
      avg_mpm = r.avg_mpm,
      last_updated = now()
    FROM rollups r
    WHERE p.id = r.player_id
    RETURNING p.id
  `;
}

export async function updatePlayerAveragesForPlayers(
  playerIds: Array<number | string | null | undefined>,
  options: { allowProcessingMatchIds?: number[] } = {},
): Promise<number> {
  const ids = normalizedIds(playerIds);

  if (ids.length === 0) return 0;

  const allowProcessingMatchIds = [...new Set(
    (options.allowProcessingMatchIds || [])
      .map((id) => Number(id))
      .filter((id) => Number.isFinite(id) && id > 0)
      .map((id) => Math.trunc(id)),
  )];

  return transaction(async (client) => {
    await client.query(
      `UPDATE players SET ${PLAYER_AVERAGE_COLUMNS} WHERE id = ANY($1::bigint[])`,
      [ids],
    );

    const params: unknown[] = [ids];
    const allowParam = allowProcessingMatchIds.length > 0 ? 2 : undefined;
    if (allowParam) params.push(allowProcessingMatchIds);

    const result = await client.query(playerAverageRollupSql(1, allowParam), params);
    return result.rowCount || result.rows.length;
  });
}

/**
 * Apply newly durable ranked matches to the cumulative player performance
 * totals. Each match is claimed once, then all of its player deltas are folded
 * into the accumulator in one set-based statement. Per-minute metrics are
 * additive samples too: avg_dpm is dpm_sum / sample_count, not a fresh scan of
 * every historical match_player row.
 */
export async function updatePlayerAveragesForMatches(
  matchIds: Array<number | string | null | undefined>,
): Promise<number> {
  const ids = normalizedIds(matchIds);
  if (ids.length === 0) return 0;

  return transaction(async (client) => {
    const claimed = await client.query<{ match_id: string }>(`
      INSERT INTO player_performance_projection_matches (match_id)
      SELECT requested.match_id
      FROM unnest($1::bigint[]) AS requested(match_id)
      JOIN matches m ON m.match_id = requested.match_id
      WHERE m.queue_id = 486
        AND COALESCE(m.limited, false) = false
        AND COALESCE(m.is_ranked, m.queue_id = 486) = true
      ON CONFLICT DO NOTHING
      RETURNING match_id
    `, [ids]);
    const claimedIds = claimed.rows.map((row) => Number(row.match_id));
    if (claimedIds.length === 0) return 0;

    const updated = await client.query(`
      WITH candidate_matches AS (
        SELECT m.match_id, m.entry_datetime, m.duration_seconds
        FROM matches m
        WHERE m.match_id = ANY($1::bigint[])
          AND COALESCE(m.limited, false) = false
      ),
      eligible_matches AS (
        SELECT cm.match_id, cm.entry_datetime, cm.duration_seconds
        FROM candidate_matches cm
        JOIN match_players mp_check
          ON mp_check.match_id = cm.match_id
         AND mp_check.entry_datetime = cm.entry_datetime
        GROUP BY cm.match_id, cm.entry_datetime, cm.duration_seconds
        HAVING COUNT(*) = 10
          AND COUNT(*) FILTER (WHERE mp_check.task_force = 1) = 5
          AND COUNT(*) FILTER (WHERE mp_check.task_force = 2) = 5
          AND COUNT(*) FILTER (
            WHERE lower(COALESCE(mp_check.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
          ) = 10
          AND (
            COUNT(*) FILTER (
              WHERE COALESCE(mp_check.source, 'direct') IN ('direct', 'recovered')
                AND mp_check.champion_id > 0
                AND mp_check.task_force IN (1, 2)
                AND lower(COALESCE(mp_check.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
            )
            + COUNT(*) FILTER (
              WHERE mp_check.player_id = 0
                AND COALESCE(mp_check.champion_id, 0) = 0
                AND upper(COALESCE(mp_check.player_name, '')) = 'PRIVATEACCOUNT'
                AND COALESCE(mp_check.source, 'minimal') = 'minimal'
            )
          ) = 10
      ),
      deltas AS (
        SELECT mp.player_id, COUNT(*)::bigint AS sample_count,
          COALESCE(SUM(mp.egpm), 0)::double precision AS egpm_sum,
          COALESCE(SUM(mp.damage_per_minute), 0)::double precision AS dpm_sum,
          COALESCE(SUM(mp.healing_per_minute), 0)::double precision AS hpm_sum,
          COALESCE(SUM(mp.healing_self_per_minute), 0)::double precision AS shpm_sum,
          COALESCE(SUM(mp.mitigation_per_minute), 0)::double precision AS mpm_sum
        FROM eligible_matches em
        JOIN match_players mp
          ON mp.match_id = em.match_id
         AND mp.entry_datetime = em.entry_datetime
        WHERE COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
          AND mp.player_id > 0
          AND mp.champion_id > 0
          AND mp.task_force IN (1, 2)
          AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
          AND em.duration_seconds > 120
        GROUP BY mp.player_id
      ),
      accumulated AS (
        INSERT INTO player_performance_aggregate (
          player_id, sample_count, egpm_sum, dpm_sum, hpm_sum, shpm_sum, mpm_sum, updated_at
        )
        SELECT player_id, sample_count, egpm_sum, dpm_sum, hpm_sum, shpm_sum, mpm_sum, now()
        FROM deltas
        ON CONFLICT (player_id) DO UPDATE SET
          sample_count = player_performance_aggregate.sample_count + EXCLUDED.sample_count,
          egpm_sum = player_performance_aggregate.egpm_sum + EXCLUDED.egpm_sum,
          dpm_sum = player_performance_aggregate.dpm_sum + EXCLUDED.dpm_sum,
          hpm_sum = player_performance_aggregate.hpm_sum + EXCLUDED.hpm_sum,
          shpm_sum = player_performance_aggregate.shpm_sum + EXCLUDED.shpm_sum,
          mpm_sum = player_performance_aggregate.mpm_sum + EXCLUDED.mpm_sum,
          updated_at = now()
        RETURNING player_id, sample_count, egpm_sum, dpm_sum, hpm_sum, shpm_sum, mpm_sum
      )
      UPDATE players p
      SET avg_egpm = ROUND((a.egpm_sum / NULLIF(a.sample_count, 0))::numeric, 2)::double precision,
          avg_dpm = ROUND((a.dpm_sum / NULLIF(a.sample_count, 0))::numeric, 2)::double precision,
          avg_hpm = ROUND((a.hpm_sum / NULLIF(a.sample_count, 0))::numeric, 2)::double precision,
          avg_shpm = ROUND((a.shpm_sum / NULLIF(a.sample_count, 0))::numeric, 2)::double precision,
          avg_mpm = ROUND((a.mpm_sum / NULLIF(a.sample_count, 0))::numeric, 2)::double precision,
          last_updated = now()
      FROM accumulated a
      WHERE p.id = a.player_id
      RETURNING p.id
    `, [claimedIds]);
    return updated.rowCount || updated.rows.length;
  });
}

export async function updatePlayerAverages(playerId: number): Promise<number> {
  return updatePlayerAveragesForPlayers([playerId]);
}

export async function rebuildPlayerAverages(client: Queryable): Promise<number> {
  await client.query(`UPDATE players SET ${PLAYER_AVERAGE_COLUMNS}`);
  const result = await client.query(playerAverageRollupSql(1), [
    await collectAllPlayerIdsWithRankedFacts(client),
  ]);
  return result.rowCount || result.rows.length;
}

async function collectAllPlayerIdsWithRankedFacts(client: Queryable): Promise<number[]> {
  const result = await client.query(`
    SELECT DISTINCT mp.player_id::bigint AS player_id
    FROM match_players mp
    JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
    WHERE m.queue_id = 486
      AND mp.player_id > 0
  `);
  return result.rows.map((row: any) => Number(row.player_id)).filter((id) => Number.isFinite(id) && id > 0);
}

export async function rebuildPlayerAveragesStandalone(): Promise<number> {
  return transaction((client) => rebuildPlayerAverages(client));
}

export async function countPlayersWithPerformanceAverages(): Promise<number> {
  const rows = await query<{ count: string }>(`
    SELECT COUNT(*)::text AS count
    FROM players
    WHERE avg_egpm IS NOT NULL
       OR avg_dpm IS NOT NULL
       OR avg_hpm IS NOT NULL
       OR avg_shpm IS NOT NULL
       OR avg_mpm IS NOT NULL
  `);
  return Number(rows[0]?.count || 0);
}
