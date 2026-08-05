import type { PoolClient } from 'pg';
import { transaction } from '../config/db';
import { championRoleSql } from '../utils/champion-roles';

type QueryClient = Pick<PoolClient, 'query'>;

export type PerformanceMetricKey = 'dpm' | 'wpm' | 'apm' | 'hpm' | 'gpm' | 'egpm' | 'mpm' | 'kda';

type HistogramRow = {
  queue_id: number;
  role_id: number;
  role_name: string;
  metric: PerformanceMetricKey;
  value: number;
  sample_count: string | number;
};

export type WeightedMetricStats = {
  queueId: number;
  roleId: number;
  roleName: string;
  metric: PerformanceMetricKey;
  min: number;
  max: number;
  mean: number;
  median: number;
  mode: number;
  p10: number;
  p25: number;
  p75: number;
  p90: number;
  sampleSize: number;
};

const roleExpression = championRoleSql('c');

function roleIdExpression(roleSql: string): string {
  return `CASE ${roleSql}
    WHEN 'Damage' THEN 1
    WHEN 'Flank' THEN 2
    WHEN 'Support' THEN 3
    WHEN 'Frontline' THEN 4
    ELSE NULL
  END`;
}

const performanceSourceSql = `
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  JOIN champions c ON c.id = mp.champion_id
`;

const performanceEligibilitySql = `
  WHERE m.queue_id = 486
    AND COALESCE(m.limited, false) = false
    AND (NOT COALESCE(m.broken, false) OR COALESCE(m.recovered, false))
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.player_id > 0
    AND mp.champion_id > 0
    AND mp.task_force IN (1, 2)
    AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND m.duration_seconds > 120
`;

function normalizedMatchIds(matchIds: number[]): number[] {
  return [...new Set(matchIds
    .map(Number)
    .filter((id) => Number.isFinite(id) && id > 0)
    .map((id) => Math.trunc(id)))];
}

const insertPerformanceRecordsSql = (matchPredicate = '') => `
  INSERT INTO performance_records_ranked (
    match_id, entry_datetime, player_id, champion_id, champion_name,
    role_id, role_name, queue_id, region, platform, gpm, dpm, hpm, mpm
  )
  SELECT
    mp.match_id,
    mp.entry_datetime,
    mp.player_id,
    mp.champion_id,
    c.name,
    ${roleIdExpression(roleExpression)},
    ${roleExpression},
    m.queue_id,
    NULLIF(mp.region, ''),
    NULLIF(mp.platform, ''),
    mp.gold_per_minute,
    mp.damage_per_minute,
    mp.healing_per_minute,
    mp.mitigation_per_minute
  ${performanceSourceSql}
  ${performanceEligibilitySql}
    ${matchPredicate}
  ON CONFLICT (match_id, entry_datetime, player_id) DO UPDATE SET
    champion_id = EXCLUDED.champion_id,
    champion_name = EXCLUDED.champion_name,
    role_id = EXCLUDED.role_id,
    role_name = EXCLUDED.role_name,
    queue_id = EXCLUDED.queue_id,
    region = EXCLUDED.region,
    platform = EXCLUDED.platform,
    gpm = EXCLUDED.gpm,
    dpm = EXCLUDED.dpm,
    hpm = EXCLUDED.hpm,
    mpm = EXCLUDED.mpm
`;

const insertMetricHistogramSql = (matchPredicate = '', additive = true) => `
  WITH metric_values AS (
    SELECT
      m.queue_id,
      ${roleIdExpression(roleExpression)} AS match_role_id,
      ${roleExpression} AS match_role_name,
      metric.metric,
      metric.value
    ${performanceSourceSql}
    CROSS JOIN LATERAL (
      VALUES
        ('dpm'::TEXT, mp.damage_per_minute::DOUBLE PRECISION),
        ('wpm'::TEXT, CASE
          WHEN COALESCE(mp.source, 'direct') <> 'recovered'
          THEN COALESCE(mp.damage_done_in_hand, 0) / (m.duration_seconds / 60.0)
        END::DOUBLE PRECISION),
        ('apm'::TEXT, CASE
          WHEN COALESCE(mp.source, 'direct') <> 'recovered'
          THEN GREATEST(
            COALESCE(mp.damage_done_physical, 0) - COALESCE(mp.damage_done_in_hand, 0),
            0
          ) / (m.duration_seconds / 60.0)
        END::DOUBLE PRECISION),
        ('hpm'::TEXT, mp.healing_per_minute::DOUBLE PRECISION),
        ('gpm'::TEXT, mp.gold_per_minute::DOUBLE PRECISION),
        ('egpm'::TEXT, mp.egpm::DOUBLE PRECISION),
        ('mpm'::TEXT, mp.mitigation_per_minute::DOUBLE PRECISION),
        ('kda'::TEXT, mp.kda::DOUBLE PRECISION)
    ) metric(metric, value)
    ${performanceEligibilitySql}
      ${matchPredicate}
  ), scoped AS (
    SELECT
      metric_values.queue_id,
      scope.role_id,
      scope.role_name,
      metric_values.metric,
      CASE WHEN metric_values.metric IN ('wpm', 'apm')
        THEN ROUND(metric_values.value::NUMERIC, 0)::DOUBLE PRECISION
        ELSE metric_values.value
      END AS value
    FROM metric_values
    CROSS JOIN LATERAL (
      VALUES
        (0, 'Global'::TEXT),
        (metric_values.match_role_id, metric_values.match_role_name)
    ) scope(role_id, role_name)
    WHERE scope.role_id IS NOT NULL
      AND metric_values.value IS NOT NULL
      AND (metric_values.value > 0 OR (metric_values.metric IN ('wpm', 'apm', 'egpm') AND metric_values.value = 0))
  )
  INSERT INTO performance_metric_histogram (
    queue_id, role_id, role_name, metric, value, sample_count, updated_at
  )
  SELECT queue_id, role_id, role_name, metric, value, COUNT(*)::BIGINT, now()
  FROM scoped
  GROUP BY queue_id, role_id, role_name, metric, value
  ON CONFLICT (queue_id, role_id, metric, value) DO UPDATE SET
    role_name = EXCLUDED.role_name,
    sample_count = ${additive
      ? 'performance_metric_histogram.sample_count + EXCLUDED.sample_count'
      : 'EXCLUDED.sample_count'},
    updated_at = now()
`;

const insertBestChampionRatingsSql = (affectedPlayerPredicate = '') => `
  WITH candidates AS (
    SELECT
      486 AS queue_id,
      pcr.player_id,
      pcr.champion_id,
      pcr.mu,
      pcr.phi,
      outcomes.total_matches AS matches_played,
      outcomes.total_wins AS wins,
      outcomes.total_losses AS losses,
      ${roleIdExpression(roleExpression)} AS champion_role_id,
      ${roleExpression} AS champion_role_name
    FROM player_champion_ratings pcr
    JOIN player_champion_outcome_summary outcomes
      ON outcomes.queue_id = 486
     AND outcomes.player_id = pcr.player_id
     AND outcomes.champion_id = pcr.champion_id
    JOIN champions c ON c.id = pcr.champion_id
    WHERE outcomes.total_matches > 0
      ${affectedPlayerPredicate}
  ), scoped AS (
    SELECT candidates.*, scope.role_id, scope.role_name
    FROM candidates
    CROSS JOIN LATERAL (
      VALUES
        (0, 'Global'::TEXT),
        (candidates.champion_role_id, candidates.champion_role_name)
    ) scope(role_id, role_name)
    WHERE scope.role_id IS NOT NULL
  ), ranked AS (
    SELECT
      scoped.*,
      ROW_NUMBER() OVER (
        PARTITION BY queue_id, role_id, player_id
        ORDER BY mu DESC, matches_played DESC, wins DESC, champion_id ASC
      ) AS best_rank
    FROM scoped
  )
  INSERT INTO player_best_champion_ratings (
    queue_id, role_id, role_name, player_id, champion_id,
    mu, phi, matches_played, wins, losses, updated_at
  )
  SELECT
    queue_id, role_id, role_name, player_id, champion_id,
    mu, phi, matches_played, wins, losses, now()
  FROM ranked
  WHERE best_rank = 1
  ON CONFLICT (queue_id, role_id, player_id) DO UPDATE SET
    role_name = EXCLUDED.role_name,
    champion_id = EXCLUDED.champion_id,
    mu = EXCLUDED.mu,
    phi = EXCLUDED.phi,
    matches_played = EXCLUDED.matches_played,
    wins = EXCLUDED.wins,
    losses = EXCLUDED.losses,
    updated_at = now()
`;

function percentileValue(rows: Array<{ value: number; count: number }>, sampleSize: number, fraction: number): number {
  const position = (sampleSize - 1) * fraction;
  const lowerIndex = Math.floor(position);
  const upperIndex = Math.ceil(position);
  let cumulative = 0;
  let lowerValue = rows[0]?.value ?? 0;
  let upperValue = lowerValue;
  let foundLower = false;

  for (const row of rows) {
    cumulative += row.count;
    if (!foundLower && cumulative > lowerIndex) {
      lowerValue = row.value;
      foundLower = true;
    }
    if (cumulative > upperIndex) {
      upperValue = row.value;
      break;
    }
  }

  return lowerValue + (upperValue - lowerValue) * (position - lowerIndex);
}

/** Calculate exact weighted percentiles without expanding histogram counts back into fact rows. */
export function calculateWeightedMetricStats(rows: HistogramRow[]): WeightedMetricStats[] {
  const groups = new Map<string, HistogramRow[]>();
  for (const row of rows) {
    const key = `${row.queue_id}:${row.role_id}:${row.metric}`;
    const group = groups.get(key) ?? [];
    group.push(row);
    groups.set(key, group);
  }

  const results: WeightedMetricStats[] = [];
  for (const group of groups.values()) {
    const first = group[0];
    const ordered = group
      .map((row) => ({ value: Number(row.value), count: Number(row.sample_count) }))
      .filter((row) => Number.isFinite(row.value) && Number.isFinite(row.count) && row.count > 0)
      .sort((left, right) => left.value - right.value);
    const sampleSize = ordered.reduce((sum, row) => sum + row.count, 0);
    if (!first || sampleSize <= 0) continue;

    const modeCounts = new Map<number, number>();
    for (const row of ordered) {
      const modeValue = first.metric === 'kda'
        ? Math.round(row.value * 10) / 10
        : Math.round(row.value);
      modeCounts.set(modeValue, (modeCounts.get(modeValue) ?? 0) + row.count);
    }
    const mode = [...modeCounts.entries()]
      .sort((left, right) => right[1] - left[1] || left[0] - right[0])[0]?.[0] ?? 0;
    const mean = ordered.reduce((sum, row) => sum + row.value * row.count, 0) / sampleSize;
    const round = (value: number) => Math.round(value * 100) / 100;

    results.push({
      queueId: Number(first.queue_id),
      roleId: Number(first.role_id),
      roleName: first.role_name,
      metric: first.metric,
      min: round(ordered[0].value),
      max: round(ordered[ordered.length - 1].value),
      mean: round(mean),
      median: round(percentileValue(ordered, sampleSize, 0.5)),
      mode: round(mode),
      p10: round(percentileValue(ordered, sampleSize, 0.1)),
      p25: round(percentileValue(ordered, sampleSize, 0.25)),
      p75: round(percentileValue(ordered, sampleSize, 0.75)),
      p90: round(percentileValue(ordered, sampleSize, 0.9)),
      sampleSize,
    });
  }
  return results;
}

export async function refreshPerformanceMetricStatsWithClient(client: QueryClient): Promise<number> {
  const histogram = await client.query<HistogramRow>(`
    SELECT queue_id, role_id, role_name, metric, value, sample_count
    FROM performance_metric_histogram
    ORDER BY queue_id, role_id, metric, value
  `);
  const stats = calculateWeightedMetricStats(histogram.rows);
  await client.query('DELETE FROM performance_metric_stats');
  for (const row of stats) {
    await client.query(`
      INSERT INTO performance_metric_stats (
        queue_id, role_id, role_name, metric,
        min_value, max_value, mean_value, median_value, mode_value,
        p10_value, p25_value, p75_value, p90_value, sample_size, updated_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, now())
    `, [
      row.queueId, row.roleId, row.roleName, row.metric,
      row.min, row.max, row.mean, row.median, row.mode,
      row.p10, row.p25, row.p75, row.p90, row.sampleSize,
    ]);
  }
  return stats.length;
}

export async function refreshPerformanceMetricStats(): Promise<number> {
  return transaction((client) => refreshPerformanceMetricStatsWithClient(client));
}

/** Add completed ranked matches to every hot performance read model as one delta batch. */
export async function upsertPerformanceProjectionsForMatches(matchIds: number[]): Promise<number> {
  const ids = normalizedMatchIds(matchIds);
  if (ids.length === 0) return 0;

  return transaction(async (client) => {
    const claim = await client.query<{ match_id: string }>(`
      INSERT INTO performance_projection_matches (match_id, projected_at)
      SELECT requested.match_id, now()
      FROM unnest($1::bigint[]) AS requested(match_id)
      JOIN matches m ON m.match_id = requested.match_id
      WHERE COALESCE(m.limited, false) = false
      ON CONFLICT (match_id) DO NOTHING
      RETURNING match_id
    `, [ids]);
    const claimedIds = claim.rows.map((row) => Number(row.match_id));
    if (claimedIds.length === 0) return 0;

    await client.query(insertPerformanceRecordsSql('AND m.match_id = ANY($1::bigint[])'), [claimedIds]);
    await client.query(insertMetricHistogramSql('AND m.match_id = ANY($1::bigint[])', true), [claimedIds]);

    // Outcome history is folded from only this claimed delta. It remains
    // available even while a late Glicko match is waiting for chronological
    // repair, so leaderboard W/L reads never need match_players history scans.
    await client.query(`
      INSERT INTO player_queue_rating_summary (queue_id,player_id,total_matches,total_wins,total_losses,updated_at)
      SELECT m.queue_id,mp.player_id,COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,now()
      FROM match_players mp JOIN matches m
        ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
      WHERE mp.match_id=ANY($1::bigint[])
        AND mp.player_id>0 AND mp.champion_id>0
        AND COALESCE(mp.source,'direct') IN ('direct','recovered')
        AND lower(COALESCE(mp.win_status,'')) IN ('winner','win','loser','loss')
      GROUP BY m.queue_id,mp.player_id
      ON CONFLICT (queue_id,player_id) DO UPDATE SET
        total_matches=player_queue_rating_summary.total_matches+EXCLUDED.total_matches,
        total_wins=player_queue_rating_summary.total_wins+EXCLUDED.total_wins,
        total_losses=player_queue_rating_summary.total_losses+EXCLUDED.total_losses,updated_at=now()
    `, [claimedIds]);

    await client.query(`
      INSERT INTO player_champion_outcome_summary (
        queue_id,player_id,champion_id,total_matches,total_wins,total_losses,last_match_at,updated_at
      )
      SELECT m.queue_id,mp.player_id,mp.champion_id,COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,
        MAX(m.entry_datetime),now()
      FROM match_players mp JOIN matches m
        ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
      WHERE mp.match_id=ANY($1::bigint[])
        AND mp.player_id>0 AND mp.champion_id>0
        AND COALESCE(mp.source,'direct') IN ('direct','recovered')
        AND lower(COALESCE(mp.win_status,'')) IN ('winner','win','loser','loss')
      GROUP BY m.queue_id,mp.player_id,mp.champion_id
      ON CONFLICT (queue_id,player_id,champion_id) DO UPDATE SET
        total_matches=player_champion_outcome_summary.total_matches+EXCLUDED.total_matches,
        total_wins=player_champion_outcome_summary.total_wins+EXCLUDED.total_wins,
        total_losses=player_champion_outcome_summary.total_losses+EXCLUDED.total_losses,
        last_match_at=GREATEST(player_champion_outcome_summary.last_match_at,EXCLUDED.last_match_at),
        updated_at=now()
    `, [claimedIds]);

    const affectedPlayers = await client.query<{ player_id: string }>(`
      SELECT DISTINCT player_id
      FROM match_players
      WHERE match_id = ANY($1::bigint[]) AND player_id > 0
    `, [claimedIds]);
    const playerIds = affectedPlayers.rows.map((row) => Number(row.player_id));
    if (playerIds.length > 0) {
      await client.query(
        'DELETE FROM player_best_champion_ratings WHERE queue_id = 486 AND player_id = ANY($1::BIGINT[])',
        [playerIds],
      );
      await client.query(insertBestChampionRatingsSql('AND pcr.player_id = ANY($1::BIGINT[])'), [playerIds]);
    }
    return claimedIds.length;
  });
}

/**
 * Add one completed ranked match to every hot performance read model.
 * The ledger claim and all additive histogram writes share one transaction, so
 * a worker crash can never count the same match twice.
 */
export async function upsertPerformanceProjectionsForMatch(matchId: number): Promise<boolean> {
  return (await upsertPerformanceProjectionsForMatches([matchId])) > 0;
}

/** Nightly repair: rebuild all hot performance projections from canonical complete facts. */
export async function rebuildPerformanceReadModelsWithClient(client: QueryClient): Promise<Record<string, number>> {
  await client.query('DELETE FROM performance_records_ranked');
  await client.query('DELETE FROM performance_metric_histogram');
  await client.query('DELETE FROM player_best_champion_ratings');
  await client.query('DELETE FROM performance_projection_matches');

  await client.query(`${insertPerformanceRecordsSql(`AND EXISTS (
    SELECT 1 FROM match_ingest_status projection_mis
    WHERE projection_mis.match_id = m.match_id AND projection_mis.status = 'complete'
  )`)}`);
  await client.query(`${insertMetricHistogramSql(`AND EXISTS (
    SELECT 1 FROM match_ingest_status projection_mis
    WHERE projection_mis.match_id = m.match_id AND projection_mis.status = 'complete'
  )`, false)}`);
  await client.query(insertBestChampionRatingsSql());
  await client.query(`
    INSERT INTO performance_projection_matches (match_id, projected_at)
    SELECT DISTINCT m.match_id, now()
    FROM matches m
    JOIN match_ingest_status mis ON mis.match_id = m.match_id AND mis.status = 'complete'
    WHERE m.queue_id = 486
  `);
  const statRows = await refreshPerformanceMetricStatsWithClient(client);

  const counts: Record<string, number> = { performance_metric_stats: statRows };
  for (const table of [
    'performance_records_ranked',
    'performance_metric_histogram',
    'player_best_champion_ratings',
    'performance_projection_matches',
  ]) {
    const result = await client.query<{ count: number }>(`SELECT COUNT(*)::INT AS count FROM ${table}`);
    counts[table] = Number(result.rows[0]?.count ?? 0);
  }
  return counts;
}

export async function rebuildBestChampionRatingProjection(): Promise<number> {
  return transaction(async (client) => {
    await client.query('DELETE FROM player_best_champion_ratings');
    await client.query(insertBestChampionRatingsSql());
    const result = await client.query<{ count: number }>('SELECT COUNT(*)::INT AS count FROM player_best_champion_ratings');
    return Number(result.rows[0]?.count ?? 0);
  });
}
