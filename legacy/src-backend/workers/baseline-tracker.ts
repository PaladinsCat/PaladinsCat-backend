import { query, one } from '../config/db';
import { championRoleSql } from '../utils/champion-roles';
import { refreshPerformanceMetricStats } from '../services/performance-projections';

const ROLE_MAP: Record<string, number> = {
  'Global': 0,
  'Damage': 1,
  'Flank': 2,
  'Support': 3,
  'Frontline': 4,
};

async function refreshChampionPerformanceBaselines(): Promise<number> {
  await one('DELETE FROM champion_performance_baselines WHERE queue_id <> 486');
  const refreshStarted = await one<{ started_at: string }>('SELECT clock_timestamp() AS started_at');
  if (!refreshStarted) throw new Error('Could not establish champion baseline refresh timestamp.');
  const refreshed = await query(`
    WITH metric_values AS MATERIALIZED (
      SELECT
        mp.champion_id,
        metric.metric,
        metric.value
      FROM match_players mp
      JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
      LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
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
      WHERE m.queue_id = 486
        AND COALESCE(mis.status, 'complete') = 'complete'
        AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
        AND mp.champion_id > 0
        AND mp.task_force IN (1, 2)
        AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
        AND m.duration_seconds > 120
    ), aggregated AS (
      SELECT
        486 AS queue_id,
        champion_id,
        metric,
        ROUND(MIN(value)::NUMERIC, 2)::DOUBLE PRECISION AS min_value,
        ROUND(MAX(value)::NUMERIC, 2)::DOUBLE PRECISION AS max_value,
        ROUND(AVG(value)::NUMERIC, 2)::DOUBLE PRECISION AS mean_value,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS median_value,
        ROUND((MODE() WITHIN GROUP (
          ORDER BY CASE WHEN metric = 'kda' THEN ROUND(value::NUMERIC, 1) ELSE ROUND(value::NUMERIC, 0) END
        ))::NUMERIC, 2)::DOUBLE PRECISION AS mode_value,
        ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p10_value,
        ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p90_value,
        COUNT(*)::INT AS sample_size
      FROM metric_values
      WHERE value IS NOT NULL
        AND (value > 0 OR (metric IN ('wpm', 'apm', 'egpm') AND value = 0))
      GROUP BY champion_id, metric
    )
    INSERT INTO champion_performance_baselines (
      queue_id, champion_id, metric,
      min_value, max_value, mean_value, median_value, mode_value,
      p10_value, p90_value, sample_size, updated_at
    )
    SELECT
      queue_id, champion_id, metric,
      min_value, max_value, mean_value, median_value, mode_value,
      p10_value, p90_value, sample_size, now()
    FROM aggregated
    ON CONFLICT (queue_id, champion_id, metric) DO UPDATE SET
      min_value = EXCLUDED.min_value,
      max_value = EXCLUDED.max_value,
      mean_value = EXCLUDED.mean_value,
      median_value = EXCLUDED.median_value,
      mode_value = EXCLUDED.mode_value,
      p10_value = EXCLUDED.p10_value,
      p90_value = EXCLUDED.p90_value,
      sample_size = EXCLUDED.sample_size,
      updated_at = EXCLUDED.updated_at
    RETURNING champion_id, metric, updated_at
  `);

  await one(
    'DELETE FROM champion_performance_baselines WHERE queue_id = 486 AND updated_at < $1',
    [refreshStarted.started_at],
  );
  return refreshed.length;
}

/**
 * Calculate baselines per role for ranked queue 486.
 *
 * `public.baselines` is a derived table, not source-of-truth data. Every run
 * rebuilds each role row from `match_players` + `matches` + `champions`.
 * If a role no longer has enough samples, the old row is deleted instead
 * of being left behind with stale metrics. That matters for dashboards and
 * debugging: a row in `baselines` should mean "currently supported by enough
 * source rows", not "this was true at some unknown point in the past".
 */
export async function calculateBaselines(): Promise<number> {
  // Public distribution summaries are derived from incremental histogram
  // buckets. Refreshing them is cheap and never scans historical match facts.
  await refreshPerformanceMetricStats();
  await one('DELETE FROM baselines WHERE queue_id <> 486');
  const queueIds = [486];
  let total = 0;

  const metricStats = (values: unknown[], includeZero = false) => {
    const sorted = values
      .filter((value) => value != null && value !== '')
      .map((value) => Number(value))
      // Most zeroes mean an absent role contribution. eCPM is different: zero
      // is a valid full-AFK observation and must lower its activity baseline.
      .filter((value) => Number.isFinite(value) && (value > 0 || (includeZero && value === 0)))
      .sort((a, b) => a - b);

    if (sorted.length === 0) {
      return { avg: null, p10: null, p25: null, p75: null, p90: null, max: null };
    }

    const avg = sorted.reduce((sum, value) => sum + value, 0) / sorted.length;
    const percentile = (fraction: number) => {
      const position = (sorted.length - 1) * fraction;
      const lower = Math.floor(position);
      const upper = Math.ceil(position);
      const weight = position - lower;
      return sorted[lower] + (sorted[upper] - sorted[lower]) * weight;
    };
    const p10 = percentile(0.1);
    const p25 = percentile(0.25);
    const p75 = percentile(0.75);
    const p90 = percentile(0.9);
    return { avg, p10, p25, p75, p90, max: sorted[sorted.length - 1] };
  };

  for (const queueId of queueIds) {
    for (const [roleName, roleId] of Object.entries(ROLE_MAP)) {
      const rows = await query(`
        -- Use championRoleSql() instead of raw champions.roles. During a
        -- reference refresh, roles may be Unknown while match facts already
        -- contain canonical champion names. Baselines are derived metrics, so
        -- they should degrade from role fallback data instead of disappearing.
        -- The match/player predicates mirror the player-average/rating guard:
        -- history observations never affect public performance baselines. A
        -- complete logical roster may contain multiple private rows. Preserve
        -- full-detail private match facts and exclude only minimal placeholders.
        SELECT mp.gold_per_minute, mp.damage_per_minute, mp.healing_per_minute, mp.healing_self_per_minute, mp.kda, mp.egpm
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
        LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
        JOIN champions c ON c.id = mp.champion_id
        WHERE ($1 = 'Global' OR ${championRoleSql('c')} = $1)
          AND m.queue_id = $2
          AND COALESCE(mis.status, 'complete') = 'complete'
          AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
          AND mp.champion_id > 0
          AND mp.task_force IN (1, 2)
          AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
          AND m.duration_seconds > 120
          AND mp.gold_per_minute > 0
          AND (
            SELECT COUNT(*)
            FROM match_players mp_check
            WHERE mp_check.match_id = m.match_id
              AND mp_check.entry_datetime = m.entry_datetime
          ) = 10
          AND ((
            SELECT COUNT(*)
            FROM match_players mp_check
            WHERE mp_check.match_id = m.match_id
              AND mp_check.entry_datetime = m.entry_datetime
              AND COALESCE(mp_check.source, 'direct') IN ('direct', 'recovered')
              AND mp_check.champion_id > 0
              AND mp_check.task_force IN (1, 2)
              AND lower(COALESCE(mp_check.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
          ) + (
            SELECT COUNT(*)
            FROM match_players mp_check
            WHERE mp_check.match_id = m.match_id
              AND mp_check.entry_datetime = m.entry_datetime
              AND mp_check.player_id = 0
              AND COALESCE(mp_check.champion_id, 0) = 0
              AND upper(COALESCE(mp_check.player_name, '')) = 'PRIVATEACCOUNT'
              AND COALESCE(mp_check.source, 'minimal') = 'minimal'
          )) = 10
      `, [roleName, queueId]);
      const sampleSize = rows.length;
      if (sampleSize < 10) {
        await one('DELETE FROM baselines WHERE role_id = $1 AND queue_id = $2', [roleId, queueId]);
        console.log(`[BASELINE] Skipping ${roleName}/${queueId}: only ${sampleSize} samples (need 10+)`);
        continue;
      }

      // PostgreSQL numeric/decimal values can arrive as strings depending on
      // the driver type parser. Coerce every metric with Number() before
      // sorting/reducing so the derived averages cannot silently turn into
      // string concatenation. Null-only optional metrics stay null.
      const gpm = metricStats(rows.map((r: any) => r.gold_per_minute));
      const dpm = metricStats(rows.map((r: any) => r.damage_per_minute));
      const hpm = metricStats(rows.map((r: any) => r.healing_per_minute));
      const shpm = metricStats(rows.map((r: any) => r.healing_self_per_minute));
      const kda = metricStats(rows.map((r: any) => r.kda));
      const egpm = metricStats(rows.map((r: any) => r.egpm), true);

      await one(`
        INSERT INTO baselines (role_id, role_name, queue_id,
          avg_gpm, p10_gpm, p25_gpm, p75_gpm, p90_gpm, max_gpm,
          avg_dpm, p10_dpm, p25_dpm, p75_dpm, p90_dpm, max_dpm,
          avg_hpm, p10_hpm, p25_hpm, p75_hpm, p90_hpm, max_hpm,
          avg_shpm, p10_shpm, p25_shpm, p75_shpm, p90_shpm, max_shpm,
          avg_kda, p10_kda, p25_kda, p75_kda, p90_kda, max_kda,
          avg_egpm, p10_egpm, p25_egpm, p75_egpm, p90_egpm, max_egpm,
          sample_size, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, now())
        ON CONFLICT (role_id, queue_id) DO UPDATE SET
          avg_gpm = $4, p10_gpm = $5, p25_gpm = $6, p75_gpm = $7, p90_gpm = $8, max_gpm = $9,
          avg_dpm = $10, p10_dpm = $11, p25_dpm = $12, p75_dpm = $13, p90_dpm = $14, max_dpm = $15,
          avg_hpm = $16, p10_hpm = $17, p25_hpm = $18, p75_hpm = $19, p90_hpm = $20, max_hpm = $21,
          avg_shpm = $22, p10_shpm = $23, p25_shpm = $24, p75_shpm = $25, p90_shpm = $26, max_shpm = $27,
          avg_kda = $28, p10_kda = $29, p25_kda = $30, p75_kda = $31, p90_kda = $32, max_kda = $33,
          avg_egpm = $34, p10_egpm = $35, p25_egpm = $36, p75_egpm = $37, p90_egpm = $38, max_egpm = $39,
          sample_size = $40, updated_at = now()
      `, [
        roleId, roleName, queueId,
        gpm.avg, gpm.p10, gpm.p25, gpm.p75, gpm.p90, gpm.max,
        dpm.avg, dpm.p10, dpm.p25, dpm.p75, dpm.p90, dpm.max,
        hpm.avg, hpm.p10, hpm.p25, hpm.p75, hpm.p90, hpm.max,
        shpm.avg, shpm.p10, shpm.p25, shpm.p75, shpm.p90, shpm.max,
        kda.avg, kda.p10, kda.p25, kda.p75, kda.p90, kda.max,
        egpm.avg, egpm.p10, egpm.p25, egpm.p75, egpm.p90, egpm.max,
        sampleSize
      ]);
      total++;
      const fmt = (value: number | null, digits = 1) => value == null ? 'n/a' : value.toFixed(digits);
      console.log(`[BASELINE] ${roleName}/${queueId}: gpm=${fmt(gpm.avg)}, dpm=${fmt(dpm.avg)}, hpm=${fmt(hpm.avg)}, shpm=${fmt(shpm.avg)}, kda=${fmt(kda.avg, 2)}, egpm=${fmt(egpm.avg)}, n=${sampleSize}`);
    }
  }
  const championPerformanceRows = await refreshChampionPerformanceBaselines();
  console.log(`[BASELINE] Refreshed ${championPerformanceRows} champion performance distribution rows.`);
  console.log(`[BASELINE] Complete. ${total} baselines computed.`);
  return total;
}

/**
 * Run the baseline refresh as a tracked background job.
 *
 * The scheduler status endpoint reads `sync_jobs`, so this wrapper is the
 * durable proof that baseline refresh actually ran. The legacy table only has
 * `matches_processed` / `players_processed` counters; for `baseline_tracker`
 * rows we store the number of recomputed baseline rows in `players_processed`
 * and keep source-table match/player counts out of this job record.
 */
export async function refreshBaselinesWithJob(source: 'scheduler' | 'manual' | 'post_ingest' = 'manual'): Promise<{ jobId: number; baselineRows: number }> {
  const job = await one(`
    INSERT INTO sync_jobs (job_type, status, started_at)
    VALUES ('baseline_tracker', 'running', now())
    RETURNING id
  `);

  try {
    console.log(`[BASELINE] Starting tracked baseline refresh (${source})...`);
    const baselineRows = await calculateBaselines();
    await one(`
      UPDATE sync_jobs
      SET status = 'completed',
          completed_at = now(),
          players_processed = $1
      WHERE id = $2
    `, [baselineRows, job.id]);
    return { jobId: Number(job.id), baselineRows };
  } catch (err) {
    await one(`
      UPDATE sync_jobs
      SET status = 'failed',
          completed_at = now(),
          error_message = $1
      WHERE id = $2
    `, [String(err), job.id]);
    throw err;
  }
}
