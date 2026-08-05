import cron from 'node-cron';
import { query } from '../config/db';
import { runExclusive } from './worker-lock';

function countValue(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

async function refreshMatchTiers(): Promise<void> {
  const row = await query(`
    SELECT
      COUNT(*) FILTER (WHERE league_tier = 0) AS t_0,
      COUNT(*) FILTER (WHERE league_tier = 1) AS t_1,
      COUNT(*) FILTER (WHERE league_tier = 2) AS t_2,
      COUNT(*) FILTER (WHERE league_tier = 3) AS t_3,
      COUNT(*) FILTER (WHERE league_tier = 4) AS t_4,
      COUNT(*) FILTER (WHERE league_tier = 5) AS t_5,
      COUNT(*) FILTER (WHERE league_tier = 6) AS t_6,
      COUNT(*) FILTER (WHERE league_tier = 7) AS t_7,
      COUNT(*) FILTER (WHERE league_tier = 8) AS t_8,
      COUNT(*) FILTER (WHERE league_tier = 9) AS t_9,
      COUNT(*) FILTER (WHERE league_tier = 10) AS t_10,
      COUNT(*) FILTER (WHERE league_tier = 11) AS t_11,
      COUNT(*) FILTER (WHERE league_tier = 12) AS t_12,
      COUNT(*) FILTER (WHERE league_tier = 13) AS t_13,
      COUNT(*) FILTER (WHERE league_tier = 14) AS t_14,
      COUNT(*) FILTER (WHERE league_tier = 15) AS t_15,
      COUNT(*) FILTER (WHERE league_tier = 16) AS t_16,
      COUNT(*) FILTER (WHERE league_tier = 17) AS t_17,
      COUNT(*) FILTER (WHERE league_tier = 18) AS t_18,
      COUNT(*) FILTER (WHERE league_tier = 19) AS t_19,
      COUNT(*) FILTER (WHERE league_tier = 20) AS t_20,
      COUNT(*) FILTER (WHERE league_tier = 21) AS t_21,
      COUNT(*) FILTER (WHERE league_tier = 22) AS t_22,
      COUNT(*) FILTER (WHERE league_tier = 23) AS t_23,
      COUNT(*) FILTER (WHERE league_tier = 24) AS t_24,
      COUNT(*) FILTER (WHERE league_tier = 25) AS t_25,
      COUNT(*) FILTER (WHERE league_tier = 26) AS t_26
    FROM match_players mp
    JOIN matches m ON m.match_id = mp.match_id
    WHERE m.is_ranked = true
  `, []);

  await query(`
    INSERT INTO tier_stats (source, tier_0, tier_1, tier_2, tier_3, tier_4, tier_5, tier_6, tier_7,
      tier_8, tier_9, tier_10, tier_11, tier_12, tier_13, tier_14, tier_15, tier_16, tier_17,
      tier_18, tier_19, tier_20, tier_21, tier_22, tier_23, tier_24, tier_25, tier_26, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
      $20, $21, $22, $23, $24, $25, $26, $27, $28, now())
    ON CONFLICT (source) DO UPDATE SET
      tier_0 = EXCLUDED.tier_0, tier_1 = EXCLUDED.tier_1, tier_2 = EXCLUDED.tier_2, tier_3 = EXCLUDED.tier_3,
      tier_4 = EXCLUDED.tier_4, tier_5 = EXCLUDED.tier_5, tier_6 = EXCLUDED.tier_6, tier_7 = EXCLUDED.tier_7,
      tier_8 = EXCLUDED.tier_8, tier_9 = EXCLUDED.tier_9, tier_10 = EXCLUDED.tier_10, tier_11 = EXCLUDED.tier_11,
      tier_12 = EXCLUDED.tier_12, tier_13 = EXCLUDED.tier_13, tier_14 = EXCLUDED.tier_14, tier_15 = EXCLUDED.tier_15,
      tier_16 = EXCLUDED.tier_16, tier_17 = EXCLUDED.tier_17, tier_18 = EXCLUDED.tier_18, tier_19 = EXCLUDED.tier_19,
      tier_20 = EXCLUDED.tier_20, tier_21 = EXCLUDED.tier_21, tier_22 = EXCLUDED.tier_22, tier_23 = EXCLUDED.tier_23,
      tier_24 = EXCLUDED.tier_24, tier_25 = EXCLUDED.tier_25, tier_26 = EXCLUDED.tier_26,
      updated_at = now()
  `, [
    'matches', row[0].t_0, row[0].t_1, row[0].t_2, row[0].t_3, row[0].t_4, row[0].t_5, row[0].t_6, row[0].t_7,
    row[0].t_8, row[0].t_9, row[0].t_10, row[0].t_11, row[0].t_12, row[0].t_13, row[0].t_14, row[0].t_15, row[0].t_16,
    row[0].t_17, row[0].t_18, row[0].t_19, row[0].t_20, row[0].t_21, row[0].t_22, row[0].t_23, row[0].t_24, row[0].t_25, row[0].t_26
  ]);

  const dPlus = ['t_18', 't_19', 't_20', 't_21', 't_22', 't_23', 't_24', 't_25', 't_26']
    .reduce((sum, key) => sum + countValue(row[0][key]), 0);
  console.log(`[tier-stats] Match tiers refreshed: ${dPlus} diamond+`);
}

async function refreshProfileTiers(): Promise<void> {
  const row = await query(`
    SELECT
      COUNT(DISTINCT CASE WHEN kbm_tier = 0 THEN id END) AS t_0,
      COUNT(DISTINCT CASE WHEN kbm_tier = 1 THEN id END) AS t_1,
      COUNT(DISTINCT CASE WHEN kbm_tier = 2 THEN id END) AS t_2,
      COUNT(DISTINCT CASE WHEN kbm_tier = 3 THEN id END) AS t_3,
      COUNT(DISTINCT CASE WHEN kbm_tier = 4 THEN id END) AS t_4,
      COUNT(DISTINCT CASE WHEN kbm_tier = 5 THEN id END) AS t_5,
      COUNT(DISTINCT CASE WHEN kbm_tier = 6 THEN id END) AS t_6,
      COUNT(DISTINCT CASE WHEN kbm_tier = 7 THEN id END) AS t_7,
      COUNT(DISTINCT CASE WHEN kbm_tier = 8 THEN id END) AS t_8,
      COUNT(DISTINCT CASE WHEN kbm_tier = 9 THEN id END) AS t_9,
      COUNT(DISTINCT CASE WHEN kbm_tier = 10 THEN id END) AS t_10,
      COUNT(DISTINCT CASE WHEN kbm_tier = 11 THEN id END) AS t_11,
      COUNT(DISTINCT CASE WHEN kbm_tier = 12 THEN id END) AS t_12,
      COUNT(DISTINCT CASE WHEN kbm_tier = 13 THEN id END) AS t_13,
      COUNT(DISTINCT CASE WHEN kbm_tier = 14 THEN id END) AS t_14,
      COUNT(DISTINCT CASE WHEN kbm_tier = 15 THEN id END) AS t_15,
      COUNT(DISTINCT CASE WHEN kbm_tier = 16 THEN id END) AS t_16,
      COUNT(DISTINCT CASE WHEN kbm_tier = 17 THEN id END) AS t_17,
      COUNT(DISTINCT CASE WHEN kbm_tier = 18 THEN id END) AS t_18,
      COUNT(DISTINCT CASE WHEN kbm_tier = 19 THEN id END) AS t_19,
      COUNT(DISTINCT CASE WHEN kbm_tier = 20 THEN id END) AS t_20,
      COUNT(DISTINCT CASE WHEN kbm_tier = 21 THEN id END) AS t_21,
      COUNT(DISTINCT CASE WHEN kbm_tier = 22 THEN id END) AS t_22,
      COUNT(DISTINCT CASE WHEN kbm_tier = 23 THEN id END) AS t_23,
      COUNT(DISTINCT CASE WHEN kbm_tier = 24 THEN id END) AS t_24,
      COUNT(DISTINCT CASE WHEN kbm_tier = 25 THEN id END) AS t_25,
      COUNT(DISTINCT CASE WHEN kbm_tier = 26 THEN id END) AS t_26
    FROM players
  `, []);

  await query(`
    INSERT INTO tier_stats (source, tier_0, tier_1, tier_2, tier_3, tier_4, tier_5, tier_6, tier_7,
      tier_8, tier_9, tier_10, tier_11, tier_12, tier_13, tier_14, tier_15, tier_16, tier_17,
      tier_18, tier_19, tier_20, tier_21, tier_22, tier_23, tier_24, tier_25, tier_26, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19,
      $20, $21, $22, $23, $24, $25, $26, $27, $28, now())
    ON CONFLICT (source) DO UPDATE SET
      tier_0 = EXCLUDED.tier_0, tier_1 = EXCLUDED.tier_1, tier_2 = EXCLUDED.tier_2, tier_3 = EXCLUDED.tier_3,
      tier_4 = EXCLUDED.tier_4, tier_5 = EXCLUDED.tier_5, tier_6 = EXCLUDED.tier_6, tier_7 = EXCLUDED.tier_7,
      tier_8 = EXCLUDED.tier_8, tier_9 = EXCLUDED.tier_9, tier_10 = EXCLUDED.tier_10, tier_11 = EXCLUDED.tier_11,
      tier_12 = EXCLUDED.tier_12, tier_13 = EXCLUDED.tier_13, tier_14 = EXCLUDED.tier_14, tier_15 = EXCLUDED.tier_15,
      tier_16 = EXCLUDED.tier_16, tier_17 = EXCLUDED.tier_17, tier_18 = EXCLUDED.tier_18, tier_19 = EXCLUDED.tier_19,
      tier_20 = EXCLUDED.tier_20, tier_21 = EXCLUDED.tier_21, tier_22 = EXCLUDED.tier_22, tier_23 = EXCLUDED.tier_23,
      tier_24 = EXCLUDED.tier_24, tier_25 = EXCLUDED.tier_25, tier_26 = EXCLUDED.tier_26,
      updated_at = now()
  `, [
    'profiles', row[0].t_0, row[0].t_1, row[0].t_2, row[0].t_3, row[0].t_4, row[0].t_5, row[0].t_6, row[0].t_7,
    row[0].t_8, row[0].t_9, row[0].t_10, row[0].t_11, row[0].t_12, row[0].t_13, row[0].t_14, row[0].t_15, row[0].t_16,
    row[0].t_17, row[0].t_18, row[0].t_19, row[0].t_20, row[0].t_21, row[0].t_22, row[0].t_23, row[0].t_24, row[0].t_25, row[0].t_26
  ]);

  const dPlus = ['t_18', 't_19', 't_20', 't_21', 't_22', 't_23', 't_24', 't_25', 't_26']
    .reduce((sum, key) => sum + countValue(row[0][key]), 0);
  console.log(`[tier-stats] Profile tiers refreshed: ${countValue(row[0].t_0)} unranked, ${dPlus} diamond+`);
}

export async function refreshTierStats(): Promise<void> {
  try {
    await refreshMatchTiers();
  } catch (err) {
    console.error(`[tier-stats] Match tier refresh failed: ${err}`);
  }

  try {
    await refreshProfileTiers();
  } catch (err) {
    console.error(`[tier-stats] Profile tier refresh failed: ${err}`);
  }
}

export const jobs = {
  refresh: cron.createTask(
    '15 * * * *',
    async () => {
      await runExclusive('tier-stats:refresh', refreshTierStats).catch((err) => {
        console.error(`[tier-stats] Refresh failed: ${err}`);
      });
    },
  ),
};

export function enableAll() {
  jobs.refresh.start();
  console.log('[tier-stats] Cron job enabled (hourly refresh at :15)');
}

export function disableAll() {
  jobs.refresh.stop();
  console.log('[tier-stats] Cron job disabled');
}

export async function runOnce(): Promise<void> {
  await runExclusive('tier-stats:refresh', refreshTierStats);
}
