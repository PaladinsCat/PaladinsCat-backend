-- Make the manually introduced complete baseline distribution part of the
-- canonical schema, then refresh the Global row from authoritative ranked facts.

ALTER TABLE baselines
  ADD COLUMN IF NOT EXISTS p25_gpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p75_gpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS max_gpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p25_dpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p75_dpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS max_dpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p25_hpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p75_hpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS max_hpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p25_shpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p75_shpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS max_shpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p25_kda NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p75_kda NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS max_kda NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p25_egpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS p75_egpm NUMERIC(8,2),
  ADD COLUMN IF NOT EXISTS max_egpm NUMERIC(8,2);

ALTER TABLE baselines DROP CONSTRAINT IF EXISTS baselines_role_id_check;
ALTER TABLE baselines ADD CONSTRAINT baselines_role_id_check CHECK (role_id BETWEEN 0 AND 4);

WITH eligible AS (
  SELECT
    mp.gold_per_minute AS gpm,
    mp.damage_per_minute AS dpm,
    mp.healing_per_minute AS hpm,
    mp.healing_self_per_minute AS shpm,
    mp.kda,
    mp.egpm
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
  WHERE m.queue_id = 486
    AND COALESCE(mis.status, 'complete') = 'complete'
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.champion_id > 0
    AND mp.task_force IN (1, 2)
    AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND mp.time_in_match > 120
    AND mp.gold_per_minute > 0
    AND mp.egpm IS NOT NULL
    AND (
      SELECT COUNT(*)
      FROM match_players roster
      WHERE roster.match_id = m.match_id
        AND roster.entry_datetime = m.entry_datetime
    ) = 10
    AND ((
      SELECT COUNT(*)
      FROM match_players roster
      WHERE roster.match_id = m.match_id
        AND roster.entry_datetime = m.entry_datetime
        AND COALESCE(roster.source, 'direct') IN ('direct', 'recovered')
        AND roster.champion_id > 0
        AND roster.task_force IN (1, 2)
        AND lower(COALESCE(roster.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    ) + (
      SELECT COUNT(*)
      FROM match_players roster
      WHERE roster.match_id = m.match_id
        AND roster.entry_datetime = m.entry_datetime
        AND roster.player_id = 0
        AND COALESCE(roster.champion_id, 0) = 0
        AND upper(COALESCE(roster.player_name, '')) = 'PRIVATEACCOUNT'
        AND COALESCE(roster.source, 'minimal') = 'minimal'
    )) = 10
), aggregate AS (
  SELECT
    COUNT(*)::INT AS sample_size,
    ROUND(AVG(gpm)::NUMERIC, 2) AS avg_gpm,
    ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p10_gpm,
    ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p25_gpm,
    ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p75_gpm,
    ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p90_gpm,
    ROUND(MAX(gpm)::NUMERIC, 2) AS max_gpm,
    ROUND(AVG(dpm)::NUMERIC, 2) AS avg_dpm,
    ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p10_dpm,
    ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p25_dpm,
    ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p75_dpm,
    ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p90_dpm,
    ROUND(MAX(dpm)::NUMERIC, 2) AS max_dpm,
    ROUND(AVG(hpm)::NUMERIC, 2) AS avg_hpm,
    ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p10_hpm,
    ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p25_hpm,
    ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p75_hpm,
    ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p90_hpm,
    ROUND(MAX(hpm)::NUMERIC, 2) AS max_hpm,
    ROUND(AVG(shpm)::NUMERIC, 2) AS avg_shpm,
    ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p10_shpm,
    ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p25_shpm,
    ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p75_shpm,
    ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p90_shpm,
    ROUND(MAX(shpm)::NUMERIC, 2) AS max_shpm,
    ROUND(AVG(kda)::NUMERIC, 2) AS avg_kda,
    ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p10_kda,
    ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p25_kda,
    ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p75_kda,
    ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p90_kda,
    ROUND(MAX(kda)::NUMERIC, 2) AS max_kda,
    ROUND(AVG(egpm)::NUMERIC, 2) AS avg_egpm,
    ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p10_egpm,
    ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p25_egpm,
    ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p75_egpm,
    ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p90_egpm,
    ROUND(MAX(egpm)::NUMERIC, 2) AS max_egpm
  FROM eligible
)
INSERT INTO baselines (
  role_id, role_name, queue_id,
  avg_gpm, p10_gpm, p25_gpm, p75_gpm, p90_gpm, max_gpm,
  avg_dpm, p10_dpm, p25_dpm, p75_dpm, p90_dpm, max_dpm,
  avg_hpm, p10_hpm, p25_hpm, p75_hpm, p90_hpm, max_hpm,
  avg_shpm, p10_shpm, p25_shpm, p75_shpm, p90_shpm, max_shpm,
  avg_kda, p10_kda, p25_kda, p75_kda, p90_kda, max_kda,
  avg_egpm, p10_egpm, p25_egpm, p75_egpm, p90_egpm, max_egpm,
  sample_size, updated_at
)
SELECT
  0, 'Global', 486,
  avg_gpm, p10_gpm, p25_gpm, p75_gpm, p90_gpm, max_gpm,
  avg_dpm, p10_dpm, p25_dpm, p75_dpm, p90_dpm, max_dpm,
  avg_hpm, p10_hpm, p25_hpm, p75_hpm, p90_hpm, max_hpm,
  avg_shpm, p10_shpm, p25_shpm, p75_shpm, p90_shpm, max_shpm,
  avg_kda, p10_kda, p25_kda, p75_kda, p90_kda, max_kda,
  avg_egpm, p10_egpm, p25_egpm, p75_egpm, p90_egpm, max_egpm,
  sample_size, now()
FROM aggregate
ON CONFLICT (role_id, queue_id) DO UPDATE SET
  role_name = EXCLUDED.role_name,
  avg_gpm = EXCLUDED.avg_gpm, p10_gpm = EXCLUDED.p10_gpm, p25_gpm = EXCLUDED.p25_gpm, p75_gpm = EXCLUDED.p75_gpm, p90_gpm = EXCLUDED.p90_gpm, max_gpm = EXCLUDED.max_gpm,
  avg_dpm = EXCLUDED.avg_dpm, p10_dpm = EXCLUDED.p10_dpm, p25_dpm = EXCLUDED.p25_dpm, p75_dpm = EXCLUDED.p75_dpm, p90_dpm = EXCLUDED.p90_dpm, max_dpm = EXCLUDED.max_dpm,
  avg_hpm = EXCLUDED.avg_hpm, p10_hpm = EXCLUDED.p10_hpm, p25_hpm = EXCLUDED.p25_hpm, p75_hpm = EXCLUDED.p75_hpm, p90_hpm = EXCLUDED.p90_hpm, max_hpm = EXCLUDED.max_hpm,
  avg_shpm = EXCLUDED.avg_shpm, p10_shpm = EXCLUDED.p10_shpm, p25_shpm = EXCLUDED.p25_shpm, p75_shpm = EXCLUDED.p75_shpm, p90_shpm = EXCLUDED.p90_shpm, max_shpm = EXCLUDED.max_shpm,
  avg_kda = EXCLUDED.avg_kda, p10_kda = EXCLUDED.p10_kda, p25_kda = EXCLUDED.p25_kda, p75_kda = EXCLUDED.p75_kda, p90_kda = EXCLUDED.p90_kda, max_kda = EXCLUDED.max_kda,
  avg_egpm = EXCLUDED.avg_egpm, p10_egpm = EXCLUDED.p10_egpm, p25_egpm = EXCLUDED.p25_egpm, p75_egpm = EXCLUDED.p75_egpm, p90_egpm = EXCLUDED.p90_egpm, max_egpm = EXCLUDED.max_egpm,
  sample_size = EXCLUDED.sample_size,
  updated_at = EXCLUDED.updated_at;
