-- Zero eCPM is a real full-AFK observation, not a missing metric. Rebuild only
-- the eCPM slices of the derived histograms so historical AFK players affect
-- averages and percentile bars immediately after migration.

DELETE FROM stats_metric_histogram WHERE metric = 'egpm';

WITH eligible AS (
  SELECT
    m.queue_id,
    COALESCE(mlt.lobby_tier, 0)::SMALLINT AS lobby_tier,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT AS role_id,
    ROUND(mp.egpm::NUMERIC, 0)::DOUBLE PRECISION AS value
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
  LEFT JOIN champions c ON c.id = mp.champion_id
  WHERE mp.champion_id > 0
    AND m.duration_seconds > 120
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.egpm >= 0
), scoped AS (
  SELECT eligible.queue_id, eligible.lobby_tier, scope.role_id, eligible.value
  FROM eligible
  CROSS JOIN LATERAL (
    SELECT DISTINCT role_id FROM (VALUES (0::SMALLINT), (eligible.role_id)) role(role_id)
  ) scope
)
INSERT INTO stats_metric_histogram
  (queue_id, lobby_tier, role_id, metric, value, sample_count, updated_at)
SELECT queue_id, lobby_tier, role_id, 'egpm', value, COUNT(*)::BIGINT, now()
FROM scoped
GROUP BY queue_id, lobby_tier, role_id, value;

DELETE FROM stats_champion_metric_histogram WHERE metric = 'egpm';

INSERT INTO stats_champion_metric_histogram
  (queue_id, lobby_tier, champion_id, metric, value, sample_count, updated_at)
SELECT
  m.queue_id,
  COALESCE(mlt.lobby_tier, 0)::SMALLINT,
  mp.champion_id,
  'egpm',
  ROUND(mp.egpm::NUMERIC, 0)::DOUBLE PRECISION,
  COUNT(*)::BIGINT,
  now()
FROM match_players mp
JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
WHERE mp.champion_id > 0
  AND m.duration_seconds > 120
  AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
  AND mp.egpm >= 0
GROUP BY m.queue_id, COALESCE(mlt.lobby_tier, 0), mp.champion_id, ROUND(mp.egpm::NUMERIC, 0);

DELETE FROM performance_metric_histogram WHERE metric = 'egpm';

WITH eligible AS (
  SELECT
    m.queue_id,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 END AS role_id,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 'Damage' WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline' END AS role_name,
    mp.egpm::DOUBLE PRECISION AS value
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  JOIN champions c ON c.id = mp.champion_id
  WHERE m.queue_id = 486
    AND (NOT COALESCE(m.broken, false) OR COALESCE(m.recovered, false))
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.player_id > 0
    AND mp.champion_id > 0
    AND mp.task_force IN (1, 2)
    AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND m.duration_seconds > 120
    AND mp.egpm >= 0
    AND EXISTS (
      SELECT 1 FROM match_ingest_status mis
      WHERE mis.match_id = m.match_id AND mis.status = 'complete'
    )
), scoped AS (
  SELECT eligible.queue_id, scope.role_id, scope.role_name, eligible.value
  FROM eligible
  CROSS JOIN LATERAL (VALUES
    (0, 'Global'::TEXT), (eligible.role_id, eligible.role_name)
  ) scope(role_id, role_name)
  WHERE scope.role_id IS NOT NULL
)
INSERT INTO performance_metric_histogram
  (queue_id, role_id, role_name, metric, value, sample_count, updated_at)
SELECT queue_id, role_id, role_name, 'egpm', value, COUNT(*)::BIGINT, now()
FROM scoped
GROUP BY queue_id, role_id, role_name, value;

DELETE FROM performance_metric_stats WHERE metric = 'egpm';

WITH eligible AS (
  SELECT
    m.queue_id,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 END AS role_id,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 'Damage' WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline' END AS role_name,
    mp.egpm::DOUBLE PRECISION AS value
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  JOIN champions c ON c.id = mp.champion_id
  WHERE m.queue_id = 486
    AND (NOT COALESCE(m.broken, false) OR COALESCE(m.recovered, false))
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.player_id > 0
    AND mp.champion_id > 0
    AND mp.task_force IN (1, 2)
    AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND m.duration_seconds > 120
    AND mp.egpm >= 0
    AND EXISTS (
      SELECT 1 FROM match_ingest_status mis
      WHERE mis.match_id = m.match_id AND mis.status = 'complete'
    )
), scoped AS (
  SELECT eligible.queue_id, scope.role_id, scope.role_name, eligible.value
  FROM eligible
  CROSS JOIN LATERAL (VALUES
    (0, 'Global'::TEXT), (eligible.role_id, eligible.role_name)
  ) scope(role_id, role_name)
  WHERE scope.role_id IS NOT NULL
), aggregated AS (
  SELECT
    queue_id, role_id, role_name,
    ROUND(MIN(value)::NUMERIC, 2)::DOUBLE PRECISION AS min_value,
    ROUND(MAX(value)::NUMERIC, 2)::DOUBLE PRECISION AS max_value,
    ROUND(AVG(value)::NUMERIC, 2)::DOUBLE PRECISION AS mean_value,
    ROUND((PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS median_value,
    ROUND((MODE() WITHIN GROUP (ORDER BY ROUND(value::NUMERIC, 0)))::NUMERIC, 2)::DOUBLE PRECISION AS mode_value,
    ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p10_value,
    ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p25_value,
    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p75_value,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p90_value,
    COUNT(*)::BIGINT AS sample_size
  FROM scoped
  GROUP BY queue_id, role_id, role_name
)
INSERT INTO performance_metric_stats
  (queue_id, role_id, role_name, metric, min_value, max_value, mean_value, median_value,
   mode_value, p10_value, p25_value, p75_value, p90_value, sample_size, updated_at)
SELECT queue_id, role_id, role_name, 'egpm', min_value, max_value, mean_value, median_value,
  mode_value, p10_value, p25_value, p75_value, p90_value, sample_size, now()
FROM aggregated;

UPDATE baselines b
SET avg_egpm = p.mean_value,
    p10_egpm = p.p10_value,
    p25_egpm = p.p25_value,
    p75_egpm = p.p75_value,
    p90_egpm = p.p90_value,
    max_egpm = p.max_value,
    updated_at = now()
FROM performance_metric_stats p
WHERE p.metric = 'egpm'
  AND p.queue_id = b.queue_id
  AND p.role_id = b.role_id;

DELETE FROM champion_performance_baselines WHERE queue_id = 486 AND metric = 'egpm';

WITH values AS (
  SELECT mp.champion_id, mp.egpm::DOUBLE PRECISION AS value
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
  WHERE m.queue_id = 486
    AND COALESCE(mis.status, 'complete') = 'complete'
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.champion_id > 0
    AND mp.task_force IN (1, 2)
    AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND m.duration_seconds > 120
    AND mp.egpm >= 0
), aggregated AS (
  SELECT 486 AS queue_id, champion_id, 'egpm'::TEXT AS metric,
    ROUND(MIN(value)::NUMERIC, 2)::DOUBLE PRECISION AS min_value,
    ROUND(MAX(value)::NUMERIC, 2)::DOUBLE PRECISION AS max_value,
    ROUND(AVG(value)::NUMERIC, 2)::DOUBLE PRECISION AS mean_value,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS median_value,
    ROUND((MODE() WITHIN GROUP (ORDER BY ROUND(value::NUMERIC, 0)))::NUMERIC, 2)::DOUBLE PRECISION AS mode_value,
    ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p10_value,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p90_value,
    COUNT(*)::INT AS sample_size
  FROM values
  GROUP BY champion_id
)
INSERT INTO champion_performance_baselines
  (queue_id, champion_id, metric, min_value, max_value, mean_value, median_value,
   mode_value, p10_value, p90_value, sample_size, updated_at)
SELECT queue_id, champion_id, metric, min_value, max_value, mean_value, median_value,
  mode_value, p10_value, p90_value, sample_size, now()
FROM aggregated;

ANALYZE stats_metric_histogram;
ANALYZE stats_champion_metric_histogram;
ANALYZE performance_metric_histogram;
ANALYZE performance_metric_stats;
ANALYZE champion_performance_baselines;
