-- Rebuild only the derived WPM/APM slices using the authoritative damage
-- contract. Recovered players retain total damage/DPM but never contribute a
-- weapon/ability split. The historical damage_done_physical column stores
-- total player damage; damage_done_magical must not be added to it.

CREATE TEMP TABLE authoritative_damage_metric_values ON COMMIT DROP AS
WITH direct_players AS (
  SELECT
    m.queue_id,
    COALESCE(mlt.lobby_tier, 0)::SMALLINT AS lobby_tier,
    mp.player_id,
    mp.champion_id,
    mp.task_force,
    mp.win_status,
    COALESCE(m.limited, false) AS is_limited,
    COALESCE(m.broken, false) AS is_broken,
    COALESCE(m.recovered, false) AS is_recovered,
    COALESCE(mis.status, 'complete') = 'complete' AS is_complete,
    CASE
      WHEN c.roles ILIKE '%Damage%' THEN 1
      WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4
      ELSE 0
    END::SMALLINT AS role_id,
    CASE
      WHEN c.roles ILIKE '%Damage%' THEN 'Damage'
      WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline'
      ELSE 'Unknown'
    END AS role_name,
    COALESCE(mp.damage_done_in_hand, 0) / (m.duration_seconds / 60.0) AS weapon_per_minute,
    GREATEST(
      COALESCE(mp.damage_done_physical, 0) - COALESCE(mp.damage_done_in_hand, 0),
      0
    ) / (m.duration_seconds / 60.0) AS ability_per_minute
  FROM match_players mp
  JOIN matches m
    ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
  LEFT JOIN match_lobby_tiers mlt
    ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
  LEFT JOIN champions c ON c.id = mp.champion_id
  WHERE m.queue_id = 486
    AND mp.champion_id > 0
    AND m.duration_seconds > 120
    AND COALESCE(mp.source, 'direct') = 'direct'
)
SELECT
  direct_players.*,
  metric.metric,
  metric.value,
  ROUND(metric.value::NUMERIC, 0)::DOUBLE PRECISION AS bucket_value
FROM direct_players
CROSS JOIN LATERAL (
  VALUES
    ('wpm'::TEXT, direct_players.weapon_per_minute::DOUBLE PRECISION),
    ('apm'::TEXT, direct_players.ability_per_minute::DOUBLE PRECISION)
) metric(metric, value)
WHERE metric.value >= 0;

CREATE INDEX ON authoritative_damage_metric_values
  (queue_id, lobby_tier, metric, role_id);
CREATE INDEX ON authoritative_damage_metric_values
  (queue_id, lobby_tier, metric, champion_id);

DELETE FROM stats_metric_histogram WHERE metric IN ('wpm', 'apm');

WITH scoped AS (
  SELECT
    value.queue_id,
    value.lobby_tier,
    scope.role_id,
    value.metric,
    value.bucket_value
  FROM authoritative_damage_metric_values value
  CROSS JOIN LATERAL (
    SELECT DISTINCT role_id
    FROM (VALUES (0::SMALLINT), (value.role_id)) role(role_id)
  ) scope
)
INSERT INTO stats_metric_histogram
  (queue_id, lobby_tier, role_id, metric, value, sample_count, updated_at)
SELECT
  queue_id,
  lobby_tier,
  role_id,
  metric,
  bucket_value,
  COUNT(*)::BIGINT,
  now()
FROM scoped
GROUP BY queue_id, lobby_tier, role_id, metric, bucket_value;

DELETE FROM stats_champion_metric_histogram WHERE metric IN ('wpm', 'apm');

INSERT INTO stats_champion_metric_histogram
  (queue_id, lobby_tier, champion_id, metric, value, sample_count, updated_at)
SELECT
  queue_id,
  lobby_tier,
  champion_id,
  metric,
  bucket_value,
  COUNT(*)::BIGINT,
  now()
FROM authoritative_damage_metric_values
GROUP BY queue_id, lobby_tier, champion_id, metric, bucket_value;

DELETE FROM performance_metric_histogram WHERE metric IN ('wpm', 'apm');

WITH scoped AS (
  SELECT
    value.queue_id,
    scope.role_id,
    scope.role_name,
    value.metric,
    value.bucket_value
  FROM authoritative_damage_metric_values value
  CROSS JOIN LATERAL (
    VALUES
      (0, 'Global'::TEXT),
      (NULLIF(value.role_id, 0), NULLIF(value.role_name, 'Unknown'))
  ) scope(role_id, role_name)
  WHERE value.is_complete
    AND NOT value.is_limited
    AND (NOT value.is_broken OR value.is_recovered)
    AND value.player_id > 0
    AND value.task_force IN (1, 2)
    AND lower(COALESCE(value.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND scope.role_id IS NOT NULL
)
INSERT INTO performance_metric_histogram
  (queue_id, role_id, role_name, metric, value, sample_count, updated_at)
SELECT
  queue_id,
  role_id,
  role_name,
  metric,
  bucket_value,
  COUNT(*)::BIGINT,
  now()
FROM scoped
GROUP BY queue_id, role_id, role_name, metric, bucket_value;

DELETE FROM performance_metric_stats WHERE metric IN ('wpm', 'apm');

WITH scoped AS (
  SELECT
    value.queue_id,
    scope.role_id,
    scope.role_name,
    value.metric,
    value.value
  FROM authoritative_damage_metric_values value
  CROSS JOIN LATERAL (
    VALUES
      (0, 'Global'::TEXT),
      (NULLIF(value.role_id, 0), NULLIF(value.role_name, 'Unknown'))
  ) scope(role_id, role_name)
  WHERE value.is_complete
    AND NOT value.is_limited
    AND (NOT value.is_broken OR value.is_recovered)
    AND value.player_id > 0
    AND value.task_force IN (1, 2)
    AND lower(COALESCE(value.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND scope.role_id IS NOT NULL
), aggregated AS (
  SELECT
    queue_id,
    role_id,
    role_name,
    metric,
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
  GROUP BY queue_id, role_id, role_name, metric
)
INSERT INTO performance_metric_stats
  (queue_id, role_id, role_name, metric, min_value, max_value, mean_value, median_value,
   mode_value, p10_value, p25_value, p75_value, p90_value, sample_size, updated_at)
SELECT
  queue_id,
  role_id,
  role_name,
  metric,
  min_value,
  max_value,
  mean_value,
  median_value,
  mode_value,
  p10_value,
  p25_value,
  p75_value,
  p90_value,
  sample_size,
  now()
FROM aggregated;

DELETE FROM champion_performance_baselines
WHERE queue_id = 486 AND metric IN ('wpm', 'apm');

WITH values AS (
  SELECT champion_id, metric, value
  FROM authoritative_damage_metric_values
  WHERE is_complete
    AND task_force IN (1, 2)
    AND lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss')
), aggregated AS (
  SELECT
    486 AS queue_id,
    champion_id,
    metric,
    ROUND(MIN(value)::NUMERIC, 2)::DOUBLE PRECISION AS min_value,
    ROUND(MAX(value)::NUMERIC, 2)::DOUBLE PRECISION AS max_value,
    ROUND(AVG(value)::NUMERIC, 2)::DOUBLE PRECISION AS mean_value,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS median_value,
    ROUND((MODE() WITHIN GROUP (ORDER BY ROUND(value::NUMERIC, 0)))::NUMERIC, 2)::DOUBLE PRECISION AS mode_value,
    ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p10_value,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p90_value,
    COUNT(*)::INT AS sample_size
  FROM values
  GROUP BY champion_id, metric
)
INSERT INTO champion_performance_baselines
  (queue_id, champion_id, metric, min_value, max_value, mean_value, median_value,
   mode_value, p10_value, p90_value, sample_size, updated_at)
SELECT
  queue_id,
  champion_id,
  metric,
  min_value,
  max_value,
  mean_value,
  median_value,
  mode_value,
  p10_value,
  p90_value,
  sample_size,
  now()
FROM aggregated;

ANALYZE stats_metric_histogram;
ANALYZE stats_champion_metric_histogram;
ANALYZE performance_metric_histogram;
ANALYZE performance_metric_stats;
ANALYZE champion_performance_baselines;
