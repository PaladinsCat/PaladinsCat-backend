-- Precompute the per-champion metric distributions used by /stats/metrics and
-- champion page bundles. The previous request path recalculated percentiles
-- from every historical match_player row and exceeded the browser timeout on
-- a cold route-cache miss.

CREATE TABLE IF NOT EXISTS champion_performance_baselines (
  queue_id INT NOT NULL DEFAULT 486,
  champion_id INT NOT NULL REFERENCES champions(id),
  metric TEXT NOT NULL CHECK (metric IN ('dpm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda')),
  min_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  max_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  mean_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  median_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  mode_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  p10_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  p90_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  sample_size INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, champion_id, metric)
);

CREATE INDEX IF NOT EXISTS idx_champion_performance_baselines_metric
  ON champion_performance_baselines (queue_id, metric, mean_value DESC);

COMMENT ON TABLE champion_performance_baselines IS
  'Baseline-worker projection of ranked per-champion metric distributions used by public stats pages.';

WITH metric_values AS MATERIALIZED (
  SELECT
    mp.champion_id,
    metric.metric,
    metric.value
  FROM match_players mp
  JOIN matches m
    ON m.match_id = mp.match_id
   AND m.entry_datetime = mp.entry_datetime
  LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
  CROSS JOIN LATERAL (
    VALUES
      ('dpm'::TEXT, mp.damage_per_minute::DOUBLE PRECISION),
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
    AND mp.time_in_match > 120
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
  WHERE value IS NOT NULL AND value >= 0
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
  updated_at = EXCLUDED.updated_at;
