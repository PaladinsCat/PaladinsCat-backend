-- Add weapon- and ability-damage-per-minute distributions without changing
-- the canonical player-match facts. Ability damage is total player damage
-- minus in-hand damage. Recovered rows with a missing in-hand field are
-- excluded because their split is unknown, while legitimate zero values from
-- detailed matches remain part of the distribution.

ALTER TABLE performance_metric_histogram
  DROP CONSTRAINT IF EXISTS performance_metric_histogram_metric_check;
ALTER TABLE performance_metric_histogram
  ADD CONSTRAINT performance_metric_histogram_metric_check
  CHECK (metric IN ('dpm','wpm','apm','hpm','gpm','egpm','mpm','kda'));

ALTER TABLE performance_metric_stats
  DROP CONSTRAINT IF EXISTS performance_metric_stats_metric_check;
ALTER TABLE performance_metric_stats
  ADD CONSTRAINT performance_metric_stats_metric_check
  CHECK (metric IN ('dpm','wpm','apm','hpm','gpm','egpm','mpm','kda'));

ALTER TABLE champion_performance_baselines
  DROP CONSTRAINT IF EXISTS champion_performance_baselines_metric_check;
ALTER TABLE champion_performance_baselines
  ADD CONSTRAINT champion_performance_baselines_metric_check
  CHECK (metric IN ('dpm','wpm','apm','hpm','gpm','egpm','mpm','kda'));

ALTER TABLE stats_metric_histogram
  DROP CONSTRAINT IF EXISTS stats_metric_histogram_metric_check;
ALTER TABLE stats_metric_histogram
  ADD CONSTRAINT stats_metric_histogram_metric_check
  CHECK (metric IN ('dpm','wpm','apm','hpm','gpm','egpm','mpm','kda'));

ALTER TABLE stats_champion_metric_histogram
  DROP CONSTRAINT IF EXISTS stats_champion_metric_histogram_metric_check;
ALTER TABLE stats_champion_metric_histogram
  ADD CONSTRAINT stats_champion_metric_histogram_metric_check
  CHECK (metric IN ('dpm','wpm','apm','hpm','gpm','egpm','mpm','kda'));

CREATE TEMP TABLE damage_split_metric_values ON COMMIT DROP AS
WITH eligible AS (
  SELECT
    mp.match_id,
    mp.entry_datetime,
    mp.player_id,
    mp.champion_id,
    mp.task_force,
    mp.win_status,
    m.queue_id,
    COALESCE(mlt.lobby_tier,0)::SMALLINT AS lobby_tier,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT AS role_id,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 'Damage' WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline' ELSE 'Unknown' END AS role_name,
    COALESCE(mis.status,'complete')='complete' AS is_complete,
    COALESCE(m.broken,false) AS is_broken,
    COALESCE(m.recovered,false) AS is_recovered,
    COALESCE(mp.damage_done_in_hand,0)/(mp.time_in_match/60.0) AS weapon_per_minute,
    GREATEST(
      COALESCE(mp.damage_done_physical,0)+COALESCE(mp.damage_done_magical,0)
        -COALESCE(mp.damage_done_in_hand,0),
      0
    )/(mp.time_in_match/60.0) AS ability_per_minute
  FROM match_players mp
  JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_ingest_status mis ON mis.match_id=m.match_id
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  LEFT JOIN champions c ON c.id=mp.champion_id
  WHERE mp.champion_id>0
    AND mp.time_in_match>120
    AND COALESCE(mp.source,'direct') IN ('direct','recovered')
    AND (COALESCE(mp.source,'direct')<>'recovered'
      OR COALESCE(mp.damage_done_in_hand,0)>0
      OR COALESCE(mp.damage_done_physical,0)+COALESCE(mp.damage_done_magical,0)=0)
)
SELECT eligible.*,metric.metric,metric.value,
  ROUND(metric.value::NUMERIC,0)::DOUBLE PRECISION AS bucket_value
FROM eligible
CROSS JOIN LATERAL (VALUES
  ('wpm'::TEXT,eligible.weapon_per_minute::DOUBLE PRECISION),
  ('apm'::TEXT,eligible.ability_per_minute::DOUBLE PRECISION)
) metric(metric,value)
WHERE metric.value IS NOT NULL AND metric.value>=0;

CREATE INDEX ON damage_split_metric_values (queue_id,lobby_tier,metric,role_id);
CREATE INDEX ON damage_split_metric_values (queue_id,lobby_tier,metric,champion_id);

-- Tier-aware compact projections, used for arbitrary champion tier ranges.
WITH scoped AS (
  SELECT value.queue_id,value.lobby_tier,scope.role_id,value.metric,value.bucket_value
  FROM damage_split_metric_values value
  CROSS JOIN LATERAL (
    SELECT DISTINCT role_id FROM (VALUES (0::SMALLINT),(value.role_id)) role(role_id)
  ) scope
)
INSERT INTO stats_metric_histogram
  (queue_id,lobby_tier,role_id,metric,value,sample_count,updated_at)
SELECT queue_id,lobby_tier,role_id,metric,bucket_value,COUNT(*)::BIGINT,now()
FROM scoped GROUP BY 1,2,3,4,5
ON CONFLICT (queue_id,lobby_tier,role_id,metric,value) DO UPDATE SET
  sample_count=EXCLUDED.sample_count,updated_at=now();

INSERT INTO stats_champion_metric_histogram
  (queue_id,lobby_tier,champion_id,metric,value,sample_count,updated_at)
SELECT queue_id,lobby_tier,champion_id,metric,bucket_value,COUNT(*)::BIGINT,now()
FROM damage_split_metric_values GROUP BY 1,2,3,4,5
ON CONFLICT (queue_id,lobby_tier,champion_id,metric,value) DO UPDATE SET
  sample_count=EXCLUDED.sample_count,updated_at=now();

-- All-history global/role histograms use the same eligibility as the ranked
-- performance projection. Whole-value buckets keep the new projections small.
WITH scoped AS (
  SELECT value.queue_id,scope.role_id,scope.role_name,value.metric,value.bucket_value
  FROM damage_split_metric_values value
  CROSS JOIN LATERAL (VALUES
    (0,'Global'::TEXT),(value.role_id,value.role_name)
  ) scope(role_id,role_name)
  WHERE value.queue_id=486 AND value.is_complete
    AND (NOT value.is_broken OR value.is_recovered)
    AND value.player_id>0 AND value.task_force IN (1,2)
    AND lower(COALESCE(value.win_status,'')) IN ('winner','win','loser','loss')
    AND scope.role_id BETWEEN 0 AND 4
)
INSERT INTO performance_metric_histogram
  (queue_id,role_id,role_name,metric,value,sample_count,updated_at)
SELECT queue_id,role_id,role_name,metric,bucket_value,COUNT(*)::BIGINT,now()
FROM scoped GROUP BY 1,2,3,4,5
ON CONFLICT (queue_id,role_id,metric,value) DO UPDATE SET
  role_name=EXCLUDED.role_name,sample_count=EXCLUDED.sample_count,updated_at=now();

WITH scoped AS (
  SELECT value.queue_id,scope.role_id,scope.role_name,value.metric,value.value
  FROM damage_split_metric_values value
  CROSS JOIN LATERAL (VALUES
    (0,'Global'::TEXT),(value.role_id,value.role_name)
  ) scope(role_id,role_name)
  WHERE value.queue_id=486 AND value.is_complete
    AND (NOT value.is_broken OR value.is_recovered)
    AND value.player_id>0 AND value.task_force IN (1,2)
    AND lower(COALESCE(value.win_status,'')) IN ('winner','win','loser','loss')
    AND scope.role_id BETWEEN 0 AND 4
), aggregated AS (
  SELECT queue_id,role_id,role_name,metric,
    ROUND(MIN(value)::NUMERIC,2)::DOUBLE PRECISION AS min_value,
    ROUND(MAX(value)::NUMERIC,2)::DOUBLE PRECISION AS max_value,
    ROUND(AVG(value)::NUMERIC,2)::DOUBLE PRECISION AS mean_value,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS median_value,
    ROUND((MODE() WITHIN GROUP (ORDER BY ROUND(value::NUMERIC,0)))::NUMERIC,2)::DOUBLE PRECISION AS mode_value,
    ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS p10_value,
    ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS p25_value,
    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS p75_value,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS p90_value,
    COUNT(*)::BIGINT AS sample_size
  FROM scoped GROUP BY 1,2,3,4
)
INSERT INTO performance_metric_stats
  (queue_id,role_id,role_name,metric,min_value,max_value,mean_value,median_value,mode_value,
   p10_value,p25_value,p75_value,p90_value,sample_size,updated_at)
SELECT queue_id,role_id,role_name,metric,min_value,max_value,mean_value,median_value,mode_value,
  p10_value,p25_value,p75_value,p90_value,sample_size,now()
FROM aggregated
ON CONFLICT (queue_id,role_id,metric) DO UPDATE SET
  role_name=EXCLUDED.role_name,min_value=EXCLUDED.min_value,max_value=EXCLUDED.max_value,
  mean_value=EXCLUDED.mean_value,median_value=EXCLUDED.median_value,mode_value=EXCLUDED.mode_value,
  p10_value=EXCLUDED.p10_value,p25_value=EXCLUDED.p25_value,p75_value=EXCLUDED.p75_value,
  p90_value=EXCLUDED.p90_value,sample_size=EXCLUDED.sample_size,updated_at=now();

-- Champion cards use exact all-history values when no tier range is selected.
WITH aggregated AS (
  SELECT 486 AS queue_id,champion_id,metric,
    ROUND(MIN(value)::NUMERIC,2)::DOUBLE PRECISION AS min_value,
    ROUND(MAX(value)::NUMERIC,2)::DOUBLE PRECISION AS max_value,
    ROUND(AVG(value)::NUMERIC,2)::DOUBLE PRECISION AS mean_value,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS median_value,
    ROUND((MODE() WITHIN GROUP (ORDER BY ROUND(value::NUMERIC,0)))::NUMERIC,2)::DOUBLE PRECISION AS mode_value,
    ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS p10_value,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC,2)::DOUBLE PRECISION AS p90_value,
    COUNT(*)::INT AS sample_size
  FROM damage_split_metric_values
  WHERE queue_id=486 AND is_complete AND task_force IN (1,2)
    AND lower(COALESCE(win_status,'')) IN ('winner','win','loser','loss')
  GROUP BY champion_id,metric
)
INSERT INTO champion_performance_baselines
  (queue_id,champion_id,metric,min_value,max_value,mean_value,median_value,mode_value,
   p10_value,p90_value,sample_size,updated_at)
SELECT queue_id,champion_id,metric,min_value,max_value,mean_value,median_value,mode_value,
  p10_value,p90_value,sample_size,now()
FROM aggregated
ON CONFLICT (queue_id,champion_id,metric) DO UPDATE SET
  min_value=EXCLUDED.min_value,max_value=EXCLUDED.max_value,mean_value=EXCLUDED.mean_value,
  median_value=EXCLUDED.median_value,mode_value=EXCLUDED.mode_value,p10_value=EXCLUDED.p10_value,
  p90_value=EXCLUDED.p90_value,sample_size=EXCLUDED.sample_size,updated_at=now();

ANALYZE stats_metric_histogram;
ANALYZE stats_champion_metric_histogram;
ANALYZE performance_metric_histogram;
ANALYZE performance_metric_stats;
ANALYZE champion_performance_baselines;
