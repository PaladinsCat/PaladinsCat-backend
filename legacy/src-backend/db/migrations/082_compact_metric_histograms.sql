-- paladinscat:requires-full-backup
-- Exact floating-point values create almost one histogram row per observation.
-- Public metrics display whole per-minute values and one KDA decimal, so keep
-- those authoritative display buckets instead. This bounds projection growth
-- while retaining materially exact public percentiles.

CREATE TEMP TABLE compact_metric_histogram AS
SELECT queue_id,lobby_tier,role_id,metric,
  CASE WHEN metric='kda' THEN ROUND(value::NUMERIC,1)::DOUBLE PRECISION
       ELSE ROUND(value::NUMERIC,0)::DOUBLE PRECISION END AS value,
  SUM(sample_count)::BIGINT AS sample_count,now() AS updated_at
FROM stats_metric_histogram
GROUP BY 1,2,3,4,5;

TRUNCATE stats_metric_histogram;
INSERT INTO stats_metric_histogram
SELECT * FROM compact_metric_histogram;
DROP TABLE compact_metric_histogram;

CREATE TABLE IF NOT EXISTS stats_champion_metric_histogram (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  champion_id INT NOT NULL,
  metric TEXT NOT NULL CHECK (metric IN ('dpm','hpm','gpm','egpm','mpm','kda')),
  value DOUBLE PRECISION NOT NULL,
  sample_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id,lobby_tier,champion_id,metric,value)
);

CREATE INDEX IF NOT EXISTS idx_stats_champion_metric_scope
  ON stats_champion_metric_histogram (queue_id,lobby_tier,metric,champion_id,value);

WITH eligible AS (
  SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT AS lobby_tier,mp.champion_id,
    mp.damage_per_minute,mp.healing_per_minute,mp.gold_per_minute,mp.egpm,
    mp.mitigation_per_minute,mp.kda
  FROM match_players mp
  JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  WHERE mp.champion_id>0 AND mp.time_in_match>120
    AND COALESCE(mp.source,'direct') IN ('direct','recovered')
), values_by_champion AS (
  SELECT e.queue_id,e.lobby_tier,e.champion_id,metric.metric,
    CASE WHEN metric.metric='kda' THEN ROUND(metric.value::NUMERIC,1)::DOUBLE PRECISION
         ELSE ROUND(metric.value::NUMERIC,0)::DOUBLE PRECISION END AS value
  FROM eligible e
  CROSS JOIN LATERAL (VALUES
    ('dpm',e.damage_per_minute::DOUBLE PRECISION),
    ('hpm',e.healing_per_minute::DOUBLE PRECISION),
    ('gpm',e.gold_per_minute::DOUBLE PRECISION),
    ('egpm',e.egpm::DOUBLE PRECISION),
    ('mpm',e.mitigation_per_minute::DOUBLE PRECISION),
    ('kda',e.kda::DOUBLE PRECISION)
  ) metric(metric,value)
  WHERE metric.value IS NOT NULL AND metric.value>0
)
INSERT INTO stats_champion_metric_histogram
SELECT queue_id,lobby_tier,champion_id,metric,value,COUNT(*)::BIGINT,now()
FROM values_by_champion GROUP BY 1,2,3,4,5
ON CONFLICT (queue_id,lobby_tier,champion_id,metric,value) DO UPDATE SET
  sample_count=EXCLUDED.sample_count,updated_at=now();

ANALYZE stats_metric_histogram;
ANALYZE stats_champion_metric_histogram;

COMMENT ON TABLE stats_champion_metric_histogram IS
  'Queue/tier/champion display-precision metric histogram for bounded percentile reads.';
