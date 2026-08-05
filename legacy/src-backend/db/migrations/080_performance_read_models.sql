-- Move the public performance and champion-ELO leaderboards off historical
-- fact scans. These are additive read models; match_players and rating tables
-- remain authoritative and the daily derived-projection repair rebuilds them.

CREATE TABLE IF NOT EXISTS performance_projection_matches (
  match_id BIGINT PRIMARY KEY,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS performance_records_ranked (
  match_id BIGINT NOT NULL,
  entry_datetime TIMESTAMPTZ NOT NULL,
  player_id BIGINT NOT NULL,
  champion_id INT NOT NULL REFERENCES champions(id),
  champion_name TEXT NOT NULL,
  role_id INT CHECK (role_id BETWEEN 1 AND 4),
  role_name TEXT NOT NULL,
  queue_id INT NOT NULL DEFAULT 486,
  region TEXT,
  platform TEXT,
  gpm DOUBLE PRECISION,
  dpm DOUBLE PRECISION,
  hpm DOUBLE PRECISION,
  mpm DOUBLE PRECISION,
  PRIMARY KEY (match_id, entry_datetime, player_id)
);

CREATE TABLE IF NOT EXISTS performance_metric_histogram (
  queue_id INT NOT NULL,
  role_id INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
  role_name TEXT NOT NULL,
  metric TEXT NOT NULL CHECK (metric IN ('dpm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda')),
  value DOUBLE PRECISION NOT NULL,
  sample_count BIGINT NOT NULL CHECK (sample_count > 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, role_id, metric, value)
);

CREATE TABLE IF NOT EXISTS performance_metric_stats (
  queue_id INT NOT NULL,
  role_id INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
  role_name TEXT NOT NULL,
  metric TEXT NOT NULL CHECK (metric IN ('dpm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda')),
  min_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  max_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  mean_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  median_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  mode_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  p10_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  p25_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  p75_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  p90_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  sample_size BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, role_id, metric)
);

CREATE TABLE IF NOT EXISTS player_best_champion_ratings (
  queue_id INT NOT NULL DEFAULT 486,
  role_id INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
  role_name TEXT NOT NULL,
  player_id BIGINT NOT NULL REFERENCES players(id),
  champion_id INT NOT NULL REFERENCES champions(id),
  mu DOUBLE PRECISION NOT NULL,
  phi DOUBLE PRECISION NOT NULL,
  matches_played INT NOT NULL DEFAULT 0,
  wins INT NOT NULL DEFAULT 0,
  losses INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, role_id, player_id)
);

WITH eligible AS (
  SELECT
    mp.*,
    m.queue_id,
    c.name AS champion_name,
    CASE
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4
      WHEN c.roles ILIKE '%Damage%' THEN 1
      WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      ELSE NULL
    END AS role_id,
    CASE
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline'
      WHEN c.roles ILIKE '%Damage%' THEN 'Damage'
      WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      ELSE COALESCE(NULLIF(c.roles, ''), 'Unknown')
    END AS role_name
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  JOIN match_ingest_status mis ON mis.match_id = m.match_id AND mis.status = 'complete'
  JOIN champions c ON c.id = mp.champion_id
  WHERE m.queue_id = 486
    AND (NOT COALESCE(m.broken, false) OR COALESCE(m.recovered, false))
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.player_id > 0
    AND mp.champion_id > 0
    AND mp.task_force IN (1, 2)
    AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND mp.time_in_match > 120
)
INSERT INTO performance_records_ranked (
  match_id, entry_datetime, player_id, champion_id, champion_name,
  role_id, role_name, queue_id, region, platform, gpm, dpm, hpm, mpm
)
SELECT
  match_id, entry_datetime, player_id, champion_id, champion_name,
  role_id, role_name, queue_id, NULLIF(region, ''), NULLIF(platform, ''),
  gold_per_minute, damage_per_minute, healing_per_minute, mitigation_per_minute
FROM eligible
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
  mpm = EXCLUDED.mpm;

WITH eligible AS (
  SELECT
    m.queue_id,
    CASE
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4
      WHEN c.roles ILIKE '%Damage%' THEN 1
      WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      ELSE NULL
    END AS match_role_id,
    CASE
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline'
      WHEN c.roles ILIKE '%Damage%' THEN 'Damage'
      WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      ELSE COALESCE(NULLIF(c.roles, ''), 'Unknown')
    END AS match_role_name,
    metric.metric,
    metric.value
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  JOIN match_ingest_status mis ON mis.match_id = m.match_id AND mis.status = 'complete'
  JOIN champions c ON c.id = mp.champion_id
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
    AND (NOT COALESCE(m.broken, false) OR COALESCE(m.recovered, false))
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    AND mp.player_id > 0
    AND mp.champion_id > 0
    AND mp.task_force IN (1, 2)
    AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    AND mp.time_in_match > 120
), scoped AS (
  SELECT eligible.queue_id, scope.role_id, scope.role_name, eligible.metric, eligible.value
  FROM eligible
  CROSS JOIN LATERAL (
    VALUES (0, 'Global'::TEXT), (eligible.match_role_id, eligible.match_role_name)
  ) scope(role_id, role_name)
  WHERE scope.role_id IS NOT NULL AND eligible.value IS NOT NULL AND eligible.value > 0
), histogram AS (
  SELECT queue_id, role_id, role_name, metric, value, COUNT(*)::BIGINT AS sample_count
  FROM scoped
  GROUP BY queue_id, role_id, role_name, metric, value
)
INSERT INTO performance_metric_histogram (
  queue_id, role_id, role_name, metric, value, sample_count, updated_at
)
SELECT queue_id, role_id, role_name, metric, value, sample_count, now()
FROM histogram
ON CONFLICT (queue_id, role_id, metric, value) DO UPDATE SET
  role_name = EXCLUDED.role_name,
  sample_count = EXCLUDED.sample_count,
  updated_at = now();

WITH expanded AS (
  SELECT
    histogram.queue_id,
    histogram.role_id,
    histogram.role_name,
    histogram.metric,
    histogram.value,
    generate_series(1, histogram.sample_count) AS occurrence
  FROM performance_metric_histogram histogram
), aggregated AS (
  SELECT
    queue_id,
    role_id,
    role_name,
    metric,
    ROUND(MIN(value)::NUMERIC, 2)::DOUBLE PRECISION AS min_value,
    ROUND(MAX(value)::NUMERIC, 2)::DOUBLE PRECISION AS max_value,
    ROUND(AVG(value)::NUMERIC, 2)::DOUBLE PRECISION AS mean_value,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS median_value,
    ROUND((MODE() WITHIN GROUP (
      ORDER BY CASE WHEN metric = 'kda' THEN ROUND(value::NUMERIC, 1) ELSE ROUND(value::NUMERIC, 0) END
    ))::NUMERIC, 2)::DOUBLE PRECISION AS mode_value,
    ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p10_value,
    ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p25_value,
    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p75_value,
    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2)::DOUBLE PRECISION AS p90_value,
    COUNT(*)::BIGINT AS sample_size
  FROM expanded
  GROUP BY queue_id, role_id, role_name, metric
)
INSERT INTO performance_metric_stats (
  queue_id, role_id, role_name, metric,
  min_value, max_value, mean_value, median_value, mode_value,
  p10_value, p25_value, p75_value, p90_value, sample_size, updated_at
)
SELECT
  queue_id, role_id, role_name, metric,
  min_value, max_value, mean_value, median_value, mode_value,
  p10_value, p25_value, p75_value, p90_value, sample_size, now()
FROM aggregated
ON CONFLICT (queue_id, role_id, metric) DO UPDATE SET
  role_name = EXCLUDED.role_name,
  min_value = EXCLUDED.min_value,
  max_value = EXCLUDED.max_value,
  mean_value = EXCLUDED.mean_value,
  median_value = EXCLUDED.median_value,
  mode_value = EXCLUDED.mode_value,
  p10_value = EXCLUDED.p10_value,
  p25_value = EXCLUDED.p25_value,
  p75_value = EXCLUDED.p75_value,
  p90_value = EXCLUDED.p90_value,
  sample_size = EXCLUDED.sample_size,
  updated_at = now();

WITH candidates AS (
  SELECT
    486 AS queue_id,
    pcr.player_id,
    pcr.champion_id,
    pcr.mu,
    pcr.phi,
    pcr.matches_played,
    pcr.wins,
    pcr.losses,
    CASE
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4
      WHEN c.roles ILIKE '%Damage%' THEN 1
      WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      ELSE NULL
    END AS champion_role_id,
    CASE
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline'
      WHEN c.roles ILIKE '%Damage%' THEN 'Damage'
      WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      ELSE COALESCE(NULLIF(c.roles, ''), 'Unknown')
    END AS champion_role_name
  FROM player_champion_ratings pcr
  JOIN champions c ON c.id = pcr.champion_id
  WHERE pcr.matches_played > 0
    AND EXISTS (
      SELECT 1
      FROM match_players qualification_mp
      JOIN matches qualification_m
        ON qualification_m.match_id = qualification_mp.match_id
       AND qualification_m.entry_datetime = qualification_mp.entry_datetime
      WHERE qualification_mp.player_id = pcr.player_id
        AND qualification_mp.champion_id = pcr.champion_id
        AND qualification_m.queue_id = 486
    )
), scoped AS (
  SELECT candidates.*, scope.role_id, scope.role_name
  FROM candidates
  CROSS JOIN LATERAL (
    VALUES (0, 'Global'::TEXT), (candidates.champion_role_id, candidates.champion_role_name)
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
  updated_at = now();

INSERT INTO performance_projection_matches (match_id, projected_at)
SELECT DISTINCT m.match_id, now()
FROM matches m
JOIN match_ingest_status mis ON mis.match_id = m.match_id AND mis.status = 'complete'
WHERE m.queue_id = 486
ON CONFLICT (match_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_performance_records_gpm
  ON performance_records_ranked (queue_id, gpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_dpm
  ON performance_records_ranked (queue_id, dpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_hpm
  ON performance_records_ranked (queue_id, hpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_mpm
  ON performance_records_ranked (queue_id, mpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_gpm
  ON performance_records_ranked (queue_id, role_name, gpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_dpm
  ON performance_records_ranked (queue_id, role_name, dpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_hpm
  ON performance_records_ranked (queue_id, role_name, hpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_mpm
  ON performance_records_ranked (queue_id, role_name, mpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_best_champion_rating_rank
  ON player_best_champion_ratings (queue_id, role_id, mu DESC, matches_played DESC, wins DESC);

ANALYZE performance_records_ranked;
ANALYZE performance_metric_histogram;
ANALYZE performance_metric_stats;
ANALYZE player_best_champion_ratings;

COMMENT ON TABLE performance_records_ranked IS
  'Narrow, eligibility-filtered read model for ranked single-match performance leaderboards.';
COMMENT ON TABLE performance_metric_histogram IS
  'Incremental exact-value histogram for global and role performance distributions.';
COMMENT ON TABLE performance_metric_stats IS
  'Compact global and role performance distribution snapshots served by public APIs.';
COMMENT ON TABLE player_best_champion_ratings IS
  'Best current champion rating per player for global and role leaderboard scopes.';
