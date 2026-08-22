-- Physically isolated casual performance read models. Mirrors the ranked
-- performance_metric_* pair (080) but scoped to the casual population
-- (queue_id IN (424,452), physically isolated in casual_matches). Separate
-- tables enforce the queue-486 rule: casual never writes ranked projections.
--
-- role_id 0 = Global; 1-4 = Damage/Flank/Support/Frontline via the shared
-- ROLE_NAME_SQL / role_id_sql() in projections.rs.

CREATE TABLE IF NOT EXISTS casual_performance_metric_histogram (
  role_id      INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
  role_name    TEXT NOT NULL,
  metric       TEXT NOT NULL CHECK (metric IN ('dpm','hpm','gpm','mpm')),
  value        DOUBLE PRECISION NOT NULL,
  sample_count BIGINT NOT NULL CHECK (sample_count > 0),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (role_id, metric, value)
);

CREATE TABLE IF NOT EXISTS casual_performance_metric_stats (
  role_id      INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
  role_name    TEXT NOT NULL,
  metric       TEXT NOT NULL CHECK (metric IN ('dpm','hpm','gpm','mpm')),
  min_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
  max_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
  mean_value   DOUBLE PRECISION NOT NULL DEFAULT 0,
  median_value DOUBLE PRECISION NOT NULL DEFAULT 0,
  mode_value   DOUBLE PRECISION NOT NULL DEFAULT 0,
  p10_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
  p25_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
  p75_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
  p90_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
  sample_size  BIGINT NOT NULL DEFAULT 0,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (role_id, metric)
);

-- Exactly-once ledger for the casual performance projection (mirrors
-- performance_projection_matches). Idempotency only; per-match evidence stays
-- canonical in casual_match_players.
CREATE TABLE IF NOT EXISTS casual_performance_projection_matches (
  match_id     BIGINT PRIMARY KEY,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ANALYZE casual_performance_metric_histogram;
ANALYZE casual_performance_metric_stats;