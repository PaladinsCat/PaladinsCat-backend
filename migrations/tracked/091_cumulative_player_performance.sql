-- Replace repeated full-history player average scans with an exactly-once,
-- additive projection. This migration performs the one required historical
-- fold; live ingestion only applies new match deltas after it completes.

CREATE TABLE IF NOT EXISTS player_performance_projection_matches (
  match_id BIGINT PRIMARY KEY,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS player_performance_aggregate (
  player_id BIGINT PRIMARY KEY REFERENCES players(id) ON DELETE CASCADE,
  sample_count BIGINT NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
  egpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  dpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  hpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  shpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  mpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TEMP TABLE eligible_player_performance_matches
ON COMMIT DROP AS
SELECT m.match_id, m.entry_datetime, m.duration_seconds
FROM matches m
JOIN match_players mp_check
  ON mp_check.match_id = m.match_id
 AND mp_check.entry_datetime = m.entry_datetime
WHERE m.queue_id = 486
  AND COALESCE(m.is_ranked, m.queue_id = 486) = true
GROUP BY m.match_id, m.entry_datetime, m.duration_seconds
HAVING COUNT(*) = 10
  AND COUNT(*) FILTER (WHERE mp_check.task_force = 1) = 5
  AND COUNT(*) FILTER (WHERE mp_check.task_force = 2) = 5
  AND COUNT(*) FILTER (
    WHERE lower(COALESCE(mp_check.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
  ) = 10
  AND (
    COUNT(*) FILTER (
      WHERE COALESCE(mp_check.source, 'direct') IN ('direct', 'recovered')
        AND mp_check.champion_id > 0
        AND mp_check.task_force IN (1, 2)
        AND lower(COALESCE(mp_check.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
    )
    + COUNT(*) FILTER (
      WHERE mp_check.player_id = 0
        AND COALESCE(mp_check.champion_id, 0) = 0
        AND upper(COALESCE(mp_check.player_name, '')) = 'PRIVATEACCOUNT'
        AND COALESCE(mp_check.source, 'minimal') = 'minimal'
    )
  ) = 10;

CREATE INDEX ON eligible_player_performance_matches (match_id, entry_datetime);

INSERT INTO player_performance_aggregate (
  player_id, sample_count, egpm_sum, dpm_sum, hpm_sum, shpm_sum, mpm_sum, updated_at
)
SELECT mp.player_id, COUNT(*)::BIGINT,
  COALESCE(SUM(mp.egpm), 0)::DOUBLE PRECISION,
  COALESCE(SUM(mp.damage_per_minute), 0)::DOUBLE PRECISION,
  COALESCE(SUM(mp.healing_per_minute), 0)::DOUBLE PRECISION,
  COALESCE(SUM(mp.healing_self_per_minute), 0)::DOUBLE PRECISION,
  COALESCE(SUM(mp.mitigation_per_minute), 0)::DOUBLE PRECISION,
  now()
FROM eligible_player_performance_matches em
JOIN match_players mp
  ON mp.match_id = em.match_id
 AND mp.entry_datetime = em.entry_datetime
WHERE COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
  AND mp.player_id > 0
  AND mp.champion_id > 0
  AND mp.task_force IN (1, 2)
  AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
  AND em.duration_seconds > 120
GROUP BY mp.player_id
ON CONFLICT (player_id) DO UPDATE SET
  sample_count = EXCLUDED.sample_count,
  egpm_sum = EXCLUDED.egpm_sum,
  dpm_sum = EXCLUDED.dpm_sum,
  hpm_sum = EXCLUDED.hpm_sum,
  shpm_sum = EXCLUDED.shpm_sum,
  mpm_sum = EXCLUDED.mpm_sum,
  updated_at = now();

INSERT INTO player_performance_projection_matches (match_id)
SELECT match_id FROM eligible_player_performance_matches
ON CONFLICT DO NOTHING;

UPDATE players p
SET avg_egpm = ROUND((a.egpm_sum / NULLIF(a.sample_count, 0))::NUMERIC, 2)::DOUBLE PRECISION,
    avg_dpm = ROUND((a.dpm_sum / NULLIF(a.sample_count, 0))::NUMERIC, 2)::DOUBLE PRECISION,
    avg_hpm = ROUND((a.hpm_sum / NULLIF(a.sample_count, 0))::NUMERIC, 2)::DOUBLE PRECISION,
    avg_shpm = ROUND((a.shpm_sum / NULLIF(a.sample_count, 0))::NUMERIC, 2)::DOUBLE PRECISION,
    avg_mpm = ROUND((a.mpm_sum / NULLIF(a.sample_count, 0))::NUMERIC, 2)::DOUBLE PRECISION,
    last_updated = now()
FROM player_performance_aggregate a
WHERE p.id = a.player_id;

COMMENT ON TABLE player_performance_aggregate IS
  'Cumulative sums and sample counts for players.avg_*; live ingestion applies only newly claimed match deltas.';
COMMENT ON TABLE player_performance_projection_matches IS
  'Exactly-once ledger for cumulative player performance match deltas.';
