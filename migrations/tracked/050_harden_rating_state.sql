-- Contain finite-but-corrupt Glicko state, retain volatility audit data, and
-- record chronological rebuild requests. Constraints remain NOT VALID until
-- the controlled rating replay removes the already-corrupt production row.

ALTER TABLE match_rating_snapshots
  ADD COLUMN IF NOT EXISTS queue_volatility_pre NUMERIC,
  ADD COLUMN IF NOT EXISTS queue_volatility_post NUMERIC,
  ADD COLUMN IF NOT EXISTS champ_volatility_pre NUMERIC,
  ADD COLUMN IF NOT EXISTS champ_volatility_post NUMERIC;

CREATE TABLE IF NOT EXISTS rating_rebuild_requests (
  request_key TEXT PRIMARY KEY,
  earliest_entry_datetime TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL,
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The known corrupt production row is finite, so it would otherwise not create
-- a deferred-rebuild request until that player appears in another match. Flag
-- existing out-of-bounds rows at deploy time and freeze incremental ratings
-- until the controlled chronological replay completes.
INSERT INTO rating_rebuild_requests (request_key, earliest_entry_datetime, reason, requested_at)
SELECT
  'global',
  COALESCE(MIN(m.entry_datetime), now()),
  'Existing queue rating is outside safe Glicko bounds',
  now()
FROM player_queue_ratings pqr
LEFT JOIN match_rating_snapshots mrs ON mrs.player_id = pqr.player_id
LEFT JOIN matches m ON m.match_id = mrs.match_id
WHERE pqr.mu NOT BETWEEN 0 AND 3500
   OR pqr.phi NOT BETWEEN 1 AND 350
   OR pqr.volatility NOT BETWEEN 0.001 AND 0.2
HAVING COUNT(*) > 0
ON CONFLICT (request_key) DO UPDATE SET
  earliest_entry_datetime = LEAST(rating_rebuild_requests.earliest_entry_datetime, EXCLUDED.earliest_entry_datetime),
  reason = EXCLUDED.reason,
  requested_at = now();

ALTER TABLE player_queue_ratings
  DROP CONSTRAINT IF EXISTS player_queue_ratings_mu_bounds,
  DROP CONSTRAINT IF EXISTS player_queue_ratings_phi_bounds,
  DROP CONSTRAINT IF EXISTS player_queue_ratings_volatility_bounds;
ALTER TABLE player_queue_ratings
  ADD CONSTRAINT player_queue_ratings_mu_bounds CHECK (mu BETWEEN 0 AND 3500) NOT VALID,
  ADD CONSTRAINT player_queue_ratings_phi_bounds CHECK (phi BETWEEN 1 AND 350) NOT VALID,
  ADD CONSTRAINT player_queue_ratings_volatility_bounds CHECK (volatility BETWEEN 0.001 AND 0.2) NOT VALID;

ALTER TABLE player_champion_ratings
  DROP CONSTRAINT IF EXISTS player_champion_ratings_mu_bounds,
  DROP CONSTRAINT IF EXISTS player_champion_ratings_phi_bounds,
  DROP CONSTRAINT IF EXISTS player_champion_ratings_volatility_bounds;
ALTER TABLE player_champion_ratings
  ADD CONSTRAINT player_champion_ratings_mu_bounds CHECK (mu BETWEEN 0 AND 3500) NOT VALID,
  ADD CONSTRAINT player_champion_ratings_phi_bounds CHECK (phi BETWEEN 1 AND 350) NOT VALID,
  ADD CONSTRAINT player_champion_ratings_volatility_bounds CHECK (volatility BETWEEN 0.001 AND 0.2) NOT VALID;
