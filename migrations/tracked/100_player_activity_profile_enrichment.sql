-- Rolling per-queue player presence and TTL-aware profile enrichment for the
-- public activity dashboard. Global uniqueness remains owned by
-- player_presence_24h; this table permits one player to appear in every queue
-- they actually played without inflating the global 24-hour total.

CREATE TABLE IF NOT EXISTS player_queue_presence_24h (
  player_id BIGINT NOT NULL,
  queue_id INT NOT NULL,
  stats_scope VARCHAR(32) NOT NULL,
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at TIMESTAMPTZ NOT NULL,
  last_match_id BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (player_id, queue_id)
);
CREATE INDEX IF NOT EXISTS idx_player_queue_presence_window
  ON player_queue_presence_24h (last_observed_at DESC, queue_id);
CREATE INDEX IF NOT EXISTS idx_player_queue_presence_queue_window
  ON player_queue_presence_24h (queue_id, last_observed_at DESC);

-- Seed the most recent known queue so the dashboard is not empty immediately
-- after deployment. Future observations add any additional queues played
-- during the rolling window.
INSERT INTO player_queue_presence_24h (
  player_id, queue_id, stats_scope, first_observed_at,
  last_observed_at, last_match_id
)
SELECT
  player_id, last_queue_id, last_stats_scope, first_observed_at,
  last_observed_at, last_match_id
FROM player_presence_24h
WHERE last_observed_at >= now() - interval '24 hours'
ON CONFLICT (player_id, queue_id) DO UPDATE SET
  first_observed_at = LEAST(
    player_queue_presence_24h.first_observed_at,
    EXCLUDED.first_observed_at
  ),
  last_observed_at = GREATEST(
    player_queue_presence_24h.last_observed_at,
    EXCLUDED.last_observed_at
  ),
  last_match_id = CASE
    WHEN EXCLUDED.last_observed_at >= player_queue_presence_24h.last_observed_at
      THEN EXCLUDED.last_match_id
    ELSE player_queue_presence_24h.last_match_id
  END,
  stats_scope = EXCLUDED.stats_scope,
  updated_at = now();

-- Successful profiles are cached in players.hirez_profile_refreshed_at. This
-- table is only the worker lease and negative/failure cache, preventing an
-- unavailable player ID from consuming another API call every hour.
CREATE TABLE IF NOT EXISTS player_activity_profile_refresh (
  player_id BIGINT PRIMARY KEY,
  status VARCHAR(24) NOT NULL DEFAULT 'pending' CHECK (
    status IN ('pending', 'fetching', 'success', 'unavailable', 'failed', 'skipped_recent')
  ),
  attempts INT NOT NULL DEFAULT 0,
  last_attempt_at TIMESTAMPTZ,
  last_success_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  lease_until TIMESTAMPTZ,
  error_message TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_player_activity_profile_refresh_due
  ON player_activity_profile_refresh (status, next_retry_at, lease_until, last_attempt_at);
CREATE INDEX IF NOT EXISTS idx_players_active_player_id
  ON players (active_player_id)
  WHERE active_player_id IS NOT NULL AND active_player_id > 0;

COMMENT ON TABLE player_queue_presence_24h IS
  'Rolling public-player presence keyed by player and queue; queries apply the 24-hour cutoff.';
COMMENT ON TABLE player_activity_profile_refresh IS
  'Lease and negative cache for getplayerbatch activity-profile enrichment; successful freshness lives on players.';
