-- Exactly-once ledger for the shared-fact -> casual item aggregate pilot.
--
-- This table owns projection idempotency only. Per-match item evidence remains
-- in match_player_items, and ranked item aggregates remain independently owned.

CREATE TABLE IF NOT EXISTS item_counts_casual_matches (
  match_id BIGINT PRIMARY KEY,
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  eligible_players SMALLINT NOT NULL CHECK (eligible_players >= 0),
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_item_counts_casual_matches_scope
  ON item_counts_casual_matches (stats_scope, queue_id, projected_at DESC);
COMMENT ON TABLE item_counts_casual_matches IS
  'Exactly-once ledger for item_counts_casual. Claims only complete non-ranked canonical match facts.';
