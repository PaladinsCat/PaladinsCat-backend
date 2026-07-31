-- Exactly-once ledgers for the remaining shared-fact -> casual aggregate
-- projections. Per-match evidence stays canonical; these tables only own
-- projection idempotency for physically isolated casual statistics.

CREATE TABLE IF NOT EXISTS talent_counts_casual_matches (
  match_id BIGINT PRIMARY KEY,
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  eligible_players SMALLINT NOT NULL CHECK (eligible_players >= 0),
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_talent_counts_casual_matches_scope
  ON talent_counts_casual_matches (stats_scope, queue_id, projected_at DESC);

CREATE TABLE IF NOT EXISTS card_counts_casual_matches (
  match_id BIGINT PRIMARY KEY,
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  eligible_players SMALLINT NOT NULL CHECK (eligible_players >= 0),
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_card_counts_casual_matches_scope
  ON card_counts_casual_matches (stats_scope, queue_id, projected_at DESC);

COMMENT ON TABLE talent_counts_casual_matches IS
  'Exactly-once ledger for talent_counts_casual. Claims only complete non-ranked canonical match facts.';
COMMENT ON TABLE card_counts_casual_matches IS
  'Exactly-once ledger for card_counts_casual. Claims only complete non-ranked canonical match facts.';
