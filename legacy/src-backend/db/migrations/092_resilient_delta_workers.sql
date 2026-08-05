-- Keep durable match facts independent from retryable projection debt and
-- replace recurring rating/outcome history scans with cumulative state.

ALTER TABLE raw_ingest_buffer
  ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_rib_pending_available
  ON raw_ingest_buffer (available_at, created_at, id)
  WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS player_champion_outcome_summary (
  queue_id INT NOT NULL,
  player_id BIGINT NOT NULL,
  champion_id INT NOT NULL,
  total_matches BIGINT NOT NULL DEFAULT 0 CHECK (total_matches >= 0),
  total_wins BIGINT NOT NULL DEFAULT 0 CHECK (total_wins >= 0),
  total_losses BIGINT NOT NULL DEFAULT 0 CHECK (total_losses >= 0),
  last_match_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, player_id, champion_id)
);

CREATE INDEX IF NOT EXISTS idx_player_champion_outcome_rank
  ON player_champion_outcome_summary
  (queue_id, champion_id, total_matches DESC, player_id);

-- This is the one historical fold. Live workers add only newly claimed match
-- deltas under performance_projection_matches, so these source tables are not
-- scanned again for ordinary match ingestion.
DELETE FROM player_queue_rating_summary;
INSERT INTO player_queue_rating_summary (
  queue_id, player_id, total_matches, total_wins, total_losses, updated_at
)
SELECT m.queue_id, mp.player_id, COUNT(*)::BIGINT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::BIGINT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::BIGINT,
  now()
FROM match_players mp
JOIN matches m
  ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
WHERE mp.player_id > 0
  AND mp.champion_id > 0
  AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
  AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
GROUP BY m.queue_id, mp.player_id;

INSERT INTO player_champion_outcome_summary (
  queue_id, player_id, champion_id,
  total_matches, total_wins, total_losses, last_match_at, updated_at
)
SELECT m.queue_id, mp.player_id, mp.champion_id, COUNT(*)::BIGINT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::BIGINT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::BIGINT,
  MAX(m.entry_datetime), now()
FROM match_players mp
JOIN matches m
  ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
WHERE mp.player_id > 0
  AND mp.champion_id > 0
  AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
  AND lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')
GROUP BY m.queue_id, mp.player_id, mp.champion_id
ON CONFLICT (queue_id, player_id, champion_id) DO UPDATE SET
  total_matches = EXCLUDED.total_matches,
  total_wins = EXCLUDED.total_wins,
  total_losses = EXCLUDED.total_losses,
  last_match_at = EXCLUDED.last_match_at,
  updated_at = now();

UPDATE player_best_champion_ratings best
SET matches_played = outcomes.total_matches,
    wins = outcomes.total_wins,
    losses = outcomes.total_losses,
    updated_at = now()
FROM player_champion_outcome_summary outcomes
WHERE outcomes.queue_id = best.queue_id
  AND outcomes.player_id = best.player_id
  AND outcomes.champion_id = best.champion_id;

CREATE TABLE IF NOT EXISTS rating_player_cursors (
  queue_id INT NOT NULL,
  player_id BIGINT NOT NULL,
  last_match_id BIGINT NOT NULL,
  last_entry_datetime TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, player_id)
);

CREATE TABLE IF NOT EXISTS rating_late_match_applications (
  match_id BIGINT PRIMARY KEY,
  entry_datetime TIMESTAMPTZ NOT NULL,
  latest_player_cursor_at TIMESTAMPTZ NOT NULL,
  policy TEXT NOT NULL CHECK (policy = 'arrival_order_delta'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Older workers marked `ratings` complete even when the global rebuild flag
-- made the calculator return without a snapshot. Reopen only active durable
-- buffer work; the new worker will apply it once under the snapshot guard.
UPDATE match_ingest_status mis
SET completed_stages = array_remove(mis.completed_stages, 'ratings'),
    updated_at = now(),
    error_message = 'rating stage reopened: no immutable snapshot was written'
WHERE mis.completed_stages @> ARRAY['ratings']::TEXT[]
  AND mis.completed_stages @> ARRAY['player_facts', 'match_bans']::TEXT[]
  AND NOT EXISTS (
    SELECT 1 FROM match_rating_snapshots snapshot
    WHERE snapshot.match_id = mis.match_id
  )
  AND EXISTS (
    SELECT 1 FROM raw_ingest_buffer rib
    WHERE rib.entity_type = 'match'
      AND rib.entity_id = mis.match_id::TEXT
      AND rib.status IN ('pending', 'processing')
  );

-- Seed one cursor per player from the immutable rating audit. Thereafter late
-- detection is an indexed lookup for the current roster, not a history join.
INSERT INTO rating_player_cursors (
  queue_id, player_id, last_match_id, last_entry_datetime, updated_at
)
SELECT DISTINCT ON (m.queue_id, mrs.player_id)
  m.queue_id, mrs.player_id, m.match_id, m.entry_datetime, now()
FROM match_rating_snapshots mrs
JOIN matches m ON m.match_id = mrs.match_id
ORDER BY m.queue_id, mrs.player_id, m.entry_datetime DESC, m.match_id DESC
ON CONFLICT (queue_id, player_id) DO UPDATE SET
  last_match_id = EXCLUDED.last_match_id,
  last_entry_datetime = EXCLUDED.last_entry_datetime,
  updated_at = now();

COMMENT ON TABLE player_queue_rating_summary IS
  'Cumulative per-player queue W/L history folded exactly once from match deltas; independent of Glicko snapshot availability.';
COMMENT ON TABLE player_champion_outcome_summary IS
  'Cumulative per-player/champion W/L history folded exactly once from match deltas.';
COMMENT ON TABLE rating_player_cursors IS
  'Latest chronologically applied rating match per queue/player; makes late-match detection bounded by roster size.';
COMMENT ON TABLE rating_late_match_applications IS
  'Audit of delayed matches applied once against current Glicko state instead of triggering an unbounded full-history replay.';
