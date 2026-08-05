-- Preserve the player profile state captured by the post-ingest getplayerbatch
-- call. Match pages must render this immutable snapshot instead of reading the
-- mutable players row or contacting Hi-Rez when somebody opens a match.

CREATE TABLE IF NOT EXISTS match_player_profile_snapshots (
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source TEXT NOT NULL DEFAULT 'post_match_ingest',
  level INTEGER,
  platform TEXT,
  region TEXT,
  global_wins INTEGER,
  global_losses INTEGER,
  kbm_tier INTEGER,
  kbm_points INTEGER,
  kbm_rank INTEGER,
  kbm_wins INTEGER,
  kbm_losses INTEGER,
  champion_wins INTEGER,
  champion_losses INTEGER,
  PRIMARY KEY (match_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_match_player_profile_snapshots_player
  ON match_player_profile_snapshots (player_id, captured_at DESC);

COMMENT ON TABLE match_player_profile_snapshots IS
  'Immutable per-match player profile state captured by the post-ingest getplayerbatch call; match reads are database-only.';
