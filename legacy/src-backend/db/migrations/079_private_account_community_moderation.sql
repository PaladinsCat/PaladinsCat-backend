-- Private identities need their own vote namespace because every private
-- scoreboard participant has the shared public player_id=0 sentinel.
ALTER TABLE players_private
  ADD COLUMN IF NOT EXISTS sus_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_players_private_suspicious
  ON players_private (sus_count DESC, last_seen DESC, id DESC)
  WHERE sus_count > 0;

CREATE TABLE IF NOT EXISTS private_account_community_votes (
  id BIGSERIAL PRIMARY KEY,
  private_player_id INTEGER NOT NULL REFERENCES players_private(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  vote_type VARCHAR(20) NOT NULL CHECK (vote_type IN ('suspicious', 'cheater')),
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_private_account_community_vote UNIQUE (private_player_id, user_id, vote_type)
);

CREATE INDEX IF NOT EXISTS idx_private_account_votes_target
  ON private_account_community_votes (private_player_id, vote_type, created_at DESC);

COMMENT ON COLUMN players_private.sus_count IS 'Unique authenticated suspicious votes for the canonical private identity.';
COMMENT ON TABLE private_account_community_votes IS 'Auditable per-user SUS and confirmed-cheater decisions for private identities.';
