-- Private identities use their canonical players_private.id namespace for
-- moderation. Never attach a decision to match_players.player_id=0: every
-- private participant shares that public sentinel.
ALTER TABLE players_private
  ADD COLUMN IF NOT EXISTS cheater BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS cheater_reason TEXT,
  ADD COLUMN IF NOT EXISTS cheater_marked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_players_private_cheater
  ON players_private (last_seen DESC, id DESC)
  WHERE cheater;

-- Operator-confirmed Terminus ability exploit cases. Resolve through the
-- durable match link so replaying this migration in another database cannot
-- tag an unrelated row that happens to reuse production's serial ID.
UPDATE players_private pp
SET cheater = TRUE,
    cheater_reason = 'Terminus kill and ability-damage exploit signature',
    cheater_marked_at = COALESCE(cheater_marked_at, now()),
    updated_at = now()
WHERE EXISTS (
  SELECT 1
  FROM match_players mp
  LEFT JOIN private_account_observations observation
    ON observation.match_id = mp.match_id
   AND observation.private_slot = CASE WHEN mp.private_slot > 0 THEN mp.private_slot ELSE 1 END
  WHERE mp.match_id IN (1280259034, 1280371818)
    AND mp.champion_id = 2477
    AND mp.player_id = 0
    AND pp.id = COALESCE(NULLIF(mp.private_player_id, 0), observation.private_player_id)
);
