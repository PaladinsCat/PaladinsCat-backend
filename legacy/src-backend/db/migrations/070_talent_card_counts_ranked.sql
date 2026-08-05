-- Compact projection for the champion-card page's selected-talent view.
-- The old request path repeatedly scanned historical card facts and timed out
-- on cold cache misses. Keep that work in the ingest/repair pipeline instead.

CREATE TABLE IF NOT EXISTS talent_card_counts_ranked (
    talent_id     INT NOT NULL,
    card_id       INT NOT NULL,
    card_level    SMALLINT NOT NULL DEFAULT 0,
    count         INT NOT NULL DEFAULT 0,
    wins          INT NOT NULL DEFAULT 0,
    losses        INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (talent_id, card_id, card_level)
);

CREATE INDEX IF NOT EXISTS idx_talent_card_counts_ranked_card
    ON talent_card_counts_ranked (card_id, talent_id);

COMMENT ON TABLE talent_card_counts_ranked IS
  'Ranked card and level usage grouped by selected talent for champion card pages.';

INSERT INTO talent_card_counts_ranked (
  talent_id, card_id, card_level, count, wins, losses, updated_at
)
SELECT
  mpt.talent_id,
  mpc.card_id,
  COALESCE(mpc.card_level, 0)::SMALLINT,
  COUNT(*)::INT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::INT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::INT,
  now()
FROM match_player_talents mpt
JOIN match_player_cards mpc
  ON mpc.match_id = mpt.match_id
 AND mpc.player_id = mpt.player_id
JOIN match_players mp
  ON mp.match_id = mpt.match_id
 AND mp.player_id = mpt.player_id
JOIN matches m ON m.match_id = mp.match_id
WHERE m.queue_id = 486
GROUP BY mpt.talent_id, mpc.card_id, COALESCE(mpc.card_level, 0)
ON CONFLICT (talent_id, card_id, card_level) DO UPDATE SET
  count = EXCLUDED.count,
  wins = EXCLUDED.wins,
  losses = EXCLUDED.losses,
  updated_at = EXCLUDED.updated_at;
