-- One authoritative tier classification per ranked match. Every public
-- aggregate uses this row so maps/bans and player-level metrics share the same
-- lobby scope. Tier 0 means the match had no known real-player ranked tier.
CREATE TABLE IF NOT EXISTS match_lobby_tiers (
  match_id BIGINT NOT NULL,
  entry_datetime TIMESTAMPTZ NOT NULL,
  lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
  known_players SMALLINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id, entry_datetime)
);

CREATE INDEX IF NOT EXISTS idx_match_lobby_tiers_scope
  ON match_lobby_tiers (lobby_tier, match_id);

INSERT INTO match_lobby_tiers (match_id, entry_datetime, lobby_tier, known_players, updated_at)
SELECT m.match_id, m.entry_datetime,
  COALESCE(ROUND(AVG(mp.league_tier) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)), 0)::SMALLINT,
  COUNT(*) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)::SMALLINT,
  now()
FROM matches m
LEFT JOIN match_players mp ON mp.match_id = m.match_id AND mp.entry_datetime = m.entry_datetime
  AND mp.player_id > 0 AND mp.champion_id > 0
WHERE m.queue_id = 486
GROUP BY m.match_id, m.entry_datetime
ON CONFLICT (match_id, entry_datetime) DO UPDATE SET
  lobby_tier = EXCLUDED.lobby_tier,
  known_players = EXCLUDED.known_players,
  updated_at = EXCLUDED.updated_at;

DELETE FROM skin_counts_ranked;

INSERT INTO skin_counts_ranked (
  champion_id, skin_id, league_tier, skin_name, count, wins, losses, updated_at
)
SELECT mp.champion_id, mp.skin_id, mlt.lobby_tier,
  MAX(COALESCE(NULLIF(mp.skin_name, ''), s.skin_name, 'Unknown Skin')),
  COUNT(*)::INT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::INT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::INT,
  now()
FROM match_players mp
JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
LEFT JOIN skins s ON s.skin_id = mp.skin_id
WHERE m.queue_id = 486 AND mp.champion_id > 0 AND mp.skin_id > 0
  AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
GROUP BY mp.champion_id, mp.skin_id, mlt.lobby_tier;

COMMENT ON TABLE match_lobby_tiers IS
  'Rounded average known real-player tier for each queue-486 match; shared filter dimension for public aggregate statistics.';
