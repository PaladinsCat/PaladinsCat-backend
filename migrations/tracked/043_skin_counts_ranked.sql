-- Tier-aware ranked skin projection. Historical facts are backfilled once;
-- buffer ingestion maintains new rows incrementally after deployment.
CREATE TABLE IF NOT EXISTS skin_counts_ranked (
  champion_id INT NOT NULL REFERENCES champions(id),
  skin_id INT NOT NULL,
  league_tier SMALLINT NOT NULL DEFAULT 0 CHECK (league_tier BETWEEN 0 AND 26),
  skin_name TEXT NOT NULL,
  count INT NOT NULL DEFAULT 0,
  wins INT NOT NULL DEFAULT 0,
  losses INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (champion_id, skin_id, league_tier)
);

CREATE INDEX IF NOT EXISTS idx_skin_counts_ranked_tier
  ON skin_counts_ranked (league_tier, champion_id, skin_id);

INSERT INTO skin_counts_ranked (
  champion_id, skin_id, league_tier, skin_name, count, wins, losses, updated_at
)
SELECT
  mp.champion_id,
  mp.skin_id,
  CASE WHEN mp.league_tier BETWEEN 1 AND 26 THEN mp.league_tier ELSE 0 END::SMALLINT,
  MAX(COALESCE(NULLIF(mp.skin_name, ''), s.skin_name, 'Unknown Skin')) AS skin_name,
  COUNT(*)::INT AS count,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::INT AS wins,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::INT AS losses,
  now()
FROM match_players mp
JOIN matches m
  ON m.match_id = mp.match_id
 AND m.entry_datetime = mp.entry_datetime
LEFT JOIN skins s ON s.skin_id = mp.skin_id
WHERE m.queue_id = 486
  AND mp.champion_id > 0
  AND mp.skin_id IS NOT NULL
  AND mp.skin_id > 0
  AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
GROUP BY
  mp.champion_id,
  mp.skin_id,
  CASE WHEN mp.league_tier BETWEEN 1 AND 26 THEN mp.league_tier ELSE 0 END
ON CONFLICT (champion_id, skin_id, league_tier) DO UPDATE SET
  skin_name = EXCLUDED.skin_name,
  count = EXCLUDED.count,
  wins = EXCLUDED.wins,
  losses = EXCLUDED.losses,
  updated_at = EXCLUDED.updated_at;

COMMENT ON TABLE skin_counts_ranked IS
  'Tier-bucketed queue-486 skin usage projection maintained by ingestion and derived-projection repair.';
