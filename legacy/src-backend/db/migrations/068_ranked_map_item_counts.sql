-- Pre-aggregate ranked item results by map and shared lobby tier. The map
-- detail endpoint previously rebuilt this comparison from every historical
-- match_player_items row, which made a cold page request take 20+ seconds.

CREATE TABLE IF NOT EXISTS map_item_counts_ranked (
  map_name TEXT NOT NULL,
  lobby_tier SMALLINT NOT NULL DEFAULT 0,
  item_id INT NOT NULL,
  count INT NOT NULL DEFAULT 0,
  wins INT NOT NULL DEFAULT 0,
  losses INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (map_name, lobby_tier, item_id)
);

CREATE INDEX IF NOT EXISTS idx_map_item_counts_ranked_item_map
  ON map_item_counts_ranked (item_id, map_name, lobby_tier);

COMMENT ON TABLE map_item_counts_ranked IS
  'Incremental queue-486 item usage grouped by exact Hi-Rez map name and shared lobby tier.';

CREATE TABLE IF NOT EXISTS map_item_counts_ranked_matches (
  match_id BIGINT PRIMARY KEY,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE map_item_counts_ranked_matches IS
  'Idempotency ledger for the incremental ranked map-item projection.';

-- Existing matches completed their count-projection stage before this table
-- existed, so seed the projection once from canonical match facts. Re-running
-- the migration produces the same totals rather than incrementing them.
INSERT INTO map_item_counts_ranked (
  map_name, lobby_tier, item_id, count, wins, losses, updated_at
)
SELECT
  m.map,
  COALESCE(mlt.lobby_tier, 0)::SMALLINT,
  mpi.item_id,
  COUNT(*)::INT,
  COUNT(*) FILTER (
    WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')
  )::INT,
  COUNT(*) FILTER (
    WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')
  )::INT,
  now()
FROM match_player_items mpi
JOIN match_players mp
  ON mp.match_id = mpi.match_id
 AND mp.player_id = mpi.player_id
JOIN matches m
  ON m.match_id = mp.match_id
 AND m.entry_datetime = mp.entry_datetime
JOIN match_lobby_tiers mlt
  ON mlt.match_id = m.match_id
 AND mlt.entry_datetime = m.entry_datetime
WHERE m.queue_id = 486
  AND NULLIF(m.map, '') IS NOT NULL
GROUP BY m.map, COALESCE(mlt.lobby_tier, 0), mpi.item_id
ON CONFLICT (map_name, lobby_tier, item_id) DO UPDATE SET
  count = EXCLUDED.count,
  wins = EXCLUDED.wins,
  losses = EXCLUDED.losses,
  updated_at = EXCLUDED.updated_at;

-- Seed the idempotency ledger in the same transaction as the historical
-- aggregate so a later targeted replay cannot count an old match twice.
INSERT INTO map_item_counts_ranked_matches (match_id)
SELECT DISTINCT m.match_id
FROM matches m
JOIN match_lobby_tiers mlt
  ON mlt.match_id = m.match_id
 AND mlt.entry_datetime = m.entry_datetime
WHERE m.queue_id = 486
  AND NULLIF(m.map, '') IS NOT NULL
ON CONFLICT (match_id) DO NOTHING;
