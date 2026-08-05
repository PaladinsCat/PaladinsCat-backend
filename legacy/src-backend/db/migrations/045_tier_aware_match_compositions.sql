-- Tier-aware ranked composition projection. A row represents one complete
-- five-player team, bucketed by the shared match lobby tier. Tier 0 is kept for
-- all-lobby totals but is excluded whenever an explicit tier range is active.
CREATE TABLE IF NOT EXISTS match_compositions_ranked (
  comp_id TEXT NOT NULL,
  lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
  frontline SMALLINT NOT NULL,
  damage SMALLINT NOT NULL,
  flank SMALLINT NOT NULL,
  support SMALLINT NOT NULL,
  count INT NOT NULL DEFAULT 0,
  wins INT NOT NULL DEFAULT 0,
  losses INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (comp_id, lobby_tier)
);

CREATE INDEX IF NOT EXISTS idx_match_compositions_ranked_tier_count
  ON match_compositions_ranked (lobby_tier, count DESC);

WITH team_comps AS (
  SELECT
    mp.match_id,
    mp.entry_datetime,
    mp.task_force,
    m.winning_task_force,
    mlt.lobby_tier,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%')::SMALLINT AS frontline,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Damage%')::SMALLINT AS damage,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Flank%')::SMALLINT AS flank,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Support%')::SMALLINT AS support
  FROM match_players mp
  JOIN matches m
    ON m.match_id = mp.match_id
   AND m.entry_datetime = mp.entry_datetime
  JOIN match_lobby_tiers mlt
    ON mlt.match_id = m.match_id
   AND mlt.entry_datetime = m.entry_datetime
  JOIN champions c ON c.id = mp.champion_id
  WHERE m.queue_id = 486
    AND mp.task_force IS NOT NULL
    AND mp.task_force != 0
    AND mp.champion_id > 0
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
  GROUP BY mp.match_id, mp.entry_datetime, mp.task_force, m.winning_task_force, mlt.lobby_tier
  HAVING COUNT(*) = 5
)
INSERT INTO match_compositions_ranked (
  comp_id, lobby_tier, frontline, damage, flank, support, count, wins, losses, updated_at
)
SELECT
  frontline || '-' || damage || '-' || flank || '-' || support,
  lobby_tier,
  frontline,
  damage,
  flank,
  support,
  COUNT(*)::INT,
  COUNT(*) FILTER (WHERE task_force = winning_task_force)::INT,
  COUNT(*) FILTER (WHERE task_force != winning_task_force)::INT,
  now()
FROM team_comps
WHERE frontline + damage + flank + support = 5
GROUP BY lobby_tier, frontline, damage, flank, support
ON CONFLICT (comp_id, lobby_tier) DO UPDATE SET
  frontline = EXCLUDED.frontline,
  damage = EXCLUDED.damage,
  flank = EXCLUDED.flank,
  support = EXCLUDED.support,
  count = EXCLUDED.count,
  wins = EXCLUDED.wins,
  losses = EXCLUDED.losses,
  updated_at = EXCLUDED.updated_at;

COMMENT ON TABLE match_compositions_ranked IS
  'Tier-bucketed queue-486 complete-team composition projection maintained by ingestion and derived-projection repair.';
