-- Canonical ranked party tracking.
--
-- PartyId is scoped to a match/session.  Store the sorted player-id set as the
-- durable group identity, and store each match contribution once.  Full party
-- groups (2-5 players) and every unordered pair derived from those groups have
-- separate aggregate projections.

CREATE TABLE IF NOT EXISTS match_party_groups (
  match_id           BIGINT NOT NULL,
  group_key          TEXT NOT NULL,
  player_ids         BIGINT[] NOT NULL,
  stack_size         SMALLINT NOT NULL,
  task_force         SMALLINT NOT NULL,
  observed_party_id  INTEGER NOT NULL,
  max_known_tier     SMALLINT NOT NULL DEFAULT 0,
  entry_datetime     TIMESTAMPTZ NOT NULL,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id, group_key),
  CHECK (stack_size BETWEEN 2 AND 5),
  CHECK (stack_size = cardinality(player_ids)),
  CHECK (task_force IN (1, 2))
);

CREATE INDEX IF NOT EXISTS idx_match_party_groups_players
  ON match_party_groups USING GIN (player_ids);
CREATE INDEX IF NOT EXISTS idx_match_party_groups_time
  ON match_party_groups (entry_datetime DESC, match_id DESC);

CREATE TABLE IF NOT EXISTS party_stack_stats (
  group_key       TEXT PRIMARY KEY,
  player_ids      BIGINT[] NOT NULL,
  stack_size      SMALLINT NOT NULL,
  match_count     INTEGER NOT NULL DEFAULT 0,
  first_seen      TIMESTAMPTZ NOT NULL,
  last_seen       TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (stack_size BETWEEN 2 AND 5),
  CHECK (stack_size = cardinality(player_ids))
);

CREATE INDEX IF NOT EXISTS idx_party_stack_stats_players
  ON party_stack_stats USING GIN (player_ids);
CREATE INDEX IF NOT EXISTS idx_party_stack_stats_size_count
  ON party_stack_stats (stack_size DESC, match_count DESC, last_seen DESC);

CREATE TABLE IF NOT EXISTS match_party_pairs (
  match_id          BIGINT NOT NULL,
  player_low_id     BIGINT NOT NULL,
  player_high_id    BIGINT NOT NULL,
  source_group_key  TEXT NOT NULL,
  entry_datetime    TIMESTAMPTZ NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id, player_low_id, player_high_id),
  CHECK (player_low_id < player_high_id)
);

CREATE INDEX IF NOT EXISTS idx_match_party_pairs_low
  ON match_party_pairs (player_low_id, entry_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_match_party_pairs_high
  ON match_party_pairs (player_high_id, entry_datetime DESC);

CREATE TABLE IF NOT EXISTS party_pair_stats (
  player_low_id   BIGINT NOT NULL,
  player_high_id  BIGINT NOT NULL,
  match_count     INTEGER NOT NULL DEFAULT 0,
  first_seen      TIMESTAMPTZ NOT NULL,
  last_seen       TIMESTAMPTZ NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (player_low_id, player_high_id),
  CHECK (player_low_id < player_high_id)
);

CREATE INDEX IF NOT EXISTS idx_party_pair_stats_low_count
  ON party_pair_stats (player_low_id, match_count DESC, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_party_pair_stats_high_count
  ON party_pair_stats (player_high_id, match_count DESC, last_seen DESC);

COMMENT ON TABLE match_party_groups IS
  'One immutable ranked party group per match. player_ids are sorted and group_key is direction-independent.';
COMMENT ON TABLE party_stack_stats IS
  'Search projection for exact ranked party groups of 2-5 identified players.';
COMMENT ON TABLE match_party_pairs IS
  'One unordered party pair per match; a five-stack contributes ten distinct pair facts.';
COMMENT ON TABLE party_pair_stats IS
  'Canonical unordered party-pair counts rebuilt from match_party_pairs.';

-- Backfill full historical groups from authoritative ranked match facts.
WITH grouped AS (
  SELECT
    mp.match_id,
    mp.task_force,
    mp.party_id AS observed_party_id,
    array_agg(DISTINCT mp.player_id ORDER BY mp.player_id) AS player_ids,
    count(DISTINCT mp.player_id)::SMALLINT AS stack_size,
    COALESCE(max(mp.league_tier) FILTER (WHERE mp.league_tier > 0), 0)::SMALLINT AS max_known_tier,
    min(mp.entry_datetime) AS entry_datetime
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id
  WHERE m.queue_id = 486
    AND COALESCE(m.is_ranked, TRUE)
    AND mp.player_id > 0
    AND mp.party_id IS NOT NULL
    AND mp.party_id <> 0
    AND mp.task_force IN (1, 2)
    AND mp.champion_id > 0
    AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
  GROUP BY mp.match_id, mp.task_force, mp.party_id
), eligible AS (
  SELECT
    match_id,
    array_to_string(player_ids, ':') AS group_key,
    player_ids,
    stack_size,
    task_force,
    observed_party_id,
    max_known_tier,
    entry_datetime
  FROM grouped
  WHERE stack_size BETWEEN 2 AND 5
    -- Diamond and above are limited to duos. Unknown tier (0) is not used to
    -- reject an observed group, but any known tier above Platinum I makes a
    -- 3-5 player PartyId group invalid/suspect.
    AND (stack_size = 2 OR max_known_tier <= 20)
)
INSERT INTO match_party_groups (
  match_id, group_key, player_ids, stack_size, task_force,
  observed_party_id, max_known_tier, entry_datetime
)
SELECT
  match_id, group_key, player_ids, stack_size, task_force,
  observed_party_id, max_known_tier, entry_datetime
FROM eligible
ON CONFLICT (match_id, group_key) DO NOTHING;

INSERT INTO party_stack_stats (
  group_key, player_ids, stack_size, match_count, first_seen, last_seen, updated_at
)
SELECT
  group_key,
  player_ids,
  stack_size,
  count(DISTINCT match_id)::INTEGER,
  min(entry_datetime),
  max(entry_datetime),
  now()
FROM match_party_groups
GROUP BY group_key, player_ids, stack_size
ON CONFLICT (group_key) DO UPDATE SET
  player_ids = EXCLUDED.player_ids,
  stack_size = EXCLUDED.stack_size,
  match_count = EXCLUDED.match_count,
  first_seen = EXCLUDED.first_seen,
  last_seen = EXCLUDED.last_seen,
  updated_at = now();

INSERT INTO match_party_pairs (
  match_id, player_low_id, player_high_id, source_group_key, entry_datetime
)
SELECT
  mpg.match_id,
  low.player_id,
  high.player_id,
  mpg.group_key,
  mpg.entry_datetime
FROM match_party_groups mpg
CROSS JOIN LATERAL unnest(mpg.player_ids) WITH ORDINALITY AS low(player_id, ordinal)
CROSS JOIN LATERAL unnest(mpg.player_ids) WITH ORDINALITY AS high(player_id, ordinal)
WHERE low.ordinal < high.ordinal
ON CONFLICT (match_id, player_low_id, player_high_id) DO NOTHING;

INSERT INTO party_pair_stats (
  player_low_id, player_high_id, match_count, first_seen, last_seen, updated_at
)
SELECT
  player_low_id,
  player_high_id,
  count(DISTINCT match_id)::INTEGER,
  min(entry_datetime),
  max(entry_datetime),
  now()
FROM match_party_pairs
GROUP BY player_low_id, player_high_id
ON CONFLICT (player_low_id, player_high_id) DO UPDATE SET
  match_count = EXCLUDED.match_count,
  first_seen = EXCLUDED.first_seen,
  last_seen = EXCLUDED.last_seen,
  updated_at = now();
