-- paladinscat:requires-full-backup
-- Restore ranked-party facts missed after the Rust ingestion cutover, then
-- rebuild both aggregate projections from their immutable match facts.

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
  JOIN matches m
    ON m.match_id = mp.match_id
   AND m.entry_datetime = mp.entry_datetime
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

DELETE FROM party_stack_stats;

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
GROUP BY group_key, player_ids, stack_size;

DELETE FROM party_pair_stats;

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
GROUP BY player_low_id, player_high_id;
