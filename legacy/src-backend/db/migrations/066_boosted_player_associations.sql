-- Players who have appeared in a canonical ranked party with a confirmed
-- cheater. This is deliberately a view rather than a copied flag: a new party
-- pair or a newly confirmed cheater is reflected immediately and reversions do
-- not leave stale boosted labels behind.

CREATE OR REPLACE VIEW player_boosted_associations AS
SELECT
  pair_stats.player_high_id AS player_id,
  cheater.id AS cheater_id,
  pair_stats.match_count,
  pair_stats.first_seen,
  pair_stats.last_seen
FROM party_pair_stats pair_stats
JOIN players cheater ON cheater.id = pair_stats.player_low_id
JOIN players boosted ON boosted.id = pair_stats.player_high_id
WHERE cheater.cheater = TRUE
  AND boosted.cheater = FALSE

UNION ALL

SELECT
  pair_stats.player_low_id AS player_id,
  cheater.id AS cheater_id,
  pair_stats.match_count,
  pair_stats.first_seen,
  pair_stats.last_seen
FROM party_pair_stats pair_stats
JOIN players cheater ON cheater.id = pair_stats.player_high_id
JOIN players boosted ON boosted.id = pair_stats.player_low_id
WHERE cheater.cheater = TRUE
  AND boosted.cheater = FALSE;

COMMENT ON VIEW player_boosted_associations IS
  'Derived ranked-party associations: each row links a non-cheater party member to a confirmed cheater and its observed party evidence.';

CREATE INDEX IF NOT EXISTS idx_players_cheater_id
  ON players (id)
  WHERE cheater = TRUE;
