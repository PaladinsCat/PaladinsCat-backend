-- A match observation is invalid when its selected talent belongs to a
-- different champion in the canonical talent reference. Preserve the source
-- observations for audit, but remove them from every derived statistic and
-- repair only the compact talent projections touched by them.

CREATE TEMP TABLE invalid_talent_facts ON COMMIT DROP AS
SELECT DISTINCT mpt.match_id, mpt.player_id, mpt.talent_id
FROM match_player_talents mpt
JOIN match_players mp
  ON mp.match_id = mpt.match_id
 AND mp.player_id = mpt.player_id
JOIN talents t ON t.talent_id = mpt.talent_id
WHERE t.champion_id IS NOT NULL
  AND t.champion_id <> mp.champion_id;

CREATE INDEX invalid_talent_facts_talent_idx
  ON invalid_talent_facts (talent_id);

CREATE TEMP TABLE affected_talents ON COMMIT DROP AS
SELECT DISTINCT talent_id FROM invalid_talent_facts;

DELETE FROM stats_talent_card_aggregate stca
USING talents t
WHERE t.talent_id = stca.talent_id
  AND t.champion_id IS NOT NULL
  AND t.champion_id <> stca.champion_id;

DELETE FROM stats_talent_aggregate sta
USING talents t
WHERE t.talent_id = sta.talent_id
  AND t.champion_id IS NOT NULL
  AND t.champion_id <> sta.champion_id;

DELETE FROM talent_counts_ranked tcr
USING affected_talents affected
WHERE affected.talent_id = tcr.talent_id;

INSERT INTO talent_counts_ranked (
  talent_id, champion_name, talent_name, count, wins, losses, winrate, updated_at
)
SELECT
  mpt.talent_id,
  c.name,
  t.talent_name,
  COUNT(*)::INT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::INT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::INT,
  COALESCE(ROUND(
    100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::NUMERIC
      / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0),
    2
  ), 0),
  now()
FROM match_player_talents mpt
JOIN affected_talents affected ON affected.talent_id = mpt.talent_id
JOIN match_players mp
  ON mp.match_id = mpt.match_id
 AND mp.player_id = mpt.player_id
JOIN matches m ON m.match_id = mpt.match_id
JOIN talents t
  ON t.talent_id = mpt.talent_id
 AND t.champion_id = mp.champion_id
JOIN champions c ON c.id = t.champion_id
WHERE m.queue_id = 486
GROUP BY mpt.talent_id, c.name, t.talent_name;

DELETE FROM talent_card_counts_ranked tcc
USING affected_talents affected
WHERE affected.talent_id = tcc.talent_id;

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
JOIN affected_talents affected ON affected.talent_id = mpt.talent_id
JOIN match_player_cards mpc
  ON mpc.match_id = mpt.match_id
 AND mpc.player_id = mpt.player_id
JOIN match_players mp
  ON mp.match_id = mpt.match_id
 AND mp.player_id = mpt.player_id
JOIN matches m ON m.match_id = mp.match_id
JOIN talents t
  ON t.talent_id = mpt.talent_id
 AND t.champion_id = mp.champion_id
WHERE m.queue_id = 486
GROUP BY mpt.talent_id, mpc.card_id, COALESCE(mpc.card_level, 0);
