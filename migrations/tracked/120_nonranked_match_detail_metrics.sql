-- Preserve the one raw field required for WPM/APM on nonranked match detail.
-- Existing history rows are reused when available; no provider refetch occurs.

ALTER TABLE casual_match_players
  ADD COLUMN IF NOT EXISTS damage_done_in_hand INT;

ALTER TABLE special_match_players
  ADD COLUMN IF NOT EXISTS damage_done_in_hand INT;

UPDATE casual_match_players fact
SET damage_done_in_hand = (history.raw_data->>'Damage_Done_In_Hand')::INT
FROM player_match_history_entries history
WHERE history.match_id = fact.match_id
  AND history.player_id = fact.player_id
  AND fact.player_id > 0
  AND fact.damage_done_in_hand IS NULL
  AND COALESCE(history.raw_data->>'Damage_Done_In_Hand', '') ~ '^[0-9]{1,10}$';

UPDATE special_match_players fact
SET damage_done_in_hand = (history.raw_data->>'Damage_Done_In_Hand')::INT
FROM player_match_history_entries history
WHERE history.match_id = fact.match_id
  AND history.player_id = fact.player_id
  AND fact.player_id > 0
  AND fact.damage_done_in_hand IS NULL
  AND COALESCE(history.raw_data->>'Damage_Done_In_Hand', '') ~ '^[0-9]{1,10}$';
