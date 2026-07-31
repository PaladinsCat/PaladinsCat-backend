-- Private-account identity v3: dynamic TP evidence and durable match links.
--
-- TP is a per-match rank observation, not a stable identity field. Persist the
-- match outcome needed to judge its direction, repair historical private-slot
-- ordinals, and restore links from match_players to the resolved identity.

ALTER TABLE private_account_observations
  ADD COLUMN IF NOT EXISTS win_status VARCHAR(20);

UPDATE private_account_observations o
SET win_status = mp.win_status,
    updated_at = now()
FROM match_players mp
WHERE mp.match_id = o.match_id
  AND mp.player_id = 0
  AND upper(COALESCE(mp.player_name, '')) = 'PRIVATEACCOUNT'
  AND (mp.private_slot = o.private_slot OR (mp.private_slot = 0 AND o.private_slot = 1))
  AND o.win_status IS DISTINCT FROM mp.win_status;

-- Migration 058 derived slot 1 for legacy player_id=0 rows when the source row
-- still carried slot 0, but it did not write that ordinal back. Repair the
-- source row so all future links and API reads use the same immutable key.
UPDATE match_players mp
SET private_slot = o.private_slot,
    private_player_id = o.private_player_id
FROM private_account_observations o
WHERE mp.match_id = o.match_id
  AND mp.player_id = 0
  AND upper(COALESCE(mp.player_name, '')) = 'PRIVATEACCOUNT'
  AND mp.private_slot = 0
  AND o.private_slot = 1
  AND NOT EXISTS (
    SELECT 1
    FROM match_players existing
    WHERE existing.match_id = mp.match_id
      AND existing.player_id = 0
      AND existing.private_slot = o.private_slot
      AND existing.entry_datetime = mp.entry_datetime
  );

UPDATE match_players mp
SET private_player_id = o.private_player_id
FROM private_account_observations o
WHERE mp.match_id = o.match_id
  AND mp.player_id = 0
  AND mp.private_slot = o.private_slot
  AND o.private_player_id IS NOT NULL
  AND mp.private_player_id IS DISTINCT FROM o.private_player_id;

CREATE INDEX IF NOT EXISTS idx_players_private_v3_active
  ON players_private (last_seen DESC, id DESC)
  WHERE tracking_version = 3 AND is_active;

COMMENT ON COLUMN private_account_observations.league_points IS
  'Observed TP at this match. Dynamic rank evidence; never a fixed identity key.';
COMMENT ON COLUMN private_account_observations.win_status IS
  'Observed match result used to evaluate the direction of the following TP observation.';
