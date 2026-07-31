-- Preserve and repair non-ranked objective time from the normalized raw player
-- fact already retained in PostgreSQL. No Hi-Rez requests are required.
--
-- The audit table is intentionally narrow: it provides an exact rollback path
-- without copying the wide raw_player JSONB value for every corrected row.

CREATE TABLE IF NOT EXISTS nonranked_objective_time_backfill_audit (
  scope TEXT NOT NULL CHECK (scope IN ('casual', 'special')),
  match_id BIGINT NOT NULL,
  roster_slot SMALLINT NOT NULL,
  old_objective_time INT NOT NULL,
  new_objective_time INT NOT NULL CHECK (new_objective_time >= 0),
  backfilled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  migration_version TEXT NOT NULL DEFAULT '107',
  PRIMARY KEY (scope, match_id, roster_slot)
);

WITH candidates AS (
  SELECT
    match_id,
    roster_slot,
    objective_time AS old_objective_time,
    (raw_player->>'objective_assists')::INT AS new_objective_time
  FROM casual_match_players
  WHERE raw_player ? 'objective_assists'
    AND COALESCE(raw_player->>'objective_assists', '') ~ '^[0-9]{1,9}$'
)
INSERT INTO nonranked_objective_time_backfill_audit (
  scope,
  match_id,
  roster_slot,
  old_objective_time,
  new_objective_time
)
SELECT
  'casual',
  match_id,
  roster_slot,
  old_objective_time,
  new_objective_time
FROM candidates
WHERE old_objective_time IS DISTINCT FROM new_objective_time
ON CONFLICT (scope, match_id, roster_slot) DO NOTHING;

WITH candidates AS (
  SELECT
    match_id,
    roster_slot,
    objective_time AS old_objective_time,
    (raw_player->>'objective_assists')::INT AS new_objective_time
  FROM special_match_players
  WHERE raw_player ? 'objective_assists'
    AND COALESCE(raw_player->>'objective_assists', '') ~ '^[0-9]{1,9}$'
)
INSERT INTO nonranked_objective_time_backfill_audit (
  scope,
  match_id,
  roster_slot,
  old_objective_time,
  new_objective_time
)
SELECT
  'special',
  match_id,
  roster_slot,
  old_objective_time,
  new_objective_time
FROM candidates
WHERE old_objective_time IS DISTINCT FROM new_objective_time
ON CONFLICT (scope, match_id, roster_slot) DO NOTHING;

UPDATE casual_match_players fact
SET objective_time = audit.new_objective_time
FROM nonranked_objective_time_backfill_audit audit
WHERE audit.scope = 'casual'
  AND audit.match_id = fact.match_id
  AND audit.roster_slot = fact.roster_slot
  AND fact.objective_time IS DISTINCT FROM audit.new_objective_time;

UPDATE special_match_players fact
SET objective_time = audit.new_objective_time
FROM nonranked_objective_time_backfill_audit audit
WHERE audit.scope = 'special'
  AND audit.match_id = fact.match_id
  AND audit.roster_slot = fact.roster_slot
  AND fact.objective_time IS DISTINCT FROM audit.new_objective_time;
