-- Non-ranked private observations can have plausible-but-insufficient identity
-- evidence. Preserve that explicit state instead of failing the observation
-- transaction or minting a duplicate private identity.

ALTER TABLE private_account_observations
  DROP CONSTRAINT IF EXISTS private_account_observations_resolution_status_check;

ALTER TABLE private_account_observations
  ADD CONSTRAINT private_account_observations_resolution_status_check
  CHECK (
    resolution_status IN (
      'unresolved', 'ambiguous', 'minimal',
      'new_identity', 'linked', 'verified'
    )
  );

DROP INDEX IF EXISTS idx_private_observations_unresolved;
CREATE INDEX idx_private_observations_unresolved
  ON private_account_observations (entry_datetime, match_id, private_slot)
  WHERE private_player_id IS NULL
    AND resolution_status IN ('unresolved', 'ambiguous');
