-- Preserve every saved deck rather than one arbitrary deck per champion.
ALTER TABLE player_loadouts ADD COLUMN IF NOT EXISTS deck_id BIGINT;
ALTER TABLE player_loadouts ADD COLUMN IF NOT EXISTS deck_key TEXT;

-- Legacy rows did not retain the vendor deck id. Their existing row id gives
-- them a stable cache key until their player is refreshed from Hi-Rez.
UPDATE player_loadouts
SET deck_key = 'legacy:' || player_id::TEXT || ':' || id::TEXT
WHERE deck_key IS NULL OR deck_key = '';

ALTER TABLE player_loadouts ALTER COLUMN deck_key SET NOT NULL;
ALTER TABLE player_loadouts DROP CONSTRAINT IF EXISTS player_loadouts_player_id_champion_id_key;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'uq_player_loadouts_deck'
      AND conrelid = 'player_loadouts'::regclass
  ) THEN
    ALTER TABLE player_loadouts
      ADD CONSTRAINT uq_player_loadouts_deck UNIQUE (player_id, deck_key);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_pl_player_champion ON player_loadouts (player_id, champion_id);

CREATE TABLE IF NOT EXISTS player_loadout_fetches (
  player_id BIGINT PRIMARY KEY,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_manual_refresh_at TIMESTAMPTZ
);
