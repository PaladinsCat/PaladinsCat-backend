-- Keep the all-time champion combat totals returned by getplayerchampions.
-- These are cached for ten minutes and power the Discord profile's global KDA.
ALTER TABLE player_champions
  ADD COLUMN IF NOT EXISTS wins INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS losses INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS kills INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS deaths INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS assists INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS minutes_played INT NOT NULL DEFAULT 0;
