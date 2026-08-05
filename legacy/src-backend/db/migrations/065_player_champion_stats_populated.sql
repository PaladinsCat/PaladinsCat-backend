-- getplayerchampions only exposes ownership/XP. Mark rows after the separate
-- getchampionranks refresh supplies real combat totals, including zero-stat
-- accounts, so legacy roster rows are refreshed once without API-call loops.
ALTER TABLE player_champions
  ADD COLUMN IF NOT EXISTS stats_populated BOOLEAN NOT NULL DEFAULT FALSE;
