-- paladinscat:requires-full-backup
-- Predicate-bounded cleanup: only canonicalize known provider aliases.
WITH aliases(alias, canonical) AS (VALUES
  ('north america','NA'),('na','NA'),('europe','EU'),('eu','EU'),
  ('brazil','BR'),('br','BR'),('south america','SA'),('sa','SA'),
  ('southeast asia','SEA'),('sea','SEA'),('australia','OCE'),
  ('oceania','OCE'),('oce','OCE'),('japan','JPN'),('jpn','JPN'),
  ('russia','RUS'),('rus','RUS'),('asia','ASIA')
)
UPDATE players AS player
SET region = aliases.canonical,
    last_updated = now()
FROM aliases
WHERE LOWER(BTRIM(player.region)) = aliases.alias
  AND player.region IS DISTINCT FROM aliases.canonical;
