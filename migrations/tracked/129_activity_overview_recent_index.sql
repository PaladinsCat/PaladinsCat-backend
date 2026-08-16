-- paladinscat:transaction=off
-- Recovery: DROP INDEX CONCURRENTLY IF EXISTS idx_matches_queue_recent;

-- The activity overview reads the newest ranked matches after its hourly
-- aggregate. queue_id alone still requires sorting the full ranked population,
-- which can exceed the public API statement timeout on a cold cache.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_matches_queue_recent
  ON matches (queue_id, entry_datetime DESC, match_id DESC);
