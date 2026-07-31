-- Support the bounded profile-batch filler query without sorting/scanning the
-- entire players read model. Never-refreshed profiles sort first, followed by
-- the oldest authoritative profile refresh.

CREATE INDEX IF NOT EXISTS idx_players_profile_backfill_priority
  ON players (hirez_profile_refreshed_at ASC NULLS FIRST, last_seen DESC);
