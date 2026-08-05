-- Skin statistics are derived from authoritative match_players rows. Keep the
-- source normalized (one row per player appearance) and add the query index
-- needed by /stats/skins rather than duplicating counters that can drift.
-- This includes broken/overflow skin IDs because PostgreSQL stores the raw
-- match value independently of the Hi-Rez catalogue lookup.
CREATE INDEX IF NOT EXISTS idx_match_players_skin_stats
  ON match_players (champion_id, skin_id, league_tier)
  WHERE skin_id IS NOT NULL AND skin_id > 0;
