-- Package B uses the production-equivalent Timescale bootstrap supplied by
-- Test-PaladinsCatRouteCompatibility.ps1. Docker runs this through psql, so
-- include the read-only mounted source files before route-specific rows.
\set ON_ERROR_STOP on
\i /bootstrap/001_schema.sql
\i /bootstrap/002_extended_schema.sql
\i /bootstrap/003_data_migrations.sql
\i /bootstrap/004_seed_data.sql
\i /bootstrap/038_clean_changelog.sql

-- One real champion/item/talent path makes the detail fixtures exercise their
-- normal payloads instead of matching two unrelated not-found serializers.
INSERT INTO champions (id, name, title, health, speed, roles)
VALUES (2205, 'Androxus', 'The Godslayer', 2100, 370, 'Flank')
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name, title = EXCLUDED.title, health = EXCLUDED.health,
  speed = EXCLUDED.speed, roles = EXCLUDED.roles;

INSERT INTO talents (talent_id, talent_name, champion_id)
VALUES (20085, 'Cursed Revolver', 2205)
ON CONFLICT (talent_id) DO UPDATE SET
  talent_name = EXCLUDED.talent_name, champion_id = EXCLUDED.champion_id;

INSERT INTO items (item_id, item_name, description, item_type, cost)
VALUES (11826, 'Quick Draw', 'Package B compatibility fixture', 'Card', 0)
ON CONFLICT (item_id) DO UPDATE SET item_name = EXCLUDED.item_name;

INSERT INTO item_counts_ranked (item_id, item_name, slot, item_level, count, wins, losses, winrate)
VALUES (11826, 'Quick Draw', 1, 1, 24, 15, 9, 62.50)
ON CONFLICT (item_id, slot, item_level) DO UPDATE SET
  count = EXCLUDED.count, wins = EXCLUDED.wins, losses = EXCLUDED.losses,
  winrate = EXCLUDED.winrate;

-- The presence fixtures assert pagination against a 24-hour, presence-tracked
-- discovery window. They deliberately do not require player or match facts.
INSERT INTO match_count_discoveries
  (match_id, queue_id, region, entry_datetime, source_date, source_hour)
SELECT 991000000 + value, 486, 'NA', now(),
  (now() AT TIME ZONE 'UTC')::date,
  EXTRACT(HOUR FROM now() AT TIME ZONE 'UTC')::int
FROM generate_series(1, 50) AS value
ON CONFLICT (match_id, queue_id) DO NOTHING;
