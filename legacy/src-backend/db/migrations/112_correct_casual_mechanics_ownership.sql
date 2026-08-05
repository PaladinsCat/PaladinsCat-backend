-- paladinscat:requires-full-backup
--
-- Correct migration 109 before its empty duplicate-fact path is activated.
--
-- Per-match item/card/talent evidence is canonical and shared in
-- match_player_items, match_player_cards, and match_player_talents. Ranked and
-- casual statistics are separate projections. Casual projections retain
-- stats_scope and queue_id, but never ranked-only lobby-tier dimensions.

DO $$
DECLARE
  table_name TEXT;
  row_count BIGINT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'nonranked_match_items',
    'nonranked_match_cards',
    'nonranked_match_talents',
    'casual_item_stats_daily',
    'casual_card_stats_daily',
    'casual_talent_stats_daily',
    'casual_composition_stats_daily',
    'item_counts_casual',
    'card_counts_casual',
    'talent_counts_casual'
  ]
  LOOP
    IF to_regclass('public.' || table_name) IS NOT NULL THEN
      EXECUTE format('SELECT count(*) FROM %I', table_name) INTO row_count;
      IF row_count <> 0 THEN
        RAISE EXCEPTION
          'migration 112 refuses to replace non-empty table % (% rows)',
          table_name,
          row_count;
      END IF;
    END IF;
  END LOOP;
END
$$;

-- These tables duplicate canonical per-match facts and must never be
-- backfilled or read by a worker.
DROP TABLE IF EXISTS nonranked_match_cards;
DROP TABLE IF EXISTS nonranked_match_talents;
DROP TABLE IF EXISTS nonranked_match_items;

-- Migration 109's daily projections introduced a second casual aggregate
-- family and a ranked-only lobby_tier dimension. They are empty and are
-- replaced by the existing canonical casual aggregate family below.
DROP TABLE IF EXISTS casual_composition_stats_daily;
DROP TABLE IF EXISTS casual_card_stats_daily;
DROP TABLE IF EXISTS casual_talent_stats_daily;
DROP TABLE IF EXISTS casual_item_stats_daily;

ALTER TABLE casual_matches DROP COLUMN IF EXISTS lobby_tier;
ALTER TABLE special_matches DROP COLUMN IF EXISTS lobby_tier;

DROP TABLE IF EXISTS item_counts_casual;
CREATE TABLE item_counts_casual (
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  item_id INT NOT NULL REFERENCES items(item_id),
  item_name TEXT,
  slot SMALLINT NOT NULL CHECK (slot BETWEEN 1 AND 4),
  item_level SMALLINT NOT NULL DEFAULT 0 CHECK (item_level BETWEEN 0 AND 3),
  count BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  winrate NUMERIC(5,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (stats_scope, queue_id, item_id, slot, item_level)
);
CREATE INDEX idx_item_counts_casual_item
  ON item_counts_casual (item_id, stats_scope, queue_id);
COMMENT ON TABLE item_counts_casual IS
  'Casual-mechanics item statistics projected from shared match_player_items facts. Includes every valid non-ranked display scope; never contains ranked rows or lobby tiers.';

DROP TABLE IF EXISTS talent_counts_casual;
CREATE TABLE talent_counts_casual (
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  talent_id INT NOT NULL REFERENCES talents(talent_id),
  champion_name TEXT,
  talent_name TEXT,
  count BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  winrate NUMERIC(5,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (stats_scope, queue_id, talent_id)
);
CREATE INDEX idx_talent_counts_casual_talent
  ON talent_counts_casual (talent_id, stats_scope, queue_id);
COMMENT ON TABLE talent_counts_casual IS
  'Casual-mechanics talent statistics projected from shared match_player_talents facts. Includes every valid non-ranked display scope.';

DROP TABLE IF EXISTS card_counts_casual;
CREATE TABLE card_counts_casual (
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  card_id INT NOT NULL,
  champion_name TEXT,
  card_name TEXT,
  card_level SMALLINT NOT NULL DEFAULT 0 CHECK (card_level BETWEEN 0 AND 5),
  count BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  winrate NUMERIC(5,2) NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (stats_scope, queue_id, card_id, card_level)
);
CREATE INDEX idx_card_counts_casual_card
  ON card_counts_casual (card_id, stats_scope, queue_id);
COMMENT ON TABLE card_counts_casual IS
  'Casual-mechanics card statistics projected from shared match_player_cards facts. Includes every valid non-ranked display scope.';
