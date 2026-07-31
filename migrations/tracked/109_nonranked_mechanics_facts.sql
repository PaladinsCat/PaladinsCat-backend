-- Normalized non-ranked mechanics facts and physically isolated aggregates.
--
-- Ranked per-match and aggregate tables are deliberately untouched. Casual,
-- special, custom, training, and rotating queues share mechanics extraction
-- but retain stats_scope and queue_id as classification dimensions.

ALTER TABLE casual_matches
  ADD COLUMN IF NOT EXISTS lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (
    lobby_tier BETWEEN 0 AND 26
  );
ALTER TABLE special_matches
  ADD COLUMN IF NOT EXISTS lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (
    lobby_tier BETWEEN 0 AND 26
  );

CREATE TABLE IF NOT EXISTS nonranked_match_items (
  match_id BIGINT NOT NULL,
  population VARCHAR(16) NOT NULL CHECK (
    population IN ('casual', 'special')
  ),
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  roster_slot SMALLINT NOT NULL CHECK (roster_slot > 0),
  player_id BIGINT NOT NULL DEFAULT 0,
  slot SMALLINT NOT NULL CHECK (slot BETWEEN 1 AND 4),
  item_id INT NOT NULL REFERENCES items(item_id),
  item_level SMALLINT NOT NULL DEFAULT 0 CHECK (item_level BETWEEN 0 AND 3),
  PRIMARY KEY (match_id, roster_slot, item_id),
  FOREIGN KEY (match_id, population)
    REFERENCES match_ingest_status(match_id, population) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_items_item
  ON nonranked_match_items (stats_scope, queue_id, item_id, match_id);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_items_player
  ON nonranked_match_items (player_id, match_id)
  WHERE player_id > 0;

CREATE TABLE IF NOT EXISTS nonranked_match_talents (
  match_id BIGINT NOT NULL,
  population VARCHAR(16) NOT NULL CHECK (
    population IN ('casual', 'special')
  ),
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  roster_slot SMALLINT NOT NULL CHECK (roster_slot > 0),
  player_id BIGINT NOT NULL DEFAULT 0,
  champion_id INT NOT NULL,
  talent_id INT NOT NULL REFERENCES talents(talent_id),
  PRIMARY KEY (match_id, roster_slot, talent_id),
  FOREIGN KEY (match_id, population)
    REFERENCES match_ingest_status(match_id, population) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_talents_talent
  ON nonranked_match_talents (stats_scope, queue_id, talent_id, match_id);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_talents_player
  ON nonranked_match_talents (player_id, match_id)
  WHERE player_id > 0;

CREATE TABLE IF NOT EXISTS nonranked_match_cards (
  match_id BIGINT NOT NULL,
  population VARCHAR(16) NOT NULL CHECK (
    population IN ('casual', 'special')
  ),
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  roster_slot SMALLINT NOT NULL CHECK (roster_slot > 0),
  player_id BIGINT NOT NULL DEFAULT 0,
  champion_id INT NOT NULL,
  talent_id INT NOT NULL DEFAULT 0,
  card_id INT NOT NULL REFERENCES cards(card_id),
  card_level SMALLINT NOT NULL DEFAULT 0 CHECK (card_level BETWEEN 0 AND 5),
  PRIMARY KEY (match_id, roster_slot, card_id),
  FOREIGN KEY (match_id, population)
    REFERENCES match_ingest_status(match_id, population) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_cards_card
  ON nonranked_match_cards (stats_scope, queue_id, card_id, match_id);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_cards_talent
  ON nonranked_match_cards (stats_scope, queue_id, talent_id, card_id)
  WHERE talent_id > 0;
CREATE INDEX IF NOT EXISTS idx_nonranked_match_cards_player
  ON nonranked_match_cards (player_id, match_id)
  WHERE player_id > 0;

COMMENT ON TABLE nonranked_match_items IS
  'Normalized per-match item facts for every non-ranked mechanics scope. Never read by ranked projections.';
COMMENT ON TABLE nonranked_match_talents IS
  'Normalized per-match talent facts for every non-ranked mechanics scope. Never read by ranked projections.';
COMMENT ON TABLE nonranked_match_cards IS
  'Normalized per-match loadout-card facts for every non-ranked mechanics scope. Never read by ranked projections.';

CREATE TABLE IF NOT EXISTS casual_item_stats_daily (
  stats_date DATE NOT NULL,
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  item_id INT NOT NULL,
  slot SMALLINT NOT NULL,
  item_level SMALLINT NOT NULL DEFAULT 0,
  plays BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    stats_date, stats_scope, queue_id, lobby_tier, region, map,
    item_id, slot, item_level
  )
);
CREATE INDEX IF NOT EXISTS idx_casual_item_stats_lookup
  ON casual_item_stats_daily (
    stats_scope, lobby_tier, item_id, stats_date DESC
  );

CREATE TABLE IF NOT EXISTS casual_talent_stats_daily (
  stats_date DATE NOT NULL,
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  champion_id INT NOT NULL,
  talent_id INT NOT NULL,
  plays BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    stats_date, stats_scope, queue_id, lobby_tier, region, map,
    champion_id, talent_id
  )
);
CREATE INDEX IF NOT EXISTS idx_casual_talent_stats_lookup
  ON casual_talent_stats_daily (
    stats_scope, lobby_tier, champion_id, talent_id, stats_date DESC
  );

CREATE TABLE IF NOT EXISTS casual_card_stats_daily (
  stats_date DATE NOT NULL,
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  champion_id INT NOT NULL,
  talent_id INT NOT NULL DEFAULT 0,
  card_id INT NOT NULL,
  card_level SMALLINT NOT NULL DEFAULT 0,
  plays BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    stats_date, stats_scope, queue_id, lobby_tier, region, map,
    champion_id, talent_id, card_id, card_level
  )
);
CREATE INDEX IF NOT EXISTS idx_casual_card_stats_lookup
  ON casual_card_stats_daily (
    stats_scope, lobby_tier, champion_id, talent_id, card_id, stats_date DESC
  );

CREATE TABLE IF NOT EXISTS casual_composition_stats_daily (
  stats_date DATE NOT NULL,
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  frontline SMALLINT NOT NULL,
  damage SMALLINT NOT NULL,
  flank SMALLINT NOT NULL,
  support SMALLINT NOT NULL,
  plays BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    stats_date, stats_scope, queue_id, lobby_tier, region, map,
    frontline, damage, flank, support
  )
);
CREATE INDEX IF NOT EXISTS idx_casual_composition_stats_lookup
  ON casual_composition_stats_daily (
    stats_scope, lobby_tier, plays DESC, stats_date DESC
  );

ALTER TABLE nonranked_champion_stats_daily
  DROP CONSTRAINT IF EXISTS nonranked_champion_stats_scope_not_ranked;
ALTER TABLE nonranked_champion_stats_daily
  ADD CONSTRAINT nonranked_champion_stats_scope_not_ranked CHECK (
    stats_scope <> 'ranked'
  );

ALTER TABLE nonranked_map_stats_daily
  DROP CONSTRAINT IF EXISTS nonranked_map_stats_scope_not_ranked;
ALTER TABLE nonranked_map_stats_daily
  ADD CONSTRAINT nonranked_map_stats_scope_not_ranked CHECK (
    stats_scope <> 'ranked'
  );

COMMENT ON TABLE casual_item_stats_daily IS
  'Casual-mechanics item aggregates, physically isolated from item_counts_ranked. stats_scope keeps special/custom/training queryable without mixing display scopes.';
COMMENT ON TABLE casual_talent_stats_daily IS
  'Casual-mechanics talent aggregates, physically isolated from talent_counts_ranked.';
COMMENT ON TABLE casual_card_stats_daily IS
  'Casual-mechanics card/talent aggregates, physically isolated from ranked card tables.';
COMMENT ON TABLE casual_composition_stats_daily IS
  'Casual-mechanics team-composition aggregates, physically isolated from match_compositions_ranked.';

COMMENT ON TABLE item_counts_casual IS
  'Legacy unclassified casual item aggregate retained for TypeScript rollback only. New writes use casual_item_stats_daily.';
COMMENT ON TABLE talent_counts_casual IS
  'Legacy unclassified casual talent aggregate retained for TypeScript rollback only. New writes use casual_talent_stats_daily.';
COMMENT ON TABLE card_counts_casual IS
  'Legacy unclassified casual card aggregate retained for TypeScript rollback only. New writes use casual_card_stats_daily.';
