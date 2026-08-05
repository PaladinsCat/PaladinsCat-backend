CREATE TABLE players (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  hz_player_name TEXT,
  hz_gamer_tag TEXT,
  region TEXT,
  platform TEXT,
  level INT DEFAULT 0,
  mastery_level INT DEFAULT 0,
  hours_played INT DEFAULT 0,
  total_xp BIGINT DEFAULT 0,
  wins INT DEFAULT 0,
  losses INT DEFAULT 0,
  kbm_tier INT,
  kbm_rank INT,
  kbm_points INT,
  kbm_wins INT,
  kbm_losses INT,
  total_matches INT DEFAULT 0,
  total_wins INT DEFAULT 0,
  total_losses INT DEFAULT 0,
  avg_dpm DOUBLE PRECISION,
  avg_hpm DOUBLE PRECISION,
  avg_mpm DOUBLE PRECISION
);

CREATE TABLE matches (
  match_id BIGINT PRIMARY KEY,
  entry_datetime TIMESTAMPTZ,
  map TEXT,
  queue_id INT,
  region TEXT,
  duration_seconds INT
);

CREATE TABLE match_players (
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL
);

CREATE TABLE champions (
  id INT PRIMARY KEY,
  name TEXT NOT NULL,
  roles TEXT
);

CREATE TABLE items (
  item_id INT PRIMARY KEY,
  item_name TEXT,
  item_type TEXT,
  champion_id INT
);

CREATE TABLE cards (
  card_id INT PRIMARY KEY,
  card_name TEXT,
  champion_id INT
);

CREATE TABLE talents (
  talent_id INT PRIMARY KEY,
  talent_name TEXT,
  champion_id INT
);

CREATE TABLE live_matches (
  match_id BIGINT PRIMARY KEY,
  queue_id INT,
  region TEXT,
  map TEXT,
  detected_at TIMESTAMPTZ NOT NULL,
  source_player_id BIGINT,
  status TEXT NOT NULL,
  ended_at TIMESTAMPTZ,
  dropped BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE live_match_players (
  id BIGSERIAL PRIMARY KEY,
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL,
  player_name TEXT,
  champion_id INT,
  champion_name TEXT,
  skin_id INT,
  skin_name TEXT,
  account_level INT,
  mastery_level INT,
  tier INT,
  tier_wins INT,
  tier_losses INT,
  task_force INT,
  platform INT
);

CREATE TABLE leaderboard_current (
  player_id BIGINT PRIMARY KEY,
  rank INT,
  wins INT,
  losses INT
);

CREATE TABLE player_queue_ratings (
  player_id BIGINT NOT NULL,
  queue_id INT NOT NULL,
  mu DOUBLE PRECISION,
  phi DOUBLE PRECISION,
  volatility DOUBLE PRECISION
);

CREATE TABLE player_champion_ratings (
  player_id BIGINT NOT NULL,
  champion_id INT NOT NULL,
  mu DOUBLE PRECISION,
  phi DOUBLE PRECISION,
  volatility DOUBLE PRECISION
);

CREATE TABLE drop_hack_suspects (
  id BIGSERIAL PRIMARY KEY,
  player_id BIGINT NOT NULL,
  player_name TEXT,
  match_id BIGINT NOT NULL,
  champion_id INT,
  champion_name TEXT,
  is_cassie BOOLEAN,
  dropped_at TIMESTAMPTZ,
  incident_count INT NOT NULL
);

CREATE TABLE hirez_raw_api_responses (
  id BIGSERIAL PRIMARY KEY,
  endpoint TEXT NOT NULL,
  operation TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_response JSONB,
  raw_response_text TEXT,
  response_sha256 TEXT,
  response_shape TEXT,
  response_count INT,
  status_code INT,
  success BOOLEAN NOT NULL,
  error_message TEXT,
  source TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE raw_ingest_buffer (
  id BIGSERIAL PRIMARY KEY,
  endpoint TEXT NOT NULL,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_data JSONB,
  status_code INT,
  session_id TEXT,
  response_time_ms INT,
  error_message TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO champions (id, name, roles)
VALUES (2205, 'Androxus', 'Flanker');

INSERT INTO players (
  id, name, hz_player_name, region, platform, level, mastery_level,
  hours_played, total_xp, wins, losses, kbm_tier, kbm_rank, kbm_points,
  kbm_wins, kbm_losses, total_matches, total_wins, total_losses,
  avg_dpm, avg_hpm, avg_mpm
)
VALUES (
  716515038, 'NabiCook', 'NabiCook', 'NA', 'Steam', 321, 88,
  4400, 296480000, 1500, 1200, 20, 100, 82,
  50, 30, 2700, 1500, 1200, 5432.1, 800.2, 432.3
);

INSERT INTO matches (
  match_id, entry_datetime, map, queue_id, region, duration_seconds
)
VALUES (
  1281115795, '2026-07-30T12:00:00Z', 'Stone Keep', 486, 'NA', 900
);

INSERT INTO match_players (match_id, player_id)
VALUES (1281115795, 716515038);

INSERT INTO items (item_id, item_name, item_type, champion_id)
VALUES (11826, 'Androxus Item', 'Card Vendor Rank 1', 2205);

INSERT INTO cards (card_id, card_name, champion_id)
VALUES (11928, 'Androxus Card', 2205);

INSERT INTO talents (talent_id, talent_name, champion_id)
VALUES (20085, 'Androxus Talent', 2205);

INSERT INTO live_matches (
  match_id, queue_id, region, map, detected_at, source_player_id, status,
  ended_at, dropped
)
VALUES
  (1281115795, 486, 'NA', 'Stone Keep', '2026-07-30T12:05:00Z',
   716515038, 'active', NULL, FALSE),
  (1281115796, 424, 'EU', 'Brightmarsh', '2026-07-30T11:00:00Z',
   716515038, 'ended', '2026-07-30T11:20:00Z', FALSE);

INSERT INTO live_match_players (
  match_id, player_id, player_name, champion_id, champion_name, skin_id,
  skin_name, account_level, mastery_level, tier, tier_wins, tier_losses,
  task_force, platform
)
VALUES (
  1281115795, 716515038, 'NabiCook', 2205, 'Androxus', 1,
  'Default', 321, 88, 20, 50, 30, 1, 1
);

INSERT INTO leaderboard_current (player_id, rank, wins, losses)
VALUES (716515038, 100, 50, 30);

INSERT INTO player_queue_ratings (
  player_id, queue_id, mu, phi, volatility
)
VALUES (716515038, 486, 1825.5, 75.0, 0.06);

INSERT INTO player_champion_ratings (
  player_id, champion_id, mu, phi, volatility
)
VALUES (716515038, 2205, 1900.0, 80.0, 0.06);

INSERT INTO drop_hack_suspects (
  player_id, player_name, match_id, champion_id, champion_name, is_cassie,
  dropped_at, incident_count
)
VALUES (
  716515038, 'NabiCook', 1281115700, 2205, 'Androxus', FALSE,
  '2026-07-30T10:00:00Z', 2
);

INSERT INTO hirez_raw_api_responses (
  endpoint, operation, entity_type, entity_id, params, raw_response,
  raw_response_text, response_sha256, response_shape, response_count,
  status_code, success, source, created_at
)
VALUES (
  'getplayerbatch', 'getPlayerBatchLookup', 'search_player_id', '716515038',
  '{"playerIds":[716515038]}', '[{"Id":716515038}]',
  '[{"Id":716515038}]', 'fixture-sha', 'array', 1,
  200, TRUE, 'fixture', '2026-07-30T12:10:00Z'
);

INSERT INTO raw_ingest_buffer (
  endpoint, params, raw_data, status_code, session_id, response_time_ms,
  created_at
)
VALUES (
  'getmatchdetailsbatch', '{"matchIds":["1281115795"]}',
  '[{"Match":1281115795}]', 200, 'fixture-session', 42,
  '2026-07-30T12:11:00Z'
);
