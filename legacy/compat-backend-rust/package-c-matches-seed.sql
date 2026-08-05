CREATE TABLE matches (
  match_id BIGINT PRIMARY KEY,
  entry_datetime TIMESTAMPTZ NOT NULL,
  map TEXT,
  queue_id INT,
  duration_seconds INT,
  region TEXT,
  winning_task_force INT,
  broken BOOLEAN DEFAULT FALSE,
  recovered BOOLEAN DEFAULT FALSE
);

CREATE TABLE match_ingest_status (
  match_id BIGINT PRIMARY KEY,
  status TEXT,
  completed_stages TEXT[]
);

CREATE TABLE casual_matches (
  match_id BIGINT PRIMARY KEY,
  entry_datetime TIMESTAMPTZ NOT NULL,
  queue_id INT NOT NULL,
  duration_seconds INT,
  region TEXT,
  map TEXT,
  team1_score INT,
  team2_score INT,
  winning_task_force INT,
  source TEXT,
  ingested_at TIMESTAMPTZ,
  quality TEXT,
  stats_eligible BOOLEAN
);

CREATE TABLE special_matches (LIKE casual_matches INCLUDING ALL);

CREATE TABLE champions (
  id INT PRIMARY KEY,
  name TEXT NOT NULL
);

CREATE TABLE players (
  id BIGINT PRIMARY KEY,
  level INT,
  platform TEXT,
  region TEXT,
  wins INT,
  losses INT,
  kbm_tier INT,
  kbm_points INT,
  cheater BOOLEAN DEFAULT FALSE,
  sus_count INT DEFAULT 0
);

CREATE TABLE players_private (
  id BIGINT PRIMARY KEY,
  alias TEXT,
  verified_name TEXT,
  account_level INT,
  league_tier INT,
  league_points INT,
  cheater BOOLEAN DEFAULT FALSE,
  sus_count INT DEFAULT 0
);

CREATE TABLE users (
  id BIGSERIAL PRIMARY KEY,
  linked_player_id BIGINT
);

CREATE TABLE private_account_observations (
  match_id BIGINT,
  private_slot INT,
  private_player_id BIGINT
);

CREATE TABLE match_players (
  match_id BIGINT,
  entry_datetime TIMESTAMPTZ,
  player_id BIGINT,
  private_slot INT DEFAULT 0,
  private_player_id BIGINT,
  player_name TEXT,
  champion_id INT,
  task_force INT,
  account_level INT,
  platform TEXT,
  region TEXT,
  league_tier INT,
  league_points INT
);

CREATE TABLE casual_match_players (
  match_id BIGINT,
  roster_slot INT,
  private_slot INT DEFAULT 0,
  player_id BIGINT,
  private_player_id BIGINT,
  player_name TEXT,
  champion_id INT,
  champion_name TEXT,
  task_force INT,
  win_status TEXT,
  kills INT,
  deaths INT,
  assists INT,
  damage INT,
  damage_taken INT,
  healing INT,
  mitigation INT,
  credits INT,
  objective_time INT,
  account_level INT,
  mastery_level INT,
  party_id INT,
  portal_id INT,
  portal_user_id TEXT,
  platform TEXT,
  participant_kind TEXT,
  source TEXT,
  raw_player JSONB
);

CREATE TABLE special_match_players (LIKE casual_match_players INCLUDING ALL);

CREATE TABLE match_bans (
  match_id BIGINT,
  ban_slot INT,
  champion_id INT
);

CREATE TABLE items (
  item_id INT PRIMARY KEY,
  item_name TEXT,
  description TEXT,
  item_type TEXT,
  cost INT,
  icon_url TEXT,
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

CREATE TABLE match_player_items (
  match_id BIGINT,
  player_id BIGINT,
  item_id INT,
  slot INT,
  item_level INT
);

CREATE TABLE match_player_cards (
  match_id BIGINT,
  player_id BIGINT,
  card_id INT,
  card_level INT
);

CREATE TABLE match_player_talents (
  match_id BIGINT,
  player_id BIGINT,
  talent_id INT
);

CREATE TABLE drop_hack_suspects (
  player_id BIGINT PRIMARY KEY,
  incident_count INT,
  last_incident_at TIMESTAMPTZ
);

CREATE TABLE stats_ban_aggregate (
  queue_id INT,
  lobby_tier INT,
  map_name TEXT,
  champion_id INT,
  ban_slot INT,
  bans BIGINT
);

CREATE TABLE stats_match_aggregate (
  queue_id INT,
  lobby_tier INT,
  map_name TEXT,
  region TEXT,
  match_count BIGINT
);

CREATE TABLE stats_composition_aggregate (
  queue_id INT,
  lobby_tier INT,
  map_name TEXT,
  comp_id TEXT,
  frontline INT,
  damage INT,
  flank INT,
  support INT,
  uses BIGINT,
  wins BIGINT,
  losses BIGINT
);

INSERT INTO champions VALUES
  (2491, 'Furia'),
  (2288, 'Makoa'),
  (2277, 'Drogoz');

INSERT INTO players VALUES
  (101, 569, 'Epic Games', 'NA', 3793, 1825, 20, 40, FALSE, 0),
  (102, 1849, 'Steam', 'NA', 13911, 4590, 26, 80, FALSE, 1);

INSERT INTO casual_matches VALUES (
  1281115795, '2026-07-29T21:55:31Z', 452, 448, 'NA',
  'LIVE Snowfall Junction (Onslaught)', 4, 3, 1, 'direct',
  '2026-07-29T22:33:13Z', 'complete', TRUE
);

INSERT INTO casual_match_players VALUES
  (
    1281115795, 1, 0, 101, NULL, 'Alpha', 2491, 'Furia', 1, 'Winner',
    2, 0, 11, 5147, 3096, 70636, 12201, 3678, 107, 573, 36, 58559,
    28, NULL, 'Epic Games', 'human', 'direct',
    '{"active_id_1": 2001, "item_active_1": "Chronos", "active_level_1": 8,
      "item_id_1": 3001, "item_purch_1": "Burning Oath", "item_level_1": 5,
      "item_id_6": 4001, "item_purch_6": "Cherish"}'
  ),
  (
    1281115795, 2, 0, 102, NULL, 'Bravo', 2288, 'Makoa', 1, 'Winner',
    3, 1, 7, 26716, 34926, 0, 17535, 3392, 98, 999, 59, 58559,
    5, 'steam-102', 'Steam', 'human', 'direct', '{}'
  );

INSERT INTO matches VALUES
  (1281115800, '2026-07-30T12:00:00Z', 'Ranked Bazaar', 486, 900, 'NA', 1, FALSE, FALSE),
  (1281115801, '2026-07-30T13:00:00Z', 'Ranked Brightmarsh', 486, 800, 'EU', 2, FALSE, FALSE);

INSERT INTO match_players VALUES
  (1281115800, '2026-07-30T12:00:00Z', 101, 0, NULL, 'Alpha', 2491, 1, 573, 'Epic Games', 'NA', 20, 40),
  (1281115801, '2026-07-30T13:00:00Z', 102, 0, NULL, 'Bravo', 2288, 2, 999, 'Steam', 'EU', 26, 80);

INSERT INTO drop_hack_suspects VALUES
  (102, 3, '2026-07-30T13:00:00Z');

INSERT INTO stats_ban_aggregate VALUES
  (486, 20, 'Ranked Bazaar', 2491, 1, 4),
  (486, 20, 'Ranked Bazaar', 2288, 2, 2);

INSERT INTO stats_match_aggregate VALUES
  (486, 20, 'Ranked Bazaar', 'NA', 10);

INSERT INTO stats_composition_aggregate VALUES
  (486, 20, 'Ranked Bazaar', '2-1-1-1', 2, 1, 1, 1, 20, 12, 8);

ALTER TABLE match_players
  ADD COLUMN win_status TEXT DEFAULT '',
  ADD COLUMN kills INT DEFAULT 0,
  ADD COLUMN deaths INT DEFAULT 0,
  ADD COLUMN assists INT DEFAULT 0,
  ADD COLUMN source TEXT DEFAULT 'direct';

UPDATE match_players
SET win_status = CASE WHEN task_force = 1 THEN 'Winner' ELSE 'Loser' END,
    kills = CASE WHEN player_id = 101 THEN 8 ELSE 3 END,
    deaths = CASE WHEN player_id = 101 THEN 2 ELSE 6 END,
    assists = CASE WHEN player_id = 101 THEN 11 ELSE 5 END;

CREATE TABLE queue_types (
  queue_id INT PRIMARY KEY,
  queue_name TEXT NOT NULL,
  is_ranked BOOLEAN NOT NULL DEFAULT FALSE,
  stats_scope TEXT NOT NULL DEFAULT 'other',
  participant_model TEXT NOT NULL DEFAULT 'unknown',
  stats_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  track_presence BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO queue_types VALUES
  (424, 'Casual Siege', FALSE, 'casual', 'pvp', TRUE, TRUE),
  (452, 'Casual Onslaught', FALSE, 'casual', 'pvp', TRUE, TRUE),
  (486, 'Ranked Siege', TRUE, 'ranked', 'pvp', TRUE, TRUE);

CREATE TABLE hourly_match_counts (
  date DATE NOT NULL,
  hour INT NOT NULL,
  queue_id INT NOT NULL,
  matches_na INT NOT NULL DEFAULT 0,
  matches_eu INT NOT NULL DEFAULT 0,
  matches_asia INT NOT NULL DEFAULT 0,
  matches_sea INT NOT NULL DEFAULT 0,
  matches_jpn INT NOT NULL DEFAULT 0,
  matches_rus INT NOT NULL DEFAULT 0,
  matches_br INT NOT NULL DEFAULT 0,
  matches_oce INT NOT NULL DEFAULT 0,
  matches_sa INT NOT NULL DEFAULT 0,
  matches_unknown INT NOT NULL DEFAULT 0,
  total_matches INT NOT NULL DEFAULT 0,
  fetched_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (date, hour, queue_id)
);

INSERT INTO hourly_match_counts (
  date, hour, queue_id, matches_na, matches_eu, total_matches
)
SELECT
  (now() AT TIME ZONE 'UTC')::date,
  EXTRACT(HOUR FROM now() AT TIME ZONE 'UTC')::int,
  486, 7, 5, 12;

CREATE TABLE match_count_discovery_region_hours (
  date DATE NOT NULL,
  hour INT NOT NULL,
  queue_id INT NOT NULL,
  region TEXT NOT NULL,
  match_count INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (date, hour, queue_id, region)
);

INSERT INTO match_count_discovery_region_hours (
  date, hour, queue_id, region, match_count
)
SELECT
  (now() AT TIME ZONE 'UTC')::date,
  EXTRACT(HOUR FROM now() AT TIME ZONE 'UTC')::int,
  452, 'NA', 4;

CREATE TABLE dropped_matches (
  match_id BIGINT PRIMARY KEY,
  date DATE NOT NULL,
  hour INT NOT NULL,
  queue_id INT NOT NULL DEFAULT 486,
  status TEXT NOT NULL,
  drop_category TEXT NOT NULL,
  reason TEXT,
  attempts INT NOT NULL DEFAULT 0,
  observed_players INT NOT NULL DEFAULT 0,
  ingest_status TEXT,
  ingest_error TEXT,
  raw_buffer_status TEXT,
  raw_buffer_error TEXT,
  first_seen_at TIMESTAMPTZ,
  last_attempt_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  staged_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE hirez_raw_api_responses (
  id BIGSERIAL PRIMARY KEY,
  endpoint TEXT NOT NULL,
  operation TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT,
  params JSONB NOT NULL DEFAULT '{}'::jsonb,
  raw_response JSONB NOT NULL,
  raw_response_text TEXT NOT NULL,
  response_sha256 TEXT NOT NULL,
  response_shape TEXT NOT NULL,
  response_count INT,
  status_code INT NOT NULL,
  success BOOLEAN NOT NULL,
  error_message TEXT,
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE player_status (
  player_id BIGINT PRIMARY KEY,
  status INT NOT NULL,
  status_string TEXT,
  current_match_id BIGINT,
  queue_id INT,
  privacy_flag BOOLEAN NOT NULL DEFAULT FALSE,
  personal_status_message TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE live_matches (
  match_id BIGINT PRIMARY KEY,
  queue_id INT NOT NULL,
  region TEXT NOT NULL,
  map TEXT,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'active',
  dropped BOOLEAN NOT NULL DEFAULT FALSE,
  ingested BOOLEAN NOT NULL DEFAULT FALSE,
  source_player_id BIGINT
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
