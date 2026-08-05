CREATE TABLE players (
  id BIGINT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  level INT DEFAULT 0,
  region VARCHAR(50),
  platform VARCHAR(20),
  kbm_tier INT DEFAULT 0,
  kbm_points INT DEFAULT 0,
  cheater BOOLEAN NOT NULL DEFAULT FALSE,
  sus_count INT NOT NULL DEFAULT 0
);

CREATE TABLE player_name_history (
  id SERIAL PRIMARY KEY,
  player_id BIGINT NOT NULL REFERENCES players(id),
  name VARCHAR(100) NOT NULL,
  changed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE player_account_merges (
  id SERIAL PRIMARY KEY,
  player_id BIGINT NOT NULL REFERENCES players(id),
  merged_into_player_id BIGINT NOT NULL,
  merged_at TIMESTAMPTZ NOT NULL,
  source VARCHAR(30)
);

CREATE TABLE player_status (
  player_id INT PRIMARY KEY,
  status INT,
  status_string VARCHAR(100),
  current_match_id BIGINT,
  queue_id INT,
  privacy_flag BOOLEAN DEFAULT FALSE,
  personal_status_message TEXT,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE player_achievements (
  achievement_id INT NOT NULL,
  player_id INT NOT NULL,
  achievement_name VARCHAR(100) NOT NULL,
  progress INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (player_id, achievement_id)
);

CREATE TABLE players_private (
  id SERIAL PRIMARY KEY,
  party_id INTEGER NOT NULL DEFAULT 0,
  account_level INTEGER NOT NULL DEFAULT 0,
  mastery_level INTEGER NOT NULL DEFAULT 0,
  league_tier INTEGER NOT NULL DEFAULT 0,
  league_points INTEGER NOT NULL DEFAULT 0,
  first_seen TIMESTAMPTZ NOT NULL,
  last_seen TIMESTAMPTZ NOT NULL,
  match_count INTEGER NOT NULL DEFAULT 0,
  alias VARCHAR(50),
  verified_name VARCHAR(100),
  identity_status VARCHAR(20) NOT NULL DEFAULT 'inferred',
  identity_confidence SMALLINT NOT NULL DEFAULT 0,
  tracking_version SMALLINT NOT NULL DEFAULT 2,
  cheater BOOLEAN NOT NULL DEFAULT FALSE,
  cheater_reason TEXT,
  cheater_marked_at TIMESTAMPTZ,
  sus_count INTEGER NOT NULL DEFAULT 0,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  merged_into_id INTEGER REFERENCES players_private(id),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  username VARCHAR(50) NOT NULL,
  email VARCHAR(255) NOT NULL,
  password_hash VARCHAR(255) NOT NULL,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  is_admin BOOLEAN NOT NULL DEFAULT FALSE,
  is_approved BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE sessions (
  id SERIAL PRIMARY KEY,
  user_id INT NOT NULL REFERENCES users(id),
  token VARCHAR NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE private_account_community_votes (
  id BIGSERIAL PRIMARY KEY,
  private_player_id INTEGER NOT NULL REFERENCES players_private(id),
  user_id INTEGER NOT NULL REFERENCES users(id),
  vote_type VARCHAR(20) NOT NULL CHECK (vote_type IN ('suspicious', 'cheater')),
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_private_account_community_vote
    UNIQUE (private_player_id, user_id, vote_type)
);

CREATE TABLE champions (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL
);

CREATE TABLE matches (
  match_id BIGINT PRIMARY KEY,
  map VARCHAR(200),
  queue_id INT,
  region VARCHAR(50),
  duration_seconds INT
);

CREATE TABLE private_account_observations (
  match_id BIGINT NOT NULL,
  private_slot SMALLINT NOT NULL,
  entry_datetime TIMESTAMPTZ NOT NULL,
  party_id INTEGER NOT NULL DEFAULT 0,
  account_level INTEGER NOT NULL DEFAULT 0,
  mastery_level INTEGER NOT NULL DEFAULT 0,
  league_tier INTEGER NOT NULL DEFAULT 0,
  league_points INTEGER NOT NULL DEFAULT 0,
  win_status VARCHAR(20),
  champion_id INTEGER,
  task_force SMALLINT,
  platform VARCHAR(20),
  source VARCHAR(20) NOT NULL DEFAULT 'direct',
  private_player_id INTEGER REFERENCES players_private(id),
  resolution_status VARCHAR(24) NOT NULL DEFAULT 'unresolved',
  resolution_confidence SMALLINT NOT NULL DEFAULT 0,
  resolution_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  PRIMARY KEY (match_id, private_slot)
);

CREATE TABLE players_private_history (
  id SERIAL PRIMARY KEY,
  player_private_id INTEGER NOT NULL REFERENCES players_private(id),
  party_id INTEGER NOT NULL,
  account_level INTEGER NOT NULL,
  mastery_level INTEGER NOT NULL,
  league_tier INTEGER NOT NULL,
  league_points INTEGER NOT NULL,
  match_id BIGINT,
  private_slot SMALLINT NOT NULL DEFAULT 0,
  resolution_confidence SMALLINT,
  resolution_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  recorded_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

INSERT INTO players
  (id, name, level, region, platform, kbm_tier, kbm_points, cheater, sus_count)
VALUES
  (1001, 'AlphaCat', 999, 'North America', 'PC', 26, 2875, FALSE, 0),
  (1002, 'BetaCat', 420, 'Europe', 'Xbox', 20, 2400, TRUE, 3),
  (1003, 'GammaCat', 315, 'North America', 'PC', 15, 1900, FALSE, 1);

INSERT INTO player_name_history (player_id, name, changed_at)
VALUES
  (1001, 'AlphaCat', '2026-07-20T12:00:00Z'),
  (1001, 'OldAlpha', '2025-02-01T10:00:00Z');

INSERT INTO player_account_merges
  (player_id, merged_into_player_id, merged_at, source)
VALUES
  (1001, 1002, '2026-01-05T09:30:00Z', 'profile');

INSERT INTO player_status
  (player_id, status, status_string, current_match_id, queue_id, privacy_flag,
   personal_status_message, updated_at)
VALUES
  (1001, 3, 'In Match', 1281115795, 486, FALSE, 'Ranked grind',
   '2026-07-29T14:00:00Z');

INSERT INTO player_achievements
  (achievement_id, player_id, achievement_name, progress, updated_at)
VALUES
  (10, 1001, 'First Blood', 12, '2026-07-28T11:00:00Z'),
  (20, 1001, 'Champion Mastery', 46, '2026-07-28T11:00:00Z');

INSERT INTO players_private
  (id, party_id, account_level, mastery_level, league_tier, league_points,
   first_seen, last_seen, match_count, alias, verified_name, identity_status,
   identity_confidence, tracking_version, cheater, cheater_reason,
   cheater_marked_at, sus_count, is_active, merged_into_id, created_at, updated_at)
VALUES
  (1, 111, 200, 35, 20, 2325, '2026-06-01T10:00:00Z',
   '2026-06-10T10:00:00Z', 2, 'Legacy Shadow', NULL, 'merged', 80, 2,
   FALSE, NULL, NULL, 0, FALSE, 2, '2026-06-01T10:00:00Z',
   '2026-06-10T10:00:00Z'),
  (2, 222, 205, 38, 21, 2400, '2026-06-01T10:00:00Z',
   '2026-07-29T10:00:00Z', 5, 'ShadowCat', 'Verified Shadow',
   'verified', 98, 2, FALSE, NULL, NULL, 1, TRUE, NULL,
   '2026-06-01T10:00:00Z', '2026-07-29T10:00:00Z'),
  (3, 333, 120, 17, 12, 1600, '2026-07-01T10:00:00Z',
   '2026-07-25T10:00:00Z', 3, 'QuietShadow', NULL, 'inferred', 65, 2,
   FALSE, NULL, NULL, 0, TRUE, NULL, '2026-07-01T10:00:00Z',
   '2026-07-25T10:00:00Z');

SELECT setval('players_private_id_seq', 3, TRUE);

INSERT INTO users
  (id, username, email, password_hash, is_active, is_admin, is_approved)
VALUES
  (1, 'approved', 'approved@example.invalid', 'fixture', TRUE, FALSE, TRUE),
  (2, 'regular', 'regular@example.invalid', 'fixture', TRUE, FALSE, FALSE),
  (3, 'seed-voter', 'seed@example.invalid', 'fixture', TRUE, FALSE, FALSE);

SELECT setval('users_id_seq', 3, TRUE);

INSERT INTO sessions (user_id, token, expires_at)
VALUES
  (1, '4fe8db7309258b59f8ee423da448ac795a87d8ef37997ff4bb6ce26204554a2d',
   '2099-01-01T00:00:00Z'),
  (2, '844610bdb4b027a0964b5d393de3d7bfa7f97e5a88e36ab0b3156bbfb21c2dd0',
   '2099-01-01T00:00:00Z');

INSERT INTO private_account_community_votes
  (private_player_id, user_id, vote_type, reason, created_at)
VALUES
  (2, 3, 'suspicious', 'Aim tracking', '2026-07-25T08:00:00Z');

INSERT INTO champions (id, name)
VALUES (2205, 'Androxus'), (2092, 'Fernando');

INSERT INTO matches (match_id, map, queue_id, region, duration_seconds)
VALUES
  (1281115794, 'Stone Keep', 486, 'North America', 900),
  (1281115795, 'Jaguar Falls', 486, 'North America', 780);

INSERT INTO private_account_observations
  (match_id, private_slot, entry_datetime, party_id, account_level,
   mastery_level, league_tier, league_points, win_status, champion_id,
   task_force, platform, source, private_player_id, resolution_status,
   resolution_confidence, resolution_reasons)
VALUES
  (1281115794, 1, '2026-07-28T10:00:00Z', 222, 204, 37, 21, 2388,
   'Win', 2092, 1, 'PC', 'history', 2, 'linked', 91,
   '["party_continuity"]'::jsonb),
  (1281115795, 1, '2026-07-29T10:00:00Z', 222, 205, 38, 21, 2400,
   'Loss', 2205, 2, 'PC', 'direct', 2, 'verified', 98,
   '["verified_name"]'::jsonb);

INSERT INTO players_private_history
  (player_private_id, party_id, account_level, mastery_level, league_tier,
   league_points, match_id, private_slot, resolution_confidence,
   resolution_reasons, recorded_at, created_at)
VALUES
  (2, 222, 204, 37, 21, 2388, 1281115794, 1, 91,
   '["party_continuity"]'::jsonb, '2026-07-28T10:00:00Z',
   '2026-07-28T10:00:00Z'),
  (2, 222, 205, 38, 21, 2400, 1281115795, 1, 98,
   '["verified_name"]'::jsonb, '2026-07-29T10:00:00Z',
   '2026-07-29T10:00:00Z');
