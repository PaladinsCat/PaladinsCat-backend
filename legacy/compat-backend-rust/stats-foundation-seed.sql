CREATE TABLE matches (
    match_id BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    queue_id INT NOT NULL,
    limited BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (match_id, entry_datetime)
);

CREATE TABLE match_players (
    match_id BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    player_id BIGINT NOT NULL,
    league_tier INT NOT NULL DEFAULT 0,
    win_status TEXT,
    PRIMARY KEY (match_id, entry_datetime, player_id)
);

CREATE TABLE leaderboard_current (
    player_id BIGINT PRIMARY KEY,
    name VARCHAR(100),
    tier INT NOT NULL,
    points INT NOT NULL DEFAULT 0,
    rank INT NOT NULL DEFAULT 1000,
    prev_rank INT,
    prev_tier INT,
    trend INT NOT NULL DEFAULT 0,
    tier_change INT DEFAULT 0,
    wins INT NOT NULL DEFAULT 0,
    losses INT NOT NULL DEFAULT 0,
    leaves INT NOT NULL DEFAULT 0,
    season INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    winrate DOUBLE PRECISION DEFAULT 0.00,
    leaverate DOUBLE PRECISION DEFAULT 0.00
);

CREATE TABLE leaderboard_update_log (
    id SERIAL PRIMARY KEY,
    updated_at TIMESTAMPTZ NOT NULL,
    season INT NOT NULL,
    round INT NOT NULL,
    queue_id INT NOT NULL DEFAULT 486,
    tiers_updated INT[] NOT NULL,
    total_players INT NOT NULL DEFAULT 0,
    trigger_type VARCHAR NOT NULL DEFAULT 'manual',
    dev_id VARCHAR,
    next_auto TIMESTAMPTZ NOT NULL,
    status VARCHAR NOT NULL DEFAULT 'completed'
);

CREATE TABLE tier_population_stats (
    tier INT PRIMARY KEY,
    tier_name TEXT NOT NULL,
    player_count BIGINT NOT NULL
);

CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    hz_player_name VARCHAR(100),
    hz_gamer_tag VARCHAR(100),
    cheater BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE player_champion_ratings (
    player_id BIGINT NOT NULL,
    champion_id INT NOT NULL,
    mu NUMERIC NOT NULL,
    phi NUMERIC NOT NULL,
    matches_played INT NOT NULL DEFAULT 0,
    wins INT NOT NULL DEFAULT 0,
    losses INT NOT NULL DEFAULT 0,
    PRIMARY KEY (player_id, champion_id)
);

INSERT INTO matches (match_id, entry_datetime, queue_id, limited)
VALUES
    (1001, '2026-03-01T00:00:00Z', 486, false),
    (1002, '2026-03-01T01:00:00Z', 486, false),
    (1003, '2026-03-01T02:00:00Z', 486, false),
    (1004, '2026-03-01T03:00:00Z', 486, true),
    (1005, '2026-03-01T04:00:00Z', 424, false),
    (1006, '2026-03-01T05:00:00Z', 486, false);

INSERT INTO match_players
    (match_id, entry_datetime, player_id, league_tier, win_status)
VALUES
    (1001, '2026-03-01T00:00:00Z', 1, 21, 'Winner'),
    (1001, '2026-03-01T00:00:00Z', 2, 21, 'Loser'),
    (1001, '2026-03-01T00:00:00Z', 3, 21, NULL),
    (1002, '2026-03-01T01:00:00Z', 1, 21, 'Win'),
    (1002, '2026-03-01T01:00:00Z', 4, 21, 'Loss'),
    (1003, '2026-03-01T02:00:00Z', 5, 22, 'Winner'),
    (1003, '2026-03-01T02:00:00Z', 6, 22, 'WINNER'),
    (1003, '2026-03-01T02:00:00Z', 7, 22, 'loser'),
    (1004, '2026-03-01T03:00:00Z', 8, 23, 'Winner'),
    (1005, '2026-03-01T04:00:00Z', 9, 24, 'Winner'),
    (1006, '2026-03-01T05:00:00Z', 10, 0, 'Winner');

INSERT INTO leaderboard_current
    (player_id, name, tier, points, rank, prev_rank, prev_tier, trend,
     tier_change, wins, losses, leaves, season, updated_at, winrate, leaverate)
VALUES
    (101, 'Alpha', 21, 1900, 1, 3, 21, 999, 0, 20, 10, 1, 8,
     '2026-03-03T00:00:00Z', 66.67, 3.33),
    (102, 'Bravo', 21, 1800, 2, NULL, 20, -99, 1, 15, 15, 0, 8,
     '2026-03-02T00:00:00Z', 50.00, 0.00),
    (103, 'Charlie', 21, 1700, 4, 2, 21, 99, 0, 9, 11, 2, 8,
     '2026-03-01T00:00:00Z', 45.00, 10.00),
    (201, 'Delta', 22, 2100, 1, 1, 22, 0, 0, 30, 5, 0, 8,
     '2026-03-04T00:00:00Z', 85.71, 0.00);

INSERT INTO leaderboard_update_log
    (id, updated_at, season, round, queue_id, tiers_updated, total_players,
     trigger_type, dev_id, next_auto, status)
VALUES
    (1, '2026-03-01T00:00:00Z', 8, 1, 486, ARRAY[21,22], 100,
     'manual', 'dev-one', '2026-03-02T00:00:00Z', 'completed'),
    (2, '2026-03-03T00:00:00Z', 8, 2, 486, ARRAY[21,22,23], 150,
     'automatic', NULL, '2026-03-04T00:00:00Z', 'completed'),
    (3, '2026-03-02T00:00:00Z', 8, 3, 486, ARRAY[24,25,26], 75,
     'manual', 'dev-two', '2026-03-03T00:00:00Z', 'failed');

SELECT setval('leaderboard_update_log_id_seq', 3, true);

INSERT INTO tier_population_stats (tier, tier_name, player_count)
VALUES
    (21, 'Diamond V', 50),
    (22, 'Diamond IV', 30),
    (26, 'Master', 20);

INSERT INTO players
    (id, name, hz_player_name, hz_gamer_tag, cheater)
VALUES
    (1, 'StoredAlpha', 'PublicAlpha', 'TagAlpha', false),
    (2, 'StoredBravo',
     'aaaaaaaaaaaaaaaaaaaaUser-bbbbbb', 'PublicBravo', false),
    (3, 'StoredCharlie', 'DummyPlayer123', 'DummyPlayer456', false),
    (4, 'DummyPlayer789',
     'ccccccccccccccccccccUser-dddddd',
     'eeeeeeeeeeeeeeeeeeeeUser-ffffff', false),
    (5, 'Cheater', 'ExcludedCheater', NULL, true),
    (6, '', '', '', false),
    (7, 'OtherChampion', 'OtherChampion', NULL, false);

INSERT INTO player_champion_ratings
    (player_id, champion_id, mu, phi, matches_played, wins, losses)
VALUES
    (1, 101, 1800.25, 50.50, 40, 30, 10),
    (2, 101, 1700.00, 75.25, 30, 18, 12),
    (3, 101, 1600.75, 100.00, 20, 11, 9),
    (4, 101, 1500.00, 350.00, 10, 4, 6),
    (5, 101, 2500.00, 20.00, 100, 90, 10),
    (6, 101, 1400.00, 300.00, 5, 2, 3),
    (7, 202, 2200.00, 40.00, 50, 40, 10);
