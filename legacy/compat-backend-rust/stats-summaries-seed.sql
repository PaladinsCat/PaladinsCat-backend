CREATE TABLE matches (
    match_id BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    queue_id INT NOT NULL,
    duration_seconds INT,
    limited BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (match_id, entry_datetime)
);

CREATE TABLE match_lobby_tiers (
    match_id BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    lobby_tier SMALLINT NOT NULL,
    PRIMARY KEY (match_id, entry_datetime)
);

CREATE TABLE match_players (
    match_id BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    player_id BIGINT NOT NULL,
    league_tier INT,
    win_status TEXT,
    PRIMARY KEY (match_id, entry_datetime, player_id)
);

CREATE TABLE stats_match_aggregate (
    queue_id INT NOT NULL,
    lobby_tier SMALLINT NOT NULL,
    stat_date DATE NOT NULL,
    region TEXT NOT NULL,
    map_name TEXT NOT NULL,
    match_count BIGINT NOT NULL,
    duration_sum BIGINT NOT NULL,
    PRIMARY KEY (queue_id, lobby_tier, stat_date, region, map_name)
);

CREATE TABLE stats_player_aggregate (
    queue_id INT NOT NULL,
    lobby_tier SMALLINT NOT NULL,
    champion_id INT NOT NULL,
    map_name TEXT NOT NULL,
    platform TEXT NOT NULL,
    plays BIGINT NOT NULL,
    wins BIGINT NOT NULL,
    losses BIGINT NOT NULL,
    dpm_sum DOUBLE PRECISION NOT NULL,
    hpm_sum DOUBLE PRECISION NOT NULL,
    metric_samples BIGINT NOT NULL
);

CREATE TABLE champions (
    id INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    kbm_tier INT NOT NULL DEFAULT 0
);

CREATE TABLE player_loadouts (
    id BIGINT PRIMARY KEY,
    player_id BIGINT NOT NULL,
    champion_id INT NOT NULL,
    card_ids INT[],
    card_levels INT[],
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE leaderboard_current (
    player_id BIGINT PRIMARY KEY,
    tier INT NOT NULL,
    rank INT NOT NULL
);

CREATE TABLE ranked_tiers (
    tier_id INT PRIMARY KEY,
    tier_name TEXT NOT NULL
);

CREATE TABLE tier_stats (
    source VARCHAR(10) PRIMARY KEY,
    tier_0 INT DEFAULT 0,
    tier_1 INT DEFAULT 0,
    tier_2 INT DEFAULT 0,
    tier_3 INT DEFAULT 0,
    tier_4 INT DEFAULT 0,
    tier_5 INT DEFAULT 0,
    tier_6 INT DEFAULT 0,
    tier_7 INT DEFAULT 0,
    tier_8 INT DEFAULT 0,
    tier_9 INT DEFAULT 0,
    tier_10 INT DEFAULT 0,
    tier_11 INT DEFAULT 0,
    tier_12 INT DEFAULT 0,
    tier_13 INT DEFAULT 0,
    tier_14 INT DEFAULT 0,
    tier_15 INT DEFAULT 0,
    tier_16 INT DEFAULT 0,
    tier_17 INT DEFAULT 0,
    tier_18 INT DEFAULT 0,
    tier_19 INT DEFAULT 0,
    tier_20 INT DEFAULT 0,
    tier_21 INT DEFAULT 0,
    tier_22 INT DEFAULT 0,
    tier_23 INT DEFAULT 0,
    tier_24 INT DEFAULT 0,
    tier_25 INT DEFAULT 0,
    tier_26 INT DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL
);

INSERT INTO matches
    (match_id, entry_datetime, queue_id, duration_seconds, limited)
VALUES
    (2001, '2026-04-01T00:00:00Z', 486, 600, false),
    (2002, '2026-04-01T01:00:00Z', 486, 900, false),
    (2003, '2026-04-01T02:00:00Z', 486, 300, true),
    (2004, '2026-04-01T03:00:00Z', 424, 700, false);

INSERT INTO match_lobby_tiers
    (match_id, entry_datetime, lobby_tier)
VALUES
    (2001, '2026-04-01T00:00:00Z', 5),
    (2002, '2026-04-01T01:00:00Z', 10),
    (2003, '2026-04-01T02:00:00Z', 20),
    (2004, '2026-04-01T03:00:00Z', 5);

INSERT INTO match_players
    (match_id, entry_datetime, player_id, league_tier, win_status)
VALUES
    (2001, '2026-04-01T00:00:00Z', 1, 5, 'Winner'),
    (2001, '2026-04-01T00:00:00Z', 2, 4, 'Loser'),
    (2002, '2026-04-01T01:00:00Z', 1, 5, 'Win'),
    (2002, '2026-04-01T01:00:00Z', 3, 15, 'Loss'),
    (2002, '2026-04-01T01:00:00Z', 4, 0, NULL),
    (2003, '2026-04-01T02:00:00Z', 5, 20, 'Winner'),
    (2004, '2026-04-01T03:00:00Z', 6, 10, 'Winner');

INSERT INTO stats_match_aggregate
    (queue_id, lobby_tier, stat_date, region, map_name, match_count,
     duration_sum)
VALUES
    (486, 5, '2026-04-01', 'North America', 'Map A', 10, 6000),
    (486, 10, '2026-04-01', 'North America', 'Map B', 5, 4000),
    (486, 10, '2026-04-01', 'Europe', 'Map B', 8, 7200),
    (486, 20, '2026-04-01', 'Europe', 'Map C', 2, 2200),
    (424, 5, '2026-04-01', 'North America', 'Map A', 99, 99000);

INSERT INTO stats_player_aggregate
    (queue_id, lobby_tier, champion_id, map_name, platform, plays, wins,
     losses, dpm_sum, hpm_sum, metric_samples)
VALUES
    (486, 5, 1, 'Map A', 'Steam', 10, 6, 4, 10000, 1000, 10),
    (486, 10, 1, 'Map B', 'Steam', 5, 2, 3, 3500, 500, 5),
    (486, 5, 2, 'Map A', 'Steam', 8, 4, 4, 4000, 0, 8),
    (486, 20, 1, 'Map C', 'PSN', 3, 3, 0, 3000, 300, 3),
    (486, 5, 1, 'Map D', 'XboxLive', 2, 1, 1, 0, 0, 0),
    (424, 5, 1, 'Map A', 'Steam', 100, 90, 10, 100000, 10000, 100);

INSERT INTO champions (id, name)
VALUES
    (1, 'Ash'),
    (3, 'Maeve');

INSERT INTO players (id, name, kbm_tier)
VALUES
    (1, 'One', 5),
    (2, 'Two', 10),
    (3, 'Three', 26),
    (4, 'Four', 26),
    (5, 'Five', 0),
    (6, 'Six', 15),
    (7, 'Seven', 1);

INSERT INTO player_loadouts
    (id, player_id, champion_id, card_ids, card_levels, updated_at)
VALUES
    (1, 1, 1, ARRAY[1,2,3], ARRAY[1,2,3], '2026-04-01T00:00:00Z'),
    (2, 2, 1, ARRAY[1,2,3], ARRAY[1,2,3], '2026-04-02T00:00:00Z'),
    (3, 2, 1, ARRAY[1,2,3], ARRAY[3,2,1], '2026-04-03T00:00:00Z'),
    (4, 3, 4, ARRAY[4], ARRAY[5], '2026-04-04T00:00:00Z'),
    (5, 6, 3, ARRAY[7,8], ARRAY[1,1], '2026-04-05T00:00:00Z'),
    (6, 6, 3, ARRAY[7,8], ARRAY[1,1], '2026-04-06T00:00:00Z'),
    (7, 6, 3, ARRAY[7,8], ARRAY[1,1], '2026-04-07T00:00:00Z');

INSERT INTO leaderboard_current (player_id, tier, rank)
VALUES
    (3, 26, 50),
    (4, 26, 150);

INSERT INTO ranked_tiers (tier_id, tier_name)
SELECT tier_id, 'Named Tier ' || tier_id::TEXT
FROM generate_series(1, 26) AS tiers(tier_id);

UPDATE ranked_tiers SET tier_name = 'Master' WHERE tier_id = 26;

INSERT INTO tier_stats
    (source, tier_0, tier_1, tier_5, tier_10, tier_26, updated_at)
VALUES
    ('matches', 99, 2, 3, 5, 10, '2026-04-08T00:00:00Z');
