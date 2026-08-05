CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    hz_player_name VARCHAR(100),
    hz_gamer_tag VARCHAR(100)
);

CREATE TABLE esports_leagues (
    league_id INT PRIMARY KEY,
    league_name VARCHAR NOT NULL,
    league_description TEXT,
    league_image_url VARCHAR,
    league_start_date TIMESTAMPTZ,
    league_end_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE esports_teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR NOT NULL,
    team_description TEXT,
    team_image_url VARCHAR,
    league_id INT REFERENCES esports_leagues(league_id),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE esports_team_players (
    player_id INT NOT NULL,
    team_id INT NOT NULL REFERENCES esports_teams(team_id),
    player_name VARCHAR,
    created_at TIMESTAMPTZ NOT NULL
);

INSERT INTO esports_leagues
    (league_id, league_name, league_description, league_image_url,
     league_start_date, league_end_date, created_at, updated_at)
VALUES
    (1, 'Alpha League', 'First league', '/alpha.png',
     '2026-01-01T00:00:00Z', '2026-02-01T00:00:00Z',
     '2026-01-01T00:00:00Z', '2026-01-02T00:00:00Z'),
    (2, 'Beta Circuit', NULL, NULL,
     '2026-03-01T00:00:00Z', NULL,
     '2026-01-03T00:00:00Z', '2026-01-04T00:00:00Z'),
    (3, 'Gamma Cup', 'Third league', '/gamma.png',
     NULL, NULL,
     '2026-01-05T00:00:00Z', '2026-01-06T00:00:00Z');

INSERT INTO esports_teams
    (team_id, team_name, team_description, team_image_url, league_id,
     created_at, updated_at)
VALUES
    (10, 'Aegis', 'Alpha first', '/aegis.png', 1,
     '2026-02-01T00:00:00Z', '2026-02-02T00:00:00Z'),
    (11, 'Blaze', 'Alpha second', NULL, 1,
     '2026-02-03T00:00:00Z', '2026-02-04T00:00:00Z'),
    (20, 'Cyclone', NULL, '/cyclone.png', 2,
     '2026-02-05T00:00:00Z', '2026-02-06T00:00:00Z'),
    (30, 'Delta', 'Independent', NULL, NULL,
     '2026-02-07T00:00:00Z', '2026-02-08T00:00:00Z');

INSERT INTO players (id, name, hz_player_name, hz_gamer_tag)
VALUES
    (100, 'Stored Name', 'Preferred Name', 'Stored Tag'),
    (101, 'Stored Two', 'DummyPlayer101', 'Visible Gamer'),
    (102, 'Fallback Name',
     '0123456789abcdef0123456789abcdefUser-abcdef', ''),
    (103, 'DummyPlayer103', NULL, NULL);

INSERT INTO esports_team_players (player_id, team_id, player_name, created_at)
VALUES
    (100, 10, 'Stale Preferred', '2026-04-01T00:00:00Z'),
    (101, 10, 'Stale Gamer', '2026-04-02T00:00:00Z'),
    (102, 20, 'Stale Fallback', '2026-04-03T00:00:00Z'),
    (103, 30, 'Stale Synthetic', '2026-04-04T00:00:00Z');
