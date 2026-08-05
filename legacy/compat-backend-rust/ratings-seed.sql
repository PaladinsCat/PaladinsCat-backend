CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    hz_player_name VARCHAR(100),
    hz_gamer_tag VARCHAR(100)
);

CREATE TABLE champions (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE matches (
    match_id BIGINT PRIMARY KEY,
    queue_id INT NOT NULL
);

CREATE TABLE champion_ratings (
    champion_id INT PRIMARY KEY REFERENCES champions(id),
    rating DOUBLE PRECISION NOT NULL,
    deviation DOUBLE PRECISION NOT NULL,
    volatility DOUBLE PRECISION NOT NULL,
    matches_played INT NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE champion_match_ratings (
    match_id BIGINT NOT NULL,
    champion_id INT NOT NULL REFERENCES champions(id),
    pre_rating NUMERIC NOT NULL,
    post_rating NUMERIC NOT NULL,
    pre_uncertainty NUMERIC NOT NULL,
    post_uncertainty NUMERIC NOT NULL,
    last_updated TIMESTAMPTZ NOT NULL
);

CREATE TABLE player_queue_ratings (
    player_id BIGINT NOT NULL REFERENCES players(id),
    queue_id INT NOT NULL,
    mu NUMERIC NOT NULL,
    phi NUMERIC NOT NULL,
    volatility NUMERIC NOT NULL,
    player_key TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (player_id, queue_id)
);

CREATE TABLE player_champion_ratings (
    player_id BIGINT NOT NULL REFERENCES players(id),
    champion_id INT NOT NULL REFERENCES champions(id),
    mu NUMERIC NOT NULL,
    phi NUMERIC NOT NULL,
    volatility NUMERIC NOT NULL,
    matches_played INT NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    wins_flag INT,
    player_key TEXT,
    PRIMARY KEY (player_id, champion_id)
);

CREATE TABLE match_rating_snapshots (
    match_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL REFERENCES players(id),
    champion_id INT NOT NULL REFERENCES champions(id),
    queue_mu_pre NUMERIC,
    queue_mu_post NUMERIC,
    champ_mu_pre NUMERIC,
    champ_mu_post NUMERIC,
    created_at TIMESTAMPTZ NOT NULL,
    queue_phi_pre NUMERIC,
    queue_phi_post NUMERIC,
    champ_phi_pre NUMERIC,
    champ_phi_post NUMERIC,
    queue_volatility_pre NUMERIC,
    queue_volatility_post NUMERIC,
    champ_volatility_pre NUMERIC,
    champ_volatility_post NUMERIC,
    PRIMARY KEY (match_id, player_id, champion_id)
);

INSERT INTO players (id, name, hz_player_name, hz_gamer_tag)
VALUES
    (100, 'Stored Name', 'Preferred Name', 'Stored Tag'),
    (101, 'Stored Two', 'DummyPlayer101', 'Visible Gamer'),
    (102, 'Fallback Name',
     '0123456789abcdef0123456789abcdefUser-abcdef', '');

INSERT INTO champions (id, name)
VALUES (1, 'Androxus'), (2, 'Barik');

INSERT INTO matches (match_id, queue_id)
VALUES (300, 486), (301, 424), (302, 486);

INSERT INTO champion_ratings
    (champion_id, rating, deviation, volatility, matches_played, wins, losses, updated_at)
VALUES
    (1, 1700.5, 80.25, 0.05, 50, 30, 20, '2026-05-01T00:00:00Z'),
    (2, 1500.0, 120.5, 0.06, 20, 10, 10, '2026-05-02T00:00:00Z');

INSERT INTO champion_match_ratings
    (match_id, champion_id, pre_rating, post_rating, pre_uncertainty,
     post_uncertainty, last_updated)
VALUES
    (300, 1, 1600.00, 1625.2500, 100.00, 95.500, '2026-05-03T00:00:00Z'),
    (301, 1, 1625.2500, 1610.00, 95.500, 93.00, '2026-05-04T00:00:00Z'),
    (302, 2, 1490.00, 1510.00, 120.00, 115.00, '2026-05-05T00:00:00Z');

INSERT INTO player_queue_ratings
    (player_id, queue_id, mu, phi, volatility, player_key, updated_at)
VALUES
    (100, 424, 1400.00, 200.00, 0.1000, 'p100', '2026-05-06T00:00:00Z'),
    (100, 486, 1600.50, 100.00, 0.0600, 'p100', '2026-05-07T00:00:00Z'),
    (100, 999, 4000.00, 100.00, 0.0600, 'invalid', '2026-05-08T00:00:00Z'),
    (101, 486, 1700.00, 90.00, 0.0500, 'p101', '2026-05-09T00:00:00Z');

INSERT INTO player_champion_ratings
    (player_id, champion_id, mu, phi, volatility, matches_played, wins,
     losses, updated_at, wins_flag, player_key)
VALUES
    (100, 1, 1650.00, 110.00, 0.0500, 25, 15, 10,
     '2026-05-10T00:00:00Z', 1, 'p100'),
    (100, 2, 1450.00, 150.00, 0.0700, 10, 4, 6,
     '2026-05-11T00:00:00Z', 0, 'p100'),
    (101, 1, 1750.00, 80.00, 0.0400, 30, 20, 10,
     '2026-05-12T00:00:00Z', 1, 'p101');

INSERT INTO match_rating_snapshots
    (match_id, player_id, champion_id, queue_mu_pre, queue_mu_post,
     champ_mu_pre, champ_mu_post, created_at, queue_phi_pre, queue_phi_post,
     champ_phi_pre, champ_phi_post, queue_volatility_pre,
     queue_volatility_post, champ_volatility_pre, champ_volatility_post)
VALUES
    (300, 100, 1, 1580.00, 1600.50, 1630.00, 1650.00,
     '2026-05-13T00:00:00Z', 105.00, 100.00, 115.00, 110.00,
     0.0610, 0.0600, 0.0510, 0.0500),
    (300, 101, 1, 1680.00, 1700.00, 1730.00, 1750.00,
     '2026-05-13T00:00:01Z', 95.00, 90.00, 85.00, 80.00,
     0.0510, 0.0500, 0.0410, 0.0400),
    (301, 100, 2, 1410.00, 1400.00, 1460.00, 1450.00,
     '2026-05-14T00:00:00Z', 195.00, 200.00, 145.00, 150.00,
     0.0990, 0.1000, 0.0690, 0.0700);
