CREATE TABLE champions (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    roles TEXT
);

CREATE TABLE items (
    item_id INT PRIMARY KEY,
    item_name TEXT
);

CREATE TABLE stats_item_aggregate (
    queue_id INT NOT NULL,
    lobby_tier SMALLINT NOT NULL,
    champion_id INT NOT NULL,
    item_id INT NOT NULL,
    slot SMALLINT NOT NULL,
    item_level SMALLINT NOT NULL,
    uses BIGINT NOT NULL,
    wins BIGINT NOT NULL,
    losses BIGINT NOT NULL
);

CREATE TABLE stats_player_aggregate (
    queue_id INT NOT NULL,
    lobby_tier SMALLINT NOT NULL,
    champion_id INT NOT NULL,
    plays BIGINT NOT NULL
);

CREATE TABLE item_counts_casual (
    stats_scope VARCHAR(32) NOT NULL,
    queue_id INT NOT NULL,
    item_id INT NOT NULL,
    item_name TEXT,
    slot SMALLINT NOT NULL,
    item_level SMALLINT NOT NULL,
    count BIGINT NOT NULL,
    wins BIGINT NOT NULL,
    losses BIGINT NOT NULL,
    winrate NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (stats_scope, queue_id, item_id, slot, item_level)
);

CREATE TABLE item_counts_casual_matches (
    match_id BIGINT PRIMARY KEY,
    stats_scope VARCHAR(32) NOT NULL,
    queue_id INT NOT NULL,
    eligible_players SMALLINT NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL
);

INSERT INTO champions (id, name, roles)
VALUES
    (1, 'Ash', 'Front Line'),
    (2, 'Maeve', 'Unknown'),
    (3, 'Viktor', 'Damage');

INSERT INTO items (item_id, item_name)
VALUES
    (100, 'Chronos'),
    (200, 'Haven'),
    (300, NULL);

INSERT INTO stats_item_aggregate
    (queue_id, lobby_tier, champion_id, item_id, slot, item_level, uses, wins, losses)
VALUES
    (486, 5, 1, 100, 1, 1, 10, 6, 4),
    (486, 10, 1, 100, 2, 2, 5, 2, 3),
    (486, 20, 2, 100, 1, 1, 3, 3, 0),
    (486, 5, 1, 200, 2, 1, 4, 1, 3),
    (486, 10, 3, 300, 3, 2, 7, 4, 3),
    (424, 5, 1, 100, 1, 1, 999, 999, 0);

INSERT INTO stats_player_aggregate (queue_id, lobby_tier, champion_id, plays)
VALUES
    (486, 5, 1, 20),
    (486, 10, 1, 10),
    (486, 20, 2, 6),
    (486, 10, 3, 14),
    (424, 5, 1, 999);

INSERT INTO item_counts_casual
    (stats_scope, queue_id, item_id, item_name, slot, item_level,
     count, wins, losses, winrate, updated_at)
VALUES
    ('casual', 424, 100, 'Chronos', 1, 1, 2, 1, 1, 50.00, '2026-07-30T00:00:00Z'),
    ('casual', 424, 100, 'Chronos', 2, 2, 1, 1, 0, 100.00, '2026-07-30T00:00:00Z'),
    ('bot', 425, 100, 'Chronos', 1, 1, 1, 0, 1, 0.00, '2026-07-30T00:00:00Z'),
    ('bot', 425, 200, 'Haven', 2, 1, 3, 2, 1, 66.67, '2026-07-30T00:00:00Z');

INSERT INTO item_counts_casual_matches
    (match_id, stats_scope, queue_id, eligible_players, projected_at)
VALUES
    (7001, 'casual', 424, 2, '2026-07-30T00:00:00Z'),
    (7002, 'casual', 424, 1, '2026-07-30T00:01:00Z'),
    (7003, 'bot', 425, 2, '2026-07-30T00:02:00Z');
