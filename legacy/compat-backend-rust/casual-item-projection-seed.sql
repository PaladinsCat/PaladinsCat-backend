CREATE TABLE matches (
    match_id BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    queue_id INT NOT NULL,
    is_ranked BOOLEAN,
    PRIMARY KEY (match_id, entry_datetime)
);

CREATE TABLE match_ingest_status (
    match_id BIGINT PRIMARY KEY,
    status TEXT NOT NULL,
    completed_stages TEXT[] NOT NULL DEFAULT '{}'
);

CREATE TABLE match_players (
    match_id BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    player_id BIGINT NOT NULL,
    win_status TEXT,
    PRIMARY KEY (match_id, entry_datetime, player_id)
);

CREATE TABLE casual_matches (
    match_id BIGINT PRIMARY KEY
);

CREATE TABLE special_matches (
    match_id BIGINT PRIMARY KEY,
    stats_scope TEXT
);

CREATE TABLE queue_types (
    queue_id INT PRIMARY KEY,
    is_ranked BOOLEAN,
    stats_scope TEXT
);

CREATE TABLE items (
    item_id INT PRIMARY KEY,
    item_name TEXT
);

CREATE TABLE match_player_items (
    match_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    item_id INT NOT NULL,
    slot SMALLINT NOT NULL,
    item_level SMALLINT
);

CREATE TABLE item_counts_casual (
    stats_scope VARCHAR(32) NOT NULL,
    queue_id INT NOT NULL,
    item_id INT NOT NULL,
    item_name TEXT,
    slot SMALLINT NOT NULL,
    item_level SMALLINT NOT NULL,
    count BIGINT NOT NULL DEFAULT 0,
    wins BIGINT NOT NULL DEFAULT 0,
    losses BIGINT NOT NULL DEFAULT 0,
    winrate NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stats_scope, queue_id, item_id, slot, item_level)
);

CREATE TABLE item_counts_casual_matches (
    match_id BIGINT PRIMARY KEY,
    stats_scope VARCHAR(32) NOT NULL,
    queue_id INT NOT NULL,
    eligible_players SMALLINT NOT NULL,
    projected_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE item_counts_ranked (
    marker INT PRIMARY KEY
);

INSERT INTO queue_types (queue_id, is_ranked, stats_scope)
VALUES
    (424, false, 'casual'),
    (486, true, 'ranked');

INSERT INTO matches (match_id, entry_datetime, queue_id, is_ranked)
VALUES
    (9100001, '2026-07-30T00:00:00Z', 424, false),
    (9100002, '2026-07-30T01:00:00Z', 486, true);

INSERT INTO match_ingest_status (match_id, status, completed_stages)
VALUES
    (9100001, 'complete', ARRAY['player_facts']),
    (9100002, 'complete', ARRAY['player_facts', 'ranked_stats']);

INSERT INTO casual_matches (match_id) VALUES (9100001);

INSERT INTO match_players
    (match_id, entry_datetime, player_id, win_status)
VALUES
    (9100001, '2026-07-30T00:00:00Z', 1, 'Winner'),
    (9100001, '2026-07-30T00:00:00Z', 2, 'Loss'),
    (9100002, '2026-07-30T01:00:00Z', 3, 'Winner');

INSERT INTO items (item_id, item_name) VALUES (100, 'Chronos');

INSERT INTO match_player_items
    (match_id, player_id, item_id, slot, item_level)
VALUES
    (9100001, 1, 100, 1, 1),
    (9100001, 2, 100, 1, 1),
    (9100002, 3, 100, 1, 1);

INSERT INTO item_counts_ranked (marker) VALUES (1);
