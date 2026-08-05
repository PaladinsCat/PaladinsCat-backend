CREATE TABLE players (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE player_relationships (
    source_player_id BIGINT NOT NULL REFERENCES players(id),
    target_player_id BIGINT NOT NULL REFERENCES players(id),
    same_team BOOLEAN NOT NULL,
    same_party BOOLEAN NOT NULL,
    count INT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_player_id, target_player_id, same_team)
);

CREATE TABLE party_pair_stats (
    player_low_id BIGINT NOT NULL REFERENCES players(id),
    player_high_id BIGINT NOT NULL REFERENCES players(id),
    match_count INT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (player_low_id, player_high_id)
);

CREATE TABLE party_stack_stats (
    group_key TEXT PRIMARY KEY,
    player_ids BIGINT[] NOT NULL,
    stack_size SMALLINT NOT NULL,
    match_count INT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL
);

CREATE TABLE mv_player_coplay_stats (
    source_player_id BIGINT NOT NULL REFERENCES players(id),
    target_player_id BIGINT NOT NULL REFERENCES players(id),
    same_team BOOLEAN NOT NULL,
    times_together INT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_player_id, target_player_id, same_team)
);

INSERT INTO players (id, name)
VALUES
    (10, 'Alice'),
    (20, 'Bob'),
    (30, 'Carol'),
    (40, 'Dave'),
    (50, 'Echo');

INSERT INTO player_relationships
    (source_player_id, target_player_id, same_team, same_party, count,
     first_seen, last_seen)
VALUES
    (10, 20, true,  true,  5, '2026-05-01T00:00:00Z', '2026-05-05T00:00:00Z'),
    (10, 30, false, false, 7, '2026-05-01T00:00:00Z', '2026-05-07T00:00:00Z'),
    (20, 30, true,  true,  3, '2026-05-02T00:00:00Z', '2026-05-03T00:00:00Z'),
    (10, 40, false, false, 2, '2026-05-02T00:00:00Z', '2026-05-04T00:00:00Z'),
    (40, 50, true,  false, 1, '2026-05-01T00:00:00Z', '2026-05-02T00:00:00Z');

INSERT INTO party_pair_stats
    (player_low_id, player_high_id, match_count, first_seen, last_seen)
VALUES
    (10, 20, 4, '2026-05-01T00:00:00Z', '2026-05-05T00:00:00Z'),
    (10, 30, 1, '2026-05-01T00:00:00Z', '2026-05-02T00:00:00Z'),
    (20, 30, 2, '2026-05-02T00:00:00Z', '2026-05-03T00:00:00Z');

INSERT INTO party_stack_stats
    (group_key, player_ids, stack_size, match_count, first_seen, last_seen)
VALUES
    ('10:20', ARRAY[10, 20]::BIGINT[], 2, 4,
     '2026-05-01T00:00:00Z', '2026-05-05T00:00:00Z'),
    ('10:20:30', ARRAY[10, 20, 30]::BIGINT[], 3, 2,
     '2026-05-02T00:00:00Z', '2026-05-03T00:00:00Z'),
    ('30:40:50', ARRAY[30, 40, 50]::BIGINT[], 3, 5,
     '2026-05-03T00:00:00Z', '2026-05-08T00:00:00Z');

INSERT INTO mv_player_coplay_stats
    (source_player_id, target_player_id, same_team, times_together,
     first_seen, last_seen)
VALUES
    (10, 20, true, 5, '2026-05-01T00:00:00Z', '2026-05-05T00:00:00Z'),
    (10, 30, false, 7, '2026-05-01T00:00:00Z', '2026-05-07T00:00:00Z'),
    (20, 30, true, 3, '2026-05-02T00:00:00Z', '2026-05-03T00:00:00Z');
