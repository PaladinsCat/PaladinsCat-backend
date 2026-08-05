CREATE TABLE site_versions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    version TEXT NOT NULL
);

CREATE TABLE stack_versions (
    id SERIAL PRIMARY KEY,
    component TEXT NOT NULL,
    environment TEXT NOT NULL,
    version TEXT NOT NULL,
    git_commit TEXT,
    git_commit_short TEXT,
    git_branch TEXT,
    git_dirty BOOLEAN,
    build_timestamp TIMESTAMPTZ,
    deployed_at TIMESTAMPTZ,
    db_schema_version TEXT,
    source TEXT,
    notes TEXT,
    metadata JSONB,
    changelog TEXT
);

CREATE TABLE item_counts_ranked (
    item_id INT NOT NULL,
    item_name TEXT,
    slot SMALLINT NOT NULL,
    item_level SMALLINT NOT NULL,
    count INT NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
    winrate NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (item_id, slot, item_level)
);

CREATE TABLE item_counts_casual (LIKE item_counts_ranked INCLUDING ALL);

CREATE TABLE talent_counts_ranked (
    talent_id INT PRIMARY KEY,
    champion_name TEXT,
    talent_name TEXT,
    count INT NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
    winrate NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE talent_counts_casual (LIKE talent_counts_ranked INCLUDING ALL);

CREATE TABLE card_counts_ranked (
    card_id INT NOT NULL,
    champion_name TEXT,
    card_name TEXT,
    card_level SMALLINT NOT NULL,
    count INT NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
    winrate NUMERIC(5,2) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (card_id, card_level)
);

CREATE TABLE card_counts_casual (LIKE card_counts_ranked INCLUDING ALL);

CREATE TABLE match_compositions_ranked (
    comp_id TEXT NOT NULL,
    lobby_tier SMALLINT NOT NULL,
    frontline SMALLINT NOT NULL,
    damage SMALLINT NOT NULL,
    flank SMALLINT NOT NULL,
    support SMALLINT NOT NULL,
    count INT NOT NULL,
    wins INT NOT NULL,
    losses INT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (comp_id, lobby_tier)
);

INSERT INTO site_versions (id, timestamp, version)
VALUES (1, '2025-12-01T00:00:00Z', 'v0.legacy');

INSERT INTO stack_versions
    (id, component, environment, version, git_commit, git_commit_short,
     git_branch, git_dirty, build_timestamp, deployed_at, db_schema_version,
     source, notes, metadata, changelog)
VALUES
    (1, 'stack', 'production', 'v1.0.0', 'aaaaaaa111111111', NULL,
     'main', false, '2026-01-01T00:00:00Z', '2026-01-01T00:10:00Z',
     '100', 'deploy', 'first useful deploy', '{}',
     E'abcdef1 First change\nabcdef2 Second change'),
    (2, 'stack', 'production', 'v1.0.0', 'aaaaaaa111111111', 'aaaaaaa',
     'main', false, '2026-01-01T01:00:00Z', '2026-01-01T01:10:00Z',
     '100', 'redeploy', 'operational duplicate', '{}', ''),
    (3, 'stack', 'production', 'v1.1.0', 'bbbbbbb222222222', NULL,
     'main', true, '2026-02-01T00:00:00Z', '2026-02-01T00:10:00Z',
     '101', 'deploy', NULL, '{"changeCount": 6}',
     E'bbbbbbb Third change\nbbbbbbc Fourth change'),
    (4, 'stack', 'staging', 'v1.2.0', NULL, NULL,
     NULL, NULL, NULL, '2026-03-01T00:10:00Z',
     NULL, NULL, NULL, NULL,
     E'**Added**\n- Manual one\n- Manual two'),
    (5, 'backend', 'production', 'v1.0.0', 'backendold111111', NULL,
     'main', false, '2026-01-01T00:00:00Z', '2026-01-01T00:05:00Z',
     '100', 'deploy', NULL, '{}', NULL),
    (6, 'backend', 'production', 'v1.2.0', 'backendnew222222', NULL,
     'main', false, '2026-03-01T00:00:00Z', '2026-03-01T00:05:00Z',
     '102', 'deploy', NULL, '{"runtime":"rust"}', NULL),
    (7, 'hirezrelay', 'production', 'v1.2.0', 'relay1234567890', 'relay12',
     'main', false, '2026-03-01T00:00:00Z', '2026-03-01T00:06:00Z',
     '102', 'deploy', NULL, '{}', NULL);

SELECT setval('site_versions_id_seq', 1, true);
SELECT setval('stack_versions_id_seq', 7, true);

INSERT INTO item_counts_ranked
    (item_id, item_name, slot, item_level, count, wins, losses, winrate, updated_at)
VALUES
    (100, 'Chronos', 1, 1, 30, 18, 12, 60.00, '2026-03-01T00:00:00'),
    (100, 'Chronos', 1, 2, 20, 10, 10, 50.00, '2026-03-02T00:00:00'),
    (101, 'Haven', 2, 1, 40, 22, 18, 55.00, '2026-03-03T00:00:00'),
    (102, NULL, 3, 1, 5, 1, 4, 20.00, '2026-03-04T00:00:00');

INSERT INTO item_counts_casual
    (item_id, item_name, slot, item_level, count, wins, losses, winrate, updated_at)
VALUES
    (201, 'Nimble', 1, 1, 90, 50, 40, 55.56, '2026-03-01T00:00:00'),
    (202, 'Veteran', 2, 2, 70, 30, 40, 42.86, '2026-03-02T00:00:00');

INSERT INTO talent_counts_ranked
    (talent_id, champion_name, talent_name, count, wins, losses, winrate, updated_at)
VALUES
    (300, 'Ash', 'Fortress Breaker', 100, 60, 40, 60.00, '2026-03-01T00:00:00'),
    (301, 'Maeve', 'Cat Burglar', 80, 36, 44, 45.00, '2026-03-02T00:00:00'),
    (302, 'Pip', NULL, 10, 2, 8, 20.00, '2026-03-03T00:00:00');

INSERT INTO talent_counts_casual
    (talent_id, champion_name, talent_name, count, wins, losses, winrate, updated_at)
VALUES
    (400, 'Ying', 'Life Exchange', 120, 72, 48, 60.00, '2026-03-01T00:00:00'),
    (401, 'Bomb King', 'Royal Subjects', 40, 18, 22, 45.00, '2026-03-02T00:00:00');

INSERT INTO card_counts_ranked
    (card_id, champion_name, card_name, card_level, count, wins, losses, winrate, updated_at)
VALUES
    (500, 'Ash', 'Brawl', 1, 200, 120, 80, 60.00, '2026-03-01T00:00:00'),
    (500, 'Ash', 'Brawl', 5, 150, 75, 75, 50.00, '2026-03-02T00:00:00'),
    (501, 'Maeve', 'Street Cred', 3, 90, 40, 50, 44.44, '2026-03-03T00:00:00');

INSERT INTO card_counts_casual
    (card_id, champion_name, card_name, card_level, count, wins, losses, winrate, updated_at)
VALUES
    (600, 'Ying', 'Brittle', 5, 300, 180, 120, 60.00, '2026-03-01T00:00:00'),
    (601, 'Pip', 'Reload', 2, 50, 20, 30, 40.00, '2026-03-02T00:00:00');

INSERT INTO match_compositions_ranked
    (comp_id, lobby_tier, frontline, damage, flank, support,
     count, wins, losses, updated_at)
VALUES
    ('1-2-1-1', 5, 1, 2, 1, 1, 10, 6, 4, '2026-03-01T00:00:00Z'),
    ('1-2-1-1', 10, 1, 2, 1, 1, 5, 2, 3, '2026-03-02T00:00:00Z'),
    ('2-1-1-1', 15, 2, 1, 1, 1, 20, 14, 6, '2026-03-03T00:00:00Z'),
    ('1-1-2-1', 20, 1, 1, 2, 1, 8, 0, 0, '2026-03-04T00:00:00Z');
