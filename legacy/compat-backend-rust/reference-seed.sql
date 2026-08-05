CREATE TABLE champions (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    title VARCHAR(200),
    health INT NOT NULL,
    speed INT NOT NULL,
    roles VARCHAR(100)
);

CREATE TABLE items (
    item_id INT PRIMARY KEY,
    item_name VARCHAR(200),
    description TEXT,
    item_type VARCHAR(100),
    cost INT,
    icon_url VARCHAR(500),
    champion_id INT,
    recharge_seconds INT,
    talent_reward_level INT
);

CREATE TABLE bounty_items (
    bounty_item_id BIGINT PRIMARY KEY,
    item_id INT,
    item_name VARCHAR(200) NOT NULL,
    champion_id INT,
    champion_name VARCHAR(100),
    initial_price INT,
    final_price INT,
    sale_type VARCHAR,
    sale_end_datetime TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE maps (
    map_id INT PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    map_type VARCHAR(50),
    queue_ids INT[],
    is_ranked BOOLEAN DEFAULT FALSE
);

CREATE TABLE ranked_tiers (
    tier_id INT PRIMARY KEY,
    tier_name VARCHAR(50) NOT NULL
);

CREATE TABLE regions (
    region_code VARCHAR(50) PRIMARY KEY,
    region_name VARCHAR(100) NOT NULL,
    continent VARCHAR(50),
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE talents (
    talent_id INT PRIMARY KEY,
    talent_name VARCHAR(200) NOT NULL,
    champion_id INT
);

CREATE TABLE queue_types (
    queue_id INT PRIMARY KEY,
    queue_name VARCHAR(50) NOT NULL,
    is_ranked BOOLEAN DEFAULT FALSE,
    stats_scope VARCHAR(32) NOT NULL,
    participant_model VARCHAR(16) NOT NULL,
    stats_enabled BOOLEAN NOT NULL,
    track_presence BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE patches (
    id SERIAL PRIMARY KEY,
    version VARCHAR(20) NOT NULL,
    release_date DATE NOT NULL,
    description TEXT,
    is_current BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE cards (
    card_id INT PRIMARY KEY,
    card_name VARCHAR(200),
    champion_id INT
);

CREATE TABLE skins (
    skin_id INT PRIMARY KEY,
    champion_id INT,
    skin_name VARCHAR(200)
);

CREATE TABLE championsquick (
    id INT PRIMARY KEY,
    ability1_id INT,
    ability2_id INT,
    ability3_id INT,
    ability4_id INT,
    ability5_id INT
);

INSERT INTO champions (id, name, title, health, speed, roles) VALUES
    (1, 'Androxus', 'The Godslayer', 2100, 370, 'Flanker'),
    (2, 'Barik', 'The Master Mechanic', 3400, 350, 'Front Line');

INSERT INTO items (
    item_id, item_name, description, item_type, cost, icon_url,
    champion_id, recharge_seconds, talent_reward_level
) VALUES
    (10, 'Cursed Revolver', 'Weapon talent', 'Talent', 0, 'https://example.test/10.png', 1, NULL, 2),
    (11, 'Bowling Ball', 'Shield card', 'Card', NULL, NULL, 2, 1, NULL);

INSERT INTO bounty_items (
    bounty_item_id, item_id, item_name, champion_id, champion_name,
    initial_price, final_price, sale_type, sale_end_datetime, is_active,
    created_at, updated_at
) VALUES
    (9000000001, 10, 'Bounty One', 1, 'Androxus', 100, 80, 'fixed',
     '2026-05-02T00:00:00Z', TRUE, '2026-05-01T00:00:00Z', '2026-05-01T01:00:00Z'),
    (9000000002, 11, 'Bounty Two', 2, 'Barik', 200, 150, 'decreasing',
     NULL, FALSE, '2026-05-03T00:00:00Z', '2026-05-03T01:00:00Z');

INSERT INTO maps (map_id, name, map_type, queue_ids, is_ranked) VALUES
    (20, 'Ascension Peak', 'Siege', ARRAY[424, 486], TRUE),
    (21, 'Magistrate''s Archives', 'Onslaught', ARRAY[452], FALSE);

INSERT INTO ranked_tiers (tier_id, tier_name) VALUES
    (1, 'Bronze V'),
    (27, 'Grandmaster');

INSERT INTO regions (region_code, region_name, continent, created_at) VALUES
    ('EU', 'Europe', 'Europe', '2026-05-01T00:00:00Z'),
    ('NA', 'North America', 'North America', '2026-05-02T00:00:00Z');

INSERT INTO talents (talent_id, talent_name, champion_id) VALUES
    (30, 'Godslayer', 1),
    (31, 'Fortify', 2);

INSERT INTO queue_types (
    queue_id, queue_name, is_ranked, stats_scope, participant_model,
    stats_enabled, track_presence, created_at
) VALUES
    (424, 'Casual Siege', FALSE, 'casual', 'pvp', TRUE, TRUE, '2026-05-01T00:00:00Z'),
    (486, 'Ranked Siege', TRUE, 'ranked', 'pvp', TRUE, TRUE, '2026-05-02T00:00:00Z');

INSERT INTO patches (
    id, version, release_date, description, is_current, created_at
) VALUES
    (1, '8.1', '2026-04-01', 'Previous patch', FALSE, '2026-04-01T00:00:00Z'),
    (2, '8.2', '2026-05-01', 'Current patch', TRUE, '2026-05-01T00:00:00Z');

INSERT INTO cards (card_id, card_name, champion_id) VALUES
    (40, 'Abyss Walker', 1),
    (41, 'Brave and Bold', 2);

INSERT INTO skins (skin_id, champion_id, skin_name) VALUES
    (50, 1, 'Default Androxus'),
    (51, 2, 'Default Barik');

INSERT INTO championsquick (
    id, ability1_id, ability2_id, ability3_id, ability4_id, ability5_id
) VALUES
    (1, 101, 102, 103, 104, 105),
    (2, 201, 202, 203, 204, 205);
