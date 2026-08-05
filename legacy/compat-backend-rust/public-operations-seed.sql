CREATE TABLE matches (
    match_id BIGINT PRIMARY KEY,
    queue_id INT,
    recovered BOOLEAN NOT NULL DEFAULT FALSE,
    broken BOOLEAN NOT NULL DEFAULT FALSE,
    entry_datetime TIMESTAMPTZ
);

CREATE TABLE players (
    id BIGINT PRIMARY KEY
);

CREATE TABLE users (
    id INT PRIMARY KEY,
    linked_player_id BIGINT
);

CREATE TABLE builds (
    id INT PRIMARY KEY,
    visibility VARCHAR(10) NOT NULL
);

CREATE TABLE tier_lists (
    post_id INT PRIMARY KEY
);

CREATE TABLE posts (
    id INT PRIMARY KEY
);

CREATE TABLE site_daily_visitors (
    visit_date DATE NOT NULL,
    visitor_hash TEXT NOT NULL,
    page_views INT NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (visit_date, visitor_hash)
);

CREATE TABLE nonranked_match_acquisition (
    match_id BIGINT PRIMARY KEY,
    source_date DATE NOT NULL,
    source_hour SMALLINT NOT NULL,
    status VARCHAR(24) NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE TABLE stack_versions (
    id SERIAL PRIMARY KEY,
    component TEXT NOT NULL,
    version TEXT NOT NULL,
    git_commit_short TEXT,
    deployed_at TIMESTAMPTZ NOT NULL
);

INSERT INTO matches (
    match_id, queue_id, recovered, broken, entry_datetime
) VALUES
    (1001, 486, FALSE, FALSE, now() - interval '1 hour'),
    (1002, 486, TRUE, TRUE, now() - interval '2 hours'),
    (1003, 424, FALSE, TRUE, now() - interval '3 hours'),
    (1004, 424, FALSE, FALSE, now() - interval '30 hours');

INSERT INTO players (id) VALUES (10), (11), (-1);

INSERT INTO users (id, linked_player_id) VALUES
    (1, 10),
    (2, NULL);

INSERT INTO builds (id, visibility) VALUES
    (1, 'public'),
    (2, 'private');

INSERT INTO tier_lists (post_id) VALUES (1);
INSERT INTO posts (id) VALUES (1), (2);

INSERT INTO site_daily_visitors (
    visit_date, visitor_hash, page_views, first_seen, last_seen
) VALUES
    (CURRENT_DATE, 'visitor-a', 3, now() - interval '1 hour', now() - interval '1 minute'),
    (CURRENT_DATE, 'visitor-b', 2, now() - interval '2 hours', now() - interval '10 minutes'),
    (CURRENT_DATE - 2, 'visitor-c', 4, now() - interval '2 days', now() - interval '2 days');

INSERT INTO nonranked_match_acquisition (
    match_id, source_date, source_hour, status, completed_at
) VALUES
    (
      2001,
      ((now() AT TIME ZONE 'UTC') - interval '1 hour')::DATE,
      EXTRACT(HOUR FROM ((now() AT TIME ZONE 'UTC') - interval '1 hour'))::SMALLINT,
      'discovered',
      NULL
    ),
    (
      2002,
      ((now() AT TIME ZONE 'UTC') - interval '2 hours')::DATE,
      EXTRACT(HOUR FROM ((now() AT TIME ZONE 'UTC') - interval '2 hours'))::SMALLINT,
      'waiting_for_completion',
      NULL
    ),
    (
      2003,
      ((now() AT TIME ZONE 'UTC') - interval '30 hours')::DATE,
      EXTRACT(HOUR FROM ((now() AT TIME ZONE 'UTC') - interval '30 hours'))::SMALLINT,
      'fetching',
      NULL
    ),
    (
      2004,
      ((now() AT TIME ZONE 'UTC') - interval '3 hours')::DATE,
      EXTRACT(HOUR FROM ((now() AT TIME ZONE 'UTC') - interval '3 hours'))::SMALLINT,
      'complete_direct',
      now() - interval '2 hours'
    );

INSERT INTO stack_versions (
    component, version, git_commit_short, deployed_at
) VALUES
    ('stack', 'v0.8.2', 'abc1234', '2026-05-01T00:00:00Z'),
    ('frontend', 'v0.8.1', 'ignored', '2026-05-02T00:00:00Z');
