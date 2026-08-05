CREATE TABLE broken_skins (
    id BIGSERIAL PRIMARY KEY,
    champion_id INT NOT NULL,
    champion_name VARCHAR(100) NOT NULL,
    skin_id INT NOT NULL,
    skin_name VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_broken_skins UNIQUE (champion_id, skin_id)
);

CREATE TABLE recovery_stats (
    match_id BIGINT PRIMARY KEY,
    dev_id VARCHAR(10),
    players INT NOT NULL,
    direct_count INT NOT NULL DEFAULT 0,
    recovered_count INT NOT NULL DEFAULT 0,
    missing_count INT NOT NULL DEFAULT 0,
    api_calls INT NOT NULL DEFAULT 0,
    total_calls INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE raw_ingest_buffer (
    id BIGSERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    endpoint VARCHAR NOT NULL,
    entity_type VARCHAR NOT NULL,
    entity_id VARCHAR
);

INSERT INTO broken_skins
  (id, champion_id, champion_name, skin_id, skin_name, created_at)
VALUES
  (1, 101, 'Fixture Champion', 40001, 'Fixture Overflow', '2026-07-01T01:02:03.456Z'),
  (2, 102, 'Other Champion', 40002, 'Other Overflow', '2026-07-02T02:03:04.567Z');
SELECT setval('broken_skins_id_seq', 2);

INSERT INTO recovery_stats
  (match_id, dev_id, players, direct_count, recovered_count, missing_count,
   api_calls, total_calls, created_at)
VALUES
  (1280000001, 'fixture', 10, 7, 3, 0, 4, 5,
   '2026-07-03T03:04:05.678Z'),
  (1280000002, NULL, 5, 5, 0, 0, 1, 1,
   '2026-07-04T04:05:06.789Z');

INSERT INTO raw_ingest_buffer
  (id, raw_data, status, created_at, endpoint, entity_type, entity_id)
VALUES
  (1, '{}', 'pending', '2026-07-05T05:06:07.890Z', 'getmatchdetailsbatch', 'match', '1280000003'),
  (2, '{}', 'failed', '2026-07-05T05:05:06.789Z', 'getmatchdetailsbatch', 'match', '1280000004'),
  (3, '{}', 'processed', '2026-07-05T05:04:05.678Z', 'getmatchdetailsbatch', 'match', '1280000005'),
  (4, '{}', 'pending', '2026-07-05T05:03:04.567Z', 'getplayer', 'player', '101');
SELECT setval('raw_ingest_buffer_id_seq', 4);
