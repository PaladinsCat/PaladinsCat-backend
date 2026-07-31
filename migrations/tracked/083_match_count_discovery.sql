CREATE TABLE IF NOT EXISTS match_count_discoveries (
  match_id BIGINT NOT NULL,
  queue_id INT NOT NULL,
  region VARCHAR(20) NOT NULL DEFAULT 'Unknown',
  entry_datetime TIMESTAMPTZ,
  active_flag BOOLEAN NOT NULL DEFAULT FALSE,
  source_date DATE NOT NULL,
  source_hour INT NOT NULL CHECK (source_hour BETWEEN 0 AND 23),
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id, queue_id)
);

CREATE INDEX IF NOT EXISTS idx_mcd_window_queue
  ON match_count_discoveries (source_date DESC, source_hour, queue_id);
CREATE INDEX IF NOT EXISTS idx_mcd_queue_region_window
  ON match_count_discoveries (queue_id, region, source_date DESC, source_hour);

CREATE TABLE IF NOT EXISTS match_count_discovery_region_hours (
  date DATE NOT NULL,
  hour INT NOT NULL CHECK (hour BETWEEN 0 AND 23),
  queue_id INT NOT NULL,
  region VARCHAR(20) NOT NULL,
  match_count INT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (date, hour, queue_id, region)
);

CREATE INDEX IF NOT EXISTS idx_mcdrh_window_queue
  ON match_count_discovery_region_hours (date DESC, hour, queue_id);

CREATE TABLE IF NOT EXISTS match_count_discovery_hours (
  date DATE NOT NULL,
  hour INT NOT NULL CHECK (hour BETWEEN 0 AND 23),
  queue_id INT NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'fetching', 'empty', 'complete', 'failed')),
  attempts INT NOT NULL DEFAULT 0,
  match_count INT NOT NULL DEFAULT 0,
  source VARCHAR(50),
  error_message TEXT,
  last_attempt_at TIMESTAMPTZ,
  next_retry_at TIMESTAMPTZ,
  lease_until TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (date, hour, queue_id)
);

CREATE INDEX IF NOT EXISTS idx_mcdh_retry
  ON match_count_discovery_hours (status, next_retry_at, lease_until);
CREATE INDEX IF NOT EXISTS idx_mcdh_queue_window
  ON match_count_discovery_hours (queue_id, date DESC, hour);

COMMENT ON TABLE match_count_discoveries IS
  'Durable ID-only observations from getmatchidsbyqueue. Non-ranked rows never enter the full global stats ingest pipeline.';
COMMENT ON TABLE match_count_discovery_hours IS
  'Scheduler state for all-queue ID-only match-count discovery and outage catch-up.';
