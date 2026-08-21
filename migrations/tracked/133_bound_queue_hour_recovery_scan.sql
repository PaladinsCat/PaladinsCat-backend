-- Automatic recovery selects only the newest durable hour. This index keeps
-- the five-minute scan on the durable marker instead of historical state.

CREATE INDEX IF NOT EXISTS idx_his_durable_recovery_newest
  ON hourly_ingest_state (date DESC, hour DESC, queue_id)
  WHERE fetched = TRUE AND fetch_succeeded = TRUE;
