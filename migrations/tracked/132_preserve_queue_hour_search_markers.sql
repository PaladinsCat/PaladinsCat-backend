-- Provider discovery is one durable attempt per queue-hour. Recovery drains
-- locally stored IDs and never turns a fact/projection failure into another
-- getMatchIdsByQueueDetails call.

UPDATE hourly_ingest_state state
SET fetched = TRUE,
    fetch_succeeded = TRUE,
    updated_at = now()
WHERE EXISTS (
  SELECT 1
  FROM match_count_discoveries discovery
  WHERE discovery.source_date = state.date
    AND discovery.source_hour = state.hour
    AND discovery.queue_id = state.queue_id
);

COMMENT ON COLUMN hourly_ingest_state.fetched IS
  'Provider queue-hour search was attempted; recovery never resets it.';
COMMENT ON COLUMN hourly_ingest_state.fetch_succeeded IS
  'Provider search produced durable local IDs; retries drain only those IDs.';
