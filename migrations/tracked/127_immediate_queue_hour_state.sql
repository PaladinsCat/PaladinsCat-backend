-- Purpose: preserve every legacy discovered match ID, then remove deferred
-- queue-hour states and retry clocks. Input: legacy recovery ledgers. Output:
-- one immediate queue-hour ledger plus match_count_discoveries as ID authority.

INSERT INTO match_count_discoveries (
  match_id, queue_id, region, active_flag, source_date, source_hour
)
SELECT match_id, queue_id, 'Unknown', FALSE, date, hour
FROM hourly_ingest_match_debt
WHERE match_id > 0 AND queue_id > 0
ON CONFLICT (match_id, queue_id) DO NOTHING;

DROP TABLE hourly_ingest_match_debt;

UPDATE hourly_ingest_state
SET status = 'failed',
    next_retry_at = NULL,
    lease_until = NULL,
    error_message = COALESCE(error_message, 'legacy deferred state normalized for immediate recovery'),
    updated_at = now()
WHERE status IN ('pending', 'staged');

UPDATE hourly_ingest_state
SET next_retry_at = NULL,
    updated_at = now()
WHERE next_retry_at IS NOT NULL;

ALTER TABLE hourly_ingest_state
  ALTER COLUMN status SET DEFAULT 'fetching';

ALTER TABLE hourly_ingest_state
  DROP CONSTRAINT IF EXISTS hourly_ingest_state_status_check;

ALTER TABLE hourly_ingest_state
  ADD CONSTRAINT hourly_ingest_state_status_check
  CHECK (status IN ('fetching', 'empty', 'complete', 'failed'));

COMMENT ON TABLE hourly_ingest_state IS
  'Immediate queue-hour execution ledger. Unfinished hours are fetching or failed; no pending, staged, retry, debt, or cooldown state is permitted.';
