-- Purpose: normalize legacy execution rows whose owner lease already expired.
-- Input: fetching rows without a live owner. Output: immediately reclaimable
-- failed rows; a currently executing queue-hour remains untouched.
-- Relationship: queue_hour_needs_recovery and claim_hourly_ingest_hour consume
-- the same lease boundary, so the database and scheduler report one truth.

UPDATE hourly_ingest_state
SET status = 'failed',
    next_retry_at = NULL,
    lease_until = NULL,
    error_message = COALESCE(error_message, 'expired execution lease normalized for immediate recovery'),
    updated_at = now()
WHERE status = 'fetching'
  AND (lease_until IS NULL OR lease_until <= now());
