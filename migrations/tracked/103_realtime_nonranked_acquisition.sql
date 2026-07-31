-- Non-ranked acquisition is a durable real-time queue. Discovered IDs must
-- remain claimable until they reach a fact-durable terminal state, and matches
-- observed while still active must not spend their one terminal lookup early.

ALTER TABLE nonranked_match_acquisition
  ADD COLUMN IF NOT EXISTS active_flag BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE nonranked_match_acquisition
  DROP CONSTRAINT IF EXISTS nonranked_match_acquisition_status_check;

ALTER TABLE nonranked_match_acquisition
  ADD CONSTRAINT nonranked_match_acquisition_status_check CHECK (
    status IN (
      'discovered', 'waiting_for_completion', 'fetching', 'complete_direct',
      'partial_roster', 'roster_only', 'service_deferred', 'dropped'
    )
  );

INSERT INTO nonranked_match_acquisition (
  match_id, queue_id, stats_scope, source_date, source_hour, region,
  discovered_entry_datetime, active_flag, status,
  first_discovered_at, last_observed_at
)
SELECT
  discovery.match_id,
  discovery.queue_id,
  COALESCE(queue.stats_scope, 'other'),
  discovery.source_date,
  discovery.source_hour,
  discovery.region,
  discovery.entry_datetime,
  discovery.active_flag,
  CASE
    WHEN discovery.active_flag THEN 'waiting_for_completion'
    ELSE 'discovered'
  END,
  discovery.first_seen_at,
  discovery.last_seen_at
FROM match_count_discoveries discovery
JOIN queue_types queue ON queue.queue_id = discovery.queue_id
LEFT JOIN nonranked_match_acquisition existing
  ON existing.match_id = discovery.match_id
WHERE discovery.queue_id <> 486
  AND existing.match_id IS NULL
ON CONFLICT (match_id) DO NOTHING;

UPDATE nonranked_match_acquisition acquisition
SET active_flag = discovery.active_flag,
    status = CASE
      WHEN discovery.active_flag
           AND acquisition.status IN ('discovered', 'fetching', 'service_deferred')
        THEN 'waiting_for_completion'
      ELSE acquisition.status
    END,
    lease_until = CASE
      WHEN discovery.active_flag
           AND acquisition.status IN ('discovered', 'fetching', 'service_deferred')
        THEN NULL
      ELSE acquisition.lease_until
    END,
    updated_at = now()
FROM match_count_discoveries discovery
WHERE discovery.match_id = acquisition.match_id
  AND discovery.queue_id = acquisition.queue_id;

-- The old worker attempted active matches immediately and made the absence of
-- post-match details terminal. Revive only that known false-terminal class.
-- The new grace gate performs one attempt after the source bucket is old enough
-- to have completed.
UPDATE nonranked_match_acquisition acquisition
SET status = 'waiting_for_completion',
    quality = 'unknown',
    active_flag = TRUE,
    detail_attempts = 0,
    roster_attempts = 0,
    last_attempt_at = NULL,
    lease_until = NULL,
    completed_at = NULL,
    terminal_reason = NULL,
    error_message = NULL,
    updated_at = now()
FROM match_count_discoveries discovery
WHERE discovery.match_id = acquisition.match_id
  AND discovery.queue_id = acquisition.queue_id
  AND discovery.active_flag = TRUE
  AND acquisition.status = 'dropped'
  AND acquisition.terminal_reason IN (
    'detail_and_roster_unavailable',
    'partial_detail_roster_unavailable'
  );

DROP INDEX IF EXISTS idx_nrma_claim;
CREATE INDEX idx_nrma_claim
  ON nonranked_match_acquisition (
    status,
    lease_until,
    source_date,
    source_hour,
    match_id
  )
  WHERE status IN (
    'discovered', 'waiting_for_completion', 'fetching', 'service_deferred'
  );

COMMENT ON COLUMN nonranked_match_acquisition.active_flag IS
  'True when getmatchidsbyqueue observed the match in progress; acquisition waits for the completion grace before spending its single terminal lookup.';
