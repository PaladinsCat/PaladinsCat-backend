-- Presence acquisition is single-pass. Close legacy deferred/interrupted rows
-- instead of recalling vendor endpoints after deployment.
UPDATE nonranked_match_acquisition
SET status = 'dropped',
    quality = 'unavailable',
    lease_until = NULL,
    terminal_reason = COALESCE(
      terminal_reason,
      'legacy_retry_terminalized_by_single_pass_policy'
    ),
    error_message = COALESCE(error_message, 'Legacy retry disabled'),
    completed_at = COALESCE(completed_at, now()),
    updated_at = now()
WHERE status IN ('service_deferred', 'fetching');
