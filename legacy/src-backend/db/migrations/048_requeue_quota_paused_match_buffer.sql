-- Requeue recent partial match payloads that exhausted their local retry count
-- while every Hi-Rez key was limited. The backend now leaves these rows pending
-- without incrementing retry_count until API headroom returns.
UPDATE raw_ingest_buffer rib
SET status = 'pending',
    retry_count = 0,
    error_message = 'quota pause: recovery-required match retained pending',
    processed_at = NULL
WHERE rib.status = 'failed'
  AND rib.entity_type = 'match'
  AND rib.endpoint = 'getmatchdetailsbatch'
  AND rib.created_at >= now() - interval '6 hours'
  AND jsonb_typeof(rib.raw_data) = 'array'
  AND (
    EXISTS (
      SELECT 1
      FROM jsonb_array_elements(rib.raw_data) player
      WHERE btrim(COALESCE(player->>'ret_msg', '')) <> ''
    )
    OR (
      SELECT COUNT(*)
      FROM jsonb_array_elements(rib.raw_data) player
      WHERE btrim(COALESCE(player->>'ret_msg', '')) = ''
    ) < 10
  )
  AND EXISTS (
    SELECT 1
    FROM hourly_ingest_match_debt debt
    WHERE debt.match_id::text = rib.entity_id
      AND debt.status IN ('pending', 'staged')
  );
