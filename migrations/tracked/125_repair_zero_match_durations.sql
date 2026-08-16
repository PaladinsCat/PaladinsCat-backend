-- Repair legacy matches whose canonical duration was stored as zero even
-- though every persisted participant reports the same positive duration.
-- The unanimity guard prevents guessing from incomplete or conflicting facts.

WITH duration_evidence AS (
  SELECT
    match_id,
    entry_datetime,
    MAX(time_in_match)::INTEGER AS duration_seconds
  FROM match_players
  GROUP BY match_id, entry_datetime
  HAVING COUNT(*) = COUNT(*) FILTER (WHERE time_in_match > 0)
     AND MIN(time_in_match) = MAX(time_in_match)
)
UPDATE matches match
SET duration_seconds = evidence.duration_seconds
FROM duration_evidence evidence
WHERE match.match_id = evidence.match_id
  AND match.entry_datetime = evidence.entry_datetime
  AND COALESCE(match.duration_seconds, 0) <= 0;

COMMENT ON COLUMN matches.duration_seconds IS
  'Canonical gameplay duration. Legacy zero values are repaired only from unanimous positive participant evidence.';
