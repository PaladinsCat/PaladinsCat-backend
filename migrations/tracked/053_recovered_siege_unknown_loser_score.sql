-- Repair recent broken-skin recoveries that were stored before unknown scores
-- were represented as NULL. The source/recovery predicates avoid changing a
-- legitimate 0-1 result from a different queue or direct payload.
UPDATE matches AS m
SET
  team1_score = CASE WHEN m.winning_task_force = 1 THEN 4 ELSE NULL END,
  team2_score = CASE WHEN m.winning_task_force = 2 THEN 4 ELSE NULL END,
  recovered = TRUE,
  broken = TRUE,
  source = 'recovery',
  ingested_at = now()
-- Keep within the uncompressed retention window; older rows are left intact
-- rather than forcing a broad rewrite of archived match chunks.
WHERE m.entry_datetime >= now() - INTERVAL '6 days'
  AND m.queue_id IN (424, 486)
  AND m.winning_task_force IN (1, 2)
  AND ((m.team1_score = 0 AND m.team2_score = 1)
    OR (m.team1_score = 1 AND m.team2_score = 0))
  AND (
    m.recovered = TRUE
    OR m.broken = TRUE
    OR EXISTS (
      SELECT 1
      FROM match_players AS mp
      WHERE mp.match_id = m.match_id
        AND mp.source = 'recovered'
    )
  );
