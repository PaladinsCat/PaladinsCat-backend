-- paladinscat:requires-full-backup
-- Backfill only Siege matches whose exact completed result can be recovered
-- from unanimous retained getmatchhistory observations.
--
-- This migration deliberately leaves every other historical score untouched.
-- Live recovery now emits 4-NULL/NULL-4 when only the winner is known, but old
-- rows without an exact retained history result do not need a speculative fix.

WITH history_consensus AS (
  SELECT
    match_id,
    MIN(NULLIF(raw_data->>'Team1Score', '')::int) AS team1_score,
    MAX(NULLIF(raw_data->>'Team1Score', '')::int) AS team1_score_max,
    MIN(NULLIF(raw_data->>'Team2Score', '')::int) AS team2_score,
    MAX(NULLIF(raw_data->>'Team2Score', '')::int) AS team2_score_max,
    MIN(NULLIF(raw_data->>'Winning_TaskForce', '')::int) AS winning_task_force,
    MAX(NULLIF(raw_data->>'Winning_TaskForce', '')::int) AS winning_task_force_max
  FROM player_match_history_entries
  WHERE raw_data ? 'Team1Score'
    AND raw_data ? 'Team2Score'
    AND raw_data ? 'Winning_TaskForce'
  GROUP BY match_id
), exact_history_scores AS (
  SELECT match_id, team1_score, team2_score, winning_task_force
  FROM history_consensus
  WHERE team1_score = team1_score_max
    AND team2_score = team2_score_max
    AND winning_task_force = winning_task_force_max
    AND (
      (winning_task_force = 1 AND team1_score = 4 AND team2_score BETWEEN 0 AND 3)
      OR (winning_task_force = 2 AND team2_score = 4 AND team1_score BETWEEN 0 AND 3)
    )
)
UPDATE matches AS m
SET
  team1_score = history.team1_score,
  team2_score = history.team2_score,
  winning_task_force = history.winning_task_force
FROM exact_history_scores AS history
WHERE m.match_id = history.match_id
  AND m.queue_id IN (424, 486)
  AND (
    m.broken
    OR m.recovered
    OR m.source = 'recovery'
    OR NOT COALESCE((
      (m.winning_task_force = 1 AND m.team1_score = 4 AND m.team2_score BETWEEN 0 AND 3)
      OR (m.winning_task_force = 2 AND m.team2_score = 4 AND m.team1_score BETWEEN 0 AND 3)
    ), FALSE)
  )
  AND (
    m.team1_score IS DISTINCT FROM history.team1_score
    OR m.team2_score IS DISTINCT FROM history.team2_score
    OR m.winning_task_force IS DISTINCT FROM history.winning_task_force
  );
