-- paladinscat:requires-full-backup
-- Repair the legacy ingest shape where Team1Score/Team2Score were retained in
-- the opposite order from the winning task force derived from player results.
--
-- This deliberately does not infer winners from objective time, kills, damage,
-- or other gameplay statistics. It updates only strict score reversals backed
-- by at least two unanimous Win/Loss player observations and leaves null/tied
-- or otherwise ambiguous historical results untouched.

WITH player_outcome_observations AS (
  SELECT
    match_id,
    entry_datetime,
    CASE
      WHEN lower(COALESCE(win_status, '')) IN ('winner', 'win')
        THEN task_force
      WHEN lower(COALESCE(win_status, '')) IN ('loser', 'loss') AND task_force = 1
        THEN 2
      WHEN lower(COALESCE(win_status, '')) IN ('loser', 'loss') AND task_force = 2
        THEN 1
      ELSE NULL
    END AS observed_winner
  FROM match_players
), player_outcome_consensus AS (
  SELECT
    match_id,
    entry_datetime,
    MIN(observed_winner) AS winning_task_force,
    MAX(observed_winner) AS winning_task_force_max,
    COUNT(observed_winner) AS observations
  FROM player_outcome_observations
  GROUP BY match_id, entry_datetime
), reversible_scores AS (
  SELECT m.match_id, m.entry_datetime
  FROM matches AS m
  JOIN player_outcome_consensus AS outcomes
    ON outcomes.match_id = m.match_id
   AND outcomes.entry_datetime = m.entry_datetime
  WHERE m.queue_id IN (424, 486)
    AND outcomes.observations >= 2
    AND outcomes.winning_task_force = outcomes.winning_task_force_max
    AND outcomes.winning_task_force = m.winning_task_force
    AND m.team1_score IS NOT NULL
    AND m.team2_score IS NOT NULL
    AND m.team1_score >= 0
    AND m.team2_score >= 0
    AND (
      (m.winning_task_force = 1 AND m.team1_score < m.team2_score)
      OR (m.winning_task_force = 2 AND m.team2_score < m.team1_score)
    )
)
UPDATE matches AS m
SET
  team1_score = m.team2_score,
  team2_score = m.team1_score
FROM reversible_scores AS repair
WHERE m.match_id = repair.match_id
  AND m.entry_datetime = repair.entry_datetime;
