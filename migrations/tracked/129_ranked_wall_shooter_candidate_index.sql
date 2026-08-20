-- Supports the ranked-only Wall Shooter directory. The route first finds a
-- Damage player in a match, then checks teammates in the same task force.
CREATE INDEX IF NOT EXISTS idx_mp_ranked_wall_shooter_team
  ON match_players (match_id, entry_datetime, task_force)
  INCLUDE (player_id, champion_id, damage_done_physical, win_status, source)
  WHERE player_id > 0
    AND champion_id > 0
    AND task_force IN (1, 2)
    AND COALESCE(source, 'direct') IN ('direct', 'recovered');

ANALYZE match_players;
