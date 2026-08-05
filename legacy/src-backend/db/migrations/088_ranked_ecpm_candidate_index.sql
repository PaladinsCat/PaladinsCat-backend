-- Keep bracketed eCPM candidate pages on a bounded, ordered index scan as the
-- match_players hypertable grows. The index contains only authoritative ranked
-- observations that can appear in the review queue.
CREATE INDEX IF NOT EXISTS idx_mp_ranked_ecpm_candidates
  ON match_players (entry_datetime DESC, match_id DESC, player_id DESC)
  INCLUDE (egpm, champion_id, win_status, task_force, source)
  WHERE is_ranked = true
    AND player_id > 0
    AND champion_id > 0
    AND egpm >= 0
    AND egpm < 120
    AND COALESCE(source, 'direct') IN ('direct', 'recovered');

ANALYZE match_players;
