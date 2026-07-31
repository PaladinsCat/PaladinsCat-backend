-- The performance leaderboard ranks individual authoritative match rows. The
-- metric-leading indexes let PostgreSQL/TimescaleDB stop after the requested
-- top records instead of scanning every ranked participant on each request.

CREATE INDEX IF NOT EXISTS idx_mp_ranked_gpm_leaderboard
    ON match_players (gold_per_minute DESC, entry_datetime DESC)
    WHERE is_ranked = true
      AND player_id > 0
      AND champion_id > 0
      AND time_in_match > 120
      AND COALESCE(source, 'direct') IN ('direct', 'recovered');

CREATE INDEX IF NOT EXISTS idx_mp_ranked_dpm_leaderboard
    ON match_players (damage_per_minute DESC, entry_datetime DESC)
    WHERE is_ranked = true
      AND player_id > 0
      AND champion_id > 0
      AND time_in_match > 120
      AND COALESCE(source, 'direct') IN ('direct', 'recovered');

CREATE INDEX IF NOT EXISTS idx_mp_ranked_hpm_leaderboard
    ON match_players (healing_per_minute DESC, entry_datetime DESC)
    WHERE is_ranked = true
      AND player_id > 0
      AND champion_id > 0
      AND time_in_match > 120
      AND COALESCE(source, 'direct') IN ('direct', 'recovered');

CREATE INDEX IF NOT EXISTS idx_mp_ranked_mpm_leaderboard
    ON match_players (mitigation_per_minute DESC, entry_datetime DESC)
    WHERE is_ranked = true
      AND player_id > 0
      AND champion_id > 0
      AND time_in_match > 120
      AND COALESCE(source, 'direct') IN ('direct', 'recovered');

COMMENT ON INDEX idx_mp_ranked_gpm_leaderboard IS 'Top authoritative ranked single-match credits-per-minute records.';
COMMENT ON INDEX idx_mp_ranked_dpm_leaderboard IS 'Top authoritative ranked single-match DPM records.';
COMMENT ON INDEX idx_mp_ranked_hpm_leaderboard IS 'Top authoritative ranked single-match HPM records.';
COMMENT ON INDEX idx_mp_ranked_mpm_leaderboard IS 'Top authoritative ranked single-match mitigation-per-minute records.';
