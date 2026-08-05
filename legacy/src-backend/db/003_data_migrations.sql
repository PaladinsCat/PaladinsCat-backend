-- ============================================================================
-- PaladinsCat - Data Migrations (PostgreSQL 18)
-- ============================================================================
-- Ordered, idempotent data backfills.
-- Must run AFTER 001_schema.sql and 002_extended_schema.sql.
-- Safe to re-run: all updates use conditional WHERE clauses.
-- ============================================================================

-- ============================================================================
-- 1. Tier backfill from match_players
-- ============================================================================
-- Seed kbm_tier from the latest known ranked match fact only when the current
-- player profile tier is 0/unknown.

WITH latest_known_tier AS (
    SELECT DISTINCT ON (mp.player_id)
        mp.player_id,
        mp.league_tier,
        mp.league_points,
        m.entry_datetime
    FROM match_players mp
    JOIN matches m ON m.match_id = mp.match_id
    WHERE mp.player_id > 0
        AND mp.league_tier BETWEEN 1 AND 26
        AND m.queue_id = 486
    ORDER BY mp.player_id, m.entry_datetime DESC
)
UPDATE players p
SET
    kbm_tier = lkt.league_tier,
    kbm_points = CASE
        WHEN COALESCE(p.kbm_points, 0) = 0 THEN COALESCE(lkt.league_points, 0)
        ELSE p.kbm_points
    END,
    last_updated = now()
FROM latest_known_tier lkt
WHERE p.id = lkt.player_id
    AND COALESCE(p.kbm_tier, 0) = 0;

-- ============================================================================
-- 3. Tier stats rebuild (profiles source)
-- ============================================================================
-- Rebuild tier_stats(source='profiles') from players as the exact baseline.

WITH counts AS (
    SELECT
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 0)::INT AS tier_0,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 1)::INT AS tier_1,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 2)::INT AS tier_2,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 3)::INT AS tier_3,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 4)::INT AS tier_4,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 5)::INT AS tier_5,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 6)::INT AS tier_6,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 7)::INT AS tier_7,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 8)::INT AS tier_8,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 9)::INT AS tier_9,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 10)::INT AS tier_10,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 11)::INT AS tier_11,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 12)::INT AS tier_12,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 13)::INT AS tier_13,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 14)::INT AS tier_14,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 15)::INT AS tier_15,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 16)::INT AS tier_16,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 17)::INT AS tier_17,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 18)::INT AS tier_18,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 19)::INT AS tier_19,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 20)::INT AS tier_20,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 21)::INT AS tier_21,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 22)::INT AS tier_22,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 23)::INT AS tier_23,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 24)::INT AS tier_24,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 25)::INT AS tier_25,
        COUNT(*) FILTER (WHERE clamp_tier_stats_bucket(kbm_tier) = 26)::INT AS tier_26
    FROM players
)
INSERT INTO tier_stats (
    source, tier_0, tier_1, tier_2, tier_3, tier_4, tier_5, tier_6, tier_7,
    tier_8, tier_9, tier_10, tier_11, tier_12, tier_13, tier_14, tier_15,
    tier_16, tier_17, tier_18, tier_19, tier_20, tier_21, tier_22, tier_23,
    tier_24, tier_25, tier_26, updated_at
)
SELECT
    'profiles', tier_0, tier_1, tier_2, tier_3, tier_4, tier_5, tier_6, tier_7,
    tier_8, tier_9, tier_10, tier_11, tier_12, tier_13, tier_14, tier_15,
    tier_16, tier_17, tier_18, tier_19, tier_20, tier_21, tier_22, tier_23,
    tier_24, tier_25, tier_26, now()
FROM counts
ON CONFLICT (source) DO UPDATE SET
    tier_0 = EXCLUDED.tier_0,
    tier_1 = EXCLUDED.tier_1,
    tier_2 = EXCLUDED.tier_2,
    tier_3 = EXCLUDED.tier_3,
    tier_4 = EXCLUDED.tier_4,
    tier_5 = EXCLUDED.tier_5,
    tier_6 = EXCLUDED.tier_6,
    tier_7 = EXCLUDED.tier_7,
    tier_8 = EXCLUDED.tier_8,
    tier_9 = EXCLUDED.tier_9,
    tier_10 = EXCLUDED.tier_10,
    tier_11 = EXCLUDED.tier_11,
    tier_12 = EXCLUDED.tier_12,
    tier_13 = EXCLUDED.tier_13,
    tier_14 = EXCLUDED.tier_14,
    tier_15 = EXCLUDED.tier_15,
    tier_16 = EXCLUDED.tier_16,
    tier_17 = EXCLUDED.tier_17,
    tier_18 = EXCLUDED.tier_18,
    tier_19 = EXCLUDED.tier_19,
    tier_20 = EXCLUDED.tier_20,
    tier_21 = EXCLUDED.tier_21,
    tier_22 = EXCLUDED.tier_22,
    tier_23 = EXCLUDED.tier_23,
    tier_24 = EXCLUDED.tier_24,
    tier_25 = EXCLUDED.tier_25,
    tier_26 = EXCLUDED.tier_26,
    updated_at = now();

-- ============================================================================
-- 4. Changelog backfill
-- ============================================================================
-- For existing stack_versions rows, set changelog from notes if available.

UPDATE stack_versions
SET changelog = notes
WHERE changelog IS NULL AND notes IS NOT NULL AND notes <> '';

-- ============================================================================
-- 5. Update notification message (remove v1.0 version reference)
-- ============================================================================
-- The original notification message included a version number that's now
-- handled by the version tracking system. Update to generic message.

UPDATE notifications
SET message = 'PaladinsCat - Open Beta. WORK IN PROGRESS. EXPECT BROKEN FEATURES.'
WHERE id = 1 AND message LIKE 'PaladinsCat v%';
