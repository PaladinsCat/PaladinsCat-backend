-- ============================================================================
-- PaladinsCat - Extended Schema (PostgreSQL 18 + TimescaleDB)
-- ============================================================================
-- Genuinely new content not covered by 001_schema.sql:
--   - player_history_retention_audit table (was 029)
--   - dropped_matches table (was 030)
--   - changelog column on stack_versions (was 037)
--   - tier stats functions + trigger (was 032_player_tier_profile_stats)
-- Idempotent: safe to run multiple times.
-- ============================================================================

-- ============================================================================
-- 1. player_history_retention_audit
-- ============================================================================
-- Compact audit summaries for hourly cleanup of player_match_history_cache
-- and player_match_history_entries. Keeps recovery/debug evidence after raw
-- history payloads are pruned.

CREATE TABLE IF NOT EXISTS player_history_retention_audit (
    id BIGSERIAL PRIMARY KEY,
    reason TEXT NOT NULL,
    table_name TEXT NOT NULL,
    delete_class TEXT NOT NULL,
    deleted_count INT NOT NULL,
    retention_seconds INT NOT NULL,
    oldest_observed_at TIMESTAMPTZ,
    newest_observed_at TIMESTAMPTZ,
    oldest_expires_at TIMESTAMPTZ,
    newest_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_player_history_retention_audit_created
    ON player_history_retention_audit (created_at DESC);

COMMENT ON TABLE player_history_retention_audit IS
    'Compact audit summaries for hourly cleanup of player_match_history_cache and player_match_history_entries. Keeps recovery/debug evidence after raw history payloads are pruned.';

-- ============================================================================
-- 2. dropped_matches
-- ============================================================================
-- Operator-facing projection of unresolved/corrupt ranked match debt.
-- Source of truth remains hourly_ingest_match_debt and match_ingest_status.

CREATE TABLE IF NOT EXISTS dropped_matches (
    match_id BIGINT PRIMARY KEY,
    date DATE NOT NULL,
    hour INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
    queue_id INT NOT NULL DEFAULT 486,
    status VARCHAR(20) NOT NULL,
    drop_category VARCHAR(60) NOT NULL,
    reason TEXT,
    attempts INT NOT NULL DEFAULT 0,
    observed_players INT NOT NULL DEFAULT 0,
    ingest_status VARCHAR(20),
    ingest_error TEXT,
    raw_buffer_status VARCHAR(20),
    raw_buffer_error TEXT,
    first_seen_at TIMESTAMPTZ,
    last_attempt_at TIMESTAMPTZ,
    next_retry_at TIMESTAMPTZ,
    staged_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dropped_matches_window
    ON dropped_matches (date DESC, hour ASC, queue_id);

CREATE INDEX IF NOT EXISTS idx_dropped_matches_status
    ON dropped_matches (status, drop_category, next_retry_at);

COMMENT ON TABLE dropped_matches IS
    'Operator-facing projection of unresolved/corrupt ranked match debt. Source of truth remains hourly_ingest_match_debt and match_ingest_status.';

-- ============================================================================
-- 3. Community player votes
-- ============================================================================
-- Positive/negative community labels are intentionally separate from
-- moderation. A user can cast one vote of each type for a player; the denormal-
-- ized counters make the player directory fast without losing the audit trail.

ALTER TABLE players
    ADD COLUMN IF NOT EXISTS weirdo_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hall_of_fame_count INT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS player_community_votes (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    vote_type VARCHAR(20) NOT NULL CHECK (vote_type IN ('suspicious', 'weirdo', 'hall_of_fame', 'cheater', 'dropper', 'afk_wintrade', 'alt_account')),
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_player_community_vote UNIQUE (player_id, user_id, vote_type)
);

CREATE INDEX IF NOT EXISTS idx_player_community_votes_type_created
    ON player_community_votes (vote_type, created_at DESC);

COMMENT ON TABLE player_community_votes IS
    'One player report or community vote per user/player/type; reason-free vote types store an empty reason.';
COMMENT ON COLUMN players.weirdo_count IS 'Community Weirdo votes accepted for this player.';
COMMENT ON COLUMN players.hall_of_fame_count IS 'Community Hall of Fame thumbs-up votes accepted for this player.';

CREATE TABLE IF NOT EXISTS player_alt_account_votes (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    main_player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    alt_player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT player_alt_account_votes_distinct_players CHECK (main_player_id <> alt_player_id),
    CONSTRAINT player_alt_account_votes_direction_unique UNIQUE (user_id, main_player_id, alt_player_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_player_alt_account_votes_user_pair
    ON player_alt_account_votes (user_id, LEAST(main_player_id, alt_player_id), GREATEST(main_player_id, alt_player_id));
CREATE INDEX IF NOT EXISTS idx_player_alt_account_votes_main
    ON player_alt_account_votes (main_player_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_player_alt_account_votes_alt
    ON player_alt_account_votes (alt_player_id, updated_at DESC);

COMMENT ON TABLE player_alt_account_votes IS
    'Directional, reason-free community votes linking one main Paladins account to one alternate account; each site user owns one vote per unordered player pair.';

-- ============================================================================
-- 4. stack_versions.changelog column
-- ============================================================================
-- Freeform changelog text for each deployment, populated from commit messages
-- by the deploy script.

ALTER TABLE stack_versions
    ADD COLUMN IF NOT EXISTS changelog TEXT;

COMMENT ON COLUMN stack_versions.changelog IS
    'Freeform changelog text for this deployment. Populated from commit messages by the deploy script.';

-- ============================================================================
-- 5. Tier stats functions + trigger
-- ============================================================================
-- Keep player-profile tier distribution cheap and current without reading the
-- full players table for every /stats page request.

CREATE OR REPLACE FUNCTION clamp_tier_stats_bucket(value INTEGER)
RETURNS INTEGER
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT CASE WHEN COALESCE(value, 0) BETWEEN 1 AND 26 THEN value ELSE 0 END
$$;

CREATE OR REPLACE FUNCTION bump_profile_tier_stats_bucket(bucket_value INTEGER, delta_value INTEGER)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    bucket INTEGER := clamp_tier_stats_bucket(bucket_value);
BEGIN
    INSERT INTO tier_stats (source)
    VALUES ('profiles')
    ON CONFLICT (source) DO NOTHING;

    EXECUTE format(
        'UPDATE tier_stats
         SET tier_%s = GREATEST(0, COALESCE(tier_%s, 0) + $1),
             updated_at = now()
         WHERE source = ''profiles''',
        bucket,
        bucket
    )
    USING delta_value;
END;
$$;

CREATE OR REPLACE FUNCTION sync_profile_tier_stats_from_players()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    old_bucket INTEGER;
    new_bucket INTEGER;
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM bump_profile_tier_stats_bucket(NEW.kbm_tier, 1);
        RETURN NEW;
    END IF;

    IF TG_OP = 'DELETE' THEN
        PERFORM bump_profile_tier_stats_bucket(OLD.kbm_tier, -1);
        RETURN OLD;
    END IF;

    old_bucket := clamp_tier_stats_bucket(OLD.kbm_tier);
    new_bucket := clamp_tier_stats_bucket(NEW.kbm_tier);

    IF old_bucket <> new_bucket THEN
        PERFORM bump_profile_tier_stats_bucket(old_bucket, -1);
        PERFORM bump_profile_tier_stats_bucket(new_bucket, 1);
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_profile_tier_stats ON players;
CREATE TRIGGER trg_sync_profile_tier_stats
    AFTER INSERT OR UPDATE OF kbm_tier OR DELETE ON players
    FOR EACH ROW
    EXECUTE FUNCTION sync_profile_tier_stats_from_players();
