-- ============================================================================
-- PaladinsCat - Consolidated Schema (PostgreSQL 18 + TimescaleDB 2.26)
-- ============================================================================
-- Merged from: 001-016 + apply_007 migrations
-- Idempotent: safe to run multiple times (IF NOT EXISTS throughout).
-- Encoding: UTF-8. Identifiers: snake_case.
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================================
-- 1. REFERENCE TABLES - Static data
-- ============================================================================

-- champions - Static champion roster (46 rows, changes only on patch)
CREATE TABLE IF NOT EXISTS champions (
    id                          INT PRIMARY KEY,
    name                        VARCHAR(100) NOT NULL,
    title                       VARCHAR(200),
    health                      INT NOT NULL,
    speed                       INT NOT NULL,
    roles                       VARCHAR(100),
    ability1_id                 INT,
    ability1_name               VARCHAR(100),
    ability1_type               VARCHAR(100),
    ability1_description        TEXT,
    ability2_id                 INT,
    ability2_name               VARCHAR(100),
    ability2_type               VARCHAR(100),
    ability2_description        TEXT,
    ability3_id                 INT,
    ability3_name               VARCHAR(100),
    ability3_type               VARCHAR(100),
    ability3_description        TEXT,
    ability4_id                 INT,
    ability4_name               VARCHAR(100),
    ability4_type               VARCHAR(100),
    ability4_description        TEXT,
    ability5_id                 INT,
    ability5_name               VARCHAR(100),
    ability5_type               VARCHAR(100),
    ability5_description        TEXT
);
COMMENT ON TABLE champions IS 'Static champion roster - 46 champions, refreshed per patch';
CREATE INDEX IF NOT EXISTS idx_champions_name ON champions (name);

-- items - All in-game items (872 rows)
CREATE TABLE IF NOT EXISTS items (
    item_id             INT PRIMARY KEY,
    item_name           VARCHAR(200),
    description         TEXT,
    item_type           VARCHAR(100),
    cost                INT,
    icon_url            VARCHAR(500),
    champion_id         INT REFERENCES champions(id),
    recharge_seconds    INT,
    talent_reward_level INT
);
COMMENT ON TABLE items IS 'All in-game items: vendor items, burn cards, champion cards - 872 rows';
COMMENT ON COLUMN items.champion_id IS 'NULL for universal items; FK to champions for champion-specific cards';
CREATE INDEX IF NOT EXISTS idx_items_item_name ON items (item_name);
CREATE INDEX IF NOT EXISTS idx_items_champion_id ON items (champion_id) WHERE champion_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_items_item_type ON items (item_type);

-- bounty_items - Time-limited sale events
CREATE TABLE IF NOT EXISTS bounty_items (
    bounty_item_id      BIGINT PRIMARY KEY,
    item_id             INT,
    item_name           VARCHAR(200) NOT NULL,
    champion_id         INT REFERENCES champions(id),
    champion_name       VARCHAR(100),
    initial_price       INT,
    final_price         INT,
    sale_type           VARCHAR,
    sale_end_datetime   TIMESTAMPTZ,
    is_active           BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE bounty_items IS 'Time-limited bounty shop sale events';
CREATE INDEX IF NOT EXISTS idx_bounty_items_champion_id ON bounty_items (champion_id);
CREATE INDEX IF NOT EXISTS idx_bounty_items_is_active ON bounty_items (is_active);
CREATE INDEX IF NOT EXISTS idx_bounty_items_sale_end ON bounty_items (sale_end_datetime);

-- maps - Static map roster
CREATE TABLE IF NOT EXISTS maps (
    map_id              INT PRIMARY KEY,
    name                VARCHAR(200) NOT NULL,
    map_type            VARCHAR(50),
    queue_ids           INT[],
    is_ranked           BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_maps_name ON maps (name);
CREATE INDEX IF NOT EXISTS idx_maps_is_ranked ON maps (is_ranked);
COMMENT ON TABLE maps IS 'Static map roster - ~15-20 maps, refreshed per patch';

-- ranked_tiers - 27 rank tiers
CREATE TABLE IF NOT EXISTS ranked_tiers (
    tier_id             INT PRIMARY KEY,
    tier_name           VARCHAR(50) NOT NULL
);
COMMENT ON TABLE ranked_tiers IS '27 compact rank tiers matching API League_Tier field (1=Bronze V ... 27=Grandmaster)';

-- regions - Server region reference
CREATE TABLE IF NOT EXISTS regions (
    region_code         VARCHAR(50) PRIMARY KEY,
    region_name         VARCHAR(100) NOT NULL,
    continent           VARCHAR(50),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE regions IS 'Server region reference - NA, EU, ASIA, OCE, etc.';

-- talents - Champion talent tree data
CREATE TABLE IF NOT EXISTS talents (
    talent_id           INT PRIMARY KEY,
    talent_name         VARCHAR(200) NOT NULL,
    champion_id         INT REFERENCES champions(id)
);
CREATE INDEX IF NOT EXISTS idx_talents_champion_id ON talents (champion_id);
COMMENT ON TABLE talents IS 'Champion talent tree - ~46 champions × ~15 talents each';

-- queue_types - Queue reference
CREATE TABLE IF NOT EXISTS queue_types (
    queue_id            INT PRIMARY KEY,
    queue_name          VARCHAR(50) NOT NULL,
    is_ranked           BOOLEAN DEFAULT FALSE,
    stats_scope         VARCHAR(32) NOT NULL DEFAULT 'other' CHECK (
      stats_scope IN ('ranked', 'casual', 'bot', 'team_deathmatch', 'arcade', 'wave_defense', 'experiment', 'newcomer', 'custom', 'other')
    ),
    participant_model   VARCHAR(16) NOT NULL DEFAULT 'unknown' CHECK (
      participant_model IN ('pvp', 'pve', 'bots', 'custom', 'unknown')
    ),
    stats_enabled       BOOLEAN NOT NULL DEFAULT FALSE,
    track_presence      BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO queue_types (
    queue_id, queue_name, is_ranked, stats_scope, participant_model, stats_enabled, track_presence
) VALUES
    (0, 'Unknown', false, 'other', 'unknown', false, false),
    (424, 'Casual Siege', false, 'casual', 'pvp', true, true),
    (425, 'Siege Training', false, 'bot', 'bots', true, true),
    (452, 'Casual Onslaught', false, 'casual', 'pvp', true, true),
    (453, 'Onslaught Training', false, 'bot', 'bots', true, true),
    (469, 'Team Deathmatch', false, 'team_deathmatch', 'pvp', true, true),
    (486, 'Ranked Siege', true, 'ranked', 'pvp', true, true),
    (10297, 'Team Deathmatch Training', false, 'bot', 'bots', true, true),
    (10332, 'Arcade', false, 'arcade', 'pvp', true, true),
    (10348, 'Wave Defense Party Beta', false, 'wave_defense', 'pve', true, true),
    (10362, 'Wave Defense Public Beta', false, 'wave_defense', 'pve', true, true),
    (10367, 'Newcomer', false, 'newcomer', 'pvp', true, true),
    (10369, 'Experiment: Subclasses', false, 'experiment', 'pvp', true, true)
ON CONFLICT (queue_id) DO NOTHING;
COMMENT ON TABLE queue_types IS 'Queue taxonomy shared by discovery, ingestion, public statistics, and rolling presence.';

-- patches - Game patch versions
CREATE TABLE IF NOT EXISTS patches (
    id              SERIAL PRIMARY KEY,
    version         VARCHAR(20) NOT NULL,
    release_date    DATE NOT NULL,
    description     TEXT,
    is_current      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE patches IS 'Game patch version history - is_current marks the active patch';
CREATE INDEX IF NOT EXISTS idx_patches_version ON patches (version);
CREATE INDEX IF NOT EXISTS idx_patches_release_date ON patches (release_date DESC);

-- ============================================================================
-- 2. PLAYERS - Player profiles
-- ============================================================================

CREATE TABLE IF NOT EXISTS players (
    id                      BIGINT PRIMARY KEY,
    active_player_id        BIGINT,
    name                    VARCHAR(100) NOT NULL,
    platform_name           VARCHAR(200),
    hz_player_name          VARCHAR(100),
    hz_gamer_tag            VARCHAR(100),
    name_source             VARCHAR(30) NOT NULL DEFAULT 'unknown',
    name_anomaly            BOOLEAN NOT NULL DEFAULT FALSE,
    name_anomaly_reason     TEXT,
    name_anomaly_detected_at TIMESTAMPTZ,
    level                   INT DEFAULT 0,
    api_level               INT NOT NULL DEFAULT 0,
    wins                    INT DEFAULT 0,
    losses                  INT DEFAULT 0,
    hours_played            INT DEFAULT 0,
    minutes_played          INT DEFAULT 0,
    mastery_level           INT DEFAULT 0,
    region                  VARCHAR(50),
    platform                VARCHAR(20),
    ret_msg                 TEXT,
    total_xp                BIGINT DEFAULT 0,
    total_worshippers       BIGINT DEFAULT 0,
    total_achievements      INT DEFAULT 0,
    avatar_id               INT,
    avatar_url              VARCHAR(500),
    title                   VARCHAR(200),
    loading_frame           VARCHAR(200),
    created_datetime        TIMESTAMPTZ,
    last_login_datetime     TIMESTAMPTZ,
    personal_status_message VARCHAR(500),
    team_id                 INT DEFAULT 0,
    team_name               VARCHAR(200),
    leaves                  INT DEFAULT 0,
    merged_players          TEXT[],
    privacy_flag            VARCHAR(1) DEFAULT 'n',
    kbm_name                VARCHAR(100),
    kbm_points              INT DEFAULT 0,
    kbm_tier                INT DEFAULT 0,
    kbm_season              INT DEFAULT 0,
    kbm_wins                INT DEFAULT 0,
    kbm_losses              INT DEFAULT 0,
    kbm_rank                INT DEFAULT 0,
    kbm_leaves              INT DEFAULT 0,
    kbm_trend               INT DEFAULT 0,
    kbm_prev_rank           INT DEFAULT 0,
    kbm_player_id           BIGINT,
    kbm_ret_msg             TEXT,
    controller_name         VARCHAR(100),
    controller_points       INT DEFAULT 0,
    controller_tier         INT DEFAULT 0,
    controller_rank         INT DEFAULT 0,
    controller_wins         INT DEFAULT 0,
    controller_losses       INT DEFAULT 0,
    controller_leaves       INT DEFAULT 0,
    controller_trend        INT DEFAULT 0,
    controller_prev_rank    INT DEFAULT 0,
    controller_season       INT DEFAULT 0,
    controller_player_id    BIGINT,
    controller_ret_msg      TEXT,
    conquest_name           VARCHAR(100),
    conquest_points         INT DEFAULT 0,
    conquest_tier           INT DEFAULT 0,
    conquest_rank           INT DEFAULT 0,
    conquest_wins           INT DEFAULT 0,
    conquest_losses         INT DEFAULT 0,
    conquest_leaves         INT DEFAULT 0,
    conquest_trend          INT DEFAULT 0,
    conquest_prev_rank      INT DEFAULT 0,
    conquest_season         INT DEFAULT 0,
    conquest_player_id      BIGINT,
    conquest_ret_msg        TEXT,
    hirez_profile_refreshed_at TIMESTAMPTZ,
    last_updated            TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_seen              TIMESTAMPTZ NOT NULL DEFAULT now(),
    portal_id               SMALLINT,
    portal_user_id          VARCHAR(30),
    total_matches           INT DEFAULT 0,
    total_wins              INT DEFAULT 0,
    total_losses            INT DEFAULT 0,
    last_seen               TIMESTAMPTZ NOT NULL DEFAULT now(),
    avg_egpm                DOUBLE PRECISION,
    avg_dpm                 DOUBLE PRECISION,
    avg_hpm                 DOUBLE PRECISION,
    avg_shpm                DOUBLE PRECISION,
    avg_mpm                 DOUBLE PRECISION,
    cheater                 BOOLEAN NOT NULL DEFAULT FALSE,
    sus_count               INT NOT NULL DEFAULT 0,
    dropper                 BOOLEAN NOT NULL DEFAULT FALSE,
    afk_wintrade            BOOLEAN NOT NULL DEFAULT FALSE,
    alt_account             BOOLEAN NOT NULL DEFAULT FALSE
);
COMMENT ON TABLE players IS 'Player profiles - upserted on each refresh, keyed by API player Id';
COMMENT ON COLUMN players.active_player_id IS 'Raw Hi-Rez ActivePlayerId from getplayer/getplayerbatch; usually matches Id but retained separately for account-merge/debug cases.';
COMMENT ON COLUMN players.name IS 'Canonical public display name. Profile endpoints prefer hz_player_name > hz_gamer_tag > non-synthetic Name; match detail playerName can repair profile-only fallbacks.';
COMMENT ON COLUMN players.platform_name IS 'Raw Hi-Rez profile Name field. For Epic accounts this can be an obfuscated platform identity such as <hex>User-<hex>; retained for audit, not public display.';
COMMENT ON COLUMN players.hz_player_name IS 'Raw Hi-Rez profile hz_player_name field, preferred public profile display name when present.';
COMMENT ON COLUMN players.hz_gamer_tag IS 'Raw Hi-Rez profile hz_gamer_tag field, fallback public profile display name when hz_player_name is absent.';
COMMENT ON COLUMN players.name_source IS 'Source used for current canonical name: match_player, hz_player_name, hz_gamer_tag, name, none, unknown, or repair labels.';
COMMENT ON COLUMN players.name_anomaly IS 'TRUE when the latest profile normalization detected a suspicious raw profile Name value.';
COMMENT ON COLUMN players.name_anomaly_reason IS 'Human-readable reason for the profile name anomaly.';
COMMENT ON COLUMN players.name_anomaly_detected_at IS 'First time a suspicious profile Name was observed for this player.';
COMMENT ON COLUMN players.privacy_flag IS '''n'' = public profile, ''y'' = private';
COMMENT ON COLUMN players.merged_players IS 'Array of player IDs merged into this account';
COMMENT ON COLUMN players.ret_msg IS 'Raw Hi-Rez ret_msg from the latest profile response.';
COMMENT ON COLUMN players.kbm_player_id IS 'Raw RankedKBM.player_id from the latest Hi-Rez profile response.';
COMMENT ON COLUMN players.kbm_ret_msg IS 'Raw RankedKBM.ret_msg from the latest Hi-Rez profile response.';
COMMENT ON COLUMN players.controller_player_id IS 'Raw RankedController.player_id from the latest Hi-Rez profile response.';
COMMENT ON COLUMN players.controller_ret_msg IS 'Raw RankedController.ret_msg from the latest Hi-Rez profile response.';
COMMENT ON COLUMN players.conquest_player_id IS 'Raw RankedConquest.player_id from the latest Hi-Rez profile response.';
COMMENT ON COLUMN players.conquest_ret_msg IS 'Raw RankedConquest.ret_msg from the latest Hi-Rez profile response.';
COMMENT ON COLUMN players.hirez_profile_refreshed_at IS 'Timestamp when the Hi-Rez profile fields on this row were last refreshed. Profile TTL uses this, not last_updated, because derived stats also update last_updated.';
COMMENT ON COLUMN players.total_matches IS 'Denormalized: updated via triggers/batch jobs';
COMMENT ON COLUMN players.avg_egpm IS 'Rolling average eGPM from authoritative complete ranked match_players only; history observations/prefetch rows are excluded.';
COMMENT ON COLUMN players.avg_dpm IS 'Rolling average damage per minute from authoritative complete ranked match_players only; history observations/prefetch rows are excluded.';
COMMENT ON COLUMN players.avg_hpm IS 'Rolling average healing per minute from authoritative complete ranked match_players only; history observations/prefetch rows are excluded.';
COMMENT ON COLUMN players.avg_shpm IS 'Rolling average self-healing per minute from authoritative complete ranked match_players only; history observations/prefetch rows are excluded.';
COMMENT ON COLUMN players.avg_mpm IS 'Rolling average mitigation per minute from authoritative complete ranked match_players only; history observations/prefetch rows are excluded.';
COMMENT ON COLUMN players.cheater IS 'Manual/automated flag used by player search filters and moderation views.';
COMMENT ON COLUMN players.sus_count IS 'Suspicion counter used by drop-hack and moderation views.';
COMMENT ON COLUMN players.dropper IS 'Materialized community Dropper flag; true after at least one accepted vote.';
COMMENT ON COLUMN players.afk_wintrade IS 'Materialized community AFK / Wintrade flag; true after at least one accepted vote.';
COMMENT ON COLUMN players.alt_account IS 'Materialized community relationship flag; true while at least one directional vote identifies this player as an alternate account.';
CREATE INDEX IF NOT EXISTS idx_players_name ON players (name);
CREATE INDEX IF NOT EXISTS idx_players_name_anomaly ON players (name_anomaly, name_anomaly_detected_at DESC) WHERE name_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_players_hirez_profile_refreshed ON players (hirez_profile_refreshed_at);
CREATE INDEX IF NOT EXISTS idx_players_profile_backfill_priority
    ON players (hirez_profile_refreshed_at ASC NULLS FIRST, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_players_platform ON players (platform);
CREATE INDEX IF NOT EXISTS idx_players_region ON players (region);
CREATE INDEX IF NOT EXISTS idx_players_kbm_tier ON players (kbm_tier);
CREATE INDEX IF NOT EXISTS idx_players_portal ON players (portal_id, portal_user_id);
CREATE INDEX IF NOT EXISTS idx_players_dropper ON players (id) WHERE dropper = TRUE;
CREATE INDEX IF NOT EXISTS idx_players_afk_wintrade ON players (id) WHERE afk_wintrade = TRUE;
CREATE INDEX IF NOT EXISTS idx_players_alt_account ON players (id) WHERE alt_account = TRUE;

-- player_name_history - Track name changes
CREATE TABLE IF NOT EXISTS player_name_history (
    id            SERIAL PRIMARY KEY,
    player_id     BIGINT NOT NULL REFERENCES players(id),
    name          VARCHAR(100) NOT NULL,
    used_from     TIMESTAMPTZ NOT NULL DEFAULT now(),
    used_to       TIMESTAMPTZ
);
COMMENT ON TABLE player_name_history IS 'Player name change history - used_to IS NULL for current name';
CREATE INDEX IF NOT EXISTS idx_player_name_history_name ON player_name_history (name);
CREATE INDEX IF NOT EXISTS idx_player_name_history_player_id ON player_name_history (player_id);

-- player_profile_merged_players - typed copy of Hi-Rez MergedPlayers profile array
CREATE TABLE IF NOT EXISTS player_profile_merged_players (
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    merged_player_id BIGINT NOT NULL,
    portal_id INT,
    merge_datetime TIMESTAMPTZ,
    profile_refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, merged_player_id)
);
COMMENT ON TABLE player_profile_merged_players IS 'Typed child table for the repeatable Hi-Rez MergedPlayers profile array.';
CREATE INDEX IF NOT EXISTS idx_player_profile_merged_players_merged
    ON player_profile_merged_players (merged_player_id);

-- player_account_merges - Account merge ledger
CREATE TABLE IF NOT EXISTS player_account_merges (
    player_id           BIGINT NOT NULL REFERENCES players(id),
    merged_from_id      BIGINT NOT NULL,
    merged_from_portal  SMALLINT,
    merge_datetime      TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (player_id, merged_from_id)
);
CREATE INDEX IF NOT EXISTS idx_pam_from ON player_account_merges (merged_from_id);
COMMENT ON TABLE player_account_merges IS 'Account merge ledger - extracted from MergedPlayers arrays. Tracks cross-platform merges.';

-- ============================================================================
-- 3. MATCHES - Match records (HYPERTABLE)
-- ============================================================================

CREATE TABLE IF NOT EXISTS matches (
    -- Group 1: Identity
    match_id              BIGINT,
    entry_datetime        TIMESTAMPTZ NOT NULL,

    -- Group 2: Match Context
    queue_id              INT REFERENCES queue_types(queue_id),
    is_ranked             BOOLEAN DEFAULT FALSE,
    duration_seconds      INT,
    region                VARCHAR(50),
    map                   VARCHAR(200),

    -- Group 3: Score & Outcome
    team1_score           INT,
    team2_score           INT,
    winning_task_force    INT,
    has_replay            BOOLEAN DEFAULT FALSE,
    surrendered           BOOLEAN DEFAULT FALSE,

    -- Group 4: Health Flags
    broken                BOOLEAN DEFAULT FALSE,
    recovered             BOOLEAN DEFAULT FALSE,
    private               BOOLEAN DEFAULT FALSE,
    limited               BOOLEAN NOT NULL DEFAULT FALSE,
    limited_reason        TEXT,

    -- Group 5: Team Aggregates
    team1_total_gold      INT,
    team2_total_gold      INT,
    team1_total_damage    INT,
    team2_total_damage    INT,
    team1_total_healing   INT,
    team2_total_healing   INT,
    team1_avg_kills       NUMERIC,
    team2_avg_kills       NUMERIC,
    team1_avg_deaths      NUMERIC,
    team2_avg_deaths      NUMERIC,

    -- Group 6: Meta
    ingested_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    dev_id                VARCHAR(10),
    source                VARCHAR(20) DEFAULT 'direct',
    match_level           INT,

    PRIMARY KEY (match_id, entry_datetime)
);
COMMENT ON TABLE matches IS 'Match records - TimescaleDB hypertable partitioned by entry_datetime';
COMMENT ON COLUMN matches.queue_id IS '486=Ranked, 424=Siege, 452=Onslaught, 469=TDM';
COMMENT ON COLUMN matches.winning_task_force IS '1 or 2 - which task force won';
COMMENT ON COLUMN matches.broken IS 'TRUE if match has missing player data (Int16 overflow)';
COMMENT ON COLUMN matches.recovered IS 'TRUE only when a broken match was repaired to 10 detailed rows. FALSE for ordinary direct matches and for incomplete/private-placeholder recovery; player-row authority determines metric eligibility.';

-- Health flag rules:
--   10 direct players = broken=false, recovered=false (normal pipeline)
--   0-9 direct + 1-10 recovered = broken=true, recovered=true (quality data from both sources)
--   N detailed + (10-N) private placeholders = broken=true, recovered=false (valid partial-private match; placeholders excluded from metrics)
--   1-9 authoritative detail rows + one unavailable roster-anchor attempt = limited=true (lookup-only; no projections)
--   Any other minimal player or fewer than 10 logical roster rows = retryable/incomplete and must not complete ingest
COMMENT ON COLUMN matches.private IS 'TRUE if match contains PRIVATEACCOUNT players';
COMMENT ON COLUMN matches.limited IS 'TRUE when authoritative direct match rows were retained without a complete logical roster. Limited matches are lookup-only and excluded from aggregate/rating/stat projections.';
COMMENT ON COLUMN matches.limited_reason IS 'Stable machine-readable reason for lookup-only limited match quality.';
COMMENT ON COLUMN matches.dev_id IS 'devId of the API key used to fetch this match';
COMMENT ON COLUMN matches.source IS 'Ingestion source: direct (getmatchdetailsbatch), recovery (broken match recovery), or minimal (recovery without match history)';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'matches') THEN
        PERFORM create_hypertable('matches', time_column_name := 'entry_datetime', number_partitions := 4);
    END IF;
END
$$;

-- Idempotent compression setup for matches
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'matches'
        AND c.reloptions IS NOT NULL
    ) THEN
        ALTER TABLE matches SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'queue_id, region',
            timescaledb.compress_orderby = 'entry_datetime DESC'
        );
    END IF;
    PERFORM add_compression_policy('matches', INTERVAL '7 days', if_not_exists => TRUE);
END
$$;

CREATE INDEX IF NOT EXISTS idx_matches_entry_datetime ON matches (entry_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_matches_queue_id ON matches (queue_id);
CREATE INDEX IF NOT EXISTS idx_matches_region ON matches (region);
CREATE INDEX IF NOT EXISTS idx_matches_map ON matches (map);
CREATE INDEX IF NOT EXISTS idx_matches_is_ranked ON matches (is_ranked);
CREATE INDEX IF NOT EXISTS idx_matches_duration ON matches (duration_seconds);
CREATE INDEX IF NOT EXISTS idx_matches_recovered ON matches (recovered) WHERE recovered = TRUE;
CREATE INDEX IF NOT EXISTS idx_matches_dev_id ON matches (dev_id) WHERE dev_id IS NOT NULL;

-- ============================================================================
-- 3.1 RAW INGEST BUFFER - ELT staging table (dump raw JSON first, process later)
-- ============================================================================

CREATE TABLE IF NOT EXISTS raw_ingest_buffer (
    id                  BIGSERIAL PRIMARY KEY,
    raw_data            JSONB NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'processed', 'failed')),
    dev_id              VARCHAR(10),
    error_message       TEXT,
    retry_count         INT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    available_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at        TIMESTAMPTZ,
    endpoint            VARCHAR NOT NULL DEFAULT '',
    entity_type         VARCHAR NOT NULL DEFAULT 'match',
    entity_id           VARCHAR,
    -- columns from merged raw_api_responses
    params              JSONB NOT NULL DEFAULT '[]'::jsonb,
    status_code         INT NOT NULL DEFAULT 200,
    session_id          VARCHAR(50),
    response_time_ms    INT
);
CREATE INDEX IF NOT EXISTS idx_rib_status ON raw_ingest_buffer (status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_rib_created ON raw_ingest_buffer (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_rib_pending_available ON raw_ingest_buffer (available_at ASC, created_at ASC, id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_rib_processing_lease ON raw_ingest_buffer (processed_at, created_at) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS idx_rib_match_entity_status ON raw_ingest_buffer (entity_id, status) WHERE entity_type = 'match' AND COALESCE(entity_id, '') <> '';
CREATE UNIQUE INDEX IF NOT EXISTS uq_rib_active_match_entity ON raw_ingest_buffer (entity_id) WHERE entity_type = 'match' AND COALESCE(entity_id, '') <> '' AND status IN ('pending', 'processing');
CREATE INDEX IF NOT EXISTS idx_rib_match_history_entity_status ON raw_ingest_buffer (entity_id, status) WHERE entity_type = 'match_history' AND COALESCE(entity_id, '') <> '';
CREATE INDEX IF NOT EXISTS idx_rib_endpoint ON raw_ingest_buffer (endpoint);
CREATE INDEX IF NOT EXISTS idx_rib_status_code ON raw_ingest_buffer (status_code);
CREATE INDEX IF NOT EXISTS idx_rib_session_id ON raw_ingest_buffer (session_id);
COMMENT ON TABLE raw_ingest_buffer IS 'ELT buffer — raw payloads are dumped here first, processed by the background worker, and then pruned by bounded retention. Permanent pass-through audits live in hirez_raw_api_responses.';

-- hirez_raw_api_responses - permanent raw pass-through audit.
-- raw_ingest_buffer is not an archive; it is queue state. These rows retain
-- operator-triggered raw Hi-Rez payloads without making the ingest worker scan
-- or retain debug-only data forever.
CREATE TABLE IF NOT EXISTS hirez_raw_api_responses (
    id                  BIGSERIAL PRIMARY KEY,
    endpoint            VARCHAR(100) NOT NULL,
    operation           VARCHAR(100) NOT NULL,
    entity_type         VARCHAR(50) NOT NULL DEFAULT '',
    entity_id           VARCHAR(200),
    params              JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_response        JSONB NOT NULL,
    raw_response_text   TEXT NOT NULL,
    response_sha256     VARCHAR(64) NOT NULL,
    response_shape      VARCHAR(20) NOT NULL
        CHECK (response_shape IN ('array', 'object', 'string', 'number', 'boolean', 'null', 'undefined')),
    response_count      INT,
    status_code         INT NOT NULL DEFAULT 200,
    success             BOOLEAN NOT NULL DEFAULT true,
    error_message       TEXT,
    source              VARCHAR(80) NOT NULL DEFAULT 'paladinscat-api-raw-pass-through',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_hirez_raw_api_responses_created
    ON hirez_raw_api_responses (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_hirez_raw_api_responses_endpoint_created
    ON hirez_raw_api_responses (endpoint, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_hirez_raw_api_responses_entity
    ON hirez_raw_api_responses (entity_type, entity_id, created_at DESC)
    WHERE entity_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_hirez_raw_api_responses_sha
    ON hirez_raw_api_responses (response_sha256);
COMMENT ON TABLE hirez_raw_api_responses IS 'Permanent audit store for raw Hi-Rez payloads returned by backend pass-through endpoints; separate from raw_ingest_buffer because the buffer is a bounded processing queue.';
COMMENT ON COLUMN hirez_raw_api_responses.raw_response IS 'Queryable JSONB copy of the backend-observed raw payload.';
COMMENT ON COLUMN hirez_raw_api_responses.raw_response_text IS 'Serialized payload text used to compute response_sha256, preserving the exact backend-side response representation for audit comparison.';

-- search_remote_lookup_cache - short TTL cache for explicit search fallbacks.
-- Universal search is local-first and must not spend Hi-Rez calls while the user
-- types. When a user explicitly chooses an exact remote lookup for an unknown
-- player/match, cache both hits and misses briefly so repeated clicks or page
-- reloads do not become a quota-burn loop.
CREATE TABLE IF NOT EXISTS search_remote_lookup_cache (
    cache_key       TEXT PRIMARY KEY,
    query           TEXT NOT NULL,
    target          VARCHAR(30) NOT NULL,
    status          VARCHAR(20) NOT NULL CHECK (status IN ('hit', 'miss', 'error')),
    result          JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_message   TEXT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_search_remote_lookup_cache_expires
    ON search_remote_lookup_cache (expires_at);
COMMENT ON TABLE search_remote_lookup_cache IS 'Short TTL cache for explicit universal-search Hi-Rez fallbacks. Prevents repeated misses for the same unknown player/match from burning API calls.';

-- raw_ingest_buffer_retention_audit - compact breadcrumb for retention cleanup.
-- Old terminal raw payload rows are intentionally pruned from raw_ingest_buffer
-- so the queue table and indexes cannot grow without bound. This table keeps
-- the small operational summary needed for debugging retention behavior without
-- preserving bulky raw JSON forever.
CREATE TABLE IF NOT EXISTS raw_ingest_buffer_retention_audit (
    id                  BIGSERIAL PRIMARY KEY,
    reason              TEXT NOT NULL,
    status              VARCHAR(20) NOT NULL,
    endpoint            VARCHAR NOT NULL DEFAULT '',
    entity_type         VARCHAR NOT NULL DEFAULT '',
    retention_seconds   INT NOT NULL,
    deleted_count       INT NOT NULL,
    oldest_created_at   TIMESTAMPTZ,
    newest_created_at   TIMESTAMPTZ,
    oldest_processed_at TIMESTAMPTZ,
    newest_processed_at TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rib_retention_audit_created
    ON raw_ingest_buffer_retention_audit (created_at DESC);
COMMENT ON TABLE raw_ingest_buffer_retention_audit IS 'Small summary of raw_ingest_buffer retention deletes by status/endpoint/entity_type; raw payloads are not stored here.';

-- match_ingest_status - explicit completion marker for staged match ingest.
-- The buffer worker writes `matches` before many downstream facts/projections.
-- This table records the true end-to-end state so guards can distinguish a
-- fully completed match from one that crashed halfway through processing.
CREATE TABLE IF NOT EXISTS match_ingest_status (
    match_id BIGINT PRIMARY KEY,
    status VARCHAR(20) NOT NULL DEFAULT 'processing'
        CHECK (status IN ('processing', 'partial', 'complete', 'limited', 'failed')),
    completed_stages TEXT[] NOT NULL DEFAULT '{}',
    source VARCHAR(50),
    attempts INT NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_mis_status_updated ON match_ingest_status (status, updated_at);
COMMENT ON TABLE match_ingest_status IS 'Durable match ingest state. complete and limited are terminal; only complete matches are eligible for aggregate projections.';

-- player_match_history_cache - compact recovery cache for getmatchhistory.
-- Each Hi-Rez getmatchhistory call returns a rolling recent window for one
-- player. Store that full response here so recovery can reuse it across buffer
-- batches and cron retries without staging every row as raw_ingest_buffer work.
CREATE TABLE IF NOT EXISTS player_match_history_cache (
    player_id   BIGINT PRIMARY KEY,
    raw_data    JSONB NOT NULL,
    match_ids   BIGINT[] NOT NULL DEFAULT '{}',
    fetched_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    source      VARCHAR(30) NOT NULL DEFAULT 'getmatchhistory'
);
CREATE INDEX IF NOT EXISTS idx_player_match_history_cache_expires ON player_match_history_cache (expires_at);
CREATE INDEX IF NOT EXISTS idx_player_match_history_cache_match_ids ON player_match_history_cache USING GIN (match_ids);
COMMENT ON TABLE player_match_history_cache IS 'Durable per-player getmatchhistory cache. Prevents repeated player history calls across recovery batches and cron retries while preserving the full 50-match window.';

-- player_match_history_entries - one-player history observations.
-- These rows are the safe home for getmatchhistory output. They can answer
-- player-history UI and seed DB-first broken-match recovery, but they are not
-- complete match facts and must not drive ranked ingest/projections by
-- themselves.
CREATE TABLE IF NOT EXISTS player_match_history_entries (
    match_id          BIGINT NOT NULL,
    player_id         BIGINT NOT NULL,
    fetched_player_id BIGINT,
    entry_datetime    TIMESTAMPTZ,
    queue_id          INT,
    region            VARCHAR(50),
    map               VARCHAR(200),
    champion_id       INT,
    champion_name     VARCHAR(100),
    skin_id           INT,
    skin_name         VARCHAR(100),
    win_status        VARCHAR(20),
    kills             INT DEFAULT 0,
    deaths            INT DEFAULT 0,
    assists           INT DEFAULT 0,
    damage            INT DEFAULT 0,
    healing           INT DEFAULT 0,
    gold_earned       INT DEFAULT 0,
    time_in_match     INT DEFAULT 0,
    task_force        SMALLINT DEFAULT 0,
    league_tier       INT DEFAULT 0,
    source            VARCHAR(30) NOT NULL DEFAULT 'getmatchhistory',
    raw_data          JSONB NOT NULL DEFAULT '{}'::jsonb,
    normalized_data   JSONB NOT NULL DEFAULT '{}'::jsonb,
    observed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at        TIMESTAMPTZ,
    PRIMARY KEY (match_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_pmhe_player_time ON player_match_history_entries (player_id, entry_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_pmhe_fetched_player_expires ON player_match_history_entries (fetched_player_id, expires_at DESC);
CREATE INDEX IF NOT EXISTS idx_pmhe_match ON player_match_history_entries (match_id);
CREATE INDEX IF NOT EXISTS idx_pmhe_queue_time ON player_match_history_entries (queue_id, entry_datetime DESC);
COMMENT ON TABLE player_match_history_entries IS 'One-player getmatchhistory observations. Used for player history display and DB-first recovery only; never drives ranked match ingest by itself.';

-- ingest_cleanup_audit - local incident cleanup breadcrumb.
-- Stores metadata for rows removed from production-facing fact tables so an
-- operator can explain what was deleted without keeping corrupted match/player
-- facts live. This is intentionally small: raw payload audit remains in
-- raw_ingest_buffer until retention cleanup unless an incident cleanup removes
-- known-poisoned staging rows.
CREATE TABLE IF NOT EXISTS ingest_cleanup_audit (
    incident_id     TEXT NOT NULL,
    match_id        BIGINT NOT NULL,
    entry_datetime  TIMESTAMPTZ,
    queue_id        INT,
    is_ranked       BOOLEAN,
    region          TEXT,
    source          TEXT,
    broken          BOOLEAN,
    recovered       BOOLEAN,
    private         BOOLEAN,
    ingested_at     TIMESTAMPTZ,
    stats           JSONB NOT NULL DEFAULT '{}'::jsonb,
    reason          TEXT NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (incident_id, match_id)
);
CREATE INDEX IF NOT EXISTS idx_ingest_cleanup_audit_incident ON ingest_cleanup_audit (incident_id, quarantined_at DESC);
COMMENT ON TABLE ingest_cleanup_audit IS 'Metadata-only audit trail for local cleanup of corrupted ingest facts; does not preserve bad production rows.';

-- Round to 2 decimal places. Values < 1 use ceil (round up), values >= 1 use round.
CREATE OR REPLACE FUNCTION roundTo2(value NUMERIC) RETURNS NUMERIC AS $$
BEGIN
    IF value = 0 THEN
        RETURN 0;
    ELSIF value < 1 THEN
        RETURN CEIL(value * 100) / 100;
    ELSE
        RETURN ROUND(value * 100, 0) / 100;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Process a batch of pending buffer rows into normalized tables
CREATE OR REPLACE FUNCTION process_ingest_buffer_batch(batch_size INT DEFAULT 50)
RETURNS TABLE(match_id BIGINT, status TEXT) AS $$
DECLARE
    v_row RECORD;
BEGIN
    FOR v_row IN
        SELECT id, match_id, raw_data, source, dev_id
        FROM raw_ingest_buffer
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT batch_size
        FOR UPDATE SKIP LOCKED
    LOOP
        -- Mark as processing
        UPDATE raw_ingest_buffer SET status = 'processing', processed_at = now() WHERE id = v_row.id;

        -- Extract match-level fields from first player in raw_data array
        INSERT INTO matches (match_id, entry_datetime, map, queue_id, duration_seconds, region,
            team1_score, team2_score, winning_task_force, has_replay, is_ranked, recovered, broken, private,
            surrendered, match_level, dev_id, ingested_at)
        SELECT
            (v_row.raw_data->0->>'Match')::BIGINT,
            (v_row.raw_data->0->>'Entry_Datetime')::TIMESTAMPTZ,
            v_row.raw_data->0->>'Map_Game',
            (v_row.raw_data->0->>'match_queue_id')::INT,
            (v_row.raw_data->0->>'Match_Duration')::INT,
            v_row.raw_data->0->>'Region',
            (v_row.raw_data->0->>'Team1Score')::INT,
            (v_row.raw_data->0->>'Team2Score')::INT,
            (v_row.raw_data->0->>'Winning_TaskForce')::INT,
            (v_row.raw_data->0->>'hasReplay')::TEXT = 'y',
            (v_row.raw_data->0->>'match_queue_id')::INT = 486,
            false, false, false,
            (v_row.raw_data->0->>'Surrendered')::BOOLEAN,
            (v_row.raw_data->0->>'Final_Match_Level')::INT,
            v_row.dev_id,
            now()
        ON CONFLICT (match_id, entry_datetime) DO UPDATE SET ingested_at = now();

        -- Insert match_players from raw_data array
        INSERT INTO match_players (match_id, player_id, player_name, champion_id, skin_id, skin_name,
            kills, deaths, assists, damage_done_in_hand, damage_done_physical, damage_done_magical,
            damage_taken, damage_taken_physical, damage_taken_magical, damage_mitigated, healing, healing_self,
            healing_bot, healing_player_self, gold_earned, gold_per_minute, objective_assists, camps_cleared,
            structure_damage, wards_placed, towers_destroyed, distance_traveled, multi_kill_max, killing_spree,
            kills_first_blood, kills_double, kills_triple, kills_quadra, kills_penta, kills_fire_giant,
            kills_gold_fury, kills_phoenix, kills_siege_jugg, kills_wild_jugg,
            win_status, task_force,
            league_tier, league_points, league_wins, league_losses, account_level, mastery_level, party_id,
            kda, damage_per_minute, healing_per_minute, healing_self_per_minute, time_in_match, entry_datetime, source, portal_id, is_ranked,
            afk_rate, private_player_id, portal_user_id, kills_player, created_at,
            platform, damage_bot, kills_single, kills_bot, final_match_level, rank_stat_league, team_id, surrendered)
        SELECT
            (p->>'Match')::BIGINT,
            (p->>'playerId')::BIGINT,
            COALESCE(p->>'playerName', p->>'Player_Name', 'PRIVATEACCOUNT'),
            (p->>'ChampionId')::INT,
            (p->>'SkinId')::INT,
            p->>'Skin',
            (p->>'Kills_Player')::INT,
            (p->>'Deaths')::INT,
            (p->>'Assists')::INT,
            (p->>'Damage_Done_In_Hand')::INT,
            (p->>'Damage_Done_Physical')::INT,
            (p->>'Damage_Done_Magical')::INT,

            (p->>'Damage_Taken')::INT,
            (p->>'Damage_Taken_Physical')::INT,
            (p->>'Damage_Taken_Magical')::INT,
            (p->>'Damage_Mitigated')::INT,
            (p->>'Healing')::INT,
            (p->>'Healing_Player_Self')::INT,
            (p->>'Healing_Bot')::INT,
            (p->>'Healing_Player_Self')::INT,
            (p->>'Gold_Earned')::INT,
            roundTo2(
                CASE WHEN (COALESCE((p->>'Time_In_Match_Seconds')::INT, (p->>'Time_In_Match')::INT)) > 0
                    THEN (p->>'Gold_Earned')::NUMERIC / ((COALESCE((p->>'Time_In_Match_Seconds')::INT, (p->>'Time_In_Match')::INT)) / 60.0)
                    ELSE 0
                END
            ),
            (p->>'Objective_Assists')::INT,
            (p->>'Camps_Cleared')::INT,
            (p->>'Structure_Damage')::INT,
            (p->>'Wards_Placed')::INT,
            (p->>'Towers_Destroyed')::INT,
            (p->>'Distance_Traveled')::INT,
            (p->>'Multi_kill_Max')::INT,
            (p->>'Killing_Spree')::INT,
            (p->>'Kills_First_Blood')::INT,
            (p->>'Kills_Double')::INT,
            (p->>'Kills_Triple')::INT,
            (p->>'Kills_Quadra')::INT,
            (p->>'Kills_Penta')::INT,
            (p->>'Kills_Fire_Giant')::INT,
            (p->>'Kills_Gold_Fury')::INT,
            (p->>'Kills_Phoenix')::INT,
            (p->>'Kills_Siege_Juggernaut')::INT,
            (p->>'Kills_Wild_Juggernaut')::INT,
            p->>'Win_Status',
            (p->>'TaskForce')::INT,
            (p->>'League_Tier')::INT,
            (p->>'League_Points')::INT,
            (p->>'League_Wins')::INT,
            (p->>'League_Losses')::INT,
            (p->>'Account_Level')::INT,
            (p->>'Mastery_Level')::INT,
            (p->>'PartyId')::INT,
            roundTo2(
                (
                    (p->>'Kills_Player')::INT + (p->>'Assists')::INT / 2.0
                )::NUMERIC / GREATEST((p->>'Deaths')::INT, 1)
            ),
            roundTo2(
                CASE WHEN (p->>'Time_In_Match_Seconds')::INT > 0
                    THEN (p->>'Damage_Done_Physical')::INT::NUMERIC / ((p->>'Time_In_Match_Seconds')::INT / 60.0)
                    ELSE 0
                END
            ),
            roundTo2(
                CASE WHEN (p->>'Time_In_Match_Seconds')::INT > 0
                    THEN ((p->>'Healing')::INT + (p->>'Healing_Player_Self')::INT)::NUMERIC / ((p->>'Time_In_Match_Seconds')::INT / 60.0)
                    ELSE 0
                END
            ),
            (p->>'Time_In_Match_Seconds')::INT,
            (v_row.raw_data->0->>'Entry_Datetime')::TIMESTAMPTZ,
            COALESCE(p->>'source', 'direct'),
            (p->>'playerPortalId')::SMALLINT,
            (p->>'match_queue_id')::INT = 486,
            0, 0, NULL,
            p->>'playerPortalUserId',
            (p->>'Kills_Player')::INT,
            now(),
            p->>'Platform',
            (p->>'Damage_Bot')::INT,
            (p->>'Kills_Single')::INT,
            (p->>'Kills_Bot')::INT,
            (p->>'Final_Match_Level')::INT,
            (p->>'Rank_Stat_League')::INT,
            (p->>'TeamId')::INT,
            (p->>'Surrendered')::BOOLEAN
        FROM jsonb_array_elements(v_row.raw_data) AS p
        WHERE (p->>'ret_msg') IS NULL OR trim(p->>'ret_msg') <> '';

        -- Mark as processed
        UPDATE raw_ingest_buffer SET status = 'processed' WHERE id = v_row.id;

        match_id := v_row.match_id;
        status := 'processed';
        RETURN NEXT;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION process_ingest_buffer_batch IS 'ELT processor - reads raw JSON from buffer, normalizes to matches + match_players, marks processed';

-- ============================================================================
-- 4. MATCH_PLAYERS - Per-player match data (HYPERTABLE)
-- ============================================================================

CREATE TABLE IF NOT EXISTS match_players (
    match_id              BIGINT NOT NULL,
    player_id             BIGINT NOT NULL,
    private_slot          SMALLINT NOT NULL DEFAULT 0,
    player_name           VARCHAR(100),
    region                VARCHAR(50),
    champion_id           INT REFERENCES champions(id),
    skin_id               INT,
    skin_name             VARCHAR(200),
    kills                 INT DEFAULT 0,
    deaths                INT DEFAULT 0,
    assists               INT DEFAULT 0,
    damage_done_in_hand   INT DEFAULT 0,
    damage_done_physical  INT DEFAULT 0,
    damage_done_magical   INT DEFAULT 0,
    damage_taken          INT DEFAULT 0,
    damage_taken_physical INT DEFAULT 0,
    damage_taken_magical  INT DEFAULT 0,
    damage_mitigated      INT DEFAULT 0,
    healing               INT DEFAULT 0,
    healing_self          INT DEFAULT 0,
    healing_bot           INT DEFAULT 0,
    healing_player_self   INT DEFAULT 0,
    gold_earned           INT DEFAULT 0,
    gold_per_minute       DOUBLE PRECISION DEFAULT 0,
    objective_assists     INT DEFAULT 0,
    camps_cleared         INT DEFAULT 0,
    structure_damage      INT DEFAULT 0,
    wards_placed          INT DEFAULT 0,
    towers_destroyed      INT DEFAULT 0,
    distance_traveled     INT DEFAULT 0,
    multi_kill_max        INT DEFAULT 0,
    killing_spree         INT DEFAULT 0,
    kills_first_blood     BOOLEAN DEFAULT FALSE,
    kills_double          INT DEFAULT 0,
    kills_triple          INT DEFAULT 0,
    kills_quadra          INT DEFAULT 0,
    kills_penta           INT DEFAULT 0,
    kills_fire_giant      INT DEFAULT 0,
    kills_gold_fury       INT DEFAULT 0,
    kills_phoenix         INT DEFAULT 0,
    kills_siege_jugg      INT DEFAULT 0,
    kills_wild_jugg       INT DEFAULT 0,
    win_status            VARCHAR(20),
    task_force            SMALLINT,
    league_tier           INT,
    league_points         INT,
    league_wins           INT,
    league_losses         INT,
    account_level         INT,
    mastery_level         INT,
    party_id              INT,
    party                 SMALLINT NOT NULL DEFAULT 0,  -- sequential party number per match (0=solo, 1-4=party)
    kda                   DOUBLE PRECISION,
    damage_per_minute     DOUBLE PRECISION,
    healing_per_minute    DOUBLE PRECISION,
    time_in_match         INT,
    entry_datetime        TIMESTAMPTZ NOT NULL DEFAULT now(),
    source                VARCHAR(20) DEFAULT 'direct',
    portal_id             SMALLINT,
    is_ranked             BOOLEAN DEFAULT FALSE,
    afk_rate              DOUBLE PRECISION DEFAULT 0.0 CHECK (afk_rate >= 0 AND afk_rate <= 3),
    egpm                  DOUBLE PRECISION,
    mitigation_per_minute DOUBLE PRECISION DEFAULT 0.0,
    private_player_id     INTEGER DEFAULT 0,
    portal_user_id        TEXT,
    kills_player          INT,
    created_at            TIMESTAMPTZ,
    platform              VARCHAR(20),
    damage_bot            INT DEFAULT 0,
    kills_single          INT DEFAULT 0,
    kills_bot             INT,
    final_match_level     INT DEFAULT 0,
    rank_stat_league      INT DEFAULT 0,
    team_id               INT,
    surrendered           BOOLEAN DEFAULT FALSE,
    healing_self_per_minute DOUBLE PRECISION,
    has_ret_msg           BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (match_id, player_id, private_slot, entry_datetime)
);

CREATE OR REPLACE FUNCTION derive_match_player_gameplay_rates()
RETURNS TRIGGER AS $$
DECLARE
  duration_seconds INTEGER;
  effective_cpm NUMERIC;
BEGIN
  SELECT NULLIF(m.duration_seconds, 0)
    INTO duration_seconds
    FROM matches m
   WHERE m.match_id = NEW.match_id
     AND m.entry_datetime = NEW.entry_datetime
   LIMIT 1;

  IF duration_seconds IS NOT NULL AND duration_seconds > 0 THEN
    NEW.gold_per_minute := ROUND(
      COALESCE(NEW.gold_earned, 0)::NUMERIC * 60 / duration_seconds,
      2
    )::DOUBLE PRECISION;
    effective_cpm := ROUND(
      (COALESCE(NEW.gold_earned, 0) - 500)::NUMERIC * 60 / duration_seconds,
      2
    );
    NEW.egpm := effective_cpm::DOUBLE PRECISION;
    NEW.damage_per_minute := ROUND(COALESCE(NEW.damage_done_physical, 0)::NUMERIC * 60 / duration_seconds, 2)::DOUBLE PRECISION;
    NEW.healing_per_minute := ROUND(COALESCE(NEW.healing, 0)::NUMERIC * 60 / duration_seconds, 2)::DOUBLE PRECISION;
    NEW.healing_self_per_minute := ROUND(COALESCE(NEW.healing_self, 0)::NUMERIC * 60 / duration_seconds, 2)::DOUBLE PRECISION;
    NEW.mitigation_per_minute := ROUND(COALESCE(NEW.damage_mitigated, 0)::NUMERIC * 60 / duration_seconds, 2)::DOUBLE PRECISION;
    NEW.afk_rate := CASE
      WHEN effective_cpm >= 70 THEN 0
      ELSE 3
    END;
  ELSE
    NEW.gold_per_minute := 0;
    NEW.egpm := 0;
    NEW.damage_per_minute := 0;
    NEW.healing_per_minute := 0;
    NEW.healing_self_per_minute := 0;
    NEW.mitigation_per_minute := 0;
    NEW.afk_rate := 0;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_match_player_gameplay_rates ON match_players;
CREATE TRIGGER trg_match_player_gameplay_rates
  BEFORE INSERT OR UPDATE OF gold_earned, damage_done_physical, healing, healing_self,
    damage_mitigated, time_in_match, match_id, entry_datetime
  ON match_players
  FOR EACH ROW
  EXECUTE FUNCTION derive_match_player_gameplay_rates();

CREATE OR REPLACE FUNCTION refresh_match_player_gameplay_rates_on_duration_change()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.duration_seconds IS DISTINCT FROM OLD.duration_seconds THEN
    UPDATE match_players
       SET gold_earned = gold_earned
     WHERE match_id = NEW.match_id
       AND entry_datetime = NEW.entry_datetime;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_match_duration_gameplay_rates ON matches;
CREATE TRIGGER trg_match_duration_gameplay_rates
  AFTER UPDATE OF duration_seconds ON matches
  FOR EACH ROW
  EXECUTE FUNCTION refresh_match_player_gameplay_rates_on_duration_change();

-- Add has_ret_msg column if it doesn't exist (recovery pipeline fix)
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'match_players' AND column_name = 'has_ret_msg') THEN
        ALTER TABLE match_players ADD COLUMN has_ret_msg BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

COMMENT ON TABLE match_players IS 'Per-player match stats - TimescaleDB hypertable partitioned by entry_datetime';
COMMENT ON COLUMN match_players.win_status IS 'Winner'' or ''Loser''';
COMMENT ON COLUMN match_players.source IS 'Authority tier for player-match facts: direct = full getmatchdetailsbatch, recovered = targeted broken-match recovery, minimal = profile-only fallback. getmatchhistory observations live in player_match_history_entries.';
COMMENT ON COLUMN match_players.afk_rate IS 'Conservative automatic AFK severity: 0=not auto-flagged (including 70-119 eCPM review bands), 3=at or below the passive-credit activity floor. Calculated from gameplay-duration eCPM.';
COMMENT ON COLUMN match_players.gold_per_minute IS 'CPM derived strictly from gold_earned and match gameplay duration.';
COMMENT ON COLUMN match_players.egpm IS 'Effective CPM = (gold_earned - 500) / gameplay minutes. Starting credits removed. Pure passive floor = 60.';
COMMENT ON COLUMN match_players.mitigation_per_minute IS 'Damage mitigated per minute (same formula as dpm/hpm).';
COMMENT ON COLUMN match_players.private_player_id IS 'Links to players_private when player_name = PRIVATEACCOUNT';
COMMENT ON COLUMN match_players.private_slot IS 'Per-match ordinal for multiple player_id=0 private participants; 0 for identified players.';
COMMENT ON COLUMN match_players.party IS 'Sequential party number per match: 0 = solo (no party), 1-4 = party group. Assigned by buffer-processor after match_players insert.';

-- Player profile values captured immediately after ingest. These values belong
-- to the match and must not change when the mutable players row is refreshed.
CREATE TABLE IF NOT EXISTS match_player_profile_snapshots (
    match_id          BIGINT NOT NULL,
    player_id         BIGINT NOT NULL,
    captured_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    source            TEXT NOT NULL DEFAULT 'post_match_ingest',
    level             INTEGER,
    platform          TEXT,
    region            TEXT,
    global_wins       INTEGER,
    global_losses     INTEGER,
    kbm_tier          INTEGER,
    kbm_points        INTEGER,
    kbm_rank          INTEGER,
    kbm_wins          INTEGER,
    kbm_losses        INTEGER,
    champion_wins     INTEGER,
    champion_losses   INTEGER,
    PRIMARY KEY (match_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_match_player_profile_snapshots_player
    ON match_player_profile_snapshots (player_id, captured_at DESC);
COMMENT ON TABLE match_player_profile_snapshots IS 'Immutable per-match player profile state captured by the post-ingest getplayerbatch call; match reads are database-only.';

-- ─── match_players Indexes ────────────────────────────────────────────────────

-- Match lookup + task_force filter (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_mp_match_team ON match_players (match_id, task_force);

-- Player lookup
CREATE INDEX IF NOT EXISTS idx_mp_player ON match_players (player_id);

-- Party tracking (only where party > 0)
CREATE INDEX IF NOT EXISTS idx_mp_party ON match_players (match_id, party) WHERE party > 0;

-- ─── Ranked-only partial indexes (WHERE is_ranked = true) ─────────────────────
-- These keep index sizes small by excluding casual matches

-- Champion + tier (leaderboard queries)
CREATE INDEX IF NOT EXISTS idx_mp_champion_tier_ranked ON match_players (champion_id, league_tier) WHERE is_ranked = true;



-- Champion + win outcome (champion stats)
CREATE INDEX IF NOT EXISTS idx_mp_champion_win_ranked ON match_players (champion_id, win_status) WHERE is_ranked = true;

-- Player + champion (player champion stats)
CREATE INDEX IF NOT EXISTS idx_mp_player_champ_ranked ON match_players (player_id, champion_id) WHERE is_ranked = true;

-- Player + tier (player tier distribution)
CREATE INDEX IF NOT EXISTS idx_mp_player_tier_ranked ON match_players (player_id, league_tier) WHERE is_ranked = true;

-- Win status + task_force (match outcome queries)
CREATE INDEX IF NOT EXISTS idx_mp_win_team_ranked ON match_players (win_status, task_force) WHERE is_ranked = true;

-- Recent low-eCPM review queue. Ordering columns come first so cursor pages do
-- not sort the growing fact table; the partial predicate keeps the index small.
CREATE INDEX IF NOT EXISTS idx_mp_ranked_ecpm_candidates
  ON match_players (entry_datetime DESC, match_id DESC, player_id DESC)
  INCLUDE (egpm, champion_id, win_status, task_force, source)
  WHERE is_ranked = true
    AND player_id > 0
    AND champion_id > 0
    AND egpm >= 0
    AND egpm < 120
    AND COALESCE(source, 'direct') IN ('direct', 'recovered');

-- ─── Player Relationships (Teammate + Opponent) ───────────────────────────────
-- Ranked-only (queue_id = 486). Casual matches are stored in match_players but
-- do NOT generate player_relationships rows. This keeps advanced metrics lean.
-- Consolidated: one row per (source, target, same_team) with count of matches.
-- Single direction: source_player_id < target_player_id always.
-- Per-match lookup is covered by match_players (party_id, task_force columns).

CREATE TABLE IF NOT EXISTS player_relationships (
    source_player_id  BIGINT NOT NULL,
    target_player_id  BIGINT NOT NULL,
    same_team         BOOLEAN NOT NULL,
    same_party        BOOLEAN NOT NULL DEFAULT false,
    count             INT NOT NULL DEFAULT 1,
    first_seen        TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_player_id, target_player_id, same_team),
    CHECK (source_player_id < target_player_id)
);

-- Golden index: source first, then same_team filter, then target for grouping
CREATE INDEX IF NOT EXISTS idx_pr_source_team_target ON player_relationships (source_player_id, same_team, target_player_id);

-- Opponent queries
CREATE INDEX IF NOT EXISTS idx_pr_source_opponent ON player_relationships (source_player_id, target_player_id) WHERE same_team = false;

-- Party queries
CREATE INDEX IF NOT EXISTS idx_pr_same_party ON player_relationships (source_player_id, target_player_id) WHERE same_party = true;

-- Top co-play / opponent by count
CREATE INDEX IF NOT EXISTS idx_pr_source ON player_relationships (source_player_id, same_team, count DESC);
CREATE INDEX IF NOT EXISTS idx_pr_target ON player_relationships (target_player_id, same_team, count DESC);

COMMENT ON TABLE player_relationships IS 'Consolidated player relationships: one row per (source, target, same_team). source < target always. count = number of matches together.';

-- ─── Co-Play Stats Materialized View ──────────────────────────────────────────
-- Regular MV (not TimescaleDB CA). Refreshed manually after each ingest cycle.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_player_coplay_stats AS
SELECT
  source_player_id,
  target_player_id,
  same_team,
  count AS times_together,
  CASE WHEN same_party THEN count ELSE 0 END AS times_in_party,
  first_seen,
  last_seen
FROM player_relationships
WITH NO DATA;

-- Auto-refresh every 1 hour
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_player_coplay_stats_unique
  ON mv_player_coplay_stats (source_player_id, target_player_id, same_team);

CREATE INDEX IF NOT EXISTS idx_mv_player_coplay_stats_source
  ON mv_player_coplay_stats (source_player_id, same_team, times_together DESC);

CREATE INDEX IF NOT EXISTS idx_mv_player_coplay_stats_target
  ON mv_player_coplay_stats (target_player_id, same_team, times_together DESC);

COMMENT ON MATERIALIZED VIEW mv_player_coplay_stats IS 'Derived ranked co-play/opponent projection rebuilt from player_relationships; no Hi-Rez API calls required.';

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'match_players') THEN
        PERFORM create_hypertable('match_players', time_column_name := 'entry_datetime', number_partitions := 4);
    END IF;
END
$$;

-- Idempotent compression setup for match_players
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = 'match_players'
        AND c.reloptions IS NOT NULL
    ) THEN
        ALTER TABLE match_players SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'champion_id, league_tier',
            timescaledb.compress_orderby = 'entry_datetime DESC'
        );
    END IF;
    PERFORM add_compression_policy('match_players', INTERVAL '30 days', if_not_exists => TRUE);
END
$$;

-- Backfill entry_datetime
DO $$
BEGIN
    UPDATE match_players mp
    SET entry_datetime = m.entry_datetime
    FROM matches m
    WHERE m.match_id = mp.match_id
      AND (mp.entry_datetime IS NULL OR mp.entry_datetime = now());
END
$$;

CREATE INDEX IF NOT EXISTS idx_match_players_player_id ON match_players (player_id);
CREATE INDEX IF NOT EXISTS idx_match_players_champion_id ON match_players (champion_id);
CREATE INDEX IF NOT EXISTS idx_match_players_win_status ON match_players (win_status);
CREATE INDEX IF NOT EXISTS idx_match_players_league_tier ON match_players (league_tier);

-- match_lobby_tiers - Shared tier dimension for all ranked aggregates
CREATE TABLE IF NOT EXISTS match_lobby_tiers (
    match_id       BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    lobby_tier     SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
    known_players  SMALLINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, entry_datetime)
);
CREATE INDEX IF NOT EXISTS idx_match_lobby_tiers_scope
    ON match_lobby_tiers (lobby_tier, match_id);
COMMENT ON TABLE match_lobby_tiers IS 'Rounded average known real-player tier for each queue-486 match; shared filter dimension for public aggregate statistics.';
-- [REMOVED: duplicate indexes for columns already indexed above;
--  also removed idx_match_players_party_id, idx_match_players_deck_hash,
--  idx_match_players_item_build_hash which reference non-existent columns
--  (party_id, deck_hash, item_build_hash don't exist on match_players).]

-- ============================================================================
-- 5. MATCH FACT TABLES - Normalized data
-- =======================================================================

-- match_bans - Draft bans
CREATE TABLE IF NOT EXISTS match_bans (
    match_id       BIGINT NOT NULL,
    ban_slot       SMALLINT NOT NULL CHECK (ban_slot BETWEEN 1 AND 8),
    champion_id    INT REFERENCES champions(id),
    PRIMARY KEY (match_id, ban_slot)
);
CREATE INDEX IF NOT EXISTS idx_mb_champion ON match_bans (champion_id);
COMMENT ON TABLE match_bans IS 'Draft bans - extracted from repeated BanId1-8/Ban_1-8 fields';

-- match_player_items - Normalized item usage
CREATE TABLE IF NOT EXISTS match_player_items (
    match_id            BIGINT NOT NULL,
    player_id           BIGINT NOT NULL,
    slot                SMALLINT NOT NULL,
    item_id             INT NOT NULL REFERENCES items(item_id),
    item_level          SMALLINT DEFAULT 0,
    PRIMARY KEY (match_id, player_id, item_id)
);
CREATE INDEX IF NOT EXISTS idx_mpi_item_id ON match_player_items (item_id);
CREATE INDEX IF NOT EXISTS idx_mpi_player_id ON match_player_items (player_id);
CREATE INDEX IF NOT EXISTS idx_mpi_match_id ON match_player_items (match_id);
COMMENT ON TABLE match_player_items IS 'Normalized item usage - ~6 rows per player per match';

-- match_player_talents - Per-match talent selections
CREATE TABLE IF NOT EXISTS match_player_talents (
    match_id            BIGINT NOT NULL,
    player_id           BIGINT NOT NULL,
    talent_id           INT NOT NULL REFERENCES talents(talent_id),
    PRIMARY KEY (match_id, player_id, talent_id)
);
CREATE INDEX IF NOT EXISTS idx_mpt_talent_id ON match_player_talents (talent_id);
CREATE INDEX IF NOT EXISTS idx_mpt_player_id ON match_player_talents (player_id);
CREATE INDEX IF NOT EXISTS idx_mpt_match_id ON match_player_talents (match_id);
COMMENT ON TABLE match_player_talents IS 'Per-match talent selections - 3 talents per player';

-- match_player_cards - Per-match loadout card investments
CREATE TABLE IF NOT EXISTS match_player_cards (
    match_id            BIGINT NOT NULL,
    player_id           BIGINT NOT NULL,
    card_id             INT NOT NULL,
    card_level          SMALLINT DEFAULT 0,
    PRIMARY KEY (match_id, player_id, card_id)
);
CREATE INDEX IF NOT EXISTS idx_mpc_card_id ON match_player_cards (card_id);
CREATE INDEX IF NOT EXISTS idx_mpc_player_id ON match_player_cards (player_id);
CREATE INDEX IF NOT EXISTS idx_mpc_match_id ON match_player_cards (match_id);
COMMENT ON TABLE match_player_cards IS 'Per-match loadout card investments - 5 cards, 1-5 levels each, totaling 15 points';

-- match_opponents - Consolidated per-champion head-to-head tracking
-- One row per (player, player_champion, opponent_champion) with accumulated wins/losses.
-- Enables per-champion win-rate against specific opponent champions.
CREATE TABLE IF NOT EXISTS match_opponents (
    player_id             BIGINT NOT NULL,
    player_champion_id    INT NOT NULL REFERENCES champions(id),
    opponent_champion_id  INT NOT NULL REFERENCES champions(id),
    wins                  INT NOT NULL DEFAULT 0,
    losses                INT NOT NULL DEFAULT 0,
    PRIMARY KEY (player_id, player_champion_id, opponent_champion_id)
);
CREATE INDEX IF NOT EXISTS idx_mop_player_id ON match_opponents (player_id);
CREATE INDEX IF NOT EXISTS idx_mop_opponent_champion ON match_opponents (opponent_champion_id);
CREATE INDEX IF NOT EXISTS idx_mop_player_champion ON match_opponents (player_champion_id);
COMMENT ON TABLE match_opponents IS 'Consolidated per-champion head-to-head: wins/losses per (player, champion vs opponent champion). Win-rate = wins / (wins + losses) * 100';

-- match_opponent_facts - per-match idempotency ledger for match_opponents.
-- match_opponents is cumulative and cannot know which match contributed a
-- counter increment. The buffer worker inserts into this ledger first, then
-- increments match_opponents only when a new fact row was created.
CREATE TABLE IF NOT EXISTS match_opponent_facts (
    match_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    player_champion_id INT NOT NULL,
    opponent_champion_id INT NOT NULL,
    wins INT NOT NULL DEFAULT 0,
    losses INT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, player_id, player_champion_id, opponent_champion_id)
);
CREATE INDEX IF NOT EXISTS idx_mof_player ON match_opponent_facts (player_id, player_champion_id);
COMMENT ON TABLE match_opponent_facts IS 'Per-match ledger that makes cumulative match_opponents updates idempotent across retries.';

-- ============================================================================
-- 6. RATING SYSTEMS - Glicko-2 + Queue ELO
-- ============================================================================

-- champion_ratings - Per-champion Glicko-2 state
CREATE TABLE IF NOT EXISTS champion_ratings (
    champion_id     INT PRIMARY KEY REFERENCES champions(id),
    rating          DOUBLE PRECISION NOT NULL DEFAULT 1500,
    deviation       DOUBLE PRECISION NOT NULL DEFAULT 350,
    volatility      DOUBLE PRECISION NOT NULL DEFAULT 0.06,
    matches_played  INT NOT NULL DEFAULT 0,
    wins            INT NOT NULL DEFAULT 0,
    losses          INT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE champion_ratings IS 'Per-champion Glicko-2 rating state';

-- champion_match_ratings - Per-match rating changes for audit trail
CREATE TABLE IF NOT EXISTS champion_match_ratings (
    match_id            BIGINT NOT NULL,
    champion_id         INT NOT NULL REFERENCES champions(id),
    pre_rating          NUMERIC NOT NULL,
    post_rating         NUMERIC NOT NULL,
    pre_uncertainty     NUMERIC NOT NULL,
    post_uncertainty    NUMERIC NOT NULL,
    last_updated        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cmr_champion ON champion_match_ratings (champion_id);
CREATE INDEX IF NOT EXISTS idx_cmr_match ON champion_match_ratings (match_id);
COMMENT ON TABLE champion_match_ratings IS 'Per-match Glicko-2 rating changes for audit trail';

-- champion_tier_ratings - Per-tier champion Glicko-2 ratings
CREATE TABLE IF NOT EXISTS champion_tier_ratings (
    champion_id     INT NOT NULL REFERENCES champions(id),
    tier            VARCHAR(10) NOT NULL,
    rating          DOUBLE PRECISION NOT NULL DEFAULT 1500,
    deviation       DOUBLE PRECISION NOT NULL DEFAULT 350,
    volatility      DOUBLE PRECISION NOT NULL DEFAULT 0.06,
    matches_played  INT NOT NULL DEFAULT 0,
    wins            INT NOT NULL DEFAULT 0,
    losses          INT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (champion_id, tier)
);
CREATE INDEX IF NOT EXISTS idx_ctr_champion ON champion_tier_ratings (champion_id);
CREATE INDEX IF NOT EXISTS idx_ctr_tier ON champion_tier_ratings (tier);

-- player_queue_ratings - Queue Glicko-2 (account-level skill per queue)
CREATE TABLE IF NOT EXISTS player_queue_ratings (
    player_id     BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    queue_id      INT NOT NULL,
    mu            NUMERIC NOT NULL DEFAULT 1500 CHECK (mu BETWEEN 0 AND 3500),
    phi           NUMERIC NOT NULL DEFAULT 350 CHECK (phi BETWEEN 1 AND 350),
    volatility    NUMERIC NOT NULL DEFAULT 0.06 CHECK (volatility BETWEEN 0.001 AND 0.2),
    player_key    TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, queue_id)
);
CREATE INDEX IF NOT EXISTS idx_pqr_mu ON player_queue_ratings (mu DESC);
CREATE INDEX IF NOT EXISTS idx_pqr_queue ON player_queue_ratings (queue_id);
COMMENT ON TABLE player_queue_ratings IS 'Queue Glicko-2 (μ, φ, σ) - account-level skill per queue';

-- player_champion_ratings - Champion Glicko-2 (per champion)
CREATE TABLE IF NOT EXISTS player_champion_ratings (
    player_id        BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    champion_id      INT NOT NULL REFERENCES champions(id) ON DELETE CASCADE,
    mu               NUMERIC NOT NULL DEFAULT 1500 CHECK (mu BETWEEN 0 AND 3500),
    phi              NUMERIC NOT NULL DEFAULT 350 CHECK (phi BETWEEN 1 AND 350),
    volatility       NUMERIC NOT NULL DEFAULT 0.06 CHECK (volatility BETWEEN 0.001 AND 0.2),
    matches_played   INT NOT NULL DEFAULT 0,
    wins             INT NOT NULL DEFAULT 0,
    losses           INT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    wins_flag        INT DEFAULT 0,
    player_key       TEXT,
    PRIMARY KEY (player_id, champion_id)
);
CREATE INDEX IF NOT EXISTS idx_pcr_mu ON player_champion_ratings (mu DESC);
CREATE INDEX IF NOT EXISTS idx_pcr_champion ON player_champion_ratings (champion_id);
CREATE INDEX IF NOT EXISTS idx_pcr_player ON player_champion_ratings (player_id);
COMMENT ON TABLE player_champion_ratings IS 'Champion Glicko-2 (μ, φ, σ) - per-champion proficiency';

-- match_rating_snapshots - Per-match rating before/after (audit trail)
CREATE TABLE IF NOT EXISTS match_rating_snapshots (
    match_id            BIGINT NOT NULL,
    player_id           BIGINT NOT NULL REFERENCES players(id),
    champion_id         INT NOT NULL REFERENCES champions(id),
    queue_mu_pre        NUMERIC,
    queue_mu_post       NUMERIC,
    champ_mu_pre        NUMERIC,
    champ_mu_post       NUMERIC,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    queue_phi_pre       NUMERIC,
    queue_phi_post      NUMERIC,
    champ_phi_pre       NUMERIC,
    champ_phi_post      NUMERIC,
    queue_volatility_pre NUMERIC,
    queue_volatility_post NUMERIC,
    champ_volatility_pre NUMERIC,
    champ_volatility_post NUMERIC,
    PRIMARY KEY (match_id, player_id, champion_id)
);
CREATE INDEX IF NOT EXISTS idx_mrs_match ON match_rating_snapshots (match_id);
CREATE INDEX IF NOT EXISTS idx_mrs_player ON match_rating_snapshots (player_id);
COMMENT ON TABLE match_rating_snapshots IS 'Per-match Glicko-2 snapshots - audit trail for μ/φ/σ changes';

CREATE TABLE IF NOT EXISTS rating_rebuild_requests (
    request_key TEXT PRIMARY KEY,
    earliest_entry_datetime TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE rating_rebuild_requests IS 'Global rating rebuild requests raised when a late match or invalid rating state cannot be safely applied incrementally.';

-- champion_stats - Per-player per-champion aggregated stats
CREATE TABLE IF NOT EXISTS champion_stats (
    player_id         BIGINT NOT NULL REFERENCES players(id),
    champion_id       INT NOT NULL REFERENCES champions(id),
    rank              INT DEFAULT 0,
    wins              INT DEFAULT 0,
    losses            INT DEFAULT 0,
    kills             INT DEFAULT 0,
    deaths            INT DEFAULT 0,
    assists           INT DEFAULT 0,
    minutes_played    INT DEFAULT 0,
    gold_earned       INT DEFAULT 0,
    minion_kills      INT DEFAULT 0,
    worshippers       BIGINT DEFAULT 0,
    last_played       TIMESTAMPTZ,
    last_updated      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, champion_id)
);
COMMENT ON TABLE champion_stats IS 'Per-player per-champion aggregated stats from getchampionranks';
CREATE INDEX IF NOT EXISTS idx_champion_stats_champion_id ON champion_stats (champion_id);
CREATE INDEX IF NOT EXISTS idx_champion_stats_rank ON champion_stats (rank DESC);
CREATE INDEX IF NOT EXISTS idx_champion_stats_wins ON champion_stats (wins DESC);

-- ============================================================================
-- 7. PRIVATE PLAYERS - Track PRIVATEACCOUNT players
-- ============================================================================

CREATE TABLE IF NOT EXISTS players_private (
    id                      SERIAL PRIMARY KEY,
    party_id                INTEGER NOT NULL DEFAULT 0,
    account_level           INTEGER NOT NULL DEFAULT 0,
    mastery_level           INTEGER NOT NULL DEFAULT 0,
    league_tier             INTEGER NOT NULL DEFAULT 0,
    league_points           INTEGER NOT NULL DEFAULT 0,
    last_known_level        INTEGER,
    last_known_mastery      INTEGER,
    last_known_league_tier  INTEGER,
    last_known_league_points INTEGER,
    first_seen              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    match_count             INTEGER NOT NULL DEFAULT 0,
    alias                   VARCHAR(50),
    notes                   TEXT,
    cheater                 BOOLEAN NOT NULL DEFAULT FALSE,
    cheater_reason          TEXT,
    cheater_marked_at       TIMESTAMPTZ,
    sus_count               INTEGER NOT NULL DEFAULT 0,
    UNIQUE (party_id, account_level, mastery_level, league_tier, league_points),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_players_private_party_id ON players_private (party_id);
CREATE INDEX IF NOT EXISTS idx_players_private_last_seen ON players_private (last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_players_private_cheater ON players_private (last_seen DESC, id DESC) WHERE cheater;
CREATE INDEX IF NOT EXISTS idx_players_private_suspicious ON players_private (sus_count DESC, last_seen DESC, id DESC) WHERE sus_count > 0;
COMMENT ON TABLE players_private IS 'Tracks individual private account players differentiated by party_id and account attributes';

CREATE TABLE IF NOT EXISTS players_private_history (
    id                  SERIAL PRIMARY KEY,
    player_private_id   INTEGER NOT NULL REFERENCES players_private(id),
    party_id            INTEGER NOT NULL,
    account_level       INTEGER NOT NULL,
    mastery_level       INTEGER NOT NULL,
    league_tier         INTEGER NOT NULL,
    league_points       INTEGER NOT NULL,
    match_id            BIGINT,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pph_player_private_id ON players_private_history (player_private_id);
CREATE INDEX IF NOT EXISTS idx_pph_recorded_at ON players_private_history (recorded_at DESC);
COMMENT ON TABLE players_private_history IS 'Audit trail of attribute changes for private players over time';

-- find_or_create_private_player function
CREATE OR REPLACE FUNCTION find_or_create_private_player(
    p_party_id        INTEGER,
    p_account_level   INTEGER,
    p_mastery_level   INTEGER,
    p_league_tier     INTEGER,
    p_league_points   INTEGER,
    p_entry_datetime  TIMESTAMP WITH TIME ZONE
) RETURNS INTEGER AS $$
DECLARE
    v_id INTEGER;
BEGIN
    SELECT id INTO v_id FROM players_private
    WHERE party_id = p_party_id AND account_level = p_account_level
      AND mastery_level = p_mastery_level AND league_tier = p_league_tier
      AND league_points = p_league_points;
    IF v_id IS NOT NULL THEN
        UPDATE players_private SET last_seen = p_entry_datetime, match_count = match_count + 1,
            last_known_level = p_account_level, last_known_mastery = p_mastery_level,
            last_known_league_tier = p_league_tier, last_known_league_points = p_league_points,
            updated_at = NOW() WHERE id = v_id;
        INSERT INTO players_private_history (player_private_id, party_id, account_level,
            mastery_level, league_tier, league_points, recorded_at)
        VALUES (v_id, p_party_id, p_account_level, p_mastery_level, p_league_tier, p_league_points, p_entry_datetime);
        RETURN v_id;
    END IF;
    SELECT id INTO v_id FROM players_private WHERE party_id = p_party_id ORDER BY last_seen DESC LIMIT 1;
    IF v_id IS NOT NULL THEN
        UPDATE players_private SET account_level = p_account_level, mastery_level = p_mastery_level,
            league_tier = p_league_tier, league_points = p_league_points,
            last_known_level = p_account_level, last_known_mastery = p_mastery_level,
            last_known_league_tier = p_league_tier, last_known_league_points = p_league_points,
            last_seen = p_entry_datetime, match_count = match_count + 1, updated_at = NOW() WHERE id = v_id;
        INSERT INTO players_private_history (player_private_id, party_id, account_level,
            mastery_level, league_tier, league_points, recorded_at)
        VALUES (v_id, p_party_id, p_account_level, p_mastery_level, p_league_tier, p_league_points, p_entry_datetime);
        RETURN v_id;
    END IF;
    INSERT INTO players_private (party_id, account_level, mastery_level, league_tier, league_points,
        last_known_level, last_known_mastery, last_known_league_tier, last_known_league_points,
        first_seen, last_seen, match_count, alias)
    VALUES (p_party_id, p_account_level, p_mastery_level, p_league_tier, p_league_points,
        p_account_level, p_mastery_level, p_league_tier, p_league_points,
        p_entry_datetime, p_entry_datetime, 1, 'Private-' || LPAD(p_party_id::TEXT, 5, '0'))
    RETURNING id INTO v_id;
    INSERT INTO players_private_history (player_private_id, party_id, account_level,
        mastery_level, league_tier, league_points, recorded_at)
    VALUES (v_id, p_party_id, p_account_level, p_mastery_level, p_league_tier, p_league_points, p_entry_datetime);
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION find_or_create_private_player IS 'Upsert helper: finds existing private player by party_id or creates new one';



-- ============================================================================
-- 7. SKINS - 3NF: champion_id, skin_id, skin_name
-- ============================================================================

CREATE TABLE IF NOT EXISTS skins (
    skin_id         INT PRIMARY KEY,
    champion_id     INT REFERENCES champions(id),
    skin_name       VARCHAR(200)
);
CREATE INDEX IF NOT EXISTS idx_skins_champion_id ON skins (champion_id);
COMMENT ON TABLE skins IS '3NF: champion_id, skin_id, skin_name only. See broken_skins for Int16 overflow tracking.';

-- ============================================================================
-- 7b. CARDS - Static game card reference
-- ============================================================================

CREATE TABLE IF NOT EXISTS cards (
    card_id         INT PRIMARY KEY,
    card_name       VARCHAR(200),
    champion_id     INT -- FK removed: champions seeded at runtime by app, not in migrations
);
CREATE INDEX IF NOT EXISTS idx_cards_champion_id ON cards (champion_id);
COMMENT ON TABLE cards IS 'Static game card reference table. Do NOT store player data here.';

-- ============================================================================
-- 8. BROKEN SKINS - Recovery tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS broken_skins (
    id              BIGSERIAL PRIMARY KEY,
    champion_id     INT NOT NULL, -- FK removed: champions seeded at runtime by app, not in migrations
    champion_name   VARCHAR(100) NOT NULL,
    skin_id         INT NOT NULL,
    skin_name       VARCHAR(200) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_broken_skins UNIQUE (champion_id, skin_id)
);
CREATE INDEX IF NOT EXISTS idx_broken_skins_champion_id ON broken_skins (champion_id);
CREATE INDEX IF NOT EXISTS idx_broken_skins_skin_id ON broken_skins (skin_id);
COMMENT ON TABLE broken_skins IS 'Skins with known Int16 overflow errors in Hi-Rez API';

-- ============================================================================
-- AUTO-POPULATE SKINS & BROKEN_SKINS
-- ============================================================================
--
-- Skins are auto-populated from match_players during ingestion (buffer-processor.ts).
-- Normal skins (skin_id <= 32767) → skins table
-- Broken skins (skin_id > 32767, Int16 overflow) → broken_skins table
--
-- One-time backfill (run once after initial data ingestion):
--
-- INSERT INTO skins (champion_id, skin_id, skin_name)
--   SELECT DISTINCT mp.champion_id, mp.skin_id, mp.skin_name
--   FROM match_players mp
--   WHERE mp.skin_id IS NOT NULL AND mp.champion_id IS NOT NULL AND mp.skin_id <= 32767
--   ON CONFLICT (skin_id) DO NOTHING;
--
-- INSERT INTO broken_skins (champion_id, champion_name, skin_id, skin_name)
--   SELECT DISTINCT mp.champion_id, c.name, mp.skin_id, mp.skin_name
--   FROM match_players mp
--   JOIN champions c ON c.id = mp.champion_id
--   WHERE mp.skin_id > 32767
--   ON CONFLICT ON CONSTRAINT uq_broken_skins DO NOTHING;
--
-- Ongoing: handled in buffer-processor.ts → autoPopulateSkins() after each batch.
--

-- broken_skins seed data moved to 004_seed_data.sql (FK to champions,
-- which is seeded by the app at runtime, not by migrations).

-- Unknown champion placeholder
INSERT INTO champions (id, name, title, health, speed, roles)
VALUES (0, 'Unknown', 'Unknown Champion', 0, 0, 'unknown')
ON CONFLICT (id) DO NOTHING;

-- ============================================================================
-- 9. RECOVERY STATS - Broken-skin recovery details
-- ============================================================================

CREATE TABLE IF NOT EXISTS recovery_stats (
    match_id          BIGINT PRIMARY KEY,
    dev_id            VARCHAR(10),
    players           INT NOT NULL,
    direct_count      INT NOT NULL DEFAULT 0,
    recovered_count   INT NOT NULL DEFAULT 0,
    missing_count     INT NOT NULL DEFAULT 0,
    api_calls         INT NOT NULL DEFAULT 0,
    total_calls       INT NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE recovery_stats IS 'Per-match recovery details - only for recovered matches';

-- ============================================================================
-- 10. API KEY MANAGEMENT
-- ============================================================================

CREATE TABLE IF NOT EXISTS api_keys (
    id                SERIAL PRIMARY KEY,
    dev_id            VARCHAR NOT NULL,
    auth_key          VARCHAR NOT NULL,
    source            VARCHAR,
    status            VARCHAR DEFAULT 'healthy',
    calls_today       INT DEFAULT 0,
    total_24h         INT DEFAULT 0,
    daily_limit       INT DEFAULT 7500,
    calls_total       INT DEFAULT 0,
    consecutive_failures INT DEFAULT 0,
    last_health_check TIMESTAMPTZ,
    last_used         TIMESTAMPTZ,
    last_sync_at      TIMESTAMPTZ,
    last_sync_error   TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE api_keys IS 'HirezRelay API key pool. 2116 is capped at 15000 calls/day; all other keys default to 7500 with a 100-call reserve.';
COMMENT ON COLUMN api_keys.status IS 'healthy / limited / unhealthy / exhausted';
CREATE INDEX IF NOT EXISTS idx_api_keys_status ON api_keys (status);
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_api_keys_dev_id'
    ) THEN
        ALTER TABLE api_keys ADD CONSTRAINT uq_api_keys_dev_id UNIQUE (dev_id);
    END IF;
END
$$;

-- Privacy-minimal first-party website traffic counters
CREATE TABLE IF NOT EXISTS site_daily_visitors (
    visit_date    DATE NOT NULL,
    visitor_hash TEXT NOT NULL,
    page_views    INT NOT NULL DEFAULT 1,
    first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (visit_date, visitor_hash)
);
CREATE INDEX IF NOT EXISTS idx_site_daily_visitors_date
    ON site_daily_visitors (visit_date DESC);
CREATE INDEX IF NOT EXISTS idx_site_daily_visitors_live_sessions
    ON site_daily_visitors (visit_date, last_seen DESC);
COMMENT ON TABLE site_daily_visitors IS 'Daily unique anonymous browser hashes and page-view totals; no raw visitor identifiers or IP addresses.';

CREATE TABLE IF NOT EXISTS site_daily_page_views (
    visit_date DATE NOT NULL,
    path       TEXT NOT NULL,
    page_views INT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (visit_date, path)
);
CREATE INDEX IF NOT EXISTS idx_site_daily_page_views_date_views
    ON site_daily_page_views (visit_date DESC, page_views DESC);
COMMENT ON TABLE site_daily_page_views IS 'Daily normalized public route view totals for the private admin dashboard.';

-- ============================================================================
-- 11. MATCH PIPELINE - Pull list & sync jobs
-- ============================================================================

-- match_pull_list - Staging table for match ingestion pipeline
CREATE TABLE IF NOT EXISTS match_pull_list (
    match_id          BIGINT PRIMARY KEY,
    queue_id          INT NOT NULL,
    entry_datetime    TIMESTAMPTZ,
    status            VARCHAR NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'pulling', 'completed'))
);
CREATE INDEX IF NOT EXISTS idx_mpl_queue_id ON match_pull_list (queue_id);
CREATE INDEX IF NOT EXISTS idx_mpl_status ON match_pull_list (status);
CREATE INDEX IF NOT EXISTS idx_mpl_pending_claim ON match_pull_list (match_id) WHERE status = 'pending';
COMMENT ON TABLE match_pull_list IS 'Staging table for match ingestion - rows deleted after successful ingestion';

-- sync_jobs - Track background sync job status
CREATE TABLE IF NOT EXISTS sync_jobs (
    id                SERIAL PRIMARY KEY,
    job_type          VARCHAR(50) NOT NULL,
    status            VARCHAR(20) DEFAULT 'pending',
    queue_id          INT,
    region            VARCHAR(50),
    date              DATE,
    hour              INT,
    matches_processed INT DEFAULT 0,
    players_processed INT DEFAULT 0,
    error_message     TEXT,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE sync_jobs IS 'Background sync job tracking';
CREATE INDEX IF NOT EXISTS idx_sync_jobs_job_type ON sync_jobs (job_type);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_status ON sync_jobs (status);
CREATE INDEX IF NOT EXISTS idx_sync_jobs_started_at ON sync_jobs (started_at DESC);

-- ============================================================================
-- 12. AUTH, COMMUNITY, BUILDS
-- ============================================================================

CREATE TABLE IF NOT EXISTS users (
    id              SERIAL PRIMARY KEY,
    username        VARCHAR(50) NOT NULL,
    email           VARCHAR(255) NOT NULL,
    password_hash   VARCHAR(255) NOT NULL,
    salt            VARCHAR(64) NOT NULL DEFAULT '',
    avatar_url      TEXT,
    bio             TEXT,
    time_zone       VARCHAR(64),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    is_admin        BOOLEAN NOT NULL DEFAULT FALSE,
    is_approved     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users (username);
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email);
COMMENT ON TABLE users IS 'User accounts for auth, community posts, and builds';

CREATE TABLE IF NOT EXISTS private_account_community_votes (
    id                  BIGSERIAL PRIMARY KEY,
    private_player_id   INTEGER NOT NULL REFERENCES players_private(id) ON DELETE CASCADE,
    user_id             INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    vote_type           VARCHAR(20) NOT NULL CHECK (vote_type IN ('suspicious', 'cheater')),
    reason              TEXT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_private_account_community_vote UNIQUE (private_player_id, user_id, vote_type)
);
CREATE INDEX IF NOT EXISTS idx_private_account_votes_target ON private_account_community_votes (private_player_id, vote_type, created_at DESC);

-- schema_migrations - Immutable production migration ledger
CREATE TABLE IF NOT EXISTS schema_migrations (
    version         TEXT PRIMARY KEY,
    file_name       TEXT NOT NULL UNIQUE,
    checksum_sha256 TEXT NOT NULL,
    git_commit      TEXT,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    execution_ms    INTEGER NOT NULL
);
COMMENT ON TABLE schema_migrations IS 'Applied forward-only SQL migrations with immutable checksums and deploy provenance';

-- player_link_verifications - One-time ownership proof via a renamed Paladins loadout
CREATE TABLE IF NOT EXISTS player_link_verifications (
    user_id     INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    player_id   BIGINT NOT NULL,
    code        VARCHAR(24) NOT NULL UNIQUE,
    expires_at  TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_player_link_verifications_expires ON player_link_verifications (expires_at);
COMMENT ON TABLE player_link_verifications IS 'Temporary Paladins account ownership challenges verified by a renamed loadout.';

CREATE TABLE IF NOT EXISTS sessions (
    id              SERIAL PRIMARY KEY,
    user_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token           VARCHAR NOT NULL,
    device          VARCHAR,
    ip_address      VARCHAR,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions (user_id);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions (token);
CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions (expires_at);
COMMENT ON TABLE sessions IS 'Auth session storage - token-based with expiration';

CREATE TABLE IF NOT EXISTS posts (
    id              SERIAL PRIMARY KEY,
    user_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title           VARCHAR(300) NOT NULL,
    content         TEXT NOT NULL,
    build_id        INT,
    likes           INT NOT NULL DEFAULT 0,
    view_count      INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts (user_id);
CREATE INDEX IF NOT EXISTS idx_posts_created_at ON posts (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_build_id ON posts (build_id);
COMMENT ON TABLE posts IS 'Community posts - can reference builds for rich embeds';

CREATE TABLE IF NOT EXISTS comments (
    id              SERIAL PRIMARY KEY,
    post_id         INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_id       INT REFERENCES comments(id) ON DELETE CASCADE,
    content         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments (post_id);
CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments (parent_id);
COMMENT ON TABLE comments IS 'Post comments - parent_id enables nested replies';

CREATE TABLE IF NOT EXISTS user_post_likes (
    user_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    post_id         INT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, post_id)
);
COMMENT ON TABLE user_post_likes IS 'User→post like relationship - prevents duplicate likes';

CREATE TABLE IF NOT EXISTS builds (
    id              SERIAL PRIMARY KEY,
    user_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    champion_id     INT NOT NULL REFERENCES champions(id),
    name            VARCHAR(200) NOT NULL,
    items           INT[] NOT NULL DEFAULT '{}',
    cards           JSONB NOT NULL DEFAULT '[]'::jsonb,
    actives         INT[] NOT NULL DEFAULT '{}',
    talents         INT[] NOT NULL DEFAULT '{}',
    notes           TEXT,
    visibility      VARCHAR(10) NOT NULL DEFAULT 'public' CHECK (visibility IN ('public', 'private')),
    likes           INT NOT NULL DEFAULT 0,
    view_count      INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_builds_user_id ON builds (user_id);
CREATE INDEX IF NOT EXISTS idx_builds_champion_id ON builds (champion_id);
CREATE INDEX IF NOT EXISTS idx_builds_visibility ON builds (visibility);
CREATE INDEX IF NOT EXISTS idx_builds_created_at ON builds (created_at DESC);
COMMENT ON TABLE builds IS 'Shared deck builds. items/talents remain integer ID arrays; cards stores [{card_id, level}] so the 5-card / 15-point loadout can be reconstructed exactly.';

CREATE TABLE IF NOT EXISTS user_build_likes (
    user_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    build_id        INT NOT NULL REFERENCES builds(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, build_id)
);
COMMENT ON TABLE user_build_likes IS 'User→build like relationship - prevents duplicate likes';

CREATE TABLE IF NOT EXISTS notifications (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    importance  INT NOT NULL DEFAULT 0,
    message     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notifications_feed ON notifications (importance DESC, timestamp DESC, id DESC);
COMMENT ON TABLE notifications IS 'Operator-managed homepage notifications.';
INSERT INTO notifications (timestamp, importance, message)
SELECT now(), 100, 'Database-backed notifications are live.'
WHERE NOT EXISTS (SELECT 1 FROM notifications);

CREATE TABLE IF NOT EXISTS site_notification_reads (
    user_id         INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    notification_id INT NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    read_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, notification_id)
);
CREATE INDEX IF NOT EXISTS idx_site_notification_reads_user ON site_notification_reads (user_id, read_at DESC);
COMMENT ON TABLE site_notification_reads IS 'Per-account read state for operator-managed site notifications.';

CREATE TABLE IF NOT EXISTS site_versions (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ NOT NULL DEFAULT now(),
    version     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_site_versions_latest ON site_versions (timestamp DESC, id DESC);
COMMENT ON TABLE site_versions IS 'Operator-managed site/application version history for public footer display.';
INSERT INTO site_versions (timestamp, version)
SELECT now(), 'v1.0.0-beta'
WHERE NOT EXISTS (SELECT 1 FROM site_versions);
CREATE TABLE IF NOT EXISTS stack_versions (
    id                SERIAL PRIMARY KEY,
    component         TEXT NOT NULL DEFAULT 'stack',
    environment       TEXT NOT NULL DEFAULT 'production',
    version           TEXT NOT NULL DEFAULT 'v1.0.0-beta',
    git_commit        TEXT,
    git_commit_short  TEXT,
    git_branch        TEXT,
    git_dirty         BOOLEAN NOT NULL DEFAULT FALSE,
    build_timestamp   TIMESTAMPTZ,
    deployed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    db_schema_version TEXT,
    source            TEXT NOT NULL DEFAULT 'manual',
    notes             TEXT,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_stack_versions_latest ON stack_versions (environment, component, deployed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_stack_versions_commit ON stack_versions (git_commit);
COMMENT ON TABLE stack_versions IS 'Deployment/version provenance for the PaladinsCat stack, including Git commit, component, environment, and DB schema stamp.';
INSERT INTO stack_versions (component, environment, version, db_schema_version, source, metadata)
SELECT 'stack', 'local', 'v1.0.0-beta', '036_stack_versions', 'schema_bootstrap', '{}'::jsonb
WHERE NOT EXISTS (SELECT 1 FROM stack_versions);

-- ============================================================================
-- 13. CHARTS & ANALYTICS - Chart data tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS global_match_stats (
    id              SERIAL PRIMARY KEY,
    entry_date      DATE NOT NULL,
    avg_kills       NUMERIC NOT NULL DEFAULT 0,
    avg_deaths      NUMERIC NOT NULL DEFAULT 0,
    avg_assists     NUMERIC NOT NULL DEFAULT 0,
    avg_dpm         NUMERIC NOT NULL DEFAULT 0,
    avg_hpm         NUMERIC NOT NULL DEFAULT 0,
    total_matches   INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_global_match_stats_date ON global_match_stats (entry_date);
COMMENT ON TABLE global_match_stats IS 'Daily aggregated stats for player comparison charts';

-- ============================================================================
-- 14. LEADERBOARD - Per-tier leaderboard tables (0-26)
-- ============================================================================

-- Generate all 27 leaderboard tables (tier 0-26)
DO $$
DECLARE
    tier INT;
    tbl TEXT;
BEGIN
    FOR tier IN 0..26 LOOP
        tbl := 'leaderboard' || tier;
        EXECUTE format('CREATE TABLE IF NOT EXISTS %I (
            player_id     BIGINT PRIMARY KEY,
            name          VARCHAR(100),
            points        INT NOT NULL DEFAULT 0,
            rank          INT NOT NULL DEFAULT 1000,
            prev_rank     INT NOT NULL DEFAULT 1000,
            wins          INT NOT NULL DEFAULT 0,
            losses        INT NOT NULL DEFAULT 0,
            leaves        INT NOT NULL DEFAULT 0,
            trend         INT NOT NULL DEFAULT 0,
            season        INT NOT NULL,
            tier          INT NOT NULL DEFAULT %s,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )', tbl, tier);
        EXECUTE format('CREATE INDEX IF NOT EXISTS idx_lb%s_points ON %I (points DESC)', tier, tbl);
    END LOOP;
END
$$;
COMMENT ON TABLE leaderboard26 IS 'League leaderboard tables - one per tier (0=Unranked, 26=Master)';

-- ============================================================================
-- 15. MATERIALIZED VIEWS - Aggregated stats
-- ============================================================================

-- champion_meta_stats - Aggregate champion performance
-- Outcome labels in match_players can be Winner/Loser or Win/Loss depending on
-- whether rows came from direct match details or recovery/history. Every view
-- below normalizes both spellings and divides win rate by wins + losses.
CREATE MATERIALIZED VIEW IF NOT EXISTS champion_meta_stats AS
SELECT mp.champion_id, c.name AS champion_name,
    COUNT(DISTINCT mp.match_id) AS total_matches, COUNT(mp.player_id) AS total_plays,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')) AS wins,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')) AS losses,
    ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::NUMERIC / NULLIF((COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')) + COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')))::NUMERIC, 0), 2) AS win_rate,
    ROUND(AVG(mp.kills)::NUMERIC, 2) AS avg_kills,
    ROUND(AVG(mp.deaths)::NUMERIC, 2) AS avg_deaths,
    ROUND(AVG(mp.assists)::NUMERIC, 2) AS avg_assists,
    ROUND(AVG(mp.damage_done_physical)::NUMERIC, 2) AS avg_damage,
    ROUND(AVG(mp.gold_earned)::NUMERIC, 2) AS avg_gold,
    ROUND(AVG(mp.league_tier)::NUMERIC, 2) AS avg_league_tier,
    now() AS last_refreshed
FROM match_players mp
JOIN champions c ON c.id = mp.champion_id
GROUP BY mp.champion_id, c.name;
CREATE INDEX IF NOT EXISTS idx_champion_meta_champion_id ON champion_meta_stats (champion_id);
CREATE INDEX IF NOT EXISTS idx_champion_meta_win_rate ON champion_meta_stats (win_rate DESC);
COMMENT ON MATERIALIZED VIEW champion_meta_stats IS 'Aggregated champion performance - REFRESH MATERIALIZED VIEW CONCURRENTLY hourly';

-- player_rankings - Top players by various metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS player_rankings AS
SELECT p.id AS player_id, p.name AS player_name, p.kbm_tier, p.kbm_points,
    p.wins AS total_wins, (p.wins + p.losses) AS total_matches,
    ROUND(100.0 * p.wins::NUMERIC / NULLIF((p.wins + p.losses), 0), 2) AS win_rate,
    p.hours_played,
    RANK() OVER (ORDER BY p.kbm_tier DESC, p.kbm_points DESC) AS rank_position,
    now() AS last_updated
FROM players p
WHERE p.kbm_tier > 0;
CREATE INDEX IF NOT EXISTS idx_player_rankings_position ON player_rankings (rank_position);
CREATE INDEX IF NOT EXISTS idx_player_rankings_kbm_tier ON player_rankings (kbm_tier DESC);
COMMENT ON MATERIALIZED VIEW player_rankings IS 'Top player rankings - REFRESH MATERIALIZED VIEW CONCURRENTLY daily';

-- daily_match_stats - Continuous aggregate for daily match counts
DROP MATERIALIZED VIEW IF EXISTS daily_match_stats;
CREATE MATERIALIZED VIEW daily_match_stats
WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', entry_datetime) AS stat_date, queue_id, region,
    COUNT(*)::BIGINT AS match_count,
    ROUND(AVG(duration_seconds)::NUMERIC, 2) AS avg_duration
FROM matches
GROUP BY time_bucket('1 day', entry_datetime), queue_id, region;
CREATE INDEX IF NOT EXISTS idx_daily_match_stats_date ON daily_match_stats (stat_date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_match_stats_queue ON daily_match_stats (queue_id);
-- NOTE: daily_match_stats is a TimescaleDB continuous aggregate — COMMENT ON TABLE/MATERIALIZED VIEW both fail on pg18.
-- Comment left as inline documentation only.

-- counter_pick_stats - Counter-pick matchup stats
CREATE MATERIALIZED VIEW IF NOT EXISTS counter_pick_stats AS
SELECT mp.champion_id AS attacker_champion_id, c.name AS attacker_champion_name,
    mp2.champion_id AS opponent_champion_id, c2.name AS opponent_champion_name,
    COUNT(DISTINCT mp.match_id) AS total_matchups, COUNT(*) AS total_encounters,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')) AS wins,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')) AS losses,
    ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::NUMERIC / NULLIF((COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')) + COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')))::NUMERIC, 0), 2) AS win_rate,
    ROUND(AVG(mp.kills)::NUMERIC, 2) AS avg_kills,
    ROUND(AVG(mp.deaths)::NUMERIC, 2) AS avg_deaths,
    ROUND(AVG(mp.damage_done_physical / GREATEST(m.duration_seconds / 60.0, 1))::NUMERIC, 2) AS avg_dpm
FROM match_players mp
JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
JOIN champions c ON c.id = mp.champion_id
JOIN match_players mp2 ON mp2.match_id = mp.match_id AND mp2.task_force != mp.task_force
JOIN champions c2 ON c2.id = mp2.champion_id
WHERE mp.is_ranked = true
GROUP BY mp.champion_id, c.name, mp2.champion_id, c2.name;
CREATE INDEX IF NOT EXISTS idx_counter_pick_attacker ON counter_pick_stats (attacker_champion_id);
CREATE INDEX IF NOT EXISTS idx_counter_pick_opponent ON counter_pick_stats (opponent_champion_id);
COMMENT ON MATERIALIZED VIEW counter_pick_stats IS 'Counter-pick matchup stats - REFRESH MATERIALIZED VIEW CONCURRENTLY hourly';

-- ============================================================================
-- 16. ADDITIONAL MATERIALIZED VIEWS - Aggregated stats
-- ============================================================================

-- champion_quick_stats - Quick champion performance
CREATE MATERIALIZED VIEW IF NOT EXISTS champion_quick_stats AS
SELECT champion_id, champion_name, total_plays, wins, losses, win_rate, avg_kills, avg_deaths, avg_assists, avg_damage, avg_gold
FROM champion_meta_stats
ORDER BY total_plays DESC;
CREATE INDEX IF NOT EXISTS idx_champion_quick_plays ON champion_quick_stats (total_plays DESC);
COMMENT ON MATERIALIZED VIEW champion_quick_stats IS 'Quick champion performance - REFRESH MATERIALIZED VIEW CONCURRENTLY hourly';

-- player_match_stats - Per-player match aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS player_match_stats AS
SELECT player_id, COUNT(*) AS total_matches,
    COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win')) AS wins,
    COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('loser', 'loss')) AS losses,
    ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC / NULLIF((COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win')) + COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('loser', 'loss')))::NUMERIC, 0), 2) AS win_rate,
    ROUND(AVG(kills)::NUMERIC, 2) AS avg_kills,
    ROUND(AVG(deaths)::NUMERIC, 2) AS avg_deaths,
    ROUND(AVG(assists)::NUMERIC, 2) AS avg_assists,
    ROUND(AVG(damage_done_physical)::NUMERIC, 2) AS avg_damage,
    ROUND(AVG(gold_earned)::NUMERIC, 2) AS avg_gold
FROM match_players
GROUP BY player_id;
CREATE INDEX IF NOT EXISTS idx_player_match_stats_player ON player_match_stats (player_id);
CREATE INDEX IF NOT EXISTS idx_player_match_stats_win_rate ON player_match_stats (win_rate DESC);
COMMENT ON MATERIALIZED VIEW player_match_stats IS 'Per-player match aggregates - REFRESH MATERIALIZED VIEW CONCURRENTLY daily';

-- queue_stats - Per-queue match aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS queue_stats AS
SELECT queue_id, COUNT(*) AS total_matches,
    ROUND(AVG(duration_seconds)::NUMERIC, 2) AS avg_duration,
    ROUND(AVG(kills)::NUMERIC, 2) AS avg_kills,
    ROUND(AVG(deaths)::NUMERIC, 2) AS avg_deaths,
    ROUND(AVG(assists)::NUMERIC, 2) AS avg_assists
FROM match_players mp
JOIN matches m ON m.match_id = mp.match_id
GROUP BY queue_id;
CREATE INDEX IF NOT EXISTS idx_queue_stats_queue ON queue_stats (queue_id);
COMMENT ON MATERIALIZED VIEW queue_stats IS 'Per-queue match aggregates - REFRESH MATERIALIZED VIEW CONCURRENTLY daily';

-- region_stats - Per-region match aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS region_stats AS
SELECT m.region AS region, COUNT(DISTINCT m.match_id) AS total_matches,
    ROUND(AVG(m.duration_seconds)::NUMERIC, 2) AS avg_duration,
    ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::NUMERIC / NULLIF((COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')) + COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')))::NUMERIC, 0), 2) AS win_rate
FROM match_players mp
JOIN matches m ON m.match_id = mp.match_id
GROUP BY m.region;
CREATE INDEX IF NOT EXISTS idx_region_stats_region ON region_stats (region);
COMMENT ON MATERIALIZED VIEW region_stats IS 'Per-region match aggregates - REFRESH MATERIALIZED VIEW CONCURRENTLY daily';

-- mv_tier_match_stats - Per-tier match aggregates (renamed from tier_stats to avoid collision with tier_stats table)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_tier_match_stats AS
SELECT league_tier, COUNT(*) AS total_plays, COUNT(DISTINCT player_id) AS unique_players,
    ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC / NULLIF((COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win')) + COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('loser', 'loss')))::NUMERIC, 0), 2) AS win_rate
FROM match_players
WHERE league_tier > 0
GROUP BY league_tier;
CREATE INDEX IF NOT EXISTS idx_mv_tier_match_stats_tier ON mv_tier_match_stats (league_tier);
COMMENT ON MATERIALIZED VIEW mv_tier_match_stats IS 'Per-tier match aggregates - REFRESH MATERIALIZED VIEW CONCURRENTLY daily';

-- item_stats - Per-item match aggregates
CREATE MATERIALIZED VIEW IF NOT EXISTS item_stats AS
SELECT mpi.item_id, i.item_name, COUNT(*) AS total_uses,
    ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC / NULLIF((COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win')) + COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('loser', 'loss')))::NUMERIC, 0), 2) AS win_rate,
    ROUND(AVG(kills)::NUMERIC, 2) AS avg_kills,
    ROUND(AVG(deaths)::NUMERIC, 2) AS avg_deaths,
    ROUND(AVG(assists)::NUMERIC, 2) AS avg_assists
FROM match_player_items mpi
JOIN items i ON i.item_id = mpi.item_id
JOIN match_players mp ON mp.match_id = mpi.match_id AND mp.player_id = mpi.player_id
GROUP BY mpi.item_id, i.item_name;
CREATE INDEX IF NOT EXISTS idx_item_stats_item ON item_stats (item_id);
COMMENT ON MATERIALIZED VIEW item_stats IS 'Per-item match aggregates - REFRESH MATERIALIZED VIEW CONCURRENTLY daily';


-- championsquick - Quick champion lookup
CREATE TABLE IF NOT EXISTS championsquick (
    id              INT PRIMARY KEY,
    ability1_id     INT,
    ability2_id     INT,
    ability3_id     INT,
    ability4_id     INT,
    ability5_id     INT
);
COMMENT ON TABLE championsquick IS 'Quick champion ability lookup';

-- ============================================================================
-- 17. AUTO INGESTER - Active match tracking
-- ===========================================================================

-- hourly_match_counts - Wide-format hourly match counts per queue
-- Regions match the codes surfaced by the ingest worker. Keep matches_asia as
-- a legacy rollup column, but SEA/JPN/RUS are explicit because Hi-Rez payloads
-- can surface them separately and buffer-processor writes those columns.
CREATE TABLE IF NOT EXISTS hourly_match_counts (
    date              DATE NOT NULL,
    hour              INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
    queue_id          INT NOT NULL REFERENCES queue_types(queue_id),
    matches_na        INT NOT NULL DEFAULT 0,
    matches_eu        INT NOT NULL DEFAULT 0,
    matches_asia      INT NOT NULL DEFAULT 0,
    matches_sea       INT NOT NULL DEFAULT 0,
    matches_jpn       INT NOT NULL DEFAULT 0,
    matches_rus       INT NOT NULL DEFAULT 0,
    matches_br        INT NOT NULL DEFAULT 0,
    matches_oce       INT NOT NULL DEFAULT 0,
    matches_sa        INT NOT NULL DEFAULT 0,
    matches_unknown   INT NOT NULL DEFAULT 0,
    total_matches     INT NOT NULL DEFAULT 0,
    fetched_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (date, hour, queue_id)
);
CREATE INDEX IF NOT EXISTS idx_hmc_date_queue ON hourly_match_counts (date, queue_id);
CREATE INDEX IF NOT EXISTS idx_hmc_queue_date ON hourly_match_counts (queue_id, date DESC);
COMMENT ON TABLE hourly_match_counts IS 'Wide-format hourly match counts per queue - auto ingester. Regions match Hi-Rez API: NA, EU, SEA, JPN, RUS, BR, OCE, SA, Unknown; matches_asia is retained for legacy rollups.';

-- hourly_ingest_state - Scheduler control state for hourly ranked-match ingest.
-- This table is intentionally separate from hourly_match_counts. A zero count
-- can be a real empty hour, a temporary Hi-Rez outage, or a still-draining
-- buffer; status/lease/retry fields below carry that distinction.
CREATE TABLE IF NOT EXISTS hourly_ingest_state (
    date                DATE NOT NULL,
    hour                INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
    queue_id            INT NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'pending'
      CHECK (status IN ('pending', 'fetching', 'staged', 'empty', 'complete', 'failed')),
    attempts            INT NOT NULL DEFAULT 0,
    raw_match_count     INT NOT NULL DEFAULT 0,
    staged_match_count  INT NOT NULL DEFAULT 0,
    fetched             BOOLEAN NOT NULL DEFAULT FALSE,
    fetch_succeeded     BOOLEAN NOT NULL DEFAULT FALSE,
    source              VARCHAR(50),
    error_message       TEXT,
    last_attempt_at     TIMESTAMPTZ,
    next_retry_at       TIMESTAMPTZ,
    lease_until         TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, hour, queue_id)
);
CREATE INDEX IF NOT EXISTS idx_his_status_retry ON hourly_ingest_state (status, next_retry_at, lease_until);
CREATE INDEX IF NOT EXISTS idx_his_queue_window ON hourly_ingest_state (queue_id, date, hour);
COMMENT ON TABLE hourly_ingest_state IS 'Scheduler control state for hourly match ingest. Keeps missing/empty/fetching/staged/complete distinct from hourly_match_counts analytics rows.';

-- hourly_ingest_match_debt - Per-match recovery debt for hourly ranked ingest.
-- Hour-level counts are not enough when Hi-Rez returns a partial batch. This
-- ledger stores every discovered match ID until the buffer worker marks that
-- match complete, so a "28 discovered / 19 staged" hour cannot lose the 9
-- unresolved IDs after a restart, retention cleanup, or changed API response.
CREATE TABLE IF NOT EXISTS hourly_ingest_match_debt (
    match_id            BIGINT PRIMARY KEY,
    date                DATE NOT NULL,
    hour                INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
    queue_id            INT NOT NULL,
    status              VARCHAR(20) NOT NULL DEFAULT 'pending'
      CHECK (status IN ('pending', 'staged', 'complete', 'unrecoverable')),
    reason              TEXT,
    attempts            INT NOT NULL DEFAULT 0,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_attempt_at     TIMESTAMPTZ,
    next_retry_at       TIMESTAMPTZ,
    staged_at           TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_himd_queue_window_status ON hourly_ingest_match_debt (queue_id, date, hour, status);
CREATE INDEX IF NOT EXISTS idx_himd_pending_retry ON hourly_ingest_match_debt (status, next_retry_at, updated_at) WHERE status = 'pending';
COMMENT ON TABLE hourly_ingest_match_debt IS 'Per-match recovery debt for hourly ranked discovery. A discovered ID remains pending/staged until the buffer worker marks match_ingest_status complete.';

-- ID-only global match counts. Ranked queue 486 mirrors the IDs fetched by the
-- full ingest worker; other queues stop here and never enter ranked projections.
CREATE TABLE IF NOT EXISTS match_count_discoveries (
    match_id         BIGINT NOT NULL,
    queue_id         INT NOT NULL,
    region           VARCHAR(20) NOT NULL DEFAULT 'Unknown',
    entry_datetime   TIMESTAMPTZ,
    active_flag      BOOLEAN NOT NULL DEFAULT FALSE,
    source_date      DATE NOT NULL,
    source_hour      INT NOT NULL CHECK (source_hour BETWEEN 0 AND 23),
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (match_id, queue_id)
);
CREATE INDEX IF NOT EXISTS idx_mcd_window_queue ON match_count_discoveries (source_date DESC, source_hour, queue_id);
CREATE INDEX IF NOT EXISTS idx_mcd_queue_region_window ON match_count_discoveries (queue_id, region, source_date DESC, source_hour);

CREATE TABLE IF NOT EXISTS match_count_discovery_region_hours (
    date             DATE NOT NULL,
    hour             INT NOT NULL CHECK (hour BETWEEN 0 AND 23),
    queue_id         INT NOT NULL,
    region           VARCHAR(20) NOT NULL,
    match_count      INT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (date, hour, queue_id, region)
);
CREATE INDEX IF NOT EXISTS idx_mcdrh_window_queue ON match_count_discovery_region_hours (date DESC, hour, queue_id);

-- ============================================================================
-- 18. LIVE MATCH TRACKER
-- ===========================================================================

-- live_matches - Current live match tracking
CREATE TABLE IF NOT EXISTS live_matches (
    match_id          BIGINT PRIMARY KEY,
    queue_id          INT NOT NULL,
    region            VARCHAR NOT NULL,
    map               VARCHAR(100),
    detected_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at          TIMESTAMPTZ,
    status            VARCHAR NOT NULL DEFAULT 'active',
    dropped           BOOLEAN NOT NULL DEFAULT FALSE,
    ingested          BOOLEAN NOT NULL DEFAULT FALSE,
    source_player_id  BIGINT
);
CREATE INDEX IF NOT EXISTS idx_live_matches_status ON live_matches (status);
CREATE INDEX IF NOT EXISTS idx_live_matches_detected ON live_matches (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_live_matches_player ON live_matches (source_player_id);
COMMENT ON TABLE live_matches IS 'Tracks live matches - active, ended, or dropped. Drop detection runs every 15 min.';

-- live_match_players - All 10 players in a live match
CREATE TABLE IF NOT EXISTS live_match_players (
    id                BIGSERIAL PRIMARY KEY,
    match_id          BIGINT NOT NULL,
    player_id         BIGINT NOT NULL,
    player_name       VARCHAR(100),
    champion_id       INT,
    champion_name     VARCHAR(100),
    skin_id           INT,
    skin_name         VARCHAR,
    account_level     INT,
    mastery_level     INT,
    tier              INT,
    tier_wins         INT,
    tier_losses       INT,
    task_force        INT,
    platform          INT,
    UNIQUE (match_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_lmp_match ON live_match_players (match_id);
CREATE INDEX IF NOT EXISTS idx_lmp_player ON live_match_players (player_id);
COMMENT ON TABLE live_match_players IS 'All 10 players in a live match. Source: LivePlayer from match.py.';

-- drop_hack_suspects - Players suspected of drop hacking
CREATE TABLE IF NOT EXISTS drop_hack_suspects (
    id                BIGSERIAL PRIMARY KEY,
    player_id         BIGINT NOT NULL,
    player_name       VARCHAR(100),
    match_id          BIGINT,
    champion_id       INT,
    champion_name     VARCHAR(100),
    is_cassie         BOOLEAN NOT NULL DEFAULT FALSE,
    dropped_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    incident_count    INT NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_dhs_player ON drop_hack_suspects (player_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dhs_player_match ON drop_hack_suspects (player_id, match_id);
CREATE INDEX IF NOT EXISTS idx_dhs_incident ON drop_hack_suspects (incident_count DESC);
COMMENT ON TABLE drop_hack_suspects IS 'Tracks drop hack suspects. is_cassie auto-flagged if champion=Cassie. incident_count > 1 across multiple dropped matches = likely drop hacker.';

-- ============================================================================
-- 19. PLAYER LOADOUTS (BETA)
-- ===========================================================================

-- player_loadouts - Player's saved loadouts from getplayerloadouts (beta feature)
CREATE TABLE IF NOT EXISTS player_loadouts (
    id              BIGSERIAL PRIMARY KEY,
    player_id       BIGINT NOT NULL,
    champion_id     INT NOT NULL REFERENCES champions(id),
    deck_id         BIGINT,
    deck_key        TEXT NOT NULL,
    loadout_name    VARCHAR(100),
    card_ids        INT[],
    card_levels     INT[],
    talent_id       INT,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (player_id, deck_key)
);
CREATE INDEX IF NOT EXISTS idx_pl_player ON player_loadouts (player_id);
CREATE INDEX IF NOT EXISTS idx_pl_player_champion ON player_loadouts (player_id, champion_id);
COMMENT ON TABLE player_loadouts IS 'Saved player decks from getplayerloadouts. One row per player deck; card win rates remain derived from match_player_cards.';

-- player_loadout_fetches - cache ledger for player saved-deck API reads.
CREATE TABLE IF NOT EXISTS player_loadout_fetches (
    player_id               BIGINT PRIMARY KEY,
    fetched_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_manual_refresh_at  TIMESTAMPTZ
);
COMMENT ON TABLE player_loadout_fetches IS 'Per-player getplayerloadouts cache ledger. A row is retained even when the player has no saved decks.';

-- player_loadout_cards - Per-card win rates derived from match_player_cards
CREATE TABLE IF NOT EXISTS player_loadout_cards (
    player_id       BIGINT NOT NULL,
    champion_id     INT NOT NULL REFERENCES champions(id),
    card_id         INT NOT NULL,
    card_level      INT CHECK (card_level BETWEEN 1 AND 5),
    times_used      INT NOT NULL DEFAULT 0,
    wins            INT NOT NULL DEFAULT 0,
    losses          INT NOT NULL DEFAULT 0,
    win_rate        NUMERIC,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id, champion_id, card_id)
);
CREATE INDEX IF NOT EXISTS idx_plc_player ON player_loadout_cards (player_id);
CREATE INDEX IF NOT EXISTS idx_plc_winrate ON player_loadout_cards (win_rate DESC);
COMMENT ON TABLE player_loadout_cards IS 'Main feature: per-card win rates derived from match_player_cards. Suggests top loadout builds.';

-- ============================================================================
-- 20. PER-PLAYER CHAMPION STATS
-- ============================================================================

-- player_champions - Player's champion roster from getplayerchampions (10 min cooldown)
CREATE TABLE IF NOT EXISTS player_champions (
    player_id       INT NOT NULL,
    champion_id     INT NOT NULL,
    champion_name   VARCHAR(100),
    xp              BIGINT DEFAULT 0,
    ownership_type  VARCHAR(20),
    wins            INT DEFAULT 0,
    losses          INT DEFAULT 0,
    kills           INT DEFAULT 0,
    deaths          INT DEFAULT 0,
    assists         INT DEFAULT 0,
    minutes_played  INT DEFAULT 0,
    stats_populated BOOLEAN NOT NULL DEFAULT FALSE,
    last_updated    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (player_id, champion_id)
);
CREATE INDEX IF NOT EXISTS idx_pc_player ON player_champions (player_id);
COMMENT ON TABLE player_champions IS 'Per-player champion stats from getplayerchampions. 10 min cooldown before re-fetch.';

-- ============================================================================
-- 21. AFK TRACKER - GPM-based AFK detection
-- ============================================================================

-- baselines - Multi-metric baselines per role per game mode (GPM, DPM, HPM, SHPM, KDA)
CREATE TABLE IF NOT EXISTS baselines (
    role_id         INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
    role_name       VARCHAR(20) NOT NULL,
    queue_id        INT NOT NULL,
    avg_gpm         DECIMAL(8,2),
    p10_gpm         DECIMAL(8,2),
    p90_gpm         DECIMAL(8,2),
    p25_gpm         DECIMAL(8,2),
    p75_gpm         DECIMAL(8,2),
    max_gpm         DECIMAL(8,2),
    avg_dpm         DECIMAL(8,2),
    p10_dpm         DECIMAL(8,2),
    p90_dpm         DECIMAL(8,2),
    p25_dpm         DECIMAL(8,2),
    p75_dpm         DECIMAL(8,2),
    max_dpm         DECIMAL(8,2),
    avg_hpm         DECIMAL(8,2),
    p10_hpm         DECIMAL(8,2),
    p90_hpm         DECIMAL(8,2),
    p25_hpm         DECIMAL(8,2),
    p75_hpm         DECIMAL(8,2),
    max_hpm         DECIMAL(8,2),
    avg_shpm        DECIMAL(8,2),
    p10_shpm        DECIMAL(8,2),
    p90_shpm        DECIMAL(8,2),
    p25_shpm        DECIMAL(8,2),
    p75_shpm        DECIMAL(8,2),
    max_shpm        DECIMAL(8,2),
    avg_kda         DECIMAL(8,2),
    p10_kda         DECIMAL(8,2),
    p90_kda         DECIMAL(8,2),
    p25_kda         DECIMAL(8,2),
    p75_kda         DECIMAL(8,2),
    max_kda         DECIMAL(8,2),
    avg_egpm        NUMERIC(8,2),
    p10_egpm        NUMERIC(8,2),
    p90_egpm        NUMERIC(8,2),
    p25_egpm        NUMERIC(8,2),
    p75_egpm        NUMERIC(8,2),
    max_egpm        NUMERIC(8,2),
    sample_size     INT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (role_id, queue_id)
);
CREATE INDEX IF NOT EXISTS idx_baselines_role_queue ON baselines (role_id, queue_id);
COMMENT ON TABLE baselines IS 'Multi-metric baselines globally and per role per game mode. Roles: 0=Global, 1=Damage, 2=Flank, 3=Support, 4=Frontline. Retained for reference; AFK detection now uses eGPM thresholds.';

-- champion_performance_baselines - Request-time projection for metric pages
--
-- Per-champion percentile distributions are expensive ordered aggregates over
-- every ranked player-match. Maintain them with the baseline worker so a cold
-- route-cache miss never turns a public page request into a historical scan.
CREATE TABLE IF NOT EXISTS champion_performance_baselines (
    queue_id      INT NOT NULL DEFAULT 486,
    champion_id   INT NOT NULL REFERENCES champions(id),
    metric        TEXT NOT NULL CHECK (metric IN ('dpm', 'wpm', 'apm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda')),
    min_value     DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_value     DOUBLE PRECISION NOT NULL DEFAULT 0,
    mean_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
    median_value  DOUBLE PRECISION NOT NULL DEFAULT 0,
    mode_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
    p10_value     DOUBLE PRECISION NOT NULL DEFAULT 0,
    p90_value     DOUBLE PRECISION NOT NULL DEFAULT 0,
    sample_size   INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, champion_id, metric)
);
CREATE INDEX IF NOT EXISTS idx_champion_performance_baselines_metric
    ON champion_performance_baselines (queue_id, metric, mean_value DESC);
COMMENT ON TABLE champion_performance_baselines IS 'Baseline-worker projection of ranked per-champion metric distributions used by public stats pages.';

-- Hot read models for public performance and champion-ELO leaderboards. The
-- canonical facts remain match_players and player_champion_ratings; ingestion
-- updates these incrementally and the derived projection worker repairs them.
CREATE TABLE IF NOT EXISTS performance_projection_matches (
    match_id       BIGINT PRIMARY KEY,
    projected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS performance_records_ranked (
    match_id       BIGINT NOT NULL,
    entry_datetime TIMESTAMPTZ NOT NULL,
    player_id      BIGINT NOT NULL,
    champion_id    INT NOT NULL REFERENCES champions(id),
    champion_name  TEXT NOT NULL,
    role_id        INT CHECK (role_id BETWEEN 1 AND 4),
    role_name      TEXT NOT NULL,
    queue_id       INT NOT NULL DEFAULT 486,
    region         TEXT,
    platform       TEXT,
    gpm            DOUBLE PRECISION,
    dpm            DOUBLE PRECISION,
    hpm            DOUBLE PRECISION,
    mpm            DOUBLE PRECISION,
    PRIMARY KEY (match_id, entry_datetime, player_id)
);
CREATE INDEX IF NOT EXISTS idx_performance_records_gpm ON performance_records_ranked (queue_id, gpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_dpm ON performance_records_ranked (queue_id, dpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_hpm ON performance_records_ranked (queue_id, hpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_mpm ON performance_records_ranked (queue_id, mpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_gpm ON performance_records_ranked (queue_id, role_name, gpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_dpm ON performance_records_ranked (queue_id, role_name, dpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_hpm ON performance_records_ranked (queue_id, role_name, hpm DESC, entry_datetime DESC, match_id DESC, player_id);
CREATE INDEX IF NOT EXISTS idx_performance_records_role_mpm ON performance_records_ranked (queue_id, role_name, mpm DESC, entry_datetime DESC, match_id DESC, player_id);

CREATE TABLE IF NOT EXISTS performance_metric_histogram (
    queue_id       INT NOT NULL,
    role_id        INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
    role_name      TEXT NOT NULL,
    metric         TEXT NOT NULL CHECK (metric IN ('dpm', 'wpm', 'apm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda')),
    value          DOUBLE PRECISION NOT NULL,
    sample_count   BIGINT NOT NULL CHECK (sample_count > 0),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, role_id, metric, value)
);

CREATE TABLE IF NOT EXISTS performance_metric_stats (
    queue_id       INT NOT NULL,
    role_id        INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
    role_name      TEXT NOT NULL,
    metric         TEXT NOT NULL CHECK (metric IN ('dpm', 'wpm', 'apm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda')),
    min_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    mean_value     DOUBLE PRECISION NOT NULL DEFAULT 0,
    median_value   DOUBLE PRECISION NOT NULL DEFAULT 0,
    mode_value     DOUBLE PRECISION NOT NULL DEFAULT 0,
    p10_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    p25_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    p75_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    p90_value      DOUBLE PRECISION NOT NULL DEFAULT 0,
    sample_size    BIGINT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, role_id, metric)
);

CREATE TABLE IF NOT EXISTS player_best_champion_ratings (
    queue_id       INT NOT NULL DEFAULT 486,
    role_id        INT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
    role_name      TEXT NOT NULL,
    player_id      BIGINT NOT NULL REFERENCES players(id),
    champion_id    INT NOT NULL REFERENCES champions(id),
    mu             DOUBLE PRECISION NOT NULL,
    phi            DOUBLE PRECISION NOT NULL,
    matches_played INT NOT NULL DEFAULT 0,
    wins           INT NOT NULL DEFAULT 0,
    losses         INT NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (queue_id, role_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_best_champion_rating_rank
    ON player_best_champion_ratings (queue_id, role_id, mu DESC, matches_played DESC, wins DESC);

-- afk_incidents table dropped (2026-05-29). AFK scoring moved inline to match_players
-- via afk_rate column (0=not auto-flagged, 2=Partial AFK, 3=Full AFK)
-- computed from eGPM at ingestion time.

-- ============================================================================
-- 22. RANKED TRACKER - League leaderboard tracking
-- ============================================================================

-- league_leaderboard - Position tracking with relative position (+1/-1)
CREATE TABLE IF NOT EXISTS league_leaderboard (
    player_id       INT PRIMARY KEY,
    player_name     VARCHAR(100),
    rank            INT,
    tier            INT CHECK (tier BETWEEN 21 AND 26),
    points          INT,
    wins            INT DEFAULT 0,
    losses          INT DEFAULT 0,
    queue_id        INT NOT NULL,
    season          INT,
    prev_rank       INT,
    leaves          INT NOT NULL DEFAULT 0,
    fetched_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    id              BIGSERIAL,
    UNIQUE (tier, season, player_id, fetched_at)
);
CREATE INDEX IF NOT EXISTS idx_ll_tier ON league_leaderboard (tier);
CREATE INDEX IF NOT EXISTS idx_ll_player ON league_leaderboard (player_id);
CREATE INDEX IF NOT EXISTS idx_ll_fetched ON league_leaderboard (fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_ll_tier_points ON league_leaderboard (tier, points DESC);
COMMENT ON TABLE league_leaderboard IS 'Ranked tracker - position tracking with relative position (+1/-1). Tiers: 21=D5, 22=D4, 23=D3, 24=D2, 25=D1, 26=Master';

-- leaderboard_update_log - Audit trail for leaderboard updates
CREATE TABLE IF NOT EXISTS leaderboard_update_log (
    id              SERIAL PRIMARY KEY,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    season          INT NOT NULL,
    round           INT NOT NULL,
    queue_id        INT NOT NULL DEFAULT 486,
    tiers_updated   INT[] NOT NULL,
    total_players   INT NOT NULL DEFAULT 0,
    trigger_type    VARCHAR NOT NULL DEFAULT 'manual',
    dev_id          VARCHAR,
    next_auto       TIMESTAMPTZ NOT NULL DEFAULT (now() + '24:00:00'::interval),
    status          VARCHAR NOT NULL DEFAULT 'completed'
);
CREATE INDEX IF NOT EXISTS idx_lul_updated ON leaderboard_update_log (updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_lul_season ON leaderboard_update_log (season);
COMMENT ON TABLE leaderboard_update_log IS 'Audit trail for leaderboard updates';

-- leaderboard_current - Unified ranked league table (tiers 21-26 merged)
-- Replaces leaderboard21-leaderboard26. UNIQUE on player_id - one row per player.
CREATE TABLE IF NOT EXISTS leaderboard_current (
    player_id       BIGINT NOT NULL,
    name            VARCHAR(100),
    tier            INT NOT NULL,
    points          INT NOT NULL DEFAULT 0,
    rank            INT NOT NULL DEFAULT 1000,
    prev_rank       INT NOT NULL DEFAULT 0,
    prev_tier       INT,
    trend           INT NOT NULL DEFAULT 0,
    tier_change     INT DEFAULT 0,
    wins            INT NOT NULL DEFAULT 0,
    losses          INT NOT NULL DEFAULT 0,
    leaves          INT NOT NULL DEFAULT 0,
    season          INT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    winrate         DOUBLE PRECISION DEFAULT 0.00,
    leaverate       DOUBLE PRECISION DEFAULT 0.00,
    PRIMARY KEY (player_id)
);

-- Trigger: auto-compute winrate and leaverate on insert/update
CREATE OR REPLACE FUNCTION compute_leaderboard_rates()
RETURNS TRIGGER AS $$
BEGIN
  NEW.winrate := CASE WHEN (NEW.wins + NEW.losses) > 0
    THEN ROUND((NEW.wins::NUMERIC / (NEW.wins + NEW.losses)) * 100, 2)::DOUBLE PRECISION
    ELSE 0.00 END;
  NEW.leaverate := CASE WHEN (NEW.wins + NEW.losses) > 0
    THEN ROUND((NEW.leaves::NUMERIC / (NEW.wins + NEW.losses)) * 100, 2)::DOUBLE PRECISION
    ELSE 0.00 END;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_compute_leaderboard_rates ON leaderboard_current;
CREATE TRIGGER trg_compute_leaderboard_rates
  BEFORE INSERT OR UPDATE ON leaderboard_current
  FOR EACH ROW EXECUTE FUNCTION compute_leaderboard_rates();
CREATE INDEX IF NOT EXISTS idx_lb_current_points ON leaderboard_current (points DESC);
CREATE INDEX IF NOT EXISTS idx_lb_current_tier_points ON leaderboard_current (tier, points DESC);
COMMENT ON TABLE leaderboard_current IS 'Unified ranked league table (tiers 21-26 merged). UNIQUE on player_id - one row per player. Trend = prev_rank - rank (positive = improved). Tier change = tier - prev_tier (positive = promoted, negative = demoted).';

-- tier_population_stats - Tier-level population distribution for ranked leaderboard.
-- Queried by /stats/tier-population (stats.ts:314). Refreshed by buffer-processor.ts:1127.
-- Columns: tier (INT), tier_name (TEXT), player_count (BIGINT).
CREATE MATERIALIZED VIEW IF NOT EXISTS tier_population_stats AS
SELECT lc.tier AS tier,
    rt.tier_name AS tier_name,
    COUNT(*)::BIGINT AS player_count
FROM leaderboard_current lc
JOIN ranked_tiers rt ON rt.tier_id = lc.tier
GROUP BY lc.tier, rt.tier_name;
CREATE UNIQUE INDEX IF NOT EXISTS idx_tier_pop_tier ON tier_population_stats (tier);
COMMENT ON MATERIALIZED VIEW tier_population_stats IS 'Tier population distribution - REFRESH MATERIALIZED VIEW CONCURRENTLY after leaderboard updates';

-- ============================================================================
-- 23. ADDITIONAL TABLES (in DB but not part of note.md taskflow)
-- ============================================================================

-- api_log - Hourly consolidated API call logging
-- One row per dev_id + endpoint per hour. Prevents unbounded growth.
CREATE TABLE IF NOT EXISTS api_log (
    dev_id            VARCHAR NOT NULL,
    endpoint          VARCHAR NOT NULL,
    consumer          VARCHAR(80) NOT NULL DEFAULT 'legacy',
    hour              TIMESTAMPTZ NOT NULL,
    call_count        INT NOT NULL DEFAULT 0,
    total_response_ms INT NOT NULL DEFAULT 0,
    avg_response_ms   INT GENERATED ALWAYS AS (CASE WHEN call_count > 0 THEN total_response_ms / call_count ELSE 0 END) STORED,
    PRIMARY KEY (dev_id, endpoint, consumer, hour)
);
COMMENT ON TABLE api_log IS 'Hourly API call logging by key, endpoint, and consumer attribution';

-- esports_leagues - Esports league tracking
CREATE TABLE IF NOT EXISTS esports_leagues (
    league_id          INT PRIMARY KEY,
    league_name        VARCHAR NOT NULL,
    league_description TEXT,
    league_image_url   VARCHAR,
    league_start_date  TIMESTAMPTZ,
    league_end_date    TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE esports_leagues IS 'Esports league tracking';

-- esports_teams - Esports team tracking
CREATE TABLE IF NOT EXISTS esports_teams (
    team_id          INT PRIMARY KEY,
    team_name        VARCHAR NOT NULL,
    team_description TEXT,
    team_image_url   VARCHAR,
    league_id        INT REFERENCES esports_leagues(league_id),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE esports_teams IS 'Esports team tracking';

-- esports_team_players - Player-team relationship
CREATE TABLE IF NOT EXISTS esports_team_players (
    player_id    INT NOT NULL,
    team_id      INT NOT NULL REFERENCES esports_teams(team_id),
    player_name  VARCHAR,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE esports_team_players IS 'Player-team relationship for esports';

-- player_achievements - Player kill/streak achievements
CREATE TABLE IF NOT EXISTS player_achievements (
    player_id              INT NOT NULL,
    player_name            VARCHAR,
    assisted_kills         INT DEFAULT 0,
    camps_cleared          INT DEFAULT 0,
    divine_spree           INT DEFAULT 0,
    double_kills           INT DEFAULT 0,
    fire_giant_kills       INT DEFAULT 0,
    first_bloods           INT DEFAULT 0,
    god_like_spree         INT DEFAULT 0,
    gold_fury_kills        INT DEFAULT 0,
    immortal_spree         INT DEFAULT 0,
    killing_spree          INT DEFAULT 0,
    minion_kills           INT DEFAULT 0,
    penta_kills            INT DEFAULT 0,
    phoenix_kills          INT DEFAULT 0,
    player_kills           INT DEFAULT 0,
    quadra_kills           INT DEFAULT 0,
    rampage_spree          INT DEFAULT 0,
    shutdown_spree         INT DEFAULT 0,
    siege_juggernaut_kills INT DEFAULT 0,
    tower_kills            INT DEFAULT 0,
    triple_kills           INT DEFAULT 0,
    unstoppable_spree      INT DEFAULT 0,
    wild_juggernaut_kills  INT DEFAULT 0,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id)
);
COMMENT ON TABLE player_achievements IS 'Player kill/streak achievements';

-- player_status - Player online status
CREATE TABLE IF NOT EXISTS player_status (
    player_id               INT NOT NULL,
    status                  INT,
    status_string           VARCHAR,
    current_match_id        BIGINT,
    queue_id                INT,
    privacy_flag            BOOLEAN DEFAULT FALSE,
    personal_status_message TEXT,
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (player_id)
);
COMMENT ON TABLE player_status IS 'Player online status';

-- ============================================================================
-- Auto-Ingest Derived Tables
-- ============================================================================

-- match_compositions - Ranked team compositions (5-player teams only)
CREATE TABLE IF NOT EXISTS match_compositions (
    comp_id     TEXT NOT NULL,
    frontline   INT NOT NULL,
    damage      INT NOT NULL,
    flank       INT NOT NULL,
    support     INT NOT NULL,
    count       INT NOT NULL DEFAULT 0,
    wins        INT NOT NULL DEFAULT 0,
    losses      INT NOT NULL DEFAULT 0,
    winrate     NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (comp_id)
);
COMMENT ON TABLE match_compositions IS 'Ranked team compositions (queue_id=486 only)';

-- match_compositions_ranked - Tier-aware ranked team compositions
CREATE TABLE IF NOT EXISTS match_compositions_ranked (
    comp_id      TEXT NOT NULL,
    lobby_tier   SMALLINT NOT NULL DEFAULT 0 CHECK (lobby_tier BETWEEN 0 AND 26),
    frontline    SMALLINT NOT NULL,
    damage       SMALLINT NOT NULL,
    flank        SMALLINT NOT NULL,
    support      SMALLINT NOT NULL,
    count        INT NOT NULL DEFAULT 0,
    wins         INT NOT NULL DEFAULT 0,
    losses       INT NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (comp_id, lobby_tier)
);
CREATE INDEX IF NOT EXISTS idx_match_compositions_ranked_tier_count
    ON match_compositions_ranked (lobby_tier, count DESC);
COMMENT ON TABLE match_compositions_ranked IS 'Tier-bucketed queue-486 complete-team composition projection maintained by ingestion and derived-projection repair.';

-- bans_ranked - Cumulative ban counts per champion (ranked only)
CREATE TABLE IF NOT EXISTS bans_ranked (
    champion_id   INT NOT NULL,
    champion_name TEXT NOT NULL,
    ban_total     INT NOT NULL DEFAULT 0,
    slot1         INT NOT NULL DEFAULT 0,
    slot2         INT NOT NULL DEFAULT 0,
    slot3         INT NOT NULL DEFAULT 0,
    slot4         INT NOT NULL DEFAULT 0,
    slot5         INT NOT NULL DEFAULT 0,
    slot6         INT NOT NULL DEFAULT 0,
    slot7         INT NOT NULL DEFAULT 0,
    slot8         INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (champion_id)
);
COMMENT ON TABLE bans_ranked IS 'Cumulative ban counts per champion (ranked only)';

-- champion_stats_ranked - Incremental ranked champion stats and bans
CREATE TABLE IF NOT EXISTS champion_stats_ranked (
    champion_id       INT PRIMARY KEY REFERENCES champions(id),
    champion_name     TEXT NOT NULL,
    total_matches     INT NOT NULL DEFAULT 0,
    wins              INT NOT NULL DEFAULT 0,
    losses            INT NOT NULL DEFAULT 0,
    sum_kills         INT NOT NULL DEFAULT 0,
    sum_deaths        INT NOT NULL DEFAULT 0,
    sum_assists       INT NOT NULL DEFAULT 0,
    sum_damage        INT NOT NULL DEFAULT 0,
    sum_gold          INT NOT NULL DEFAULT 0,
    sum_heal          INT NOT NULL DEFAULT 0,
    sum_mitigation    INT NOT NULL DEFAULT 0,
    sum_league_tier   INT NOT NULL DEFAULT 0,
    league_tier_count INT NOT NULL DEFAULT 0,
    ban_total         INT NOT NULL DEFAULT 0,
    slot1             INT NOT NULL DEFAULT 0,
    slot2             INT NOT NULL DEFAULT 0,
    slot3             INT NOT NULL DEFAULT 0,
    slot4             INT NOT NULL DEFAULT 0,
    slot5             INT NOT NULL DEFAULT 0,
    slot6             INT NOT NULL DEFAULT 0,
    slot7             INT NOT NULL DEFAULT 0,
    slot8             INT NOT NULL DEFAULT 0,
    win_rate          NUMERIC(5,2),
    pick_rate         NUMERIC(10,4),
    ban_rate          NUMERIC(10,4),
    kda               NUMERIC(10,2),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE champion_stats_ranked IS 'Incremental ranked champion stats and ban projection maintained by buffer-processor.ts';

-- item_counts_ranked - Ranked item usage
CREATE TABLE IF NOT EXISTS item_counts_ranked (
    item_id      INT NOT NULL,
    item_name    TEXT,
    slot         SMALLINT NOT NULL,
    item_level   SMALLINT NOT NULL DEFAULT 0,
    count        INT NOT NULL DEFAULT 0,
    wins         INT NOT NULL DEFAULT 0,
    losses       INT NOT NULL DEFAULT 0,
    winrate      NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (item_id, slot, item_level)
);
COMMENT ON TABLE item_counts_ranked IS 'Ranked item usage stats';

-- map_item_counts_ranked - Ranked item usage by map and lobby tier
--
-- Map detail pages compare an item's result across the full ranked map pool.
-- Keeping that projection incremental avoids scanning every historical item
-- fact whenever one map page is opened.
CREATE TABLE IF NOT EXISTS map_item_counts_ranked (
    map_name     TEXT NOT NULL,
    lobby_tier   SMALLINT NOT NULL DEFAULT 0,
    item_id      INT NOT NULL,
    count        INT NOT NULL DEFAULT 0,
    wins         INT NOT NULL DEFAULT 0,
    losses       INT NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (map_name, lobby_tier, item_id)
);
CREATE INDEX IF NOT EXISTS idx_map_item_counts_ranked_item_map
    ON map_item_counts_ranked (item_id, map_name, lobby_tier);
COMMENT ON TABLE map_item_counts_ranked IS 'Incremental queue-486 item usage grouped by exact Hi-Rez map name and shared lobby tier.';
CREATE TABLE IF NOT EXISTS map_item_counts_ranked_matches (
    match_id       BIGINT PRIMARY KEY,
    projected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
COMMENT ON TABLE map_item_counts_ranked_matches IS 'Idempotency ledger for the incremental ranked map-item projection.';

-- item_counts_casual - Casual item usage
CREATE TABLE IF NOT EXISTS item_counts_casual (
    item_id      INT NOT NULL,
    item_name    TEXT,
    slot         SMALLINT NOT NULL,
    item_level   SMALLINT NOT NULL DEFAULT 0,
    count        INT NOT NULL DEFAULT 0,
    wins         INT NOT NULL DEFAULT 0,
    losses       INT NOT NULL DEFAULT 0,
    winrate      NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at   TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (item_id, slot, item_level)
);
COMMENT ON TABLE item_counts_casual IS 'Casual item usage stats';

-- talent_counts_ranked - Ranked talent usage
CREATE TABLE IF NOT EXISTS talent_counts_ranked (
    talent_id     INT NOT NULL,
    champion_name TEXT,
    talent_name   TEXT,
    count         INT NOT NULL DEFAULT 0,
    wins          INT NOT NULL DEFAULT 0,
    losses        INT NOT NULL DEFAULT 0,
    winrate       NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (talent_id)
);
COMMENT ON TABLE talent_counts_ranked IS 'Ranked talent usage stats';

-- talent_counts_casual - Casual talent usage
CREATE TABLE IF NOT EXISTS talent_counts_casual (
    talent_id     INT NOT NULL,
    champion_name TEXT,
    talent_name   TEXT,
    count         INT NOT NULL DEFAULT 0,
    wins          INT NOT NULL DEFAULT 0,
    losses        INT NOT NULL DEFAULT 0,
    winrate       NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (talent_id)
);
COMMENT ON TABLE talent_counts_casual IS 'Casual talent usage stats';

-- card_counts_ranked - Ranked card usage
CREATE TABLE IF NOT EXISTS card_counts_ranked (
    card_id       INT NOT NULL,
    champion_name TEXT,
    card_name     TEXT,
    card_level    SMALLINT NOT NULL DEFAULT 0,
    count         INT NOT NULL DEFAULT 0,
    wins          INT NOT NULL DEFAULT 0,
    losses        INT NOT NULL DEFAULT 0,
    winrate       NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (card_id, card_level)
);
COMMENT ON TABLE card_counts_ranked IS 'Ranked card usage stats';

-- talent_card_counts_ranked - Ranked card usage within a selected talent
CREATE TABLE IF NOT EXISTS talent_card_counts_ranked (
    talent_id     INT NOT NULL,
    card_id       INT NOT NULL,
    card_level    SMALLINT NOT NULL DEFAULT 0,
    count         INT NOT NULL DEFAULT 0,
    wins          INT NOT NULL DEFAULT 0,
    losses        INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (talent_id, card_id, card_level)
);
CREATE INDEX IF NOT EXISTS idx_talent_card_counts_ranked_card
    ON talent_card_counts_ranked (card_id, talent_id);
COMMENT ON TABLE talent_card_counts_ranked IS 'Ranked card and level usage grouped by selected talent for champion card pages.';

-- skin_counts_ranked - Tier-aware ranked skin usage
CREATE TABLE IF NOT EXISTS skin_counts_ranked (
    champion_id  INT NOT NULL REFERENCES champions(id),
    skin_id      INT NOT NULL,
    league_tier  SMALLINT NOT NULL DEFAULT 0 CHECK (league_tier BETWEEN 0 AND 26),
    skin_name    TEXT NOT NULL,
    count        INT NOT NULL DEFAULT 0,
    wins         INT NOT NULL DEFAULT 0,
    losses       INT NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (champion_id, skin_id, league_tier)
);
CREATE INDEX IF NOT EXISTS idx_skin_counts_ranked_tier
    ON skin_counts_ranked (league_tier, champion_id, skin_id);
COMMENT ON TABLE skin_counts_ranked IS 'Tier-bucketed queue-486 skin usage projection maintained by ingestion and derived-projection repair.';

-- card_counts_casual - Casual card usage
CREATE TABLE IF NOT EXISTS card_counts_casual (
    card_id       INT NOT NULL,
    champion_name TEXT,
    card_name     TEXT,
    card_level    SMALLINT NOT NULL DEFAULT 0,
    count         INT NOT NULL DEFAULT 0,
    wins          INT NOT NULL DEFAULT 0,
    losses        INT NOT NULL DEFAULT 0,
    winrate       NUMERIC(5,2) NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (card_id, card_level)
);
COMMENT ON TABLE card_counts_casual IS 'Casual card usage stats';

-- api_key_hourly_usage - Per-key, per-hour call counts.
-- Normalized buckets are used instead of hour_00..hour_23 columns so relay
-- restarts and hourly sync timing can never wipe an active hour's calls.
CREATE TABLE IF NOT EXISTS api_key_hourly_usage (
    dev_id      TEXT NOT NULL,
    hour_bucket TIMESTAMPTZ NOT NULL,
    call_count  INT NOT NULL DEFAULT 0,
    PRIMARY KEY (dev_id, hour_bucket)
);
CREATE INDEX IF NOT EXISTS idx_api_key_hourly_usage_hour ON api_key_hourly_usage (hour_bucket DESC);
COMMENT ON TABLE api_key_hourly_usage IS 'Per-key hourly API call counts; normalized UTC buckets prevent reset-time data loss';

-- ============================================================================
-- END OF CONSOLIDATED SCHEMA
-- ============================================================================
-- Summary (verified against live DB 2026-05-31):
--   Reference tables: 11 (champions, items, bounty_items, maps, ranked_tiers, regions, talents, queue_types, patches, cards, skins)
--   Player tables: 9 (players, player_name_history, player_account_merges, champion_stats, player_status, player_champions, player_achievements, player_loadouts, player_loadout_cards, player_queue_ratings)
--   Match tables: 2 (matches, match_players)
--   Fact tables: 5 (match_bans, match_player_items, match_player_talents, match_player_cards, match_opponents)
--   Rating tables: 6 (champion_ratings, champion_match_ratings, champion_tier_ratings, player_queue_ratings, player_champion_ratings, match_rating_snapshots)
--   Private players: 2 (players_private, players_private_history)
--   Recovery: 2 (broken_skins, recovery_stats)
--   API: 3 (api_keys, api_log, api_key_hourly_usage)
--   Auth/community: 7 (users, sessions, posts, comments, user_post_likes, user_build_likes, builds)
--   Charts: 1 (global_match_stats)
--   Leaderboard: 24 (leaderboard0-20, leaderboard_current, league_leaderboard, leaderboard_update_log)
--   Live match: 2 (live_matches, live_match_players)
--   Auto-ingest: hourly_match_counts, match_compositions, bans_ranked, ranked item/talent/card/talent-card projections, and legacy casual count tables
--   Esports: 3 (esports_leagues, esports_teams, esports_team_players)
--   Other: 7 (raw_ingest_buffer, match_pull_list, sync_jobs, drop_hack_suspects, baselines, championsquick, player_relationships)
--   Materialized views: 12 (champion_meta_stats, tier_population_stats, mv_player_coplay_stats, player_rankings, daily_match_stats, counter_pick_stats, champion_quick_stats, player_match_stats, queue_stats, region_stats, mv_tier_match_stats, item_stats)
--   Tier distribution: 1 (tier_stats — wide-format table tracking match participation + profile snapshots per tier)
--   Hypertables: 1 (matches only)
--   Total: 93 base tables + 12 materialized views

-- =====================================================================
-- tier_stats — Tier Distribution Summary Table
-- =====================================================================
CREATE TABLE IF NOT EXISTS tier_stats (
    source      VARCHAR(10) NOT NULL,
    tier_0      INTEGER DEFAULT 0,
    tier_1      INTEGER DEFAULT 0,
    tier_2      INTEGER DEFAULT 0,
    tier_3      INTEGER DEFAULT 0,
    tier_4      INTEGER DEFAULT 0,
    tier_5      INTEGER DEFAULT 0,
    tier_6      INTEGER DEFAULT 0,
    tier_7      INTEGER DEFAULT 0,
    tier_8      INTEGER DEFAULT 0,
    tier_9      INTEGER DEFAULT 0,
    tier_10     INTEGER DEFAULT 0,
    tier_11     INTEGER DEFAULT 0,
    tier_12     INTEGER DEFAULT 0,
    tier_13     INTEGER DEFAULT 0,
    tier_14     INTEGER DEFAULT 0,
    tier_15     INTEGER DEFAULT 0,
    tier_16     INTEGER DEFAULT 0,
    tier_17     INTEGER DEFAULT 0,
    tier_18     INTEGER DEFAULT 0,
    tier_19     INTEGER DEFAULT 0,
    tier_20     INTEGER DEFAULT 0,
    tier_21     INTEGER DEFAULT 0,
    tier_22     INTEGER DEFAULT 0,
    tier_23     INTEGER DEFAULT 0,
    tier_24     INTEGER DEFAULT 0,
    tier_25     INTEGER DEFAULT 0,
    tier_26     INTEGER DEFAULT 0,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (source)
);

COMMENT ON TABLE tier_stats IS 'Aggregated ranked tier distribution from matches and player profiles';
