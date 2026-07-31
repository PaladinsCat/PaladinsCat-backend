-- Private-account identity v2.
--
-- A Hi-Rez PartyId describes match/session context; it is not a durable player
-- identifier.  The old function used PartyId as a person key and also wrote a
-- history row on every retry.  V2 keeps the immutable match slot as the source
-- of truth and stores every inference (including its evidence) separately.

ALTER TABLE players_private
  ADD COLUMN IF NOT EXISTS tracking_version SMALLINT NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS identity_status VARCHAR(20) NOT NULL DEFAULT 'inferred',
  ADD COLUMN IF NOT EXISTS identity_confidence SMALLINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS state_observed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS merged_into_id INTEGER REFERENCES players_private(id),
  ADD COLUMN IF NOT EXISTS verified_name VARCHAR(100),
  ADD COLUMN IF NOT EXISTS name_verified_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS name_verified_by VARCHAR(100),
  ADD COLUMN IF NOT EXISTS name_evidence_ref TEXT;

DO $$
DECLARE
  constraint_name TEXT;
BEGIN
  SELECT c.conname INTO constraint_name
  FROM pg_constraint c
  WHERE c.conrelid = 'players_private'::regclass
    AND c.contype = 'u'
    AND pg_get_constraintdef(c.oid) =
      'UNIQUE (party_id, account_level, mastery_level, league_tier, league_points)'
  LIMIT 1;

  IF constraint_name IS NOT NULL THEN
    EXECUTE format('ALTER TABLE players_private DROP CONSTRAINT %I', constraint_name);
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'players_private'::regclass
      AND conname = 'players_private_identity_status_check'
  ) THEN
    ALTER TABLE players_private
      ADD CONSTRAINT players_private_identity_status_check
      CHECK (identity_status IN ('inferred', 'verified', 'legacy', 'merged'));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'players_private'::regclass
      AND conname = 'players_private_identity_confidence_check'
  ) THEN
    ALTER TABLE players_private
      ADD CONSTRAINT players_private_identity_confidence_check
      CHECK (identity_confidence BETWEEN 0 AND 100);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_players_private_v2_active
  ON players_private (last_seen DESC, id DESC)
  WHERE tracking_version = 2 AND is_active;
CREATE INDEX IF NOT EXISTS idx_players_private_verified_name
  ON players_private (lower(verified_name))
  WHERE verified_name IS NOT NULL AND is_active;

ALTER TABLE players_private_history
  ADD COLUMN IF NOT EXISTS private_slot SMALLINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS resolution_confidence SMALLINT,
  ADD COLUMN IF NOT EXISTS resolution_reasons JSONB NOT NULL DEFAULT '[]'::jsonb;

CREATE UNIQUE INDEX IF NOT EXISTS uq_private_history_match_slot
  ON players_private_history (match_id, private_slot)
  WHERE match_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS private_account_observations (
  match_id                BIGINT NOT NULL,
  private_slot            SMALLINT NOT NULL,
  entry_datetime          TIMESTAMPTZ NOT NULL,
  party_id                INTEGER NOT NULL DEFAULT 0,
  account_level           INTEGER NOT NULL DEFAULT 0,
  mastery_level           INTEGER NOT NULL DEFAULT 0,
  league_tier             INTEGER NOT NULL DEFAULT 0,
  league_points           INTEGER NOT NULL DEFAULT 0,
  champion_id             INTEGER,
  task_force              SMALLINT,
  portal_id               SMALLINT,
  portal_user_id          TEXT,
  platform                VARCHAR(20),
  source                  VARCHAR(20) NOT NULL DEFAULT 'direct',
  source_priority         SMALLINT NOT NULL DEFAULT 0,
  party_member_ids        BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
  private_player_id       INTEGER REFERENCES players_private(id) ON DELETE SET NULL,
  resolution_status       VARCHAR(24) NOT NULL DEFAULT 'unresolved',
  resolution_confidence   SMALLINT NOT NULL DEFAULT 0,
  resolution_reasons      JSONB NOT NULL DEFAULT '[]'::jsonb,
  resolved_at             TIMESTAMPTZ,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id, private_slot),
  CHECK (private_slot > 0),
  CHECK (resolution_confidence BETWEEN 0 AND 100),
  CHECK (resolution_status IN ('unresolved', 'minimal', 'new_identity', 'linked', 'verified'))
);

CREATE INDEX IF NOT EXISTS idx_private_observations_identity_time
  ON private_account_observations (private_player_id, entry_datetime DESC)
  WHERE private_player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_private_observations_party_time
  ON private_account_observations (party_id, entry_datetime DESC)
  WHERE party_id <> 0;
CREATE INDEX IF NOT EXISTS idx_private_observations_unresolved
  ON private_account_observations (entry_datetime, match_id, private_slot)
  WHERE private_player_id IS NULL AND resolution_status = 'unresolved';
CREATE INDEX IF NOT EXISTS idx_private_observations_party_members
  ON private_account_observations USING GIN (party_member_ids);

COMMENT ON TABLE private_account_observations IS
  'Immutable per-match private roster observations. Identity links are evidence-based and retry-safe; PartyId is contextual evidence only.';
COMMENT ON COLUMN private_account_observations.party_id IS
  'Hi-Rez party/session context. Never a durable person identifier.';
COMMENT ON COLUMN private_account_observations.party_member_ids IS
  'Known public player IDs sharing PartyId in this match; useful cross-match identity evidence.';

CREATE TABLE IF NOT EXISTS private_account_name_verifications (
  id                    BIGSERIAL PRIMARY KEY,
  private_player_id     INTEGER NOT NULL REFERENCES players_private(id),
  verified_name         VARCHAR(100) NOT NULL,
  evidence_ref          TEXT NOT NULL,
  evidence_sha256       CHAR(64),
  notes                 TEXT,
  verified_by           VARCHAR(100) NOT NULL,
  is_active             BOOLEAN NOT NULL DEFAULT TRUE,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at            TIMESTAMPTZ,
  CHECK (evidence_sha256 IS NULL OR evidence_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_private_name_verifications_identity
  ON private_account_name_verifications (private_player_id, created_at DESC);

-- Clear every private-match link produced by the legacy PartyId resolver.  Do
-- not rewrite the historical public rows that inherited the old zero default:
-- match_players is large, and a full-table sentinel cleanup belongs in a later
-- online/batched migration.  V2 never reads zero as an identity and every new
-- row receives NULL instead.
UPDATE match_players
SET private_player_id = NULL
WHERE player_id = 0
  AND upper(COALESCE(player_name, '')) = 'PRIVATEACCOUNT';

ALTER TABLE match_players
  ALTER COLUMN private_player_id DROP DEFAULT;

-- Seed historical source facts without trusting the old PartyId-based links.
-- The application backfill resolves these chronologically after deployment.
INSERT INTO private_account_observations (
  match_id, private_slot, entry_datetime, party_id, account_level,
  mastery_level, league_tier, league_points, champion_id, task_force,
  portal_id, portal_user_id, platform, source, source_priority
)
SELECT
  mp.match_id,
  CASE WHEN mp.private_slot > 0 THEN mp.private_slot ELSE 1 END,
  mp.entry_datetime,
  COALESCE(mp.party_id, 0),
  COALESCE(mp.account_level, 0),
  COALESCE(mp.mastery_level, 0),
  COALESCE(mp.league_tier, 0),
  COALESCE(mp.league_points, 0),
  mp.champion_id,
  mp.task_force,
  mp.portal_id,
  NULLIF(mp.portal_user_id, ''),
  NULLIF(mp.platform, ''),
  COALESCE(mp.source, 'direct'),
  CASE COALESCE(mp.source, 'direct')
    WHEN 'direct' THEN 30
    WHEN 'recovered' THEN 20
    ELSE 10
  END
FROM match_players mp
WHERE mp.player_id = 0
  AND upper(COALESCE(mp.player_name, '')) = 'PRIVATEACCOUNT'
ON CONFLICT (match_id, private_slot) DO NOTHING;

-- Compatibility no-op for the old backend during rolling deployment.  It
-- cannot safely resolve an identity because its signature has no match/slot.
-- Returning NULL preserves the private match row; the startup backfill then
-- records and resolves it through v2.  Drop this shim in a later contract-only
-- migration after every running backend uses private_account_observations.
CREATE OR REPLACE FUNCTION find_or_create_private_player(
  p_party_id INTEGER,
  p_account_level INTEGER,
  p_mastery_level INTEGER,
  p_league_tier INTEGER,
  p_league_points INTEGER,
  p_entry_datetime TIMESTAMP WITH TIME ZONE
) RETURNS INTEGER AS $$
BEGIN
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION find_or_create_private_player(
  INTEGER, INTEGER, INTEGER, INTEGER, INTEGER, TIMESTAMP WITH TIME ZONE
) IS 'Deprecated rolling-deploy shim. PartyId is not a person key; v2 resolves immutable match-slot observations.';
