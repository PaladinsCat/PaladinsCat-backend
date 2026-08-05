-- Canonical, population-aware match lifecycle for the incremental Rust worker.
--
-- This is an expand-only migration. TypeScript remains the production worker
-- owner until the Rust worker parity and ownership gates pass. The added
-- columns let either implementation reuse durable evidence without repeating
-- Hi-Rez calls, while ranked and non-ranked projections remain physically
-- isolated.

ALTER TABLE match_ingest_status
  ADD COLUMN IF NOT EXISTS queue_id INT,
  ADD COLUMN IF NOT EXISTS population VARCHAR(16) NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS acquisition_state VARCHAR(24) NOT NULL DEFAULT 'discovered',
  ADD COLUMN IF NOT EXISTS detail_attempted_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS roster_resolved_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS demo_resolved_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS direct_player_count SMALLINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS roster_player_count SMALLINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS unresolved_player_ids BIGINT[] NOT NULL DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(100),
  ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;

ALTER TABLE match_ingest_status
  DROP CONSTRAINT IF EXISTS match_ingest_status_population_check;
ALTER TABLE match_ingest_status
  ADD CONSTRAINT match_ingest_status_population_check CHECK (
    population IN ('ranked', 'casual', 'special', 'unknown')
  );

ALTER TABLE match_ingest_status
  DROP CONSTRAINT IF EXISTS match_ingest_status_acquisition_state_check;
ALTER TABLE match_ingest_status
  ADD CONSTRAINT match_ingest_status_acquisition_state_check CHECK (
    acquisition_state IN (
      'discovered', 'detail_pending', 'detail_complete', 'recovery_pending',
      'facts_ready', 'complete', 'limited', 'unavailable'
    )
  );

CREATE INDEX IF NOT EXISTS idx_match_ingest_claim
  ON match_ingest_status (acquisition_state, lease_until, updated_at, match_id)
  WHERE acquisition_state IN (
    'discovered', 'detail_pending', 'recovery_pending', 'facts_ready'
  );

CREATE INDEX IF NOT EXISTS idx_match_ingest_population
  ON match_ingest_status (population, queue_id, updated_at DESC);

-- Composite foreign keys from classified fact tables use this key. match_id is
-- already the primary key; the additional unique constraint lets PostgreSQL
-- enforce the population alongside it instead of trusting worker code.
ALTER TABLE match_ingest_status
  DROP CONSTRAINT IF EXISTS uq_match_ingest_status_population;
ALTER TABLE match_ingest_status
  ADD CONSTRAINT uq_match_ingest_status_population UNIQUE (match_id, population);

COMMENT ON COLUMN match_ingest_status.population IS
  'Immutable projection population after an authoritative queue is known. Ranked, casual, and special projectors must only write their own tables.';
COMMENT ON COLUMN match_ingest_status.acquisition_state IS
  'Compact DB-first acquisition checkpoint shared by cron, requested lookup, and recovery. This is separate from end-to-end projection status.';
COMMENT ON COLUMN match_ingest_status.unresolved_player_ids IS
  'Public player IDs still missing target-match history. Empty means no unresolved public history, not necessarily a complete private roster.';
COMMENT ON COLUMN match_ingest_status.lease_owner IS
  'Cross-process match owner. The in-process request map is only an optimization; this lease is authoritative.';

-- Sparse participant anchors exist only to resume an incomplete match. They
-- are not player statistics and are never read by ranked or casual aggregate
-- queries. Complete match facts continue to use their classified fact tables.
CREATE TABLE IF NOT EXISTS match_ingest_participants (
  match_id BIGINT NOT NULL REFERENCES match_ingest_status(match_id) ON DELETE CASCADE,
  roster_slot SMALLINT NOT NULL CHECK (roster_slot > 0),
  player_id BIGINT NOT NULL DEFAULT 0,
  participant_kind VARCHAR(16) NOT NULL DEFAULT 'human' CHECK (
    participant_kind IN ('human', 'private', 'bot', 'unknown')
  ),
  source VARCHAR(24) NOT NULL CHECK (
    source IN ('direct', 'roster', 'history', 'local')
  ),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id, roster_slot)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_match_ingest_participant_public
  ON match_ingest_participants (match_id, player_id)
  WHERE player_id > 0;

CREATE INDEX IF NOT EXISTS idx_match_ingest_participant_player
  ON match_ingest_participants (player_id, match_id)
  WHERE player_id > 0;

COMMENT ON TABLE match_ingest_participants IS
  'Sparse normalized roster anchors for resumable broken-match recovery. Contains no match statistics and is never an aggregate source.';

-- Supersede the old single-pass terminal policy. TypeScript does not claim
-- this new state; the Rust lifecycle resumes it after its ownership gate.
ALTER TABLE nonranked_match_acquisition
  DROP CONSTRAINT IF EXISTS nonranked_match_acquisition_status_check;
ALTER TABLE nonranked_match_acquisition
  ADD CONSTRAINT nonranked_match_acquisition_status_check CHECK (
    status IN (
      'discovered', 'waiting_for_completion', 'fetching', 'complete_direct',
      'partial_roster', 'roster_only', 'recovery_pending',
      'service_deferred', 'dropped'
    )
  );

ALTER TABLE nonranked_match_acquisition
  ADD COLUMN IF NOT EXISTS canonical_adopted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_nonranked_match_canonical_adoption
  ON nonranked_match_acquisition (match_id)
  WHERE canonical_adopted_at IS NULL;

COMMENT ON COLUMN nonranked_match_acquisition.canonical_adopted_at IS
  'DB-only checkpoint for bounded adoption into match_ingest_status. Adoption never performs a provider call.';

ALTER TABLE player_activity_profile_refresh
  ADD COLUMN IF NOT EXISTS needs_platform BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS needs_region BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS claim_owner VARCHAR(100);

CREATE INDEX IF NOT EXISTS idx_player_activity_profile_unknown_due
  ON player_activity_profile_refresh (
    needs_platform DESC,
    needs_region DESC,
    status,
    next_retry_at,
    lease_until,
    player_id
  )
  WHERE needs_platform OR needs_region;

COMMENT ON COLUMN player_activity_profile_refresh.needs_platform IS
  'True only when the resolved local player identity has no known platform.';
COMMENT ON COLUMN player_activity_profile_refresh.needs_region IS
  'True only when the resolved local player identity has no known region.';
COMMENT ON COLUMN player_activity_profile_refresh.claim_owner IS
  'Rust/TypeScript compatible cross-process owner for a getplayerbatch claim.';
