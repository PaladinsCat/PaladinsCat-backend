-- Durable desired-engine selector for one-domain-at-a-time worker migration.
--
-- Ownership rows in migration 110 are crash-expiring runtime leases. This
-- table is the stable operator intent: TypeScript remains the default for all
-- six domains until a verified stage is explicitly transferred to Rust.

CREATE TABLE IF NOT EXISTS worker_scheduler_assignments (
  scheduler_key VARCHAR(64) PRIMARY KEY CHECK (
    scheduler_key IN (
      'ranked_tracker',
      'auto_ingester',
      'baseline_tracker',
      'derived_projection_tracker',
      'hourly_gap_checker',
      'tier_stats'
    )
  ),
  desired_engine VARCHAR(16) NOT NULL CHECK (
    desired_engine IN ('typescript', 'rust')
  ),
  generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
  updated_by VARCHAR(120) NOT NULL DEFAULT 'migration',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO worker_scheduler_assignments (
  scheduler_key,
  desired_engine,
  generation,
  updated_by
)
SELECT scheduler_key, 'typescript', 1, 'migration-111'
FROM unnest(ARRAY[
  'ranked_tracker',
  'auto_ingester',
  'baseline_tracker',
  'derived_projection_tracker',
  'hourly_gap_checker',
  'tier_stats'
]::text[]) scheduler_key
ON CONFLICT (scheduler_key) DO NOTHING;

COMMENT ON TABLE worker_scheduler_assignments IS
  'Durable per-domain migration selector. Changing desired_engine requires a quiesced source owner, passing parity evidence, and a rollback record.';
COMMENT ON COLUMN worker_scheduler_assignments.generation IS
  'Monotonic handoff generation. Increment on every TypeScript/Rust transfer or rollback.';
