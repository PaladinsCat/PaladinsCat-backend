-- Exclusive ownership and per-job leases for the full native worker runtime.
--
-- This migration does not transfer production ownership. It creates the
-- database authority used by the Rust candidate, the eventual TypeScript ->
-- Rust cutover, and rollback. Each scheduler domain transfers independently
-- after its own parity gate; the final wave may acquire all six atomically.

CREATE TABLE IF NOT EXISTS worker_scheduler_ownership (
  scheduler_key VARCHAR(64) PRIMARY KEY,
  owner_id VARCHAR(120) NOT NULL,
  engine VARCHAR(16) NOT NULL CHECK (engine IN ('typescript', 'rust')),
  lease_until TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_worker_scheduler_ownership_lease
  ON worker_scheduler_ownership (lease_until, scheduler_key);

COMMENT ON TABLE worker_scheduler_ownership IS
  'Exclusive per-domain runtime ownership for staged scheduler migration. A timer may execute only while its engine holds that domain row.';

CREATE TABLE IF NOT EXISTS worker_job_leases (
  job_key VARCHAR(100) PRIMARY KEY,
  scheduler_key VARCHAR(64) NOT NULL,
  owner_id VARCHAR(120) NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempt BIGINT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_worker_job_leases_due
  ON worker_job_leases (lease_until, scheduler_key, job_key);

COMMENT ON TABLE worker_job_leases IS
  'Cross-process, crash-expiring execution lease for one concrete background job. It replaces process-local mutexes as the authority.';

CREATE TABLE IF NOT EXISTS worker_job_run_log (
  run_id BIGSERIAL PRIMARY KEY,
  job_key VARCHAR(100) NOT NULL,
  scheduler_key VARCHAR(64) NOT NULL,
  owner_id VARCHAR(120) NOT NULL,
  trigger VARCHAR(32) NOT NULL,
  status VARCHAR(16) NOT NULL CHECK (
    status IN ('running', 'completed', 'failed', 'cancelled', 'skipped')
  ),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  duration_ms BIGINT,
  result JSONB,
  error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_worker_job_run_log_status
  ON worker_job_run_log (job_key, started_at DESC, status);

COMMENT ON TABLE worker_job_run_log IS
  'Compact native worker execution audit. Payloads are summaries only; provider responses remain owned by HirezRelay and normalized fact tables.';
