import { one, query } from '../config/db';
import { ensureHourlyIngestMatchDebtTable } from './hourly-ingest-match-debt';

export type HourlyIngestStatus = 'pending' | 'fetching' | 'staged' | 'empty' | 'complete' | 'failed';

export type HourlyIngestStateRow = {
  date: string;
  hour: number;
  queue_id: number;
  status: HourlyIngestStatus;
  attempts: number;
  raw_match_count: number;
  staged_match_count: number;
  fetched: boolean;
  fetch_succeeded: boolean;
  source: string | null;
  error_message: string | null;
  last_attempt_at: string | null;
  next_retry_at: string | null;
  lease_until: string | null;
  completed_at: string | null;
};

const FETCH_LEASE_MINUTES = 30;
const STAGED_LEASE_MINUTES = 60;
const FAILED_RETRY_MINUTES = 30;
const QUOTA_WAIT_RETRY_MINUTES = Math.max(
  5,
  Number(process.env.HOURLY_INGEST_QUOTA_WAIT_RETRY_MINUTES || 15),
);

// Recovery failures are quota-related but also time-sensitive. Broken skin
// recovery often depends on player match histories, and Hi-Rez only returns the
// recent window. Waiting six hours after a budget stop can let busy players move
// beyond that 50-match window. Keep this retry slower than generic transient
// failures, but short enough that a poisoned ranked hour is retried while its
// history anchors are still likely to exist.
const BUDGET_EXHAUSTED_RETRY_MINUTES = Math.max(
  FAILED_RETRY_MINUTES,
  Number(process.env.HOURLY_INGEST_BUDGET_RETRY_MINUTES || 60),
);
const EMPTY_RECHECK_MINUTES = 6 * 60;
const EMPTY_SLOW_RECHECK_MINUTES = 24 * 60;

let hourlyIngestStateReady = false;

export async function ensureHourlyIngestStateTable(): Promise<void> {
  if (hourlyIngestStateReady) return;

  // This table is the scheduler control plane for hourly ranked-match ingest.
  // `hourly_match_counts.total_matches = 0` is an analytics value, not a safe
  // retry signal: a real zero-match hour, a temporary Hi-Rez empty response, a
  // crashed backend, and a still-draining raw buffer can all look like "zero."
  // The state table separates those cases so gap recovery can resume missed
  // work without blindly refetching the same hour on every cron tick.
  await one(`
    CREATE TABLE IF NOT EXISTS hourly_ingest_state (
      date DATE NOT NULL,
      hour INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
      queue_id INT NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'fetching', 'staged', 'empty', 'complete', 'failed')),
      attempts INT NOT NULL DEFAULT 0,
      raw_match_count INT NOT NULL DEFAULT 0,
      staged_match_count INT NOT NULL DEFAULT 0,
      fetched BOOLEAN NOT NULL DEFAULT FALSE,
      fetch_succeeded BOOLEAN NOT NULL DEFAULT FALSE,
      source VARCHAR(50),
      error_message TEXT,
      last_attempt_at TIMESTAMPTZ,
      next_retry_at TIMESTAMPTZ,
      lease_until TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (date, hour, queue_id)
    )`);
  await one(`CREATE INDEX IF NOT EXISTS idx_his_status_retry ON hourly_ingest_state (status, next_retry_at, lease_until)`);
  await one(`CREATE INDEX IF NOT EXISTS idx_his_queue_window ON hourly_ingest_state (queue_id, date, hour)`);
  hourlyIngestStateReady = true;
}

/**
 * Durably remember an hour that could not cross the first Hi-Rez boundary
 * because every key was already at its reserve. This is intentionally not a
 * fetch claim: attempts stay at zero and no lease is taken. Existing rows are
 * never downgraded, so active/staged/complete work remains authoritative.
 */
export async function recordHourlyIngestQuotaWait(
  date: string,
  hour: number,
  queueId: number,
  source: string,
  reason: string,
): Promise<void> {
  await ensureHourlyIngestStateTable();
  await one(
    `INSERT INTO hourly_ingest_state (
       date, hour, queue_id, status, attempts, raw_match_count,
       staged_match_count, fetched, fetch_succeeded, source, error_message,
       last_attempt_at, next_retry_at, lease_until, updated_at
     )
     VALUES (
       $1::date, $2, $3, 'pending', 0, 0,
       0, false, false, $4, $5,
       NULL, now() + ($6::int * interval '1 minute'), NULL, now()
     )
     ON CONFLICT (date, hour, queue_id) DO NOTHING`,
    [date, hour, queueId, source, reason, QUOTA_WAIT_RETRY_MINUTES],
  );
}

export async function claimHourlyIngestHour(
  date: string,
  hour: number,
  queueId: number,
  source: string,
  allowDueDebtRetry = false,
): Promise<boolean> {
  await ensureHourlyIngestStateTable();

  const rows = await query(
    `INSERT INTO hourly_ingest_state (
       date, hour, queue_id, status, attempts, fetched, fetch_succeeded,
       source, error_message, last_attempt_at, next_retry_at, lease_until, updated_at
     )
     VALUES (
       $1::date, $2, $3, 'fetching', 1, false, false,
       $4, NULL, now(), NULL, now() + ($5::int * interval '1 minute'), now()
     )
     ON CONFLICT (date, hour, queue_id) DO UPDATE
     SET status = 'fetching',
         attempts = hourly_ingest_state.attempts + 1,
         fetched = false,
         fetch_succeeded = false,
         source = EXCLUDED.source,
         error_message = NULL,
         last_attempt_at = now(),
         next_retry_at = NULL,
         lease_until = now() + ($5::int * interval '1 minute'),
         updated_at = now()
     WHERE
       (
         hourly_ingest_state.status IN ('pending', 'failed')
         AND (hourly_ingest_state.next_retry_at IS NULL OR hourly_ingest_state.next_retry_at <= now())
       )
       OR (
         hourly_ingest_state.status IN ('fetching', 'staged')
         AND (hourly_ingest_state.lease_until IS NULL OR hourly_ingest_state.lease_until <= now())
       )
       OR (
         hourly_ingest_state.status = 'empty'
         AND (hourly_ingest_state.next_retry_at IS NULL OR hourly_ingest_state.next_retry_at <= now())
       )
       OR (
          $6::boolean = true
          AND hourly_ingest_state.status IN ('pending', 'failed', 'staged', 'complete')
       )
     RETURNING date`,
    [date, hour, queueId, source, FETCH_LEASE_MINUTES, allowDueDebtRetry],
  );

  return rows.length > 0;
}

export async function markHourlyIngestEmpty(date: string, hour: number, queueId: number): Promise<void> {
  await ensureHourlyIngestStateTable();
  await one(
    `UPDATE hourly_ingest_state
     SET status = 'empty',
         raw_match_count = 0,
         staged_match_count = 0,
         fetched = true,
         fetch_succeeded = true,
         error_message = NULL,
         lease_until = NULL,
         next_retry_at = now() + (
           CASE WHEN attempts >= 3 THEN $4::int ELSE $5::int END * interval '1 minute'
         ),
         updated_at = now()
     WHERE date = $1::date AND hour = $2 AND queue_id = $3`,
    [date, hour, queueId, EMPTY_SLOW_RECHECK_MINUTES, EMPTY_RECHECK_MINUTES],
  );

  // Optional analytics row for dashboards. It is deliberately not used as the
  // gap-checker control signal, and it never overwrites a positive count.
  await one(
    `INSERT INTO hourly_match_counts (date, hour, queue_id, total_matches, fetched_at)
     VALUES ($1::date, $2, $3, 0, now())
     ON CONFLICT (date, hour, queue_id) DO NOTHING`,
    [date, hour, queueId],
  );
}

export async function markHourlyIngestStaged(
  date: string,
  hour: number,
  queueId: number,
  rawMatchCount: number,
  stagedMatchCount: number,
): Promise<void> {
  await ensureHourlyIngestStateTable();
  await one(
    `UPDATE hourly_ingest_state
     SET status = 'staged',
         raw_match_count = GREATEST(raw_match_count, $4),
         staged_match_count = GREATEST(staged_match_count, $5),
         fetched = true,
         fetch_succeeded = true,
         error_message = NULL,
         lease_until = now() + ($6::int * interval '1 minute'),
         next_retry_at = NULL,
         updated_at = now()
     WHERE date = $1::date AND hour = $2 AND queue_id = $3`,
    [date, hour, queueId, rawMatchCount, stagedMatchCount, STAGED_LEASE_MINUTES],
  );
}

export async function markHourlyIngestComplete(
  date: string,
  hour: number,
  queueId: number,
  totalMatches: number,
): Promise<void> {
  await ensureHourlyIngestStateTable();
  await ensureHourlyIngestMatchDebtTable();
  await one(
    `WITH terminal_debt AS (
       SELECT COUNT(*)::int AS terminal_count
       FROM hourly_ingest_match_debt debt
       WHERE debt.date = $1::date
         AND debt.hour = $2
         AND debt.queue_id = $3
         AND debt.status = 'unrecoverable'
     )
     UPDATE hourly_ingest_state
     SET status = 'complete',
         raw_match_count = GREATEST(raw_match_count, $4),
         staged_match_count = GREATEST(staged_match_count, LEAST(GREATEST(raw_match_count, $4), $4 + terminal_debt.terminal_count)),
         fetched = true,
         fetch_succeeded = true,
         error_message = NULL,
         lease_until = NULL,
         next_retry_at = NULL,
         completed_at = now(),
         updated_at = now()
     FROM terminal_debt
     WHERE date = $1::date
       AND hour = $2
       AND queue_id = $3
       AND (
         raw_match_count = 0
         OR $4 >= raw_match_count
         OR $4 + terminal_debt.terminal_count >= raw_match_count
         OR status = 'complete'
       )
       AND NOT EXISTS (
         SELECT 1
         FROM hourly_ingest_match_debt debt
         WHERE debt.date = $1::date
           AND debt.hour = $2
           AND debt.queue_id = $3
           AND debt.status IN ('pending', 'staged')
       )`,
    [date, hour, queueId, totalMatches],
  );
}

export async function markHourlyIngestFailed(
  date: string,
  hour: number,
  queueId: number,
  error: unknown,
  rawMatchCount: number | null = null,
  stagedMatchCount: number | null = null,
): Promise<void> {
  await ensureHourlyIngestStateTable();
  const message = error instanceof Error ? error.message : String(error);
  const retryMinutes = /budget exhausted|massive drop/i.test(message)
    ? BUDGET_EXHAUSTED_RETRY_MINUTES
    : FAILED_RETRY_MINUTES;

  // Budget exhaustion is different from an ordinary network failure. It means
  // the hour contained enough poisoned getmatchdetailsbatch rows that discovery
  // deliberately stopped spending calls. Retrying that again 30 minutes later
  // can burn the same capped budget repeatedly, so those failures use a slower
  // retry window while keeping raw/staged counts visible for monitoring.
  await one(
    `UPDATE hourly_ingest_state
     SET status = 'failed',
         raw_match_count = GREATEST(raw_match_count, COALESCE($6::int, raw_match_count)),
         staged_match_count = GREATEST(staged_match_count, COALESCE($7::int, staged_match_count)),
         fetched = true,
         fetch_succeeded = false,
         error_message = $4,
         lease_until = NULL,
         next_retry_at = now() + ($5::int * interval '1 minute'),
         updated_at = now()
     WHERE date = $1::date AND hour = $2 AND queue_id = $3`,
    [date, hour, queueId, message, retryMinutes, rawMatchCount, stagedMatchCount],
  );
}

export async function getHourlyIngestStates(
  queueId: number,
  minDate: string,
  maxDate: string,
): Promise<HourlyIngestStateRow[]> {
  await ensureHourlyIngestStateTable();
  return query<HourlyIngestStateRow>(
    `SELECT date::text, hour, queue_id, status, attempts, raw_match_count, staged_match_count,
            fetched, fetch_succeeded, source, error_message,
            last_attempt_at::text, next_retry_at::text, lease_until::text, completed_at::text
     FROM hourly_ingest_state
     WHERE queue_id = $1
       AND date >= $2::date
       AND date <= $3::date`,
    [queueId, minDate, maxDate],
  );
}
