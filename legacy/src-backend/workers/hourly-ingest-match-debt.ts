import { one, query } from '../config/db';

export type HourlyIngestMatchDebtStatus = 'pending' | 'staged' | 'complete' | 'unrecoverable';

const DEFAULT_RETRY_MINUTES = Math.max(
  5,
  Number(process.env.HOURLY_INGEST_MATCH_DEBT_RETRY_MINUTES || 10),
);
const NO_AUTH_PAYLOAD_SLOW_RETRY_AFTER_ATTEMPTS = Math.max(
  1,
  Number(process.env.HOURLY_INGEST_NO_AUTH_SLOW_RETRY_AFTER_ATTEMPTS || 5),
);
const NO_AUTH_PAYLOAD_SLOW_RETRY_MINUTES = Math.max(
  DEFAULT_RETRY_MINUTES,
  Number(process.env.HOURLY_INGEST_NO_AUTH_SLOW_RETRY_MINUTES || 180),
);
const NO_AUTH_PAYLOAD_FRESH_RETRY_MINUTES = Math.max(
  1,
  Number(process.env.HOURLY_INGEST_NO_AUTH_FRESH_RETRY_MINUTES || 5),
);
const NO_AUTH_PAYLOAD_FRESH_WINDOW_HOURS = Math.max(
  1,
  Number(process.env.HOURLY_INGEST_NO_AUTH_FRESH_WINDOW_HOURS || 6),
);
const BATCH_ONLY_NO_AUTHORITY_MAX_ATTEMPTS = Math.max(
  1,
  Number(
    process.env.HOURLY_INGEST_BATCH_ONLY_NO_AUTHORITY_MAX_ATTEMPTS
    || process.env.HOURLY_INGEST_BATCH_ONLY_PROFILE_ONLY_MAX_ATTEMPTS
    || 2,
  ),
);

let hourlyIngestMatchDebtReady = false;

function normalizeMatchIds(matchIds: number[]): number[] {
  return [...new Set(
    matchIds
      .map(id => Number(id))
      .filter(id => Number.isFinite(id) && id > 0),
  )];
}

/**
 * Durable per-match debt ledger for ranked hourly discovery.
 *
 * `hourly_ingest_state` answers "should this hour run again?" but it only
 * stores counts. That is good enough to avoid loops, but not enough to prove
 * every discovered match ID eventually finished. A partial hour like
 * "28 discovered / 19 staged" needs the 9 missing IDs stored explicitly so a
 * restart, retention cleanup, or changed Hi-Rez hourly response cannot erase
 * the recovery obligation.
 *
 * Status contract:
 * - pending: the match ID is known and due for another detail/recovery attempt.
 *   Fresh ranked debt must retry aggressively because getmatchhistory is a
 *   rolling 50-match window; delaying recovery does not save budget, it only
 *   increases permanent loss risk. Slow retry is reserved for old no-authority
 *   rows after the fresh recovery window has passed.
 * - staged: a payload or another worker claim exists; do not spend API calls.
 * - complete: the match header, authoritative roster, per-player facts, and
 *   match bans are durable and the PostgreSQL match-detail read model is ready.
 *   Derived statistics and search/profile projections may still be running.
 * - unrecoverable: explicit terminal no-anchor cases, plus the narrow
 *   batch-only no-authority fuse after the fresh recovery window. A match can
 *   land here immediately when discovery proves there are no player IDs to
 *   search (`getmatchdetailsbatch` cannot parse it and `getplayerbatchfrommatch`
 *   returned no usable anchors). It can also land here after the configured
 *   batch-only retry cap when the worker already spent the exact-ID recovery
 *   path but still received only profile/demo rows, empty recovery, or other
 *   non-promotable data after the fresh window closed. That second class stays
 *   retryable while fresh so exact known IDs keep trying before players roll out
 *   of the 50-match history window, but it cannot burn forever.
 */
export async function ensureHourlyIngestMatchDebtTable(): Promise<void> {
  if (hourlyIngestMatchDebtReady) return;

  await one(`
    CREATE TABLE IF NOT EXISTS hourly_ingest_match_debt (
      match_id BIGINT PRIMARY KEY,
      date DATE NOT NULL,
      hour INT NOT NULL CHECK (hour >= 0 AND hour <= 23),
      queue_id INT NOT NULL,
      status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'staged', 'complete', 'unrecoverable')),
      reason TEXT,
      attempts INT NOT NULL DEFAULT 0,
      first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_attempt_at TIMESTAMPTZ,
      next_retry_at TIMESTAMPTZ,
      staged_at TIMESTAMPTZ,
      completed_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await one(`
    CREATE INDEX IF NOT EXISTS idx_himd_queue_window_status
      ON hourly_ingest_match_debt (queue_id, date, hour, status)`);
  await one(`
    CREATE INDEX IF NOT EXISTS idx_himd_pending_retry
      ON hourly_ingest_match_debt (status, next_retry_at, updated_at)
      WHERE status = 'pending'`);
  hourlyIngestMatchDebtReady = true;
}

export async function recordHourlyIngestDiscoveredMatches(
  date: string,
  hour: number,
  queueId: number,
  matchIds: number[],
  reason = 'discovered by hourly ingest',
): Promise<void> {
  const ids = normalizeMatchIds(matchIds);
  if (ids.length === 0) return;
  await ensureHourlyIngestMatchDebtTable();

  await one(
    `INSERT INTO hourly_ingest_match_debt (
       match_id, date, hour, queue_id, status, reason, attempts,
       first_seen_at, last_attempt_at, next_retry_at, updated_at
     )
     SELECT id::bigint, $1::date, $2::int, $3::int, 'pending', $5::text, 1,
            now(), now(), now() + ($6::int * interval '1 minute'), now()
     FROM unnest($4::bigint[]) AS ids(id)
     ON CONFLICT (match_id) DO UPDATE
     SET date = EXCLUDED.date,
         hour = EXCLUDED.hour,
         queue_id = EXCLUDED.queue_id,
         status = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.status
           ELSE 'pending'
         END,
         reason = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.reason
           ELSE EXCLUDED.reason
         END,
         attempts = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.attempts
           ELSE hourly_ingest_match_debt.attempts + 1
         END,
         last_attempt_at = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.last_attempt_at
           ELSE now()
         END,
         next_retry_at = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.next_retry_at
           ELSE now() + ($6::int * interval '1 minute')
         END,
         updated_at = now()`,
    [date, hour, queueId, ids.map(String), reason, DEFAULT_RETRY_MINUTES],
  );
}

/**
 * Reconcile IDs that discovery did not need to fetch because they are already
 * locally handled or already staged. This prevents "skipped by guard" matches
 * from remaining as open debt forever.
 */
export async function markHourlyIngestMatchDebtStagedOrComplete(
  matchIds: number[],
  reason = 'staged or already handled by ingest guard',
): Promise<void> {
  const ids = normalizeMatchIds(matchIds);
  if (ids.length === 0) return;
  await ensureHourlyIngestMatchDebtTable();

  await one(
    `WITH ids AS (
       SELECT id::bigint AS match_id FROM unnest($1::bigint[]) AS ids(id)
     ),
     complete_ids AS (
       SELECT mis.match_id
       FROM match_ingest_status mis
       JOIN ids ON ids.match_id = mis.match_id
       WHERE mis.status IN ('complete', 'limited')
       UNION
       SELECT m.match_id
       FROM matches m
       JOIN ids ON ids.match_id = m.match_id
       LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
       JOIN (
         SELECT match_id
         FROM match_players
         GROUP BY match_id
         HAVING count(*) >= 10
       ) mp ON mp.match_id = m.match_id
       WHERE mis.status IS NULL OR mis.status IN ('complete', 'limited')
     )
     UPDATE hourly_ingest_match_debt debt
     SET status = CASE WHEN complete_ids.match_id IS NULL THEN 'staged' ELSE 'complete' END,
         reason = $2,
         staged_at = CASE
           WHEN complete_ids.match_id IS NULL THEN COALESCE(debt.staged_at, now())
           ELSE debt.staged_at
         END,
         completed_at = CASE
           WHEN complete_ids.match_id IS NOT NULL THEN COALESCE(debt.completed_at, now())
           ELSE debt.completed_at
         END,
         next_retry_at = NULL,
         updated_at = now()
     FROM ids
     LEFT JOIN complete_ids ON complete_ids.match_id = ids.match_id
     WHERE debt.match_id = ids.match_id
       AND debt.status NOT IN ('complete', 'unrecoverable')`,
    [ids.map(String), reason],
  );
}

export async function markHourlyIngestMatchDebtPending(
  date: string,
  hour: number,
  queueId: number,
  matchIds: number[],
  reason: string,
  retryMinutes = DEFAULT_RETRY_MINUTES,
): Promise<void> {
  const ids = normalizeMatchIds(matchIds);
  if (ids.length === 0) return;
  await ensureHourlyIngestMatchDebtTable();

  await one(
    `INSERT INTO hourly_ingest_match_debt (
       match_id, date, hour, queue_id, status, reason, attempts,
       first_seen_at, last_attempt_at, next_retry_at, updated_at
     )
     SELECT id::bigint, $1::date, $2::int, $3::int, 'pending', $5::text, 1,
            now(), now(), now() + ($6::int * interval '1 minute'), now()
     FROM unnest($4::bigint[]) AS ids(id)
     ON CONFLICT (match_id) DO UPDATE
     SET date = EXCLUDED.date,
         hour = EXCLUDED.hour,
         queue_id = EXCLUDED.queue_id,
         status = CASE
           WHEN hourly_ingest_match_debt.status = 'complete' THEN 'complete'
           WHEN hourly_ingest_match_debt.status = 'unrecoverable' THEN 'unrecoverable'
           WHEN ($5::text ILIKE '%batch-only profile-only%' OR $5::text ILIKE '%batch-only non-authoritative%')
             AND hourly_ingest_match_debt.first_seen_at < now() - ($9::int * interval '1 hour')
             AND hourly_ingest_match_debt.attempts + 1 >= $11::int
             THEN 'unrecoverable'
           ELSE 'pending'
         END,
         reason = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.reason
           ELSE EXCLUDED.reason
         END,
         attempts = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.attempts
           ELSE hourly_ingest_match_debt.attempts + 1
         END,
         last_attempt_at = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.last_attempt_at
           ELSE now()
         END,
         next_retry_at = CASE
           WHEN hourly_ingest_match_debt.status IN ('complete', 'unrecoverable') THEN hourly_ingest_match_debt.next_retry_at
           WHEN ($5::text ILIKE '%batch-only profile-only%' OR $5::text ILIKE '%batch-only non-authoritative%')
             AND hourly_ingest_match_debt.first_seen_at < now() - ($9::int * interval '1 hour')
             AND hourly_ingest_match_debt.attempts + 1 >= $11::int
             THEN NULL
           WHEN ($5::text ILIKE '%batch-only profile-only%' OR $5::text ILIKE '%batch-only non-authoritative%')
             THEN now() + ($6::int * interval '1 minute')
           WHEN $5::text ILIKE '%no authoritative payload%'
             AND hourly_ingest_match_debt.first_seen_at < now() - ($9::int * interval '1 hour')
             AND hourly_ingest_match_debt.attempts + 1 >= $7::int
             THEN now() + ($8::int * interval '1 minute')
           WHEN $5::text ILIKE '%no authoritative payload%'
             THEN now() + ($10::int * interval '1 minute')
           ELSE now() + ($6::int * interval '1 minute')
         END,
         updated_at = now()`,
    [
      date,
      hour,
      queueId,
      ids.map(String),
      reason,
      retryMinutes,
      NO_AUTH_PAYLOAD_SLOW_RETRY_AFTER_ATTEMPTS,
      NO_AUTH_PAYLOAD_SLOW_RETRY_MINUTES,
      NO_AUTH_PAYLOAD_FRESH_WINDOW_HOURS,
      NO_AUTH_PAYLOAD_FRESH_RETRY_MINUTES,
      BATCH_ONLY_NO_AUTHORITY_MAX_ATTEMPTS,
    ],
  );
}

export async function markHourlyIngestMatchDebtComplete(matchId: number): Promise<void> {
  const id = Number(matchId);
  if (!Number.isFinite(id) || id <= 0) return;
  await ensureHourlyIngestMatchDebtTable();

  await one(
     `UPDATE hourly_ingest_match_debt
     SET status = 'complete',
          reason = 'match facts durable and readable',
         completed_at = COALESCE(completed_at, now()),
         next_retry_at = NULL,
         updated_at = now()
     WHERE match_id = $1`,
    [id],
  );
}

export async function markHourlyIngestMatchDebtUnrecoverable(
  matchIds: number[],
  reason: string,
): Promise<void> {
  const ids = normalizeMatchIds(matchIds);
  if (ids.length === 0) return;
  await ensureHourlyIngestMatchDebtTable();

  await one(
    `UPDATE hourly_ingest_match_debt
     SET status = CASE WHEN status = 'complete' THEN status ELSE 'unrecoverable' END,
         reason = CASE WHEN status = 'complete' THEN reason ELSE $2 END,
         next_retry_at = NULL,
         updated_at = now()
     WHERE match_id = ANY($1::bigint[])
       AND status <> 'complete'`,
    [ids.map(String), reason],
  );
}

export async function reviveRetryableHourlyIngestMatchDebt(
  date: string,
  hour: number,
  queueId: number,
): Promise<number> {
  await ensureHourlyIngestMatchDebtTable();

  const rows = await query<{ match_id: string }>(
    `UPDATE hourly_ingest_match_debt
     SET status = 'pending',
         reason = 'retryable revival: previous terminal classification did not prove api_no_data; ' || COALESCE(reason, ''),
         next_retry_at = now(),
         updated_at = now()
     WHERE date = $1::date
       AND hour = $2
       AND queue_id = $3
       AND status = 'unrecoverable'
       AND COALESCE(reason, '') NOT ILIKE 'api_no_data:%'
     RETURNING match_id::text`,
    [date, hour, queueId],
  );

  return rows.length;
}

export async function reviveFreshNoAuthorityHourlyIngestMatchDebt(
  queueId: number,
  minDate: string,
  maxDate: string,
): Promise<Array<{ date: string; hour: number; revived: number }>> {
  await ensureHourlyIngestMatchDebtTable();

  // Automatic fresh revival is only for exact known match IDs, never blind
  // rediscovery. Batch-only no-authority rows are allowed back into pending
  // while they are still fresh because a Hi-Rez detail outage can temporarily
  // expose only demo/profile anchors; after the fresh window, the fuse remains
  // terminal so the same full-history burn cannot continue forever.
  const rows = await query<{ date: string; hour: number; revived: string }>(
    `WITH revived AS (
       UPDATE hourly_ingest_match_debt
       SET status = 'pending',
           reason = 'fresh retryable revival: previous terminal classification did not prove api_no_data; ' || COALESCE(reason, ''),
           next_retry_at = now(),
           updated_at = now()
       WHERE queue_id = $1
         AND date >= $2::date
         AND date <= $3::date
         AND status = 'unrecoverable'
         AND first_seen_at >= now() - ($4::int * interval '1 hour')
         AND COALESCE(reason, '') NOT ILIKE 'api_no_data:%'
         AND (
           COALESCE(reason, '') ILIKE '%no authoritative payload%'
           OR COALESCE(reason, '') ILIKE 'dropped/corrupt:%'
         )
       RETURNING date, hour
     )
     SELECT date::text, hour, count(*)::int AS revived
     FROM revived
     GROUP BY date, hour
     ORDER BY date ASC, hour ASC`,
    [queueId, minDate, maxDate, NO_AUTH_PAYLOAD_FRESH_WINDOW_HOURS],
  );

  return rows.map(row => ({
    date: row.date,
    hour: Number(row.hour),
    revived: Number(row.revived) || 0,
  }));
}
export async function markHourlyIngestMatchDebtRetryable(
  matchId: number,
  reason: string,
  retryMinutes = DEFAULT_RETRY_MINUTES,
): Promise<void> {
  const id = Number(matchId);
  if (!Number.isFinite(id) || id <= 0) return;
  await ensureHourlyIngestMatchDebtTable();

  await one(
    `UPDATE hourly_ingest_match_debt
     SET status = CASE WHEN status = 'complete' THEN status ELSE 'pending' END,
         reason = CASE WHEN status = 'complete' THEN reason ELSE $2 END,
         next_retry_at = CASE
           WHEN status = 'complete' THEN next_retry_at
           ELSE now() + ($3::int * interval '1 minute')
         END,
         updated_at = now()
     WHERE match_id = $1
       AND status <> 'unrecoverable'`,
    [id, reason, retryMinutes],
  );
}

export async function getDueHourlyIngestMatchDebtIds(
  date: string,
  hour: number,
  queueId: number,
  limit = 250,
  includeRetryCooldown = false,
): Promise<number[]> {
  await ensureHourlyIngestMatchDebtTable();
  const rows = await query<{ match_id: string }>(
    `SELECT match_id::text
     FROM hourly_ingest_match_debt
     WHERE date = $1::date
       AND hour = $2
       AND queue_id = $3
       AND status = 'pending'
       AND ($5::boolean = true OR next_retry_at IS NULL OR next_retry_at <= now())
     ORDER BY attempts ASC, first_seen_at ASC
     LIMIT $4`,
    [date, hour, queueId, limit, includeRetryCooldown],
  );
  return rows.map(row => Number(row.match_id)).filter(id => Number.isFinite(id) && id > 0);
}

export async function getDueHourlyIngestDebtHours(
  queueId: number,
  minDate: string,
  maxDate: string,
): Promise<Array<{ date: string; hour: number }>> {
  await ensureHourlyIngestMatchDebtTable();
  return query<{ date: string; hour: number }>(
    `SELECT DISTINCT date::text, hour
     FROM hourly_ingest_match_debt
     WHERE queue_id = $1
       AND date >= $2::date
       AND date <= $3::date
       AND status = 'pending'
       AND (next_retry_at IS NULL OR next_retry_at <= now())
     ORDER BY date ASC, hour ASC`,
    [queueId, minDate, maxDate],
  );
}
