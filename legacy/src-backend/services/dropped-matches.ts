import { one, query } from '../config/db';

export type DroppedMatchFilters = {
  date: string;
  queueId: number;
  hour?: number | null;
  status?: string | null;
  category?: string | null;
  limit?: number;
  offset?: number;
};

let droppedMatchesReady = false;

function normalizeLimit(value: unknown, fallback = 500): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), 2000);
}

function normalizeOffset(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 0) return 0;
  return Math.floor(parsed);
}

export function normalizeDroppedMatchDate(value: unknown): string {
  const raw = String(value || new Date().toISOString().slice(0, 10)).trim();
  if (/^\d{8}$/.test(raw)) return raw.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3');
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  throw new Error('date must be YYYY-MM-DD or YYYYMMDD');
}

export function normalizeDroppedMatchFilters(queryParams: any): DroppedMatchFilters {
  const hour = queryParams.hour === undefined || queryParams.hour === ''
    ? null
    : Number(queryParams.hour);
  if (hour !== null && (!Number.isInteger(hour) || hour < 0 || hour > 23)) {
    throw new Error('hour must be an integer from 0 to 23');
  }

  return {
    date: normalizeDroppedMatchDate(queryParams.date),
    queueId: Number.isFinite(Number(queryParams.queueId)) ? Number(queryParams.queueId) : 486,
    hour,
    // The endpoint name is intentionally strict: by default it returns only
    // true dropped/no-data matches. Use status=open to inspect every unresolved
    // recovery-debt row, including broken-skin/Int16 blockers that still have
    // Hi-Rez data but cannot yet produce an authoritative normalized payload.
    status: queryParams.status ? String(queryParams.status).trim().toLowerCase() : 'dropped',
    category: queryParams.category ? String(queryParams.category).trim() : null,
    limit: normalizeLimit(queryParams.limit),
    offset: normalizeOffset(queryParams.offset),
  };
}

export async function ensureDroppedMatchesTable(): Promise<void> {
  if (droppedMatchesReady) return;

  // Keep this runtime guard in addition to the SQL migration because local
  // desktop stacks are often rebuilt from an already-running database. The
  // endpoint should become available immediately after a backend deploy without
  // requiring a manual migration command.
  await one(`
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
    )`);
  await one(`CREATE INDEX IF NOT EXISTS idx_dropped_matches_window ON dropped_matches (date DESC, hour ASC, queue_id)`);
  await one(`CREATE INDEX IF NOT EXISTS idx_dropped_matches_status ON dropped_matches (status, drop_category, next_retry_at)`);
  droppedMatchesReady = true;
}

export async function refreshDroppedMatches(date: string, queueId = 486): Promise<number> {
  await ensureDroppedMatchesTable();

  const result = await query<{ match_id: string }>(
    `WITH source AS (
       SELECT
         debt.match_id,
         debt.date,
         debt.hour,
         debt.queue_id,
         CASE
           WHEN debt.status = 'complete' OR mis.status IN ('complete', 'limited') THEN 'complete'
           ELSE debt.status
         END AS status,
         debt.reason,
         debt.attempts,
         debt.first_seen_at,
         debt.last_attempt_at,
         debt.next_retry_at,
         debt.staged_at,
         debt.completed_at,
         mis.status AS ingest_status,
         mis.error_message AS ingest_error,
         hist.observed_players,
         rib.status AS raw_buffer_status,
         rib.error_message AS raw_buffer_error,
         existing.match_id IS NOT NULL AS already_tracked
       FROM hourly_ingest_match_debt debt
       LEFT JOIN match_ingest_status mis ON mis.match_id = debt.match_id
       LEFT JOIN dropped_matches existing ON existing.match_id = debt.match_id
       LEFT JOIN LATERAL (
         SELECT COUNT(DISTINCT player_id)::int AS observed_players
         FROM player_match_history_entries
         WHERE match_id = debt.match_id
       ) hist ON true
       LEFT JOIN LATERAL (
         SELECT status, error_message
         FROM raw_ingest_buffer
         WHERE entity_type = 'match'
           AND entity_id = debt.match_id::text
         ORDER BY created_at DESC, id DESC
         LIMIT 1
       ) rib ON true
       WHERE debt.date = $1::date
         AND debt.queue_id = $2
         AND (
           existing.match_id IS NOT NULL
           OR debt.status <> 'complete'
           OR COALESCE(debt.reason, '') ILIKE '%no authoritative payload%'
           OR COALESCE(mis.status, '') = 'failed'
         )
     ),
     classified AS (
       SELECT *,
         CASE
           WHEN status = 'complete' OR ingest_status IN ('complete', 'limited') THEN 'complete'
           WHEN COALESCE(ingest_error, raw_buffer_error, '') ILIKE '%missing positive queue_id%'
             OR COALESCE(ingest_error, raw_buffer_error, '') ILIKE '%missing authoritative map%'
             THEN 'invalid_payload'
           WHEN COALESCE(ingest_error, raw_buffer_error, '') ILIKE '%foreign key%'
             OR COALESCE(ingest_error, raw_buffer_error, '') ILIKE '%_fkey%'
             THEN 'local_ingest_failed'
           WHEN COALESCE(reason, '') ILIKE '%massive drop detected%'
             OR COALESCE(reason, '') ILIKE '%partial discovery unresolved%'
             OR COALESCE(reason, '') ILIKE 'api_no_data:%'
             THEN 'api_no_data'
           WHEN status = 'unrecoverable' THEN 'unrecoverable'
           WHEN COALESCE(reason, '') ILIKE '%no authoritative payload%' THEN 'broken_recovery_pending'
           WHEN COALESCE(src.observed_players, 0) = 0 THEN 'no_history_anchor'
           WHEN COALESCE(src.observed_players, 0) BETWEEN 1 AND 9 THEN 'partial_history_anchor'
           ELSE 'pending_recovery'
         END AS drop_category
       FROM source src
     ),
     upserted AS (
       INSERT INTO dropped_matches (
         match_id, date, hour, queue_id, status, drop_category, reason, attempts,
         observed_players, ingest_status, ingest_error, raw_buffer_status, raw_buffer_error,
         first_seen_at, last_attempt_at, next_retry_at, staged_at, completed_at, resolved_at, updated_at
       )
       SELECT
         match_id, date, hour, queue_id, status, drop_category, reason, attempts,
         COALESCE(observed_players, 0), ingest_status, ingest_error, raw_buffer_status, raw_buffer_error,
         first_seen_at, last_attempt_at, next_retry_at, staged_at, completed_at,
         CASE WHEN status = 'complete' OR ingest_status IN ('complete', 'limited') THEN COALESCE(completed_at, now()) ELSE NULL END,
         now()
       FROM classified
       ON CONFLICT (match_id) DO UPDATE SET
         date = EXCLUDED.date,
         hour = EXCLUDED.hour,
         queue_id = EXCLUDED.queue_id,
         status = EXCLUDED.status,
         drop_category = EXCLUDED.drop_category,
         reason = EXCLUDED.reason,
         attempts = EXCLUDED.attempts,
         observed_players = EXCLUDED.observed_players,
         ingest_status = EXCLUDED.ingest_status,
         ingest_error = EXCLUDED.ingest_error,
         raw_buffer_status = EXCLUDED.raw_buffer_status,
         raw_buffer_error = EXCLUDED.raw_buffer_error,
         first_seen_at = EXCLUDED.first_seen_at,
         last_attempt_at = EXCLUDED.last_attempt_at,
         next_retry_at = EXCLUDED.next_retry_at,
         staged_at = EXCLUDED.staged_at,
         completed_at = EXCLUDED.completed_at,
         resolved_at = CASE
           WHEN EXCLUDED.status = 'complete' OR EXCLUDED.ingest_status IN ('complete', 'limited')
             THEN COALESCE(dropped_matches.resolved_at, EXCLUDED.resolved_at, now())
           ELSE NULL
         END,
         updated_at = now()
       RETURNING match_id::text
     )
     SELECT match_id FROM upserted`,
    [date, queueId],
  );

  return result.length;
}

function statusClause(status: string | null | undefined): string {
  switch (status) {
    case 'dropped':
      return `AND status <> 'complete' AND drop_category = 'api_no_data'`;
    case 'all':
      return '';
    case 'complete':
    case 'resolved':
      return `AND status = 'complete'`;
    case 'pending':
      return `AND status = 'pending'`;
    case 'staged':
      return `AND status = 'staged'`;
    case 'unrecoverable':
      return `AND status = 'unrecoverable'`;
    case 'open':
    default:
      return `AND status <> 'complete'`;
  }
}

export async function listDroppedMatches(filters: DroppedMatchFilters): Promise<any[]> {
  await ensureDroppedMatchesTable();
  const params: any[] = [filters.date, filters.queueId];
  let sql = `
    SELECT *
    FROM dropped_matches
    WHERE date = $1::date
      AND queue_id = $2
      ${statusClause(filters.status)}
  `;
  if (filters.hour !== null && filters.hour !== undefined) {
    params.push(filters.hour);
    sql += ` AND hour = $${params.length}`;
  }
  if (filters.category) {
    params.push(filters.category);
    sql += ` AND drop_category = $${params.length}`;
  }
  params.push(filters.limit || 500, filters.offset || 0);
  sql += ` ORDER BY hour ASC, status ASC, attempts DESC, match_id ASC LIMIT $${params.length - 1} OFFSET $${params.length}`;
  return query(sql, params);
}

export async function summarizeDroppedMatches(date: string, queueId = 486): Promise<any[]> {
  await ensureDroppedMatchesTable();
  return query(
    `SELECT
       hour,
       COUNT(*)::int AS tracked,
       COUNT(*) FILTER (WHERE status <> 'complete')::int AS open,
       COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
       COUNT(*) FILTER (WHERE status = 'staged')::int AS staged,
       COUNT(*) FILTER (WHERE status = 'complete')::int AS resolved,
       COUNT(*) FILTER (WHERE drop_category = 'api_no_data' AND status <> 'complete')::int AS dropped,
       COUNT(*) FILTER (WHERE drop_category = 'broken_recovery_pending' AND status <> 'complete')::int AS broken_recovery_pending,
       COUNT(*) FILTER (WHERE drop_category = 'no_authoritative_payload' AND status <> 'complete')::int AS no_authoritative_payload,
       COUNT(*) FILTER (WHERE drop_category = 'no_history_anchor' AND status <> 'complete')::int AS no_history_anchor,
       COUNT(*) FILTER (WHERE drop_category = 'partial_history_anchor' AND status <> 'complete')::int AS partial_history_anchor,
       COUNT(*) FILTER (WHERE drop_category = 'local_ingest_failed' AND status <> 'complete')::int AS local_ingest_failed,
       COUNT(*) FILTER (WHERE drop_category = 'invalid_payload' AND status <> 'complete')::int AS invalid_payload,
       MIN(next_retry_at) FILTER (WHERE status = 'pending') AS next_retry_at
     FROM dropped_matches
     WHERE date = $1::date
       AND queue_id = $2
     GROUP BY hour
     ORDER BY hour ASC`,
    [date, queueId],
  );
}
