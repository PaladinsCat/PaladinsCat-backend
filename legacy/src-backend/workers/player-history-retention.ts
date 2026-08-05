import { one, query } from '../config/db';
import { runExclusive } from './worker-lock';

const HISTORY_RETENTION_BATCH_SIZE = positiveIntFromEnv('PLAYER_HISTORY_RETENTION_BATCH_SIZE', 5000);
const HISTORY_CACHE_EXPIRED_GRACE_HOURS = positiveIntFromEnv('PLAYER_HISTORY_CACHE_EXPIRED_GRACE_HOURS', 24);
const HISTORY_ENTRY_EXPIRED_GRACE_HOURS = positiveIntFromEnv('PLAYER_HISTORY_ENTRY_EXPIRED_GRACE_HOURS', 24);
const HISTORY_ENTRY_AUTHORITY_GRACE_HOURS = positiveIntFromEnv('PLAYER_HISTORY_ENTRY_AUTHORITY_GRACE_HOURS', 24);

let historyRetentionTablesReady = false;

function positiveIntFromEnv(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

export type PlayerHistoryRetentionResult = {
  cacheExpiredDeleted: number;
  entryExpiredDeleted: number;
  entryAuthoritativeDeleted: number;
  totalDeleted: number;
};

async function ensurePlayerHistoryRetentionAuditTable(): Promise<void> {
  if (historyRetentionTablesReady) return;

  // The player history tables are intentionally cache/observation tables, not
  // permanent match facts:
  //
  // - player_match_history_cache keeps the full raw 50-match Hi-Rez response
  //   by player so recovery does not spend getmatchhistory repeatedly while a
  //   player's rolling history window is fresh.
  // - player_match_history_entries keeps one-player observations for DB-first
  //   recovery and player match-history display. These rows must not become a
  //   second ingest queue, and once match_players has authoritative direct or
  //   recovered data, the observation is no longer the source of truth.
  //
  // Retention deletes raw-ish payloads in capped batches and records compact
  // counts/ranges here. That keeps PostgreSQL bounded without erasing the
  // operator evidence needed to diagnose future API burn or recovery issues.
  await one(`
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
    )`);
  await one(`
    CREATE INDEX IF NOT EXISTS idx_player_history_retention_audit_created
    ON player_history_retention_audit (created_at DESC)`);

  historyRetentionTablesReady = true;
}

async function deleteExpiredHistoryCacheRows(reason: string): Promise<number> {
  const rows = await query<{ deleted: number }>(
    `WITH doomed AS (
       SELECT player_id
       FROM player_match_history_cache
       WHERE expires_at < now() - ($1::int * interval '1 hour')
       ORDER BY expires_at ASC, player_id ASC
       LIMIT $2
     ),
     deleted AS (
       DELETE FROM player_match_history_cache c
       USING doomed d
       WHERE c.player_id = d.player_id
       RETURNING c.fetched_at, c.expires_at
     ),
     inserted AS (
       INSERT INTO player_history_retention_audit (
         reason,
         table_name,
         delete_class,
         deleted_count,
         retention_seconds,
         oldest_observed_at,
         newest_observed_at,
         oldest_expires_at,
         newest_expires_at
       )
       SELECT
         $3,
         'player_match_history_cache',
         'expired_cache',
         count(*)::int,
         $1::int * 3600,
         min(fetched_at),
         max(fetched_at),
         min(expires_at),
         max(expires_at)
       FROM deleted
       HAVING count(*) > 0
       RETURNING deleted_count
     )
     SELECT COALESCE(sum(deleted_count), 0)::int AS deleted
     FROM inserted`,
    [HISTORY_CACHE_EXPIRED_GRACE_HOURS, HISTORY_RETENTION_BATCH_SIZE, reason],
  );

  return Number(rows[0]?.deleted || 0);
}

async function deleteExpiredHistoryEntryRows(reason: string): Promise<number> {
  const rows = await query<{ deleted: number }>(
    `WITH doomed AS (
       SELECT match_id, player_id
       FROM player_match_history_entries
       WHERE expires_at IS NOT NULL
         AND expires_at < now() - ($1::int * interval '1 hour')
       ORDER BY expires_at ASC, match_id ASC, player_id ASC
       LIMIT $2
     ),
     deleted AS (
       DELETE FROM player_match_history_entries e
       USING doomed d
       WHERE e.match_id = d.match_id
         AND e.player_id = d.player_id
       RETURNING e.observed_at, e.expires_at
     ),
     inserted AS (
       INSERT INTO player_history_retention_audit (
         reason,
         table_name,
         delete_class,
         deleted_count,
         retention_seconds,
         oldest_observed_at,
         newest_observed_at,
         oldest_expires_at,
         newest_expires_at
       )
       SELECT
         $3,
         'player_match_history_entries',
         'expired_entry',
         count(*)::int,
         $1::int * 3600,
         min(observed_at),
         max(observed_at),
         min(expires_at),
         max(expires_at)
       FROM deleted
       HAVING count(*) > 0
       RETURNING deleted_count
     )
     SELECT COALESCE(sum(deleted_count), 0)::int AS deleted
     FROM inserted`,
    [HISTORY_ENTRY_EXPIRED_GRACE_HOURS, HISTORY_RETENTION_BATCH_SIZE, reason],
  );

  return Number(rows[0]?.deleted || 0);
}

async function deleteAuthoritativeCoveredHistoryEntryRows(reason: string): Promise<number> {
  const rows = await query<{ deleted: number }>(
    `WITH doomed AS (
       SELECT e.match_id, e.player_id
       FROM player_match_history_entries e
       WHERE e.observed_at < now() - ($1::int * interval '1 hour')
         AND EXISTS (
           SELECT 1
           FROM match_players mp
           WHERE mp.match_id = e.match_id
             AND mp.player_id = e.player_id
             AND mp.source IN ('direct', 'recovered')
         )
       ORDER BY e.observed_at ASC, e.match_id ASC, e.player_id ASC
       LIMIT $2
     ),
     deleted AS (
       DELETE FROM player_match_history_entries e
       USING doomed d
       WHERE e.match_id = d.match_id
         AND e.player_id = d.player_id
       RETURNING e.observed_at, e.expires_at
     ),
     inserted AS (
       INSERT INTO player_history_retention_audit (
         reason,
         table_name,
         delete_class,
         deleted_count,
         retention_seconds,
         oldest_observed_at,
         newest_observed_at,
         oldest_expires_at,
         newest_expires_at
       )
       SELECT
         $3,
         'player_match_history_entries',
         'covered_by_authoritative_match_players',
         count(*)::int,
         $1::int * 3600,
         min(observed_at),
         max(observed_at),
         min(expires_at),
         max(expires_at)
       FROM deleted
       HAVING count(*) > 0
       RETURNING deleted_count
     )
     SELECT COALESCE(sum(deleted_count), 0)::int AS deleted
     FROM inserted`,
    [HISTORY_ENTRY_AUTHORITY_GRACE_HOURS, HISTORY_RETENTION_BATCH_SIZE, reason],
  );

  return Number(rows[0]?.deleted || 0);
}

/**
 * Periodic retention for player history cache/observation tables.
 *
 * This prevents a subtle "cache turns into archive" failure mode:
 * read paths already ignore expired rows, but PostgreSQL would keep storing
 * every historical player-match observation forever unless a delete path exists.
 * The cleanup is deliberately DB-first and low-risk:
 *
 * 1. Expired full getmatchhistory cache rows are deleted only after a grace
 *    window, so recovery can still be debugged shortly after TTL expiry.
 * 2. Expired one-player observations are deleted after a separate grace window.
 * 3. Observations already covered by authoritative match_players direct/recovered
 *    facts are pruned after their debug grace window, because match_players is
 *    the canonical source once a match is ingested.
 *
 * Active/recent observations stay available for recovery and player-history UI,
 * and every batch writes compact audit rows before removing raw payloads.
 */
export async function cleanupPlayerHistoryRetention(reason = 'manual'): Promise<PlayerHistoryRetentionResult> {
  await ensurePlayerHistoryRetentionAuditTable();

  const result = await runExclusive('player-history:retention', async () => {
    const cacheExpiredDeleted = await deleteExpiredHistoryCacheRows(reason);
    const entryExpiredDeleted = await deleteExpiredHistoryEntryRows(reason);
    const entryAuthoritativeDeleted = await deleteAuthoritativeCoveredHistoryEntryRows(reason);
    const totalDeleted = cacheExpiredDeleted + entryExpiredDeleted + entryAuthoritativeDeleted;

    if (totalDeleted > 0) {
      console.log(
        `[player-history-retention] ${reason}: deleted ${totalDeleted} old history rows ` +
        `(cache_expired=${cacheExpiredDeleted}, entry_expired=${entryExpiredDeleted}, ` +
        `entry_authoritative=${entryAuthoritativeDeleted})`,
      );
    }

    return { cacheExpiredDeleted, entryExpiredDeleted, entryAuthoritativeDeleted, totalDeleted };
  });

  return result ?? {
    cacheExpiredDeleted: 0,
    entryExpiredDeleted: 0,
    entryAuthoritativeDeleted: 0,
    totalDeleted: 0,
  };
}
