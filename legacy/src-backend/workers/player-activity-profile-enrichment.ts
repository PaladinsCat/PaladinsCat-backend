import cron from 'node-cron';
import type { PoolClient } from 'pg';
import { query, transaction } from '../config/db';
import { getPlayerBatch } from '../services/hirez';
import { normalizePlayerProfile } from '../services/normalizer';
import { upsertPlayerProfile } from '../services/player-profile-store';
import { recordRawHirezResponse } from '../services/raw-hirez-response-audit';
import { getApiHeadroomSnapshot } from './api-headroom';
import {
  ACTIVITY_PROFILE_BATCH_SIZE,
  ACTIVITY_PROFILE_TTL_HOURS,
  chunkActivityProfileIds,
  requestedIdsSatisfiedByProfiles,
} from './player-activity-profile-policy';
import { PUBLIC_PLAYER_EVIDENCE_CTES_SQL } from './player-presence-evidence';
import { runExclusive } from './worker-lock';

const CRON = process.env.PLAYER_ACTIVITY_PROFILE_CRON || '50 * * * *';
const MAX_CALLS_PER_RUN = Math.min(
  500,
  Math.max(1, Number(process.env.PLAYER_ACTIVITY_PROFILE_MAX_CALLS_PER_RUN || 100)),
);
const LEASE_MINUTES = 30;
const FAILED_RETRY_MINUTES = 60;

type RefreshRow = { player_id: string | number };

export type PlayerActivityProfileRunResult = {
  claimed: number;
  calls: number;
  refreshed: number;
  unavailable: number;
  skippedRecent: number;
  failed: number;
};

const emptyResult = (): PlayerActivityProfileRunResult => ({
  claimed: 0,
  calls: 0,
  refreshed: 0,
  unavailable: 0,
  skippedRecent: 0,
  failed: 0,
});

/**
 * Keep the worker-facing projections complete even after a fresh deployment
 * or a temporarily skipped ingest-side upsert. Public counts do not read these
 * tables, but profile enrichment uses them as its bounded active-player work
 * queue.
 */
export async function reconcilePlayerPresenceCache(): Promise<void> {
  await query(
    `WITH ${PUBLIC_PLAYER_EVIDENCE_CTES_SQL},
     deduplicated_participation AS MATERIALIZED (
       SELECT DISTINCT
         player_id, match_id, queue_id, stats_scope, observed_at
       FROM participation
     ),
     global_rows AS MATERIALIZED (
       SELECT DISTINCT ON (player_id)
         player_id,
         MIN(observed_at) OVER (PARTITION BY player_id) AS first_observed_at,
         observed_at AS last_observed_at,
         match_id AS last_match_id,
         queue_id AS last_queue_id,
         stats_scope AS last_stats_scope
       FROM deduplicated_participation
       ORDER BY player_id, observed_at DESC, match_id DESC, queue_id DESC
     ),
     queue_rows AS MATERIALIZED (
       SELECT DISTINCT ON (player_id, queue_id)
         player_id,
         queue_id,
         stats_scope,
         MIN(observed_at) OVER (PARTITION BY player_id, queue_id) AS first_observed_at,
         observed_at AS last_observed_at,
         match_id AS last_match_id
       FROM deduplicated_participation
       ORDER BY player_id, queue_id, observed_at DESC, match_id DESC
     ),
     global_upsert AS (
       INSERT INTO player_presence_24h (
         player_id, first_observed_at, last_observed_at, last_match_id,
         last_queue_id, last_stats_scope
       )
       SELECT
         player_id, first_observed_at, last_observed_at, last_match_id,
         last_queue_id, last_stats_scope
       FROM global_rows
       ON CONFLICT (player_id) DO UPDATE SET
         first_observed_at = LEAST(
           player_presence_24h.first_observed_at,
           EXCLUDED.first_observed_at
         ),
         last_observed_at = GREATEST(
           player_presence_24h.last_observed_at,
           EXCLUDED.last_observed_at
         ),
         last_match_id = CASE
           WHEN EXCLUDED.last_observed_at >= player_presence_24h.last_observed_at
             THEN EXCLUDED.last_match_id
           ELSE player_presence_24h.last_match_id
         END,
         last_queue_id = CASE
           WHEN EXCLUDED.last_observed_at >= player_presence_24h.last_observed_at
             THEN EXCLUDED.last_queue_id
           ELSE player_presence_24h.last_queue_id
         END,
         last_stats_scope = CASE
           WHEN EXCLUDED.last_observed_at >= player_presence_24h.last_observed_at
             THEN EXCLUDED.last_stats_scope
           ELSE player_presence_24h.last_stats_scope
         END,
         updated_at = now()
       RETURNING player_id
     )
     INSERT INTO player_queue_presence_24h (
       player_id, queue_id, stats_scope, first_observed_at,
       last_observed_at, last_match_id
     )
     SELECT
       player_id, queue_id, stats_scope, first_observed_at,
       last_observed_at, last_match_id
     FROM queue_rows
     ON CONFLICT (player_id, queue_id) DO UPDATE SET
       first_observed_at = LEAST(
         player_queue_presence_24h.first_observed_at,
         EXCLUDED.first_observed_at
       ),
       last_observed_at = GREATEST(
         player_queue_presence_24h.last_observed_at,
         EXCLUDED.last_observed_at
       ),
       last_match_id = CASE
         WHEN EXCLUDED.last_observed_at >= player_queue_presence_24h.last_observed_at
           THEN EXCLUDED.last_match_id
         ELSE player_queue_presence_24h.last_match_id
       END,
       stats_scope = EXCLUDED.stats_scope,
       updated_at = now()`,
    [null],
  );
}

function errorText(error: unknown): string {
  return error instanceof Error ? error.message.slice(0, 1000) : String(error).slice(0, 1000);
}

async function seedRefreshLedger(): Promise<void> {
  await query(
    `INSERT INTO player_activity_profile_refresh (player_id)
     SELECT player_id
     FROM player_presence_24h
     WHERE last_observed_at >= now() - interval '24 hours'
     ON CONFLICT (player_id) DO NOTHING`,
  );
}

async function claimDuePlayers(limit: number): Promise<number[]> {
  if (limit <= 0) return [];
  return transaction(async (client: PoolClient) => {
    const result = await client.query<RefreshRow>(
      `WITH due AS (
         SELECT refresh.player_id
         FROM player_activity_profile_refresh refresh
         JOIN player_presence_24h presence
           ON presence.player_id = refresh.player_id
          AND presence.last_observed_at >= now() - interval '24 hours'
         LEFT JOIN LATERAL (
           SELECT candidate.hirez_profile_refreshed_at
           FROM (
             SELECT profile.hirez_profile_refreshed_at, 0 AS identity_priority
             FROM players profile
             WHERE profile.id = refresh.player_id
             UNION ALL
             SELECT profile.hirez_profile_refreshed_at, 1 AS identity_priority
             FROM players profile
             WHERE profile.active_player_id = refresh.player_id
               AND profile.active_player_id > 0
               AND profile.id <> refresh.player_id
           ) candidate
           ORDER BY
             candidate.identity_priority,
             candidate.hirez_profile_refreshed_at DESC NULLS LAST
           LIMIT 1
         ) resolved_profile ON TRUE
         WHERE (
             resolved_profile.hirez_profile_refreshed_at IS NULL
             OR resolved_profile.hirez_profile_refreshed_at
                < now() - ($2::int * interval '1 hour')
           )
           AND (
             refresh.status = 'pending'
             OR (refresh.status = 'fetching' AND refresh.lease_until <= now())
             OR (refresh.status = 'failed' AND refresh.next_retry_at <= now())
           )
           AND (refresh.lease_until IS NULL OR refresh.lease_until <= now())
         ORDER BY presence.last_observed_at DESC, refresh.player_id
         LIMIT $1
         FOR UPDATE OF refresh SKIP LOCKED
       )
       UPDATE player_activity_profile_refresh refresh SET
         status = 'fetching',
         lease_until = now() + ($3::int * interval '1 minute'),
         error_message = NULL,
         updated_at = now()
       FROM due
       WHERE refresh.player_id = due.player_id
       RETURNING refresh.player_id`,
      [limit, ACTIVITY_PROFILE_TTL_HOURS, LEASE_MINUTES],
    );
    return result.rows.map(row => Number(row.player_id)).filter(id => Number.isSafeInteger(id) && id > 0);
  });
}

async function filterStillStale(playerIds: number[]): Promise<{
  stale: number[];
  recent: number[];
}> {
  if (playerIds.length === 0) return { stale: [], recent: [] };
  const rows = await query<{ player_id: string | number; is_recent: boolean }>(
    `SELECT requested.player_id,
            COALESCE(resolved_profile.hirez_profile_refreshed_at
              >= now() - ($2::int * interval '1 hour'), FALSE) AS is_recent
     FROM unnest($1::bigint[]) AS requested(player_id)
     LEFT JOIN LATERAL (
       SELECT candidate.hirez_profile_refreshed_at
       FROM (
         SELECT profile.hirez_profile_refreshed_at, 0 AS identity_priority
         FROM players profile
         WHERE profile.id = requested.player_id
         UNION ALL
         SELECT profile.hirez_profile_refreshed_at, 1 AS identity_priority
         FROM players profile
         WHERE profile.active_player_id = requested.player_id
           AND profile.active_player_id > 0
           AND profile.id <> requested.player_id
       ) candidate
       ORDER BY
         candidate.identity_priority,
         candidate.hirez_profile_refreshed_at DESC NULLS LAST
       LIMIT 1
     ) resolved_profile ON TRUE`,
    [playerIds, ACTIVITY_PROFILE_TTL_HOURS],
  );
  const recent = rows.filter(row => row.is_recent).map(row => Number(row.player_id));
  const recentSet = new Set(recent);
  return { stale: playerIds.filter(id => !recentSet.has(id)), recent };
}

async function markPlayers(
  playerIds: number[],
  status: 'success' | 'unavailable' | 'failed' | 'skipped_recent' | 'pending',
  options: { error?: string | null; attempted?: boolean; successful?: boolean; retry?: 'ttl' | 'failed' | 'now' | 'never' } = {},
): Promise<void> {
  if (playerIds.length === 0) return;
  await query(
    `UPDATE player_activity_profile_refresh SET
       status = $2,
       attempts = attempts + CASE WHEN $5::boolean THEN 1 ELSE 0 END,
       last_attempt_at = CASE WHEN $5::boolean THEN now() ELSE last_attempt_at END,
       last_success_at = CASE WHEN $6::boolean THEN now() ELSE last_success_at END,
       next_retry_at = CASE $8::text
         WHEN 'ttl' THEN now() + ($3::int * interval '1 hour')
         WHEN 'failed' THEN now() + ($4::int * interval '1 minute')
         WHEN 'never' THEN NULL
         ELSE now()
       END,
       lease_until = NULL,
       error_message = $7,
       updated_at = now()
     WHERE player_id = ANY($1::bigint[])`,
    [
      playerIds,
      status,
      ACTIVITY_PROFILE_TTL_HOURS,
      FAILED_RETRY_MINUTES,
      Boolean(options.attempted),
      Boolean(options.successful),
      options.error ?? null,
      options.retry ?? 'now',
    ],
  );
}

async function releaseUnprocessed(playerIds: number[]): Promise<void> {
  if (playerIds.length === 0) return;
  await markPlayers(playerIds, 'pending', { retry: 'now' });
}

async function cleanupOldState(): Promise<void> {
  await Promise.all([
    query(`DELETE FROM player_queue_presence_24h WHERE last_observed_at < now() - interval '7 days'`),
    query(
      `DELETE FROM player_activity_profile_refresh refresh
       WHERE refresh.updated_at < now() - interval '7 days'
         AND NOT EXISTS (
           SELECT 1 FROM player_presence_24h presence
           WHERE presence.player_id = refresh.player_id
             AND presence.last_observed_at >= now() - interval '24 hours'
         )`,
    ),
  ]);
}

export async function runPlayerActivityProfileEnrichment(
  reason = 'manual',
): Promise<PlayerActivityProfileRunResult> {
  const result = emptyResult();
  await reconcilePlayerPresenceCache();
  await seedRefreshLedger();

  const headroom = await getApiHeadroomSnapshot();
  if (!headroom.hasUsableKeys) {
    console.warn('[player-activity-profile] Paused: no usable Hi-Rez API headroom');
    return result;
  }
  const allowedCalls = headroom.totalKeys === 0
    ? MAX_CALLS_PER_RUN
    : Math.min(MAX_CALLS_PER_RUN, headroom.totalUsableBeforeReserve);
  if (allowedCalls <= 0) return result;

  const claimed = await claimDuePlayers(allowedCalls * ACTIVITY_PROFILE_BATCH_SIZE);
  result.claimed = claimed.length;
  const batches = chunkActivityProfileIds(claimed);

  for (let index = 0; index < batches.length; index += 1) {
    const claimedBatch = batches[index];
    const { stale, recent } = await filterStillStale(claimedBatch);
    if (recent.length > 0) {
      await markPlayers(recent, 'skipped_recent', { successful: true, retry: 'never' });
      result.skippedRecent += recent.length;
    }
    if (stale.length === 0) continue;

    try {
      const rawProfiles = await getPlayerBatch(stale);
      result.calls += 1;
      await recordRawHirezResponse({
        endpoint: 'getplayerbatch',
        operation: 'getPlayerBatch',
        entityType: 'player_activity_profile_enrichment',
        params: { playerIds: stale, reason },
        rawResponse: rawProfiles,
        source: 'player-activity-profile-enrichment',
      });

      const persistedRaw: any[] = [];
      for (const raw of Array.isArray(rawProfiles) ? rawProfiles : []) {
        if (String(raw?.ret_msg || '').trim()) continue;
        try {
          const profile = normalizePlayerProfile(raw);
          if (profile.player_id <= 0) continue;
          await upsertPlayerProfile(profile);
          persistedRaw.push(raw);
        } catch (error) {
          console.error(`[player-activity-profile] Profile persistence failed: ${errorText(error)}`);
        }
      }

      const satisfied = requestedIdsSatisfiedByProfiles(stale, persistedRaw);
      const refreshed = stale.filter(id => satisfied.has(id));
      const unavailable = stale.filter(id => !satisfied.has(id));
      await markPlayers(refreshed, 'success', {
        attempted: true,
        successful: true,
        retry: 'never',
      });
      await markPlayers(unavailable, 'unavailable', {
        attempted: true,
        retry: 'never',
        error: 'Hi-Rez getplayerbatch returned no usable profile for this player ID.',
      });
      result.refreshed += refreshed.length;
      result.unavailable += unavailable.length;
    } catch (error) {
      const message = errorText(error);
      await markPlayers(stale, 'failed', { attempted: true, retry: 'failed', error: message });
      result.failed += stale.length;
      const remaining = batches.slice(index + 1).flat();
      await releaseUnprocessed(remaining);
      console.error(
        `[player-activity-profile] ${reason}: batch failed; released ${remaining.length} unprocessed ID(s): ${message}`,
      );
      break;
    }
  }

  await cleanupOldState();
  console.log(
    `[player-activity-profile] ${reason}: claimed=${result.claimed}, calls=${result.calls}, ` +
    `refreshed=${result.refreshed}, unavailable=${result.unavailable}, ` +
    `recent=${result.skippedRecent}, failed=${result.failed}`,
  );
  return result;
}

async function runScheduled(reason: string): Promise<void> {
  await runExclusive(
    'player-activity-profile-enrichment',
    () => runPlayerActivityProfileEnrichment(reason),
  );
}

export const jobs = {
  enrichment: cron.createTask(CRON, () => runScheduled('cron')),
};

export function enableAll(): void {
  jobs.enrichment.start();
  console.log(
    `[player-activity-profile] Enabled (${CRON}, batch=${ACTIVITY_PROFILE_BATCH_SIZE}, ` +
    `maxCalls=${MAX_CALLS_PER_RUN}, ttl=${ACTIVITY_PROFILE_TTL_HOURS}h)`,
  );
}

export function disableAll(): void {
  jobs.enrichment.stop();
}
