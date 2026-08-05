import { query, one } from '../config/db';
import { cleanupFetchedPlayersCache, getMatchIdsByQueueDetails, getMatchDetailsBatch, dumpRawPayloads } from '../services/hirez';
import { filterAlreadyHandledMatchIds } from './ingest-guards';
import {
  claimHourlyIngestHour,
  markHourlyIngestComplete,
  markHourlyIngestEmpty,
  markHourlyIngestFailed,
  recordHourlyIngestQuotaWait,
  markHourlyIngestStaged,
} from './hourly-ingest-state';
import {
  getDueHourlyIngestMatchDebtIds,
  markHourlyIngestMatchDebtPending,
  markHourlyIngestMatchDebtStagedOrComplete,
  markHourlyIngestMatchDebtUnrecoverable,
  recordHourlyIngestDiscoveredMatches,
} from './hourly-ingest-match-debt';
import { getApiHeadroomSnapshot } from './api-headroom';
import { extractMatchBanFields } from '../utils/match-bans';
import {
  MATCH_DETAIL_SERVICE_OUTAGE_KEY,
  classifyHirezServiceOutageMessage,
  markHirezServiceRecovered,
  recordHirezServiceOutage,
} from './hirez-service-outage';
import { recordMatchCountDiscoveryResult } from './match-count-discovery';
import { MATCH_COUNT_QUEUE_DEFINITIONS } from './match-count-discovery-policy';
import { fetchCompletedMatchesContinuously } from './completed-match-batching';

/**
 * Auto Ingester - Discovery Worker
 *
 * Runs at HH:30 UTC every hour. Fetches match IDs from (HH-1):00 UTC window.
 * 30-minute headroom ensures matches ending after HH:00 are included.
 *
 * Queue: 486 (ranked only)
 *
 * Flow:
 *   1. Fetch match IDs via getmatchidsbyqueue
 *   2. Fetch match details in batches via getmatchdetailsbatchsorted
 *   3. Build raw payloads and dump to raw_ingest_buffer
 *   4. buffer-processor processes them → matches, match_players, hourly_match_counts, AFK, baselines, MV refresh
 *
 * No intermediate staging table. raw_ingest_buffer is the single staging point.
 */

const RANKED_QUEUE = 486;
const PRESENCE_QUEUES = MATCH_COUNT_QUEUE_DEFINITIONS.filter(queue => !queue.ranked);

export type CompletedDiscoveryWindow = {
  statDate: string;
  apiDate: string;
  hour: number;
};

function schedulerDiscoveryNow(): Date {
  if (process.env.PALADINSCAT_SCHEDULER_CAPTURE_ENABLE !== 'true') return new Date();
  const value = process.env.PALADINSCAT_SCHEDULER_CAPTURE_NOW;
  if (!value) throw new Error('scheduler capture clock is required');
  const now = new Date(value);
  if (!Number.isFinite(now.getTime())) throw new Error('scheduler capture clock is invalid');
  return now;
}

export function resolveCompletedDiscoveryWindow(now = schedulerDiscoveryNow()): CompletedDiscoveryWindow {
  const hoursBehind = now.getUTCMinutes() < 30 ? 2 : 1;
  const completedWindow = new Date(now.getTime() - hoursBehind * 3600000);
  const statDate = completedWindow.toISOString().slice(0, 10);
  return {
    statDate,
    apiDate: statDate.replace(/-/g, ''),
    hour: completedWindow.getUTCHours(),
  };
}

export async function discoverPresenceQueue(
  queueId: number,
  apiDate?: string,
  hour?: number,
  source = 'auto-ingester-presence',
): Promise<number> {
  const resolved = apiDate && hour !== undefined
    ? {
        statDate: apiDate.replace(/^(\d{4})(\d{2})(\d{2})$/, '$1-$2-$3'),
        apiDate: apiDate.replace(/-/g, ''),
        hour,
      }
    : resolveCompletedDiscoveryWindow();
  const definition = PRESENCE_QUEUES.find(queue => queue.queueId === queueId);
  if (!definition) throw new Error(`Queue ${queueId} is not configured for presence discovery`);

  const claimed = await claimHourlyIngestHour(
    resolved.statDate,
    resolved.hour,
    queueId,
    source,
  );
  if (!claimed) {
    console.log(
      `[PRESENCE-DISCOVERY] Skipping ${definition.name} (${queueId}) | ` +
      `${resolved.statDate}T${String(resolved.hour).padStart(2, '0')}Z; window is already final or leased`,
    );
    return 0;
  }

  try {
    const observations = await getMatchIdsByQueueDetails(
      queueId,
      resolved.apiDate,
      resolved.hour,
      'presence_discovery',
    );
    const stored = await recordMatchCountDiscoveryResult(
      resolved.statDate,
      resolved.hour,
      queueId,
      observations,
      source,
    );
    // A successful empty response is a final observation. Presence discovery
    // never retries a completed window in hopes that missing vendor data will
    // appear later; only an actually missed/failed cron window is backfilled.
    await markHourlyIngestComplete(resolved.statDate, resolved.hour, queueId, stored);
    console.log(
      `[PRESENCE-DISCOVERY] ${definition.name} (${queueId}) | ` +
      `${resolved.statDate}T${String(resolved.hour).padStart(2, '0')}Z | ${stored} match ID(s)`,
    );
    return stored;
  } catch (error) {
    await markHourlyIngestFailed(resolved.statDate, resolved.hour, queueId, error);
    throw error;
  }
}

export async function discoverPresenceQueues(
  apiDate?: string,
  hour?: number,
  source = 'auto-ingester-presence',
): Promise<number> {
  const headroom = await getApiHeadroomSnapshot();
  if (!headroom.hasUsableKeys) {
    console.warn('[PRESENCE-DISCOVERY] No usable Hi-Rez key headroom; leaving queue-hours for cron backfill');
    return 0;
  }

  let total = 0;
  for (const queue of PRESENCE_QUEUES) {
    try {
      total += await discoverPresenceQueue(queue.queueId, apiDate, hour, source);
    } catch (error) {
      console.error(
        `[PRESENCE-DISCOVERY] ${queue.name} (${queue.queueId}) failed; ` +
        `continuing remaining queues: ${error instanceof Error ? error.message : error}`,
      );
    }
  }
  return total;
}

const BUFFER_DUMP_CHUNK_SIZE = Math.max(
  1,
  Number(process.env.DISCOVERY_BUFFER_DUMP_CHUNK_SIZE || 1),
);
const BATCH_ONLY_NO_AUTHORITY_RETRY_MINUTES = Math.max(
  10,
  Number(
    process.env.HOURLY_INGEST_BATCH_ONLY_NO_AUTHORITY_RETRY_MINUTES
    || process.env.HOURLY_INGEST_BATCH_ONLY_PROFILE_ONLY_RETRY_MINUTES
    || 10,
  ),
);

type StageMatchesCallback = (matches: any[], reason: string) => Promise<void>;

type DiscoverOptions = {
  /**
   * Recover only known due match debt for this hour.
   *
   * Gap-checker uses this for aggressive recovery passes. The normal hourly
   * discovery path still calls getmatchidsbyqueue to discover fresh IDs; this
   * mode intentionally skips that endpoint because the IDs are already stored
   * in hourly_ingest_match_debt.
   */
  debtOnly?: boolean;
  /**
   * Manual/operator override for known debt.
   *
   * Cron and startup catch-up should leave this false so next_retry_at remains
   * the anti-loop brake. The HTTP /matches/discover route may set it only when
   * an operator explicitly asks to recover already-discovered IDs now; this
   * includes rows that are still inside their retry cooldown without calling
   * getmatchidsbyqueue again.
   */
  forceDebt?: boolean;
};

function isVendorDetailOutageError(error: unknown): boolean {
  return classifyHirezServiceOutageMessage(error)?.serviceKey === MATCH_DETAIL_SERVICE_OUTAGE_KEY;
}

function chunkArray<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let index = 0; index < items.length; index += size) {
    chunks.push(items.slice(index, index + size));
  }
  return chunks;
}

function buildRawMatchPayloads(matches: any[]): any[] {
  // Keep this transformation beside discovery rather than inside the relay:
  // discovery owns the ranked-hour debt ledger and must know exactly which
  // match IDs became durable queue work. The relay remains the DB handoff, but
  // it should not decide hourly_ingest_match_debt state on behalf of workers.
  return matches.map((match: any) => ({
    endpoint: 'getmatchdetailsbatch',
    entity_type: 'match',
    entity_id: match.match_id,
    raw_data: match.players.map((p: any, index: number) => {
      const scoreObservation = match.direct_score_observations?.[index];
      return ({
      ...p,
      Match: match.match_id,
      Entry_Datetime: match.entry_datetime,
      Map_Game: match.map,
      match_queue_id: match.queue_id,
      Match_Duration: match.duration_seconds,
      Minutes: match.minutes,
      Region: match.region,
      Team1Score: scoreObservation?.team1 ?? match.team1_score,
      Team2Score: scoreObservation?.team2 ?? match.team2_score,
      Winning_TaskForce: scoreObservation?.winner ?? match.winning_task_force,
      hasReplay: match.has_replay ? 'y' : 'n',
      recovery_source: match.recovery_source,
      recovery_api_calls: match.recovery_api_calls,
      recovery_attempted: match.recovery_attempted === true,
      recovery_terminal: match.recovery_terminal === true,
      limited: match.limited === true,
      ...extractMatchBanFields(match, match.players),
      });
    }),
    source: 'batch',
  }));
}

async function dumpRawPayloadsInChunks(payloads: any[]): Promise<number> {
  let inserted = 0;
  for (const chunk of chunkArray(payloads, BUFFER_DUMP_CHUNK_SIZE)) {
    inserted += await dumpRawPayloads(chunk);
  }
  return inserted;
}

async function fetchCanonicalMatchDetailsContinuously(
  matchIds: number[],
  recoveryAttemptedIds: Set<number>,
  terminalNoAnchorIds: Set<number>,
  terminalNoAnchorReasons: Map<number, string>,
  batchOnlyNoAuthorityIds: Set<number>,
  stageMatches?: StageMatchesCallback,
): Promise<any[]> {
  const matches: any[] = [];
  try {
    await fetchCompletedMatchesContinuously(
      matchIds.map(matchId => ({ matchId, queueId: RANKED_QUEUE })),
      requests => getMatchDetailsBatch(requests, 'ranked_recovery'),
      {
        onResult: async outcome => {
          if (outcome.status !== 'complete_direct') {
            recoveryAttemptedIds.add(outcome.matchId);
          }
          switch (outcome.status) {
            case 'complete_direct':
            case 'complete_recovered':
              if (outcome.match) {
                matches.push(outcome.match);
                await markHirezServiceRecovered(
                  MATCH_DETAIL_SERVICE_OUTAGE_KEY,
                  'canonical completed-match batch returned authoritative rows',
                );
                await stageMatches?.(
                  [outcome.match],
                  outcome.status === 'complete_direct'
                    ? 'canonical direct match checkpoint'
                    : 'canonical recovered match checkpoint',
                );
              }
              break;
            case 'limited':
              if (outcome.match) {
                matches.push(outcome.match);
                await stageMatches?.(
                  [outcome.match],
                  'canonical limited-match checkpoint',
                );
              }
              batchOnlyNoAuthorityIds.add(outcome.matchId);
              terminalNoAnchorReasons.set(
                outcome.matchId,
                `limited relay result: ${outcome.reason || outcome.match?.recovery_source || 'automatic recovery'} retained authoritative partial facts`,
              );
              break;
            case 'recovery_pending':
              batchOnlyNoAuthorityIds.add(outcome.matchId);
              terminalNoAnchorReasons.set(
                outcome.matchId,
                `no authoritative payload: relay recovery remains pending (${outcome.reason || outcome.match?.recovery_source || 'target history unresolved'})`,
              );
              break;
            case 'roster_only':
            case 'dropped':
              terminalNoAnchorIds.add(outcome.matchId);
              terminalNoAnchorReasons.set(
                outcome.matchId,
                outcome.reason
                  || 'api_no_data: canonical relay lookup returned no durable ranked match facts',
              );
              break;
          }
        },
      },
    );
    return matches;
  } catch (error) {
    if (isVendorDetailOutageError(error)) {
      await recordHirezServiceOutage(
        MATCH_DETAIL_SERVICE_OUTAGE_KEY,
        'Hi-Rez match detail service outage: Server_Regions temp-table failure',
      );
      for (const matchId of matchIds) {
        batchOnlyNoAuthorityIds.add(matchId);
        terminalNoAnchorReasons.set(
          matchId,
          'no authoritative payload: vendor detail service outage (Server_Regions); exact ID retry delayed without history fan-out',
        );
      }
    }
    throw error;
  }
}

/**
 * Discover and ingest matches for the previous hour window
 */
export async function discover(
  queueId?: number,
  apiDate?: string,
  hour?: number,
  options: DiscoverOptions = {},
): Promise<number> {
  const targetQueue = queueId ?? RANKED_QUEUE;

  let statDate: string, targetHour: number;
  let claimedRawMatchCountForFailure: number | null = null;
  const checkpointedIdsForFailure = new Set<number>();

  if (apiDate && hour !== undefined) {
    // Explicit date/hour from API endpoint
    statDate = apiDate.replace(/^\d{4}(\d{2})(\d{2})$/, (m, mm, dd) => `${m.slice(0,4)}-${mm}-${dd}`);
    targetHour = hour;
  } else {
    // Auto mode: fetch the latest hour whose match-completion buffer has
    // elapsed. The cron normally fires at HH:30 and fetches HH-1. Startup or
    // manual runs can happen earlier (for example 05:15 UTC); in that case
    // fetching 04:00-04:59 is too aggressive because a 04:59 match may not be
    // complete. We therefore fall back one extra hour before :30.
    //
    // Examples:
    //   09:30 UTC -> fetch 08:00-08:59 UTC of today
    //   05:15 UTC -> fetch 03:00-03:59 UTC of today
    //   00:15 UTC -> fetch 22:00-22:59 UTC of yesterday
    const completedWindow = resolveCompletedDiscoveryWindow();
    targetHour = completedWindow.hour;
    statDate = completedWindow.statDate;
  }

  const dateStr = apiDate ?? statDate.replace(/-/g, '');
  const source = options.debtOnly
    ? 'gap-checker-debt'
    : apiDate && hour !== undefined
      ? 'gap-checker'
      : 'auto-ingester';

  try {
    try {
      await cleanupFetchedPlayersCache();
    } catch (cacheError) {
      console.warn(
        `[DISCOVERY] Failed to clear relay recovery cache before ${statDate} hour ${targetHour}: ` +
        `${cacheError instanceof Error ? cacheError.message : cacheError}`
      );
    }

    const headroom = await getApiHeadroomSnapshot();
    if (!headroom.hasUsableKeys) {
      // Do this before claimHourlyIngestHour(). If all keys are already at the
      // reserve, claiming the hour would turn a budget wait into a failed ingest
      // attempt and make gap-checker revisit it with noisy relay errors. Leaving
      // the state untouched lets the next cron/startup pass pick it up naturally
      // after the relay's hourly sync revives a key.
      const quotaReason =
        `no usable Hi-Rez key headroom (usableKeys=${headroom.usableKeys}, ` +
        `usableBeforeReserve=${headroom.totalUsableBeforeReserve})`;
      await recordHourlyIngestQuotaWait(
        statDate,
        targetHour,
        targetQueue,
        `${source}-quota-wait`,
        quotaReason,
      );
      console.warn(
        `[DISCOVERY] Skipping Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z; ` +
        `${quotaReason}; durable pending hour recorded`
      );
      return 0;
    }

    const preClaimDebtIds = options.debtOnly
      ? await getDueHourlyIngestMatchDebtIds(
          statDate,
          targetHour,
          targetQueue,
          250,
          options.forceDebt === true,
        )
      : null;
    if (options.debtOnly && (!preClaimDebtIds || preClaimDebtIds.length === 0)) {
      console.log(
        `[DISCOVERY] No ${options.forceDebt ? 'known' : 'due'} match debt for Queue ${targetQueue} | ` +
        `${statDate}T${String(targetHour).padStart(2, '0')}Z; leaving hourly_ingest_state unchanged`
      );
      return 0;
    }

    const claimed = await claimHourlyIngestHour(statDate, targetHour, targetQueue, source, options.debtOnly === true);
    if (!claimed) {
      console.log(`[DISCOVERY] Skipping Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z; hour is already complete, leased, or waiting for retry`);
      return 0;
    }

    // ----------------------------------------------------------------
    // Step 1: Get all match IDs for this hour.
    // This is the first external-call boundary, and it only happens after
    // hourly_ingest_state successfully claims the hour. That state row is the
    // real scheduler lock: it records whether this hour is fetching, staged,
    // empty, failed, or complete. We never treat a zero hourly_match_counts row
    // as "done" because a true empty hour and a temporary Hi-Rez empty response
    // look identical from that analytics table.
    //
    // If Hi-Rez returns zero IDs, markHourlyIngestEmpty() records a retry time
    // and may insert a zero analytics row for dashboards. The gap checker uses
    // hourly_ingest_state.next_retry_at, not total_matches=0, to decide when
    // to recheck.
    // Source: User report 2026-06-02 — "hourly_match_counts zero-row trap:
    //   placeholder inserted before fetch, failures leave permanent zeros"
    // ----------------------------------------------------------------
    const dueDebtIds = preClaimDebtIds
      ?? await getDueHourlyIngestMatchDebtIds(statDate, targetHour, targetQueue);
    const apiObservations = options.debtOnly
      ? []
      : await getMatchIdsByQueueDetails(targetQueue, dateStr, targetHour, 'ranked_discovery');
    const apiMatchIds = apiObservations.map(observation => observation.matchId);
    if (!options.debtOnly) {
      try {
        // Queue 486 shares its one hourly discovery response with global match
        // counting. This storage-only side effect does not fetch details and
        // cannot route casual queues into ranked projections.
        await recordMatchCountDiscoveryResult(
          statDate,
          targetHour,
          targetQueue,
          apiObservations,
          source,
        );
      } catch (countError) {
        // Match-count reporting must never block the perfected ranked recovery
        // path. A later count-discovery catch-up can repair this observation.
        console.warn(
          `[DISCOVERY] Failed to mirror Queue ${targetQueue} IDs into match-count storage: ` +
          `${countError instanceof Error ? countError.message : countError}`,
        );
      }
    }
    const rawMatchIds = [...new Set([...apiMatchIds, ...dueDebtIds])];

    if (options.debtOnly) {
      console.warn(
        `[DISCOVERY] Debt-only recovery pass for Queue ${targetQueue} | ` +
        `${statDate}T${String(targetHour).padStart(2, '0')}Z with ${dueDebtIds.length} due match ID(s); ` +
        `skipping getmatchidsbyqueue`
      );
    }

    if (dueDebtIds.length > 0) {
      console.warn(
        `[DISCOVERY] Rehydrated ${dueDebtIds.length} unresolved match debt ID(s) for ` +
        `Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z before detail fetch`
      );
    }

    if (rawMatchIds.length === 0) {
      if (options.debtOnly) {
        const error = new Error('debt-only pass found no due match IDs after claim');
        await markHourlyIngestFailed(statDate, targetHour, targetQueue, error);
        console.log(
          `[DISCOVERY] No due match debt for Queue ${targetQueue} | ` +
          `${statDate}T${String(targetHour).padStart(2, '0')}Z`
        );
        return 0;
      }
      console.log(`[DISCOVERY] No matches for Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z`);
      await markHourlyIngestEmpty(statDate, targetHour, targetQueue);
      return 0;
    }

    await recordHourlyIngestDiscoveredMatches(
      statDate,
      targetHour,
      targetQueue,
      rawMatchIds,
      dueDebtIds.length > 0 ? 'hourly discovery plus unresolved debt retry' : 'hourly discovery',
    );

    // ----------------------------------------------------------------
    // Step 2: Filter out matches that are already final OR in-flight.
    // Checking only `matches` is not enough during backlogs: a match can be
    // waiting in raw_ingest_buffer while the final tables are still empty.
    // Treating that as "not fetched" caused repeated detail fetches and buffer
    // floods. This guard checks matches, match_players, raw_ingest_buffer, and
    // match_pull_list before any expensive detail call.
    // ----------------------------------------------------------------
    const guard = await filterAlreadyHandledMatchIds(rawMatchIds);
    const matchIds = guard.fetchIds;

    if (guard.skipped.totalUnique > 0) {
      console.log(
        `[DISCOVERY] Skipped ${guard.skipped.totalUnique} already-handled matches ` +
        `(matches=${guard.skipped.matches}, players=${guard.skipped.matchPlayers}, ` +
        `buffer=${guard.skipped.rawBuffer}, pull_list=${guard.skipped.pullList}; ` +
        `fetching ${matchIds.length}) | Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z`
      );
    }

    if (matchIds.length === 0) {
      console.log(`[DISCOVERY] No new matches for Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z`);
      await markHourlyIngestMatchDebtStagedOrComplete(
        rawMatchIds,
        'all discovered IDs already handled before detail fetch',
      );
      if (guard.skipped.rawBuffer > 0 || guard.skipped.pullList > 0) {
        await markHourlyIngestStaged(statDate, targetHour, targetQueue, rawMatchIds.length, 0);
      } else {
        await markHourlyIngestComplete(statDate, targetHour, targetQueue, rawMatchIds.length);
      }
      return 0;
    }

    console.log(`[DISCOVERY] Queue ${targetQueue} | ${matchIds.length} NEW matches | ${statDate}T${String(targetHour).padStart(2, '0')}Z`);

    // Fetch match details and build raw payloads — only for IDs we don't have
    // yet. This is now a checkpointed flow, not a single giant hour-level
    // payload. Every successful 10-ID detail window and every authoritative
    // broken-match recovery writes immediately to raw_ingest_buffer in tiny
    // local relay chunks. That preserves the batch-of-10 Hi-Rez strategy while
    // preventing one large dumpRawPayloads request from losing the whole hour.
    claimedRawMatchCountForFailure = rawMatchIds.length;
    const recoveryAttemptedIds = new Set<number>();
    const terminalNoAnchorIds = new Set<number>();
    const terminalNoAnchorReasons = new Map<number, string>();
    const batchOnlyNoAuthorityIds = new Set<number>();
    const checkpointedIds = new Set<number>();
    let checkpointedPayloadCount = 0;
    const checkpointMatches: StageMatchesCallback = async (checkpointMatchesRaw, reason) => {
      const sanitized = checkpointMatchesRaw
        .filter((m: any) => Number(m?.match_id || 0) > 0)
        .map((m: any) => JSON.parse(JSON.stringify(m).replace(/\\u0000/g, '')));
      if (sanitized.length === 0) return;

      const payloads = buildRawMatchPayloads(sanitized)
        .filter((payload: any) => !checkpointedIds.has(Number(payload.entity_id)));
      if (payloads.length === 0) return;

      const guardAfterCheckpoint = await filterAlreadyHandledMatchIds(
        payloads.map((p: any) => Number(p.entity_id)),
      );
      const idsToWrite = new Set(guardAfterCheckpoint.fetchIds);
      const payloadsToWrite = payloads.filter((p: any) => idsToWrite.has(Number(p.entity_id)));
      const resolvedIds = new Set<number>([
        ...guardAfterCheckpoint.skippedIds,
        ...payloadsToWrite
          .map((p: any) => Number(p.entity_id))
          .filter((id: number) => Number.isFinite(id) && id > 0),
      ]);

      if (payloadsToWrite.length > 0) {
        const inserted = await dumpRawPayloadsInChunks(payloadsToWrite);
        checkpointedPayloadCount += payloadsToWrite.length;
        console.log(
          `[DISCOVERY] Checkpointed ${payloadsToWrite.length} payload(s) ` +
          `(${inserted} inserted) after ${reason}`
        );
      }
      if (resolvedIds.size > 0) {
        for (const id of resolvedIds) {
          checkpointedIds.add(id);
          checkpointedIdsForFailure.add(id);
        }
        await markHourlyIngestMatchDebtStagedOrComplete(
          [...resolvedIds],
          `checkpointed during discovery: ${reason}`,
        );
      }
    };

    const allMatches = await fetchCanonicalMatchDetailsContinuously(
      matchIds,
      recoveryAttemptedIds,
      terminalNoAnchorIds,
      terminalNoAnchorReasons,
      batchOnlyNoAuthorityIds,
      checkpointMatches,
    );
    const orphanCount = allMatches.filter(m => m.match_id === 0).length;
    if (orphanCount > 0) {
      console.warn(`[DISCOVERY] Dropped ${orphanCount} orphan entries with match_id=0`);
    }
    const matches = allMatches.filter(m => m.match_id !== 0);

    // ----------------------------------------------------------------
    // Build matched-IDs set ONCE for O(1) lookup — find IDs still unresolved
    // after the canonical continuous pass. Every 10-ID window checkpoints its
    // returned outcomes, isolates only the first omitted ID through the same
    // relay operation, then refills from the remaining ordered debt. Therefore
    // every missing durable match here already has an explicit pending or
    // terminal relay outcome; no second worker recovery path is permitted.
    // ----------------------------------------------------------------
    const matchedIdsSet = new Set(matches.map((m: any) => m.match_id));
    const droppedIds = matchIds.filter(id => !matchedIdsSet.has(id));

    // Strip null bytes: JSON.stringify converts \x00 to literal "\\u0000" text
    const sanitizedMatches = matches.map((m: any) => JSON.parse(JSON.stringify(m).replace(/\\u0000/g, '')));
    const payloads = buildRawMatchPayloads(sanitizedMatches)
      .filter((payload: any) => !checkpointedIds.has(Number(payload.entity_id)));

    const postFetchGuard = await filterAlreadyHandledMatchIds(payloads.map((p: any) => Number(p.entity_id)));
    const payloadIdsToWrite = new Set(postFetchGuard.fetchIds);
    const payloadsToWrite = payloads.filter((p: any) => payloadIdsToWrite.has(Number(p.entity_id)));

    if (postFetchGuard.skipped.totalUnique > 0) {
      console.log(
        `[DISCOVERY] Dropped ${postFetchGuard.skipped.totalUnique} payloads already staged during fetch ` +
        `(writing ${payloadsToWrite.length}) | Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z`
      );
    }

    // Dump anything not already checkpointed. Most recovered/batched payloads
    // should have been written during ordered salvage; this final pass catches
    // any unusual canonical outcome path that did not hit the callback.
    if (payloadsToWrite.length > 0) {
      await dumpRawPayloadsInChunks(payloadsToWrite);
    }

    const resolvedIds = new Set<number>([
      ...guard.skippedIds,
      ...postFetchGuard.skippedIds,
      ...checkpointedIds,
      ...payloadsToWrite.map((p: any) => Number(p.entity_id)).filter((id: number) => Number.isFinite(id) && id > 0),
    ]);
    const resolvedCount = resolvedIds.size;
    const unresolvedIds = rawMatchIds.filter(id => !resolvedIds.has(id));
    const terminalUnrecoverableIds = unresolvedIds.filter(id => terminalNoAnchorIds.has(id));
    const attemptedButUnresolvedIds = unresolvedIds.filter(id => !terminalNoAnchorIds.has(id));
    if (resolvedCount > 0) {
      await markHourlyIngestMatchDebtStagedOrComplete(
        [...resolvedIds],
        'payload staged or guard proved local coverage',
      );
    }
    if (terminalUnrecoverableIds.length > 0) {
      const idsByReason = new Map<string, number[]>();
      for (const id of terminalUnrecoverableIds) {
        const reason = terminalNoAnchorReasons.get(id)
          || 'broken_unrecoverable: detail endpoint failed and getplayerbatchfrommatch returned no usable player IDs';
        idsByReason.set(reason, [...(idsByReason.get(reason) || []), id]);
      }
      for (const [reason, ids] of idsByReason) {
        await markHourlyIngestMatchDebtUnrecoverable(ids, reason);
      }
    }
    if (attemptedButUnresolvedIds.length > 0) {
      const defaultReason = droppedIds.every(id => recoveryAttemptedIds.has(id))
        ? `no authoritative payload: canonical relay recovery attempted but no authoritative payload was produced`
        : `no authoritative payload: canonical continuous discovery produced no authoritative payload`;
      const pendingGroups = new Map<string, { ids: number[]; reason: string; retryMinutes?: number }>();
      for (const id of attemptedButUnresolvedIds) {
        const reason = terminalNoAnchorReasons.get(id) || defaultReason;
        const retryMinutes = batchOnlyNoAuthorityIds.has(id)
          ? BATCH_ONLY_NO_AUTHORITY_RETRY_MINUTES
          : undefined;
        const key = `${retryMinutes || 'default'}\n${reason}`;
        const group = pendingGroups.get(key) || { ids: [], reason, retryMinutes };
        group.ids.push(id);
        pendingGroups.set(key, group);
      }
      for (const group of pendingGroups.values()) {
        await markHourlyIngestMatchDebtPending(
          statDate,
          targetHour,
          targetQueue,
          group.ids,
          group.reason,
          group.retryMinutes,
        );
      }
    }

    if (unresolvedIds.length > 0) {
      const error = new Error(
        terminalUnrecoverableIds.length === unresolvedIds.length
          ? `terminal no-player-anchor recovery for ${terminalUnrecoverableIds.length}/${rawMatchIds.length} unresolved match IDs`
          : droppedIds.every(id => recoveryAttemptedIds.has(id))
            ? `ordered recovery attempted for ${attemptedButUnresolvedIds.length}/${rawMatchIds.length} unresolved match IDs but no authoritative payload was produced`
            : `partial discovery unresolved ${attemptedButUnresolvedIds.length}/${rawMatchIds.length} match IDs`
      );
      if (attemptedButUnresolvedIds.length > 0) {
        await markHourlyIngestFailed(
          statDate,
          targetHour,
          targetQueue,
          error,
          rawMatchIds.length,
          resolvedCount,
        );
      } else if (resolvedCount > 0) {
        await markHourlyIngestStaged(statDate, targetHour, targetQueue, rawMatchIds.length, resolvedCount);
      } else {
        await markHourlyIngestComplete(statDate, targetHour, targetQueue, rawMatchIds.length);
      }
      console.warn(
        `[DISCOVERY] Partially staged ${resolvedCount}/${rawMatchIds.length} matches for ` +
        `Queue ${targetQueue} | ${statDate}T${String(targetHour).padStart(2, '0')}Z; ` +
        `${terminalUnrecoverableIds.length} terminal and ${attemptedButUnresolvedIds.length} retryable unresolved match ID(s): ${error.message}`
      );
      console.log(`[DISCOVERY] Dumped ${checkpointedPayloadCount + payloadsToWrite.length} match payloads to raw_ingest_buffer`);
      return checkpointedPayloadCount + payloadsToWrite.length;
    }

    if (payloadsToWrite.length > 0 || droppedIds.length > 0 || postFetchGuard.skipped.rawBuffer > 0 || postFetchGuard.skipped.pullList > 0) {
      await markHourlyIngestStaged(statDate, targetHour, targetQueue, rawMatchIds.length, resolvedCount);
    } else {
      await markHourlyIngestComplete(statDate, targetHour, targetQueue, rawMatchIds.length);
    }

    console.log(`[DISCOVERY] Dumped ${checkpointedPayloadCount + payloadsToWrite.length} match payloads to raw_ingest_buffer`);
    return checkpointedPayloadCount + payloadsToWrite.length;
  } catch (err) {
    console.error(`[DISCOVERY] Failed for queue ${targetQueue}, ${statDate}T${targetHour}: ${err}`);
    await markHourlyIngestFailed(
      statDate,
      targetHour,
      targetQueue,
      err,
      claimedRawMatchCountForFailure,
      checkpointedIdsForFailure.size > 0 ? checkpointedIdsForFailure.size : null,
    );
    return 0;
  }
}

/**
 * Get latest hourly match counts
 */
export async function getHourlySummary(limit = 50): Promise<any[]> {
  const result = await query(`
    SELECT date, hour, queue_id,
      matches_na, matches_eu, matches_sea, matches_br, matches_oce, matches_sa, matches_unknown,
      total_matches, fetched_at
    FROM hourly_match_counts
    ORDER BY date DESC, hour DESC
    LIMIT $1
  `, [limit]);
  return result;
}
