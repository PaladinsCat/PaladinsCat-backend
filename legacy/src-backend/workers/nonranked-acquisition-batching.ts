import type {
  CompletedMatchRequest,
  CompletedMatchResolution,
  MatchDetails,
} from '../contracts/hirez-relay';
import { isIncompleteDirectMatch } from '../services/batch-int16';
import { fetchCompletedMatchesContinuously } from './completed-match-batching';

const BATCH_SIZE = 10;

export type HybridFetchResult = {
  matchId: number;
  detail?: MatchDetails;
  roster?: any[];
  state: 'complete_direct' | 'partial_roster' | 'roster_only' | 'dropped';
  terminalReason?: string;
};

export interface HybridFetchDependencies {
  getMatchDetailsBatch: (
    requests: CompletedMatchRequest[],
  ) => Promise<CompletedMatchResolution[]>;
}

export type HybridFetchOptions = {
  onResult?: (result: HybridFetchResult) => Promise<void>;
};

function clean(value: unknown): string {
  return String(value ?? '').trim();
}

function finiteInt(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

export function usableMatchPlayers(detail?: MatchDetails): any[] {
  if (!detail || !Array.isArray(detail.players)) return [];
  return detail.players.filter(player => !player?.has_ret_msg && !clean(player?.ret_msg));
}

export function isCompleteMatchDetail(detail?: MatchDetails): boolean {
  return Boolean(detail && !isIncompleteDirectMatch(detail) && usableMatchPlayers(detail).length === 10);
}

export function isCompleteNonrankedMatchDetail(
  detail: MatchDetails | undefined,
  participantModel: 'pvp' | 'pve' | 'bots' | 'custom' | 'unknown',
): boolean {
  if (!detail) return false;
  if (participantModel === 'pve' || participantModel === 'bots') {
    // Hi-Rez omits AI participants from these queues. One or more usable
    // human rows is therefore a complete direct response, not a broken
    // ten-player PvP response that needs a roster fallback.
    return usableMatchPlayers(detail).length > 0;
  }
  return isCompleteMatchDetail(detail);
}

/**
 * Presence upserts from concurrent matches can overlap on multiple players.
 * Always acquire those PostgreSQL rows in the same player-ID order so two
 * transactions cannot deadlock when the vendor returns opposite roster order.
 * The Map also prevents a malformed roster from writing one player twice.
 */
export function orderUniquePresenceFacts<T extends { playerId: number }>(facts: T[]): T[] {
  const byPlayerId = new Map<number, T>();
  for (const fact of facts) {
    if (Number.isInteger(fact.playerId) && fact.playerId > 0 && !byPlayerId.has(fact.playerId)) {
      byPlayerId.set(fact.playerId, fact);
    }
  }
  return [...byPlayerId.values()].sort((left, right) => left.playerId - right.playerId);
}

/**
 * Split a claim page into bounded continuous streams without creating partial
 * vendor batches unnecessarily. Ten-ID windows are distributed round-robin so
 * every lane can keep one full request in flight and refill independently
 * after isolating a blocker.
 */
export function buildContinuousFetchLanes(
  matchIds: number[],
  concurrency: number,
): number[][] {
  const ids = [...new Set(matchIds.filter(id => Number.isInteger(id) && id > 0))];
  if (ids.length === 0) return [];
  const laneCount = Math.min(
    Math.max(1, Math.trunc(concurrency)),
    Math.ceil(ids.length / BATCH_SIZE),
  );
  const lanes = Array.from({ length: laneCount }, () => [] as number[]);
  for (let offset = 0, windowIndex = 0; offset < ids.length; offset += BATCH_SIZE, windowIndex++) {
    lanes[windowIndex % laneCount].push(...ids.slice(offset, offset + BATCH_SIZE));
  }
  return lanes.filter(lane => lane.length > 0);
}

/**
 * Run known non-ranked requests through the canonical relay pipeline.
 *
 * The relay owns the one permitted roster fallback. This worker owns only
 * continuous batch formation and persistence: omitted IDs are isolated through
 * the same canonical singleton operation, then the remaining ordered IDs
 * refill the next ten-match window.
 */
export async function fetchNonrankedMatchesContinuously(
  requests: CompletedMatchRequest[],
  dependencies: HybridFetchDependencies,
  options: HybridFetchOptions = {},
): Promise<HybridFetchResult[]> {
  const results: HybridFetchResult[] = [];
  const emit = async (result: HybridFetchResult): Promise<void> => {
    // Persisting through this callback makes every released prefix/blocker
    // durable before the next vendor request. If a later batch hits a
    // service-wide failure, already-fetched matches do not get thrown away and
    // fetched again on the next pass.
    await options.onResult?.(result);
    results.push(result);
  };

  await fetchCompletedMatchesContinuously(
    requests,
    dependencies.getMatchDetailsBatch,
    {
      onResult: async outcome => {
        const detail = outcome.match;
        const roster = outcome.roster;
        const hasDetail = usableMatchPlayers(detail).length > 0;
        const hasRoster = Array.isArray(roster) && roster.length > 0;
        let state: HybridFetchResult['state'];
        switch (outcome.status) {
          case 'complete_direct':
          case 'complete_recovered':
            state = 'complete_direct';
            break;
          case 'limited':
            state = hasDetail ? 'partial_roster' : hasRoster ? 'roster_only' : 'dropped';
            break;
          case 'roster_only':
            state = hasRoster ? 'roster_only' : 'dropped';
            break;
          case 'recovery_pending':
          case 'dropped':
          default:
            state = 'dropped';
            break;
        }
        await emit({
          matchId: outcome.matchId,
          detail,
          roster: hasRoster ? roster : undefined,
          state,
          terminalReason: state === 'complete_direct'
            ? undefined
            : outcome.reason || (
              state === 'dropped'
                ? 'single_pass_no_match_facts'
                : 'single_pass_presence_only'
            ),
        });
      },
    },
  );
  return results;
}
