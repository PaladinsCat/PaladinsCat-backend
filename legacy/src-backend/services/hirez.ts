/**
 * HirezRelay client facade.
 *
 * Backend workers keep importing this module, but all Hi-Rez-specific API
 * behavior now lives in hirez-relay. This file is intentionally transport-only:
 * it sends typed operations to the relay. Completed-match consumers share one
 * canonical getMatchDetailsBatch contract; recovery-only compatibility
 * operations are intentionally not exposed here.
 */
import { randomUUID } from 'crypto';
import {
  CompletedMatchRequest,
  CompletedMatchResolution,
  MatchDetails,
  MatchIdObservation,
  PlayerDetails,
  RawPayload,
  RelayCallAttribution,
  RelayCallResponse,
} from '../contracts/hirez-relay';

export {
  CompletedMatchRequest,
  CompletedMatchResolution,
  MatchDetails,
  PlayerDetails,
};

const RELAY_URL = (process.env.HIREZ_RELAY_URL || 'http://127.0.0.1:3015').replace(/\/+$/, '');
const RELAY_TIMEOUT_MS = Number(process.env.HIREZ_RELAY_TIMEOUT_MS || 120000);

async function relayCall<T>(
  operation: string,
  args: any[] = [],
  attribution?: RelayCallAttribution | string,
): Promise<T> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), RELAY_TIMEOUT_MS);
  const requestId = randomUUID();

  try {
    const response = await fetch(`${RELAY_URL}/v1/call`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        operation,
        args,
        requestId,
        attribution: typeof attribution === 'string'
          ? { consumer: attribution }
          : attribution ?? { consumer: 'backend_unattributed' },
      }),
      signal: controller.signal,
    });

    const data = await response.json().catch(() => null) as RelayCallResponse<T> | null;
    if (!response.ok || !data?.ok) {
      throw new Error(data?.error || `HirezRelay ${operation} failed with HTTP ${response.status}`);
    }
    return data.result as T;
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      throw new Error(`HirezRelay ${operation} timed out after ${RELAY_TIMEOUT_MS}ms`);
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}

async function relaySignal(operation: string): Promise<void> {
  // These relay "signals" mutate state inside the relay process rather than in
  // the backend process. They must be awaited by workers that depend on the
  // state change. The buffer processor, for example, clears the relay's
  // recovery-history cache at the start of a cycle so one match's recovery data
  // cannot leak into the next cycle. The previous fire-and-forget version could
  // start processing before the relay received the cleanup request, making cache
  // behavior timing-dependent and very hard to debug.
  await relayCall(operation);
}

export async function cleanupFetchedPlayersCache(): Promise<void> {
  await relaySignal('cleanupFetchedPlayersCache');
}

export async function clearMatchHistoryCache(): Promise<void> {
  await relaySignal('clearMatchHistoryCache');
}

export async function getMatchDetailsBatch(
  requests: CompletedMatchRequest[],
  attribution?: RelayCallAttribution | string,
): Promise<CompletedMatchResolution[]> {
  if (requests.length === 0) return [];
  return relayCall<CompletedMatchResolution[]>(
    'getMatchDetailsBatch',
    [requests],
    attribution,
  );
}

export async function getDataUsed(devId: string): Promise<any> {
  return relayCall<any>('getDataUsed', [devId]);
}

export async function syncApiKeyUsage(devId: string): Promise<boolean> {
  return relayCall<boolean>('syncApiKeyUsage', [devId]);
}

export async function getMatchIdsByQueue(
  queueId: number,
  date: string,
  hour: number,
  attribution?: RelayCallAttribution | string,
): Promise<number[]> {
  return relayCall<number[]>('getMatchIdsByQueue', [queueId, date, hour], attribution);
}

export async function getMatchIdsByQueueDetails(
  queueId: number,
  date: string,
  hour: number,
  attribution?: RelayCallAttribution | string,
): Promise<MatchIdObservation[]> {
  return relayCall<MatchIdObservation[]>('getMatchIdsByQueueDetails', [queueId, date, hour], attribution);
}

export async function getMatchDetailsBatchRaw(
  matchIds: number[],
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  if (matchIds.length === 0) return [];
  return relayCall<any[]>('getMatchDetailsBatchRaw', [matchIds], attribution);
}

export async function getMatchDetailsRaw(matchId: number): Promise<any[]> {
  return relayCall<any[]>('getMatchDetailsRaw', [matchId]);
}

export async function getPlayerChampions(playerId: number): Promise<any[]> {
  return relayCall<any[]>('getPlayerChampions', [playerId]);
}

export async function getChampionRanks(
  playerId: number,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getChampionRanks', [playerId], attribution);
}

export async function getChampions(
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getChampions', [], attribution);
}

export async function getItems(
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getItems', [], attribution);
}

export async function getEsportsProLeagueDetails(
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getEsportsProLeagueDetails', [], attribution);
}

export async function getPlayerLoadouts(
  playerId: number,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getPlayerLoadouts', [playerId], attribution);
}

export async function getPlayerStatus(
  playerId: number,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getPlayerStatus', [playerId], attribution);
}

export async function getMatchPlayerDetails(
  matchId: number,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getMatchPlayerDetails', [matchId], attribution);
}

export async function getLeagueLeaderboard(
  queueId: number,
  tier: number,
  season: number,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getLeagueLeaderboard', [queueId, tier, season], attribution);
}

export async function getLeagueSeasons(
  queueId: number,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getLeagueSeasons', [queueId], attribution);
}

export async function getPlayerBatchFromMatch(
  matchId: number,
  attribution: RelayCallAttribution | string = 'presence_acquisition',
): Promise<any[]> {
  return relayCall<any[]>('getPlayerBatchFromMatch', [matchId], attribution);
}

export async function getDemoDetails(
  matchId: number,
  attribution?: RelayCallAttribution | string,
): Promise<any> {
  return relayCall<any>('getDemoDetails', [matchId], attribution);
}

export async function getPlayerBatch(
  playerIds: number[],
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  if (playerIds.length === 0) return [];
  return relayCall<any[]>('getPlayerBatch', [playerIds], attribution);
}

export async function getPlayerBatchLookup(
  playerIds: number[],
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  if (playerIds.length === 0) return [];
  return relayCall<any[]>('getPlayerBatchLookup', [playerIds], attribution);
}

export async function getMatchHistory(
  playerId: number,
  limit = 50,
  forceRefresh = false,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getMatchHistory', [playerId, limit, forceRefresh], attribution);
}

export async function getPlayers(names: string[]): Promise<any[]> {
  if (names.length === 0) return [];
  return relayCall<any[]>('getPlayers', [names]);
}

export async function getPlayerIdByName(
  playerName: string,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getPlayerIdByName', [playerName], attribution);
}

export async function searchPlayersRemote(
  searchPlayer: string,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('searchPlayers', [searchPlayer], attribution);
}

export async function getPlayerIdsByGamerTag(
  portalId: number,
  gamerTag: string,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getPlayerIdsByGamerTag', [portalId, gamerTag], attribution);
}

export async function getPlayerIdByPortalUserId(
  portalId: number,
  portalUserId: string,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getPlayerIdByPortalUserId', [portalId, portalUserId], attribution);
}

export async function getMatchLeaderboard(
  tier: number,
  season: number,
  attribution?: RelayCallAttribution | string,
): Promise<any[]> {
  return relayCall<any[]>('getMatchLeaderboard', [tier, season], attribution);
}

export async function dumpRawPayloads(payloads: RawPayload[]): Promise<number> {
  if (payloads.length === 0) return 0;
  return relayCall<number>('dumpRawPayloads', [payloads]);
}

export async function resetDummyApiCallCounts(): Promise<Record<string, number>> {
  return relayCall<Record<string, number>>('resetDummyApiCallCounts');
}

export async function getDummyApiCallCounts(): Promise<Record<string, number>> {
  return relayCall<Record<string, number>>('getDummyApiCallCounts');
}

export async function setDummyMatchScenario(matchId: number, scenario: string): Promise<boolean> {
  return relayCall<boolean>('setDummyMatchScenario', [matchId, scenario]);
}

export async function resetDummyMatchScenarios(): Promise<boolean> {
  return relayCall<boolean>('resetDummyMatchScenarios');
}

export async function reloadApiKeyPool(): Promise<boolean> {
  return relayCall<boolean>('reloadApiKeyPool');
}

export async function getApiKeyStatus(): Promise<any[]> {
  return relayCall<any[]>('getApiKeyStatus');
}
