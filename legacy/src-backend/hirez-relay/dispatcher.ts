import { dispatchDummy } from './dummy-data';
import { validateRelayOperationFromManifest } from '../contracts/hirez-relay-operation-contract';

export type RelayMode = 'dummy' | 'real';

export class RelayValidationError extends Error {
  statusCode = 400;
  code = 'VALIDATION_ERROR';

  constructor(message: string) {
    super(message);
    this.name = 'RelayValidationError';
  }
}

export function getRelayMode(environment: NodeJS.ProcessEnv = process.env): RelayMode {
  if (environment.HIREZ_RELAY_MODE === 'real') return 'real';

  // Dummy responses are exclusively for local/test execution. A production
  // relay must never silently serve synthetic data because its mode variable
  // was omitted or misspelled.
  if (environment.NODE_ENV === 'production') {
    throw new Error('HIREZ_RELAY_MODE must be set to "real" in production. Refusing to start with dummy data.');
  }

  return 'dummy';
}

export function validateRelayOperation(
  operation: string,
  args: any[] = [],
  mode?: RelayMode,
): void {
  // The dispatcher is the shared contract for dummy and real mode. Validating
  // here prevents a test-only dummy call from accepting shapes that would later
  // fail against the real Hi-Rez path, and it keeps malformed worker payloads
  // away from session/key/rate-limit logic entirely.
  validateRelayOperationFromManifest(
    operation,
    args,
    mode,
    (message): never => {
      throw new RelayValidationError(message);
    },
  );
}

async function dispatchReal(operation: string, args: any[] = []): Promise<unknown> {
  const core = await import('./core.js');
  const handlers: Record<string, (...innerArgs: any[]) => unknown> = {
    getMatchDetailsBatch: core.getMatchDetailsBatch,
    resumeMatchRecovery: core.resumeMatchRecovery,
    getDataUsed: core.getDataUsed,
    syncApiKeyUsage: core.syncApiKeyUsage,
    getMatchIdsByQueue: core.getMatchIdsByQueue,
    getMatchIdsByQueueDetails: core.getMatchIdsByQueueDetails,
    getMatchDetailsBatchRaw: core.getMatchDetailsBatchRaw,
    getMatchDetailsRaw: core.getMatchDetailsRaw,
    getPlayerChampions: core.getPlayerChampions,
    getChampionRanks: core.getChampionRanks,
    getChampions: core.getChampions,
    getItems: core.getItems,
    getEsportsProLeagueDetails: core.getEsportsProLeagueDetails,
    getPlayerLoadouts: core.getPlayerLoadouts,
    getPlayerStatus: core.getPlayerStatus,
    getMatchPlayerDetails: core.getMatchPlayerDetails,
    getLeagueLeaderboard: core.getLeagueLeaderboard,
    getLeagueSeasons: core.getLeagueSeasons,
    getPlayerBatchFromMatch: core.getPlayerBatchFromMatch,
    getDemoDetails: core.getDemoDetails,
    getPlayerBatch: core.getPlayerBatch,
    getPlayerBatchLookup: core.getPlayerBatchLookup,
    getMatchHistory: core.getMatchHistory,
    getPlayers: core.getPlayers,
    getPlayerIdByName: core.getPlayerIdByName,
    searchPlayers: core.searchPlayers,
    getPlayerIdsByGamerTag: core.getPlayerIdsByGamerTag,
    getPlayerIdByPortalUserId: core.getPlayerIdByPortalUserId,
    getMatchLeaderboard: core.getMatchLeaderboard,
    dumpRawPayloads: core.dumpRawPayloads,
    cleanupFetchedPlayersCache: core.cleanupFetchedPlayersCache,
    clearMatchHistoryCache: core.clearMatchHistoryCache,
    resetDummyApiCallCounts: () => dispatchDummy('resetDummyApiCallCounts'),
    getDummyApiCallCounts: () => dispatchDummy('getDummyApiCallCounts'),
    reloadApiKeyPool: async () => {
      const { apiKeyPool } = await import('../services/api-key-pool.js');
      await apiKeyPool.loadKeys();
      return true;
    },
    getApiKeyStatus: async () => {
      const { apiKeyPool } = await import('../services/api-key-pool.js');
      return apiKeyPool.getStatus();
    },
  };

  const handler = handlers[operation];
  if (!handler) throw new RelayValidationError(`Unsupported HirezRelay operation: ${operation}`);
  return handler(...args);
}

export async function dispatchRelayOperation(operation: string, args: any[] = [], mode = getRelayMode()): Promise<unknown> {
  if (!Array.isArray(args)) throw new RelayValidationError('args must be an array');
  validateRelayOperation(operation, args, mode);

  // Dummy mode replaces Hi-Rez network calls with synthetic responses, but
  // `dumpRawPayloads` is not a Hi-Rez call. It is the durable handoff from the
  // relay into PaladinsCat's DB-backed queue (`raw_ingest_buffer`). Returning a
  // fake count here makes discovery workers believe staging succeeded while
  // the buffer processor sees an empty table, which hides pipeline failures and
  // recreates the "worker is blind during ingest" class of bugs. Keep staging
  // real in every mode so dummy tests exercise the same queue, idempotency
  // guards, and buffer-drain logic that production uses, without consuming API
  // quota.
  if (operation === 'dumpRawPayloads') {
    const core = await import('./core.js');
    return core.dumpRawPayloads(args[0]);
  }

  if (mode === 'dummy' && operation === 'getMatchHistory') {
    // getMatchHistory is both an API call and a persistence hook. Even in
    // dummy mode, run the real relay function so synthetic history gets written
    // to player_match_history_cache/player_match_history_entries instead of
    // returning an in-memory object that the backend cannot reuse.
    const core = await import('./core.js');
    return core.getMatchHistory(args[0], args[1], args[2]);
  }

  if (mode === 'dummy' && (
    operation === 'getMatchDetailsBatch'
    || operation === 'resumeMatchRecovery'
  )) {
    // Canonical match lookup includes relay-owned recovery. Dummy mode must run
    // that same orchestration while core.apiRequest() supplies synthetic
    // endpoint responses and consumes zero Hi-Rez quota.
    const core = await import('./core.js');
    return operation === 'getMatchDetailsBatch'
      ? core.getMatchDetailsBatch(args[0])
      : core.resumeMatchRecovery(args[0]);
  }

  if (mode === 'dummy') return dispatchDummy(operation, args);
  return dispatchReal(operation, args);
}
