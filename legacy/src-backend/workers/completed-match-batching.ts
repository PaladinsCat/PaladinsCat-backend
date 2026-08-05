import type {
  CompletedMatchRequest,
  CompletedMatchResolution,
} from '../contracts/hirez-relay';

const MATCH_BATCH_SIZE = 10;

export type CompletedMatchBatchFetcher = (
  requests: CompletedMatchRequest[],
) => Promise<CompletedMatchResolution[]>;

export type ContinuousCompletedMatchOptions = {
  /**
   * Persist/checkpoint each outcome before another vendor window is opened.
   * This is how healthy prefixes become durable even if a later blocker fails.
   */
  onResult?: (result: CompletedMatchResolution) => Promise<void>;
  /**
   * Only parser/shape failures with no usable prefix may be bisected.
   * Quota, session exhaustion, transport, and service-wide failures must escape
   * without singleton fan-out.
   */
  isRecoverableBatchError?: (error: unknown) => boolean;
};

export class CompletedMatchBatchContractError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CompletedMatchBatchContractError';
  }
}

export function isRecoverableCompletedMatchBatchError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /HIREZ_UNKNOWN_RETURN|Int16|skin[_ ]?id|too large|too small/i.test(message);
}

function normalizeRequests(requests: CompletedMatchRequest[]): CompletedMatchRequest[] {
  const normalized: CompletedMatchRequest[] = [];
  const seen = new Set<number>();
  for (const request of requests) {
    const matchId = Number(request.matchId);
    const queueId = request.queueId == null ? undefined : Number(request.queueId);
    if (!Number.isInteger(matchId) || matchId <= 0) {
      throw new CompletedMatchBatchContractError('matchId must be a positive integer');
    }
    if (queueId !== undefined && (!Number.isInteger(queueId) || queueId <= 0)) {
      throw new CompletedMatchBatchContractError(
        `queueId for match ${matchId} must be a positive integer`,
      );
    }
    if (seen.has(matchId)) continue;
    seen.add(matchId);
    normalized.push({ matchId, queueId });
  }
  return normalized;
}

function validateOutcomes(
  window: CompletedMatchRequest[],
  outcomes: CompletedMatchResolution[],
): Map<number, CompletedMatchResolution> {
  const requested = new Set(window.map(request => request.matchId));
  const byId = new Map<number, CompletedMatchResolution>();
  for (const outcome of outcomes) {
    const matchId = Number(outcome?.matchId);
    if (!requested.has(matchId)) {
      throw new CompletedMatchBatchContractError(
        `relay returned unrequested match ${matchId || 'unknown'}`,
      );
    }
    if (byId.has(matchId)) {
      throw new CompletedMatchBatchContractError(
        `relay returned duplicate outcome for match ${matchId}`,
      );
    }
    byId.set(matchId, outcome);
  }
  return byId;
}

/**
 * Worker-owned continuous completed-match batching.
 *
 * HirezRelay owns all upstream acquisition and reconstruction. This routine
 * owns only ordered batch formation:
 *
 * 1. Call the canonical relay operation with up to ten requests.
 * 2. Checkpoint every returned direct/recovered/pending/terminal outcome.
 * 3. If a multi-match response omits IDs, isolate only the first missing ID
 *    through the same canonical singleton operation.
 * 4. Remove the healthy prefix and isolated blocker, refill the next ten slots
 *    from the remaining ordered requests, and continue.
 * 5. Bisect only a classified parser/shape failure that returned no outcomes;
 *    service-wide failures escape without fan-out.
 */
export async function fetchCompletedMatchesContinuously(
  requests: CompletedMatchRequest[],
  fetchBatch: CompletedMatchBatchFetcher,
  options: ContinuousCompletedMatchOptions = {},
): Promise<CompletedMatchResolution[]> {
  const ordered = normalizeRequests(requests);
  if (ordered.length === 0) return [];

  const completed = new Map<number, CompletedMatchResolution>();
  const isRecoverable = options.isRecoverableBatchError
    ?? isRecoverableCompletedMatchBatchError;

  const emit = async (outcome: CompletedMatchResolution): Promise<void> => {
    if (completed.has(outcome.matchId)) {
      throw new CompletedMatchBatchContractError(
        `worker attempted to emit match ${outcome.matchId} twice`,
      );
    }
    await options.onResult?.(outcome);
    completed.set(outcome.matchId, outcome);
  };

  const process = async (batchRequests: CompletedMatchRequest[]): Promise<void> => {
    const pending = [...batchRequests];
    while (pending.length > 0) {
      const window = pending.slice(0, MATCH_BATCH_SIZE);
      let outcomes: CompletedMatchResolution[];
      try {
        outcomes = await fetchBatch(window);
      } catch (error) {
        if (!isRecoverable(error) || window.length === 1) throw error;

        // No safe blocker identity exists when the whole multi-match operation
        // throws. Split only that failed window; already-checkpointed results
        // and requests behind the window remain untouched.
        const midpoint = Math.ceil(window.length / 2);
        await process(window.slice(0, midpoint));
        await process(window.slice(midpoint));
        pending.splice(0, window.length);
        continue;
      }

      const byId = validateOutcomes(window, outcomes);
      for (const request of window) {
        const outcome = byId.get(request.matchId);
        if (outcome) await emit(outcome);
      }

      const returnedIds = new Set(byId.keys());
      for (let index = pending.length - 1; index >= 0; index--) {
        if (returnedIds.has(pending[index].matchId)) pending.splice(index, 1);
      }

      const blocker = window.find(request => !returnedIds.has(request.matchId));
      if (!blocker) continue;

      const singleton = await fetchBatch([blocker]);
      const singletonById = validateOutcomes([blocker], singleton);
      const blockerOutcome = singletonById.get(blocker.matchId);
      if (!blockerOutcome) {
        throw new CompletedMatchBatchContractError(
          `canonical singleton returned no outcome for match ${blocker.matchId}`,
        );
      }
      await emit(blockerOutcome);
      const blockerIndex = pending.findIndex(
        request => request.matchId === blocker.matchId,
      );
      if (blockerIndex >= 0) pending.splice(blockerIndex, 1);
    }
  };

  await process(ordered);
  return ordered.map(request => {
    const outcome = completed.get(request.matchId);
    if (!outcome) {
      throw new CompletedMatchBatchContractError(
        `continuous batching failed to account for match ${request.matchId}`,
      );
    }
    return outcome;
  });
}
