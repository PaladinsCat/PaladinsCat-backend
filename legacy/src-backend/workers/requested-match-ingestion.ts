import { one } from '../config/db';
import {
  dumpRawPayloads,
  getMatchDetailsBatch,
} from '../services/hirez';
import { processPendingMatchBufferRows } from './buffer-processor';
import {
  fetchRequestedMatchPayload,
  type RequestedMatchDependencies,
} from './requested-match-fetch';

const REQUEST_COMPLETION_TIMEOUT_MS = Number(process.env.MATCH_LOOKUP_INGEST_TIMEOUT_MS || 120000);
const REQUEST_COMPLETION_POLL_MS = 200;

export type RequestedMatchIngestionStatus =
  | 'ready'
  | 'not_found'
  | 'recovery_failed'
  | 'processing_timeout';

export interface RequestedMatchIngestionResult {
  matchId: number;
  status: RequestedMatchIngestionStatus;
  error?: string;
}

const inFlight = new Map<number, Promise<RequestedMatchIngestionResult>>();

const defaultDependencies: RequestedMatchDependencies = {
  getMatchDetailsBatch: async matchIds => {
    const outcomes = await getMatchDetailsBatch(
      matchIds.map(matchId => ({ matchId })),
      'requested_match_lookup',
    );
    return outcomes.flatMap(outcome => outcome.match ? [outcome.match] : []);
  },
};

async function waitForDurableMatch(
  matchId: number,
): Promise<Extract<RequestedMatchIngestionStatus, 'ready' | 'recovery_failed' | 'processing_timeout'>> {
  const deadline = Date.now() + REQUEST_COMPLETION_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const row = await one<{ match_exists: boolean; ingest_status: string | null; completed_stages: string[] | null }>(
      `SELECT EXISTS(SELECT 1 FROM matches WHERE match_id = $1) AS match_exists,
              (SELECT status FROM match_ingest_status WHERE match_id = $1) AS ingest_status,
              (SELECT completed_stages FROM match_ingest_status WHERE match_id = $1) AS completed_stages`,
      [matchId],
    );
    if (
      row?.match_exists
      && (
        ['complete', 'limited'].includes(row.ingest_status || '')
        || (
          row.completed_stages?.includes('player_facts')
          && row.completed_stages.includes('match_bans')
        )
      )
    ) return 'ready';
    if (row?.ingest_status === 'failed') return 'recovery_failed';
    await new Promise(resolve => setTimeout(resolve, REQUEST_COMPLETION_POLL_MS));
  }
  return 'processing_timeout';
}

async function ingestOneRequestedMatch(matchId: number): Promise<RequestedMatchIngestionResult> {
  try {
    const payload = await fetchRequestedMatchPayload(matchId, defaultDependencies);
    if (!payload) return { matchId, status: 'not_found' };

    // dumpRawPayloads is guarded by uq_rib_active_match_entity. If hourly ingest
    // or another request already staged this ID, the insert becomes a no-op and
    // the targeted processor/wait path joins that existing work.
    await dumpRawPayloads([payload]);
    await processPendingMatchBufferRows([matchId]);
    const status = await waitForDurableMatch(matchId);
    return { matchId, status };
  } catch (error: any) {
    return {
      matchId,
      status: 'recovery_failed',
      error: error?.message || String(error),
    };
  }
}

export async function ingestRequestedMatchesDetailed(
  matchIds: number[],
): Promise<RequestedMatchIngestionResult[]> {
  const ids = [...new Set(matchIds.map(Number).filter(id => Number.isFinite(id) && id > 0))];
  return Promise.all(ids.map(async (matchId) => {
    let work = inFlight.get(matchId);
    if (!work) {
      work = ingestOneRequestedMatch(matchId).finally(() => inFlight.delete(matchId));
      inFlight.set(matchId, work);
    }
    return work;
  }));
}

export async function ingestRequestedMatches(matchIds: number[]): Promise<number[]> {
  const results = await ingestRequestedMatchesDetailed(matchIds);
  return results
    .filter(result => result.status === 'ready')
    .map(result => result.matchId);
}
