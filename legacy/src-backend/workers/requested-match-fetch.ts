import type { MatchDetails, RawPayload } from '../contracts/hirez-relay';
import { extractMatchBanFields } from '../utils/match-bans';
import { isValidCompletedMatchScore } from '../services/ranked-score';

export type RequestedMatchDependencies = {
  getMatchDetailsBatch: (matchIds: number[]) => Promise<MatchDetails[]>;
};

export class RequestedMatchRecoveryError extends Error {
  constructor(
    public readonly matchId: number,
    message: string,
    public readonly cause?: unknown,
  ) {
    super(message);
    this.name = 'RequestedMatchRecoveryError';
  }
}

export function isRecoverableRequestedMatchError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /HIREZ_UNKNOWN_RETURN|Int16|skin[_ ]?id|too large|too small/i.test(message);
}

function hasAuthoritativeShell(match: MatchDetails): boolean {
  return Number(match.match_id) > 0
    && Boolean(match.entry_datetime)
    && Number.isFinite(Date.parse(match.entry_datetime))
    && Number(match.queue_id) > 0
    && Boolean(String(match.map || '').trim())
    && Number(match.duration_seconds) > 0
    && isValidCompletedMatchScore(match.team1_score, match.team2_score, match.winning_task_force)
    && Array.isArray(match.players)
    && match.players.length > 0;
}

export function buildRequestedMatchPayload(match: MatchDetails): RawPayload {
  return {
    endpoint: 'getmatchdetailsbatch',
    entity_type: 'match',
    entity_id: match.match_id,
    source: 'manual_lookup',
    raw_data: match.players.map((player: any, index: number) => {
      const scoreObservation = match.direct_score_observations?.[index];
      return ({
      ...player,
      Match: match.match_id,
      Entry_Datetime: match.entry_datetime,
      Map_Game: match.map,
      match_queue_id: match.queue_id,
      Match_Duration: match.duration_seconds,
      Minutes: match.minutes,
      Region: match.region || player.region,
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
  };
}

/**
 * Fetch one specifically requested match through the canonical relay lookup.
 * The relay has already completed direct lookup and any required recovery
 * before this function receives a result; this backend adapter only stages it.
 */
export async function fetchRequestedMatchPayload(
  matchId: number,
  dependencies: RequestedMatchDependencies,
): Promise<RawPayload | null> {
  try {
    const matches = await dependencies.getMatchDetailsBatch([matchId]);
    const match = matches.find(candidate => Number(candidate.match_id) === matchId) || null;
    if (match?.recovery_pending === true) {
      throw new RequestedMatchRecoveryError(
        matchId,
        `HirezRelay recovery remains pending for match ${matchId}: ${match.recovery_source || 'target history unresolved'}`,
      );
    }
    return match && hasAuthoritativeShell(match) ? buildRequestedMatchPayload(match) : null;
  } catch (error) {
    if (!isRecoverableRequestedMatchError(error)) throw error;
    throw new RequestedMatchRecoveryError(
      matchId,
      `HirezRelay could not reconstruct a durable response for match ${matchId}`,
      error,
    );
  }
}
