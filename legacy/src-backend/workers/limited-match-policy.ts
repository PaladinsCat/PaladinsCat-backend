export const LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE = 'roster_anchor_unavailable';

const TERMINAL_RECOVERY_SOURCES = new Set([
  'no_player_anchors',
  'getplayerbatchfrommatch_failed',
]);

export type LimitedMatchCandidate = {
  playerCount: number;
  teamOneCount: number;
  teamTwoCount: number;
  allRowsAuthoritative: boolean;
  recoverySource?: unknown;
  recoveryTerminal?: unknown;
  recoveryApiCalls?: unknown;
  anchorPlayerCount?: unknown;
};

/**
 * Decide whether an incomplete direct roster is safe to retain as lookup-only
 * match data. This is deliberately narrow: one roster-anchor request must have
 * failed or returned no anchors, and every surviving row must still be an
 * authoritative direct/recovered detail row with a plausible two-team shape.
 *
 * Other partial payloads remain retryable. In particular, this policy never
 * promotes profile/history observations or a zero-player shell into matches.
 */
export function limitedMatchReason(candidate: LimitedMatchCandidate): string | null {
  const playerCount = Number(candidate.playerCount);
  const teamOneCount = Number(candidate.teamOneCount);
  const teamTwoCount = Number(candidate.teamTwoCount);
  const recoveryApiCalls = Number(candidate.recoveryApiCalls);
  const anchorPlayerCount = Number(candidate.anchorPlayerCount);
  const recoverySource = String(candidate.recoverySource || '').toLowerCase();
  const terminalRecovery = candidate.recoveryTerminal === true
    || TERMINAL_RECOVERY_SOURCES.has(recoverySource)
    || anchorPlayerCount === 0;

  if (!Number.isInteger(playerCount) || playerCount < 1 || playerCount >= 10) return null;
  if (!candidate.allRowsAuthoritative) return null;
  if (!Number.isInteger(teamOneCount) || teamOneCount < 1 || teamOneCount > 5) return null;
  if (!Number.isInteger(teamTwoCount) || teamTwoCount < 1 || teamTwoCount > 5) return null;
  if (teamOneCount + teamTwoCount !== playerCount) return null;
  if (!terminalRecovery || recoveryApiCalls !== 1) return null;

  return LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE;
}
