/**
 * Match-result integrity is intentionally queue-agnostic. Queue IDs decide
 * which analytical projections are ranked-only; they must not decide whether
 * a recovered match gets a coherent result shell.
 */
export type MatchScore = {
  team1: number;
  team2: number;
  winner: 1 | 2;
  canonicalized: boolean;
};

export type MatchScoreResolution = MatchScore & {
  source: 'direct' | 'history';
};

export type MatchScoreInput = {
  team1?: unknown;
  team2?: unknown;
  winner?: unknown;
};

export type MatchPlayerOutcomeInput = {
  task_force?: unknown;
  win_status?: unknown;
};

export type MatchPlayerOutcomeConsensus = {
  coherent: boolean;
  observations: number;
  winner: 1 | 2 | null;
};

export type RecoveredMatchScoreResolution = MatchScoreResolution;

/**
 * A completed result is coherent when both non-negative integer scores agree
 * with the reported winner. This deliberately has no mode-specific score cap:
 * Siege, Onslaught, TDM, and future queues use different win conditions.
 */
export function isValidCompletedMatchScore(
  team1Value: unknown,
  team2Value: unknown,
  winnerValue: unknown,
): boolean {
  const team1 = Number(team1Value);
  const team2 = Number(team2Value);
  const winner = Number(winnerValue);
  return Number.isInteger(team1)
    && Number.isInteger(team2)
    && (winner === 1 || winner === 2)
    && team1 >= 0
    && team2 >= 0
    && (winner === 1 ? team1 > team2 : team2 > team1);
}

/**
 * Derive the winning task force from player result rows. Winner rows name the
 * winning team directly; loser rows imply that the opposing task force won.
 * A single row is not enough authority to rewrite match-level score metadata.
 */
export function resolvePlayerOutcomeConsensus(
  players: MatchPlayerOutcomeInput[],
): MatchPlayerOutcomeConsensus {
  let observations = 0;
  let winner: 1 | 2 | null = null;

  for (const player of players) {
    const status = String(player?.win_status || '').toLowerCase();
    const isWin = status === 'winner' || status === 'win';
    const isLoss = status === 'loser' || status === 'loss';
    if (!isWin && !isLoss) continue;

    observations++;
    const taskForce = Number(player?.task_force);
    if (taskForce !== 1 && taskForce !== 2) {
      return { coherent: false, observations, winner: null };
    }

    const observedWinner = (isWin ? taskForce : (taskForce === 1 ? 2 : 1)) as 1 | 2;
    if (winner !== null && observedWinner !== winner) {
      return { coherent: false, observations, winner: null };
    }
    winner = observedWinner;
  }

  return {
    coherent: true,
    observations,
    winner: observations >= 2 ? winner : null,
  };
}

/**
 * Hi-Rez has historically emitted some Siege score pairs in the opposite team
 * order from TaskForce/Win_Status. Swap only a strict reversal supported by
 * repeated unanimous player outcomes; never infer a score from combat stats.
 */
export function reconcileSiegeMatchScore(
  direct: MatchScoreInput,
  playerOutcomes: MatchPlayerOutcomeConsensus,
): MatchScoreResolution | null {
  if (!playerOutcomes.coherent) return null;

  const directScore = resolveCompletedMatchScore(direct);
  if (playerOutcomes.winner === null) return directScore;
  if (directScore?.winner === playerOutcomes.winner) return directScore;

  const team1 = Number(direct.team1);
  const team2 = Number(direct.team2);
  if (!Number.isInteger(team1) || !Number.isInteger(team2) || team1 < 0 || team2 < 0 || team1 === team2) {
    return null;
  }

  const swapped = {
    team1: team2,
    team2: team1,
    winner: playerOutcomes.winner,
  };
  if (!isValidCompletedMatchScore(swapped.team1, swapped.team2, swapped.winner)) return null;

  return {
    ...swapped,
    canonicalized: true,
    source: 'direct',
  };
}

/** Prefer an exact coherent detail result over every recovery source. */
export function resolveCompletedMatchScore(
  direct: MatchScoreInput,
): MatchScoreResolution | null {
  if (!isValidCompletedMatchScore(direct.team1, direct.team2, direct.winner)) return null;
  return {
    team1: Number(direct.team1),
    team2: Number(direct.team2),
    winner: Number(direct.winner) as 1 | 2,
    canonicalized: false,
    source: 'direct',
  };
}

function resolveObservedMatchScore(
  observations: MatchScoreInput[],
  source: 'direct' | 'history',
): MatchScoreResolution | null {
  const scoreBearing = observations.filter(observation => (
    [observation.team1, observation.team2, observation.winner]
      .some(value => value !== undefined && value !== null && value !== '')
  ));
  // A broken response must have more than one independent row agreeing on the
  // result. One surviving row can itself be the poisoned/partial boundary and
  // is not enough authority to complete a recovered match.
  if (scoreBearing.length < 2) return null;

  let resolved: MatchScoreResolution | null = null;
  for (const observation of scoreBearing) {
    if (!isValidCompletedMatchScore(observation.team1, observation.team2, observation.winner)) return null;
    const candidate: MatchScoreResolution = {
      team1: Number(observation.team1),
      team2: Number(observation.team2),
      winner: Number(observation.winner) as 1 | 2,
      canonicalized: false,
      source,
    };
    if (resolved && (
      candidate.team1 !== resolved.team1
      || candidate.team2 !== resolved.team2
      || candidate.winner !== resolved.winner
    )) return null;
    resolved = candidate;
  }
  return resolved;
}

export function resolveDirectMatchScore(
  observations: MatchScoreInput[],
): MatchScoreResolution | null {
  return resolveObservedMatchScore(observations, 'direct');
}

/**
 * History is usable only when all score-bearing target-match observations
 * report the same coherent result. It is a recovery source, not a queue gate.
 */
export function resolveHistoryMatchScore(
  observations: MatchScoreInput[],
): MatchScoreResolution | null {
  return resolveObservedMatchScore(observations, 'history');
}

export function resolveRecoveredMatchScoreSources(
  direct: MatchScoreInput[],
  history: MatchScoreInput[],
): RecoveredMatchScoreResolution | null {
  return resolveDirectMatchScore(direct)
    ?? resolveHistoryMatchScore(history);
}
