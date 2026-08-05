export const PRIVATE_IDENTITY_VERSION = 3;
export const PRIVATE_IDENTITY_LINK_THRESHOLD = 68;
export const PRIVATE_IDENTITY_MARGIN = 12;

export interface PrivateAccountObservation {
  matchId: number;
  privateSlot: number;
  entryDatetime: string;
  partyId: number;
  accountLevel: number;
  masteryLevel: number;
  leagueTier: number;
  leaguePoints: number;
  championId: number;
  taskForce: number;
  winStatus: string;
  portalId: number;
  portalUserId: string;
  platform: string;
  source: string;
  partyMemberIds: number[];
  queueId?: number;
  statsScope?: string;
  map?: string;
  matchEndDatetime?: string;
  observationQuality?: string;
}

export interface IdentityScore {
  score: number;
  reasons: string[];
  hardConflict: boolean;
}

function finiteInt(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function cleanString(value: unknown): string {
  return String(value ?? '').trim();
}

function toIso(value: string | Date): string {
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? new Date(0).toISOString() : date.toISOString();
}

export function privateObservationFromPlayer(
  matchId: number,
  privateSlot: number,
  player: any,
  entryDatetime: string,
): PrivateAccountObservation {
  return {
    matchId,
    privateSlot,
    entryDatetime: toIso(entryDatetime),
    partyId: finiteInt(player?.party_id),
    accountLevel: finiteInt(player?.account_level),
    masteryLevel: finiteInt(player?.mastery_level),
    leagueTier: finiteInt(player?.league_tier),
    leaguePoints: finiteInt(player?.league_points),
    championId: finiteInt(player?.champion_id),
    taskForce: finiteInt(player?.task_force),
    winStatus: cleanString(player?.win_status).toLowerCase(),
    portalId: finiteInt(player?.portal_id),
    portalUserId: cleanString(player?.portal_user_id),
    platform: cleanString(player?.platform).toLowerCase(),
    source: cleanString(player?.source).toLowerCase() || 'direct',
    partyMemberIds: [],
    queueId: finiteInt(player?.queue_id),
    statsScope: cleanString(player?.stats_scope).toLowerCase(),
    map: cleanString(player?.map),
    matchEndDatetime: undefined,
    observationQuality: cleanString(player?.observation_quality).toLowerCase() || 'complete',
  };
}

export function hasPrivateIdentityEvidence(observation: PrivateAccountObservation): boolean {
  return observation.accountLevel > 0
    || observation.masteryLevel > 0
    || observation.leagueTier > 0
    || observation.leaguePoints > 0
    || observation.partyId > 0
    || observation.championId > 0
    || observation.portalId > 0
    || observation.portalUserId.length > 0
    || observation.partyMemberIds.length > 0;
}

function elapsedHours(left: PrivateAccountObservation, right: PrivateAccountObservation): number {
  return Math.abs(new Date(left.entryDatetime).getTime() - new Date(right.entryDatetime).getTime()) / 3_600_000;
}

function overlapCount(left: number[], right: number[]): number {
  const rightSet = new Set(right);
  return left.reduce((count, value) => count + (rightSet.has(value) ? 1 : 0), 0);
}

function normalizedOutcome(value: string): 'win' | 'loss' | null {
  const normalized = cleanString(value).toLowerCase();
  if (normalized === 'winner' || normalized === 'win') return 'win';
  if (normalized === 'loser' || normalized === 'loss') return 'loss';
  return null;
}

/**
 * Score two private observations without treating PartyId as a person key.
 * Strong links require several independent signals and chronological sanity.
 */
export function scorePrivateIdentity(
  incoming: PrivateAccountObservation,
  existing: PrivateAccountObservation,
): IdentityScore {
  const reasons: string[] = [];
  let score = 0;
  let hardConflict = false;
  const incomingTime = new Date(incoming.entryDatetime).getTime();
  const existingTime = new Date(existing.entryDatetime).getTime();
  const later = incomingTime >= existingTime ? incoming : existing;
  const earlier = incomingTime >= existingTime ? existing : incoming;
  const hours = elapsedHours(incoming, existing);

  if (incoming.matchId === existing.matchId) {
    return { score: 0, reasons: ['same_match_conflict'], hardConflict: true };
  }

  if (incoming.portalUserId && existing.portalUserId) {
    if (incoming.portalUserId !== existing.portalUserId) {
      reasons.push('portal_user_conflict');
      hardConflict = true;
    } else if (!incoming.portalId || !existing.portalId || incoming.portalId === existing.portalId) {
      score += 100;
      reasons.push('portal_user_exact');
    }
  }
  if (incoming.portalId > 0 && existing.portalId > 0 && incoming.portalId !== existing.portalId) {
    reasons.push('portal_conflict');
    hardConflict = true;
  }

  if (earlier.accountLevel > 0 && later.accountLevel > 0) {
    const regression = earlier.accountLevel - later.accountLevel;
    if (regression > 1) {
      reasons.push('account_level_regression');
      hardConflict = true;
    } else if (incoming.accountLevel === existing.accountLevel) {
      score += incoming.accountLevel >= 999 ? 8 : 18;
      reasons.push(incoming.accountLevel >= 999 ? 'account_level_cap_exact' : 'account_level_exact');
    } else if (later.accountLevel >= earlier.accountLevel && later.accountLevel - earlier.accountLevel <= 2) {
      score += 12;
      reasons.push('account_level_progression');
    }
  }

  const sameChampion = incoming.championId > 0 && incoming.championId === existing.championId;
  if (sameChampion) {
    score += 3;
    reasons.push('champion_exact');
    if (earlier.masteryLevel > 0 && later.masteryLevel > 0) {
      const regression = earlier.masteryLevel - later.masteryLevel;
      if (regression > 2) {
        reasons.push('mastery_regression');
        hardConflict = true;
      } else if (incoming.masteryLevel === existing.masteryLevel) {
        score += 14;
        reasons.push('mastery_exact');
      } else if (later.masteryLevel >= earlier.masteryLevel && later.masteryLevel - earlier.masteryLevel <= 2) {
        score += 8;
        reasons.push('mastery_progression');
      }
    }
  }

  if (incoming.leagueTier > 0 && incoming.leagueTier === existing.leagueTier) {
    score += 10;
    reasons.push('league_tier_exact');
  }
  let tpProgressionCompatible = false;
  if (
    incoming.leagueTier > 0
    && incoming.leaguePoints >= 0
    && existing.leaguePoints >= 0
    && incoming.leagueTier === existing.leagueTier
  ) {
    const pointDifference = Math.abs(incoming.leaguePoints - existing.leaguePoints);
    const pointDelta = later.leaguePoints - earlier.leaguePoints;
    const earlierOutcome = normalizedOutcome(earlier.winStatus);
    const directionMatches = earlierOutcome === 'win' ? pointDelta >= 0 : earlierOutcome === 'loss' ? pointDelta <= 0 : false;
    if (pointDifference === 0) {
      score += 5;
      tpProgressionCompatible = true;
      reasons.push('tp_stable');
    } else if (directionMatches && pointDifference <= 25) {
      score += 10;
      tpProgressionCompatible = true;
      reasons.push(`tp_${earlierOutcome}_progression`);
    } else if (directionMatches && pointDifference <= 50) {
      score += 7;
      tpProgressionCompatible = true;
      reasons.push(`tp_${earlierOutcome}_extended_progression`);
    } else if (directionMatches) {
      score += 4;
      tpProgressionCompatible = true;
      reasons.push(`tp_${earlierOutcome}_large_progression`);
    } else if (!earlierOutcome && pointDifference <= 25) {
      score += 3;
      tpProgressionCompatible = true;
      reasons.push('tp_near_without_outcome');
    } else {
      // TP is a changing match observation, never a hard identity conflict.
      // Missing games, promotions, and delayed API snapshots can all make the
      // visible delta larger or opposite to the immediately observed result.
      reasons.push('tp_progression_uncertain');
    }
  }
  if (incoming.platform && incoming.platform === existing.platform) {
    score += 4;
    reasons.push('platform_exact');
  }

  const companions = overlapCount(incoming.partyMemberIds, existing.partyMemberIds);
  if (companions > 0) {
    score += 50 + Math.min(12, (companions - 1) * 6);
    reasons.push(`party_companion_overlap:${companions}`);
  }

  if (incoming.partyId > 0 && incoming.partyId === existing.partyId) {
    if (hours <= 12) {
      score += 20;
      reasons.push('party_session_exact');
      if (
        tpProgressionCompatible
        && incoming.accountLevel > 0
        && incoming.accountLevel === existing.accountLevel
        && incoming.leagueTier > 0
        && incoming.leagueTier === existing.leagueTier
      ) {
        score += 8;
        reasons.push('ranked_session_progression');
      }
    } else {
      score += 3;
      reasons.push('party_id_context_only');
    }
  }
  if (hours <= 12) {
    score += 5;
    reasons.push('time_within_12h');
  } else if (hours <= 24 * 7) {
    score += 2;
    reasons.push('time_within_7d');
  }

  // Casual-only private accounts do not have ranked tier/TP anchors. A
  // short-window account + same-champion mastery + platform trajectory is the
  // strongest safe replacement. Level-cap rows are excluded because level 999
  // is common and no longer progresses.
  if (
    hours <= 12
    && incoming.accountLevel > 0
    && incoming.accountLevel < 999
    && incoming.accountLevel === existing.accountLevel
    && sameChampion
    && incoming.masteryLevel > 0
    && incoming.masteryLevel === existing.masteryLevel
    && incoming.platform
    && incoming.platform === existing.platform
    && (!incoming.statsScope || incoming.statsScope !== 'ranked')
    && (!existing.statsScope || existing.statsScope !== 'ranked')
  ) {
    score += 25;
    reasons.push('casual_progression_bundle');
  }

  return { score: hardConflict ? 0 : Math.min(100, score), reasons, hardConflict };
}
