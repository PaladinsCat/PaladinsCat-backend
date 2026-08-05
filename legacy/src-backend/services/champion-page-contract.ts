function numberValue(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function nullableNumberValue(value: unknown): number | null {
  if (value == null || value === '') return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

/**
 * PostgreSQL returns BIGINT and NUMERIC aggregate columns as strings. Champion
 * page bundles are public JSON contracts, so normalize nested stats before
 * caching or forwarding them to clients.
 */
export function normalizeChampionTalentStatsPayload(raw: any) {
  return {
    totalMatches: numberValue(raw?.totalMatches),
    talentCoveredMatches: numberValue(raw?.talentCoveredMatches),
    disconnectedPlayers: numberValue(raw?.disconnectedPlayers),
    disconnectedWins: numberValue(raw?.disconnectedWins),
    disconnectedLosses: numberValue(raw?.disconnectedLosses),
    disconnectedWinRate: nullableNumberValue(raw?.disconnectedWinRate),
    talentCoverageRate: nullableNumberValue(raw?.talentCoverageRate),
    talents: Array.isArray(raw?.talents) ? raw.talents.map((talent: any) => ({
      talentId: numberValue(talent?.talentId),
      talentName: String(talent?.talentName ?? 'Unknown'),
      totalPlays: numberValue(talent?.totalPlays),
      wins: numberValue(talent?.wins),
      losses: numberValue(talent?.losses),
      winRate: numberValue(talent?.winRate),
    })) : [],
  };
}

export function normalizeChampionCardStatsPayload(raw: any) {
  return {
    totalMatches: numberValue(raw?.totalMatches),
    talentId: raw?.talentId == null ? null : numberValue(raw.talentId),
    cards: Array.isArray(raw?.cards) ? raw.cards.map((card: any) => ({
      cardId: numberValue(card?.cardId),
      cardName: String(card?.cardName ?? 'Unknown'),
      totalPlays: numberValue(card?.totalPlays),
      wins: numberValue(card?.wins),
      losses: numberValue(card?.losses),
      winRate: numberValue(card?.winRate),
      levels: Array.isArray(card?.levels) ? card.levels.map((level: any) => ({
        level: numberValue(level?.level),
        plays: numberValue(level?.plays),
        wins: numberValue(level?.wins),
        losses: numberValue(level?.losses),
        winRate: numberValue(level?.winRate),
      })) : [],
    })) : [],
  };
}
