const TIERS = new Set(['S', 'A', 'B', 'C', 'D', 'F']);

export type TierListEntryInput = {
  championId: number;
  tier: string;
  position: number;
};

export function parseTierListEntries(value: unknown): TierListEntryInput[] | null {
  if (!Array.isArray(value)) return null;
  const entries = value.map((entry): TierListEntryInput | null => {
    if (!entry || typeof entry !== 'object') return null;
    const candidate = entry as Record<string, unknown>;
    const championId = Number(candidate.championId);
    const tier = String(candidate.tier ?? '').toUpperCase();
    const position = Number(candidate.position);
    if (!Number.isInteger(championId) || championId <= 0 || !TIERS.has(tier) || !Number.isInteger(position) || position < 0) return null;
    return { championId, tier, position };
  });
  return entries.every((entry): entry is TierListEntryInput => entry != null) ? entries : null;
}

export function validateTierListEntries(
  entries: TierListEntryInput[],
  championIds: ReadonlySet<number>,
): string | null {
  if (entries.length === 0) return 'Place at least one champion in the tier list';
  const submittedIds = new Set(entries.map((entry) => entry.championId));
  if (submittedIds.size !== entries.length) return 'Each champion can appear only once';
  const positions = new Set(entries.map((entry) => `${entry.tier}:${entry.position}`));
  if (positions.size !== entries.length) return 'Each tier position can contain only one champion';
  if ([...submittedIds].some((id) => !championIds.has(id))) return 'Tier list contains an unknown champion';
  return null;
}
