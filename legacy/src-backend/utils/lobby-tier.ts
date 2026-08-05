export type LobbyTierBounds = {
  active: boolean;
  min: number | null;
  max: number | null;
};

export function parseLobbyTierBounds(query: Record<string, unknown> | undefined): LobbyTierBounds | null {
  const rawMin = query?.tierMin;
  const rawMax = query?.tierMax;
  const min = rawMin == null || rawMin === '' ? null : Number(rawMin);
  const max = rawMax == null || rawMax === '' ? null : Number(rawMax);
  if ((min != null && (!Number.isInteger(min) || min < 1 || min > 26))
    || (max != null && (!Number.isInteger(max) || max < 1 || max > 26))
    || (min != null && max != null && min > max)) {
    return null;
  }
  return { active: min != null || max != null, min, max };
}

export function appendLobbyTierPredicate(
  bounds: LobbyTierBounds,
  params: unknown[],
  where: string[],
  alias = 'mlt',
): void {
  if (bounds.min != null) {
    params.push(bounds.min);
    where.push(`${alias}.lobby_tier >= $${params.length}`);
  }
  if (bounds.max != null) {
    params.push(bounds.max);
    where.push(`${alias}.lobby_tier <= $${params.length}`);
  }
}

export function lobbyTierQueryString(bounds: LobbyTierBounds): string {
  const params = new URLSearchParams();
  if (bounds.min != null) params.set('tierMin', String(bounds.min));
  if (bounds.max != null) params.set('tierMax', String(bounds.max));
  return params.toString();
}
