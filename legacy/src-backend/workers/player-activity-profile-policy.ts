export const ACTIVITY_PROFILE_TTL_HOURS = 24;
export const ACTIVITY_PROFILE_BATCH_SIZE = 20;

function positiveId(value: unknown): number | null {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
}

export function uniquePlayerIds(values: unknown[]): number[] {
  const unique = new Set<number>();
  for (const value of values) {
    const id = positiveId(value);
    if (id != null) unique.add(id);
  }
  return [...unique];
}

export function chunkActivityProfileIds(
  values: unknown[],
  batchSize = ACTIVITY_PROFILE_BATCH_SIZE,
): number[][] {
  const size = Math.max(1, Math.min(ACTIVITY_PROFILE_BATCH_SIZE, Math.floor(batchSize)));
  const ids = uniquePlayerIds(values);
  const batches: number[][] = [];
  for (let offset = 0; offset < ids.length; offset += size) {
    batches.push(ids.slice(offset, offset + size));
  }
  return batches;
}

/**
 * A merged account response may identify the requested row through either Id
 * or ActivePlayerId. Treat both as satisfied so the negative cache does not
 * immediately re-request an alias that Hi-Rez already resolved.
 */
export function requestedIdsSatisfiedByProfiles(
  requestedIds: number[],
  rawProfiles: any[],
): Set<number> {
  const requested = new Set(uniquePlayerIds(requestedIds));
  const satisfied = new Set<number>();
  for (const raw of rawProfiles) {
    if (String(raw?.ret_msg ?? '').trim()) continue;
    for (const candidate of uniquePlayerIds([
      raw?.Id,
      raw?.id,
      raw?.player_id,
      raw?.ActivePlayerId,
      raw?.active_player_id,
    ])) {
      if (requested.has(candidate)) satisfied.add(candidate);
    }
  }
  return satisfied;
}
