export const PLAYER_PROFILE_BATCH_MIN_SIZE = 10;
export const PLAYER_PROFILE_BATCH_MAX_SIZE = 20;

export function uniquePositivePlayerIds(values: Iterable<unknown>): number[] {
  const ids = new Set<number>();
  for (const value of values) {
    const id = Number(value);
    if (Number.isFinite(id) && id > 0) ids.add(id);
  }
  return [...ids];
}

/**
 * Remove one outbound-ready profile batch from a shared pending-ID set.
 *
 * A partial tail remains queued until another match contributes enough IDs.
 * This is the quota guard that prevents background ingest from turning a
 * batch endpoint into one request per stale player.
 */
export function takeReadyPlayerProfileBatch(
  pendingIds: Set<number>,
  maxSize = PLAYER_PROFILE_BATCH_MAX_SIZE,
  minSize = PLAYER_PROFILE_BATCH_MIN_SIZE,
): number[] {
  const boundedMax = Math.max(minSize, Math.min(PLAYER_PROFILE_BATCH_MAX_SIZE, Math.floor(maxSize)));
  if (pendingIds.size < minSize) return [];

  const batch = [...pendingIds].slice(0, boundedMax);
  for (const playerId of batch) pendingIds.delete(playerId);
  return batch;
}
