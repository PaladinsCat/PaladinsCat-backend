function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * Normal reads are database-only. This long TTL protects explicit/search and
 * Discord paths from repeating known Hi-Rez responses; the profile refresh
 * action may deliberately bypass it under its own rate limit.
 */
export const PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES = positiveInteger(
  process.env.PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES,
  24 * 60,
);
