export const MATCH_BAN_SLOT_COUNT = 8;

export type MatchBanFields = Record<`ban_id_${number}`, number>;

function flattenBanSources(sources: unknown[]): any[] {
  return sources.flatMap(source => Array.isArray(source) ? source : [source])
    .filter(source => source && typeof source === 'object');
}

/**
 * Normalize the three ban field shapes used across Hi-Rez and local payloads.
 *
 * `getdemodetails` exposes BanId1..BanId8, raw match rows may expose Ban_1..Ban_8,
 * and normalized player rows use ban_id_1..ban_id_8. The first positive value
 * for each slot wins, allowing authoritative direct metadata to precede a demo
 * fallback while ignoring the zero placeholders on getmatchhistory rows.
 */
export function extractMatchBanFields(...sources: unknown[]): MatchBanFields {
  const candidates = flattenBanSources(sources);
  const fields = {} as MatchBanFields;

  for (let slot = 1; slot <= MATCH_BAN_SLOT_COUNT; slot++) {
    let championId = 0;
    for (const source of candidates) {
      const rawValues = [source[`ban_id_${slot}`], source[`BanId${slot}`], source[`Ban_${slot}`]];
      for (const rawValue of rawValues) {
        const parsed = Number(rawValue || 0);
        if (Number.isFinite(parsed) && parsed > 0) {
          championId = Math.trunc(parsed);
          break;
        }
      }
      if (championId > 0) break;
    }
    fields[`ban_id_${slot}`] = championId;
  }

  return fields;
}

export function matchBanEntries(...sources: unknown[]): Array<{ banSlot: number; championId: number }> {
  const fields = extractMatchBanFields(...sources);
  const entries: Array<{ banSlot: number; championId: number }> = [];
  for (let slot = 1; slot <= MATCH_BAN_SLOT_COUNT; slot++) {
    const championId = fields[`ban_id_${slot}`];
    if (championId > 0) entries.push({ banSlot: slot, championId });
  }
  return entries;
}

/**
 * Detect a complete synthetic roster already produced by broken-match
 * recovery. Hi-Rez's direct payload has no local source='recovered' marker;
 * only our recovery adapters emit it. Requiring ten rows and no ret_msg keeps
 * partial/direct payloads on the normal recovery path.
 */
export function isStagedRecoveryRoster(players: any[], hasExplicitApiReturn: boolean): boolean {
  return players.length === 10
    && !hasExplicitApiReturn
    && players.some(player => String(player?.source || '').toLowerCase() === 'recovered');
}
