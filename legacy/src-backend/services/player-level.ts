/**
 * Convert the total XP returned by Hi-Rez into the player's theoretical
 * account level. Hi-Rez caps its `Level` response at 999, while Total_XP keeps
 * increasing. This mirrors aRez's calculated_level behavior.
 */
export const PLAYER_LEVEL_50_XP = 25_480_000;
export const PLAYER_LEVEL_AFTER_50_XP = 1_000_000;

export function calculatePlayerLevel(totalXp: unknown): number | null {
  const parsed = Number(totalXp);
  if (!Number.isFinite(parsed) || parsed < 0) return null;

  const experience = Math.floor(parsed);
  if (experience >= PLAYER_LEVEL_50_XP) {
    return Math.floor((experience - PLAYER_LEVEL_50_XP) / PLAYER_LEVEL_AFTER_50_XP) + 50;
  }

  // Level 1 starts at zero XP. Reaching levels 2 through 50 costs an
  // additional 20,000 XP per level: 40k for level 2, 60k for level 3, etc.
  let threshold = 0;
  for (let level = 2; level <= 50; level += 1) {
    threshold += level * 20_000;
    if (threshold > experience) return level - 1;
  }

  return 50;
}

export function resolvePlayerLevel(totalXp: unknown, apiLevel: unknown): number {
  const calculated = calculatePlayerLevel(totalXp);
  if (calculated != null) return calculated;

  const parsedApiLevel = Number(apiLevel);
  return Number.isFinite(parsedApiLevel) && parsedApiLevel > 0 ? Math.floor(parsedApiLevel) : 0;
}
