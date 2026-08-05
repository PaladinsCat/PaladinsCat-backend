/**
 * Paladins KDA: kills plus half an assist, divided by deaths.
 * A deathless match uses 1 as the denominator so the UI shows a finite score.
 */
export function calculateKda(kills: number, deaths: number, assists: number): number {
  const numerator = Number(kills || 0) + Number(assists || 0) / 2;
  return numerator / Math.max(Number(deaths || 0), 1);
}
