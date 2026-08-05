export const HIREZ_LANGUAGE_ID = 1;

/**
 * Hi-Rez requires a language ID after the player ID for getplayerloadouts.
 * Omitting it produces an HTTP 404 rather than a structured API error.
 */
export function getPlayerLoadoutRequestParams(playerId: number): string[] {
  return [String(playerId), String(HIREZ_LANGUAGE_ID)];
}

/** Hi-Rez fixes the history window at 50; a limit path segment causes HTTP 404. */
export function getMatchHistoryRequestParams(playerId: number): string[] {
  return [String(playerId)];
}
