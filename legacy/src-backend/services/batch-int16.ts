/**
 * getmatchdetailsbatch is an ordered stream. A broken-skin Int16 sentinel may
 * follow a healthy prefix, so this one response shape must remain available to
 * discovery instead of becoming a request-wide exception.
 */
export function shouldPreserveBrokenSkinBatchResponse(method: string, retMsgs: string[]): boolean {
  return method.toLowerCase() === 'getmatchdetailsbatch'
    && retMsgs.length > 0
    && retMsgs.every((message) => /int16/i.test(message) && /skin[ _]?id|skinid/i.test(message));
}

/**
 * A direct match with fewer than ten usable rows is the proven ordered-stream
 * blocker. It is staged for targeted buffer recovery; later unseen batch IDs
 * are refetched as a healthy batch instead of recovered one by one.
 */
export function isIncompleteDirectMatch(match: any): boolean {
  const players = Array.isArray(match?.players) ? match.players : [];
  const usablePlayers = players.filter((player: any) => (
    !player?.has_ret_msg && !String(player?.ret_msg || '').trim()
  ));
  return usablePlayers.length < 10;
}

export function matchPayloadRequiresRecovery(rawData: unknown): boolean {
  if (!Array.isArray(rawData)) return false;
  const hasReturnSentinel = rawData.some((player: any) => String(player?.ret_msg || '').trim());
  const usablePlayers = rawData.filter((player: any) => !String(player?.ret_msg || '').trim());
  return hasReturnSentinel || usablePlayers.length < 10;
}
