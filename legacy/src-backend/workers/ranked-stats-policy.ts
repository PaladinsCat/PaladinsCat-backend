export const RANKED_STATS_QUEUE_ID = 486;

/**
 * Queue 486 is the only source of PaladinsCat aggregate performance metrics.
 * Other queues may be persisted as match facts for lookup, but must not mutate
 * player, champion, opponent, composition, loadout, or rating projections.
 */
export function isRankedStatsQueue(queueId: unknown): boolean {
  return Number(queueId) === RANKED_STATS_QUEUE_ID;
}
