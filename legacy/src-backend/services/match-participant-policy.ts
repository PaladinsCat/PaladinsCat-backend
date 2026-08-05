export type MatchParticipantModel = 'pvp' | 'pve' | 'bots' | 'custom' | 'unknown';

const PARTICIPANT_MODEL_BY_QUEUE = new Map<number, MatchParticipantModel>([
  [424, 'pvp'],
  [425, 'bots'],
  [452, 'pvp'],
  [453, 'bots'],
  [469, 'pvp'],
  [486, 'pvp'],
  [10297, 'bots'],
  [10332, 'pvp'],
  [10348, 'pve'],
  [10362, 'pve'],
  [10367, 'pvp'],
  [10369, 'pvp'],
]);

export function getMatchParticipantModel(queueId: number): MatchParticipantModel {
  return PARTICIPANT_MODEL_BY_QUEUE.get(Number(queueId)) ?? 'unknown';
}

/**
 * Hi-Rez omits AI participants from bot/PvE completed-match responses. Those
 * queues are complete with one or more usable human rows; PvP remains a
 * ten-player authority boundary.
 */
export function isVariableHumanRosterQueue(queueId: number): boolean {
  const model = getMatchParticipantModel(queueId);
  return model === 'bots' || model === 'pve';
}
