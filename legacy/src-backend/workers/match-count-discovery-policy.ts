import {
  getMatchParticipantModel,
  type MatchParticipantModel,
} from '../services/match-participant-policy';

export const MATCH_STAT_SCOPES = [
  'ranked',
  'casual',
  'bot',
  'team_deathmatch',
  'arcade',
  'wave_defense',
  'experiment',
  'newcomer',
  'custom',
  'other',
] as const;

export type MatchStatScope = typeof MATCH_STAT_SCOPES[number];
export type { MatchParticipantModel } from '../services/match-participant-policy';

export type MatchCountQueueDefinition = {
  queueId: number;
  name: string;
  ranked: boolean;
  scope: MatchStatScope;
  participantModel: MatchParticipantModel;
  statsEnabled: boolean;
  trackPresence: boolean;
};

/**
 * Only queues that returned match IDs in production belong in scheduled
 * discovery. Queue 486 is listed for activity-page metadata, but ranked
 * discovery owns its API call.
 */
export const MATCH_COUNT_QUEUE_DEFINITIONS: MatchCountQueueDefinition[] = [
  { queueId: 424, name: 'Casual Siege', ranked: false, scope: 'casual', participantModel: getMatchParticipantModel(424), statsEnabled: true, trackPresence: true },
  { queueId: 425, name: 'Siege Training', ranked: false, scope: 'bot', participantModel: getMatchParticipantModel(425), statsEnabled: true, trackPresence: true },
  { queueId: 452, name: 'Casual Onslaught', ranked: false, scope: 'casual', participantModel: getMatchParticipantModel(452), statsEnabled: true, trackPresence: true },
  { queueId: 453, name: 'Onslaught Training', ranked: false, scope: 'bot', participantModel: getMatchParticipantModel(453), statsEnabled: true, trackPresence: true },
  { queueId: 469, name: 'Team Deathmatch', ranked: false, scope: 'team_deathmatch', participantModel: getMatchParticipantModel(469), statsEnabled: true, trackPresence: true },
  { queueId: 486, name: 'Ranked Siege', ranked: true, scope: 'ranked', participantModel: getMatchParticipantModel(486), statsEnabled: true, trackPresence: true },
  { queueId: 10297, name: 'Team Deathmatch Training', ranked: false, scope: 'bot', participantModel: getMatchParticipantModel(10297), statsEnabled: true, trackPresence: true },
  { queueId: 10332, name: 'Arcade', ranked: false, scope: 'arcade', participantModel: getMatchParticipantModel(10332), statsEnabled: true, trackPresence: true },
  { queueId: 10348, name: 'Wave Defense Party Beta', ranked: false, scope: 'wave_defense', participantModel: getMatchParticipantModel(10348), statsEnabled: true, trackPresence: true },
  { queueId: 10362, name: 'Wave Defense Public Beta', ranked: false, scope: 'wave_defense', participantModel: getMatchParticipantModel(10362), statsEnabled: true, trackPresence: true },
  { queueId: 10367, name: 'Newcomer', ranked: false, scope: 'newcomer', participantModel: getMatchParticipantModel(10367), statsEnabled: true, trackPresence: true },
  { queueId: 10369, name: 'Experiment: Subclasses', ranked: false, scope: 'experiment', participantModel: getMatchParticipantModel(10369), statsEnabled: true, trackPresence: true },
];

const QUEUE_DEFINITION_BY_ID = new Map(MATCH_COUNT_QUEUE_DEFINITIONS.map(definition => [definition.queueId, definition]));

export function getMatchQueueDefinition(queueId: number): MatchCountQueueDefinition {
  return QUEUE_DEFINITION_BY_ID.get(queueId) ?? {
    queueId,
    name: queueId > 0 ? `Queue ${queueId}` : 'Unknown',
    ranked: false,
    scope: 'other',
    participantModel: 'unknown',
    statsEnabled: false,
    trackPresence: false,
  };
}

export function isPublicStatsScope(value: unknown): value is MatchStatScope {
  return typeof value === 'string'
    && MATCH_STAT_SCOPES.includes(value as MatchStatScope)
    && value !== 'custom'
    && value !== 'other';
}
