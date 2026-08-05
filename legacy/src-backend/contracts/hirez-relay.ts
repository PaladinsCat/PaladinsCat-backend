/** Shared backend-to-relay wire contract. The relay runtime is implemented in Rust. */
export interface MatchDetails {
  match_id: number;
  entry_datetime: string;
  map: string;
  queue_id: number;
  duration_seconds: number;
  minutes: number;
  region: string;
  team1_score: number | null;
  team2_score: number | null;
  winning_task_force: number | null;
  direct_score_observations?: Array<{ team1?: unknown; team2?: unknown; winner?: unknown }>;
  has_replay: boolean | null;
  ban_id_1?: number;
  ban_id_2?: number;
  ban_id_3?: number;
  ban_id_4?: number;
  ban_id_5?: number;
  ban_id_6?: number;
  ban_id_7?: number;
  ban_id_8?: number;
  recovery_source?: string;
  recovery_api_calls?: number;
  /**
   * Set only by the canonical relay match-lookup operation after it has
   * completed the one permitted recovery pass for this match. Backend workers
   * must treat this as a terminal relay decision and must never call recovery
   * endpoints themselves.
   */
  recovery_attempted?: boolean;
  recovery_terminal?: boolean;
  recovery_pending?: boolean;
  limited?: boolean;
  players: PlayerDetails[];
}

export interface MatchIdObservation {
  matchId: number;
  entryDatetime: string | null;
  region: string;
  activeFlag: boolean;
}

export interface CompletedMatchRequest {
  matchId: number;
  /**
   * Discovery already knows the queue even when Hi-Rez omits the match from a
   * detail batch. The relay treats this as a hint and still prefers queue
   * metadata recovered from the match itself.
   */
  queueId?: number;
}

export type CompletedMatchResolutionStatus =
  | 'complete_direct'
  | 'complete_recovered'
  | 'recovery_pending'
  | 'limited'
  | 'roster_only'
  | 'dropped';

/**
 * Canonical completed-match batch outcome.
 *
 * The relay returns outcomes only for matches identified by the direct batch
 * response, plus an explicitly recovered/terminal outcome for a singleton
 * request. An ID omitted from a multi-match response remains absent so the
 * backend worker can isolate that ordered blocker and refill its continuous
 * batch through this same operation.
 */
export interface CompletedMatchResolution {
  matchId: number;
  queueId: number;
  status: CompletedMatchResolutionStatus;
  match?: MatchDetails;
  roster?: any[];
  reason?: string;
}

export interface PlayerDetails {
  player_id: number;
  player_name: string;
  match_id: number;
  entry_datetime: string;
  queue_id: number;
  champion_id: number;
  champion_name?: string;
  skin_id: number;
  skin_name: string;
  kills: number;
  deaths: number;
  assists: number;
  damage_done_in_hand: number;
  damage_done_physical: number;
  damage_done_magical: number;
  damage_taken: number;
  damage_mitigated: number;
  healing: number;
  healing_self: number;
  gold_earned: number;
  gold_per_minute: number;
  objective_assists: number;
  killing_spree: number;
  multi_kill_max: number;
  win_status: string;
  task_force: number;
  league_tier: number;
  league_points: number;
  account_level: number;
  mastery_level: number;
  party_id: number;
  time_in_match: number;
  distance_traveled: number;
  structure_damage: number;
  camps_cleared: number;
  source: string;
  portal_id: number;
  portal_user_id: string;
  kills_player: number;
  region?: string;
  healing_player_self: number;
  damage_taken_physical: number;
  damage_taken_magical: number;
  kills_fire_giant: number;
  kills_gold_fury: number;
  kills_phoenix: number;
  kills_siege_jugg: number;
  kills_wild_jugg: number;
  kills_bot: number;
  kills_single: number;
  kills_double: number;
  kills_triple: number;
  kills_quadra: number;
  kills_penta: number;
  kills_first_blood: number;
  wards_placed: number;
  towers_destroyed: number;
  league_wins: number;
  league_losses: number;
  healing_bot: number;
  damage_bot: number;
  platform: string;
  surrendered: number;
  team_id: number;
  team_name: string;
  rank_stat_league: number;
  final_match_level: number;
  match_duration: number;
  active_id_1: number;
  active_id_2: number;
  active_id_3: number;
  active_id_4: number;
  active_level_1: number;
  active_level_2: number;
  active_level_3: number;
  active_level_4: number;
  item_active_1: string;
  item_active_2: string;
  item_active_3: string;
  item_active_4: string;
  item_id_1: number;
  item_id_2: number;
  item_id_3: number;
  item_id_4: number;
  item_id_5: number;
  item_id_6: number;
  item_level_1: number;
  item_level_2: number;
  item_level_3: number;
  item_level_4: number;
  item_level_5: number;
  item_level_6: number;
  item_purch_1: string;
  item_purch_2: string;
  item_purch_3: string;
  item_purch_4: string;
  item_purch_5: string;
  item_purch_6: string;
  ban_id_1: number;
  ban_id_2: number;
  ban_id_3: number;
  ban_id_4: number;
  ban_id_5: number;
  ban_id_6: number;
  ban_id_7: number;
  ban_id_8: number;
  merged_players: { player_id: number; portal_id: number | null; merge_datetime: string }[] | null;
  has_ret_msg: boolean;
  ret_msg?: string | null;
  [key: string]: any;
}

export interface RawPayload {
  endpoint: string;
  entity_type: string;
  entity_id?: number | string;
  raw_data: any[];
  source?: string;
}

export interface RelayCallRequest {
  operation: string;
  args?: any[];
  requestId?: string;
  attribution?: RelayCallAttribution;
}

export interface RelayCallAttribution {
  consumer: string;
  reason?: string;
}

export interface RelayCallResponse<T = unknown> {
  ok: boolean;
  mode: 'dummy' | 'real';
  operation: string;
  requestId: string;
  latencyMs: number;
  result?: T;
  error?: string;
  errorCode?: string;
}
