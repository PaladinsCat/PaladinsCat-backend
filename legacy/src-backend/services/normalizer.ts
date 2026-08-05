/**
 * =====================================================================
 * normalizer.ts — Hi-Rez API Response Normalizer
 * =====================================================================
 * Purpose: Transforms raw Hi-Rez API responses into consistent internal
 * types. Each Hi-Rez endpoint returns data in a different format (nested
 * objects, arrays, flat objects, mixed case). This module flattens and
 * normalizes them so downstream code (buffer-processor, MeiliSearch,
 * routes) works with a single predictable shape.
 *
 * Architecture:
 * - roundTo2(): Consistent rounding (ceil for <1, round for >=1).
 * - normalizeRegion(): Maps full region names → short codes (NA, EU, etc.).
 *   Handles both player profile (full names) and match detail (short codes).
 * - normalizeMatchDetails(): Flattens getmatchdetailsbatch responses.
 * - normalizePlayerDetails(): Flattens getplayer/getplayerbatch responses.
 * - normalizePlayerHistory(): Flattens getplayerhistory responses.
 *
 * Called by:
 * - workers/buffer-processor.ts — normalizes raw API data before DB insert.
 * - hirez.ts — normalizes responses before returning to callers.
 *
 * Fixed 2026-05-30:
 * - VALID_SHORT_CODES set added — short codes (NA, EU) pass through
 *   unchanged instead of mapping to 'Unknown' and triggering false recovery.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import type { PlayerDetails } from '../services/hirez';
import { resolvePlayerLevel } from './player-level';

// ─── Rounding Helper ─────────────────────────────────────────────────────────

/**
 * Round to 2 decimal places. Values < 1 use ceil (round up), values >= 1 use round.
 * This ensures 0.xx values are always rounded up (e.g., 0.004 → 0.01),
 * while larger values round normally (e.g., 313.227 → 313.23).
 * Guards against NaN and Infinity — returns 0 for non-finite inputs.
 */
export function roundTo2(value: number): number {
  // CRITICAL: Guard against NaN and Infinity. NaN propagates through all math
  // operations silently — NaN * 100 = NaN, Math.ceil(NaN) = NaN. The resulting
  // NaN value gets written to PostgreSQL as NULL (libpq converts NaN to NULL).
  // Infinity produces the string "Infinity" which causes VARCHAR overflow.
  // Source: Debug 2026-05-31 — "roundTo2 NaN/Infinity propagation"
  if (!Number.isFinite(value)) return 0;
  if (value === 0) return 0;
  return value < 1 ? Math.ceil(value * 100) / 100 : Math.round(value * 100) / 100;
}

// ─── Region Normalization ────────────────────────────────────────────────────
// Player profile endpoint returns full names ("North America"), match endpoints return codes ("NA").
// Normalize all to the match endpoint codes for consistency.
const REGION_MAP: Record<string, string> = {
  'North America': 'NA',
  'Europe': 'EU',
  'Brazil': 'BR',
  'Australia': 'OCE',
  'Southeast Asia': 'SEA',
  'Japan': 'JPN',
  'Russia': 'RUS',
};

// Short codes returned directly by match endpoints (getmatchdetailsbatch).
// These must pass through unchanged — they are NOT in REGION_MAP keys.
// Without this set, normalizeRegion('NA') → 'Unknown' → triggers recovery for every healthy match.
const VALID_SHORT_CODES = new Set(['NA', 'EU', 'BR', 'OCE', 'SEA', 'JPN', 'RUS', 'SA']);

/**
 * Normalize region name from Hi-Rez API to short code.
 *
 * Hi-Rez endpoints return region in two formats:
 *   - Player profile endpoints (getplayer, getplayerbatch): full names like "North America", "Europe"
 *   - Match detail endpoints (getmatchdetailsbatch): short codes like "NA", "EU" or null
 *
 * Returns 'Unknown' for:
 *   - Empty/null input (getmatchdetailsbatch p0.Region is null for private/platform-masked accounts)
 *   - Unmapped region strings (e.g., "Latin America North" — not in REGION_MAP)
 *
 * BUG FIX (2026-05-30):
 *   - Previously returned '' for empty input → PostgreSQL stored as NULL → lost region data
 *   - Previously returned raw string for unmapped input → long strings like "Latin America North"
 *     reached DB, causing VARCHAR overflow and silent data corruption
 *   - Recovery trigger uses meta.region === 'Unknown' sentinel check (not truthiness)
 *
 * Affected: matches.region, match_players.region, players.region, hourly_match_counts columns
 */
export function normalizeRegion(raw: string): string {
  if (!raw) return 'Unknown';

  // Full name → map to short code (e.g., "North America" → "NA")
  if (raw in REGION_MAP) return REGION_MAP[raw];

  // Already a valid short code → pass through unchanged (e.g., "NA", "EU", "SA")
  if (VALID_SHORT_CODES.has(raw)) return raw;

  // Unmapped long string → flag as Unknown (e.g., "Latin America North")
  return 'Unknown';
}

// ─── Endpoint Source Tags ────────────────────────────────────────────────────

export type EndpointSource =
  | 'direct'
  | 'match_history'
  | 'player_profile'
  | 'player_batch'
  | 'prefetch'
  | 'champion_leaderboard'
  | 'player_loadouts'
  | 'champion'
  | 'item'
  | 'esports'
  | 'recovered'
  | 'minimal';

// ─── Normalized Types ────────────────────────────────────────────────────────

export interface NormalizedPlayer extends PlayerDetails {
  source: EndpointSource;
}

export interface NormalizedPlayerProfile {
  player_id: number;
  active_player_id: number;
  player_name: string;
  platform_name: string | null;
  level: number;
  api_level: number;
  wins: number;
  losses: number;
  leaves: number;
  mastery_level: number;
  region: string;
  platform: string | null;
  hours_played: number;
  minutes_played: number;
  total_xp: number;
  total_worshippers: number;
  total_achievements: number;
  title: string;
  avatar_id: number;
  avatar_url: string | null;
  team_id: number;
  team_name: string;
  hz_gamer_tag: string | null;
  hz_player_name: string | null;
  name_source: 'hz_player_name' | 'hz_gamer_tag' | 'name' | 'none';
  name_anomaly: boolean;
  name_anomaly_reason: string | null;
  ret_msg: string | null;
  privacy_flag: boolean;
  created_at: string | null;
  last_login: string | null;
  loading_frame: string;
  personal_status_message: string;
  ranked_kbm: NormalizedRankedQueue;
  ranked_controller: NormalizedRankedQueue;
  ranked_conquest: NormalizedRankedQueue;
  tier_ranked_kbm: number;
  tier_ranked_controller: number;
  tier_conquest: number;
  merged_players: { player_id: number; portal_id: number | null; merge_datetime: string }[] | null;
  raw_profile: any;
}

export interface NormalizedRankedQueue {
  name: string;
  rank: number;
  tier: number;
  points: number;
  wins: number;
  losses: number;
  leaves: number;
  trend: number;
  prev_rank: number;
  season: number;
  ret_msg: string | null;
  player_id: number | null;
}

export interface NormalizedLeaderboardEntry {
  champion_id: number;
  player_id: number;
  player_name: string;
  rank: number;
  player_ranking: number;
  wins: number;
  losses: number;
}

export interface NormalizedLoadout {
  player_id: number;
  player_name: string;
  champion_id: number;
  deck_id: number;
  deck_name: string;
  cards: NormalizedLoadoutCard[];
}

export interface NormalizedLoadoutCard {
  item_id: number;
  item_name: string;
  points: number;
}

// ─── Match Player Normalizers ────────────────────────────────────────────────

/**
 * Normalize a player from getmatchdetailsbatch.
 * These endpoints return full match data with all fields present.
 */
export function normalizeMatchPlayer(raw: any): NormalizedPlayer {
  return {
    player_id: Number(raw.playerId || raw.PlayerId || raw.player_id || 0),
    player_name: raw.playerName || raw.Player_Name || raw.player_name || 'PRIVATEACCOUNT',
    match_id: raw.Match || raw.match_id || 0,
    entry_datetime: raw.Entry_Datetime || raw.entry_datetime || '',
    queue_id: raw.match_queue_id || raw.Match_Queue_Id || 0,
    // BUG FIX (2026-05-30): Added region field. getmatchdetailsbatch does NOT include Region for all players.
    // When Region is null, normalizeRegion('') returns 'Unknown' → triggers recovery pipeline (getplayerbatchfrommatch).
    // Without this field, match_players.region was always undefined → stored as NULL in DB.
    region: normalizeRegion(raw.Region || raw.region || ''),

    champion_id: raw.ChampionId || raw.champion_id || 0,
    champion_name: raw.Champion || raw.ChampionName || raw.champion_name || '',
    skin_id: raw.SkinId || raw.skin_id || 0,
    skin_name: raw.Skin || raw.skin_name || '',
    kills: raw.Kills_Player || raw.kills || 0,
    deaths: raw.Deaths || raw.deaths || 0,
    assists: raw.Assists || raw.assists || 0,
    damage_done_in_hand: raw.Damage_Done_In_Hand || raw.damage_done_in_hand || 0,
    // `Damage_Player` is the authoritative total damage dealt to players.
    // The historical `damage_done_physical` column stores that total even
    // though its name suggests one damage component. Detail responses do not
    // consistently populate the physical/magical breakdown, and adding those
    // fields can double-count damage already included in `Damage_Player`.
    damage_done_physical: raw.Damage_Player ?? raw.damage_done_physical ?? raw.Damage_Done_Physical ?? 0,
    damage_done_magical: raw.Damage_Done_Magical || raw.damage_done_magical || 0,
    damage_taken: raw.Damage_Taken || raw.damage_taken || 0,
    damage_mitigated: raw.Damage_Mitigated || raw.damage_mitigated || 0,
    healing: raw.Healing || raw.healing || 0,
    healing_self: raw.Healing_Player_Self || raw.healing_self || 0,
    gold_earned: raw.Gold_Earned || raw.gold_earned || 0,
    gold_per_minute: (raw.Time_In_Match_Seconds || raw.Time_In_Match || raw.time_in_match) > 0 ? roundTo2((raw.Gold_Earned || raw.gold_earned || 0) / ((raw.Time_In_Match_Seconds || raw.Time_In_Match || raw.time_in_match) / 60)) : 0,
    objective_assists: raw.Objective_Assists || raw.objective_assists || 0,
    killing_spree: raw.Killing_Spree || raw.killing_spree || 0,
    multi_kill_max: raw.Multi_kill_Max || raw.multi_kill_max || 0,
    win_status: raw.Win_Status || raw.win_status || '',
    task_force: raw.TaskForce || raw.task_force || 0,
    league_tier: raw.League_Tier || raw.league_tier || 0,
    league_points: raw.League_Points || raw.league_points || 0,
    account_level: raw.Account_Level || raw.account_level || 0,
    mastery_level: raw.Mastery_Level || raw.mastery_level || 0,
    party_id: raw.PartyId || raw.party_id || 0,
    time_in_match: raw.Time_In_Match_Seconds || raw.time_in_match || 0,
    distance_traveled: raw.Distance_Traveled || raw.distance_traveled || 0,
    structure_damage: raw.Structure_Damage || raw.structure_damage || 0,
    camps_cleared: raw.Camps_Cleared || raw.camps_cleared || 0,
    // Discovery can stage already-normalized recovery rows back through the raw
    // buffer. Preserve their authority tier instead of laundering every row to
    // `direct`; metric eligibility depends on distinguishing recovered facts
    // from zero-valued private/minimal placeholders.
    source: ['direct', 'recovered', 'minimal'].includes(String(raw.source || '').toLowerCase())
      ? String(raw.source).toLowerCase() as EndpointSource
      : 'direct',
    portal_id: Number(raw.playerPortalId) || 0,
    portal_user_id: raw.playerPortalUserId || '',
    kills_player: raw.Kills_Player || raw.kills_player || 0,
    healing_player_self: raw.Healing_Player_Self || raw.healing_player_self || 0,
    damage_taken_physical: raw.Damage_Taken_Physical || raw.damage_taken_physical || 0,
    damage_taken_magical: raw.Damage_Taken_Magical || raw.damage_taken_magical || 0,
    kills_fire_giant: raw.Kills_Fire_Giant || raw.kills_fire_giant || 0,
    kills_gold_fury: raw.Kills_Gold_Fury || raw.kills_gold_fury || 0,
    kills_phoenix: raw.Kills_Phoenix || raw.kills_phoenix || 0,
    kills_siege_jugg: raw.Kills_Siege_Juggernaut || raw.kills_siege_jugg || 0,
    kills_wild_jugg: raw.Kills_Wild_Juggernaut || raw.kills_wild_jugg || 0,
    kills_bot: raw.Kills_Bot || raw.kills_bot || 0,
    kills_single: raw.Kills_Single || raw.kills_single || 0,
    kills_double: raw.Kills_Double || raw.kills_double || 0,
    kills_triple: raw.Kills_Triple || raw.kills_triple || 0,
    kills_quadra: raw.Kills_Quadra || raw.kills_quadra || 0,
    kills_penta: raw.Kills_Penta || raw.kills_penta || 0,
    kills_first_blood: raw.Kills_First_Blood || raw.kills_first_blood || 0,
    wards_placed: raw.Wards_Placed || raw.wards_placed || 0,
    towers_destroyed: raw.Towers_Destroyed || raw.towers_destroyed || 0,
    league_wins: raw.League_Wins || raw.league_wins || 0,
    league_losses: raw.League_Losses || raw.league_losses || 0,
    healing_bot: raw.Healing_Bot || raw.healing_bot || 0,
    damage_bot: raw.Damage_Bot || raw.damage_bot || 0,
    platform: raw.Platform || raw.platform || '',
    surrendered: raw.Surrendered || raw.surrendered || 0,
    team_id: raw.TeamId || raw.team_id || 0,
    team_name: raw.Team_Name || raw.team_name || '',
    rank_stat_league: raw.Rank_Stat_League || raw.rank_stat_league || 0,
    final_match_level: raw.Final_Match_Level || raw.final_match_level || 0,
    match_duration: raw.Match_Duration || raw.match_duration || 0,
    active_id_1: raw.ActiveId1 || raw.active_id_1 || 0,
    active_id_2: raw.ActiveId2 || raw.active_id_2 || 0,
    active_id_3: raw.ActiveId3 || raw.active_id_3 || 0,
    active_id_4: raw.ActiveId4 || raw.active_id_4 || 0,
    active_level_1: raw.ActiveLevel1 || raw.active_level_1 || 0,
    active_level_2: raw.ActiveLevel2 || raw.active_level_2 || 0,
    active_level_3: raw.ActiveLevel3 || raw.active_level_3 || 0,
    active_level_4: raw.ActiveLevel4 || raw.active_level_4 || 0,
    item_active_1: raw.Item_Active_1 || raw.item_active_1 || '',
    item_active_2: raw.Item_Active_2 || raw.item_active_2 || '',
    item_active_3: raw.Item_Active_3 || raw.item_active_3 || '',
    item_active_4: raw.Item_Active_4 || raw.item_active_4 || '',
    item_id_1: raw.ItemId1 || raw.item_id_1 || 0,
    item_id_2: raw.ItemId2 || raw.item_id_2 || 0,
    item_id_3: raw.ItemId3 || raw.item_id_3 || 0,
    item_id_4: raw.ItemId4 || raw.item_id_4 || 0,
    item_id_5: raw.ItemId5 || raw.item_id_5 || 0,
    item_id_6: raw.ItemId6 || raw.item_id_6 || 0,
    item_level_1: raw.ItemLevel1 || raw.item_level_1 || 0,
    item_level_2: raw.ItemLevel2 || raw.item_level_2 || 0,
    item_level_3: raw.ItemLevel3 || raw.item_level_3 || 0,
    item_level_4: raw.ItemLevel4 || raw.item_level_4 || 0,
    item_level_5: raw.ItemLevel5 || raw.item_level_5 || 0,
    item_level_6: raw.ItemLevel6 || raw.item_level_6 || 0,
    item_purch_1: raw.Item_Purch_1 || raw.item_purch_1 || '',
    item_purch_2: raw.Item_Purch_2 || raw.item_purch_2 || '',
    item_purch_3: raw.Item_Purch_3 || raw.item_purch_3 || '',
    item_purch_4: raw.Item_Purch_4 || raw.item_purch_4 || '',
    item_purch_5: raw.Item_Purch_5 || raw.item_purch_5 || '',
    item_purch_6: raw.Item_Purch_6 || raw.item_purch_6 || '',
    ban_id_1: raw.BanId1 || raw.ban_id_1 || 0,
    ban_id_2: raw.BanId2 || raw.ban_id_2 || 0,
    ban_id_3: raw.BanId3 || raw.ban_id_3 || 0,
    ban_id_4: raw.BanId4 || raw.ban_id_4 || 0,
    ban_id_5: raw.BanId5 || raw.ban_id_5 || 0,
    ban_id_6: raw.BanId6 || raw.ban_id_6 || 0,
    ban_id_7: raw.BanId7 || raw.ban_id_7 || 0,
    ban_id_8: raw.BanId8 || raw.ban_id_8 || 0,
    // BUG FIX (2026-05-30): Was raw.MergedPlayers || null — unnormalized camelCase API object.
    // normalizeMergedPlayers maps playerId→player_id, portalId→portal_id, mergeDatetime→merge_datetime.
    // Without this, DB stored raw camelCase keys → downstream readers crashed on field mismatch.
    merged_players: normalizeMergedPlayers(raw.MergedPlayers),
    has_ret_msg: !!(raw.ret_msg || '').trim(),
  };
}

/**
 * Normalize a player from getmatchhistory.
 * This endpoint returns partial data with different field names:
 * - Kills → kills (not Kills_Player)
 * - Damage → combined damage (no physical/magical split)
 * - Gold → gold (not Gold_Earned)
 * - Champion → champion name (not Reference_Name)
 * - Active_1-4 → active names (not Item_Active_1-4)
 * - Item_1-6 → purchased cards (not Item_Purch_1-6)
 * - Match_Time → datetime (not Entry_Datetime)
 * - Match_Queue_Id → queue_id (not match_queue_id)
 * - Win_Status → "Win"/"Loss" (not "Winner"/"Loser")
 * - ActiveLevel1-4 → scaled by 8× (divide by 4 to get actual level)
 */
export function normalizeMatchHistoryPlayer(raw: any): NormalizedPlayer {
  const winStatus = raw.Win_Status === 'Win' ? 'Winner'
    : raw.Win_Status === 'Loss' ? 'Loser'
      : raw.Win_Status || '';
  const historyMatchNumber = (...keys: string[]): number | null => {
    for (const key of keys) {
      const value = raw?.[key];
      if (value === undefined || value === null || value === '') continue;
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  };

  return {
    player_id: Number(raw.playerId || raw.player_id || 0),
    player_name: raw.playerName || raw.player_name || 'PRIVATEACCOUNT',
    match_id: raw.Match || raw.match_id || 0,
    entry_datetime: raw.Match_Time || raw.entry_datetime || '',
    queue_id: raw.Match_Queue_Id || raw.match_queue_id || 0,
    region: normalizeRegion(raw.Region || raw.region || ''),

    champion_id: raw.ChampionId || 0,
    champion_name: raw.Champion || raw.ChampionName || raw.champion_name || '',
    skin_id: raw.SkinId || 0,
    skin_name: raw.Skin || '',
    kills: raw.Kills || 0,
    deaths: raw.Deaths || 0,
    assists: raw.Assists || 0,
    damage_done_in_hand: raw.Damage_Done_In_Hand || 0,
    // Match history exposes combined player damage as `Damage` and may omit
    // every damage-breakdown field. Preserve the combined value for DPM; a
    // recovered row must never be used for WPM/APM.
    damage_done_physical: raw.Damage ?? raw.damage_done_physical ?? raw.Damage_Player ?? raw.Damage_Done_Physical ?? 0,
    damage_done_magical: raw.Damage_Done_Magical || 0,
    damage_taken: raw.Damage_Taken || 0,
    damage_mitigated: raw.Damage_Mitigated || 0,
    healing: raw.Healing || 0,
    healing_self: raw.Healing_Player_Self || 0,
    gold_earned: raw.Gold || 0,
    gold_per_minute: 0, // Match_Time is a datetime string, not duration — recalculated in hirez.ts from getdemodetails
    objective_assists: raw.Objective_Assists || 0,
    killing_spree: raw.Killing_Spree || 0,
    multi_kill_max: raw.Multi_kill_Max || 0,
    win_status: winStatus,
    task_force: raw.TaskForce || 0,
    // These match-level fields are repeated on every real getmatchhistory row.
    // Keep them namespaced so history can supply an exact recovery score
    // without being mistaken for a direct getmatchdetails player row.
    history_team1_score: historyMatchNumber('history_team1_score', 'Team1Score', 'Team1_Score'),
    history_team2_score: historyMatchNumber('history_team2_score', 'Team2Score', 'Team2_Score'),
    history_winning_task_force: historyMatchNumber(
      'history_winning_task_force',
      'Winning_TaskForce',
      'Winning_Task_Force',
    ),
    league_tier: Number(raw.League_Tier || raw.league_tier || 0),
    league_points: Number(raw.League_Points || raw.league_points || 0),
    account_level: Number(raw.Account_Level || raw.account_level || 0),
    mastery_level: Number(raw.Mastery_Level || raw.mastery_level || 0),
    party_id: Number(raw.PartyId || raw.party_id || 0),
    time_in_match: raw.Time_In_Match_Seconds || 0,
    distance_traveled: raw.Distance_Traveled || 0,
    structure_damage: raw.Damage_Structure || raw.Structure_Damage || 0,
    camps_cleared: raw.Creeps || 0,
    source: 'match_history',
    portal_id: Number(raw.playerPortalId || raw.portal_id || 0),
    portal_user_id: raw.playerPortalUserId || raw.portal_user_id || '',
    kills_player: raw.Kills || 0,
    healing_player_self: raw.Healing_Player_Self || 0,
    damage_taken_physical: raw.Damage_Taken_Physical || 0,
    damage_taken_magical: raw.Damage_Taken_Magical || 0,
    kills_fire_giant: Number(raw.Kills_Fire_Giant || raw.kills_fire_giant || 0),
    kills_gold_fury: Number(raw.Kills_Gold_Fury || raw.kills_gold_fury || 0),
    kills_phoenix: Number(raw.Kills_Phoenix || raw.kills_phoenix || 0),
    kills_siege_jugg: Number(raw.Kills_Siege_Juggernaut || raw.kills_siege_jugg || 0),
    kills_wild_jugg: Number(raw.Kills_Wild_Juggernaut || raw.kills_wild_jugg || 0),
    // BUG FIX (2026-05-30): Was raw.Damage_Bot || 0 — typo mapping kill count to damage value.
    // getmatchhistory does not return bot kill data. All kill fields in this function default to 0.
    // Impact: kills_bot was populated with bot damage (thousands) → corrupted player aggregate stats.
    kills_bot: Number(raw.Kills_Bot || raw.kills_bot || 0),
    kills_single: Number(raw.Kills_Single || raw.kills_single || 0),
    kills_double: Number(raw.Kills_Double || raw.kills_double || 0),
    kills_triple: Number(raw.Kills_Triple || raw.kills_triple || 0),
    kills_quadra: Number(raw.Kills_Quadra || raw.kills_quadra || 0),
    kills_penta: Number(raw.Kills_Penta || raw.kills_penta || 0),
    kills_first_blood: Number(raw.Kills_First_Blood || raw.kills_first_blood || 0),
    wards_placed: raw.Wards_Placed || 0,
    towers_destroyed: Number(raw.Towers_Destroyed || raw.towers_destroyed || 0),
    league_wins: Number(raw.League_Wins || raw.league_wins || 0),
    league_losses: Number(raw.League_Losses || raw.league_losses || 0),
    healing_bot: raw.Healing_Bot || 0,
    damage_bot: raw.Damage_Bot || 0,
    platform: raw.Platform || raw.platform || '',
    surrendered: raw.Surrendered || 0,
    team_id: Number(raw.TeamId || raw.team_id || 0),
    team_name: raw.Team_Name || raw.team_name || '',
    rank_stat_league: Number(raw.Rank_Stat_League || raw.rank_stat_league || 0),
    final_match_level: Number(raw.Final_Match_Level || raw.final_match_level || 0),
    match_duration: Number(raw.Match_Duration || raw.match_duration || 0),
    active_id_1: raw.ActiveId1 || 0,
    active_id_2: raw.ActiveId2 || 0,
    active_id_3: raw.ActiveId3 || 0,
    active_id_4: raw.ActiveId4 || 0,
    // BUG FIX (2026-05-30): getmatchhistory ActiveLevel1-4 scaled by 8×. Divide by 4 for actual item level.
    // Without this, recovered matches had inflated item levels (e.g., 8 instead of 2, 16 instead of 4).
    active_level_1: Math.round((raw.ActiveLevel1 || 0) / 4),
    active_level_2: Math.round((raw.ActiveLevel2 || 0) / 4),
    active_level_3: Math.round((raw.ActiveLevel3 || 0) / 4),
    active_level_4: Math.round((raw.ActiveLevel4 || 0) / 4),
    item_active_1: raw.Active_1 || '',
    item_active_2: raw.Active_2 || '',
    item_active_3: raw.Active_3 || '',
    item_active_4: raw.Active_4 || '',
    item_id_1: raw.ItemId1 || 0,
    item_id_2: raw.ItemId2 || 0,
    item_id_3: raw.ItemId3 || 0,
    item_id_4: raw.ItemId4 || 0,
    item_id_5: raw.ItemId5 || 0,
    item_id_6: raw.ItemId6 || 0,
    item_level_1: raw.ItemLevel1 || 0,
    item_level_2: raw.ItemLevel2 || 0,
    item_level_3: raw.ItemLevel3 || 0,
    item_level_4: raw.ItemLevel4 || 0,
    item_level_5: raw.ItemLevel5 || 0,
    item_level_6: raw.ItemLevel6 || 0,
    item_purch_1: raw.Item_1 || '',
    item_purch_2: raw.Item_2 || '',
    item_purch_3: raw.Item_3 || '',
    item_purch_4: raw.Item_4 || '',
    item_purch_5: raw.Item_5 || '',
    item_purch_6: raw.Item_6 || '',
    ban_id_1: Number(raw.BanId1 || raw.ban_id_1 || 0),
    ban_id_2: Number(raw.BanId2 || raw.ban_id_2 || 0),
    ban_id_3: Number(raw.BanId3 || raw.ban_id_3 || 0),
    ban_id_4: Number(raw.BanId4 || raw.ban_id_4 || 0),
    ban_id_5: Number(raw.BanId5 || raw.ban_id_5 || 0),
    ban_id_6: Number(raw.BanId6 || raw.ban_id_6 || 0),
    ban_id_7: Number(raw.BanId7 || raw.ban_id_7 || 0),
    ban_id_8: Number(raw.BanId8 || raw.ban_id_8 || 0),
    merged_players: null,
    has_ret_msg: false,
  };
}

// ─── Player Profile Normalizers ──────────────────────────────────────────────

const EPIC_SYNTHETIC_PLATFORM_NAME_RE = /^[0-9a-f]{20,}User-[0-9a-f]{6,}$/i;
const RELAY_DUMMY_PLAYER_NAME_RE = /^DummyPlayer[0-9]+$/i;

function cleanText(value: any): string {
  if (value == null) return '';
  return String(value).trim();
}

function nullableText(value: any): string | null {
  const text = cleanText(value);
  return text.length > 0 ? text : null;
}

function nullableTimestamp(value: any): string | null {
  const text = cleanText(value);
  if (!text) return null;
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

/**
 * Detect profile endpoint name values that are transport/test artifacts rather
 * than Paladins display names.
 *
 * Hi-Rez `getplayer` / `getplayerbatch` can return multiple name-like fields:
 * - `Name`: platform account identity. For Epic accounts this can be an
 *   obfuscated value like `70f0...User-1bb...`.
 * - `hz_player_name`: Paladins / Hi-Rez display name, when Hi-Rez supplies it.
 * - `hz_gamer_tag`: secondary gamer-tag fallback.
 *
 * The dev API examples in the `dev/api-repo/arez.../arez/player.py` packages
 * preserve `Name`
 * as `platform_name`, then choose display name in this order:
 * `hz_player_name` > `hz_gamer_tag` > `Name`.
 *
 * PaladinsCat previously treated `Name` as canonical, which let Epic platform
 * identifiers leak into `players.name`. Later, relay dummy-mode profile rows
 * such as `DummyPlayer6607951` leaked through `hz_player_name` after a live DB
 * had been tested against synthetic responses. This helper keeps those raw
 * identities observable while blocking known synthetic values from becoming the
 * public display name.
 */
export function isSyntheticProfileDisplayName(value: any): boolean {
  const text = cleanText(value);
  return EPIC_SYNTHETIC_PLATFORM_NAME_RE.test(text) || RELAY_DUMMY_PLAYER_NAME_RE.test(text);
}

export function isSyntheticPlatformPlayerName(value: any): boolean {
  return isSyntheticProfileDisplayName(value);
}

function syntheticProfileReason(value: string | null, fieldName: string): string | null {
  if (!value) return null;
  if (EPIC_SYNTHETIC_PLATFORM_NAME_RE.test(value)) {
    return `profile ${fieldName} is an obfuscated Epic platform identifier`;
  }
  if (RELAY_DUMMY_PLAYER_NAME_RE.test(value)) {
    return `profile ${fieldName} is a HirezRelay dummy-mode synthetic name`;
  }
  return null;
}

function normalizeProfileName(raw: any): {
  playerName: string;
  platformName: string | null;
  hzPlayerName: string | null;
  hzGamerTag: string | null;
  source: NormalizedPlayerProfile['name_source'];
  anomaly: boolean;
  anomalyReason: string | null;
} {
  const platformName = nullableText(raw.Name);
  const hzPlayerName = nullableText(raw.hz_player_name);
  const hzGamerTag = nullableText(raw.hz_gamer_tag);
  const platformNameReason = syntheticProfileReason(platformName, 'Name');
  const hzPlayerNameReason = syntheticProfileReason(hzPlayerName, 'hz_player_name');
  const hzGamerTagReason = syntheticProfileReason(hzGamerTag, 'hz_gamer_tag');
  const anomalyReason = [platformNameReason, hzPlayerNameReason, hzGamerTagReason].filter(Boolean).join('; ') || null;

  if (hzPlayerName && !hzPlayerNameReason) {
    return {
      playerName: hzPlayerName,
      platformName,
      hzPlayerName,
      hzGamerTag,
      source: 'hz_player_name',
      anomaly: Boolean(anomalyReason),
      anomalyReason,
    };
  }

  if (hzGamerTag && !hzGamerTagReason) {
    return {
      playerName: hzGamerTag,
      platformName,
      hzPlayerName,
      hzGamerTag,
      source: 'hz_gamer_tag',
      anomaly: Boolean(anomalyReason),
      anomalyReason,
    };
  }

  if (platformName && !platformNameReason) {
    return {
      playerName: platformName,
      platformName,
      hzPlayerName,
      hzGamerTag,
      source: 'name',
      anomaly: Boolean(anomalyReason),
      anomalyReason,
    };
  }

  return {
    playerName: '',
    platformName,
    hzPlayerName,
    hzGamerTag,
    source: 'none',
    anomaly: Boolean(anomalyReason),
    anomalyReason: anomalyReason
      ? `${anomalyReason}; no usable display fallback was present`
      : 'profile payload did not contain a usable display name',
  };
}

/**
 * Normalize a player profile from getplayer / getplayerbatch.
 * Both endpoints return the same flat player object structure.
 */
export function normalizePlayerProfile(raw: any): NormalizedPlayerProfile {
  const nameInfo = normalizeProfileName(raw);
  const apiLevel = Number(raw.Level ?? raw.level ?? 0);
  const totalXpValue = raw.Total_XP ?? raw.total_xp;
  const totalXp = totalXpValue === '' || totalXpValue == null ? Number.NaN : Number(totalXpValue);

  return {
    player_id: Number(raw.Id || raw.ActivePlayerId || 0),
    active_player_id: Number(raw.ActivePlayerId || raw.Id || 0),
    player_name: nameInfo.playerName,
    platform_name: nameInfo.platformName,
    // Hi-Rez caps Level at 999. Total_XP continues to grow, so use the
    // calculated level everywhere PaladinsCat presents a player profile while
    // retaining the API's capped value separately for diagnostics.
    level: resolvePlayerLevel(totalXp, apiLevel),
    api_level: Number.isFinite(apiLevel) && apiLevel > 0 ? Math.floor(apiLevel) : 0,
    wins: raw.Wins || 0,
    losses: raw.Losses || 0,
    leaves: raw.Leaves || 0,
    mastery_level: raw.MasteryLevel || 0,
    region: normalizeRegion(raw.Region || ''),
    platform: raw.Platform || null,
    hours_played: raw.HoursPlayed || 0,
    minutes_played: raw.MinutesPlayed || 0,
    total_xp: Number.isFinite(totalXp) && totalXp >= 0 ? Math.floor(totalXp) : 0,
    total_worshippers: raw.Total_Worshippers || 0,
    total_achievements: raw.Total_Achievements || 0,
    title: raw.Title || '',
    avatar_id: raw.AvatarId || 0,
    avatar_url: nullableText(raw.AvatarURL),
    team_id: raw.TeamId || 0,
    team_name: raw.Team_Name || '',
    hz_gamer_tag: nameInfo.hzGamerTag,
    hz_player_name: nameInfo.hzPlayerName,
    name_source: nameInfo.source,
    name_anomaly: nameInfo.anomaly,
    name_anomaly_reason: nameInfo.anomalyReason,
    ret_msg: nullableText(raw.ret_msg),
    // BUG FIX (2026-05-30): Was raw.privacy_flag === 'y' — case-sensitive. If Hi-Rez returns 'Y',
    // evaluates to false → private accounts exposed as public. normalizePlayerStatus already used toLowerCase().
    privacy_flag: (raw.privacy_flag || '').toLowerCase() === 'y',
    created_at: nullableTimestamp(raw.Created_Datetime),
    last_login: nullableTimestamp(raw.Last_Login_Datetime),
    loading_frame: raw.LoadingFrame || '',
    personal_status_message: raw.Personal_Status_Message || '',
    ranked_kbm: normalizeRankedQueue(raw.RankedKBM),
    ranked_controller: normalizeRankedQueue(raw.RankedController),
    ranked_conquest: normalizeRankedQueue(raw.RankedConquest),
    tier_ranked_kbm: raw.Tier_RankedKBM || 0,
    tier_ranked_controller: raw.Tier_RankedController || 0,
    tier_conquest: raw.Tier_Conquest || 0,
    merged_players: normalizeMergedPlayers(raw.MergedPlayers),
    raw_profile: raw,
  };
}

function normalizeRankedQueue(raw: any): NormalizedRankedQueue {
  if (!raw) {
    return {
      name: '',
      rank: 0,
      tier: 0,
      points: 0,
      wins: 0,
      losses: 0,
      leaves: 0,
      trend: 0,
      prev_rank: 0,
      season: 0,
      ret_msg: null,
      player_id: null,
    };
  }
  return {
    name: raw.Name || '',
    rank: raw.Rank || 0,
    tier: raw.Tier || 0,
    points: raw.Points || 0,
    wins: raw.Wins || 0,
    losses: raw.Losses || 0,
    leaves: raw.Leaves || 0,
    trend: raw.Trend || 0,
    prev_rank: raw.PrevRank || 0,
    season: raw.Season || 0,
    ret_msg: nullableText(raw.ret_msg),
    player_id: Number(raw.player_id) || null,
  };
}

function normalizeMergedPlayers(raw: any): { player_id: number; portal_id: number | null; merge_datetime: string }[] | null {
  if (!raw || !Array.isArray(raw)) return null;
  return raw.map((m: any) => ({
    player_id: Number(m.playerId) || 0,
    portal_id: Number(m.portalId) || null,
    merge_datetime: m.merge_datetime || '',
  }));
}

// ─── Leaderboard Normalizers ─────────────────────────────────────────────────

/**
 * Normalize a champion leaderboard entry from getchampionleaderboard.
 * All fields are strings in the API response, need to be converted to numbers.
 */
export function normalizeLeaderboardEntry(raw: any): NormalizedLeaderboardEntry {
  return {
    champion_id: Number(raw.champion_id) || 0,
    player_id: Number(raw.player_id) || 0,
    player_name: raw.player_name || '',
    rank: Number(raw.rank) || 0,
    player_ranking: Number(raw.player_ranking) || 0,
    wins: Number(raw.wins) || 0,
    losses: Number(raw.losses) || 0,
  };
}

// ─── Loadout Normalizers ─────────────────────────────────────────────────────

/**
 * Normalize a player loadout from getplayerloadouts.
 * Returns array of decks, each with 5 cards.
 */
export function normalizeLoadout(raw: any): NormalizedLoadout {
  return {
    player_id: Number(raw.playerId ?? raw.PlayerId ?? raw.player_id) || 0,
    player_name: raw.playerName ?? raw.PlayerName ?? raw.player_name ?? '',
    champion_id: Number(raw.ChampionId ?? raw.championId ?? raw.champion_id) || 0,
    deck_id: Number(raw.DeckId ?? raw.deckId ?? raw.deck_id) || 0,
    deck_name: raw.DeckName ?? raw.deckName ?? raw.deck_name ?? '',
    cards: (raw.LoadoutItems ?? raw.loadout_items ?? []).map((card: any) => ({
      item_id: Number(card.ItemId ?? card.itemId ?? card.item_id) || 0,
      item_name: card.ItemName ?? card.itemName ?? card.item_name ?? '',
      points: Number(card.Points ?? card.points ?? card.level) || 0,
    })),
  };
}

// ─── Match Metadata ─────────────────────────────────────────────────────────

/**
 * Extract match metadata from player array.
 *
 * BUG FIX (2026-05-30): Previously took single player object (p0). If p0 had null Region
 * (private/platform-masked account), entire match got 'Unknown' region → triggered unnecessary
 * recovery pipeline. Now accepts players array, searches for first player with valid Region.
 * Region data exists in getmatchdetailsbatch but is null for some players — not all players.
 *
 * Call site: buffer-processor.ts → extractMatchMetadata(players) (was extractMatchMetadata(p0))
 * Affected: matches.region, hourly_match_counts region columns, recovery pipeline trigger
 */
export function extractMatchMetadata(players: any[]): {
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
  direct_score_observations: Array<{ team1: number | null; team2: number | null; winner: number | null }>;
  has_replay: boolean;
} {
  // p0 used for match-level fields (match_id, entry_datetime, etc.) — same for all players.
  // playerWithRegion used for region — search for first player with valid Region to avoid p0 null trap.
  const p0 = players[0] || {};
  const playerWithRegion = players.find(p => p?.Region || p?.region) || p0;
  const nullableMatchNumber = (...keys: string[]): number | null => {
    for (const key of keys) {
      if (!Object.prototype.hasOwnProperty.call(p0, key)) continue;
      const value = p0[key];
      // Preserve explicit null during normalization. The final match boundary
      // rejects incomplete scores; normalization itself must not convert null
      // into a false zero before that validation runs.
      if (value === null) return null;
      if (value === '') continue;
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  };
  const rowNumber = (row: any, ...keys: string[]): number | null => {
    for (const key of keys) {
      if (!Object.prototype.hasOwnProperty.call(row || {}, key)) continue;
      const value = row[key];
      if (value === undefined || value === null || value === '') continue;
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return null;
  };

  return {
    match_id: p0.Match || p0.match_id || 0,
    // Full match payloads should always carry their real match start time.
    // Do not fall back to "now": during the 2026-06-18 getmatchhistory fan-out
    // incident, normalized history rows were accidentally routed as full match
    // payloads and the old fallback created fake current-hour queue-0 matches.
    // Returning an empty value lets the buffer processor reject malformed full
    // match facts before they reach `matches` or `hourly_match_counts`.
    entry_datetime: p0.Entry_Datetime || p0.entry_datetime || p0.Match_Time || p0.match_time || '',
    map: p0.Map_Game || p0['map'] || '',
    queue_id: p0.match_queue_id || p0.Match_Queue_Id || p0.queue_id || p0.Queue || 0,
    duration_seconds: p0.Match_Duration || p0.duration_seconds || 0,
    minutes: p0.Minutes || p0.minutes || 0,
    region: normalizeRegion(playerWithRegion.Region || playerWithRegion.region || ''),
    team1_score: nullableMatchNumber('Team1Score', 'Team1_Score', 'team1_score'),
    team2_score: nullableMatchNumber('Team2Score', 'Team2_Score', 'team2_score'),
    winning_task_force: nullableMatchNumber('Winning_TaskForce', 'Winning_Task_Force', 'winning_task_force'),
    direct_score_observations: players.map(row => ({
      team1: rowNumber(row, 'Team1Score', 'Team1_Score', 'team1_score'),
      team2: rowNumber(row, 'Team2Score', 'Team2_Score', 'team2_score'),
      winner: rowNumber(row, 'Winning_TaskForce', 'Winning_Task_Force', 'winning_task_force'),
    })),
    has_replay: (p0.hasReplay || p0.Has_Replay || '').toLowerCase() === 'y',
  };
}

/**
 * Detect endpoint source from raw data structure.
 * Heuristic: check for fields unique to each endpoint format.
 */
export function detectEndpointSource(raw: any): EndpointSource {
  // BUG FIX (2026-05-30): Was raw.Kills !== undefined — brittle. If Hi-Rez omits zero-value fields
  // from JSON, raw.Kills is undefined even for match_history data → fallback routes to 'direct' path.
  // 'Kills' in raw checks property presence regardless of value (0, null, undefined).
  // This is a fallback heuristic only — primary paths call correct normalizer directly.
  // Match history: uses "Kills" (not "Kills_Player"), "Gold" (not "Gold_Earned")
  if ('Kills' in raw && !('Kills_Player' in raw) && 'Gold' in raw && !('Gold_Earned' in raw)) {
    return 'match_history';
  }
  // Player profile: has "Level", "Wins", "Losses", "MasteryLevel"
  if ('MasteryLevel' in raw && 'HoursPlayed' in raw) {
    return 'player_profile';
  }
  // Leaderboard: has "champion_id", "player_ranking" (snake_case)
  if ('champion_id' in raw && 'player_ranking' in raw) {
    return 'champion_leaderboard';
  }
  // Loadouts: has "LoadoutItems", "DeckId"
  if ('LoadoutItems' in raw && 'DeckId' in raw) {
    return 'player_loadouts';
  }
  // Default: match details (has Kills_Player, Gold_Earned)
  return 'direct';
}

// ─── Player Status ───────────────────────────────────────────────────────────

export interface NormalizedPlayerStatus {
  player_id: number;
  status: number;
  status_string: string;
  current_match_id: number | null;
  queue_id: number | null;
  privacy_flag: boolean;
  personal_status_message: string | null;
}

/** PostgreSQL text values cannot contain the Unicode NUL character. */
function sanitizePlayerStatusText(value: unknown): string {
  return String(value ?? '').replace(/\u0000/g, '');
}

export function normalizePlayerStatus(raw: any): NormalizedPlayerStatus {
  const personalStatusMessage = sanitizePlayerStatusText(raw.personal_status_message);

  return {
    player_id: Number(raw.player_id || 0),
    status: Number(raw.status || 0),
    status_string: sanitizePlayerStatusText(raw.status_string),
    current_match_id: raw.Match ? Number(raw.Match) : null,
    queue_id: raw.match_queue_id ? Number(raw.match_queue_id) : null,
    privacy_flag: (raw.privacy_flag || '').toLowerCase() === 'y',
    personal_status_message: personalStatusMessage || null,
  };
}

// ─── Player Champions ────────────────────────────────────────────────────────

export interface NormalizedPlayerChampion {
  player_id: number;
  champion_id: number;
  champion_name: string;
  xp: number;
  ownership_type: string;
  wins: number;
  losses: number;
  kills: number;
  deaths: number;
  assists: number;
  minutes_played: number;
}

const PLAYER_CHAMPION_COMBAT_FIELDS = ['Wins', 'Losses', 'Kills', 'Deaths', 'Assists', 'Minutes'] as const;

/**
 * getplayerchampions is a roster/mastery endpoint and does not include combat
 * totals. Only accept a payload as champion stats when every field supplied by
 * getchampionranks is actually present, including legitimate zero values.
 */
export function hasPlayerChampionCombatStats(raw: any): boolean {
  return raw !== null
    && typeof raw === 'object'
    && PLAYER_CHAMPION_COMBAT_FIELDS.every((field) => (
      Object.prototype.hasOwnProperty.call(raw, field)
      && raw[field] !== null
      && raw[field] !== ''
      && Number.isFinite(Number(raw[field]))
    ));
}

export function normalizePlayerChampion(raw: any): NormalizedPlayerChampion {
  return {
    player_id: Number(raw.PlayerId ?? raw.player_id ?? 0),
    champion_id: Number(raw.ChampionId ?? raw.champion_id ?? 0),
    champion_name: raw.Champion ?? raw.champion ?? '',
    xp: Number(raw.XP ?? raw.Worshippers ?? 0),
    ownership_type: raw.OwnershipType || '',
    wins: Number(raw.Wins || 0),
    losses: Number(raw.Losses || 0),
    kills: Number(raw.Kills || 0),
    deaths: Number(raw.Deaths || 0),
    assists: Number(raw.Assists || 0),
    minutes_played: Number(raw.Minutes || 0),
  };
}

// ─── Player Achievements ─────────────────────────────────────────────────────

export interface NormalizedPlayerAchievements {
  player_id: number;
  player_name: string;
  assisted_kills: number;
  camps_cleared: number;
  divine_spree: number;
  double_kills: number;
  fire_giant_kills: number;
  first_bloods: number;
  god_like_spree: number;
  gold_fury_kills: number;
  immortal_spree: number;
  killing_spree: number;
  minion_kills: number;
  penta_kills: number;
  phoenix_kills: number;
  player_kills: number;
  quadra_kills: number;
  rampage_spree: number;
  shutdown_spree: number;
  siege_juggernaut_kills: number;
  tower_kills: number;
  triple_kills: number;
  unstoppable_spree: number;
  wild_juggernaut_kills: number;
}

export function normalizePlayerAchievements(raw: any): NormalizedPlayerAchievements {
  return {
    player_id: Number(raw.Id || 0),
    player_name: raw.Name || '',
    assisted_kills: Number(raw.AssistedKills || 0),
    camps_cleared: Number(raw.CampsCleared || 0),
    divine_spree: Number(raw.DivineSpree || 0),
    double_kills: Number(raw.DoubleKills || 0),
    fire_giant_kills: Number(raw.FireGiantKills || 0),
    first_bloods: Number(raw.FirstBloods || 0),
    god_like_spree: Number(raw.GodLikeSpree || 0),
    gold_fury_kills: Number(raw.GoldFuryKills || 0),
    immortal_spree: Number(raw.ImmortalSpree || 0),
    killing_spree: Number(raw.KillingSpree || 0),
    minion_kills: Number(raw.MinionKills || 0),
    penta_kills: Number(raw.PentaKills || 0),
    phoenix_kills: Number(raw.PhoenixKills || 0),
    player_kills: Number(raw.PlayerKills || 0),
    quadra_kills: Number(raw.QuadraKills || 0),
    rampage_spree: Number(raw.RampageSpree || 0),
    shutdown_spree: Number(raw.ShutdownSpree || 0),
    siege_juggernaut_kills: Number(raw.SiegeJuggernautKills || 0),
    tower_kills: Number(raw.TowerKills || 0),
    triple_kills: Number(raw.TripleKills || 0),
    unstoppable_spree: Number(raw.UnstoppableSpree || 0),
    wild_juggernaut_kills: Number(raw.WildJuggernautKills || 0),
  };
}

// ─── Skins ───────────────────────────────────────────────────────────────────

export interface NormalizedSkin {
  skin_id: number;
  champion_id: number;
  champion_name: string;
  skin_name: string;
  skin_name_english: string;
  rarity: string;
  external_skin_url: string;
}

export function normalizeSkin(raw: any): NormalizedSkin {
  return {
    skin_id: Number(raw.skin_id2 || 0),
    champion_id: Number(raw.champion_id || 0),
    champion_name: raw.champion_name || '',
    skin_name: raw.skin_name || '',
    skin_name_english: raw.skin_name_english || '',
    rarity: raw.rarity || '',
    external_skin_url: raw.external_skin_url || '',
  };
}

// ─── Bounty Items ────────────────────────────────────────────────────────────

export interface NormalizedBountyItem {
  item_id: number;
  item_name: string;
  champion_id: number;
  champion_name: string;
  sale_type: string;
  initial_price: number;
  final_price: number;
  sale_end_date: string | null;
  active: boolean;
}

export function normalizeBountyItem(raw: any): NormalizedBountyItem {
  return {
    item_id: Number(raw.bounty_item_id1 || 0),
    item_name: raw.bounty_item_name || '',
    champion_id: Number(raw.champion_id || 0),
    champion_name: raw.champion_name || '',
    sale_type: raw.sale_type || '',
    initial_price: Number(raw.initial_price || 0),
    final_price: Number(raw.final_price || 0),
    sale_end_date: raw.sale_end_datetime || null,
    active: (raw.active || '').toLowerCase() === 'y',
  };
}

// ─── League Leaderboard ──────────────────────────────────────────────────────

export interface NormalizedLeagueLeaderboardEntry {
  player_id: number;
  player_name: string;
  rank: number;
  tier: number;
  points: number;
  wins: number;
  losses: number;
  queue_id: number;
  season: number;
}

export function normalizeLeagueLeaderboardEntry(raw: any): NormalizedLeagueLeaderboardEntry {
  return {
    player_id: Number(raw.player_id || 0),
    player_name: raw.player_name || '',
    rank: Number(raw.rank || 0),
    tier: Number(raw.tier || 0),
    points: Number(raw.points || 0),
    wins: Number(raw.wins || 0),
    losses: Number(raw.losses || 0),
    queue_id: Number(raw.queue_id || 0),
    season: Number(raw.season || 0),
  };
}

// ─── Champion ────────────────────────────────────────────────────────────────

export interface NormalizedChampion {
  id: number;
  name: string;
  title: string;
  health: number;
  speed: number;
  roles: string;
  ability1_id: number;
  ability1_name: string;
  ability1_type: string;
  ability1_description: string;
  ability2_id: number;
  ability2_name: string;
  ability2_type: string;
  ability2_description: string;
  ability3_id: number;
  ability3_name: string;
  ability3_type: string;
  ability3_description: string;
  ability4_id: number;
  ability4_name: string;
  ability4_type: string;
  ability4_description: string;
  ability5_id: number;
  ability5_name: string;
  ability5_type: string;
  ability5_description: string;
}

export function normalizeChampion(raw: any): NormalizedChampion {
  return {
    id: Number(raw.Id || 0),
    name: raw.Name || '',
    title: raw.Title || '',
    health: Number(raw.Health || 0),
    speed: Number(raw.Speed || 0),
    roles: raw.Roles || '',
    ability1_id: Number(raw.Ability1Id || 0),
    ability1_name: raw.Ability1Name || '',
    ability1_type: raw.Ability1Type || '',
    ability1_description: raw.Ability1Description || '',
    ability2_id: Number(raw.Ability2Id || 0),
    ability2_name: raw.Ability2Name || '',
    ability2_type: raw.Ability2Type || '',
    ability2_description: raw.Ability2Description || '',
    ability3_id: Number(raw.Ability3Id || 0),
    ability3_name: raw.Ability3Name || '',
    ability3_type: raw.Ability3Type || '',
    ability3_description: raw.Ability3Description || '',
    ability4_id: Number(raw.Ability4Id || 0),
    ability4_name: raw.Ability4Name || '',
    ability4_type: raw.Ability4Type || '',
    ability4_description: raw.Ability4Description || '',
    ability5_id: Number(raw.Ability5Id || 0),
    ability5_name: raw.Ability5Name || '',
    ability5_type: raw.Ability5Type || '',
    ability5_description: raw.Ability5Description || '',
  };
}

// ─── Item ────────────────────────────────────────────────────────────────────

export interface NormalizedItem {
  id: number;
  name: string;
  description: string;
  type: string;
  cost: number;
  icon_url: string;
  recharge_seconds: number;
  champion_id: number | null;
  talent_reward_level: number | null;
}

export function normalizeItem(raw: any): NormalizedItem {
  return {
    id: Number(raw.Id || 0),
    name: raw.Name || '',
    description: raw.Description || '',
    type: raw.Type || '',
    cost: Number(raw.Cost || 0),
    icon_url: raw.Icon || '',
    recharge_seconds: Number(raw.RechargeSeconds || 0),
    champion_id: raw.ChampionId ? Number(raw.ChampionId) : null,
    talent_reward_level: raw.TalentRewardLevel ? Number(raw.TalentRewardLevel) : null,
  };
}

// ─── Esports ─────────────────────────────────────────────────────────────────

export interface NormalizedEsportsLeague {
  league_id: number;
  league_name: string;
  league_description: string;
  league_image_url: string;
  league_start_date: string;
  league_end_date: string;
  teams: NormalizedEsportsTeam[];
}

export interface NormalizedEsportsTeam {
  team_id: number;
  team_name: string;
  team_description: string;
  team_image_url: string;
  players: NormalizedEsportsPlayer[];
}

export interface NormalizedEsportsPlayer {
  player_id: number;
  player_name: string;
  team_id: number;
}

export function normalizeEsportsLeague(raw: any): NormalizedEsportsLeague {
  return {
    league_id: Number(raw.LeagueId || 0),
    league_name: raw.LeagueName || '',
    league_description: raw.LeagueDescription || '',
    league_image_url: raw.LeagueImage || '',
    league_start_date: raw.LeagueStartDate || '',
    league_end_date: raw.LeagueEndDate || '',
    teams: (raw.Teams || []).map((t: any) => normalizeEsportsTeam(t)),
  };
}

export function normalizeEsportsTeam(raw: any): NormalizedEsportsTeam {
  return {
    team_id: Number(raw.TeamId || 0),
    team_name: raw.TeamName || '',
    team_description: raw.TeamDescription || '',
    team_image_url: raw.TeamImage || '',
    players: (raw.Players || []).map((p: any) => normalizeEsportsPlayer(p)),
  };
}

export function normalizeEsportsPlayer(raw: any): NormalizedEsportsPlayer {
  return {
    player_id: Number(raw.PlayerId || 0),
    player_name: raw.PlayerName || '',
    team_id: Number(raw.TeamId || 0),
  };
}

// ── Live Match Player ────────────────────────────────────────────────────────

/**
 * Normalize a live match player from getmatchplayerdetails response.
 * Based on LivePlayer class from match.py:
 * playerId, playerName, playerPortalId, ChampionId, ChampionName,
 * SkinId, Skin, Tier, tierWins, tierLosses, Account_Level,
 * Mastery_Level, taskForce
 */
export function normalizeLiveMatchPlayer(raw: any): any {
  return {
    player_id: Number(raw.playerId || raw.player_id || raw.PlayerId || 0),
    player_name: raw.playerName || raw.player_name || raw.PlayerName || '',
    portal_id: Number(raw.playerPortalId || raw.player_portal_id || raw.PlayerPortalId || 0),
    champion_id: Number(raw.ChampionId || raw.champion_id || 0),
    champion_name: raw.ChampionName || raw.champion_name || '',
    skin_id: Number(raw.SkinId || raw.skin_id || 0),
    skin_name: raw.Skin || raw.skin_name || '',
    tier: Number(raw.Tier || raw.tier || 0),
    tier_wins: Number(raw.tierWins || raw.tier_wins || 0),
    tier_losses: Number(raw.tierLosses || raw.tier_losses || 0),
    account_level: Number(raw.Account_Level || raw.account_level || 0),
    mastery_level: Number(raw.Mastery_Level || raw.mastery_level || 0),
    task_force: Number(raw.taskForce || raw.task_force || raw.TaskForce || 0),
  };
}
