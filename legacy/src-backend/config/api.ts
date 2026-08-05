import { readFileSync } from 'fs';
import { join } from 'path';

export const RANKED_QUEUES = [486];

export const TIMEOUT_MS = 15000;

interface BrokenSkin {
  champion_id: number;
  champion: string;
  skin_id: number;
  skin_name: string;
}

// CRITICAL: Wrap readFileSync in try-catch. If broken-skins.json is missing
// (e.g., fresh deployment without build step) or contains malformed JSON,
// the unhandled exception crashes the entire backend at module import time.
// All routes and services that import this module become unavailable.
// Fallback: empty array → no broken skins detected (acceptable degradation).
// Source: Fault #3 — "No error handling on readFileSync"
let brokenSkinsData: BrokenSkin[] = [];
try {
  brokenSkinsData = JSON.parse(
    readFileSync(join(__dirname, 'broken-skins.json'), 'utf-8')
  );
} catch (err) {
  console.warn(`[api] Failed to load broken-skins.json: ${err}`);
}

export const BROKEN_SKINS = new Set(brokenSkinsData.map((s) => s.skin_id));

/**
 * Check if a skin_id is broken (Int16 overflow > 32767 or known broken skin).
 * Int16 overflow causes skin_id to wrap around, producing incorrect values.
 */
export function isBrokenSkin(skinId: number): boolean {
  return skinId > 32767 || BROKEN_SKINS.has(skinId);
}

export const FIELD_MAP: Record<string, string> = {
  match_id: 'match_id',
  match_queue_id: 'queue_id',
  Map_Game: 'map',
  Entry_DateTime: 'entry_datetime',
  Duration_Seconds: 'duration_seconds',
  Region: 'region',
  Team1_Score: 'team1_score',
  Team2_Score: 'team2_score',
  Winning_Task_Force: 'winning_task_force',
  Has_Replay: 'has_replay',
  PlayerId: 'player_id',
  Player_Name: 'player_name',
  ChampionId: 'champion_id',
  Reference_Name: 'champion_name',
  SkinId: 'skin_id',
  Skin: 'skin_name',
  Kills_Player: 'kills',
  Deaths: 'deaths',
  Assists: 'assists',
  Damage_Done_In_Hand: 'damage_done_in_hand',
  Damage_Done_Physical: 'damage_done_physical',
  Damage_Done_Magical: 'damage_done_magical',
  Damage_Taken: 'damage_taken',
  Damage_Mitigated: 'damage_mitigated',
  Healing: 'healing',
  Healing_Player_Self: 'healing_self',
  Gold_Earned: 'gold_earned',
  Gold_Per_Minute: 'gold_per_minute',
  Objective_Assists: 'objective_assists',
  Killing_Spree: 'killing_spree',
  Multi_kill_Max: 'multi_kill_max',
  Win_Status: 'win_status',
  TaskForce: 'task_force',
  League_Tier: 'league_tier',
  League_Points: 'league_points',
  Account_Level: 'account_level',
  Mastery_Level: 'mastery_level',
  PartyId: 'party_id',
  Time_In_Match_Seconds: 'time_in_match',
  Distance_Traveled: 'distance_traveled',
  Structure_Damage: 'structure_damage',
  Camps_Cleared: 'camps_cleared',
  Damage_Taken_Physical: 'damage_taken_physical',
  Damage_Taken_Magical: 'damage_taken_magical',
  Kills_Fire_Giant: 'kills_fire_giant',
  Kills_Gold_Fury: 'kills_gold_fury',
  Kills_Phoenix: 'kills_phoenix',
  Kills_Siege_Juggernaut: 'kills_siege_jugg',
  Kills_Wild_Juggernaut: 'kills_wild_jugg',
  Kills_Bot: 'kills_bot',
  Wards_Placed: 'wards_placed',
  Towers_Destroyed: 'towers_destroyed',
  League_Wins: 'league_wins',
  League_Losses: 'league_losses',
  Healing_Bot: 'healing_bot',
};

export const API_CONFIG = {
  // The production default remains the Hi-Rez Paladins API. An explicit
  // override is required for isolated relay parity tests and disaster drills;
  // keeping the provider origin injectable prevents those tests from ever
  // spending production quota.
  BASE_URL: process.env.HIREZ_API_BASE_URL || 'https://api.paladins.com/paladinsapi.svc',
  SESSION_TTL_MS: 14 * 60 * 1000,
  // Match-detail batches are limited to 10 IDs, while getplayerbatch accepts
  // up to 20. Keep the limits separate so profile enrichment can combine two
  // match rosters without splitting them back into 10-ID outbound calls.
  BATCH_SIZE: 10,
  PLAYER_BATCH_SIZE: 20,
  RETRY_DELAY_MS: 1000,
  MAX_RETRIES: 3,
};
