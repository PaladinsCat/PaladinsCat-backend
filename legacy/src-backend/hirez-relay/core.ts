/**
 * =====================================================================
 * hirez.ts - Hi-Rez API Client (Primary Entry Point)
 * =====================================================================
 * Purpose: The main client for all Hi-Rez API calls. Handles session
 * acquisition, request signing, API calls, error handling, and response
 * normalization. Every Hi-Rez endpoint goes through this file - it is
 * the single point of contact between PaladinsCat and Hi-Rez servers.
 *
 * Architecture:
 * - apiRequest(): Core internal function. Acquires session via
 *   sessionManager.getActiveSession(), signs the request using
 *   sessionManager.sign()/timestamp(), makes the fetch, handles
 *   "Invalid session" by calling sessionManager.invalidateSession().
 *   All public functions delegate to this.
 * - Public functions: One per Hi-Rez endpoint (getMatchDetailsBatch,
 *   getMatchIdsByQueue, getMatchIdsByPlayer, getPlayerBatch, etc.).
 *   Each wraps apiRequest() with endpoint-specific params and normalizes
 *   the response using normalizer.ts functions.
 * - Error handling: "Invalid session" → invalidate + retry once.
 *   ret_msg errors → throw with message. Network errors → propagate.
 * - Usage tracking: incrementUsage() called after every successful API
 *   call (including createsession). recordSuccess() on success, no
 *   recordFailure() here (handled by caller via catch).
 *
 * Called by:
 * - workers/match-discovery.ts - fetches match IDs by queue/player.
 * - workers/buffer-processor.ts - fetches match details, player details.
 * - routes/* - API endpoints that proxy Hi-Rez data (leaderboards, etc.).
 * - scripts/* - one-time data ingestion scripts.
 *
 * Fixed 2026-05-30:
 * - All 12 individual functions now use apiRequest() internally instead
 *   of duplicating session/sign/fetch logic. Removed refreshSession()
 *   from all individual catch blocks - unified in apiRequest().
 * - incrementUsage() added after every successful API call.
 * - recordSuccess() added on success for key health tracking.
 * - invalidateSession() replaces refreshSession() on "Invalid session"
 *   - lazy delete instead of eager recreate (saves createsession calls).
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import { query, one } from '../config/db';
import { apiKeyPool } from '../services/api-key-pool';
import { sessionManager } from '../services/session-manager';
import { API_CONFIG, FIELD_MAP, TIMEOUT_MS } from '../config/api';
import { dispatchDummyApiRequest } from './dummy-data';
import { normalizeMatchPlayer, normalizeMatchHistoryPlayer, extractMatchMetadata, detectEndpointSource, roundTo2, normalizeRegion, normalizePlayerProfile, NormalizedPlayer } from '../services/normalizer';
import { shouldPreserveBrokenSkinBatchResponse } from '../services/batch-int16';
import {
  resolveDirectMatchScore,
  resolveRecoveredMatchScoreSources,
  resolveHistoryMatchScore,
  MatchScoreInput,
} from '../services/ranked-score';
import { extractMatchBanFields } from '../utils/match-bans';
import { upsertPlayerProfile } from '../services/player-profile-store';
import {
  getMatchHistoryRequestParams,
  getPlayerLoadoutRequestParams,
} from '../contracts/hirez-request-params';
import { PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES } from '../services/player-history-policy';
import { currentRelayConsumer } from './request-context';
import { isVariableHumanRosterQueue } from '../services/match-participant-policy';
import type {
  CompletedMatchRequest,
  CompletedMatchResolution,
} from '../contracts/hirez-relay';

interface MatchDetails {
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
  direct_score_observations?: MatchScoreInput[];
  has_replay: boolean;
  recovery_source?: string;
  recovery_api_calls?: number;
  recovery_attempted?: boolean;
  recovery_terminal?: boolean;
  recovery_pending?: boolean;
  limited?: boolean;
  players: PlayerDetails[];
}

interface PlayerDetails {
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
  damage_player?: number;
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
  history_team1_score?: number | null;
  history_team2_score?: number | null;
  history_winning_task_force?: number | null;
}

// Endpoint timeout overrides (ms). Default is TIMEOUT_MS.
const ENDPOINT_TIMEOUTS: Record<string, number> = {
  getgods: 10000,
  getitems: 10000,
  getchampions: 10000,
  searchplayers: 10000,
  getqueuestats: 10000,
  getbountyitems: 10000,
  getmatchdetails: 10000,
  getmatchhistory: 10000,
  getplayeridbyname: 10000,
  getplayerloadouts: 10000,
  getplayerstatus: 10000,
  getmatchplayerdetails: 10000,
  getplayeridsbygamertag: 10000,
  getplayeridbyportaluserid: 10000,
  getfriends: 20000,
  getplayerbatch: 20000,
  getmatchidsbyqueue: 20000,
  getmatchdetailsbatch: 20000,
  getchampionskins: 30000,
};

/**
 * CRITICAL: In-memory deduplication for recoverBrokenMatch.
 *
 * When multiple broken matches share the same missing players,
 * this prevents redundant getmatchhistory API calls by caching
 * player histories across recovery invocations in this relay process.
 * The durable cross-batch/restart guard lives in player_match_history_cache;
 * this map is only the fast in-process layer and is cleared between
 * buffer-processor cycles via cleanupFetchedPlayersCache().
 */
let globalFetchedPlayers: Map<number, { matches: any[] }> | null = null;

type RecoveryHistoryResult = {
  playerId: number;
  data: { matches: any[] };
  success: boolean;
  source: 'memory' | 'database' | 'api';
};

function positiveIntFromEnv(name: string, fallback: number): number {
  const value = Number(process.env[name] ?? fallback);
  return Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;
}

// The Hi-Rez player history endpoint returns only the recent rolling history
// window. We therefore cache the whole returned history by player, but we do not
// use raw_ingest_buffer as that cache. raw_ingest_buffer is work to drain; using
// it as a memory store is what made recovery fan-out turn into a 45k+ row
// backlog. This table is a compact per-player cache used to ensure a player
// history is fetched at most once during the freshness window.
const PLAYER_HISTORY_CACHE_TTL_HOURS = positiveIntFromEnv('RECOVERY_PLAYER_HISTORY_CACHE_TTL_HOURS', 24);
const PLAYER_HISTORY_CACHE_SOURCE = 'getmatchhistory-v2';

// Full player histories are retained in player_match_history_cache for recovery
// lookups. Individual player-match observations are stored in
// player_match_history_entries, not raw_ingest_buffer and not match_players.
// This keeps getmatchhistory data available for DB-first recovery/player UI
// while preventing partial history rows from becoming ingest work.
let playerHistoryCacheReady = false;

function historyMatchesFromData(data: any): any[] {
  const rawMatches = Array.isArray(data) ? data : data?.matches;
  return Array.isArray(rawMatches) ? rawMatches : [];
}

function historyMatchId(entry: any): number {
  return Number(entry?.Match || entry?.match_id || entry?.MatchId || 0);
}

function historyPlayerId(entry: any, fallbackPlayerId = 0): number {
  return Number(entry?.ActivePlayerId || entry?.player_id || entry?.playerId || entry?.Id || fallbackPlayerId || 0);
}

function nullableHistoryNumber(...values: unknown[]): number | null {
  for (const value of values) {
    if (value === undefined || value === null || value === '') continue;
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

function attachHistoryScoreFields(normalized: any, entry: any): void {
  normalized.history_team1_score = nullableHistoryNumber(
    normalized.history_team1_score,
    entry?.history_team1_score,
    entry?.Team1Score,
    entry?.Team1_Score,
  );
  normalized.history_team2_score = nullableHistoryNumber(
    normalized.history_team2_score,
    entry?.history_team2_score,
    entry?.Team2Score,
    entry?.Team2_Score,
  );
  normalized.history_winning_task_force = nullableHistoryNumber(
    normalized.history_winning_task_force,
    entry?.history_winning_task_force,
    entry?.Winning_TaskForce,
    entry?.Winning_Task_Force,
  );
}

function historyScoreObservations(players: Iterable<any>): MatchScoreInput[] {
  return [...players].map(player => ({
    team1: player?.history_team1_score,
    team2: player?.history_team2_score,
    winner: player?.history_winning_task_force,
  }));
}

function directScoreObservations(meta: any): MatchScoreInput[] {
  if (!Array.isArray(meta?.direct_score_observations)) return [];
  return meta.direct_score_observations.map((observation: any) => ({
    team1: observation?.team1 ?? observation?.team1_score ?? observation?.Team1Score ?? observation?.Team1_Score,
    team2: observation?.team2 ?? observation?.team2_score ?? observation?.Team2Score ?? observation?.Team2_Score,
    winner: observation?.winner ?? observation?.winning_task_force ?? observation?.Winning_TaskForce ?? observation?.Winning_Task_Force,
  }));
}

function applyExactHistoryScore(meta: any, players: Iterable<any>): any {
  if (!meta) return meta;
  const score = resolveDirectMatchScore(directScoreObservations(meta))
    ?? resolveHistoryMatchScore(historyScoreObservations(players));
  if (!score) return {
    ...meta,
    team1_score: null,
    team2_score: null,
    winning_task_force: null,
    score_source: null,
  };
  return {
    ...meta,
    team1_score: score.team1,
    team2_score: score.team2,
    winning_task_force: score.winner,
    score_source: score.source,
    score_recovered: false,
  };
}

function normalizeHistoryEntryForRecovery(entry: any, matchId: number, fallbackPlayerId = 0, fallbackEntryDatetime = ''): PlayerDetails | null {
  const source = String(entry?.source || '');
  const looksAlreadyNormalized =
    Boolean(entry?.match_id || entry?.player_id) &&
    ['prefetch', 'recovered', 'match_history', 'history_observation', 'legacy_prefetch'].includes(source);
  const normalized: any = looksAlreadyNormalized ? { ...entry } : normalizeMatchHistoryPlayer(entry);
  const normalizedMatchId = Number(normalized.match_id || historyMatchId(entry));
  const playerId = Number(normalized.player_id || historyPlayerId(entry, fallbackPlayerId));

  if (normalizedMatchId !== matchId || !playerId || playerId <= 0) return null;

  normalized.match_id = matchId;
  normalized.player_id = playerId;
  normalized.entry_datetime = normalized.entry_datetime || fallbackEntryDatetime;
  normalized.source = 'recovered';
  normalized.has_ret_msg = false;
  attachHistoryScoreFields(normalized, entry);
  return normalized as PlayerDetails;
}

function historyEntryTime(entry: any): Date | null {
  const raw = entry?.Match_Time || entry?.entry_datetime || entry?.Entry_Datetime || entry?.match_time;
  if (!raw) return null;
  const date = new Date(String(raw));
  return Number.isFinite(date.getTime()) ? date : null;
}

function historyBufferEntityId(matchId: number, playerId: number): string {
  return `${matchId}:${playerId}`;
}

async function ensurePlayerHistoryCacheTable(): Promise<void> {
  if (playerHistoryCacheReady) return;

  await query(`
    CREATE TABLE IF NOT EXISTS player_match_history_cache (
      player_id BIGINT PRIMARY KEY,
      raw_data JSONB NOT NULL,
      match_ids BIGINT[] NOT NULL DEFAULT '{}',
      fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      expires_at TIMESTAMPTZ NOT NULL,
      source VARCHAR(30) NOT NULL DEFAULT 'getmatchhistory'
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_player_match_history_cache_expires
      ON player_match_history_cache (expires_at)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_player_match_history_cache_match_ids
      ON player_match_history_cache USING GIN (match_ids)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_rib_match_history_entity_status
      ON raw_ingest_buffer (entity_id, status)
      WHERE entity_type = 'match_history' AND COALESCE(entity_id, '') <> ''
  `);

  await query(`
    CREATE TABLE IF NOT EXISTS player_match_history_entries (
      match_id BIGINT NOT NULL,
      player_id BIGINT NOT NULL,
      fetched_player_id BIGINT,
      entry_datetime TIMESTAMPTZ,
      queue_id INT,
      region VARCHAR(50),
      map VARCHAR(200),
      champion_id INT,
      champion_name VARCHAR(100),
      skin_id INT,
      skin_name VARCHAR(100),
      win_status VARCHAR(20),
      kills INT DEFAULT 0,
      deaths INT DEFAULT 0,
      assists INT DEFAULT 0,
      damage INT DEFAULT 0,
      healing INT DEFAULT 0,
      gold_earned INT DEFAULT 0,
      time_in_match INT DEFAULT 0,
      task_force SMALLINT DEFAULT 0,
      league_tier INT DEFAULT 0,
      source VARCHAR(30) NOT NULL DEFAULT 'getmatchhistory',
      raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
      normalized_data JSONB NOT NULL DEFAULT '{}'::jsonb,
      observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      expires_at TIMESTAMPTZ,
      PRIMARY KEY (match_id, player_id)
    )
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_pmhe_player_time
      ON player_match_history_entries (player_id, entry_datetime DESC)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_pmhe_fetched_player_expires
      ON player_match_history_entries (fetched_player_id, expires_at DESC)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_pmhe_match
      ON player_match_history_entries (match_id)
  `);

  await query(`
    CREATE INDEX IF NOT EXISTS idx_pmhe_queue_time
      ON player_match_history_entries (queue_id, entry_datetime DESC)
  `);

  playerHistoryCacheReady = true;
}

async function readCachedPlayerHistory(playerId: number, targetMatchId: number, targetEntryTime: Date | null): Promise<any[] | null> {
  await ensurePlayerHistoryCacheTable();
  const rows = await query(
    `SELECT raw_data, match_ids, fetched_at
     FROM player_match_history_cache
     WHERE player_id = $1
       AND expires_at > now()`,
    [playerId],
  );
  if (rows.length === 0) return null;
  const row = rows[0];
  const matches = historyMatchesFromData(row.raw_data);
  const cachedMatchIds = new Set((row.match_ids || []).map((id: any) => Number(id)));
  if (cachedMatchIds.has(targetMatchId)) return matches;

  // Recovery must maximize fresh broken-match capture before players roll out
  // of Hi-Rez's 50-match history window. A cached history that *contains* the
  // target match is authoritative and reusable. A cached history that does not
  // contain the target is only a negative observation, and negative observations
  // are unsafe for fresh recovery because getmatchhistory can lag behind match
  // completion. Treat target-absent cache rows as misses so the next debt retry
  // can spend one more targeted getmatchhistory call instead of waiting on a
  // stale "not found" cache until the match disappears from every player.
  //
  // This deliberately differs from public player-history TTL behavior, where a
  // fresh empty/no-history response should suppress page-refresh API burns. The
  // recovery path is narrower and higher value: it only asks for specific
  // anchored players from a known ranked match debt row.
  void targetEntryTime;
  return null;
}

async function readFreshPlayerHistoryCache(playerId: number): Promise<any[] | null> {
  await ensurePlayerHistoryCacheTable();
  const rows = await query(
    `SELECT raw_data, source
     FROM player_match_history_cache
     WHERE player_id = $1
       AND fetched_at >= now() - ($2::int * interval '1 minute')
       AND expires_at > now()`,
    [playerId, PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES],
  );
  if (rows.length === 0) return null;
  const matches = historyMatchesFromData(rows[0].raw_data);
  // Before v2 the relay appended a non-existent limit path segment. Hi-Rez
  // answered 404 and the public path cached that as an empty result. Refresh
  // only those poisoned empty rows once; valid non-empty legacy caches remain
  // reusable and legitimate v2 empty histories still suppress quota burn.
  if (matches.length === 0 && rows[0].source !== PLAYER_HISTORY_CACHE_SOURCE) return null;
  return matches;
}

async function writeCachedPlayerHistory(playerId: number, matches: any[]): Promise<void> {
  await ensurePlayerHistoryCacheTable();
  const matchIds = [...new Set(matches.map(historyMatchId).filter(id => Number.isFinite(id) && id > 0))];
  const rawJson = jsonForDb(matches ?? []);

  await one(
    `INSERT INTO player_match_history_cache (player_id, raw_data, match_ids, fetched_at, expires_at, source)
     VALUES ($1, $2::jsonb, $3::bigint[], now(), now() + ($4::int * interval '1 hour'), $5)
     ON CONFLICT (player_id) DO UPDATE SET
       raw_data = EXCLUDED.raw_data,
       match_ids = EXCLUDED.match_ids,
       fetched_at = EXCLUDED.fetched_at,
       expires_at = EXCLUDED.expires_at,
       source = EXCLUDED.source`,
    [playerId, rawJson, matchIds, PLAYER_HISTORY_CACHE_TTL_HOURS, PLAYER_HISTORY_CACHE_SOURCE],
  );

  await writePlayerMatchHistoryEntries(playerId, matches, 'getmatchhistory');
}

function jsonForDb(value: any): string {
  // PostgreSQL jsonb refuses the escaped NUL sequence `\u0000` even though it
  // appears as ordinary text after JSON.stringify(). Hi-Rez occasionally sends
  // player/history strings containing NUL bytes; if we only strip the actual
  // character, JSON.stringify(value) turns it into the literal six-character
  // escape and the INSERT fails with "unsupported Unicode escape sequence".
  //
  // Recovery depends on player_match_history_cache/player_match_history_entries
  // as the durable evidence source for broken ranked matches. A storage-only
  // sanitizer failure must not leave a recoverable match stuck in pending debt
  // while the 50-match history window ages out, so every JSONB write in this
  // relay path uses this helper.
  return JSON.stringify(value ?? {})
    .replace(/\u0000/g, '')
    .replace(/\\u0000/g, '');
}

function timestampOrNull(value: unknown): string | null {
  if (!value) return null;
  const date = new Date(String(value));
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

function normalizeHistoryEntryForStorage(entry: any, fetchedPlayerId: number): any | null {
  const source = String(entry?.source || '').toLowerCase();
  const alreadyNormalized =
    Boolean(entry?.match_id || entry?.player_id) &&
    ['prefetch', 'recovered', 'match_history', 'history_observation', 'legacy_prefetch'].includes(source);
  const normalized: any = alreadyNormalized ? { ...entry } : normalizeMatchHistoryPlayer(entry);
  const matchId = Number(normalized.match_id || historyMatchId(entry));
  const playerId = Number(normalized.player_id || historyPlayerId(entry, fetchedPlayerId));
  if (!Number.isFinite(matchId) || matchId <= 0 || !Number.isFinite(playerId) || playerId <= 0) {
    return null;
  }

  normalized.match_id = matchId;
  normalized.player_id = playerId;
  normalized.entry_datetime = normalized.entry_datetime || entry?.Match_Time || entry?.Entry_Datetime || '';
  normalized.queue_id = Number(normalized.queue_id || entry?.Match_Queue_Id || entry?.Queue || 0);
  normalized.map = normalized.map || entry?.Map_Game || entry?.Map || '';
  normalized.region = normalizeRegion(normalized.region || entry?.Region || '');
  normalized.champion_id = Number(normalized.champion_id || entry?.ChampionId || 0);
  normalized.champion_name = normalized.champion_name || entry?.Champion || entry?.ChampionName || '';
  normalized.skin_id = Number(normalized.skin_id || entry?.SkinId || 0);
  normalized.skin_name = normalized.skin_name || entry?.Skin || '';
  normalized.source = 'match_history';
  normalized.has_ret_msg = false;
  return normalized;
}

async function writePlayerMatchHistoryEntries(
  fetchedPlayerId: number,
  matches: any[],
  source = 'getmatchhistory',
): Promise<number> {
  await ensurePlayerHistoryCacheTable();
  if (!Array.isArray(matches) || matches.length === 0) return 0;

  let written = 0;
  for (const rawEntry of matches) {
    if ((rawEntry?.ret_msg || '').trim()) continue;
    const normalized = normalizeHistoryEntryForStorage(rawEntry, fetchedPlayerId);
    if (!normalized) continue;

    // `damage_done_physical` is the historical canonical column for the
    // endpoint's total player damage. Magical damage, when present, is a
    // breakdown field and must not be added to the total.
    const damage = Number(normalized.damage_done_physical || 0);
    await one(
      `INSERT INTO player_match_history_entries (
         match_id, player_id, fetched_player_id, entry_datetime, queue_id, region, map,
         champion_id, champion_name, skin_id, skin_name, win_status,
         kills, deaths, assists, damage, healing, gold_earned, time_in_match,
         task_force, league_tier, source, raw_data, normalized_data, observed_at, expires_at
       )
       VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,
         $23::jsonb,$24::jsonb,now(),now() + ($25::int * interval '1 hour')
       )
       ON CONFLICT (match_id, player_id) DO UPDATE SET
         fetched_player_id = EXCLUDED.fetched_player_id,
         entry_datetime = COALESCE(EXCLUDED.entry_datetime, player_match_history_entries.entry_datetime),
         queue_id = COALESCE(EXCLUDED.queue_id, player_match_history_entries.queue_id),
         region = COALESCE(EXCLUDED.region, player_match_history_entries.region),
         map = COALESCE(NULLIF(EXCLUDED.map, ''), player_match_history_entries.map),
         champion_id = COALESCE(EXCLUDED.champion_id, player_match_history_entries.champion_id),
         champion_name = COALESCE(NULLIF(EXCLUDED.champion_name, ''), player_match_history_entries.champion_name),
         skin_id = COALESCE(EXCLUDED.skin_id, player_match_history_entries.skin_id),
         skin_name = COALESCE(NULLIF(EXCLUDED.skin_name, ''), player_match_history_entries.skin_name),
         win_status = COALESCE(NULLIF(EXCLUDED.win_status, ''), player_match_history_entries.win_status),
         kills = EXCLUDED.kills,
         deaths = EXCLUDED.deaths,
         assists = EXCLUDED.assists,
         damage = EXCLUDED.damage,
         healing = EXCLUDED.healing,
         gold_earned = EXCLUDED.gold_earned,
         time_in_match = EXCLUDED.time_in_match,
         task_force = EXCLUDED.task_force,
         league_tier = EXCLUDED.league_tier,
         source = EXCLUDED.source,
         raw_data = EXCLUDED.raw_data,
         normalized_data = EXCLUDED.normalized_data,
         observed_at = EXCLUDED.observed_at,
         expires_at = EXCLUDED.expires_at`,
      [
        normalized.match_id,
        normalized.player_id,
        fetchedPlayerId || normalized.player_id,
        timestampOrNull(normalized.entry_datetime),
        Number(normalized.queue_id || 0) || null,
        normalized.region || null,
        normalized.map || null,
        Number(normalized.champion_id || 0) || null,
        normalized.champion_name || null,
        Number(normalized.skin_id || 0) || null,
        normalized.skin_name || null,
        normalized.win_status || null,
        Number(normalized.kills || 0),
        Number(normalized.deaths || 0),
        Number(normalized.assists || 0),
        damage,
        Number(normalized.healing || 0),
        Number(normalized.gold_earned || 0),
        Number(normalized.time_in_match || normalized.match_duration || 0),
        Number(normalized.task_force || 0),
        Number(normalized.league_tier || 0),
        source,
        jsonForDb(rawEntry),
        jsonForDb(normalized),
        PLAYER_HISTORY_CACHE_TTL_HOURS,
      ],
    );
    written++;
  }

  return written;
}

function historyEntryRowToRecoveryPlayer(row: any, fallbackEntryDatetime = ''): PlayerDetails | null {
  const normalizedData = row.normalized_data && typeof row.normalized_data === 'object'
    ? row.normalized_data
    : {};
  const rawData = row.raw_data && typeof row.raw_data === 'object' ? row.raw_data : {};
  const entryDatetime = row.entry_datetime
    ? new Date(row.entry_datetime).toISOString()
    : fallbackEntryDatetime;
  const player = normalizeHistoryEntryForRecovery(
    {
      ...rawData,
      ...normalizedData,
      match_id: Number(row.match_id),
      player_id: Number(row.player_id),
      entry_datetime: normalizedData.entry_datetime || entryDatetime,
      queue_id: normalizedData.queue_id || row.queue_id || 0,
      champion_id: normalizedData.champion_id || row.champion_id || 0,
      champion_name: normalizedData.champion_name || row.champion_name || '',
      skin_id: normalizedData.skin_id || row.skin_id || 0,
      skin_name: normalizedData.skin_name || row.skin_name || '',
      source: 'match_history',
    },
    Number(row.match_id),
    Number(row.player_id),
    entryDatetime,
  );
  if (!player) return null;
  player.source = 'recovered';
  player.queue_id = Number(player.queue_id || row.queue_id || 0);
  player.region = normalizeRegion(player.region || row.region || '');
  return player;
}

async function readPlayerMatchHistoryEntries(
  matchId: number,
  playerIds: number[] = [],
  fallbackEntryDatetime = '',
): Promise<Map<number, PlayerDetails>> {
  await ensurePlayerHistoryCacheTable();
  const wantedIds = [...new Set(playerIds.map(Number).filter(id => Number.isFinite(id) && id > 0))];
  const params: any[] = [matchId];
  let playerFilter = '';
  if (wantedIds.length > 0) {
    params.push(wantedIds);
    playerFilter = `AND player_id = ANY($2::bigint[])`;
  }

  const rows = await query(
    `SELECT *
     FROM player_match_history_entries
     WHERE match_id = $1
       ${playerFilter}
       AND (expires_at IS NULL OR expires_at > now())
     ORDER BY observed_at DESC`,
    params,
  );

  const playersById = new Map<number, PlayerDetails>();
  for (const row of rows) {
    const player = historyEntryRowToRecoveryPlayer(row, fallbackEntryDatetime);
    if (!player) continue;
    if (wantedIds.length > 0 && !wantedIds.includes(player.player_id)) continue;
    if (!playersById.has(player.player_id)) {
      playersById.set(player.player_id, player);
    }
  }

  return playersById;
}

async function readActiveBufferedHistoryPlayers(
  matchId: number,
  playerIds: number[] = [],
  fallbackEntryDatetime = '',
): Promise<Map<number, PlayerDetails>> {
  const wantedIds = new Set(playerIds.map(Number).filter(id => Number.isFinite(id) && id > 0));
  const compositeEntityIds = [...wantedIds].map(playerId => historyBufferEntityId(matchId, playerId));
  const rows = await query(
    `SELECT entity_id, raw_data
     FROM raw_ingest_buffer
     WHERE status IN ('pending', 'processing')
       AND endpoint IN ('getmatchhistory', 'getplayermatchhistory', 'getplayermatchhistoryafterdatetime')
       AND entity_type IN ('match_history', 'prefetch_match', 'match')
       AND (
         entity_id = ANY($1::text[])
         OR entity_id = $2
         OR entity_id LIKE $3
       )
     ORDER BY created_at ASC`,
    [compositeEntityIds, String(matchId), `${matchId}:%`],
  );

  const playersById = new Map<number, PlayerDetails>();
  for (const row of rows) {
    const entityId = String(row.entity_id || '');
    const entityPlayerId = entityId.includes(':') ? Number(entityId.split(':')[1]) : 0;
    const entries = Array.isArray(row.raw_data) ? row.raw_data : [row.raw_data];

    for (const entry of entries) {
      const player = normalizeHistoryEntryForRecovery(entry, matchId, entityPlayerId, fallbackEntryDatetime);
      if (!player) continue;
      if (wantedIds.size > 0 && !wantedIds.has(player.player_id)) continue;
      if (!playersById.has(player.player_id)) {
        playersById.set(player.player_id, player);
      }
    }
  }

  return playersById;
}

/**
 * Clear the fetched players cache.
 * Call this at the start of each buffer-processor cycle to ensure
 * the short-lived in-memory map cannot leak stale objects between
 * processor passes. This does NOT clear player_match_history_cache;
 * that table is the durable quota guard that prevents refetching the
 * same player's 50-match history across batches, restarts, and cron
 * retries while the cache TTL is valid.
 */
export function cleanupFetchedPlayersCache() {
  globalFetchedPlayers = null;
}

const MAX_RETRIES = 3;

type ApiRequestOptions = {
  timeoutMs?: number;
  maxRetries?: number;
};

const SINGLE_ATTEMPT_LOOKUP: ApiRequestOptions = { maxRetries: 0 };

type HirezRetMsgAction = 'session' | 'quota' | 'empty' | 'retry' | 'terminal' | 'unknown';

interface HirezRetMsgClassification {
  action: HirezRetMsgAction;
  code: string;
}

function classifyRetMsg(retMsg: string): HirezRetMsgClassification {
  const msg = retMsg.toLowerCase();
  if (msg.includes('invalid session')) return { action: 'session', code: 'HIREZ_INVALID_SESSION' };
  if (msg.includes('daily request limit')) return { action: 'quota', code: 'HIREZ_DAILY_LIMIT' };
  if (msg.includes('no match history')) return { action: 'empty', code: 'HIREZ_NO_MATCH_HISTORY' };
  if (msg.includes('privacy flag') || msg.includes('private')) return { action: 'empty', code: 'HIREZ_PRIVACY_FLAG' };
  if (msg.includes('not found') || msg.includes('invalid player') || msg.includes('invalid match')) {
    return { action: 'terminal', code: 'HIREZ_NOT_FOUND_OR_INVALID' };
  }
  if (msg.includes('exception') || msg.includes('maintenance') || msg.includes('temporarily') || msg.includes('timeout')) {
    return { action: 'retry', code: 'HIREZ_RETRYABLE_RETURN' };
  }
  return { action: 'unknown', code: 'HIREZ_UNKNOWN_RETURN' };
}

async function handleRetMsg(
  retMsg: string,
  keyDevId: string,
  invalidateSession: () => Promise<void>,
  syncUsage: () => Promise<void>,
): Promise<'continue' | 'empty'> {
  // Hi-Rez reports many application states through HTTP 200 + ret_msg instead
  // of normal HTTP status codes. Keep the classification here in HirezRelay so
  // backend workers do not need to know which strings mean quota, privacy,
  // session expiry, or transient maintenance. A classified return also gives
  // operators a stable code in logs instead of a one-off vendor phrase.
  const classification = classifyRetMsg(retMsg);
  switch (classification.action) {
    case 'session':
      await invalidateSession();
      return 'continue';
    case 'quota':
      await syncUsage();
      return 'continue';
    case 'empty':
      console.warn(`[Hi-Rez] ${classification.code} for key ${keyDevId}: ${retMsg}`);
      return 'empty';
    case 'retry':
      throw new Error(`${classification.code}: ${retMsg}`);
    case 'terminal':
    case 'unknown':
      throw new Error(`${classification.code}: ${retMsg}`);
  }
}

async function fetchJSON(url: string, timeoutMs?: number): Promise<any> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs ?? TIMEOUT_MS);

  try {
    // CRITICAL: fetch() may throw on network failure, DNS error, or abort.
    // Without try/finally, clearTimeout(timer) below is bypassed on throw.
    // Under heavy load with spotty connections, thousands of orphaned
    // timeout objects accumulate in the Node.js event loop heap, eventually
    // causing an Out of Memory (OOM) crash. The finally block guarantees
    // the timer is cleared regardless of how the function exits.
    // Source: Debug 2026-05-31 — "fetchJSON memory/handle leak"
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`);
    // Strip UTF-8 BOM if present (matches Python behavior)
    const text = await res.text();
    const cleaned = text.startsWith('\ufeff') ? text.slice(1) : text;
    // CRITICAL: Wrap JSON.parse in try-catch. Hi-Rez may return a non-JSON
    // response with HTTP 200 (e.g., HTML error page during maintenance,
    // plain text "Service Unavailable", or malformed JSON mid-write).
    // Without this guard, JSON.parse throws SyntaxError on every retry,
    // wasting all MAX_RETRIES+1 attempts on a parse error instead of
    // failing fast. The error message distinguishes parse failures from
    // network errors so the caller can handle them differently.
    // Source: Fault #8 — "fetchJSON no JSON parse guard"
    try {
      return JSON.parse(cleaned);
    } catch (parseErr) {
      throw new Error(`JSON parse error from Hi-Rez: ${parseErr instanceof Error ? parseErr.message : parseErr}`);
    }
  } finally {
    // Guaranteed to execute — clears the timeout timer regardless of
    // how the function exits (success, network throw, abort, parse error).
    // Prevents orphaned timeout objects from accumulating in the event loop.
    clearTimeout(timer);
  }
}

/**
 * Save a raw API response to the database for debugging and analysis.
 */
function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Core API request function with retry logic and exponential backoff.
 * Fetches the active session DYNAMICALLY inside the retry loop so that
 * key rotation (waterfall) works correctly on "Daily request limit" errors.
 *
 * CRITICAL ARCHITECTURAL CHANGE (2026-05-30):
 * Previously, `key` was a function parameter. On "Daily request limit",
 * the code called `await apiKeyPool.loadKeys()` then `continue;` - but
 * `key` was a local variable, so it kept pointing to the SAME exhausted
 * key. The retry loop reused the same key → infinite daily limit loop →
 * request permanently failed after MAX_RETRIES.
 *
 * FIX: Remove `key` parameter entirely. Call `sessionManager.getActiveSession()`
 * inside the loop. On daily limit, `continue;` triggers a fresh
 * `getActiveSession()` call on the next iteration, which runs waterfall
 * selection and returns a DIFFERENT key. Daily limit rotation now works.
 *
 * Also fixes "incrementUsage Drift Trap": previously, incrementUsage() was
 * called AFTER fetchJSON(). If fetchJSON threw (HTTP 500/404/network), the
 * increment was skipped, but Hi-Rez still deducted the call. Internal memory
 * permanently drifted lower than Hi-Rez's actual count. Now, incrementUsage()
 * fires BEFORE fetchJSON() - usage tracked even on HTTP errors.
 *
 * Source: Feedback 2026-05-30 - "Daily Limit Infinite Loop" (Claim 2),
 * "Architectural Disconnect" (Claim 4), "incrementUsage Drift Trap" (Claim 5).
 *
 * @param method - Hi-Rez endpoint name (e.g., "getmatchdetailsbatch").
 * @param params - URL path parameters (e.g., [matchIds.join(',')]).
 * @param options - Optional timeout/retry override (falls back to endpoint defaults).
 * @returns Parsed JSON response.
 * @throws Error if request fails after the configured attempt budget.
 */
async function apiRequest(
  method: string,
  params: string[],
  options?: number | ApiRequestOptions
): Promise<any> {
  if (process.env.HIREZ_RELAY_MODE !== 'real') {
    // Dummy mode must never acquire sessions, sign requests, or touch real
    // Hi-Rez keys. Keep this branch inside apiRequest instead of bypassing
    // recovery at the public dispatcher layer so the canonical match lookup
    // still executes the production algorithm: get all player IDs, consult local DB
    // recovered/history rows, reuse in-cycle cache, and only then synthesize the missing
    // endpoint calls. The dummy provider records per-endpoint counts, which is
    // how the broken-skin regression test verifies that DB-first history logic
    // suppresses unnecessary getmatchhistory calls.
    const startTime = Date.now();
    const data = await dispatchDummyApiRequest(method, params);
    console.log(`[HirezRelay:dummy-api] ${method} params=${JSON.stringify(params)} latencyMs=${Date.now() - startTime}`);
    return data;
  }

  const requestOptions: ApiRequestOptions = typeof options === 'number'
    ? { timeoutMs: options }
    : (options ?? {});
  const timeout = requestOptions.timeoutMs ?? ENDPOINT_TIMEOUTS[method] ?? TIMEOUT_MS;
  const maxRetries = requestOptions.maxRetries ?? MAX_RETRIES;
  let lastError: any = null;
  let lastAppErrorMsg: string = ''; // Track application-level errors that survive loop fall-through
  let lastKeyDevId: string | null = null; // Track key for failure recording below
  let attemptsMade = 0;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    attemptsMade = attempt + 1;
    let countedDevId: string | null = null;
    let responseTimeMs = 0;
    let endpointLogged = false;
    let requestStartTime = 0;

    const logCountedAttempt = async () => {
      if (!countedDevId || endpointLogged) return;
      await apiKeyPool.logEndpoint(countedDevId, method, responseTimeMs, currentRelayConsumer());
      endpointLogged = true;
    };

    try {
      // ----------------------------------------------------------------
      // CRITICAL: Fetch active session INSIDE the loop. On "Daily request
      // limit" continue;, the next iteration calls getActiveSession() again,
      // which triggers waterfall selection and returns a DIFFERENT key.
      // Previously, key was a parameter - continue; reused the same exhausted key.
      // Source: "Daily Limit Infinite Loop" fix.
      // ----------------------------------------------------------------
      const { apiKey: key, session } = await sessionManager.getActiveSession();
      lastKeyDevId = key.devId; // Track for failure recording at end of function

      const ts = sessionManager.timestamp();
      const sig = sessionManager.sign(method, key.devId, key.authKey, ts);

      let url: string;
      if (params.length > 0) {
        url = `${API_CONFIG.BASE_URL}/${method}Json/${key.devId}/${sig}/${session.sessionKey}/${ts}/${params.join('/')}`;
      } else {
        url = `${API_CONFIG.BASE_URL}/${method}Json/${key.devId}/${sig}/${session.sessionKey}/${ts}`;
      }

      // ----------------------------------------------------------------
      // CRITICAL: Increment usage BEFORE the fetch. If fetchJSON throws
      // (HTTP 500, 404, network error), the request still reached Hi-Rez
      // and burned a call. Skipping increment causes permanent drift
      // between internal memory and Hi-Rez's actual count.
      // Source: "incrementUsage Drift Trap" fix.
      // ----------------------------------------------------------------
      apiKeyPool.incrementUsage(key.devId);
      countedDevId = key.devId;

      requestStartTime = Date.now();
      const data = await fetchJSON(url, timeout);
      responseTimeMs = Date.now() - requestStartTime;

      // Handle list responses (check first item for error messages)
      if (Array.isArray(data)) {
        if (data.length > 0) {
          const retMsgs = data.map((item: any) => item.ret_msg || '').filter(Boolean);
          if (retMsgs.length > 0) {
            if (shouldPreserveBrokenSkinBatchResponse(method, retMsgs)) {
              // Keep the healthy prefix and the broken-match sentinel. The
              // normalizer groups both by match_id, allowing discovery to
              // checkpoint healthy matches and stage only the incomplete
              // blocker for the buffer worker's targeted recovery path.
              await logCountedAttempt();
              await apiKeyPool.recordSuccess(key.devId);
              console.warn(
                `[Hi-Rez] Preserving partial ${method} response after broken-skin Int16 sentinel; ` +
                `ordered discovery will continue behind the blocker`,
              );
              return data;
            }
            lastAppErrorMsg = retMsgs[0];
            const action = await handleRetMsg(
              retMsgs[0],
              key.devId,
              () => sessionManager.invalidateSession(key.devId),
              () => apiKeyPool.syncUsage(key.devId),
            );
            if (action === 'continue') {
              await logCountedAttempt();
              continue;
            }
            if (action === 'empty') {
              await logCountedAttempt();
              await apiKeyPool.recordSuccess(key.devId);
              return [];
            }
          }
        }
        await logCountedAttempt();
        await apiKeyPool.recordSuccess(key.devId);
        return data;
      }

      // Handle dict responses
      if (typeof data === 'object' && data !== null) {
        const retMsg = data.ret_msg || '';
        if (retMsg) {
          lastAppErrorMsg = retMsg;
          const action = await handleRetMsg(
            retMsg,
            key.devId,
            () => sessionManager.invalidateSession(key.devId),
            () => apiKeyPool.syncUsage(key.devId),
          );
          if (action === 'continue') {
            await logCountedAttempt();
            continue;
          }
          if (action === 'empty') {
            await logCountedAttempt();
            await apiKeyPool.recordSuccess(key.devId);
            return [];
          }
        }
        await logCountedAttempt();
        await apiKeyPool.recordSuccess(key.devId);
        return data;
      }

     // Handle primitive responses (string, number, boolean).
        // Hi-Rez rarely returns primitives, but some endpoints may return
        // a simple value (e.g., "OK" for health check). Without this handler,
        // primitives bypass logEndpoint and recordSuccess — the call is
        // never logged and the key is never marked as healthy.
        // Source: Fault #7 — "apiRequest skips logging for primitives"
        await logCountedAttempt();
        await apiKeyPool.recordSuccess(key.devId);
        return data;

    } catch (err: any) {
      if (countedDevId && !endpointLogged) {
        // The usage counter is incremented before fetchJSON because any request
        // that leaves the relay can burn quota even if it later returns HTTP
        // 500, times out, or trips a transport exception. Mirror that in
        // api_log so endpoint-level diagnostics account for failed outbound
        // attempts too.
        responseTimeMs = responseTimeMs || (requestStartTime ? Date.now() - requestStartTime : 0);
        await logCountedAttempt().catch((logErr) => {
          console.error(`[HirezRelay] Failed to log ${method} attempt for ${countedDevId}: ${logErr}`);
        });
      }
      lastError = err;
      // Handle 503 - return early without retrying
      if (err.message?.includes('503')) {
        // ----------------------------------------------------------------
        // CRITICAL: Throw, don't return an object. Downstream callers expect
        // apiRequest() to return an Array or throw. Returning an object causes
        // TypeError: data.map is not a function when callers do data.map().
        // Example: getMatchHistory does (data.matches || data || []).map(...).
        // Source: Feedback 2026-05-30 - "503 Type Error Crash"
        // ----------------------------------------------------------------
        throw new Error(`API temporarily unavailable (503): ${err.message}`);
      }
      if (err.message?.includes('HIREZ_NOT_FOUND_OR_INVALID') || err.message?.includes('HIREZ_UNKNOWN_RETURN')) {
        // Terminal application returns are data-quality outcomes, not network
        // flakiness. Retrying the exact same getmatchdetailsbatch Int16 skin
        // response burns quota four times and still returns the same vendor
        // error. Break immediately and let the caller choose a recovery path.
        break;
      }
      if (attempt < maxRetries) {
        // Exponential backoff: (attempt+1) * 0.5s with ±10% jitter
        const baseDelay = (attempt + 1) * 500;
        const jitter = baseDelay * (0.9 + Math.random() * 0.2);
        const delay = Math.max(100, Math.min(jitter, 10000));
        await sleep(delay);
      }
    }
  }

  // All retries exhausted - record failure for the last tried key.
  // lastKeyDevId tracks the key from the final iteration (safe even
  // if the first getActiveSession() call itself threw).
  // ----------------------------------------------------------------
  // CRITICAL: Combine network errors (lastError) with application errors
  // (lastAppErrorMsg). If the loop fell through via continue; on the last
  // iteration, lastError is null but lastAppErrorMsg holds the actual reason
  // (e.g., "Invalid session"). Without this, msg = '' → isKeyFault = true
  // → wrong failure recorded + confusing error message.
  // Source: Feedback 2026-05-30 - "apiRequest Loop Fall-Through Trap"
  // ----------------------------------------------------------------
  const msg = lastError?.message || lastAppErrorMsg || 'Unknown application error';
  const terminalVendorReturn = msg.includes('HIREZ_NOT_FOUND_OR_INVALID') || msg.includes('HIREZ_UNKNOWN_RETURN');
  const isKeyFault = !terminalVendorReturn && !msg.includes('404') && !msg.includes('422') && !msg.includes('ECONNRESET') && !msg.includes('ETIMEDOUT');
  if (lastKeyDevId) {
    await apiKeyPool.recordFailure(lastKeyDevId, isKeyFault);
  }
  throw new Error(`Request failed after ${attemptsMade} attempt${attemptsMade === 1 ? '' : 's'}: ${msg}`);
}

/**
 * Get multiple match details via batch endpoint.
 * getmatchdetailsbatch returns a flat array of player objects (one per player).
 * We group by match_id and normalize each player.
 */
function normalizeFlatMatchDetailRows(data: unknown): MatchDetails[] {
  const results: MatchDetails[] = [];

  // ----------------------------------------------------------------
  // CRITICAL: Enforce Array.isArray(). Hi-Rez occasionally returns
  // non-standard objects during maintenance (e.g., { error: "Service degraded" }).
  // data.length is undefined on objects -> undefined === 0 is false -> guard bypassed
  // -> for (const player of data) throws TypeError: data is not iterable.
  // Source: Feedback 2026-05-30 - "Iterable TypeError Crash"
  // ----------------------------------------------------------------
  if (!Array.isArray(data) || data.length === 0) return results;

  // Group flat player objects by match_id. Both detail endpoints return the
  // same flat row shape, though broken-skin recovery never repeats a failed
  // batch through the singleton endpoint.
  const byMatch = new Map<string, any[]>();
  for (const player of data) {
    const mid = String(player.match_id ?? player.Match ?? '');
    if (!mid) continue;
    if (!byMatch.has(mid)) byMatch.set(mid, []);
    byMatch.get(mid)!.push(player);
  }

  for (const [, players] of byMatch) {
    if (!players.length) continue;
    // ----------------------------------------------------------------
    // CRITICAL: extractMatchMetadata expects an ARRAY of players (any[]),
    // not a single player object. It loops through players to find the
    // first valid region. Passing a single object causes
    // "TypeError: players.find is not a function" crash.
    // Source: Feedback 2026-05-30 - "extractMatchMetadata Type Crash" (Claim 1).
    // ----------------------------------------------------------------
    const meta = extractMatchMetadata(players);

    results.push({
      match_id: meta.match_id,
      entry_datetime: meta.entry_datetime,
      map: meta.map,
      queue_id: meta.queue_id,
      duration_seconds: meta.duration_seconds,
      minutes: meta.minutes,
      region: meta.region,
      team1_score: meta.team1_score,
      team2_score: meta.team2_score,
      winning_task_force: meta.winning_task_force,
      direct_score_observations: meta.direct_score_observations,
      has_replay: meta.has_replay,
      players: players.map(normalizeMatchPlayer),
    });
  }

  return results;
}

async function getMatchDetailsBatchDirect(
  matchIds: number[],
  requestOptions?: ApiRequestOptions,
): Promise<MatchDetails[]> {
  const results: MatchDetails[] = [];
  const chunks = chunkArray(matchIds, API_CONFIG.BATCH_SIZE);

  for (const chunk of chunks) {
    // Removed redundant try-catch — apiRequest() handles all errors
    // internally (Invalid session, Daily limit, retries, backoff).
    // Re-throwing the error provides no value and misleads readers
    // into thinking there's special error handling here.
    // Source: Fault #9 — "getMatchDetailsBatch redundant try-catch"
    const data = await apiRequest('getmatchdetailsbatch', [chunk.join(',')], requestOptions);
    results.push(...normalizeFlatMatchDetailRows(data));
  }
  return results;
}

function usableDirectPlayers(match: MatchDetails): PlayerDetails[] {
  return (Array.isArray(match.players) ? match.players : []).filter(player => (
    !player?.has_ret_msg && !String(player?.ret_msg || '').trim()
  ));
}

function directMatchNeedsRecovery(match: MatchDetails): boolean {
  const usable = usableDirectPlayers(match);
  if (isVariableHumanRosterQueue(Number(match.queue_id || 0))) {
    // Hi-Rez omits AI participants from bot/PvE responses. Any usable human
    // rows are the authoritative direct response for these queues.
    return usable.length === 0;
  }
  return usable.length !== 10;
}

function mergeRelayPlayers(
  directPlayers: PlayerDetails[],
  recoveredPlayers: PlayerDetails[],
): PlayerDetails[] {
  const merged: PlayerDetails[] = [];
  const publicIds = new Set<number>();
  let privateCount = 0;

  for (const player of [...directPlayers, ...recoveredPlayers]) {
    if (player?.has_ret_msg || String(player?.ret_msg || '').trim()) continue;
    const playerId = Number(player?.player_id || 0);
    if (playerId > 0) {
      if (publicIds.has(playerId)) continue;
      publicIds.add(playerId);
      merged.push(player);
      continue;
    }

    // Recovery returns the known private direct rows again. A private player
    // has no stable ID, so retain no more private rows than the larger of the
    // direct/recovered private counts instead of duplicating them.
    const targetPrivateCount = Math.max(
      directPlayers.filter(candidate => Number(candidate?.player_id || 0) === 0 && !candidate?.has_ret_msg && !String(candidate?.ret_msg || '').trim()).length,
      recoveredPlayers.filter(candidate => Number(candidate?.player_id || 0) === 0 && !candidate?.has_ret_msg && !String(candidate?.ret_msg || '').trim()).length,
    );
    if (privateCount >= targetPrivateCount) continue;
    privateCount++;
    merged.push(player);
  }
  return merged;
}

function recoveredMatchResult(
  matchId: number,
  direct: MatchDetails | null,
  recovery: { recovered?: PlayerDetails[]; meta?: any },
): MatchDetails | null {
  const meta = recovery?.meta || {};
  const recovered = Array.isArray(recovery?.recovered) ? recovery.recovered : [];
  const directPlayers = direct ? usableDirectPlayers(direct) : [];
  const players = mergeRelayPlayers(directPlayers, recovered);
  const first = players[0] as any;
  const recoverySource = String(meta.recovery_source || 'broken_match');
  const recoveryTerminal = meta.recovery_terminal === true;
  const recoveryPending = recoverySource === 'target_history_unresolved';
  const limitedRecovery = Boolean(
    direct
    && directPlayers.length > 0
    && directPlayers.length < 10
    && (
      recoveryTerminal
      || recoverySource === 'no_player_anchors'
      || recoverySource === 'getplayerbatchfrommatch_failed'
    )
  );

  if (!direct && players.length === 0 && !recoveryPending) return null;
  if (direct && players.length < 10 && !limitedRecovery && !recoveryPending) {
    // The relay has player anchors but not enough target-history authority.
    // Preserve the exact ID as retryable worker debt; staging the direct prefix
    // would make the buffer treat an unresolved recovery as durable work.
    return null;
  }

  return {
    match_id: matchId,
    entry_datetime: String(meta.entry_datetime || direct?.entry_datetime || first?.entry_datetime || ''),
    map: String(meta.map || direct?.map || first?.map || ''),
    queue_id: Number(meta.queue_id || direct?.queue_id || first?.queue_id || 0),
    duration_seconds: Number(meta.duration_seconds || direct?.duration_seconds || first?.match_duration || 0),
    minutes: Number(meta.minutes ?? direct?.minutes ?? Math.floor(Number(meta.duration_seconds || first?.match_duration || 0) / 60)),
    region: String(meta.region || direct?.region || first?.region || 'Unknown'),
    team1_score: meta.team1_score ?? direct?.team1_score ?? null,
    team2_score: meta.team2_score ?? direct?.team2_score ?? null,
    winning_task_force: meta.winning_task_force ?? direct?.winning_task_force ?? null,
    direct_score_observations: direct?.direct_score_observations,
    has_replay: Boolean(meta.has_replay ?? direct?.has_replay ?? false),
    ...extractMatchBanFields(meta, direct, players),
    recovery_source: recoverySource,
    recovery_api_calls: Number(meta.recovery_api_calls || 0),
    recovery_attempted: true,
    recovery_terminal: recoveryTerminal,
    recovery_pending: recoveryPending,
    limited: limitedRecovery,
    players,
  };
}

function isRecoverableMatchDetailError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /HIREZ_UNKNOWN_RETURN|Int16|skin[_ ]?id|too large|too small/i.test(message);
}

/**
 * Canonical completed-match lookup.
 *
 * Every backend caller uses this operation. Each request includes the match ID
 * and, when discovery already knows it, the queue ID. The relay owns the direct
 * getmatchdetailsbatch call and, only when that response identifies an
 * incomplete match (or a singleton parser/miss), the permitted recovery pass.
 * Workers never call roster/history/demo recovery endpoints themselves.
 *
 * A multi-ID batch may omit a suffix when one match blocks Hi-Rez parsing.
 * Omitted IDs are intentionally not fanned out here: the worker sees the
 * missing IDs, reforms its ordered continuous batch, and recalls this same
 * operation. Once an ID is isolated as a singleton, ranked/unknown queues run
 * full reconstruction while known non-ranked queues run roster-only presence
 * recovery.
 */
export async function getMatchDetailsBatch(
  requests: CompletedMatchRequest[],
): Promise<CompletedMatchResolution[]> {
  const normalized = requests.map(request => ({
    matchId: Number(request.matchId),
    queueId: request.queueId == null ? undefined : Number(request.queueId),
  }));
  if (normalized.length === 0) return [];
  if (normalized.length > API_CONFIG.BATCH_SIZE) {
    throw new Error(`getMatchDetailsBatch accepts at most ${API_CONFIG.BATCH_SIZE} matches`);
  }

  let directMatches: MatchDetails[];
  try {
    directMatches = await getMatchDetailsBatchDirect(
      normalized.map(request => request.matchId),
    );
  } catch (error) {
    // A hard multi-match error contains no safe blocker identity. The worker
    // owns split/re-batching. A singleton identifies the exact match and can
    // safely enter relay-owned recovery.
    if (normalized.length !== 1 || !isRecoverableMatchDetailError(error)) throw error;
    return [await resolveIdentifiedMatch(normalized[0], null)];
  }

  const directById = new Map(
    directMatches.map(match => [Number(match.match_id), match]),
  );
  const work: Array<{
    request: CompletedMatchRequest;
    direct: MatchDetails | null;
  }> = [];
  for (const request of normalized) {
    const direct = directById.get(request.matchId);
    if (direct) {
      work.push({ request, direct });
    } else if (normalized.length === 1) {
      // Absence from a multi-match response is the worker's continuous-batch
      // signal. A singleton absence is an identified match and must return one
      // explicit recovered/pending/terminal outcome.
      work.push({ request, direct: null });
    }
  }
  const outcomes = await Promise.all(
    work.map(({ request, direct }) => resolveIdentifiedMatch(request, direct)),
  );
  const byId = new Map(outcomes.map(outcome => [outcome.matchId, outcome]));
  return normalized.flatMap(request => {
    const outcome = byId.get(request.matchId);
    return outcome ? [outcome] : [];
  });
}

/**
 * Continue a lifecycle-ledger recovery after detail and roster anchors are
 * already durable. This operation never calls getmatchdetails(batch) or
 * getplayerbatchfrommatch. It reads target histories already persisted by the
 * relay and spends at most one getdemodetails call for the match shell.
 */
export async function resumeMatchRecovery(
  requests: CompletedMatchRequest[],
): Promise<CompletedMatchResolution[]> {
  if (!Array.isArray(requests) || requests.length !== 1) {
    throw new Error('resumeMatchRecovery requires exactly one match');
  }
  const request = {
    matchId: Number(requests[0]?.matchId || 0),
    queueId: requests[0]?.queueId == null ? undefined : Number(requests[0].queueId),
  };
  const historyById = await readPlayerMatchHistoryEntries(request.matchId, [], '');
  const recoveredRows = await query(
    `SELECT *
     FROM match_players
     WHERE match_id = $1
       AND player_id > 0
       AND source = 'recovered'
     ORDER BY entry_datetime DESC`,
    [request.matchId],
  );
  const playersById = new Map<number, PlayerDetails>();
  for (const row of recoveredRows) {
    const player = matchPlayerRowToRecoveryPlayer(row);
    if (player.player_id > 0 && !playersById.has(player.player_id)) {
      playersById.set(player.player_id, player);
    }
  }
  for (const [playerId, player] of historyById) {
    if (playerId > 0 && !playersById.has(playerId)) playersById.set(playerId, player);
  }
  const players = [...playersById.values()];
  const resolvedScore = resolveRecoveredMatchScoreSources(
    [],
    historyScoreObservations(players),
  );
  if (players.length !== 10 || !resolvedScore) {
    return [{
      matchId: request.matchId,
      queueId: Number(request.queueId || 0),
      status: 'recovery_pending',
      reason: 'local recovery evidence is not yet complete',
    }];
  }

  const rawDemo = await getDemoDetails(request.matchId).catch(() => null);
  const demo = Array.isArray(rawDemo) ? (rawDemo[0] || {}) : (rawDemo || {});
  const queueId = Number(demo.Queue || demo.match_queue_id || demo.Queue_Id || request.queueId || 0);
  const entryDatetime = String(demo.Entry_Datetime || demo.entry_datetime || '');
  const durationSeconds = Number(
    demo.Match_Time || demo.Match_Duration || demo.duration_seconds || 0,
  );
  if (queueId <= 0 || !entryDatetime || durationSeconds <= 0) {
    return [{
      matchId: request.matchId,
      queueId,
      status: 'recovery_pending',
      reason: 'demo detail did not provide an authoritative match shell',
    }];
  }

  return [terminalizeRelayMatch(request, {
    match_id: request.matchId,
    entry_datetime: entryDatetime,
    map: String(demo.Map_Game || demo.map || 'Unknown'),
    queue_id: queueId,
    duration_seconds: durationSeconds,
    minutes: Number(demo.Minutes || demo.minutes || Math.floor(durationSeconds / 60)),
    region: String(
      demo.Region
      || demo.region
      || players.find(player => String(player.region || '').trim())?.region
      || 'Unknown',
    ),
    team1_score: resolvedScore.team1,
    team2_score: resolvedScore.team2,
    winning_task_force: resolvedScore.winner,
    has_replay: String(demo.hasReplay || demo.has_replay || '').toLowerCase() === 'y'
      || demo.hasReplay === true
      || demo.has_replay === true,
    recovery_source: 'local_resume',
    recovery_api_calls: 1,
    recovery_attempted: true,
    recovery_terminal: false,
    recovery_pending: false,
    limited: false,
    players,
    ...extractMatchBanFields(demo, players),
  })];
}

function terminalizeRelayMatch(
  request: CompletedMatchRequest,
  match: MatchDetails | null,
): CompletedMatchResolution {
  const queueId = Number(match?.queue_id || request.queueId || 0);
  if (!match) {
    return {
      matchId: request.matchId,
      queueId,
      status: 'dropped',
      reason: 'relay recovery returned no authoritative match or roster facts',
    };
  }

  const stableMatch: MatchDetails = {
    ...match,
    players: [...usableDirectPlayers(match)].sort((left, right) => {
      const leftTaskForce = Number(left.task_force || left.team_id || 0);
      const rightTaskForce = Number(right.task_force || right.team_id || 0);
      if (leftTaskForce !== rightTaskForce) return leftTaskForce - rightTaskForce;

      const leftId = Number(left.player_id || 0);
      const rightId = Number(right.player_id || 0);
      if (leftId > 0 && rightId > 0 && leftId !== rightId) return leftId - rightId;
      if (leftId > 0 && rightId <= 0) return -1;
      if (leftId <= 0 && rightId > 0) return 1;

      const nameOrder = String(left.player_name || '').localeCompare(String(right.player_name || ''));
      if (nameOrder !== 0) return nameOrder;
      return Number(left.champion_id || 0) - Number(right.champion_id || 0);
    }),
  };

  if (stableMatch.recovery_pending === true) {
    return {
      matchId: request.matchId,
      queueId,
      status: 'recovery_pending',
      match: stableMatch,
      reason: stableMatch.recovery_source || 'target history unresolved',
    };
  }

  if (stableMatch.limited === true) {
    return {
      matchId: request.matchId,
      queueId,
      status: 'limited',
      match: stableMatch,
      reason: stableMatch.recovery_source || 'limited recovery',
    };
  }

  return {
    matchId: request.matchId,
    queueId,
    status: stableMatch.recovery_attempted === true
      ? 'complete_recovered'
      : 'complete_direct',
    match: stableMatch,
  };
}

async function resolveRankedMatch(
  request: CompletedMatchRequest,
  direct: MatchDetails | null,
): Promise<CompletedMatchResolution> {
  if (direct && !directMatchNeedsRecovery(direct)) {
    return terminalizeRelayMatch(request, direct);
  }

  const directPlayers = direct ? usableDirectPlayers(direct) : [];
  const recovery = await recoverBrokenMatch(
    request.matchId,
    directPlayers,
    directPlayers,
    direct,
  );
  const recovered = recoveredMatchResult(request.matchId, direct, recovery);
  return terminalizeRelayMatch(request, recovered);
}

async function resolvePresenceMatch(
  request: CompletedMatchRequest,
  direct: MatchDetails | null,
): Promise<CompletedMatchResolution> {
  const queueId = Number(direct?.queue_id || request.queueId || 0);
  if (direct && !directMatchNeedsRecovery(direct)) {
    return terminalizeRelayMatch(request, direct);
  }

  let roster: any[] = [];
  let rosterError: unknown = null;
  try {
    roster = await getPlayerBatchFromMatch(request.matchId);
  } catch (error) {
    rosterError = error;
  }
  const usableRoster = Array.isArray(roster)
    ? roster.filter(player => !String(player?.ret_msg || '').trim())
    : [];
  const directPlayers = direct ? usableDirectPlayers(direct) : [];

  if (direct && directPlayers.length > 0) {
    return {
      matchId: request.matchId,
      queueId,
      status: 'limited',
      match: {
        ...direct,
        // A broken-skin/parser sentinel is diagnostic transport evidence, not
        // a participant. Persist only the usable direct prefix.
        players: directPlayers,
        recovery_attempted: true,
        recovery_terminal: true,
        recovery_pending: false,
        limited: true,
      },
      roster: usableRoster.length > 0 ? usableRoster : undefined,
      reason: usableRoster.length > 0
        ? 'presence roster recovered from incomplete detail'
        : `presence detail retained without roster anchors${rosterError ? `: ${rosterError instanceof Error ? rosterError.message : String(rosterError)}` : ''}`,
    };
  }

  if (usableRoster.length > 0) {
    return {
      matchId: request.matchId,
      queueId,
      status: 'roster_only',
      roster: usableRoster,
      reason: 'presence roster recovered without match detail',
    };
  }

  return {
    matchId: request.matchId,
    queueId,
    status: 'dropped',
    reason: `single relay pass returned no match or roster facts${rosterError ? `: ${rosterError instanceof Error ? rosterError.message : String(rosterError)}` : ''}`,
  };
}

async function resolveIdentifiedMatch(
  request: CompletedMatchRequest,
  direct: MatchDetails | null,
): Promise<CompletedMatchResolution> {
  const directQueueId = Number(direct?.queue_id || 0);
  const effectiveRequest = {
    ...request,
    // Discovery's queue classification is authoritative for recovery scope.
    // A malformed/partial direct row must not promote a known non-ranked match
    // into ranked history/demo fan-out. Exact-ID callers omit queueId, so they
    // still inherit a valid queue from the direct response when available.
    queueId: request.queueId ?? (directQueueId > 0 ? directQueueId : undefined),
  };
  if (direct && !directMatchNeedsRecovery(direct)) {
    return terminalizeRelayMatch(effectiveRequest, direct);
  }

  // Unknown singleton queues use the full compatibility recovery path because
  // exact-ID custom lookups may not have discovery metadata. Scheduled
  // non-ranked workers always supply queueId and therefore use roster-only
  // recovery without target-history or demo fan-out.
  if (effectiveRequest.queueId == null || effectiveRequest.queueId === 486) {
    return resolveRankedMatch(effectiveRequest, direct);
  }
  return resolvePresenceMatch(effectiveRequest, direct);
}

export async function getMatchDetailsRaw(matchId: number): Promise<any[]> {
  const data = await apiRequest('getmatchdetails', [String(matchId)]);
  return Array.isArray(data) ? data : [];
}

export async function getDataUsed(devId: string): Promise<any> {
  // Returns Hi-Rez getdatausedJson response:
  //   Total_Requests_Today: number of requests burned today
  //   Request_Limit_Daily: daily request limit
  //   Active_Sessions, Concurrent_Sessions, Session_Cap, Session_Time_Limit, Total_Sessions_Today
  // Used by: syncUsage() drift correction, api-key-sync worker
  try {
    // Use the target key's own session - getdataused reports usage for the calling key.
    // getKeyForMonitoring() does NOT increment usage (monitoring only).
    const key = await apiKeyPool.getKeyForMonitoring(devId);
    // getDataUsed is special: it must use a specific key (the target of monitoring),
    // not the active waterfall key. Keep the old pattern here - acquireSession + apiRequest
    // with key parameter. All other functions use apiRequest() without key parameter.
    const session = await sessionManager.acquireSession(key);
    const ts = sessionManager.timestamp();
    const sig = sessionManager.sign('getdataused', key.devId, key.authKey, ts);
    const url = `${API_CONFIG.BASE_URL}/getdatausedJson/${key.devId}/${sig}/${session.sessionKey}/${ts}`;
    // ----------------------------------------------------------------
    // CRITICAL: Increment usage for the getdatausedJson call. getDataUsed()
    // bypasses apiRequest() and calls fetchJSON() directly. While
    // acquireSession() increments usage when creating a NEW session, the
    // actual getdataused call burns 1 request on Hi-Rez that is never tracked.
    // Internal memory drifts by 1 per call without this increment.
    // Source: Feedback 2026-05-30 - "getDataUsed Silent Drift"
    // ----------------------------------------------------------------
    apiKeyPool.incrementUsage(key.devId);
    const startTime = Date.now();
    let responseTimeMs = 0;
    try {
      const data = await fetchJSON(url);
      responseTimeMs = Date.now() - startTime;
      await apiKeyPool.logEndpoint(key.devId, 'getdataused', responseTimeMs, 'quota_sync');
      await apiKeyPool.recordSuccess(key.devId);
      // Hi-Rez returns an array [ {...} ] — extract first element.
      // syncUsage expects { Total_Requests_Today, Request_Limit_Daily } as an object.
      return (Array.isArray(data) && data[0]) || (data as object) || {};
    } catch (error) {
      responseTimeMs = Date.now() - startTime;
      await apiKeyPool.logEndpoint(key.devId, 'getdataused', responseTimeMs, 'quota_sync').catch((logErr) => {
        console.error(`[Hi-Rez] Failed to log getdataused for ${devId}: ${logErr}`);
      });
      await apiKeyPool.recordFailure(key.devId, true).catch((failureErr) => {
        console.error(`[Hi-Rez] Failed to record getdataused failure for ${devId}: ${failureErr}`);
      });
      throw error;
    }
  } catch (err) {
    // Log the failure for operational visibility. Without logging,
    // getdataused failures are invisible — the caller (syncUsage) sees
    // an empty object and skips the sync silently. Over time, this masks
    // API issues (e.g., wrong devId, network problems, Hi-Rez downtime).
    // The caller handles the empty object gracefully (skips sync), so
    // logging is sufficient — no need to throw or retry.
    console.warn(`[Hi-Rez] getDataUsed failed for ${devId}: ${err instanceof Error ? err.message : err}`);
    return {};
  }
}

export async function syncApiKeyUsage(devId: string): Promise<boolean> {
  // Relay-owned key sync boundary. Backend/admin callers should request this
  // operation over HTTP instead of importing apiKeyPool.syncUsage directly,
  // because syncUsage ultimately calls Hi-Rez getdataused and therefore belongs
  // inside the relay's session/key/error isolation layer.
  await apiKeyPool.syncUsage(devId);
  return true;
}

export async function getMatchIdsByQueueDetails(
  queueId: number,
  date: string,
  hour: number,
): Promise<Array<{ matchId: number; entryDatetime: string | null; region: string; activeFlag: boolean }>> {
  // Presence discovery gets one vendor observation per scheduled queue-hour.
  // A failed cron call is recovered from hourly_ingest_state; retrying inside
  // the request would hide that distinction and spend against the same answer.
  const requestOptions = currentRelayConsumer() === 'presence_discovery'
    ? SINGLE_ATTEMPT_LOOKUP
    : undefined;
  const data = await apiRequest(
    'getmatchidsbyqueue',
    [String(queueId), date, String(hour)],
    requestOptions,
  );
  if (Array.isArray(data)) {
    return data
      .map((item: any) => ({
        matchId: Number(item?.Match || item?.match_id || item || 0),
        entryDatetime: String(item?.Entry_Datetime || item?.entry_datetime || '').trim() || null,
        region: String(item?.Region || item?.region || '').trim() || 'Unknown',
        activeFlag: String(item?.Active_Flag ?? item?.active_flag ?? 'n').toLowerCase() === 'y'
          || item?.Active_Flag === true
          || item?.active_flag === true,
      }))
      .filter(item => item.matchId > 0);
  }
  // CRITICAL: Guard match_ids as array. Hi-Rez may return an object where
  // match_ids is a string or number (e.g., during maintenance). Without
  // this guard, downstream callers that do result.map() will crash with
  // "data.match_ids.map is not a function". The || [] fallback doesn't
  // help — if match_ids is a string, it passes through as-is.
  //
  // CRITICAL: Use optional chaining (data?.match_ids). apiRequest can return
  // null for primitive responses. Accessing data.match_ids on null throws
  // "TypeError: Cannot read properties of null (reading 'match_ids')".
  // The optional chain returns undefined → Array.isArray(undefined) → false → [].
  // Source: Fault #4 — "getMatchIdsByQueue no guard on match_ids"
  //         Debug 2026-05-31 — "Null property access on data.match_ids"
  return Array.isArray(data?.match_ids)
    ? data.match_ids
      .map((matchId: unknown) => ({
        matchId: Number(matchId),
        entryDatetime: null,
        region: 'Unknown',
        activeFlag: false,
      }))
      .filter((item: any) => item.matchId > 0)
    : [];
}

export async function getMatchIdsByQueue(queueId: number, date: string, hour: number): Promise<number[]> {
  return (await getMatchIdsByQueueDetails(queueId, date, hour)).map(item => item.matchId);
}

export async function getMatchDetailsBatchRaw(matchIds: number[]): Promise<any[]> {
  // ----------------------------------------------------------------
  // CRITICAL: Guard empty array. [].join(',') produces '' → URL ends
  // in // → Hi-Rez returns HTTP 404 → burns API call + wastes 20s retry.
  // Source: Feedback 2026-05-30 - "Empty Array API Trap"
  // ----------------------------------------------------------------
  if (matchIds.length === 0) return [];

  // CRITICAL: Chunk the match IDs. Hi-Rez batch endpoints have strict limits
  // (BATCH_SIZE = 10). Passing 500 match IDs in a single URL produces a massive
  // string that exceeds HTTP URI length limits → HTTP 414 URI Too Long or 400
  // Bad Request. The caller's ingestion cycle crashes permanently.
  // Fix: chunkArray into BATCH_SIZE chunks, call apiRequest per chunk, flatten.
  // Source: Debug 2026-05-31 — "Unbounded batching trap"
  const results: any[] = [];
  const chunks = chunkArray(matchIds, API_CONFIG.BATCH_SIZE);
  for (const chunk of chunks) {
    const data = await apiRequest('getmatchdetailsbatch', [chunk.join(',')]);
    if (Array.isArray(data)) results.push(...data);
  }
  return results;
}

export async function getPlayerChampions(playerId: number): Promise<any[]> {
  const data = await apiRequest('getplayerchampions', [String(playerId)]);
  return data || [];
}

/** All-time per-champion combat totals used for global player KDA. */
export async function getChampionRanks(playerId: number): Promise<any[]> {
  const data = await apiRequest('getchampionranks', [String(playerId)]);
  return data || [];
}

export async function getChampions(): Promise<any[]> {
  const data = await apiRequest('getchampions', ['1']);
  return Array.isArray(data) ? data : [];
}

export async function getItems(): Promise<any[]> {
  const data = await apiRequest('getitems', ['1']);
  return Array.isArray(data) ? data : [];
}

export async function getEsportsProLeagueDetails(): Promise<any[]> {
  const data = await apiRequest('getesportsproleaguedetails', []);
  return Array.isArray(data) ? data : [];
}

export async function getPlayerLoadouts(playerId: number): Promise<any[]> {
  const data = await apiRequest('getplayerloadouts', getPlayerLoadoutRequestParams(playerId));
  return data || [];
}

export async function getPlayerStatus(playerId: number): Promise<any[]> {
  // This is an explicit UI lookup. A single click must not inherit the relay's
  // multi-retry recovery budget and fan out into four quota charges.
  const data = await apiRequest('getplayerstatus', [String(playerId)], SINGLE_ATTEMPT_LOOKUP);
  return Array.isArray(data) ? data : [];
}

export async function getMatchPlayerDetails(matchId: number): Promise<any[]> {
  const data = await apiRequest('getmatchplayerdetails', [String(matchId)], SINGLE_ATTEMPT_LOOKUP);
  return Array.isArray(data) ? data : [];
}

export async function getLeagueLeaderboard(queueId: number, tier: number, season: number): Promise<any[]> {
  const data = await apiRequest('getleagueleaderboard', [String(queueId), String(tier), String(season)]);
  return data || [];
}

export async function getLeagueSeasons(queueId: number): Promise<any[]> {
  const data = await apiRequest('getleagueseasons', [String(queueId)]);
  return data || [];
}

export async function getPlayerBatchFromMatch(matchId: number): Promise<any[]> {
  // This endpoint is the terminal roster fallback for an already-known match
  // ID. A miss is authoritative for this acquisition attempt: internal retries
  // waste quota and cannot repair a vendor response that omitted the match.
  const data = await apiRequest('getplayerbatchfrommatch', [String(matchId)], SINGLE_ATTEMPT_LOOKUP);
  // Guard: ensure array. Hi-Rez returns an object for private matches
  // (e.g., { ret_msg: "Match Privacy Flag set..." }). Without this guard,
  // downstream callers that do data.map() will crash with TypeError.
  return Array.isArray(data) ? data : [];
}

export async function getDemoDetails(matchId: number): Promise<any> {
  const data = await apiRequest('getdemodetails', [String(matchId)]);
  // Guard: ensure object. Hi-Rez returns [] for matches without replay data.
  // Downstream callers expect an object (e.g., demo.Match_Time). Returning
  // null or [] causes property access crashes. Return {} as safe fallback.
  return (typeof data === 'object' && data !== null) ? data : {};
}

async function getPlayerBatchWithOptions(playerIds: number[], requestOptions?: ApiRequestOptions): Promise<any[]> {
  // Same empty array guard as getMatchDetailsBatchRaw above.
  if (playerIds.length === 0) return [];

  // CRITICAL: Chunk the player IDs. getplayerbatch supports 20 IDs, which is
  // intentionally larger than the 10-ID getmatchdetailsbatch limit. Passing an
  // unbounded list still exceeds HTTP URI limits, so cap only this endpoint at
  // PLAYER_BATCH_SIZE.
  // Source: Debug 2026-05-31 — "Unbounded batching trap"
  const results: any[] = [];
  const chunks = chunkArray(playerIds, API_CONFIG.PLAYER_BATCH_SIZE);
  for (const chunk of chunks) {
    const data = await apiRequest('getplayerbatch', [chunk.join(',')], requestOptions);
    if (Array.isArray(data)) results.push(...data);
  }
  return results;
}

export async function getPlayerBatch(playerIds: number[]): Promise<any[]> {
  return getPlayerBatchWithOptions(playerIds);
}

export async function getPlayerBatchLookup(playerIds: number[]): Promise<any[]> {
  // Universal search is a user-facing explicit fallback, not a recovery worker.
  // One lookup should spend at most one getplayerbatch call per chunk; repeated
  // misses are handled by search_remote_lookup_cache rather than retrying the
  // same name/id four times inside the relay.
  const results = await getPlayerBatchWithOptions(playerIds, SINGLE_ATTEMPT_LOOKUP);

  // Hi-Rez returns [] for private accounts (no ret_msg, no data). Detect them
  // by checking missing IDs against getPlayerIdByName — which returns privacy_flag.
  const foundIds = new Set(results.map(r => Number(r.Id || r.player_id || 0)));
  const missingIds = playerIds.filter(id => !foundIds.has(id));

  if (missingIds.length > 0) {
    // Look up missing IDs to detect private accounts
    const privacyMarkers: any[] = [];
    for (const id of missingIds) {
      // Get name from our DB to check via getPlayerIdByName
      const dbRows = await query('SELECT name FROM players WHERE id = $1', [id]);
      const name = dbRows.length > 0 ? dbRows[0].name : null;

      if (name) {
        try {
          const nameResult = await getPlayerIdByName(name);
          if (Array.isArray(nameResult) && nameResult.length > 0) {
            const match = nameResult.find(r => Number(r.player_id || r.Id) === id);
            if (match && (match.privacy_flag || '').toLowerCase() === 'y') {
              privacyMarkers.push({
                Id: id,
                ActivePlayerId: id,
                Name: name,
                ret_msg: 'Player Privacy Flag set',
                privacy_flag: 'y',
              });
            }
          }
        } catch {
          // getPlayerIdByName failed — skip, treat as not found
        }
      }
    }

    // Append privacy markers so callers can distinguish private from not-found
    if (privacyMarkers.length > 0) {
      results.push(...privacyMarkers);
    }
  }

  return results;
}

export async function getMatchHistory(playerId: number, limit = 50, forceRefresh = false): Promise<any[]> {
  const resultLimit = Math.max(1, Math.min(50, Math.floor(Number(limit) || 50)));
  const cached = forceRefresh ? null : await readFreshPlayerHistoryCache(playerId);
  if (cached !== null) {
    // Public player-history lookups should honor the same durable TTL cache as
    // recovery. This includes cached empty histories: a recent "no usable
    // history" result is still knowledge, and re-calling Hi-Rez on every page
    // refresh would turn frontend traffic into quota burn.
    await writePlayerMatchHistoryEntries(playerId, cached, 'cache_backfill');
    return cached.map(normalizeMatchHistoryPlayer).slice(0, resultLimit);
  }

  let data: any;
  try {
    data = await apiRequest('getmatchhistory', getMatchHistoryRequestParams(playerId));
  } catch (error: any) {
    const message = error?.message || String(error);
    const terminalEmpty =
      message.includes('HTTP 404')
      || message.includes('HIREZ_NO_MATCH_HISTORY')
      || message.includes('HIREZ_PRIVACY_FLAG')
      || message.includes('HIREZ_NOT_FOUND_OR_INVALID');

    if (!terminalEmpty) {
      throw error;
    }

    // Hi-Rez often expresses "no history available" as HTTP 404 or a terminal
    // ret_msg. Cache that empty result with the normal history TTL so repeated
    // frontend profile opens do not spend another outbound call until the
    // freshness window expires.
    await writeCachedPlayerHistory(playerId, []);
    return [];
  }
  // CRITICAL: Guard data.matches as array. Hi-Rez returns an object for
  // private profiles (e.g., { ret_msg: "Player Privacy Flag set..." }).
  // When data.matches is an object (not array), .map() throws TypeError.
  // The || data || [] fallback also doesn't help — data is an object,
  // and .map() on an object throws "data.matches.map is not a function".
  // Source: Fault #2 — "getMatchHistory crashes on non-array matches"
  const matches = Array.isArray(data.matches) ? data.matches
    : Array.isArray(data) ? data
      : [];
  // The public player-history endpoint is allowed to refresh Hi-Rez history,
  // but those rows must remain observations. Persist them inside the relay's
  // history tables so they can be reused by recovery/UI without entering
  // raw_ingest_buffer or match_players.
  await writeCachedPlayerHistory(playerId, matches);
  return matches.map(normalizeMatchHistoryPlayer).slice(0, resultLimit);
}

export async function getPlayers(names: string[]): Promise<any[]> {
  // Same empty array guard as above.
  if (names.length === 0) return [];

  // CRITICAL: Chunk the player names. Same scaling issue as batch endpoints.
  // Passing 500 names in a single URL exceeds HTTP URI length limits → HTTP 414.
  // Fix: chunk into BATCH_SIZE. Source: Debug 2026-05-31 — "Unbounded batching trap"
  const results: any[] = [];
  const chunks = chunkArray(names, API_CONFIG.BATCH_SIZE);
  for (const chunk of chunks) {
    const data = await apiRequest('getplayers', [chunk.join(',')]);
    if (Array.isArray(data)) results.push(...data);
  }
  return results;
}

export async function getPlayerIdByName(playerName: string): Promise<any[]> {
  // Name-to-id resolution is used by explicit universal-search fallback.
  // It should never behave like recovery ingestion: a miss is not worth
  // multiple paid retries. Resolve once, audit the raw response upstream, then
  // fetch the canonical profile through getplayerbatch by returned id.
  const data = await apiRequest('getplayeridbyname', [playerName], SINGLE_ATTEMPT_LOOKUP);
  return Array.isArray(data) ? data : (data ? [data] : []);
}

export async function searchPlayers(searchPlayer: string): Promise<any[]> {
  // One broad fuzzy lookup is the safe fallback after exact PC name lookup
  // fails. It may return PC and console candidates, but the backend filters to
  // exact-ish names before spending a getplayerbatch call on profile hydration.
  const data = await apiRequest('searchplayers', [searchPlayer], SINGLE_ATTEMPT_LOOKUP);
  return Array.isArray(data) ? data : (data ? [data] : []);
}

export async function getPlayerIdsByGamerTag(portalId: number, gamerTag: string): Promise<any[]> {
  // Console gamertag lookup requires a portal id. Keep this as an explicit
  // one-portal call so a single unknown name cannot fan out across Xbox/PSN/
  // Switch and burn several requests before the user has confirmed intent.
  const data = await apiRequest('getplayeridsbygamertag', [String(portalId), gamerTag], SINGLE_ATTEMPT_LOOKUP);
  return Array.isArray(data) ? data : (data ? [data] : []);
}

export async function getPlayerIdByPortalUserId(portalId: number, portalUserId: string): Promise<any[]> {
  const data = await apiRequest('getplayeridbyportaluserid', [String(portalId), portalUserId], SINGLE_ATTEMPT_LOOKUP);
  return Array.isArray(data) ? data : (data ? [data] : []);
}

export async function getMatchLeaderboard(tier: number, season: number): Promise<any[]> {
  const data = await apiRequest('getmatchleaderboard', [String(tier), String(season)]);
  // Guard: ensure array. Hi-Rez may return an object during maintenance.
  return Array.isArray(data) ? data : [];
}



function chunkArray<T>(arr: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

/**
 * Dump raw API payload to the buffer table for ELT processing.
 * Batch insert for performance - one round-trip per chunk.
 * Generic: supports match and non-match endpoints.
 */
export async function dumpRawPayloads(payloads: Array<{
  endpoint: string;
  entity_type: string;  // 'match' | 'player' | 'champion' | 'item' | 'leaderboard' | 'loadout'
  entity_id?: number | string;   // match_id, player_id, or match_id:player_id for match_history
  raw_data: any[];
  source?: string;
}>): Promise<number> {
  if (payloads.length === 0) return 0;

  const dedupeEntityTypes = new Set(['match', 'match_history', 'prefetch_match']);
  const keyedPayloads = payloads.filter(p => dedupeEntityTypes.has(p.entity_type) && String(p.entity_id ?? '') !== '');
  const existingKeys = new Set<string>();

  if (keyedPayloads.length > 0) {
    const entityTypes = [...new Set(keyedPayloads.map(p => p.entity_type))];
    const entityIds = [...new Set(keyedPayloads.map(p => String(p.entity_id ?? '')))];

    const existingBufferRows = await query(
      `SELECT DISTINCT entity_type, entity_id
       FROM raw_ingest_buffer
       WHERE entity_type = ANY($1)
         AND entity_id = ANY($2)
         AND status IN ('pending', 'processing')`,
      [entityTypes, entityIds],
    );

    for (const row of existingBufferRows) {
      existingKeys.add(`${row.entity_type}|${row.entity_id}`);
    }

    const matchIds = keyedPayloads
      .filter(p => p.entity_type === 'match')
      .map(p => Number(p.entity_id))
      .filter(id => Number.isFinite(id) && id > 0);

    if (matchIds.length > 0) {
      let existingMatchRows: any[];
      try {
        existingMatchRows = await query(
          `SELECT m.match_id
           FROM matches m
           LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
           WHERE m.match_id = ANY($1)
              AND (mis.status IN ('complete', 'limited') OR mis.status IS NULL)
           UNION
           SELECT mp.match_id
           FROM match_players mp
           LEFT JOIN match_ingest_status mis ON mis.match_id = mp.match_id
           WHERE mp.match_id = ANY($1)
              AND (mis.status IN ('complete', 'limited') OR mis.status IS NULL)
           GROUP BY mp.match_id
           HAVING count(*) >= 10`,
          [[...new Set(matchIds)]],
        );
      } catch {
        // Backward-compatible fallback for databases that have not received the
        // match_ingest_status migration yet. Once the table exists, only
        // complete, limited, or legacy-without-status matches block staging; partial or
        // processing rows can be repaired by a future payload.
        existingMatchRows = await query(
          `SELECT match_id FROM matches WHERE match_id = ANY($1)
           UNION
           SELECT match_id
           FROM match_players
           WHERE match_id = ANY($1)
           GROUP BY match_id
           HAVING count(*) >= 10`,
          [[...new Set(matchIds)]],
        );
      }

      for (const row of existingMatchRows) {
        existingKeys.add(`match|${String(row.match_id)}`);
      }
    }
  }

  const seenInsertKeys = new Set<string>();
  const payloadsToInsert = payloads.filter(p => {
    const key = `${p.entity_type}|${String(p.entity_id ?? '')}`;
    if (dedupeEntityTypes.has(p.entity_type) && String(p.entity_id ?? '') !== '') {
      if (existingKeys.has(key) || seenInsertKeys.has(key)) return false;
      seenInsertKeys.add(key);
    }
    return true;
  });
  const skipped = payloads.length - payloadsToInsert.length;
  if (skipped > 0) {
    console.log(`[HirezRelay] dumpRawPayloads skipped ${skipped} already-staged/already-ingested payloads`);
  }
  if (payloadsToInsert.length === 0) return 0;

  let inserted = 0;
  const chunks = chunkArray(payloadsToInsert, 5000);
  for (const chunk of chunks) {
    const values: string[] = [];
    const params: any[] = [];
    let idx = 1;

    for (const p of chunk) {
    values.push(`($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`);
    // Sanitize: remove null bytes - replace literal "\\u0000" text from JSON.stringify
    // CRITICAL: Default p.raw_data to {} via ?? {}. If an upstream function passes
    // undefined (e.g., a mapping error in a new worker), JSON.stringify(undefined)
    // evaluates to the primitive undefined (not a string). Calling .replace() on
    // undefined instantly throws a TypeError and crashes the entire batch insert.
    // The ?? {} ensures JSON.stringify always receives an object, returning "{}".
    // Source: Debug 2026-05-31 — "dumpRawPayloads stringify trap"
    const rawJson = jsonForDb(p.raw_data ?? {});
    params.push(
      rawJson,                    // raw_data
      'pending',                  // status
      p.endpoint,                 // endpoint
      p.entity_type,              // entity_type
      String(p.entity_id || ''),  // entity_id
    );
  }

    await query(
      `INSERT INTO raw_ingest_buffer (raw_data, status, endpoint, entity_type, entity_id) VALUES ${values.join(', ')}`,
      params
    );
    inserted += chunk.length;
  }
  return inserted;
}

export { MatchDetails, PlayerDetails };




/**
 * Recover broken match via getplayerbatchfrommatch → getmatchhistory.
 * Sequential version (Phase 2 will make this concurrent).
 */
/**
 * Batch-level dedup cache for getmatchhistory calls across multiple
 * recoverBrokenMatch invocations. Keyed by player_id, stores the raw
 * getmatchhistory response so subsequent calls skip the API entirely.
 * Cleared after each processing cycle (buffer-processor drain cycle).
 */
/** Clear the in-memory dedup cache between processing cycles. This is retained
 * for the older relay operation name; it intentionally leaves the durable
 * player_match_history_cache table intact. */
export function clearMatchHistoryCache(): void {
  cleanupFetchedPlayersCache();
}

function matchPlayerRowToRecoveryPlayer(row: any, source: 'recovered' = 'recovered'): PlayerDetails {
  // Convert an already-normalized match_players row back into the PlayerDetails
  // shape expected by the buffer worker. This is intentionally narrow and local
  // to recovery: only authoritative recovered rows should reach this adapter.
  // getmatchhistory observations are read through
  // player_match_history_entries/historyEntryRowToRecoveryPlayer() so partial
  // history never shares the match_players authority ladder.
  return {
    player_id: Number(row.player_id),
    player_name: row.player_name,
    match_id: Number(row.match_id),
    entry_datetime: row.entry_datetime,
    queue_id: row.queue_id,
    champion_id: row.champion_id,
    skin_id: row.skin_id,
    skin_name: row.skin_name || '',
    kills: row.kills,
    deaths: row.deaths,
    assists: row.assists,
    damage_done_in_hand: row.damage_done_in_hand,
    damage_done_physical: row.damage_done_physical,
    damage_done_magical: row.damage_done_magical,
    damage_taken: row.damage_taken,
    damage_taken_physical: row.damage_taken_physical,
    damage_taken_magical: row.damage_taken_magical,
    damage_mitigated: row.damage_mitigated,
    healing: row.healing,
    healing_self: row.healing_self,
    healing_player_self: row.healing_player_self,
    gold_earned: row.gold_earned,
    gold_per_minute: row.gold_per_minute,
    objective_assists: row.objective_assists,
    camps_cleared: row.camps_cleared,
    structure_damage: row.structure_damage,
    wards_placed: row.wards_placed,
    towers_destroyed: row.towers_destroyed,
    distance_traveled: row.distance_traveled,
    multi_kill_max: row.multi_kill_max,
    killing_spree: row.killing_spree,
    kills_first_blood: row.kills_first_blood,
    kills_double: row.kills_double,
    kills_triple: row.kills_triple,
    kills_quadra: row.kills_quadra,
    kills_penta: row.kills_penta,
    kills_fire_giant: row.kills_fire_giant,
    kills_gold_fury: row.kills_gold_fury,
    kills_phoenix: row.kills_phoenix,
    kills_siege_jugg: row.kills_siege_jugg,
    kills_wild_jugg: row.kills_wild_jugg,
    win_status: row.win_status,
    task_force: row.task_force,
    league_tier: row.league_tier,
    league_points: row.league_points,
    league_wins: row.league_wins,
    league_losses: row.league_losses,
    account_level: row.account_level,
    mastery_level: row.mastery_level,
    party_id: row.party_id,
    time_in_match: row.time_in_match,
    source,
    portal_id: row.portal_id,
    portal_user_id: row.portal_user_id,
    kills_player: row.kills_player,
    region: row.region,
    platform: row.platform,
    damage_bot: row.damage_bot,
    kills_single: row.kills_single,
    kills_bot: row.kills_bot,
    final_match_level: row.final_match_level,
    rank_stat_league: row.rank_stat_league,
    team_id: row.team_id,
    team_name: row.team_name,
    surrendered: row.surrendered,
    match_duration: row.match_duration,
    merged_players: null,
    healing_bot: row.healing_bot,
    has_ret_msg: false,
  } as PlayerDetails;
}

async function recoverBrokenMatch(
  matchId: number,
  knownPlayers: PlayerDetails[],
  directPlayers: PlayerDetails[],
  meta: any = null
): Promise<{ recovered: PlayerDetails[]; meta: any }> {
  let recoveryApiCalls = 0;
  const countedApiRequest = async (method: string, params: string[], timeoutMs?: number): Promise<any> => {
    recoveryApiCalls++;
    return apiRequest(method, params, timeoutMs);
  };

  // Separate known players: real IDs vs private (player_id=0 without ret_msg)
  // Players with ret_msg are broken - they are NOT private accounts
  const knownReal = knownPlayers.filter(p => p.player_id > 0);
  const knownPrivate = knownPlayers.filter(p => p.player_id === 0 && !p.has_ret_msg);
  const knownRealIds = new Set(knownReal.map(p => p.player_id));

  // Zero-call local preflight:
  // If previous recovery/history work already saved enough missing players for
  // this match, there is no reason to call getplayerbatchfrommatch just to learn
  // IDs we can infer from the union of current direct rows, recovered
  // match_players, dedicated history observations, and legacy active
  // raw_ingest_buffer match_history rows. The history table is the important
  // quota guard: a rolling 50-match response is paid for once by player_id and
  // reused here without letting partial observations become match ingest work.
  if (knownPrivate.length === 0) {
    const recoveredPreflightRows = await query(
      `SELECT *
       FROM match_players
       WHERE match_id = $1
         AND player_id > 0
         AND source = 'recovered'
       ORDER BY entry_datetime DESC`,
      [matchId],
    );
    const recoveredPreflightById = new Map<number, any>();
    for (const row of recoveredPreflightRows) {
      const playerId = Number(row.player_id);
      if (!knownRealIds.has(playerId) && !recoveredPreflightById.has(playerId)) {
        recoveredPreflightById.set(playerId, row);
      }
    }

    const historyPreflightById = await readPlayerMatchHistoryEntries(matchId, [], meta?.entry_datetime || '');
    const preflightHistoryScorePlayers: PlayerDetails[] = [...historyPreflightById.values()];
    for (const playerId of [...knownRealIds, ...recoveredPreflightById.keys()]) {
      historyPreflightById.delete(playerId);
    }

    const bufferedPreflightById = await readActiveBufferedHistoryPlayers(matchId, [], meta?.entry_datetime || '');
    preflightHistoryScorePlayers.push(...bufferedPreflightById.values());
    for (const playerId of [...knownRealIds, ...recoveredPreflightById.keys(), ...historyPreflightById.keys()]) {
      bufferedPreflightById.delete(playerId);
    }

    const preflightRosterSize = knownRealIds.size
      + recoveredPreflightById.size
      + historyPreflightById.size
      + bufferedPreflightById.size;
    if (preflightRosterSize > 10) {
      console.warn(
        `[RECOVERY] Local preflight found ${preflightRosterSize} distinct player observations for match ${matchId}; ` +
        `roster is ambiguous, so getplayerbatchfrommatch must identify the ten participants`,
      );
    }
    if (preflightRosterSize === 10) {
      const metaQueueId = Number(meta?.queue_id || 0);
      const metaEntryTime = historyEntryTime({ Match_Time: meta?.entry_datetime });
      if (!meta || !Number.isFinite(metaQueueId) || metaQueueId <= 0 || !metaEntryTime) {
        console.log(
          `[RECOVERY] Local preflight found enough player observations for match ${matchId}, ` +
          `but caller has no authoritative match metadata; continuing to recovery metadata endpoints`
        );
      } else {
        const preflightMeta = applyExactHistoryScore(meta, preflightHistoryScorePlayers);
        if (
          preflightMeta?.team1_score != null
          && preflightMeta?.team2_score != null
          && preflightMeta?.winning_task_force != null
        ) {
          console.log(`[RECOVERY] Local preflight: match ${matchId} completed from ${knownRealIds.size} direct + ${recoveredPreflightById.size} recovered rows + ${historyPreflightById.size} history observations + ${bufferedPreflightById.size} active buffer rows; skipped Hi-Rez recovery calls`);
          return {
            recovered: [
              ...[...recoveredPreflightById.values()].map(row => matchPlayerRowToRecoveryPlayer(row, 'recovered')),
              ...historyPreflightById.values(),
              ...bufferedPreflightById.values(),
            ],
            meta: {
              ...preflightMeta,
              recovery_source: 'local_preflight',
              recovery_api_calls: recoveryApiCalls,
            },
          };
        }
        console.log(`[RECOVERY] Local preflight for match ${matchId} lacks a coherent repeated direct/history score; continuing`);
      }
    }
  }

  // Step 1: getplayerbatchfrommatch - get all player IDs + profile data
  const profileMap = new Map<number, any>();
  const privateProfiles: any[] = [];
  let allPlayerIds: number[] = [];
  try {
    const batchData = await countedApiRequest('getplayerbatchfrommatch', [String(matchId)]);
    if (Array.isArray(batchData)) {
      for (const p of batchData) {
        const id = Number(p.ActivePlayerId || p.playerId || p.player_id || p.Id || 0);
        if (id > 0) {
          allPlayerIds.push(id);
          profileMap.set(id, p);
        } else {
          privateProfiles.push(p);
        }
      }
    }
  } catch (err) {
    console.error(`[RECOVERY] getplayerbatchfrommatch failed for match ${matchId}: ${err}`);
    return {
      recovered: [],
      meta: {
        ...(meta || {}),
        recovery_source: 'getplayerbatchfrommatch_failed',
        recovery_api_calls: recoveryApiCalls,
      },
    };
  }

  // Missing = real IDs from getplayerbatchfrommatch that we don't have stats for
  const missingIds = allPlayerIds.filter(id => !knownRealIds.has(id));
  console.log(`[RECOVERY] Match ${matchId}: ${knownReal.length} known real, ${knownPrivate.length} known private, ${missingIds.length} missing out of ${allPlayerIds.length} total (${privateProfiles.length} private from API)`);

  if (missingIds.length === 0 && privateProfiles.length === 0 && knownPrivate.length === 0) {
    // This is the terminal recovery shape for ranked discovery: the batch
    // endpoint cannot parse the match, and getplayerbatchfrommatch did not give
    // us any player IDs to search in the 50-match getmatchhistory window. There
    // is no local or targeted Hi-Rez path left to maximize, so retrying the same
    // ID every gap-check tick is a quota loop rather than useful pending work.
    return {
      recovered: [],
      meta: {
        ...(meta || {}),
        recovery_source: 'no_player_anchors',
        recovery_terminal: true,
        recovery_api_calls: recoveryApiCalls,
      },
    };
  }

  const isBatchOnlyRecovery = knownPlayers.length === 0 && directPlayers.length === 0 && !meta;

  // getplayerbatchfrommatch already returned the complete current profile
  // objects for the ten participant anchors. Persist every public profile from
  // that single response; repeating the same roster through getplayerbatch is
  // duplicate quota and is not part of match recovery.
  for (const rawProfile of profileMap.values()) {
    try {
      if (String(rawProfile?.ret_msg || '').trim()) continue;
      const profile = normalizePlayerProfile(rawProfile);
      if (profile.player_id > 0) await upsertPlayerProfile(profile);
    } catch (err) {
      console.warn(`[RECOVERY] Profile persistence failed for match ${matchId}: ${err}`);
    }
  }

  // Demo is conditional: a surviving direct prefix already carries the match
  // shell. Full/no-direct recovery uses demo only for non-score shell fields.
  let demoData: any = null;
  if (isBatchOnlyRecovery) {
    console.log(`[RECOVERY] Full no-direct recovery for match ${matchId}; fetching demo non-score shell`);
    try {
      demoData = await countedApiRequest('getdemodetails', [String(matchId)]).catch(() => null);
    } catch (err) {
      console.log(`[RECOVERY] batch-only getdemodetails failed for match ${matchId}: ${err}`);
    }
  }

  // Step 3: local-first - check if we already have these real players in
  // authoritative recovered rows, the dedicated player history observation
  // table, or legacy active raw_ingest_buffer match_history rows. New history
  // observations never use raw_ingest_buffer, but the buffer check remains as a
  // compatibility drain for rows staged before the history-table migration.
  const existingRows = missingIds.length > 0
    ? await query(
        `SELECT * FROM match_players 
         WHERE match_id = $1 AND player_id = ANY($2) 
         AND source = 'recovered'`,
        [matchId, missingIds]
      )
    : [];
  const existingMap = new Map<number, any>();
  for (const row of existingRows) existingMap.set(Number(row.player_id), row);
  const demoForBufferedHistory = Array.isArray(demoData) ? (demoData[0] || {}) : (demoData || {});
  const historyMissingIds = missingIds.filter(id => !existingMap.has(id));
  const historyEntryMap = historyMissingIds.length > 0
    ? await readPlayerMatchHistoryEntries(
        matchId,
        historyMissingIds,
        meta?.entry_datetime || demoForBufferedHistory.Entry_Datetime || demoForBufferedHistory.entry_datetime || '',
      )
    : new Map<number, PlayerDetails>();
  const bufferedMissingIds = missingIds.filter(id => !existingMap.has(id) && !historyEntryMap.has(id));
  const bufferedHistoryMap = bufferedMissingIds.length > 0
    ? await readActiveBufferedHistoryPlayers(
        matchId,
        bufferedMissingIds,
        meta?.entry_datetime || demoForBufferedHistory.Entry_Datetime || demoForBufferedHistory.entry_datetime || '',
      )
    : new Map<number, PlayerDetails>();
  console.log(`[RECOVERY] Local-first check: found ${existingRows.length} recovered match_players rows, ${historyEntryMap.size} history observations, and ${bufferedHistoryMap.size} legacy active buffer rows for match ${matchId}`);

  // Step 4: Concurrent getmatchhistory for all missing real players (Phase 2)
  const recovered: PlayerDetails[] = [];
  const directTimeInMatch = directPlayers.length > 0 ? directPlayers[0].time_in_match : 0;

  // Separate: IDs already local vs IDs that need cache/API history.
  const apiMissingIds = missingIds.filter(id => !existingMap.has(id) && !historyEntryMap.has(id) && !bufferedHistoryMap.has(id));
  const dbFoundIds = missingIds.filter(id => existingMap.has(id));
  const historyFoundIds = missingIds.filter(id => historyEntryMap.has(id));
  const bufferFoundIds = missingIds.filter(id => bufferedHistoryMap.has(id));
  console.log(`[RECOVERY] Local-first: ${dbFoundIds.length} from recovered match_players, ${historyFoundIds.length} from history table, ${bufferFoundIds.length} from active buffer, ${apiMissingIds.length} need cache/API`);

  // Use DB data for already-recovered players.
  for (const playerId of dbFoundIds) {
    const row = existingMap.get(playerId);
    recovered.push(matchPlayerRowToRecoveryPlayer(row, 'recovered'));
    console.log(`[RECOVERY] DB-first: player ${playerId} for match ${matchId} found in recovered match_players`);
  }

  for (const playerId of historyFoundIds) {
    const player = historyEntryMap.get(playerId);
    if (!player) continue;
    recovered.push(player);
    console.log(`[RECOVERY] History-first: player ${playerId} for match ${matchId} found in player_match_history_entries`);
  }

  for (const playerId of bufferFoundIds) {
    const player = bufferedHistoryMap.get(playerId);
    if (!player) continue;
    recovered.push(player);
    console.log(`[RECOVERY] Buffer-first: player ${playerId} for match ${matchId} found in active match_history buffer`);
  }

  // API calls only for truly missing players
  // ----------------------------------------------------------------
  // CRITICAL: Wrap playerId inside the promise so it survives rejection.
  // Promise.allSettled returns { status: 'rejected', reason: Error } on
  // failure - no playerId property. Previously, (result as any).playerId
  // was undefined → fallback pushed { player_id: undefined } → PostgreSQL
  // NOT NULL constraint violation → crash → match dropped forever.
  // Also: strip apiKeyPool.getNext() - apiRequest now fetches key internally.
  // Source: Feedback 2026-05-30 - "Promise Rejection Database Wipe" (Claim 3).
  // ----------------------------------------------------------------
  // CRITICAL: Use concurrency limiter to prevent overwhelming Hi-Rez API.
  // Without limiting, 50+ concurrent requests can trigger HTTP 429 errors
  // or session invalidation from Hi-Rez's hidden concurrent-session limits.
  const limit = require('p-limit');
  const limiter = limit(5); // Max 5 concurrent API calls

  // Recovery history dedupe:
  // 1. Memory cache catches repeated players inside the current relay process pass.
  // 2. player_match_history_cache catches the same player across buffer batches,
  //    relay/backend restarts, and gap-checker retries.
  // 3. Hi-Rez getmatchhistory is only called for IDs missing from both caches.
  //
  // This is intentionally keyed by player_id, not by match_id. Hi-Rez returns up
  // to 50 recent matches per player, so one paid call can recover the current
  // target match and later targets involving that same player while the history
  // window is still fresh.
  if (!globalFetchedPlayers) {
    globalFetchedPlayers = new Map();
  }

  const cachedResults: RecoveryHistoryResult[] = [];
  const playersToFetch: number[] = [];
  let memoryCacheHits = 0;
  let dbCacheHits = 0;
  const demoForTime = Array.isArray(demoData) ? (demoData[0] || {}) : (demoData || {});
  const firstProfileForTime = profileMap.values().next().value || {};
  const targetEntryTime = historyEntryTime({
    Match_Time: meta?.entry_datetime
      || demoForTime.Entry_Datetime
      || demoForTime.entry_datetime
      || firstProfileForTime.Entry_Datetime
      || firstProfileForTime.entry_datetime,
  });

  for (const playerId of apiMissingIds) {
    const memoryCached = globalFetchedPlayers.get(playerId);
    if (memoryCached && Array.isArray(memoryCached.matches)) {
      memoryCacheHits++;
      cachedResults.push({ playerId, data: { matches: memoryCached.matches }, success: true, source: 'memory' });
      continue;
    }

    const dbCached = await readCachedPlayerHistory(playerId, matchId, targetEntryTime);
    if (dbCached) {
      dbCacheHits++;
      await writePlayerMatchHistoryEntries(playerId, dbCached, 'cache_backfill');
      globalFetchedPlayers.set(playerId, { matches: dbCached });
      cachedResults.push({ playerId, data: { matches: dbCached }, success: true, source: 'database' });
      continue;
    }

    playersToFetch.push(playerId);
  }

  console.log(`[RECOVERY] Match ${matchId}: ${apiMissingIds.length} missing players, ${playersToFetch.length} to fetch, ${memoryCacheHits} memory-cache hits, ${dbCacheHits} DB-cache hits`);

  // Only fetch players that were not found in either cache. Empty histories are
  // cached too: a private/no-history result is still useful knowledge for the
  // current freshness window and prevents repeated calls for the same player.
  let fetchedResults: RecoveryHistoryResult[];
  if (playersToFetch.length > 0) {
    console.log(`[RECOVERY] Fetching ${playersToFetch.length} unique player histories via getmatchhistory`);
    fetchedResults = await Promise.all(playersToFetch.map(async (playerId) => {
      try {
        const data = await limiter(() => countedApiRequest('getmatchhistory', [String(playerId)]));
        const matches = historyMatchesFromData(data);

        globalFetchedPlayers!.set(playerId, { matches });
        await writeCachedPlayerHistory(playerId, matches);

        return { playerId, data: { matches }, success: true, source: 'api' };
      } catch (err) {
        console.warn(`[RECOVERY] getmatchhistory failed for player ${playerId}: ${err}`);
        return { playerId, data: { matches: [] }, success: false, source: 'api' };
      }
    }));
  } else {
    // All missing players were in cache - no API calls needed for this match
    fetchedResults = [];
  }

  const results: RecoveryHistoryResult[] = [...cachedResults, ...fetchedResults];

  // Each getmatchhistory result has already been persisted through
  // writeCachedPlayerHistory()/writePlayerMatchHistoryEntries(). Do not stage
  // non-target matches into raw_ingest_buffer: those observations are useful
  // cache material, not match ingest work.

  for (const result of results) {
    const { playerId, data, success } = result;

    if (success && data) {
      // ----------------------------------------------------------------
      // CRITICAL: Enforce array before .find(). Hi-Rez returns a pure object
      // for private profiles (e.g., { ret_msg: "Player Privacy Flag set..." }).
      // When data is an object, data.matches is undefined → OR falls to data
      // → matches becomes an Object → .find() throws TypeError.
      // Source: Feedback 2026-05-30 - "matches.find Private Profile Crash"
      // ----------------------------------------------------------------
      const rawMatches = data.matches || data;
      const matches = Array.isArray(rawMatches) ? rawMatches : [];

      const matchEntry = matches.find((m: any) => {
        const mId = Number(m.Match || m.match_id || m.MatchId || 0);
        return mId === matchId;
      });

      if (matchEntry) {
        const normalized = normalizeHistoryEntryForRecovery(
          matchEntry,
          matchId,
          playerId,
          meta?.entry_datetime || demoForTime.Entry_Datetime || demoForTime.entry_datetime || '',
        );
        if (!normalized) continue;
        // normalizeMatchHistoryPlayer focuses on player stats and historically
        // did not expose Map_Game. Batch-only discovery promotion needs the map
        // to build a full matches row, so carry it through on the recovered
        // player object and let finalMeta harvest it below.
        (normalized as any).map = (matchEntry as any).Map_Game || (matchEntry as any).Map || (matchEntry as any).map || '';

        // Keep the two upstream duration concepts separate. getdemodetails
        // Match_Time is the match-level gameplay duration, while
        // getmatchhistory Time_In_Match_Seconds is the participant/per-minute
        // denominator. They routinely differ (including on fully direct
        // matches), so replacing time_in_match here made recovered values
        // depend on whether the history row came from the API, memory, or DB.
        // finalMeta below owns the demo duration; the normalized player keeps
        // its history duration and the database derives CPM/eCPM from it.

        // In-memory enrichment: apply platform/level from the existing
        // getplayerbatchfrommatch profile response.
        const enrichment = profileMap.get(playerId);
        if (enrichment) {
          if (!normalized.platform) normalized.platform = enrichment.Platform || '';
          if (!normalized.account_level) normalized.account_level = Number(enrichment.Level) || 0;
        }
        recovered.push(normalized);
        console.log(`[RECOVERY] Recovered player ${playerId} for match ${matchId}`);

        // Non-target rows from the same history response stay in
        // player_match_history_entries. They can seed future recovery/player UI
        // without creating raw-buffer backlog or recursively recovering every
        // casual/ranked match visible in a player's 50-match window.

      } else {
        // A profile is not a recovered match player. If the target match is not
        // in this player's history, leave the recovery unresolved instead of
        // manufacturing zero-stat/minimal match facts.
        console.warn(`[RECOVERY] Target match ${matchId} missing from history for player ${playerId}`);
        continue;
      }
    } // closes if (success && data)
    // NOTE: Removed dead 'else if (success)' block. When success=true, data is
    // always non-null (promise returns { playerId, data, success: true }), so
    // 'success && data' is always true. The 'else' branch above already handles
    // the case where success=true but match was not found in history.
  }

  // Step 5: Handle private accounts
  // knownPrivate = private accounts from the direct pull (have stats if before broken skin)
  // privateProfiles = private accounts from getplayerbatchfrommatch (no stats)
  // Only create minimal private accounts for profiles we DON'T already have stats for
  // Count how many private accounts we already have stats for (from knownPrivate)
  const knownPrivateCount = knownPrivate.length;

  // We need (privateProfiles.length - knownPrivateCount) minimal private accounts
  // because knownPrivate already have stats from the direct pull
  const unresolvedPrivateProfiles = Math.max(0, privateProfiles.length - knownPrivateCount);
  if (unresolvedPrivateProfiles > 0) {
    console.warn(
      `[RECOVERY] Match ${matchId} has ${unresolvedPrivateProfiles} private participant(s) without direct match facts; ` +
      `profile-only placeholders cannot complete recovery`,
    );
  }

  // Add known private accounts (they already have stats from the direct pull)
  for (const priv of knownPrivate) {
    recovered.push(priv);
    console.log(`[RECOVERY] Known private account retained (source=${priv.source}) for match ${matchId}`);
  }

  // Step 6: Build final metadata.
  //
  // `getmatchdetailsbatch` can return a partial shell before the Int16 skin
  // overflow aborts the response. Preserve a coherent direct score; otherwise
  // use the exact result repeated by target getmatchhistory rows. Demo numerical
  // scores are replay snapshots and never participate in final-score recovery.
  const firstRecovered = recovered.find(p => p.source === 'recovered');
  const demoShell = Array.isArray(demoData) ? (demoData[0] || {}) : (demoData || {});
  const demoNumber = (...values: unknown[]): number | undefined => {
    for (const value of values) {
      if (value === undefined || value === null || value === '') continue;
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
    return undefined;
  };
  const recoveredQueueId = Number((meta && meta.queue_id) || (firstRecovered && firstRecovered.queue_id) || demoShell.Queue || demoShell.match_queue_id || demoShell.Queue_Id || 0);
  const resolvedMatchResult = resolveRecoveredMatchScoreSources(
    directScoreObservations(meta),
    historyScoreObservations(recovered),
  );
  const demoHasMatchShell = Boolean(
    demoShell
    && Object.keys(demoShell).length > 0
    && Number(demoShell.Match || matchId) === matchId,
  );

  const finalMeta = {
    ...(meta || {}),
    entry_datetime: (meta && meta.entry_datetime) || (firstRecovered && firstRecovered.entry_datetime) || demoShell.Entry_Datetime || demoShell.entry_datetime || new Date().toISOString(),
    map: (meta && meta.map) || ((firstRecovered as any) && (firstRecovered as any)['map']) || demoShell.Map_Game || demoShell['map'] || 'Unknown',
    queue_id: recoveredQueueId,
    duration_seconds: demoNumber(meta && meta.duration_seconds)
      ?? demoNumber(demoShell.Match_Time, demoShell.Match_Duration, demoShell.duration_seconds)
      ?? demoNumber(firstRecovered && firstRecovered.match_duration)
      ?? 0,
    region: (meta && meta.region) || (recovered.find((p: any) => p.region && p.region !== '')?.region) || demoShell.Region || demoShell.region || 'Unknown',
    team1_score: resolvedMatchResult?.team1 ?? null,
    team2_score: resolvedMatchResult?.team2 ?? null,
    winning_task_force: resolvedMatchResult?.winner ?? null,
    has_replay: Boolean(demoShell.hasReplay || demoShell.has_replay || (meta && meta.has_replay)),
    minutes: demoNumber(demoShell.Minutes, demoShell.minutes) ?? ((meta && meta.minutes) ?? 0),
    // Completed-match recovery never treats demo numerical scores as authoritative.
    // Keep the winner-availability fact separate for diagnostics.
    demo_scores_authoritative: false,
    demo_shell_available: demoHasMatchShell,
    demo_scores_canonicalized: false,
    score_recovered: resolvedMatchResult?.source === 'history',
    score_source: resolvedMatchResult?.source ?? null,
    // getmatchhistory player rows do not carry draft bans. Preserve the
    // authoritative getdemodetails BanId1-8 fields on recovery metadata so
    // every caller can stage them with the recovered roster.
    ...extractMatchBanFields(meta, demoShell),
  };

  const recoveredHistoryCount = recovered.filter((p: any) => String(p?.source || '').toLowerCase() === 'recovered').length;
  const finalRecoveryMeta = {
    ...finalMeta,
    recovery_api_calls: recoveryApiCalls,
    batch_only: isBatchOnlyRecovery,
    anchor_player_count: allPlayerIds.length,
    target_history_observation_count: recoveredHistoryCount,
    profile_only_count: 0,
    ...(allPlayerIds.length === 0 ? {
      recovery_source: 'no_player_anchors',
      recovery_terminal: true,
    } : {}),
  };

  const recoveredPublicIds = new Set(
    recovered.map(player => Number(player.player_id || 0)).filter(playerId => playerId > 0),
  );
  const unresolvedPlayerIds = missingIds.filter(playerId => !recoveredPublicIds.has(playerId));
  if (unresolvedPlayerIds.length > 0 || unresolvedPrivateProfiles > 0 || !resolvedMatchResult) {
    const reasons = [
      unresolvedPlayerIds.length > 0 ? `missing target history for ${unresolvedPlayerIds.length} player(s)` : null,
      unresolvedPrivateProfiles > 0 ? `${unresolvedPrivateProfiles} private participant(s) lack direct match facts` : null,
      !resolvedMatchResult ? 'no coherent repeated direct/history score' : null,
    ].filter(Boolean).join('; ');
    console.warn(`[RECOVERY] Match ${matchId} remains unresolved: ${reasons}`);
    return {
      recovered: [],
      meta: {
        ...finalRecoveryMeta,
        recovery_source: 'target_history_unresolved',
        recovery_terminal: false,
        unresolved_player_ids: unresolvedPlayerIds,
      },
    };
  }

// Step 7 intentionally removed:
// Older recovery queued non-target getmatchhistory rows into raw_ingest_buffer
// as match_history/prefetch work. That made the buffer grow with every recovery
// batch and gave delayed workers a second chance to treat partial history as
// broken match ingest. The relay now persists those rows directly in
// player_match_history_entries when the player history is fetched/cached.

  // Profile persistence already used the single getplayerbatchfrommatch
  // response above. Do not repeat these IDs through getplayerbatch.
  for (const player of recovered) {
    const enrichment = profileMap.get(Number(player.player_id || 0));
    if (!enrichment) continue;
    if (!player.platform) player.platform = enrichment.Platform || '';
    if (!player.account_level) player.account_level = Number(enrichment.Level) || 0;
  }
  const recoveredBanFields = extractMatchBanFields(finalRecoveryMeta, recovered);
  for (const player of recovered) {
    Object.assign(player, recoveredBanFields);
  }
  finalRecoveryMeta.recovery_api_calls = recoveryApiCalls;

  return { recovered, meta: finalRecoveryMeta };
}
