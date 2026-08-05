import { FastifyInstance } from 'fastify';
import crypto from 'crypto';
import type { PoolClient } from 'pg';
import { pool, query, one } from '../config/db';
import { getChampionRanks, getPlayerBatch, getPlayerIdByName, getPlayerLoadouts, getMatchHistory } from '../services/hirez';
import { recordRawHirezResponse } from '../services/raw-hirez-response-audit';
import { hasPlayerChampionCombatStats, normalizePlayerChampion, normalizePlayerProfile } from '../services/normalizer';
import { upsertPlayerProfile } from '../services/player-profile-store';
import { syncPlayer } from '../services/meilisearch';
import { err, paginate, bulkIds, DISPLAY_NAME_SQL, requireAdminSession } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';
import { championRoleSql } from '../utils/champion-roles';
import { invalidateRouteCache, registerReadThroughCache } from '../utils/route-cache';
import {
  guardPlayerRefreshAttempt,
  guardVendorFallback,
  PLAYER_REFRESH_ATTEMPT_LIMIT,
  PLAYER_REFRESH_WINDOW_MS,
  RequestSecurityError,
} from '../services/request-security';
import { internalRequestHeaders } from '../services/internal-request';
import { normalizePlayerLoadoutDeck, type StoredPlayerLoadout } from '../services/player-loadout-normalizer';
import { get as getCachedValue, set as setCachedValue } from '../services/cache';
import { PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES } from '../services/player-history-policy';

const PERFORMANCE_COLUMNS: Record<string, { projectionColumn: string }> = {
  dpm: { projectionColumn: 'pr.dpm' },
  hpm: { projectionColumn: 'pr.hpm' },
  gpm: { projectionColumn: 'pr.gpm' },
  mpm: { projectionColumn: 'pr.mpm' },
};

const CASUAL_PERFORMANCE_EXPRESSIONS: Record<string, string> = {
  dpm: 'cmp.damage * 60.0 / NULLIF(cm.duration_seconds, 0)',
  hpm: 'cmp.healing * 60.0 / NULLIF(cm.duration_seconds, 0)',
  gpm: 'cmp.credits * 60.0 / NULLIF(cm.duration_seconds, 0)',
  mpm: 'cmp.mitigation * 60.0 / NULLIF(cm.duration_seconds, 0)',
};

const RANKED_QUEUE_ID = 486;
const PLAYER_PROFILE_TTL_MS = 24 * 60 * 60 * 1000;
// Discord and web reads share the same long profile cache. Explicit web refresh
// owns on-demand freshness; ordinary bot commands do not repeatedly poll Hi-Rez.
const DISCORD_PLAYER_PROFILE_TTL_MS = 24 * 60 * 60 * 1000;
const PLAYER_CHAMPION_STATS_TTL_MS = 10 * 60 * 1000;
const DISCORD_PLAYER_CHAMPION_STATS_TTL_MS = 24 * 60 * 60 * 1000;
const DISCORD_PLAYER_CHAMPION_STATS_RETRY_TTL_SECONDS = 5 * 60;
const PLAYER_LOADOUT_TTL_MS = 24 * 60 * 60 * 1000;
const PLAYER_LOADOUT_MANUAL_REFRESH_COOLDOWN_MS = 10 * 60 * 1000;

interface PlayerProfileFreshness {
  ttl_seconds: number;
  refreshed_at: string | null;
  expires_at: string | null;
  remaining_seconds: number;
  expired: boolean;
}

interface PlayerProfileRefreshResult {
  refreshed: boolean;
  freshness: PlayerProfileFreshness;
  audit?: any;
  response_count?: number;
}

interface PlayerChampionStatsFreshness {
  ttl_seconds: number;
  refreshed_at: string | null;
  expires_at: string | null;
  remaining_seconds: number;
  expired: boolean;
}

interface PlayerLoadoutFreshness {
  ttl_seconds: number;
  refreshed_at: string | null;
  expires_at: string | null;
  remaining_seconds: number;
  expired: boolean;
  manual_refresh_available_at: string | null;
  manual_refresh_remaining_seconds: number;
}

function parseLimit(value: unknown, fallback = 100, max = 100): number {
  const parsed = parseInt(String(value ?? fallback), 10);
  if (!Number.isInteger(parsed) || parsed <= 0) return fallback;
  return Math.min(parsed, max);
}

function parseQueueId(value: unknown, fallback = 486): number | undefined {
  const parsed = parseInt(String(value ?? fallback), 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : undefined;
}

function normalizeRole(value: unknown): { role: string; roleId: number } | null {
  const key = String(value ?? '').toLowerCase().replace(/[\s_-]/g, '');
  if (!key) return null;
  const normalizedKey = key === 'front' || key === 'frontline' || key === 'frontlinepaladins' ? 'frontline' : key;
  if (!['frontline', 'damage', 'flank', 'support'].includes(normalizedKey)) return null;
  const role = normalizedKey === 'frontline' ? 'Frontline' : normalizedKey.charAt(0).toUpperCase() + normalizedKey.slice(1);
  const roleId = ({ damage: 1, flank: 2, support: 3, frontline: 4 } as Record<string, number>)[normalizedKey];
  return { role, roleId };
}

function normalizePerformanceMetric(value: unknown): { metric: string; projectionColumn: string } | null {
  const metric = String(value ?? '').toLowerCase();
  const columns = PERFORMANCE_COLUMNS[metric];
  return columns ? { metric, ...columns } : null;
}

function escapeLike(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
}

function playerSearchDocument(player: any): any {
  return {
    id: player.id,
    name: player.name,
    level: player.level,
    wins: player.wins,
    losses: player.losses,
    mastery_level: player.mastery_level,
    region: player.region,
    platform: player.platform,
    kbm_tier: player.kbm_tier,
    kbm_points: player.kbm_points,
    cheater: player.cheater,
    sus_count: player.sus_count,
    dropper: player.dropper,
    afk_wintrade: player.afk_wintrade,
    alt_account: player.alt_account,
    weirdo_count: player.weirdo_count,
    hall_of_fame_count: player.hall_of_fame_count,
    portal_id: player.portal_id,
    portal_user_id: player.portal_user_id,
    first_seen: player.first_seen,
    last_seen: player.last_seen,
    last_updated: player.last_updated,
  };
}

function getPlayerProfileFreshness(player: any, now = Date.now(), ttlMs = PLAYER_PROFILE_TTL_MS): PlayerProfileFreshness {
  const refreshedAt = player?.hirez_profile_refreshed_at
    ? new Date(player.hirez_profile_refreshed_at).getTime()
    : Number.NaN;
  if (!Number.isFinite(refreshedAt)) {
    return {
      ttl_seconds: ttlMs / 1000,
      refreshed_at: null,
      expires_at: null,
      remaining_seconds: 0,
      expired: true,
    };
  }

  const expiresAt = refreshedAt + ttlMs;
  const remainingMs = Math.max(0, expiresAt - now);
  return {
    ttl_seconds: ttlMs / 1000,
    refreshed_at: new Date(refreshedAt).toISOString(),
    expires_at: new Date(expiresAt).toISOString(),
    remaining_seconds: Math.ceil(remainingMs / 1000),
    expired: remainingMs <= 0,
  };
}

async function publishPlayerModerationChange(playerId: number): Promise<void> {
  await invalidateRouteCache('route:players');
  const refreshed = await one('SELECT p.* FROM players p WHERE p.id = $1', [playerId]);
  if (refreshed) void syncPlayer(playerId, playerSearchDocument(refreshed));
}

interface PlayerVoteSession {
  user_id: number;
  is_admin: boolean;
  is_approved: boolean;
}

async function requirePlayerVoteSession(req: any, reply: any): Promise<PlayerVoteSession | null> {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) {
    reply.status(401).send(err('AUTH', 'Authentication required'));
    return null;
  }
  const tokenHash = crypto.createHash('sha256').update(token).digest('hex');
  const session = await one(
    'SELECT s.user_id, u.is_admin, u.is_approved FROM sessions s JOIN users u ON u.id = s.user_id WHERE s.token = $1 AND s.expires_at > now()',
    [tokenHash],
  );
  if (!session) {
    reply.status(401).send(err('AUTH', 'Invalid session'));
    return null;
  }
  return session as PlayerVoteSession;
}

function getPlayerLoadoutFreshness(fetchState: any, now = Date.now()): PlayerLoadoutFreshness {
  const fetchedAt = fetchState?.fetched_at ? new Date(fetchState.fetched_at).getTime() : Number.NaN;
  const manualRefreshAt = fetchState?.last_manual_refresh_at
    ? new Date(fetchState.last_manual_refresh_at).getTime()
    : Number.NaN;
  const expiresAt = Number.isFinite(fetchedAt) ? fetchedAt + PLAYER_LOADOUT_TTL_MS : Number.NaN;
  const manualRefreshAvailableAt = Number.isFinite(manualRefreshAt)
    ? manualRefreshAt + PLAYER_LOADOUT_MANUAL_REFRESH_COOLDOWN_MS
    : Number.NaN;
  const remainingMs = Number.isFinite(expiresAt) ? Math.max(0, expiresAt - now) : 0;
  const manualRemainingMs = Number.isFinite(manualRefreshAvailableAt)
    ? Math.max(0, manualRefreshAvailableAt - now)
    : 0;
  return {
    ttl_seconds: PLAYER_LOADOUT_TTL_MS / 1000,
    refreshed_at: Number.isFinite(fetchedAt) ? new Date(fetchedAt).toISOString() : null,
    expires_at: Number.isFinite(expiresAt) ? new Date(expiresAt).toISOString() : null,
    remaining_seconds: Math.ceil(remainingMs / 1000),
    expired: !Number.isFinite(expiresAt) || remainingMs <= 0,
    manual_refresh_available_at: Number.isFinite(manualRefreshAvailableAt)
      ? new Date(manualRefreshAvailableAt).toISOString()
      : null,
    manual_refresh_remaining_seconds: Math.ceil(manualRemainingMs / 1000),
  };
}

async function readPlayerLoadoutFetchState(playerId: number, client?: PoolClient): Promise<any> {
  const sql = 'SELECT player_id, fetched_at, last_manual_refresh_at FROM player_loadout_fetches WHERE player_id = $1';
  if (client) return (await client.query(sql, [playerId])).rows[0] ?? null;
  return one(sql, [playerId]);
}

async function readCachedPlayerLoadouts(playerId: number): Promise<any[]> {
  return query(
    `SELECT
       pl.id,
       pl.deck_id,
       pl.deck_key,
       pl.champion_id,
       COALESCE(c.name, 'Champion ' || pl.champion_id::TEXT) AS champion_name,
       pl.loadout_name,
       COALESCE(pl.card_ids, '{}') AS card_ids,
       COALESCE(pl.card_levels, '{}') AS card_levels,
       pl.talent_id,
       pl.fetched_at,
       pl.updated_at
     FROM player_loadouts pl
     LEFT JOIN champions c ON c.id = pl.champion_id
     WHERE pl.player_id = $1
     ORDER BY champion_name ASC, pl.loadout_name ASC, pl.id ASC`,
    [playerId],
  );
}

function getPlayerChampionStatsFreshness(
  lastUpdated: unknown,
  statsPopulated: unknown,
  now = Date.now(),
  ttlMs = PLAYER_CHAMPION_STATS_TTL_MS,
): PlayerChampionStatsFreshness {
  const updatedAt = new Date(String(lastUpdated ?? '')).getTime();
  if (!Boolean(statsPopulated) || !Number.isFinite(updatedAt)) {
    return {
      ttl_seconds: ttlMs / 1000,
      refreshed_at: null,
      expires_at: null,
      remaining_seconds: 0,
      expired: true,
    };
  }

  const expiresAt = updatedAt + ttlMs;
  const remainingMs = Math.max(0, expiresAt - now);
  return {
    ttl_seconds: ttlMs / 1000,
    refreshed_at: new Date(updatedAt).toISOString(),
    expires_at: new Date(expiresAt).toISOString(),
    remaining_seconds: Math.ceil(remainingMs / 1000),
    expired: remainingMs <= 0,
  };
}

async function readPlayerChampionStatsFreshness(
  playerId: number,
  client?: PoolClient,
  ttlMs = PLAYER_CHAMPION_STATS_TTL_MS,
): Promise<PlayerChampionStatsFreshness> {
  const sql = `SELECT MAX(last_updated)::text AS last_updated, COUNT(*) > 0 AS stats_populated
    FROM player_champions WHERE player_id = $1 AND stats_populated`;
  const row = client
    ? (await client.query<{ last_updated: string | null; stats_populated: boolean }>(sql, [playerId])).rows[0]
    : await one<{ last_updated: string | null; stats_populated: boolean }>(sql, [playerId]);
  return getPlayerChampionStatsFreshness(row?.last_updated, row?.stats_populated, Date.now(), ttlMs);
}

async function hasFreshPlayerHistoryCache(playerId: number): Promise<boolean> {
  try {
    return Boolean(await one(
      `SELECT 1
       FROM player_match_history_cache
       WHERE player_id = $1
         AND fetched_at >= now() - ($2::int * interval '1 minute')
         AND expires_at > now()`,
      [playerId, PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES],
    ));
  } catch (cacheError: any) {
    // HirezRelay creates this table lazily on a new stack. Treat only that
    // bootstrap condition as a miss; the guarded fallback remains fail-closed
    // if its shared protection store is unavailable.
    if (cacheError?.code !== '42P01') {
      console.warn(
        `[PLAYER-HISTORY] Unable to inspect history cache for ${playerId}: ${cacheError?.message || cacheError}`,
      );
    }
    return false;
  }
}

async function readPlayerGlobalStats(playerId: number): Promise<any | null> {
  const globalStats = await one(
    `SELECT COALESCE(SUM(wins), 0)::BIGINT AS wins, COALESCE(SUM(losses), 0)::BIGINT AS losses,
            COALESCE(SUM(kills), 0)::BIGINT AS kills, COALESCE(SUM(deaths), 0)::BIGINT AS deaths,
            COALESCE(SUM(assists), 0)::BIGINT AS assists
     FROM player_champions
     WHERE player_id = $1 AND stats_populated`,
    [playerId],
  );
  const hasStats = ['wins', 'losses', 'kills', 'deaths', 'assists']
    .some((field) => Number(globalStats?.[field] ?? 0) > 0);
  return hasStats ? globalStats : null;
}

/**
 * Refresh all-time player champion totals after a refresh request.
 * The TTL and advisory lock collapse repeated requests into one vendor
 * call and leave the last known database copy available if Hi-Rez is down.
 */
async function refreshPlayerChampionStatsIfExpired(
  playerId: number,
  ttlMs = PLAYER_CHAMPION_STATS_TTL_MS,
  reason = 'manual_champion_stats_refresh',
  source = 'player-champion-stats-manual-refresh',
  beforeRefresh?: () => Promise<void>,
): Promise<boolean> {
  const client = await pool.connect();
  const lockName = `player-champion-stats-refresh:${playerId}`;
  let locked = false;
  try {
    await client.query('SELECT pg_advisory_lock(hashtext($1))', [lockName]);
    locked = true;
    const freshness = await readPlayerChampionStatsFreshness(playerId, client, ttlMs);
    if (!freshness.expired) return false;

    await beforeRefresh?.();
    const raw = await getChampionRanks(
      playerId,
      reason === 'discord_player_champion_stats'
        ? 'discord_player_command'
        : 'manual_profile_refresh',
    );
    await recordRawHirezResponse({
      endpoint: 'getchampionranks',
      operation: 'getChampionRanks',
      entityType: 'player_champions',
      entityId: playerId,
      params: { playerId, reason },
      rawResponse: raw,
      source,
    }, client);
    if (!Array.isArray(raw)) throw new Error('Hi-Rez returned an invalid player champions response.');
    if (raw.some((row: any) => String(row?.ret_msg ?? '').trim()) && raw.every((row: any) => String(row?.ret_msg ?? '').trim())) {
      throw new Error('Hi-Rez could not return player champion totals right now.');
    }

    const validRows = raw.filter((row: any) => !String(row?.ret_msg ?? '').trim());
    if (validRows.some((row: any) => !hasPlayerChampionCombatStats(row))) {
      throw new Error('Hi-Rez returned champion rows without combat totals.');
    }

    const champions = validRows
      .map((row: any) => normalizePlayerChampion(row))
      .filter((champion) => champion.player_id === playerId && champion.champion_id > 0);
    if (champions.length === 0) return false;

    await client.query('BEGIN');
    try {
      for (const champion of champions) {
        await client.query(
          `INSERT INTO player_champions (player_id, champion_id, champion_name, xp, ownership_type, wins, losses, kills, deaths, assists, minutes_played, stats_populated, last_updated)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,true,now())
           ON CONFLICT (player_id, champion_id) DO UPDATE SET
             champion_name = EXCLUDED.champion_name,
             xp = CASE WHEN EXCLUDED.xp > 0 THEN EXCLUDED.xp ELSE player_champions.xp END,
             ownership_type = COALESCE(NULLIF(EXCLUDED.ownership_type, ''), player_champions.ownership_type),
             wins = EXCLUDED.wins, losses = EXCLUDED.losses, kills = EXCLUDED.kills, deaths = EXCLUDED.deaths,
             assists = EXCLUDED.assists, minutes_played = EXCLUDED.minutes_played, stats_populated = true, last_updated = now()`,
          [champion.player_id, champion.champion_id, champion.champion_name, champion.xp, champion.ownership_type,
            champion.wins, champion.losses, champion.kills, champion.deaths, champion.assists, champion.minutes_played],
        );
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
    return true;
  } finally {
    if (locked) await client.query('SELECT pg_advisory_unlock(hashtext($1))', [lockName]);
    client.release();
  }
}

async function refreshDiscordPlayerChampionStatsIfExpired(
  playerId: number,
  beforeRefresh?: () => Promise<void>,
): Promise<boolean> {
  const freshness = await readPlayerChampionStatsFreshness(
    playerId,
    undefined,
    DISCORD_PLAYER_CHAMPION_STATS_TTL_MS,
  );
  if (!freshness.expired) return false;

  // A short cross-process retry guard prevents a private profile or vendor
  // outage from spending one getchampionranks call per repeated bot command.
  const retryKey = `discord:player-champion-stats-attempt:${playerId}`;
  if (await getCachedValue<boolean>(retryKey)) return false;
  await setCachedValue(retryKey, true, DISCORD_PLAYER_CHAMPION_STATS_RETRY_TTL_SECONDS);
  return refreshPlayerChampionStatsIfExpired(
    playerId,
    DISCORD_PLAYER_CHAMPION_STATS_TTL_MS,
    'discord_player_champion_stats',
    'discord-player-champion-stats',
    beforeRefresh,
  );
}

async function refreshPlayerLoadouts(
  playerId: number,
  manual: boolean,
  beforeRefresh?: () => Promise<void>,
): Promise<{ freshness: PlayerLoadoutFreshness; refreshed: boolean }> {
  const client = await pool.connect();
  const lockName = `player-loadout-refresh:${playerId}`;
  let locked = false;
  try {
    await client.query('SELECT pg_advisory_lock(hashtext($1))', [lockName]);
    locked = true;
    const before = await readPlayerLoadoutFetchState(playerId, client);
    const freshness = getPlayerLoadoutFreshness(before);
    if (!manual && (!freshness.expired || freshness.manual_refresh_remaining_seconds > 0)) {
      return { freshness, refreshed: false };
    }
    if (manual && freshness.manual_refresh_remaining_seconds > 0) {
      const error: any = new Error('Loadouts were refreshed recently. Try again after the cooldown.');
      error.code = 'LOADOUT_REFRESH_COOLDOWN';
      error.freshness = freshness;
      throw error;
    }

    await beforeRefresh?.();

    // Count a manual attempt toward the cooldown even if Hi-Rez is unavailable.
    // This prevents repeated refresh clicks from consuming the API allowance.
    // Epoch keeps a first failed attempt expired, so normal page access may try
    // again once the vendor recovers without treating an empty cache as fresh.
    if (manual) {
      await client.query(
        `INSERT INTO player_loadout_fetches (player_id, fetched_at, last_manual_refresh_at)
         VALUES ($1, to_timestamp(0), now())
         ON CONFLICT (player_id) DO UPDATE SET last_manual_refresh_at = now()`,
        [playerId],
      );
    }

    const raw = await getPlayerLoadouts(playerId, 'operator_raw_audit');
    await recordRawHirezResponse({
      endpoint: 'getplayerloadouts',
      operation: 'getPlayerLoadouts',
      entityType: 'player_loadout',
      entityId: playerId,
      params: { playerId, reason: manual ? 'manual_loadout_refresh' : 'loadout_ttl_refresh' },
      rawResponse: raw,
      source: manual ? 'player-loadout-manual-refresh' : 'player-loadout-ttl-refresh',
    }, client);
    if (!Array.isArray(raw)) throw new Error('Hi-Rez returned an invalid loadout response.');
    if (raw.some((row: any) => String(row?.ret_msg ?? '').trim()) && raw.every((row: any) => String(row?.ret_msg ?? '').trim())) {
      throw new Error('Hi-Rez could not return player loadouts right now.');
    }
    const normalizedDecks = raw
      .filter((row: any) => !String(row?.ret_msg ?? '').trim())
      .map((row: any) => normalizePlayerLoadoutDeck(row))
      .filter((deck): deck is StoredPlayerLoadout => deck !== null);
    const championIds = [...new Set(normalizedDecks.map((deck) => deck.championId))];
    const knownChampionRows = championIds.length > 0
      ? await client.query<{ id: number }>(
        'SELECT id FROM champions WHERE id = ANY($1::int[])',
        [championIds],
      )
      : { rows: [] as Array<{ id: number }> };
    const knownChampionIds = new Set(knownChampionRows.rows.map((row) => Number(row.id)));
    const decks = normalizedDecks.filter((deck) => knownChampionIds.has(deck.championId));
    if (decks.length !== normalizedDecks.length) {
      const skippedIds = [...new Set(
        normalizedDecks.filter((deck) => !knownChampionIds.has(deck.championId)).map((deck) => deck.championId),
      )];
      console.warn(
        `[PLAYER-LOADOUTS] Skipping ${normalizedDecks.length - decks.length} deck(s) for player ${playerId} `
        + `with unknown/non-playable champion IDs: ${skippedIds.join(',')}`,
      );
    }

    await client.query('BEGIN');
    try {
      for (const deck of decks) {
        await client.query(
          `INSERT INTO player_loadouts (player_id, champion_id, deck_id, deck_key, loadout_name, card_ids, card_levels, talent_id, fetched_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, now(), now())
           ON CONFLICT (player_id, deck_key) DO UPDATE SET
             champion_id = EXCLUDED.champion_id,
             deck_id = EXCLUDED.deck_id,
             loadout_name = EXCLUDED.loadout_name,
             card_ids = EXCLUDED.card_ids,
             card_levels = EXCLUDED.card_levels,
             fetched_at = now(),
             updated_at = now()`,
          [playerId, deck.championId, deck.deckId || null, deck.deckKey, deck.deckName, deck.cardIds, deck.cardLevels],
        );
      }
      if (decks.length > 0) {
        await client.query('DELETE FROM player_loadouts WHERE player_id = $1 AND NOT (deck_key = ANY($2::TEXT[]))', [playerId, decks.map((deck) => deck.deckKey)]);
      } else {
        await client.query('DELETE FROM player_loadouts WHERE player_id = $1', [playerId]);
      }
      await client.query(
        `INSERT INTO player_loadout_fetches (player_id, fetched_at, last_manual_refresh_at)
         VALUES ($1, now(), CASE WHEN $2 THEN now() ELSE NULL END)
         ON CONFLICT (player_id) DO UPDATE SET
           fetched_at = now(),
           last_manual_refresh_at = CASE WHEN $2 THEN now() ELSE player_loadout_fetches.last_manual_refresh_at END`,
        [playerId, manual],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    }
    const after = await readPlayerLoadoutFetchState(playerId, client);
    return { freshness: getPlayerLoadoutFreshness(after), refreshed: true };
  } finally {
    if (locked) await client.query('SELECT pg_advisory_unlock(hashtext($1))', [lockName]);
    client.release();
  }
}

async function refreshPlayerProfileFromHirez(
  playerId: number,
  client?: PoolClient,
  reason = 'manual_profile_refresh',
  source = 'player-profile-manual-refresh',
): Promise<{ audit: any; count: number }> {
  const consumer = source.startsWith('discord-') ? 'discord_player_command' : 'manual_profile_refresh';
  const raw = await getPlayerBatch([playerId], consumer);
  const audit = await recordRawHirezResponse({
    endpoint: 'getplayerbatch',
    operation: 'getPlayerBatch',
    entityType: 'player',
    entityId: playerId,
    params: { playerIds: [playerId], reason },
    rawResponse: raw,
    source,
  }, client);

  const profileRaw = Array.isArray(raw)
    ? raw.find((row: any) => Number(row?.Id || row?.ActivePlayerId || 0) === playerId) || raw[0]
    : null;
  const profile = profileRaw ? normalizePlayerProfile(profileRaw) : null;
  if (!profile || profile.player_id <= 0) {
    throw new Error(`Hi-Rez returned no usable player profile for ${playerId}`);
  }

  await upsertPlayerProfile(profile, client);
  return { audit, count: Array.isArray(raw) ? raw.length : 0 };
}

/**
 * Refresh one stale profile while holding a per-player PostgreSQL advisory lock.
 *
 * The explicit refresh caller rechecks hirez_profile_refreshed_at after
 * acquiring the lock. This prevents double-clicks and separate backend
 * instances from consuming duplicate Hi-Rez requests for the same player.
 */
async function refreshPlayerProfileIfExpired(
  playerId: number,
  ttlMs = PLAYER_PROFILE_TTL_MS,
  reason = 'manual_profile_refresh',
  source = 'player-profile-manual-refresh',
  beforeRefresh?: () => Promise<void>,
  forceRefresh = false,
): Promise<PlayerProfileRefreshResult> {
  const client = await pool.connect();
  const lockName = `player-profile-refresh:${playerId}`;
  let locked = false;

  try {
    await client.query('SELECT pg_advisory_lock(hashtext($1))', [lockName]);
    locked = true;

    const currentResult = await client.query(
      'SELECT id, hirez_profile_refreshed_at FROM players WHERE id = $1',
      [playerId],
    );
    const current = currentResult.rows[0] || null;
    const currentFreshness = getPlayerProfileFreshness(current, Date.now(), ttlMs);
    if (!forceRefresh && current && !currentFreshness.expired) {
      return { refreshed: false, freshness: currentFreshness };
    }

    await beforeRefresh?.();
    const refresh = await refreshPlayerProfileFromHirez(playerId, client, reason, source);
    const refreshedResult = await client.query(
      'SELECT id, hirez_profile_refreshed_at FROM players WHERE id = $1',
      [playerId],
    );
    const refreshed = refreshedResult.rows[0] || null;
    return {
      refreshed: true,
      freshness: getPlayerProfileFreshness(refreshed, Date.now(), ttlMs),
      audit: refresh.audit,
      response_count: refresh.count,
    };
  } finally {
    if (locked) {
      try {
        await client.query('SELECT pg_advisory_unlock(hashtext($1))', [lockName]);
      } catch (error) {
        console.warn(`[PLAYER-PROFILE] Failed to release refresh lock for ${playerId}: ${error}`);
      }
    }
    client.release();
  }
}

export default async function playersRoutes(fastify: FastifyInstance) {
  registerReadThroughCache(fastify, {
    namespace: 'route:players',
    shouldCache: (req) => (
      req.url.startsWith('/players/overview')
      ||
      req.url.startsWith('/players/boosted')
      ||
      req.url.startsWith('/players/automatic-afk')
      ||
      req.url.startsWith('/players/search')
      || req.url.startsWith('/players/alt-account-relations')
      || req.url.startsWith('/players/leaderboard/class')
      || req.url.startsWith('/players/leaderboard/champion-elo')
      || req.url.startsWith('/players/leaderboard/performance')
    ),
    ttlSeconds: (req) => {
      if (req.url.startsWith('/players/overview')) return 300;
      if (req.url.startsWith('/players/search')) return 60;
      if (req.url.startsWith('/players/alt-account-relations')) return 60;
      return 300;
    },
  });

  // A cold overview fans out to several independently cached read models.
  // Coalesce simultaneous misses so one browser request performs the work and
  // the rest await the same result instead of duplicating database queries.
  let playersOverviewInFlight: Promise<Record<string, any>> | null = null;

  const buildPlayersOverview = async (): Promise<Record<string, any>> => {
    if (playersOverviewInFlight) return playersOverviewInFlight;

    playersOverviewInFlight = (async () => {
      const routes = {
        cheaters: '/players/search?cheater=true&limit=1&perPage=1',
        boosted: '/players/boosted?limit=1&perPage=1',
        suspicious: '/players/search?susOnly=true&limit=1&perPage=1',
        weirdos: '/players/search?weirdoOnly=true&limit=1&perPage=1',
        hallOfFame: '/players/search?hallOfFameOnly=true&limit=1&perPage=1',
        droppers: '/players/search?dropperOnly=true&limit=1&perPage=1',
        afkWintrade: '/players/search?afkWintradeOnly=true&limit=1&perPage=1',
        altAccounts: '/players/search?altAccountOnly=true&limit=1&perPage=1',
        privateAccounts: '/player-ext/private?perPage=1',
        parties: '/coplay/parties?perPage=1',
      } as const;

      const entries = await Promise.all(
        Object.entries(routes).map(async ([key, url]) => {
          const response = await fastify.inject({ method: 'GET', url, headers: internalRequestHeaders() });
          if (response.statusCode >= 400) {
            fastify.log.warn({ source: key, statusCode: response.statusCode }, 'Player overview source failed');
            return [key, null] as const;
          }
          return [key, response.json()] as const;
        }),
      );

      const source = Object.fromEntries(entries) as Record<string, any>;
      return {
        champion_elo: { data: [] },
        performance: {},
        ranked: [],
        account_elo: { data: [] },
        cheaters: source.cheaters,
        boosted: source.boosted,
        suspicious: source.suspicious,
        weirdos: source.weirdos,
        hall_of_fame: source.hallOfFame,
        droppers: source.droppers,
        afk_wintrade: source.afkWintrade,
        alt_accounts: source.altAccounts,
        private_accounts: source.privateAccounts,
        party_pairs: source.parties,
        community_counts: {
          cheaters: Number(source.cheaters?.[0]?.total_count ?? 0),
          boosted: Number(source.boosted?.[0]?.total_count ?? 0),
          suspicious: Number(source.suspicious?.[0]?.total_count ?? 0),
          weirdos: Number(source.weirdos?.[0]?.total_count ?? 0),
          hall_of_fame: Number(source.hallOfFame?.[0]?.total_count ?? 0),
          droppers: Number(source.droppers?.[0]?.total_count ?? 0),
          afk_wintrade: Number(source.afkWintrade?.[0]?.total_count ?? 0),
          alt_accounts: Number(source.altAccounts?.[0]?.total_count ?? 0),
        },
        directory_counts: {
          private_accounts: Number(source.privateAccounts?.[0]?.total_count ?? 0),
          parties: Number(source.parties?.[0]?.total_count ?? 0),
        },
      };
    })();

    try {
      return await playersOverviewInFlight;
    } finally {
      playersOverviewInFlight = null;
    }
  };

  /**
   * GET /players/overview — Cached composition of the directory landing page.
   *
   * The page used to issue eleven browser requests for these independent
   * cards. Composing them here removes that request fan-out, while
   * `fastify.inject` deliberately reuses each established route (including
   * its validation and route-level cache) without an HTTP round trip.
   */
  fastify.get('/overview', async (_req: any, reply: any) => {
    reply.header('Cache-Control', 'public, max-age=60, s-maxage=300, stale-while-revalidate=1800');
    return buildPlayersOverview();
  });

  /**
   * GET /players/search — Search players by name with optional filters.
   *
   * Query params:
   *   ?name=        — Player name substring or exact numeric player ID
   *   ?region=      — Filter by region (e.g. "na", "eu")
   *   ?platform=    — Filter by platform (e.g. "pc", "playstation")
   *   ?tierMin=     — Minimum KBM tier (0-26)
   *   ?tierMax=     — Maximum KBM tier (0-26)
   *   ?cheater=     — "true" or "false" to filter by cheater flag
   *   ?susOnly=     — "true" to return suspicious-but-not-confirmed players
   *   ?weirdoOnly=  — "true" to return players with community Weirdo votes
   *   ?hallOfFameOnly= — "true" to return Hall of Fame community votes
   *   ?limit=       — Max results (default 20, max 100)
   *
   * Returns: Array of player summaries with moderation flags plus rolling
   * performance metrics. This endpoint intentionally keeps the old bare-array
   * shape because several frontend consumers use it through the generic
   * fetchJson() helper that unwraps { data } envelopes.
   */
  fastify.get('/search', async (req: any, reply: any) => {
    const name = (req.query.name || req.query.q) as string | undefined;
    const susOnly = req.query.susOnly === 'true';
    const weirdoOnly = req.query.weirdoOnly === 'true';
    const hallOfFameOnly = req.query.hallOfFameOnly === 'true';
    const dropperOnly = req.query.dropperOnly === 'true';
    const afkWintradeOnly = req.query.afkWintradeOnly === 'true';
    const altAccountOnly = req.query.altAccountOnly === 'true';
    const communityFilters = Number(susOnly) + Number(weirdoOnly) + Number(hallOfFameOnly)
      + Number(dropperOnly) + Number(afkWintradeOnly) + Number(altAccountOnly);
    const hasModerationFilter = req.query.cheater !== undefined || communityFilters > 0;
    if (!name && !hasModerationFilter) {
      return reply.status(400).send(err('VALIDATION', 'Missing required query parameter: name or a player moderation filter'));
    }
    if (communityFilters > 1) {
      return reply.status(400).send(err('VALIDATION', 'Only one community player filter may be used at a time'));
    }

    const fb = new FilterBuilder();
    if (name) {
      if (/^\d+$/.test(name.trim())) fb.eq('id', name.trim());
      else fb.like('name', `%${escapeLike(name)}%`);
    }
    if (req.query.region) fb.eq('region', req.query.region);
    if (req.query.platform) fb.eq('platform', req.query.platform);
    if (req.query.tierMin) fb.gte('kbm_tier', parseInt(req.query.tierMin, 10));
    if (req.query.tierMax) fb.lte('kbm_tier', parseInt(req.query.tierMax, 10));
    if (req.query.cheater === 'true') fb.eq('cheater', true);
    if (req.query.cheater === 'false') fb.eq('cheater', false);
    if (susOnly) {
      fb.eq('cheater', false);
      fb.gt('sus_count', 0);
    }
    if (weirdoOnly) fb.gt('weirdo_count', 0);
    if (hallOfFameOnly) fb.gt('hall_of_fame_count', 0);
    if (dropperOnly) fb.eq('dropper', true);
    if (afkWintradeOnly) fb.eq('afk_wintrade', true);
    if (altAccountOnly) fb.eq('alt_account', true);

    const { clause, params } = fb.build();
    const limit = parseLimit(req.query.limit ?? req.query.perPage, 20, 100);
    const offset = Math.max(0, parseInt(String(req.query.offset ?? '0'), 10) || 0);
    const reasonVoteType = susOnly
      ? 'suspicious'
      : req.query.cheater === 'true'
        ? 'cheater'
        : null;
    const topReasonsSelect = reasonVoteType
      ? `COALESCE((
           SELECT jsonb_agg(
             jsonb_build_object('reason', reason_counts.reason, 'count', reason_counts.reason_count)
             ORDER BY reason_counts.reason_count DESC, reason_counts.last_reported_at DESC
           )
           FROM (
             SELECT
               btrim(pcv.reason) AS reason,
               COUNT(*)::INT AS reason_count,
               MAX(pcv.created_at) AS last_reported_at
             FROM player_community_votes pcv
             WHERE pcv.player_id = players.id
               AND pcv.vote_type = '${reasonVoteType}'
               AND btrim(pcv.reason) <> ''
             GROUP BY btrim(pcv.reason)
             ORDER BY reason_count DESC, last_reported_at DESC
             LIMIT ${reasonVoteType === 'cheater' ? 1 : 3}
           ) reason_counts
         ), '[]'::jsonb)`
      : `'[]'::jsonb`;
    reply.header('Cache-Control', 'public, max-age=60');
    const results = await query(
      `SELECT
        id, name, level, wins, losses, kbm_tier, kbm_points, region, platform,
        cheater, sus_count, weirdo_count, hall_of_fame_count,
        dropper, afk_wintrade, alt_account,
        EXISTS (
          SELECT 1 FROM player_boosted_associations association
          WHERE association.player_id = players.id
        ) AS boosted,
        avg_dpm, avg_hpm, avg_egpm, avg_mpm, total_matches,
        ${topReasonsSelect} AS top_reasons,
        COUNT(*) OVER() AS total_count,
        ROUND(
          CASE
            WHEN total_matches > 0 THEN total_wins::NUMERIC * 100 / total_matches
            WHEN (wins + losses) > 0 THEN wins::NUMERIC * 100 / (wins + losses)
            ELSE NULL
          END,
          2
        ) AS win_rate
       FROM players${clause}
       ORDER BY ${weirdoOnly ? 'weirdo_count' : hallOfFameOnly ? 'hall_of_fame_count' : 'total_matches'} DESC, name ASC
       LIMIT $${params.length + 1}
       OFFSET $${params.length + 2}`,
      [...params, limit, offset]
    );
    return results;
  });

  /**
   * GET /players/discord?player=<name-or-id>&history=true
   *
   * Discord commands are allowed to discover a player that has not reached our
   * database yet. A durable lookup cache prevents repeated unknown-name
   * searches from spending Hi-Rez calls, while the profile timestamp limits a
   * known player's refresh to once per day across bot instances.
   * Match history deliberately goes through getMatchHistory without force so
   * it uses the same durable DB cache and TTL as the public web history path.
   */
  fastify.get('/discord', async (req: any, reply: any) => {
    const input = String(req.query.player ?? '').trim();
    if (!input || input.length > 128) {
      return reply.status(400).send(err('VALIDATION', 'Missing or invalid player name or ID'));
    }

    const lookupKey = input.normalize('NFKC').toLocaleLowerCase();
    const numericId = /^\d+$/.test(input) ? Number(input) : 0;
    let playerId = 0;
    if (numericId > 0) {
      const local = await one<{ id: number }>('SELECT id FROM players WHERE id = $1', [numericId]);
      playerId = Number(local?.id ?? 0);
    } else {
      const local = await one<{ id: number }>(
        `SELECT id FROM players
         WHERE lower(name) = lower($1)
            OR lower(COALESCE(hz_player_name, '')) = lower($1)
            OR lower(COALESCE(hz_gamer_tag, '')) = lower($1)
         ORDER BY id ASC
         LIMIT 1`,
        [input],
      );
      playerId = Number(local?.id ?? 0);
    }

    if (!playerId) {
      const cached = await one<{ player_id: number | null }>(
        `SELECT player_id FROM discord_player_lookup_cache
         WHERE lookup_key = $1 AND expires_at > now()`,
        [lookupKey],
      );
      if (cached) playerId = Number(cached.player_id ?? 0);
      if (cached && !playerId) {
        return reply.status(404).send(err('NOT_FOUND', 'Player not found', { player: input, cached: true }));
      }
      // A numeric ID has no separate name-resolution call. If it is not
      // cached, profile refresh below is its one permitted Hi-Rez attempt.
      if (!playerId && numericId > 0) playerId = numericId;
    }

    if (!playerId && !numericId) {
      await guardVendorFallback(req, reply, {
        scope: 'discord-player-name',
        entity: lookupKey,
      });
      const remote = await getPlayerIdByName(input, 'discord_player_command');
      const candidate = Array.isArray(remote) ? remote[0] : null;
      playerId = Number(candidate?.player_id ?? candidate?.playerId ?? candidate?.Id ?? candidate?.id ?? 0);
      await query(
        `INSERT INTO discord_player_lookup_cache (lookup_key, player_id, fetched_at, expires_at)
         VALUES ($1, $2, now(), now() + interval '24 hours')
         ON CONFLICT (lookup_key) DO UPDATE SET
           player_id = EXCLUDED.player_id, fetched_at = EXCLUDED.fetched_at, expires_at = EXCLUDED.expires_at`,
        [lookupKey, playerId > 0 ? playerId : null],
      );
      if (!playerId) return reply.status(404).send(err('NOT_FOUND', 'Player not found', { player: input }));
    }

    try {
      await refreshPlayerProfileIfExpired(
        playerId,
        DISCORD_PLAYER_PROFILE_TTL_MS,
        'discord_player_lookup',
        'discord-player-lookup',
        () => guardVendorFallback(req, reply, {
          scope: 'discord-player-profile',
          entity: playerId,
        }),
      );
    } catch (error) {
      const existing = await one('SELECT id FROM players WHERE id = $1', [playerId]);
      if (!existing) {
        // A Hi-Rez profile response that contains no usable player is a stable
        // not-found result. Cache that fact so repeated numeric-ID commands do
        // not turn into repeated outbound requests. Do not cache outages.
        if (String(error).includes('returned no usable player profile')) {
          await query(
            `INSERT INTO discord_player_lookup_cache (lookup_key, player_id, fetched_at, expires_at)
             VALUES ($1, NULL, now(), now() + interval '24 hours')
             ON CONFLICT (lookup_key) DO UPDATE SET
               player_id = NULL, fetched_at = EXCLUDED.fetched_at, expires_at = EXCLUDED.expires_at`,
            [lookupKey],
          );
          return reply.status(404).send(err('NOT_FOUND', 'Player not found', { player: input }));
        }
        throw error;
      }
      // Hi-Rez being unavailable must not turn a cached Discord lookup into a
      // failure. Serve the last known database record instead.
      console.warn(`[DISCORD-PLAYER] Refresh failed for ${playerId}; serving database copy: ${error}`);
    }

    const player = await one('SELECT p.* FROM players p WHERE p.id = $1', [playerId]);
    if (!player) return reply.status(404).send(err('NOT_FOUND', 'Player not found', { player: input }));
    try {
      await refreshDiscordPlayerChampionStatsIfExpired(
        playerId,
        () => guardVendorFallback(req, reply, {
          scope: 'discord-player-champions',
          entity: playerId,
        }),
      );
    } catch (error) {
      // Career totals are supplemental. Preserve the cached profile response
      // if Hi-Rez is unavailable and omit KDA unless populated totals exist.
      console.warn(`[DISCORD-PLAYER] Champion totals refresh failed for ${playerId}; serving cached totals: ${error}`);
    }
    const globalStats = await readPlayerGlobalStats(playerId);
    const wantsHistory = req.query.history === 'true';
    let history: any[] | undefined;
    if (wantsHistory) {
      if (!await hasFreshPlayerHistoryCache(playerId)) {
        await guardVendorFallback(req, reply, {
          scope: 'discord-player-history',
          entity: playerId,
        });
      }
      history = await getMatchHistory(playerId, 50, false, 'discord_player_command');
    }
    return {
      player,
      globalStats,
      profileRefresh: {
        ...getPlayerProfileFreshness(player, Date.now(), DISCORD_PLAYER_PROFILE_TTL_MS),
        source: 'database-or-hirez',
      },
      ...(wantsHistory ? { history } : {}),
    };
  });

  /**
   * GET /players/discord/saved-player?discordUserId=<snowflake>
   *
   * Returns the durable default Paladins account for a Discord user. Saved
   * mappings always require the configured bot service credential, so public
   * clients cannot read or replace another Discord user's default.
   */
  fastify.get('/discord/saved-player', async (req: any, reply: any) => {
    const discordUserId = String(req.query.discordUserId ?? '').trim();
    if (!/^\d{1,32}$/.test(discordUserId)) {
      return reply.status(400).send(err('VALIDATION', 'Missing or invalid Discord user ID'));
    }

    const saved = await one<{ id: string; name: string }>(
      `SELECT p.id::text AS id, ${DISPLAY_NAME_SQL} AS name
       FROM discord_saved_players dsp
       JOIN players p ON p.id = dsp.player_id
       WHERE dsp.discord_user_id = $1`,
      [discordUserId],
    );
    if (!saved) {
      return reply.status(404).send(err(
        'NO_SAVED_PLAYER',
        'No saved player is linked to this Discord account',
      ));
    }
    reply.header('Cache-Control', 'private, no-store');
    return { player: saved };
  });

  /**
   * PUT /players/discord/saved-player
   *
   * The bot first resolves/refreshes the entered name through GET
   * /players/discord, then persists that authoritative numeric player ID here.
   */
  fastify.put('/discord/saved-player', async (req: any, reply: any) => {
    const discordUserId = String(req.body?.discordUserId ?? '').trim();
    const playerId = String(req.body?.playerId ?? '').trim();
    if (!/^\d{1,32}$/.test(discordUserId)) {
      return reply.status(400).send(err('VALIDATION', 'Missing or invalid Discord user ID'));
    }
    if (
      !/^\d{1,20}$/.test(playerId)
      || BigInt(playerId) <= 0n
      || BigInt(playerId) > 9_223_372_036_854_775_807n
    ) {
      return reply.status(400).send(err('VALIDATION', 'Missing or invalid Paladins player ID'));
    }

    const player = await one<{ id: string; name: string }>(
      `SELECT p.id::text AS id, ${DISPLAY_NAME_SQL} AS name
       FROM players p
       WHERE p.id = $1`,
      [playerId],
    );
    if (!player) {
      return reply.status(404).send(err('NOT_FOUND', 'Player not found', { playerId }));
    }

    await query(
      `INSERT INTO discord_saved_players (discord_user_id, player_id, saved_at, updated_at)
       VALUES ($1, $2, now(), now())
       ON CONFLICT (discord_user_id) DO UPDATE SET
         player_id = EXCLUDED.player_id,
         updated_at = now()`,
      [discordUserId, playerId],
    );
    reply.header('Cache-Control', 'private, no-store');
    return { player };
  });

  /**
   * GET /players/leaderboard/class — Account or champion Glicko leaderboard.
   *
   * mode=account:
   *   Reads player_queue_ratings. This is the marketing-facing "Account ELO"
   *   leaderboard: one row per player per queue, no champion column, no role
   *   filter. The /players/class/[role] page uses this by default so it does
   *   not imply a champion-specific value where the database is storing an
   *   account-level queue rating.
   *
   * mode=champion:
   *   Reads player_champion_ratings and filters champions by role. This is the
   *   champion-specific leaderboard and is intentionally separate from Account
   *   ELO. The queue filter is a qualification check because the rating table is
   *   currently per player/champion, not per player/champion/queue.
   */
  fastify.get('/leaderboard/class', async (req: any, reply: any) => {
    const normalizedRole = normalizeRole(req.query.role);
    if (!normalizedRole) {
      return reply.status(400).send(err('VALIDATION', 'Invalid role. Use Frontline, Damage, Flank, or Support.'));
    }
    const queueId = parseQueueId(req.query.queueId);
    if (!queueId) return reply.status(400).send(err('VALIDATION', 'Invalid queueId.'));

    const mode = req.query.mode === 'account' ? 'account' : 'champion';
    const limit = parseLimit(req.query.limit);
    reply.header('Cache-Control', 'public, max-age=300');

    if (mode === 'account') {
      const rows = await query(
        `SELECT
          ROW_NUMBER() OVER (ORDER BY pqr.mu DESC, pqr.updated_at DESC, ${DISPLAY_NAME_SQL} ASC) AS rank,
          pqr.player_id,
          ${DISPLAY_NAME_SQL} AS player_name,
          NULL::TEXT AS champion_name,
          NULL::INT AS champion_id,
          pqr.mu::DOUBLE PRECISION AS elo,
          pqr.mu::DOUBLE PRECISION AS mu,
          pqr.phi::DOUBLE PRECISION AS phi,
          COALESCE(rc.total_matches, 0)::BIGINT AS total_matches,
          COALESCE(rc.total_wins, 0)::BIGINT AS total_wins,
          ROUND(
            CASE
              WHEN COALESCE(rc.total_matches, 0) > 0
                THEN COALESCE(rc.total_wins, 0)::NUMERIC * 100 / rc.total_matches
              ELSE NULL
            END,
            2
          ) AS win_rate,
          p.region,
          COUNT(*) OVER() AS _total
        FROM player_queue_ratings pqr
        JOIN players p ON p.id = pqr.player_id
        JOIN player_queue_rating_summary rc
          ON rc.player_id = pqr.player_id AND rc.queue_id = pqr.queue_id
        WHERE pqr.queue_id = $1
          AND pqr.mu BETWEEN 0 AND 3500 AND pqr.phi BETWEEN 1 AND 350 AND pqr.volatility BETWEEN 0.001 AND 0.2
          AND NOT p.cheater
        ORDER BY pqr.mu DESC, pqr.updated_at DESC, ${DISPLAY_NAME_SQL} ASC
        LIMIT $2`,
        [queueId, limit]
      );

      const total = rows.length > 0 ? Number((rows[0] as any)._total) : 0;
      return {
        data: rows.map(({ _total, ...row }: any) => row),
        total,
        mode,
        role: normalizedRole.role,
        queue_id: queueId,
        page: { current: 1, size: limit, totalPages: total > 0 ? Math.ceil(total / limit) : 0 },
      };
    }

    const rows = await query(
      `SELECT
        ROW_NUMBER() OVER (
          ORDER BY best.mu DESC, best.matches_played DESC, best.wins DESC, best.player_id ASC
        ) AS rank,
        best.player_id,
        ${DISPLAY_NAME_SQL} AS player_name,
        c.name AS champion_name,
        best.champion_id,
        best.mu::DOUBLE PRECISION AS elo,
        best.mu::DOUBLE PRECISION AS mu,
        best.phi::DOUBLE PRECISION AS phi,
        ROUND(
          CASE WHEN best.matches_played > 0 THEN best.wins::NUMERIC * 100 / best.matches_played ELSE NULL END,
          2
        ) AS win_rate,
        best.matches_played AS total_matches,
        best.wins AS total_wins,
        p.region,
        COUNT(*) OVER() AS _total
      FROM player_best_champion_ratings best
      JOIN players p ON p.id = best.player_id
      JOIN champions c ON c.id = best.champion_id
      WHERE best.role_id = $1
        AND best.queue_id = $2
        AND NOT p.cheater
      ORDER BY best.mu DESC, best.matches_played DESC, best.wins DESC, best.player_id ASC
      LIMIT $3`,
      [normalizedRole.roleId, queueId, limit]
    );

    const total = rows.length > 0 ? Number((rows[0] as any)._total) : 0;
    return {
      data: rows.map(({ _total, ...row }: any) => row),
      total,
      mode,
      role: normalizedRole.role,
      queue_id: queueId,
      page: { current: 1, size: limit, totalPages: total > 0 ? Math.ceil(total / limit) : 0 },
    };
  });

  /**
   * GET /players/leaderboard/champion-elo — Global champion ELO leaderboard.
   *
   * Supports three modes:
   *   1. No filter: top 100 players by their best champion's ELO (global)
   *   2. ?role=Damage: top N players whose best champion is in that class
   *   3. ?championId=2417: top N players specifically for that champion
   */
  fastify.get('/leaderboard/champion-elo', async (req: any, reply: any) => {
    const queueId = parseQueueId(req.query.queueId);
    if (!queueId) return reply.status(400).send(err('VALIDATION', 'Invalid queueId.'));

    const championId = req.query.championId ? parseInt(req.query.championId as string, 10) : null;
    if (championId !== null && (!Number.isInteger(championId) || championId <= 0)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
    }

    const limit = Math.min(parseInt(req.query.limit as string) || 100, 200);
    reply.header('Cache-Control', 'public, max-age=300');

    // Specific champion filter — direct query, no "best per player" logic
    if (championId) {
      const rows = await query(
        `SELECT
           ROW_NUMBER() OVER (ORDER BY pcr.mu DESC, pcr.matches_played DESC) AS rank,
           pcr.player_id,
           ${DISPLAY_NAME_SQL} AS player_name,
           pcr.champion_id,
           c.name AS champion_name,
           ${championRoleSql('c')} AS class_name,
           pcr.mu::DOUBLE PRECISION AS elo,
           pcr.phi::DOUBLE PRECISION AS phi,
           pcr.matches_played AS total_matches,
           pcr.wins AS total_wins,
           ROUND(CASE WHEN pcr.matches_played > 0 THEN pcr.wins::NUMERIC * 100 / pcr.matches_played ELSE NULL END, 2) AS win_rate,
           p.region,
           COUNT(*) OVER() AS _total
         FROM player_champion_ratings pcr
         JOIN champions c ON c.id = pcr.champion_id
         JOIN players p ON p.id = pcr.player_id
         WHERE pcr.champion_id = $1
           AND NOT p.cheater
           AND pcr.matches_played > 0
         ORDER BY pcr.mu DESC, pcr.matches_played DESC
         LIMIT $2`,
        [championId, limit]
      );
      const total = rows.length > 0 ? Number((rows[0] as any)._total) : 0;
      return {
        data: rows.map(({ _total, ...row }: any) => row),
        total,
        champion_id: championId,
        queue_id: queueId,
      };
    }

    // Global or role-filtered: top players by their BEST champion's ELO
    const roleFilter = req.query.role ? normalizeRole(req.query.role) : null;
    if (req.query.role && !roleFilter) {
      return reply.status(400).send(err('VALIDATION', 'Invalid role. Use Frontline, Damage, Flank, or Support.'));
    }

    const rows = await query(
      `SELECT
        ROW_NUMBER() OVER (
          ORDER BY best.mu DESC, best.matches_played DESC, best.wins DESC, best.player_id ASC
        ) AS rank,
        best.player_id,
        ${DISPLAY_NAME_SQL} AS player_name,
        c.name AS champion_name,
        best.champion_id,
        ${championRoleSql('c')} AS class_name,
        best.mu::DOUBLE PRECISION AS elo,
        best.phi::DOUBLE PRECISION AS phi,
        ROUND(
          CASE WHEN best.matches_played > 0 THEN best.wins::NUMERIC * 100 / best.matches_played ELSE NULL END,
          2
        ) AS win_rate,
        best.matches_played AS total_matches,
        best.wins AS total_wins,
        p.region,
        COUNT(*) OVER() AS _total
      FROM player_best_champion_ratings best
      JOIN players p ON p.id = best.player_id
      JOIN champions c ON c.id = best.champion_id
      WHERE best.queue_id = $1
        AND best.role_id = $2
        AND NOT p.cheater
      ORDER BY best.mu DESC, best.matches_played DESC, best.wins DESC, best.player_id ASC
      LIMIT $3`,
      [queueId, roleFilter?.roleId ?? 0, limit]
    );

    const total = rows.length > 0 ? Number((rows[0] as any)._total) : 0;
    return {
      data: rows.map(({ _total, ...row }: any) => row),
      total,
      role: roleFilter?.role ?? 'Global',
      queue_id: queueId,
    };
  });

  /**
   * GET /players/leaderboard/performance
   *
   * One row is one ranked match performance. This is deliberately independent
   * of the cumulative `players.avg_*` projections: a record leaderboard ranks
   * the highest authoritative per-match DPM/HPM/GPM/MPM facts and links back
   * to the match that produced each value.
   */
  fastify.get('/leaderboard/performance', async (req: any, reply: any) => {
    const normalizedMetric = normalizePerformanceMetric(req.query.metric);
    if (!normalizedMetric) {
      return reply.status(400).send(err('VALIDATION', 'Invalid metric. Use dpm, hpm, gpm, or mpm.'));
    }
    const scope = String(req.query.scope ?? 'ranked').trim().toLowerCase();
    if (!['ranked', 'casual'].includes(scope)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid scope. Use ranked or casual.'));
    }

    const roleFilter = req.query.role ? normalizeRole(req.query.role) : null;
    if (req.query.role && !roleFilter) {
      return reply.status(400).send(err('VALIDATION', 'Invalid role. Use Frontline, Damage, Flank, or Support.'));
    }

    const limit = parseLimit(req.query.limit);
    reply.header('Cache-Control', 'public, max-age=60, stale-while-revalidate=300');

    if (scope === 'casual') {
      const metricExpression = CASUAL_PERFORMANCE_EXPRESSIONS[normalizedMetric.metric];
      const params: any[] = [];
      const performanceWhere: string[] = [
        'cm.stats_eligible = true',
        `cm.quality = 'complete'`,
        'cmp.stats_eligible = true',
        `cmp.participant_kind = 'human'`,
        'cmp.player_id > 0',
        'cmp.task_force IN (1, 2)',
        `lower(COALESCE(cmp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')`,
        'cm.duration_seconds > 0',
        `(${metricExpression}) > 0`,
        'NOT COALESCE(p.cheater, false)',
      ];

      if (roleFilter) {
        params.push(roleFilter.role);
        performanceWhere.push(`${championRoleSql('c')} = $${params.length}`);
      }
      if (req.query.region) {
        params.push(req.query.region);
        performanceWhere.push(`cm.region = $${params.length}`);
      }
      params.push(limit);
      const limitParam = params.length;

      const rows = await query(
        `SELECT
          cmp.match_id,
          cm.entry_datetime,
          cmp.player_id,
          COALESCE(${DISPLAY_NAME_SQL}, NULLIF(cmp.player_name, ''), 'Player ' || cmp.player_id::text) AS player_name,
          COALESCE(NULLIF(cmp.champion_name, ''), c.name) AS champion_name,
          cmp.champion_id,
          ${championRoleSql('c')} AS class_name,
          (${metricExpression})::DOUBLE PRECISION AS value,
          cm.region,
          COALESCE(NULLIF(cmp.platform, ''), p.platform) AS platform
        FROM casual_match_players cmp
        JOIN casual_matches cm ON cm.match_id = cmp.match_id
        LEFT JOIN players p ON p.id = cmp.player_id
        LEFT JOIN champions c ON c.id = cmp.champion_id
        WHERE ${performanceWhere.join(' AND ')}
        ORDER BY value DESC, cm.entry_datetime DESC, cmp.match_id DESC, cmp.player_id ASC
        LIMIT $${limitParam}`,
        params,
      );

      const data = rows.map((row: any, index: number) => ({ ...row, rank: index + 1 }));
      return {
        data,
        total: data.length,
        metric: normalizedMetric.metric,
        scope,
        queue_ids: [424, 452],
        page: { current: 1, size: limit, totalPages: data.length > 0 ? 1 : 0 },
      };
    }

    const queueId = parseQueueId(req.query.queueId);
    if (!queueId) return reply.status(400).send(err('VALIDATION', 'Invalid queueId.'));
    const params: any[] = [queueId];
    const performanceWhere: string[] = [
      'pr.queue_id = $1',
      `${normalizedMetric.projectionColumn} IS NOT NULL`,
      `${normalizedMetric.projectionColumn} > 0`,
      'NOT p.cheater',
    ];

    if (roleFilter) {
      params.push(roleFilter.role);
      performanceWhere.push(`pr.role_name = $${params.length}`);
    }
    if (req.query.region) {
      params.push(req.query.region);
      performanceWhere.push(`COALESCE(NULLIF(pr.region, ''), p.region) = $${params.length}`);
    }
    params.push(limit);
    const limitParam = params.length;

    if (queueId !== RANKED_QUEUE_ID) {
      return {
        data: [],
        total: 0,
        metric: normalizedMetric.metric,
        queue_id: queueId,
        page: { current: 1, size: limit, totalPages: 0 },
      };
    }

    const rows = await query(
      `SELECT
        pr.match_id,
        pr.entry_datetime,
        pr.player_id,
        ${DISPLAY_NAME_SQL} AS player_name,
        pr.champion_name,
        pr.champion_id,
        pr.role_name AS class_name,
        ${normalizedMetric.projectionColumn}::DOUBLE PRECISION AS value,
        COALESCE(NULLIF(pr.region, ''), p.region) AS region,
        COALESCE(NULLIF(pr.platform, ''), p.platform) AS platform
      FROM performance_records_ranked pr
      JOIN players p ON p.id = pr.player_id
      WHERE ${performanceWhere.join(' AND ')}
      ORDER BY ${normalizedMetric.projectionColumn} DESC, pr.entry_datetime DESC, pr.match_id DESC, pr.player_id ASC
      LIMIT $${limitParam}`,
      params,
    );

    const data = rows.map((row: any, index: number) => ({ ...row, rank: index + 1 }));
    return {
      data,
      total: data.length,
      metric: normalizedMetric.metric,
      scope,
      queue_id: queueId,
      page: { current: 1, size: limit, totalPages: data.length > 0 ? 1 : 0 },
    };
  });

  /**
   * GET /players/raw/profile?playerId=720714285
   *
   * Operator raw pass-through for Hi-Rez `getplayerbatch` by player id.
   *
   * This endpoint intentionally does not normalize, repair, or route the
   * payload into `raw_ingest_buffer`. It is used to answer "what did Hi-Rez
   * send for this account?" during audits such as corrupted display-name
   * investigations. Because raw_ingest_buffer is a short-lived worker queue,
   * the endpoint first writes the backend-observed payload to
   * `hirez_raw_api_responses` and only then returns the same raw array to the
   * caller. If the durable audit insert fails, the request fails too; that is
   * the safety property that keeps pass-through inspection from losing evidence.
   */
  fastify.get('/raw/profile', async (req: any, reply: any) => {
    const playerId = parseInt(String(req.query.playerId ?? req.query.id ?? ''), 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Missing or invalid query param: playerId'));
    }

    await guardVendorFallback(req, reply, {
      scope: 'raw-player-profile',
      entity: playerId,
    });
    const raw = await getPlayerBatch([playerId], 'operator_raw_audit');
    const audit = await recordRawHirezResponse({
      endpoint: 'getplayerbatch',
      operation: 'getPlayerBatch',
      entityType: 'player',
      entityId: playerId,
      params: { playerIds: [playerId] },
      rawResponse: raw,
    });

    return reply.code(200).send({
      endpoint: 'getplayerbatch',
      player_id: playerId,
      count: Array.isArray(raw) ? raw.length : null,
      audit,
      data: raw,
    });
  });

  /**
   * GET /players/raw/loadouts?playerId=720714285
   *
   * Operator raw pass-through for Hi-Rez `getplayerloadouts`. This must remain
   * separate from `/players/:id/loadouts`: it performs no normalization or
   * profile-table upsert, and persists the untouched response to the durable
   * raw audit before returning it for contract verification.
   */
  fastify.get('/raw/loadouts', async (req: any, reply: any) => {
    const playerId = parseInt(String(req.query.playerId ?? req.query.id ?? ''), 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Missing or invalid query param: playerId'));
    }

    await guardVendorFallback(req, reply, {
      scope: 'raw-player-loadouts',
      entity: playerId,
    });
    const raw = await getPlayerLoadouts(playerId, 'manual_profile_refresh');
    const audit = await recordRawHirezResponse({
      endpoint: 'getplayerloadouts',
      operation: 'getPlayerLoadouts',
      entityType: 'player_loadout',
      entityId: playerId,
      params: { playerId },
      rawResponse: raw,
    });

    return reply.code(200).send({
      endpoint: 'getplayerloadouts',
      player_id: playerId,
      count: Array.isArray(raw) ? raw.length : null,
      audit,
      data: raw,
    });
  });

  /**
   * GET /players/boosted — Players observed in a ranked party with a confirmed
   * cheater. The underlying view is derived from party_pair_stats, so this
   * updates automatically when either party evidence or a cheater flag changes.
   */
  fastify.get('/boosted', async (req: any, reply: any) => {
    const limit = parseLimit(req.query.limit ?? req.query.perPage, 20, 100);
    reply.header('Cache-Control', 'public, max-age=60');

    return query(`
      SELECT
        p.id,
        p.name,
        p.platform,
        p.region,
        p.kbm_tier,
        p.kbm_points,
        p.cheater,
        p.sus_count,
        p.weirdo_count,
        p.hall_of_fame_count,
        p.total_matches,
        p.total_wins,
        p.avg_dpm,
        p.avg_hpm,
        p.avg_egpm,
        p.avg_mpm,
        SUM(association.match_count)::INT AS party_match_count,
        MIN(association.first_seen) AS first_seen,
        MAX(association.last_seen) AS last_seen,
        jsonb_agg(
          jsonb_build_object(
            'id', cheater.id,
            'name', cheater.name,
            'match_count', association.match_count,
            'first_seen', association.first_seen,
            'last_seen', association.last_seen
          )
          ORDER BY association.match_count DESC, association.last_seen DESC, cheater.id
        ) AS cheaters,
        COUNT(*) OVER()::INT AS total_count,
        ROUND(
          CASE
            WHEN p.total_matches > 0 THEN p.total_wins::NUMERIC * 100 / p.total_matches
            WHEN (p.wins + p.losses) > 0 THEN p.wins::NUMERIC * 100 / (p.wins + p.losses)
            ELSE NULL
          END,
          2
        ) AS win_rate
      FROM player_boosted_associations association
      JOIN players p ON p.id = association.player_id
      JOIN players cheater ON cheater.id = association.cheater_id
      GROUP BY
        p.id, p.name, p.platform, p.region, p.kbm_tier, p.kbm_points,
        p.cheater, p.sus_count, p.weirdo_count, p.hall_of_fame_count,
        p.total_matches, p.total_wins, p.wins, p.losses,
        p.avg_dpm, p.avg_hpm, p.avg_egpm, p.avg_mpm
      ORDER BY party_match_count DESC, last_seen DESC, p.name ASC
      LIMIT $1
    `, [limit]);
  });

  /**
   * GET /players/boosted/:id — Boosted-player evidence detail.
   * Returns the current confirmed-cheater associations plus every canonical
   * ranked-party match supporting those associations.
   */
  fastify.get('/boosted/:id', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.id, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }
    reply.header('Cache-Control', 'public, max-age=60');

    const player = await one(`
      SELECT
        p.id,
        p.name,
        p.platform,
        p.region,
        p.kbm_tier,
        p.kbm_points,
        p.cheater,
        p.sus_count,
        p.weirdo_count,
        p.hall_of_fame_count,
        p.total_matches,
        p.total_wins,
        p.avg_dpm,
        p.avg_hpm,
        p.avg_egpm,
        p.avg_mpm,
        SUM(association.match_count)::INT AS party_match_count,
        MIN(association.first_seen) AS first_seen,
        MAX(association.last_seen) AS last_seen,
        jsonb_agg(
          jsonb_build_object(
            'id', cheater.id,
            'name', cheater.name,
            'match_count', association.match_count,
            'first_seen', association.first_seen,
            'last_seen', association.last_seen
          )
          ORDER BY association.match_count DESC, association.last_seen DESC, cheater.id
        ) AS cheaters,
        ROUND(
          CASE
            WHEN p.total_matches > 0 THEN p.total_wins::NUMERIC * 100 / p.total_matches
            WHEN (p.wins + p.losses) > 0 THEN p.wins::NUMERIC * 100 / (p.wins + p.losses)
            ELSE NULL
          END,
          2
        ) AS win_rate
      FROM player_boosted_associations association
      JOIN players p ON p.id = association.player_id
      JOIN players cheater ON cheater.id = association.cheater_id
      WHERE association.player_id = $1
      GROUP BY
        p.id, p.name, p.platform, p.region, p.kbm_tier, p.kbm_points,
        p.cheater, p.sus_count, p.weirdo_count, p.hall_of_fame_count,
        p.total_matches, p.total_wins, p.wins, p.losses,
        p.avg_dpm, p.avg_hpm, p.avg_egpm, p.avg_mpm
    `, [playerId]);

    if (!player) {
      return reply.status(404).send(err('NOT_FOUND', 'Boosted player not found', { playerId }));
    }

    const matches = await query(`
      WITH related_matches AS (
        SELECT
          pair.match_id,
          pair.entry_datetime,
          jsonb_agg(DISTINCT jsonb_build_object(
            'id', cheater.id,
            'name', cheater.name
          )) AS cheaters
        FROM player_boosted_associations association
        JOIN match_party_pairs pair
          ON pair.player_low_id = LEAST(association.player_id, association.cheater_id)
         AND pair.player_high_id = GREATEST(association.player_id, association.cheater_id)
        JOIN players cheater ON cheater.id = association.cheater_id
        WHERE association.player_id = $1
        GROUP BY pair.match_id, pair.entry_datetime
      )
      SELECT
        related.match_id,
        related.entry_datetime,
        m.map,
        m.queue_id,
        COALESCE(m.region, mp.region) AS region,
        COALESCE(m.duration_seconds, mp.time_in_match, 0) AS duration_seconds,
        m.team1_score,
        m.team2_score,
        m.winning_task_force,
        mp.champion_id,
        champion.name AS champion_name,
        mp.win_status,
        mp.kills,
        mp.deaths,
        mp.assists,
        mp.league_tier,
        mp.league_points,
        mp.source,
        related.cheaters
      FROM related_matches related
      JOIN matches m
        ON m.match_id = related.match_id
       AND m.entry_datetime = related.entry_datetime
      JOIN match_players mp
        ON mp.match_id = related.match_id
       AND mp.entry_datetime = related.entry_datetime
       AND mp.player_id = $1
      LEFT JOIN champions champion ON champion.id = mp.champion_id
      ORDER BY related.entry_datetime DESC, related.match_id DESC
    `, [playerId]);

    return { player, matches };
  });

  /**
   * GET /players/automatic-afk — One row per player with automatic full-AFK
   * evidence from complete ranked matches below the conservative 70 eCPM
   * threshold. Community moderation remains in players.afk_wintrade.
   */
  fastify.get('/automatic-afk', async (req: any, reply: any) => {
    const limit = parseLimit(req.query.limit ?? req.query.perPage, 20, 100);
    const offset = Math.max(0, Number.parseInt(String(req.query.offset ?? '0'), 10) || 0);
    const tierMinRaw = req.query.tierMin == null ? null : Number.parseInt(String(req.query.tierMin), 10);
    const tierMaxRaw = req.query.tierMax == null ? null : Number.parseInt(String(req.query.tierMax), 10);
    if ((tierMinRaw != null && (!Number.isInteger(tierMinRaw) || tierMinRaw < 1 || tierMinRaw > 26))
      || (tierMaxRaw != null && (!Number.isInteger(tierMaxRaw) || tierMaxRaw < 1 || tierMaxRaw > 26))
      || (tierMinRaw != null && tierMaxRaw != null && tierMinRaw > tierMaxRaw)) {
      return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    }

    const params: any[] = [];
    const where = [
      'm.queue_id = 486',
      'mp.egpm >= 0',
      'mp.egpm < 70',
      "COALESCE(mis.status, 'complete') = 'complete'",
      "COALESCE(mp.source, 'direct') IN ('direct', 'recovered')",
      'mp.is_ranked = true',
      'mp.player_id > 0',
      'mp.champion_id > 0',
      'mp.task_force IN (1, 2)',
      "LOWER(BTRIM(COALESCE(mp.win_status, ''))) IN ('winner', 'loser', 'win', 'loss')",
      'm.duration_seconds > 120',
    ];
    if (tierMinRaw != null || tierMaxRaw != null) {
      params.push(tierMinRaw ?? 1, tierMaxRaw ?? 26);
      where.push(`mlt.lobby_tier BETWEEN $${params.length - 1} AND $${params.length}`);
    }
    params.push(limit, offset);
    reply.header('Cache-Control', 'public, max-age=60');

    return query(`
      SELECT
        p.id,
        p.name,
        p.platform,
        p.region,
        p.kbm_tier,
        p.kbm_points,
        p.cheater,
        p.sus_count,
        p.weirdo_count,
        p.hall_of_fame_count,
        p.dropper,
        p.afk_wintrade,
        p.alt_account,
        p.total_matches,
        p.total_wins,
        p.avg_dpm,
        p.avg_hpm,
        p.avg_egpm,
        p.avg_mpm,
        EXISTS (
          SELECT 1 FROM player_boosted_associations association
          WHERE association.player_id = p.id
        ) AS boosted,
        COUNT(*)::INT AS automatic_match_count,
        MIN(mp.entry_datetime) AS first_seen,
        MAX(mp.entry_datetime) AS last_seen,
        ROUND(MIN(mp.egpm)::NUMERIC, 2)::DOUBLE PRECISION AS lowest_ecpm,
        ROUND(AVG(mp.egpm)::NUMERIC, 2)::DOUBLE PRECISION AS average_ecpm,
        COUNT(*) OVER()::INT AS total_count,
        ROUND(
          CASE
            WHEN p.total_matches > 0 THEN p.total_wins::NUMERIC * 100 / p.total_matches
            WHEN (p.wins + p.losses) > 0 THEN p.wins::NUMERIC * 100 / (p.wins + p.losses)
            ELSE NULL
          END,
          2
        ) AS win_rate
      FROM match_players mp
      JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
      LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
      LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      JOIN players p ON p.id = mp.player_id
      WHERE ${where.join(' AND ')}
      GROUP BY
        p.id, p.name, p.platform, p.region, p.kbm_tier, p.kbm_points,
        p.cheater, p.sus_count, p.weirdo_count, p.hall_of_fame_count,
        p.dropper, p.afk_wintrade, p.alt_account,
        p.total_matches, p.total_wins, p.wins, p.losses,
        p.avg_dpm, p.avg_hpm, p.avg_egpm, p.avg_mpm
      HAVING COUNT(*) >= 10
      ORDER BY automatic_match_count DESC, last_seen DESC, p.name ASC
      LIMIT $${params.length - 1}
      OFFSET $${params.length}
    `, params);
  });

  /** GET /players/automatic-afk/:id — All automatic full-AFK match evidence for one player. */
  fastify.get('/automatic-afk/:id', async (req: any, reply: any) => {
    const playerId = Number.parseInt(String(req.params.id), 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }
    const tierMinRaw = req.query.tierMin == null ? null : Number.parseInt(String(req.query.tierMin), 10);
    const tierMaxRaw = req.query.tierMax == null ? null : Number.parseInt(String(req.query.tierMax), 10);
    if ((tierMinRaw != null && (!Number.isInteger(tierMinRaw) || tierMinRaw < 1 || tierMinRaw > 26))
      || (tierMaxRaw != null && (!Number.isInteger(tierMaxRaw) || tierMaxRaw < 1 || tierMaxRaw > 26))
      || (tierMinRaw != null && tierMaxRaw != null && tierMinRaw > tierMaxRaw)) {
      return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    }

    const params: any[] = [playerId];
    const where = [
      'mp.player_id = $1',
      'm.queue_id = 486',
      'mp.egpm >= 0',
      'mp.egpm < 70',
      "COALESCE(mis.status, 'complete') = 'complete'",
      "COALESCE(mp.source, 'direct') IN ('direct', 'recovered')",
      'mp.is_ranked = true',
      'mp.champion_id > 0',
      'mp.task_force IN (1, 2)',
      "LOWER(BTRIM(COALESCE(mp.win_status, ''))) IN ('winner', 'loser', 'win', 'loss')",
      'm.duration_seconds > 120',
    ];
    if (tierMinRaw != null || tierMaxRaw != null) {
      params.push(tierMinRaw ?? 1, tierMaxRaw ?? 26);
      where.push(`mlt.lobby_tier BETWEEN $${params.length - 1} AND $${params.length}`);
    }
    const joins = `
      FROM match_players mp
      JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
      LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
      LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime`;
    reply.header('Cache-Control', 'public, max-age=60');

    const player = await one(`
      SELECT
        p.id,
        p.name,
        p.platform,
        p.region,
        p.afk_wintrade,
        COUNT(*)::INT AS automatic_match_count,
        MIN(mp.entry_datetime) AS first_seen,
        MAX(mp.entry_datetime) AS last_seen,
        ROUND(MIN(mp.egpm)::NUMERIC, 2)::DOUBLE PRECISION AS lowest_ecpm,
        ROUND(AVG(mp.egpm)::NUMERIC, 2)::DOUBLE PRECISION AS average_ecpm
      ${joins}
      JOIN players p ON p.id = mp.player_id
      WHERE ${where.join(' AND ')}
      GROUP BY p.id, p.name, p.platform, p.region, p.afk_wintrade
    `, params);
    if (!player) {
      return reply.status(404).send(err('NOT_FOUND', 'Automatically flagged player not found', { playerId }));
    }

    const matches = await query(`
      SELECT
        mp.match_id,
        mp.entry_datetime,
        m.map,
        m.queue_id,
        COALESCE(m.region, mp.region) AS region,
        m.duration_seconds,
        m.team1_score,
        m.team2_score,
        m.winning_task_force,
        mp.champion_id,
        champion.name AS champion_name,
        mp.win_status,
        mp.kills,
        mp.deaths,
        mp.assists,
        mp.league_tier,
        mp.league_points,
        mp.source,
        ROUND(mp.egpm::NUMERIC, 2)::DOUBLE PRECISION AS ecpm
      ${joins}
      LEFT JOIN champions champion ON champion.id = mp.champion_id
      WHERE ${where.join(' AND ')}
      ORDER BY mp.entry_datetime DESC, mp.match_id DESC
    `, params);

    return { player, matches };
  });

  /**
   * GET /players/:id — Database-first player profile with optional sections.
   *
   * Query params:
   *   ?include=    — Comma-separated: ratings,champions,loadouts (default: all)
   *   ?fields=     — Comma-separated column names to include in player object
   *
   * All reads are database-only. The explicit POST refresh route is the only
   * profile/history freshness boundary.
   *
   * Returns: { player, profileRefresh, queueRatings?, championRatings? }
   *   - player: full players row, including columnized Hi-Rez fields such as
   *     avatar_url, title, ranked_* values, plus derived avg_* stats.
   *   - queueRatings: [{ queue_id, mu, phi, volatility }]
   *   - championRatings: [{ champion_id, mu, phi, volatility, matches_played, wins, losses }]
   */
  fastify.get('/:id', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }

    const readPlayer = () => one(`
        SELECT p.*,
          EXISTS (
            SELECT 1 FROM player_boosted_associations association
            WHERE association.player_id = p.id
          ) AS boosted,
          EXISTS (
            SELECT 1 FROM users verified_user
            WHERE verified_user.linked_player_id = p.id
          ) AS verified,
          COALESCE(lc.rank, p.kbm_rank) AS kbm_rank,
          COALESCE(lc.wins, p.kbm_wins) AS kbm_wins,
          COALESCE(lc.losses, p.kbm_losses) AS kbm_losses,
          COALESCE(lc.leaves, p.kbm_leaves) AS kbm_leaves
        FROM players p
        LEFT JOIN leaderboard_current lc ON lc.player_id = p.id
        WHERE p.id = $1
      `, [id]);
    let player = await readPlayer();
    const initialFreshness = getPlayerProfileFreshness(player);
    let profileRefresh: any = {
      ...initialFreshness,
      was_expired: initialFreshness.expired,
      attempted: false,
      refreshed: false,
      source: player && !initialFreshness.expired ? 'database' : 'stale-database',
    };

    if (!player) return reply.status(404).send(err('NOT_FOUND', 'Player not found', { playerId: id }));

    // Build the response entirely from local read models.
    const include = (req.query.include || 'ratings,champions').split(',').map((s: string) => s.trim());

    // Cached all-time per-champion totals are updated only by explicit refresh.
    const globalStats = await readPlayerGlobalStats(id);
    // Never expose the rolling-deployment compatibility mirror as a second
    // public moderation field. All web clients consume `cheater` only.
    const publicPlayer = { ...player };
    delete publicPlayer.cheater_status;
    const result: any = { player: publicPlayer, profileRefresh, globalStats };

    if (include.includes('ratings')) {
      result.queueRatings = await query(`
        WITH queue_rating_counts AS (
          SELECT
            mrs.player_id,
            m.queue_id,
            COUNT(DISTINCT mrs.match_id)::INT AS matches_played,
            COUNT(DISTINCT mrs.match_id) FILTER (
              WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')
            )::INT AS wins,
            COUNT(DISTINCT mrs.match_id) FILTER (
              WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')
            )::INT AS losses
          FROM match_rating_snapshots mrs
          JOIN matches m ON m.match_id = mrs.match_id
          LEFT JOIN match_players mp
            ON mp.match_id = m.match_id
           AND mp.entry_datetime = m.entry_datetime
           AND mp.player_id = mrs.player_id
          WHERE mrs.player_id = $1
          GROUP BY mrs.player_id, m.queue_id
        )
        SELECT
          pqr.queue_id,
          pqr.mu::DOUBLE PRECISION AS mu,
          pqr.phi::DOUBLE PRECISION AS phi,
          pqr.volatility::FLOAT4 AS volatility,
          COALESCE(qrc.matches_played, 0)::INT AS matches_played,
          COALESCE(qrc.wins, 0)::INT AS wins,
          COALESCE(qrc.losses, 0)::INT AS losses
        FROM player_queue_ratings pqr
        LEFT JOIN queue_rating_counts qrc
          ON qrc.player_id = pqr.player_id
         AND qrc.queue_id = pqr.queue_id
        WHERE pqr.player_id = $1
          AND pqr.mu BETWEEN 0 AND 3500 AND pqr.phi BETWEEN 1 AND 350 AND pqr.volatility BETWEEN 0.001 AND 0.2
      `, [id]);
      if (include.includes('champions')) {
        result.championRatings = await query(`
          SELECT pcr.champion_id, c.name AS champion_name,
            pcr.mu::DOUBLE PRECISION AS mu, pcr.phi::DOUBLE PRECISION AS phi,
            pcr.volatility::FLOAT4 AS volatility,
            pcr.matches_played, pcr.wins, pcr.losses
          FROM player_champion_ratings pcr
          JOIN champions c ON c.id = pcr.champion_id
          WHERE pcr.player_id = $1
            AND pcr.mu BETWEEN 0 AND 3500 AND pcr.phi BETWEEN 1 AND 350 AND pcr.volatility BETWEEN 0.001 AND 0.2
          ORDER BY pcr.mu DESC`, [id]);
      }
    }

    return result;
  });

  /**
   * POST /players/:id/refresh — Refresh profile and match-history data.
   * The explicit action bypasses the long read cache and may be used up to
   * five times per ten-minute window so a recently finished match can appear.
   */
  fastify.post('/:id/refresh', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }

    const refreshQuota = await guardPlayerRefreshAttempt(req, reply, id);
    try {
      const refresh = await refreshPlayerProfileIfExpired(
        id,
        PLAYER_PROFILE_TTL_MS,
        'manual_profile_refresh',
        'player-profile-manual-refresh',
        () => guardVendorFallback(req, reply, {
          scope: 'player-profile-refresh',
          entity: id,
          entityLimit: PLAYER_REFRESH_ATTEMPT_LIMIT,
          entityWindowMs: PLAYER_REFRESH_WINDOW_MS,
        }),
        true,
      );

      if (refresh.refreshed) {
        const refreshed = await one('SELECT p.* FROM players p WHERE p.id = $1', [id]);
        if (refreshed) void syncPlayer(id, playerSearchDocument(refreshed));
      }
      let historyRefresh: { refreshed: boolean; count?: number; error?: string };
      try {
        await guardVendorFallback(req, reply, {
          scope: 'player-history-refresh',
          entity: id,
          entityLimit: PLAYER_REFRESH_ATTEMPT_LIMIT,
          entityWindowMs: PLAYER_REFRESH_WINDOW_MS,
        });
        const history = await getMatchHistory(id, 50, true, 'manual_profile_refresh');
        historyRefresh = { refreshed: true, count: history.length };
      } catch (historyError: any) {
        const message = historyError?.message || 'Match history refresh failed';
        console.warn(`[PLAYER-PROFILE] Profile refreshed but match history failed for ${id}: ${message}`);
        historyRefresh = { refreshed: false, error: message };
      }
      let championStatsRefresh: { refreshed: boolean; error?: string };
      try {
        championStatsRefresh = {
          refreshed: await refreshPlayerChampionStatsIfExpired(
            id,
            PLAYER_CHAMPION_STATS_TTL_MS,
            'manual_champion_stats_refresh',
            'player-champion-stats-manual-refresh',
            () => guardVendorFallback(req, reply, {
              scope: 'player-champions-refresh',
              entity: id,
              entityLimit: PLAYER_REFRESH_ATTEMPT_LIMIT,
              entityWindowMs: PLAYER_REFRESH_WINDOW_MS,
            }),
          ),
        };
      } catch (championStatsError: any) {
        const message = championStatsError?.message || 'Champion totals refresh failed';
        console.warn(`[PLAYER-PROFILE] Profile refreshed but champion totals failed for ${id}: ${message}`);
        championStatsRefresh = { refreshed: false, error: message };
      }
      const refreshedParts = [
        refresh.refreshed ? 'Profile' : null,
        historyRefresh.refreshed ? 'match history' : null,
        championStatsRefresh.refreshed ? 'champion totals' : null,
      ].filter((part): part is string => Boolean(part));
      return {
        success: true,
        message: refreshedParts.length > 0
          ? `${refreshedParts.join(', ')} refreshed`
          : 'Player data is already current',
        profileRefresh: {
          ...refresh.freshness,
          attempted: true,
          refreshed: refresh.refreshed,
          source: refresh.refreshed ? 'hirez' : 'database',
        },
        historyRefresh,
        championStatsRefresh,
        refreshQuota: {
          limit: refreshQuota.limit,
          remaining: refreshQuota.remaining,
          reset_at: new Date(refreshQuota.resetAt).toISOString(),
          remaining_seconds: refreshQuota.remainingSeconds,
        },
        audit: refresh.audit,
      };
    } catch (e: any) {
      if (e instanceof RequestSecurityError) throw e;
      return reply.status(502).send(err('REFRESH_FAILED', e?.message || 'Refresh failed'));
    }
  });

  /**
   * GET /players/alt-account-relations — Aggregate directional community votes
   * into one card per main account with all voted alternate accounts nested.
   */
  fastify.get('/alt-account-relations', async (req: any, reply: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });
    const search = String(req.query.q ?? '').trim();
    const params: any[] = [];
    let searchClause = '';
    if (search) {
      params.push(`%${escapeLike(search)}%`);
      const pattern = `$${params.length}`;
      searchClause = `WHERE main_player.name ILIKE ${pattern} ESCAPE '\\'
        OR alt_player.name ILIKE ${pattern} ESCAPE '\\'
        OR main_player.id::TEXT = $${params.length + 1}
        OR alt_player.id::TEXT = $${params.length + 1}`;
      params.push(search);
    }
    params.push(perPage, offset);
    const limitParam = `$${params.length - 1}`;
    const offsetParam = `$${params.length}`;

    reply.header('Cache-Control', 'public, max-age=60');
    return query(`
      WITH pair_votes AS (
        SELECT
          main_player_id,
          alt_player_id,
          COUNT(*)::INT AS vote_count,
          MAX(updated_at) AS last_voted_at
        FROM player_alt_account_votes
        GROUP BY main_player_id, alt_player_id
      ), matching_mains AS (
        SELECT DISTINCT relation.main_player_id
        FROM pair_votes relation
        JOIN players main_player ON main_player.id = relation.main_player_id
        JOIN players alt_player ON alt_player.id = relation.alt_player_id
        ${searchClause}
      ), main_totals AS (
        SELECT
          relation.main_player_id,
          SUM(relation.vote_count)::INT AS total_votes,
          COUNT(*)::INT AS alt_count,
          MAX(relation.last_voted_at) AS last_voted_at
        FROM pair_votes relation
        JOIN matching_mains matched ON matched.main_player_id = relation.main_player_id
        GROUP BY relation.main_player_id
      ), paged_mains AS (
        SELECT
          totals.*,
          COUNT(*) OVER()::INT AS total_count
        FROM main_totals totals
        JOIN players main_player ON main_player.id = totals.main_player_id
        ORDER BY totals.total_votes DESC, totals.last_voted_at DESC, main_player.name ASC
        LIMIT ${limitParam} OFFSET ${offsetParam}
      )
      SELECT
        paged.total_count,
        paged.total_votes,
        paged.alt_count,
        paged.last_voted_at,
        main_player.id AS main_player_id,
        main_player.name AS main_player_name,
        main_player.region AS main_player_region,
        main_player.platform AS main_player_platform,
        main_player.cheater AS main_player_cheater,
        main_player.sus_count AS main_player_sus_count,
        main_player.dropper AS main_player_dropper,
        main_player.afk_wintrade AS main_player_afk_wintrade,
        main_player.alt_account AS main_player_alt_account,
        jsonb_agg(
          jsonb_build_object(
            'id', alt_player.id,
            'name', alt_player.name,
            'region', alt_player.region,
            'platform', alt_player.platform,
            'cheater', alt_player.cheater,
            'sus_count', alt_player.sus_count,
            'dropper', alt_player.dropper,
            'afk_wintrade', alt_player.afk_wintrade,
            'alt_account', alt_player.alt_account,
            'vote_count', relation.vote_count,
            'last_voted_at', relation.last_voted_at
          )
          ORDER BY relation.vote_count DESC, relation.last_voted_at DESC, alt_player.name ASC
        ) AS alt_accounts
      FROM paged_mains paged
      JOIN players main_player ON main_player.id = paged.main_player_id
      JOIN pair_votes relation ON relation.main_player_id = paged.main_player_id
      JOIN players alt_player ON alt_player.id = relation.alt_player_id
      GROUP BY
        paged.total_count, paged.total_votes, paged.alt_count, paged.last_voted_at,
        main_player.id, main_player.name, main_player.region, main_player.platform,
        main_player.cheater, main_player.sus_count, main_player.dropper,
        main_player.afk_wintrade, main_player.alt_account
      ORDER BY paged.total_votes DESC, paged.last_voted_at DESC, main_player.name ASC
    `, params);
  });

  /** GET the authenticated user's own relationship votes involving a player. */
  fastify.get('/:id/alt-account-relations/mine', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    const session = await requirePlayerVoteSession(req, reply);
    if (!session) return;
    reply.header('Cache-Control', 'private, no-store');

    return query(`
      SELECT
        relation.id,
        relation.main_player_id,
        main_player.name AS main_player_name,
        relation.alt_player_id,
        alt_player.name AS alt_player_name,
        relation.created_at,
        relation.updated_at
      FROM player_alt_account_votes relation
      JOIN players main_player ON main_player.id = relation.main_player_id
      JOIN players alt_player ON alt_player.id = relation.alt_player_id
      WHERE relation.user_id = $1
        AND (relation.main_player_id = $2 OR relation.alt_player_id = $2)
      ORDER BY relation.updated_at DESC, relation.id DESC
    `, [session.user_id, id]);
  });

  /** Create or correct the authenticated user's directional vote for a pair. */
  fastify.post('/:id/alt-account-relations', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    const otherPlayerId = parseInt(String((req.body as any)?.otherPlayerId ?? ''), 10);
    const otherRole = String((req.body as any)?.otherRole ?? '');
    if (!Number.isInteger(id) || id <= 0 || !Number.isInteger(otherPlayerId) || otherPlayerId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Both player IDs must be valid'));
    }
    if (id === otherPlayerId) return reply.status(400).send(err('VALIDATION', 'An account cannot be linked to itself'));
    if (otherRole !== 'main' && otherRole !== 'alt') {
      return reply.status(400).send(err('VALIDATION', 'otherRole must be "main" or "alt"'));
    }
    const session = await requirePlayerVoteSession(req, reply);
    if (!session) return;

    const players = await query<{ id: string; name: string }>(
      'SELECT id, name FROM players WHERE id = ANY($1::bigint[])',
      [[id, otherPlayerId]],
    );
    if (players.length !== 2) return reply.status(404).send(err('NOT_FOUND', 'One of the selected players does not exist'));

    const mainPlayerId = otherRole === 'main' ? otherPlayerId : id;
    const altPlayerId = otherRole === 'alt' ? otherPlayerId : id;
    const client = await pool.connect();
    let replaced = false;
    try {
      await client.query('BEGIN');
      const removed = await client.query(
        `DELETE FROM player_alt_account_votes
         WHERE user_id = $1
           AND LEAST(main_player_id, alt_player_id) = LEAST($2::bigint, $3::bigint)
           AND GREATEST(main_player_id, alt_player_id) = GREATEST($2::bigint, $3::bigint)
         RETURNING id`,
        [session.user_id, id, otherPlayerId],
      );
      replaced = (removed.rowCount ?? 0) > 0;
      await client.query(
        `INSERT INTO player_alt_account_votes (user_id, main_player_id, alt_player_id)
         VALUES ($1, $2, $3)`,
        [session.user_id, mainPlayerId, altPlayerId],
      );
      await client.query(
        `UPDATE players player
         SET alt_account = EXISTS (
           SELECT 1 FROM player_alt_account_votes relation WHERE relation.alt_player_id = player.id
         )
         WHERE player.id = ANY($1::bigint[])`,
        [[id, otherPlayerId]],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    await Promise.all([publishPlayerModerationChange(id), publishPlayerModerationChange(otherPlayerId)]);
    const names = new Map(players.map((player) => [Number(player.id), player.name]));
    return {
      success: true,
      replaced,
      relation: {
        main_player_id: mainPlayerId,
        main_player_name: names.get(mainPlayerId) ?? String(mainPlayerId),
        alt_player_id: altPlayerId,
        alt_player_name: names.get(altPlayerId) ?? String(altPlayerId),
      },
    };
  });

  /** Delete only the authenticated user's vote for this unordered player pair. */
  fastify.delete('/:id/alt-account-relations/:otherId', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    const otherPlayerId = parseInt((req.params as any).otherId, 10);
    if (!Number.isInteger(id) || id <= 0 || !Number.isInteger(otherPlayerId) || otherPlayerId <= 0 || id === otherPlayerId) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player relationship'));
    }
    const session = await requirePlayerVoteSession(req, reply);
    if (!session) return;

    const client = await pool.connect();
    let removed = false;
    try {
      await client.query('BEGIN');
      const deleted = await client.query(
        `DELETE FROM player_alt_account_votes
         WHERE user_id = $1
           AND LEAST(main_player_id, alt_player_id) = LEAST($2::bigint, $3::bigint)
           AND GREATEST(main_player_id, alt_player_id) = GREATEST($2::bigint, $3::bigint)
         RETURNING id`,
        [session.user_id, id, otherPlayerId],
      );
      removed = (deleted.rowCount ?? 0) > 0;
      await client.query(
        `UPDATE players player
         SET alt_account = EXISTS (
           SELECT 1 FROM player_alt_account_votes relation WHERE relation.alt_player_id = player.id
         )
         WHERE player.id = ANY($1::bigint[])`,
        [[id, otherPlayerId]],
      );
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    if (removed) await Promise.all([publishPlayerModerationChange(id), publishPlayerModerationChange(otherPlayerId)]);
    return { success: true, removed };
  });

  /**
   * POST /players/:id/report — Report or label a player.
   *
   * Body: { type: 'suspicious' | 'cheater' | 'approve' | 'weirdo' | 'hall_of_fame' | 'dropper' | 'afk_wintrade', reason?: string }
   *   - Dropper and AFK / Wintrade are reason-free community votes
   *   - Alt Account uses the directional /alt-account-relations endpoint
   *   - 'suspicious': increments sus_count once per user/player
   *   - 'cheater': confirms the flag for an approved reporter and stores why
   *   - 'approve': legacy alias for confirming the cheater flag
   *   - community vote types allow one vote per user and player
   * Auth: Bearer token required (logged-in user)
   */
  fastify.post('/:id/report', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }

    const session = await requirePlayerVoteSession(req, reply);
    if (!session) return;

    const body = req.body as { type?: string; reason?: string };
    const reportType = body.type;
    if (!reportType || !['suspicious', 'cheater', 'approve', 'weirdo', 'hall_of_fame', 'dropper', 'afk_wintrade'].includes(reportType)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid report type.'));
    }

    const reason = body.reason?.trim();
    const reasonFreeCommunityVote = ['dropper', 'afk_wintrade'].includes(reportType);
    if (reportType !== 'approve' && !reasonFreeCommunityVote && !reason) {
      return reply.status(400).send(err('VALIDATION', 'A reason is required for every player report or vote'));
    }

    // Only confirmed-cheater decisions require reviewer privileges. Community
    // votes are available to every authenticated account and remain idempotent.
    if (['cheater', 'approve'].includes(reportType)) {
      if (!session.is_admin && !session.is_approved) {
        return reply.status(403).send(err('PERMISSION', 'Action requires admin or approved status'));
      }
    }

    const existing = await one('SELECT id, name, cheater, sus_count, weirdo_count, hall_of_fame_count, dropper, afk_wintrade, alt_account FROM players WHERE id = $1', [id]);
    if (!existing) return reply.status(404).send(err('NOT_FOUND', 'Player not found', { playerId: id }));

    if (reportType === 'approve') {
      // Compatibility for older moderator clients. There is no second pending
      // player state: approving means setting the one canonical boolean.
      await query('UPDATE players SET cheater = TRUE WHERE id = $1', [id]);
      await publishPlayerModerationChange(id);
      return { success: true, message: 'Player confirmed as cheater', cheater: true, reason: body.reason ?? null };
    }

    if (reportType === 'suspicious' || reportType === 'weirdo' || reportType === 'hall_of_fame') {
      // Reason-backed insertion and counter increment are one statement. The
      // unique constraint makes refreshes/double-clicks idempotent without
      // allowing a user to inflate a player's placement on any directory card.
      const column = reportType === 'suspicious'
        ? 'sus_count'
        : reportType === 'weirdo'
          ? 'weirdo_count'
          : 'hall_of_fame_count';
      const voteRows = await query<{ created: boolean; count: number | null }>(
        `WITH inserted_vote AS (
           INSERT INTO player_community_votes (player_id, user_id, vote_type, reason)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (player_id, user_id, vote_type) DO NOTHING
           RETURNING id
         ), updated_player AS (
           UPDATE players
           SET ${column} = ${column} + 1
           WHERE id = $1 AND EXISTS (SELECT 1 FROM inserted_vote)
           RETURNING ${column} AS count
         )
         SELECT EXISTS (SELECT 1 FROM inserted_vote) AS created,
                (SELECT count FROM updated_player) AS count`,
        [id, session.user_id, reportType, reason],
      );
      const vote = voteRows[0];
      const created = Boolean(vote?.created);
      return {
        success: true,
        message: created
          ? reportType === 'suspicious'
            ? 'Player reported as Suspicious'
            : `Player added to ${reportType === 'weirdo' ? 'Weirdo' : 'Hall of Fame'}`
          : `You have already submitted a ${reportType === 'suspicious' ? 'Suspicious report' : reportType === 'weirdo' ? 'Weirdo vote' : 'Hall of Fame vote'} for this player`,
        already_voted: !created,
        count: vote?.count == null ? Number(existing[column]) : Number(vote.count),
      };
    }

    if (reportType === 'dropper' || reportType === 'afk_wintrade') {
      const column = reportType;
      const label = reportType === 'dropper' ? 'Dropper' : reportType === 'afk_wintrade' ? 'AFK / Wintrade' : 'Alt account';
      const moderationVote = await query<{ created: boolean }>(
        `WITH inserted_vote AS (
           INSERT INTO player_community_votes (player_id, user_id, vote_type, reason)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (player_id, user_id, vote_type) DO NOTHING
           RETURNING id
         )
         UPDATE players
         SET ${column} = TRUE
         WHERE id = $1
         RETURNING EXISTS (SELECT 1 FROM inserted_vote) AS created`,
        [id, session.user_id, reportType, reason ?? ''],
      );
      await publishPlayerModerationChange(id);
      return {
        success: true,
        message: Boolean(moderationVote[0]?.created) ? `${label} vote recorded` : `You have already voted ${label} for this player`,
        [column]: true,
        already_voted: !Boolean(moderationVote[0]?.created),
      };
    }

    // Cheater reports are restricted to approved reviewers, but use the same
    // durable reason store as community reports so moderation is auditable.
    const cheaterVote = await query<{ created: boolean }>(
      `WITH inserted_vote AS (
         INSERT INTO player_community_votes (player_id, user_id, vote_type, reason)
         VALUES ($1, $2, 'cheater', $3)
         ON CONFLICT (player_id, user_id, vote_type) DO NOTHING
         RETURNING id
       )
       UPDATE players
       SET cheater = TRUE
       WHERE id = $1
       RETURNING EXISTS (SELECT 1 FROM inserted_vote) AS created`,
      [id, session.user_id, reason],
    );
    await publishPlayerModerationChange(id);
    return {
      success: true,
      message: Boolean(cheaterVote[0]?.created) ? 'Player confirmed as cheater' : 'Cheater report already recorded for this player',
      cheater: true,
      already_voted: !Boolean(cheaterVote[0]?.created),
    };
  });

  /**
   * POST /players/:id/clear-tag — Clear a moderation/community-derived tag.
   * This is intentionally admin-only; approved reviewers can add evidence but
   * cannot erase an existing moderation decision.
   */
  fastify.post('/:id/clear-tag', async (req: any, reply: any) => {
    try {
      await requireAdminSession(req);
    } catch {
      return reply.status(401).send(err('UNAUTHORIZED', 'Admin access required'));
    }

    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }

    const tag = String((req.body as any)?.tag ?? '');
    const storedTags = ['cheater', 'suspicious', 'dropper', 'afk_wintrade', 'alt_account'] as const;
    if (!storedTags.includes(tag as typeof storedTags[number])) {
      return reply.status(400).send(err('VALIDATION', 'Invalid moderation tag'));
    }

    const client = await pool.connect();
    let player: { cheater: boolean; sus_count: number; dropper: boolean; afk_wintrade: boolean; alt_account: boolean } | undefined;
    let removedReports = 0;
    try {
      await client.query('BEGIN');
      const result = await client.query<{ cheater: boolean; sus_count: number; dropper: boolean; afk_wintrade: boolean; alt_account: boolean }>(
        'SELECT cheater, sus_count, dropper, afk_wintrade, alt_account FROM players WHERE id = $1 FOR UPDATE',
        [id],
      );
      player = result.rows[0];
      if (!player) {
        await client.query('ROLLBACK');
        return reply.status(404).send(err('NOT_FOUND', 'Player not found', { playerId: id }));
      }

      if (tag === 'suspicious') {
        await client.query('UPDATE players SET sus_count = 0 WHERE id = $1', [id]);
      } else {
        await client.query(`UPDATE players SET ${tag} = FALSE WHERE id = $1`, [id]);
      }
      const deleted = await client.query(
        'DELETE FROM player_community_votes WHERE player_id = $1 AND vote_type = $2 RETURNING id',
        [id, tag],
      );
      removedReports = deleted.rowCount ?? 0;
      if (tag === 'alt_account') {
        const deletedRelations = await client.query(
          'DELETE FROM player_alt_account_votes WHERE alt_player_id = $1 RETURNING id',
          [id],
        );
        removedReports += deletedRelations.rowCount ?? 0;
      }
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }

    await publishPlayerModerationChange(id);
    const wasTagged = tag === 'suspicious'
      ? player!.sus_count > 0
      : Boolean(player![tag as 'cheater' | 'dropper' | 'afk_wintrade' | 'alt_account']);
    const tagLabel = tag === 'suspicious' ? 'Suspicious' : tag === 'afk_wintrade' ? 'AFK / Wintrade' : tag === 'alt_account' ? 'Alt account' : tag[0].toUpperCase() + tag.slice(1);
    return {
      success: true,
      cleared: wasTagged,
      removed_reports: removedReports,
      message: wasTagged
        ? `${tagLabel} tag cleared`
        : `${tagLabel} tag was already clear`,
    };
  });

  /**
   * GET /players/:id/matches — Player match history with optional filters.
   *
   * Query params:
   *   ?limit=        — Max results per page (default: 20, max: 100)
   *   ?offset=       — Page offset (default: 0)
   *   ?queueId=      — Filter by queue (e.g. 486 for ranked)
   *   ?championId=   — Filter by champion played
   *   ?winStatus=    — "Winner"/"Loser" or compact "Win"/"Loss"
   *   ?from=         — ISO 8601 start date (inclusive)
   *   ?to=           — ISO 8601 end date (inclusive)
   *
   * This endpoint is database-only. Users refresh profile/history explicitly
   * through POST /players/:id/refresh.
   *
   * Returns: Array of { match_id, entry_datetime, map, queue_id, duration_seconds, region,
   *   champion_id, champion_name, win_status, kills, deaths, assists, league_tier, afk_score }
   */
  fastify.get('/:id/matches', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }

    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const offset = parseInt(req.query.offset as string) || 0;

    const params: any[] = [id];
    const authoritativeFilters = ['mp.player_id = $1'];
    const historyFilters = [
      'h.player_id = $1',
      `(h.expires_at IS NULL OR h.expires_at > now())`,
      `NOT EXISTS (
         SELECT 1
         FROM match_players existing
         WHERE existing.match_id = h.match_id
           AND existing.player_id = h.player_id
         UNION ALL
         SELECT 1 FROM casual_match_players existing
         WHERE existing.match_id = h.match_id AND existing.player_id = h.player_id
         UNION ALL
         SELECT 1 FROM special_match_players existing
         WHERE existing.match_id = h.match_id AND existing.player_id = h.player_id
       )`,
    ];
    const addFilter = (authSql: string, historySql: string, value: any) => {
      params.push(value);
      const placeholder = `$${params.length}`;
      authoritativeFilters.push(authSql.replace('?', placeholder));
      historyFilters.push(historySql.replace('?', placeholder));
    };

    if (req.query.queueId) {
      addFilter('m.queue_id = ?', 'COALESCE(m.queue_id, h.queue_id) = ?', parseInt(req.query.queueId, 10));
    }
    if (req.query.championId) {
      addFilter('mp.champion_id = ?', 'h.champion_id = ?', parseInt(req.query.championId, 10));
    }
    if (req.query.winStatus) {
      addFilter('mp.win_status = ?', 'h.win_status = ?', req.query.winStatus);
    }
    if (req.query.from) {
      addFilter('m.entry_datetime >= ?', 'COALESCE(m.entry_datetime, h.entry_datetime) >= ?', new Date(req.query.from));
    }
    if (req.query.to) {
      addFilter('m.entry_datetime <= ?', 'COALESCE(m.entry_datetime, h.entry_datetime) <= ?', new Date(req.query.to));
    }

    params.push(limit, offset);
    const limitPlaceholder = `$${params.length - 1}`;
    const offsetPlaceholder = `$${params.length}`;
    const casualFilters = authoritativeFilters.map(filter => filter.replaceAll('mp.', 'cmp.').replaceAll('m.', 'cm.'));
    const specialFilters = authoritativeFilters.map(filter => filter.replaceAll('mp.', 'smp.').replaceAll('m.', 'sm.'));

    const matches = await query(
      `WITH authoritative AS (
         SELECT
           m.match_id, m.entry_datetime, m.map, m.queue_id, m.duration_seconds, m.region,
           mp.champion_id, c.name AS champion_name, mp.win_status, mp.kills, mp.deaths,
           mp.assists,
           mp.damage_done_physical AS damage_done,
           mp.damage_per_minute,
           mp.league_tier,
           mp.afk_rate AS afk_score,
           mp.source,
           true AS authoritative
         FROM match_players mp
         JOIN matches m ON m.match_id = mp.match_id
         LEFT JOIN champions c ON c.id = mp.champion_id
         WHERE ${authoritativeFilters.join(' AND ')}
         UNION ALL
         SELECT
           cm.match_id,cm.entry_datetime,cm.map,cm.queue_id,cm.duration_seconds,cm.region,
           cmp.champion_id,COALESCE(c.name,cmp.champion_name) AS champion_name,
           cmp.win_status,cmp.kills,cmp.deaths,cmp.assists,cmp.damage AS damage_done,
           CASE WHEN cm.duration_seconds > 0 THEN ROUND(cmp.damage::numeric*60/cm.duration_seconds,2)::double precision ELSE NULL END AS damage_per_minute,
           NULL::int AS league_tier,NULL::double precision AS afk_score,cmp.source,true AS authoritative
         FROM casual_match_players cmp
         JOIN casual_matches cm ON cm.match_id=cmp.match_id
         LEFT JOIN champions c ON c.id=cmp.champion_id
         WHERE ${casualFilters.join(' AND ')}
         UNION ALL
         SELECT
           sm.match_id,sm.entry_datetime,sm.map,sm.queue_id,sm.duration_seconds,sm.region,
           smp.champion_id,COALESCE(c.name,smp.champion_name) AS champion_name,
           smp.win_status,smp.kills,smp.deaths,smp.assists,smp.damage AS damage_done,
           CASE WHEN sm.duration_seconds > 0 THEN ROUND(smp.damage::numeric*60/sm.duration_seconds,2)::double precision ELSE NULL END AS damage_per_minute,
           NULL::int AS league_tier,NULL::double precision AS afk_score,smp.source,true AS authoritative
         FROM special_match_players smp
         JOIN special_matches sm ON sm.match_id=smp.match_id
         LEFT JOIN champions c ON c.id=smp.champion_id
         WHERE ${specialFilters.join(' AND ')}
       ),
       history_observations AS (
         SELECT
           h.match_id,
           COALESCE(m.entry_datetime, h.entry_datetime) AS entry_datetime,
           COALESCE(m.map, h.map) AS map,
           COALESCE(m.queue_id, h.queue_id) AS queue_id,
           COALESCE(m.duration_seconds, h.time_in_match) AS duration_seconds,
           COALESCE(m.region, h.region) AS region,
           h.champion_id,
           COALESCE(c.name, h.champion_name) AS champion_name,
           h.win_status,
           h.kills,
           h.deaths,
           h.assists,
           h.damage AS damage_done,
           CASE
             WHEN COALESCE(h.time_in_match, 0) > 0
               THEN ROUND((COALESCE(h.damage, 0)::NUMERIC / h.time_in_match) * 60, 2)::DOUBLE PRECISION
             ELSE NULL::DOUBLE PRECISION
           END AS damage_per_minute,
           h.league_tier,
           NULL::double precision AS afk_score,
           h.source,
           false AS authoritative
         FROM player_match_history_entries h
         LEFT JOIN matches m ON m.match_id = h.match_id
         LEFT JOIN champions c ON c.id = h.champion_id
         WHERE ${historyFilters.join(' AND ')}
       )
       SELECT *
       FROM (
         SELECT DISTINCT ON (match_id) *
         FROM (
           SELECT * FROM authoritative
           UNION ALL
           SELECT * FROM history_observations
         ) combined
         ORDER BY match_id, authoritative DESC, entry_datetime DESC NULLS LAST
       ) deduplicated
       ORDER BY entry_datetime DESC NULLS LAST
       LIMIT ${limitPlaceholder} OFFSET ${offsetPlaceholder}`,
      params
    );
    return matches;
  });

  fastify.get('/:id/champions', async (req: any, reply: any) => {
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }
    if (!await one('SELECT 1 FROM players WHERE id = $1', [id])) {
      return reply.status(404).send(err('NOT_FOUND', 'Player not found'));
    }

    const role = championRoleSql('c');
    return query(`
      SELECT
        c.id AS champion_id,
        c.name AS champion_name,
        ${role} AS role,
        COALESCE(pc.xp, 0)::BIGINT AS xp,
        COALESCE(pc.ownership_type, '') AS ownership_type,
        COALESCE(pc.wins, 0)::INTEGER AS wins,
        COALESCE(pc.losses, 0)::INTEGER AS losses,
        COALESCE(pc.kills, 0)::INTEGER AS kills,
        COALESCE(pc.deaths, 0)::INTEGER AS deaths,
        COALESCE(pc.assists, 0)::INTEGER AS assists,
        COALESCE(pc.minutes_played, 0)::INTEGER AS minutes_played,
        (COALESCE(pc.wins, 0) + COALESCE(pc.losses, 0))::INTEGER AS matches_played,
        CASE WHEN COALESCE(pc.wins, 0) + COALESCE(pc.losses, 0) > 0
          THEN ROUND(COALESCE(pc.wins, 0)::NUMERIC * 100 / (COALESCE(pc.wins, 0) + COALESCE(pc.losses, 0)), 2)
          ELSE NULL
        END AS win_rate,
        pc.last_updated
      FROM champions c
      LEFT JOIN player_champions pc ON pc.player_id = $1 AND pc.champion_id = c.id
      WHERE c.id > 0
      ORDER BY
        CASE ${role}
          WHEN 'Frontline' THEN 1
          WHEN 'Damage' THEN 2
          WHEN 'Flank' THEN 3
          WHEN 'Support' THEN 4
          ELSE 5
        END,
        c.name ASC
    `, [id]);
  });

  /** Manually refresh cached Champion Stats, subject to the same 10-minute TTL as normal profile reads. */
  fastify.post('/:id/champions/refresh', async (req: any, reply: any) => {
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }
    if (!await one('SELECT 1 FROM players WHERE id = $1', [id])) {
      return reply.status(404).send(err('NOT_FOUND', 'Player not found'));
    }

    const freshness = await readPlayerChampionStatsFreshness(id);
    if (!freshness.expired) {
      return reply.status(429).send(err(
        'CHAMPION_STATS_REFRESH_COOLDOWN',
        'Champion stats were refreshed recently. Try again after the cooldown.',
        { ...freshness },
      ));
    }

    try {
      const refreshed = await refreshPlayerChampionStatsIfExpired(
        id,
        PLAYER_CHAMPION_STATS_TTL_MS,
        'manual_champion_stats_refresh',
        'player-champion-stats-manual-refresh',
        () => guardVendorFallback(req, reply, {
          scope: 'player-champions-refresh',
          entity: id,
        }),
      );
      return { refreshed, freshness: await readPlayerChampionStatsFreshness(id) };
    } catch (error) {
      if (error instanceof RequestSecurityError) throw error;
      return reply.status(502).send(err(
        'CHAMPION_STATS_REFRESH_FAILED',
        error instanceof Error ? error.message : 'Could not refresh champion stats.',
        { ...await readPlayerChampionStatsFreshness(id) },
      ));
    }
  });

  /**
   * GET /players/:id/charts — Player chart data (recent match history for graphs).
   *
   * Query params:
   *   ?limit=    — Max results (default: 100, max: 500)
   *   ?from=     — ISO 8601 start date (inclusive)
   *   ?to=       — ISO 8601 end date (inclusive)
   *
   * Returns: Array of { entry_datetime, champion_id, kills, deaths, assists, damage_per_minute, gold_earned, win_status, rating }
   */
  fastify.get('/:id/charts', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    }

    const limit = Math.min(parseInt(req.query.limit as string) || 100, 500);

    const fb = new FilterBuilder().eq('mp.player_id', id);
    if (req.query.from) fb.gte('mp.entry_datetime', new Date(req.query.from));
    if (req.query.to) fb.lte('mp.entry_datetime', new Date(req.query.to));

    const { clause, params } = fb.build();
    const history = await query(
      `SELECT mp.entry_datetime, mp.champion_id, mp.kills, mp.deaths, mp.assists,
         mp.damage_per_minute, mp.gold_earned, mp.win_status,
         mrs.queue_mu_post::DOUBLE PRECISION AS rating
       FROM match_players mp
       LEFT JOIN match_rating_snapshots mrs
         ON mrs.match_id = mp.match_id
        AND mrs.player_id = mp.player_id
        AND mrs.champion_id = mp.champion_id
       ${clause} ORDER BY mp.entry_datetime DESC LIMIT $${params.length + 1}`,
      [...params, limit]
    );
    return history;
  });

  /**
   * GET /players/:id/loadouts — Database-only player saved decks.
   *
   * The explicit POST refresh route owns all vendor calls.
   */
  fastify.get('/:id/loadouts', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    const freshness = getPlayerLoadoutFreshness(await readPlayerLoadoutFetchState(id));
    return { loadouts: await readCachedPlayerLoadouts(id), freshness, refreshed: false, refresh_error: null };
  });

  /** Manually refresh saved decks. This bypasses the 24-hour TTL but has a 10-minute per-player cooldown. */
  fastify.post('/:id/loadouts/refresh', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    if (!Number.isInteger(id) || id <= 0) return reply.status(400).send(err('VALIDATION', 'Invalid player ID'));
    try {
      const result = await refreshPlayerLoadouts(
        id,
        true,
        () => guardVendorFallback(req, reply, {
          scope: 'player-loadouts-refresh',
          entity: id,
        }),
      );
      return { loadouts: await readCachedPlayerLoadouts(id), freshness: result.freshness, refreshed: result.refreshed };
    } catch (error: any) {
      if (error?.code === 'LOADOUT_REFRESH_COOLDOWN') {
        return reply.status(429).send(err('LOADOUT_REFRESH_COOLDOWN', error.message, error.freshness));
      }
      // Preserve and return the last known DB result when the vendor refresh
      // fails. The response remains useful and includes the cooldown state.
      return {
        loadouts: await readCachedPlayerLoadouts(id),
        freshness: getPlayerLoadoutFreshness(await readPlayerLoadoutFetchState(id)),
        refreshed: false,
        refresh_error: error instanceof Error ? error.message : 'Could not refresh player loadouts.',
      };
    }
  });

  /** A single saved deck, retained as a dedicated endpoint for the deck detail page. */
  fastify.get('/:id/loadouts/decks/:loadoutId', async (req: any, reply: any) => {
    const id = parseInt((req.params as any).id, 10);
    const loadoutId = parseInt((req.params as any).loadoutId, 10);
    if (!Number.isInteger(id) || id <= 0 || !Number.isInteger(loadoutId) || loadoutId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid player or loadout ID'));
    }
    const loadout = await one(
      `SELECT
         pl.id,
         pl.deck_id,
         pl.deck_key,
         pl.champion_id,
         COALESCE(c.name, 'Champion ' || pl.champion_id::TEXT) AS champion_name,
         pl.loadout_name,
         COALESCE(pl.card_ids, '{}') AS card_ids,
         COALESCE(pl.card_levels, '{}') AS card_levels,
         pl.talent_id,
         pl.fetched_at,
         pl.updated_at
       FROM player_loadouts pl
       LEFT JOIN champions c ON c.id = pl.champion_id
       WHERE pl.player_id = $1 AND pl.id = $2`,
      [id, loadoutId],
    );
    if (!loadout) return reply.status(404).send(err('NOT_FOUND', 'Saved loadout not found.'));
    return { loadout, freshness: getPlayerLoadoutFreshness(await readPlayerLoadoutFetchState(id)) };
  });

  // Per-card win rates (main feature) - derived from match_player_cards
  fastify.get('/:id/card-winrates', async (req: any) => {
    const id = parseInt((req.params as any).id);
    const championId = parseInt(req.query.championId as string);

    // Compute card win rates on demand (derived from existing data)
    const { computeCardWinRates } = await import('../workers/loadout-tracker.js');
    await computeCardWinRates(id);

    if (championId) {
      // Top builds for specific champion
      const { getTopBuilds } = await import('../workers/loadout-tracker.js');
      return getTopBuilds(id, championId);
    }

    // All card win rates for player
    return await query('SELECT * FROM player_loadout_cards WHERE player_id = $1 ORDER BY win_rate DESC', [id]);
  });

  /**
   * GET /players/bulk — Batch player lookup by IDs.
   *
   * Query params:
   *   ?ids=    — Comma-separated player IDs (required, max 50)
   *
   * Returns: { players: [...], count: N, notFound: [...] }
   *   - players: [{ id, name, level, region, platform, kbm_tier, kbm_points, cheater, sus_count, verified }]
   *   - notFound: array of IDs that were not found (omitted if empty)
   */
  fastify.get('/bulk', async (req: any, reply: any) => {
    const ids = bulkIds(req.query.ids as string, 50);
    if (ids.length === 0) {
      return reply.status(400).send(err('VALIDATION', 'Missing or invalid ids parameter. Provide comma-separated player IDs.'));
    }

    const rows = await query(
      `SELECT p.id, p.name, p.level, p.region, p.platform, p.kbm_tier, p.kbm_points,
              p.cheater, p.sus_count, p.dropper, p.afk_wintrade, p.alt_account,
              EXISTS (
                SELECT 1 FROM player_boosted_associations association
                WHERE association.player_id = p.id
              ) AS boosted,
              EXISTS (SELECT 1 FROM users u WHERE u.linked_player_id = p.id) AS verified
       FROM players p
       WHERE p.id = ANY($1)`,
      [ids]
    );
    const found = new Set(rows.map((r: any) => Number(r.id)));
    const notFound = ids.filter((id: number) => !found.has(id));

    return { players: rows, count: rows.length, notFound: notFound.length > 0 ? notFound : undefined };
  });
}
