import { FastifyInstance } from 'fastify';
import { query } from '../config/db';
import { searchPlayers, searchMatches } from '../services/meilisearch';
import {
  getPlayerBatchLookup,
  getPlayerIdByName,
  getPlayerIdByPortalUserId,
  getPlayerIdsByGamerTag,
  searchPlayersRemote,
} from '../services/hirez';
import { recordRawHirezResponse } from '../services/raw-hirez-response-audit';
import { normalizePlayerProfile } from '../services/normalizer';
import { upsertPlayerProfile } from '../services/player-profile-store';
import { syncPlayer } from '../services/meilisearch';
import { fetchMatches } from './matches';
import { guardVendorFallback } from '../services/request-security';
import { isDeveloperApiRequest } from '../services/developer-api';

type UniversalSearchType = 'player' | 'match' | 'champion' | 'item' | 'card' | 'talent';
type RemoteLookupTarget = 'player-id' | 'player-name' | 'match-id';
type PortalLookupMode = 'gamerTag' | 'portalUserId';

interface PortalLookupHint {
  portalId: number;
  label: string;
  value: string;
  mode: PortalLookupMode;
}

interface UniversalSearchResult {
  type: UniversalSearchType;
  id: string;
  title: string;
  subtitle: string;
  href: string;
  score: number;
  meta?: Record<string, unknown>;
}

interface RemoteLookupCacheRow {
  cache_key: string;
  status: 'hit' | 'miss' | 'error';
  result: UniversalSearchResult[];
  error_message: string | null;
  expires_at: string;
}

interface RemoteLookupInfo {
  attempted: boolean;
  target?: RemoteLookupTarget;
  cacheHit?: boolean;
  skipped?: boolean;
  reason?: string;
  status?: 'hit' | 'miss' | 'error';
  error?: string;
}

let remoteLookupCacheReady: Promise<void> | null = null;

function ensureRemoteLookupCache(): Promise<void> {
  if (!remoteLookupCacheReady) {
    remoteLookupCacheReady = query(`
      CREATE TABLE IF NOT EXISTS search_remote_lookup_cache (
        cache_key TEXT PRIMARY KEY,
        query TEXT NOT NULL,
        target VARCHAR(30) NOT NULL,
        status VARCHAR(20) NOT NULL CHECK (status IN ('hit', 'miss', 'error')),
        result JSONB NOT NULL DEFAULT '[]'::jsonb,
        error_message TEXT,
        fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        expires_at TIMESTAMPTZ NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_search_remote_lookup_cache_expires
        ON search_remote_lookup_cache (expires_at);
      COMMENT ON TABLE search_remote_lookup_cache IS
        'Durable cache for explicit universal-search Hi-Rez fallbacks. Prevents repeated hits and misses from burning API calls.';
    `).then(() => undefined);
  }
  return remoteLookupCacheReady;
}

function parseLimit(value: unknown, fallback = 30, max = 60): number {
  const parsed = parseInt(String(value ?? fallback), 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return Math.min(Math.floor(parsed), max);
}

function escapeLike(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
}

function normalizeText(value: unknown): string {
  return String(value ?? '').trim().toLowerCase();
}

function normalizeLookupName(value: unknown): string {
  return normalizeText(value).replace(/\s+/g, ' ');
}

const PORTAL_PREFIXES: Record<string, { portalId: number; label: string }> = {
  xbox: { portalId: 10, label: 'Xbox' },
  xbl: { portalId: 10, label: 'Xbox' },
  psn: { portalId: 9, label: 'PlayStation' },
  ps: { portalId: 9, label: 'PlayStation' },
  playstation: { portalId: 9, label: 'PlayStation' },
  switch: { portalId: 22, label: 'Nintendo Switch' },
  nintendo: { portalId: 22, label: 'Nintendo Switch' },
};

function parsePortalLookupHint(value: string): PortalLookupHint | null {
  const match = value.trim().match(/^(xbox|xbl|psn|ps|playstation|switch|nintendo)[:/](.+)$/i);
  if (!match) return null;
  const portal = PORTAL_PREFIXES[match[1].toLowerCase()];
  const rawValue = match[2].trim();
  if (!portal || rawValue.length < 2 || rawValue.length > 64) return null;
  return {
    ...portal,
    value: rawValue,
    mode: /^\d{6,}$/.test(rawValue) ? 'portalUserId' : 'gamerTag',
  };
}

function championSlug(name: string | null | undefined): string {
  return String(name ?? '').toLowerCase().replace(/[^a-z0-9]/g, '');
}

function isNumericId(value: string): boolean {
  return /^\d{2,}$/.test(value.trim());
}

function isLikelyMatchId(value: string): boolean {
  const q = value.trim();
  if (!/^\d{10,13}$/.test(q)) return false;
  return Number(q) >= 1_000_000_000;
}

function isSafeExactPlayerName(value: string): boolean {
  const trimmed = value.trim();
  if (parsePortalLookupHint(trimmed)) return true;
  if (trimmed.length < 3 || trimmed.length > 32) return false;
  if (/^\d+$/.test(trimmed)) return false;
  if (/[,%*?]/.test(trimmed)) return false;
  return true;
}

function normalizeRemoteTarget(value: unknown, q: string): RemoteLookupTarget | null {
  const target = String(value ?? '').trim().toLowerCase();
  if (target === 'player-id' && isNumericId(q)) return 'player-id';
  if (target === 'match-id' && isLikelyMatchId(q)) return 'match-id';
  if (target === 'player-name' && isSafeExactPlayerName(q)) return 'player-name';
  return null;
}

function rankName(name: unknown, q: string, base: number): number {
  const normalizedName = normalizeText(name);
  const normalizedQuery = normalizeText(q);
  if (!normalizedName || !normalizedQuery) return base;
  if (normalizedName === normalizedQuery) return base + 30;
  if (normalizedName.startsWith(normalizedQuery)) return base + 18;
  if (normalizedName.includes(normalizedQuery)) return base + 8;
  return base;
}

function uniqueResults(results: UniversalSearchResult[]): UniversalSearchResult[] {
  const seen = new Set<string>();
  const deduped: UniversalSearchResult[] = [];
  for (const result of results) {
    const key = `${result.type}:${result.id}:${result.href}`;
    if (seen.has(key)) continue;
    seen.add(key);
    deduped.push(result);
  }
  return deduped;
}

async function safeRows<T>(label: string, work: () => Promise<T[]>): Promise<T[]> {
  try {
    return await work();
  } catch (err) {
    // Search must be a low-risk read path. If one source table is temporarily
    // unavailable or a search index is stale, the universal endpoint should
    // still return useful hits from the remaining sources instead of failing
    // the whole homepage search experience.
    console.error(`[search] ${label} failed: ${err}`);
    return [];
  }
}

async function readRemoteLookupCache(cacheKey: string): Promise<RemoteLookupCacheRow | null> {
  await ensureRemoteLookupCache();
  const rows = await query<RemoteLookupCacheRow>(
    `SELECT cache_key, status, result, error_message, expires_at::text
     FROM search_remote_lookup_cache
     WHERE cache_key = $1
       AND expires_at > now()
     LIMIT 1`,
    [cacheKey],
  );
  return rows[0] ?? null;
}

async function writeRemoteLookupCache(
  cacheKey: string,
  queryText: string,
  target: RemoteLookupTarget,
  status: 'hit' | 'miss' | 'error',
  result: UniversalSearchResult[],
  errorMessage: string | null = null,
): Promise<void> {
  await ensureRemoteLookupCache();
  const ttlSeconds = status === 'hit' ? 24 * 60 * 60 : 6 * 60 * 60;
  await query(
    `INSERT INTO search_remote_lookup_cache (
       cache_key, query, target, status, result, error_message, fetched_at, expires_at
     )
     VALUES ($1, $2, $3, $4, $5::jsonb, $6, now(), now() + ($7::int * interval '1 second'))
     ON CONFLICT (cache_key) DO UPDATE SET
       query = EXCLUDED.query,
       target = EXCLUDED.target,
       status = EXCLUDED.status,
       result = EXCLUDED.result,
       error_message = EXCLUDED.error_message,
       fetched_at = EXCLUDED.fetched_at,
       expires_at = EXCLUDED.expires_at`,
    [cacheKey, queryText, target, status, JSON.stringify(result), errorMessage, ttlSeconds],
  );
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
    kbm_rank: player.kbm_rank,
    kbm_points: player.kbm_points,
    cheater: player.cheater,
    sus_count: player.sus_count,
    portal_id: player.portal_id,
    portal_user_id: player.portal_user_id,
    first_seen: player.first_seen,
    last_seen: player.last_seen,
    last_updated: player.last_updated,
  };
}

function playerRowToResult(row: any, q: string, scoreBase = 112): UniversalSearchResult {
  return {
    type: 'player',
    id: String(row.id),
    title: row.name || `Player ${row.id}`,
    subtitle: [
      row.region || 'Unknown region',
      row.platform || 'Unknown platform',
      row.kbm_tier != null ? `Tier ${row.kbm_tier}` : null,
      `${Number(row.total_matches ?? 0).toLocaleString()} matches`,
    ].filter(Boolean).join(' · '),
    href: `/players/${row.id}`,
    score: Number(row.score ?? rankName(row.name, q, scoreBase)),
    meta: {
      region: row.region,
      platform: row.platform,
      tier: row.kbm_tier,
      rank: row.kbm_rank,
      totalMatches: row.total_matches,
      totalWins: row.total_wins,
    },
  };
}

async function playerResultsByIds(playerIds: number[], q: string): Promise<UniversalSearchResult[]> {
  if (playerIds.length === 0) return [];
  const rows = await query(
    `SELECT id, name, region, platform, kbm_tier, kbm_rank, total_matches, total_wins
     FROM players
     WHERE id = ANY($1::bigint[])
     ORDER BY total_matches DESC NULLS LAST, name ASC`,
    [[...new Set(playerIds)]],
  );
  return rows.map((row: any) => playerRowToResult(row, q, 112));
}

async function upsertRemotePlayerProfiles(raw: any[]): Promise<number[]> {
  const ids: number[] = [];
  for (const row of raw) {
    const profile = normalizePlayerProfile(row);
    if (!profile || profile.player_id <= 0) continue;
    await upsertPlayerProfile(profile);
    ids.push(profile.player_id);
  }
  for (const playerId of ids) {
    try {
      const rows = await query('SELECT * FROM players WHERE id = $1', [playerId]);
      if (rows[0]) void syncPlayer(playerId, playerSearchDocument(rows[0]));
    } catch (err) {
      console.warn(`[search] failed to sync remote player ${playerId} into MeiliSearch: ${err}`);
    }
  }
  return ids;
}

function remoteRows(raw: unknown): any[] {
  if (Array.isArray(raw)) return raw.filter(row => row && typeof row === 'object');
  return raw && typeof raw === 'object' ? [raw] : [];
}

function positivePlayerId(value: unknown): number | null {
  const id = Number(value);
  return Number.isFinite(id) && id > 0 ? Math.floor(id) : null;
}

function playerIdsFromRemote(raw: unknown): number[] {
  const fields = [
    'player_id',
    'playerId',
    'playerID',
    'PlayerId',
    'Id',
    'id',
    'ActivePlayerId',
    'active_player_id',
  ];
  const ids: number[] = [];
  for (const row of remoteRows(raw)) {
    for (const field of fields) {
      const id = positivePlayerId(row[field]);
      if (id) ids.push(id);
    }
  }
  return [...new Set(ids)].slice(0, 20);
}

function remoteNameCandidates(row: any): string[] {
  return [
    row?.Name,
    row?.name,
    row?.hz_player_name,
    row?.hz_gamer_tag,
    row?.player_name,
    row?.playerName,
    row?.gamerTag,
    row?.GamerTag,
  ].map(value => String(value ?? '').trim()).filter(Boolean);
}

function exactRemoteNameRows(raw: unknown, requestedName: string): any[] {
  const wanted = normalizeLookupName(requestedName);
  if (!wanted) return [];
  return remoteRows(raw).filter(row =>
    remoteNameCandidates(row).some(candidate => normalizeLookupName(candidate) === wanted)
  );
}

function isRemoteLookupMiss(err: unknown): boolean {
  const message = err instanceof Error ? err.message : String(err);
  return message.includes('HTTP 404')
    || message.includes('HIREZ_NOT_FOUND_OR_INVALID')
    || message.includes('HIREZ_NO_MATCH_HISTORY')
    || message.includes('not found');
}

async function auditSearchLookup(input: {
  endpoint: string;
  operation: string;
  entityType: string;
  entityId: string | number;
  params: Record<string, unknown>;
  rawResponse: unknown;
}): Promise<void> {
  await recordRawHirezResponse({
    endpoint: input.endpoint,
    operation: input.operation,
    entityType: input.entityType,
    entityId: input.entityId,
    params: { ...input.params, reason: 'universal_search_remote_lookup' },
    rawResponse: input.rawResponse,
    source: 'universal-search-remote-lookup',
  });
}

async function runRemoteLookup(
  q: string,
  target: RemoteLookupTarget,
  options: { bypassCache?: boolean; beforeRemote?: () => Promise<void> } = {},
): Promise<{ results: UniversalSearchResult[]; info: RemoteLookupInfo }> {
  const cacheKey = `universal:${target}:${target === 'player-name' ? normalizeText(q) : q}`;
  // Auto match-ID search uses the short miss cache so a page reload or repeated
  // typing cannot hammer Hi-Rez for the same unavailable match. Explicit
  // button-triggered match lookup may bypass that cache, but the route-level
  // local DB preflight still runs first, so existing matches never spend calls.
  const cached = options.bypassCache ? null : await readRemoteLookupCache(cacheKey);
  if (cached) {
    return {
      results: Array.isArray(cached.result) ? cached.result : [],
      info: {
        attempted: true,
        target,
        cacheHit: true,
        status: cached.status,
        error: cached.error_message ?? undefined,
      },
    };
  }

  try {
    if (target === 'player-id') {
      await options.beforeRemote?.();
      const playerId = Number(q);
      const raw = await getPlayerBatchLookup([playerId], 'universal_search');
      await recordRawHirezResponse({
        endpoint: 'getplayerbatch',
        operation: 'getPlayerBatchLookup',
        entityType: 'search_player_id',
        entityId: playerId,
        params: { playerIds: [playerId], reason: 'universal_search_remote_lookup' },
        rawResponse: raw,
        source: 'universal-search-remote-lookup',
      });
      const ids = await upsertRemotePlayerProfiles(Array.isArray(raw) ? raw : []);
      const results = await playerResultsByIds(ids, q);
      await writeRemoteLookupCache(cacheKey, q, target, results.length > 0 ? 'hit' : 'miss', results);
      return { results, info: { attempted: true, target, cacheHit: false, status: results.length > 0 ? 'hit' : 'miss' } };
    }

    if (target === 'player-name') {
      await options.beforeRemote?.();
      const portalHint = parsePortalLookupHint(q);
      const lookupName = portalHint?.value ?? q;
      let candidateIds: number[] = [];

      if (portalHint) {
        try {
          const raw = portalHint.mode === 'portalUserId'
            ? await getPlayerIdByPortalUserId(portalHint.portalId, portalHint.value, 'universal_search')
            : await getPlayerIdsByGamerTag(portalHint.portalId, portalHint.value, 'universal_search');
          await auditSearchLookup({
            endpoint: portalHint.mode === 'portalUserId' ? 'getplayeridbyportaluserid' : 'getplayeridsbygamertag',
            operation: portalHint.mode === 'portalUserId' ? 'getPlayerIdByPortalUserId' : 'getPlayerIdsByGamerTag',
            entityType: portalHint.mode === 'portalUserId' ? 'search_player_portal_user_id' : 'search_player_gamertag',
            entityId: `${portalHint.portalId}:${portalHint.value}`,
            params: { portalId: portalHint.portalId, value: portalHint.value, portal: portalHint.label },
            rawResponse: raw,
          });
          candidateIds = playerIdsFromRemote(raw);
        } catch (err) {
          if (!isRemoteLookupMiss(err)) throw err;
        }
      } else {
        try {
          const rawNameId = await getPlayerIdByName(lookupName, 'universal_search');
          await auditSearchLookup({
            endpoint: 'getplayeridbyname',
            operation: 'getPlayerIdByName',
            entityType: 'search_player_name_id',
            entityId: normalizeText(lookupName),
            params: { playerName: lookupName },
            rawResponse: rawNameId,
          });
          candidateIds = playerIdsFromRemote(rawNameId);
        } catch (err) {
          if (!isRemoteLookupMiss(err)) throw err;
        }

        if (candidateIds.length === 0) {
          const rawSearch = await searchPlayersRemote(lookupName, 'universal_search');
          await auditSearchLookup({
            endpoint: 'searchplayers',
            operation: 'searchPlayers',
            entityType: 'search_player_name_fuzzy',
            entityId: normalizeText(lookupName),
            params: { searchPlayer: lookupName },
            rawResponse: rawSearch,
          });
          candidateIds = playerIdsFromRemote(exactRemoteNameRows(rawSearch, lookupName));
        }
      }

      const rawProfiles = candidateIds.length > 0
        ? await getPlayerBatchLookup(candidateIds, 'universal_search')
        : [];
      if (candidateIds.length > 0) {
        await auditSearchLookup({
          endpoint: 'getplayerbatch',
          operation: 'getPlayerBatchLookup',
          entityType: 'search_player_name_profile',
          entityId: normalizeText(q),
          params: { playerIds: candidateIds, resolvedFrom: portalHint ? `${portalHint.label}:${portalHint.mode}` : 'player_name' },
          rawResponse: rawProfiles,
        });
      }
      const ids = await upsertRemotePlayerProfiles(Array.isArray(rawProfiles) ? rawProfiles : []);
      const results = await playerResultsByIds(ids, q);
      await writeRemoteLookupCache(cacheKey, q, target, results.length > 0 ? 'hit' : 'miss', results);
      return { results, info: { attempted: true, target, cacheHit: false, status: results.length > 0 ? 'hit' : 'miss' } };
    }

    const matchId = Number(q);
    // An exact match-ID lookup is the compatibility path for queues that Hi-Rez
    // omits from getmatchhistory, including map-specific custom queues. Persist
    // the authoritative match and its players through the same canonical
    // requested-ingestion path as the match page. The callback runs only after
    // fetchMatches proves that PostgreSQL cannot satisfy the lookup.
    const fetched = await fetchMatches([matchId], {
      allowHirezFallback: true,
      beforeHirezFallback: options.beforeRemote
        ? async () => options.beforeRemote?.()
        : undefined,
    });
    const found = Array.isArray(fetched?.matches) && fetched.matches.length > 0;
    const match = found ? fetched.matches[0]?.match : null;
    const playerCount = found && Array.isArray(fetched.matches[0]?.players) ? fetched.matches[0].players.length : null;
    const results: UniversalSearchResult[] = found ? [{
      type: 'match',
      id: String(matchId),
      title: `Match ${matchId}`,
      subtitle: [
        match?.map || 'Unknown map',
        match?.region || 'Unknown region',
        match?.queue_id ? `Queue ${match.queue_id}` : null,
        playerCount != null ? `${playerCount}/10 players` : null,
      ].filter(Boolean).join(' · '),
      href: `/matches/${matchId}`,
      score: 118,
      meta: {
        remoteLookup: true,
        playerCount,
      },
    }] : [];
    await writeRemoteLookupCache(cacheKey, q, target, found ? 'hit' : 'miss', results);
    return { results, info: { attempted: true, target, cacheHit: false, status: found ? 'hit' : 'miss' } };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    await writeRemoteLookupCache(cacheKey, q, target, 'error', [], message);
    return { results: [], info: { attempted: true, target, cacheHit: false, status: 'error', error: message } };
  }
}

export default async function searchRoutes(fastify: FastifyInstance) {
  /**
   * GET /search/players — Full-text player search via MeiliSearch.
   * Supports fuzzy matching, typo tolerance, and relevance ranking.
   *
   * Query params:
   *   ?q=        — Search query (player name, required)
   *   ?limit=    — Max results (default: 20, max: 100)
   *
   * Returns: Array of player documents from MeiliSearch index.
   */
  fastify.get('/players', async (req: any, reply: any) => {
    const q = req.query.q as string;
    if (!q || q.trim().length === 0) {
      return reply.status(400).send({ error: 'Missing required query parameter: q' });
    }
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const results = await searchPlayers(q.trim(), limit);
    return results;
  });

  /**
   * GET /search/matches — Full-text match search via MeiliSearch.
   * Supports fuzzy matching, typo tolerance, and relevance ranking.
   *
   * Query params:
   *   ?q=        — Search query (match ID, player name, etc., required)
   *   ?limit=    — Max results (default: 20, max: 100)
   *
   * Returns: Array of match documents from MeiliSearch index.
   */
  fastify.get('/matches', async (req: any, reply: any) => {
    const q = req.query.q as string;
    if (!q || q.trim().length === 0) {
      return reply.status(400).send({ error: 'Missing required query parameter: q' });
    }
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);
    const results = await searchMatches(q.trim(), limit);
    return results;
  });

  /**
   * GET /search/universal — Single search box for the public frontend.
   *
   * The homepage search intentionally fans out across independent read models:
   * - players: PostgreSQL canonical profile rows, with exact player-id support
   * - matches: PostgreSQL canonical match rows, with exact match-id support
   * - champions/items/cards/talents: reference tables
   *
   * This route is local-only for fuzzy text, player names, player IDs,
   * champions, cards, talents, and items. The one intentional automatic
   * exception is an exact match-ID shaped query: match IDs have a stable
   * numeric shape, so the endpoint checks the local DB first and then falls
   * through to `fetchMatches`, which runs the same direct/recovery persistence
   * path used by the match detail page. That lets a match from any positive
   * queue ID—including a custom map queue omitted by player history—become
   * visible from search without requiring an extra button click,
   * while avoiding blind Hi-Rez calls for ordinary typing.
   */
  fastify.get('/universal', async (req: any, reply: any) => {
    const q = String(req.query.q ?? '').trim();
    if (q.length === 0) {
      return reply.status(400).send({ error: 'Missing required query parameter: q' });
    }

    const limit = parseLimit(req.query.limit, 30, 60);
    const perSourceLimit = Math.min(Math.max(limit, 10), 25);
    const like = `%${escapeLike(q)}%`;
    const numeric = isNumericId(q);
    const numericValue = numeric ? q : null;
    // Keep public API search deterministic and database-only. Exact resource
    // read-through belongs on GET /api/v1/matches/:id, where the request can be
    // guarded, persisted, and reported with match-specific status semantics.
    const autoMatchLookup = isLikelyMatchId(q) && !isDeveloperApiRequest(req);
    const explicitRemote = req.query.remote === 'true' || req.query.remote === '1';
    const wantsRemote = explicitRemote || autoMatchLookup;
    const remoteTarget = autoMatchLookup ? 'match-id' : (explicitRemote ? normalizeRemoteTarget(req.query.remoteTarget, q) : null);
    const bypassRemoteCache = explicitRemote
      && remoteTarget === 'match-id'
      && (req.query.refresh === 'true' || req.query.refresh === '1' || req.query.bypassCache === 'true');

    const [
      playerRows,
      matchRows,
      championRows,
      itemRows,
      cardRows,
      talentRows,
    ] = await Promise.all([
      safeRows('players', async () => query(
        `SELECT
          id, name, hz_player_name, hz_gamer_tag, region, platform, kbm_tier, kbm_rank, total_matches, total_wins,
          CASE
            WHEN $1::BIGINT IS NOT NULL AND id = $1::BIGINT THEN 120
            WHEN lower(name) = lower($2) THEN 110
            WHEN lower(name) LIKE lower($3) ESCAPE '\\' THEN 92
            ELSE 70
          END
          + LEAST(COALESCE(total_matches, 0), 5000) / 500 AS score
         FROM players
         WHERE
           ($1::BIGINT IS NOT NULL AND id = $1::BIGINT)
           OR name ILIKE $4 ESCAPE '\\'
           OR hz_player_name ILIKE $4 ESCAPE '\\'
           OR hz_gamer_tag ILIKE $4 ESCAPE '\\'
         ORDER BY score DESC, total_matches DESC NULLS LAST, name ASC
         LIMIT $5`,
        [numericValue, q, `${escapeLike(q)}%`, like, perSourceLimit]
      )),
      safeRows('matches', async () => numeric
        ? query(
          `SELECT
            m.match_id,
            MAX(m.entry_datetime) AS entry_datetime,
            MAX(m.map) AS map,
            MAX(m.queue_id) AS queue_id,
            MAX(m.region) AS region,
            MAX(m.duration_seconds) AS duration_seconds,
            COUNT(DISTINCT mp.player_id)::INT AS player_count
           FROM matches m
           LEFT JOIN match_players mp ON mp.match_id = m.match_id
           WHERE m.match_id = $1::BIGINT
           GROUP BY m.match_id
           LIMIT 5`,
          [numericValue]
        )
        : []
      ),
      safeRows('champions', async () => query(
        `SELECT id, name, roles
         FROM champions
         WHERE name ILIKE $1 ESCAPE '\\'
         ORDER BY
           CASE
             WHEN lower(name) = lower($2) THEN 0
             WHEN lower(name) LIKE lower($3) ESCAPE '\\' THEN 1
             ELSE 2
           END,
           name ASC
         LIMIT $4`,
        [like, q, `${escapeLike(q)}%`, perSourceLimit]
      )),
      safeRows('items', async () => query(
        `SELECT item_id, item_name, item_type, champion_id, c.name AS champion_name
         FROM items i
         LEFT JOIN champions c ON c.id = i.champion_id
         WHERE i.item_name ILIKE $1 ESCAPE '\\'
         ORDER BY
           CASE
             WHEN lower(i.item_name) = lower($2) THEN 0
             WHEN lower(i.item_name) LIKE lower($3) ESCAPE '\\' THEN 1
             ELSE 2
           END,
           i.champion_id NULLS FIRST,
           i.item_name ASC
         LIMIT $4`,
        [like, q, `${escapeLike(q)}%`, perSourceLimit]
      )),
      safeRows('cards', async () => query(
        `SELECT card_id, card_name, champion_id, c.name AS champion_name
         FROM cards ca
         LEFT JOIN champions c ON c.id = ca.champion_id
         WHERE ca.card_name ILIKE $1 ESCAPE '\\'
         ORDER BY
           CASE
             WHEN lower(ca.card_name) = lower($2) THEN 0
             WHEN lower(ca.card_name) LIKE lower($3) ESCAPE '\\' THEN 1
             ELSE 2
           END,
           ca.card_name ASC
         LIMIT $4`,
        [like, q, `${escapeLike(q)}%`, perSourceLimit]
      )),
      safeRows('talents', async () => query(
        `SELECT talent_id, talent_name, champion_id, c.name AS champion_name
         FROM talents t
         LEFT JOIN champions c ON c.id = t.champion_id
         WHERE t.talent_name ILIKE $1 ESCAPE '\\'
         ORDER BY
           CASE
             WHEN lower(t.talent_name) = lower($2) THEN 0
             WHEN lower(t.talent_name) LIKE lower($3) ESCAPE '\\' THEN 1
             ELSE 2
           END,
           t.talent_name ASC
         LIMIT $4`,
        [like, q, `${escapeLike(q)}%`, perSourceLimit]
      )),
    ]);

    const results: UniversalSearchResult[] = [
      ...playerRows.map((row: any) => playerRowToResult(row, q, 70)),
      ...matchRows.map((row: any) => ({
        type: 'match' as const,
        id: String(row.match_id),
        title: `Match ${row.match_id}`,
        subtitle: [
          row.map || 'Unknown map',
          row.region || 'Unknown region',
          row.queue_id ? `Queue ${row.queue_id}` : null,
          row.player_count != null ? `${row.player_count}/10 players` : null,
        ].filter(Boolean).join(' · '),
        href: `/matches/${row.match_id}`,
        score: 118,
        meta: {
          entryDatetime: row.entry_datetime,
          map: row.map,
          queueId: row.queue_id,
          region: row.region,
          playerCount: row.player_count,
          durationSeconds: row.duration_seconds,
        },
      })),
      ...championRows.map((row: any) => ({
        type: 'champion' as const,
        id: String(row.id),
        title: row.name,
        subtitle: `${row.roles || 'Champion'} · stats, talents, cards, leaderboards`,
        href: `/champions/${championSlug(row.name)}`,
        score: rankName(row.name, q, 88),
        meta: {
          role: row.roles,
        },
      })),
      ...itemRows.map((row: any) => ({
        type: 'item' as const,
        id: String(row.item_id),
        title: row.item_name || `Item ${row.item_id}`,
        subtitle: [
          row.item_type || 'Item',
          row.champion_name ? `${row.champion_name} reference` : 'Universal item',
        ].filter(Boolean).join(' · '),
        href: row.champion_name ? `/champions/${championSlug(row.champion_name)}` : '/stats/items',
        score: rankName(row.item_name, q, 72),
        meta: {
          itemType: row.item_type,
          championId: row.champion_id,
          championName: row.champion_name,
        },
      })),
      ...cardRows.map((row: any) => ({
        type: 'card' as const,
        id: String(row.card_id),
        title: row.card_name || `Card ${row.card_id}`,
        subtitle: row.champion_name ? `${row.champion_name} loadout card` : 'Loadout card',
        href: row.champion_name ? `/champions/${championSlug(row.champion_name)}` : '/stats/loadouts',
        score: rankName(row.card_name, q, 76),
        meta: {
          championId: row.champion_id,
          championName: row.champion_name,
        },
      })),
      ...talentRows.map((row: any) => ({
        type: 'talent' as const,
        id: String(row.talent_id),
        title: row.talent_name || `Talent ${row.talent_id}`,
        subtitle: row.champion_name ? `${row.champion_name} talent` : 'Champion talent',
        href: row.champion_name ? `/champions/${championSlug(row.champion_name)}` : '/stats/talents',
        score: rankName(row.talent_name, q, 78),
        meta: {
          championId: row.champion_id,
          championName: row.champion_name,
        },
      })),
    ];

    let remoteInfo: RemoteLookupInfo = { attempted: false };
    const exactLocalPlayer = numeric
      ? playerRows.some((row: any) => String(row.id) === q)
      : playerRows.some((row: any) => normalizeText(row.name) === normalizeText(q)
        || normalizeText(row.hz_player_name) === normalizeText(q)
        || normalizeText(row.hz_gamer_tag) === normalizeText(q));
    const exactLocalMatch = numeric && matchRows.some((row: any) => String(row.match_id) === q);

    if (wantsRemote && !remoteTarget) {
      remoteInfo = {
        attempted: false,
        skipped: true,
        reason: 'remote lookup requires remoteTarget=player-id, player-name, or match-id with an exact safe query',
      };
    } else if (remoteTarget) {
      // Remote lookup is exact and guarded. Player fallbacks are explicit
      // button actions; match-ID shaped searches can enter here automatically
      // because the numeric pattern is stable. Either way, the DB preflight
      // below prevents a user from burning calls for data already present
      // locally, and `runRemoteLookup(match-id)` writes through `fetchMatches`
      // so direct details, broken-match recovery, casual persistence, and
      // ranked-metric exclusion stay on the normal match ingestion path.
      if ((remoteTarget === 'player-id' || remoteTarget === 'player-name') && exactLocalPlayer) {
        remoteInfo = { attempted: false, target: remoteTarget, skipped: true, reason: 'player already exists locally' };
      } else if (remoteTarget === 'match-id' && exactLocalMatch) {
        remoteInfo = { attempted: false, target: remoteTarget, skipped: true, reason: 'match already exists locally' };
      } else {
        const remote = await runRemoteLookup(q, remoteTarget, {
          bypassCache: bypassRemoteCache,
          beforeRemote: () => guardVendorFallback(req, reply, {
            scope: `search-${remoteTarget}`,
            entity: remoteTarget === 'match-id'
              ? q
              : q.normalize('NFKC').toLocaleLowerCase(),
          }),
        });
        remoteInfo = remote.info;
        results.push(...remote.results);
      }
    }

    const data = uniqueResults(results)
      .sort((a, b) => b.score - a.score || a.type.localeCompare(b.type) || a.title.localeCompare(b.title))
      .slice(0, limit);

    reply.header('Cache-Control', wantsRemote ? 'private, no-store' : 'public, max-age=60');
    return {
      query: q,
      total: data.length,
      data,
      remote: remoteInfo,
    };
  });
}
