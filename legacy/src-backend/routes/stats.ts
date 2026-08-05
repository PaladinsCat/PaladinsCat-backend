import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { err, DISPLAY_NAME_SQL } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';
import { championRoleSql } from '../utils/champion-roles';
import { registerReadThroughCache } from '../utils/route-cache';
import pLimit from 'p-limit';
import { internalRequestHeaders } from '../services/internal-request';
import { appendLobbyTierPredicate, lobbyTierQueryString, parseLobbyTierBounds } from '../utils/lobby-tier';
import { calculateWeightedMetricStats } from '../services/performance-projections';
import {
  isPublicStatsScope,
  MATCH_STAT_SCOPES,
} from '../workers/match-count-discovery-policy';
import {
  decodePresenceDetailCursor,
  encodePresenceDetailCursor,
  parsePresenceDetailLimit,
  parsePresenceDetailQueueId,
  parsePresenceEvidenceLimit,
  parsePresenceEvidencePage,
  parsePresencePlayerSort,
} from '../workers/presence-detail-policy';
import { PUBLIC_PLAYER_EVIDENCE_CTES_SQL } from '../workers/player-presence-evidence';

type PerformanceMetricKey = 'dpm' | 'wpm' | 'apm' | 'hpm' | 'gpm' | 'egpm' | 'mpm' | 'kda';
type EcpmCandidateBracket = 'possible-disconnect' | 'disconnected' | 'partial-afk' | 'full-afk';
const ACTIVITY_STALE_TTL_SECONDS = 6 * 60 * 60;
// Casual percentile aggregates scan large fact tables. Keep the established
// cache contract, but serialize cold computations on the 2-core VPS so one
// metrics page cannot saturate PostgreSQL with parallel scans.
const casualPerformanceMetricQueryLimit = pLimit(1);

function statsCacheFreshTtlSeconds(req: { url: string }): number {
  const pathname = new URL(req.url, 'http://paladinscat.local').pathname;
  if (pathname === '/stats/hourly-match-counts') return 60;
  if (pathname === '/stats/presence') return 60;
  if (pathname === '/stats/performance-metrics') return 900;
  if (pathname === '/stats/tier-population') return 900;
  return 300;
}

function statsCacheStaleTtlSeconds(req: { url: string }): number {
  const url = new URL(req.url, 'http://paladinscat.local');
  if (url.pathname === '/stats/presence' && url.searchParams.get('view') === 'activity-v4') {
    return ACTIVITY_STALE_TTL_SECONDS;
  }
  return statsCacheFreshTtlSeconds(req) * 3;
}

const ECPM_CANDIDATE_BRACKETS: Record<EcpmCandidateBracket, {
  minimum: number;
  maximum: number;
  automaticFlag: boolean;
}> = {
  'possible-disconnect': { minimum: 110, maximum: 120, automaticFlag: false },
  disconnected: { minimum: 90, maximum: 110, automaticFlag: false },
  'partial-afk': { minimum: 70, maximum: 90, automaticFlag: false },
  'full-afk': { minimum: 0, maximum: 70, automaticFlag: true },
};

interface EcpmCandidateCursor {
  at: string;
  matchId: string;
  playerId: string;
}

function decodeEcpmCandidateCursor(value: unknown): EcpmCandidateCursor | null {
  if (value == null || value === '') return null;
  try {
    const parsed = JSON.parse(Buffer.from(String(value), 'base64url').toString('utf8')) as Partial<EcpmCandidateCursor>;
    const at = String(parsed.at ?? '');
    const matchId = String(parsed.matchId ?? '');
    const playerId = String(parsed.playerId ?? '');
    if (!at || Number.isNaN(Date.parse(at)) || !/^\d+$/.test(matchId) || !/^\d+$/.test(playerId)) return null;
    return { at, matchId, playerId };
  } catch {
    return null;
  }
}

function encodeEcpmCandidateCursor(row: any): string {
  const at = row.entry_datetime instanceof Date ? row.entry_datetime.toISOString() : String(row.entry_datetime);
  return Buffer.from(JSON.stringify({
    at,
    matchId: String(row.match_id),
    playerId: String(row.player_id),
  })).toString('base64url');
}

// The legacy `damage_done_physical` column stores total player damage.
// Recovered history rows never have a trustworthy weapon/ability split, even
// if one partial breakdown field happens to be present.
const SQL_TOTAL_DAMAGE = 'COALESCE(mp.damage_done_physical, 0)';
const SQL_HAS_DAMAGE_SPLIT = `COALESCE(mp.source, 'direct') <> 'recovered'`;

const PERFORMANCE_METRICS: Record<PerformanceMetricKey, string> = {
  dpm: 'mp.damage_per_minute',
  wpm: `CASE WHEN m.duration_seconds > 0 AND ${SQL_HAS_DAMAGE_SPLIT}
    THEN COALESCE(mp.damage_done_in_hand, 0) / (m.duration_seconds / 60.0) END`,
  apm: `CASE WHEN m.duration_seconds > 0 AND ${SQL_HAS_DAMAGE_SPLIT}
    THEN GREATEST(${SQL_TOTAL_DAMAGE} - COALESCE(mp.damage_done_in_hand, 0), 0) / (m.duration_seconds / 60.0) END`,
  hpm: 'mp.healing_per_minute',
  gpm: 'mp.gold_per_minute',
  egpm: 'mp.egpm',
  mpm: 'mp.mitigation_per_minute',
  kda: 'mp.kda',
};

const CASUAL_PERFORMANCE_METRICS: Partial<Record<PerformanceMetricKey, string>> = {
  dpm: 'cmp.damage * 60.0 / NULLIF(cm.duration_seconds, 0)',
  hpm: 'cmp.healing * 60.0 / NULLIF(cm.duration_seconds, 0)',
  gpm: 'cmp.credits * 60.0 / NULLIF(cm.duration_seconds, 0)',
  mpm: 'cmp.mitigation * 60.0 / NULLIF(cm.duration_seconds, 0)',
};

// Hi-Rez direct match details usually store outcomes as Winner/Loser, while
// recovery/history-derived rows can surface as Win/Loss. Public win-rate
// queries must normalize both spellings; otherwise recovered authoritative rows
// inflate denominators while their wins vanish from the numerator.
const SQL_NORMALIZED_WIN = `lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')`;
const SQL_NORMALIZED_LOSS = `lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')`;
const SQL_NORMALIZED_OUTCOME = `(${SQL_NORMALIZED_WIN} OR ${SQL_NORMALIZED_LOSS})`;

function requireRankedStatsMode(req: any, reply: any): boolean {
  const mode = String(req.query?.mode ?? 'ranked').toLowerCase();
  if (mode === 'ranked') return true;
  reply.code(400).send(err('VALIDATION', 'Only ranked queue 486 is available for aggregate statistics.'));
  return false;
}

function parseQueueId(value: unknown, fallback = 486): number | undefined {
  const parsed = parseInt(String(value ?? fallback), 10);
  return parsed === 486 ? parsed : undefined;
}

function normalizeMetric(value: unknown): { key: PerformanceMetricKey; expression: string } | null {
  const key = String(value ?? '').toLowerCase() as PerformanceMetricKey;
  const expression = PERFORMANCE_METRICS[key];
  return expression ? { key, expression } : null;
}

function normalizeRole(value: unknown): { role: string } | null {
  const key = String(value ?? '').toLowerCase().replace(/[\s_-]/g, '');
  const normalizedKey = key === 'front' || key === 'frontline' ? 'frontline' : key;
  if (!['frontline', 'damage', 'flank', 'support'].includes(normalizedKey)) return null;
  const role = normalizedKey === 'frontline' ? 'Frontline' : normalizedKey.charAt(0).toUpperCase() + normalizedKey.slice(1);
  return { role };
}

function metricModeExpression(metric: PerformanceMetricKey): string {
  return metric === 'kda' ? 'ROUND(value::NUMERIC, 1)' : 'ROUND(value::NUMERIC, 0)';
}

async function performanceMetricSummary(metric: PerformanceMetricKey, expression: string, req: any) {
  const queueId = parseQueueId(req.query.queueId);
  if (!queueId) throw new Error('INVALID_QUEUE');
  const roleFilter = req.query.role ? normalizeRole(req.query.role) : null;
  if (req.query.role && !roleFilter) throw new Error('INVALID_ROLE');
  const lobbyTier = parseLobbyTierBounds(req.query);
  if (!lobbyTier) throw new Error('INVALID_TIER');

  if (lobbyTier.active) {
    const params:any[]=[queueId,roleFilter ? ({ Damage:1,Flank:2,Support:3,Frontline:4 } as Record<string,number>)[roleFilter.role] : 0,metric];
    const where=['queue_id=$1','role_id=$2','metric=$3'];
    appendLobbyTierPredicate(lobbyTier,params,where,'stats_metric_histogram');
    const histogram=await query<any>(`SELECT queue_id,role_id,
        CASE role_id WHEN 1 THEN 'Damage' WHEN 2 THEN 'Flank' WHEN 3 THEN 'Support' WHEN 4 THEN 'Frontline' ELSE 'Global' END AS role_name,
        metric,value,SUM(sample_count)::BIGINT AS sample_count
      FROM stats_metric_histogram WHERE ${where.join(' AND ')}
      GROUP BY queue_id,role_id,metric,value ORDER BY value`,params);
    const stats=calculateWeightedMetricStats(histogram)[0];
    return stats ? {
      min:stats.min,max:stats.max,mean:stats.mean,median:stats.median,mode:stats.mode,
      p10:stats.p10,p25:stats.p25,p75:stats.p75,p90:stats.p90,sample_size:stats.sampleSize,
    } : { min:0,max:0,mean:0,median:0,mode:0,p10:0,p25:0,p75:0,p90:0,sample_size:0 };
  }

  // The common all-history scopes read a compact projection maintained during
  // ingest. Only ad-hoc lobby-tier ranges retain the fact-table fallback.
  if (!lobbyTier.active) {
    const row = await one<any>(
      `SELECT
        min_value AS min,
        max_value AS max,
        mean_value AS mean,
        median_value AS median,
        mode_value AS mode,
        p10_value AS p10,
        p25_value AS p25,
        p75_value AS p75,
        p90_value AS p90,
        sample_size::DOUBLE PRECISION AS sample_size,
        updated_at
      FROM performance_metric_stats
      WHERE queue_id = $1 AND role_name = $2 AND metric = $3`,
      [queueId, roleFilter?.role ?? 'Global', metric],
    );
    return row ?? { min: 0, max: 0, mean: 0, median: 0, mode: 0, p10: 0, p25: 0, p75: 0, p90: 0, sample_size: 0 };
  }

  // Role filter → compute from match_players (role-specific baselines not yet cached)
  const params: any[] = [queueId];
  const where: string[] = ['m.queue_id = $1'];
  if (roleFilter) {
    params.push(roleFilter.role);
    where.push(`${championRoleSql('c')} = $${params.length}`);
  }
  appendLobbyTierPredicate(lobbyTier, params, where);

  const row = await one<any>(
    `WITH metric_values AS (
      SELECT ${expression}::DOUBLE PRECISION AS value
      FROM match_players mp
      JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
      JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      LEFT JOIN champions c ON c.id = mp.champion_id
      WHERE ${where.join(' AND ')}
    )
    SELECT
      COALESCE(ROUND(MIN(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS min,
      COALESCE(ROUND(MAX(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS max,
      COALESCE(ROUND(AVG(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS mean,
      COALESCE(ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS median,
      COALESCE(ROUND((MODE() WITHIN GROUP (ORDER BY ${metricModeExpression(metric)}))::NUMERIC, 2), 0)::DOUBLE PRECISION AS mode,
      COALESCE(ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p10,
      COALESCE(ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p25,
      COALESCE(ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p75,
      COALESCE(ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p90,
      COUNT(*)::INT AS sample_size
    FROM metric_values
    WHERE value IS NOT NULL
      AND (value > 0 OR ($${params.length + 1}::TEXT IN ('wpm', 'apm', 'egpm') AND value = 0))`,
    [...params, metric]
  );

  return row ?? { min: 0, max: 0, mean: 0, median: 0, mode: 0, p10: 0, p25: 0, p75: 0, p90: 0, sample_size: 0 };
}

async function casualPerformanceMetricSummary(metric: PerformanceMetricKey, req: any) {
  const expression = CASUAL_PERFORMANCE_METRICS[metric];
  if (!expression) throw new Error('INVALID_CASUAL_METRIC');
  if (req.query.tierMin != null || req.query.tierMax != null) {
    throw new Error('CASUAL_TIER_FILTER');
  }
  const roleFilter = req.query.role ? normalizeRole(req.query.role) : null;
  if (req.query.role && !roleFilter) throw new Error('INVALID_ROLE');

  const params: any[] = [];
  const where = [
    'cm.stats_eligible = true',
    `cm.quality = 'complete'`,
    'cmp.stats_eligible = true',
    `cmp.participant_kind = 'human'`,
    'cmp.player_id > 0',
    'cmp.task_force IN (1, 2)',
    `lower(COALESCE(cmp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')`,
    'cm.duration_seconds > 0',
  ];
  if (roleFilter) {
    params.push(roleFilter.role);
    where.push(`${championRoleSql('c')} = $${params.length}`);
  }

  const row = await casualPerformanceMetricQueryLimit(() => one<any>(
    `WITH metric_values AS (
      SELECT (${expression})::DOUBLE PRECISION AS value
      FROM casual_match_players cmp
      JOIN casual_matches cm ON cm.match_id = cmp.match_id
      LEFT JOIN champions c ON c.id = cmp.champion_id
      WHERE ${where.join(' AND ')}
    )
    SELECT
      COALESCE(ROUND(MIN(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS min,
      COALESCE(ROUND(MAX(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS max,
      COALESCE(ROUND(AVG(value)::NUMERIC, 2), 0)::DOUBLE PRECISION AS mean,
      COALESCE(ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS median,
      COALESCE(ROUND((MODE() WITHIN GROUP (ORDER BY ${metricModeExpression(metric)}))::NUMERIC, 2), 0)::DOUBLE PRECISION AS mode,
      COALESCE(ROUND((PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p10,
      COALESCE(ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p25,
      COALESCE(ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p75,
      COALESCE(ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY value))::NUMERIC, 2), 0)::DOUBLE PRECISION AS p90,
      COUNT(*)::INT AS sample_size
    FROM metric_values
    WHERE value > 0`,
    params,
  ));
  return row ?? { min: 0, max: 0, mean: 0, median: 0, mode: 0, p10: 0, p25: 0, p75: 0, p90: 0, sample_size: 0 };
}

export default async function statsRoutes(fastify: FastifyInstance) {
  registerReadThroughCache(fastify, {
    // Version the namespace when aggregate or response semantics change so a
    // deployment cannot return an earlier payload contract from Redis.
    namespace: 'route:stats:v5',
    shouldCache: (req) => {
      const pathname = new URL(req.url, 'http://paladinscat.local').pathname;
      return pathname !== '/leaderboard-log'
        && pathname !== '/stats/leaderboard-log'
        && !pathname.startsWith('/ecpm-candidates')
        && !pathname.startsWith('/stats/ecpm-candidates');
    },
    ttlSeconds: statsCacheFreshTtlSeconds,
    staleTtlSeconds: statsCacheStaleTtlSeconds,
  });

  /**
   * GET /stats/overview — Main Stats dashboard data in one cached response.
   * Individual detail routes remain reusable; this route composes their public
   * contracts internally so a dashboard visit does not fan out in the browser.
   */
  fastify.get('/overview', async (req: any, reply: any) => {
    reply.header('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const scope = lobbyTierQueryString(lobbyTier);
    const scoped = (url: string) => scope ? `${url}${url.includes('?') ? '&' : '?'}${scope}` : url;
    const routes = {
      metrics: scoped('/stats/performance-metrics'),
      champions: scoped('/champions/overview'),
      items: scoped('/stats/items?mode=ranked&limit=50'),
      maps: scoped('/stats/maps?queueId=486&limit=25'),
      profileTiers: '/stats/tiers?source=profiles',
      activeTiers: '/stats/tiers?source=matches',
    } as const;
    const entries = await Promise.all(Object.entries(routes).map(async ([key, url]) => {
      const response = await fastify.inject({ method: 'GET', url, headers: internalRequestHeaders() });
      if (response.statusCode >= 400) {
        fastify.log.warn({ source: key, statusCode: response.statusCode }, 'Stats overview source failed');
        return [key, null] as const;
      }
      return [key, response.json()] as const;
    }));
    const source = Object.fromEntries(entries) as Record<string, any>;
    return {
      metrics: source.metrics ?? {},
      champions: source.champions ?? { champions: [], stats: [] },
      items: source.items ?? [],
      maps: source.maps ?? [],
      profile_tiers: source.profileTiers ?? [],
      active_tiers: source.activeTiers ?? [],
    };
  });

  /**
   * GET /stats/page-data — Cached root stats page bundle.
   *
   * The root page combines the overview with eCPM baselines, skin activity,
   * popular team compositions, and broken-skin coverage. Keep those reusable
   * routes separate for detail pages, but compose them once here so a stats
   * landing-page visit is a single server-cached request.
   */
  fastify.get('/page-data', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const scope = lobbyTierQueryString(lobbyTier);
    const scoped = (url: string) => scope ? `${url}${url.includes('?') ? '&' : '?'}${scope}` : url;
    const routes = {
      overview: scoped('/stats/overview'),
      baselines: scoped('/stats/baselines?queueId=486'),
      skins: scoped('/stats/skins?limit=5'),
      compositions: scoped('/matches/compositions?limit=10'),
      brokenSkins: scoped('/stats/broken-skins'),
    } as const;
    const entries = await Promise.all(Object.entries(routes).map(async ([key, url]) => {
      const response = await fastify.inject({ method: 'GET', url, headers: internalRequestHeaders() });
      return [key, response.statusCode < 400 ? response.json() : null] as const;
    }));
    const source = Object.fromEntries(entries) as Record<string, any>;
    reply.header('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
    return {
      overview: source.overview ?? { metrics: {}, champions: { champions: [], stats: [] }, items: [], maps: [], profile_tiers: [], active_tiers: [] },
      baselines: source.baselines ?? [],
      skins: source.skins?.data ?? source.skins ?? [],
      compositions: source.compositions?.data ?? source.compositions ?? [],
      broken_skins: source.brokenSkins?.data ?? source.brokenSkins ?? [],
    };
  });

  /**
   * GET /stats/performance-metrics — Global player-match distributions.
   *
   * This reads match_players directly because the page needs a distribution
   * over every observed player-match, not the rolled-up players.avg_* values.
   * The rolled-up columns are better for player leaderboards; this endpoint is
   * for math like percentiles, medians, and modes.
   */
  fastify.get('/performance-metrics', async (req: any, reply: any) => {
    const requestedMetric = req.query.metric ? normalizeMetric(req.query.metric) : null;
    if (req.query.metric && !requestedMetric) {
      return reply.status(400).send(err('VALIDATION', 'Invalid metric. Use dpm, wpm, apm, hpm, gpm, egpm, mpm, or kda.'));
    }
    const scope = String(req.query.scope ?? 'ranked').trim().toLowerCase();
    if (!['ranked', 'casual'].includes(scope)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid scope. Use ranked or casual.'));
    }

    reply.header('Cache-Control', 'public, max-age=900');

    try {
      if (requestedMetric) {
        const summary = scope === 'casual'
          ? await casualPerformanceMetricSummary(requestedMetric.key, req)
          : await performanceMetricSummary(requestedMetric.key, requestedMetric.expression, req);
        const includeRoles = !req.query.role && ['1', 'true'].includes(String(req.query.includeRoles ?? '').toLowerCase());
        if (!includeRoles) return { [requestedMetric.key]: summary };

        // The metrics screen needs the global value and all four role anchors.
        // Returning them together removes four browser/API round trips per tab;
        // the response is cached as one SWR entry below.
        const roles = ['Frontline', 'Damage', 'Flank', 'Support'];
        const roleEntries = await Promise.all(roles.map(async (role) => [
          role,
          scope === 'casual'
            ? await casualPerformanceMetricSummary(requestedMetric.key, { query: { ...req.query, role } })
            : await performanceMetricSummary(requestedMetric.key, requestedMetric.expression, { query: { ...req.query, role } }),
        ] as const));
        return {
          [requestedMetric.key]: summary,
          roles: Object.fromEntries(roleEntries),
        };
      }

      const metrics = scope === 'casual'
        ? Object.keys(CASUAL_PERFORMANCE_METRICS) as PerformanceMetricKey[]
        : Object.keys(PERFORMANCE_METRICS) as PerformanceMetricKey[];
      const entries = await Promise.all(
        metrics.map(async (metric) => [
          metric,
          scope === 'casual'
            ? await casualPerformanceMetricSummary(metric, req)
            : await performanceMetricSummary(metric, PERFORMANCE_METRICS[metric], req),
        ])
      );
      return Object.fromEntries(entries);
    } catch (error) {
      if ((error as Error).message === 'INVALID_QUEUE') {
        return reply.status(400).send(err('VALIDATION', 'Invalid queueId.'));
      }
      if ((error as Error).message === 'INVALID_ROLE') {
        return reply.status(400).send(err('VALIDATION', 'Invalid role. Use damage, flank, support, or frontline.'));
      }
      if ((error as Error).message === 'INVALID_TIER') {
        return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
      }
      if ((error as Error).message === 'INVALID_CASUAL_METRIC') {
        return reply.status(400).send(err('VALIDATION', 'Casual performance supports dpm, hpm, gpm, and mpm.'));
      }
      if ((error as Error).message === 'CASUAL_TIER_FILTER') {
        return reply.status(400).send(err('VALIDATION', 'Lobby-tier filters apply only to ranked performance.'));
      }
      throw error;
    }
  });

  /**
   * GET /stats/performance-metrics/by-champion — Champion-level distribution.
   *
   * Each row is one champion's distribution for one metric. Tier-filtered
   * requests read the compact champion histogram maintained during ingest;
   * the frontend never needs to rescan historical player-match facts.
   */
  fastify.get('/performance-metrics/by-champion', async (req: any, reply: any) => {
    const requestedMetric = normalizeMetric(req.query.metric);
    if (!requestedMetric) {
      return reply.status(400).send(err('VALIDATION', 'Invalid metric. Use dpm, wpm, apm, hpm, gpm, egpm, mpm, or kda.'));
    }
    const queueId = parseQueueId(req.query.queueId);
    if (!queueId) return reply.status(400).send(err('VALIDATION', 'Invalid queueId.'));

    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    let championId: number | null = null;
    if (req.query.championId) {
      championId = parseInt(req.query.championId as string, 10);
      if (!Number.isInteger(championId) || championId <= 0) {
        return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
      }
    }

    reply.header('Cache-Control', 'public, max-age=900');

    if (!lobbyTier.active) {
      const projectionParams: any[] = [queueId, requestedMetric.key];
      const projectionWhere = ['cpb.queue_id = $1', 'cpb.metric = $2'];
      if (championId != null) {
        projectionParams.push(championId);
        projectionWhere.push(`cpb.champion_id = $${projectionParams.length}`);
      }
      const rows = await query(
        `SELECT
          cpb.champion_id,
          c.name AS champion_name,
          ${championRoleSql('c')} AS class,
          cpb.min_value AS min,
          cpb.max_value AS max,
          cpb.mean_value AS mean,
          cpb.median_value AS median,
          cpb.mode_value AS mode,
          cpb.p10_value AS p10,
          cpb.p90_value AS p90,
          cpb.mean_value AS avg_value,
          cpb.sample_size AS total_matches
        FROM champion_performance_baselines cpb
        JOIN champions c ON c.id = cpb.champion_id
        WHERE ${projectionWhere.join(' AND ')}
        ORDER BY cpb.mean_value DESC, cpb.sample_size DESC, c.name ASC`,
        projectionParams,
      );
      return { data: rows, total: rows.length, metric: requestedMetric.key, queue_id: queueId };
    }

    const params: any[] = [queueId,requestedMetric.key];
    const where: string[] = ['scmh.queue_id = $1','scmh.metric = $2'];
    if (championId != null) {
      params.push(championId);
      where.push(`scmh.champion_id = $${params.length}`);
    }
    appendLobbyTierPredicate(lobbyTier, params, where,'scmh');
    const histogram=await query<any>(`SELECT scmh.queue_id,scmh.champion_id AS role_id,c.name AS role_name,
        ${championRoleSql('c')} AS class,scmh.metric,scmh.value,SUM(scmh.sample_count)::BIGINT AS sample_count
      FROM stats_champion_metric_histogram scmh JOIN champions c ON c.id=scmh.champion_id
      WHERE ${where.join(' AND ')} GROUP BY scmh.queue_id,scmh.champion_id,c.name,c.roles,scmh.metric,scmh.value
      ORDER BY scmh.champion_id,scmh.value`,params);
    const classes=new Map<number,string>(histogram.map((row:any)=>[Number(row.role_id),String(row.class)]));
    const rows=calculateWeightedMetricStats(histogram).map((stat)=>({
      champion_id:stat.roleId,champion_name:stat.roleName,class:classes.get(stat.roleId)??'Unknown',
      min:stat.min,max:stat.max,mean:stat.mean,median:stat.median,mode:stat.mode,
      p10:stat.p10,p90:stat.p90,avg_value:stat.mean,total_matches:stat.sampleSize,
    })).sort((a,b)=>b.avg_value-a.avg_value||b.total_matches-a.total_matches||a.champion_name.localeCompare(b.champion_name));
    return { data: rows, total: rows.length, metric: requestedMetric.key, queue_id: queueId };
  });

  /**
   * GET /stats/leaderboard — Tier leaderboard.
   *
   * Query params:
   *   ?tier=    — Ranked tier 0-26 (required)
   *
   * Returns: Top 100 players for the tier ordered by points.
   */
  fastify.get('/leaderboard', async (req: any, reply: any) => {
    const tier = req.query.tier ? parseInt(req.query.tier as string) : -1;
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    // Without tier: return champion-level leaderboard from the maintained
    // ranked projection. champion_meta_stats is an old materialized view that
    // is no longer refreshed by the ingest worker, so using it here made this
    // endpoint fail on databases where that MV was never created.
    if (!Number.isInteger(tier) || tier < 0 || tier > 26) {
      if (lobbyTier.active) {
        const scope = lobbyTierQueryString(lobbyTier);
        const response = await fastify.inject({ method: 'GET', url: `/stats/champions?limit=100&${scope}`, headers: internalRequestHeaders() });
        if (response.statusCode >= 400) return reply.status(response.statusCode).send(response.json());
        return (response.json() as any[]).map((row: any, index: number) => ({
          rank: index + 1,
          championId: row.champion_id,
          championName: row.champion_name,
          winRate: Number(row.win_rate),
          totalPlays: row.total_matches,
        }));
      }
      const rows = await query(
        `SELECT champion_id, champion_name, win_rate, total_matches AS total_plays
         FROM champion_stats_ranked
         WHERE total_matches >= 50
         ORDER BY win_rate DESC
         LIMIT 100`,
        []
      );
      return rows.map((r: any, i: number) => ({
        rank: i + 1,
        championId: r.champion_id,
        championName: r.champion_name,
        winRate: Number(r.win_rate),
        totalPlays: r.total_plays,
      }));
    }

    // With tier: return player rankings for that tier
    const tbl = `leaderboard${tier}`;
    const rows = await query(`SELECT * FROM ${tbl} ORDER BY points DESC LIMIT 100`);
    return rows;
  });

  /**
   * GET /stats/trends — Daily match trends.
   *
   * Query params:
   *   ?days=      — Number of days back (default: 30)
   *   ?from=      — ISO 8601 start date (overrides ?days=)
   *   ?to=        — ISO 8601 end date
   *   ?queueId=   — Filter by queue
   *   ?region=    — Filter by region
   *
   * Returns: Array of { stat_date, queue_id, region, match_count, avg_duration }
   */
  fastify.get('/trends', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    {
      const params: any[] = [486];
      const where = ['sma.queue_id = $1'];
      const from = req.query.from ? new Date(req.query.from) : new Date(Date.now() - (parseInt(req.query.days as string) || 30) * 86400000);
      params.push(from);
      where.push(`sma.stat_date >= $${params.length}::DATE`);
      if (req.query.to) {
        params.push(new Date(req.query.to));
        where.push(`sma.stat_date <= $${params.length}::DATE`);
      }
      if (req.query.region) {
        params.push(req.query.region);
        where.push(`sma.region = $${params.length}`);
      }
      appendLobbyTierPredicate(lobbyTier, params, where, 'sma');
      return query(`SELECT sma.stat_date,sma.queue_id,sma.region,
          SUM(sma.match_count)::BIGINT AS match_count,
          ROUND(SUM(sma.duration_sum)::NUMERIC/NULLIF(SUM(sma.match_count),0),2) AS avg_duration
        FROM stats_match_aggregate sma WHERE ${where.join(' AND ')}
        GROUP BY sma.stat_date,sma.queue_id,sma.region ORDER BY sma.stat_date`,params);
    }
    if (lobbyTier.active) {
      const params: any[] = [];
      const where = ['m.queue_id = 486'];
      const from = req.query.from ? new Date(req.query.from) : new Date(Date.now() - (parseInt(req.query.days as string) || 30) * 86400000);
      params.push(from);
      where.push(`m.entry_datetime >= $${params.length}`);
      if (req.query.to) {
        params.push(new Date(req.query.to));
        where.push(`m.entry_datetime <= $${params.length}`);
      }
      if (req.query.region) {
        params.push(req.query.region);
        where.push(`m.region = $${params.length}`);
      }
      appendLobbyTierPredicate(lobbyTier, params, where);
      return query(`SELECT m.entry_datetime::DATE AS stat_date, 486 AS queue_id, m.region,
          COUNT(*)::INT AS match_count, ROUND(AVG(m.duration_seconds)::NUMERIC, 2) AS avg_duration
        FROM matches m JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        WHERE ${where.join(' AND ')} GROUP BY m.entry_datetime::DATE, m.region ORDER BY stat_date`, params);
    }
    const params: any[] = [];
    const where: string[] = ['COALESCE(m.limited, false) = false'];
    if (req.query.from) {
      params.push(new Date(req.query.from));
      where.push(`m.entry_datetime >= $${params.length}`);
    } else {
      const days = parseInt(req.query.days as string) || 30;
      params.push(new Date(Date.now() - days * 86400000));
      where.push(`m.entry_datetime >= $${params.length}`);
    }
    if (req.query.to) {
      params.push(new Date(req.query.to));
      where.push(`m.entry_datetime <= $${params.length}`);
    }
    const queueId = parseQueueId(req.query.queueId);
    if (!queueId) return reply.status(400).send(err('VALIDATION', 'Only ranked queue 486 is available for aggregate statistics.'));
    params.push(queueId);
    where.push(`m.queue_id = $${params.length}`);
    if (req.query.region) {
      params.push(req.query.region);
      where.push(`m.region = $${params.length}`);
    }

    // Production databases created before the consolidated schema do not have
    // the legacy Timescale continuous aggregate `daily_match_stats`. Keep this
    // correctness fallback on canonical facts; the scalable path is replaced
    // by the queue/tier daily projection in the following migration.
    const trends = await query(`SELECT m.entry_datetime::DATE AS stat_date,
        m.queue_id, m.region, COUNT(*)::INT AS match_count,
        ROUND(AVG(m.duration_seconds)::NUMERIC, 2) AS avg_duration
      FROM matches m
      WHERE ${where.join(' AND ')}
      GROUP BY m.entry_datetime::DATE, m.queue_id, m.region
      ORDER BY stat_date`, params);
    return trends;
  });

  /**
   * GET /stats/ecpm-candidates — Recent low-eCPM player-match observations.
   *
   * Review-only brackets are deliberately distinct from automatic AFK flags.
   * Keyset pagination keeps subsequent pages fast and prevents duplicate rows
   * when new matches arrive while an analyst is paging through candidates.
   */
  fastify.get('/ecpm-candidates', async (req: any, reply: any) => {
    const bracket = String(req.query.bracket ?? 'possible-disconnect') as EcpmCandidateBracket;
    const bracketDefinition = ECPM_CANDIDATE_BRACKETS[bracket];
    if (!bracketDefinition) {
      return reply.status(400).send(err('VALIDATION', 'Invalid bracket. Use possible-disconnect, disconnected, partial-afk, or full-afk.'));
    }

    const queueId = parseQueueId(req.query.queueId);
    if (!queueId) return reply.status(400).send(err('VALIDATION', 'Invalid queueId.'));
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    const rawLimit = Number.parseInt(String(req.query.limit ?? '20'), 10);
    if (!Number.isInteger(rawLimit) || rawLimit < 1) {
      return reply.status(400).send(err('VALIDATION', 'Limit must be a positive integer.'));
    }
    const limit = Math.min(rawLimit, 50);
    const cursor = decodeEcpmCandidateCursor(req.query.cursor);
    if (req.query.cursor && !cursor) {
      return reply.status(400).send(err('VALIDATION', 'Invalid candidate cursor.'));
    }

    const params: any[] = [queueId, bracketDefinition.minimum, bracketDefinition.maximum];
    const where = [
      'm.queue_id = $1',
      'mp.egpm >= $2',
      'mp.egpm < $3',
      "COALESCE(mis.status, 'complete') = 'complete'",
      "COALESCE(mp.source, 'direct') IN ('direct', 'recovered')",
      'mp.is_ranked = true',
      'mp.player_id > 0',
      'mp.champion_id > 0',
      'mp.task_force IN (1, 2)',
      SQL_NORMALIZED_OUTCOME,
      'm.duration_seconds > 120',
    ];
    appendLobbyTierPredicate(lobbyTier, params, where);
    if (cursor) {
      params.push(cursor.at, cursor.matchId, cursor.playerId);
      where.push(`(mp.entry_datetime, mp.match_id, mp.player_id)
        < ($${params.length - 2}::TIMESTAMPTZ, $${params.length - 1}::BIGINT, $${params.length}::BIGINT)`);
    }
    params.push(limit + 1);

    const rows = await query<any>(
      `SELECT
        mp.player_id,
        COALESCE(${DISPLAY_NAME_SQL}, NULLIF(mp.player_name, ''), 'Player ' || mp.player_id::TEXT) AS player_name,
        mp.match_id,
        mp.entry_datetime,
        mp.champion_id,
        c.name AS champion_name,
        ${championRoleSql('c')} AS class_name,
        ROUND(mp.egpm::NUMERIC, 2)::DOUBLE PRECISION AS egpm,
        mp.win_status,
        m.map,
        m.region,
        m.duration_seconds,
        COALESCE(m.recovered, false) AS recovered
      FROM match_players mp
      JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
      LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
      JOIN champions c ON c.id = mp.champion_id
      LEFT JOIN players p ON p.id = mp.player_id
      LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      WHERE ${where.join(' AND ')}
      ORDER BY mp.entry_datetime DESC, mp.match_id DESC, mp.player_id DESC
      LIMIT $${params.length}`,
      params,
    );

    const hasMore = rows.length > limit;
    const data = hasMore ? rows.slice(0, limit) : rows;
    const countParams: any[] = [queueId];
    const possible = ECPM_CANDIDATE_BRACKETS['possible-disconnect'];
    const disconnected = ECPM_CANDIDATE_BRACKETS.disconnected;
    const partial = ECPM_CANDIDATE_BRACKETS['partial-afk'];
    const full = ECPM_CANDIDATE_BRACKETS['full-afk'];
    let countRow: any;
    if (lobbyTier.active) {
      const countWhere = ["smh.queue_id = $1", "smh.role_id = 0", "smh.metric = 'egpm'"];
      appendLobbyTierPredicate(lobbyTier, countParams, countWhere, 'smh');
      countRow = await one<any>(
        `SELECT
          COALESCE(SUM(smh.sample_count), 0)::BIGINT AS total,
          COALESCE(SUM(smh.sample_count) FILTER (WHERE smh.value >= ${possible.minimum} AND smh.value < ${possible.maximum}), 0)::BIGINT AS possible_disconnect,
          COALESCE(SUM(smh.sample_count) FILTER (WHERE smh.value >= ${disconnected.minimum} AND smh.value < ${disconnected.maximum}), 0)::BIGINT AS disconnected,
          COALESCE(SUM(smh.sample_count) FILTER (WHERE smh.value >= ${partial.minimum} AND smh.value < ${partial.maximum}), 0)::BIGINT AS partial_afk,
          COALESCE(SUM(smh.sample_count) FILTER (WHERE smh.value >= ${full.minimum} AND smh.value < ${full.maximum}), 0)::BIGINT AS full_afk
        FROM stats_metric_histogram smh WHERE ${countWhere.join(' AND ')}`,
        countParams,
      );
    } else {
      countRow = await one<any>(
        `SELECT
          COALESCE(SUM(pmh.sample_count), 0)::BIGINT AS total,
          COALESCE(SUM(pmh.sample_count) FILTER (WHERE pmh.value >= ${possible.minimum} AND pmh.value < ${possible.maximum}), 0)::BIGINT AS possible_disconnect,
          COALESCE(SUM(pmh.sample_count) FILTER (WHERE pmh.value >= ${disconnected.minimum} AND pmh.value < ${disconnected.maximum}), 0)::BIGINT AS disconnected,
          COALESCE(SUM(pmh.sample_count) FILTER (WHERE pmh.value >= ${partial.minimum} AND pmh.value < ${partial.maximum}), 0)::BIGINT AS partial_afk,
          COALESCE(SUM(pmh.sample_count) FILTER (WHERE pmh.value >= ${full.minimum} AND pmh.value < ${full.maximum}), 0)::BIGINT AS full_afk
        FROM performance_metric_histogram pmh
        WHERE pmh.queue_id = $1 AND pmh.role_id = 0 AND pmh.metric = 'egpm'`,
        countParams,
      );
    }
    const total = Number(countRow?.total ?? 0);
    const countFor = (key: string) => {
      const count = Number(countRow?.[key] ?? 0);
      return { count, percentage: total > 0 ? Math.round((count / total) * 10_000) / 100 : 0 };
    };
    reply.header('Cache-Control', 'private, no-store');
    return {
      data,
      next_cursor: hasMore ? encodeEcpmCandidateCursor(data[data.length - 1]) : null,
      bracket,
      range: { minimum: bracketDefinition.minimum, maximumExclusive: bracketDefinition.maximum },
      automatic_flag: bracketDefinition.automaticFlag,
      sample_size: total,
      bracket_counts: {
        'possible-disconnect': countFor('possible_disconnect'),
        disconnected: countFor('disconnected'),
        'partial-afk': countFor('partial_afk'),
        'full-afk': countFor('full_afk'),
      },
    };
  });

  /**
   * GET /stats/charts — Global match stats over time.
   *
   * Query params:
   *   ?from=    — ISO 8601 start date
   *   ?to=      — ISO 8601 end date
   *   ?limit=   — Max results (default: 90, max: 365)
   *
   * Returns: Array of { entry_date, avg_kills, avg_deaths, avg_assists, avg_dpm, avg_hpm, total_matches }
   */
  fastify.get('/charts', async (req: any, reply: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 90, 365);
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    if (lobbyTier.active) {
      const params: any[] = [];
      const where = ['m.queue_id = 486'];
      if (req.query.from) { params.push(new Date(req.query.from)); where.push(`m.entry_datetime >= $${params.length}`); }
      if (req.query.to) { params.push(new Date(req.query.to)); where.push(`m.entry_datetime <= $${params.length}`); }
      appendLobbyTierPredicate(lobbyTier, params, where);
      params.push(limit);
      return query(`SELECT m.entry_datetime::DATE AS entry_date,
          ROUND(AVG(mp.kills)::NUMERIC, 2) AS avg_kills,
          ROUND(AVG(mp.deaths)::NUMERIC, 2) AS avg_deaths,
          ROUND(AVG(mp.assists)::NUMERIC, 2) AS avg_assists,
          ROUND(AVG(mp.damage_per_minute)::NUMERIC, 2) AS avg_dpm,
          ROUND(AVG(mp.healing_per_minute)::NUMERIC, 2) AS avg_hpm,
          COUNT(DISTINCT m.match_id)::INT AS total_matches
        FROM matches m JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        JOIN match_players mp ON mp.match_id = m.match_id AND mp.entry_datetime = m.entry_datetime
        WHERE ${where.join(' AND ')} GROUP BY m.entry_datetime::DATE ORDER BY entry_date DESC LIMIT $${params.length}`, params);
    }
    const fb = new FilterBuilder();
    if (req.query.from) fb.gte('entry_date', new Date(req.query.from));
    if (req.query.to) fb.lte('entry_date', new Date(req.query.to));

    const { clause, params } = fb.build();
    const stats = await query(
      `SELECT entry_date, avg_kills, avg_deaths, avg_assists, avg_dpm, avg_hpm, total_matches
       FROM global_match_stats${clause} ORDER BY entry_date DESC LIMIT $${params.length + 1}`,
      [...params, limit]
    );
    return stats;
  });

 /**
   * GET /stats/champions — Champion ranked stats (from consolidated champion_stats_ranked table).
   *
   * Reads directly from the single consolidated table that holds both player stats and ban data.
   * Updated incrementally on every ranked match ingest for real-time accuracy without JOINs.
   *
   * Query params:
   *   ?sort=    — "win_rate", "total_matches", "avg_kills", "ban_rate", "kda" (default: win_rate)
   *   ?order=   — "asc" or "desc" (default: desc)
   *   ?limit=   — Max results (default: 50, max: 200)
   *
   * Returns: Array of { champion_id, champion_name, total_plays, wins, losses,
   *   win_rate, pick_rate, ban_rate, ban_total, kda, avg_kills, avg_deaths, avg_assists,
   *   avg_damage, avg_gold, avg_heal, avg_mitigation, avg_league_tier }
   *
   * Source: Updated 2026-06-03 to read from consolidated champion_stats_ranked table
   *   (stats + bans merged), eliminating LEFT JOIN with former bans_ranked table.
   */
  fastify.get('/champions', async (req: any, reply: any) => {
    // Map user-facing sort keys to column names; whitelist safe values for ORDER BY interpolation
    const sort = req.query.sort === 'avg_kills' ? 'avg_kills' : req.query.sort === 'ban_rate' ? 'ban_rate' : req.query.sort === 'kda' ? 'kda' : 'win_rate';
    const order = req.query.order === 'asc' ? 'ASC' : 'DESC';
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const statsScope = String(req.query.scope || 'ranked').trim().toLowerCase();
    if (!isPublicStatsScope(statsScope)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid statistics scope.'));
    }
    if (statsScope !== 'ranked') {
      const params: any[] = [statsScope, limit];
      const queueId = req.query.queueId == null ? null : parseInt(String(req.query.queueId), 10);
      let queueFilter = '';
      if (queueId != null) {
        if (!Number.isInteger(queueId) || queueId <= 0) {
          return reply.status(400).send(err('VALIDATION', 'queueId must be a positive integer.'));
        }
        params.push(queueId);
        queueFilter = `AND n.queue_id = $${params.length}`;
      }
      return query(`WITH rolled AS (
          SELECT n.champion_id,COALESCE(MAX(c.name),'Champion '||n.champion_id::text) AS champion_name,
            SUM(n.plays)::BIGINT AS total_matches,SUM(n.wins)::BIGINT AS wins,SUM(n.losses)::BIGINT AS losses,
            SUM(n.kills_sum)::BIGINT AS sum_kills,SUM(n.deaths_sum)::BIGINT AS sum_deaths,
            SUM(n.assists_sum)::BIGINT AS sum_assists,SUM(n.damage_sum)::BIGINT AS sum_damage,
            SUM(n.credits_sum)::BIGINT AS sum_gold,SUM(n.healing_sum)::BIGINT AS sum_heal,
            SUM(n.mitigation_sum)::BIGINT AS sum_mitigation
          FROM nonranked_champion_stats_daily n LEFT JOIN champions c ON c.id=n.champion_id
          WHERE n.stats_scope=$1 ${queueFilter}
          GROUP BY n.champion_id
        ), rated AS (
          SELECT *,
            COALESCE(ROUND(100.0*wins::numeric/NULLIF((wins+losses)::numeric,0),2),0) AS win_rate,
            COALESCE(ROUND(total_matches::numeric/NULLIF(SUM(total_matches) OVER(),0),4),0) AS pick_rate,
            ROUND((sum_kills+sum_assists/2.0)::numeric/GREATEST(sum_deaths,1),2) AS kda
          FROM rolled
        )
        SELECT champion_id,champion_name,total_matches,wins,losses,win_rate,pick_rate,
          NULL::numeric AS ban_rate,NULL::bigint AS ban_total,kda,
          ROUND(sum_kills::numeric/NULLIF(total_matches,0),2) AS avg_kills,
          ROUND(sum_deaths::numeric/NULLIF(total_matches,0),2) AS avg_deaths,
          ROUND(sum_assists::numeric/NULLIF(total_matches,0),2) AS avg_assists,
          ROUND(sum_damage::numeric/NULLIF(total_matches,0),2) AS avg_damage,
          ROUND(sum_gold::numeric/NULLIF(total_matches,0),2) AS avg_gold,
          ROUND(sum_heal::numeric/NULLIF(total_matches,0),2) AS avg_heal,
          ROUND(sum_mitigation::numeric/NULLIF(total_matches,0),2) AS avg_mitigation,
          NULL::numeric AS avg_league_tier
        FROM rated ORDER BY ${sort} ${order} LIMIT $2`, params);
    }
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    if (lobbyTier.active) {
      const params: any[] = [];
      const playerWhere = ['spa.queue_id = 486'];
      const banWhere = ['sba.queue_id = 486'];
      if (lobbyTier.min != null) {
        params.push(lobbyTier.min);
        playerWhere.push(`spa.lobby_tier >= $${params.length}`);
        banWhere.push(`sba.lobby_tier >= $${params.length}`);
      }
      if (lobbyTier.max != null) {
        params.push(lobbyTier.max);
        playerWhere.push(`spa.lobby_tier <= $${params.length}`);
        banWhere.push(`sba.lobby_tier <= $${params.length}`);
      }
      params.push(limit);
      return query(`WITH player_agg AS (
          SELECT spa.champion_id,MAX(c.name) AS champion_name,SUM(spa.plays)::BIGINT AS total_matches,
            SUM(spa.wins)::BIGINT AS wins,SUM(spa.losses)::BIGINT AS losses,
            SUM(spa.kills_sum)::BIGINT AS sum_kills,SUM(spa.deaths_sum)::BIGINT AS sum_deaths,
            SUM(spa.assists_sum)::BIGINT AS sum_assists,SUM(spa.damage_sum)::BIGINT AS sum_damage,
            SUM(spa.gold_sum)::BIGINT AS sum_gold,SUM(spa.healing_sum)::BIGINT AS sum_heal,
            SUM(spa.mitigation_sum)::BIGINT AS sum_mitigation,
            SUM(spa.lobby_tier::BIGINT*spa.plays)::BIGINT AS sum_league_tier
          FROM stats_player_aggregate spa JOIN champions c ON c.id=spa.champion_id
          WHERE ${playerWhere.join(' AND ')} GROUP BY spa.champion_id
        ), ban_agg AS (
          SELECT champion_id,SUM(bans)::BIGINT AS ban_total FROM stats_ban_aggregate sba
          WHERE ${banWhere.join(' AND ')} GROUP BY champion_id
        ), merged AS (
          SELECT p.*,COALESCE(b.ban_total,0)::BIGINT AS ban_total FROM player_agg p
          LEFT JOIN ban_agg b ON b.champion_id=p.champion_id
        ), rated AS (
          SELECT *,ROUND(100.0*wins::NUMERIC/NULLIF((wins+losses)::NUMERIC,0),2) AS win_rate,
            ROUND(total_matches::NUMERIC/NULLIF(SUM(total_matches) OVER(),0),4) AS pick_rate,
            ROUND(ban_total::NUMERIC/NULLIF(SUM(ban_total) OVER(),0),4) AS ban_rate,
            ROUND((sum_kills+sum_assists/2.0)::NUMERIC/GREATEST(sum_deaths,1),2) AS kda
          FROM merged
        ) SELECT champion_id,champion_name,total_matches,wins,losses,win_rate,pick_rate,ban_rate,ban_total,kda,
          ROUND(sum_kills::NUMERIC/NULLIF(total_matches,0),2) AS avg_kills,
          ROUND(sum_deaths::NUMERIC/NULLIF(total_matches,0),2) AS avg_deaths,
          ROUND(sum_assists::NUMERIC/NULLIF(total_matches,0),2) AS avg_assists,
          ROUND(sum_damage::NUMERIC/NULLIF(total_matches,0),2) AS avg_damage,
          ROUND(sum_gold::NUMERIC/NULLIF(total_matches,0),2) AS avg_gold,
          ROUND(sum_heal::NUMERIC/NULLIF(total_matches,0),2) AS avg_heal,
          ROUND(sum_mitigation::NUMERIC/NULLIF(total_matches,0),2) AS avg_mitigation,
          ROUND(sum_league_tier::NUMERIC/NULLIF(total_matches,0),2) AS avg_league_tier
        FROM rated ORDER BY ${sort} ${order} LIMIT $${params.length}`,params);

      const where = ['m.queue_id = 486', 'mp.champion_id > 0', `COALESCE(mp.source, 'direct') IN ('direct', 'recovered')`];
      appendLobbyTierPredicate(lobbyTier, params, where);
      params.push(limit);
      return query(`
        WITH player_agg AS (
          SELECT mp.champion_id, COALESCE(c.name, 'Champion ' || mp.champion_id::TEXT) AS champion_name,
            COUNT(*)::INT AS total_matches,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses,
            COALESCE(SUM(mp.kills), 0)::BIGINT AS sum_kills,
            COALESCE(SUM(mp.deaths), 0)::BIGINT AS sum_deaths,
            COALESCE(SUM(mp.assists), 0)::BIGINT AS sum_assists,
            COALESCE(SUM(mp.damage_done_physical), 0)::BIGINT AS sum_damage,
            COALESCE(SUM(mp.gold_earned), 0)::BIGINT AS sum_gold,
            COALESCE(SUM(mp.healing), 0)::BIGINT AS sum_heal,
            COALESCE(SUM(mp.damage_mitigated), 0)::BIGINT AS sum_mitigation,
            COALESCE(SUM(mp.league_tier) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26), 0)::BIGINT AS sum_league_tier,
            COUNT(*) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)::INT AS league_tier_count
          FROM match_players mp
          JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          LEFT JOIN champions c ON c.id = mp.champion_id
          WHERE ${where.join(' AND ')}
          GROUP BY mp.champion_id, COALESCE(c.name, 'Champion ' || mp.champion_id::TEXT)
        ), ban_agg AS (
          SELECT mb.champion_id, COUNT(*)::INT AS ban_total
          FROM match_bans mb
          JOIN matches m ON m.match_id = mb.match_id
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          WHERE m.queue_id = 486${where.slice(3).length ? ` AND ${where.slice(3).join(' AND ')}` : ''}
            AND mb.champion_id > 0
          GROUP BY mb.champion_id
        ), merged AS (
          SELECT p.*, COALESCE(b.ban_total, 0)::INT AS ban_total
          FROM player_agg p LEFT JOIN ban_agg b ON b.champion_id = p.champion_id
        ), rated AS (
          SELECT *,
            ROUND(100.0 * wins::NUMERIC / NULLIF((wins + losses)::NUMERIC, 0), 2) AS win_rate,
            ROUND(total_matches::NUMERIC / NULLIF(SUM(total_matches) OVER (), 0), 4) AS pick_rate,
            ROUND(ban_total::NUMERIC / NULLIF(SUM(ban_total) OVER (), 0), 4) AS ban_rate,
            ROUND((sum_kills + sum_assists / 2.0)::NUMERIC / GREATEST(sum_deaths, 1), 2) AS kda
          FROM merged
        )
        SELECT champion_id, champion_name, total_matches, wins, losses, win_rate, pick_rate, ban_rate, ban_total, kda,
          ROUND(sum_kills::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_kills,
          ROUND(sum_deaths::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_deaths,
          ROUND(sum_assists::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_assists,
          ROUND(sum_damage::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_damage,
          ROUND(sum_gold::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_gold,
          ROUND(sum_heal::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_heal,
          ROUND(sum_mitigation::NUMERIC / NULLIF(total_matches, 0), 2) AS avg_mitigation,
          ROUND(sum_league_tier::NUMERIC / NULLIF(league_tier_count, 0), 2) AS avg_league_tier
        FROM rated ORDER BY ${sort} ${order} LIMIT $${params.length}
      `, params);
    }

    // Read directly from consolidated table — no JOINs needed; averages computed inline from sum columns / total_matches.
    return query(`
      SELECT champion_id, champion_name, total_matches, wins, losses, win_rate, pick_rate, ban_rate, ban_total, kda,
        CASE WHEN total_matches > 0 THEN ROUND(sum_kills::NUMERIC / total_matches, 2) ELSE NULL END AS avg_kills,
        CASE WHEN total_matches > 0 THEN ROUND(sum_deaths::NUMERIC / total_matches, 2) ELSE NULL END AS avg_deaths,
        CASE WHEN total_matches > 0 THEN ROUND(sum_assists::NUMERIC / total_matches, 2) ELSE NULL END AS avg_assists,
        CASE WHEN total_matches > 0 THEN ROUND(sum_damage::NUMERIC / total_matches, 2) ELSE NULL END AS avg_damage,
        CASE WHEN total_matches > 0 THEN ROUND(sum_gold::NUMERIC / total_matches, 2) ELSE NULL END AS avg_gold,
        CASE WHEN total_matches > 0 THEN ROUND(sum_heal::NUMERIC / total_matches, 2) ELSE NULL END AS avg_heal,
        CASE WHEN total_matches > 0 THEN ROUND(sum_mitigation::NUMERIC / total_matches, 2) ELSE NULL END AS avg_mitigation,
        CASE WHEN league_tier_count > 0 THEN ROUND(sum_league_tier::NUMERIC / league_tier_count, 2) ELSE NULL END AS avg_league_tier
      FROM champion_stats_ranked ORDER BY ${sort} ${order} LIMIT $1`, [limit]);
  });

  /**
   * GET /stats/presence — Rolling 24-hour unique observed population.
   *
   * The discovery ledger plus persisted match-player facts are authoritative
   * for public identities, matching /presence/players exactly. Presence cache
   * tables remain worker/enrichment projections and cannot define public
   * counts because a fresh deployment may not contain a complete 24-hour
   * history. Unresolved private observations are reported separately and are
   * never guessed into the unique-person total.
   */
  fastify.get('/presence', async () => {
    const [
      publicPresence,
      privateTotals,
      privateByScope,
      unresolvedByScope,
    ] = await Promise.all([
      one(
        `WITH ${PUBLIC_PLAYER_EVIDENCE_CTES_SQL},
         public_identities AS MATERIALIZED (
           SELECT DISTINCT player_id
           FROM participation
         ),
         resolved_identities AS MATERIALIZED (
           SELECT
             identity.player_id,
             resolved_profile.platform,
             resolved_profile.region,
             resolved_profile.hirez_profile_refreshed_at
           FROM public_identities identity
           LEFT JOIN LATERAL (
             SELECT
               candidate.platform,
               candidate.region,
               candidate.hirez_profile_refreshed_at
             FROM (
               SELECT
                 profile.platform,
                 profile.region,
                 profile.hirez_profile_refreshed_at,
                 0 AS identity_priority
               FROM players profile
               WHERE profile.id = identity.player_id
               UNION ALL
               SELECT
                 profile.platform,
                 profile.region,
                 profile.hirez_profile_refreshed_at,
                 1 AS identity_priority
               FROM players profile
               WHERE profile.active_player_id = identity.player_id
                 AND profile.active_player_id > 0
                 AND profile.id <> identity.player_id
             ) candidate
             ORDER BY
               candidate.identity_priority,
               candidate.hirez_profile_refreshed_at DESC NULLS LAST
             LIMIT 1
           ) resolved_profile ON TRUE
         )
         SELECT
           (SELECT COUNT(*)::int FROM public_identities) AS public_players,
           (
             SELECT COUNT(*) FILTER (WHERE unresolved_slots_upper > 0)::int
             FROM match_uncertainty
           ) AS unresolved_matches,
           (
             SELECT COALESCE(SUM(unresolved_slots_upper), 0)::int
             FROM match_uncertainty
           ) AS unresolved_player_slots_upper,
           COALESCE((
             SELECT jsonb_agg(
               jsonb_build_object(
                 'stats_scope', scope_count.stats_scope,
                 'players', scope_count.players
               )
               ORDER BY scope_count.stats_scope
             )
             FROM (
               SELECT stats_scope, COUNT(DISTINCT player_id)::int AS players
               FROM participation
               GROUP BY stats_scope
             ) scope_count
           ), '[]'::jsonb) AS public_by_scope,
           COALESCE((
             SELECT jsonb_agg(
               jsonb_build_object(
                 'queue_id', queue_count.queue_id,
                 'queue_name', queue_count.queue_name,
                 'stats_scope', queue_count.stats_scope,
                 'players', queue_count.players
               )
               ORDER BY queue_count.players DESC, queue_count.queue_id
             )
             FROM (
               SELECT
                 queue_id,
                 MAX(queue_name) AS queue_name,
                 MAX(stats_scope) AS stats_scope,
                 COUNT(DISTINCT player_id)::int AS players
               FROM participation
               GROUP BY queue_id
             ) queue_count
           ), '[]'::jsonb) AS public_by_queue,
           COALESCE((
             SELECT jsonb_agg(
               jsonb_build_object(
                 'platform', platform_count.platform,
                 'players', platform_count.players
               )
               ORDER BY platform_count.players DESC, platform_count.platform
             )
             FROM (
               SELECT
                 COALESCE(NULLIF(BTRIM(platform), ''), 'Unknown') AS platform,
                 COUNT(*)::int AS players
               FROM resolved_identities
               GROUP BY 1
             ) platform_count
           ), '[]'::jsonb) AS public_by_platform,
           COALESCE((
             SELECT jsonb_agg(
               jsonb_build_object(
                 'region', region_count.region,
                 'players', region_count.players
               )
               ORDER BY region_count.players DESC, region_count.region
             )
             FROM (
               SELECT
                 COALESCE(NULLIF(BTRIM(region), ''), 'Unknown') AS region,
                 COUNT(*)::int AS players
               FROM resolved_identities
               GROUP BY 1
             ) region_count
           ), '[]'::jsonb) AS public_by_region,
           (SELECT COUNT(*)::int FROM resolved_identities) AS profile_total,
           (
             SELECT COUNT(*) FILTER (
               WHERE hirez_profile_refreshed_at >= now() - interval '24 hours'
             )::int
             FROM resolved_identities
           ) AS profile_fresh,
           (
             SELECT COUNT(*) FILTER (
               WHERE NULLIF(BTRIM(platform), '') IS NOT NULL
             )::int
             FROM resolved_identities
           ) AS platform_known,
           (
             SELECT COUNT(*) FILTER (
               WHERE NULLIF(BTRIM(platform), '') IS NULL
             )::int
             FROM resolved_identities
           ) AS platform_unknown,
           (
             SELECT MAX(last_attempt_at)::text
             FROM player_activity_profile_refresh
           ) AS last_enrichment_at`,
        [null],
      ),
      one(
        `SELECT
           (SELECT COUNT(*)::int
            FROM private_player_presence_24h
            WHERE last_observed_at >= now() - interval '24 hours') AS private_players,
           (SELECT COUNT(*)::int
            FROM unresolved_private_presence
            WHERE observed_at >= now() - interval '24 hours') AS unresolved_private_observations`,
      ),
      query(
        `SELECT last_stats_scope AS stats_scope, COUNT(*)::int AS players
         FROM private_player_presence_24h
         WHERE last_observed_at >= now() - interval '24 hours'
         GROUP BY last_stats_scope
         ORDER BY last_stats_scope`,
      ),
      query(
        `SELECT stats_scope, COUNT(*)::int AS observations
         FROM unresolved_private_presence
         WHERE observed_at >= now() - interval '24 hours'
         GROUP BY stats_scope
         ORDER BY stats_scope`,
      ),
    ]);

    return {
      window_hours: 24,
      observed_at: new Date().toISOString(),
      public_players: Number(publicPresence?.public_players ?? 0),
      unresolved_player_slots_lower: 0,
      unresolved_player_slots_upper: Number(
        publicPresence?.unresolved_player_slots_upper ?? 0,
      ),
      unresolved_matches: Number(publicPresence?.unresolved_matches ?? 0),
      public_players_lower_bound: Number(publicPresence?.public_players ?? 0),
      public_players_upper_bound: Number(publicPresence?.public_players ?? 0)
        + Number(publicPresence?.unresolved_player_slots_upper ?? 0),
      private_players: Number(privateTotals?.private_players ?? 0),
      unresolved_private_observations: Number(
        privateTotals?.unresolved_private_observations ?? 0,
      ),
      public_by_scope: Array.isArray(publicPresence?.public_by_scope)
        ? publicPresence.public_by_scope
        : [],
      private_by_scope: privateByScope,
      unresolved_by_scope: unresolvedByScope,
      public_by_queue: Array.isArray(publicPresence?.public_by_queue)
        ? publicPresence.public_by_queue
        : [],
      public_by_platform: Array.isArray(publicPresence?.public_by_platform)
        ? publicPresence.public_by_platform
        : [],
      public_by_region: Array.isArray(publicPresence?.public_by_region)
        ? publicPresence.public_by_region
        : [],
      profile_coverage: {
        total: Number(publicPresence?.profile_total ?? 0),
        fresh: Number(publicPresence?.profile_fresh ?? 0),
        platform_known: Number(publicPresence?.platform_known ?? 0),
        platform_unknown: Number(publicPresence?.platform_unknown ?? 0),
        last_enrichment_at: publicPresence?.last_enrichment_at ?? null,
      },
    };
  });

  /**
   * GET /stats/presence/match-ids — Compact, text-only match evidence.
   */
  fastify.get('/presence/match-ids', async (req: any, reply: any) => {
    const perPage = parsePresenceEvidenceLimit(req.query?.per_page ?? req.query?.limit);
    const page = parsePresenceEvidencePage(req.query?.page);
    const offset = (page - 1) * perPage;

    let queueId: number | null = null;
    if (req.query?.queue_id != null && req.query.queue_id !== '') {
      queueId = parsePresenceDetailQueueId(req.query.queue_id);
      if (queueId == null) {
        return reply.status(400).send(err('VALIDATION', 'queue_id must be a valid PostgreSQL integer.'));
      }
    }

    const [trackedTotal, queueOptions, rows] = await Promise.all([
      one(
        `SELECT COUNT(*)::int AS total
         FROM match_count_discoveries d
         JOIN queue_types q ON q.queue_id = d.queue_id
         WHERE d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date
           AND COALESCE(
                 d.entry_datetime AT TIME ZONE 'UTC',
                 d.source_date + (d.source_hour * interval '1 hour')
               ) >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
           AND q.track_presence = TRUE
           AND ($1::int IS NULL OR d.queue_id = $1::int)`,
        [queueId],
      ),
      query(
        `SELECT d.queue_id, q.queue_name, COUNT(*)::int AS matches
         FROM match_count_discoveries d
         JOIN queue_types q ON q.queue_id = d.queue_id
         WHERE d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date
           AND COALESCE(
                 d.entry_datetime AT TIME ZONE 'UTC',
                 d.source_date + (d.source_hour * interval '1 hour')
               ) >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
           AND q.track_presence = TRUE
         GROUP BY d.queue_id, q.queue_name
         ORDER BY matches DESC, d.queue_id`,
      ),
      query(
        `SELECT
           d.match_id::text, d.queue_id, d.source_date::text, d.source_hour
         FROM match_count_discoveries d
         JOIN queue_types q ON q.queue_id = d.queue_id
         WHERE d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date
           AND COALESCE(
                 d.entry_datetime AT TIME ZONE 'UTC',
                 d.source_date + (d.source_hour * interval '1 hour')
               ) >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
           AND q.track_presence = TRUE
           AND ($1::int IS NULL OR d.queue_id = $1::int)
         ORDER BY d.source_date DESC, d.source_hour DESC,
                  d.match_id DESC, d.queue_id DESC
         LIMIT $2::int
         OFFSET $3::int`,
        [queueId, perPage, offset],
      ),
    ]);

    const totalMatches = Number(trackedTotal?.total ?? 0);
    return {
      window_hours: 24,
      observed_at: new Date().toISOString(),
      total_matches: totalMatches,
      selected_queue_id: queueId,
      page: {
        current: page,
        size: perPage,
        total_pages: Math.ceil(totalMatches / perPage),
      },
      queues: queueOptions.map(row => ({
        queue_id: Number(row.queue_id),
        queue_name: String(row.queue_name),
        matches: Number(row.matches),
      })),
      match_ids: rows.map(row => ({
        match_id: String(row.match_id),
        queue_id: Number(row.queue_id),
      })),
    };
  });

  /**
   * GET /stats/presence/players — Compact public-player evidence with each
   * identity's distinct match participation in the same rolling window.
   * The discovery ledger and persisted match-player facts are the authority
   * for both membership and counts, so every returned identity has at least
   * one reconstructable match.
   */
  fastify.get('/presence/players', async (req: any, reply: any) => {
    const perPage = parsePresenceEvidenceLimit(req.query?.per_page ?? req.query?.limit);
    const page = parsePresenceEvidencePage(req.query?.page);
    const sort = parsePresencePlayerSort(req.query?.sort);
    const offset = (page - 1) * perPage;

    let queueId: number | null = null;
    if (req.query?.queue_id != null && req.query.queue_id !== '') {
      queueId = parsePresenceDetailQueueId(req.query.queue_id);
      if (queueId == null) {
        return reply.status(400).send(err('VALIDATION', 'queue_id must be a valid PostgreSQL integer.'));
      }
    }

    const orderBy = sort === 'alphabetical'
      ? 'LOWER(player_name), player_id'
      : 'matches_played DESC, player_id';

    const rows = await query(
      `WITH ${PUBLIC_PLAYER_EVIDENCE_CTES_SQL},
         participation_counts AS MATERIALIZED (
           SELECT
             player_id,
             COUNT(DISTINCT match_id)::int AS matches_played,
             MAX(observed_name) AS observed_name
           FROM participation
           GROUP BY player_id
         ),
         player_rows AS MATERIALIZED (
           SELECT
             participation_counts.player_id::text AS player_id,
             COALESCE(
               NULLIF(BTRIM(resolved_profile.name), ''),
               participation_counts.observed_name,
               'Player #' || participation_counts.player_id::text
             ) AS player_name,
             participation_counts.matches_played
           FROM participation_counts
           LEFT JOIN LATERAL (
             SELECT candidate.name
             FROM (
               SELECT profile.name, profile.hirez_profile_refreshed_at,
                      0 AS identity_priority
               FROM players profile
               WHERE profile.id = participation_counts.player_id
               UNION ALL
               SELECT profile.name, profile.hirez_profile_refreshed_at,
                      1 AS identity_priority
               FROM players profile
               WHERE profile.active_player_id = participation_counts.player_id
                 AND profile.active_player_id > 0
                 AND profile.id <> participation_counts.player_id
             ) candidate
             ORDER BY candidate.identity_priority,
                      candidate.hirez_profile_refreshed_at DESC NULLS LAST
             LIMIT 1
           ) resolved_profile ON TRUE
         ),
         evidence_summary AS MATERIALIZED (
           SELECT
             (SELECT COUNT(*) FROM recent_discoveries)::int AS total_matches,
             (SELECT COUNT(DISTINCT match_id) FROM participation)::int AS represented_matches,
             (SELECT COUNT(*) FROM participation_counts)::int AS total_players,
             (
               SELECT COUNT(*) FILTER (WHERE unresolved_slots_upper > 0)::int
               FROM match_uncertainty
             ) AS unresolved_matches,
             (
               SELECT COALESCE(SUM(unresolved_slots_upper), 0)::int
               FROM match_uncertainty
             ) AS unresolved_player_slots_upper,
             COALESCE((SELECT SUM(matches_played) FROM participation_counts), 0)::int
               AS total_participations
         ),
         paged AS MATERIALIZED (
           SELECT
             player_id, player_name, matches_played,
             ROW_NUMBER() OVER (ORDER BY ${orderBy})::int AS page_order
           FROM player_rows
           ORDER BY ${orderBy}
           LIMIT $2::int
           OFFSET $3::int
         )
         SELECT
           paged.player_id,
           paged.player_name,
           paged.matches_played,
           summary.total_matches,
           summary.represented_matches,
           summary.total_players,
           summary.unresolved_matches,
           summary.unresolved_player_slots_upper,
           summary.total_participations
         FROM evidence_summary summary
         LEFT JOIN paged ON TRUE
         ORDER BY paged.page_order`,
      [queueId, perPage, offset],
    );

    const summary = rows[0] ?? {};
    const totalMatches = Number(summary.total_matches ?? 0);
    const representedMatches = Number(summary.represented_matches ?? 0);
    const totalPlayers = Number(summary.total_players ?? 0);
    const unresolvedMatches = Number(summary.unresolved_matches ?? 0);
    const unresolvedPlayerSlotsUpper = Number(summary.unresolved_player_slots_upper ?? 0);
    const totalParticipations = Number(summary.total_participations ?? 0);
    return {
      window_hours: 24,
      observed_at: new Date().toISOString(),
      total_players: totalPlayers,
      unresolved_player_slots_lower: 0,
      unresolved_player_slots_upper: unresolvedPlayerSlotsUpper,
      unresolved_matches: unresolvedMatches,
      public_players_lower_bound: totalPlayers,
      public_players_upper_bound: totalPlayers + unresolvedPlayerSlotsUpper,
      total_matches: totalMatches,
      represented_matches: representedMatches,
      unrepresented_matches: Math.max(0, totalMatches - representedMatches),
      total_participations: totalParticipations,
      selected_queue_id: queueId,
      sort,
      page: {
        current: page,
        size: perPage,
        total_pages: Math.ceil(totalPlayers / perPage),
      },
      players: rows.filter(row => row.player_id != null).map(row => ({
        player_id: String(row.player_id),
        player_name: String(row.player_name),
        matches_played: Number(row.matches_played),
      })),
    };
  });

  /**
   * GET /stats/presence/details — Inspectable evidence behind the rolling
   * active-player count.
   *
   * The discovery ledger is the match authority for this page. Cursor
   * pagination keeps the full rolling window reachable without returning
   * tens of thousands of matches in one response. Player rows remain facts
   * scoped to their match, so one player can appear in every queue they
   * actually played while the headline count remains globally deduplicated.
   */
  fastify.get('/presence/details', async (req: any, reply: any) => {
    const limit = parsePresenceDetailLimit(req.query?.limit);
    const cursor = decodePresenceDetailCursor(req.query?.cursor);
    if (req.query?.cursor && !cursor) {
      return reply.status(400).send(err('VALIDATION', 'Invalid presence-detail cursor.'));
    }

    let queueId: number | null = null;
    if (req.query?.queue_id != null && req.query.queue_id !== '') {
      queueId = parsePresenceDetailQueueId(req.query.queue_id);
      if (queueId == null) {
        return reply.status(400).send(err('VALIDATION', 'queue_id must be a non-negative integer.'));
      }
    }

    const params: unknown[] = [];
    const addParam = (value: unknown): string => {
      params.push(value);
      return `$${params.length}`;
    };
    const predicates = [
      `d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date`,
      `COALESCE(
         d.entry_datetime AT TIME ZONE 'UTC',
         d.source_date + (d.source_hour * interval '1 hour')
       ) >= (now() AT TIME ZONE 'UTC') - interval '24 hours'`,
      'q.track_presence = TRUE',
    ];
    if (queueId != null) {
      predicates.push(`d.queue_id = ${addParam(queueId)}::int`);
    }
    if (cursor) {
      const dateParam = addParam(cursor.date);
      const hourParam = addParam(cursor.hour);
      const matchParam = addParam(cursor.matchId);
      const queueParam = addParam(cursor.queueId);
      predicates.push(
        `(d.source_date, d.source_hour, d.match_id, d.queue_id)
          < (${dateParam}::date, ${hourParam}::int, ${matchParam}::bigint, ${queueParam}::int)`,
      );
    }
    const where = `WHERE ${predicates.join(' AND ')}`;
    const pageLimitParam = addParam(limit + 1);

    const [trackedTotal, queueOptions, rawMatches] = await Promise.all([
      one(
        `SELECT COUNT(*)::int AS total
         FROM match_count_discoveries d
         JOIN queue_types q ON q.queue_id = d.queue_id
         WHERE d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date
           AND COALESCE(
                 d.entry_datetime AT TIME ZONE 'UTC',
                 d.source_date + (d.source_hour * interval '1 hour')
               ) >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
           AND q.track_presence = TRUE
           AND ($1::int IS NULL OR d.queue_id = $1::int)`,
        [queueId],
      ),
      query(
        `SELECT d.queue_id, q.queue_name, q.stats_scope, COUNT(*)::int AS matches
         FROM match_count_discoveries d
         JOIN queue_types q ON q.queue_id = d.queue_id
         WHERE d.source_date >= ((now() AT TIME ZONE 'UTC') - interval '25 hours')::date
           AND COALESCE(
                 d.entry_datetime AT TIME ZONE 'UTC',
                 d.source_date + (d.source_hour * interval '1 hour')
               ) >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
           AND q.track_presence = TRUE
         GROUP BY d.queue_id, q.queue_name, q.stats_scope
         ORDER BY matches DESC, d.queue_id`,
      ),
      query(
        `WITH page AS MATERIALIZED (
           SELECT
             d.match_id, d.queue_id, d.region, d.entry_datetime,
             d.source_date, d.source_hour, q.queue_name, q.stats_scope
           FROM match_count_discoveries d
           JOIN queue_types q ON q.queue_id = d.queue_id
           ${where}
           ORDER BY d.source_date DESC, d.source_hour DESC, d.match_id DESC, d.queue_id DESC
           LIMIT ${pageLimitParam}::int
         )
         SELECT
           page.match_id::text,
           page.queue_id,
           page.queue_name,
           page.stats_scope,
           page.source_date::text,
           page.source_hour,
           COALESCE(
             page.entry_datetime,
             ranked.entry_datetime,
             casual.entry_datetime,
             special.entry_datetime,
             page.source_date + (page.source_hour * interval '1 hour')
           )::text AS entry_datetime,
           COALESCE(NULLIF(ranked.region, ''), NULLIF(casual.region, ''),
                    NULLIF(special.region, ''), page.region, 'Unknown') AS region,
           COALESCE(NULLIF(ranked.map, ''), NULLIF(casual.map, ''),
                    NULLIF(special.map, ''), 'Unknown') AS map,
           CASE
             WHEN page.queue_id = 486 AND ranked.match_id IS NULL THEN 'discovered'
             WHEN page.queue_id = 486 AND ranked.limited THEN 'limited'
             WHEN page.queue_id = 486 AND ranked.recovered THEN 'recovered'
             WHEN page.queue_id = 486 AND ranked.broken THEN 'broken'
             WHEN page.queue_id = 486 THEN 'complete'
             ELSE COALESCE(acquisition.status, 'discovered')
           END AS status,
           CASE
             WHEN page.queue_id = 486 AND ranked.limited THEN 'limited'
             WHEN page.queue_id = 486 AND ranked.broken AND NOT ranked.recovered THEN 'partial'
             WHEN page.queue_id = 486 AND ranked.match_id IS NOT NULL THEN 'complete'
             ELSE COALESCE(acquisition.quality, casual.quality, special.quality, 'unknown')
           END AS quality,
           COALESCE(acquisition.terminal_reason, ranked.limited_reason) AS terminal_reason
         FROM page
         LEFT JOIN nonranked_match_acquisition acquisition
           ON acquisition.match_id = page.match_id
         LEFT JOIN casual_matches casual ON casual.match_id = page.match_id
         LEFT JOIN special_matches special ON special.match_id = page.match_id
         LEFT JOIN LATERAL (
           SELECT
             match.match_id, match.entry_datetime, match.region, match.map,
             match.broken, match.recovered, match.limited, match.limited_reason
           FROM matches match
           WHERE match.match_id = page.match_id
             AND match.entry_datetime >= now() - interval '25 hours'
           ORDER BY match.entry_datetime DESC
           LIMIT 1
         ) ranked ON TRUE
         ORDER BY page.source_date DESC, page.source_hour DESC,
                  page.match_id DESC, page.queue_id DESC`,
        params,
      ),
    ]);

    const hasMore = rawMatches.length > limit;
    const matchRows = rawMatches.slice(0, limit);
    const matchIds = matchRows.map(row => String(row.match_id));
    const playerRows = matchIds.length === 0 ? [] : await query(
      `WITH player_facts AS (
         SELECT
           ranked_fact.match_id, ranked_fact.player_id, ranked_fact.player_name,
           ranked_fact.platform, ranked_fact.participant_kind, ranked_fact.source
         FROM (
           SELECT DISTINCT ON (mp.match_id, mp.player_id, mp.private_slot)
             mp.match_id, mp.player_id, mp.player_name, mp.platform,
             CASE WHEN mp.player_id > 0 THEN 'human' ELSE 'private' END AS participant_kind,
             mp.source, mp.private_slot, mp.entry_datetime
           FROM match_players mp
           WHERE mp.match_id = ANY($1::bigint[])
             AND mp.entry_datetime >= now() - interval '25 hours'
           ORDER BY mp.match_id, mp.player_id, mp.private_slot, mp.entry_datetime DESC
         ) ranked_fact
         UNION ALL
         SELECT
           cmp.match_id, cmp.player_id, cmp.player_name, cmp.platform,
           cmp.participant_kind, cmp.source
         FROM casual_match_players cmp
         WHERE cmp.match_id = ANY($1::bigint[])
         UNION ALL
         SELECT
           smp.match_id, smp.player_id, smp.player_name, smp.platform,
           smp.participant_kind, smp.source
         FROM special_match_players smp
         WHERE smp.match_id = ANY($1::bigint[])
       )
       SELECT
         fact.match_id::text,
         fact.player_id::text,
         CASE
           WHEN fact.participant_kind = 'private' THEN 'Private account'
           WHEN NULLIF(BTRIM(fact.player_name), '') IS NOT NULL THEN BTRIM(fact.player_name)
           WHEN NULLIF(BTRIM(resolved_profile.name), '') IS NOT NULL THEN BTRIM(resolved_profile.name)
           WHEN fact.participant_kind = 'bot' THEN 'Bot'
           ELSE 'Unknown player'
         END AS player_name,
         CASE
           WHEN fact.participant_kind = 'private' THEN 'Private'
           WHEN fact.participant_kind = 'bot' THEN 'Bot'
           ELSE COALESCE(
             NULLIF(BTRIM(resolved_profile.platform), ''),
             NULLIF(BTRIM(fact.platform), ''),
             'Unknown'
           )
         END AS platform,
         fact.participant_kind,
         fact.source
       FROM player_facts fact
       LEFT JOIN LATERAL (
         SELECT candidate.name, candidate.platform
         FROM (
           SELECT profile.name, profile.platform,
                  profile.hirez_profile_refreshed_at, 0 AS identity_priority
           FROM players profile
           WHERE fact.player_id > 0 AND profile.id = fact.player_id
           UNION ALL
           SELECT profile.name, profile.platform,
                  profile.hirez_profile_refreshed_at, 1 AS identity_priority
           FROM players profile
           WHERE fact.player_id > 0
             AND profile.active_player_id = fact.player_id
             AND profile.active_player_id > 0
             AND profile.id <> fact.player_id
         ) candidate
         ORDER BY candidate.identity_priority,
                  candidate.hirez_profile_refreshed_at DESC NULLS LAST
         LIMIT 1
       ) resolved_profile ON TRUE
       ORDER BY fact.match_id DESC, platform, player_name`,
      [matchIds],
    );

    const playersByMatch = new Map<string, any[]>();
    for (const row of playerRows) {
      const matchId = String(row.match_id);
      const players = playersByMatch.get(matchId) ?? [];
      players.push({
        player_id: String(row.player_id),
        player_name: String(row.player_name),
        platform: String(row.platform),
        participant_kind: String(row.participant_kind),
        source: String(row.source ?? 'unknown'),
      });
      playersByMatch.set(matchId, players);
    }

    return {
      window_hours: 24,
      observed_at: new Date().toISOString(),
      total_matches: Number(trackedTotal?.total ?? 0),
      selected_queue_id: queueId,
      queues: queueOptions.map(row => ({
        queue_id: Number(row.queue_id),
        queue_name: String(row.queue_name),
        stats_scope: String(row.stats_scope),
        matches: Number(row.matches),
      })),
      matches: matchRows.map(row => ({
        match_id: String(row.match_id),
        queue_id: Number(row.queue_id),
        queue_name: String(row.queue_name),
        stats_scope: String(row.stats_scope),
        entry_datetime: String(row.entry_datetime),
        region: String(row.region),
        map: String(row.map),
        status: String(row.status),
        quality: String(row.quality),
        terminal_reason: row.terminal_reason == null ? null : String(row.terminal_reason),
        players: playersByMatch.get(String(row.match_id)) ?? [],
      })),
      next_cursor: hasMore
        ? encodePresenceDetailCursor(matchRows[matchRows.length - 1])
        : null,
    };
  });

  /**
   * GET /stats/skins — Ranked skin performance from authoritative match facts.
   *
   * Skin identities are captured during normal ingestion and accumulated in
   * skin_counts_ranked by player lobby tier. The request path rolls up only
   * those compact buckets; it never scans match_players or calls Hi-Rez.
   */
  fastify.get('/skins', async (req: any, reply: any) => {
    const limit = Math.min(Math.max(parseInt(req.query.limit as string, 10) || 50, 1), 200);
    const params: any[] = [];
    const where: string[] = [];

    if (req.query.championId != null) {
      const championId = parseInt(req.query.championId as string, 10);
      if (!Number.isInteger(championId) || championId <= 0) {
        return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
      }
      params.push(championId);
      where.push(`scr.champion_id = $${params.length}`);
    }

    const tierMin = req.query.tierMin == null ? null : parseInt(req.query.tierMin as string, 10);
    const tierMax = req.query.tierMax == null ? null : parseInt(req.query.tierMax as string, 10);
    if ((tierMin != null && (!Number.isInteger(tierMin) || tierMin < 1 || tierMin > 26))
      || (tierMax != null && (!Number.isInteger(tierMax) || tierMax < 1 || tierMax > 26))
      || (tierMin != null && tierMax != null && tierMin > tierMax)) {
      return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    }
    if (tierMin != null) {
      params.push(tierMin);
      where.push(`scr.league_tier >= $${params.length}`);
    }
    if (tierMax != null) {
      params.push(tierMax);
      where.push(`scr.league_tier <= $${params.length}`);
    }

    params.push(limit);
    const rows = await query(
      `SELECT
         scr.skin_id,
         MAX(scr.skin_name) AS skin_name,
         scr.champion_id,
         c.name AS champion_name,
         SUM(scr.count)::INT AS total_plays,
         SUM(scr.wins)::INT AS wins,
         SUM(scr.losses)::INT AS losses,
         COALESCE(
           ROUND(
             100.0 * SUM(scr.wins)::NUMERIC
             / NULLIF((SUM(scr.wins) + SUM(scr.losses))::NUMERIC, 0),
             2
           ),
           0
         )::DOUBLE PRECISION AS win_rate
       FROM skin_counts_ranked scr
       JOIN champions c ON c.id = scr.champion_id
       ${where.length > 0 ? `WHERE ${where.join(' AND ')}` : ''}
       GROUP BY scr.skin_id, scr.champion_id, c.name
       ORDER BY win_rate DESC, total_plays DESC, skin_name ASC
       LIMIT $${params.length}`,
      params,
    );
    return { data: rows, total: rows.length, tier_min: tierMin, tier_max: tierMax };
  });

  /**
   * GET /stats/broken-skins — Skins with known Int16 overflow errors (skin_id > 32767).
   *
   * Returns aggregated stats for each broken skin plus usage share relative to
   * the champion's total ranked plays. Supports optional championId filter.
   *
   * Query params:
   *   ?championId=  — Optional: filter to a single champion
   *   ?tierMin/Max — Optional: lobby tier bounds (1-26)
   *
   * Returns: Array of { skin_id, skin_name, champion_id, champion_name,
   *            total_plays, wins, losses, win_rate, usage_share }
   *            ordered by total_plays DESC.
   */
  fastify.get('/broken-skins', async (req: any, reply: any) => {
    const params: any[] = [];
    const where: string[] = ['scr.skin_id > 32767'];

    if (req.query.championId != null) {
      const championId = parseInt(req.query.championId as string, 10);
      if (!Number.isInteger(championId) || championId <= 0) {
        return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
      }
      params.push(championId);
      where.push(`scr.champion_id = $${params.length}`);
    }

    const tierMin = req.query.tierMin == null ? null : parseInt(req.query.tierMin as string, 10);
    const tierMax = req.query.tierMax == null ? null : parseInt(req.query.tierMax as string, 10);
    if ((tierMin != null && (!Number.isInteger(tierMin) || tierMin < 1 || tierMin > 26))
      || (tierMax != null && (!Number.isInteger(tierMax) || tierMax < 1 || tierMax > 26))
      || (tierMin != null && tierMax != null && tierMin > tierMax)) {
      return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    }
    if (tierMin != null) {
      params.push(tierMin);
      where.push(`scr.league_tier >= $${params.length}`);
    }
    if (tierMax != null) {
      params.push(tierMax);
      where.push(`scr.league_tier <= $${params.length}`);
    }

    const rows = await query(
      `WITH broken AS (
         SELECT
           scr.skin_id,
           MAX(scr.skin_name) AS skin_name,
           scr.champion_id,
           SUM(scr.count)::INT AS total_plays,
           SUM(scr.wins)::INT AS wins,
           SUM(scr.losses)::INT AS losses,
           COALESCE(
             ROUND(100.0 * SUM(scr.wins)::NUMERIC / NULLIF((SUM(scr.wins) + SUM(scr.losses))::NUMERIC, 0), 2
           ), 0)::DOUBLE PRECISION AS win_rate
         FROM skin_counts_ranked scr
         JOIN champions c ON c.id = scr.champion_id
         WHERE ${where.join(' AND ')}
         GROUP BY scr.skin_id, scr.champion_id, c.name
       ),
       champ_totals AS (
         SELECT champion_id, SUM(total_plays) AS champion_total
         FROM broken
         GROUP BY champion_id
       )
       SELECT
         b.skin_id,
         b.skin_name,
         b.champion_id,
         c.name AS champion_name,
         b.total_plays,
         b.wins,
         b.losses,
         b.win_rate,
         ROUND(b.total_plays::NUMERIC / ct.champion_total * 100, 1) AS usage_share
       FROM broken b
       JOIN champ_totals ct ON ct.champion_id = b.champion_id
       JOIN champions c ON c.id = b.champion_id
       ORDER BY b.champion_id, b.total_plays DESC`,
      params,
    );
    return { data: rows, total: rows.length, tier_min: tierMin, tier_max: tierMax };
  });

  /**
   * GET /stats/items — Item usage stats from the derived item-count tables.
   *
   * The public stats page calls this on every visit, so it should not repeat a
   * raw aggregate over match_player_items + match_players. Ingest already
   * maintains item_counts_ranked after a queue-486 match is accepted;
   * the daily derived-projection repair job can also rebuild those tables from
   * source facts if they ever drift. This keeps the request path cheap while
   * preserving the source-of-truth flow:
   *
   *   ranked match facts -> buffer processor -> item_counts_ranked -> API
   *
   * item_counts_* stores one row per item/slot/level so the endpoint rolls those
   * rows back up to item-level totals for the current frontend. Win rate is
   * recomputed from summed wins/counts rather than averaging stored winrates,
   * otherwise high-count rows and low-count rows would have equal weight.
   *
   * Query params:
   *   ?mode=     — "ranked" (default) or "casual"
   *   ?limit=    — Max results (default: 50)
   *   ?role=     — Optional champion class: Frontline, Damage, Flank, Support
   *   ?scope=    — Casual display scope (optional; all scopes by default)
   *   ?queueId=  — Casual queue filter (optional)
   *
   * Returns: Array of { item_id, item_name, total_uses, win_rate }
   */
  fastify.get('/items', async (req: any, reply: any) => {
    const mode = String(req.query?.mode ?? 'ranked').toLowerCase();
    if (!['ranked', 'casual'].includes(mode)) {
      return reply.status(400).send(err('VALIDATION', 'Mode must be ranked or casual.'));
    }
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);

    if (mode === 'casual') {
      for (const rankedOnlyFilter of ['tierMin', 'tierMax', 'tier', 'lobby', 'championId', 'role']) {
        if (req.query?.[rankedOnlyFilter] != null && req.query[rankedOnlyFilter] !== '') {
          return reply.status(400).send(err(
            'VALIDATION',
            `${rankedOnlyFilter} is available only for ranked item statistics.`,
          ));
        }
      }

      const params: any[] = [];
      const itemWhere: string[] = ['1=1'];
      const populationWhere: string[] = ['1=1'];
      const requestedScope = String(req.query?.scope ?? '').trim().toLowerCase();
      if (requestedScope) {
        const allowedCasualScopes = MATCH_STAT_SCOPES.filter(scope => scope !== 'ranked');
        if (!allowedCasualScopes.includes(requestedScope as any)) {
          return reply.status(400).send(err(
            'VALIDATION',
            `Invalid casual scope. Use ${allowedCasualScopes.join(', ')}.`,
          ));
        }
        params.push(requestedScope);
        itemWhere.push(`casual.stats_scope = $${params.length}`);
        populationWhere.push(`ledger.stats_scope = $${params.length}`);
      }

      if (req.query?.queueId != null && req.query.queueId !== '') {
        const queueId = Number(req.query.queueId);
        if (!Number.isInteger(queueId) || queueId <= 0 || queueId === 486) {
          return reply.status(400).send(err(
            'VALIDATION',
            'queueId must identify a positive non-ranked queue.',
          ));
        }
        params.push(queueId);
        itemWhere.push(`casual.queue_id = $${params.length}`);
        populationWhere.push(`ledger.queue_id = $${params.length}`);
      }

      params.push(limit);
      return query(
        `WITH item_rows AS (
           SELECT
             casual.item_id,
             casual.slot,
             casual.item_level,
             SUM(casual.count)::BIGINT AS uses,
             SUM(casual.wins)::BIGINT AS wins,
             SUM(casual.losses)::BIGINT AS losses
           FROM item_counts_casual casual
           WHERE ${itemWhere.join(' AND ')}
           GROUP BY casual.item_id, casual.slot, casual.item_level
         ),
         player_count AS (
           SELECT COALESCE(SUM(ledger.eligible_players), 0)::BIGINT AS total
           FROM item_counts_casual_matches ledger
           WHERE ${populationWhere.join(' AND ')}
         ),
         item_totals AS (
           SELECT
             item_id,
             SUM(uses)::BIGINT AS total_uses,
             SUM(wins)::BIGINT AS wins,
             SUM(losses)::BIGINT AS losses
           FROM item_rows
           GROUP BY item_id
         ),
         slot_rows AS (
           SELECT
             item_id,
             slot,
             SUM(uses)::BIGINT AS total_uses,
             COALESCE(ROUND(
               100.0 * SUM(wins)::NUMERIC
               / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0),
               2
             ), 0) AS win_rate
           FROM item_rows
           GROUP BY item_id, slot
         ),
         level_rows AS (
           SELECT
             item_id,
             item_level,
             SUM(uses)::BIGINT AS total_uses,
             COALESCE(ROUND(
               100.0 * SUM(wins)::NUMERIC
               / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0),
               2
             ), 0) AS win_rate
           FROM item_rows
           GROUP BY item_id, item_level
         ),
         breakdown_rows AS (
           SELECT
             item_id,
             slot,
             item_level,
             uses AS total_uses,
             COALESCE(ROUND(
               100.0 * wins::NUMERIC
               / NULLIF((wins + losses)::NUMERIC, 0),
               2
             ), 0) AS win_rate,
             COALESCE(ROUND(
               100.0 * uses::NUMERIC
               / NULLIF((SELECT total FROM player_count)::NUMERIC, 0),
               2
             ), 0) AS pick_rate
           FROM item_rows
         )
         SELECT
           totals.item_id,
           COALESCE(item.item_name, 'Item ' || totals.item_id::TEXT) AS item_name,
           totals.total_uses,
           COALESCE(ROUND(
             100.0 * totals.wins::NUMERIC
             / NULLIF((totals.wins + totals.losses)::NUMERIC, 0),
             2
           ), 0) AS win_rate,
           COALESCE(ROUND(
             100.0 * totals.total_uses::NUMERIC
             / NULLIF((SELECT total FROM player_count)::NUMERIC, 0),
             2
           ), 0) AS pick_rate,
           COALESCE((
             SELECT jsonb_agg(jsonb_build_object(
               'slot', slot,
               'total_uses', total_uses,
               'win_rate', win_rate
             ) ORDER BY slot)
             FROM slot_rows slot_row
             WHERE slot_row.item_id = totals.item_id
           ), '[]'::JSONB) AS slots,
           COALESCE((
             SELECT jsonb_agg(jsonb_build_object(
               'item_level', item_level,
               'total_uses', total_uses,
               'win_rate', win_rate
             ) ORDER BY item_level)
             FROM level_rows level_row
             WHERE level_row.item_id = totals.item_id
           ), '[]'::JSONB) AS levels,
           COALESCE((
             SELECT jsonb_agg(jsonb_build_object(
               'slot', slot,
               'item_level', item_level,
               'total_uses', total_uses,
               'win_rate', win_rate,
               'pick_rate', pick_rate
             ) ORDER BY slot, item_level)
             FROM breakdown_rows breakdown
             WHERE breakdown.item_id = totals.item_id
           ), '[]'::JSONB) AS breakdown
         FROM item_totals totals
         LEFT JOIN items item ON item.item_id = totals.item_id
         ORDER BY totals.total_uses DESC, item_name ASC
         LIMIT $${params.length}`,
        params,
      );
    }

    const tableName = 'item_counts_ranked';
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    let championId: number | null = null;
    if (req.query.championId != null) {
      championId = parseInt(String(req.query.championId), 10);
      if (!Number.isInteger(championId) || championId <= 0) {
        return reply.code(400).send({ error: 'Invalid champion id' });
      }
    }
    const roleFilter = req.query.role ? normalizeRole(req.query.role) : null;
    if (req.query.role && !roleFilter) {
      return reply.status(400).send(err('VALIDATION', 'Invalid role. Use Frontline, Damage, Flank, or Support.'));
    }

    // Every public item scope now reads the queue/tier projection. Champion
    // and role filters select a small champion set first; arbitrary tier
    // ranges then sum exact-tier rows without touching historical item facts.
    const projectionParams: any[] = [486];
    const championWhere: string[] = ['1=1'];
    if (championId != null) {
      projectionParams.push(championId);
      championWhere.push(`c.id = $${projectionParams.length}`);
    }
    if (roleFilter) {
      projectionParams.push(roleFilter.role);
      championWhere.push(`${championRoleSql('c')} = $${projectionParams.length}`);
    }
    const itemWhere = ['sia.queue_id = $1'];
    const playerWhere = ['spa.queue_id = $1'];
    if (lobbyTier.min != null) {
      projectionParams.push(lobbyTier.min);
      itemWhere.push(`sia.lobby_tier >= $${projectionParams.length}`);
      playerWhere.push(`spa.lobby_tier >= $${projectionParams.length}`);
    }
    if (lobbyTier.max != null) {
      projectionParams.push(lobbyTier.max);
      itemWhere.push(`sia.lobby_tier <= $${projectionParams.length}`);
      playerWhere.push(`spa.lobby_tier <= $${projectionParams.length}`);
    }
    projectionParams.push(limit);
    return query(`WITH eligible_champions AS (
        SELECT c.id FROM champions c WHERE ${championWhere.join(' AND ')}
      ), item_rows AS (
        SELECT sia.item_id, sia.slot, sia.item_level,
          SUM(sia.uses)::BIGINT AS uses, SUM(sia.wins)::BIGINT AS wins, SUM(sia.losses)::BIGINT AS losses
        FROM stats_item_aggregate sia
        JOIN eligible_champions ec ON ec.id = sia.champion_id
        WHERE ${itemWhere.join(' AND ')}
        GROUP BY sia.item_id, sia.slot, sia.item_level
      ), player_count AS (
        SELECT COALESCE(SUM(spa.plays), 0)::BIGINT AS total
        FROM stats_player_aggregate spa
        JOIN eligible_champions ec ON ec.id = spa.champion_id
        WHERE ${playerWhere.join(' AND ')}
      ), item_totals AS (
        SELECT item_id, SUM(uses)::BIGINT AS total_uses, SUM(wins)::BIGINT AS wins, SUM(losses)::BIGINT AS losses
        FROM item_rows GROUP BY item_id
      ), slot_rows AS (
        SELECT item_id, slot, SUM(uses)::BIGINT AS total_uses,
          COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins)+SUM(losses))::NUMERIC,0),2),0) AS win_rate
        FROM item_rows GROUP BY item_id,slot
      ), level_rows AS (
        SELECT item_id,item_level,SUM(uses)::BIGINT AS total_uses,
          COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins)+SUM(losses))::NUMERIC,0),2),0) AS win_rate
        FROM item_rows GROUP BY item_id,item_level
      ), breakdown_rows AS (
        SELECT item_id,slot,item_level,uses AS total_uses,
          COALESCE(ROUND(100.0*wins::NUMERIC/NULLIF((wins+losses)::NUMERIC,0),2),0) AS win_rate,
          COALESCE(ROUND(100.0*uses::NUMERIC/NULLIF((SELECT total FROM player_count)::NUMERIC,0),2),0) AS pick_rate
        FROM item_rows
      )
      SELECT totals.item_id, COALESCE(i.item_name,'Item '||totals.item_id::TEXT) AS item_name,
        totals.total_uses,
        COALESCE(ROUND(100.0*totals.wins::NUMERIC/NULLIF((totals.wins+totals.losses)::NUMERIC,0),2),0) AS win_rate,
        COALESCE(ROUND(100.0*totals.total_uses::NUMERIC/NULLIF((SELECT total FROM player_count)::NUMERIC,0),2),0) AS pick_rate,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('slot',slot,'total_uses',total_uses,'win_rate',win_rate) ORDER BY slot)
          FROM slot_rows sr WHERE sr.item_id=totals.item_id),'[]'::JSONB) AS slots,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('item_level',item_level,'total_uses',total_uses,'win_rate',win_rate) ORDER BY item_level)
          FROM level_rows lr WHERE lr.item_id=totals.item_id),'[]'::JSONB) AS levels,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('slot',slot,'item_level',item_level,'total_uses',total_uses,'win_rate',win_rate,'pick_rate',pick_rate) ORDER BY slot,item_level)
          FROM breakdown_rows br WHERE br.item_id=totals.item_id),'[]'::JSONB) AS breakdown
      FROM item_totals totals LEFT JOIN items i ON i.item_id=totals.item_id
      ORDER BY totals.total_uses DESC,item_name ASC LIMIT $${projectionParams.length}`,
      projectionParams);

    if (championId != null || roleFilter) {
      const params: any[] = [];
      const playerWhere = ['m.queue_id = 486'];
      if (championId != null) {
        params.push(championId);
        playerWhere.push(`mp.champion_id = $${params.length}`);
      }
      if (roleFilter) {
        params.push(roleFilter!.role);
        playerWhere.push(`${championRoleSql('c')} = $${params.length}`);
      }
      appendLobbyTierPredicate(lobbyTier, params, playerWhere);
      params.push(limit);
      if (String(req.query.summary ?? '').toLowerCase() === 'true') {
        return query(`WITH champion_players AS MATERIALIZED (
            SELECT mp.match_id, mp.player_id, mp.win_status
            FROM match_players mp
            JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
            JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
            LEFT JOIN champions c ON c.id = mp.champion_id
            WHERE ${playerWhere.join(' AND ')}
          )
          SELECT
            mpi.item_id,
            COALESCE(MAX(i.item_name), 'Item ' || mpi.item_id::TEXT) AS item_name,
            COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(
              100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(cp.win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(cp.win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0),
              2
            ), 0) AS win_rate
          FROM match_player_items mpi
          JOIN champion_players cp ON cp.match_id = mpi.match_id AND cp.player_id = mpi.player_id
          LEFT JOIN items i ON i.item_id = mpi.item_id
          GROUP BY mpi.item_id
          ORDER BY total_uses DESC, item_name ASC
          LIMIT $${params.length}`,
          params,
        );
      }
      return query(`WITH champion_players AS (
          SELECT mp.match_id, mp.player_id, mp.win_status
          FROM match_players mp
          JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          LEFT JOIN champions c ON c.id = mp.champion_id
          WHERE ${playerWhere.join(' AND ')}
        ), champion_match_count AS (
          SELECT COUNT(*)::INT AS total_matches FROM champion_players
        ), item_rows AS (
          SELECT
            mpi.item_id,
            COALESCE(i.item_name, 'Item ' || mpi.item_id::TEXT) AS item_name,
            mpi.slot,
            COALESCE(mpi.item_level, 0)::SMALLINT AS item_level,
            cp.win_status
          FROM match_player_items mpi
          JOIN champion_players cp
            ON cp.match_id = mpi.match_id
           AND cp.player_id = mpi.player_id
          LEFT JOIN items i ON i.item_id = mpi.item_id
        ), item_totals AS (
          SELECT
            item_id,
            MAX(item_name) AS item_name,
            COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(
              100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0),
              2
            ), 0) AS win_rate
          FROM item_rows
          GROUP BY item_id
        ), slot_rows AS (
          SELECT
            item_id,
            slot,
            COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(
              100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0),
              2
            ), 0) AS win_rate
          FROM item_rows
          GROUP BY item_id, slot
        ), slots AS (
          SELECT item_id, jsonb_agg(jsonb_build_object('slot', slot, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY slot) AS slots
          FROM slot_rows
          GROUP BY item_id
        ), level_rows AS (
          SELECT
            item_id,
            item_level,
            COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(
              100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0),
              2
            ), 0) AS win_rate
          FROM item_rows
          GROUP BY item_id, item_level
        ), levels AS (
          SELECT item_id, jsonb_agg(jsonb_build_object('item_level', item_level, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY item_level) AS levels
          FROM level_rows
          GROUP BY item_id
        ), breakdown_rows AS (
          SELECT
            item_id,
            slot,
            item_level,
            COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(
              100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0),
              2
            ), 0) AS win_rate,
            COALESCE(ROUND(
              100.0 * COUNT(*)::NUMERIC / NULLIF((SELECT total_matches FROM champion_match_count)::NUMERIC, 0),
              2
            ), 0) AS pick_rate
          FROM item_rows
          GROUP BY item_id, slot, item_level
        ), breakdowns AS (
          SELECT item_id, jsonb_agg(jsonb_build_object('slot', slot, 'item_level', item_level, 'total_uses', total_uses, 'win_rate', win_rate, 'pick_rate', pick_rate) ORDER BY slot, item_level) AS breakdown
          FROM breakdown_rows
          GROUP BY item_id
        )
        SELECT
          totals.*,
          COALESCE(ROUND(
            100.0 * totals.total_uses::NUMERIC / NULLIF((SELECT total_matches FROM champion_match_count)::NUMERIC, 0),
            2
          ), 0) AS pick_rate,
          COALESCE(slots.slots, '[]'::JSONB) AS slots,
          COALESCE(levels.levels, '[]'::JSONB) AS levels,
          COALESCE(breakdowns.breakdown, '[]'::JSONB) AS breakdown
        FROM item_totals totals
        LEFT JOIN slots ON slots.item_id = totals.item_id
        LEFT JOIN levels ON levels.item_id = totals.item_id
        LEFT JOIN breakdowns ON breakdowns.item_id = totals.item_id
        ORDER BY total_uses DESC, item_name ASC
        LIMIT $${params.length}`,
        params
      );
    }

    if (lobbyTier.active) {
      const params: any[] = [];
      const where = ['m.queue_id = 486'];
      appendLobbyTierPredicate(lobbyTier, params, where);
      params.push(limit);
      return query(`WITH item_rows AS (
          SELECT mpi.item_id, COALESCE(i.item_name, 'Item ' || mpi.item_id::TEXT) AS item_name,
            mpi.slot, COALESCE(mpi.item_level, 0)::SMALLINT AS item_level, mp.win_status
          FROM match_player_items mpi
          JOIN match_players mp ON mp.match_id = mpi.match_id AND mp.player_id = mpi.player_id
          JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          LEFT JOIN items i ON i.item_id = mpi.item_id
          WHERE ${where.join(' AND ')}
        ), item_totals AS (
          SELECT item_id, MAX(item_name) AS item_name, COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0), 2), 0) AS win_rate
          FROM item_rows GROUP BY item_id
        ), slot_rows AS (
          SELECT item_id, slot, COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0), 2), 0) AS win_rate
          FROM item_rows GROUP BY item_id, slot
        ), slots AS (
          SELECT item_id, jsonb_agg(jsonb_build_object('slot', slot, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY slot) AS slots
          FROM slot_rows GROUP BY item_id
        ), level_rows AS (
          SELECT item_id, item_level, COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0), 2), 0) AS win_rate
          FROM item_rows GROUP BY item_id, item_level
        ), levels AS (
          SELECT item_id, jsonb_agg(jsonb_build_object('item_level', item_level, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY item_level) AS levels
          FROM level_rows GROUP BY item_id
        ), breakdown_rows AS (
          SELECT item_id, slot, item_level, COUNT(*)::INT AS total_uses,
            COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win'))::NUMERIC
              / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0), 2), 0) AS win_rate
          FROM item_rows GROUP BY item_id, slot, item_level
        ), breakdowns AS (
          SELECT item_id, jsonb_agg(jsonb_build_object('slot', slot, 'item_level', item_level, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY slot, item_level) AS breakdown
          FROM breakdown_rows GROUP BY item_id
        )
        SELECT totals.*, COALESCE(slots.slots, '[]'::JSONB) AS slots, COALESCE(levels.levels, '[]'::JSONB) AS levels,
          COALESCE(breakdowns.breakdown, '[]'::JSONB) AS breakdown
        FROM item_totals totals LEFT JOIN slots ON slots.item_id = totals.item_id LEFT JOIN levels ON levels.item_id = totals.item_id
        LEFT JOIN breakdowns ON breakdowns.item_id = totals.item_id
        ORDER BY total_uses DESC, item_name ASC LIMIT $${params.length}`, params);
    }

    return query(`WITH item_rows AS (
        SELECT item_id, item_name, slot, item_level, count, wins, losses
        FROM ${tableName}
      ), item_totals AS (
        SELECT item_id, MAX(item_name) AS item_name,
          SUM(count)::INT AS total_uses,
          COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2), 0) AS win_rate
        FROM item_rows GROUP BY item_id
      ), slot_rows AS (
        SELECT item_id, slot, SUM(count)::INT AS total_uses,
          COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2), 0) AS win_rate
        FROM item_rows GROUP BY item_id, slot
      ), slots AS (
        SELECT item_id, jsonb_agg(jsonb_build_object('slot', slot, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY slot) AS slots
        FROM slot_rows GROUP BY item_id
      ), level_rows AS (
        SELECT item_id, item_level, SUM(count)::INT AS total_uses,
          COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2), 0) AS win_rate
        FROM item_rows GROUP BY item_id, item_level
      ), levels AS (
        SELECT item_id, jsonb_agg(jsonb_build_object('item_level', item_level, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY item_level) AS levels
        FROM level_rows GROUP BY item_id
      ), breakdown_rows AS (
        SELECT item_id, slot, item_level, SUM(count)::INT AS total_uses,
          COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2), 0) AS win_rate
        FROM item_rows GROUP BY item_id, slot, item_level
      ), breakdowns AS (
        SELECT item_id, jsonb_agg(jsonb_build_object('slot', slot, 'item_level', item_level, 'total_uses', total_uses, 'win_rate', win_rate) ORDER BY slot, item_level) AS breakdown
        FROM breakdown_rows GROUP BY item_id
      )
      SELECT totals.*, COALESCE(slots.slots, '[]'::JSONB) AS slots, COALESCE(levels.levels, '[]'::JSONB) AS levels,
        COALESCE(breakdowns.breakdown, '[]'::JSONB) AS breakdown
      FROM item_totals totals
      LEFT JOIN slots ON slots.item_id = totals.item_id
      LEFT JOIN levels ON levels.item_id = totals.item_id
      LEFT JOIN breakdowns ON breakdowns.item_id = totals.item_id
      ORDER BY total_uses DESC, item_name ASC
      LIMIT $1`,
      [limit]
    );
  });

  /**
   * GET /stats/items/:itemId — Item performance by purchase slot and level.
   *
   * The item-count projections retain the exact slot/level combination.  This
   * endpoint rolls those rows up three ways so callers can distinguish an item
   * bought early from an item upgraded further, without losing the complete
   * slot-by-level breakdown. The optional role filter recomputes the same
   * aggregates from source facts for one normalized champion class.
   */
  fastify.get('/items/:itemId', async (req: any, reply: any) => {
    const itemId = parseInt(req.params.itemId, 10);
    if (!Number.isInteger(itemId) || itemId <= 0) {
      return reply.code(400).send({ error: 'Invalid item id' });
    }

    if (!requireRankedStatsMode(req, reply)) return;
    const mode = 'ranked';
    const tableName = 'item_counts_ranked';
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const statColumns = `
      SUM(count)::INT AS total_uses,
      SUM(wins)::INT AS wins,
      SUM(losses)::INT AS losses,
      COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2), 0) AS win_rate`;

    let championId: number | null = null;
    if (req.query.championId != null) {
      championId = parseInt(String(req.query.championId), 10);
      if (!Number.isInteger(championId) || championId <= 0) {
        return reply.code(400).send({ error: 'Invalid champion id' });
      }
    }
    const roleFilter = req.query.role ? normalizeRole(req.query.role) : null;
    if (req.query.role && !roleFilter) {
      return reply.status(400).send(err('VALIDATION', 'Invalid role. Use Frontline, Damage, Flank, or Support.'));
    }

    if (lobbyTier.active || championId != null || roleFilter) {
      const params: any[] = [itemId];
      const where = ['sia.item_id = $1', 'sia.queue_id = 486'];
      if (championId != null) {
        params.push(championId);
        where.push(`sia.champion_id = $${params.length}`);
      }
      if (roleFilter) {
        params.push(roleFilter.role);
        where.push(`${championRoleSql('c')} = $${params.length}`);
      }
      appendLobbyTierPredicate(lobbyTier, params, where,'sia');
      const rows = await query<any>(`SELECT sia.slot,sia.item_level,
          SUM(sia.uses)::BIGINT AS total_uses,SUM(sia.wins)::BIGINT AS wins,SUM(sia.losses)::BIGINT AS losses
        FROM stats_item_aggregate sia
        LEFT JOIN champions c ON c.id=sia.champion_id
        WHERE ${where.join(' AND ')}
        GROUP BY sia.slot,sia.item_level ORDER BY sia.slot,sia.item_level`, params);
      if (rows.length === 0) return reply.code(404).send({ error: 'Item statistics not found' });
      const item = await one<any>('SELECT item_id, item_name FROM items WHERE item_id = $1', [itemId]);
      const summarize = (subset: any[]) => {
        const totalUses = subset.reduce((sum, row) => sum + Number(row.total_uses), 0);
        const wins = subset.reduce((sum, row) => sum + Number(row.wins), 0);
        const losses = subset.reduce((sum, row) => sum + Number(row.losses), 0);
        return { total_uses: totalUses, wins, losses, win_rate: wins + losses > 0 ? Number((100 * wins / (wins + losses)).toFixed(2)) : 0 };
      };
      const slots = [...new Set(rows.map((row) => Number(row.slot)))].map((slot) => ({ slot, ...summarize(rows.filter((row) => Number(row.slot) === slot)) }));
      const levels = [...new Set(rows.map((row) => Number(row.item_level)))].map((item_level) => ({ item_level, ...summarize(rows.filter((row) => Number(row.item_level) === item_level)) }));
      return { mode, item_id: itemId, item_name: item?.item_name ?? `Item ${itemId}`, ...summarize(rows), slots, levels, breakdown: rows.map((row) => ({ ...row, win_rate: Number(row.wins) + Number(row.losses) > 0 ? Number((100 * Number(row.wins) / (Number(row.wins) + Number(row.losses))).toFixed(2)) : 0 })) };
    }

    const [overall, slots, levels, breakdown] = await Promise.all([
      one<any>(`SELECT item_id, MAX(item_name) AS item_name, ${statColumns}
        FROM ${tableName} WHERE item_id = $1 GROUP BY item_id`, [itemId]),
      query(`SELECT slot, ${statColumns}
        FROM ${tableName} WHERE item_id = $1 GROUP BY slot ORDER BY slot`, [itemId]),
      query(`SELECT item_level, ${statColumns}
        FROM ${tableName} WHERE item_id = $1 GROUP BY item_level ORDER BY item_level`, [itemId]),
      query(`SELECT slot, item_level, ${statColumns}
        FROM ${tableName} WHERE item_id = $1 GROUP BY slot, item_level ORDER BY slot, item_level`, [itemId]),
    ]);

    if (!overall) return reply.code(404).send({ error: 'Item statistics not found' });
    return { mode, ...overall, slots, levels, breakdown };
  });

  /**
   * GET /stats/maps — Map play counts and distribution across the map pool.
   *
   * Query params:
   *   ?queueId=  — Optional queue filter
   *   ?limit=    — Max results (default: 25, max: 100)
   *   ?includeUnknown=true — Include legacy/recovery-debt rows whose map is
   *     missing. Public stats exclude them by default because Unknown is not a
   *     playable map; it is a data-quality signal from broken Hi-Rez payloads.
   *
   * A map has no intrinsic win/loss outcome: both teams play the same map in
   * every match. Exposing Team 1's outcome as a map "win rate" was misleading
   * and also undercounted rows where recovery lacked winning_task_force.
   * Returns: Array of { map, total_matches, distribution_rate, avg_duration_seconds }
   */
  fastify.get('/maps', async (req: any, reply: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 25, 100);
    const includeUnknown = String(req.query.includeUnknown || '').toLowerCase() === 'true';
    const statsScope = String(req.query.scope || 'ranked').trim().toLowerCase();
    if (!isPublicStatsScope(statsScope)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid statistics scope.'));
    }
    if (statsScope !== 'ranked') {
      const params: any[] = [statsScope];
      const where = ['stats_scope=$1'];
      if (!includeUnknown) where.push(`map <> 'Unknown'`);
      if (req.query.queueId) {
        const queueId = parseInt(req.query.queueId as string, 10);
        if (!Number.isInteger(queueId) || queueId <= 0) {
          return reply.status(400).send(err('VALIDATION', 'queueId must be a positive integer.'));
        }
        params.push(queueId);
        where.push(`queue_id=$${params.length}`);
      }
      params.push(limit);
      return query(`WITH map_counts AS (
          SELECT map,SUM(matches)::BIGINT AS total_matches,
            COALESCE(ROUND(SUM(duration_sum)::numeric/NULLIF(SUM(matches),0),2),0)::double precision AS avg_duration_seconds
          FROM nonranked_map_stats_daily WHERE ${where.join(' AND ')} GROUP BY map
        )
        SELECT map,total_matches,
          COALESCE(ROUND(100.0*total_matches::numeric/NULLIF(SUM(total_matches) OVER(),0),2),0)::double precision AS distribution_rate,
          avg_duration_seconds
        FROM map_counts ORDER BY total_matches DESC,map ASC LIMIT $${params.length}`, params);
    }
    const params: any[] = [486];
    const where: string[] = ['sma.queue_id = $1'];
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    if (req.query.queueId) {
      const queueId = parseInt(req.query.queueId as string, 10);
      if (queueId !== 486) {
        return reply.status(400).send(err('VALIDATION', 'Only ranked queue 486 is available for aggregate statistics.'));
      }
    }

    if (!includeUnknown) {
      where.push(`sma.map_name <> 'Unknown'`);
    }
    appendLobbyTierPredicate(lobbyTier, params, where, 'sma');

    params.push(limit);
    const whereSql = `WHERE ${where.join(' AND ')}`;
    return query(
      `WITH map_counts AS (
         SELECT
           sma.map_name AS map,
           SUM(sma.match_count)::BIGINT AS total_matches,
           COALESCE(ROUND(SUM(sma.duration_sum)::NUMERIC/NULLIF(SUM(sma.match_count),0),2),0)::DOUBLE PRECISION AS avg_duration_seconds
         FROM stats_match_aggregate sma
         ${whereSql}
         GROUP BY sma.map_name
       )
       SELECT
         map,
         total_matches,
         COALESCE(ROUND(100.0 * total_matches::NUMERIC / NULLIF(SUM(total_matches) OVER (), 0), 2), 0)::DOUBLE PRECISION AS distribution_rate,
         avg_duration_seconds
       FROM map_counts
       ORDER BY total_matches DESC, map ASC
       LIMIT $${params.length}`,
      params
    );
  });

  /**
   * GET /stats/champions/:championId/maps — Ranked map performance for one champion.
   *
   * Pick rate is deliberately champion-relative: it is the share of this
   * champion's ranked plays observed on each map, rather than the champion's
   * share of every player slot on that map. The rows therefore sum to 100%
   * within the selected lobby-tier scope.
   */
  fastify.get('/champions/:championId/maps', async (req: any, reply: any) => {
    const championId = parseInt(String(req.params.championId), 10);
    if (!Number.isInteger(championId) || championId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Champion id must be a positive integer.'));
    }
    const statsScope = String(req.query.scope || 'ranked').trim().toLowerCase();
    if (!isPublicStatsScope(statsScope)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid statistics scope.'));
    }
    if (statsScope !== 'ranked') {
      const params: any[] = [championId, statsScope];
      const where = ['champion_id=$1', 'stats_scope=$2', `map <> 'Unknown'`];
      if (req.query.queueId) {
        const queueId = parseInt(String(req.query.queueId), 10);
        if (!Number.isInteger(queueId) || queueId <= 0) {
          return reply.status(400).send(err('VALIDATION', 'queueId must be a positive integer.'));
        }
        params.push(queueId);
        where.push(`queue_id=$${params.length}`);
      }
      return query(`WITH champion_map_counts AS (
          SELECT map,SUM(plays)::BIGINT AS total_plays,SUM(wins)::BIGINT AS wins,SUM(losses)::BIGINT AS losses
          FROM nonranked_champion_stats_daily WHERE ${where.join(' AND ')} GROUP BY map
        )
        SELECT map,total_plays,wins,losses,
          COALESCE(ROUND(100.0*wins::numeric/NULLIF((wins+losses)::numeric,0),2),0)::double precision AS win_rate,
          COALESCE(ROUND(100.0*total_plays::numeric/NULLIF(SUM(total_plays) OVER(),0),2),0)::double precision AS pick_rate
        FROM champion_map_counts ORDER BY total_plays DESC,map ASC`, params);
    }

    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    const params: any[] = [championId, 486];
    const where = [
      'spa.champion_id = $1',
      'spa.queue_id = $2',
      `spa.map_name <> 'Unknown'`,
    ];
    appendLobbyTierPredicate(lobbyTier, params, where, 'spa');

    return query(`WITH champion_map_counts AS (
        SELECT
          spa.map_name AS map,
          SUM(spa.plays)::BIGINT AS total_plays,
          SUM(spa.wins)::BIGINT AS wins,
          SUM(spa.losses)::BIGINT AS losses
        FROM stats_player_aggregate spa
        WHERE ${where.join(' AND ')}
        GROUP BY spa.map_name
      )
      SELECT
        map,
        total_plays,
        wins,
        losses,
        COALESCE(ROUND(100.0 * wins::NUMERIC / NULLIF((wins + losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS win_rate,
        COALESCE(ROUND(100.0 * total_plays::NUMERIC / NULLIF(SUM(total_plays) OVER (), 0), 2), 0)::DOUBLE PRECISION AS pick_rate
      FROM champion_map_counts
      ORDER BY total_plays DESC, map ASC`, params);
  });

  /**
   * GET /stats/maps/:mapName/comparison — Deferred cross-map comparison.
   *
   * `section` selects one category so opening a map detail page remains cheap.
   * One request returns that category for every other ranked map; the browser
   * never fans out into one full map-detail request per comparison target.
   */
  fastify.get('/maps/:mapName/comparison', async (req: any, reply: any) => {
    const mapName = String(req.params.mapName || '').trim();
    if (!mapName) return reply.status(400).send(err('VALIDATION', 'Map name is required.'));

    const section = String(req.query.section || '').toLowerCase();
    if (!['champions', 'talents', 'items', 'compositions'].includes(section)) {
      return reply.status(400).send(err('VALIDATION', 'section must be champions, talents, items, or compositions.'));
    }

    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    const requestedLimit = req.query.limit == null ? null : Math.min(Math.max(parseInt(String(req.query.limit),10)||250,1),500);
    let after:{entity:string;map:string}|null=null;
    if(req.query.cursor!=null){
      try{
        const parsed=JSON.parse(Buffer.from(String(req.query.cursor),'base64url').toString('utf8'));
        if(typeof parsed.entity!=='string'||typeof parsed.map!=='string') throw new Error('invalid');
        after=parsed;
      }catch{return reply.status(400).send(err('VALIDATION','Invalid comparison cursor.'));}
    }
    const paging = (params: any[], entityExpression: string, mapExpression:string) => {
      const predicates: string[] = [];
      if (after) {
        params.push(after.entity,after.map);
        predicates.push(`(${entityExpression},${mapExpression}) > ($${params.length-1},$${params.length})`);
      }
      let limitSql = '';
      if (requestedLimit != null) {
        params.push(requestedLimit+1);
        limitSql = ` LIMIT $${params.length}`;
      }
      return { predicates,limitSql };
    };
    const finishComparison = (rows: any[]) => {
      const data = requestedLimit == null ? rows : rows.slice(0,requestedLimit);
      const nextCursor = requestedLimit != null && rows.length>requestedLimit && data.length>0
        ? Buffer.from(JSON.stringify({entity:String(data[data.length-1].entity_key),map:String(data[data.length-1].map_name)})).toString('base64url') : null;
      return { section,rows:data,next_cursor:nextCursor };
    };

    if (section === 'champions') {
      const p: any[] = [486,mapName];
      const playerWhere = ['spa.queue_id=$1','spa.map_name<>$2',"spa.map_name<>'Unknown'"];
      const banWhere = ['sba.queue_id=$1','sba.map_name<>$2',"sba.map_name<>'Unknown'"];
      appendLobbyTierPredicate(lobbyTier,p,playerWhere,'spa');
      // Reuse the same tier placeholders for the ban projection.
      if (lobbyTier.min != null) banWhere.push(`sba.lobby_tier >= $3`);
      if (lobbyTier.max != null) banWhere.push(`sba.lobby_tier <= $${lobbyTier.min != null ? 4 : 3}`);
      const page = paging(p,'cr.champion_id::TEXT','cr.map_name');
      const rows = await query(`WITH champion_rows AS (
          SELECT spa.map_name,spa.champion_id,SUM(spa.plays)::BIGINT AS total_count,
            SUM(spa.wins)::BIGINT AS wins,SUM(spa.losses)::BIGINT AS losses
          FROM stats_player_aggregate spa WHERE ${playerWhere.join(' AND ')} GROUP BY 1,2
        ), map_totals AS (
          SELECT map_name,SUM(total_count)::BIGINT AS player_count FROM champion_rows GROUP BY map_name
        ), match_totals AS (
          SELECT map_name,SUM(match_count)::BIGINT AS match_count FROM stats_match_aggregate sma
          WHERE sma.queue_id=$1 AND sma.map_name<>$2 AND sma.map_name<>'Unknown'
          ${lobbyTier.min != null ? `AND sma.lobby_tier >= $3` : ''}
          ${lobbyTier.max != null ? `AND sma.lobby_tier <= $${lobbyTier.min != null ? 4 : 3}` : ''}
          GROUP BY map_name
        ), ban_rows AS (
          SELECT sba.map_name,sba.champion_id,SUM(sba.bans)::BIGINT AS total_bans
          FROM stats_ban_aggregate sba WHERE ${banWhere.join(' AND ')} GROUP BY 1,2
        ) SELECT cr.champion_id::TEXT AS entity_key,cr.map_name,cr.total_count,cr.wins,cr.losses,
          COALESCE(br.total_bans,0)::BIGINT AS total_bans,
          COALESCE(ROUND(100.0*cr.wins::NUMERIC/NULLIF((cr.wins+cr.losses)::NUMERIC,0),2),0) AS win_rate,
          COALESCE(ROUND(100.0*cr.total_count::NUMERIC/NULLIF(mt.player_count,0),2),0) AS pick_rate,
          COALESCE(ROUND(100.0*COALESCE(br.total_bans,0)::NUMERIC/NULLIF(mmt.match_count,0),2),0) AS ban_rate
        FROM champion_rows cr JOIN map_totals mt USING(map_name) JOIN match_totals mmt USING(map_name)
        LEFT JOIN ban_rows br ON br.map_name=cr.map_name AND br.champion_id=cr.champion_id
        ${page.predicates.length ? `WHERE ${page.predicates.join(' AND ')}` : ''}
        ORDER BY cr.champion_id::TEXT,win_rate DESC,cr.total_count DESC,cr.map_name${page.limitSql}`,p);
      return finishComparison(rows);
    }
    if (section === 'talents') {
      const p:any[]=[486,mapName]; const where=['sta.queue_id=$1','sta.map_name<>$2',"sta.map_name<>'Unknown'"];
      appendLobbyTierPredicate(lobbyTier,p,where,'sta'); const page=paging(p,'tr.talent_id::TEXT','tr.map_name');
      const rows=await query(`WITH talent_rows AS (
          SELECT sta.talent_id,sta.champion_id,sta.map_name,SUM(sta.uses)::BIGINT AS total_count,
            SUM(sta.wins)::BIGINT AS wins,SUM(sta.losses)::BIGINT AS losses
          FROM stats_talent_aggregate sta
          JOIN talents t ON t.talent_id=sta.talent_id AND t.champion_id=sta.champion_id
          WHERE ${where.join(' AND ')} GROUP BY 1,2,3
        ), champion_totals AS (
          SELECT champion_id,map_name,SUM(plays)::BIGINT AS plays FROM stats_player_aggregate spa
          WHERE spa.queue_id=$1 AND spa.map_name<>$2 AND spa.map_name<>'Unknown'
          ${lobbyTier.min!=null?'AND spa.lobby_tier >= $3':''} ${lobbyTier.max!=null?`AND spa.lobby_tier <= $${lobbyTier.min!=null?4:3}`:''}
          GROUP BY 1,2
        ) SELECT tr.talent_id::TEXT AS entity_key,tr.map_name,tr.total_count,tr.wins,tr.losses,0::BIGINT AS total_bans,
          COALESCE(ROUND(100.0*tr.wins::NUMERIC/NULLIF((tr.wins+tr.losses)::NUMERIC,0),2),0) AS win_rate,
          COALESCE(ROUND(100.0*tr.total_count::NUMERIC/NULLIF(ct.plays,0),2),0) AS pick_rate,0::NUMERIC AS ban_rate
        FROM talent_rows tr JOIN champion_totals ct USING(champion_id,map_name)
        ${page.predicates.length?`WHERE ${page.predicates.join(' AND ')}`:''}
        ORDER BY tr.talent_id::TEXT,win_rate DESC,tr.total_count DESC,tr.map_name${page.limitSql}`,p);
      return finishComparison(rows);
    }
    if (section === 'items') {
      const p:any[]=[486,mapName]; const where=['sia.queue_id=$1','sia.map_name<>$2',"sia.map_name<>'Unknown'"];
      appendLobbyTierPredicate(lobbyTier,p,where,'sia'); const page=paging(p,'ir.item_id::TEXT','ir.map_name');
      const rows=await query(`WITH item_rows AS (
          SELECT sia.item_id,sia.map_name,SUM(sia.uses)::BIGINT AS total_count,SUM(sia.wins)::BIGINT AS wins,SUM(sia.losses)::BIGINT AS losses
          FROM stats_item_aggregate sia WHERE ${where.join(' AND ')} GROUP BY 1,2
        ), map_totals AS (
          SELECT map_name,SUM(plays)::BIGINT AS plays FROM stats_player_aggregate spa
          WHERE spa.queue_id=$1 AND spa.map_name<>$2 AND spa.map_name<>'Unknown'
          ${lobbyTier.min!=null?'AND spa.lobby_tier >= $3':''} ${lobbyTier.max!=null?`AND spa.lobby_tier <= $${lobbyTier.min!=null?4:3}`:''} GROUP BY map_name
        ) SELECT ir.item_id::TEXT AS entity_key,ir.map_name,ir.total_count,ir.wins,ir.losses,0::BIGINT AS total_bans,
          COALESCE(ROUND(100.0*ir.wins::NUMERIC/NULLIF((ir.wins+ir.losses)::NUMERIC,0),2),0) AS win_rate,
          COALESCE(ROUND(100.0*ir.total_count::NUMERIC/NULLIF(mt.plays,0),2),0) AS pick_rate,0::NUMERIC AS ban_rate
        FROM item_rows ir JOIN map_totals mt USING(map_name)
        ${page.predicates.length?`WHERE ${page.predicates.join(' AND ')}`:''}
        ORDER BY ir.item_id::TEXT,win_rate DESC,ir.total_count DESC,ir.map_name${page.limitSql}`,p);
      return finishComparison(rows);
    }
    if (section === 'compositions') {
      const p:any[]=[486,mapName]; const where=['sca.queue_id=$1','sca.map_name<>$2',"sca.map_name<>'Unknown'"];
      appendLobbyTierPredicate(lobbyTier,p,where,'sca'); const page=paging(p,'sca.comp_id','sca.map_name');
      const rows=await query(`SELECT sca.comp_id AS entity_key,sca.map_name,SUM(sca.uses)::BIGINT AS total_count,
          SUM(sca.wins)::BIGINT AS wins,SUM(sca.losses)::BIGINT AS losses,0::BIGINT AS total_bans,
          COALESCE(ROUND(100.0*SUM(sca.wins)::NUMERIC/NULLIF((SUM(sca.wins)+SUM(sca.losses))::NUMERIC,0),2),0) AS win_rate,
          0::NUMERIC AS pick_rate,0::NUMERIC AS ban_rate
        FROM stats_composition_aggregate sca WHERE ${where.join(' AND ')}
          ${page.predicates.length?`AND ${page.predicates.join(' AND ')}`:''}
        GROUP BY sca.comp_id,sca.map_name ORDER BY sca.comp_id,win_rate DESC,total_count DESC,sca.map_name${page.limitSql}`,p);
      return finishComparison(rows);
    }

    const params: any[] = [mapName];
    const predicates = [
      'm.queue_id = 486',
      `COALESCE(NULLIF(m.map, ''), 'Unknown') <> 'Unknown'`,
      `COALESCE(NULLIF(m.map, ''), 'Unknown') <> $1`,
    ];
    appendLobbyTierPredicate(lobbyTier, params, predicates);
    const scopedMatches = `SELECT
        m.match_id,
        m.entry_datetime,
        COALESCE(NULLIF(m.map, ''), 'Unknown') AS map_name,
        m.winning_task_force
      FROM matches m
      JOIN match_lobby_tiers mlt
        ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      WHERE ${predicates.join(' AND ')}`;

    let rows: any[];
    if (section === 'champions') {
      rows = await query(`WITH scoped_matches AS (${scopedMatches}),
        map_players AS (
          SELECT sm.map_name, mp.match_id, mp.player_id, mp.champion_id, mp.win_status
          FROM scoped_matches sm
          JOIN match_players mp
            ON mp.match_id = sm.match_id AND mp.entry_datetime = sm.entry_datetime
          WHERE mp.champion_id > 0
        ), map_player_totals AS (
          SELECT map_name, COUNT(*)::INT AS player_count
          FROM map_players GROUP BY map_name
        ), map_match_totals AS (
          SELECT map_name, COUNT(*)::INT AS match_count
          FROM scoped_matches GROUP BY map_name
        ), champion_rows AS (
          SELECT map_name, champion_id,
            COUNT(*)::INT AS total_count,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses
          FROM map_players mp GROUP BY map_name, champion_id
        ), ban_rows AS (
          SELECT sm.map_name, mb.champion_id, COUNT(*)::INT AS total_bans
          FROM scoped_matches sm
          JOIN match_bans mb ON mb.match_id = sm.match_id
          WHERE mb.champion_id IS NOT NULL
          GROUP BY sm.map_name, mb.champion_id
        )
        SELECT cr.champion_id::TEXT AS entity_key, cr.map_name, cr.total_count,
          cr.wins, cr.losses, COALESCE(br.total_bans, 0)::INT AS total_bans,
          COALESCE(ROUND(100.0 * cr.wins::NUMERIC / NULLIF((cr.wins + cr.losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS win_rate,
          COALESCE(ROUND(100.0 * cr.total_count::NUMERIC / NULLIF(mpt.player_count::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS pick_rate,
          COALESCE(ROUND(100.0 * COALESCE(br.total_bans, 0)::NUMERIC / NULLIF(mmt.match_count::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS ban_rate
        FROM champion_rows cr
        JOIN map_player_totals mpt USING (map_name)
        JOIN map_match_totals mmt USING (map_name)
        LEFT JOIN ban_rows br ON br.map_name = cr.map_name AND br.champion_id = cr.champion_id
        ORDER BY cr.champion_id, win_rate DESC, cr.total_count DESC, cr.map_name ASC`, params);
    } else if (section === 'talents') {
      rows = await query(`WITH scoped_matches AS (${scopedMatches}),
        map_players AS (
          SELECT sm.map_name, mp.match_id, mp.player_id, mp.champion_id, mp.win_status
          FROM scoped_matches sm
          JOIN match_players mp
            ON mp.match_id = sm.match_id AND mp.entry_datetime = sm.entry_datetime
          WHERE mp.champion_id > 0
        ), champion_totals AS (
          SELECT map_name, champion_id, COUNT(*)::INT AS champion_plays
          FROM map_players GROUP BY map_name, champion_id
        ), talent_rows AS (
          SELECT mp.map_name, mpt.talent_id, mp.champion_id,
            COUNT(*)::INT AS total_count,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses
          FROM map_players mp
          JOIN match_player_talents mpt
            ON mpt.match_id = mp.match_id AND mpt.player_id = mp.player_id
          JOIN talents t
            ON t.talent_id = mpt.talent_id AND t.champion_id = mp.champion_id
          GROUP BY mp.map_name, mpt.talent_id, mp.champion_id
        )
        SELECT tr.talent_id::TEXT AS entity_key, tr.map_name, tr.total_count,
          tr.wins, tr.losses, 0::INT AS total_bans,
          COALESCE(ROUND(100.0 * tr.wins::NUMERIC / NULLIF((tr.wins + tr.losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS win_rate,
          COALESCE(ROUND(100.0 * tr.total_count::NUMERIC / NULLIF(ct.champion_plays::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS pick_rate,
          0::DOUBLE PRECISION AS ban_rate
        FROM talent_rows tr
        JOIN champion_totals ct ON ct.map_name = tr.map_name AND ct.champion_id = tr.champion_id
        ORDER BY tr.talent_id, win_rate DESC, tr.total_count DESC, tr.map_name ASC`, params);
    } else if (section === 'items') {
      rows = await query(`WITH scoped_matches AS (${scopedMatches}),
        map_players AS (
          SELECT sm.map_name, mp.match_id, mp.player_id, mp.win_status
          FROM scoped_matches sm
          JOIN match_players mp
            ON mp.match_id = sm.match_id AND mp.entry_datetime = sm.entry_datetime
        ), map_player_totals AS (
          SELECT map_name, COUNT(*)::INT AS player_count
          FROM map_players GROUP BY map_name
        ), item_rows AS (
          SELECT mp.map_name, mpi.item_id,
            COUNT(*)::INT AS total_count,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses
          FROM map_players mp
          JOIN match_player_items mpi
            ON mpi.match_id = mp.match_id AND mpi.player_id = mp.player_id
          GROUP BY mp.map_name, mpi.item_id
        )
        SELECT ir.item_id::TEXT AS entity_key, ir.map_name, ir.total_count,
          ir.wins, ir.losses, 0::INT AS total_bans,
          COALESCE(ROUND(100.0 * ir.wins::NUMERIC / NULLIF((ir.wins + ir.losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS win_rate,
          COALESCE(ROUND(100.0 * ir.total_count::NUMERIC / NULLIF(mpt.player_count::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS pick_rate,
          0::DOUBLE PRECISION AS ban_rate
        FROM item_rows ir
        JOIN map_player_totals mpt USING (map_name)
        ORDER BY ir.item_id, win_rate DESC, ir.total_count DESC, ir.map_name ASC`, params);
    } else {
      rows = await query(`WITH scoped_matches AS (${scopedMatches}),
        map_players AS (
          SELECT sm.map_name, mp.match_id, mp.task_force, sm.winning_task_force,
            ${championRoleSql('c')} AS champion_role
          FROM scoped_matches sm
          JOIN match_players mp
            ON mp.match_id = sm.match_id AND mp.entry_datetime = sm.entry_datetime
          JOIN champions c ON c.id = mp.champion_id
          WHERE mp.task_force IS NOT NULL
            AND mp.task_force <> 0
            AND mp.champion_id > 0
            AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
        ), team_compositions AS (
          SELECT map_name, match_id, task_force, winning_task_force,
            COUNT(*) FILTER (WHERE champion_role = 'Frontline')::SMALLINT AS frontline,
            COUNT(*) FILTER (WHERE champion_role = 'Damage')::SMALLINT AS damage,
            COUNT(*) FILTER (WHERE champion_role = 'Flank')::SMALLINT AS flank,
            COUNT(*) FILTER (WHERE champion_role = 'Support')::SMALLINT AS support
          FROM map_players
          GROUP BY map_name, match_id, task_force, winning_task_force
          HAVING COUNT(*) = 5
        ), valid_compositions AS (
          SELECT *, frontline || '-' || damage || '-' || flank || '-' || support AS comp_id
          FROM team_compositions
          WHERE frontline + damage + flank + support = 5
        )
        SELECT comp_id AS entity_key, map_name, COUNT(*)::INT AS total_count,
          COUNT(*) FILTER (WHERE task_force = winning_task_force)::INT AS wins,
          COUNT(*) FILTER (WHERE task_force <> winning_task_force)::INT AS losses,
          0::INT AS total_bans,
          COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE task_force = winning_task_force)::NUMERIC
            / NULLIF(COUNT(*) FILTER (WHERE winning_task_force IS NOT NULL)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS win_rate,
          0::DOUBLE PRECISION AS pick_rate,
          0::DOUBLE PRECISION AS ban_rate
        FROM valid_compositions
        GROUP BY comp_id, map_name
        ORDER BY comp_id, win_rate DESC, total_count DESC, map_name ASC`, params);
    }

    reply.header('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
    return { section, rows };
  });

  /**
   * GET /stats/maps/:mapName — Ranked champion, talent, item and composition meta for one map.
   *
   * Map rows carry the ranked prefix/version used by Hi-Rez (for example,
   * "Ranked Stone Keep V2 Night"), so the exact returned map name is used as
   * the detail route identifier.
   */
  fastify.get('/maps/:mapName', async (req: any, reply: any) => {
    const mapName = String(req.params.mapName || '').trim();
    if (!mapName) return reply.status(400).send(err('VALIDATION', 'Map name is required.'));
    const statsScope = String(req.query.scope || 'ranked').trim().toLowerCase();
    if (!isPublicStatsScope(statsScope)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid statistics scope.'));
    }
    if (statsScope !== 'ranked') {
      const [map, champions] = await Promise.all([
        one(`WITH map_counts AS (
            SELECT map,SUM(matches)::bigint AS total_matches,SUM(duration_sum)::bigint AS duration_sum
            FROM nonranked_map_stats_daily
            WHERE stats_scope=$2 AND map<>'Unknown'
            GROUP BY map
          ), totals AS (SELECT SUM(total_matches)::bigint AS total_matches FROM map_counts)
          SELECT mc.map,mc.total_matches,
            COALESCE(ROUND(100.0*mc.total_matches::numeric/NULLIF(t.total_matches,0),2),0) AS distribution_rate,
            COALESCE(ROUND(mc.duration_sum::numeric/NULLIF(mc.total_matches,0),2),0) AS avg_duration_seconds
          FROM map_counts mc CROSS JOIN totals t WHERE mc.map=$1`, [mapName, statsScope]),
        query(`WITH rows AS (
            SELECT champion_id,SUM(plays)::bigint AS total_plays,SUM(wins)::bigint AS wins,SUM(losses)::bigint AS losses
            FROM nonranked_champion_stats_daily
            WHERE map=$1 AND stats_scope=$2
            GROUP BY champion_id
          ), totals AS (SELECT SUM(total_plays)::bigint AS plays FROM rows)
          SELECT r.champion_id,COALESCE(c.name,'Champion '||r.champion_id::text) AS champion_name,
            r.total_plays,r.wins,r.losses,NULL::bigint AS total_bans,
            COALESCE(ROUND(100.0*r.wins::numeric/NULLIF((r.wins+r.losses)::numeric,0),2),0) AS win_rate,
            COALESCE(ROUND(100.0*r.total_plays::numeric/NULLIF(t.plays,0),2),0) AS pick_rate,
            NULL::numeric AS ban_rate
          FROM rows r LEFT JOIN champions c ON c.id=r.champion_id CROSS JOIN totals t
          ORDER BY r.total_plays DESC,champion_name`, [mapName, statsScope]),
      ]);
      if (!map) return reply.status(404).send(err('NOT_FOUND', 'Map statistics not found.'));
      return { map, champions, talents: [], items: [], compositions: [], stats_scope: statsScope };
    }
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    {
      const p:any[]=[mapName,486];
      const tier = (alias:string) => {
        const clauses:string[]=[];
        if (lobbyTier.min!=null) { p.length===2&&p.push(lobbyTier.min); clauses.push(`${alias}.lobby_tier >= $3`); }
        if (lobbyTier.max!=null) {
          const index=lobbyTier.min!=null?4:3;
          while(p.length<index) p.push(lobbyTier.max);
          clauses.push(`${alias}.lobby_tier <= $${index}`);
        }
        return clauses.length?` AND ${clauses.join(' AND ')}`:'';
      };
      const matchTier=tier('sma'); const playerTier=tier('spa'); const talentTier=tier('sta');
      const itemTier=tier('sia'); const banTier=tier('sba'); const compTier=tier('sca');
      const [map,champions,talents,items,compositions]=await Promise.all([
        one<any>(`WITH map_counts AS (
            SELECT map_name,SUM(match_count)::BIGINT AS total_matches,SUM(duration_sum)::BIGINT AS duration_sum
            FROM stats_match_aggregate sma WHERE sma.queue_id=$2 AND sma.map_name<>'Unknown'${matchTier} GROUP BY map_name
          ), all_maps AS (
            SELECT SUM(total_matches)::BIGINT AS total_matches FROM map_counts
          ) SELECT mc.map_name AS map,mc.total_matches,
            COALESCE(ROUND(100.0*mc.total_matches::NUMERIC/NULLIF(am.total_matches,0),2),0) AS distribution_rate,
            COALESCE(ROUND(mc.duration_sum::NUMERIC/NULLIF(mc.total_matches,0),2),0) AS avg_duration_seconds
          FROM map_counts mc CROSS JOIN all_maps am WHERE mc.map_name=$1`,p),
        query(`WITH player_rows AS (
            SELECT spa.champion_id,SUM(spa.plays)::BIGINT AS total_plays,SUM(spa.wins)::BIGINT AS wins,SUM(spa.losses)::BIGINT AS losses
            FROM stats_player_aggregate spa WHERE spa.queue_id=$2 AND spa.map_name=$1${playerTier} GROUP BY spa.champion_id
          ), totals AS (SELECT SUM(total_plays)::BIGINT AS plays FROM player_rows), bans AS (
            SELECT champion_id,SUM(bans)::BIGINT AS total_bans FROM stats_ban_aggregate sba
            WHERE sba.queue_id=$2 AND sba.map_name=$1${banTier} GROUP BY champion_id
          ), matches AS (
            SELECT SUM(match_count)::BIGINT AS count FROM stats_match_aggregate sma WHERE sma.queue_id=$2 AND sma.map_name=$1${matchTier}
          ) SELECT pr.champion_id,c.name AS champion_name,pr.total_plays,pr.wins,pr.losses,
            COALESCE(b.total_bans,0)::BIGINT AS total_bans,
            COALESCE(ROUND(100.0*pr.wins::NUMERIC/NULLIF((pr.wins+pr.losses)::NUMERIC,0),2),0) AS win_rate,
            COALESCE(ROUND(100.0*pr.total_plays::NUMERIC/NULLIF(t.plays,0),2),0) AS pick_rate,
            COALESCE(ROUND(100.0*COALESCE(b.total_bans,0)::NUMERIC/NULLIF(m.count,0),2),0) AS ban_rate
          FROM player_rows pr JOIN champions c ON c.id=pr.champion_id CROSS JOIN totals t CROSS JOIN matches m
          LEFT JOIN bans b ON b.champion_id=pr.champion_id ORDER BY pr.total_plays DESC,c.name`,p),
        query(`WITH rows AS (
            SELECT sta.talent_id,sta.champion_id,SUM(sta.uses)::BIGINT AS total_plays,SUM(sta.wins)::BIGINT AS wins,SUM(sta.losses)::BIGINT AS losses
            FROM stats_talent_aggregate sta WHERE sta.queue_id=$2 AND sta.map_name=$1${talentTier} GROUP BY 1,2
          ), champion_totals AS (
            SELECT champion_id,SUM(plays)::BIGINT AS plays FROM stats_player_aggregate spa
            WHERE spa.queue_id=$2 AND spa.map_name=$1${playerTier} GROUP BY champion_id
          ) SELECT r.talent_id,t.talent_name,r.champion_id,c.name AS champion_name,r.total_plays,r.wins,r.losses,
            COALESCE(ROUND(100.0*r.wins::NUMERIC/NULLIF((r.wins+r.losses)::NUMERIC,0),2),0) AS win_rate,
            COALESCE(ROUND(100.0*r.total_plays::NUMERIC/NULLIF(ct.plays,0),2),0) AS pick_rate
          FROM rows r JOIN talents t ON t.talent_id=r.talent_id AND t.champion_id=r.champion_id JOIN champions c ON c.id=r.champion_id
          JOIN champion_totals ct ON ct.champion_id=r.champion_id ORDER BY r.total_plays DESC,t.talent_name`,p),
        query(`WITH rows AS (
            SELECT sia.item_id,SUM(sia.uses)::BIGINT AS total_uses,SUM(sia.wins)::BIGINT AS wins,SUM(sia.losses)::BIGINT AS losses
            FROM stats_item_aggregate sia WHERE sia.queue_id=$2 AND sia.map_name=$1${itemTier} GROUP BY sia.item_id
          ), total AS (
            SELECT SUM(plays)::BIGINT AS plays FROM stats_player_aggregate spa WHERE spa.queue_id=$2 AND spa.map_name=$1${playerTier}
          ) SELECT r.item_id,COALESCE(i.item_name,'Item '||r.item_id::TEXT) AS item_name,r.total_uses,r.wins,r.losses,
            COALESCE(ROUND(100.0*r.wins::NUMERIC/NULLIF((r.wins+r.losses)::NUMERIC,0),2),0) AS win_rate,
            COALESCE(ROUND(100.0*r.total_uses::NUMERIC/NULLIF(t.plays,0),2),0) AS pick_rate
          FROM rows r LEFT JOIN items i ON i.item_id=r.item_id CROSS JOIN total t ORDER BY r.total_uses DESC,item_name`,p),
        query(`SELECT sca.comp_id,sca.frontline,sca.damage,sca.flank,sca.support,SUM(sca.uses)::BIGINT AS count,
            SUM(sca.wins)::BIGINT AS wins,SUM(sca.losses)::BIGINT AS losses,
            COALESCE(ROUND(100.0*SUM(sca.wins)::NUMERIC/NULLIF((SUM(sca.wins)+SUM(sca.losses))::NUMERIC,0),2),0) AS winrate
          FROM stats_composition_aggregate sca WHERE sca.queue_id=$2 AND sca.map_name=$1${compTier}
          GROUP BY sca.comp_id,sca.frontline,sca.damage,sca.flank,sca.support ORDER BY count DESC,sca.comp_id`,p),
      ]);
      if(!map) return reply.status(404).send(err('NOT_FOUND','Map statistics not found.'));
      return {map,champions,talents,items,compositions};
    }
    const params: any[] = [mapName];
    const mapPredicates = [`m.queue_id = 486`, `COALESCE(NULLIF(m.map, ''), 'Unknown') = $1`];
    appendLobbyTierPredicate(lobbyTier, params, mapPredicates);
    const mapWhere = mapPredicates.join(' AND ');
    const scopeOnly = mapPredicates.slice(2);
    const [map, champions, talents, items, compositions] = await Promise.all([
      one<any>(`WITH ranked_map_counts AS (
          SELECT
            COALESCE(NULLIF(map, ''), 'Unknown') AS map,
            COUNT(*)::INT AS total_matches,
            COALESCE(ROUND(AVG(duration_seconds)::NUMERIC, 2), 0)::DOUBLE PRECISION AS avg_duration_seconds
          FROM matches m
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          WHERE m.queue_id = 486
            AND COALESCE(NULLIF(m.map, ''), 'Unknown') <> 'Unknown'
            ${scopeOnly.length ? `AND ${scopeOnly.join(' AND ')}` : ''}
          GROUP BY COALESCE(NULLIF(m.map, ''), 'Unknown')
        ), ranked_map_total AS (
          SELECT SUM(total_matches) AS total_matches FROM ranked_map_counts
        )
        SELECT
          mc.map,
          mc.total_matches,
          COALESCE(ROUND(100.0 * mc.total_matches::NUMERIC / NULLIF(mt.total_matches, 0), 2), 0)::DOUBLE PRECISION AS distribution_rate,
          mc.avg_duration_seconds
        FROM ranked_map_counts mc
        CROSS JOIN ranked_map_total mt
        WHERE mc.map = $1`, params),
      query(`WITH map_matches AS (
          SELECT m.match_id FROM matches m JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime WHERE ${mapWhere}
        ), map_players AS (
          SELECT mp.match_id, mp.player_id, mp.champion_id, mp.win_status
          FROM match_players mp JOIN map_matches mm ON mm.match_id = mp.match_id
        ), champion_plays AS (
          SELECT champion_id, COUNT(*)::INT AS total_plays,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses
          FROM map_players mp GROUP BY champion_id
        ), bans AS (
          SELECT mb.champion_id, COUNT(*)::INT AS total_bans
          FROM match_bans mb JOIN map_matches mm ON mm.match_id = mb.match_id
          GROUP BY mb.champion_id
        ) SELECT cp.champion_id, c.name AS champion_name, cp.total_plays, cp.wins, cp.losses,
            COALESCE(b.total_bans, 0)::INT AS total_bans,
            COALESCE(ROUND(100.0 * cp.wins::NUMERIC / NULLIF((cp.wins + cp.losses)::NUMERIC, 0), 2), 0) AS win_rate,
            COALESCE(ROUND(100.0 * cp.total_plays::NUMERIC / NULLIF((SELECT COUNT(*) FROM map_players)::NUMERIC, 0), 2), 0) AS pick_rate,
            COALESCE(ROUND(100.0 * COALESCE(b.total_bans, 0)::NUMERIC / NULLIF((SELECT COUNT(*) FROM map_matches)::NUMERIC, 0), 2), 0) AS ban_rate
          FROM champion_plays cp
          JOIN champions c ON c.id = cp.champion_id
          LEFT JOIN bans b ON b.champion_id = cp.champion_id
          ORDER BY cp.total_plays DESC, c.name ASC`, params),
      query(`WITH map_matches AS (
          SELECT m.match_id FROM matches m JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime WHERE ${mapWhere}
        ), champion_plays AS (
          SELECT mp.champion_id, COUNT(*)::INT AS total_plays
          FROM match_players mp JOIN map_matches mm ON mm.match_id = mp.match_id
          GROUP BY mp.champion_id
        ) SELECT t.talent_id, t.talent_name, t.champion_id, c.name AS champion_name,
            COUNT(*)::INT AS total_plays,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses,
            COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2), 0) AS win_rate,
            COALESCE(ROUND(100.0 * COUNT(*)::NUMERIC / NULLIF(cp.total_plays::NUMERIC, 0), 2), 0) AS pick_rate
          FROM match_player_talents mpt
          JOIN match_players mp ON mp.match_id = mpt.match_id AND mp.player_id = mpt.player_id
          JOIN map_matches mm ON mm.match_id = mpt.match_id
          JOIN talents t ON t.talent_id = mpt.talent_id AND t.champion_id = mp.champion_id
          JOIN champions c ON c.id = t.champion_id
          JOIN champion_plays cp ON cp.champion_id = t.champion_id
          GROUP BY t.talent_id, t.talent_name, t.champion_id, c.name, cp.total_plays
          ORDER BY total_plays DESC, t.talent_name ASC`, params),
      query(`WITH map_matches AS (
          SELECT m.match_id FROM matches m JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime WHERE ${mapWhere}
        ), map_players AS (
          SELECT mp.match_id, mp.player_id, mp.win_status
          FROM match_players mp JOIN map_matches mm ON mm.match_id = mp.match_id
        ) SELECT mpi.item_id, MAX(i.item_name) AS item_name, COUNT(*)::INT AS total_uses,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
            COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses,
            COALESCE(ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2), 0) AS win_rate,
            COALESCE(ROUND(100.0 * COUNT(*)::NUMERIC / NULLIF((SELECT COUNT(*) FROM map_players)::NUMERIC, 0), 2), 0) AS pick_rate
          FROM match_player_items mpi
          JOIN map_players mp ON mp.match_id = mpi.match_id AND mp.player_id = mpi.player_id
          JOIN items i ON i.item_id = mpi.item_id
          GROUP BY mpi.item_id
          ORDER BY total_uses DESC, item_name ASC`, params),
      query(`WITH map_players AS (
          SELECT mp.match_id, mp.task_force, m.winning_task_force,
            ${championRoleSql('c')} AS champion_role
          FROM match_players mp
          JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          JOIN champions c ON c.id = mp.champion_id
          WHERE ${mapWhere}
            AND mp.task_force IS NOT NULL
            AND mp.task_force <> 0
            AND mp.champion_id > 0
            AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
        ), team_compositions AS (
          SELECT match_id, task_force, winning_task_force,
            COUNT(*) FILTER (WHERE champion_role = 'Frontline')::SMALLINT AS frontline,
            COUNT(*) FILTER (WHERE champion_role = 'Damage')::SMALLINT AS damage,
            COUNT(*) FILTER (WHERE champion_role = 'Flank')::SMALLINT AS flank,
            COUNT(*) FILTER (WHERE champion_role = 'Support')::SMALLINT AS support
          FROM map_players
          GROUP BY match_id, task_force, winning_task_force
          HAVING COUNT(*) = 5
        ), valid_compositions AS (
          SELECT *, frontline || '-' || damage || '-' || flank || '-' || support AS comp_id
          FROM team_compositions
          WHERE frontline + damage + flank + support = 5
        )
        SELECT comp_id, frontline, damage, flank, support,
          COUNT(*)::INT AS count,
          COUNT(*) FILTER (WHERE task_force = winning_task_force)::INT AS wins,
          COUNT(*) FILTER (WHERE task_force <> winning_task_force)::INT AS losses,
          COALESCE(ROUND(
            100.0 * COUNT(*) FILTER (WHERE task_force = winning_task_force)::NUMERIC
            / NULLIF(COUNT(*) FILTER (WHERE winning_task_force IS NOT NULL)::NUMERIC, 0),
            2
          ), 0) AS winrate
        FROM valid_compositions
        GROUP BY comp_id, frontline, damage, flank, support
        ORDER BY count DESC, comp_id ASC`, params),
    ]);

    if (!map) return reply.status(404).send(err('NOT_FOUND', 'Map statistics not found.'));
    return { map, champions, talents, items, compositions };
  });

  /**
   * GET /stats/hourly-match-counts — Public hourly match activity.
   *
   * Query params:
   *   ?date=     — UTC date as YYYYMMDD (default: today UTC)
   *   ?hour=     — Optional UTC hour 0-23
   *   ?queueId=  — Optional queue filter
   *
   * Returns hourly rows from hourly_match_counts without requiring admin auth.
   */
  fastify.get('/hourly-match-counts', async (req: any, reply: any) => {
    const todayUtc = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    const date = String(req.query.date || todayUtc);
    if (!/^\d{8}$/.test(date)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid date. Use YYYYMMDD.'));
    }

    const params: any[] = [date, 486];
    const where = ['date = $1', 'queue_id = $2'];

    if (req.query.hour != null) {
      const hour = parseInt(req.query.hour as string, 10);
      if (!Number.isInteger(hour) || hour < 0 || hour > 23) {
        return reply.status(400).send(err('VALIDATION', 'Invalid hour. Use 0-23.'));
      }
      params.push(hour);
      where.push(`hour = $${params.length}`);
    }

    if (req.query.queueId != null) {
      const queueId = parseInt(req.query.queueId as string, 10);
      if (queueId !== 486) {
        return reply.status(400).send(err('VALIDATION', 'Only ranked queue 486 is available for aggregate statistics.'));
      }
    }

    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    if (lobbyTier.active) {
      const rawParams: any[] = [date];
      const rawWhere = [`m.queue_id = 486`, `m.entry_datetime >= to_date($1, 'YYYYMMDD')`, `m.entry_datetime < to_date($1, 'YYYYMMDD') + INTERVAL '1 day'`];
      if (req.query.hour != null) {
        rawParams.push(parseInt(req.query.hour as string, 10));
        rawWhere.push(`EXTRACT(HOUR FROM m.entry_datetime AT TIME ZONE 'UTC') = $${rawParams.length}`);
      }
      appendLobbyTierPredicate(lobbyTier, rawParams, rawWhere);
      return query(`SELECT to_char(m.entry_datetime AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS date,
          EXTRACT(HOUR FROM m.entry_datetime AT TIME ZONE 'UTC')::INT AS hour, 486 AS queue_id,
          COUNT(*) FILTER (WHERE m.region = 'NA')::INT AS matches_na,
          COUNT(*) FILTER (WHERE m.region = 'EU')::INT AS matches_eu,
          COUNT(*) FILTER (WHERE m.region = 'Asia')::INT AS matches_asia,
          COUNT(*) FILTER (WHERE m.region = 'SEA')::INT AS matches_sea,
          COUNT(*) FILTER (WHERE m.region = 'JPN')::INT AS matches_jpn,
          COUNT(*) FILTER (WHERE m.region = 'BR')::INT AS matches_br,
          COUNT(*) FILTER (WHERE m.region = 'OCE')::INT AS matches_oce,
          COUNT(*) FILTER (WHERE m.region = 'SA')::INT AS matches_sa,
          COUNT(*) FILTER (WHERE m.region IS NULL OR m.region NOT IN ('NA','EU','Asia','SEA','JPN','BR','OCE','SA'))::INT AS matches_unknown,
          COUNT(*)::INT AS total_matches, now() AS fetched_at
        FROM matches m JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        WHERE ${rawWhere.join(' AND ')} GROUP BY 1, 2 ORDER BY hour DESC`, rawParams);
    }

    return query(
      `SELECT
        date::TEXT AS date,
        hour,
        queue_id,
        matches_na,
        matches_eu,
        matches_asia,
        matches_br,
        matches_oce,
        matches_sa,
        matches_unknown,
        total_matches,
        fetched_at
       FROM hourly_match_counts
       WHERE ${where.join(' AND ')}
       ORDER BY hour DESC, queue_id ASC`,
      params
    );
  });

  /**
   * GET /stats/talents — Talent aggregate stats (global).
   *
   * Query params:
   *   ?mode=     — Optional; only "ranked" is accepted
   *   ?limit=    — Max results (default: 50)
   */
  fastify.get('/talents', async (req: any, reply: any) => {
    if (!requireRankedStatsMode(req, reply)) return;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    const projectionParams: any[] = [486];
    const projectionWhere = ['sta.queue_id = $1'];
    appendLobbyTierPredicate(lobbyTier, projectionParams, projectionWhere, 'sta');
    projectionParams.push(limit);
    return query(`SELECT sta.talent_id,
        COALESCE(t.talent_name,'Talent '||sta.talent_id::TEXT) AS name,
        COALESCE(t.talent_name,'Talent '||sta.talent_id::TEXT) AS talent_name,
        sta.champion_id,COALESCE(c.name,'Unknown') AS champion_name,
        SUM(sta.uses)::BIGINT AS total_uses,SUM(sta.uses)::BIGINT AS total_plays,
        COALESCE(ROUND(100.0*SUM(sta.wins)::NUMERIC/NULLIF((SUM(sta.wins)+SUM(sta.losses))::NUMERIC,0),2),0) AS win_rate,
        ROUND(SUM(sta.kills_sum)::NUMERIC/NULLIF(SUM(sta.uses),0),2) AS avg_kills,
        ROUND(SUM(sta.deaths_sum)::NUMERIC/NULLIF(SUM(sta.uses),0),2) AS avg_deaths,
        ROUND(SUM(sta.assists_sum)::NUMERIC/NULLIF(SUM(sta.uses),0),2) AS avg_assists
      FROM stats_talent_aggregate sta JOIN talents t ON t.talent_id=sta.talent_id AND t.champion_id=sta.champion_id
      LEFT JOIN champions c ON c.id=sta.champion_id
      WHERE ${projectionWhere.join(' AND ')}
      GROUP BY sta.talent_id,t.talent_name,sta.champion_id,c.name
      ORDER BY total_uses DESC,talent_name ASC LIMIT $${projectionParams.length}`,projectionParams);

    // Ingestion and derived-projection repair already maintain one compact row
    // per ranked talent. The default talents page must read that projection;
    // regrouping every historical talent/player/match fact exceeds the browser
    // timeout on a cold route-cache miss.
    if (!lobbyTier.active) {
      return query(`SELECT
        tcr.talent_id,
        COALESCE(t.talent_name, tcr.talent_name, 'Talent ' || tcr.talent_id::TEXT) AS name,
        COALESCE(t.talent_name, tcr.talent_name, 'Talent ' || tcr.talent_id::TEXT) AS talent_name,
        t.champion_id,
        COALESCE(c.name, tcr.champion_name, 'Unknown') AS champion_name,
        tcr.count AS total_uses,
        tcr.count AS total_plays,
        COALESCE(ROUND(100.0 * tcr.wins::NUMERIC / NULLIF((tcr.wins + tcr.losses)::NUMERIC, 0), 2), 0) AS win_rate,
        NULL::NUMERIC AS avg_kills,
        NULL::NUMERIC AS avg_deaths,
        NULL::NUMERIC AS avg_assists
        FROM talent_counts_ranked tcr
        LEFT JOIN talents t ON t.talent_id = tcr.talent_id
        LEFT JOIN champions c ON c.id = t.champion_id
        ORDER BY tcr.count DESC, talent_name ASC
        LIMIT $1`, [limit]);
    }

    const params: any[] = [];
    const where = ['m.queue_id = 486'];
    appendLobbyTierPredicate(lobbyTier, params, where);
    params.push(limit);

    return query(`SELECT
      mpt.talent_id,
      COALESCE(t.talent_name, 'Talent ' || mpt.talent_id::TEXT) AS name,
      COALESCE(t.talent_name, 'Talent ' || mpt.talent_id::TEXT) AS talent_name,
      t.champion_id,
      COALESCE(c.name, 'Unknown') AS champion_name,
      COUNT(*) AS total_uses,
      COUNT(*) AS total_plays,
      ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2) as win_rate,
      ROUND(AVG(mp.kills)::NUMERIC, 2) as avg_kills,
      ROUND(AVG(mp.deaths)::NUMERIC, 2) as avg_deaths,
      ROUND(AVG(mp.assists)::NUMERIC, 2) as avg_assists
      FROM match_player_talents mpt
      JOIN match_players mp ON mp.match_id = mpt.match_id AND mp.player_id = mpt.player_id
      JOIN matches m ON m.match_id = mpt.match_id
      JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      LEFT JOIN talents t ON t.talent_id = mpt.talent_id
      LEFT JOIN champions c ON c.id = t.champion_id
      WHERE ${where.join(' AND ')}
      GROUP BY mpt.talent_id, t.champion_id, COALESCE(c.name, 'Unknown'), COALESCE(t.talent_name, 'Talent ' || mpt.talent_id::TEXT) ORDER BY total_uses DESC LIMIT $${params.length}`,
      params
    );
  });

  /**
   * GET /stats/talents/:championId — Champion talent stats.
   *
   * LEFT JOINs from the reference `talents` table so every talent appears,
   * even with zero plays. The frontend uses this for the champion detail page.
   *
   * Query params:
   *   ?mode=     — Optional; only "ranked" is accepted
   *
   * Returns: { totalMatches, talents: { talentId, talentName, totalPlays, wins, losses, winRate }[] }
   */
  fastify.get('/talents/:championId', async (req: any, reply: any) => {
    const championId = parseInt(req.params.championId as string, 10);
    if (!Number.isInteger(championId) || championId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
    }
    if (!requireRankedStatsMode(req, reply)) return;
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const params: any[] = [championId];
    {
      const p:any[]=[championId,486];
      const playerWhere=['spa.champion_id=$1','spa.queue_id=$2'];
      const talentWhere=['sta.champion_id=$1','sta.queue_id=$2'];
      if(lobbyTier.min!=null){p.push(lobbyTier.min);playerWhere.push(`spa.lobby_tier >= $${p.length}`);talentWhere.push(`sta.lobby_tier >= $${p.length}`);}
      if(lobbyTier.max!=null){p.push(lobbyTier.max);playerWhere.push(`spa.lobby_tier <= $${p.length}`);talentWhere.push(`sta.lobby_tier <= $${p.length}`);}
      const [coverage,talents]=await Promise.all([
        one<any>(`WITH players AS (
            SELECT SUM(plays)::BIGINT AS total,SUM(wins)::BIGINT AS wins,SUM(losses)::BIGINT AS losses
            FROM stats_player_aggregate spa WHERE ${playerWhere.join(' AND ')}
          ), covered AS (
            SELECT SUM(uses)::BIGINT AS total,SUM(wins)::BIGINT AS wins,SUM(losses)::BIGINT AS losses
            FROM stats_talent_aggregate sta
            JOIN talents t ON t.talent_id=sta.talent_id AND t.champion_id=sta.champion_id
            WHERE ${talentWhere.join(' AND ')}
          ) SELECT COALESCE(p.total,0)::BIGINT AS total,COALESCE(c.total,0)::BIGINT AS talent_covered,
            GREATEST(COALESCE(p.total,0)-COALESCE(c.total,0),0)::BIGINT AS disconnected_players,
            GREATEST(COALESCE(p.wins,0)-COALESCE(c.wins,0),0)::BIGINT AS disconnected_wins,
            GREATEST(COALESCE(p.losses,0)-COALESCE(c.losses,0),0)::BIGINT AS disconnected_losses
          FROM players p CROSS JOIN covered c`,p),
        query(`SELECT t.talent_id AS "talentId",t.talent_name AS "talentName",
            COALESCE(SUM(sta.uses),0)::BIGINT AS "totalPlays",COALESCE(SUM(sta.wins),0)::BIGINT AS wins,
            COALESCE(SUM(sta.losses),0)::BIGINT AS losses,
            ROUND(100.0*COALESCE(SUM(sta.wins),0)::NUMERIC/NULLIF((COALESCE(SUM(sta.wins),0)+COALESCE(SUM(sta.losses),0))::NUMERIC,0),2) AS "winRate"
          FROM talents t LEFT JOIN stats_talent_aggregate sta ON sta.talent_id=t.talent_id AND ${talentWhere.join(' AND ')}
          WHERE t.champion_id=$1 GROUP BY t.talent_id,t.talent_name ORDER BY "totalPlays" DESC`,p),
      ]);
      const totalMatches=Number(coverage?.total??0),talentCoveredMatches=Number(coverage?.talent_covered??0);
      const disconnectedPlayers=Number(coverage?.disconnected_players??0),disconnectedWins=Number(coverage?.disconnected_wins??0),disconnectedLosses=Number(coverage?.disconnected_losses??0);
      return {totalMatches,talentCoveredMatches,disconnectedPlayers,disconnectedWins,disconnectedLosses,
        disconnectedWinRate:disconnectedWins+disconnectedLosses>0?Number((100*disconnectedWins/(disconnectedWins+disconnectedLosses)).toFixed(2)):null,
        talentCoverageRate:totalMatches>0?Number((100*talentCoveredMatches/totalMatches).toFixed(2)):null,talents};
    }
    const tierWhere: string[] = [];
    appendLobbyTierPredicate(lobbyTier, params, tierWhere);
    const queueClause = ` AND m.queue_id = 486${tierWhere.length ? ` AND ${tierWhere.join(' AND ')}` : ''}`;

    // Total champion rows for this mode. Rows with no matching talent fact are
    // tracked as disconnected/no-loadout coverage debt: Hi-Rez can return the
    // player/champion but no selected talent/card/item when the player never
    // fully joins or disconnects before loadout selection. They are not assigned
    // to a fake talent, but the response exposes the count so the UI/audits can
    // explain why talent picks do not sum to 100% of champion plays.
    const matchCount = await one<{
      total: number;
      talent_covered: number;
      disconnected_players: number;
      disconnected_wins: number;
      disconnected_losses: number;
    }>(
      `SELECT
         COUNT(*)::INT AS total,
         COUNT(*) FILTER (WHERE mpt.talent_id IS NOT NULL)::INT AS talent_covered,
         COUNT(*) FILTER (WHERE mpt.talent_id IS NULL)::INT AS disconnected_players,
         COUNT(*) FILTER (WHERE mpt.talent_id IS NULL AND ${SQL_NORMALIZED_WIN})::INT AS disconnected_wins,
         COUNT(*) FILTER (WHERE mpt.talent_id IS NULL AND ${SQL_NORMALIZED_LOSS})::INT AS disconnected_losses
       FROM match_players mp
       JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
       JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
       LEFT JOIN match_player_talents mpt ON mpt.match_id = mp.match_id AND mpt.player_id = mp.player_id
       WHERE mp.champion_id = $1${queueClause}`,
      params
    );

    // Talent stats — LEFT JOIN from reference table so zero-play talents appear
    const talents = await query(`
      SELECT
        t.talent_id AS "talentId",
        t.talent_name AS "talentName",
        COALESCE(SUM(raw.count), 0)::INT AS "totalPlays",
        COALESCE(SUM(raw.wins), 0)::INT AS "wins",
        COALESCE(SUM(raw.losses), 0)::INT AS "losses",
        ROUND(100.0 * COALESCE(SUM(raw.wins), 0)::NUMERIC / NULLIF((COALESCE(SUM(raw.wins), 0) + COALESCE(SUM(raw.losses), 0))::NUMERIC, 0), 2)::DOUBLE PRECISION AS "winRate"
      FROM talents t
      LEFT JOIN LATERAL (
        SELECT COUNT(*)::INT AS count,
               COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
               COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses
        FROM match_player_talents mpt
        JOIN match_players mp ON mp.match_id = mpt.match_id AND mp.player_id = mpt.player_id
        JOIN matches m ON m.match_id = mpt.match_id
        JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        WHERE mpt.talent_id = t.talent_id${queueClause}
      ) raw ON true
      WHERE t.champion_id = $1
      GROUP BY t.talent_id, t.talent_name
      ORDER BY "totalPlays" DESC
    `, params);

    const totalMatches = matchCount?.total ?? 0;
    const talentCoveredMatches = matchCount?.talent_covered ?? 0;
    const disconnectedPlayers = matchCount?.disconnected_players ?? 0;
    const disconnectedWins = matchCount?.disconnected_wins ?? 0;
    const disconnectedLosses = matchCount?.disconnected_losses ?? 0;

    return {
      totalMatches,
      talentCoveredMatches,
      disconnectedPlayers,
      disconnectedWins,
      disconnectedLosses,
      disconnectedWinRate: disconnectedWins + disconnectedLosses > 0
        ? Number(((disconnectedWins / (disconnectedWins + disconnectedLosses)) * 100).toFixed(2))
        : null,
      talentCoverageRate: totalMatches > 0
        ? Number(((talentCoveredMatches / totalMatches) * 100).toFixed(2))
        : null,
      talents,
    };
  });

  /**
   * GET /stats/cards — Card aggregate stats (global).
   *
   * Query params:
   *   ?mode=     — Optional; only "ranked" is accepted
   *   ?limit=    — Max results (default: 50)
   */
  fastify.get('/cards', async (req: any, reply: any) => {
    if (!requireRankedStatsMode(req, reply)) return;
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    const projectionParams: any[] = [486];
    const projectionWhere = ['sca.queue_id = $1'];
    appendLobbyTierPredicate(lobbyTier, projectionParams, projectionWhere, 'sca');
    projectionParams.push(limit);
    return query(`SELECT sca.card_id,COALESCE(c.card_name,'Card '||sca.card_id::TEXT) AS name,
        SUM(sca.uses)::BIGINT AS total_uses,
        COALESCE(ROUND(100.0*SUM(sca.wins)::NUMERIC/NULLIF((SUM(sca.wins)+SUM(sca.losses))::NUMERIC,0),2),0) AS win_rate,
        ROUND(SUM(sca.kills_sum)::NUMERIC/NULLIF(SUM(sca.uses),0),2) AS avg_kills,
        ROUND(SUM(sca.deaths_sum)::NUMERIC/NULLIF(SUM(sca.uses),0),2) AS avg_deaths,
        ROUND(SUM(sca.assists_sum)::NUMERIC/NULLIF(SUM(sca.uses),0),2) AS avg_assists
      FROM stats_card_aggregate sca LEFT JOIN cards c ON c.card_id=sca.card_id
      WHERE ${projectionWhere.join(' AND ')} GROUP BY sca.card_id,c.card_name
      ORDER BY total_uses DESC,name ASC LIMIT $${projectionParams.length}`,projectionParams);

    if (!lobbyTier.active) {
      return query(`SELECT
        ccr.card_id,
        COALESCE(c.card_name, MAX(ccr.card_name), 'Card ' || ccr.card_id::TEXT) AS name,
        SUM(ccr.count)::INT AS total_uses,
        COALESCE(ROUND(100.0 * SUM(ccr.wins)::NUMERIC / NULLIF((SUM(ccr.wins) + SUM(ccr.losses))::NUMERIC, 0), 2), 0) AS win_rate,
        NULL::NUMERIC AS avg_kills,
        NULL::NUMERIC AS avg_deaths,
        NULL::NUMERIC AS avg_assists
        FROM card_counts_ranked ccr
        LEFT JOIN cards c ON c.card_id = ccr.card_id
        GROUP BY ccr.card_id, c.card_name
        ORDER BY total_uses DESC, name ASC
        LIMIT $1`, [limit]);
    }

    const params: any[] = [];
    const where = ['m.queue_id = 486'];
    appendLobbyTierPredicate(lobbyTier, params, where);
    params.push(limit);

    return query(`SELECT mpc.card_id, COALESCE(c.card_name, 'Card ' || mpc.card_id::TEXT) AS name, COUNT(*) as total_uses,
      ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2) as win_rate,
      ROUND(AVG(mp.kills)::NUMERIC, 2) as avg_kills,
      ROUND(AVG(mp.deaths)::NUMERIC, 2) as avg_deaths,
      ROUND(AVG(mp.assists)::NUMERIC, 2) as avg_assists
      FROM match_player_cards mpc
      JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
      JOIN matches m ON m.match_id = mpc.match_id
      JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      LEFT JOIN cards c ON c.card_id = mpc.card_id
      WHERE ${where.join(' AND ')}
      GROUP BY mpc.card_id, COALESCE(c.card_name, 'Card ' || mpc.card_id::TEXT) ORDER BY total_uses DESC LIMIT $${params.length}`,
      params
    );
  });

  /**
   * GET /stats/cards/:championId — Champion card stats.
   *
   * LEFT JOINs from the reference `cards` table so every card appears,
   * even with zero plays. Includes per-level breakdown for level distribution.
   *
   * Query params:
   *   ?mode=     — Optional; only "ranked" is accepted
   *   ?talentId= — Optional selected talent. When present, card totals, win
   *                 rates, and level rows are limited to players on this
   *                 champion who used that talent. This powers the champion
   *                 page talent filter without touching Hi-Rez.
   *
   * Returns: { totalMatches, talentId, cards: { cardId, cardName, totalPlays, wins, losses, winRate, levels: { level, plays, winRate }[] }[] }
   */
  fastify.get('/cards/:championId', async (req: any, reply: any) => {
    const championId = parseInt(req.params.championId as string, 10);
    if (!Number.isInteger(championId) || championId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
    }
    if (!requireRankedStatsMode(req, reply)) return;
    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const selectedTalentId = req.query.talentId != null ? parseInt(String(req.query.talentId), 10) : null;
    if (selectedTalentId != null && (!Number.isInteger(selectedTalentId) || selectedTalentId <= 0)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid talentId.'));
    }
    const params: any[] = selectedTalentId ? [championId, selectedTalentId] : [championId];
    {
      const p:any[]=[championId,486];
      const sourceAlias=selectedTalentId==null?'sca':'stca';
      const sourceTable=selectedTalentId==null?'stats_card_aggregate':'stats_talent_card_aggregate';
      const where=[`${sourceAlias}.champion_id=$1`,`${sourceAlias}.queue_id=$2`];
      if(selectedTalentId!=null){p.push(selectedTalentId);where.push(`stca.talent_id=$3`);}
      if(lobbyTier.min!=null){p.push(lobbyTier.min);where.push(`${sourceAlias}.lobby_tier >= $${p.length}`);}
      if(lobbyTier.max!=null){p.push(lobbyTier.max);where.push(`${sourceAlias}.lobby_tier <= $${p.length}`);}
      const denominatorTable=selectedTalentId==null?'stats_player_aggregate':'stats_talent_aggregate';
      const denominatorAlias=selectedTalentId==null?'spa':'sta';
      const denominatorWhere=[`${denominatorAlias}.champion_id=$1`,`${denominatorAlias}.queue_id=$2`];
      if(selectedTalentId!=null)denominatorWhere.push(`${denominatorAlias}.talent_id=$3`);
      const tierStart=selectedTalentId==null?3:4;
      if(lobbyTier.min!=null)denominatorWhere.push(`${denominatorAlias}.lobby_tier >= $${tierStart}`);
      if(lobbyTier.max!=null)denominatorWhere.push(`${denominatorAlias}.lobby_tier <= $${tierStart+(lobbyTier.min!=null?1:0)}`);
      const [matchCount,levelStats,cardRows]=await Promise.all([
        one<any>(`SELECT COALESCE(SUM(${selectedTalentId==null?'plays':'uses'}),0)::BIGINT AS total
          FROM ${denominatorTable} ${denominatorAlias} WHERE ${denominatorWhere.join(' AND ')}`,p),
        query<any>(`SELECT ${sourceAlias}.card_id,${sourceAlias}.card_level,SUM(${sourceAlias}.uses)::BIGINT AS plays,
            SUM(${sourceAlias}.wins)::BIGINT AS wins,SUM(${sourceAlias}.losses)::BIGINT AS losses,
            COALESCE(ROUND(100.0*SUM(${sourceAlias}.wins)::NUMERIC/NULLIF((SUM(${sourceAlias}.wins)+SUM(${sourceAlias}.losses))::NUMERIC,0),2),0) AS "winRate"
          FROM ${sourceTable} ${sourceAlias} WHERE ${where.join(' AND ')} GROUP BY 1,2 ORDER BY 1,2`,p),
        query<any>(`SELECT c.card_id AS "cardId",c.card_name AS "cardName",COALESCE(SUM(raw.uses),0)::BIGINT AS "totalPlays",
            COALESCE(SUM(raw.wins),0)::BIGINT AS wins,COALESCE(SUM(raw.losses),0)::BIGINT AS losses,
            COALESCE(ROUND(100.0*SUM(raw.wins)::NUMERIC/NULLIF((SUM(raw.wins)+SUM(raw.losses))::NUMERIC,0),2),0) AS "winRate"
          FROM cards c LEFT JOIN ${sourceTable} raw ON raw.card_id=c.card_id AND ${where.map((condition)=>condition.replaceAll(`${sourceAlias}.`,'raw.')).join(' AND ')}
          WHERE c.champion_id=$1 GROUP BY c.card_id,c.card_name ORDER BY "totalPlays" DESC,c.card_name`,p),
      ]);
      const levelMap=new Map<number,any[]>();
      for(const row of levelStats){if(!levelMap.has(row.card_id))levelMap.set(row.card_id,[]);levelMap.get(row.card_id)!.push({level:row.card_level,plays:row.plays,wins:row.wins,losses:row.losses,winRate:row.winRate??0});}
      const deduped=new Map<string,any>();
      for(const row of cardRows){const key=String(row.cardName??'').normalize('NFKD').toLowerCase().replace(/[^a-z0-9]/g,'');const value={...row,levels:levelMap.get(row.cardId)??[]};const existing=deduped.get(key);if(!existing||Number(value.totalPlays)>Number(existing.totalPlays))deduped.set(key,value);}
      const cards=[...deduped.values()].sort((a,b)=>Number(b.totalPlays)-Number(a.totalPlays)||String(a.cardName).localeCompare(String(b.cardName)));
      return {totalMatches:matchCount?.total??0,talentId:selectedTalentId,cards};
    }
    const tierWhere: string[] = [];
    appendLobbyTierPredicate(lobbyTier, params, tierWhere);
    const queueClause = ` AND m.queue_id = 486${tierWhere.length ? ` AND ${tierWhere.join(' AND ')}` : ''}`;
    const talentJoin = selectedTalentId
      ? ' JOIN match_player_talents mpt_filter ON mpt_filter.match_id = mp.match_id AND mpt_filter.player_id = mp.player_id AND mpt_filter.talent_id = $2'
      : '';

    let matchCount: { total: number } | null;
    let levelStats: any[];
    let cardRows: any[];

    if (!lobbyTier.active) {
      // Default champion-card pages use projections maintained during ingest.
      // Keep reference-table LEFT JOINs so zero-play and legacy cards remain
      // visible, but never rescan match_player_cards for optional decoration.
      if (selectedTalentId == null) {
        [matchCount, levelStats, cardRows] = await Promise.all([
          one<{ total: number }>(
            'SELECT total_matches::INT AS total FROM champion_stats_ranked WHERE champion_id = $1',
            [championId],
          ),
          query(`SELECT
            ccr.card_id,
            ccr.card_level,
            SUM(ccr.count)::INT AS plays,
            SUM(ccr.wins)::INT AS wins,
            SUM(ccr.losses)::INT AS losses,
            COALESCE(ROUND(100.0 * SUM(ccr.wins)::NUMERIC / NULLIF((SUM(ccr.wins) + SUM(ccr.losses))::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS "winRate"
            FROM card_counts_ranked ccr
            JOIN cards c ON c.card_id = ccr.card_id
            WHERE c.champion_id = $1
            GROUP BY ccr.card_id, ccr.card_level
            ORDER BY ccr.card_id, ccr.card_level`, [championId]),
          query(`SELECT
            c.card_id AS "cardId",
            c.card_name AS "cardName",
            COALESCE(raw.count, 0)::INT AS "totalPlays",
            COALESCE(raw.wins, 0)::INT AS "wins",
            COALESCE(raw.losses, 0)::INT AS "losses",
            COALESCE(ROUND(100.0 * raw.wins::NUMERIC / NULLIF((raw.wins + raw.losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS "winRate"
            FROM cards c
            LEFT JOIN (
              SELECT card_id,
                SUM(count)::INT AS count,
                SUM(wins)::INT AS wins,
                SUM(losses)::INT AS losses
              FROM card_counts_ranked
              GROUP BY card_id
            ) raw ON raw.card_id = c.card_id
            WHERE c.champion_id = $1
            ORDER BY "totalPlays" DESC, c.card_name ASC`, [championId]),
        ]);
      } else {
        [matchCount, levelStats, cardRows] = await Promise.all([
          one<{ total: number }>(`SELECT tcr.count::INT AS total
            FROM talent_counts_ranked tcr
            JOIN talents t ON t.talent_id = tcr.talent_id
            WHERE t.champion_id = $1 AND tcr.talent_id = $2`, [championId, selectedTalentId]),
          query(`SELECT
            tcc.card_id,
            tcc.card_level,
            tcc.count::INT AS plays,
            tcc.wins::INT AS wins,
            tcc.losses::INT AS losses,
            COALESCE(ROUND(100.0 * tcc.wins::NUMERIC / NULLIF((tcc.wins + tcc.losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS "winRate"
            FROM talent_card_counts_ranked tcc
            JOIN cards c ON c.card_id = tcc.card_id
            WHERE c.champion_id = $1 AND tcc.talent_id = $2
            ORDER BY tcc.card_id, tcc.card_level`, [championId, selectedTalentId]),
          query(`SELECT
            c.card_id AS "cardId",
            c.card_name AS "cardName",
            COALESCE(raw.count, 0)::INT AS "totalPlays",
            COALESCE(raw.wins, 0)::INT AS "wins",
            COALESCE(raw.losses, 0)::INT AS "losses",
            COALESCE(ROUND(100.0 * raw.wins::NUMERIC / NULLIF((raw.wins + raw.losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS "winRate"
            FROM cards c
            LEFT JOIN (
              SELECT card_id,
                SUM(count)::INT AS count,
                SUM(wins)::INT AS wins,
                SUM(losses)::INT AS losses
              FROM talent_card_counts_ranked
              WHERE talent_id = $2
              GROUP BY card_id
            ) raw ON raw.card_id = c.card_id
            WHERE c.champion_id = $1
            ORDER BY "totalPlays" DESC, c.card_name ASC`, [championId, selectedTalentId]),
        ]);
      }
    } else {
      // The denominator follows the selected view. In global mode it is all
      // champion player rows for the queue; in talent mode it is only rows where
      // the selected talent was actually present. Players with no talent/card rows
      // are disconnected/no-loadout rows and intentionally excluded from a selected
      // talent view rather than being assigned to fake data.
      matchCount = await one<{ total: number }>(
        `SELECT COUNT(*)::INT AS total
         FROM match_players mp
         JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
         JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
         ${talentJoin}
         WHERE mp.champion_id = $1${queueClause}`,
        params
      );

      // Card-level stats from raw facts. The optional talent join is on the same
      // match/player identity as the card fact, so selecting a talent shows level
      // performance for this exact champion+talent pairing.
      levelStats = await query(`
        SELECT mpc.card_id, mpc.card_level,
          COUNT(*)::INT AS plays,
          COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
          COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses,
          ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2)::DOUBLE PRECISION AS "winRate"
        FROM match_player_cards mpc
        JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
        JOIN matches m ON m.match_id = mpc.match_id
        JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        JOIN cards c ON c.card_id = mpc.card_id
        ${talentJoin}
        WHERE c.champion_id = $1 AND mp.champion_id = $1${queueClause}
        GROUP BY mpc.card_id, mpc.card_level
        ORDER BY mpc.card_id, mpc.card_level
      `, params);

      // Overall card stats — LEFT JOIN from reference table so zero-play cards
      // remain visible, which lets the UI distinguish "not used with selected
      // talent" from "card reference missing".
      cardRows = await query(`
        SELECT
          c.card_id AS "cardId",
          c.card_name AS "cardName",
          COALESCE(SUM(raw.count), 0)::INT AS "totalPlays",
          COALESCE(SUM(raw.wins), 0)::INT AS "wins",
          COALESCE(SUM(raw.losses), 0)::INT AS "losses",
          ROUND(100.0 * COALESCE(SUM(raw.wins), 0)::NUMERIC / NULLIF((COALESCE(SUM(raw.wins), 0) + COALESCE(SUM(raw.losses), 0))::NUMERIC, 0), 2)::DOUBLE PRECISION AS "winRate"
        FROM cards c
        LEFT JOIN LATERAL (
          SELECT COUNT(*)::INT AS count,
                 COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
                 COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses
          FROM match_player_cards mpc
          JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
          JOIN matches m ON m.match_id = mpc.match_id
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          ${talentJoin}
          WHERE mpc.card_id = c.card_id AND mp.champion_id = $1${queueClause}
        ) raw ON true
        WHERE c.champion_id = $1
        GROUP BY c.card_id, c.card_name
        ORDER BY "totalPlays" DESC, c.card_name ASC
      `, params);
    }

    const levelMap = new Map<number, any[]>();
    for (const row of levelStats) {
      if (!levelMap.has(row.card_id)) levelMap.set(row.card_id, []);
      levelMap.get(row.card_id)!.push({
        level: row.card_level,
        plays: row.plays,
        wins: row.wins,
        losses: row.losses,
        winRate: row.winRate ?? row.winrate ?? 0,
      });
    }

    // Some champions have legacy/rework duplicate card IDs with the same display
    // name (Moji is the loudest case: 16 current names, 32 reference rows). The
    // frontend and users reason about card names, so collapse duplicates here and
    // prefer the row with real observed plays. Otherwise a zero-play legacy row
    // can overwrite the live row when the UI builds a name-keyed map.
    const dedupedByName = new Map<string, any>();
    for (const row of cardRows) {
      const key = String(row.cardName ?? '').normalize('NFKD').toLowerCase().replace(/[^a-z0-9]/g, '');
      const withLevels = { ...row, levels: levelMap.get(row.cardId) ?? [] };
      const existing = dedupedByName.get(key);
      if (!existing || Number(withLevels.totalPlays ?? 0) > Number(existing.totalPlays ?? 0)) {
        dedupedByName.set(key, withLevels);
      }
    }
    const cards = Array.from(dedupedByName.values()).sort((a, b) => {
      const playsDelta = Number(b.totalPlays ?? 0) - Number(a.totalPlays ?? 0);
      if (playsDelta !== 0) return playsDelta;
      return String(a.cardName ?? '').localeCompare(String(b.cardName ?? ''));
    });

    return { totalMatches: matchCount?.total ?? 0, talentId: selectedTalentId, cards };
  });

  /**
   * GET /stats/cards/:championId/:cardId — Single card detail.
   *
   * The detail view answers two questions from the same match facts:
   *   1. Which talents are paired with this card, and how often do they win?
   *   2. How does the card perform at each investment level (L1-L5)?
   *
   * Query params:
   *   ?mode=     — Optional; only "ranked" is accepted
   *   ?talentId= — Optional talent filter for the headline/level rows. The
   *                 talent breakdown still returns every talent for comparison.
   */
  fastify.get('/cards/:championId/:cardId', async (req: any, reply: any) => {
    const championId = parseInt(req.params.championId as string, 10);
    const cardId = parseInt(req.params.cardId as string, 10);
    if (!Number.isInteger(championId) || championId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
    }
    if (!Number.isInteger(cardId) || cardId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid cardId.'));
    }
    if (!requireRankedStatsMode(req, reply)) return;
    const mode = 'ranked';
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const selectedTalentId = req.query.talentId != null ? parseInt(String(req.query.talentId), 10) : null;
    if (selectedTalentId != null && (!Number.isInteger(selectedTalentId) || selectedTalentId <= 0)) {
      return reply.status(400).send(err('VALIDATION', 'Invalid talentId.'));
    }

    const card = await one<any>(`
      SELECT c.card_id AS "cardId", c.card_name AS "cardName", c.champion_id AS "championId", ch.name AS "championName"
      FROM cards c
      LEFT JOIN champions ch ON ch.id = c.champion_id
      WHERE c.champion_id = $1 AND c.card_id = $2
    `, [championId, cardId]);
    if (!card) return reply.status(404).send(err('NOT_FOUND', 'Card not found for champion.'));

    if (!lobbyTier.active) {
      const projectionTable = selectedTalentId == null ? 'card_counts_ranked' : 'talent_card_counts_ranked';
      const projectionParams = selectedTalentId == null ? [cardId] : [cardId, selectedTalentId];
      const talentPredicate = selectedTalentId == null ? '' : ' AND talent_id = $2';
      const [summary, levels, talents] = await Promise.all([
        one<any>(`SELECT
          COALESCE(SUM(count), 0)::INT AS "totalPlays",
          COALESCE(SUM(wins), 0)::INT AS wins,
          COALESCE(SUM(losses), 0)::INT AS losses,
          COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS "winRate"
          FROM ${projectionTable}
          WHERE card_id = $1${talentPredicate}`, projectionParams),
        query<any>(`WITH level_ref AS (SELECT generate_series(1, 5)::SMALLINT AS level),
          raw AS (
            SELECT card_level AS level,
              SUM(count)::INT AS plays,
              SUM(wins)::INT AS wins,
              SUM(losses)::INT AS losses,
              COALESCE(ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS "winRate"
            FROM ${projectionTable}
            WHERE card_id = $1${talentPredicate}
            GROUP BY card_level
          )
          SELECT level_ref.level,
            COALESCE(raw.plays, 0)::INT AS plays,
            COALESCE(raw.wins, 0)::INT AS wins,
            COALESCE(raw.losses, 0)::INT AS losses,
            COALESCE(raw."winRate", 0)::DOUBLE PRECISION AS "winRate"
          FROM level_ref
          LEFT JOIN raw ON raw.level = level_ref.level
          ORDER BY level_ref.level`, projectionParams),
        query<any>(`SELECT
          t.talent_id AS "talentId",
          t.talent_name AS "talentName",
          COALESCE(raw.count, 0)::INT AS "totalPlays",
          COALESCE(raw.wins, 0)::INT AS wins,
          COALESCE(raw.losses, 0)::INT AS losses,
          COALESCE(ROUND(100.0 * raw.wins::NUMERIC / NULLIF((raw.wins + raw.losses)::NUMERIC, 0), 2), 0)::DOUBLE PRECISION AS "winRate"
          FROM talents t
          LEFT JOIN (
            SELECT talent_id,
              SUM(count)::INT AS count,
              SUM(wins)::INT AS wins,
              SUM(losses)::INT AS losses
            FROM talent_card_counts_ranked
            WHERE card_id = $2
            GROUP BY talent_id
          ) raw ON raw.talent_id = t.talent_id
          WHERE t.champion_id = $1
          ORDER BY "totalPlays" DESC, t.talent_name ASC`, [championId, cardId]),
      ]);

      return {
        ...card,
        mode,
        talentId: selectedTalentId,
        totalPlays: summary?.totalPlays ?? 0,
        wins: summary?.wins ?? 0,
        losses: summary?.losses ?? 0,
        winRate: summary?.winRate ?? 0,
        levels,
        talents,
      };
    }

    const detailParams: any[] = selectedTalentId ? [championId, cardId, selectedTalentId] : [championId, cardId];
    const tierWhere: string[] = [];
    appendLobbyTierPredicate(lobbyTier, detailParams, tierWhere);
    const queueClause = ` AND m.queue_id = 486${tierWhere.length ? ` AND ${tierWhere.join(' AND ')}` : ''}`;
    const selectedTalentJoin = selectedTalentId
      ? ' JOIN match_player_talents selected_mpt ON selected_mpt.match_id = mp.match_id AND selected_mpt.player_id = mp.player_id AND selected_mpt.talent_id = $3'
      : '';

    const summary = await one<any>(`
      SELECT
        COUNT(*)::INT AS "totalPlays",
        COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
        COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses,
        ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2)::DOUBLE PRECISION AS "winRate"
      FROM match_player_cards mpc
      JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
      JOIN matches m ON m.match_id = mpc.match_id
      JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      ${selectedTalentJoin}
      WHERE mp.champion_id = $1 AND mpc.card_id = $2${queueClause}
    `, detailParams);

    const levels = await query<any>(`
      WITH level_ref AS (SELECT generate_series(1, 5)::SMALLINT AS level),
      raw AS (
        SELECT mpc.card_level AS level,
          COUNT(*)::INT AS plays,
          COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
          COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses,
          ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2)::DOUBLE PRECISION AS "winRate"
        FROM match_player_cards mpc
        JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
        JOIN matches m ON m.match_id = mpc.match_id
        JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        ${selectedTalentJoin}
        WHERE mp.champion_id = $1 AND mpc.card_id = $2${queueClause}
        GROUP BY mpc.card_level
      )
      SELECT level_ref.level,
             COALESCE(raw.plays, 0)::INT AS plays,
             COALESCE(raw.wins, 0)::INT AS wins,
             COALESCE(raw.losses, 0)::INT AS losses,
             COALESCE(raw."winRate", 0)::DOUBLE PRECISION AS "winRate"
      FROM level_ref
      LEFT JOIN raw ON raw.level = level_ref.level
      ORDER BY level_ref.level
    `, detailParams);

    const talents = await query<any>(`
      SELECT
        t.talent_id AS "talentId",
        t.talent_name AS "talentName",
        COALESCE(raw.total_plays, 0)::INT AS "totalPlays",
        COALESCE(raw.wins, 0)::INT AS wins,
        COALESCE(raw.losses, 0)::INT AS losses,
        ROUND(100.0 * COALESCE(raw.wins, 0)::NUMERIC / NULLIF((COALESCE(raw.wins, 0) + COALESCE(raw.losses, 0))::NUMERIC, 0), 2)::DOUBLE PRECISION AS "winRate"
      FROM talents t
      LEFT JOIN LATERAL (
        SELECT COUNT(*)::INT AS total_plays,
               COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::INT AS wins,
               COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_LOSS})::INT AS losses
        FROM match_player_cards mpc
        JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
        JOIN matches m ON m.match_id = mpc.match_id
        JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        JOIN match_player_talents mpt ON mpt.match_id = mp.match_id AND mpt.player_id = mp.player_id AND mpt.talent_id = t.talent_id
        WHERE mp.champion_id = $1 AND mpc.card_id = $2${selectedTalentId ? ' AND $3::INT IS NOT NULL' : ''}${queueClause}
      ) raw ON true
      WHERE t.champion_id = $1
      ORDER BY "totalPlays" DESC, t.talent_name ASC
    `, detailParams);

    return {
      ...card,
      mode,
      talentId: selectedTalentId,
      totalPlays: summary?.totalPlays ?? 0,
      wins: summary?.wins ?? 0,
      losses: summary?.losses ?? 0,
      winRate: summary?.winRate ?? 0,
      levels,
      talents,
    };
  });
  /**
   * GET /stats/queues — Queue stats.
   *
   * Returns: Array of { queue_id, total_matches, avg_duration, win_rate }
   */
  fastify.get('/queues', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const params: any[] = [];
    const where = ['m.queue_id = 486'];
    appendLobbyTierPredicate(lobbyTier, params, where);
    return query(`SELECT queue_id, COUNT(*) as total_matches, ROUND(AVG(duration_seconds)::NUMERIC, 2) as avg_duration,
      ROUND(AVG(CASE WHEN ${SQL_NORMALIZED_WIN} THEN 1 WHEN ${SQL_NORMALIZED_LOSS} THEN 0 ELSE NULL END)::NUMERIC, 4) as win_rate
      FROM matches m
      JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
      JOIN match_players mp ON mp.match_id = m.match_id AND mp.entry_datetime = m.entry_datetime
      WHERE ${where.join(' AND ')}
      GROUP BY queue_id`, params);
  });

  /**
   * GET /stats/regions — Region stats.
   *
   * Returns: Array of { region, total_matches, avg_duration }
   */
  fastify.get('/regions', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const params: any[] = [486];
    const where = ['sma.queue_id = $1'];
    appendLobbyTierPredicate(lobbyTier, params, where, 'sma');
    return query(`SELECT sma.region,SUM(sma.match_count)::BIGINT AS total_matches,
        ROUND(SUM(sma.duration_sum)::NUMERIC/NULLIF(SUM(sma.match_count),0),2) AS avg_duration
      FROM stats_match_aggregate sma
      WHERE ${where.join(' AND ')}
      GROUP BY sma.region
      ORDER BY sma.region`,params);
  });

  /**
   * GET /stats/platforms — Champion performance by platform.
   *
   * The frontend platform tab expects champion rows per platform. This reads
   * observed player-match facts directly because there is not yet a maintained
   * platform projection table.
   */
  fastify.get('/platforms', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    const params: any[] = [486];
    const where = ['spa.queue_id = $1'];
    appendLobbyTierPredicate(lobbyTier, params, where, 'spa');
    return query(`
      SELECT
        spa.platform,spa.champion_id,
        COALESCE(c.name, 'Champion ' || spa.champion_id::TEXT) AS champion_name,
        SUM(spa.plays)::BIGINT AS total_matches,
        ROUND(100.0*SUM(spa.wins)::NUMERIC/NULLIF((SUM(spa.wins)+SUM(spa.losses))::NUMERIC,0),2)::DOUBLE PRECISION AS win_rate,
        ROUND(SUM(spa.dpm_sum)::NUMERIC/NULLIF(SUM(spa.metric_samples),0),2)::DOUBLE PRECISION AS avg_dpm,
        ROUND(SUM(spa.hpm_sum)::NUMERIC/NULLIF(SUM(spa.metric_samples),0),2)::DOUBLE PRECISION AS avg_hpm
      FROM stats_player_aggregate spa
      LEFT JOIN champions c ON c.id=spa.champion_id
      WHERE ${where.join(' AND ')}
      GROUP BY spa.platform,spa.champion_id,c.name
      ORDER BY platform ASC, total_matches DESC, win_rate DESC
      LIMIT 250
    `, params);
  });

  /**
   * GET /stats/loadouts — Player loadout combinations.
   *
   * player_loadouts is currently sparse on live data. Return the DB-backed
   * aggregate shape the frontend expects, even when that source is empty.
   */
  fastify.get('/loadouts', async (req: any, reply: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const offset = Math.max(parseInt(req.query.offset as string) || 0, 0);
    const minPlays = Math.max(parseInt(req.query.minPlays as string) || 1, 1);
    const params: any[] = [minPlays, limit, offset];
    const where = ['1=1'];
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));

    if (req.query.championId) {
      const championId = parseInt(req.query.championId as string, 10);
      if (!Number.isInteger(championId) || championId <= 0) {
        return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
      }
      params.push(championId);
      where.push(`pl.champion_id = $${params.length}`);
    }
    if (lobbyTier.min != null) {
      params.push(lobbyTier.min);
      where.push(`p.kbm_tier >= $${params.length}`);
    }
    if (lobbyTier.max != null) {
      params.push(lobbyTier.max);
      where.push(`p.kbm_tier <= $${params.length}`);
    }

    return query(`
      WITH loadout_rows AS (
        SELECT
          md5(
            COALESCE(array_to_string(pl.card_ids, ','), '') || ':' ||
            COALESCE(array_to_string(pl.card_levels, ','), '')
          ) AS deck_hash,
          pl.champion_id,
          COALESCE(c.name, 'Champion ' || pl.champion_id::TEXT) AS champion_name,
          COUNT(*)::INT AS total_uses,
          MAX(pl.updated_at) AS last_refreshed
        FROM player_loadouts pl
        JOIN players p ON p.id = pl.player_id
        LEFT JOIN champions c ON c.id = pl.champion_id
        WHERE ${where.join(' AND ')}
        GROUP BY deck_hash, pl.champion_id, COALESCE(c.name, 'Champion ' || pl.champion_id::TEXT)
      )
      SELECT
        deck_hash,
        champion_id,
        champion_name,
        total_uses AS total_matches,
        total_uses,
        0::INT AS wins,
        0::INT AS losses,
        0::DOUBLE PRECISION AS win_rate,
        0::INT AS ranked_wins,
        0::DOUBLE PRECISION AS ranked_win_rate,
        0::INT AS high_tier_wins,
        0::DOUBLE PRECISION AS high_tier_win_rate,
        0::DOUBLE PRECISION AS avg_kills,
        0::DOUBLE PRECISION AS avg_deaths,
        0::DOUBLE PRECISION AS avg_assists,
        0::DOUBLE PRECISION AS avg_dpm,
        0::DOUBLE PRECISION AS avg_hpm,
        NULL::JSONB AS loadout_items,
        last_refreshed
      FROM loadout_rows
      WHERE total_uses >= $1
      ORDER BY total_uses DESC, champion_name ASC
      LIMIT $2 OFFSET $3
    `, params);
  });

  /**
   * GET /stats/leagues — League tier stats.
   *
   * Returns: Array of { league_tier, total_plays, unique_players, win_rate }
   */
  fastify.get('/leagues', async () => {
    return query(`SELECT mp.league_tier, COUNT(*) as total_plays, COUNT(DISTINCT mp.player_id) as unique_players,
      ROUND(100.0 * COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_WIN})::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE ${SQL_NORMALIZED_OUTCOME})::NUMERIC, 0), 2) as win_rate
      FROM match_players mp
      JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
      WHERE m.queue_id = 486 AND COALESCE(m.limited, false) = false AND mp.league_tier > 0
      GROUP BY mp.league_tier ORDER BY mp.league_tier`);
  });

  /**
   * GET /stats/tiers — League-tier distribution for the frontend tier tab.
   *
   * Public tier distribution is a ranked-profile view, not a recovery-audit
   * view. Bucket 0 means "profile tier unresolved" and can be very noisy while
   * fresh match ingest is still backfilling getplayerbatch profile data. Keep
   * that bucket out of the default list and percentage denominator so the stats
   * page shows only real ranked tiers. Operators can still inspect tier_0
   * directly in tier_stats when auditing profile coverage.
   */
  fastify.get('/tiers', async (req: any) => {
    const source = req.query.source === 'matches' ? 'matches' : 'profiles';
    if (source === 'profiles') {
      return query(`
        WITH effective_player_tiers AS (
          SELECT
            CASE
              WHEN p.kbm_tier = 26 AND lc.rank BETWEEN 1 AND 100 THEN 27
              ELSE p.kbm_tier
            END AS tier_sort,
            COUNT(DISTINCT p.id)::INT AS total_plays
          FROM players p
          LEFT JOIN leaderboard_current lc
            ON lc.player_id = p.id
           AND lc.tier = 26
          WHERE p.kbm_tier BETWEEN 1 AND 26
          GROUP BY 1
        ),
        tiers AS (
          SELECT generate_series(1, 27) AS tier_sort
        ),
        filled AS (
          SELECT
            tiers.tier_sort,
            COALESCE(effective_player_tiers.total_plays, 0)::INT AS total_plays
          FROM tiers
          LEFT JOIN effective_player_tiers ON effective_player_tiers.tier_sort = tiers.tier_sort
        )
        SELECT
          CASE
            WHEN filled.tier_sort = 27 THEN 'Grandmaster'
            ELSE COALESCE(rt.tier_name, 'Tier ' || filled.tier_sort::TEXT)
          END AS tier,
          filled.tier_sort::INT AS tier_sort,
          filled.total_plays::INT AS total_plays,
          ROUND(100.0 * filled.total_plays::NUMERIC / NULLIF(SUM(filled.total_plays) OVER (), 0), 2)::DOUBLE PRECISION AS percentage,
          NULL::DOUBLE PRECISION AS avg_win_rate
        FROM filled
        LEFT JOIN ranked_tiers rt ON rt.tier_id = filled.tier_sort
        ORDER BY filled.tier_sort
      `);
    }

    return query(`
      WITH source_row AS (
        SELECT *
        FROM tier_stats
        WHERE source = $1
        UNION ALL
        SELECT
          $1::VARCHAR(10),
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          now()
        WHERE NOT EXISTS (SELECT 1 FROM tier_stats WHERE source = $1)
        LIMIT 1
      ),
      tiers AS (
        SELECT *
        FROM source_row,
        LATERAL (VALUES
          (0, tier_0), (1, tier_1), (2, tier_2), (3, tier_3), (4, tier_4),
          (5, tier_5), (6, tier_6), (7, tier_7), (8, tier_8), (9, tier_9),
          (10, tier_10), (11, tier_11), (12, tier_12), (13, tier_13), (14, tier_14),
          (15, tier_15), (16, tier_16), (17, tier_17), (18, tier_18), (19, tier_19),
          (20, tier_20), (21, tier_21), (22, tier_22), (23, tier_23), (24, tier_24),
          (25, tier_25), (26, tier_26)
        ) AS unpivoted(tier_sort, total_plays)
      )
      SELECT
        COALESCE(rt.tier_name, 'Tier ' || tiers.tier_sort::TEXT) AS tier,
        tiers.tier_sort::INT AS tier_sort,
        tiers.total_plays::INT AS total_plays,
        ROUND(100.0 * tiers.total_plays::NUMERIC / NULLIF(SUM(tiers.total_plays) OVER (), 0), 2)::DOUBLE PRECISION AS percentage,
        NULL::DOUBLE PRECISION AS avg_win_rate
      FROM tiers
      LEFT JOIN ranked_tiers rt ON rt.tier_id = tiers.tier_sort
      WHERE tiers.tier_sort BETWEEN 1 AND 26
      ORDER BY tiers.tier_sort
    `, [source]);
  });

  /**
   * GET /stats/tiers/summary — Match-level tier activity summary.
   *
   * The distribution endpoint returns one row per tier bucket. This summary
   * keeps the heavier "average match tier" calculation separate: it first
   * averages ranked tiers inside each match, then averages those match means.
   * That answers a different question from the match-player distribution,
   * which is participation-weighted and can be skewed by highly active players.
   */
  fastify.get('/tiers/summary', async () => {
    const rows = await query(`
      WITH ranked_player_tiers AS (
        SELECT
          mp.match_id,
          mp.player_id,
          mp.league_tier
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
        WHERE m.queue_id = 486
          AND COALESCE(m.limited, false) = false
          AND mp.league_tier BETWEEN 1 AND 26
      ),
      per_match AS (
        SELECT
          match_id,
          AVG(league_tier)::NUMERIC AS avg_tier,
          COUNT(*)::INT AS player_rows
        FROM ranked_player_tiers
        GROUP BY match_id
      ),
      profile_tiers AS (
        SELECT
          CASE
            WHEN p.kbm_tier = 26 AND lc.rank BETWEEN 1 AND 100 THEN 27
            ELSE p.kbm_tier
          END AS effective_tier
        FROM players p
        LEFT JOIN leaderboard_current lc
          ON lc.player_id = p.id
         AND lc.tier = 26
        WHERE p.kbm_tier BETWEEN 1 AND 26
      )
      SELECT
        (SELECT COUNT(*)::INT FROM profile_tiers) AS profile_players,
        (SELECT ROUND(AVG(effective_tier)::NUMERIC, 2)::DOUBLE PRECISION FROM profile_tiers) AS avg_profile_tier,
        (SELECT COUNT(*)::INT FROM ranked_player_tiers) AS match_player_rows,
        (SELECT COUNT(DISTINCT player_id)::INT FROM ranked_player_tiers) AS active_players,
        (SELECT COUNT(*)::INT FROM per_match) AS ranked_matches,
        (SELECT ROUND(AVG(league_tier)::NUMERIC, 2)::DOUBLE PRECISION FROM ranked_player_tiers) AS avg_participation_tier,
        (SELECT ROUND(AVG(avg_tier)::NUMERIC, 2)::DOUBLE PRECISION FROM per_match) AS avg_match_tier,
        (SELECT ROUND((PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY avg_tier))::NUMERIC, 2)::DOUBLE PRECISION FROM per_match) AS median_match_tier
    `);

    return rows[0] ?? {
      profile_players: 0,
      avg_profile_tier: null,
      match_player_rows: 0,
      active_players: 0,
      ranked_matches: 0,
      avg_participation_tier: null,
      avg_match_tier: null,
      median_match_tier: null,
    };
  });

  /**
   * GET /stats/baselines — Role/queue performance baselines.
   *
   * Query params:
   *   ?role=      — Role filter (e.g. "frontline", "damage", "flank", "support")
   *   ?queueId=   — Queue filter (default: 486 for ranked)
   *
   * Returns: Array of role/queue baseline metric averages and percentiles.
   */
  fastify.get('/baselines', async (req: any, reply: any) => {
    const fb = new FilterBuilder();
    let requestedRole: string | null = null;
    if (req.query.role) {
      const roleKey = String(req.query.role).toLowerCase().replace(/[\s_-]/g, '');
      const roleMap: Record<string, string> = {
        damage: 'Damage',
        flank: 'Flank',
        support: 'Support',
        frontline: 'Frontline',
        front: 'Frontline',
      };
      const roleName = roleMap[roleKey];
      if (!roleName) {
        return reply.status(400).send(err('VALIDATION', 'Invalid role. Use damage, flank, support, or frontline.'));
      }
      requestedRole = roleName;
      fb.eq('b.role_name', roleName);
    }
    const queueId = parseQueueId(req.query.queueId);
    if (!queueId) return reply.status(400).send(err('VALIDATION', 'Only ranked queue 486 is available for aggregate statistics.'));
    fb.eq('b.queue_id', queueId);

    const lobbyTier = parseLobbyTierBounds(req.query)!;
    if (!lobbyTier) return reply.status(400).send(err('VALIDATION', 'Tier bounds must be between 1 and 26.'));
    if (lobbyTier.active) {
      const params: any[] = [queueId];
      const histogramWhere=['smh.queue_id=$1'];
      if(requestedRole){
        const roleId=({Damage:1,Flank:2,Support:3,Frontline:4} as Record<string,number>)[requestedRole];
        params.push(roleId); histogramWhere.push(`smh.role_id=$${params.length}`);
      }
      appendLobbyTierPredicate(lobbyTier,params,histogramWhere,'smh');
      const histogram=await query<any>(`SELECT smh.queue_id,smh.role_id,
          CASE smh.role_id WHEN 1 THEN 'Damage' WHEN 2 THEN 'Flank' WHEN 3 THEN 'Support' WHEN 4 THEN 'Frontline' ELSE 'Global' END AS role_name,
          smh.metric,smh.value,SUM(smh.sample_count)::BIGINT AS sample_count
        FROM stats_metric_histogram smh WHERE ${histogramWhere.join(' AND ')}
        GROUP BY smh.queue_id,smh.role_id,smh.metric,smh.value ORDER BY smh.role_id,smh.metric,smh.value`,params);
      const calculated=calculateWeightedMetricStats(histogram);
      const roles=new Map<number,any>();
      for(const stat of calculated){
        const row=roles.get(stat.roleId)??{role_id:stat.roleId,role:stat.roleName,queue_id:queueId,sample_size:0,updated_at:new Date().toISOString()};
        const prefix=stat.metric==='mpm'?'shpm':stat.metric;
        row[`avg_${prefix}`]=stat.mean; row[`p10_${prefix}`]=stat.p10; row[`p25_${prefix}`]=stat.p25;
        row[`p75_${prefix}`]=stat.p75; row[`p90_${prefix}`]=stat.p90; row[`max_${prefix}`]=stat.max;
        row.sample_size=Math.max(row.sample_size,stat.sampleSize); roles.set(stat.roleId,row);
      }
      return [...roles.values()].sort((a,b)=>a.role_id-b.role_id);

      const where = [
        'm.queue_id = $1',
        'mp.champion_id > 0',
        "COALESCE(mp.source, 'direct') IN ('direct', 'recovered')",
        'mp.task_force IN (1, 2)',
        "lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss')",
        'm.duration_seconds > 120',
        'mp.gold_per_minute > 0',
        'mp.egpm IS NOT NULL',
      ];
      if (requestedRole) {
        params.push(requestedRole);
        where.push(`${championRoleSql('c')} = $${params.length}`);
      }
      appendLobbyTierPredicate(lobbyTier, params, where);
      return query(`WITH values_by_role AS (
          SELECT ${championRoleSql('c')} AS role,
            CASE WHEN mp.gold_per_minute > 0 THEN mp.gold_per_minute END AS gpm,
            CASE WHEN mp.damage_per_minute > 0 THEN mp.damage_per_minute END AS dpm,
            CASE WHEN mp.healing_per_minute > 0 THEN mp.healing_per_minute END AS hpm,
            CASE WHEN mp.mitigation_per_minute > 0 THEN mp.mitigation_per_minute END AS shpm,
            CASE WHEN mp.kda > 0 THEN mp.kda END AS kda,
            CASE WHEN mp.egpm >= 0 THEN mp.egpm END AS egpm
          FROM match_players mp
          JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
          JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
          JOIN champions c ON c.id = mp.champion_id
          WHERE ${where.join(' AND ')}
        )
        SELECT CASE COALESCE(role, 'Global') WHEN 'Damage' THEN 1 WHEN 'Flank' THEN 2 WHEN 'Support' THEN 3 WHEN 'Frontline' THEN 4 ELSE 0 END AS role_id,
          COALESCE(role, 'Global') AS role, $1::INT AS queue_id,
          ROUND(AVG(gpm)::NUMERIC, 2) AS avg_gpm, ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p10_gpm, ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p25_gpm, ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p75_gpm, ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY gpm)::NUMERIC, 2) AS p90_gpm, ROUND(MAX(gpm)::NUMERIC, 2) AS max_gpm,
          ROUND(AVG(dpm)::NUMERIC, 2) AS avg_dpm, ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p10_dpm, ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p25_dpm, ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p75_dpm, ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY dpm)::NUMERIC, 2) AS p90_dpm, ROUND(MAX(dpm)::NUMERIC, 2) AS max_dpm,
          ROUND(AVG(hpm)::NUMERIC, 2) AS avg_hpm, ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p10_hpm, ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p25_hpm, ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p75_hpm, ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY hpm)::NUMERIC, 2) AS p90_hpm, ROUND(MAX(hpm)::NUMERIC, 2) AS max_hpm,
          ROUND(AVG(shpm)::NUMERIC, 2) AS avg_shpm, ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p10_shpm, ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p25_shpm, ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p75_shpm, ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY shpm)::NUMERIC, 2) AS p90_shpm, ROUND(MAX(shpm)::NUMERIC, 2) AS max_shpm,
          ROUND(AVG(kda)::NUMERIC, 2) AS avg_kda, ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p10_kda, ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p25_kda, ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p75_kda, ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY kda)::NUMERIC, 2) AS p90_kda, ROUND(MAX(kda)::NUMERIC, 2) AS max_kda,
          ROUND(AVG(egpm)::NUMERIC, 2) AS avg_egpm, ROUND(PERCENTILE_CONT(.1) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p10_egpm, ROUND(PERCENTILE_CONT(.25) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p25_egpm, ROUND(PERCENTILE_CONT(.75) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p75_egpm, ROUND(PERCENTILE_CONT(.9) WITHIN GROUP (ORDER BY egpm)::NUMERIC, 2) AS p90_egpm, ROUND(MAX(egpm)::NUMERIC, 2) AS max_egpm,
          COUNT(*)::INT AS sample_size, now() AS updated_at
        FROM values_by_role
        GROUP BY ${requestedRole ? 'role' : 'GROUPING SETS ((role), ())'}
        ORDER BY role_id`, params);
    }

    const { clause, params } = fb.build();
    return query(`
      SELECT
        b.role_id,
        b.role_name AS role,
        b.queue_id,
        b.avg_gpm, b.p10_gpm, b.p25_gpm, b.p75_gpm, b.p90_gpm, b.max_gpm,
        b.avg_dpm, b.p10_dpm, b.p25_dpm, b.p75_dpm, b.p90_dpm, b.max_dpm,
        b.avg_hpm, b.p10_hpm, b.p25_hpm, b.p75_hpm, b.p90_hpm, b.max_hpm,
        b.avg_shpm, b.p10_shpm, b.p25_shpm, b.p75_shpm, b.p90_shpm, b.max_shpm,
        b.avg_kda, b.p10_kda, b.p25_kda, b.p75_kda, b.p90_kda, b.max_kda,
        b.avg_egpm, b.p10_egpm, b.p25_egpm, b.p75_egpm, b.p90_egpm, b.max_egpm,
        b.sample_size,
        b.updated_at
      FROM baselines b${clause}
      ORDER BY b.queue_id, b.role_id
    `, params);
  });

  /**
   * GET /stats/ranked-leaderboard — Ranked league leaderboard (from leaderboard_current).
   *
   * Query params:
   *   ?tier=    — Ranked tier 21-26 (Diamond 5 to Master, required)
   *   ?top=     — Max results (default: 50)
   *
   * Returns: Array of { rank, player_id, name, tier, points, prev_rank, trend }
   *   - trend: +N means moved up N positions, -N means moved down, 0 means unchanged
   */
  fastify.get('/ranked-leaderboard', async (req: any, reply: any) => {
    const tier = parseInt(req.query.tier as string);
    if (!Number.isInteger(tier) || tier < 21 || tier > 26) {
      return reply.status(400).send(err('VALIDATION', 'Invalid tier. Must be 21-26 (Diamond 5 to Master).'));
    }
    const top = Math.min(parseInt(req.query.top as string) || 50, 200);

    const rows = await query(`
      SELECT *,
        CASE
          WHEN prev_rank IS NULL THEN 0
          ELSE prev_rank - rank
        END AS trend
      FROM leaderboard_current
      WHERE tier = $1
      ORDER BY points DESC
      LIMIT $2
    `, [tier, top]);
    return rows;
  });

  /**
   * GET /stats/leaderboard-log — Leaderboard sync job history (from leaderboard_update_log).
   *
   * Query params:
   *   ?page=      — Page number (default: 1)
   *   ?perPage=   — Results per page (default: 20, max: 100)
   *
   * Returns: Rows from leaderboard_update_log ordered by updated_at descending.
   */
  fastify.get('/leaderboard-log', async (req: any) => {
    const page = parseInt(req.query.page as string) || 1;
    const perPage = Math.min(parseInt(req.query.perPage as string) || 20, 100);
    const offset = (page - 1) * perPage;

    return query(
      'SELECT * FROM leaderboard_update_log ORDER BY updated_at DESC LIMIT $1 OFFSET $2',
      [perPage, offset]
    );
  });

  /**
   * GET /stats/tier-population — Tier population distribution (from tier_population_stats MV).
   *
   * Returns: Array of { tier, tier_name, player_count, percentage }
   *   - percentage: % of total ranked players in this tier
   */
  fastify.get('/tier-population', async () => {
    return query(`
      SELECT tier, tier_name, player_count,
        ROUND(100.0 * player_count / NULLIF(SUM(player_count) OVER(), 0), 2) as percentage
      FROM tier_population_stats
      ORDER BY tier
    `);
  });

  /**
   * GET /stats/champion-leaderboard — Per-champion player leaderboard (Glicko-2).
   *
   * Query params:
   *   ?championId=  — Champion ID (required)
   *   ?limit=       — Max results (default: 25, max: 100)
   *
   * Returns: Array of { rank, playerId, playerName, mu, phi, matchesPlayed, wins, losses }
   */
  fastify.get('/champion-leaderboard', async (req: any, reply: any) => {
    const championId = parseInt(req.query.championId as string, 10);
    if (!Number.isInteger(championId) || championId <= 0) {
      return reply.status(400).send(err('VALIDATION', 'Invalid championId.'));
    }
    const limit = Math.min(parseInt(req.query.limit as string) || 25, 100);

    const rows = await query(
      `SELECT
         pcr.player_id AS "playerId",
         ${DISPLAY_NAME_SQL} AS "playerName",
         pcr.mu,
         pcr.phi,
         pcr.matches_played AS "matchesPlayed",
         pcr.wins,
         pcr.losses
       FROM player_champion_ratings pcr
       JOIN players p ON p.id = pcr.player_id
       WHERE pcr.champion_id = $1
         AND NOT p.cheater
       ORDER BY pcr.mu DESC
       LIMIT $2`,
      [championId, limit]
    );

    return rows.map((r: any, i: number) => ({
      rank: i + 1,
      ...r,
    }));
  });
}
