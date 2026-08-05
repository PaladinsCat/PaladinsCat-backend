import { FastifyInstance } from 'fastify';
import { DB_POOL_MAX, healthCheck as dbHealth, pool } from '../config/db';
import { healthCheck as redisHealth } from '../services/cache';
import { healthCheck as meiliHealth } from '../config/meilisearch';
import { query, one } from '../config/db';
import { BUDGET_THRESHOLD, effectiveDailyLimit } from '../contracts/hirez-key-policy';
import { flush } from '../services/cache';
import { requireAuth } from '../utils/query-helpers';
import {
  BACKEND_SCHEDULER_JOB_TYPES,
  BACKEND_SCHEDULERS,
  getOwnedBackendSchedulerKeys,
} from '../workers/scheduler-registry';
import {
  MATCH_DETAIL_SERVICE_OUTAGE_KEY,
  classifyHirezServiceOutageMessage,
  getActiveHirezServiceOutages,
  isHirezServiceOutageProbeDue,
} from '../workers/hirez-service-outage';
import { getLocalDeploymentState } from '../services/deployment-control';

type HirezOutageSignal = {
  source: string;
  message: string;
  observedAt: string | null;
  code: string;
  serviceKey: string;
  title: string;
  severity: 'critical' | 'warning';
  publicMessage: string;
};

const HIREZ_OUTAGE_SIGNAL_LOOKBACK_MINUTES = Math.max(
  5,
  Number(process.env.HIREZ_OUTAGE_SIGNAL_LOOKBACK_MINUTES || 30),
);

async function tableExists(tableName: string): Promise<boolean> {
  const row = await one<{ exists: boolean }>(
    `SELECT to_regclass($1) IS NOT NULL AS exists`,
    [`public.${tableName}`],
  );
  return Boolean(row?.exists);
}

async function collectRecentHirezOutageSignals(): Promise<HirezOutageSignal[]> {
  const signalRows: Array<{ source: string; message: string | null; observed_at: string | null }> = [];

  // These tables are local control-plane evidence, not Hi-Rez calls. The banner
  // can safely poll this endpoint because it only reads the database state that
  // workers already wrote while handling ingest/recovery failures.
  if (await tableExists('hourly_ingest_match_debt')) {
    signalRows.push(...await query(
      `SELECT 'hourly_ingest_match_debt' AS source,
              reason AS message,
              updated_at::text AS observed_at
       FROM hourly_ingest_match_debt
       WHERE updated_at >= now() - ($1::int * interval '1 minute')
         AND reason IS NOT NULL
       ORDER BY updated_at DESC
       LIMIT 50`,
      [HIREZ_OUTAGE_SIGNAL_LOOKBACK_MINUTES],
    ));
  }

  if (await tableExists('hourly_ingest_state')) {
    signalRows.push(...await query(
      `SELECT 'hourly_ingest_state' AS source,
              error_message AS message,
              updated_at::text AS observed_at
       FROM hourly_ingest_state
       WHERE updated_at >= now() - ($1::int * interval '1 minute')
         AND error_message IS NOT NULL
       ORDER BY updated_at DESC
       LIMIT 50`,
      [HIREZ_OUTAGE_SIGNAL_LOOKBACK_MINUTES],
    ));
  }

  const signals = signalRows
    .map((row) => {
      const classification = classifyHirezServiceOutageMessage(row.message || '');
      if (!classification) return null;
      return {
        source: row.source,
        message: row.message || '',
        observedAt: row.observed_at,
        code: classification.code,
        serviceKey: classification.serviceKey,
        title: classification.title,
        severity: classification.severity,
        publicMessage: classification.publicMessage,
      } satisfies HirezOutageSignal;
    })
    .filter((signal): signal is HirezOutageSignal => signal !== null);

  const seen = new Set<string>();
  return signals.filter((signal) => {
    const key = `${signal.serviceKey}|${signal.code}|${signal.source}|${signal.message}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).slice(0, 10);
}

function activeOutageTitle(serviceKey: string, reason: string | null): string {
  return classifyHirezServiceOutageMessage(reason || '')?.title
    || (serviceKey === MATCH_DETAIL_SERVICE_OUTAGE_KEY ? 'Hi-Rez match detail outage' : 'Hi-Rez API outage');
}

function activeOutageMessage(serviceKey: string, reason: string | null): string {
  return classifyHirezServiceOutageMessage(reason || '')?.publicMessage
    || (serviceKey === MATCH_DETAIL_SERVICE_OUTAGE_KEY
      ? 'Hi-Rez match detail endpoints are currently blocked. PaladinsCat is preserving exact match debt and probing safely.'
      : 'Hi-Rez API is currently degraded. PaladinsCat is serving local data where possible.');
}
export default async function systemRoutes(fastify: FastifyInstance) {
  fastify.get('/deployment/status', async (_req, reply) => {
    reply.header('Cache-Control', 'no-store, max-age=0');
    // The process initializes this state from Redis before listen and every
    // deployment transition is applied locally by the authenticated control
    // endpoint. Do not turn every browser poll into another Redis command.
    return getLocalDeploymentState();
  });

  /**
   * GET /health — Health check endpoint.
   *
   * Checks database, Redis, and MeiliSearch connectivity.
   *
   * Returns: { status: "healthy"|"degraded", db: bool, redis: bool, meilisearch: bool, timestamp }
   */
  fastify.get('/health', async () => {
    const db = await dbHealth();
    const redis = await redisHealth();
    const meili = await meiliHealth();
    const status = db && redis && meili ? 'healthy' : 'degraded';
    return { status, db, redis, meilisearch: meili, timestamp: new Date().toISOString() };
  });

  /**
   * GET /status — System status overview.
   *
   * Returns: { matches, players, pendingPulls, lastMatch, bufferStats, mvFreshness, timestamp }
   *   - bufferStats: counts from raw_ingest_buffer by status
   *   - mvFreshness: last refresh timestamps for materialized views
   */
  fastify.get('/status', async () => {
    const matches = await one('SELECT COUNT(*) as count FROM matches');
    const players = await one('SELECT COUNT(*) as count FROM players');
    const pullList = await one('SELECT COUNT(*) as count FROM match_pull_list');
    const lastMatch = await one('SELECT entry_datetime FROM matches ORDER BY entry_datetime DESC LIMIT 1');

    // Buffer stats
    const bufferStats = await query(`SELECT status, COUNT(*) as count FROM raw_ingest_buffer GROUP BY status`);

    // MV freshness (approximate via pg_stat_user_tables). Keep this list to
    // materialized views still maintained by backend workers/routes; the old
    // champion_meta_stats MV was replaced by champion_stats_ranked.
    const mvFreshness = await query(`
      SELECT relname as mv_name, last_autoanalyze as last_updated
      FROM pg_stat_user_tables
      WHERE relname IN ('mv_player_coplay_stats', 'tier_population_stats', 'player_rankings')
      ORDER BY relname
    `);

    return {
      matches: matches?.count || 0,
      players: players?.count || 0,
      pendingPulls: pullList?.count || 0,
      lastMatch: lastMatch?.entry_datetime,
      bufferStats,
      mvFreshness,
      timestamp: new Date().toISOString(),
    };
  });

  const hirezStatusHandler = async () => {
    const [activeOutages, recentSignals, debtSummary] = await Promise.all([
      getActiveHirezServiceOutages(),
      collectRecentHirezOutageSignals(),
      tableExists('hourly_ingest_match_debt').then(async (exists) => {
        if (!exists) return null;
        return one<{
          pending_vendor_debt: string | number;
          due_vendor_debt: string | number;
          affected_hours: string | number;
          next_retry_at: string | null;
        }>(
          `SELECT
             COUNT(*) FILTER (
               WHERE status = 'pending'
                 AND COALESCE(reason, '') ILIKE '%vendor detail service outage%'
             ) AS pending_vendor_debt,
             COUNT(*) FILTER (
               WHERE status = 'pending'
                 AND COALESCE(reason, '') ILIKE '%vendor detail service outage%'
                 AND (next_retry_at IS NULL OR next_retry_at <= now())
             ) AS due_vendor_debt,
             COUNT(DISTINCT (date::text || ':' || hour::text)) FILTER (
               WHERE status = 'pending'
                 AND COALESCE(reason, '') ILIKE '%vendor detail service outage%'
             ) AS affected_hours,
             (MIN(next_retry_at) FILTER (
               WHERE status = 'pending'
                 AND COALESCE(reason, '') ILIKE '%vendor detail service outage%'
             ))::text AS next_retry_at
           FROM hourly_ingest_match_debt`,
        );
      }),
    ]);

    const pendingVendorDebt = Number(debtSummary?.pending_vendor_debt || 0);
    const dueVendorDebt = Number(debtSummary?.due_vendor_debt || 0);
    const affectedHours = Number(debtSummary?.affected_hours || 0);
    const nextDebtRetryAt = debtSummary?.next_retry_at || null;

    const mappedOutages = activeOutages.map((outage) => ({
      serviceKey: outage.service_key,
      status: outage.status,
      title: activeOutageTitle(outage.service_key, outage.reason),
      severity: classifyHirezServiceOutageMessage(outage.reason || '')?.severity || 'critical',
      message: activeOutageMessage(outage.service_key, outage.reason),
      reason: outage.reason,
      firstDetectedAt: outage.first_detected_at,
      lastDetectedAt: outage.last_detected_at,
      nextProbeAt: outage.next_probe_at,
      probeDue: isHirezServiceOutageProbeDue(outage),
      probeCount: Number(outage.probe_count || 0),
      updatedAt: outage.updated_at,
    }));

    // Defense in depth: an older worker or a race can leave exact match debt
    // with the vendor-outage reason even when the latch row is absent/recovered.
    // Debt with that reason still means broad detail retries are unsafe, so the
    // public status must stay red until the exact-ID debt drains or the latch
    // records a fresh recovered state.
    const debtOnlyOutages = pendingVendorDebt > 0 && mappedOutages.length === 0
      ? [{
          serviceKey: MATCH_DETAIL_SERVICE_OUTAGE_KEY,
          status: 'active',
          title: 'Hi-Rez match detail outage',
          severity: 'critical' as const,
          message: activeOutageMessage(MATCH_DETAIL_SERVICE_OUTAGE_KEY, null),
          reason: 'pending vendor detail service outage debt',
          firstDetectedAt: null,
          lastDetectedAt: null,
          nextProbeAt: nextDebtRetryAt,
          probeDue: dueVendorDebt > 0,
          probeCount: 0,
          updatedAt: null,
        }]
      : [];
    const activeOutageItems = [...mappedOutages, ...debtOnlyOutages];

    const status = activeOutageItems.some(outage => outage.severity === 'critical')
      ? 'outage'
      : activeOutageItems.length > 0 || recentSignals.length > 0
        ? 'degraded'
        : 'ok';

    return {
      status,
      outage: status === 'outage',
      degraded: status === 'degraded',
      message: activeOutageItems[0]?.message || recentSignals[0]?.publicMessage || 'Hi-Rez API is operating normally.',
      activeOutages: activeOutageItems,
      recentSignals,
      pendingVendorDebt,
      dueVendorDebt,
      affectedHours,
      nextDebtRetryAt,
      signalLookbackMinutes: HIREZ_OUTAGE_SIGNAL_LOOKBACK_MINUTES,
      timestamp: new Date().toISOString(),
    };
  };

  /**
   * GET /system/hirez-status — Public Hi-Rez outage status for the website banner.
   *
   * This endpoint is intentionally DB-only: it reads worker/relay evidence that
   * already exists and never calls Hi-Rez. The header can poll it safely without
   * increasing API usage or waking recovery loops.
   */
  fastify.get('/system/hirez-status', hirezStatusHandler);
  fastify.get('/hirez-status', hirezStatusHandler);

  /**
   * GET /schedulers — Background scheduler status.
   *
   * Returns status of backend-owned node-cron schedulers. Hi-Rez API key sync
   * runs inside HirezRelay in real mode, not in this backend process.
   *
   * Returns: { rankedTracker, autoIngester, baselineTracker, apiKeySync }
   *   Each: { enabled: bool, lastRun: ISO8601|null, nextRun: ISO8601|null, runCount: number }
   */
  fastify.get('/schedulers', async () => {
    const ownedSchedulerKeys = new Set(getOwnedBackendSchedulerKeys());

    // Check sync_jobs table for recent scheduler activity. Scheduler ownership
    // lives in scheduler-registry.ts so startup and status reporting cannot
    // drift apart when workers are added, renamed, or moved to HirezRelay.
    const recentJobs = await query(`
      SELECT
        CASE WHEN job_type = 'afk_tracker' THEN 'baseline_tracker' ELSE job_type END AS job_type,
        status,
        created_at,
        COALESCE(completed_at, started_at, created_at) AS updated_at
      FROM sync_jobs
      WHERE job_type = ANY($1)
      ORDER BY created_at DESC
      LIMIT 20
    `, [BACKEND_SCHEDULER_JOB_TYPES]);

    // Group by job_type
    const schedulers: any = {};
    for (const scheduler of BACKEND_SCHEDULERS) {
      const type = scheduler.key;
      const jobs = recentJobs.filter((j: any) => j.job_type === type);
      const lastJob = jobs[0];
      schedulers[type] = {
        // `enabled` is the live process state, not just "this scheduler exists
        // in the registry". This distinction matters during VPS migration:
        // BACKEND_SCHEDULERS_ENABLED=false should let operators validate a
        // restored database over HTTP without hourly ingest/gap workers
        // claiming rows or spending Hi-Rez calls.
        enabled: ownedSchedulerKeys.has(type),
        description: scheduler.description,
        lastRun: lastJob?.updated_at || lastJob?.created_at || null,
        lastStatus: lastJob?.status || null,
        recentRuns: jobs.length,
      };
    }

    return schedulers;
  });

  /**
   * GET /database — Database statistics.
   *
   * Returns table row counts, connection pool info, and disk usage estimates.
   *
   * Returns PostgreSQL server activity separately from this process's real
   * node-postgres pool. `pg_stat_activity.wait_event` is not a pool waiter:
   * idle clients normally wait on ClientRead.
   */
  fastify.get('/database', async () => {
    // Table row counts for major tables
    // Always include matches and players even if stats are stale;
    // pg_stat_user_tables can miss tables that haven't had ANALYZE run.
    const tables = await query(`
      SELECT relname as name, n_live_tup as row_count
      FROM pg_stat_user_tables
      WHERE relname IN ('matches', 'players')
         OR n_live_tup > 0
      ORDER BY n_live_tup DESC
      LIMIT 50
    `);

    const serverStats = await query(`
      SELECT
        current_setting('max_connections')::int AS max_connections,
        current_setting('superuser_reserved_connections')::int AS superuser_reserved_connections,
        COUNT(*) as total_connections,
        COUNT(CASE WHEN state = 'active' THEN 1 END) as active_connections,
        COUNT(CASE WHEN state = 'idle' THEN 1 END) as idle_connections,
        COUNT(CASE WHEN state <> 'idle' AND wait_event IS NOT NULL THEN 1 END) as waiting_connections
      FROM pg_stat_activity
      WHERE datname = current_database()
    `);

    return {
      tables,
      server: serverStats[0] || {},
      pool: {
        max_connections: DB_POOL_MAX,
        total_connections: pool.totalCount,
        idle_connections: pool.idleCount,
        waiting_requests: pool.waitingCount,
      },
      timestamp: new Date().toISOString(),
    };
  });

  /**
   * POST /cache/flush — Flush all cache.
   *
   * CRITICAL: Requires Bearer token authentication (ADMIN_SECRET env var).
   * Without auth, anyone who can reach the API can nuke all cached data,
   * causing a thundering herd of cache misses → all requests hit PostgreSQL
   * simultaneously → potential DB overload.
   *
   * Clears all cached data in Redis.
   *
   * Returns: { message: "Cache flushed" }
   */
  fastify.post('/cache/flush', async (req: any, reply: any) => {
    try {
      await requireAuth(req);
    } catch {
      return reply.status(401).send({ error: { code: 'UNAUTHORIZED', message: 'Authentication required' } });
    }
    await flush();
    return { message: 'Cache flushed' };
  });

  /**
   * POST /cache/flush/:pattern — Flush cache by key pattern.
   *
   * CRITICAL: Requires Bearer token authentication (ADMIN_SECRET env var).
   * Without auth, anyone can selectively flush cached data by pattern.
   *
   * URL param: pattern — Redis key pattern (e.g. "match:*", "ref:*", "live_match:*")
   *
   * Returns: { message: "Cache flushed", pattern, keysDeleted: number }
   */
  fastify.post('/cache/flush/:pattern', async (req: any, reply: any) => {
    try {
      await requireAuth(req);
    } catch {
      return reply.status(401).send({ error: { code: 'UNAUTHORIZED', message: 'Authentication required' } });
    }
    const pattern = req.params.pattern as string;
    if (!pattern) {
      return reply.status(400).send({ error: 'Missing pattern parameter' });
    }

    const { redis } = await import('../services/cache.js');
    const keys = await redis.keys(pattern);
    if (keys.length > 0) {
      await redis.del(keys);
    }
    return { message: 'Cache flushed', pattern, keysDeleted: keys.length };
  });

   fastify.post('/api-keys/encrypt', async (req: any, reply: any) => {
    // CRITICAL: Require auth. This endpoint uses the MEK (Master Encryption Key)
    // to encrypt API keys. Without auth, anyone can encrypt arbitrary strings,
    // potentially using the output to verify MEK guesses (known-plaintext attack
    // against GCM auth tag). Also returns 200 on error — fixed below.
    // Source: Debug 2026-05-31 — "No auth on /api-keys/encrypt"
    try {
      await requireAuth(req);
    } catch {
      return reply.status(401).send({ error: { code: 'UNAUTHORIZED', message: 'Authentication required' } });
    }
    const { auth_key_plaintext } = req.body as any;
    if (!auth_key_plaintext) {
      return reply.status(400).send({ error: 'auth_key_plaintext is required' });
    }
    const { encrypt } = await import('../utils/crypto.js');
    return { auth_key_encrypted: encrypt(auth_key_plaintext) };
  });

  fastify.get('/api-keys/status', async () => {
    const rows = await query(`
      SELECT dev_id, status, total_24h, daily_limit, calls_total,
             consecutive_failures, last_used
      FROM api_keys
      ORDER BY dev_id ASC
    `);

    return rows.map((row: any) => {
      const limit = effectiveDailyLimit(String(row.dev_id), Number(row.daily_limit || 0));
      const used = Number(row.total_24h || 0);
      const remaining = Math.max(0, limit - used);
      return {
        devId: String(row.dev_id),
        status: row.status,
        used_24h: used,
        daily_limit: limit,
        remaining,
        left_calls: remaining,
        reserve_threshold: BUDGET_THRESHOLD,
        turns_off_at_remaining: BUDGET_THRESHOLD,
        usable: remaining > BUDGET_THRESHOLD && row.status === 'healthy',
        calls_total: Number(row.calls_total || 0),
        consecutive_failures: Number(row.consecutive_failures || 0),
        last_used: row.last_used,
      };
    });
  });

  }
