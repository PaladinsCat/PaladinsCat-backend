import { FastifyInstance } from 'fastify';
import { query, one, transaction } from '../config/db';
import { paginate } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';
import { requireAuth } from '../utils/query-helpers';
import { effectiveDailyLimit } from '../contracts/hirez-key-policy';
import { getDataUsed, reloadApiKeyPool, syncApiKeyUsage } from '../services/hirez';
import { guardVendorFallback, RequestSecurityError } from '../services/request-security';
import {
  backfillPrivateAccountIdentities,
  getPrivateBackfillReport,
  PRIVATE_IDENTITY_VERSION,
} from '../services/private-account-resolver';
import {
  getDeploymentState,
  isDeploymentPhase,
  setDeploymentState,
} from '../services/deployment-control';
import {
  getBackendRuntimeState,
  quiesceBackendWork,
  warmBackendForDeployment,
} from '../services/backend-lifecycle';

/**
 * Admin & Internal Ops Routes.
 * All endpoints require Bearer token authentication.
 * Tables: sync_jobs, match_pull_list, api_log, api_key_hourly_usage, hourly_match_counts.
 */
export default async function adminRoutes(fastify: FastifyInstance) {
  // Auth guard for all admin routes
  fastify.addHook('preHandler', async (req: any, reply: any) => {
    try {
      await requireAuth(req);
    } catch {
      return reply.status(401).send({ error: { code: 'UNAUTHORIZED', message: 'Authentication required' } });
    }
  });

  /**
   * GET /admin/sync-jobs — Job tracking and status
   * ?type=, ?status=, ?page=, ?perPage=
   */
  fastify.get('/sync-jobs', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.type) fb.eq('job_type', req.query.type);
    if (req.query.status) fb.eq('status', req.query.status);

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM sync_jobs${clause} ORDER BY created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /admin/sync-jobs/:type — Filter by job type
   */
  fastify.get('/sync-jobs/:type', async (req: any) => {
    const jobType = req.params.type as string;
    const rows = await query(
      `SELECT * FROM sync_jobs WHERE job_type = $1 ORDER BY created_at DESC LIMIT 50`,
      [jobType]
    );
    return rows;
  });

  /**
   * GET /admin/pull-list — match_pull_list staging buffer
   */
  fastify.get('/pull-list', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const rows = await query(
      `SELECT * FROM match_pull_list ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
      [perPage, offset]
    );
    return rows;
  });

  /**
   * GET /admin/api-log — Hourly consolidated API logging
   * ?devId=, ?endpoint=, ?consumer=, ?from=, ?to=, ?page=, ?perPage=
   */
  fastify.get('/api-log', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.devId) fb.eq('dev_id', req.query.devId);
    if (req.query.endpoint) fb.eq('endpoint', req.query.endpoint);
    if (req.query.consumer) fb.eq('consumer', req.query.consumer);
    if (req.query.from) fb.gte('hour', new Date(req.query.from));
    if (req.query.to) fb.lte('hour', new Date(req.query.to));

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM api_log${clause} ORDER BY hour DESC, dev_id ASC, consumer ASC, endpoint ASC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /admin/api-log/:devId — Per-key API log
   */
  fastify.get('/api-log/:devId', async (req: any) => {
    const devId = req.params.devId as string;
    const rows = await query(
      `SELECT * FROM api_log WHERE dev_id = $1 ORDER BY hour DESC, consumer ASC, endpoint ASC LIMIT 100`,
      [devId]
    );
    return rows;
  });

  /**
   * GET /admin/hourly-usage — Per-key hourly call breakdown
   * ?devId=, ?from=, ?to=
   */
  fastify.get('/hourly-usage', async (req: any) => {
    const fb = new FilterBuilder();
    if (req.query.devId) fb.eq('dev_id', req.query.devId);
    if (req.query.from) fb.gte('hour_bucket', new Date(req.query.from));
    if (req.query.to) fb.lte('hour_bucket', new Date(req.query.to));

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM api_key_hourly_usage${clause} ORDER BY hour_bucket DESC, dev_id ASC LIMIT 100`,
      params
    );
    return rows;
  });

  /**
   * GET /admin/hourly-match-counts — Hourly match counts by region/queue
   * ?date=, ?hour=, ?queueId=, ?page=, ?perPage=
   */
  fastify.get('/hourly-match-counts', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.date) fb.eq('date', req.query.date);
    if (req.query.hour !== undefined && req.query.hour !== '') fb.eq('hour', parseInt(req.query.hour, 10));
    if (req.query.queueId) fb.eq('queue_id', parseInt(req.query.queueId, 10));

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM hourly_match_counts${clause} ORDER BY date DESC, hour DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /admin/hourly-match-counts/:date — Filter by date
   */
  fastify.get('/hourly-match-counts/:date', async (req: any) => {
    const date = req.params.date as string;
    const rows = await query(
      `SELECT * FROM hourly_match_counts WHERE date = $1 ORDER BY hour DESC`,
      [date]
    );
    return rows;
  });

  /**
   * POST /admin/batch-fetch - Re-ingest specific match IDs with API call tracking.
   *
   * Body: { matchIds: number[] }
   *
   * Uses our getMatchDetailsBatch code path (not raw Hi-Rez API).
   * Tracks all API calls made during ingestion including recovery pipeline.
   * Returns detailed breakdown of calls per endpoint + total count.
   */
  fastify.post('/batch-fetch', async (req: any, reply: any) => {
    const { matchIds } = req.body as any;
    if (!Array.isArray(matchIds) || matchIds.length === 0) {
      return reply.status(400).send({ error: 'matchIds is required (array of numbers)' });
    }
    const normalizedMatchIds = [...new Set(
      matchIds
        .map(Number)
        .filter((matchId: number) => Number.isSafeInteger(matchId) && matchId > 0),
    )];
    if (normalizedMatchIds.length !== matchIds.length || normalizedMatchIds.length > 10) {
      return reply.status(400).send({ error: 'matchIds must contain 1-10 unique positive safe integers' });
    }

    // Track API calls before/after ingestion
    const apiLogBefore = await query(
      `SELECT dev_id, endpoint, consumer, call_count FROM api_log WHERE hour >= now() - interval '10 minutes' ORDER BY dev_id, consumer, endpoint`
    );

    try {
      // Import match processing logic from matches.ts
      const { fetchMatches } = await import('../routes/matches.js');

      // Explicit admin action may re-fetch broken matches, but the override is
      // request-scoped. Never mutate process-wide config while public requests
      // may be running concurrently.
      const result = await fetchMatches(normalizedMatchIds, {
        allowHirezFallback: true,
        forceRefresh: true,
        beforeHirezFallback: (missingIds) => guardVendorFallback(req, reply, {
          scope: 'admin-batch-fetch',
          entity: missingIds.join(','),
        }),
      });

      // Get updated API log to calculate diff
      const apiLogAfter = await query(
        `SELECT dev_id, endpoint, consumer, call_count FROM api_log WHERE hour >= now() - interval '10 minutes' ORDER BY dev_id, consumer, endpoint`
      );

      // Calculate API calls used during this operation
      const apiCallsUsed: Record<string, number> = {};
      for (const after of apiLogAfter) {
        const beforeRow = apiLogBefore.find((b: any) => (
          b.dev_id === after.dev_id
          && b.endpoint === after.endpoint
          && b.consumer === after.consumer
        ));
        const beforeCount = beforeRow ? beforeRow.call_count : 0;
        const used = after.call_count - beforeCount;
        if (used > 0) {
          apiCallsUsed[`${after.consumer}:${after.endpoint} (key ${after.dev_id})`] = used;
        }
      }

      return {
        success: true,
        matchesFetched: result.count,
        notFound: result.notFound || [],
        apiCalls: apiCallsUsed,
        totalApiCalls: Object.values(apiCallsUsed).reduce((sum: number, v: number) => sum + v, 0),
      };
    } catch (error) {
      if (error instanceof RequestSecurityError) throw error;
      console.error('[batch-fetch] Error:', error);
      return reply.status(500).send({
        success: false,
        error: String(error)
      });
    }
  });

  /**
   * DELETE /admin/hourly-match-counts/:date/:hour/:queueId — Remove an analytics
   * projection row.
   *
   * Historical context: before hourly_ingest_state existed, a stale zero-count
   * row could suppress rediscovery forever. That is no longer true; zero rows
   * are dashboard data only, and retry/skip decisions come from
   * hourly_ingest_state.status/next_retry_at/lease_until. This endpoint remains
   * useful for correcting displayed hourly counts, but deleting this row alone
   * is not the canonical way to requeue ingest work.
   */
  fastify.delete('/hourly-match-counts/:date/:hour/:queueId', async (req: any, reply: any) => {
    const date = req.params.date as string;
    const hour = parseInt(req.params.hour as string, 10);
    const queueId = parseInt(req.params.queueId as string, 10);

    if (isNaN(hour) || isNaN(queueId)) {
      return reply.status(400).send({ error: 'Invalid hour or queueId parameters' });
    }

    const result = await query(
      `DELETE FROM hourly_match_counts WHERE date = $1 AND "hour" = $2 AND queue_id = $3`,
      [date, hour, queueId]
    );

    return { deleted: true, date, hour, queueId };
  });

  /**
   * POST /admin/buffer/process — Manually trigger buffer processing
   * Body: { batch? } (optional batch size, default 50)
   */
  fastify.post('/buffer/process', async (req: any, reply: any) => {
    const batchSize = (req.body as any)?.batch || 50;
    const { processBufferBatch } = await import('../workers/buffer-processor.js');
    const result = await processBufferBatch(batchSize);
    return result;
  });

  /**
   * POST /admin/buffer/retention — Manually prune old terminal raw buffer rows.
   *
   * Cron normally runs this hourly through auto-ingester-scheduler. This manual
   * hook exists for incident cleanup and local verification after imports. It
   * uses the exact same worker function and advisory lock as cron: active
   * `pending`/`processing` rows are never deleted, while old `processed`/`failed`
   * rows are summarized in raw_ingest_buffer_retention_audit before removal.
   */
  fastify.post('/buffer/retention', async (req: any) => {
    const bodyReason = String((req.body as any)?.reason || '').trim();
    const reason = bodyReason ? `manual: ${bodyReason}` : 'manual admin endpoint';
    const { cleanupRawIngestBufferRetention } = await import('../workers/buffer-processor.js');
    return cleanupRawIngestBufferRetention(reason);
  });

  /**
   * POST /admin/refresh-coplay — Refresh mv_player_coplay_stats materialized view
   */
  fastify.post('/refresh-coplay', async (req: any, reply: any) => {
    try {
      await query('REFRESH MATERIALIZED VIEW mv_player_coplay_stats');
      return { message: 'Materialized view refreshed successfully' };
    } catch (err: any) {
      return reply.status(500).send({ error: { code: 'INTERNAL', message: 'Failed to refresh materialized view', details: { error: err.message } } });
    }
  });

  /**
   * POST /admin/refresh-baselines — Rebuild public.baselines now.
   *
   * public.baselines is a derived table sourced from match_players + matches +
   * champions. The normal refresh paths are the daily baseline scheduler and the
   * throttled post-ingest hook after large buffer batches. This manual endpoint
   * exists for operator verification/repair after schema changes, imports, or
   * suspected stale derived rows. It uses the same advisory lock and sync_jobs
   * tracking as the scheduled job, so manual refreshes cannot overlap with cron.
   */
  fastify.post('/refresh-baselines', async (req: any, reply: any) => {
    try {
      const { runExclusive } = await import('../workers/worker-lock.js');
      const { refreshBaselinesWithJob } = await import('../workers/baseline-tracker.js');
      const result = await runExclusive('baseline:refresh', () => refreshBaselinesWithJob('manual'));
      return { message: 'Baselines refreshed successfully', ...result };
    } catch (err: any) {
      return reply.status(500).send({ error: { code: 'INTERNAL', message: 'Failed to refresh baselines', details: { error: err.message } } });
    }
  });

  /**
   * POST /admin/refresh-derived-projections — Rebuild derived count tables.
   *
   * This repairs projection tables from durable source facts without making any
   * Hi-Rez calls. Use it after schema changes, reference-table repair, or when
   * older completed matches missed a projection stage. It rebuilds:
   * hourly_match_counts, match_compositions, bans_ranked,
   * item/talent/card/talent-card count tables, and champion_stats_ranked.
   */
  fastify.post('/refresh-derived-projections', async (req: any, reply: any) => {
    try {
      const { runExclusive } = await import('../workers/worker-lock.js');
      const { refreshDerivedProjectionsWithJob } = await import('../workers/derived-projection-tracker.js');
      const result = await runExclusive('derived-projections:refresh', () => refreshDerivedProjectionsWithJob('manual'));
      return { message: 'Derived projections refreshed successfully', ...result };
    } catch (err: any) {
      return reply.status(500).send({ error: { code: 'INTERNAL', message: 'Failed to refresh derived projections', details: { error: err.message } } });
    }
  });

  /**
   * POST /admin/api-keys/sync — Manually ask HirezRelay to sync key usage
   * from Hi-Rez and override DB.
   * Body: { devId: string } or omit to sync all keys.
   *
   * The backend deliberately does not call getDataUsed before sync. The relay's
   * sync operation already performs that outbound call and tracks it. Calling
   * getDataUsed here first would double-burn one usage request per key.
   */
  fastify.post('/api-keys/sync', async (req: any, reply: any) => {
    const devId = (req.body as any)?.devId;
    const synced: Array<{ devId: string; total_24h: number; daily_limit: number; remaining: number; status: string }> = [];

    if (devId) {
      await guardVendorFallback(req, reply, {
        scope: 'admin-api-key-sync',
        entity: devId,
      });
      await syncApiKeyUsage(devId);
      const row = await one('SELECT status, total_24h, daily_limit FROM api_keys WHERE dev_id = $1', [devId]);
      if (row) {
        synced.push({
          devId,
          total_24h: Number(row.total_24h || 0),
          daily_limit: Number(row.daily_limit || 0),
          remaining: Math.max(0, Number(row.daily_limit || 0) - Number(row.total_24h || 0)),
          status: row.status,
        });
      }
    } else {
      const keys = await query('SELECT dev_id FROM api_keys ORDER BY dev_id');
      for (const k of keys) {
        await guardVendorFallback(req, reply, {
          scope: 'admin-api-key-sync',
          entity: k.dev_id,
        });
        await syncApiKeyUsage(k.dev_id);
        const row = await one('SELECT status, total_24h, daily_limit FROM api_keys WHERE dev_id = $1', [k.dev_id]);
        if (row) {
          synced.push({
            devId: k.dev_id,
            total_24h: Number(row.total_24h || 0),
            daily_limit: Number(row.daily_limit || 0),
            remaining: Math.max(0, Number(row.daily_limit || 0) - Number(row.total_24h || 0)),
            status: row.status,
          });
        }
      }
    }

    return { synced };
  });

  /**
   * POST /admin/api-keys/reset-budgets — Manually reset and retest all API key usage budgets.
   *
   * Purpose: Clear local stats, reset health status, fetch fresh data from Hi-Rez getdataused,
   *          override database values, and return per-key results. Use this after a mass
   *          API burn event or when internal tracking drifts far from Hi-Rez reality.
   *
   * Flow per key:
   *   1. Read current state (total_24h, status) for before/after comparison.
   *   2. Call getDataUsed(devId) → fetches { Total_Requests_Today, Request_Limit_Daily }.
   *   3. Override api_keys: SET total_24h = Total_Requests_Today, daily_limit = Request_Limit_Daily, status = 'active'.
   *   4. Delete local hourly buckets in api_key_hourly_usage for this key
   *      (clean slate after authoritative server sync).
   *   5. Return per-key result with before/after values and remaining budget.
   *
   * After all keys processed: call apiKeyPool.loadKeys() to reload fresh state into memory.
   * This ensures the in-memory pool reflects the DB overrides immediately.
   *
   * Per-key errors do NOT abort the batch. If Hi-Rez API fails for one key, it's marked
   * "error" and processing continues with remaining keys.
   *
   * Auth: Admin-only (preHandler hook applies to /admin/* routes).
   * Body: Optional { devId: string } to process a single key; omit to process all.
   *
   * Affected tables: api_keys (UPDATE), api_key_hourly_usage (DELETE).
   * Source: User request 2026-06-01 — "add an api to manually reset and retest api key usage budgets."
   */
  fastify.post('/api-keys/reset-budgets', async (req: any, reply: any) => {
    const targetDevId = (req.body as any)?.devId; // Optional: process single key only.
    const devIds = targetDevId ? [targetDevId] : [];

    // Enumerate keys to process — from DB if no specific target, or just the one requested.
    const keysToProcess = devIds.length > 0
      ? devIds.map((id: string) => ({ dev_id: id }))
      : await query('SELECT dev_id FROM api_keys ORDER BY dev_id');

    const results: Array<{
      dev_id: string;
      status: 'success' | 'error';
      previous_total_24h: number | null;
      hi_rez_total_requests_today: number | null;
      hi_rez_daily_limit: number | null;
      new_total_24h: number | null;
      remaining: number | null;
      error: string | null;
    }> = [];

    for (const k of keysToProcess) {
      const devId = k.dev_id as string;
      let result: typeof results[number] = {
        dev_id: devId, status: 'success',
        previous_total_24h: null, hi_rez_total_requests_today: null,
        hi_rez_daily_limit: null, new_total_24h: null, remaining: null, error: null,
      };

      try {
        // Step 1: Read current state for before/after comparison.
        const currentRow = await one('SELECT total_24h, daily_limit, status FROM api_keys WHERE dev_id = $1', [devId]);
        if (!currentRow) throw new Error('Key not found');
        result.previous_total_24h = currentRow.total_24h ?? 0;

        // Step 2: Fetch fresh data from Hi-Rez getdataused API.
        await guardVendorFallback(req, reply, {
          scope: 'admin-api-key-budget-reset',
          entity: devId,
        });
        const hiRez = await getDataUsed(devId);
        if (!hiRez) throw new Error('Hi-Rez API returned null');
        if (!Object.prototype.hasOwnProperty.call(hiRez, 'Total_Requests_Today')) {
          throw new Error('Hi-Rez API did not return Total_Requests_Today; refusing to overwrite local usage');
        }

        const actualUsage = hiRez.Total_Requests_Today ?? 0;
        const reportedLimit = hiRez.Request_Limit_Daily ?? 0;
        const actualLimit = effectiveDailyLimit(devId, reportedLimit);
        result.hi_rez_total_requests_today = actualUsage;
        result.hi_rez_daily_limit = reportedLimit;

        // Step 3: Override api_keys — set fresh usage and the configured
        // effective limit. 2116 is capped at 15000; all other keys are capped
        // at 7500 even if a server response reports a larger value. If Hi-Rez
        // ever reports a lower positive limit, effectiveDailyLimit() respects
        // that lower server-side value for safety.
        await query(
          `UPDATE api_keys
           SET total_24h = $1,
               daily_limit = $2,
               status = CASE WHEN ($2 - $1) > 500 THEN 'healthy' ELSE 'limited' END,
               consecutive_failures = CASE WHEN ($2 - $1) > 500 THEN 0 ELSE consecutive_failures END
           WHERE dev_id = $3`,
          [actualUsage, actualLimit, devId]
        );
        result.new_total_24h = actualUsage;
        result.remaining = Math.max(0, actualLimit - actualUsage);

        // Step 4: Clear local hourly buckets for a clean local projection.
        // api_keys.total_24h remains the authoritative server-side value from
        // getdataused; fresh local calls will repopulate timestamped buckets.
        await query(`DELETE FROM api_key_hourly_usage WHERE dev_id = $1`, [devId]);

      } catch (err: any) {
        result.status = 'error';
        result.error = err.message || String(err);
      }

      results.push(result);
    }

    // After all keys processed: ask the relay to reload its in-memory pool.
    // The backend process no longer owns outbound Hi-Rez calls, so reloading a
    // backend-local ApiKeyPool would leave the real relay pool stale.
    await reloadApiKeyPool();

    const successful = results.filter(r => r.status === 'success').length;
    const failed = results.length - successful;

    return { keys: results, total_keys: results.length, successful, failed };
  });

  fastify.get('/deployment/status', async () => ({
    state: await getDeploymentState(),
    runtime: getBackendRuntimeState(),
  }));

  fastify.post('/deployment/state', async (req: any, reply: any) => {
    const body = req.body || {};
    if (!isDeploymentPhase(body.phase)) {
      return reply.status(400).send({ error: 'Invalid deployment phase' });
    }
    try {
      return await setDeploymentState({
        id: String(body.id || ''),
        phase: body.phase,
        message: typeof body.message === 'string' ? body.message : null,
        ttlSeconds: Number(body.ttlSeconds) || undefined,
      });
    } catch (error) {
      req.log.error({ error }, 'Failed to persist deployment state');
      return reply.status(503).send({ error: 'Deployment state could not be persisted' });
    }
  });

  fastify.post('/deployment/drain', async (req: any, reply: any) => {
    const body = req.body || {};
    const id = String(body.id || '').trim();
    if (!id) return reply.status(400).send({ error: 'Deployment id is required' });
    const timeoutSeconds = Math.min(300, Math.max(5, Number(body.timeoutSeconds) || 90));

    try {
      const state = await setDeploymentState({
        id,
        phase: 'draining',
        message: typeof body.message === 'string' ? body.message : null,
        ttlSeconds: Number(body.ttlSeconds) || 1800,
      });
      const drain = await quiesceBackendWork(timeoutSeconds * 1000);
      if (!drain.drained) {
        return reply.status(409).send({ state, drain });
      }
      return { state, drain };
    } catch (error) {
      req.log.error({ error }, 'Failed to drain backend for deployment');
      return reply.status(503).send({ error: 'Backend drain failed' });
    }
  });

  fastify.post('/deployment/warm', async (req: any, reply: any) => {
    const body = req.body || {};
    const id = String(body.id || '').trim();
    if (!id) return reply.status(400).send({ error: 'Deployment id is required' });
    try {
      const state = await setDeploymentState({
        id,
        phase: 'warming',
        message: typeof body.message === 'string' ? body.message : null,
        ttlSeconds: Number(body.ttlSeconds) || 1800,
      });
      await warmBackendForDeployment(fastify);
      return { state, runtime: getBackendRuntimeState() };
    } catch (error) {
      req.log.error({ error }, 'Failed to warm backend for deployment');
      return reply.status(503).send({ error: 'Backend warm-up failed' });
    }
  });

  /**
   * POST /admin/private-accounts/reconcile
   *
   * Body: { apply?: boolean }.  Omitted/false is a dry run.  Apply mode seeds
   * immutable historical match-slot observations, resolves them in time order,
   * repairs match_players links, and retires unverified v1 PartyId clusters only
   * after every detailed observation has a current identity.
   */
  fastify.post('/private-accounts/reconcile', async (req: any, reply: any) => {
    try {
      const apply = (req.body as any)?.apply === true;
      return apply
        ? await backfillPrivateAccountIdentities(true)
        : await getPrivateBackfillReport(false);
    } catch (error: any) {
      return reply.status(500).send({
        error: { code: 'PRIVATE_RECONCILIATION_FAILED', message: error?.message || String(error) },
      });
    }
  });

  /**
   * POST /admin/private-accounts/:privateId/verify-name
   *
   * Maps an operator-verified in-game name to an inferred private identity.
   * Direct Hi-Rez lookup semantics are unchanged: the account can remain
   * private while its observed matches use this verified display name.
   */
  fastify.post('/private-accounts/:privateId/verify-name', async (req: any, reply: any) => {
    const privateId = parseInt(req.params.privateId, 10);
    const body = (req.body || {}) as any;
    const name = String(body.name || '').trim();
    const evidenceRef = String(body.evidenceRef || '').trim();
    const evidenceSha256 = String(body.evidenceSha256 || '').trim().toLowerCase() || null;
    const notes = String(body.notes || '').trim() || null;
    const verifiedBy = String(body.verifiedBy || 'admin-api').trim().slice(0, 100);

    if (!Number.isInteger(privateId) || privateId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid private account ID' } });
    }
    if (!name || name.length > 100 || /[\u0000-\u001f]/.test(name)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Name must contain 1-100 printable characters' } });
    }
    if (!evidenceRef || evidenceRef.length > 2_000) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'evidenceRef is required and must be at most 2000 characters' } });
    }
    if (evidenceSha256 && !/^[0-9a-f]{64}$/.test(evidenceSha256)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'evidenceSha256 must be a lowercase SHA-256 hex digest' } });
    }
    if (notes && notes.length > 5_000) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'notes must be at most 5000 characters' } });
    }

    const result = await transaction(async client => {
      const identity = (await client.query(
        `SELECT id, verified_name
         FROM players_private
         WHERE id = $1 AND tracking_version = $2 AND is_active
         FOR UPDATE`,
        [privateId, PRIVATE_IDENTITY_VERSION],
      )).rows[0];
      if (!identity) return null;

      await client.query(
        `UPDATE private_account_name_verifications
         SET is_active = FALSE, revoked_at = now()
         WHERE private_player_id = $1 AND is_active`,
        [privateId],
      );
      const verification = (await client.query(
        `INSERT INTO private_account_name_verifications (
           private_player_id, verified_name, evidence_ref, evidence_sha256,
           notes, verified_by
         ) VALUES ($1,$2,$3,$4,$5,$6)
         RETURNING id, created_at`,
        [privateId, name, evidenceRef, evidenceSha256, notes, verifiedBy],
      )).rows[0];
      await client.query(
        `UPDATE players_private SET
           verified_name = $2,
           name_verified_at = now(),
           name_verified_by = $3,
           name_evidence_ref = $4,
           identity_status = 'verified',
           updated_at = now()
         WHERE id = $1`,
        [privateId, name, verifiedBy, evidenceRef],
      );
      const possibleDuplicates = (await client.query(
        `SELECT id FROM players_private
         WHERE id <> $1 AND is_active AND lower(verified_name) = lower($2)
         ORDER BY id`,
        [privateId, name],
      )).rows.map(row => Number(row.id));
      return { privateId, name, verificationId: Number(verification.id), verifiedAt: verification.created_at, possibleDuplicates };
    });

    if (!result) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Active private identity not found' } });
    }
    return result;
  });

  /**
   * POST /admin/private-accounts/:privateId/moderation
   * Body: { cheater: boolean, reason?: string }
   *
   * Private accounts have no Hi-Rez player ID. Moderation is attached to the
   * canonical players_private identity and follows a merge to its active row.
   */
  fastify.post('/private-accounts/:privateId/moderation', async (req: any, reply: any) => {
    const privateId = parseInt(req.params.privateId, 10);
    const body = (req.body || {}) as { cheater?: boolean; reason?: string };
    const cheater = body.cheater;
    const reason = String(body.reason || '').trim();
    if (!Number.isInteger(privateId) || privateId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid private account ID' } });
    }
    if (typeof cheater !== 'boolean') {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'cheater must be a boolean' } });
    }
    if (cheater && !reason) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'A reason is required when confirming a cheater' } });
    }
    if (reason.length > 2_000) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'reason must be at most 2000 characters' } });
    }

    const updated = await one(
      `WITH RECURSIVE identity_chain AS (
         SELECT id, merged_into_id, is_active, 0 AS depth
         FROM players_private WHERE id = $1
         UNION ALL
         SELECT next.id, next.merged_into_id, next.is_active, chain.depth + 1
         FROM players_private next
         JOIN identity_chain chain ON next.id = chain.merged_into_id
         WHERE chain.depth < 16
       ), canonical AS (
         SELECT id FROM identity_chain WHERE is_active ORDER BY depth DESC LIMIT 1
       )
       UPDATE players_private account
       SET cheater = $2,
           cheater_reason = CASE WHEN $2 THEN $3 ELSE NULL END,
           cheater_marked_at = CASE WHEN $2 THEN now() ELSE NULL END,
           updated_at = now()
       FROM canonical
       WHERE account.id = canonical.id
       RETURNING account.id, account.alias, account.verified_name,
                 account.cheater, account.cheater_reason, account.cheater_marked_at`,
      [privateId, cheater, reason || null],
    );
    if (!updated) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Private account not found' } });
    }
    return { account: updated };
  });
}
