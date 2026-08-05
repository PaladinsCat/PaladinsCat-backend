import { FastifyInstance } from 'fastify';
import { query } from '../config/db';
import { getActivePublicRequestCount } from '../services/deployment-control';
import { internalRequestHeaders } from '../services/internal-request';
import { championPageWarmUrls, type ChampionTalentCacheRow } from './champion-page-cache-urls';

const DEFAULT_INTERVAL_MS = 10 * 60 * 1000;
const DEFAULT_STARTUP_DELAY_MS = 60 * 1000;
// Keep one DB slot for cache warming by default. A single cache miss must not
// turn into several concurrent aggregate queries when a visitor arrives.
const DEFAULT_CONCURRENCY = 1;

let refreshTimer: NodeJS.Timeout | null = null;
let startupTimer: NodeJS.Timeout | null = null;
let refreshRunning = false;
let stopping = false;

function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * Touch every canonical champion and talent page-data key.
 *
 * A missing key is built immediately. Fresh keys are cheap hits, while stale
 * keys return immediately and enter the route cache's bounded background
 * revalidation queue. Running this more frequently than the cache's fresh +
 * stale lifetime keeps the first aggregate build away from public visitors.
 */
export async function warmChampionPageCache(fastify: FastifyInstance): Promise<void> {
  if (refreshRunning || stopping) return;
  // Warming is opportunistic. Do not begin its discovery query while public
  // traffic is active, and leave the database capacity for live requests.
  if (getActivePublicRequestCount() > 0) return;
  refreshRunning = true;
  const startedAt = Date.now();

  try {
    const champions = await query<ChampionTalentCacheRow>(
      `SELECT c.name, t.talent_id
       FROM champions c
       LEFT JOIN talents t ON t.champion_id = c.id
       WHERE c.id > 0
       ORDER BY c.name, t.talent_id`,
    );
    const urls = championPageWarmUrls(champions);
    const concurrency = positiveInteger(
      process.env.CHAMPION_PAGE_CACHE_WARM_CONCURRENCY,
      DEFAULT_CONCURRENCY,
    );
    let nextIndex = 0;
    let warmed = 0;
    let failed = 0;

    const worker = async () => {
      while (!stopping) {
        // Existing warm requests are allowed to finish, but never start the
        // next one while a visitor is waiting on the application.
        if (getActivePublicRequestCount() > 0) return;
        const index = nextIndex++;
        if (index >= urls.length) return;

        try {
          const response = await fastify.inject({
            method: 'GET',
            url: urls[index],
            headers: internalRequestHeaders(),
          });
          if (response.statusCode >= 400) {
            failed += 1;
            fastify.log.warn(
              { statusCode: response.statusCode, url: urls[index] },
              'Champion page cache warm-up request failed',
            );
          } else {
            warmed += 1;
          }
        } catch (error) {
          failed += 1;
          fastify.log.warn(
            { error, url: urls[index] },
            'Champion page cache warm-up request failed',
          );
        }
      }
    };

    await Promise.all(
      Array.from({ length: Math.min(concurrency, urls.length) }, () => worker()),
    );
    fastify.log.info(
      { warmed, failed, durationMs: Date.now() - startedAt },
      'Champion page cache warm-up completed',
    );
  } catch (error) {
    fastify.log.warn({ error }, 'Champion page cache warm-up failed');
  } finally {
    refreshRunning = false;
  }
}

export function startChampionPageCacheWarmer(fastify: FastifyInstance): void {
  if (refreshTimer) return;
  stopping = false;
  const intervalMs = positiveInteger(
    process.env.CHAMPION_PAGE_CACHE_WARM_INTERVAL_MS,
    DEFAULT_INTERVAL_MS,
  );
  const startupDelayMs = positiveInteger(
    process.env.CHAMPION_PAGE_CACHE_WARM_STARTUP_DELAY_MS,
    DEFAULT_STARTUP_DELAY_MS,
  );

  // Startup also launches ingest/search maintenance that uses the same DB
  // pool. Let those short catch-up jobs settle before filling a cold cache.
  startupTimer = setTimeout(() => {
    startupTimer = null;
    void warmChampionPageCache(fastify);
  }, startupDelayMs);
  startupTimer.unref();
  refreshTimer = setInterval(() => {
    void warmChampionPageCache(fastify);
  }, intervalMs);
  refreshTimer.unref();
  fastify.log.info(
    {
      intervalMs,
      startupDelayMs,
      concurrency: positiveInteger(
        process.env.CHAMPION_PAGE_CACHE_WARM_CONCURRENCY,
        DEFAULT_CONCURRENCY,
      ),
    },
    'Champion page cache warmer started',
  );
}

export function stopChampionPageCacheWarmer(): void {
  stopping = true;
  if (startupTimer) clearTimeout(startupTimer);
  if (refreshTimer) clearInterval(refreshTimer);
  startupTimer = null;
  refreshTimer = null;
}

export function isChampionPageCacheWarmerRunning(): boolean {
  return refreshRunning;
}

export async function waitForChampionPageCacheWarmer(
  timeoutMs: number,
  pollIntervalMs = 100,
): Promise<boolean> {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  while (refreshRunning && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }
  return !refreshRunning;
}

export { championPageSlug } from './champion-page-cache-urls';
