import { FastifyInstance } from 'fastify';
import { routeCacheRevalidationHeaders } from '../utils/route-cache';
import { getActivePublicRequestCount } from '../services/deployment-control';
import {
  DEPLOYMENT_CRITICAL_API_WARM_URLS,
  MAIN_API_WARM_URLS,
  mainPageWarmPaths,
} from './site-cache-warm-targets';

const DEFAULT_INTERVAL_MS = 10 * 60 * 1000;
const DEFAULT_STARTUP_DELAY_MS = 5 * 1000;
const DEFAULT_PAGE_CONCURRENCY = 2;
const DEFAULT_PAGE_TIMEOUT_MS = 20 * 1000;
const DEFAULT_MINIMUM_PAGE_PRIORITY = 0.8;
const DEFAULT_FRONTEND_ORIGIN = 'http://frontend:3000';

let refreshTimer: NodeJS.Timeout | null = null;
let startupTimer: NodeJS.Timeout | null = null;
let refreshRunning = false;
let stopping = false;
let activePageAbort: AbortController | null = null;

function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function positiveNumber(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function frontendOrigin(): string {
  return (process.env.SITE_CACHE_WARM_FRONTEND_ORIGIN || DEFAULT_FRONTEND_ORIGIN).replace(/\/$/, '');
}

async function warmApiUrls(
  fastify: FastifyInstance,
  urls: readonly string[],
  stopWhenRequested = true,
  yieldForLiveRequests = true,
) {
  let warmed = 0;
  let deferred = 0;
  const failures: Array<{ url: string; statusCode?: number; error?: string }> = [];
  // Expensive aggregate queries are deliberately sequential. Parallel cold
  // misses contend for the same small production DB pool and make every first
  // visitor slower—the opposite of what warming is meant to achieve.
  for (const [index, url] of urls.entries()) {
    if (stopWhenRequested && stopping) break;
    // A warm aggregate can still consume database CPU and I/O. Leave that
    // capacity to live requests and resume warming on the next idle interval.
    if (yieldForLiveRequests && getActivePublicRequestCount() > 0) {
      deferred = urls.length - index;
      break;
    }
    try {
      // Bypass a fresh/stale route-cache hit and synchronously replace the
      // entry. Plain internal headers would only read stale data and launch a
      // separate four-wide refresh, defeating this worker's DB backpressure.
      const response = await fastify.inject({
        method: 'GET',
        url,
        headers: routeCacheRevalidationHeaders(),
      });
      if (response.statusCode >= 400) failures.push({ url, statusCode: response.statusCode });
      else warmed += 1;
    } catch (error) {
      failures.push({
        url,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return { warmed, deferred, failed: failures.length, failures: failures.slice(0, 10) };
}

async function fetchPage(url: string, signal: AbortSignal): Promise<boolean> {
  try {
    const response = await fetch(url, {
      headers: {
        accept: 'text/html',
        'user-agent': 'PaladinsCat-internal-cache-warmer/1.0',
      },
      redirect: 'follow',
      signal,
    });
    await response.arrayBuffer();
    return response.ok && (response.headers.get('content-type') || '').includes('text/html');
  } catch {
    return false;
  }
}

async function warmMainPages(): Promise<{
  discovered: number;
  warmed: number;
  failed: number;
  failedPaths: string[];
}> {
  const origin = frontendOrigin();
  const timeoutMs = positiveInteger(process.env.SITE_CACHE_WARM_PAGE_TIMEOUT_MS, DEFAULT_PAGE_TIMEOUT_MS);
  const sitemapResponse = await fetch(`${origin}/sitemap.xml`, {
    headers: { 'user-agent': 'PaladinsCat-internal-cache-warmer/1.0' },
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!sitemapResponse.ok) throw new Error(`Frontend sitemap returned ${sitemapResponse.status}`);
  const paths = mainPageWarmPaths(
    await sitemapResponse.text(),
    positiveNumber(process.env.SITE_CACHE_WARM_MIN_PRIORITY, DEFAULT_MINIMUM_PAGE_PRIORITY),
  );
  const concurrency = Math.min(
    positiveInteger(process.env.SITE_CACHE_WARM_PAGE_CONCURRENCY, DEFAULT_PAGE_CONCURRENCY),
    Math.max(1, paths.length),
  );
  const failures: string[] = [];
  let warmed = 0;
  let nextIndex = 0;
  activePageAbort = new AbortController();

  const worker = async () => {
    while (!stopping) {
      const index = nextIndex++;
      if (index >= paths.length) return;
      const timeout = AbortSignal.timeout(timeoutMs);
      const signal = AbortSignal.any([activePageAbort!.signal, timeout]);
      if (await fetchPage(`${origin}${paths[index]}`, signal)) warmed += 1;
      else failures.push(paths[index]);
    }
  };
  await Promise.all(Array.from({ length: concurrency }, () => worker()));
  activePageAbort = null;
  return {
    discovered: paths.length,
    warmed,
    failed: failures.length,
    failedPaths: failures.slice(0, 10),
  };
}

export async function warmDeploymentCriticalCaches(fastify: FastifyInstance): Promise<void> {
  const startedAt = Date.now();
  // The explicit deployment warm runs while scheduled work is quiesced, so it
  // must not be cancelled by the scheduler stop flag. The deploy endpoint
  // awaits this bounded four-route set before marking the swap complete.
  const api = await warmApiUrls(fastify, DEPLOYMENT_CRITICAL_API_WARM_URLS, false, false);
  if (api.failed > 0) {
    throw new Error(`Deployment cache warm-up failed for ${api.failed} critical API route(s)`);
  }
  fastify.log.info(
    { ...api, durationMs: Date.now() - startedAt },
    'Deployment-critical API cache warm-up completed',
  );
}

export async function warmMainSiteCaches(fastify: FastifyInstance): Promise<void> {
  if (refreshRunning || stopping) return;
  refreshRunning = true;
  const startedAt = Date.now();
  try {
    const api = await warmApiUrls(fastify, MAIN_API_WARM_URLS);
    const pages = stopping || api.deferred > 0
      ? { discovered: 0, warmed: 0, failed: 0, failedPaths: [] as string[] }
      : await warmMainPages();
    fastify.log.info(
      { api, pages, durationMs: Date.now() - startedAt },
      'Main site cache warm-up completed',
    );
  } catch (error) {
    if (!stopping) fastify.log.warn({ error }, 'Main site cache warm-up failed');
  } finally {
    activePageAbort = null;
    refreshRunning = false;
  }
}

export function startMainSiteCacheWarmer(fastify: FastifyInstance): void {
  if (refreshTimer) return;
  stopping = false;
  const intervalMs = positiveInteger(process.env.SITE_CACHE_WARM_INTERVAL_MS, DEFAULT_INTERVAL_MS);
  const startupDelayMs = positiveInteger(
    process.env.SITE_CACHE_WARM_STARTUP_DELAY_MS,
    DEFAULT_STARTUP_DELAY_MS,
  );
  startupTimer = setTimeout(() => {
    startupTimer = null;
    void warmMainSiteCaches(fastify);
  }, startupDelayMs);
  startupTimer.unref();
  refreshTimer = setInterval(() => void warmMainSiteCaches(fastify), intervalMs);
  refreshTimer.unref();
  fastify.log.info(
    {
      intervalMs,
      startupDelayMs,
      apiConcurrency: 1,
      pageConcurrency: positiveInteger(
        process.env.SITE_CACHE_WARM_PAGE_CONCURRENCY,
        DEFAULT_PAGE_CONCURRENCY,
      ),
      minimumPagePriority: positiveNumber(
        process.env.SITE_CACHE_WARM_MIN_PRIORITY,
        DEFAULT_MINIMUM_PAGE_PRIORITY,
      ),
    },
    'Main site cache warmer started',
  );
}

export function stopMainSiteCacheWarmer(): void {
  stopping = true;
  if (startupTimer) clearTimeout(startupTimer);
  if (refreshTimer) clearInterval(refreshTimer);
  activePageAbort?.abort();
  startupTimer = null;
  refreshTimer = null;
}

export function isMainSiteCacheWarmerRunning(): boolean {
  return refreshRunning;
}

export async function waitForMainSiteCacheWarmer(
  timeoutMs: number,
  pollIntervalMs = 100,
): Promise<boolean> {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  while (refreshRunning && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }
  return !refreshRunning;
}
