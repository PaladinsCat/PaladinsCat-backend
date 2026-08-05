import { FastifyInstance } from 'fastify';
import {
  dropActivePublicRequests,
  getActivePublicRequestCount,
  waitForActivePublicRequests,
} from './deployment-control';
import {
  areBackendSchedulersRunning,
  disableBackendSchedulers,
  enableBackendSchedulers,
} from '../workers/scheduler-registry';
import {
  isChampionPageCacheWarmerRunning,
  startChampionPageCacheWarmer,
  stopChampionPageCacheWarmer,
  waitForChampionPageCacheWarmer,
} from '../workers/champion-page-cache-warmer';
import {
  isMainSiteCacheWarmerRunning,
  startMainSiteCacheWarmer,
  stopMainSiteCacheWarmer,
  waitForMainSiteCacheWarmer,
  warmDeploymentCriticalCaches,
} from '../workers/site-cache-warmer';
import { getActiveWorkerJobs, waitForActiveWorkerJobs } from '../workers/worker-lock';

export interface BackendDrainResult {
  drained: boolean;
  activeRequests: number;
  activeWorkerJobs: string[];
  cacheWarmerRunning: boolean;
  elapsedMs: number;
  droppedRequests: number;
  oldestDroppedRequestMs: number;
}

export async function warmBackendForDeployment(fastify: FastifyInstance): Promise<void> {
  await warmDeploymentCriticalCaches(fastify);
  fastify.log.info('Deployment cache warm-up completed');
}

export async function startBackendWork(fastify: FastifyInstance): Promise<void> {
  if (process.env.CHAMPION_PAGE_CACHE_WARMER_ENABLED === 'false') {
    fastify.log.info('Champion page cache warmer disabled by environment');
  } else {
    startChampionPageCacheWarmer(fastify);
  }

  if (process.env.SITE_CACHE_WARMER_ENABLED === 'false') {
    fastify.log.info('Main site cache warmer disabled by environment');
  } else {
    startMainSiteCacheWarmer(fastify);
  }

  if (process.env.BACKEND_SCHEDULERS_ENABLED === 'false') {
    fastify.log.info('Backend schedulers disabled by environment');
  } else {
    await enableBackendSchedulers();
  }

}

export async function quiesceBackendWork(timeoutMs: number): Promise<BackendDrainResult> {
  const startedAt = Date.now();
  await disableBackendSchedulers();
  stopChampionPageCacheWarmer();
  stopMainSiteCacheWarmer();

  const remaining = () => Math.max(0, timeoutMs - (Date.now() - startedAt));
  await Promise.all([
    waitForActivePublicRequests(remaining()),
    waitForActiveWorkerJobs(remaining()),
    waitForChampionPageCacheWarmer(remaining()),
    waitForMainSiteCacheWarmer(remaining()),
  ]);

  const activeWorkerJobs = getActiveWorkerJobs();
  const cacheWarmerRunning = (
    isChampionPageCacheWarmerRunning()
    || isMainSiteCacheWarmerRunning()
  );
  let activeRequests = getActivePublicRequestCount();
  let droppedRequests = 0;
  let oldestDroppedRequestMs = 0;
  if (activeRequests > 0 && activeWorkerJobs.length === 0 && !cacheWarmerRunning) {
    const dropped = dropActivePublicRequests();
    droppedRequests = dropped.droppedRequests;
    oldestDroppedRequestMs = dropped.oldestRequestMs;
    activeRequests = getActivePublicRequestCount();
  }
  return {
    drained: activeRequests === 0 && activeWorkerJobs.length === 0 && !cacheWarmerRunning,
    activeRequests,
    activeWorkerJobs,
    cacheWarmerRunning,
    elapsedMs: Date.now() - startedAt,
    droppedRequests,
    oldestDroppedRequestMs,
  };
}

export function getBackendRuntimeState() {
  return {
    schedulersRunning: areBackendSchedulersRunning(),
    activeRequests: getActivePublicRequestCount(),
    activeWorkerJobs: getActiveWorkerJobs(),
    cacheWarmerRunning: (
      isChampionPageCacheWarmerRunning()
      || isMainSiteCacheWarmerRunning()
    ),
  };
}
