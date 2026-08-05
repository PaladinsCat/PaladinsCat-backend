import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import crypto from 'node:crypto';
import { gzipSync,gunzipSync } from 'node:zlib';
import pLimit from 'p-limit';
import { del,get,redis,set } from '../services/cache';
import { internalRequestHeaders, isInternalRequest } from '../services/internal-request';

type CacheOptions = {
  namespace: string;
  ttlSeconds: (req: FastifyRequest) => number;
  shouldCache: (req: FastifyRequest) => boolean;
  /**
   * How long an expired response may be served while a single background
   * request replaces it. Defaults to three fresh TTL windows.
   */
  staleTtlSeconds?: (req: FastifyRequest) => number;
};

const cacheHit = Symbol('routeCacheHit');
const cacheKey = Symbol('routeCacheKey');
const cacheLease = Symbol('routeCacheLease');
const refreshInFlight = new Set<string>();
const REVALIDATE_HEADER = 'x-pc-route-cache-revalidate';
const revalidationLimit = pLimit(4);
const COLD_MISS_WAIT_MS = 1_500;
const LEASE_TTL_MS = 30_000;
const IGNORED_QUERY_PARAMS = new Set(['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content', 'fbclid', 'gclid']);

/** Stable cache identity independent of query ordering or tracking params. */
export function canonicalRouteCacheUrl(rawUrl: string): string {
  const url = new URL(rawUrl, 'http://paladinscat.local');
  const entries = [...url.searchParams.entries()]
    .filter(([key]) => !IGNORED_QUERY_PARAMS.has(key.toLowerCase()))
    .sort(([leftKey, leftValue], [rightKey, rightValue]) => (
      leftKey.localeCompare(rightKey) || leftValue.localeCompare(rightValue)
    ));
  const query = new URLSearchParams(entries).toString();
  return `${url.pathname}${query ? `?${query}` : ''}`;
}

async function acquireLease(key: string): Promise<string | null | undefined> {
  if (redis.status !== 'ready') return undefined;
  try {
    const token = crypto.randomUUID();
    const acquired = await redis.set(`${key}:lease`, token, 'PX', LEASE_TTL_MS, 'NX');
    return acquired === 'OK' ? token : null;
  } catch {
    return undefined;
  }
}

async function releaseLease(key: string, token: string): Promise<void> {
  try {
    await redis.eval(
      `if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end`,
      1,
      `${key}:lease`,
      token,
    );
  } catch {
    // A lease has a short TTL; Redis failure cannot compromise correctness.
  }
}

async function waitForColdMiss(key: string): Promise<CachedRouteResponse | null> {
  const deadline = Date.now() + COLD_MISS_WAIT_MS;
  let delayMs = 25;
  while (Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    const cached = await get<unknown>(key);
    if (isCachedRouteResponse(cached)) return cached;
    delayMs = Math.min(delayMs * 2, 150);
  }
  return null;
}

export function routeCacheRevalidationHeaders(): Record<string, string> {
  return {
    ...internalRequestHeaders(),
    [REVALIDATE_HEADER]: '1',
  };
}

type CachedRouteResponse = {
  payload?: unknown;
  compressedPayload?: string;
  encoding?: 'gzip-base64';
  freshUntil: number;
};

function isCachedRouteResponse(value: unknown): value is CachedRouteResponse {
  return Boolean(
    value
    && typeof value === 'object'
    && ('payload' in value || 'compressedPayload' in value)
    && 'freshUntil' in value
    && Number.isFinite((value as CachedRouteResponse).freshUntil)
  );
}

function cachedPayload(cached:CachedRouteResponse):unknown{
  if(cached.encoding==='gzip-base64'&&cached.compressedPayload){
    return JSON.parse(gunzipSync(Buffer.from(cached.compressedPayload,'base64')).toString('utf8'));
  }
  return cached.payload;
}

export function registerReadThroughCache(fastify: FastifyInstance, options: CacheOptions) {
  fastify.addHook('preHandler', async (req: FastifyRequest, reply: FastifyReply) => {
    // A stale hit triggers an internal request to refresh the entry. That
    // request must reach the real handler rather than recursively reading the
    // same stale value.
    const revalidating = isInternalRequest(req) && req.headers[REVALIDATE_HEADER] === '1';
    if (req.method !== 'GET' || revalidating || !options.shouldCache(req)) return;

    const key = `${options.namespace}:${canonicalRouteCacheUrl(req.url)}`;
    (reply as any)[cacheKey] = key;
    const cached = await get<unknown>(key);
    if (!isCachedRouteResponse(cached)) {
      // Cross-process singleflight: one request computes a cold aggregate;
      // followers wait briefly for its Redis result instead of multiplying the
      // same database query across every backend replica.
      const lease = await acquireLease(key);
      if (lease) {
        (reply as any)[cacheLease] = lease;
        return;
      }
      if (lease === undefined) return;
      const filled = await waitForColdMiss(key);
      if (!filled) return;
      try{
        (reply as any)[cacheHit] = true;
        reply.header('X-Cache', 'COALESCED');
        return reply.send(cachedPayload(filled));
      }catch{
        delete (reply as any)[cacheHit];
        await del(key);
        return;
      }
    }

    (reply as any)[cacheHit] = true;
    const stale = cached.freshUntil <= Date.now();
    reply.header('X-Cache', stale ? 'STALE' : 'HIT');
    reply.header('X-Cache-Age', String(Math.max(0, Math.floor((Date.now() - cached.freshUntil) / 1000))));

    if (stale && !refreshInFlight.has(key)) {
      refreshInFlight.add(key);
      // Do not await this request: the caller gets the last successful
      // response immediately, while one coalesced refresh repopulates Redis.
      void revalidationLimit(async () => {
        const lease = await acquireLease(key);
        if (!lease) return;
        try {
          await fastify.inject({ method: 'GET', url: req.url, headers: routeCacheRevalidationHeaders() });
        } finally {
          await releaseLease(key, lease);
        }
      }).catch((error) => {
        fastify.log.warn({ error, url: req.url }, 'Route-cache background refresh failed');
      }).finally(() => refreshInFlight.delete(key));
    }

    try{
      return reply.send(cachedPayload(cached));
    }catch{
      delete (reply as any)[cacheHit];
      await del(key);
      return;
    }
  });

  fastify.addHook('onSend', async (req: FastifyRequest, reply: FastifyReply, payload) => {
    if (req.method !== 'GET' || !options.shouldCache(req) || (reply as any)[cacheHit]) return payload;
    if (reply.statusCode < 200 || reply.statusCode >= 300) {
      const lease = (reply as any)[cacheLease] as string | undefined;
      const key = (reply as any)[cacheKey] as string | undefined;
      if (lease && key) await releaseLease(key, lease);
      return payload;
    }

    try {
      const text = Buffer.isBuffer(payload) ? payload.toString('utf8') : String(payload);
      const freshTtlSeconds = options.ttlSeconds(req);
      const staleTtlSeconds = options.staleTtlSeconds?.(req) ?? freshTtlSeconds * 3;
      // Redis keeps the entry through the stale window. `freshUntil` is
      // separate from Redis expiry so a request never waits for an expensive
      // aggregate query simply because its fresh TTL elapsed.
      const key = (reply as any)[cacheKey] as string | undefined
        ?? `${options.namespace}:${canonicalRouteCacheUrl(req.url)}`;
      const entry:CachedRouteResponse=text.length>=64*1024
        ? {compressedPayload:gzipSync(text,{level:6}).toString('base64'),encoding:'gzip-base64',freshUntil:Date.now()+freshTtlSeconds*1_000}
        : {payload:JSON.parse(text),freshUntil:Date.now()+freshTtlSeconds*1_000};
      await set(key,entry,freshTtlSeconds+staleTtlSeconds);
      const revalidating = isInternalRequest(req) && req.headers[REVALIDATE_HEADER] === '1';
      reply.header('X-Cache', revalidating ? 'REFRESH' : 'MISS');
    } catch {
      // Cache is best-effort; leave the response untouched if serialization fails.
    }

    const lease = (reply as any)[cacheLease] as string | undefined;
    const key = (reply as any)[cacheKey] as string | undefined;
    if (lease && key) await releaseLease(key, lease);
    return payload;
  });

  fastify.addHook('onError', async (_req, reply) => {
    const lease = (reply as any)[cacheLease] as string | undefined;
    const key = (reply as any)[cacheKey] as string | undefined;
    if (lease && key) await releaseLease(key, lease);
  });
}

export async function invalidateRouteCache(namespace: string) {
  try {
    // KEYS blocks Redis in proportion to the entire keyspace. Incremental
    // SCAN keeps invalidation safe as casual traffic grows the cache catalog.
    let cursor='0';
    do {
      const [next,keys]=await redis.scan(cursor,'MATCH',`${namespace}:*`,'COUNT',200);
      cursor=next;
      if(keys.length>0)await redis.del(...keys);
    } while(cursor!=='0');
  } catch {
    // Cache invalidation is best-effort; the TTL will expire stale entries.
  }
}
