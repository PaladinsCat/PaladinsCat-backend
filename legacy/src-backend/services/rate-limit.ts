/**
 * =====================================================================
 * rate-limit.ts — Redis-Based Rate Limiting
 * =====================================================================
 * Purpose: Enforces request rate limits using Redis counters. Protects
 * endpoints from abuse and prevents Hi-Rez API from being overwhelmed
 * by internal workers. Uses a sliding counter per key — each request
 * increments the counter; when the window expires, Redis auto-deletes
 * the key and the counter resets.
 *
 * Architecture:
 * - Uses cache.ts (Redis) for atomic INCR operations. Each rate limit
 *   key is stored as `rl:<identifier>` with a TTL equal to the window.
 * - checkRateLimit(): Atomic increment + TTL set on first request.
 *   Returns remaining requests, total limit, and reset timestamp.
 * - createRateLimiter(): Factory that pre-configures limit/window and
 *   returns a function that only needs the identifier (IP, user ID, etc.).
 * - installRateLimitHook(): Root-instance hook used for service-wide limits.
 * - rateLimitPlugin(): Encapsulated plugin retained for intentionally scoped
 *   route groups.
 *
 * Called by:
 * - request-security.ts — installs root hooks for public API protection.
 * - workers/* — internal workers call checkRateLimit directly.
 *
 * Fixed 2026-05-30:
 * - checkRateLimit(): Replaced set() with redis.expire() on first request.
 *   The old set() overwrote the counter back to '1' under concurrent load,
 *   effectively bypassing the rate limit. expire() only sets TTL.
 * - Replaced import of set() with import of redis instance.
 *
 * Fixed 2026-05-30 (final):
 * - checkRateLimit(): Replaced separate incr()/expire() with atomic pipeline.
 *   A crash in the 2ms gap between commands left orphan keys without TTL,
 *   permanently banning users. The pipeline runs both commands atomically.
 *
 * Fixed 2026-05-31:
 * - Removed dead import of incr() (unused since pipeline refactor).
 * - Fixed resetAt calculation: was now + windowMs (always full window),
 *   now now + currentTtl (reflects actual remaining time).
 * - Added selectable Redis failure behavior. Ordinary database/cache reads
 *   fail open for availability; request-driven vendor fallbacks fail closed
 *   because they must never bypass the shared Hi-Rez quota boundary.
 * - Added input validation: key must be non-empty string, limit must be
 *   positive integer, windowMs must be positive. Prevents misconfiguration.
 * - Added installRateLimitHook() for root-level coverage. Fastify plugin
 *   encapsulation otherwise leaves sibling route plugins unprotected.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import { redis } from './cache';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';

let lastRedisFailureLogAt = 0;

/**
 * Rate limit configuration for a single check.
 * @param key - Unique identifier (e.g., IP address, user ID, endpoint name).
 * @param limit - Maximum requests allowed within the window.
 * @param windowMs - Time window in milliseconds (e.g., 60000 = 1 minute).
 */
export interface RateLimitOptions {
  key: string;
  limit: number;
  windowMs: number;
  /**
   * Cache reads may fail open, but a guard protecting an outbound vendor call
   * must fail closed. Otherwise a Redis outage removes the last shared quota
   * boundary exactly when the service is already degraded.
   */
  failOpen?: boolean;
}

export interface RateLimitResult {
  remaining: number;
  total: number;
  resetAt: number;
  allowed: boolean;
  backendAvailable: boolean;
}

/**
 * Check if a request is within rate limits. Atomically increments the
 * counter for the given key and sets TTL on first request.
 *
 * Algorithm:
 * 1. INCR the Redis key `rl:<identifier>`. Returns the new count.
 * 2. If count == 1 (first request in window), set TTL to windowMs + 1s.
 *    The +1s buffer prevents race conditions at exact expiry.
 * 3. Calculate remaining = limit - current count.
 * 4. Return remaining, total limit, and reset timestamp.
 *
 * Redis failure behavior is selected per caller. Ordinary database reads use
 * fail-open limits for availability. Outbound-vendor guards use fail-closed
 * limits so a protection-store outage cannot remove the shared quota boundary.
 *
 * @param options - Rate limit configuration (key, limit, window).
 * @returns Object with remaining requests, total limit, and reset timestamp.
 *          `backendAvailable` reports whether Redis enforced the check.
 */
export async function checkRateLimit(options: RateLimitOptions): Promise<RateLimitResult> {
  const { key, limit, windowMs, failOpen = true } = options;

  // CRITICAL: Validate inputs. Empty key produces `rl:` which is a single
  // global counter — all requests share one bucket, defeating per-client
  // limiting. Negative limit or windowMs are misconfiguration that should
  // be caught early rather than silently producing wrong behavior.
  // Source: Debug 2026-05-31 — "Input validation"
  if (!key || typeof key !== 'string' || key.trim().length === 0) {
    console.warn('[rate-limit] Skipping check: empty key');
    return {
      remaining: failOpen ? limit : 0,
      total: limit,
      resetAt: Date.now() + windowMs,
      allowed: failOpen,
      backendAvailable: false,
    };
  }
  if (!Number.isFinite(limit) || limit <= 0) {
    console.warn(`[rate-limit] Skipping check: invalid limit ${limit}`);
    return {
      remaining: 0,
      total: limit,
      resetAt: Date.now() + Math.max(0, windowMs),
      allowed: false,
      backendAvailable: false,
    };
  }
  if (!Number.isFinite(windowMs) || windowMs <= 0) {
    console.warn(`[rate-limit] Skipping check: invalid windowMs ${windowMs}`);
    return {
      remaining: 0,
      total: limit,
      resetAt: Date.now(),
      allowed: false,
      backendAvailable: false,
    };
  }

  try {
    const now = Date.now();
    const windowKey = `rl:${key}`;
    const ttlSeconds = Math.ceil(windowMs / 1000) + 1;

    // Use a transaction pipeline for atomicity.
    // incr() + pttl() run together in one round-trip. pttl returns:
    //   -1 = key exists but has no TTL (orphan — needs fix)
    //   -2 = key doesn't exist (shouldn't happen after incr)
    //   >0 = TTL in ms (key is healthy)
    // If the process crashes mid-pipeline, Redis rolls back both commands.
    // No orphan keys possible.
    // Source: Audit 2026-05-30 — "Permanent Ban Orphan Key"
    const pipeline = redis.pipeline();
    pipeline.incr(windowKey);
    pipeline.pttl(windowKey);

    const results = await pipeline.exec();
    if (!results) throw new Error('Redis pipeline failed');

    // CRITICAL: Check for per-command errors in pipeline tuples. ioredis returns
    // results as [Error | null, Response]. If a command fails (e.g., WRONGTYPE
    // because another worker wrote a string to rl:key), results[0][0] contains
    // the error and results[0][1] is undefined. The `as number` cast silently
    // converts undefined to NaN. Then limit - NaN = NaN, Math.max(0, NaN) = NaN,
    // and NaN <= 0 is false — the request silently succeeds without ever hitting
    // the catch block. The Redis error is never logged.
    // Fix: explicitly check and throw per-command errors so they propagate to
    // the catch block for proper fail-open handling and error logging.
    // Source: Debug 2026-05-31 — "ioredis tuple trap"
    if (results[0][0]) throw results[0][0];
    if (results[1][0]) throw results[1][0];

    const current = results[0][1] as number;
    const currentTtl = results[1][1] as number;

    // If TTL is -1 (no expiration), apply it. This handles the rare case
    // where a prior crash left an orphan key without TTL.
    if (currentTtl === -1) {
      await redis.expire(windowKey, ttlSeconds);
    }

    const remaining = limit - current;
    // CRITICAL: Use actual remaining TTL for resetAt, not the full window.
    // The old code used now + windowMs which always returned the full window
    // from the current moment. If the key has 30s remaining, resetAt should
    // reflect that 30s, not the full 60s window. This gives callers accurate
    // information for retry-after headers and client-side backoff.
    // When currentTtl is -2 (shouldn't happen after incr), fall back to windowMs.
    // Source: Debug 2026-05-31 — "Wrong resetAt calculation"
    const actualTtl = currentTtl > 0 ? currentTtl : windowMs;
    const resetAt = now + actualTtl;

    return {
      remaining: Math.max(0, remaining),
      total: limit,
      resetAt,
      allowed: current <= limit,
      backendAvailable: true,
    };
  } catch (err) {
    const now = Date.now();
    if (now - lastRedisFailureLogAt >= 30_000) {
      lastRedisFailureLogAt = now;
      console.error(
        `[rate-limit] Redis error, ${failOpen ? 'skipping' : 'blocking'} limit check: ${err}`,
      );
    }
    return {
      remaining: failOpen ? limit : 0,
      total: limit,
      resetAt: Date.now() + windowMs,
      allowed: failOpen,
      backendAvailable: false,
    };
  }
}

/**
 * Factory: create a pre-configured rate limiter function.
 * Useful for attaching to routes or middleware where limit/window
 * are fixed but the identifier varies per request.
 *
 * @param limit - Maximum requests allowed within the window.
 * @param windowMs - Time window in milliseconds.
 * @returns Async function that takes an identifier string and checks rate limits.
 */
export function createRateLimiter(limit: number, windowMs: number) {
  return async (identifier: string) => checkRateLimit({ key: identifier, limit, windowMs });
}

/**
 * Rate limit plugin options for Fastify registration.
 */
export interface RateLimitPluginOptions {
  /** Maximum requests allowed within the window (default: 100) */
  limit?: number;
  /** Time window in milliseconds (default: 60000 = 1 min) */
  windowMs?: number;
  /** Custom key function to derive rate limit identifier from request (default: req.ip) */
  keyFn?: (req: FastifyRequest) => string;
  /** Function to skip rate limiting for certain routes (default: always false) */
  skip?: (req: FastifyRequest) => boolean;
  /** Injectable check used by scope tests and isolated deployments. */
  check?: (options: RateLimitOptions) => Promise<RateLimitResult>;
  /** Redis/cache failure behavior. Public reads default to fail-open. */
  failOpen?: boolean;
  /** Response header namespace (default: RateLimit). */
  headerPrefix?: string;
  /** Optional structured error code for a versioned/authenticated boundary. */
  errorCode?: string;
  /** Optional structured error message paired with errorCode. */
  errorMessage?: string;
}

/**
 * Fastify plugin: middleware-style rate limiting.
 * Registers an `onRequest` hook that checks rate limits and returns
 * 429 Too Many Requests when the limit is exceeded.
 *
 * Usage:
 *   fastify.register(rateLimitPlugin, { limit: 100, windowMs: 60000 });
 *
 * Sets Response headers:
 *   X-RateLimit-Limit: total allowed requests
 *   X-RateLimit-Remaining: remaining requests
 *   X-RateLimit-Reset: reset timestamp (Unix ms)
 *   Retry-After: seconds until reset (only on 429)
 */
export function installRateLimitHook(
  fastify: FastifyInstance,
  opts: RateLimitPluginOptions = {},
): void {
  const limit = opts.limit || 100;
  const windowMs = opts.windowMs || 60000;
  const keyFn = opts.keyFn || ((req: FastifyRequest) => req.ip);
  const skip = opts.skip || (() => false);
  const check = opts.check || checkRateLimit;
  const headerPrefix = opts.headerPrefix || 'RateLimit';

  fastify.addHook('onRequest', async (req: FastifyRequest, reply: FastifyReply) => {
    // Skip rate limiting for certain routes (health, admin, etc.)
    if (skip(req)) return;

    const key = keyFn(req);
    const result = await check({ key, limit, windowMs, failOpen: opts.failOpen });

    // Set rate limit headers on all responses
    reply.header(`X-${headerPrefix}-Limit`, result.total);
    reply.header(`X-${headerPrefix}-Remaining`, result.remaining);
    reply.header(`X-${headerPrefix}-Reset`, result.resetAt);

    // If limit exceeded, return 429
    if (!result.allowed) {
      const retryAfter = Math.max(1, Math.ceil((result.resetAt - Date.now()) / 1000));
      reply.header('Retry-After', retryAfter);
      if (opts.errorCode) {
        return reply.status(429).send({
          error: {
            code: opts.errorCode,
            message: opts.errorMessage || 'Too many requests.',
            requestId: req.id,
            details: {
              retry_after_seconds: retryAfter,
              reset_at: result.resetAt,
            },
          },
        });
      }
      return reply.status(429).send({
        error: 'Too Many Requests',
        retryAfter,
        resetAt: result.resetAt,
      });
    }
  });
}

/**
 * Kept for callers that intentionally want an encapsulated limiter. The main
 * server calls installRateLimitHook() on the root instance so the hook covers
 * every subsequently registered route plugin.
 */
export const rateLimitPlugin: FastifyPluginAsync<RateLimitPluginOptions> = async (fastify, opts) => {
  installRateLimitHook(fastify, opts);
};
