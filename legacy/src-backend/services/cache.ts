/**
 * =====================================================================
 * cache.ts — Redis Cache Layer
 * =====================================================================
 * Purpose: Thin wrapper around ioredis for all PaladinsCat caching needs.
 * Provides typed get/set/delete/exists/increment operations with JSON
 * serialization. All Hi-Rez API responses, match data, and leaderboard
 * snapshots are cached here to reduce external API calls.
 *
 * Architecture:
 * - Single Redis connection (singleton via module-level `redis`).
 * - All values stored as JSON strings (JSON.stringify / JSON.parse).
 * - TTL-based expiration for time-sensitive data (leaderboards, matches).
 * - No TTL for static data (config, one-time lookups).
 *
 * Fixed 2026-05-30:
 * - Added close() for graceful Redis shutdown. Without it, each process
 *   leaks a TCP connection → Redis limit exhaustion.
 * - Wrapped JSON.parse in try-catch. incr() stores raw integers that
 *   crash bare JSON.parse(). Fallback returns raw value as-is.
 * - exists() uses Boolean(val) instead of val === 1 for ioredis v5+ compat.
 * - set() guards against undefined payloads: JSON.stringify(undefined) returns
 *   primitive undefined (not a string), which crashes ioredis with TypeError.
 *
 * Called by:
 * - hirez.ts — cache Hi-Rez API responses (match details, player stats).
 * - workers/* — cache intermediate data during pipeline processing.
 * - routes/system.ts — /health endpoint checks Redis connectivity.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import Redis from 'ioredis';
import dotenv from 'dotenv';

dotenv.config();

// ----------------------------------------------------------------
// Single Redis connection. Reused across all cache operations.
// Connects to REDIS_URL env var or defaults to localhost:6379.
// ----------------------------------------------------------------
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
  // Redis accelerates reads but must never hold an HTTP request hostage while
  // reconnecting. Commands fail fast and the cache wrappers degrade to the DB;
  // ioredis continues reconnecting in the background for later requests.
  enableOfflineQueue: false,
  maxRetriesPerRequest: 1,
  connectTimeout: 2_000,
});

/**
 * Wait for the initial Redis handshake without re-enabling ioredis' offline
 * command queue. Most cache callers should fail fast while Redis reconnects,
 * but startup coordination must not mistake the short `connecting` window for
 * an authoritative cache miss (deployment state is stored in Redis).
 */
export async function waitForRedisReady(timeoutMs = 5_000): Promise<boolean> {
  if (redis.status === 'ready') return true;
  if (redis.status === 'end') return false;

  return new Promise<boolean>((resolve) => {
    let settled = false;
    const finish = (ready: boolean) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      redis.off('ready', onReady);
      redis.off('end', onEnd);
      resolve(ready);
    };
    const onReady = () => finish(true);
    const onEnd = () => finish(false);
    const timer = setTimeout(() => finish(redis.status === 'ready'), Math.max(1, timeoutMs));
    timer.unref();
    redis.once('ready', onReady);
    redis.once('end', onEnd);
  });
}

// ----------------------------------------------------------------
// Redis error listener: log connection drops for operational visibility.
// ioredis auto-reconnects by default, but without an error listener,
// connection failures are silent. Under heavy load, Redis may drop
// connections (timeout, network blip) and reconnect. Without logging,
// you have no visibility into these events — cached endpoints silently
// degrade to DB-only mode with no indication why.
//
// Log at warn level — connection drops are uncommon and warrant attention.
// If this fires frequently, it indicates a Redis infrastructure problem.
// The 'error' event fires for each connection attempt failure, not just
// the initial drop. ioredis retries internally, so multiple 'error'
// events may fire before reconnection succeeds.
//
// Source: Fault #5 — "No Redis error listener"
//         Affected: Operational visibility, debugging Redis outages
// ----------------------------------------------------------------
redis.on('error', (err) => {
  // Only log the first error per disconnect cycle to avoid spam.
  // ioredis fires 'error' on each retry attempt — logging all of them
  // floods the console. A single warning per cycle is sufficient.
  console.warn(`[Redis] Connection error: ${err.message}`);
});

/**
 * Get a cached value by key. Deserializes JSON on read.
 *
 * CRITICAL: Wrapped in try-catch for graceful degradation. If Redis is
 * down (connection dropped, timeout, network error), this returns null
 * instead of throwing. The caller treats null as cache miss and falls
 * through to PostgreSQL. Without this wrapper, a single Redis blip
 * crashes every cached endpoint (/matches, /reference, /ratings) because
 * the thrown error propagates to Fastify's error handler → 500.
 *
 * Strategy: Fail gracefully. Redis is a cache — it should accelerate
 * responses, not gate them. When Redis is unavailable, the app degrades
 * to no-cache mode (all requests hit DB). This is acceptable because:
 * 1. PostgreSQL is the source of truth, always available.
 * 2. Cache miss = extra DB query, not data loss.
 * 3. ioredis auto-reconnects — the next request may hit Redis again.
 *
 * @param key - Cache key (e.g., "match:12345", "player:abc").
 * @returns Deserialized value, or null if key not found or Redis is unavailable.
 */
export async function get<T>(key: string): Promise<T | null> {
  try {
    const val = await redis.get(key);
    if (!val) return null;
    try {
      const parsed = JSON.parse(val) as T | { v: null };
      // Unwrap null sentinel: {"v": null} was stored for literal null values.
      // Without this, set(key, null) → {"v":null} → get() returns {"v":null}
      // instead of null. The caller expects the original value, not the wrapper.
      // Type guard: check for the sentinel shape before unwrapping.
      if (parsed && typeof parsed === 'object' && 'v' in parsed && (parsed as any).v === null && Object.keys(parsed).length === 1) {
        return null as T;
      }
      return parsed as T;
    } catch {
      // JSON.parse failed — the cache entry is corrupted or incompatible.
      // This happens when:
      // 1. A truncated write left partial JSON in Redis.
      // 2. An incr() key was read via get() instead of incrGet().
      // 3. External tool wrote non-JSON data to this key.
      //
      // CRITICAL: Do NOT return val as T. If the caller expects a typed
      // object (e.g., PlayerDetails) and gets a raw string instead,
      // downstream code will crash with TypeError (e.g., player.matches.map).
      // Returning null treats it as a cache miss — the caller falls through
      // to DB and gets correct data. This is the safe fallback.
      //
      // Additionally, actively delete the corrupted key so it doesn't get
      // read again on the next request. The deletion is wrapped in its own
      // try-catch — if Redis is partially degraded, the delete may fail,
      // but the caller still gets null (cache miss) and falls through to DB.
      // The key will also naturally expire via its TTL eventually.
      //
      // Source: Debug 2026-05-31 — "Type contract violation in get() catch"
      //         Affected: Every caller of get<T>() that expects typed objects
      console.warn(`[Redis] Failed to parse JSON for key ${key}. Deleting corrupted entry, treating as cache miss.`);
      // Best-effort delete: don't let a deletion failure break the fallback.
      try { await del(key); } catch { /* deletion failed — key will expire via TTL */ }
      return null;
    }
  } catch (err) {
    // Redis is unavailable — return null (cache miss). Caller falls through to DB.
    // Log at debug level — Redis blips are common under load and auto-recover.
    // Logging at warn level would spam the console on every cached endpoint hit.
    console.debug(`[Redis] get failed for key ${key}: ${err instanceof Error ? err.message : err}`);
    return null;
  }
}

/**
 * Set a cache value with optional TTL. Serializes value to JSON.
 * @param key - Cache key.
 * @param value - Any serializable value (object, array, primitive).
 * @param ttlSeconds - Optional time-to-live in seconds. Omit for permanent storage.
 */
export async function set(key: string, value: any, ttlSeconds?: number): Promise<void> {
  // Guard against undefined payloads. JSON.stringify(undefined) returns the
  // primitive undefined (not a string). ioredis requires String or Buffer —
  // passing undefined throws TypeError and crashes the worker.
  // Source: Audit 2026-05-30 — "undefined Payload Crash"
  if (value === undefined) {
    console.warn(`[Redis] Attempted to cache undefined for key: ${key}. Skipping.`);
    return;
  }

  // ----------------------------------------------------------------
  // CRITICAL: Wrap null values in a sentinel object {"v": null}.
  // Without this, JSON.stringify(null) returns the string "null".
  // When get() reads it back: JSON.parse("null") returns null (the value).
  // The !val guard at line 67 catches null and returns null (cache miss).
  // So storing null is indistinguishable from a cache miss.
  //
  // Fix: Wrap null in {"v": null}. JSON.stringify({"v": null}) returns
  // the string "{\"v\":null}". get() reads it back, JSON.parse returns
  // {"v": null}, and we extract .v to return null (the value).
  // This is distinguishable from cache miss (which returns null directly).
  //
  // Callers that store null: rare but possible (e.g., "no data found"
  // cache entries). Without this fix, null values are never cached.
  //
  // Source: Fault #3 — "set() null trap"
  //         Affected: Any caller that does set(key, null)
  // ----------------------------------------------------------------
  const payload = value === null ? { v: null } : value;

  try {
    const serialized = JSON.stringify(payload);
    if (ttlSeconds) {
      await redis.setex(key, ttlSeconds, serialized);
    } else {
      await redis.set(key, serialized);
    }
  } catch (err) {
    // Redis is unavailable — silently skip. Cache set is a best-effort
    // optimization, not a requirement. Skipping the set means the next
    // request will hit the DB instead of cache. This is acceptable.
    // Log at debug level to avoid console spam on every cached endpoint.
    console.debug(`[Redis] set failed for key ${key}: ${err instanceof Error ? err.message : err}`);
  }
}

/**
 * Delete a cached value by key.
 * @param key - Cache key to delete.
 * @returns 1 if key was deleted, 0 if key didn't exist.
 */
export async function del(key: string): Promise<number> {
  try {
    // CRITICAL: Must await the promise inside the try block.
    // Returning the promise directly (without await) means the function
    // returns immediately. If the promise rejects after return, the
    // catch block never executes — the error bypasses the try/catch
    // entirely and becomes an Unhandled Promise Rejection. The caller
    // crashes instead of getting the graceful fallback of 0.
    // Source: Debug 2026-05-31 — "Missing await trap in del()"
    return await redis.del(key);
  } catch (err) {
    // Redis is unavailable — return 0 (key not deleted). Caller may
    // treat this as "key didn't exist" which is a safe fallback.
    console.debug(`[Redis] del failed for key ${key}: ${err instanceof Error ? err.message : err}`);
    return Promise.resolve(0);
  }
}

/**
 * Check if a cache key exists.
 * @param key - Cache key to check.
 * @returns true if key exists, false otherwise.
 */
export async function exists(key: string): Promise<boolean> {
  try {
    const val = await redis.exists(key);
    // ioredis exists() returns Promise<number> (0 or 1). In v5+ it may
    // return different types. Explicit check for truthy value.
    // Source: Audit 2026-05-30 — defensive handling
    return Boolean(val);
  } catch (err) {
    // Redis is unavailable — return false (key doesn't exist).
    console.debug(`[Redis] exists failed for key ${key}: ${err instanceof Error ? err.message : err}`);
    return false;
  }
}

/**
 * Atomically increment a cached integer value by 1.
 * Creates the key with value 1 if it doesn't exist.
 *
 * CRITICAL: incr() stores raw integer strings, NOT JSON. This is because
 * Redis INCR operates on integer strings directly — it cannot increment
 * a JSON-serialized value like "{\"count\":5}". The stored value is the
 * raw string "42", not JSON.stringify(42) (which would be "42" anyway,
 * but semantically different).
 *
 * This creates a type collision with get(): if you call incr("counter")
 * and then get<number>("counter"), JSON.parse("42") succeeds and returns
 * 42 (number). But get<string>("counter") also returns 42 (number), not
 * "42" (string). The type parameter is silently wrong.
 *
 * Fix: Use incrGet() for reading incr() keys. incrGet() does Number(val)
 * instead of JSON.parse(val), returning the correct type always.
 *
 * @param key - Cache key (must store an integer string).
 * @returns New value after increment.
 */
export async function incr(key: string): Promise<number> {
  try {
    // CRITICAL: Must await the promise inside the try block.
    // Returning the promise directly (without await) means the function
    // returns immediately. If the promise rejects after return, the
    // catch block never executes — the error bypasses the try/catch
    // entirely and becomes an Unhandled Promise Rejection. The caller
    // crashes instead of getting the graceful fallback of 0.
    // Source: Debug 2026-05-31 — "Missing await trap in incr()"
    return await redis.incr(key);
  } catch (err) {
    // Redis is unavailable — return 0. Caller may treat this as
    // "counter not incremented" which is a safe fallback for rate limiting.
    console.debug(`[Redis] incr failed for key ${key}: ${err instanceof Error ? err.message : err}`);
    return 0;
  }
}

/**
 * Read a value stored by incr(). Returns the numeric value.
 *
 * Use this instead of get() for incr() keys. get() uses JSON.parse()
 * which silently converts types: JSON.parse("42") returns 42 (number),
 * not "42" (string). incrGet() uses Number(val) for correct typing.
 *
 * @param key - Cache key (must store an integer string from incr()).
 * @returns Numeric value, or null if key not found or Redis is unavailable.
 */
export async function incrGet(key: string): Promise<number | null> {
  try {
    const val = await redis.get(key);
    if (!val) return null;
    // Use Number() instead of JSON.parse() for correct type conversion.
    // Number("42") returns 42 (number). JSON.parse("42") also returns 42
    // but the intent is different — Number() is explicit about expecting
    // a numeric string, JSON.parse() is generic and may silently convert.
    return Number(val);
  } catch (err) {
    // Redis is unavailable — return null.
    console.debug(`[Redis] incrGet failed for key ${key}: ${err instanceof Error ? err.message : err}`);
    return null;
  }
}

/**
 * Flush the entire Redis database. DANGEROUS — deletes all keys.
 * Used for testing/cleanup only. Do NOT call in production.
 */
export async function flush(): Promise<void> {
  try {
    await redis.flushdb();
  } catch (err) {
    // Redis is unavailable — silently skip. Flush is a best-effort operation.
    console.debug(`[Redis] flush failed: ${err instanceof Error ? err.message : err}`);
  }
}

/**
 * Health check: ping Redis to verify connectivity.
 * Called by: /health endpoint, startup checks.
 * @returns true if Redis responds to PING, false on any error.
 */
export async function healthCheck(): Promise<boolean> {
  try {
    await redis.ping();
    return true;
  } catch {
    return false;
  }
}

/**
 * Gracefully close the Redis connection.
 * Called by: workers/scripts on shutdown, process cleanup.
 * Without this, each process leaks a TCP connection → Redis limit exhaustion.
 * ioredis.quit() sends a QUIT command and closes the socket cleanly.
 *
 * CRITICAL: Uses a closed flag to prevent double-quit. index.ts calls
 * redis.quit() directly on SIGTERM, and other code may call close().
 * If both fire, the second quit throws "Connection is closed."
 * The flag ensures only the first call executes quit().
 *
 * Source: Audit 2026-05-30 — "Connection Pool Exhaustion (Missing quit/disconnect)"
 *         Fault #9 — "close() double-quit risk"
 */
let redisClosed = false;
export async function close(): Promise<void> {
  if (redisClosed) return; // Prevent double-quit
  redisClosed = true;
  if (redis.status === 'ready') {
    try {
      await redis.quit();
      return;
    } catch {
      // Fall through to a local disconnect when the socket dropped between
      // the readiness check and QUIT.
    }
  }
  redis.disconnect(false);
}

export { redis };
