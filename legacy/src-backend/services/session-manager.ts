/**
 * =====================================================================
 * session-manager.ts — Hi-Rez Session Lifecycle Manager
 * =====================================================================
 * Purpose: Manages Hi-Rez API session creation, caching, and invalidation.
 * Every Hi-Rez API call requires a valid session key obtained via
 * createsession. Sessions expire after SESSION_TTL_MS (configurable).
 * This manager caches sessions in-memory with TTL, handles creation
 * with single-flight promise coalescing, and invalidates sessions on
 * Hi-Rez "Invalid session" errors.
 *
 * Architecture:
 * - acquireSession(): Get or create a session for a given API key.
 *   Uses per-key promise coalescing (pendingSessionPromises Map) to
 *   prevent thundering herd when sessions expire concurrently.
 * - getActiveSession(): Returns { apiKey, session } tuple for the
 *   current active key. Used by hirez.ts for all API calls.
 * - invalidateSession(): Lazy-delete a session when Hi-Rez says it's dead.
 * - sign()/timestamp(): Exclusive MD5 signature and timestamp generators.
 *   hirez.ts MUST use these exclusively to prevent signature mismatches.
 *
 *   Called by:
 * - hirez.ts — acquires sessions, signs requests, invalidates on errors.
 * - scripts/run-pipeline.ts — acquires sessions for batch operations.
 *
 * Fixed 2026-05-30:
 * - Per-key promise coalescing (Map instead of single variable) — prevents
 *   key-rotation identity crisis during concurrent waterfall rotation.
 * - HTTP 200 trap: ret_msg !== 'Approved' check before caching sessions.
 * - Audit blind spot: sessionIdToLog coalesces undefined to 'FAILED'.
 * - Deleted getValidSession() — legacy trap that dropped authKey.
 * - acquireSession() INSERT removed dev_id column — raw_ingest_buffer
 *   table has no dev_id column. The INSERT crashed silently, returning {}
 *   from getDataUsed, which prevented syncUsage from ever correcting
 *   total_24h drift. Key 2116 was stuck at 12,429 vs actual 4,252.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import crypto from 'crypto';
import { APIKey, apiKeyPool } from './api-key-pool';
import { API_CONFIG } from '../config/api';
import { query } from '../config/db';

interface Session {
  devId: string;
  sessionKey: string;
  expiresAt: number;
}

class SessionManager {
  private sessions: Map<string, Session> = new Map();

  // ----------------------------------------------------------------
  // pendingSessionPromises: Key-specific single-flight locks for session
  // creation. Uses a Map keyed by devId so that when the active key
  // rotates (waterfall from Key A to Key B), Worker 2 awaiting Key B's
  // session doesn't get handed Key A's session from a shared lock.
  // Without per-key locks, a concurrent key rotation causes:
  // Worker 1 locks promise for Key A → Worker 2 gets Key B via waterfall
  // → Worker 2 sees non-null promise → Worker 2 gets Key A's session
  // → Mixed tuple { apiKey: Key B, session: Key A's session } →
  // signature mismatch → Hi-Rez rejects with "Invalid Signature".
  // Cleared via .finally() when each request resolves or rejects.
  // Source: Feedback 2026-05-30 — "Key-Rotation Identity Crisis"
  // ----------------------------------------------------------------
  private pendingSessionPromises: Map<string, Promise<Session>> = new Map();

  async getSession(devId: string): Promise<Session | null> {
    const session = this.sessions.get(devId);
    if (session && session.expiresAt > Date.now()) {
      return session;
    }
    this.sessions.delete(devId);
    return null;
  }

  /**
   * Generate an MD5 signature for Hi-Rez API authentication.
   * Format: MD5(devId + method + authKey + timestamp).
   * Used by: acquireSession() (createsession), hirez.ts (all other endpoints).
   *
   * CRITICAL: hirez.ts MUST use this method exclusively for all signature
   * generation. Independent timestamp/sign implementations risk format
   * mismatches (e.g., off-by-one-second, different padding) that cause
   * Hi-Rez to reject with "Invalid Signature" errors.
   * Source: Feedback 2026-05-30 — "Timestamps are Misaligned for Hashing"
   *
   * @param method - The API endpoint name (e.g., "createsession", "getmatchidsbyqueue").
   * @param devId - The developer ID (API key identifier).
   * @param authKey - The authentication key (decrypted).
   * @param timestamp - The timestamp string (from this.timestamp()).
   * @returns MD5 hex digest string.
   */
  sign(method: string, devId: string, authKey: string, timestamp: string): string {
    const str = `${devId}${method}${authKey}${timestamp}`;
    return crypto.createHash('md5').update(str).digest('hex');
  }

  /**
   * Generate a UTC timestamp string in Hi-Rez format:
   * YYYYMMDDHHmmss (e.g., "20260530050200").
   * Used by: acquireSession() (createsession), hirez.ts (all other endpoints).
   *
   * CRITICAL: hirez.ts MUST use this method exclusively for all timestamp
   * generation. Independent implementations risk format mismatches that
   * cause Hi-Rez to reject with "Invalid Signature" errors.
   * Source: Feedback 2026-05-30 — "Timestamps are Misaligned for Hashing"
   *
   * @returns Timestamp string in YYYYMMDDHHmmss format.
   */
  timestamp(): string {
    const now = new Date();
    const pad = (n: number) => String(n).padStart(2, '0');
    return `${now.getUTCFullYear()}${pad(now.getUTCMonth() + 1)}${pad(now.getUTCDate())}${pad(now.getUTCHours())}${pad(now.getUTCMinutes())}${pad(now.getUTCSeconds())}`;
  }

  /**
   * Acquire a session for the given API key. Uses per-key promise
   * coalescing to prevent thundering herd: if a createsession request
   * is already in flight for THIS SPECIFIC KEY, subsequent callers
   * await the same promise instead of spawning new requests.
   *
   * Uses a Map keyed by devId (not a single promise variable) so that
   * when the active key rotates (waterfall from Key A to Key B),
   * Worker 2 awaiting Key B's session doesn't get handed Key A's
   * session from a shared lock.
   *
   * Flow:
   * 1. Check cache — if valid session exists, return it immediately.
   * 2. Check pendingSessionPromises for this devId — if in flight, await it.
   * 3. Lock: set pendingSessionPromises[devId] to the new request promise.
   * 4. Execute _executeSessionCreation() — the actual fetch + audit logic.
   * 5. Unlock: delete pendingSessionPromises[devId] via .finally().
   *
   * Source: Feedback 2026-05-30 — "Thundering Herd Race Condition"
   *         + "Key-Rotation Identity Crisis"
   *
   * @param apiKey - The API key to acquire a session for.
   * @returns A valid Session object.
   * @throws Error if session acquisition fails (HTTP error, no session_id).
   */
  async acquireSession(apiKey: APIKey): Promise<Session> {
    // ----------------------------------------------------------------
    // Step 1: Check cache. If a valid session exists (not expired),
    // return it immediately. No API call, no usage increment.
    // getSession() internally deletes expired sessions from the map.
    // ----------------------------------------------------------------
    const existing = await this.getSession(apiKey.devId);
    if (existing) return existing;

    // ----------------------------------------------------------------
    // Step 2: Per-key single-flight check. If another caller already
    // triggered createsession for THIS SPECIFIC KEY, await the same
    // promise. This prevents N concurrent createsession calls per key
    // when the session expires and multiple workers hit cache miss.
    // Using a Map ensures Worker 2 (Key B) doesn't get Worker 1's
    // promise for Key A during concurrent key rotation.
    // Source: "Key-Rotation Identity Crisis" fix.
    // ----------------------------------------------------------------
    const existingPromise = this.pendingSessionPromises.get(apiKey.devId);
    if (existingPromise) {
      return existingPromise;
    }

    // ----------------------------------------------------------------
    // Step 3: Lock this specific key. Set pendingSessionPromises[devId]
    // to the new request promise. All subsequent callers for this key
    // will hit Step 2 and await this promise. .finally() deletes the
    // entry when the request completes (success or fail).
    // ----------------------------------------------------------------
    const promise = this._executeSessionCreation(apiKey)
      .finally(() => {
        this.pendingSessionPromises.delete(apiKey.devId); // Clear lock when done
      });

    this.pendingSessionPromises.set(apiKey.devId, promise);
    return promise;
  }

  /**
   * Internal: Execute the actual createsession API call.
   * Called only by acquireSession() via the single-flight lock.
   * This method contains the raw fetch logic, audit logging, and
   * session caching — isolated from the coalescing logic above.
   *
   * Source: Refactored from acquireSession() to separate coalescing
   * from execution. See "Thundering Herd" fix.
   */
  private async _executeSessionCreation(apiKey: APIKey): Promise<Session> {
    // ----------------------------------------------------------------
    // Generate timestamp and signature for createsession API call.
    // Uses this.timestamp() and this.sign() — the same methods hirez.ts
    // uses for all other endpoints. Ensures consistent MD5 hash format.
    // ----------------------------------------------------------------
    const ts = this.timestamp();
    const sig = this.sign('createsession', apiKey.devId, apiKey.authKey, ts);
    const url = `${API_CONFIG.BASE_URL}/createsessionJson/${apiKey.devId}/${sig}/${ts}`;

   // ----------------------------------------------------------------
     // Fetch the session from Hi-Rez. This is the only network call in
     // the entire session acquisition path. All other work is local.
     // Uses AbortSignal.timeout() to prevent indefinite hangs. If Hi-Rez
     // doesn't respond within 10 seconds, the request is aborted and the
     // error propagates to the caller. Without this timeout, a stalled
     // createsession call blocks the pendingSessionPromises slot forever,
     // preventing all future session acquisition for this key.
     // Source: Debug 2026-05-31 — "fetch() has no timeout"
     // ----------------------------------------------------------------
     const startTime = Date.now();
     let responseTimeMs = 0;
     let endpointLogged = false;
     let statusCode = 0;

     // createsession is a real Hi-Rez API call. Count it before the network
     // request so HTTP errors, timeouts, and application-level 200/error
     // payloads still reduce local budget exactly like they reduce Hi-Rez
     // budget. Coalescing above ensures this only happens for actual outbound
     // session requests, not for callers reusing a cached/pending session.
     apiKeyPool.incrementUsage(apiKey.devId);

     const logCreateSessionAttempt = async () => {
       if (endpointLogged) return;
       await apiKeyPool.logEndpoint(apiKey.devId, 'createsession', responseTimeMs, 'session_management');
       endpointLogged = true;
     };

    let data: any;
    try {
      const response = await fetch(url, { signal: AbortSignal.timeout(10000) });
      statusCode = response.status;
      responseTimeMs = Date.now() - startTime;
      await logCreateSessionAttempt();
      if (!response.ok) {
        throw new Error(`Session acquisition failed: ${response.statusText}`);
      }

      data = await response.json();
    } catch (error) {
      responseTimeMs = responseTimeMs || (Date.now() - startTime);
      await logCreateSessionAttempt().catch((logErr) => {
        console.error(`[session-manager] Failed to log createsession for ${apiKey.devId}: ${logErr}`);
      });
      await apiKeyPool.recordFailure(apiKey.devId, true).catch((failureErr) => {
        console.error(`[session-manager] Failed to record createsession failure for ${apiKey.devId}: ${failureErr}`);
      });
      throw error;
    }

    // ----------------------------------------------------------------
    // CRITICAL: Hi-Rez almost never returns 4xx/5xx for application errors.
    // If the key hits daily limit, signature is invalid, or developer portal
    // is down, Hi-Rez returns HTTP 200 OK with a JSON error payload like:
    // {"ret_msg": "Exception while validating developer access"}
    // Without this check, the code bypasses !response.ok, extracts
    // data.session_id (undefined), caches a broken session, and returns it.
    // Every subsequent API call fails with "Invalid session" → retry loop.
    // Source: Feedback 2026-05-30 — "Hi-Rez HTTP 200 Trap"
    // ----------------------------------------------------------------
    if (!data || data.ret_msg !== 'Approved') {
      await apiKeyPool.recordFailure(apiKey.devId, true);
      throw new Error(`Hi-Rez Session API Error: ${data?.ret_msg || 'Unknown Error'}`);
    }
    await apiKeyPool.recordSuccess(apiKey.devId);

    // ----------------------------------------------------------------
    // Save raw session response to DB for audit trail.
    // Coalesce data.session_id to 'FAILED' if undefined — safety net.
    // The ret_msg check above should catch this case, but if Hi-Rez
    // ever returns ret_msg='Approved' without a session_id (unlikely),
    // this prevents PostgreSQL from throwing on undefined → $6.
    // Source: Feedback 2026-05-30 — "Database Audit Blind Spot"
    // ----------------------------------------------------------------
    const sessionIdToLog = data.session_id || 'FAILED';

    try {
      await query(
        `INSERT INTO raw_ingest_buffer (endpoint, params, raw_data, status_code, session_id, response_time_ms, status, entity_type)
         VALUES ($1, $2, $3, $4, $5, $6, 'processed', 'audit')`,
        ['createsession', JSON.stringify([]), JSON.stringify(data), statusCode || 200, sessionIdToLog, responseTimeMs]
      );
    } catch (err) {
      // Non-critical — audit log failure should not block session creation
      console.error(`[RAW API] Failed to save session response: ${err}`);
    }

    // ----------------------------------------------------------------
    // Safety guard: if no session_id was returned despite ret_msg='Approved',
    // don't cache a broken session. Throw so the caller can retry.
    // This is unlikely but protects against edge cases.
    // ----------------------------------------------------------------
    if (!data.session_id) {
      throw new Error(`Session acquisition returned no session_id despite Approved: ${JSON.stringify(data)}`);
    }

    const session: Session = {
      devId: apiKey.devId,
      sessionKey: data.session_id,
      expiresAt: Date.now() + API_CONFIG.SESSION_TTL_MS,
    };

    this.sessions.set(apiKey.devId, session);
    return session;
  }

 /**
   * Invalidate (delete) a cached session without creating a new one.
   * Used when Hi-Rez explicitly says "Invalid session" — the session is
   * dead and needs to be recreated on the next API call.
   *
   * Unlike refreshSession(), this does NOT call createsession immediately.
   * It just removes the cache entry. The next acquireSession() call will
   * create a fresh session. This avoids wasting a createsession call when
   * the current API call will be retried anyway.
   *
   * Source: apihandling.md — "Only nuke the session if Hi-Rez tells you
   * the session is dead."
   *
   * @param devId - The key whose session should be invalidated.
   */
  async invalidateSession(devId: string): Promise<void> {
    this.sessions.delete(devId);
  }

  /**
   * Get a valid session for the active key. Unlike the old getValidSession(),
   * this does NOT increment usage if a cached session is returned.
   *
   * Flow:
   * 1. getActiveKey() — picks the active key (no usage increment, no DB query).
   * 2. acquireSession(key) — checks cache first; creates session only if needed.
   * 3. acquireSession() calls incrementUsage() ONLY when createsession API is called.
   *
   * Source: apihandling.md — "Retrieves a valid session bound to the CURRENT Active Key."
   *
   * @returns Object with both the API key and the valid session.
   */
  async getActiveSession(): Promise<{ apiKey: APIKey; session: Session }> {
    // Pick the active key — no usage increment, no DB query
    const apiKey = await apiKeyPool.getActiveKey();

    // acquireSession() checks cache first; creates session only if expired/missing
    // It calls incrementUsage() internally only when createsession API is actually called
    const session = await this.acquireSession(apiKey);

    return { apiKey, session };
  }

}

export const sessionManager = new SessionManager();
