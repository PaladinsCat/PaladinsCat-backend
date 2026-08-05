/**
 * =====================================================================
 * api-key-pool.ts - API Key Pool with Waterfall Selection & In-Memory Tracking
 * =====================================================================
 * Purpose: Manages the pool of Hi-Rez API keys with intelligent selection,
 * in-memory usage tracking, and async DB flushing. Replaces per-call DB
 * queries with in-memory counters flushed every 60 seconds. Implements
 * waterfall architecture: sticks to one active key until budget drops
 * below threshold, then falls to the next healthy key.
 *
 * Architecture:
 * - InMemKey: Hot-path representation. Tracks usedToday, pendingIncrements,
 *   consecutiveFailures in memory. No DB queries on the critical path.
 * - getActiveKey(): Sticky active key with waterfall fallback. Returns
 *   the first healthy key with more than BUDGET_THRESHOLD (100) calls left. Caches
 *   activeDevId to prevent sorting thrash.
 * - incrementUsage(): In-memory only. Increments usedToday and
 *   pendingIncrements. Actual DB write happens via flushUsageToDB().
 * - flushUsageToDB(): Async batch flush every 60s. Writes pending
 *   increments to api_key_hourly_usage and api_keys.total_24h. Replaces
 *   per-call DB writes.
 * - recordSuccess/recordFailure: Update key status based on outcomes.
 *   recordSuccess only recovers 'unhealthy' → 'healthy' (never overwrites 'limited').
 * - syncUsage(): Hourly drift correction. Compares internal usage vs
 *   Hi-Rez's getdataused and keeps api_keys.total_24h authoritative.
 *
 * Called by:
 * - session-manager.ts - acquires API keys for session creation.
 * - hirez.ts - increments usage after successful API calls.
 * - workers/api-key-sync.ts - hourly sync with Hi-Rez usage data.
 * - routes/system.ts - /api/keys monitoring endpoint.
 *
 * Fixed 2026-05-30:
 * - initialize() SELECT now includes calls_total, consecutive_failures
 *   (was silently resetting them to 0 on every restart).
 * - syncUsage() captures internal snapshot BEFORE UPDATE (drift was always 0).
 * - recordSuccess only recovers 'unhealthy', never overwrites 'limited'.
 * - recordFailure no longer manipulates calls_total (prevents double-counting).
 * - syncUsage() revives 'limited'/'unhealthy' keys by checking actual rolling 24h usage.
 * - syncUsage() field mapping fixed: Hi-Rez returns Total_Requests_Today /
 *   Request_Limit_Daily (not calls_used/daily_limit). Without this fix,
 *   syncUsage always read 0 and never corrected total_24h drift.
 *   Key 2116 total_24h was stuck at 12,429 vs actual 4,252 - 3x inflation.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import { query, one, pool } from '../config/db';
import { existsSync, readFileSync } from 'node:fs';
import { decrypt, encrypt, validateMEK, smokeTest } from '../utils/crypto';
import { API_CONFIG } from '../config/api';
import {
  BUDGET_THRESHOLD,
  DEFAULT_DAILY_LIMIT,
  SPECIAL_DAILY_LIMITS,
  configuredDailyLimit,
  effectiveDailyLimit,
} from '../contracts/hirez-key-policy';

export {
  BUDGET_THRESHOLD,
  DEFAULT_DAILY_LIMIT,
  SPECIAL_DAILY_LIMITS,
  configuredDailyLimit,
  effectiveDailyLimit,
} from '../contracts/hirez-key-policy';

export interface APIKey {
  devId: string;
  authKey: string;
  status: 'healthy' | 'limited' | 'unhealthy';
  daily_limit: number;
  used_24h: number;
  remaining: number;
  reserve_threshold: number;
  calls_total: number;
  consecutive_failures: number;
  last_used: string;
}

// Internal representation (snake_case for DB compatibility)
interface ApiKey {
  dev_id: string;
  auth_key: string;
  status: 'healthy' | 'limited' | 'unhealthy';
  daily_limit: number;
  calls_total: number;
  total_24h: number;
  consecutive_failures: number;
  last_used: string;
}

// ----------------------------------------------------------------
// InMemKey: Internal representation for in-memory tracking.
// Replaces the DB-heavy ApiKey interface for hot-path operations.
// - usedToday: mirrored from total_24h on load; incremented in-memory on each API call.
// - pendingIncrements: accumulator for async DB flush (avoids per-call DB writes).
// - consecutiveFailures: tracks key faults; >= 5 → auto-mark unhealthy.
// ----------------------------------------------------------------
interface InMemKey {
  devId: string;
  authKey: string;
  status: 'healthy' | 'limited' | 'unhealthy';
  dailyLimit: number;        // effective limit: 2116=15000, all other keys=7500 unless server reports lower
  usedToday: number;         // mirrored from api_keys.total_24h; incremented in-memory
  pendingIncrements: number; // accumulator flushed to DB every 60s
  consecutiveFailures: number;
  callsTotal: number;        // lifetime counter (only updated on DB flush)
}

// ----------------------------------------------------------------
// BUDGET_THRESHOLD: Waterfall trigger. When remaining budget is <= 100,
// fall to the next healthy key. This keeps a hard reserve for session,
// monitoring, and emergency manual calls.
// Source: apihandling.md feedback - "falls to the next available key"
// when remaining budget falls below the configured reserve.
// ----------------------------------------------------------------
function hasUsableBudget(used: number, limit: number): boolean {
  // Reserve is inclusive: once remaining calls are 100 or lower, the key is
  // turned off and waterfall moves on. This keeps a hard safety margin for
  // sync/getdataused/session calls and avoids scraping the last few requests.
  return (limit - used) > BUDGET_THRESHOLD;
}

// ----------------------------------------------------------------
// FLUSH_INTERVAL_MS: How often pending usage increments are batched
// and written to the database. 60 seconds balances I/O reduction with
// acceptable staleness. During this window, usage is accurate in-memory
// but may lag in the DB by up to 60s.
// Source: apihandling.md feedback - "asynchronously flushed to the
// database every 60 seconds to save I/O."
// ----------------------------------------------------------------
const FLUSH_INTERVAL_MS = 60000; // 1 minute

function currentUtcHourBucket(): Date {
  const hour = new Date();
  hour.setUTCMinutes(0, 0, 0);
  return hour;
}

export class ApiKeyPool {
  // ----------------------------------------------------------------
  // keys: In-memory array of all API keys. Loaded from DB on startup.
  // All hot-path operations (getActiveKey, incrementUsage) work against
  // this array - no DB queries. DB is updated asynchronously via flushTimer.
  // ----------------------------------------------------------------
  private keys: InMemKey[] = [];

  // ----------------------------------------------------------------
  // activeDevId: The "sticky" key. All API calls use this key until it
  // hits BUDGET_THRESHOLD remaining or reaches 5 consecutive failures.
  // This eliminates the "sorting trap" where keys oscillate between calls.
  // Source: apihandling.md - "The pool holds one activeDevId."
  // ----------------------------------------------------------------
  private activeDevId: string | null = null;

  private initialized = false;

  // ----------------------------------------------------------------
  // flushTimer: Background interval that batches pendingIncrements and
  // writes them to the database. Started on initialize(), cleared on
  // process exit. This replaces per-call DB writes in the old increment().
  // ----------------------------------------------------------------
  private flushTimer: NodeJS.Timeout | null = null;

  // ----------------------------------------------------------------
  // revivalPromise: Single-flight lock for the graceful degradation
  // revival loop in getActiveKey(). Prevents the "thundering herd" where
  // 50 concurrent requests all hit an exhausted pool and each spawns its
  // own syncUsage() loop - resulting in 250 concurrent getdataused calls
  // to Hi-Rez (50 requests × 5 keys) → immediate rate-limit or IP ban.
  //
  // Works like session-manager's pendingSessionPromises Map: the first
  // caller creates the promise, subsequent callers await the same one.
  // Cleared after completion (success or failure) so the next exhaustion
  // event gets a fresh revival cycle.
  //
  // Source: Debug report 2026-05-31 - "Thundering Herd on Revival Loop"
  //         Affected: getActiveKey() graceful degradation (Phase 5)
  // ----------------------------------------------------------------
  private revivalPromise: Promise<void> | null = null;

  // ----------------------------------------------------------------
  // lastRevivalAttempt: Timestamp (ms) of the last revival attempt.
  // Prevents the revival loop from firing too frequently when all keys
  // are exhausted. Without this, every failed API call triggers syncUsage
  // on ALL keys — burning getdataused calls rapidly even when keys have
  // no budget available yet. With rolling 24h window, keys gradually free
  // up as old calls age out — checking once per hour is sufficient.
  //
  // REVIVAL_COOLDOWN_MS = 30 minutes (half the hourly sync interval).
  // This ensures revival checks happen between hourly sync cycles without
  // overwhelming Hi-Rez with redundant getdataused calls on exhausted keys.
  // If all keys are truly exhausted, waiting 30 min before re-checking is
  // acceptable — discovery will retry at :30 anyway.
  //
  // Source: Budget burn issue 2026-06-01 — backup keys (3693/4114/4187/4556)
  //         burned through entire 7500-call budget due to unlimited revival
  //         spam. Each syncUsage call burns a Hi-Rez request even when it
  //         fails with "Daily request limit reached".
  //         Affected: getActiveKey() revival loop, all callers via hirez.ts.
  // ----------------------------------------------------------------
  private lastRevivalAttempt = 0;
  private static readonly REVIVAL_COOLDOWN_MS = 30 * 60 * 1000; // 30 minutes

  // ----------------------------------------------------------------
  // Safety net: per-key revival cooldown for estimate-based revivals.
  // When a key is revived via hourly estimate (because getdataused failed
  // with "Daily request limit reached"), the estimate may be stale — old
  // hours were perpetually wiped before Phase 3 fix, so estimated usage
  // was way lower than Hi-Rez's actual rolling window. Result: key gets
  // revived → discovery tries it → immediately fails "limit reached"
  // → marked limited again → next sync cycle revives from estimate again.
  // Rapid toggle wastes getdataused calls and creates noise.
  //
  // Fix: Track last revival timestamp per key. If a key was recently
  // revived via estimate, skip the estimate check for this key until
  // ESTIMATE_REVIVAL_COOLDOWN has elapsed. The normal syncUsage path
  // (getdataused succeeds) still works — it's only the estimate fallback
  // that's rate-limited.
  //
  // Source: Feedback - "Limited keys revived but Hi-Rez still says limited"
  //         — hourly table data was stale after Phase 3 fix, causing false
  //         positive revivals. Safety net prevents rapid toggle until data
  //         converges with reality.
  //         Affected: syncUsage() estimate fallback path (Phase 4).
  // ----------------------------------------------------------------
  private lastEstimateRevivalByDevId = new Map<string, number>();
  private static readonly ESTIMATE_REVIVAL_COOLDOWN_MS = 5 * 60 * 1000; // 5 min

  private async ensureSchemaCompatibility(): Promise<void> {
    // The key pool is one of the first services initialized in real relay mode.
    // If a fresh database has the older `api_keys` shape, initialization fails
    // before the relay can enforce quota safety. Keep this compatibility block
    // here as a defensive boot-time guard, while the canonical SQL/migration
    // files define the desired structure for managed deployments.
    await query(`ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS total_24h INT DEFAULT 0`);
    await query(`ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS daily_limit INT DEFAULT ${DEFAULT_DAILY_LIMIT}`);
    await query(`ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS consecutive_failures INT DEFAULT 0`);
    await query(`ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS last_sync_at TIMESTAMPTZ`);
    await query(`ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS last_sync_error TEXT`);
    await query(`ALTER TABLE api_keys ALTER COLUMN daily_limit SET DEFAULT ${DEFAULT_DAILY_LIMIT}`);
    await query(`ALTER TABLE api_log ADD COLUMN IF NOT EXISTS consumer VARCHAR(80) NOT NULL DEFAULT 'legacy'`);
    await query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1
          FROM pg_constraint constraint_row
          JOIN pg_class table_row ON table_row.oid = constraint_row.conrelid
          WHERE table_row.relname = 'api_log'
            AND constraint_row.contype = 'p'
            AND pg_get_constraintdef(constraint_row.oid)
                ILIKE '%(dev_id, endpoint, consumer, hour)%'
        ) THEN
          ALTER TABLE api_log DROP CONSTRAINT IF EXISTS api_log_pkey;
          ALTER TABLE api_log
            ADD CONSTRAINT api_log_pkey PRIMARY KEY (dev_id, endpoint, consumer, hour);
        END IF;
      END $$`);
    await query(
      `UPDATE api_keys
       SET daily_limit = CASE WHEN dev_id = '2116' THEN $1::int ELSE $2::int END
       WHERE daily_limit IS NULL
          OR daily_limit <> CASE WHEN dev_id = '2116' THEN $1::int ELSE $2::int END`,
      [SPECIAL_DAILY_LIMITS['2116'], DEFAULT_DAILY_LIMIT],
    );

    // api_log.dev_id must be text because dev IDs are handled as strings
    // throughout TypeScript and dummy/test IDs may be non-numeric. The DO block
    // avoids rewriting the table when it is already text/varchar.
    await query(`
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1
          FROM information_schema.columns
          WHERE table_name = 'api_log'
            AND column_name = 'dev_id'
            AND data_type <> 'character varying'
            AND data_type <> 'text'
        ) THEN
          ALTER TABLE api_log ALTER COLUMN dev_id TYPE VARCHAR USING dev_id::text;
        END IF;
      END $$`);

    const hourlyUsageColumns = await query<{ column_name: string }>(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'api_key_hourly_usage'`,
    );
    const columnNames = new Set(hourlyUsageColumns.map((row) => row.column_name));
    const hasNormalizedHourlyUsage = columnNames.has('hour_bucket');
    const hasWideHourlyUsage = columnNames.has('hour_00');
    let convertedWideHourlyUsage = false;

    if (hasWideHourlyUsage && !hasNormalizedHourlyUsage) {
      // The original relay aggregate used hour_00..hour_23 columns and a
      // reset job. That design erased real calls whenever the hourly sync ran
      // after traffic had already occurred in the same UTC hour, which made
      // api_key_hourly_usage totals much lower than api_log and Hi-Rez
      // getdataused. Archive the old projection before replacing it; the
      // archive is for post-mortem context only because the wide data may
      // already have been wiped by prior resets.
      await query(`
        CREATE TABLE IF NOT EXISTS api_key_hourly_usage_wide_archive AS
        SELECT *, now() AS archived_at
        FROM api_key_hourly_usage
        WHERE false
      `);
      await query(`
        INSERT INTO api_key_hourly_usage_wide_archive
        SELECT *, now() AS archived_at
        FROM api_key_hourly_usage
      `);
      await query(`DROP TABLE api_key_hourly_usage`);
      convertedWideHourlyUsage = true;
    }

    await query(`
      CREATE TABLE IF NOT EXISTS api_key_hourly_usage (
        dev_id TEXT NOT NULL,
        hour_bucket TIMESTAMPTZ NOT NULL,
        call_count INT NOT NULL DEFAULT 0,
        PRIMARY KEY (dev_id, hour_bucket)
      )
    `);
    await query(`
      CREATE INDEX IF NOT EXISTS idx_api_key_hourly_usage_hour
      ON api_key_hourly_usage (hour_bucket DESC)
    `);

    if (convertedWideHourlyUsage) {
      // api_log is the more reliable post-mortem ledger because it is keyed by
      // real endpoint + hour rows and was not affected by the wide-table reset.
      // Backfill the normalized local projection from api_log so operator
      // dashboards remain useful immediately after automatic conversion.
      await query(`
        INSERT INTO api_key_hourly_usage (dev_id, hour_bucket, call_count)
        SELECT dev_id, date_trunc('hour', hour) AS hour_bucket, SUM(call_count)::int AS call_count
        FROM api_log
        WHERE hour >= date_trunc('hour', now()) - interval '23 hours'
        GROUP BY dev_id, date_trunc('hour', hour)
        ON CONFLICT (dev_id, hour_bucket) DO UPDATE
        SET call_count = GREATEST(api_key_hourly_usage.call_count, EXCLUDED.call_count)
      `);
    }
  }

  private async bootstrapKeysFromFileIfNeeded(): Promise<void> {
    const keyFile = process.env.HIREZ_API_KEYS_FILE || process.env.API_KEYS_FILE || '/run/secrets/paladinscat_api_keys';
    if (!existsSync(keyFile)) return;

    type PlainKey = {
      devId?: string | number;
      dev_id?: string | number;
      authKey?: string;
      auth_key?: string;
    };

    let parsed: unknown;
    try {
      parsed = JSON.parse(readFileSync(keyFile, 'utf8'));
    } catch (err) {
      throw new Error(`Failed to read HIREZ_API_KEYS_FILE: ${err instanceof Error ? err.message : String(err)}`);
    }

    const entries = Array.isArray(parsed)
      ? parsed
      : Array.isArray((parsed as any)?.keys)
        ? (parsed as any).keys
        : [];

    if (entries.length === 0) {
      throw new Error('HIREZ_API_KEYS_FILE did not contain an array of Hi-Rez keys');
    }

    // Real mode must recover cleanly after a local DB reset. The mounted
    // plaintext backup file is read-only, encrypted immediately with the MEK,
    // and inserted only for missing dev IDs. Existing DB rows win, which keeps
    // usage/status history intact across normal restarts.
    let inserted = 0;
    for (const entry of entries as PlainKey[]) {
      const devId = String(entry.devId ?? entry.dev_id ?? '').trim();
      const authKey = String(entry.authKey ?? entry.auth_key ?? '').trim();
      if (!devId || !authKey) continue;

      const result = await query(
        `INSERT INTO api_keys (
           dev_id, auth_key, source, status, calls_today, calls_total,
           total_24h, daily_limit, consecutive_failures
         )
         VALUES ($1, $2, 'file-bootstrap', 'healthy', 0, 0, 0, $3, 0)
         ON CONFLICT (dev_id) DO NOTHING
         RETURNING dev_id`,
        [devId, encrypt(authKey), effectiveDailyLimit(devId, 0)],
      );
      inserted += (result as any[]).length;
    }

    if (inserted > 0) {
      console.log(`[api-key-pool] Bootstrapped ${inserted} encrypted Hi-Rez key row(s) from mounted key file.`);
    }
  }

  async initialize(): Promise<void> {
    await this.ensureSchemaCompatibility();
    // ----------------------------------------------------------------
    // Validate MEK (Master Encryption Key) on startup.
    // MEK is 64 hex chars used for AES-256-GCM decryption of auth_keys.
    // Without valid MEK, keys cannot be decrypted → pool unusable.
    // ----------------------------------------------------------------
    if (!validateMEK()) {
      throw new Error('MEK is not set or invalid. Must be 64 hex characters.');
    }

    // ----------------------------------------------------------------
    // Smoke test: encrypt/decrypt a test string to verify MEK works.
    // Catches wrong MEK early before loading keys.
    // ----------------------------------------------------------------
    if (!smokeTest()) {
      throw new Error('Encryption smoke test failed. MEK may be wrong.');
    }

    await this.bootstrapKeysFromFileIfNeeded();

    // ----------------------------------------------------------------
    // Load all keys from api_keys table. ORDER BY dev_id ASC for
    // deterministic waterfall ordering (lowest dev_id first).
    // Fields needed: dev_id, auth_key (encrypted), status, total_24h,
    // daily_limit. We no longer need calls_total, last_used for hot path.
    // ----------------------------------------------------------------
    // ----------------------------------------------------------------
    // CRITICAL: Fetch ALL columns needed for InMemKey mapping.
    // Previously, this query omitted calls_total and consecutive_failures,
    // causing r.calls_total and r.consecutive_failures to be undefined.
    // The || 0 fallback silently defaulted them to 0 on every restart:
    // 1. A key with 4 failures (near unhealthy threshold) reboots →
    //    consecutiveFailures resets to 0 → key spams Hi-Rez API.
    // 2. callsTotal resets to 0 → flushUsageToDB() adds current batch to 0,
    //    losing all lifetime metrics from the last restart.
    // Source: Feedback 2026-05-30 - "The Silent Memory Wipe (Initialization Flaw)"
    // ----------------------------------------------------------------
    const rows = await query(
      'SELECT dev_id, auth_key, status, total_24h, daily_limit, calls_total, consecutive_failures FROM api_keys ORDER BY dev_id ASC'
    );

    // ----------------------------------------------------------------
    // Use flatMap instead of map to exclude keys that fail decryption.
    // flatMap returns an array per element - we return [key] on success
    // or [] on failure. This filters zombie keys from the pool entirely.
    //
    // Previously, decrypt failure set authKey = '' but kept the key in
    // the pool with status = 'healthy'. The zombie key would be selected
    // by getActiveKey, sent to Hi-Rez with empty auth, and waste API
    // calls before failing. Now, failed keys are excluded at load time.
    //
    // Source: Fault #14 - "decrypt failure produces zombie keys"
    //         Affected: initialize() key loading, getActiveKey selection
    // ----------------------------------------------------------------
    this.keys = rows.flatMap((r: any) => {
      // Decrypt auth_key on-the-fly (DB stores encrypted)
      let authKey: string;
      try {
        authKey = decrypt(r.auth_key);
      } catch (err) {
        // Decrypt failure: exclude this key from the pool entirely.
        // Returning [] from flatMap filters it out. Logging the error
        // for operational visibility - if all keys fail, the pool is
        // empty and getActiveKey will throw with no keys available.
        console.error(`[api-key-pool] EXCLUDED key ${r.dev_id}: decrypt failed - ${err instanceof Error ? err.message : err}`);
        return []; // Exclude this key from the pool
      }

      // ----------------------------------------------------------------
      // Map DB row to InMemKey. usedToday mirrors total_24h from DB.
      // pendingIncrements starts at 0 (nothing to flush yet).
      // dailyLimit uses the effective policy limit if DB value is null/missing.
      // Return [key] to include in the pool (flatMap flattens the array).
      // ----------------------------------------------------------------
      // ----------------------------------------------------------------
      // CRITICAL: Normalize DB status values to match code expectations.
      // The api_keys table stores 'active' as the default status (set by
      // admin reset endpoint, initial inserts, and migration scripts).
      // However, getActiveKey() waterfall selection checks for 'healthy',
      // and recordSuccess/recordFailure only ever write 'healthy' or
      // 'unhealthy'. This mismatch causes all keys to fail selection on
      // startup - even when they have budget available.
      //
      // Mapping here (not in DB UPDATE) because:
      // 1. Admin reset endpoint writes 'active' intentionally (line ~315
      //    of admin.ts: "status = 'active'"). Changing that breaks the
      //    admin workflow where 'active' means "manually verified OK".
      // 2. Migration scripts and initial inserts use 'active'. We can't
      //    control every write path to api_keys.
      // 3. Normalizing on load is idempotent - safe even if DB later
      //    writes 'healthy' consistently (the mapping becomes a no-op).
      //
      // Mapping logic:
      // - 'active' → 'healthy' (default OK state, functionally identical)
      // - 'healthy' → 'healthy' (already correct, passthrough)
      // - 'limited' → 'limited' (budget exhausted, preserve as-is)
      // - 'unhealthy' → 'unhealthy' (API faults, preserve as-is)
      // - anything else → 'healthy' (defensive fallback for unknown values)
      //
      // Source: Fault #1 - "Status mismatch: DB says active, code checks healthy"
      //         Impact: All keys fail waterfall selection on startup.
      //         Affected: getActiveKey() line 602, line 624, line 689.
      //         Related: admin.ts reset-budgets writes 'active' (line ~315).
      // ----------------------------------------------------------------
      const rawStatus = r.status;
      let normalizedStatus: InMemKey['status'];
      if (rawStatus === 'limited' || rawStatus === 'unhealthy') {
        // Penalized states - preserve them. These are explicitly set by
        // getActiveKey() when budget exhausted ('limited') or by
        // recordFailure() when >=5 consecutive failures ('unhealthy').
        normalizedStatus = rawStatus;
      } else {
        // Everything else maps to 'healthy'. This covers 'active' (the
        // common case), 'healthy' (already correct), and any unknown
        // values that might appear due to manual DB edits or migrations.
        // Defaulting to healthy is safe - the key will be penalized if
        // it actually fails API calls (recordFailure tracks this).
        normalizedStatus = 'healthy';
      }

      // ----------------------------------------------------------------
      // CRITICAL: Preserve pendingIncrements from existing in-memory key
      // if one exists. loadKeys() is called by hourly sync (line 49 of
      // api-key-sync.ts) AFTER increments have accumulated but BEFORE the
      // flush timer fires. Resetting pendingIncrements to 0 here causes all
      // pending increments to be lost — they never reach the DB.
      //
      // Example scenario that loses data:
      //   T=0s: 15 API calls → incrementUsage() → pendingIncrements = 15
      //   T=30s: Hourly sync runs → loadKeys() resets pendingIncrements to 0
      //   T=60s: Flush timer fires → sees pendingIncrements = 0 → writes nothing
      //   Result: 15 API calls burned on Hi-Rez but never logged to hourly table.
      //
      // Fix: If an existing in-memory key has pendingIncrements > 0, preserve
      // them. The flush timer will write them to DB before the next loadKeys()
      // cycle. This ensures no increments are lost between flush cycles.
      //
      // Source: api_key_hourly_usage all columns zero despite real usage in
      //         api_log (~13,696 calls for key 2116). Root cause: loadKeys() at
      //         end of hourly sync wiped pending increments before flush ran.
      //         Affected: api-key-sync.ts line 49 (loadKeys), flushUsageToDB().
      // ----------------------------------------------------------------
      const existingKey = this.keys.find(k => k.devId === r.dev_id);
      const preservedPending = existingKey ? existingKey.pendingIncrements : 0;

      return [{
        devId: r.dev_id,
        authKey,
        status: normalizedStatus,
        dailyLimit: effectiveDailyLimit(r.dev_id, r.daily_limit),
        usedToday: r.total_24h || 0,
        pendingIncrements: preservedPending,
        consecutiveFailures: r.consecutive_failures || 0,
        callsTotal: r.calls_total || 0,
      }];
    });

    // ----------------------------------------------------------------
    // Set the active key to the first healthy key with budget > threshold.
    // This establishes the "sticky" starting point for waterfall selection.
    // ----------------------------------------------------------------
    this.activeDevId = null; // Will be set on first getActiveKey() call

    this.initialized = true;

    // ----------------------------------------------------------------
    // Start background flush timer. Every FLUSH_INTERVAL_MS (60s),
    // pending increments are batched and written to the DB.
    // Only start one timer - if already running, skip.
    // ----------------------------------------------------------------
    if (!this.flushTimer) {
      this.flushTimer = setInterval(() => this.flushUsageToDB(), FLUSH_INTERVAL_MS);
      // Make timer unref so it doesn't prevent process exit
      if (typeof this.flushTimer.unref === 'function') {
        (this.flushTimer as any).unref();
      }
    }
  }

  /**
   * Backward compat: same as initialize.
   */
  async init(): Promise<void> {
    await this.initialize();
  }

  /**
   * Reload keys from database.
   *
   * CRITICAL ORDER: Flush pending increments BEFORE reloading.
   * initialize() replaces this.keys with a fresh array from DB.
   * Any pendingIncrements > 0 on the old array are permanently lost
   * if we reload before flushing. This causes usage data loss:
   * 1. Worker calls incrementUsage() → pendingIncrements++ in memory
   * 2. api-key-sync worker calls loadKeys() → initialize() replaces array
   * 3. Old pendingIncrements discarded → those API calls never reach DB
   * 4. Internal tracking drifts below Hi-Rez actual → key appears to have
   *    more budget than reality → hits daily limit unexpectedly.
   *
   * Fix: flushUsageToDB() first writes pending batches to DB, then
   * initialize() reloads fresh state that includes those writes.
   *
   * Also clears the old flushTimer before starting a new one,
   * preventing duplicate intervals from accumulating on repeated reloads.
   * Each loadKeys() call starts a fresh 60s flush cycle.
   *
   * Called by: api-key-sync.ts (after hourly sync), hirez.ts (legacy),
   *           scripts/run-pipeline.ts, tests/api-key-pool.test.ts
   *
   * Source: Fault #1 - "loadKeys silently drops pending increments"
   *         Affected: api-key-sync.ts:35, any caller of loadKeys()
   */
  async loadKeys(): Promise<void> {
    // Flush pending increments to DB before discarding the in-memory array.
    // Without this, pendingIncrements > 0 are lost when initialize()
    // replaces this.keys with a fresh array from the database.
    await this.flushUsageToDB();

    // Clear the existing flush timer to prevent duplicate intervals.
    // Each loadKeys() call will start a fresh timer via initialize().
    // Without clearing, repeated reloads accumulate multiple setInterval
    // calls that all fire independently - 2x, 3x flush frequency.
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    await this.initialize();
  }

  /**
   * Get remaining budget for a key - DB-backed version for monitoring/sync.
   * NOT used in hot path (getActiveKey uses in-memory tracking).
   * Called by: syncUsage(), api-key-sync worker, monitoring endpoints.
   *
   * Primary source: api_keys.total_24h (synced with Hi-Rez getdataused).
   * Fallback: SUM(api_key_hourly_usage.call_count) over the last 24h
   * (local internal tracking).
   *
   * @param devId - The API key to check.
   * @returns Object with used, remaining, limit, percentage.
   */
  async getRemaining(devId: string): Promise<{ used: number; remaining: number; limit: number; percentage: number }> {
    // ----------------------------------------------------------------
    // This method queries the DB - it is NOT on the hot path.
    // Hot path uses in-memory keys[].usedToday (see getActiveKey()).
    // This exists for: syncUsage() drift correction, monitoring endpoints,
    // and the api-key-sync worker that runs hourly.
    // ----------------------------------------------------------------
    const key = this.keys.find(k => k.devId === devId);

    // Primary: total_24h from api_keys (source of truth, synced with getdataused)
    const row = await one(
      `SELECT total_24h, daily_limit FROM api_keys WHERE dev_id = $1`,
      [devId]
    );
    const limit = effectiveDailyLimit(devId, row?.daily_limit ?? key?.dailyLimit);
    const total24h = Number(row?.total_24h || 0);

    // Fallback: if total_24h is 0/null, sum the relay's normalized hourly
    // buckets for the last 24h. This is local evidence only; authoritative
    // totals come from api_keys.total_24h after getdataused sync succeeds.
    const used = total24h > 0 ? total24h : Number(
      (await one(
        `SELECT COALESCE(SUM(call_count), 0) as used
         FROM api_key_hourly_usage
         WHERE dev_id = $1
           AND hour_bucket >= date_trunc('hour', now()) - interval '23 hours'`,
        [devId]
      ))?.used ?? 0
    );

    const remaining = Math.max(0, limit - used);
    const percentage = Math.round((used / limit) * 100);

    return { used, remaining, limit, percentage };
  }

  /**
   * Increment usage for a key - IN-MEMORY ONLY.
   * Does NOT query the DB. The actual DB write happens asynchronously
   * via flushUsageToDB() every 60 seconds.
   *
   * Called immediately after a successful API call (including createsession).
   * This separates "pick a key" from "record usage" - the old getNext()
   * did both atomically, which caused ghost usage when cached sessions
   * were returned without making actual API calls.
   *
   * Source: apihandling.md - "Explicitly record usage AFTER an actual
   * API call is made."
   *
   * @param devId - The API key that just made an API call.
   */
  incrementUsage(devId: string): void {
    const key = this.keys.find(k => k.devId === devId);
    if (key) {
      key.usedToday++;
      key.pendingIncrements++;
      if (!hasUsableBudget(key.usedToday, key.dailyLimit)) {
        key.status = 'limited';
        if (this.activeDevId === devId) {
          this.activeDevId = null;
        }
      }
    } else {
      // ----------------------------------------------------------------
      // Key not found in memory - this indicates a mismatch between
      // the caller's expectation and the loaded key pool. Possible causes:
      // 1. Key was added to DB after initialize() ran (pool not reloaded).
      // 2. Key was excluded from pool due to decrypt failure (Phase 6).
      // 3. Caller has a stale devId from a previous pool state.
      //
      // Without this warning, usage increments silently disappear.
      // The API call still reached Hi-Rez and burned a call, but our
      // internal tracking doesn't know about it. Over time, this causes
      // permanent drift between internal memory and Hi-Rez's actual count.
      // The hourly syncUsage corrects drift, but only once per hour.
      //
      // Source: Fault #10 - "incrementUsage silent no-op for unknown keys"
      //         Affected: hirez.ts apiRequest(), session-manager.ts acquireSession
      // ----------------------------------------------------------------
      console.warn(`[api-key-pool] incrementUsage: key ${devId} not found in pool - usage increment lost`);
    }
  }

  /**
   * Legacy alias for incrementUsage. Kept for backward compatibility
   * with code that still calls apiKeyPool.increment(). Will be removed
   * after all callers are migrated to incrementUsage().
   */
  increment(devId: string): void {
    this.incrementUsage(devId);
  }

  /**
   * Background task: Flush pending increments to the database.
   * Runs every FLUSH_INTERVAL_MS (60s) via flushTimer.
   *
   * For each key with pendingIncrements > 0:
   * 1. Batch the count into a single UPDATE (not one UPDATE per call).
   * 2. Update both api_key_hourly_usage (current UTC hour_bucket) and
   *    api_keys (total_24h, calls_total, last_used).
   * 3. Reset pendingIncrements to 0 EARLY (before await) so new
   *    calls during the flush are captured in the next cycle.
   *
   * CRITICAL: Both UPDATE queries run inside a single PostgreSQL
   * transaction (BEGIN ... COMMIT). Previously, two separate await query()
   * calls ran independently. If the first succeeded (hourly_usage +N) but
   * the second failed (total_24h unchanged), the catch block restored
   * pendingIncrements. On the next flush cycle (60s later), the same batch
   * would flush again - double-counting into api_key_hourly_usage forever.
   * syncUsage corrects total_24h hourly, but endpoint/hour diagnostics remain
   * corrupted. Transaction ensures atomicity: both succeed
   * or both fail, so the restore is always correct.
   *
   * Uses pool.connect() to get a dedicated client for transaction scope.
   * The query() helper uses pool.query() directly (no transaction), so we
   * cannot use it here. Instead we use client.query() within the transaction.
   *
   * Source: apihandling.md - "Background task: Flush pending increments"
   *         Debug 2026-05-31 - "Partial Flush Double-Counting"
   *         Affected: api_key_hourly_usage, api_keys metrics
   */
  // Made public (was private) so hourly sync can call it before loadKeys()
  // to ensure pending increments hit DB before pool reload. Source: Phase 3
  // fix — api_key_hourly_usage was perpetually empty because flush ran after
  // loadKeys() wiped pendingIncrements.
  async flushUsageToDB(): Promise<void> {
    const hourBucket = currentUtcHourBucket();

    for (const key of this.keys) {
      if (key.pendingIncrements > 0) {
        const batch = key.pendingIncrements;
        key.pendingIncrements = 0; // Reset early to catch new calls during await

        try {
          // Get a dedicated client from the pool for transaction scope.
          // We need a single client so BEGIN/COMMIT work on the same connection.
          // The query() helper uses pool.query() directly (no transaction),
          // so we use client.query() within the transaction block instead.
          const client = await pool.connect();

          try {
            // Begin transaction - both updates must succeed or both fail.
            // Without this, a failure between the two UPDATEs causes
            // permanent double-counting in api_key_hourly_usage buckets.
            await client.query('BEGIN');

            // Update normalized hourly usage. The first implementation used
            // fixed hour_00..hour_23 columns and reset the current hour on the
            // sync timer. Because the timer runs on process start and then
            // every 60 minutes from that start time, a restart at 19:11 caused
            // the 19:00 bucket to be cleared at 19:11 every cycle, erasing
            // real calls already made in that hour. A timestamped bucket never
            // needs a reset: each flush lands in the actual UTC hour, and
            // cleanup removes old rows after they leave the rolling window.
            await client.query(
              `INSERT INTO api_key_hourly_usage (dev_id, hour_bucket, call_count)
               VALUES ($1, $2, $3)
               ON CONFLICT (dev_id, hour_bucket) DO UPDATE
               SET call_count = api_key_hourly_usage.call_count + EXCLUDED.call_count`,
              [key.devId, hourBucket.toISOString(), batch]
            );

            // Update api_keys: total_24h (primary metric), calls_total
            // (lifetime), last_used, and the budget status if this batch pushed
            // the key down to the 100-call reserve. The relay already flipped
            // the in-memory key in incrementUsage(); this keeps DB monitoring
            // aligned even before the hourly getdataused override runs.
            await client.query(
              `UPDATE api_keys
               SET total_24h = total_24h + $1,
                   calls_total = calls_total + $1,
                   last_used = NOW(),
                   status = CASE
                     WHEN (daily_limit - (total_24h + $1)) <= $3 THEN 'limited'
                     ELSE status
                   END
               WHERE dev_id = $2`,
              [batch, key.devId, BUDGET_THRESHOLD]
            );

            // Commit the transaction - both updates are now atomic.
            // If either query above threw, we'd jump to catch → ROLLBACK.
            await client.query('COMMIT');

            // Update in-memory callsTotal to match the committed DB state.
            // Only update after successful COMMIT - if we updated before
            // and the transaction rolled back, memory would be ahead of DB.
            key.callsTotal += batch;
          } catch (txErr) {
            // Transaction failed - rollback to undo any partial updates.
            // CRITICAL: Wrap ROLLBACK in its own try/catch. If the DB
            // connection is dead (timeout, network drop), calling
            // client.query('ROLLBACK') on a dead connection will throw.
            // That throw would skip the pendingIncrements restoration
            // below, permanently losing those increments from memory.
            // The nested catch ensures memory restoration always runs.
            try {
              await client.query('ROLLBACK');
            } catch (rollbackErr) {
              // Ignore - connection is already dead, rollback is moot.
              // The client.release() in the outer finally will handle cleanup.
            }

            // Restore pending increments so they aren't lost on failure.
            // The transaction rolled back (or connection died), so DB
            // state is unchanged. Memory state must also be unchanged.
            key.pendingIncrements += batch;
            console.error(`[api-key-pool] Failed to flush usage for ${key.devId}: ${txErr instanceof Error ? txErr.message : txErr}`);
          } finally {
            // Always release the client back to the pool.
            // Without release, the connection is leaked and the pool
            // eventually exhausts (max: 20 in db.ts config).
            client.release();
          }
        } catch (err) {
          // pool.connect() itself failed - no client acquired, no ROLLBACK needed.
          // Restore pending increments so they aren't lost.
          key.pendingIncrements += batch;
          console.error(`[api-key-pool] Failed to acquire DB connection for flush: ${err instanceof Error ? err.message : err}`);
        }
      }
    }
  }

  /**
   * Maintain the normalized rolling-window projection.
   *
   * Historical note: this method used to reset the current hour_XX column in
   * a wide table. That looked reasonable on paper, but it was tied to the sync
   * worker's runtime rather than a true hour-boundary ledger. If the relay
   * booted or synced at :11, calls made from :00-:11 were erased from
   * api_key_hourly_usage even though api_log and Hi-Rez counted them. The
   * normalized table stores `(dev_id, hour_bucket, call_count)`, so maintenance
   * is only cleanup of expired rows. The old method name is kept because the
   * sync worker calls it, but it no longer clears an active bucket.
   */
  async resetCurrentHour(): Promise<void> {
    await this.cleanupHourlyUsage();
  }

  async cleanupHourlyUsage(): Promise<void> {
    await query(`
      DELETE FROM api_key_hourly_usage
      WHERE hour_bucket < date_trunc('hour', now()) - interval '23 hours'
    `);
  }

  /**
   * Log a single API call to api_log for per-endpoint breakdown.
   * Does NOT increment total_24h or calls_total - those are handled by increment().
   * Writes response_ms for latency tracking.
   */
  async logEndpoint(
    devId: string,
    endpoint: string,
    responseMs: number,
    consumer = 'unattributed',
  ): Promise<void> {
    const hour = new Date();
    hour.setMinutes(0, 0, 0); // truncate to current hour

    try {
      await query(
        `INSERT INTO api_log (dev_id, endpoint, consumer, hour, call_count, total_response_ms)
         VALUES ($1, $2, $3, $4, 1, $5)
         ON CONFLICT (dev_id, endpoint, consumer, hour) DO UPDATE
         SET call_count = api_log.call_count + 1,
             total_response_ms = api_log.total_response_ms + $5`,
        [devId, endpoint, consumer, hour.toISOString(), responseMs]
      );
    } catch (err) {
      // Non-critical - api_log is for diagnostics only
      console.error(`[api-key-pool] logEndpoint failed: ${err instanceof Error ? err.message : err}`);
    }
  }

  /**
   * Clean up old log entries outside the 24h rolling window.
   * Keeps only the last 24 hours of data.
   */
  async cleanupOldLogs(): Promise<void> {
    try {
      await query(`DELETE FROM api_log WHERE hour < date_trunc('hour', now()) - interval '23 hours'`);
      await this.cleanupHourlyUsage();
    } catch (err) {
      console.error(`[api-key-pool] cleanupOldLogs failed: ${err instanceof Error ? err.message : err}`);
    }
  }

  /**
   * WATERFALL SELECTION: Returns the active key if it has more than 100 calls left.
   * Otherwise, falls to the next healthy key in the array.
   *
   * DOES NOT INCREMENT USAGE. Usage is incremented separately via
   * incrementUsage() after the actual API call succeeds.
   * DOES NOT QUERY THE DB. All budget checks use in-memory tracking.
   *
   * This replaces the old getNext() which:
   * - Did N DB queries per call (one getRemaining() per key)
   * - Incremented usage before knowing if an API call would actually happen
   * - Sorted keys by remaining DESC, causing oscillation between calls
   *
   * Source: apihandling.md - "The Active Key" + "The Waterfall"
   *
   * @returns APIKey interface (devId, authKey, status, daily_limit)
   * @throws Error if all keys are exhausted or at/below the reserve threshold.
   */
  async getActiveKey(): Promise<APIKey> {
    // Ensure pool is initialized (lazy init on first call)
    if (!this.initialized) await this.initialize();

    // ----------------------------------------------------------------
    // Step 1: Check current active key. If it's healthy and has budget
    // > BUDGET_THRESHOLD (100), return it. This is the "sticky" behavior.
    // ----------------------------------------------------------------
    if (this.activeDevId) {
      const active = this.keys.find(k => k.devId === this.activeDevId);
      if (active) {
        if (active.status === 'healthy' && hasUsableBudget(active.usedToday, active.dailyLimit)) {
          return this.formatKey(active);
        }
        // Active key exhausted - mark it limited in memory and DB
        if (active.status !== 'limited') {
          active.status = 'limited';
          try {
            await query('UPDATE api_keys SET status = $1 WHERE dev_id = $2', ['limited', active.devId]);
          } catch (err) {
            console.error(`[api-key-pool] Failed to mark ${active.devId} as limited: ${err}`);
          }
        }
      }
    }

    // ----------------------------------------------------------------
    // Step 2: Waterfall - find the next healthy key with budget > threshold.
    // Iterates in array order (dev_id ASC from initialize()).
    // This is deterministic: same key will be picked every time until
    // it hits the threshold.
    // ----------------------------------------------------------------
    let nextKey = this.keys.find(
      k => k.status === 'healthy' && hasUsableBudget(k.usedToday, k.dailyLimit)
    );

    // ----------------------------------------------------------------
    // Graceful degradation: Before throwing, try to revive exhausted keys.
    // All keys may appear exhausted in memory because:
    // 1. Rolling 24h window shifted - old calls aged out but our in-memory
    //    hasn't been updated yet (syncUsage runs hourly, not real-time).
    // 2. In-memory drift: pendingIncrements inflated usedToday above actual.
    // 3. Keys marked 'limited' after budget exhaustion, never recovered.
    //
    // CRITICAL: The revival loop uses await (yielding to event loop).
    // Under concurrent load (e.g., 50 requests hit an exhausted pool),
    // all 50 would bypass the if (!nextKey) check and each spawn its own
    // syncUsage loop - 50 × 5 keys = 250 concurrent getdataused calls.
    // Hi-Rez would immediately rate-limit or IP-ban the server.
    //
    // Fix: Use revivalPromise as a single-flight lock. The first caller
    // creates the promise (async IIFE that syncs all limited keys). All
    // subsequent callers await the same promise. After completion (success
    // or failure), the promise is cleared so the next exhaustion event
    // gets a fresh revival cycle. This ensures exactly one round of
    // syncUsage calls regardless of concurrent request count.
    //
    // Source: Fault #9 - "getActiveKey throws with no recovery path"
    //         Debug 2026-05-31 - "Thundering Herd on Revival Loop"
    //         Affected: All callers via hirez.ts apiRequest() → sessionManager
    //           → getActiveSession → getActiveKey. Workers crash on throw.
    //         Related: syncUsage revival logic (Phase 2), BUDGET_THRESHOLD
    // ----------------------------------------------------------------
    if (!nextKey) {
      // ----------------------------------------------------------------
      // Cooldown check: Prevent revival loop from firing too frequently.
      // Without this, every failed API call triggers syncUsage on ALL keys —
      // burning getdataused calls rapidly even when keys have no budget yet.
      // With rolling 24h window, keys gradually free up as old calls age out.
      // Checking once per 30 minutes is sufficient (hourly sync also checks).
      //
      // If cooldown hasn't elapsed: skip revival and throw immediately.
      // This prevents budget burn from redundant getdataused calls on
      // already-exhausted keys that fail with "Daily request limit reached".
      //
      // Source: Budget burn issue 2026-06-01 — backup keys burned through
      //         entire 7500-call budget due to unlimited revival spam.
      //         Affected: getActiveKey() revival loop, all callers via hirez.ts.
      // ----------------------------------------------------------------
      const now = Date.now();
      if (now - this.lastRevivalAttempt < ApiKeyPool.REVIVAL_COOLDOWN_MS) {
        console.warn(`[api-key-pool] All keys exhausted — skipping revival (cooldown: ${Math.round((this.lastRevivalAttempt + ApiKeyPool.REVIVAL_COOLDOWN_MS - now) / 1000)}s remaining).`);
        throw new Error('CRITICAL: All API keys exhausted or at/below reserve threshold. Revival on cooldown.');
      }

      // Single-flight lock: if a revival is already in progress,
      // await the existing promise instead of spawning a new loop.
      // This prevents N concurrent callers from each firing their own
      // syncUsage loops - ensuring exactly one round of Hi-Rez calls.
      this.lastRevivalAttempt = now; // Mark revival attempt time before starting.
      if (!this.revivalPromise) {
        this.revivalPromise = (async () => {
          for (const key of this.keys) {
            // ----------------------------------------------------------------
            // CRITICAL: Sync ALL keys when the pool is exhausted, not just
            // 'limited' or 'unhealthy'. After Phase 1 normalization, in-memory
            // status should be 'healthy', 'limited', or 'unhealthy'. But if a
            // race condition occurs (e.g., loadKeys() runs mid-selection), a key
            // might temporarily have stale state. Syncing everything ensures we
            // catch any drift between Hi-Rez reality and our tracking.
            //
            // Why sync healthy keys too? Because 'healthy' in memory doesn't
            // mean the key actually has budget - usedToday might be stale if
            // flushUsageToDB() hasn't run yet, or if calls have aged out of
            // Hi-Rez's side but we haven't noticed. syncUsage calls getdataused
            // to verify actual usage and corrects both total_24h and in-memory
            // usedToday. The cost is 1 extra getdataused call per key during
            // revival - acceptable since this only fires when ALL keys are
            // exhausted (rare event, not hot path).
            //
            // Before: Only synced 'limited' or 'unhealthy' keys. Missed the case
            // where all keys were 'active' in DB → mapped to 'healthy' in memory
            // → but actually had no budget because usedToday was stale. Revival
            // did nothing, discovery kept failing with "All API keys exhausted".
            //
            // Source: Fault #2 - "Revival loop skips active keys"
            //         Impact: Hours 16-19 missed on 2026-06-01. Discovery failed
            //         repeatedly because revival never corrected stale usage data.
            //         Affected: getActiveKey() lines 655-689.
            //         Related: Phase 1A status normalization (initialize()).
            // ----------------------------------------------------------------
            await this.syncUsage(key.devId);
          }
        })();
      }
      // CRITICAL: Always clear the lock in a finally block. If the await
      // throws (e.g., unhandled exception inside the IIFE), execution
      // jumps out and this.revivalPromise = null is never reached.
      // The pool is then permanently locked - no future revival can
      // create a new promise because this.revivalPromise is still set.
      // finally guarantees clearance regardless of success or failure.
      try {
        await this.revivalPromise;
      } finally {
        this.revivalPromise = null;
      }

      // Re-check waterfall after sync attempts - a key may have been revived
      nextKey = this.keys.find(
        k => k.status === 'healthy' && hasUsableBudget(k.usedToday, k.dailyLimit)
      );

      // Still exhausted after sync attempts - throw with informative message
      if (!nextKey) {
        throw new Error('CRITICAL: All API keys exhausted or at/below reserve threshold. No key available after sync attempt.');
      }
    }

    // Set as new active key (sticky until exhausted)
    this.activeDevId = nextKey.devId;
    return this.formatKey(nextKey);
  }

  /**
   * Legacy alias for getActiveKey. Kept for backward compatibility
   * with code that still calls apiKeyPool.getNext(). Will be removed
   * after all callers are migrated to getActiveKey().
   */
  async getNext(): Promise<APIKey> {
    return this.getActiveKey();
  }

  /**
   * Format an InMemKey to the public APIKey interface.
   * Used by getActiveKey() to return the key to callers.
   */
  private formatKey(k: InMemKey): APIKey {
    const remaining = Math.max(0, k.dailyLimit - k.usedToday);
    return {
      devId: k.devId,
      authKey: k.authKey,
      status: k.status,
      daily_limit: k.dailyLimit,
      used_24h: k.usedToday,
      remaining,
      reserve_threshold: BUDGET_THRESHOLD,
      calls_total: k.callsTotal,
      consecutive_failures: k.consecutiveFailures,
      last_used: '', // Not tracked in-memory; DB has last_updated
    };
  }

  /**
   * Record a successful API call for a key. Resets consecutive failures
   * and ensures the key is marked healthy. Updates both in-memory and DB.
   *
   * Called after a successful Hi-Rez API response (not by getActiveKey
   * since usage is now tracked separately via incrementUsage()).
   *
   * @param devId - The key that just succeeded.
   */
  async recordSuccess(devId: string): Promise<void> {
    // Update in-memory state
    const key = this.keys.find(k => k.devId === devId);
    let shouldUpdateStatus = false;

    // ----------------------------------------------------------------
    // Capture whether the key had failures BEFORE resetting to 0.
    // This determines whether we need a DB write below. Without this,
    // every successful API call fires UPDATE consecutive_failures = 0
    // even when already 0 - ~99% of writes are redundant no-ops that
    // waste DB round-trips on the hot path. The else branch at line 556
    // fires unconditionally for non-unhealthy keys, hammering the DB.
    //
    // Fix: Track hadFailures before the reset. Only write to DB when
    // there's actual state to change. Combined with AND consecutive_failures > 0
    // in the SQL, this eliminates ~99% of DB writes while preserving
    // correctness under memory/DB divergence (DB guard is safety net).
    //
    // Source: Fault #8 - "recordSuccess always hits DB unnecessarily"
    //         Affected: Every successful API call via hirez.ts apiRequest()
    //         Related: recordFailure already uses AND consecutive_failures > 0
    // ----------------------------------------------------------------
    let hadFailures = false;

    if (key) {
      hadFailures = key.consecutiveFailures > 0;
      key.consecutiveFailures = 0;
      // ----------------------------------------------------------------
      // ONLY recover to healthy if the key was penalized for network/API
      // faults (unhealthy). DO NOT overwrite 'limited' - that is strictly
      // budget-controlled by getActiveKey(). Overwriting 'limited' with
      // 'healthy' would cause an infinite UPDATE loop: getActiveKey marks
      // it limited → recordSuccess marks it healthy → getActiveKey marks
      // it limited again, hammering the DB.
      // Source: Feedback - "recordSuccess overrides the 'limited' state"
      // ----------------------------------------------------------------
      if (key.status === 'unhealthy') {
        key.status = 'healthy';
        shouldUpdateStatus = true;
      }
    }

    // Update DB - only write when there's actual state to change.
    // Path 1: Recovering from unhealthy - update both status and failures.
    // Path 2: Had failures in memory - reset failures only.
    //         SQL includes AND consecutive_failures > 0 as safety net
    //         in case DB is ahead of memory (e.g., concurrent failure).
    // Path 3: No failures, not unhealthy - skip DB entirely.
    if (shouldUpdateStatus) {
      await query(
        `UPDATE api_keys SET consecutive_failures = 0, status = 'healthy' WHERE dev_id = $1`,
        [devId]
      );
    } else if (hadFailures) {
      await query(
        `UPDATE api_keys SET consecutive_failures = 0 WHERE dev_id = $1 AND consecutive_failures > 0`,
        [devId]
      );
    }
    // else: no failures to reset, no status to change - skip DB write entirely
  }

  /**
   * Record a key failure. Only penalizes the key if the failure is the
   * KEY's fault (session expired, daily limit, 5xx). Does NOT penalize
   * for code errors (404, bad params, network error).
   *
   * Key fault: consecutive_failures++ → if >= 5, auto-mark unhealthy.
   * Code fault: reset consecutive_failures to 0 (key was fine).
   *
   * When a key becomes unhealthy (>= 5 failures), it is skipped by
   * getActiveKey() waterfall selection. Recovery requires manual
   * intervention or the hourly sync worker.
   *
   * @param devId - The key that failed.
   * @param isKeyFault - true = key's fault (session expired, daily limit).
   *                     false = code fault (404, bad params, network).
   */
  async recordFailure(devId: string, isKeyFault: boolean = true): Promise<void> {
    const key = this.keys.find(k => k.devId === devId);

    if (isKeyFault) {
      // Key fault: increment failures in memory and DB
      if (key) {
        key.consecutiveFailures++;
        if (key.consecutiveFailures >= 5) {
          key.status = 'unhealthy';
          // If this was the active key, force waterfall to next key
          if (this.activeDevId === devId) {
            this.activeDevId = null;
          }
        }
      }

      // ----------------------------------------------------------------
      // Do NOT touch calls_total here. incrementUsage() already queued +1
      // for the background flusher. Touching it here would double-count:
      // flushUsageToDB adds +1 AND this UPDATE adds +1 = 2x drift.
      // Let flushUsageToDB be the sole owner of call metrics.
      // Source: Feedback - "Double-Counting calls_total on Failures"
      // ----------------------------------------------------------------
      await query(
        `UPDATE api_keys SET consecutive_failures = consecutive_failures + 1, status = CASE WHEN consecutive_failures >= 5 THEN 'unhealthy' ELSE status END WHERE dev_id = $1`,
        [devId]
      );
    } else {
      // Code fault: the key was fine, our code was wrong (404, bad params, network).
      // Do NOT reset consecutive_failures to 0 here.
      //
      // CRITICAL: Previously, this block unconditionally reset failures to 0.
      // If the key was 'unhealthy' from 5 prior key faults, a code fault would
      // reset failures to 0 but leave status = 'unhealthy'. The key enters a
      // zombie state: status says unhealthy (skipped by waterfall), but
      // consecutive_failures = 0 (no failures to recover from).
      // recordSuccess sees status != 'unhealthy' → skips recovery.
      // syncUsage revives unhealthy keys by checking actual rolling usage,
      // but that runs hourly at best - the key sits dead until then.
      //
      // Fix: Leave failures untouched on code faults. The key's failure count
      // reflects real key faults (session expired, daily limit). A code fault
      // is orthogonal - it doesn't make the key healthier or worse.
      // When a real key fault succeeds later, recordSuccess will reset
      // failures and recover unhealthy → healthy naturally.
      // If the key is healthy (not unhealthy), code faults are a no-op - correct.
      // If the key is unhealthy, code faults are a no-op - also correct.
      // The unhealthy key will recover via recordSuccess or syncUsage revival.
      //
      // This also eliminates a redundant DB write on every code fault.
      //
      // Source: Debug 2026-05-31 - "Unhealthy Code-Fault Trap"
      //         Affected: recordFailure(isKeyFault=false), recordSuccess recovery
      //         Related: Phase 2 syncUsage revival, Phase 4 recordSuccess optimization
    }
  }

  /**
   * Sync internal tracking with Hi-Rez's actual usage via getdataused.
   * total_24h is overwritten with actual API count (source of truth).
   * Hourly buckets are adjusted to match.
   */
  async syncUsage(devId: string): Promise<void> {
    try {
      const { getDataUsed } = await import('../hirez-relay/core.js');
      const actual = await getDataUsed(devId);
      // Hi-Rez getdatausedJson response structure:
      //   Total_Requests_Today: number of requests today (what we track as total_24h)
      //   Request_Limit_Daily: daily limit (what we track as daily_limit)
      //   Active_Sessions, Concurrent_Sessions, Session_Cap, Session_Time_Limit, Total_Sessions_Today
      // Source: Hi-Rez API getdatausedJson endpoint, verified 2026-05-30
      const hasAuthoritativeUsage = actual && Object.prototype.hasOwnProperty.call(actual, 'Total_Requests_Today');
      const actualUsed = hasAuthoritativeUsage ? Math.max(0, Number(actual.Total_Requests_Today) || 0) : 0;
      const actualLimit = effectiveDailyLimit(devId, Number(actual?.Request_Limit_Daily || 0));

      if (hasAuthoritativeUsage) {
        // ----------------------------------------------------------------
        // CRITICAL ORDER: Capture the internal usage snapshot BEFORE
        // overwriting the database. getRemaining(devId) queries
        // SELECT total_24h FROM api_keys. If we UPDATE first, then
        // SELECT, it fetches the brand-new actualUsed value we just saved.
        // Result: internalUsed == actualUsed → drift is always 0.
        // The drift correction code inside if (Math.abs(drift) > 0)
        // never executes, and api_key_hourly_usage permanently drifts.
        // Source: Feedback 2026-05-30 - "The Zero-Drift Illusion (Order of Operations Flaw)"
        // ----------------------------------------------------------------
        // Step 1: Fetch internal usage FIRST - before overwriting the DB
        const { used: internalUsed } = await this.getRemaining(devId);
        const drift = actualUsed - internalUsed;

        const remainingBudget = actualLimit - actualUsed;
        const nextStatus: InMemKey['status'] = hasUsableBudget(actualUsed, actualLimit) ? 'healthy' : 'limited';

        // Step 2: NOW update the database with the authoritative usage and
        // effective limit. This intentionally overwrites local total_24h even
        // when Hi-Rez reports 0. A true zero means the rolling server window
        // cleared; an empty response without Total_Requests_Today falls into
        // the estimate fallback below instead.
        await query(
          `UPDATE api_keys
           SET total_24h = $1,
               daily_limit = $2,
               status = $3::varchar,
               consecutive_failures = CASE WHEN $3::varchar = 'healthy' THEN 0 ELSE consecutive_failures END,
               last_sync_at = now(),
               last_sync_error = NULL
           WHERE dev_id = $4`,
          [actualUsed, actualLimit, nextStatus, devId],
        );

        // Drift logging only - do NOT adjust hourly buckets with drift.
        // Drift is the difference between total_24h (rolling 24h sum) and
        // Hi-Rez's actual count. It is NOT the current hour's call count.
        // Hourly buckets are managed exclusively by:
        //   - flushUsageToDB(): increments the current UTC hour bucket
        //   - cleanupHourlyUsage(): removes buckets after they leave the window
        // Adjusting hourly buckets with drift corrupts per-hour data when
        // drift is large (e.g., key 2116 had drift=-1293, which zeroed
        // the current hour bucket with a meaningless number).
        if (Math.abs(drift) > 0) {
          console.log(`[api-key-pool] Sync ${devId}: internal=${internalUsed}, actual=${actualUsed}, drift=${drift} (total_24h corrected, hourly buckets untouched)`);
        }

        // ----------------------------------------------------------------
        // Revival check: If the key is 'limited' (budget exhausted) but
        // the actual usage shows calls have aged out of the rolling window,
        // rolled over on Hi-Rez's servers), revive it back to 'healthy'.
        // Without this, keys get stuck in 'limited' forever after daily
        // reset, causing 'CRITICAL: All API keys exhausted' crashes.
        // Also sync in-memory usedToday to match the actual DB value.
        // Source: Feedback - "Keys Get Stuck in 'limited' (No Daily Revival)"
        // ----------------------------------------------------------------
        const key = this.keys.find(k => k.devId === devId);
        if (key) {
          key.usedToday = actualUsed; // Sync in-memory to match DB
          key.dailyLimit = actualLimit;

          // ----------------------------------------------------------------
          // Revival check: As calls age out of the rolling 24h window,
          // actualUsed drops but our in-memory key may still be 'limited' or
          // 'unhealthy'. Without this revival, keys stay dead permanently:
          // - 'limited' keys: budget exhausted → never picked by waterfall →
          //   stuck forever even as old calls naturally expire from the window.
          // - 'unhealthy' keys: ≥5 consecutive failures → skipped by
          //   getActiveKey entirely → no code path recovers them.
          //   recordSuccess only recovers 'unhealthy' → 'healthy' on success,
          //   but getActiveKey never picks 'unhealthy' keys → chicken/egg.
          //
          // Fix: Check remainingBudget against BUDGET_THRESHOLD. If sufficient
          // budget exists AND the key is penalized ('limited' or 'unhealthy'),
          // revive to 'healthy'. For 'unhealthy' keys, also reset
          // consecutiveFailures to 0 since rolling window usage has been corrected.
          // The DB UPDATE includes consecutive_failures = 0 for both cases
          // to keep memory and DB in sync.
          //
          // Source: Fault #3 - "unhealthy keys never revive when calls age out"
          //         Affected: getActiveKey() skips unhealthy keys permanently
          //         Related: recordSuccess() only recovers unhealthy→healthy
          //           on actual API success, but waterfall never picks them
          // ----------------------------------------------------------------
          const previousStatus = key.status;
          key.status = nextStatus;
          if (nextStatus === 'healthy') {
            // Reset consecutive failures for unhealthy keys — rolling window usage corrected.
            // Setting it for limited keys too is harmless (was already 0 or low).
            key.consecutiveFailures = 0;
            if (previousStatus === 'limited' || previousStatus === 'unhealthy') {
              console.log(`[api-key-pool] Revived key ${devId} — server usage=${actualUsed}/${actualLimit}, remaining=${remainingBudget}.`);
            }
          } else {
            if (this.activeDevId === devId) this.activeDevId = null;
            if (previousStatus !== 'limited') {
              console.log(`[api-key-pool] Limited key ${devId} — server usage=${actualUsed}/${actualLimit}, remaining=${remainingBudget} <= reserve=${BUDGET_THRESHOLD}.`);
            }
          }
        }
      } else {
        // ----------------------------------------------------------------
        // getdataused returned empty — likely "Daily request limit reached"
        // for a limited/unhealthy key. This creates a deadlock:
        //   Limited key → can't call Hi-Rez (no budget)
        //   Can't call Hi-Rez → can't check actual usage
        //   Can't check usage → revival never fires → stuck forever.
        //
        // Fix: Estimate rolling 24h usage from api_key_hourly_usage table,
        // which now has real data (Phase 3 fix). Sum all hourly columns to
        // get our internal estimate. If estimated remaining is above BUDGET_THRESHOLD,
        // revive the key. The next successful syncUsage will correct total_24h
        // with actual Hi-Rez values.
        //
        // Source: Feedback - "Limited keys stuck, untested" — revival check
        //         inside if (actualUsed > 0) never fires for limited keys.
        //         Affected: syncUsage() line ~1027, all limited/unhealthy keys.
        // ----------------------------------------------------------------
        const key = this.keys.find(k => k.devId === devId);
        if (key && (key.status === 'limited' || key.status === 'unhealthy')) {
          // ----------------------------------------------------------------
          // Safety net: skip estimate revival if this key was recently revived.
          // Prevents rapid toggle when hourly table data is stale (e.g., old
          // hours wiped before Phase 3 fix caused false-positive revivals).
          // The normal syncUsage path (getdataused succeeds) is unaffected —
          // only the estimate fallback is rate-limited here.
          // Source: Safety net for estimate-based revival (Phase 4).
          // ----------------------------------------------------------------
          const lastEstRevive = this.lastEstimateRevivalByDevId.get(devId) || 0;
          if (Date.now() - lastEstRevive < ApiKeyPool.ESTIMATE_REVIVAL_COOLDOWN_MS) {
            console.log(
              `[api-key-pool] Skipping estimate revival for ${devId} — ` +
                `within cooldown (${Math.round((lastEstRevive - Date.now()) / 1000)}s ago).`,
            );
          } else {
            try {
              // Sum normalized hourly buckets to estimate rolling 24h usage.
              // This is only used when getdataused returns no authoritative
              // Total_Requests_Today, usually because the key is currently
              // limited. The estimate is local and conservative; successful
              // getdataused sync will overwrite api_keys.total_24h later.
              const rows = await query(
                `SELECT COALESCE(SUM(call_count), 0) AS total
                 FROM api_key_hourly_usage
                 WHERE dev_id = $1
                   AND hour_bucket >= date_trunc('hour', now()) - interval '23 hours'`,
                [devId]
              );
              const estimatedUsed = Number((rows as any[])[0]?.total || 0);
              const dailyLimit = effectiveDailyLimit(devId, key.dailyLimit);
              const estimatedRemaining = dailyLimit - estimatedUsed;

              if (estimatedRemaining > BUDGET_THRESHOLD) {
                key.status = 'healthy';
                key.consecutiveFailures = 0;
                this.lastEstimateRevivalByDevId.set(devId, Date.now());
                await query(
                  `UPDATE api_keys
                   SET status = 'healthy',
                       consecutive_failures = 0,
                       total_24h = $1,
                       daily_limit = $2,
                       last_sync_at = now(),
                       last_sync_error = 'getdataused returned no Total_Requests_Today; revived from hourly usage estimate'
                   WHERE dev_id = $3`,
                  [estimatedUsed, dailyLimit, devId],
                );
                key.usedToday = estimatedUsed;
                key.dailyLimit = dailyLimit;
                console.log(
                  `[api-key-pool] Revived key ${devId} via hourly estimate — ` +
                    `estimated usage=${estimatedUsed}/${dailyLimit}, remaining=${estimatedRemaining}.`,
                );
              } else {
                console.log(
                  `[api-key-pool] Key ${devId} still limited — estimated usage=${estimatedUsed}/${dailyLimit}, ` +
                    `remaining=${estimatedRemaining} <= reserve=${BUDGET_THRESHOLD}.`,
                );
              }
            } catch (estErr) {
              // Estimate failed — leave key as-is. Next sync cycle will retry.
              console.warn(
                `[api-key-pool] Estimated revival failed for ${devId}: ` +
                  `${estErr instanceof Error ? estErr.message : estErr}`,
              );
            }
          }
        }
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      await query(
        `UPDATE api_keys SET last_sync_at = now(), last_sync_error = $1 WHERE dev_id = $2`,
        [message.slice(0, 2000), devId],
      ).catch(() => undefined);
      console.log(`[api-key-pool] Sync failed for ${devId}: ${message}`);
    }
  }

  /**
   * Return all keys for monitoring endpoints. Maps InMemKey → APIKey.
   * Used by: system route (/api/keys), monitoring dashboards.
   */
  getKeys(): APIKey[] {
    return this.keys.map(k => this.formatKey(k));
  }

  /**
   * Alias for getKeys(). Kept for backward compatibility.
   */
  getStatus(): APIKey[] {
    return this.getKeys();
  }

  /**
   * Get a specific key for monitoring (e.g., getdataused) without
   * incrementing usage. Used by syncUsage() to call getdataused with
   * the target key's own session to check its own usage.
   *
   * Does NOT call incrementUsage() - monitoring only.
   * Does NOT change the active key.
   *
   * @param devId - The key to retrieve for monitoring.
   * @throws Error if key not found.
   */
  async getKeyForMonitoring(devId: string): Promise<APIKey> {
    if (!this.initialized) await this.initialize();

    const key = this.keys.find(k => k.devId === devId);
    if (!key) {
      throw new Error('No key available for monitoring: ' + devId);
    }

    // Monitoring is the recovery path for limited/unhealthy keys. Normal API
    // selection still excludes unhealthy keys, but getdataused must be able to
    // run against the target key so syncUsage() can see whether the rolling
    // window has recovered and flip the key back to healthy. Rejecting unhealthy
    // here creates a deadlock: only monitoring can revive the key, but monitoring
    // was refusing to look at it.
    return this.formatKey(key);
  }

  /**
   * Destroy the pool: clear the flush timer and reset state.
   *
   * Used primarily for test cleanup. The singleton apiKeyPool persists
   * across tests, and each initialize() call starts a new flushTimer
   * (setInterval). Without cleanup, tests accumulate intervals that
   * fire independently - causing 2x, 3x flush frequency and flaky tests.
   *
   * Also clears activeDevId and resets the initialized flag so that
   * subsequent calls to getActiveKey will re-initialize cleanly.
   *
   * Does NOT close the DB pool - that's managed by the application
   * lifecycle (index.ts shutdown). This method only cleans up the
   * ApiKeyPool's own resources (the flush timer interval).
   *
   * Called by: tests/api-key-pool.test.ts (afterEach), application shutdown.
   *
   * Source: Fault #12 - "singleton prevents test isolation"
   *         Affected: All tests that use apiKeyPool, test cleanup lifecycle
   */
  destroy(): void {
    // Clear the flush timer to stop background DB writes.
    // Without this, the interval keeps firing after the pool is destroyed,
    // causing DB writes from a stale key array and potential errors.
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    // Reset state so the pool can be re-initialized cleanly.
    // activeDevId = null forces waterfall to re-select on next getActiveKey.
    // initialized = false forces re-load from DB on next lazy initialization.
    this.activeDevId = null;
    this.initialized = false;
    // Clear revival lock so a stale promise doesn't block future revivals.
    // Without this, if destroy() is called mid-revival, the next exhaustion
    // event would await the old promise (which never resolves cleanly).
    this.revivalPromise = null;
  }
}

export const apiKeyPool = new ApiKeyPool();
