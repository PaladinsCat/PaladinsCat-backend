import { apiKeyPool, BUDGET_THRESHOLD } from '../services/api-key-pool';
import { query } from '../config/db';
import { runExclusive } from '../workers/worker-lock';

let syncTimer: NodeJS.Timeout | null = null;

/**
 * Sync API key usage with Hi-Rez every 1 hour.
 * - Verifies internal tracking vs actual usage
 * - Swaps key when remaining calls are at/below the 100-call reserve
 * - Recovers keys when budget frees up
 */
export async function syncApiKeyUsage(): Promise<void> {
  await runExclusive('api-key-sync:usage', syncApiKeyUsageInner);
}

async function syncApiKeyUsageInner(): Promise<void> {
  // ----------------------------------------------------------------
  // CRITICAL ORDERING:
  // 1. Flush pending increments to DB first — ensures all accumulated
  //    calls hit the DB before we reset anything.
  // 2. Prune expired api_key_hourly_usage rows — the table is normalized as
  //    (dev_id, hour_bucket, call_count), so it never clears an active hour.
  //    Old wide hour_00..hour_23 columns caused lost accounting when the sync
  //    worker ran at :11 and erased calls from :00-:11. Cleanup now only
  //    removes buckets after they leave the 24h window.
  //
  // Previously called resetCurrentHour() here to clear a wide current-hour
  // column. The method name remains for compatibility, but it now performs
  // normalized cleanup only. This preserves calls that already occurred in
  // the current UTC hour while still keeping the local 24h projection bounded.
  //
  // Source: Phase 3B fix removed resetCurrentHour due to loadKeys()
  //         wiping pendingIncrements. The loadKeys bug is now fixed;
  //         we need the hourly reset back for rolling window tracking.
  // ----------------------------------------------------------------
  await apiKeyPool.flushUsageToDB();
  await apiKeyPool.resetCurrentHour();

  const keys = apiKeyPool.getKeys();
  // ----------------------------------------------------------------
  // CRITICAL: Sync ALL non-test keys regardless of status. Limited and
  // unhealthy keys need syncUsage to run so their revival checks fire.
  // Previously filtered out 'unhealthy' keys, but that prevented the
  // revival logic inside syncUsage (line ~1128) from ever executing for
  // those keys — creating a deadlock where limited/unhealthy keys could
  // never recover because syncUsage was the only path that checks remaining
  // budget and revives them.
  // ----------------------------------------------------------------
  const allKeys = keys.filter(k => k.devId !== 'test');

  for (const key of allKeys) {
    await apiKeyPool.syncUsage(key.devId);

    // Check if key should be swapped or recovered
    // ----------------------------------------------------------------
    // Proactive reserve mark: syncUsage revives keys when budget allows,
    // and this check keeps DB status aligned with the hard 100-call reserve.
    //
    // The recovery (limited→healthy, unhealthy→healthy) is handled by
    // syncUsage above — it queries Hi-Rez for actual usage and revives
    // when remainingBudget >= BUDGET_THRESHOLD. Removing the redundant
    // recovery block here prevents status toggle races:
    // 1. Worker calls getKeys() → key.status = 'limited' (stale copy)
    // 2. syncUsage revives in memory → this.keys[N].status = 'healthy'
    // 3. Worker checks stale key.status → still 'limited'
    // 4. Worker marks limited again → overwrites the revival
    //
    // Source: Fault #2 — "syncUsage → loadKeys status overwrite race"
    //         Affected: api-key-sync.ts sync loop, syncUsage revival logic
    // ----------------------------------------------------------------
    const { used, limit, percentage } = await apiKeyPool.getRemaining(key.devId);

    // ----------------------------------------------------------------
    // Proactive mark: only block keys that are genuinely near their hard
    // budget limit. A key at 95% usage with ~612 remaining (e.g., key 2116
    // at 14388/15000) still has usable budget above BUDGET_THRESHOLD (100).
    // Blocking it forces waterfall to use backup keys that may themselves
    // be maxed out on Hi-Rez's side — leaving discovery with no working key.
    //
    // Fix: Mark as 'limited' when remaining <= BUDGET_THRESHOLD. The 100-call
    // reserve is inclusive: at exactly 100 calls left, the key turns off and
    // waterfall moves on. Keys above the reserve remain usable even if they are
    // over 90%, because 2116 has a larger daily limit and may still have safe
    // headroom at high percentages.
    //
    // Source: Feedback 2026-06-01 — "hour 16 recovery blocked: primary key
    //         has ~612 remaining but was marked limited at 95%, backups are
    //         healthy on paper but Hi-Rez rejects them. Discovery stuck."
    //         Affected: api-key-sync.ts proactive mark block.
    // ----------------------------------------------------------------
    const remaining = limit - used;
    if (remaining <= BUDGET_THRESHOLD && key.status !== 'limited') {
      await query('UPDATE api_keys SET status = $1 WHERE dev_id = $2', ['limited', key.devId]);
      console.log(`[api-key-sync] ${key.devId} at ${percentage}% — marked limited (remaining: ${remaining} <= reserve: ${BUDGET_THRESHOLD})`);
    } else if (percentage >= 90 && remaining > BUDGET_THRESHOLD) {
      // Early warning — approaching limit but still usable.
      console.log(`[api-key-sync] ${key.devId} at ${percentage}% — approaching limit but still has budget (remaining: ${remaining} > reserve: ${BUDGET_THRESHOLD})`);
    }
  }

  // Cleanup old logs (24h rolling window)
  await apiKeyPool.cleanupOldLogs();

  // Reload pool to pick up status changes
  await apiKeyPool.loadKeys();
}

/**
 * Enable hourly sync using setInterval (replaces node-cron dependency).
 *
 * Replaced require('node-cron') with built-in setInterval to eliminate
 * the runtime dependency. node-cron was loaded via require() in an
 * ES-module TypeScript project — it would fail silently if not installed,
 * and the error wouldn't surface until the cron fired. setInterval is
 * built into Node.js, requires no npm install, and behaves identically
 * for this use case (hourly execution).
 *
 * Uses unref() on the timer so it doesn't prevent process exit when
 * the application shuts down. Without unref, the setInterval keeps
 * the event loop alive even after all other work is done.
 *
 * Schedule: Every 3600000ms (1 hour). The old cron schedule '0 * * * *'
 * meant "every hour on the hour" — setInterval fires 1 hour after start,
 * then every hour after that. Slight difference: cron fires at :00,
 * setInterval fires 1h after enable. Acceptable tradeoff.
 *
 * Source: Fault #16 — "require(node-cron) runtime dynamic import"
 *         Affected: api-key-sync.ts, any caller of enableApiKeySync()
 */
export function enableApiKeySync(): void {
  if (syncTimer) return;
  // ----------------------------------------------------------------
  // Run immediately on boot, then schedule hourly interval.
  // Old code: setInterval waited full 1h before first fire — API keys
  // operated on stale data for an entire hour after every deploy/restart.
  // New: run once now + then every hour. Ensures fresh data on startup.
  // Source: User report 2026-06-01 — "delayed first sync trap: setInterval
  //   waits full delay before executing, leaving keys stale after restarts."
  // ----------------------------------------------------------------
  const runSync = async () => {
    try {
      await syncApiKeyUsage();
    } catch (err: any) {
      console.error('[api-key-sync] Error:', err?.message || err);
    }
  };

  // Run immediately on boot.
  runSync();

  // Then schedule the hourly interval.
  syncTimer = setInterval(runSync, 3600000); // 1 hour

  // Prevent the timer from keeping the process alive on shutdown.
  if (typeof syncTimer.unref === 'function') {
    syncTimer.unref();
  }

  console.log('[api-key-sync] Hourly sync enabled (running initial sync now)');
}

export function disableApiKeySync(): void {
  if (!syncTimer) return;
  clearInterval(syncTimer);
  syncTimer = null;
  console.log('[api-key-sync] Hourly sync disabled');
}
