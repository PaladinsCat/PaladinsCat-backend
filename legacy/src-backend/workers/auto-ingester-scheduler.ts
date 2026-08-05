import cron from 'node-cron';
import { discover, discoverPresenceQueues } from './active-match-discovery';
import { cleanupRawIngestBufferRetention, drainRawIngestBuffer } from './buffer-processor';
import { cleanupPlayerHistoryRetention } from './player-history-retention';
import { runExclusive } from './worker-lock';
import { runNonrankedMatchAcquisition } from './nonranked-match-acquisition';

// Incremented whenever deployment/shutdown quiesces schedulers. Each running
// drain captures its starting generation, so re-enabling schedulers after a
// failed deployment cannot accidentally cancel the stop request for that old
// run before it has observed it.
let schedulerGeneration = 0;

/**
 * Auto Ingester Scheduler
 *
 * Runs at :30 each hour. Fetches matches from the previous hour,
 * then processes the raw_ingest_buffer through the full pipeline.
 * Example: at 00:30, fetches 23:00-23:59 of the previous day.
 *
 * Restart behavior:
 * - Cron is only the trigger; hourly_ingest_state is the durable source of
 *   truth for whether the previous hour still needs discovery.
 * - On backend startup we run one delayed catch-up discovery for the previous
 *   full hour. If the normal cron already staged/completed it, discover() will
 *   skip before making another relay/API call.
 * - We also drain the buffer on startup so rows staged before a crash do not
 *   sit idle until the next 5-minute cron tick.
 */

async function runDiscoveryCycle(reason: string): Promise<void> {
  const runGeneration = schedulerGeneration;
  const shouldStop = () => schedulerGeneration !== runGeneration;
  await runExclusive('auto-ingester:discovery', async () => {
    if (shouldStop()) return;
    console.log(`[auto-ingester] Running discovery (${reason})...`);
    await discover().catch((error) => {
      console.error(`[auto-ingester] Ranked discovery failed (${reason}); continuing presence queues: ${error}`);
    });
    await discoverPresenceQueues(undefined, undefined, `auto-ingester-${reason.replace(/\s+/g, '-')}`);
    await runNonrankedMatchAcquisition(`auto-ingester ${reason}`, { seedLedger: false });
  }).catch((err) => {
    console.error(`[auto-ingester] Discovery failed (${reason}): ${err}`);
  });

  // Discovery and the five-minute/startup drain must share one lock. The old
  // nested drain ran under auto-ingester:discovery while cron used
  // auto-ingester:buffer-drain, allowing two 50-row claims to overlap and mark
  // each other's slow projection work stale.
  if (!shouldStop()) await runBufferDrain(`${reason} post-discovery`);
}

async function runBufferDrain(reason: string): Promise<void> {
  const runGeneration = schedulerGeneration;
  const shouldStop = () => schedulerGeneration !== runGeneration;
  await runExclusive('auto-ingester:buffer-drain', async () => {
    if (shouldStop()) return;
    console.log(`[auto-ingester] Processing buffer (${reason})...`);
    const result = await drainRawIngestBuffer({
      batchSize: 50,
      reason: `auto-ingester ${reason}`,
      shouldStop,
    });
    console.log(
      `[auto-ingester] Buffer drain (${reason}): ${result.processed} complete, ` +
      `${result.deferred} facts-first, ${result.failed} failed`,
    );
  }).catch((err) => {
    console.error(`[auto-ingester] Buffer drain failed (${reason}): ${err}`);
  });
}

async function runBufferRetention(reason: string): Promise<void> {
  await runExclusive('auto-ingester:buffer-retention', async () => {
    console.log(`[auto-ingester] Running raw buffer retention (${reason})...`);
    const result = await cleanupRawIngestBufferRetention(`auto-ingester ${reason}`);
    console.log(
      `[auto-ingester] Raw buffer retention (${reason}): ` +
      `${result.totalDeleted} deleted ` +
      `(processed=${result.processedDeleted}, failed=${result.failedDeleted})`,
    );
  });
}

async function runPlayerHistoryRetention(reason: string): Promise<void> {
  await runExclusive('auto-ingester:player-history-retention', async () => {
    console.log(`[auto-ingester] Running player history retention (${reason})...`);
    const result = await cleanupPlayerHistoryRetention(`auto-ingester ${reason}`);
    console.log(
      `[auto-ingester] Player history retention (${reason}): ` +
      `${result.totalDeleted} deleted ` +
      `(cache_expired=${result.cacheExpiredDeleted}, ` +
      `entry_expired=${result.entryExpiredDeleted}, ` +
      `entry_authoritative=${result.entryAuthoritativeDeleted})`,
    );
  });
}

export const jobs = {
  discovery: cron.createTask(
    '30 * * * *',
    async () => {
      await runDiscoveryCycle('cron');
    },
  ),

  // Drain pending buffer items every 5 minutes (no minimum batch threshold)
  bufferDrain: cron.createTask(
    '*/5 * * * *',
    async () => {
      await runBufferDrain('cron');
    },
  ),

  // Keep raw_ingest_buffer bounded without erasing the most useful debugging
  // window. The cleanup function only removes terminal rows and writes a compact
  // endpoint/entity summary into raw_ingest_buffer_retention_audit first. It is
  // deliberately offset from MV refresh (:05) and discovery (:30).
  bufferRetention: cron.createTask(
    '17 * * * *',
    async () => {
      await runBufferRetention('cron').catch((err) => {
        console.error(`[auto-ingester] Raw buffer retention failed: ${err}`);
      });
    },
  ),

  // History retention is separate from raw buffer retention because these rows
  // are not queue work. They are reusable getmatchhistory cache/observation
  // facts. Read paths ignore expired rows, but without this hourly delete pass
  // the tables would still grow by every unique player/match history lookup
  // ever observed. Keep this offset from buffer retention so both capped delete
  // jobs do not compete for the database at the same minute.
  playerHistoryRetention: cron.createTask(
    '23 * * * *',
    async () => {
      await runPlayerHistoryRetention('cron').catch((err) => {
        console.error(`[auto-ingester] Player history retention failed: ${err}`);
      });
    },
  ),

  // ----------------------------------------------------------------
  // Materialized view refresh — hourly cron job.
  // Moved here from the hot path of processBufferBatch() to prevent
  // "materialized view thrashing" during large catch-up runs. Old code
  // refreshed MVs after every batch of 10+ matches, causing hundreds of
  // back-to-back expensive refreshes that collapsed PostgreSQL performance.
  // Now: runs once per hour regardless of queue volume, decoupled from
  // buffer processing entirely.
  // Source: User report 2026-06-01 — "materialized view thrashing:
  //   buffer processor refreshes MVs on every batch, collapsing PG perf."
  // ----------------------------------------------------------------
  mvRefresh: cron.createTask(
    '5 * * * *',
    async () => {
      await runExclusive('auto-ingester:mv-refresh', async () => {
        console.log('[auto-ingester] Running hourly Materialized View refresh...');
        const { refreshMaterializedViews } = await import('./buffer-processor.js');
        await refreshMaterializedViews();
      }).catch((err) => {
        console.error(`[auto-ingester] MV refresh failed: ${err}`);
      });
    },
  ),
};

/**
 * Compatibility evidence entrypoint.  This executes the cron task itself
 * rather than duplicating its callback; it is only invoked by the local,
 * disposable scheduler capture runner.
 */
export async function runScheduledDiscoveryDueJobForParity(): Promise<void> {
  await jobs.discovery.execute();
}



const startupTimers = new Set<NodeJS.Timeout>();

function scheduleStartupTask(task: () => void, delayMs: number): void {
  const timer = setTimeout(() => {
    startupTimers.delete(timer);
    task();
  }, delayMs);
  timer.unref();
  startupTimers.add(timer);
}

export function enableAll() {
  jobs.discovery.start();
  jobs.bufferDrain.start();
  jobs.bufferRetention.start();
  jobs.playerHistoryRetention.start();
  jobs.mvRefresh.start();
  console.log('[auto-ingester] Cron jobs enabled (discovery + buffer drain + raw/history retention + hourly MV refresh)');

  // Startup catch-up is delayed a few seconds so Fastify, DB pools, and the
  // relay dependency can finish their own initialization first. The durable
  // hourly_ingest_state claim in discover() makes this idempotent: a restart
  // right after a successful HH:30 cron tick will log a skip instead of burning
  // another detail-fetch cycle.
  scheduleStartupTask(() => {
    runDiscoveryCycle('startup catch-up').catch((err) => {
      console.error(`[auto-ingester] Startup discovery failed: ${err}`);
    });
  }, 10_000);

  scheduleStartupTask(() => {
    runBufferDrain('startup drain').catch((err) => {
      console.error(`[auto-ingester] Startup buffer drain failed: ${err}`);
    });
  }, 15_000);

  scheduleStartupTask(() => {
    runBufferRetention('startup retention').catch((err) => {
      console.error(`[auto-ingester] Startup raw buffer retention failed: ${err}`);
    });
  }, 20_000);

  scheduleStartupTask(() => {
    runPlayerHistoryRetention('startup retention').catch((err) => {
      console.error(`[auto-ingester] Startup player history retention failed: ${err}`);
    });
  }, 25_000);
}

export function disableAll() {
  schedulerGeneration++;
  for (const timer of startupTimers) clearTimeout(timer);
  startupTimers.clear();
  jobs.discovery.stop();
  jobs.bufferDrain.stop();
  jobs.bufferRetention.stop();
  jobs.playerHistoryRetention.stop();
  jobs.mvRefresh.stop();
  console.log('[auto-ingester] Cron jobs disabled');
}
