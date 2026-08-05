import cron from 'node-cron';
import { refreshDerivedProjectionsWithJob } from './derived-projection-tracker';
import { runExclusive } from './worker-lock';

/**
 * Derived Projection Scheduler.
 *
 * The ingest worker updates projection tables incrementally for fresh matches,
 * but those tables are still derived from facts. A daily off-peak rebuild gives
 * the backend a self-healing path when old completed matches missed a new
 * projection, reference data was absent, or a projection bug was fixed.
 */
export const jobs = {
  refresh: cron.createTask(
    '30 3 * * *',
    async () => {
      await runExclusive('derived-projections:refresh', async () => {
        await refreshDerivedProjectionsWithJob('scheduler');
      }).catch((err) => {
        console.error(`[derived-projection-tracker] Refresh failed: ${err}`);
      });
    },
  ),
};

export function enableAll() {
  jobs.refresh.start();
  console.log('[derived-projection-tracker] Cron job enabled (daily at 03:30)');
}

export function disableAll() {
  jobs.refresh.stop();
  console.log('[derived-projection-tracker] Cron job disabled');
}
