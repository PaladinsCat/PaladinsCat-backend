import cron from 'node-cron';
import { refreshBaselinesWithJob } from './baseline-tracker';
import { runExclusive } from './worker-lock';

/**
 * Baseline Tracker Scheduler - node-cron jobs.
 *
 * Baseline recalculation: 0 3 * * * (3 AM daily)
 *
 * This worker only maintains `public.baselines`, the derived role/queue metric
 * table used for dashboards and analysis. AFK scoring itself happens inline in
 * buffer-processor via `afk_rate`/`egpm`; naming this scheduler "baseline"
 * keeps that distinction visible when debugging worker status.
 */

export const jobs = {
  baseline: cron.createTask(
    '0 3 * * *',
    async () => {
      await runExclusive('baseline:refresh', async () => {
        console.log('[baseline-tracker] Recalculating baselines...');
        await refreshBaselinesWithJob('scheduler');
      }).catch((err) => {
        console.error(`[baseline-tracker] Baseline calculation failed: ${err}`);
      });
    },
  ),
};

export function enableAll() {
  jobs.baseline.start();
  console.log('[baseline-tracker] Cron job enabled');
}

export function disableAll() {
  jobs.baseline.stop();
  console.log('[baseline-tracker] Cron job disabled');
}
