import cron from 'node-cron';
import { query } from '../config/db';
import { track } from './ranked-tracker';
import { runExclusive } from './worker-lock';

/**
 * Ranked Tracker Scheduler
 *
 * Every 4 hours: cron expression for every-4-hours
 *
 * Restart behavior:
 * - `sync_jobs` is the durable heartbeat for leaderboard snapshots.
 * - On startup we inspect the most recent ranked_tracker row. If there is no
 *   recent successful completion and no fresh running lease, we run once
 *   immediately instead of waiting for the next 4-hour cron edge.
 * - A completely empty leaderboard result is marked failed in ranked-tracker.ts,
 *   so temporary Hi-Rez outages do not masquerade as healthy completed syncs.
 */

const LEADERBOARD_STALE_HOURS = 4;
const RUNNING_LEASE_MINUTES = 45;

async function runLeaderboardFetch(reason: string): Promise<void> {
  await runExclusive('ranked-tracker:leaderboard', async () => {
    console.log(`[ranked-tracker] Running leaderboard fetch (${reason})...`);
    await track();
  }).catch((err) => {
    console.error(`[ranked-tracker] Leaderboard fetch failed (${reason}): ${err}`);
  });
}

async function runStartupCatchup(): Promise<void> {
  const rows = await query<{
    status: string;
    started_at: string | null;
    completed_at: string | null;
    completed_recent: boolean;
    running_recent: boolean;
  }>(`
    SELECT status,
           started_at::text,
           completed_at::text,
           (status = 'completed' AND completed_at >= now() - ($1::int * interval '1 hour')) AS completed_recent,
           (status = 'running' AND started_at >= now() - ($2::int * interval '1 minute')) AS running_recent
    FROM sync_jobs
    WHERE job_type IN ('ranked_tracker', 'ranked-tracker')
    ORDER BY COALESCE(completed_at, started_at, created_at) DESC
    LIMIT 1
  `, [LEADERBOARD_STALE_HOURS, RUNNING_LEASE_MINUTES]);

  const latest = rows[0];
  if (latest?.running_recent) {
    console.log(`[ranked-tracker] Startup catch-up skipped; latest job is still running from ${latest.started_at}`);
    return;
  }

  if (latest?.completed_recent) {
    console.log(`[ranked-tracker] Startup catch-up skipped; latest completed at ${latest.completed_at}`);
    return;
  }

  await runLeaderboardFetch('startup catch-up');
}

export const jobs = {
  rankedTracker: cron.createTask(
    '0 */4 * * *',
    async () => {
      await runLeaderboardFetch('cron');
    },
  ),
};

let startupTimer: NodeJS.Timeout | null = null;

export function enableAll() {
  jobs.rankedTracker.start();
  console.log('[ranked-tracker] Cron job enabled');

  startupTimer = setTimeout(() => {
    startupTimer = null;
    runStartupCatchup().catch((err) => {
      console.error(`[ranked-tracker] Startup catch-up failed: ${err}`);
    });
  }, 25_000);
  startupTimer.unref();
}

export function disableAll() {
  if (startupTimer) clearTimeout(startupTimer);
  startupTimer = null;
  jobs.rankedTracker.stop();
  console.log('[ranked-tracker] Cron job disabled');
}
