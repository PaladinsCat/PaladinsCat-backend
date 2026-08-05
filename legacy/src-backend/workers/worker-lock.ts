import { pool } from '../config/db';
import type { PoolClient } from 'pg';

const localRunning = new Set<string>();

async function recordSchedulerCaptureEvent(
  client: PoolClient,
  jobName: string,
  eventType: 'lock-acquired' | 'run-started' | 'run-completed' | 'run-failed',
): Promise<void> {
  const marker = process.env.PALADINSCAT_SCHEDULER_CAPTURE_MARKER;
  if (process.env.PALADINSCAT_SCHEDULER_CAPTURE_ENABLE !== 'true' || !marker) return;
  const databaseHost = new URL(process.env.DATABASE_URL || '').hostname;
  const relayHost = new URL(process.env.HIREZ_RELAY_URL || '').hostname;
  if (!['127.0.0.1', '::1', 'localhost'].includes(databaseHost) ||
      !['127.0.0.1', '::1', 'localhost'].includes(relayHost)) {
    throw new Error('scheduler capture evidence requires loopback DB and relay');
  }
  const allowed = await client.query<{ allowed: boolean }>(
    'SELECT EXISTS(SELECT 1 FROM scheduler_capture_marker WHERE marker=$1) AS allowed',
    [marker],
  );
  if (!allowed.rows[0]?.allowed) throw new Error('scheduler capture marker is not present in this DB');
  await client.query(
    `INSERT INTO scheduler_capture_events(marker, runtime, event_type, job_key)
     VALUES ($1, 'typescript', $2, $3)`,
    [marker, eventType, jobName],
  );
}

export function getActiveWorkerJobs(): string[] {
  return [...localRunning].sort();
}

export async function waitForActiveWorkerJobs(
  timeoutMs: number,
  pollIntervalMs = 100,
): Promise<{ drained: boolean; activeJobs: string[] }> {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  while (localRunning.size > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }
  const activeJobs = getActiveWorkerJobs();
  return { drained: activeJobs.length === 0, activeJobs };
}

/**
 * Run a scheduled worker with both local and PostgreSQL advisory locking.
 *
 * Why both locks:
 * - The local Set prevents the same Node process from starting a second copy
 *   of a long job when the next cron tick arrives before the previous one ends.
 * - pg_try_advisory_lock() prevents multiple backend instances from doing the
 *   same API or heavy DB work at the same time. This matters for PaladinsCat
 *   because duplicate scheduler instances can multiply Hi-Rez calls or stack
 *   expensive materialized-view/tier refreshes.
 *
 * The advisory lock is held on a dedicated pooled client for the whole job and
 * released in finally. If the process crashes, PostgreSQL releases the session
 * lock when the connection dies, so stale locks do not need manual cleanup.
 */
export async function runExclusive<T>(jobName: string, fn: () => Promise<T>): Promise<T | null> {
  if (localRunning.has(jobName)) {
    console.log(`[worker-lock] ${jobName} already running locally; skipping overlapping tick`);
    return null;
  }

  localRunning.add(jobName);
  let client: PoolClient | null = null;
  let locked = false;

  try {
    client = await pool.connect();
    const lockResult = await client.query(
      'SELECT pg_try_advisory_lock(hashtext($1)) AS locked',
      [jobName],
    );
    locked = Boolean(lockResult.rows[0]?.locked);
    if (!locked) {
      console.log(`[worker-lock] ${jobName} already running in another process; skipping`);
      return null;
    }
    await recordSchedulerCaptureEvent(client, jobName, 'lock-acquired');
    await recordSchedulerCaptureEvent(client, jobName, 'run-started');
    try {
      const result = await fn();
      await recordSchedulerCaptureEvent(client, jobName, 'run-completed');
      return result;
    } catch (error) {
      await recordSchedulerCaptureEvent(client, jobName, 'run-failed');
      throw error;
    }
  } finally {
    if (locked && client) {
      try {
        await client.query('SELECT pg_advisory_unlock(hashtext($1))', [jobName]);
      } catch (error) {
        console.warn(`[worker-lock] Failed to release ${jobName}: ${error}`);
      }
    }
    client?.release();
    localRunning.delete(jobName);
  }
}
