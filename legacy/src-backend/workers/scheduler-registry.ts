import { enableAll as enableAutoIngest, disableAll as disableAutoIngest } from './auto-ingester-scheduler';
import { enableAll as enableBaselineTracker, disableAll as disableBaselineTracker } from './baseline-tracker-scheduler';
import { enableAll as enableDerivedProjectionTracker, disableAll as disableDerivedProjectionTracker } from './derived-projection-scheduler';
import { enableAll as enableGapChecker, disableAll as disableGapChecker } from './hourly-gap-checker';
import { enableAll as enablePlayerActivityProfile, disableAll as disablePlayerActivityProfile } from './player-activity-profile-enrichment';
import { enableAll as enableRankedTracker, disableAll as disableRankedTracker } from './ranked-tracker-scheduler';
import { enableAll as enableTierStats, disableAll as disableTierStats } from './tier-stats-scheduler';
import {
  acquireTypescriptSchedulerOwnership,
  heartbeatTypescriptSchedulerOwnership,
  releaseTypescriptSchedulerOwnership,
} from '../services/scheduler-ownership';

export interface BackendSchedulerDefinition {
  key: string;
  jobTypes: string[];
  description: string;
  enable: () => void;
  disable: () => void;
}

/**
 * Active backend scheduler registry.
 *
 * This is the one place that should answer "what background jobs does the
 * backend process own?" Worker implementation files still stay separate because
 * they own different data domains, but startup and status reporting should not
 * each maintain their own independent scheduler lists.
 *
 * Ownership split:
 * - Backend owns DB ingest, buffer drain, derived projections, baselines,
 *   ranked leaderboard pulls, gap checks, and tier snapshots.
 * - HirezRelay owns Hi-Rez session/key usage sync in real mode.
 */
export const BACKEND_SCHEDULERS: BackendSchedulerDefinition[] = [
  {
    key: 'ranked_tracker',
    jobTypes: ['ranked_tracker', 'ranked-tracker'],
    description: 'Ranked leaderboard snapshots every 4 hours plus startup catch-up',
    enable: enableRankedTracker,
    disable: disableRankedTracker,
  },
  {
    key: 'auto_ingester',
    jobTypes: ['auto_ingester'],
    description: 'Hourly match discovery, bounded active-player profile enrichment, 5-minute raw buffer drain, hourly raw/history retention, and hourly MV refresh',
    enable: () => {
      enableAutoIngest();
      enablePlayerActivityProfile();
    },
    disable: () => {
      disablePlayerActivityProfile();
      disableAutoIngest();
    },
  },
  {
    key: 'baseline_tracker',
    jobTypes: ['baseline_tracker', 'afk_tracker'],
    description: 'Daily role/queue baseline rebuild',
    enable: enableBaselineTracker,
    disable: disableBaselineTracker,
  },
  {
    key: 'derived_projection_tracker',
    jobTypes: ['derived_projection_tracker'],
    description: 'Daily repair rebuild for local derived projection tables',
    enable: enableDerivedProjectionTracker,
    disable: disableDerivedProjectionTracker,
  },
  {
    key: 'hourly_gap_checker',
    jobTypes: [],
    description: 'Hourly scan for missed ingest windows using hourly_ingest_state',
    enable: enableGapChecker,
    disable: disableGapChecker,
  },
  {
    key: 'tier_stats',
    jobTypes: [],
    description: 'Hourly tier_stats snapshot refresh from local tables',
    enable: enableTierStats,
    disable: disableTierStats,
  },
];

export const BACKEND_SCHEDULER_JOB_TYPES = BACKEND_SCHEDULERS.flatMap((scheduler) => scheduler.jobTypes);
export const BACKEND_SCHEDULER_KEYS = BACKEND_SCHEDULERS.map((scheduler) => scheduler.key);

const OWNERSHIP_HEARTBEAT_MS = 15_000;
const activeSchedulers = new Map<string, BackendSchedulerDefinition>();
let backendSchedulersInitialized = false;
let ownershipHeartbeat: NodeJS.Timeout | null = null;
let heartbeatRunning = false;

async function heartbeatOwnedSchedulers(): Promise<void> {
  if (heartbeatRunning) return;
  heartbeatRunning = true;
  try {
    for (const scheduler of [...activeSchedulers.values()]) {
      let retained = false;
      try {
        retained = await heartbeatTypescriptSchedulerOwnership(scheduler.key);
      } catch (error) {
        console.error(
          `[scheduler-registry] Ownership heartbeat failed for ${scheduler.key}: ${error}`,
        );
      }
      if (retained) continue;
      scheduler.disable();
      activeSchedulers.delete(scheduler.key);
      console.error(
        `[scheduler-registry] Disabled ${scheduler.key} after ownership loss`,
      );
    }
  } finally {
    heartbeatRunning = false;
  }
}

export async function enableBackendSchedulers(): Promise<void> {
  if (backendSchedulersInitialized) return;
  const enabled: BackendSchedulerDefinition[] = [];
  const claimed: BackendSchedulerDefinition[] = [];
  try {
    for (const scheduler of BACKEND_SCHEDULERS) {
      const acquired = await acquireTypescriptSchedulerOwnership(scheduler.key);
      if (!acquired) {
        console.log(
          `[scheduler-registry] ${scheduler.key} is assigned to or leased by another engine`,
        );
        continue;
      }
      claimed.push(scheduler);
      scheduler.enable();
      enabled.push(scheduler);
      activeSchedulers.set(scheduler.key, scheduler);
    }
    backendSchedulersInitialized = true;
    if (activeSchedulers.size > 0) {
      ownershipHeartbeat = setInterval(() => {
        void heartbeatOwnedSchedulers();
      }, OWNERSHIP_HEARTBEAT_MS);
      ownershipHeartbeat.unref();
    }
    console.log(
      `[startup] TypeScript scheduler domains enabled: ${
        [...activeSchedulers.keys()].join(', ') || 'none'
      }; Hi-Rez key sync is owned by HirezRelay`,
    );
  } catch (error) {
    for (const scheduler of enabled.reverse()) {
      try { scheduler.disable(); } catch { /* preserve the startup error */ }
    }
    await Promise.allSettled(
      claimed.map((scheduler) => releaseTypescriptSchedulerOwnership(scheduler.key)),
    );
    activeSchedulers.clear();
    throw error;
  }
}

export async function disableBackendSchedulers(): Promise<void> {
  const enabled = [...activeSchedulers.values()];
  const disabled: BackendSchedulerDefinition[] = [];
  const failures: unknown[] = [];
  for (const scheduler of enabled.reverse()) {
    try {
      scheduler.disable();
      disabled.push(scheduler);
      activeSchedulers.delete(scheduler.key);
    } catch (error) {
      failures.push(error);
      console.error(
        `[scheduler-registry] Failed to disable ${scheduler.key}; retaining ownership: ${error}`,
      );
    }
  }
  await Promise.allSettled(
    disabled.map((scheduler) => releaseTypescriptSchedulerOwnership(scheduler.key)),
  );
  if (failures.length > 0) {
    throw new AggregateError(
      failures,
      'One or more TypeScript scheduler domains failed to quiesce',
    );
  }
  if (ownershipHeartbeat) clearInterval(ownershipHeartbeat);
  ownershipHeartbeat = null;
  backendSchedulersInitialized = false;
}

export function areBackendSchedulersRunning(): boolean {
  return activeSchedulers.size > 0;
}

export function getOwnedBackendSchedulerKeys(): string[] {
  return [...activeSchedulers.keys()];
}
