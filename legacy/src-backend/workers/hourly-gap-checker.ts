import cron from 'node-cron';
import { query } from '../config/db';
import { discover, discoverPresenceQueue } from './active-match-discovery';
import { getHourlyIngestStates, HourlyIngestStateRow, recordHourlyIngestQuotaWait } from './hourly-ingest-state';
import { getDueHourlyIngestDebtHours, reviveFreshNoAuthorityHourlyIngestMatchDebt } from './hourly-ingest-match-debt';
import { runExclusive } from './worker-lock';
import { getApiHeadroomSnapshot } from './api-headroom';
import {
  MATCH_DETAIL_SERVICE_OUTAGE_KEY,
  getActiveHirezServiceOutage,
  isHirezServiceOutageProbeDue,
} from './hirez-service-outage';
import { MATCH_COUNT_QUEUE_DEFINITIONS } from './match-count-discovery-policy';

/**
 * Hourly Gap Checker
 *
 * Periodically scans explicit hourly ingest state for missing hours and
 * backfills them.
 *
 * Why: The auto-ingester runs at HH:30 each hour and fetches the previous hour.
 * If it fails (crash, API error, network issue), that hour is permanently lost.
 * This checker detects gaps and triggers backfill via `discover()`.
 *
 * Scope:
 * - New missing-hour discovery checks fetch ticks for the current UTC date, not
 *   raw match-date hours. This avoids historical crawls while still covering
 *   the midnight rollover: today's 00:30 fetch tick targets yesterday
 *   23:00-23:59.
 * - Existing retryable hourly_ingest_state rows are scanned through a short
 *   lookback window. They are safe to retry after midnight because the row is
 *   already explicit scheduler state, not a guessed historical gap.
 * - A one-hour hole bracketed by state for the same queue is also explicit
 *   outage evidence. Recovering only those interior holes catches a crashed
 *   tick without making a fresh database crawl historical hours.
 * - Checks every HH:30 fetch tick that has already elapsed today. At 05:15 UTC,
 *   the latest elapsed tick is 04:30, which targets 03:00-03:59. At 05:40 UTC,
 *   the latest elapsed tick is 05:30, which targets 04:00-04:59.
 * - Every configured queue uses the same hourly_ingest_state control plane.
 *   Ranked may recover known match debt; presence queues only recover a missed
 *   queue-hour discovery call.
 * - If backfill finds 0 matches, records status='empty' with next_retry_at.
 *   A zero analytics row may exist for dashboards, but it is never the
 *   skip/retry signal.
 *
 * Schedule: Offset checks several times per hour. The default avoids the :30
 * discovery tick while still retrying known match debt quickly.
 *
 * Source: User request 2026-06-01 — "no logic checks for continuous hours
 * and spots the missing one and backfills."
 */

const QUEUE_ID = 486; // Ranked only — matches auto-ingester scope
const MIN_CHECK_DATE = '2026-05-31'; // Deployment date — never check before this
const RETRY_STATE_LOOKBACK_DAYS = Math.max(
  1,
  Number(process.env.GAP_CHECKER_RETRY_STATE_LOOKBACK_DAYS || 2),
);
const GAP_CHECKER_CRON_EXPRESSION = process.env.GAP_CHECKER_CRON_EXPRESSION || '5,15,25,40,50 * * * *';

type GapCandidate = {
  date: string;
  hour: number;
  queueId?: number;
  presenceOnly?: boolean;
  debtOnly?: boolean;
};

const PRESENCE_QUEUE_IDS = MATCH_COUNT_QUEUE_DEFINITIONS
  .filter(queue => !queue.ranked)
  .map(queue => queue.queueId);

function expectedElapsedDiscoveryHours(now = new Date()): Array<{ date: string; hour: number }> {
  const latestElapsedFetchTickHour = now.getUTCMinutes() >= 30
    ? now.getUTCHours()
    : now.getUTCHours() - 1;
  if (latestElapsedFetchTickHour < 0) return [];
  const dayStart = Date.UTC(
    now.getUTCFullYear(),
    now.getUTCMonth(),
    now.getUTCDate(),
    0,
    30,
  );
  return Array.from({ length: latestElapsedFetchTickHour + 1 }, (_, fetchHour) => {
    const target = new Date(dayStart + fetchHour * 3600000 - 3600000);
    return { date: target.toISOString().slice(0, 10), hour: target.getUTCHours() };
  }).filter(window => window.date >= MIN_CHECK_DATE);
}

async function findMissingPresenceHours(): Promise<GapCandidate[]> {
  if (PRESENCE_QUEUE_IDS.length === 0) return [];
  const now = new Date();
  const expected = expectedElapsedDiscoveryHours(now);
  if (expected.length === 0) return [];
  const retryLookbackDate = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - RETRY_STATE_LOOKBACK_DAYS),
  ).toISOString().slice(0, 10);
  const minDate = retryLookbackDate < MIN_CHECK_DATE ? MIN_CHECK_DATE : retryLookbackDate;
  const maxDate = expected[expected.length - 1].date;
  const rows = await query<HourlyIngestStateRow>(
    `SELECT date::text, hour, queue_id, status, attempts, raw_match_count,
            staged_match_count, fetched, fetch_succeeded, source, error_message,
            last_attempt_at::text, next_retry_at::text, lease_until::text,
            completed_at::text
     FROM hourly_ingest_state
     WHERE queue_id = ANY($1::int[])
       AND date BETWEEN $2::date AND $3::date`,
    [PRESENCE_QUEUE_IDS, minDate, maxDate],
  );
  const states = new Map(rows.map(row => [`${row.queue_id}|${row.date}|${row.hour}`, row]));
  const nowMs = Date.now();
  const due = (value: string | null): boolean => {
    if (!value) return true;
    const parsed = new Date(value).getTime();
    return !Number.isFinite(parsed) || parsed <= nowMs;
  };
  const missing = new Map<string, GapCandidate>();
  for (const queueId of PRESENCE_QUEUE_IDS) {
    for (const window of expected) {
      const state = states.get(`${queueId}|${window.date}|${window.hour}`);
      if (!state) {
        missing.set(`${queueId}|${window.date}|${window.hour}`, { ...window, queueId, presenceOnly: true });
        continue;
      }
      // Complete and legacy empty observations are final, including zero IDs.
      if (state.status === 'complete' || state.status === 'empty') continue;
      if (
        (state.status === 'fetching' || state.status === 'staged')
          ? due(state.lease_until)
          : due(state.next_retry_at)
      ) {
        missing.set(`${queueId}|${window.date}|${window.hour}`, { ...window, queueId, presenceOnly: true });
      }
    }
  }

  // Failed presence observations remain explicit recovery work after midnight.
  // Without this pass, an outage row from yesterday is queried but never added
  // because only today's expected tick grid is enumerated above.
  for (const row of rows) {
    if (row.status === 'complete' || row.status === 'empty') continue;
    const retryable = (row.status === 'fetching' || row.status === 'staged')
      ? due(row.lease_until)
      : due(row.next_retry_at);
    if (retryable) {
      missing.set(`${row.queue_id}|${row.date}|${row.hour}`, {
        date: row.date,
        hour: row.hour,
        queueId: row.queue_id,
        presenceOnly: true,
      });
    }
  }

  // A process crash can leave no state row at all. Restrict reconstruction to
  // a single missing hour with observed state immediately on both sides for
  // the same queue, inside the existing bounded lookback. This catches an
  // outage tick (for example 22:00 between 21:00 and 23:00) without guessing
  // at a reset database or a multi-hour historical interval.
  const earliestMs = Date.parse(`${minDate}T00:00:00.000Z`);
  const latest = expected[expected.length - 1];
  const latestMs = Date.UTC(
    Number(latest.date.slice(0, 4)),
    Number(latest.date.slice(5, 7)) - 1,
    Number(latest.date.slice(8, 10)),
    latest.hour,
  );
  for (const queueId of PRESENCE_QUEUE_IDS) {
    for (let atMs = earliestMs + 3600000; atMs < latestMs; atMs += 3600000) {
      const target = new Date(atMs);
      const date = target.toISOString().slice(0, 10);
      const hour = target.getUTCHours();
      const key = `${queueId}|${date}|${hour}`;
      if (states.has(key)) continue;
      const previous = new Date(atMs - 3600000);
      const next = new Date(atMs + 3600000);
      const previousKey = `${queueId}|${previous.toISOString().slice(0, 10)}|${previous.getUTCHours()}`;
      const nextKey = `${queueId}|${next.toISOString().slice(0, 10)}|${next.getUTCHours()}`;
      if (states.has(previousKey) && states.has(nextKey)) {
        missing.set(key, { date, hour, queueId, presenceOnly: true });
      }
    }
  }

  return [...missing.values()].sort((left, right) => (
    left.date.localeCompare(right.date)
    || left.hour - right.hour
    || Number(left.queueId || 0) - Number(right.queueId || 0)
  ));
}

/**
 * Find missing target hours for fetch ticks on the current UTC date.
 *
 * Builds a grid of target (date, hour) pairs by enumerating today's HH:30
 * scheduler ticks and subtracting one hour from each tick. This means:
 *   - 2026-06-17 00:30 fetch tick -> target 2026-06-16 23:00
 *   - 2026-06-17 05:30 fetch tick -> target 2026-06-17 04:00
 *
 * Includes two bounded work sources:
 * - Expected fetch ticks for the current UTC date. This prevents a fresh DB from
 *   becoming a historical crawler.
 * - Existing retryable hourly_ingest_state rows from a short lookback window.
 *   This is deliberately not a crawl: the row must already exist, which means a
 *   prior worker discovered/claimed the hour. Without this, failed rows from
 *   yesterday 00:00-22:00 become stranded after midnight because only the
 *   00:30 rollover tick points at yesterday 23:00.
 *
 * Excludes future fetch ticks that have not reached HH:30 yet.
 *
 * @returns Array of gap candidates. `debtOnly=true` means the match IDs are
 * already stored in hourly_ingest_match_debt, so backfill should skip
 * getmatchidsbyqueue and spend only on known unresolved IDs.
 */
async function findMissingHours(): Promise<GapCandidate[]> {
  const now = new Date();
  const currentDate = now.toISOString().slice(0, 10); // YYYY-MM-DD

  if (currentDate < MIN_CHECK_DATE) return [];

  // Generate expected target hours in JS, query DB for existing ones.
  const expectedHours = expectedElapsedDiscoveryHours(now);

  // Query DB for existing analytics counts and the explicit scheduler state.
  // Positive hourly_match_counts rows are still accepted as legacy completion
  // evidence. Zero-count rows are NOT accepted as a control signal anymore:
  // they can mean "true empty hour", "Hi-Rez returned empty while down", or
  // "buffer has not drained yet." hourly_ingest_state carries that distinction.
  const retryLookbackDate = new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - RETRY_STATE_LOOKBACK_DAYS),
  ).toISOString().slice(0, 10);
  const minDate = retryLookbackDate < MIN_CHECK_DATE ? MIN_CHECK_DATE : retryLookbackDate;
  const maxDate = currentDate;

  const existing = await query(`
    SELECT date::text AS d, "hour", total_matches
    FROM hourly_match_counts
    WHERE queue_id = $1
      AND date >= $2::date
      AND date <= $3::date
  `, [QUEUE_ID, minDate, maxDate]);
  const states = await getHourlyIngestStates(QUEUE_ID, minDate, maxDate);

  // Match debt is the authoritative per-ID ledger. A failed hour with
  // raw_match_count=54 and 50 normalized matches is still complete when the
  // remaining 4 IDs are terminal unrecoverable rows and no pending/staged debt
  // exists. Counting that terminal debt here prevents gap-checker from
  // rediscovering a closed hour just because hourly_match_counts only stores
  // successfully ingested matches.
  const debtSummaries = await query(`
    SELECT date::text AS d,
           "hour",
           COUNT(*) FILTER (WHERE status = 'unrecoverable')::int AS terminal_count,
           COUNT(*) FILTER (WHERE status IN ('pending', 'staged'))::int AS open_count
    FROM hourly_ingest_match_debt
    WHERE queue_id = $1
      AND date >= $2::date
      AND date <= $3::date
    GROUP BY date, "hour"
  `, [QUEUE_ID, minDate, maxDate]);

  // Fresh no-authoritative rows must remain recoverable known debt. This covers
  // terminal rows written by older worker versions that labeled a full hour as
  // `dropped/corrupt` after targeted recovery produced no authoritative match
  // payload. True `api_no_data` and explicit no-player-anchor broken rows stay
  // terminal; only the mistaken no-authority class is revived here so gap-check
  // can retry exact match IDs before the 50-match history window is lost.
  const revivedFreshDebt = await reviveFreshNoAuthorityHourlyIngestMatchDebt(QUEUE_ID, minDate, maxDate);
  if (revivedFreshDebt.length > 0) {
    console.warn(
      `[gap-checker] Revived fresh no-authority match debt: ` +
      revivedFreshDebt.map(row => `${row.date}T${String(row.hour).padStart(2, '0')}Z=${row.revived}`).join(', ')
    );
  }

  const dueDebtHours = await getDueHourlyIngestDebtHours(QUEUE_ID, minDate, maxDate);

  const countsByKey = new Map(existing.map((row: any) => [`${row.d}|${row.hour}`, Number(row.total_matches) || 0]));
  const statesByKey = new Map(states.map(row => [`${row.date}|${row.hour}`, row]));
  const debtByKey = new Map(debtSummaries.map((row: any) => [`${row.d}|${row.hour}`, {
    terminal: Number(row.terminal_count) || 0,
    open: Number(row.open_count) || 0,
  }]));
  const nowMs = Date.now();

  const isFutureTimestamp = (value: string | null | undefined): boolean => {
    if (!value) return false;
    const ts = new Date(value).getTime();
    return Number.isFinite(ts) && ts > nowMs;
  };

  const isHandled = (date: string, hour: number): boolean => {
    const key = `${date}|${hour}`;
    const count = countsByKey.get(key) ?? 0;
    const state = statesByKey.get(key);
    const debt = debtByKey.get(key) || { terminal: 0, open: 0 };

    if (!state) return count > 0;
    if (state.status === 'complete') return true;
    if (state.raw_match_count > 0 && count >= state.raw_match_count) return true;
    if (state.raw_match_count > 0 && debt.open === 0 && count + debt.terminal >= state.raw_match_count) return true;

    if (state.status === 'empty') {
      return isFutureTimestamp(state.next_retry_at);
    }

    if (state.status === 'fetching' || state.status === 'staged') {
      return isFutureTimestamp(state.lease_until);
    }

    if (state.status === 'failed' || state.status === 'pending') {
      return isFutureTimestamp(state.next_retry_at);
    }

    return false;
  };

  const isRetryableExistingState = (row: HourlyIngestStateRow): boolean => {
    const key = `${row.date}|${row.hour}`;
    const count = countsByKey.get(key) ?? 0;
    const debt = debtByKey.get(key) || { terminal: 0, open: 0 };

    if (row.status === 'complete') return false;
    if (row.raw_match_count > 0 && count >= row.raw_match_count) return false;
    if (row.raw_match_count > 0 && debt.open === 0 && count + debt.terminal >= row.raw_match_count) return false;

    if (row.status === 'empty') {
      return !isFutureTimestamp(row.next_retry_at);
    }

    if (row.status === 'fetching' || row.status === 'staged') {
      return !isFutureTimestamp(row.lease_until);
    }

    if (row.status === 'failed' || row.status === 'pending') {
      return !isFutureTimestamp(row.next_retry_at);
    }

    return false;
  };

  const missingByKey = new Map<string, GapCandidate>();
  for (const candidate of expectedHours) {
    if (!isHandled(candidate.date, candidate.hour)) {
      missingByKey.set(`${candidate.date}|${candidate.hour}`, candidate);
    }
  }
  for (const row of states) {
    if (isRetryableExistingState(row)) {
      missingByKey.set(`${row.date}|${row.hour}`, { date: row.date, hour: row.hour });
    }
  }
  for (const row of dueDebtHours) {
    missingByKey.set(`${row.date}|${row.hour}`, { date: row.date, hour: row.hour, debtOnly: true });
  }

  return [...missingByKey.values()].sort((a, b) => {
    const dateCmp = a.date.localeCompare(b.date);
    return dateCmp !== 0 ? dateCmp : a.hour - b.hour;
  });
}

/**
 * Backfill a single missing hour.
 *
 * Thundering-herd-safe flow:
 *   1. Call discover(), which claims hourly_ingest_state before any Hi-Rez call.
 *      Fresh `fetching`, `staged`, and `empty` states suppress duplicate cron
 *      work; stale leases become retryable if the backend crashed mid-run.
 *   2. discover() fetches IDs, filters existing/staged matches, and dumps new
 *      payloads to raw_ingest_buffer.
 *   3. Buffer-drain cron processes payloads into matches/match_players and
 *      upserts hourly_match_counts.
 *   4. buffer-processor marks hourly_ingest_state complete only once processed
 *      counts catch up to the raw ID count discovered for that hour.
 *
 * A 0-match response becomes status='empty' with a scheduled recheck. The gap
 * checker no longer uses total_matches=0 as proof that an hour is done.
 *
 * Source: User report 2026-06-01 — "thundering herd race condition: gap checker
 *   re-fires every 10 min because buffer-processor hasn't finished yet."
 *
 * @param date - Date string (YYYY-MM-DD)
 * @param hour - Hour (0-23)
 */
async function backfillHour(
  date: string,
  hour: number,
  queueId = QUEUE_ID,
  debtOnly = false,
  presenceOnly = false,
): Promise<void> {
  const apiDate = date.replace(/-/g, ''); // YYYYMMDD for Hi-Rez API
  const tsLabel = `${date}T${String(hour).padStart(2, '0')}Z`;

  try {
    console.log(
      `[gap-checker] Backfilling ${tsLabel} queue=${queueId}` +
      `${debtOnly ? ' (known debt only)' : ''}...`
    );
    const newCount = presenceOnly
      ? await discoverPresenceQueue(queueId, apiDate, hour, 'gap-checker-presence-backfill')
      : await discover(queueId, apiDate, hour, { debtOnly });

    if (newCount > 0) {
      console.log(`[gap-checker] Discovered ${newCount} new matches for ${tsLabel} — buffer-processor will update stats`);
    } else {
      console.log(`[gap-checker] No new payloads for ${tsLabel} — hourly_ingest_state controls retry/empty handling`);
    }
  } catch (err) {
    console.error(`[gap-checker] Failed to backfill ${tsLabel}: ${err}`);
  }
}

/**
 * Main gap check function.
 *
 * Finds missing hours in the window and backfills them one at a time.
 * Limits backfill to MAX_BACKFILL_PER_RUN to avoid overwhelming the API.
 */
const MAX_BACKFILL_PER_RUN = Math.max(
  1,
  Number(process.env.GAP_CHECKER_MAX_BACKFILL_PER_RUN || 8),
); // Max fetch windows to backfill per run; high enough for same-day recovery, still bounded by hourly_ingest_state.

async function checkGaps(): Promise<void> {
  try {
    const rankedMissing = await findMissingHours();
    const presenceMissing = await findMissingPresenceHours();
    const missing = [...rankedMissing, ...presenceMissing];

    if (missing.length === 0) {
      return; // No gaps — nothing to do.
    }

    console.log(`[gap-checker] Found ${missing.length} missing current-day hours`);

    const headroom = await getApiHeadroomSnapshot();
    if (!headroom.hasUsableKeys) {
      // Gap-checker can find many retryable windows after a restart or backlog
      // recovery. When every relay key is at the 100-call reserve, iterating
      // those windows only asks the relay to refuse each request. That protects
      // Hi-Rez, but it pollutes logs and bumps retry state without doing useful
      // work. Pause the whole pass until the relay's usage sync revives a key.
      const quotaReason =
        `no usable Hi-Rez key headroom (usableKeys=${headroom.usableKeys}, ` +
        `usableBeforeReserve=${headroom.totalUsableBeforeReserve})`;
      await Promise.all(missing.map(candidate => recordHourlyIngestQuotaWait(
        candidate.date,
        candidate.hour,
        candidate.queueId ?? QUEUE_ID,
        'gap-checker-quota-wait',
        quotaReason,
      )));
      console.warn(
        `[gap-checker] Pausing ${missing.length} gap backfill candidate(s); ` +
        `${quotaReason}; every absent candidate is recorded as pending`
      );
      return;
    }

    const activeDetailOutage = await getActiveHirezServiceOutage(MATCH_DETAIL_SERVICE_OUTAGE_KEY);
    const detailProbeDue = activeDetailOutage
      ? isHirezServiceOutageProbeDue(activeDetailOutage)
      : false;
    if (activeDetailOutage && !detailProbeDue) {
      // Vendor detail outages are qualitatively different from a broken match:
      // every due hour would return the same upstream failure, so retrying all
      // of them every cron tick burns API calls without increasing recovery
      // odds. While the outage latch is active, the checker stays idle until
      // the next probe time. Discovery clears the latch only after a real
      // authoritative getmatchdetailsbatch row returns.
      console.warn(
        `[gap-checker] Hi-Rez detail outage active; skipping ${rankedMissing.length} ranked candidate(s) ` +
        `until next probe at ${activeDetailOutage.next_probe_at || 'now'} ` +
        `(probes=${activeDetailOutage.probe_count}, reason=${activeDetailOutage.reason || 'unknown'})`
      );
      if (presenceMissing.length === 0) return;
    }

    // Backfill up to MAX_BACKFILL_PER_RUN hours in normal mode. During a
    // service-wide detail outage, run exactly one candidate as a health probe.
    // Discovery itself stops after the first failing 10-ID detail window and
    // marks the remaining exact IDs as delayed known debt, so this pass costs
    // one detail probe instead of testing every backed-up match.
    const outageProbe = rankedMissing.find(candidate => candidate.debtOnly === true)
      || rankedMissing[0];
    const eligible = activeDetailOutage
      ? [...presenceMissing, ...(detailProbeDue && outageProbe ? [outageProbe] : [])]
      : missing;
    // Ranked debt can span many old hours. Reserve half of every healthy pass
    // for presence queues so outage gaps on the public activity report cannot
    // starve indefinitely behind ranked recovery work.
    const presenceBudget = activeDetailOutage
      ? Math.min(presenceMissing.length, MAX_BACKFILL_PER_RUN)
      : Math.min(presenceMissing.length, Math.ceil(MAX_BACKFILL_PER_RUN / 2));
    const presenceBatch = presenceMissing.slice(0, presenceBudget);
    const rankedBatch = rankedMissing.slice(0, MAX_BACKFILL_PER_RUN - presenceBatch.length);
    const toBackfill = activeDetailOutage
      ? eligible.slice(0, MAX_BACKFILL_PER_RUN)
      : [...presenceBatch, ...rankedBatch];

    if (activeDetailOutage && detailProbeDue) {
      if (outageProbe) {
        console.warn(
          `[gap-checker] Hi-Rez detail outage probe due; testing one ranked ` +
          `${outageProbe.date}T${String(outageProbe.hour).padStart(2, '0')}Z candidate`,
        );
      }
    }

    for (const { date, hour, queueId, debtOnly, presenceOnly } of toBackfill) {
      await backfillHour(
        date,
        hour,
        queueId ?? QUEUE_ID,
        debtOnly === true,
        presenceOnly === true,
      );
    }

    const remaining = missing.length - toBackfill.length;
    if (remaining > 0) {
      console.log(`[gap-checker] ${remaining} hours remaining — will backfill on next run`);
    }
  } catch (err) {
    console.error(`[gap-checker] Gap check failed: ${err}`);
  }
}

async function runGapCheck(reason: string): Promise<void> {
  await runExclusive('hourly-gap-checker:check', async () => {
    console.log(`[gap-checker] Running gap check (${reason})...`);
    await checkGaps();
  }).catch((err) => {
    console.error(`[gap-checker] Gap check failed (${reason}): ${err}`);
  });
}

/**
 * Cron jobs.
 */
export const jobs = {
  /**
   * Run gap checks several times per hour, deliberately offset from the :30
   * hourly discovery tick. More frequent checks make due match debt recover
   * quickly, while hourly_ingest_state leases, match-debt retry timestamps, and
   * worker-lock/advisory locks prevent repeated blind fetches.
   */
  check: cron.createTask(
    GAP_CHECKER_CRON_EXPRESSION,
    async () => {
      await runGapCheck('cron');
    },
  ),
};

let startupTimer: NodeJS.Timeout | null = null;

/**
 * Enable all gap checker jobs.
 */
export function enableAll() {
  jobs.check.start();
  console.log(`[gap-checker] Cron jobs enabled (gap check cron: ${GAP_CHECKER_CRON_EXPRESSION})`);

  // A missed cron edge should not wait up to an hour after backend restart.
  // This startup pass scans the durable state table and only reclaims hours
  // whose retry time or lease has expired, so it is safe to run even when a
  // normal cron tick happened shortly before the restart.
  startupTimer = setTimeout(() => {
    startupTimer = null;
    runGapCheck('startup catch-up').catch((err) => {
      console.error(`[gap-checker] Startup catch-up failed: ${err}`);
    });
  }, 20_000);
  startupTimer.unref();
}

/**
 * Disable all gap checker jobs.
 */
export function disableAll() {
  if (startupTimer) clearTimeout(startupTimer);
  startupTimer = null;
  jobs.check.stop();
  console.log('[gap-checker] Cron jobs disabled');
}

/**
 * Run a one-time gap check (no cron).
 * Used for manual invocation or testing.
 */
export async function runOnce(): Promise<void> {
  await runGapCheck('manual');
}
