import dotenv from 'dotenv';
import path from 'path';
import { configuredApiKeyReserveCalls } from '../config/api-budget';

const envCandidates = [
  path.resolve(process.cwd(), 'env/backend.env'),
  path.resolve(process.cwd(), 'env/postgres.env'),
  path.resolve(process.cwd(), '../../env/backend.env'),
  path.resolve(process.cwd(), '../../env/postgres.env'),
  path.resolve(__dirname, '../../../env/backend.env'),
  path.resolve(__dirname, '../../../env/postgres.env'),
  path.resolve(__dirname, '../../../../env/backend.env'),
  path.resolve(__dirname, '../../../../env/postgres.env'),
];

for (const candidate of envCandidates) {
  dotenv.config({ path: candidate, override: false });
}

// The backend container uses DATABASE_URL=...@paladinscat-postgres:5432, while
// host-side desktop runs usually reach the same database through localhost:5433.
// Keep the default container URL untouched, but allow the forecast command to be
// pointed at the host mapping without editing shared env files:
//   RECOVERY_FORECAST_USE_HOST_DB=true npm run forecast:recovery
// or:
//   RECOVERY_FORECAST_DATABASE_URL=postgresql://... npm run forecast:recovery
if (process.env.RECOVERY_FORECAST_DATABASE_URL) {
  process.env.DATABASE_URL = process.env.RECOVERY_FORECAST_DATABASE_URL;
} else if (process.env.RECOVERY_FORECAST_USE_HOST_DB === 'true') {
  const user = process.env.POSTGRES_USER || 'paladins';
  const password = process.env.POSTGRES_PASSWORD || '';
  const database = process.env.POSTGRES_DB || 'paladinscat';
  const host = process.env.RECOVERY_FORECAST_DB_HOST || 'localhost';
  const port = process.env.RECOVERY_FORECAST_DB_PORT || '5433';
  process.env.DATABASE_URL = `postgresql://${encodeURIComponent(user)}:${encodeURIComponent(password)}@${host}:${port}/${database}`;
}

const QUEUE_ID = Number(process.env.RECOVERY_FORECAST_QUEUE_ID || 486);
const API_RESERVE_PER_KEY = configuredApiKeyReserveCalls();
const BATCH_SIZE = 10;
const HISTORY_CALLS_EXPECTED_PER_UNRESOLVED = Number(
  process.env.RECOVERY_FORECAST_HISTORY_CALLS_PER_UNRESOLVED || 3,
);
const HISTORY_CALLS_WORST_PER_UNRESOLVED = Number(
  process.env.RECOVERY_FORECAST_HISTORY_CALLS_WORST_PER_UNRESOLVED || 10,
);
const RECOVERY_FIXED_CALLS_PER_UNRESOLVED = Number(
  // Conservative full-corruption fixed cost: one profile roster call and one
  // non-score demo shell call. Partial recovery uses only the roster call.
  process.env.RECOVERY_FORECAST_FIXED_CALLS_PER_UNRESOLVED || 2,
);

type KeyRow = {
  dev_id: string;
  status: string;
  daily_limit: number;
  total_24h: number;
  remaining: number;
  usable_before_reserve: number;
};

type HourRow = {
  date: string;
  hour: number;
  status: string;
  raw_match_count: number;
  staged_match_count: number;
  unresolved: number;
  attempts: number;
  next_retry_at: string | null;
  error_message: string | null;
};

type EndpointRow = {
  endpoint: string;
  calls: number;
};

function ceilDiv(value: number, divisor: number): number {
  return Math.ceil(Math.max(0, value) / divisor);
}

function estimateWorstOrderedDetailCalls(matchCount: number): number {
  // Current discovery salvage treats getmatchdetailsbatch as an ordered stream:
  // a poisoned match can stop the response, so the worker accepts the healthy
  // prefix, recovers the first missing/blocking ID, removes it, and refills the
  // 10-ID window. If every unresolved match is the next blocker, the worst
  // detail-attempt count is one getmatchdetailsbatch call per unresolved ID.
  // If all are healthy, the base estimate is still ceil(N / 10).
  return Math.max(0, matchCount);
}

function estimateBaseBatchCalls(matchCount: number): number {
  return ceilDiv(matchCount, BATCH_SIZE);
}

function pct(part: number, whole: number): string {
  if (whole <= 0) return '0.0%';
  return `${((part / whole) * 100).toFixed(1)}%`;
}

async function main(): Promise<void> {
  const db = await import('../config/db.js');
  const queryDb: <T = any>(text: string, params?: any[]) => Promise<T[]> = db.query;

  try {
    const keyRows = await queryDb<KeyRow>(
    `SELECT
       dev_id,
       status,
       daily_limit,
       total_24h,
       GREATEST(daily_limit - total_24h, 0) AS remaining,
       GREATEST(daily_limit - total_24h - $1::int, 0) AS usable_before_reserve
     FROM api_keys
     ORDER BY dev_id`,
    [API_RESERVE_PER_KEY],
  );

    const hourRows = await queryDb<HourRow>(
    `SELECT
       date::text,
       hour,
       status,
       raw_match_count,
       staged_match_count,
       GREATEST(raw_match_count - staged_match_count, 0) AS unresolved,
       attempts,
       next_retry_at::text,
       error_message
     FROM hourly_ingest_state
     WHERE queue_id = $1
       AND status IN ('pending', 'fetching', 'staged', 'failed', 'empty')
       AND GREATEST(raw_match_count - staged_match_count, 0) > 0
     ORDER BY date, hour`,
    [QUEUE_ID],
  );

    const endpointRows = await queryDb<EndpointRow>(
    `SELECT endpoint, SUM(call_count)::int AS calls
     FROM api_log
     WHERE hour >= now() - interval '24 hours'
     GROUP BY endpoint
     ORDER BY calls DESC, endpoint`,
  );

    const totalLimit = keyRows.reduce((sum, row) => sum + Number(row.daily_limit || 0), 0);
    const used24h = keyRows.reduce((sum, row) => sum + Number(row.total_24h || 0), 0);
    const usableBeforeReserve = keyRows.reduce((sum, row) => sum + Number(row.usable_before_reserve || 0), 0);
    const unresolved = hourRows.reduce((sum, row) => sum + Number(row.unresolved || 0), 0);
    const raw = hourRows.reduce((sum, row) => sum + Number(row.raw_match_count || 0), 0);
    const staged = hourRows.reduce((sum, row) => sum + Number(row.staged_match_count || 0), 0);
    const hours = hourRows.length;

    const baseDetailCalls = hourRows.reduce((sum, row) => sum + estimateBaseBatchCalls(Number(row.unresolved || 0)), 0);
    const worstOrderedDetailCalls = hourRows.reduce((sum, row) => sum + estimateWorstOrderedDetailCalls(Number(row.unresolved || 0)), 0);
    const fixedRecoveryCalls = unresolved * RECOVERY_FIXED_CALLS_PER_UNRESOLVED;
    const expectedHistoryCalls = unresolved * HISTORY_CALLS_EXPECTED_PER_UNRESOLVED;
    const worstHistoryCalls = unresolved * HISTORY_CALLS_WORST_PER_UNRESOLVED;

    const orderedRecoveryBaseCost = worstOrderedDetailCalls + fixedRecoveryCalls;
    const expectedHistoryCost = orderedRecoveryBaseCost + expectedHistoryCalls;
    const worstHistoryCost = orderedRecoveryBaseCost + worstHistoryCalls;
    const peakHourRaw = hourRows.reduce((max, row) => Math.max(max, Number(row.raw_match_count || 0)), 0);
    const peakHourUnresolved = hourRows.reduce((max, row) => Math.max(max, Number(row.unresolved || 0)), 0);
    const peakHourOrderedBase = estimateWorstOrderedDetailCalls(peakHourUnresolved) + (peakHourUnresolved * RECOVERY_FIXED_CALLS_PER_UNRESOLVED);
    const peakHourExpectedHistory = peakHourOrderedBase + (peakHourUnresolved * HISTORY_CALLS_EXPECTED_PER_UNRESOLVED);
    const peakHourWorstHistory = peakHourOrderedBase + (peakHourUnresolved * HISTORY_CALLS_WORST_PER_UNRESOLVED);

    console.log('=== PaladinsCat recovery budget forecast ===');
    console.log(`Queue: ${QUEUE_ID}`);
    console.log(`API limit: ${totalLimit} daily calls`);
    console.log(`Used 24h: ${used24h}`);
    console.log(`Usable before ${API_RESERVE_PER_KEY}/key reserve: ${usableBeforeReserve}`);
    console.log('');

    console.log('Backlog pressure from hourly_ingest_state:');
    console.log(`  retryable hours: ${hours}`);
    console.log(`  raw/staged/unresolved: ${raw}/${staged}/${unresolved} (${pct(unresolved, raw)} unresolved)`);
    console.log(`  peak failed hour: raw=${peakHourRaw}, unresolved=${peakHourUnresolved}`);
    console.log('');

    console.log('Estimated calls to clear current unresolved backlog:');
    console.log(`  base detail calls if no split is needed: ${baseDetailCalls}`);
    console.log(`  worst ordered detail calls if every ID blocks: ${worstOrderedDetailCalls}`);
    console.log(`  fixed recovery calls (+${RECOVERY_FIXED_CALLS_PER_UNRESOLVED}/match): ${fixedRecoveryCalls}`);
    console.log(`  ordered recovery base total: ${orderedRecoveryBaseCost} (${pct(orderedRecoveryBaseCost, usableBeforeReserve)} of usable headroom)`);
    console.log(`  expected history-assisted total (+${HISTORY_CALLS_EXPECTED_PER_UNRESOLVED}/match): ${expectedHistoryCost} (${pct(expectedHistoryCost, usableBeforeReserve)} of usable headroom)`);
    console.log(`  worst history-assisted total (+${HISTORY_CALLS_WORST_PER_UNRESOLVED}/match): ${worstHistoryCost} (${pct(worstHistoryCost, usableBeforeReserve)} of usable headroom)`);
    console.log('');

    console.log('Cold-start vs steady-state interpretation:');
    console.log('  current backlog estimate is a one-time catch-up cost; it should not be treated as the normal daily burn');
    console.log('  Hi-Rez and local counters use rolling windows, so calls spent during catch-up fade out naturally over the next 24h/1h windows');
    console.log('  steady-state should look closer to one new hourly window plus that hour\'s broken-match recovery, not the full backlog total');
    console.log('');

    console.log('Estimated calls for one peak unresolved hour:');
    console.log(`  ordered recovery base: ${peakHourOrderedBase}`);
    console.log(`  expected history-assisted: ${peakHourExpectedHistory}`);
    console.log(`  worst history-assisted: ${peakHourWorstHistory}`);
    console.log('');

    console.log('Per-key headroom:');
    for (const row of keyRows) {
      console.log(
        `  ${row.dev_id}: status=${row.status}, used=${row.total_24h}/${row.daily_limit}, ` +
        `remaining=${row.remaining}, usable_before_reserve=${row.usable_before_reserve}`,
      );
    }
    console.log('');

    console.log('Top endpoints in last 24h:');
    for (const row of endpointRows.slice(0, 12)) {
      console.log(`  ${row.endpoint}: ${row.calls}`);
    }
  } finally {
    await db.shutdown();
  }
}

main()
  .catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
