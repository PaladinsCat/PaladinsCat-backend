const RANKED_QUEUE_ID = 486;

export const RANKED_PRIORITY_MAX_MATCHES_PER_HOUR = Math.max(
  1,
  Number(process.env.RANKED_PRIORITY_MAX_MATCHES_PER_HOUR || 75),
);
export const RANKED_PRIORITY_CALLS_PER_MATCH = Math.max(
  1,
  Number(process.env.RANKED_PRIORITY_CALLS_PER_MATCH || 13),
);
export const RANKED_PRIORITY_DISCOVERY_CALLS = Math.max(
  1,
  Number(process.env.RANKED_PRIORITY_DISCOVERY_CALLS || 1),
);
export const RANKED_PRIORITY_PEAK_LOOKBACK_DAYS = Math.max(
  1,
  Number(process.env.RANKED_PRIORITY_PEAK_LOOKBACK_DAYS || 30),
);

export type RankedPriorityReserveSnapshot = {
  dueRankedMatches: number;
  configuredHourlyFloor: number;
  observedHourlyPeak: number;
  protectedRankedMatches: number;
  callsPerMatch: number;
  discoveryCalls: number;
  reservedCalls: number;
};

type RankedPriorityReserveInput = {
  dueRankedMatches: number;
  observedHourlyPeak?: number;
  maxMatchesPerHour?: number;
  callsPerMatch?: number;
  discoveryCalls?: number;
};

type BackgroundMatchAllowanceInput = {
  usableCalls: number;
  rankedPriorityReserveCalls: number;
  worstCaseCallsPerMatch: number;
};

function nonNegativeInteger(value: number): number {
  return Number.isFinite(value) ? Math.max(0, Math.floor(value)) : 0;
}

/**
 * Protect the next peak ranked hour plus any larger known ranked recovery debt.
 *
 * Production's observed hourly maximum is 75 ranked matches. The pipeline's
 * documented cold-recovery ceiling is 13 Hi-Rez calls per match:
 * one ordered detail attempt, two recovery setup calls, and ten histories.
 * One additional call is reserved for getmatchidsbyqueue.
 */
export function calculateRankedPriorityReserveCalls(
  input: RankedPriorityReserveInput,
): RankedPriorityReserveSnapshot {
  const dueRankedMatches = nonNegativeInteger(input.dueRankedMatches);
  const observedHourlyPeak = nonNegativeInteger(
    input.observedHourlyPeak ?? 0,
  );
  const maxMatchesPerHour = Math.max(
    1,
    nonNegativeInteger(
      input.maxMatchesPerHour ?? RANKED_PRIORITY_MAX_MATCHES_PER_HOUR,
    ),
  );
  const callsPerMatch = Math.max(
    1,
    nonNegativeInteger(
      input.callsPerMatch ?? RANKED_PRIORITY_CALLS_PER_MATCH,
    ),
  );
  const discoveryCalls = Math.max(
    1,
    nonNegativeInteger(
      input.discoveryCalls ?? RANKED_PRIORITY_DISCOVERY_CALLS,
    ),
  );
  const protectedRankedMatches = Math.max(
    maxMatchesPerHour,
    observedHourlyPeak,
    dueRankedMatches,
  );

  return {
    dueRankedMatches,
    configuredHourlyFloor: maxMatchesPerHour,
    observedHourlyPeak,
    protectedRankedMatches,
    callsPerMatch,
    discoveryCalls,
    reservedCalls: discoveryCalls + protectedRankedMatches * callsPerMatch,
  };
}

export function calculateBackgroundMatchAllowance(
  input: BackgroundMatchAllowanceInput,
): number {
  const spendableCalls = Math.max(
    0,
    nonNegativeInteger(input.usableCalls)
      - nonNegativeInteger(input.rankedPriorityReserveCalls),
  );
  const worstCaseCallsPerMatch = Math.max(
    1,
    nonNegativeInteger(input.worstCaseCallsPerMatch),
  );
  return Math.floor(spendableCalls / worstCaseCallsPerMatch);
}

export async function getRankedPriorityReserveSnapshot(): Promise<RankedPriorityReserveSnapshot> {
  // Keep the pure reserve calculator importable by policy tests and operator
  // tools that do not load a database environment. Runtime acquisition calls
  // this function only after backend configuration has initialized.
  const { one } = await import('../config/db.js');
  const row = await one<{
    pending_ranked_matches: string | number;
    observed_hourly_peak: string | number;
  }>(
    `SELECT
       (
         SELECT COUNT(*)
         FROM hourly_ingest_match_debt
         WHERE queue_id = $1
           AND status = 'pending'
       ) AS pending_ranked_matches,
       (
         SELECT COALESCE(MAX(raw_match_count), 0)
         FROM hourly_ingest_state
         WHERE queue_id = $1
           AND date >= current_date - ($2::int * interval '1 day')
       ) AS observed_hourly_peak`,
    [RANKED_QUEUE_ID, RANKED_PRIORITY_PEAK_LOOKBACK_DAYS],
  );

  return calculateRankedPriorityReserveCalls({
    dueRankedMatches: Number(row?.pending_ranked_matches || 0),
    observedHourlyPeak: Number(row?.observed_hourly_peak || 0),
  });
}
