import { one, query } from '../config/db';

export const MATCH_DETAIL_SERVICE_OUTAGE_KEY = 'match_detail_server_regions';

export type HirezServiceOutageClassification = {
  serviceKey: string;
  code: string;
  title: string;
  severity: 'critical' | 'warning';
  reason: string;
  publicMessage: string;
};

const OUTAGE_SIGNATURES: Array<{
  pattern: RegExp;
  classify: (message: string) => HirezServiceOutageClassification;
}> = [
  {
    pattern: /Server_Regions|##sql_paladins_api|Invalid object name/i,
    classify: (message) => ({
      serviceKey: MATCH_DETAIL_SERVICE_OUTAGE_KEY,
      code: 'HIREZ_DETAIL_SERVER_REGIONS',
      title: 'Hi-Rez match detail outage',
      severity: 'critical',
      reason: message,
      publicMessage:
        'Hi-Rez match detail endpoints are returning a server-side error. Ranked match backfill is being held to one safe probe until service recovers.',
    }),
  },
  {
    pattern: /maintenance|temporarily unavailable|service unavailable|API temporarily unavailable|HTTP 503/i,
    classify: (message) => ({
      serviceKey: 'hirez_api_service_unavailable',
      code: 'HIREZ_SERVICE_UNAVAILABLE',
      title: 'Hi-Rez API service degraded',
      severity: 'warning',
      reason: message,
      publicMessage:
        'Hi-Rez API is reporting temporary service issues. Live lookups may be delayed while PaladinsCat keeps using local data.',
    }),
  },
];

const DEFAULT_DETAIL_OUTAGE_PROBE_MINUTES = Math.max(
  15,
  Number(process.env.HIREZ_DETAIL_OUTAGE_PROBE_MINUTES || 45),
);

export type HirezServiceOutageState = {
  service_key: string;
  status: 'active' | 'recovered';
  reason: string | null;
  first_detected_at: string | null;
  last_detected_at: string | null;
  next_probe_at: string | null;
  probe_count: number;
  last_success_at: string | null;
  updated_at: string | null;
};

let outageTableReady = false;

export function classifyHirezServiceOutageMessage(
  value: unknown,
): HirezServiceOutageClassification | null {
  const message = value instanceof Error ? value.message : String(value || '');
  if (!message.trim()) return null;
  const match = OUTAGE_SIGNATURES.find(signature => signature.pattern.test(message));
  return match ? match.classify(message) : null;
}

/**
 * Durable upstream-outage latch for Hi-Rez service failures.
 *
 * This is intentionally separate from hourly_ingest_state and
 * hourly_ingest_match_debt. Those tables answer "which exact match/hour still
 * needs work?" This table answers "is the upstream detail service currently
 * broken in a way that makes every exact retry wasteful?"
 *
 * Example: Hi-Rez can return `Invalid object name
 * ##sql_paladins_api_Server_Regions` for getmatchdetailsbatch/getmatchdetails.
 * That is not a broken match. It is a vendor-side detail service outage. During
 * that period, trying every pending hour every cron tick would spend calls and
 * only re-record the same pending debt. The gap checker reads this latch and
 * probes just one due batch at a slow interval until the detail path returns
 * authoritative rows again.
 */
export async function ensureHirezServiceOutageTable(): Promise<void> {
  if (outageTableReady) return;

  await one(`
    CREATE TABLE IF NOT EXISTS hirez_service_outage_state (
      service_key TEXT PRIMARY KEY,
      status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'recovered')),
      reason TEXT,
      first_detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_detected_at TIMESTAMPTZ,
      next_probe_at TIMESTAMPTZ,
      probe_count INT NOT NULL DEFAULT 0,
      last_success_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await one(`
    CREATE INDEX IF NOT EXISTS idx_hirez_service_outage_active_probe
      ON hirez_service_outage_state (status, next_probe_at)
      WHERE status = 'active'`);

  outageTableReady = true;
}

export async function recordHirezServiceOutage(
  serviceKey: string,
  reason: string,
  retryMinutes = DEFAULT_DETAIL_OUTAGE_PROBE_MINUTES,
): Promise<void> {
  await ensureHirezServiceOutageTable();

  await one(
    `INSERT INTO hirez_service_outage_state (
       service_key, status, reason, first_detected_at, last_detected_at,
       next_probe_at, probe_count, updated_at
     )
     VALUES (
       $1, 'active', $2, now(), now(),
       now() + ($3::int * interval '1 minute'), 1, now()
     )
     ON CONFLICT (service_key) DO UPDATE
     SET status = 'active',
         reason = EXCLUDED.reason,
         last_detected_at = now(),
         next_probe_at = now() + ($3::int * interval '1 minute'),
         probe_count = hirez_service_outage_state.probe_count + 1,
         updated_at = now()`,
    [serviceKey, reason, retryMinutes],
  );
}

export async function markHirezServiceRecovered(
  serviceKey: string,
  reason = 'authoritative detail response returned',
): Promise<void> {
  await ensureHirezServiceOutageTable();

  await one(
    `UPDATE hirez_service_outage_state
     SET status = 'recovered',
         reason = $2,
         next_probe_at = NULL,
         last_success_at = now(),
         updated_at = now()
     WHERE service_key = $1
       AND status = 'active'`,
    [serviceKey, reason],
  );
}

export async function getActiveHirezServiceOutage(
  serviceKey: string,
): Promise<HirezServiceOutageState | null> {
  await ensureHirezServiceOutageTable();

  const rows = await query<HirezServiceOutageState>(
    `SELECT service_key, status, reason,
            first_detected_at::text,
            last_detected_at::text,
            next_probe_at::text,
            probe_count,
            last_success_at::text,
            updated_at::text
     FROM hirez_service_outage_state
     WHERE service_key = $1
       AND status = 'active'
     LIMIT 1`,
    [serviceKey],
  );

  if (rows.length === 0) return null;
  return {
    ...rows[0],
    probe_count: Number(rows[0].probe_count) || 0,
  };
}

export async function getActiveHirezServiceOutages(): Promise<HirezServiceOutageState[]> {
  await ensureHirezServiceOutageTable();

  const rows = await query<HirezServiceOutageState>(
    `SELECT service_key, status, reason,
            first_detected_at::text,
            last_detected_at::text,
            next_probe_at::text,
            probe_count,
            last_success_at::text,
            updated_at::text
     FROM hirez_service_outage_state
     WHERE status = 'active'
     ORDER BY
       CASE service_key WHEN $1 THEN 0 ELSE 1 END,
       updated_at DESC`,
    [MATCH_DETAIL_SERVICE_OUTAGE_KEY],
  );

  return rows.map(row => ({
    ...row,
    probe_count: Number(row.probe_count) || 0,
  }));
}

export function isHirezServiceOutageProbeDue(
  outage: HirezServiceOutageState | null | undefined,
): boolean {
  if (!outage) return false;
  if (!outage.next_probe_at) return true;
  const nextProbeAt = new Date(outage.next_probe_at).getTime();
  return !Number.isFinite(nextProbeAt) || nextProbeAt <= Date.now();
}

