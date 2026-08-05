export interface PresenceDetailCursor {
  date: string;
  hour: number;
  matchId: string;
  queueId: number;
}

const POSTGRES_BIGINT_MAX = 9_223_372_036_854_775_807n;
const POSTGRES_INT_MAX = 2_147_483_647;

function isValidIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const parsed = new Date(`${value}T00:00:00.000Z`);
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value;
}

export function parsePresenceDetailQueueId(value: unknown): number | null {
  if (value == null || value === '') return null;
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= 0 && parsed <= POSTGRES_INT_MAX
    ? parsed
    : null;
}

export function parsePresenceDetailLimit(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 25;
  return Math.min(50, Math.max(10, Math.trunc(parsed)));
}

export function parsePresenceEvidenceLimit(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 250;
  return Math.min(500, Math.max(50, Math.trunc(parsed)));
}

export function parsePresenceEvidencePage(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return 1;
  return Math.min(1_000_000, Math.max(1, Math.trunc(parsed)));
}

export type PresencePlayerSort = 'matches' | 'alphabetical';

export function parsePresencePlayerSort(value: unknown): PresencePlayerSort {
  return value === 'alphabetical' ? 'alphabetical' : 'matches';
}

export function decodePresenceDetailCursor(value: unknown): PresenceDetailCursor | null {
  if (value == null || value === '') return null;
  try {
    const parsed = JSON.parse(
      Buffer.from(String(value), 'base64url').toString('utf8'),
    ) as Partial<PresenceDetailCursor>;
    const date = String(parsed.date ?? '');
    const hour = Number(parsed.hour);
    const matchId = String(parsed.matchId ?? '');
    const queueId = Number(parsed.queueId);
    let matchIdValue: bigint;
    try {
      matchIdValue = BigInt(matchId);
    } catch {
      return null;
    }
    if (
      !isValidIsoDate(date)
      || !Number.isInteger(hour)
      || hour < 0
      || hour > 23
      || !/^\d+$/.test(matchId)
      || matchIdValue < 0n
      || matchIdValue > POSTGRES_BIGINT_MAX
      || parsePresenceDetailQueueId(queueId) == null
    ) {
      return null;
    }
    return { date, hour, matchId, queueId };
  } catch {
    return null;
  }
}

export function encodePresenceDetailCursor(row: {
  source_date: string | Date;
  source_hour: number;
  match_id: string | number;
  queue_id: number;
}): string {
  const date = row.source_date instanceof Date
    ? row.source_date.toISOString().slice(0, 10)
    : String(row.source_date).slice(0, 10);
  return Buffer.from(JSON.stringify({
    date,
    hour: Number(row.source_hour),
    matchId: String(row.match_id),
    queueId: Number(row.queue_id),
  })).toString('base64url');
}
