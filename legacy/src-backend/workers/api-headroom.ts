import { one } from '../config/db';
import { configuredApiKeyReserveCalls } from '../config/api-budget';

const API_RESERVE_PER_KEY = configuredApiKeyReserveCalls();

export type ApiHeadroomSnapshot = {
  totalKeys: number;
  usableKeys: number;
  totalUsableBeforeReserve: number;
  hasUsableKeys: boolean;
};

/**
 * Read relay-owned API-key budget state before a worker starts a Hi-Rez fetch.
 *
 * The relay is the only component that should choose keys, create sessions, and
 * increment usage. Backend workers still need a cheap "should I even ask the
 * relay right now?" check. Without it, a gap-check pass can loop through many
 * retryable hours while every key is already at the 100-call reserve. The relay
 * correctly refuses before leaving the process, so it does not burn Hi-Rez
 * calls, but the backend still creates noisy failed attempts and updates retry
 * state.
 *
 * `totalKeys === 0` is treated as usable so dummy/test databases without seeded
 * API keys do not accidentally disable ingest tests. Real mode has seeded rows
 * in `api_keys`, and then at least one key must be above the reserve.
 */
export async function getApiHeadroomSnapshot(): Promise<ApiHeadroomSnapshot> {
  const row = await one<{
    total_keys: string | number;
    usable_keys: string | number;
    total_usable_before_reserve: string | number | null;
  }>(
    `SELECT
       COUNT(*) AS total_keys,
       COUNT(*) FILTER (
         WHERE status NOT IN ('limited', 'unhealthy', 'exhausted')
           AND GREATEST(daily_limit - total_24h, 0) > $1::int
       ) AS usable_keys,
       COALESCE(SUM(GREATEST(daily_limit - total_24h - $1::int, 0)), 0) AS total_usable_before_reserve
     FROM api_keys`,
    [API_RESERVE_PER_KEY],
  );

  const totalKeys = Number(row?.total_keys || 0);
  const usableKeys = Number(row?.usable_keys || 0);
  const totalUsableBeforeReserve = Number(row?.total_usable_before_reserve || 0);

  return {
    totalKeys,
    usableKeys,
    totalUsableBeforeReserve,
    hasUsableKeys: totalKeys === 0 || usableKeys > 0,
  };
}
