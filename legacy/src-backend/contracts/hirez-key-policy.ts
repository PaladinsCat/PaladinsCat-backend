import { configuredApiKeyReserveCalls } from '../config/api-budget';

/**
 * Shared Hi-Rez key-budget policy.
 *
 * This module is intentionally transport-neutral. Public/admin routes may read
 * these values without importing the legacy TypeScript relay key pool.
 */
export const BUDGET_THRESHOLD = configuredApiKeyReserveCalls();
export const DEFAULT_DAILY_LIMIT = 7500;
export const SPECIAL_DAILY_LIMITS: Record<string, number> = {
  '2116': 15000,
};

export function configuredDailyLimit(devId: string): number {
  return SPECIAL_DAILY_LIMITS[String(devId)] ?? DEFAULT_DAILY_LIMIT;
}

export function effectiveDailyLimit(devId: string, reportedLimit?: number | null): number {
  const configured = configuredDailyLimit(devId);
  const serverLimit = Number(reportedLimit);
  if (Number.isFinite(serverLimit) && serverLimit > 0) {
    return Math.min(serverLimit, configured);
  }
  return configured;
}
