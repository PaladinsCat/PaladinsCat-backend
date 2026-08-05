import { roundTo2 } from '../services/normalizer';

export interface CreditRates {
  cpm: number;
  ecpm: number;
}

export const SIEGE_STARTING_CREDITS = 500;

/** Accept only actual gameplay time; the API player timer is never a metric denominator. */
export function resolveGameplayDuration(matchDurationSeconds: unknown): number {
  const matchSeconds = Number(matchDurationSeconds);
  if (Number.isFinite(matchSeconds) && matchSeconds > 0) return matchSeconds;
  return 0;
}

export function calculatePerMinute(value: unknown, durationSeconds: unknown): number {
  const amount = Number(value);
  const seconds = Number(durationSeconds);
  if (!Number.isFinite(amount) || !Number.isFinite(seconds) || seconds <= 0) return 0;
  return roundTo2(amount * 60 / seconds);
}

/** Derive canonical Siege credit rates from credits earned during gameplay. */
export function calculateCreditRates(credits: unknown, durationSeconds: unknown): CreditRates {
  const earned = Number(credits);
  const seconds = Number(durationSeconds);
  if (!Number.isFinite(earned) || !Number.isFinite(seconds) || seconds <= 0) {
    return { cpm: 0, ecpm: 0 };
  }
  return {
    cpm: calculatePerMinute(earned, seconds),
    ecpm: calculatePerMinute(earned - SIEGE_STARTING_CREDITS, seconds),
  };
}

/**
 * Conservative automatic AFK severity derived from gameplay-time eCPM.
 * 70–119 eCPM stays review-only. Values below 70 sit at or below the
 * roughly 60 eCPM passive-credit floor and are the only automatic signal.
 */
export function calculateAfkRate(credits: unknown, durationSeconds: unknown): number {
  const seconds = Number(durationSeconds);
  if (!Number.isFinite(seconds) || seconds <= 0) return 0;
  const { ecpm } = calculateCreditRates(credits, durationSeconds);
  return ecpm >= 70 ? 0 : 3;
}
