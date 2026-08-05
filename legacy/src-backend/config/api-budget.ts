export const DEFAULT_API_KEY_RESERVE_CALLS = 100;

export function configuredApiKeyReserveCalls(
  value: string | number | undefined = process.env.API_KEY_RESERVE_CALLS,
): number {
  const configured = Number(value ?? DEFAULT_API_KEY_RESERVE_CALLS);
  return Number.isFinite(configured) && configured >= 0
    ? Math.floor(configured)
    : DEFAULT_API_KEY_RESERVE_CALLS;
}
