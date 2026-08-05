import crypto from 'node:crypto';

export const ACTIVE_USER_WINDOW_SECONDS = 5 * 60;
export const LIVE_SESSION_HEARTBEAT_SECONDS = 60;

const VISITOR_ID_PATTERN = /^[A-Za-z0-9_-]{16,128}$/;

export function isValidAnonymousVisitorId(value: unknown): value is string {
  return typeof value === 'string' && VISITOR_ID_PATTERN.test(value.trim());
}

export function anonymousVisitorIdentity(
  visitorId: string,
  visitDate = new Date().toISOString().slice(0, 10),
  salt = process.env.ANALYTICS_SALT || process.env.ADMIN_SECRET || 'paladinscat-anonymous-analytics',
): { visitorHash: string; visitDate: string } {
  return {
    visitorHash: crypto.createHash('sha256').update(`${visitDate}:${salt}:${visitorId.trim()}`).digest('hex'),
    visitDate,
  };
}
