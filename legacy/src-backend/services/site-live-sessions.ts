import { one, query } from '../config/db';
import {
  ACTIVE_USER_WINDOW_SECONDS,
  LIVE_SESSION_HEARTBEAT_SECONDS,
  anonymousVisitorIdentity,
} from './site-live-session-policy';

export { isValidAnonymousVisitorId } from './site-live-session-policy';

export async function touchAnonymousLiveSession(
  visitorId: string,
  incrementPageView: boolean,
): Promise<void> {
  const { visitorHash, visitDate } = anonymousVisitorIdentity(visitorId);
  const pageViewIncrement = incrementPageView ? 1 : 0;
  await query(
    `INSERT INTO site_daily_visitors (visit_date, visitor_hash, page_views, first_seen, last_seen)
     VALUES ($2::DATE, $1, $3, now(), now())
     ON CONFLICT (visit_date, visitor_hash) DO UPDATE SET
       page_views = site_daily_visitors.page_views + $3,
       last_seen = now()`,
    [visitorHash, visitDate, pageViewIncrement],
  );
}

export interface ActiveUserSnapshot {
  active_users: number;
  active_window_seconds: number;
  heartbeat_seconds: number;
}

export async function getActiveUserSnapshot(): Promise<ActiveUserSnapshot> {
  const row = await one<{ active_users: number }>(
    `SELECT COUNT(*)::INT AS active_users
     FROM site_daily_visitors
     WHERE visit_date = (now() AT TIME ZONE 'UTC')::DATE
       AND last_seen >= now() - make_interval(secs => $1::INT)`,
    [ACTIVE_USER_WINDOW_SECONDS],
  );
  return {
    active_users: Number(row?.active_users ?? 0),
    active_window_seconds: ACTIVE_USER_WINDOW_SECONDS,
    heartbeat_seconds: LIVE_SESSION_HEARTBEAT_SECONDS,
  };
}
