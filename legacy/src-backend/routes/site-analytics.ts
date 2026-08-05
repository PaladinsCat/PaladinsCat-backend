import { FastifyInstance } from 'fastify';
import { query } from '../config/db';
import {
  isValidAnonymousVisitorId,
  touchAnonymousLiveSession,
} from '../services/site-live-sessions';

function normalizePublicPath(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const path = value.trim().split(/[?#]/, 1)[0];
  if (!path.startsWith('/') || path.length > 200) return null;
  if (path === '/admin' || path.startsWith('/admin/') || path === '/auth' || path.startsWith('/auth/')) return null;
  return (path.replace(/\/+/g, '/').replace(/\/\d{4,}(?=\/|$)/g, '/[id]') || '/').slice(0, 200);
}

export default async function siteAnalyticsRoutes(fastify: FastifyInstance) {
  fastify.post('/visit', async (req: any, reply: any) => {
    const visitorId = typeof req.body?.visitorId === 'string' ? req.body.visitorId.trim() : '';
    const path = normalizePublicPath(req.body?.path);
    if (!path || !isValidAnonymousVisitorId(visitorId)) {
      return reply.status(204).send();
    }

    const visitDate = new Date().toISOString().slice(0, 10);
    await Promise.all([
      touchAnonymousLiveSession(visitorId, true),
      query(
        `INSERT INTO site_daily_page_views (visit_date, path, page_views, updated_at)
         VALUES ($2::DATE, $1, 1, now())
         ON CONFLICT (visit_date, path) DO UPDATE SET
           page_views = site_daily_page_views.page_views + 1,
           updated_at = now()`,
        [path, visitDate],
      ),
    ]);

    return reply.status(204).send();
  });

  fastify.post('/heartbeat', async (req: any, reply: any) => {
    const visitorId = typeof req.body?.visitorId === 'string' ? req.body.visitorId.trim() : '';
    if (!isValidAnonymousVisitorId(visitorId)) {
      return reply.status(204).send();
    }

    reply.header('Cache-Control', 'private, no-store');
    await touchAnonymousLiveSession(visitorId, false);
    return reply.status(204).send();
  });
}
