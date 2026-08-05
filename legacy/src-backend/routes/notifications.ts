import { FastifyInstance } from 'fastify';
import { query } from '../config/db';
import { registerReadThroughCache } from '../utils/route-cache';

export default async function notificationsRoutes(fastify: FastifyInstance) {
  registerReadThroughCache(fastify, {
    namespace: 'route:notifications',
    shouldCache: () => true,
    ttlSeconds: () => 60,
  });

  /**
   * GET /notifications — Public homepage notification feed.
   *
   * Query params:
   *   ?limit= — Max active messages to return (default 5, max 20)
   */
  fastify.get('/', async (req: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 5, 20);

    return query(
      `SELECT id, timestamp, importance, message
       FROM notifications
       ORDER BY importance DESC, timestamp DESC, id DESC
       LIMIT $1`,
      [limit]
    );
  });
}
