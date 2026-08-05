import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { requireAdminSession } from '../utils/query-helpers';
import { invalidateRouteCache } from '../utils/route-cache';

function normalizeNotificationBody(body: any, partial = false) {
  const values: Record<string, any> = {};

  if (!partial || body.message !== undefined) {
    const message = String(body.message ?? '').trim();
    if (!message) throw new Error('message is required');
    if (message.length > 500) throw new Error('message must be 500 characters or fewer');
    values.message = message;
  }

  if (!partial || body.importance !== undefined) {
    const importance = parseInt(String(body.importance ?? 0), 10);
    if (!Number.isInteger(importance)) throw new Error('importance must be an integer');
    values.importance = importance;
  }

  if (!partial || body.timestamp !== undefined) {
    const timestamp = body.timestamp ?? null;
    values.timestamp = timestamp ? new Date(timestamp) : new Date();
    if (Number.isNaN(values.timestamp.getTime())) throw new Error('timestamp must be a valid date');
  }

  return values;
}

/**
 * Admin Notification Routes (session-based auth).
 * All endpoints require a valid user session with is_admin = true.
 * Tables: notifications.
 */
export default async function adminNotificationRoutes(fastify: FastifyInstance) {
  // Session-based admin guard for all routes
  fastify.addHook('preHandler', async (req: any, reply: any) => {
    try {
      await requireAdminSession(req);
    } catch {
      return reply.status(401).send({ error: { code: 'UNAUTHORIZED', message: 'Admin access required' } });
    }
  });

  /**
   * GET /admin/notifications — Admin-managed homepage notifications.
   */
  fastify.get('/notifications', async () => {
    const rows = await query(
      `SELECT id, timestamp, importance, message
       FROM notifications
       ORDER BY importance DESC, timestamp DESC, id DESC
       LIMIT 100`
    );
    return rows;
  });

  /**
   * POST /admin/notifications — Create a homepage notification.
   */
  fastify.post('/notifications', async (req: any, reply: any) => {
    let values: Record<string, any>;
    try {
      values = normalizeNotificationBody(req.body ?? {});
    } catch (error: any) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: error.message } });
    }

    const row = await one(
      `INSERT INTO notifications (timestamp, importance, message)
       VALUES ($1, $2, $3)
       RETURNING id, timestamp, importance, message`,
      [values.timestamp, values.importance, values.message]
    );

    await invalidateRouteCache('route:notifications');
    return reply.status(201).send(row);
  });

  /**
   * PUT /admin/notifications/:id — Update a homepage notification.
   */
  fastify.put('/notifications/:id', async (req: any, reply: any) => {
    const id = parseInt(req.params.id as string, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid notification id' } });
    }

    let values: Record<string, any>;
    try {
      values = normalizeNotificationBody(req.body ?? {}, true);
    } catch (error: any) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: error.message } });
    }

    const entries = Object.entries(values);
    if (entries.length === 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'No fields to update' } });
    }

    const assignments = entries.map(([key], index) => `${key} = $${index + 2}`);
    const row = await one(
      `UPDATE notifications
       SET ${assignments.join(', ')}
       WHERE id = $1
       RETURNING id, timestamp, importance, message`,
      [id, ...entries.map(([, value]) => value)]
    );

    if (!row) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Notification not found' } });
    }
    await invalidateRouteCache('route:notifications');
    return row;
  });

  /**
   * DELETE /admin/notifications/:id — Delete a homepage notification.
   */
  fastify.delete('/notifications/:id', async (req: any, reply: any) => {
    const id = parseInt(req.params.id as string, 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid notification id' } });
    }

    const row = await one(
      `DELETE FROM notifications
       WHERE id = $1
       RETURNING id`,
      [id]
    );
    if (!row) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Notification not found' } });
    }
    await invalidateRouteCache('route:notifications');
    return { deleted: true, id };
  });
}
