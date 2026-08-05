import { FastifyInstance } from 'fastify';
import { query } from '../config/db';

export async function rawApiResponsesRoutes(fastify: FastifyInstance) {
  /**
   * Durable raw Hi-Rez pass-through audit.
   *
   * `/api/raw-responses` below reads raw_ingest_buffer, which is intentionally
   * pruned after the buffer worker drains rows. These `/api/hirez-raw-responses`
   * routes read hirez_raw_api_responses instead. That table is for operator
   * evidence: raw pass-through endpoints write here before returning payloads.
   */
  fastify.get('/hirez-raw-responses', {
    schema: {
      description: 'Get durable raw Hi-Rez pass-through responses',
      tags: ['raw-api'],
      querystring: {
        type: 'object',
        properties: {
          endpoint: { type: 'string', description: 'Filter by endpoint name' },
          entityType: { type: 'string', description: 'Filter by entity type' },
          entityId: { type: 'string', description: 'Filter by entity id' },
          limit: { type: 'number', default: 50, description: 'Number of results' },
          includeText: { type: 'string', description: 'Set true to include raw_response_text' },
        },
      },
    },
    handler: async (request, reply) => {
      const q = request.query as any;
      const limit = Math.min(parseInt(String(q.limit ?? '50'), 10) || 50, 500);
      const includeText = q.includeText === 'true';
      const params: any[] = [];
      const conditions: string[] = [];

      if (q.endpoint) {
        conditions.push(`endpoint = $${params.length + 1}`);
        params.push(String(q.endpoint));
      }
      if (q.entityType) {
        conditions.push(`entity_type = $${params.length + 1}`);
        params.push(String(q.entityType));
      }
      if (q.entityId) {
        conditions.push(`entity_id = $${params.length + 1}`);
        params.push(String(q.entityId));
      }

      const textColumn = includeText ? ', raw_response_text' : '';
      let sql = `
        SELECT id, endpoint, operation, entity_type, entity_id, params,
               raw_response${textColumn}, response_sha256, response_shape,
               response_count, status_code, success, error_message, source,
               created_at
        FROM hirez_raw_api_responses
      `;
      if (conditions.length > 0) sql += ` WHERE ${conditions.join(' AND ')}`;
      sql += ` ORDER BY created_at DESC LIMIT $${params.length + 1}`;
      params.push(limit);

      const results = await query(sql, params);
      return reply.code(200).send({ data: results, count: results.length });
    },
  });

  fastify.get('/hirez-raw-responses/stats', async (_request: any, reply: any) => {
    const stats = await query(`
      SELECT
        endpoint,
        operation,
        entity_type,
        COUNT(*)::INT AS total_requests,
        COUNT(*) FILTER (WHERE success)::INT AS success_count,
        COUNT(*) FILTER (WHERE NOT success)::INT AS error_count,
        SUM(COALESCE(response_count, 0))::INT AS total_response_items,
        MIN(created_at) AS first_request,
        MAX(created_at) AS last_request
      FROM hirez_raw_api_responses
      GROUP BY endpoint, operation, entity_type
      ORDER BY total_requests DESC, endpoint ASC
    `);
    return reply.code(200).send({ data: stats });
  });

  fastify.get('/hirez-raw-responses/:id', {
    schema: {
      description: 'Get a durable raw Hi-Rez pass-through response by audit ID',
      tags: ['raw-api'],
      params: {
        type: 'object',
        properties: { id: { type: 'number' } },
      },
      querystring: {
        type: 'object',
        properties: {
          includeText: { type: 'string', description: 'Set true to include raw_response_text' },
        },
      },
    },
    handler: async (request, reply) => {
      const { id } = request.params as any;
      const includeText = (request.query as any).includeText === 'true';
      const textColumn = includeText ? ', raw_response_text' : '';
      const result = await query(`
        SELECT id, endpoint, operation, entity_type, entity_id, params,
               raw_response${textColumn}, response_sha256, response_shape,
               response_count, status_code, success, error_message, source,
               created_at
        FROM hirez_raw_api_responses
        WHERE id = $1
      `, [id]);
      if (result.length === 0) {
        return reply.code(404).send({ error: 'Not found' });
      }
      return reply.code(200).send(result[0]);
    },
  });

  // Get recent raw API responses
  fastify.get('/raw-responses', {
    schema: {
      description: 'Get recent raw API responses',
      tags: ['raw-api'],
      querystring: {
        type: 'object',
        properties: {
          endpoint: { type: 'string', description: 'Filter by endpoint name' },
          limit: { type: 'number', default: 50, description: 'Number of results' },
        },
      },
    },
    handler: async (request, reply) => {
      const { endpoint, limit = 50 } = request.query as any;
      let sql = `SELECT id, endpoint, params, raw_data as raw_response, status_code, session_id, response_time_ms, error_message, created_at FROM raw_ingest_buffer`;
      const params: any[] = [];
      const conditions: string[] = [];

      if (endpoint) {
        conditions.push(`endpoint = $${params.length + 1}`);
        params.push(endpoint);
      }

      if (conditions.length > 0) {
        sql += ` WHERE ${conditions.join(' AND ')}`;
      }

      sql += ` ORDER BY created_at DESC LIMIT $${params.length + 1}`;
      params.push(limit);

      const results = await query(sql, params);
      return reply.code(200).send({ data: results, count: results.length });
    },
  });

  // Get a single raw API response by ID
  fastify.get('/raw-responses/:id', {
    schema: {
      description: 'Get a single raw API response by ID',
      tags: ['raw-api'],
      params: {
        type: 'object',
        properties: {
          id: { type: 'number' },
        },
      },
    },
    handler: async (request, reply) => {
      const { id } = request.params as any;
      const result = await query(`SELECT id, endpoint, params, raw_data as raw_response, status_code, session_id, response_time_ms, error_message, created_at FROM raw_ingest_buffer WHERE id = $1`, [id]);
      if (result.length === 0) {
        return reply.code(404).send({ error: 'Not found' });
      }
      return reply.code(200).send(result[0]);
    },
  });

  // Get raw API response statistics
  fastify.get('/raw-responses/stats', {
    schema: {
      response: {
        200: { type: 'object', properties: { data: { type: 'array' } } },
      },
    },
    handler: async (request, reply) => {
      const stats = await query(`
        SELECT 
          endpoint,
          COUNT(*) as total_requests,
          COUNT(CASE WHEN status_code = 200 THEN 1 END) as success_count,
          COUNT(CASE WHEN status_code != 200 THEN 1 END) as error_count,
          AVG(response_time_ms) as avg_response_time_ms,
          MIN(created_at) as first_request,
          MAX(created_at) as last_request
        FROM raw_ingest_buffer
        GROUP BY endpoint
        ORDER BY total_requests DESC
      `);
      return reply.code(200).send({ data: stats });
    },
  });

  // Get raw responses for a specific match (via match_id in params)
  fastify.get('/raw-responses/match/:matchId', {
    schema: {
      description: 'Get raw API responses for a specific match',
      tags: ['raw-api'],
      params: {
        type: 'object',
        properties: {
          matchId: { type: 'number' },
        },
      },
    },
    handler: async (request, reply) => {
      const { matchId } = request.params as any;
      // Search for match_id in the params field (JSONB)
      const results = await query(`
        SELECT id, endpoint, params, raw_data as raw_response, status_code, session_id, response_time_ms, error_message, created_at
        FROM raw_ingest_buffer
        WHERE endpoint = 'getmatchdetailsbatch' AND params::text LIKE $1
        ORDER BY created_at DESC
      `, [`"${matchId}"`]);
      return reply.code(200).send({ data: results, count: results.length });
    },
  });
}
