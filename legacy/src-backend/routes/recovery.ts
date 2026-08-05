import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { paginate } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';

/**
 * Recovery Diagnostics Routes.
 * Exposes broken skin registry and recovery audit trail.
 * Tables: broken_skins, recovery_stats, raw_ingest_buffer.
 */
export default async function recoveryRoutes(fastify: FastifyInstance) {
  /**
   * GET /recovery/broken-skins — List known broken skin IDs
   * ?championId= filter
   */
  fastify.get('/broken-skins', async (req: any) => {
    const fb = new FilterBuilder();
    if (req.query.championId) fb.eq('champion_id', parseInt(req.query.championId, 10));

    const { clause, params } = fb.build();
    const rows = await query(`SELECT * FROM broken_skins${clause} ORDER BY champion_id, skin_id`, params);
    return rows;
  });

  /**
   * GET /recovery/broken-skins/:championId — Broken skins for a specific champion
   */
  fastify.get('/broken-skins/:championId', async (req: any, reply: any) => {
    const championId = parseInt(req.params.championId, 10);
    if (!Number.isInteger(championId)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid champion ID' } });
    }

    const rows = await query('SELECT * FROM broken_skins WHERE champion_id = $1 ORDER BY skin_id', [championId]);
    return rows;
  });

  /**
   * GET /recovery/stats — Recovery stats overview
   * ?from=, ?to=, ?page=, ?perPage=
   */
  fastify.get('/stats', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.from) fb.gte('created_at', new Date(req.query.from));
    if (req.query.to) fb.lte('created_at', new Date(req.query.to));

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM recovery_stats${clause} ORDER BY created_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /recovery/stats/:matchId — Recovery details for a specific match
   */
  fastify.get('/stats/:matchId', async (req: any, reply: any) => {
    const matchId = parseInt(req.params.matchId, 10);
    if (!Number.isInteger(matchId)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid match ID' } });
    }

    const row = await one('SELECT * FROM recovery_stats WHERE match_id = $1', [matchId]);
    if (!row) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Recovery stats not found', details: { matchId } } });
    }
    return row;
  });

  /**
   * GET /recovery/pending — Matches pending recovery (from raw_ingest_buffer)
   * Shows entries with status='pending' or status='failed' for match entities
   * ?limit=
   */
  fastify.get('/pending', async (req: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);

    const rows = await query(
      `SELECT entity_id, entity_type, status, endpoint, created_at
       FROM raw_ingest_buffer
       WHERE entity_type = 'match' AND status IN ('pending', 'failed')
       ORDER BY created_at DESC
       LIMIT $1`,
      [limit]
    );
    return rows;
  });
}
