import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { paginate, parseQuery, sorting, DISPLAY_NAME_SQL } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';

const RATING_CACHE_TTL = 300;

export default async function ratingsRoutes(fastify: FastifyInstance) {
  /**
   * GET /ratings/queue/:playerId — Player queue ratings (player_queue_ratings)
   */
  fastify.get('/queue/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const fb = new FilterBuilder().eq('player_id', playerId);
    if (req.query.queueId) fb.eq('queue_id', parseInt(req.query.queueId, 10));

    const { clause, params } = fb.build();
    const sort = sorting(req.query.sort, req.query.order, ['mu', 'phi', 'volatility', 'queue_id']);

    const rows = await query(
      `SELECT * FROM player_queue_ratings${clause}${clause ? ' AND' : ' WHERE'} mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2${sort}`,
      params,
    );
    return rows;
  });

  /**
   * GET /ratings/champion/:playerId — Player champion ratings (player_champion_ratings)
   */
  fastify.get('/champion/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const fb = new FilterBuilder().eq('player_id', playerId);
    if (req.query.championId) fb.eq('champion_id', parseInt(req.query.championId, 10));

    const { clause, params } = fb.build();
    const sort = sorting(req.query.sort, req.query.order, ['mu', 'phi', 'matches_played', 'wins', 'losses', 'champion_id']);

    const rows = await query(
      `SELECT * FROM player_champion_ratings${clause}${clause ? ' AND' : ' WHERE'} mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2${sort}`,
      params,
    );
    return rows;
  });

  /**
   * GET /ratings/champion/meta — Aggregate champion ratings (champion_ratings)
   */
  fastify.get('/champion/meta', async () => {
    return query('SELECT * FROM champion_ratings ORDER BY rating DESC LIMIT 100');
  });

  /**
   * GET /ratings/champion/match-history/:championId — Champion match ratings
   */
  fastify.get('/champion/match-history/:championId', async (req: any) => {
    const championId = parseInt(req.params.championId, 10);
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const rows = await query(
      `SELECT * FROM champion_match_ratings WHERE champion_id = $1 ORDER BY match_id DESC LIMIT $2 OFFSET $3`,
      [championId, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /ratings/snapshots/:matchId — Pre/post rating snapshots for a match
   */
  fastify.get('/snapshots/:matchId', async (req: any, reply: any) => {
    const matchId = parseInt(req.params.matchId, 10);
    if (!Number.isInteger(matchId)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid match ID' } });
    }

    const rows = await query(
      `SELECT ms.*, ${DISPLAY_NAME_SQL} as player_name
       FROM match_rating_snapshots ms
       JOIN players p ON p.id = ms.player_id
       WHERE ms.match_id = $1
       ORDER BY ${DISPLAY_NAME_SQL}`,
      [matchId]
    );
    return rows;
  });

  /**
   * GET /ratings/snapshots/player/:playerId — Rating snapshots for a player across matches
   */
  fastify.get('/snapshots/player/:playerId', async (req: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder().eq('ms.player_id', playerId);
    if (req.query.from) fb.gte('ms.match_id', req.query.from); // proxy: use match_id as proxy for date
    let { clause, params } = fb.build();
    if (req.query.queueId) {
      params.push(parseInt(req.query.queueId, 10));
      clause += `${clause ? ' AND' : ' WHERE'} EXISTS (
        SELECT 1 FROM matches m
        WHERE m.match_id = ms.match_id
          AND m.queue_id = $${params.length}
      )`;
    }
    const rows = await query(
      `SELECT ms.*, ${DISPLAY_NAME_SQL} as player_name
       FROM match_rating_snapshots ms
       JOIN players p ON p.id = ms.player_id${clause}
       ORDER BY ms.match_id DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /ratings/distribution — Rating distribution by mu ranges
   */
  fastify.get('/distribution', async (req: any) => {
    const binSize = parseInt(req.query.binSize as string) || 50;

    const rows = await query(`
      SELECT
        FLOOR(mu / $1::BIGINT) * $1::BIGINT as bin_start,
        (FLOOR(mu / $1::BIGINT) + 1) * $1::BIGINT as bin_end,
        COUNT(*) as player_count,
        ROUND(AVG(mu)::NUMERIC, 2) as avg_mu,
        ROUND(AVG(phi)::NUMERIC, 2) as avg_phi
      FROM player_queue_ratings
      WHERE mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2
      GROUP BY FLOOR(mu / $1::BIGINT)
      ORDER BY bin_start`,
      [binSize]
    );
    return rows;
  });

  /**
   * GET /ratings/volatility/:playerId — Volatility history for a player
   */
  fastify.get('/volatility/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const rows = await query(
      `SELECT * FROM player_queue_ratings WHERE player_id = $1
        AND mu BETWEEN 0 AND 3500 AND phi BETWEEN 1 AND 350 AND volatility BETWEEN 0.001 AND 0.2
        ORDER BY queue_id`,
      [playerId]
    );
    return rows.map((r: any) => ({ queue_id: r.queue_id, volatility: r.volatility }));
  });
}
