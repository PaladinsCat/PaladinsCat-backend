import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { paginate, DISPLAY_NAME_SQL } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';

/**
 * Esports Routes.
 * Exposes league, team, and roster data from esports_leagues, esports_teams, esports_team_players.
 */
export default async function esportsRoutes(fastify: FastifyInstance) {
  /**
   * GET /esports/leagues — List all esports leagues
   */
  fastify.get('/leagues', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.q) fb.like('league_name', `%${req.query.q}%`);

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM esports_leagues${clause} ORDER BY league_name LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /esports/leagues/:id — League detail + teams
   */
  fastify.get('/leagues/:id', async (req: any, reply: any) => {
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid league ID' } });
    }

    const league = await one('SELECT * FROM esports_leagues WHERE league_id = $1', [id]);
    if (!league) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'League not found', details: { id } } });
    }

    const teams = await query('SELECT * FROM esports_teams WHERE league_id = $1 ORDER BY team_name', [id]);
    return { league, teams };
  });

  /**
   * GET /esports/teams — List all teams
   */
  fastify.get('/teams', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.leagueId) fb.eq('league_id', parseInt(req.query.leagueId, 10));
    if (req.query.q) fb.like('team_name', `%${req.query.q}%`);

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM esports_teams${clause} ORDER BY team_name LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /esports/teams/:id — Team detail + roster
   */
  fastify.get('/teams/:id', async (req: any, reply: any) => {
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid team ID' } });
    }

    const team = await one('SELECT * FROM esports_teams WHERE team_id = $1', [id]);
    if (!team) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Team not found', details: { id } } });
    }

    const players = await query(
      `SELECT etp.*, ${DISPLAY_NAME_SQL} as player_name FROM esports_team_players etp JOIN players p ON p.id = etp.player_id WHERE etp.team_id = $1 ORDER BY ${DISPLAY_NAME_SQL}, etp.player_id`,
      [id]
    );
    return { team, players };
  });

  /**
   * GET /esports/teams/:id/players — Team roster
   */
  fastify.get('/teams/:id/players', async (req: any, reply: any) => {
    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid team ID' } });
    }

    const players = await query(
      `SELECT etp.*, ${DISPLAY_NAME_SQL} as player_name FROM esports_team_players etp JOIN players p ON p.id = etp.player_id WHERE etp.team_id = $1 ORDER BY ${DISPLAY_NAME_SQL}, etp.player_id`,
      [id]
    );
    return players;
  });

  /**
   * GET /esports/search — Search teams by name
   */
  fastify.get('/search', async (req: any, reply: any) => {
    const q = req.query.q as string;
    if (!q) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Missing required query param: q' } });
    }

    const rows = await query(
      `SELECT * FROM esports_teams WHERE team_name ILIKE $1 ORDER BY team_name LIMIT 20`,
      [`%${q}%`]
    );
    return rows;
  });
}
