import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { paginate, bulkIds, requireUserSession } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';

/**
 * Extended Player Routes.
 * Exposes name history, account merges, status, achievements, private accounts.
 * Tables: player_name_history, player_account_merges, player_status,
 *         player_achievements, players_private, players_private_history.
 */
export default async function playerExtRoutes(fastify: FastifyInstance) {
  /**
   * GET /player-ext/name-history/:playerId — Player name change history
   */
  fastify.get('/name-history/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const rows = await query(
      `SELECT * FROM player_name_history WHERE player_id = $1 ORDER BY changed_at DESC LIMIT $2 OFFSET $3`,
      [playerId, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /player-ext/merges/:playerId — Account merge history
   */
  fastify.get('/merges/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const rows = await query(
      `SELECT * FROM player_account_merges WHERE player_id = $1 OR merged_into_player_id = $1 ORDER BY merged_at DESC`,
      [playerId]
    );
    return rows;
  });

  /**
   * GET /player-ext/status/:playerId — Current player status
   * May refresh via Hi-Rez API if cached data is stale
   */
  fastify.get('/status/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const row = await one('SELECT * FROM player_status WHERE player_id = $1', [playerId]);
    if (!row) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Player status not found', details: { playerId } } });
    }
    return row;
  });

  /**
   * GET /player-ext/achievements/:playerId — Player achievements
   */
  fastify.get('/achievements/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const rows = await query('SELECT * FROM player_achievements WHERE player_id = $1 ORDER BY achievement_id', [playerId]);
    return rows;
  });

  /**
   * GET /player-ext/private — List private accounts
   */
  fastify.get('/private', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });
    const search = String(req.query.q || '').trim();
    const params: any[] = [];
    let where = ' WHERE is_active';
    if (search) {
      params.push(`%${search}%`);
      where += ` AND (alias ILIKE $${params.length} OR verified_name ILIKE $${params.length})`;
    }
    if (req.query.cheater === 'true') {
      where += ' AND cheater';
    } else if (req.query.cheater === 'false') {
      where += ' AND NOT cheater';
    }
    if (req.query.suspicious === 'true') {
      where += ' AND sus_count > 0';
    }
    const rows = await query(
      `SELECT id, party_id, account_level, mastery_level, league_tier, league_points,
              first_seen, last_seen, match_count, alias, verified_name,
              COALESCE(verified_name, alias) AS display_name,
              identity_status, identity_confidence, tracking_version,
              cheater, cheater_reason, cheater_marked_at, sus_count,
              COALESCE((
                SELECT jsonb_agg(reason_group ORDER BY reason_group.count DESC, reason_group.reason)
                FROM (
                  SELECT vote.reason, COUNT(*)::INT AS count
                  FROM private_account_community_votes vote
                  WHERE vote.private_player_id = players_private.id
                    AND vote.vote_type = 'suspicious'
                  GROUP BY vote.reason
                  ORDER BY COUNT(*) DESC, vote.reason
                  LIMIT 3
                ) reason_group
              ), '[]'::jsonb) AS top_reasons,
              COUNT(*) OVER()::INT AS total_count
       FROM players_private${where}
       ORDER BY last_seen DESC, id DESC
       LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /player-ext/private/bulk?ids=1,2 — Stored private-account moderation.
   * This is database-only and deliberately uses the private identity namespace.
   */
  fastify.get('/private/bulk', async (req: any, reply: any) => {
    const ids = bulkIds(req.query.ids);
    if (ids.length === 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Missing or invalid ids parameter' } });
    }
    const accounts = await query(
      `SELECT id, cheater, cheater_reason, cheater_marked_at, sus_count
       FROM players_private
       WHERE id = ANY($1) AND is_active`,
      [ids],
    );
    const found = new Set(accounts.map((account: any) => Number(account.id)));
    return {
      accounts,
      count: accounts.length,
      notFound: ids.filter((id: number) => !found.has(id)),
    };
  });

  /**
   * GET /player-ext/private/:privateId — Evidence-backed private identity.
   * Screenshot/evidence locations are intentionally admin-only; this public
   * response exposes the verified name and match observations, not the report.
   */
  fastify.get('/private/:privateId', async (req: any, reply: any) => {
    const privateId = parseInt(req.params.privateId, 10);
    if (!Number.isInteger(privateId) || privateId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid private account ID' } });
    }
    const canonical = await one<{ id: number }>(
      `WITH RECURSIVE identity_chain AS (
         SELECT id, merged_into_id, is_active, 0 AS depth
         FROM players_private WHERE id = $1
         UNION ALL
         SELECT next.id, next.merged_into_id, next.is_active, chain.depth + 1
         FROM players_private next
         JOIN identity_chain chain ON next.id = chain.merged_into_id
         WHERE chain.depth < 16
       )
       SELECT id FROM identity_chain
       WHERE is_active
       ORDER BY depth DESC
       LIMIT 1`,
      [privateId],
    );
    if (!canonical) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Private account not found' } });
    }
    const account = await one(
      `SELECT id, party_id, account_level, mastery_level, league_tier, league_points,
              first_seen, last_seen, match_count, alias, verified_name,
              COALESCE(verified_name, alias) AS display_name,
              identity_status, identity_confidence, tracking_version,
              cheater, cheater_reason, cheater_marked_at, sus_count
       FROM players_private
       WHERE id = $1 AND is_active`,
      [canonical.id],
    );
    if (!account) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Private account not found' } });
    }
    const observations = await query(
      `WITH ordered AS (
         SELECT o.*,
                lag(o.league_points) OVER (
                  ORDER BY o.entry_datetime, o.match_id, o.private_slot
                ) AS previous_league_points,
                lag(o.league_tier) OVER (
                  ORDER BY o.entry_datetime, o.match_id, o.private_slot
                ) AS previous_league_tier
         FROM private_account_observations o
         WHERE o.private_player_id = $1
       ), timeline AS (
         SELECT ordered.*,
                CASE
                  WHEN league_tier = previous_league_tier
                    THEN league_points - previous_league_points
                  ELSE NULL
                END AS tp_delta
         FROM ordered
       )
       SELECT o.match_id, o.private_slot, o.entry_datetime, o.account_level,
              o.mastery_level, o.league_tier, o.league_points, o.tp_delta,
              o.win_status, o.champion_id, c.name AS champion_name,
              o.task_force, o.platform, o.source,
              o.resolution_status, o.resolution_confidence, o.resolution_reasons,
              m.map, m.queue_id, m.region, m.duration_seconds
       FROM timeline o
       LEFT JOIN matches m ON m.match_id = o.match_id
       LEFT JOIN champions c ON c.id = o.champion_id
       ORDER BY o.entry_datetime DESC, o.match_id DESC, o.private_slot DESC
       LIMIT 250`,
      [canonical.id],
    );
    return { account, observations, requested_private_id: privateId };
  });

  /**
   * POST /player-ext/private/:privateId/report
   * Community SUS votes and approved confirmed-cheater decisions use the
   * canonical private identity, never the shared match player_id=0 sentinel.
   */
  fastify.post('/private/:privateId/report', async (req: any, reply: any) => {
    const privateId = parseInt(req.params.privateId, 10);
    if (!Number.isInteger(privateId) || privateId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid private account ID' } });
    }

    let session: Awaited<ReturnType<typeof requireUserSession>>;
    try {
      session = await requireUserSession(req);
    } catch {
      return reply.status(401).send({ error: { code: 'AUTH', message: 'Authentication required' } });
    }

    const body = (req.body || {}) as { type?: string; reason?: string };
    const reportType = String(body.type || '');
    const reason = String(body.reason || '').trim();
    if (reportType !== 'suspicious' && reportType !== 'cheater') {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Private accounts support suspicious and cheater reports' } });
    }
    if (!reason) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'A reason is required' } });
    }
    if (reason.length > 2_000) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'reason must be at most 2000 characters' } });
    }
    if (reportType === 'cheater' && !session.user.isAdmin && !session.user.isApproved) {
      return reply.status(403).send({ error: { code: 'PERMISSION', message: 'Action requires admin or approved status' } });
    }

    const canonical = await one<{ id: number }>(
      `WITH RECURSIVE identity_chain AS (
         SELECT id, merged_into_id, is_active, 0 AS depth
         FROM players_private WHERE id = $1
         UNION ALL
         SELECT next.id, next.merged_into_id, next.is_active, chain.depth + 1
         FROM players_private next
         JOIN identity_chain chain ON next.id = chain.merged_into_id
         WHERE chain.depth < 16
       )
       SELECT id FROM identity_chain WHERE is_active ORDER BY depth DESC LIMIT 1`,
      [privateId],
    );
    if (!canonical) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Private account not found' } });
    }

    if (reportType === 'suspicious') {
      const result = await one<{ created: boolean; count: number | null }>(
        `WITH inserted_vote AS (
           INSERT INTO private_account_community_votes (private_player_id, user_id, vote_type, reason)
           VALUES ($1, $2, 'suspicious', $3)
           ON CONFLICT (private_player_id, user_id, vote_type) DO NOTHING
           RETURNING id
         ), updated_account AS (
           UPDATE players_private
           SET sus_count = sus_count + 1, updated_at = now()
           WHERE id = $1 AND EXISTS (SELECT 1 FROM inserted_vote)
           RETURNING sus_count AS count
         )
         SELECT EXISTS (SELECT 1 FROM inserted_vote) AS created,
                (SELECT count FROM updated_account) AS count`,
        [canonical.id, session.user.id, reason],
      );
      const current = result?.count ?? (await one<{ sus_count: number }>(
        'SELECT sus_count FROM players_private WHERE id = $1',
        [canonical.id],
      ))?.sus_count ?? 0;
      return {
        success: true,
        message: result?.created ? 'Private account reported as Suspicious' : 'You have already reported this private account as Suspicious',
        already_voted: !result?.created,
        private_id: canonical.id,
        sus_count: Number(current),
      };
    }

    const result = await one<{ created: boolean; cheater: boolean }>(
      `WITH inserted_vote AS (
         INSERT INTO private_account_community_votes (private_player_id, user_id, vote_type, reason)
         VALUES ($1, $2, 'cheater', $3)
         ON CONFLICT (private_player_id, user_id, vote_type) DO NOTHING
         RETURNING id
       )
       UPDATE players_private
       SET cheater = TRUE,
           cheater_reason = $3,
           cheater_marked_at = COALESCE(cheater_marked_at, now()),
           updated_at = now()
       WHERE id = $1
       RETURNING EXISTS (SELECT 1 FROM inserted_vote) AS created, cheater`,
      [canonical.id, session.user.id, reason],
    );
    return {
      success: true,
      message: result?.created ? 'Private account confirmed as cheater' : 'Cheater report already recorded for this private account',
      already_voted: !result?.created,
      private_id: canonical.id,
      cheater: true,
    };
  });

  /**
   * GET /player-ext/private-history — Private account history
   */
  fastify.get('/private-history', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const rows = await query(
      `SELECT * FROM players_private_history ORDER BY recorded_at DESC LIMIT $1 OFFSET $2`,
      [perPage, offset]
    );
    return rows;
  });

  /**
   * GET /player-ext/bulk — Batch player lookup
   * ?ids=comma,separated (max 50)
   */
  fastify.get('/bulk', async (req: any, reply: any) => {
    const ids = bulkIds(req.query.ids as string, 50);
    if (ids.length === 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Missing or invalid ids parameter' } });
    }

    const rows = await query('SELECT id, name, level, region, platform, kbm_tier, kbm_points, cheater, sus_count FROM players WHERE id = ANY($1)', [ids]);
    const found = new Set(rows.map((r: any) => r.id));
    const notFound = ids.filter((id: number) => !found.has(id));

    return { players: rows, count: rows.length, notFound: notFound.length > 0 ? notFound : undefined };
  });

  /**
   * GET /player-ext/search — Advanced player search
   * ?q=, ?region=, ?tierMin=, ?tierMax=, ?platform=, ?cheater=, ?page=, ?perPage=
   */
  fastify.get('/search', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.q) fb.like('name', `%${req.query.q}%`);
    if (req.query.region) fb.eq('region', req.query.region);
    if (req.query.platform) fb.eq('platform', req.query.platform);
    if (req.query.tierMin) fb.gte('kbm_tier', parseInt(req.query.tierMin, 10));
    if (req.query.tierMax) fb.lte('kbm_tier', parseInt(req.query.tierMax, 10));
    if (req.query.cheater === 'true') fb.eq('cheater', true);
    if (req.query.cheater === 'false') fb.eq('cheater', false);

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT id, name, level, region, platform, kbm_tier, kbm_points, cheater, sus_count FROM players${clause} ORDER BY kbm_points DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });
}
