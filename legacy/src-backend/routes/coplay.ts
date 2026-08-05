import { FastifyInstance } from 'fastify';
import { query } from '../config/db';
import { paginate } from '../utils/query-helpers';

/**
 * Co-Play & Relationships Routes.
 * Exposes teammate/opponent co-play data plus canonical ranked party groups.
 * All data is ranked-only (queue_id=486).
 *
 * Storage model:
 * player_relationships stores one normalized row per unordered pair:
 *   source_player_id < target_player_id, plus same_team.
 * This avoids double-counting A/B and B/A, but every route that looks up a
 * player's relationships must search both source_player_id and target_player_id
 * and then derive the "other player" with a CASE expression.
 *
 * Party counts instead come from immutable match_party_groups and
 * match_party_pairs facts. player_relationships.count includes ordinary
 * teammate matches and therefore is not a valid party-match count.
 */
export default async function coplayRoutes(fastify: FastifyInstance) {
  /**
   * GET /coplay/parties — Canonical ranked party pairs or exact 2-5 stacks.
   * ?kind=pairs (default) returns every unordered pair emitted by a party.
   * ?kind=stacks returns the complete observed party membership.
   */
  fastify.get('/parties', async (req: any, reply: any) => {
    const requestedPerPage = req.query.perPage ?? req.query.limit;
    const { perPage, offset } = paginate({ page: req.query.page, perPage: requestedPerPage });
    const search = String(req.query.q ?? '').trim();
    const kind = String(req.query.kind ?? 'pairs').toLowerCase();
    if (!['pairs', 'stacks'].includes(kind)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'kind must be pairs or stacks' } });
    }

    if (kind === 'stacks') {
      const requestedSize = parseInt(String(req.query.size ?? ''), 10);
      const stackSize = Number.isInteger(requestedSize) && requestedSize >= 2 && requestedSize <= 5
        ? requestedSize
        : null;
      const params: Array<string | number> = [];
      const clauses: string[] = [];
      if (search) {
        params.push(`%${search}%`);
        clauses.push(`EXISTS (
          SELECT 1
          FROM unnest(pss.player_ids) AS searched(player_id)
          JOIN players searched_player ON searched_player.id = searched.player_id
          WHERE searched_player.name ILIKE $${params.length}
        )`);
      }
      if (stackSize) {
        params.push(stackSize);
        clauses.push(`pss.stack_size = $${params.length}`);
      }
      const where = clauses.length > 0 ? `WHERE ${clauses.join(' AND ')}` : '';
      params.push(perPage, offset);
      const limitParam = params.length - 1;
      const offsetParam = params.length;

      return query(`
        SELECT
          pss.group_key,
          pss.player_ids,
          members.player_names,
          pss.stack_size,
          pss.match_count,
          pss.first_seen,
          pss.last_seen,
          COUNT(*) OVER()::INT AS total_count
        FROM party_stack_stats pss
        JOIN LATERAL (
          SELECT array_agg(COALESCE(p.name, 'Player ' || member.player_id::TEXT) ORDER BY member.ordinal) AS player_names
          FROM unnest(pss.player_ids) WITH ORDINALITY AS member(player_id, ordinal)
          LEFT JOIN players p ON p.id = member.player_id
        ) members ON TRUE
        ${where}
        ORDER BY pss.stack_size DESC, pss.match_count DESC, pss.last_seen DESC, pss.group_key
        LIMIT $${limitParam} OFFSET $${offsetParam}`,
        params,
      );
    }

    const params: Array<string | number> = [];
    let searchClause = '';

    if (search) {
      params.push(`%${search}%`);
      searchClause = `AND (source.name ILIKE $${params.length} OR target.name ILIKE $${params.length})`;
    }

    params.push(perPage, offset);
    const limitParam = params.length - 1;
    const offsetParam = params.length;

    return query(`
      SELECT
        pps.player_low_id AS source_player_id,
        source.name AS source_player_name,
        pps.player_high_id AS target_player_id,
        target.name AS target_player_name,
        pps.match_count,
        pps.first_seen,
        pps.last_seen,
        COUNT(*) OVER()::INT AS total_count
      FROM party_pair_stats pps
      JOIN players source ON source.id = pps.player_low_id
      JOIN players target ON target.id = pps.player_high_id
      WHERE TRUE
        ${searchClause}
      ORDER BY pps.match_count DESC, pps.last_seen DESC, pps.player_low_id, pps.player_high_id
      LIMIT $${limitParam} OFFSET $${offsetParam}`,
      params
    );
  });

  /**
   * GET /coplay/teammates/:playerId — Top teammates (same_team=true)
   */
  fastify.get('/teammates/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);

    const rows = await query(`
      SELECT
        $1::BIGINT AS player_id,
        CASE WHEN pr.source_player_id = $1 THEN pr.target_player_id ELSE pr.source_player_id END AS other_player_id,
        p2.name AS other_player_name,
        pr.source_player_id,
        pr.target_player_id,
        pr.same_team,
        pr.same_party,
        pr.count AS match_count,
        pr.first_seen,
        pr.last_seen
      FROM player_relationships pr
      JOIN players p2 ON p2.id = CASE WHEN pr.source_player_id = $1 THEN pr.target_player_id ELSE pr.source_player_id END
      WHERE (pr.source_player_id = $1 OR pr.target_player_id = $1)
        AND pr.same_team = true
      ORDER BY pr.count DESC, pr.last_seen DESC
      LIMIT $2`,
      [playerId, limit]
    );
    return rows;
  });

  /**
   * GET /coplay/opponents/:playerId — Top opponents (same_team=false)
   */
  fastify.get('/opponents/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);

    const rows = await query(`
      SELECT
        $1::BIGINT AS player_id,
        CASE WHEN pr.source_player_id = $1 THEN pr.target_player_id ELSE pr.source_player_id END AS other_player_id,
        p2.name AS other_player_name,
        pr.source_player_id,
        pr.target_player_id,
        pr.same_team,
        pr.same_party,
        pr.count AS match_count,
        pr.first_seen,
        pr.last_seen
      FROM player_relationships pr
      JOIN players p2 ON p2.id = CASE WHEN pr.source_player_id = $1 THEN pr.target_player_id ELSE pr.source_player_id END
      WHERE (pr.source_player_id = $1 OR pr.target_player_id = $1)
        AND pr.same_team = false
      ORDER BY pr.count DESC, pr.last_seen DESC
      LIMIT $2`,
      [playerId, limit]
    );
    return rows;
  });

  /**
   * GET /coplay/party/:playerId — Canonical party partners.
   * A five-stack contributes four partners to each member, once per match.
   */
  fastify.get('/party/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);

    const rows = await query(`
      SELECT
        $1::BIGINT AS player_id,
        CASE WHEN pps.player_low_id = $1 THEN pps.player_high_id ELSE pps.player_low_id END AS other_player_id,
        p2.name AS other_player_name,
        pps.player_low_id AS source_player_id,
        pps.player_high_id AS target_player_id,
        true AS same_team,
        true AS same_party,
        pps.match_count,
        pps.first_seen,
        pps.last_seen
      FROM party_pair_stats pps
      JOIN players p2 ON p2.id = CASE WHEN pps.player_low_id = $1 THEN pps.player_high_id ELSE pps.player_low_id END
      WHERE pps.player_low_id = $1 OR pps.player_high_id = $1
      ORDER BY pps.match_count DESC, pps.last_seen DESC
      LIMIT $2`,
      [playerId, limit]
    );
    return rows;
  });

  /**
   * GET /coplay/pair/:sourceId/:targetId — Relationship between two players
   */
  fastify.get('/pair/:sourceId/:targetId', async (req: any, reply: any) => {
    const sourceId = parseInt(req.params.sourceId, 10);
    const targetId = parseInt(req.params.targetId, 10);

    if (!Number.isInteger(sourceId) || sourceId <= 0 || !Number.isInteger(targetId) || targetId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player IDs' } });
    }

    // Normalize the lookup to the table's source < target invariant. The route
    // still returns the requested pair ids so callers do not need to know how
    // the row is stored internally.
    const pairSourceId = Math.min(sourceId, targetId);
    const pairTargetId = Math.max(sourceId, targetId);

    const teammate = await query(`
      SELECT * FROM player_relationships
      WHERE source_player_id = $1 AND target_player_id = $2 AND same_team = true`,
      [pairSourceId, pairTargetId]
    );

    const opponent = await query(`
      SELECT * FROM player_relationships
      WHERE source_player_id = $1 AND target_player_id = $2 AND same_team = false`,
      [pairSourceId, pairTargetId]
    );
    const party = await query(`
      SELECT match_count, first_seen, last_seen
      FROM party_pair_stats
      WHERE player_low_id = $1 AND player_high_id = $2`,
      [pairSourceId, pairTargetId]
    );

    return {
      source_player_id: sourceId,
      target_player_id: targetId,
      teammate: teammate[0] || null,
      opponent: opponent[0] || null,
      party: party[0] || null,
    };
  });

  /**
   * GET /coplay/stats/:playerId — Pre-computed stats from mv_player_coplay_stats
   */
  fastify.get('/stats/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const rows = await query(`
      SELECT
        $1::BIGINT AS player_id,
        CASE WHEN mv.source_player_id = $1 THEN mv.target_player_id ELSE mv.source_player_id END AS other_player_id,
        p2.name AS other_player_name,
        mv.source_player_id,
        mv.target_player_id,
        mv.same_team,
        mv.times_together,
        COALESCE(pps.match_count, 0) AS times_in_party,
        mv.first_seen,
        mv.last_seen
      FROM mv_player_coplay_stats mv
      JOIN players p2 ON p2.id = CASE WHEN mv.source_player_id = $1 THEN mv.target_player_id ELSE mv.source_player_id END
      LEFT JOIN party_pair_stats pps
        ON pps.player_low_id = mv.source_player_id
       AND pps.player_high_id = mv.target_player_id
      WHERE mv.source_player_id = $1 OR mv.target_player_id = $1
      ORDER BY mv.times_together DESC, mv.last_seen DESC`,
      [playerId]
    );
    return rows;
  });

  /**
   * GET /coplay/top-pairs — Most common player pairs across all players
   */
  fastify.get('/top-pairs', async (req: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const sameTeam = req.query.sameTeam as string | undefined;

    const fb: string[] = [];
    const params: any[] = [];

    if (sameTeam === 'true') {
      fb.push('pr.same_team = true');
    } else if (sameTeam === 'false') {
      fb.push('pr.same_team = false');
    }

    const where = fb.length > 0 ? ` WHERE ${fb.join(' AND ')}` : '';

    const rows = await query(`
      SELECT pr.source_player_id, pr.target_player_id,
             p1.name as source_player_name, p2.name as target_player_name,
             pr.count AS match_count, pr.last_seen, pr.same_team
      FROM player_relationships pr
      JOIN players p1 ON p1.id = pr.source_player_id
      JOIN players p2 ON p2.id = pr.target_player_id${where}
      ORDER BY pr.count DESC, pr.last_seen DESC,
               pr.source_player_id, pr.target_player_id, pr.same_team
      LIMIT $1`,
      [limit]
    );
    return rows;
  });
}
