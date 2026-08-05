import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { paginate } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';
import { createRateLimiter } from '../services/rate-limit';
import { clientRateLimitIdentity, guardVendorFallback } from '../services/request-security';

const liveLookupRateLimit = createRateLimiter(12, 60_000);

/**
 * Live matches are deliberately DB-first. The live discovery worker writes the
 * light-weight live_match_players snapshot, while this projection fills in
 * PaladinsCat's already-cached profile and rating fields without consuming a
 * Hi-Rez request for every person in a lobby.
 */
async function getEnrichedLiveMatchPlayers(matchId: number) {
  return query(
    `SELECT
       lmp.match_id,
       lmp.player_id,
       COALESCE(p.name, lmp.player_name) AS player_name,
       lmp.champion_id,
       COALESCE(c.name, lmp.champion_name) AS champion_name,
       lmp.skin_id,
       lmp.skin_name,
       lmp.account_level,
       lmp.mastery_level,
       lmp.tier AS live_tier,
       lmp.tier_wins,
       lmp.tier_losses,
       lmp.task_force,
       lmp.platform,
       (p.id IS NOT NULL) AS has_profile,
       p.level AS profile_level,
       p.mastery_level AS profile_mastery_level,
       p.platform AS profile_platform,
       p.region AS profile_region,
       p.hours_played AS profile_hours_played,
       p.total_xp AS profile_total_xp,
       p.kbm_tier,
       COALESCE(lc.rank, p.kbm_rank) AS kbm_rank,
       p.kbm_points,
       p.wins AS profile_wins,
       p.losses AS profile_losses,
       (COALESCE(p.wins, 0) + COALESCE(p.losses, 0))::INT AS profile_matches,
       CASE WHEN (COALESCE(p.wins, 0) + COALESCE(p.losses, 0)) > 0
         THEN ROUND(
           100.0 * COALESCE(p.wins, 0)::NUMERIC
           / (COALESCE(p.wins, 0) + COALESCE(p.losses, 0)),
           1
         )::DOUBLE PRECISION
         ELSE NULL
       END AS profile_win_rate,
       COALESCE(lc.wins, p.kbm_wins) AS ranked_wins,
       COALESCE(lc.losses, p.kbm_losses) AS ranked_losses,
       (COALESCE(lc.wins, p.kbm_wins, 0) + COALESCE(lc.losses, p.kbm_losses, 0))::INT AS ranked_matches,
       CASE WHEN (COALESCE(lc.wins, p.kbm_wins, 0) + COALESCE(lc.losses, p.kbm_losses, 0)) > 0
         THEN ROUND(
           100.0 * COALESCE(lc.wins, p.kbm_wins, 0)::NUMERIC
           / (COALESCE(lc.wins, p.kbm_wins, 0) + COALESCE(lc.losses, p.kbm_losses, 0)),
           1
         )::DOUBLE PRECISION
         ELSE NULL
       END AS ranked_win_rate,
       p.total_matches,
       p.total_wins,
       p.total_losses,
       p.avg_dpm,
       p.avg_hpm,
       p.avg_mpm,
       pqr.mu::DOUBLE PRECISION AS queue_elo,
       pqr.phi::DOUBLE PRECISION AS queue_phi,
       pcr.mu::DOUBLE PRECISION AS champion_elo,
       pcr.phi::DOUBLE PRECISION AS champion_phi
     FROM live_match_players lmp
     JOIN live_matches lm ON lm.match_id = lmp.match_id
     LEFT JOIN players p ON p.id = lmp.player_id
     LEFT JOIN leaderboard_current lc ON lc.player_id = lmp.player_id
     LEFT JOIN champions c ON c.id = lmp.champion_id
     LEFT JOIN player_queue_ratings pqr
       ON pqr.player_id = lmp.player_id
      AND pqr.queue_id = lm.queue_id
      AND pqr.mu BETWEEN 0 AND 3500 AND pqr.phi BETWEEN 1 AND 350 AND pqr.volatility BETWEEN 0.001 AND 0.2
     LEFT JOIN player_champion_ratings pcr
       ON pcr.player_id = lmp.player_id
      AND pcr.champion_id = lmp.champion_id
      AND pcr.mu BETWEEN 0 AND 3500 AND pcr.phi BETWEEN 1 AND 350 AND pcr.volatility BETWEEN 0.001 AND 0.2
     WHERE lmp.match_id = $1
     ORDER BY lmp.task_force, COALESCE(p.name, lmp.player_name), lmp.player_id`,
    [matchId],
  );
}

/**
 * Live Match Routes.
 * Exposes live match tracking: live_matches, live_match_players, drop_hack_suspects.
 * Moves /matches/live/* logic to /live/* prefix for cleaner organization.
 */
export default async function liveRoutes(fastify: FastifyInstance) {
  /**
   * GET /live/matches — All active live matches
   */
  fastify.get('/matches', async (req: any) => {
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.status) fb.eq('status', req.query.status);

    const { clause, params } = fb.build();
    const rows = await query(
      `SELECT * FROM live_matches${clause} ORDER BY detected_at DESC LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /live/matches/:matchId — Live match detail + players
   */
  fastify.get('/matches/:matchId', async (req: any, reply: any) => {
    const matchId = parseInt(req.params.matchId, 10);
    if (!Number.isInteger(matchId)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid match ID' } });
    }

    const match = await one('SELECT * FROM live_matches WHERE match_id = $1', [matchId]);
    if (!match) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Live match not found', details: { matchId } } });
    }

    const players = await getEnrichedLiveMatchPlayers(matchId);
    return { match, players };
  });

  /**
   * GET /live/players/:playerId — Player's current live match
   * Same logic as /matches/live/:playerId
   */
  fastify.get('/players/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId, 10);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid player ID' } });
    }

    const rate = await liveLookupRateLimit(`live-player:${clientRateLimitIdentity(req)}`);
    if (!rate.allowed) {
      return reply.status(429).send({
        error: { code: 'RATE_LIMITED', message: 'Too many live-match lookups. Try again shortly.' },
        retryAfter: Math.max(1, Math.ceil((rate.resetAt - Date.now()) / 1000)),
      });
    }
    reply.header('Cache-Control', 'private, no-store');

    const { getPlayerLiveMatch } = await import('../workers/live-match-tracker.js');
    const live = await getPlayerLiveMatch(
      playerId,
      (stage, entity) => guardVendorFallback(req, reply, {
        scope: `live-${stage}`,
        entity,
        entityWindowMs: stage === 'player-status' ? 30_000 : 10_000,
      }),
    );
    if (live?.pending) {
      return {
        match: null,
        players: [],
        player_id: playerId,
        pending: true,
        message: 'Live lobby details are not ready yet. Try again shortly.',
      };
    }
    if (!live?.match) return { match: null, players: [], player_id: playerId };
    return {
      match: live.match,
      players: await getEnrichedLiveMatchPlayers(Number(live.match.match_id)),
    };
  });

  /**
   * GET /live/drop-hack-suspects — Drop-hack suspect list
   * Same logic as /matches/live/drop-hack-suspects
   */
  fastify.get('/drop-hack-suspects', async (req: any) => {
    const limit = parseInt(req.query.limit as string) || 50;

    const { getDropHackSuspects } = await import('../workers/live-match-tracker.js');
    return getDropHackSuspects(limit);
  });

  /**
   * GET /live/ended — Recently ended matches not yet ingested
   */
  fastify.get('/ended', async (req: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 20, 100);

    const rows = await query(
      `SELECT * FROM live_matches WHERE status = 'ended' ORDER BY ended_at DESC LIMIT $1`,
      [limit]
    );
    return rows;
  });
}
