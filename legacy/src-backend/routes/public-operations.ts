import { FastifyInstance } from 'fastify';
import { one } from '../config/db';
import { getActiveUserSnapshot } from '../services/site-live-sessions';

export default async function publicOperationsRoutes(fastify: FastifyInstance) {
  fastify.get('/stats', async (_req: any, reply: any) => {
    reply.header('Cache-Control', 'public, max-age=30, s-maxage=60, stale-while-revalidate=120');

    const [totals, traffic, activeUsers, ingestCoverage, release] = await Promise.all([
      one(`
        SELECT
          (SELECT COUNT(*)::INT FROM matches) AS matches,
          (SELECT COUNT(*)::INT FROM matches WHERE queue_id = 486) AS ranked_matches,
          (SELECT COUNT(*)::INT FROM matches WHERE queue_id IS DISTINCT FROM 486) AS casual_matches,
          (SELECT COUNT(*)::INT FROM players WHERE id > 0) AS players,
          (SELECT COUNT(*)::INT FROM users) AS registered_users,
          (SELECT COUNT(*)::INT FROM users WHERE linked_player_id IS NOT NULL) AS verified_users,
          (SELECT COUNT(*)::INT FROM builds WHERE visibility = 'public') AS community_builds,
          (SELECT COUNT(*)::INT FROM tier_lists) AS tier_lists,
          (SELECT COUNT(*)::INT FROM posts) AS community_posts,
          (SELECT COUNT(*)::INT FROM matches WHERE recovered = TRUE) AS recovered_matches,
          (SELECT COUNT(*)::INT FROM matches WHERE broken = TRUE AND recovered = FALSE) AS incomplete_matches,
          (SELECT MAX(entry_datetime) FROM matches) AS latest_match_at
      `),
      one(`
        SELECT
          COUNT(*) FILTER (WHERE visit_date = (now() AT TIME ZONE 'UTC')::DATE)::INT AS visitors_today,
          COALESCE(SUM(page_views) FILTER (WHERE visit_date = (now() AT TIME ZONE 'UTC')::DATE), 0)::INT AS views_today,
          COUNT(*) FILTER (WHERE visit_date >= (now() AT TIME ZONE 'UTC')::DATE - 6)::INT AS visitor_days_7d,
          COALESCE(SUM(page_views) FILTER (WHERE visit_date >= (now() AT TIME ZONE 'UTC')::DATE - 6), 0)::INT AS views_7d
        FROM site_daily_visitors
        WHERE visit_date >= (now() AT TIME ZONE 'UTC')::DATE - 6
      `),
      getActiveUserSnapshot(),
      one(`
        SELECT
          COUNT(*)::INT AS total_matches,
          COUNT(*) FILTER (WHERE broken IS NOT TRUE AND recovered IS NOT TRUE)::INT AS direct_matches,
          COUNT(*) FILTER (WHERE recovered IS TRUE)::INT AS recovered_matches,
          (
            SELECT COUNT(*)::INT
            FROM nonranked_match_acquisition acquisition
            WHERE acquisition.status IN (
              'discovered', 'waiting_for_completion', 'fetching'
            )
              AND acquisition.source_date + acquisition.source_hour * interval '1 hour'
                    >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
          ) AS nonranked_open_24h,
          (
            SELECT COUNT(*)::INT
            FROM nonranked_match_acquisition acquisition
            WHERE acquisition.status = 'waiting_for_completion'
              AND acquisition.source_date + acquisition.source_hour * interval '1 hour'
                    >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
          ) AS nonranked_waiting_for_completion_24h,
          (
            SELECT COUNT(*)::INT
            FROM nonranked_match_acquisition acquisition
            WHERE acquisition.status IN (
              'discovered', 'waiting_for_completion', 'fetching'
            )
              AND acquisition.source_date + acquisition.source_hour * interval '1 hour'
                    < (now() AT TIME ZONE 'UTC') - interval '24 hours'
          ) AS nonranked_historical_open,
          (
            SELECT MIN(acquisition.source_date + acquisition.source_hour * interval '1 hour')
            FROM nonranked_match_acquisition acquisition
            WHERE acquisition.status IN (
              'discovered', 'waiting_for_completion', 'fetching'
            )
          ) AS nonranked_oldest_open_hour,
          (
            SELECT MAX(acquisition.completed_at)
            FROM nonranked_match_acquisition acquisition
          ) AS nonranked_last_completed_at
        FROM matches
        WHERE entry_datetime >= now() - INTERVAL '24 hours'
      `),
      one(`
        SELECT version, git_commit_short, deployed_at
        FROM stack_versions
        WHERE component = 'stack'
        ORDER BY deployed_at DESC, id DESC
        LIMIT 1
      `),
    ]);

    return {
      generated_at: new Date().toISOString(),
      release: release ?? {
        version: process.env.PALADINSCAT_VERSION ?? '',
        git_commit_short: process.env.PALADINSCAT_GIT_COMMIT_SHORT ?? '',
        deployed_at: process.env.PALADINSCAT_BUILD_TIMESTAMP ?? null,
      },
      traffic: { summary: { ...traffic, ...activeUsers } },
      catalog: totals,
      ingest_coverage: ingestCoverage,
    };
  });
}
