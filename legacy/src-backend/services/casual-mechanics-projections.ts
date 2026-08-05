import { transaction } from '../config/db';

export type CasualItemProjectionResult = 'projected' | 'already_projected';

type ProjectionClaim = {
  result: CasualItemProjectionResult;
  stats_scope: string;
  queue_id: number;
};

/**
 * Project one complete non-ranked match from the shared canonical item facts.
 *
 * The ledger claim and aggregate delta commit in one transaction, so retries
 * either apply the whole match once or observe the prior completed claim.
 * Ranked classification is rejected in SQL before the ledger can be written.
 */
export async function upsertCasualItemProjectionForMatch(
  matchId: number,
): Promise<CasualItemProjectionResult> {
  return transaction(async client => {
    const claimResult = await client.query<ProjectionClaim>(
      `WITH match_context AS (
         SELECT
           m.match_id,
           m.queue_id,
           (
             SELECT COUNT(*)::SMALLINT
             FROM match_players player
             WHERE player.match_id = m.match_id
               AND player.entry_datetime = m.entry_datetime
               AND lower(COALESCE(player.win_status, ''))
                 IN ('winner', 'win', 'loser', 'loss')
           ) AS eligible_players,
           COALESCE(
             NULLIF(special.stats_scope, ''),
             CASE WHEN casual.match_id IS NOT NULL THEN 'casual' END,
             NULLIF(queue.stats_scope, 'ranked'),
             'other'
           ) AS stats_scope
         FROM matches m
         JOIN match_ingest_status ingest ON ingest.match_id = m.match_id
         LEFT JOIN casual_matches casual ON casual.match_id = m.match_id
         LEFT JOIN special_matches special ON special.match_id = m.match_id
         LEFT JOIN queue_types queue ON queue.queue_id = m.queue_id
         WHERE m.match_id = $1
           AND m.queue_id <> 486
           AND NOT COALESCE(queue.is_ranked, m.is_ranked, false)
           AND (
             ingest.status = 'complete'
             OR ingest.completed_stages @> ARRAY['player_facts']::text[]
           )
         ORDER BY m.entry_datetime DESC
         LIMIT 1
       ),
       claimed AS (
         INSERT INTO item_counts_casual_matches (
           match_id, stats_scope, queue_id, eligible_players
         )
         SELECT match_id, stats_scope, queue_id, eligible_players
         FROM match_context
         ON CONFLICT (match_id) DO NOTHING
         RETURNING stats_scope, queue_id
       )
       SELECT
         'projected'::text AS result,
         claimed.stats_scope,
         claimed.queue_id
       FROM claimed
       UNION ALL
       SELECT
         'already_projected'::text AS result,
         existing.stats_scope,
         existing.queue_id
       FROM item_counts_casual_matches existing
       JOIN match_context context ON context.match_id = existing.match_id
       WHERE existing.match_id = $1
         AND NOT EXISTS (SELECT 1 FROM claimed)
       LIMIT 1`,
      [matchId],
    );
    const claim = claimResult.rows[0];
    if (!claim) {
      throw new Error(
        `Casual item projection rejected match ${matchId}: `
        + 'complete non-ranked canonical facts were not available',
      );
    }
    if (claim.result === 'already_projected') return claim.result;

    await client.query(
      `INSERT INTO item_counts_casual (
         stats_scope, queue_id, item_id, item_name, slot, item_level,
         count, wins, losses, winrate, updated_at
       )
       SELECT
         $2,
         $3,
         item_fact.item_id,
         item.item_name,
         item_fact.slot,
         COALESCE(item_fact.item_level, 0)::SMALLINT,
         COUNT(*)::BIGINT,
         COUNT(*) FILTER (
           WHERE lower(COALESCE(player.win_status, '')) IN ('winner', 'win')
         )::BIGINT,
         COUNT(*) FILTER (
           WHERE lower(COALESCE(player.win_status, '')) IN ('loser', 'loss')
         )::BIGINT,
         ROUND(
           COUNT(*) FILTER (
             WHERE lower(COALESCE(player.win_status, '')) IN ('winner', 'win')
           )::NUMERIC
           / NULLIF(
             COUNT(*) FILTER (
               WHERE lower(COALESCE(player.win_status, ''))
                 IN ('winner', 'win', 'loser', 'loss')
             ),
             0
           )::NUMERIC
           * 100,
           2
         ),
         now()
       FROM match_player_items item_fact
       JOIN match_players player
         ON player.match_id = item_fact.match_id
        AND player.player_id = item_fact.player_id
       JOIN items item ON item.item_id = item_fact.item_id
       WHERE item_fact.match_id = $1
         AND lower(COALESCE(player.win_status, ''))
           IN ('winner', 'win', 'loser', 'loss')
       GROUP BY
         item_fact.item_id,
         item.item_name,
         item_fact.slot,
         COALESCE(item_fact.item_level, 0)
       ON CONFLICT (stats_scope, queue_id, item_id, slot, item_level)
       DO UPDATE SET
         item_name = EXCLUDED.item_name,
         count = item_counts_casual.count + EXCLUDED.count,
         wins = item_counts_casual.wins + EXCLUDED.wins,
         losses = item_counts_casual.losses + EXCLUDED.losses,
         winrate = ROUND(
           (item_counts_casual.wins + EXCLUDED.wins)::NUMERIC
           / NULLIF(
             item_counts_casual.wins + EXCLUDED.wins
             + item_counts_casual.losses + EXCLUDED.losses,
             0
           )::NUMERIC
           * 100,
           2
         ),
         updated_at = EXCLUDED.updated_at`,
      [matchId, claim.stats_scope, claim.queue_id],
    );

    return claim.result;
  });
}
