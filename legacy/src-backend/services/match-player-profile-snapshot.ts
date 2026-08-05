import { query } from '../config/db';
import type { NormalizedPlayerProfile } from './normalizer';

function uniqueMatchIds(matchIds: Iterable<number>): number[] {
  return [...new Set([...matchIds]
    .map(Number)
    .filter((matchId) => Number.isInteger(matchId) && matchId > 0))];
}

/**
 * Attach one post-ingest profile response to every match that queued the
 * player. The match/player row supplies the champion and timestamp, allowing
 * the local champion record to be frozen at that same historical boundary.
 */
export async function persistMatchPlayerProfileSnapshots(
  profile: NormalizedPlayerProfile,
  matchIds: Iterable<number>,
): Promise<void> {
  const normalizedMatchIds = uniqueMatchIds(matchIds);
  if (profile.player_id <= 0 || normalizedMatchIds.length === 0) return;

  await query(
    `INSERT INTO match_player_profile_snapshots (
       match_id,
       player_id,
       captured_at,
       source,
       level,
       platform,
       region,
       global_wins,
       global_losses,
       kbm_tier,
       kbm_points,
       kbm_rank,
       kbm_wins,
       kbm_losses,
       champion_wins,
       champion_losses
     )
     SELECT
       mp.match_id,
       mp.player_id,
       now(),
       'post_match_ingest',
       $3,
       $4,
       $5,
       $6,
       $7,
       $8,
       $9,
       $10,
       $11,
       $12,
       COALESCE(champion_record.wins, 0),
       COALESCE(champion_record.losses, 0)
     FROM match_players mp
     LEFT JOIN LATERAL (
       SELECT
         COUNT(*) FILTER (
           WHERE lower(COALESCE(history.win_status, '')) IN ('winner', 'win')
         )::INTEGER AS wins,
         COUNT(*) FILTER (
           WHERE lower(COALESCE(history.win_status, '')) IN ('loser', 'loss')
         )::INTEGER AS losses
       FROM match_players history
       WHERE history.player_id = mp.player_id
         AND history.champion_id = mp.champion_id
         AND history.is_ranked = TRUE
         AND history.entry_datetime <= mp.entry_datetime
     ) champion_record ON TRUE
     WHERE mp.match_id = ANY($1::BIGINT[])
       AND mp.player_id = $2
     ON CONFLICT (match_id, player_id) DO NOTHING`,
    [
      normalizedMatchIds,
      profile.player_id,
      profile.level,
      profile.platform,
      profile.region,
      profile.wins,
      profile.losses,
      profile.ranked_kbm.tier || profile.tier_ranked_kbm || 0,
      profile.ranked_kbm.points,
      profile.ranked_kbm.rank,
      profile.ranked_kbm.wins,
      profile.ranked_kbm.losses,
    ],
  );
}

/**
 * Recovery may already have fetched and persisted the same profiles moments
 * before the buffer reaches its profile-snapshot stage. Reuse that post-match
 * database state instead of issuing a duplicate getplayerbatch call.
 */
export async function persistRecentPlayerProfileSnapshotsForMatch(
  matchId: number,
  playerIds: Iterable<number>,
  maxAgeMs: number,
): Promise<Set<number>> {
  const normalizedPlayerIds = [...new Set([...playerIds]
    .map(Number)
    .filter((playerId) => Number.isInteger(playerId) && playerId > 0))];
  if (!Number.isInteger(matchId) || matchId <= 0 || normalizedPlayerIds.length === 0) {
    return new Set<number>();
  }

  await query(
    `INSERT INTO match_player_profile_snapshots (
       match_id,
       player_id,
       captured_at,
       source,
       level,
       platform,
       region,
       global_wins,
       global_losses,
       kbm_tier,
       kbm_points,
       kbm_rank,
       kbm_wins,
       kbm_losses,
       champion_wins,
       champion_losses
     )
     SELECT
       mp.match_id,
       mp.player_id,
       player.hirez_profile_refreshed_at,
       'recent_player_cache',
       player.level,
       player.platform,
       player.region,
       player.wins,
       player.losses,
       player.kbm_tier,
       player.kbm_points,
       player.kbm_rank,
       player.kbm_wins,
       player.kbm_losses,
       COALESCE(champion_record.wins, 0),
       COALESCE(champion_record.losses, 0)
     FROM match_players mp
     JOIN matches m ON m.match_id = mp.match_id
     JOIN players player ON player.id = mp.player_id
     LEFT JOIN LATERAL (
       SELECT
         COUNT(*) FILTER (
           WHERE lower(COALESCE(history.win_status, '')) IN ('winner', 'win')
         )::INTEGER AS wins,
         COUNT(*) FILTER (
           WHERE lower(COALESCE(history.win_status, '')) IN ('loser', 'loss')
         )::INTEGER AS losses
       FROM match_players history
       WHERE history.player_id = mp.player_id
         AND history.champion_id = mp.champion_id
         AND history.is_ranked = TRUE
         AND history.entry_datetime <= mp.entry_datetime
     ) champion_record ON TRUE
     WHERE mp.match_id = $1
       AND mp.player_id = ANY($2::BIGINT[])
       AND player.hirez_profile_refreshed_at >= now() - ($3::BIGINT * interval '1 millisecond')
       AND player.hirez_profile_refreshed_at >= m.entry_datetime
         + (COALESCE(m.duration_seconds, 0) * interval '1 second')
     ON CONFLICT (match_id, player_id) DO NOTHING`,
    [matchId, normalizedPlayerIds, maxAgeMs],
  );

  const stored = await query<{ player_id: string | number }>(
    `SELECT player_id
     FROM match_player_profile_snapshots
     WHERE match_id = $1
       AND player_id = ANY($2::BIGINT[])`,
    [matchId, normalizedPlayerIds],
  );
  return new Set(stored.map((row) => Number(row.player_id)));
}
