import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';

function oneOrNone(text: string, params: any[]) {
  return query(text, params).then(r => r.length > 0 ? r[0] : null);
}

function isValidTimeZone(value: string): boolean {
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: value });
    return true;
  } catch {
    return false;
  }
}

function nextDate(date: string): string {
  const [year, month, day] = date.split('-').map(Number);
  return new Date(Date.UTC(year, month - 1, day + 1)).toISOString().slice(0, 10);
}

function zonedDateHourToUtc(date: string, hour: number, timeZone: string): Date {
  const [year, month, day] = date.split('-').map(Number);
  const targetUtcMs = Date.UTC(year, month - 1, day, hour, 0, 0);
  const formatter = new Intl.DateTimeFormat('en-US', {
    timeZone,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  });
  const localAsUtcMs = (timestamp: number) => {
    const parts = Object.fromEntries(formatter.formatToParts(new Date(timestamp))
      .filter((part) => part.type !== 'literal')
      .map((part) => [part.type, part.value]));
    return Date.UTC(Number(parts.year), Number(parts.month) - 1, Number(parts.day), Number(parts.hour), Number(parts.minute), Number(parts.second));
  };
  let candidate = targetUtcMs - (localAsUtcMs(targetUtcMs) - targetUtcMs);
  candidate = targetUtcMs - (localAsUtcMs(candidate) - candidate);
  return new Date(candidate);
}

type MatchCursor = { at: string; id: number };

function encodeMatchCursor(row: { entry_datetime: string | Date; match_id: string | number }): string {
  return Buffer.from(JSON.stringify({
    at: new Date(row.entry_datetime).toISOString(),
    id: Number(row.match_id),
  } satisfies MatchCursor)).toString('base64url');
}

function parseMatchCursor(value: unknown): MatchCursor | null {
  if (value == null || value === '') return null;
  try {
    const parsed = JSON.parse(Buffer.from(String(value), 'base64url').toString('utf8')) as MatchCursor;
    const at = new Date(parsed.at);
    if (!Number.isInteger(parsed.id) || parsed.id <= 0 || Number.isNaN(at.getTime())) return null;
    return { at: at.toISOString(), id: parsed.id };
  } catch {
    return null;
  }
}
import { del, get, set } from '../services/cache';
import { getMatchIdsByQueue, getPlayerBatchFromMatch, getMatchDetailsBatchRaw, getDemoDetails } from '../services/hirez';
import { recordRawHirezResponse } from '../services/raw-hirez-response-audit';
import { reviveRetryableHourlyIngestMatchDebt } from '../workers/hourly-ingest-match-debt';
import { FilterBuilder } from '../utils/filter-builder';
import { appendLobbyTierPredicate, lobbyTierQueryString, parseLobbyTierBounds } from '../utils/lobby-tier';
import { registerReadThroughCache } from '../utils/route-cache';
import { internalRequestHeaders } from '../services/internal-request';
import {
  listDroppedMatches,
  normalizeDroppedMatchDate,
  normalizeDroppedMatchFilters,
  refreshDroppedMatches,
  summarizeDroppedMatches,
} from '../services/dropped-matches';
import {
  ingestRequestedMatchesDetailed,
  type RequestedMatchIngestionResult,
} from '../workers/requested-match-ingestion';
import { guardVendorFallback } from '../services/request-security';
import { RANKED_STATS_QUEUE_ID } from '../workers/ranked-stats-policy';
import { ensureMatchCountDiscoveryTables } from '../workers/match-count-discovery';
import { MATCH_COUNT_QUEUE_DEFINITIONS } from '../workers/match-count-discovery-policy';
import {
  overlayCurrentPlayerModeration,
  type StoredPlayerModeration,
  type StoredPrivateModeration,
} from '../services/match-moderation';

const CACHE_TTL_MATCH = 3600;
// Bump when a persisted match shape changes so repaired result shells do not
// remain hidden behind an older Redis detail entry.
const MATCH_DETAIL_CACHE_VERSION = 13;

export { fetchMatches };

class RequestedMatchReadThroughError extends Error {
  constructor(public readonly result: RequestedMatchIngestionResult) {
    super(
      result.error
      || (
        result.status === 'not_found'
          ? `Match ${result.matchId} was not found by Hi-Rez`
          : `Match ${result.matchId} could not be reconstructed`
      ),
    );
    this.name = 'RequestedMatchReadThroughError';
  }
}

/**
 * Fetch match details for 1–10 match IDs.
 * Reads PostgreSQL first. Callers may opt into the normal requested-match
 * ingestion/recovery fallback for missing or incomplete IDs.
 */
async function fetchMatches(
  matchIds: number[],
  options: {
    allowHirezFallback?: boolean;
    beforeHirezFallback?: (matchIds: number[]) => Promise<void>;
    strictReadThrough?: boolean;
    forceRefresh?: boolean;
  } = { allowHirezFallback: false },
): Promise<any> {
  const normalizedMatchIds = [...new Set(
    matchIds
      .map(id => Number(id))
      .filter(id => Number.isFinite(id) && id > 0)
  )];

  if (normalizedMatchIds.length === 0) {
    return { matches: [], count: 0, notFound: matchIds };
  }
  const forceRefresh = options.forceRefresh === true;

  // 1. Check which matches are already in DB
  const dbResults = await query(
    `SELECT m.match_id, m.broken, m.recovered, mis.status AS ingest_status,
       mis.completed_stages AS ingest_completed_stages
     FROM matches m
     LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
     WHERE m.match_id = ANY($1)
     UNION ALL
     SELECT cm.match_id,false,false,'complete'::text,ARRAY['player_facts']::text[]
     FROM casual_matches cm WHERE cm.match_id = ANY($1)
     UNION ALL
     SELECT sm.match_id,false,false,'complete'::text,ARRAY['player_facts']::text[]
     FROM special_matches sm WHERE sm.match_id = ANY($1)`,
    [normalizedMatchIds]
  );
  // PostgreSQL BIGINT values are returned by node-postgres as strings. The
  // request path and batch APIs use JavaScript numbers. Keep this comparison
  // boundary normalized or a perfectly healthy local match row looks "missing"
  // and the read endpoint falls through to Hi-Rez, which is both expensive and
  // fragile for broken-skin matches that can throw Int16 vendor errors.
  const dbMatchId = (row: any) => Number(row.match_id);
  const inDb = new Set(dbResults.map(dbMatchId));
  const brokenInDb = new Set(dbResults.filter((r: any) => r.broken).map(dbMatchId));
  const incompleteBrokenInDb = new Set(dbResults
    // recovered=false can be a valid, complete private-placeholder match. Only
    // retry when match/player facts are still incomplete. Derived projections
    // deliberately run after the match becomes available from the database.
    .filter((r: any) => {
      const completedStages = Array.isArray(r.ingest_completed_stages)
        ? r.ingest_completed_stages
        : [];
      return r.broken && !r.recovered
        && !['complete', 'limited'].includes(r.ingest_status)
        && !(
          completedStages.includes('player_facts')
          && completedStages.includes('match_bans')
        );
    })
    .map(dbMatchId));

  // Direct match lookup is allowed to promote an incomplete DB row. This is
  // especially important for casual/non-ranked matches discovered only as
  // low-authority `prefetch` rows from a player's 50-match history: a user
  // opening that match should trigger one targeted detail/recovery attempt,
  // then replace the partial row with direct/recovered data. This is still
  // request-scoped, not a cron fan-out over every history match ID.
  const missingOrBroken = forceRefresh
    ? normalizedMatchIds.filter(id => !inDb.has(id) || brokenInDb.has(id))
    : normalizedMatchIds.filter(id => !inDb.has(id) || incompleteBrokenInDb.has(id));

  // 2. Clean up stale failed buffer entries for broken matches being re-fetched
  // so the recovery pipeline can re-run (test mode only)
  if (forceRefresh && brokenInDb.size > 0) {
    await query(
      `DELETE FROM raw_ingest_buffer WHERE entity_id = ANY($1::text[]) AND status = 'failed'`,
      [Array.from(brokenInDb)]
    );
  }

  // 3. Fetch missing/broken matches through the same durable buffer processor
  // used by hourly ingestion. A broken singleton enters the relay's canonical
  // queue-appropriate recovery flow directly; it does not repeat the parser
  // failure through getmatchdetails.
  const requestedOutcomes = new Map<number, RequestedMatchIngestionResult>();
  if (missingOrBroken.length > 0 && options.allowHirezFallback === true) {
    await options.beforeHirezFallback?.(missingOrBroken);
    const outcomes = await ingestRequestedMatchesDetailed(missingOrBroken);
    for (const outcome of outcomes) requestedOutcomes.set(outcome.matchId, outcome);
    const completedIds = outcomes
      .filter(outcome => outcome.status === 'ready')
      .map(outcome => outcome.matchId);
    await Promise.all(completedIds.map(matchId => del(`match:v${MATCH_DETAIL_CACHE_VERSION}:${matchId}`)));

    if (options.strictReadThrough) {
      const failed = outcomes.find(outcome => outcome.status !== 'ready');
      if (failed) throw new RequestedMatchReadThroughError(failed);
    }
  }

  // 4. Format results from DB (with cache)
  const matches: any[] = [];
  const notFound: number[] = [];

  for (const matchId of normalizedMatchIds) {
    // A failed requested-ingestion attempt may have written an incomplete core
    // shell before recovery validation stopped. Never expose that shell as a
    // successful read-through response. Only a durable ready/limited fact
    // boundary may be formatted after an upstream miss.
    const requestedOutcome = requestedOutcomes.get(matchId);
    if (requestedOutcome && requestedOutcome.status !== 'ready') {
      notFound.push(matchId);
      continue;
    }
    const result = await formatMatchResult(matchId);
    if (result) {
      matches.push(result);
    } else {
      notFound.push(matchId);
    }
  }

  return {
    matches,
    count: matches.length,
    notFound: notFound.length > 0 ? notFound : undefined,
  };
}

/**
 * Format a single match result for the API response.
 * Fetches players and bans from DB (persisted by getMatchDetailsBatch).
 */
async function formatMatchResult(matchId: number): Promise<any> {
  const cacheKey = `match:v${MATCH_DETAIL_CACHE_VERSION}:${matchId}`;
  const cached = await get(cacheKey);
  if (cached) return withCurrentPlayerModeration(cached);

  const match = await one('SELECT * FROM matches WHERE match_id = $1', [matchId]);
  if (!match) return formatNonrankedMatchResult(matchId, cacheKey);

  // The match detail page is the authoritative read surface for a single match.
  // Keep this query intentionally broad: the ingest pipeline already normalizes
  // every useful Hi-Rez player stat into match_players, and the frontend should
  // not need a second endpoint just to display GPM/DPM/HPM/mitigation/tier/etc.
  // Source tables affected: matches, match_players, match_bans.
  const players = await query(
    `SELECT mp.*, c.name AS champion_name,
            COALESCE(NULLIF(mp.private_player_id, 0), o.private_player_id) AS private_player_id,
            pp.alias AS private_account_alias,
            pp.verified_name AS private_account_verified_name,
            COALESCE(pp.verified_name, pp.alias) AS private_account_display_name,
            CASE WHEN mp.player_id > 0 THEN jsonb_build_object(
              'captured_at', COALESCE(
                current_player.hirez_profile_refreshed_at,
                current_player.last_updated,
                profile_snapshot.captured_at,
                mp.created_at,
                mp.entry_datetime
              ),
              'source', CASE
                WHEN current_player.id IS NOT NULL THEN 'players_database'
                WHEN profile_snapshot.player_id IS NOT NULL THEN profile_snapshot.source
                ELSE 'match_player'
              END,
              'level', COALESCE(NULLIF(current_player.level, 0), profile_snapshot.level, NULLIF(mp.account_level, 0)),
              'platform', COALESCE(NULLIF(current_player.platform, ''), profile_snapshot.platform, NULLIF(mp.platform, '')),
              'region', COALESCE(
                NULLIF(NULLIF(current_player.region, ''), 'Unknown'),
                NULLIF(NULLIF(profile_snapshot.region, ''), 'Unknown'),
                NULLIF(NULLIF(mp.region, ''), 'Unknown'),
                mp.region
              ),
              'global_wins', COALESCE(current_player.wins, profile_snapshot.global_wins),
              'global_losses', COALESCE(current_player.losses, profile_snapshot.global_losses),
              'kbm_tier', COALESCE(current_player.kbm_tier, profile_snapshot.kbm_tier, NULLIF(mp.league_tier, 0), mp.league_tier),
              'kbm_points', COALESCE(current_player.kbm_points, profile_snapshot.kbm_points, NULLIF(mp.league_points, 0), mp.league_points),
              'kbm_rank', CASE
                -- RankedKBM.Rank on the player profile is not the Master
                -- leaderboard position (it is commonly 1 or 2). Grandmaster
                -- is earned only by a current tier-26 leaderboard rank in the
                -- top 100; rank 101+ remains Master.
                WHEN COALESCE(current_player.kbm_tier, profile_snapshot.kbm_tier, NULLIF(mp.league_tier, 0), mp.league_tier) = 26
                  THEN CASE WHEN current_leaderboard.tier = 26 THEN current_leaderboard.rank ELSE NULL END
                ELSE COALESCE(current_leaderboard.rank, current_player.kbm_rank, profile_snapshot.kbm_rank)
              END,
              'kbm_wins', COALESCE(current_player.kbm_wins, profile_snapshot.kbm_wins, NULLIF(mp.league_wins, 0), mp.league_wins),
              'kbm_losses', COALESCE(current_player.kbm_losses, profile_snapshot.kbm_losses, NULLIF(mp.league_losses, 0), mp.league_losses),
              'champion_wins', COALESCE(current_champion.wins, profile_snapshot.champion_wins),
              'champion_losses', COALESCE(current_champion.losses, profile_snapshot.champion_losses),
              'queue_elo', COALESCE(current_queue_rating.mu, rating_snapshot.queue_mu_post),
              'champion_elo', COALESCE(current_champion_rating.mu, rating_snapshot.champ_mu_post),
              'cheater', COALESCE(current_player.cheater, FALSE),
              'sus_count', COALESCE(current_player.sus_count, 0),
              'verified', EXISTS (
                SELECT 1 FROM users verified_user
                WHERE verified_user.linked_player_id = mp.player_id
              )
            ) WHEN COALESCE(NULLIF(mp.private_player_id, 0), o.private_player_id) > 0 THEN jsonb_build_object(
              'captured_at', COALESCE(pp.updated_at, pp.last_seen, mp.created_at, mp.entry_datetime),
              'source', 'private_account_database',
              'level', COALESCE(NULLIF(pp.account_level, 0), NULLIF(mp.account_level, 0)),
              'platform', NULLIF(mp.platform, ''),
              'region', NULLIF(NULLIF(mp.region, ''), 'Unknown'),
              'global_wins', NULL,
              'global_losses', NULL,
              'kbm_tier', COALESCE(NULLIF(pp.league_tier, 0), NULLIF(mp.league_tier, 0)),
              'kbm_points', COALESCE(pp.league_points, mp.league_points),
              'kbm_rank', NULL,
              'kbm_wins', NULL,
              'kbm_losses', NULL,
              'champion_wins', NULL,
              'champion_losses', NULL,
              'queue_elo', NULL,
              'champion_elo', NULL,
              'cheater', COALESCE(pp.cheater, FALSE),
              'sus_count', COALESCE(pp.sus_count, 0),
              'verified', FALSE
            ) ELSE NULL END AS profile_snapshot
     FROM match_players mp
     LEFT JOIN champions c ON c.id = mp.champion_id
     LEFT JOIN players current_player ON current_player.id = mp.player_id
     -- Keep rank enrichment bounded to one primary-key lookup per match player.
     -- LIMIT prevents PostgreSQL from turning this small correlated lookup into
     -- a full leaderboard hash scan as leaderboard_current grows.
     LEFT JOIN LATERAL (
       SELECT leaderboard.tier, leaderboard.rank
       FROM leaderboard_current leaderboard
       WHERE leaderboard.player_id = mp.player_id
       LIMIT 1
     ) current_leaderboard ON TRUE
     LEFT JOIN match_player_profile_snapshots profile_snapshot
       ON profile_snapshot.match_id = mp.match_id
      AND profile_snapshot.player_id = mp.player_id
     LEFT JOIN match_rating_snapshots rating_snapshot
       ON rating_snapshot.match_id = mp.match_id
      AND rating_snapshot.player_id = mp.player_id
      AND rating_snapshot.champion_id = mp.champion_id
     LEFT JOIN player_champions current_champion
       ON current_champion.player_id = mp.player_id
      AND current_champion.champion_id = mp.champion_id
     LEFT JOIN player_queue_ratings current_queue_rating
       ON current_queue_rating.player_id = mp.player_id
      AND current_queue_rating.queue_id = 486
     LEFT JOIN player_champion_ratings current_champion_rating
       ON current_champion_rating.player_id = mp.player_id
      AND current_champion_rating.champion_id = mp.champion_id
     LEFT JOIN private_account_observations o
       ON mp.player_id = 0
      AND o.match_id = mp.match_id
      AND (o.private_slot = mp.private_slot OR (mp.private_slot = 0 AND o.private_slot = 1))
     LEFT JOIN players_private pp
       ON pp.id = COALESCE(NULLIF(mp.private_player_id, 0), o.private_player_id)
     WHERE mp.match_id = $1
       AND mp.entry_datetime = $2::timestamptz
     ORDER BY mp.task_force, mp.player_id, mp.private_slot`,
    [matchId, match.entry_datetime]
  );
  const bans = await query(
    `SELECT mb.match_id, mb.ban_slot, mb.champion_id, c.name AS champion_name
     FROM match_bans mb
     LEFT JOIN champions c ON c.id = mb.champion_id
     WHERE mb.match_id = $1
     ORDER BY mb.ban_slot`,
    [matchId],
  );
  const result = { match, players, bans };
  await set(cacheKey, result, CACHE_TTL_MATCH);
  return withCurrentPlayerModeration(result);
}

async function formatNonrankedMatchResult(matchId: number, cacheKey: string): Promise<any> {
  const match = await one(
    `SELECT match_id,entry_datetime,queue_id,false AS is_ranked,duration_seconds,
            region,map,team1_score,team2_score,winning_task_force,false AS has_replay,
            quality <> 'complete' AS broken,false AS recovered,
            quality <> 'complete' AS limited,
            CASE WHEN quality='complete' THEN NULL ELSE quality END AS limited_reason,
            source,ingested_at,quality,stats_eligible
     FROM casual_matches WHERE match_id=$1
     UNION ALL
     SELECT match_id,entry_datetime,queue_id,false AS is_ranked,duration_seconds,
            region,map,team1_score,team2_score,winning_task_force,false AS has_replay,
            quality <> 'complete' AS broken,false AS recovered,
            quality <> 'complete' AS limited,
            CASE WHEN quality='complete' THEN NULL ELSE quality END AS limited_reason,
            source,ingested_at,quality,stats_eligible
     FROM special_matches WHERE match_id=$1
     LIMIT 1`,
    [matchId],
  );
  if (!match) return null;
  const players = await query(
    `WITH facts AS (
       SELECT match_id,roster_slot,private_slot,player_id,private_player_id,
              player_name,champion_id,champion_name,task_force,win_status,
              kills,deaths,assists,damage,damage_taken,healing,mitigation,
              credits,objective_time,account_level,mastery_level,party_id,
              portal_id,portal_user_id,platform,participant_kind,source
       FROM casual_match_players WHERE match_id=$1
       UNION ALL
       SELECT match_id,roster_slot,private_slot,player_id,private_player_id,
              player_name,champion_id,champion_name,task_force,win_status,
              kills,deaths,assists,damage,damage_taken,healing,mitigation,
              credits,objective_time,account_level,mastery_level,party_id,
              portal_id,portal_user_id,platform,participant_kind,source
       FROM special_match_players WHERE match_id=$1
     ),
     party_groups AS (
       SELECT party_id
       FROM facts
       WHERE party_id > 0
       GROUP BY party_id
       HAVING COUNT(*) > 1
     ),
     party_numbered AS (
       SELECT party_id, ROW_NUMBER() OVER (ORDER BY party_id) AS party_num
       FROM party_groups
     )
     SELECT f.match_id,f.player_id,f.private_slot,f.player_name,f.champion_id,
            COALESCE(c.name,f.champion_name) AS champion_name,f.task_force,f.win_status,
            f.kills,f.deaths,f.assists,f.damage AS damage_done_physical,
            f.damage_taken,f.healing,f.mitigation AS damage_mitigated,
            f.credits AS gold_earned,f.objective_time AS objective_assists,
            f.account_level,f.mastery_level,f.party_id,
            COALESCE(party_numbered.party_num, 0) AS party,
            f.portal_id,
            f.portal_user_id,f.platform,f.participant_kind,f.source,
            f.private_player_id,pp.alias AS private_account_alias,
            pp.verified_name AS private_account_verified_name,
            COALESCE(pp.verified_name,pp.alias) AS private_account_display_name,
            CASE WHEN f.player_id > 0 THEN jsonb_build_object(
              'source','players_database',
              'level',COALESCE(NULLIF(p.level,0),NULLIF(f.account_level,0)),
              'platform',COALESCE(NULLIF(p.platform,''),NULLIF(f.platform,'')),
              'region',NULLIF(NULLIF(p.region,''),'Unknown'),
              'global_wins',p.wins,'global_losses',p.losses,
              'kbm_tier',p.kbm_tier,'kbm_points',p.kbm_points,
              'cheater',COALESCE(p.cheater,false),'sus_count',COALESCE(p.sus_count,0),
              'verified',EXISTS (SELECT 1 FROM users u WHERE u.linked_player_id=f.player_id)
            ) WHEN f.private_player_id IS NOT NULL THEN jsonb_build_object(
              'source','private_account_database','level',COALESCE(NULLIF(pp.account_level,0),NULLIF(f.account_level,0)),
              'platform',NULLIF(f.platform,''),'kbm_tier',NULL,'kbm_points',NULL,
              'queue_elo',NULL,'champion_elo',NULL,'cheater',COALESCE(pp.cheater,false),
              'sus_count',COALESCE(pp.sus_count,0),'verified',false
            ) ELSE NULL END AS profile_snapshot
     FROM facts f
     LEFT JOIN champions c ON c.id=f.champion_id
     LEFT JOIN players p ON p.id=f.player_id
     LEFT JOIN players_private pp ON pp.id=f.private_player_id
     LEFT JOIN party_numbered ON party_numbered.party_id=f.party_id
     ORDER BY f.task_force,f.roster_slot`,
    [matchId],
  );
  const result = { match, players, bans: [] };
  await set(cacheKey, result, CACHE_TTL_MATCH);
  return withCurrentPlayerModeration(result);
}

/**
 * Match facts are immutable enough to cache for an hour; moderation is not.
 * Overlay the canonical stored player flags whenever a cached match is read so
 * the response never republishes an hour-old cheater or suspicious state. This
 * is a PostgreSQL-only lookup and never triggers a Hi-Rez profile refresh.
 */
async function withCurrentPlayerModeration(result: any): Promise<any> {
  const players = Array.isArray(result?.players) ? result.players : [];
  const playerIds = [...new Set(
    players
      .map((player: any) => Number(player?.player_id))
      .filter((playerId: number) => Number.isSafeInteger(playerId) && playerId > 0),
  )];
  const privateIds = [...new Set(
    players
      .filter((player: any) => Number(player?.player_id) === 0)
      .map((player: any) => Number(player?.private_player_id))
      .filter((privateId: number) => Number.isSafeInteger(privateId) && privateId > 0),
  )];
  if (playerIds.length === 0 && privateIds.length === 0) return result;

  const [rows, privateRows] = await Promise.all([
    playerIds.length > 0
      ? query<StoredPlayerModeration>(
          `SELECT player.id, player.cheater, player.sus_count,
                  EXISTS (SELECT 1 FROM users verified_user WHERE verified_user.linked_player_id = player.id) AS verified
           FROM players player
           WHERE player.id = ANY($1)`,
          [playerIds],
        )
      : Promise.resolve([]),
    privateIds.length > 0
      ? query<StoredPrivateModeration>('SELECT id, cheater, sus_count FROM players_private WHERE id = ANY($1) AND is_active', [privateIds])
      : Promise.resolve([]),
  ]);
  return overlayCurrentPlayerModeration(result, rows, privateRows);
}

function imageAssetSegment(name: unknown): string | null {
  if (typeof name !== 'string') return null;
  const trimmed = name.trim();
  if (!trimmed) return null;
  // Keep valid filename punctuation such as `!` and apostrophes. The previous
  // broad punctuation strip turned real assets like Card_Never_Surrender!.avif
  // into a guaranteed 404. Remove only path/Windows-reserved characters.
  return trimmed.replace(/[<>:"/\\|?*\x00-\x1F]/g, '').replace(/\s+/g, '_');
}

function spacedAssetSegment(name: unknown): string | null {
  if (typeof name !== 'string') return null;
  const trimmed = name.trim();
  // Published talent assets omit commas because Next's production static-file
  // handler does not serve comma-containing local paths. Keep spaces, !, and
  // apostrophes so the path still matches the canonical champion asset map.
  return trimmed ? trimmed.replace(/,/g, '') : null;
}

function itemIconUrl(itemName: unknown): string | null {
  const segment = imageAssetSegment(itemName);
  return segment ? `/images/items/${segment}_Icon.avif` : null;
}

function itemFallbackIconUrl(itemName: unknown): string | null {
  const segment = imageAssetSegment(itemName);
  return segment ? `/images/items/${segment}_Icon.png` : null;
}

function cardIconUrl(cardName: unknown): string | null {
  const segment = imageAssetSegment(cardName);
  return segment ? `/images/cards/Card_${segment}.avif` : null;
}

function cardFallbackIconUrl(cardName: unknown): string | null {
  const segment = imageAssetSegment(cardName);
  // Match pages use local card assets named with the same underscore convention
  // as champion pages: Card_Survival.avif / Card_Survival.png. Do not emit the
  // older space-form fallback (Card Survival.avif); Next treats that as a real
  // different filename and every resolved card would log a noisy 404 before the
  // browser falls back.
  return segment ? `/images/cards/Card_${segment}.png` : null;
}

function talentIconUrl(championName: unknown, talentName: unknown): string | null {
  const champion = spacedAssetSegment(championName);
  const talent = spacedAssetSegment(talentName);
  if (!champion || !talent) return null;
  const assetName = champion === 'Seris' && talent === 'Resuscitate' ? 'Seris Soul Collector' : `${champion} ${talent}`;
  return `/images/champions/Talent ${assetName}.avif`;
}

function talentFallbackIconUrl(championName: unknown, talentName: unknown): string | null {
  const champion = spacedAssetSegment(championName);
  const talent = spacedAssetSegment(talentName);
  if (!champion || !talent) return null;
  const assetName = champion === 'Seris' && talent === 'Resuscitate' ? 'Seris Soul Collector' : `${champion} ${talent}`;
  return `/images/champions/Talent ${assetName}.png`;
}

const ACTIVITY_STALE_TTL_SECONDS = 6 * 60 * 60;

function matchesCacheFreshTtlSeconds(req: { url: string }): number {
  const pathname = new URL(req.url, 'http://paladinscat.local').pathname;
  return pathname === '/matches/overview' || pathname === '/matches/hourly-stats'
    ? 60
    : 300;
}

function matchesCacheStaleTtlSeconds(req: { url: string }): number {
  const url = new URL(req.url, 'http://paladinscat.local');
  if (url.pathname === '/matches/overview' && url.searchParams.get('view') === 'activity-v3') {
    return ACTIVITY_STALE_TTL_SECONDS;
  }
  return matchesCacheFreshTtlSeconds(req) * 3;
}

export default async function matchesRoutes(fastify: FastifyInstance) {
  registerReadThroughCache(fastify, {
    namespace: 'route:matches',
    shouldCache: (req) => (
      req.url.startsWith('/matches/overview')
      ||
      req.url.startsWith('/matches/recent')
      || req.url.startsWith('/matches/queue/')
      || req.url.startsWith('/matches/search')
      || req.url.startsWith('/matches/bans')
      || req.url.startsWith('/matches/hourly-stats')
      || req.url.startsWith('/matches/compositions')
    ),
    ttlSeconds: matchesCacheFreshTtlSeconds,
    staleTtlSeconds: matchesCacheStaleTtlSeconds,
  });

  /**
   * GET /matches/overview — Recent matches plus live ingest-health chart.
   *
   * The former page requested hourly stats then two requests per visible date.
   * Each dropped-match response already contains both its summary and rows, so
   * this combines the work into a single cached browser request.
   */
  fastify.get('/overview', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const tierQuery = lobbyTierQueryString(lobbyTier);
    const hourlyQuery = [
      tierQuery,
      req.query?.view === 'activity-v3' ? 'includePlayers=true' : '',
    ].filter(Boolean).join('&');
    reply.header('Cache-Control', 'public, max-age=60, stale-while-revalidate=180');
    const [hourlyResponse, recentResponse] = await Promise.all([
      fastify.inject({ method: 'GET', url: `/matches/hourly-stats${hourlyQuery ? `?${hourlyQuery}` : ''}`, headers: internalRequestHeaders() }),
      fastify.inject({ method: 'GET', url: '/matches/recent?limit=20', headers: internalRequestHeaders() }),
    ]);
    const hourly = hourlyResponse.statusCode < 400 ? hourlyResponse.json() : null;
    const activityView = req.query?.view === 'activity-v3';
    // A transient aggregate failure used to become a successful overview with
    // `hourly: null`, which the route cache retained for six hours. Activity
    // has a stable 24-bucket contract, so reject an incomplete composition:
    // the cache will retain its last valid response and a cold miss is visible
    // to the client as a retryable outage rather than "no match activity".
    if (activityView && (!hourly || !Array.isArray(hourly.hourly) || hourly.hourly.length !== 24)) {
      fastify.log.warn(
        { statusCode: hourlyResponse.statusCode },
        'Activity overview hourly source unavailable',
      );
      return reply.status(503).send({ error: 'Activity data is temporarily unavailable' });
    }
    const dates = Array.from(new Set((hourly.hourly ?? []).map((entry: any) => entry.date).filter(Boolean))) as string[];
    // Dropped matches have no complete player roster and therefore no
    // authoritative average lobby tier. Keep them visible in the all-lobbies
    // operator view, but never attribute them to a selected tier range.
    const droppedDays = lobbyTier.active ? [] : await Promise.all(dates.map(async (date) => {
      const response = await fastify.inject({
        method: 'GET',
        // Public overview requests are reads. Projection refreshes belong to
        // the ingest/repair workers, never to cache misses on a GET route.
        url: `/matches/dropped?date=${encodeURIComponent(date)}&queueId=486&status=dropped&limit=500&refresh=false`,
        headers: internalRequestHeaders(),
      });
      if (response.statusCode >= 400) {
        fastify.log.warn({ date, statusCode: response.statusCode }, 'Matches overview dropped source failed');
        return null;
      }
      return response.json();
    }));
    const droppedByHour: Record<string, number> = {};
    const droppedIdsByHour: Record<string, string[]> = {};
    for (const day of droppedDays) {
      if (!day) continue;
      for (const entry of day.summary ?? []) {
        droppedByHour[`${day.date}|${entry.hour}`] = Number(entry.dropped ?? 0);
      }
      for (const match of day.matches ?? []) {
        const key = `${day.date}|${match.hour}`;
        droppedIdsByHour[key] = [...(droppedIdsByHour[key] ?? []), String(match.match_id)];
      }
    }
    return {
      hourly,
      recent: recentResponse.statusCode < 400 ? recentResponse.json() : [],
      dropped_by_hour: droppedByHour,
      dropped_ids_by_hour: droppedIdsByHour,
    };
  });

  /**
   * GET /matches/dropped/summary
   *
   * Operator-facing health projection for match IDs discovered by the ranked
   * hourly ingest pipeline but not yet safely converted into authoritative
   * match rows. This deliberately reads from the canonical debt/status tables
   * (`hourly_ingest_match_debt`, `match_ingest_status`) and writes only to the
   * `dropped_matches` projection table; workers must never use this endpoint as
   * a queue. The goal is visibility into possible drop-hack/corrupt-history
   * cases by UTC fetch hour without creating another source of truth.
   */
  fastify.get('/dropped/summary', async (req: any, reply) => {
    try {
      const date = normalizeDroppedMatchDate(req.query?.date);
      const queueId = Number.isFinite(Number(req.query?.queueId)) ? Number(req.query.queueId) : 486;
      const shouldRefresh = String(req.query?.refresh ?? 'true').toLowerCase() !== 'false';
      const refreshed = shouldRefresh ? await refreshDroppedMatches(date, queueId) : 0;
      const summary = await summarizeDroppedMatches(date, queueId);
      return { date, queue_id: queueId, refreshed, summary };
    } catch (err: any) {
      return reply.status(400).send({ error: err?.message || 'Invalid dropped match summary request' });
    }
  });

  /**
   * GET /matches/dropped
   *
   * Lists tracked match IDs, their UTC hour, retry state, observed history
   * anchors, and the latest local ingest/raw-buffer error. Use `status=open`
   * (default) for unresolved debt, `status=all` for the full projection, and
   * optional `hour`, `category`, `limit`, and `offset` filters when auditing a
   * specific poisoned hourly batch.
   */
  fastify.get('/dropped', async (req: any, reply) => {
    try {
      const filters = normalizeDroppedMatchFilters(req.query || {});
      const shouldRefresh = String(req.query?.refresh ?? 'true').toLowerCase() !== 'false';
      const refreshed = shouldRefresh ? await refreshDroppedMatches(filters.date, filters.queueId) : 0;
      const [summary, matches] = await Promise.all([
        summarizeDroppedMatches(filters.date, filters.queueId),
        listDroppedMatches(filters),
      ]);
      return {
        date: filters.date,
        queue_id: filters.queueId,
        status: filters.status || 'open',
        category: filters.category || null,
        hour: filters.hour ?? null,
        refreshed,
        count: matches.length,
        summary,
        matches,
      };
    } catch (err: any) {
      return reply.status(400).send({ error: err?.message || 'Invalid dropped match request' });
    }
  });

  /**
   * GET /matches/:id - Get a single match by ID.
   *
   * This public read is database-first. A profile history observation can land
   * before hourly discovery; opening that match runs the same durable requested
   * ingest/recovery pipeline, waits for completion, and then serves the result.
   *
   * Response format: { matches: [...], count: N, notFound: [...] }
   */
  fastify.get('/:id', async (req: any, reply) => {
    const rawMatchId = String((req.params as any).id ?? '');
    const matchId = /^\d+$/.test(rawMatchId) ? Number(rawMatchId) : Number.NaN;
    if (!Number.isSafeInteger(matchId) || matchId <= 0) {
      return reply.status(400).send({ error: 'Invalid match ID' });
    }
    try {
      return await fetchMatches([matchId], {
        // Exact IDs are safe read-through keys. A database/cache hit remains
        // local; a miss enters the relay-backed requested recovery pipeline.
        // The response is assembled only after normalized facts cross the
        // durable boundary—never from the raw vendor response.
        allowHirezFallback: String(req.method).toUpperCase() === 'GET',
        strictReadThrough: String(req.method).toUpperCase() === 'GET',
        beforeHirezFallback: (missingIds) => guardVendorFallback(req, reply, {
          scope: 'requested-match',
          entity: missingIds[0],
        }),
      });
    } catch (error) {
      if (!(error instanceof RequestedMatchReadThroughError)) throw error;
      const { result } = error;
      if (result.status === 'not_found') {
        return reply.status(404).send({
          error: {
            code: 'MATCH_NOT_FOUND',
            message: `Match ${matchId} was not found.`,
            matchId,
          },
        });
      }
      const statusCode = result.status === 'processing_timeout' ? 504 : 502;
      return reply.status(statusCode).send({
        error: {
          code: result.status === 'processing_timeout'
            ? 'MATCH_RECOVERY_TIMEOUT'
            : 'MATCH_RECOVERY_FAILED',
          message: result.status === 'processing_timeout'
            ? `Match ${matchId} recovery did not reach the durable fact boundary in time.`
            : `Match ${matchId} could not be reconstructed from the Hi-Rez relay response.`,
          matchId,
        },
      });
    }
  });

  /**
   * GET /matches/dropped/nonranked
   *
   * Public terminal ledger for hourly non-ranked discovery. Unlike Ranked's
   * recovery-debt projection, these rows have already spent their single
   * roster fallback and will not consume more vendor calls.
   */
  fastify.get('/dropped/nonranked', async (req: any, reply) => {
    const date = req.query?.date == null ? null : String(req.query.date).trim();
    if (date && !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return reply.status(400).send({ error: 'date must be YYYY-MM-DD' });
    }
    const scope = req.query?.scope == null ? null : String(req.query.scope).trim().toLowerCase();
    const hour = req.query?.hour == null || req.query.hour === '' ? null : Number(req.query.hour);
    if (hour != null && (!Number.isInteger(hour) || hour < 0 || hour > 23)) {
      return reply.status(400).send({ error: 'hour must be an integer from 0 to 23' });
    }
    const limit = Math.min(Math.max(Number(req.query?.limit) || 500, 1), 2000);
    const offset = Math.max(Number(req.query?.offset) || 0, 0);
    const params: any[] = [];
    const where = [`a.status='dropped'`];
    if (date) {
      params.push(date);
      where.push(`a.source_date=$${params.length}::date`);
    }
    if (scope) {
      params.push(scope);
      where.push(`a.stats_scope=$${params.length}`);
    }
    if (hour != null) {
      params.push(hour);
      where.push(`a.source_hour=$${params.length}`);
    }
    params.push(limit, offset);
    const rows = await query(
      `SELECT a.match_id::text,a.source_date::text AS date,a.source_hour AS hour,
              a.queue_id,q.queue_name,a.stats_scope,a.region,
              a.discovered_entry_datetime,a.quality,a.direct_player_count,
              a.roster_player_count,a.detail_attempts,a.roster_attempts,
              a.terminal_reason,a.completed_at
       FROM nonranked_match_acquisition a
       JOIN queue_types q ON q.queue_id=a.queue_id
       WHERE ${where.join(' AND ')}
       ORDER BY a.source_date DESC,a.source_hour DESC,a.match_id DESC
       LIMIT $${params.length - 1} OFFSET $${params.length}`,
      params,
    );
    const summaryParams = params.slice(0, params.length - 2);
    const summary = await query(
      `SELECT a.source_date::text AS date,a.source_hour AS hour,a.stats_scope,
              a.queue_id,q.queue_name,COUNT(*)::int AS dropped
       FROM nonranked_match_acquisition a
       JOIN queue_types q ON q.queue_id=a.queue_id
       WHERE ${where.join(' AND ')}
       GROUP BY a.source_date,a.source_hour,a.stats_scope,a.queue_id,q.queue_name
       ORDER BY a.source_date DESC,a.source_hour DESC,a.stats_scope,a.queue_id`,
      summaryParams,
    );
    return { date, scope, hour, count: rows.length, summary, matches: rows };
  });

  /**
   * GET /matches/batch - Get 1 to 10 match details.
   *
   * Query parameter: ids=1234,5678,9012 (comma-separated, up to 10)
   *
   * This public batch read is database-only. It never launches discovery or
   * recovery work for missing IDs.
   *
   * Response format: { matches: [...], count: N, notFound: [...] }
   */
  fastify.get('/batch', async (req, reply) => {
    const idsParam = (req.query as any).ids as string;
    if (!idsParam) {
      return reply.status(400).send({ error: 'Missing ids query parameter' });
    }

    const matchIds = idsParam.split(',')
      .map(s => parseInt(s.trim(), 10))
      .filter(id => !isNaN(id));

    if (matchIds.length === 0) {
      return reply.status(400).send({ error: 'Invalid match IDs' });
    }
    if (matchIds.length > 10) {
      return reply.status(400).send({ error: 'Maximum 10 match IDs per request', received: matchIds.length });
    }

    return fetchMatches(matchIds, { allowHirezFallback: false });
  });

  fastify.get('/recent', async (req: any, reply: any) => {
    const requestedLimit = parseInt(req.query.limit as string, 10);
    const limit = Number.isInteger(requestedLimit) && requestedLimit > 0
      ? Math.min(requestedLimit, 100)
      : 20;
    const cursor = parseMatchCursor(req.query.cursor);
    if (req.query.cursor && !cursor) return reply.status(400).send({ error: 'Invalid cursor' });
    const params: any[] = [RANKED_STATS_QUEUE_ID];
    const where = ['m.queue_id = $1'];
    if (cursor) {
      params.push(cursor.at, cursor.id);
      where.push(`(m.entry_datetime,m.match_id) < ($${params.length - 1}::TIMESTAMPTZ,$${params.length}::BIGINT)`);
    }
    params.push(limit + 1);
    const matches = await query(`SELECT m.match_id, m.entry_datetime, m.map, m.queue_id, m.duration_seconds, m.region, m.winning_task_force,
      (SELECT c.name FROM match_players mp JOIN champions c ON c.id = mp.champion_id WHERE mp.match_id = m.match_id LIMIT 1) as sample_champion
      FROM matches m
      WHERE ${where.join(' AND ')}
      ORDER BY m.entry_datetime DESC,m.match_id DESC LIMIT $${params.length}`,params);
    const page = matches.slice(0, limit);
    if (matches.length > limit && page.length > 0) reply.header('X-Next-Cursor', encodeMatchCursor(page[page.length - 1]));
    return page;
  });

  fastify.get('/queue/:queueId', async (req: any, reply: any) => {
    const queueId = parseInt(req.params.queueId as string);
    const limit = Math.min(Math.max(parseInt(req.query.limit as string) || 20, 1), 100);
    const cursor = parseMatchCursor(req.query.cursor);
    if (req.query.cursor && !cursor) return reply.status(400).send({ error: 'Invalid cursor' });
    const params: any[] = [queueId];
    const where = ['m.queue_id = $1'];
    if (cursor) {
      params.push(cursor.at,cursor.id);
      where.push(`(m.entry_datetime,m.match_id) < ($${params.length - 1}::TIMESTAMPTZ,$${params.length}::BIGINT)`);
    }
    params.push(limit + 1);
    const matches = await query(`SELECT m.match_id, m.entry_datetime, m.map, m.duration_seconds, m.region, m.winning_task_force
      FROM matches m WHERE ${where.join(' AND ')} ORDER BY m.entry_datetime DESC,m.match_id DESC LIMIT $${params.length}`,params);
    const page = matches.slice(0,limit);
    if (matches.length > limit && page.length > 0) reply.header('X-Next-Cursor',encodeMatchCursor(page[page.length-1]));
    return page;
  });

  /**
   * POST /matches/pull - Legacy endpoint (kept for backward compatibility).
   * Prefer POST /matches/discover for direct Hi-Rez API passthrough.
   */
  fastify.post('/pull', async (req: any, reply: any) => {
    const { queueId, from, to, region } = req.body as any;
    const normalizedQueueId = Number(queueId);
    const normalizedFrom = String(from ?? '');
    if (
      !Number.isSafeInteger(normalizedQueueId)
      || normalizedQueueId <= 0
      || !/^\d{4}-\d{2}-\d{2}T\d{2}/.test(normalizedFrom)
    ) {
      return reply.status(400).send({ error: 'Invalid queueId or from timestamp' });
    }
    await guardVendorFallback(req, reply, {
      scope: 'manual-match-pull',
      entity: `${normalizedQueueId}:${normalizedFrom.slice(0, 13)}`,
    });
    const { populate } = await import('../workers/match-discovery.js');
    const count = await populate(
      normalizedQueueId,
      normalizedFrom.replace(/-/g, ''),
      parseInt(normalizedFrom.split('T')[1].split(':')[0], 10),
    );
    return { message: `Queued ${count} matches for ingestion` };
  });

  /**
   * POST /matches/discover - Trigger match discovery and ingestion.
   *
   * Same inputs as Hi-Rez getmatchidsbyqueue API.
   * Body: { queueId, date, hour }
   *
   * - queueId: number (e.g. 486 for ranked)
   * - date: string, format YYYYMMDD (e.g. "20260527")
   * - hour: number, 0-23 UTC
   *
   * Flow: Fetches match IDs → fetches match details → dumps to raw_ingest_buffer → buffer-processor processes.
   */
  fastify.post('/discover', async (req: any, reply: any) => {
    const { queueId, date, hour, force } = req.body as any;
    if (!queueId || !date || hour === undefined) {
      return reply.status(400).send({ error: 'Missing required fields: queueId, date, hour' });
    }
    const queueNum = Number(queueId);
    const hourNum = Number(hour);
    if (isNaN(queueNum) || isNaN(hourNum) || hourNum < 0 || hourNum > 23) {
      return reply.status(400).send({ error: 'Invalid queueId or hour. hour must be 0-23.' });
    }
    const dateStr = String(date).replace(/[^0-9]/g, '');
    if (dateStr.length !== 8) {
      return reply.status(400).send({ error: 'Invalid date format. Use YYYYMMDD.' });
    }

    const dbDate = dateStr.replace(/^\d{4}(\d{2})(\d{2})$/, (m, mm, dd) => `${m.slice(0,4)}-${mm}-${dd}`);
    const existing = await oneOrNone(
      `SELECT * FROM hourly_match_counts WHERE date = $1 AND hour = $2 AND queue_id = $3`,
      [dbDate, hourNum, queueNum]
    );
    const openDebt = await oneOrNone(
      `SELECT count(*)::int AS count
       FROM hourly_ingest_match_debt debt
       LEFT JOIN match_ingest_status mis ON mis.match_id = debt.match_id
       WHERE debt.date = $1::date
         AND debt.hour = $2
         AND debt.queue_id = $3
         AND (
           debt.status <> 'complete'
           OR COALESCE(mis.status, '') NOT IN ('complete', 'limited')
         )`,
      [dbDate, hourNum, queueNum],
    );
    const hasOpenDebt = Number(openDebt?.count || 0) > 0;
    let revivedDebt = 0;
    if (hasOpenDebt && Boolean(force)) {
      // Manual force is the operator escape hatch for a bad terminal
      // classification. It revives only non-api_no_data debt for this exact
      // UTC hour, preserving the anti-loop rule: discovery retries known
      // match IDs through debtOnly mode and does not call getmatchidsbyqueue.
      revivedDebt = await reviveRetryableHourlyIngestMatchDebt(dbDate, hourNum, queueNum);
    }
    // ----------------------------------------------------------------
    // DB-first, but not zero-row-blind:
    // - Positive hourly_match_counts rows are safe to serve from DB because
    //   they prove at least one match already reached the projection table.
    // - Zero rows are only analytics. A zero can mean a true empty hour, an
    //   upstream Hi-Rez outage returning no IDs, or a still-draining buffer.
    //   hourly_ingest_state carries the retry/lease decision, so manual
    //   discover calls obey the same guardrails as cron.
    // Source: 2026-06-17 ingest hardening after the API-burn incident.
    // ----------------------------------------------------------------
    const existingCount = Number(existing?.total_matches ?? 0);
    let canServeExisting = Boolean(existing && existingCount > 0);
    if (canServeExisting && hasOpenDebt) {
      // A positive hourly_match_counts row proves some matches for the hour are
      // ingested, but it does not prove the hour is complete. Manual discovery
      // is often used exactly when an operator sees partial counts and wants to
      // recover the known debt. Check the per-match debt ledger before serving
      // the analytics row from cache; otherwise a partial hour like "12
      // discovered / 5 ingested" would keep returning `source: database` and
      // the API could not trigger the ordered blocker/refill worker.
      canServeExisting = false;
    }

    if (existing && existingCount === 0) {
      const { ensureHourlyIngestStateTable } = await import('../workers/hourly-ingest-state.js');
      await ensureHourlyIngestStateTable();
      const state = await oneOrNone(
        `SELECT status, next_retry_at::text, lease_until::text
         FROM hourly_ingest_state
         WHERE date = $1::date AND hour = $2 AND queue_id = $3`,
        [dbDate, hourNum, queueNum]
      );
      const nowMs = Date.now();
      const isFutureTimestamp = (value: string | null | undefined): boolean => {
        if (!value) return false;
        const ts = new Date(value).getTime();
        return Number.isFinite(ts) && ts > nowMs;
      };

      canServeExisting = Boolean(
        state?.status === 'complete'
        || (state?.status === 'empty' && isFutureTimestamp(state.next_retry_at))
        || ((state?.status === 'fetching' || state?.status === 'staged') && isFutureTimestamp(state.lease_until))
        || ((state?.status === 'pending' || state?.status === 'failed') && isFutureTimestamp(state.next_retry_at))
      );
    }

    if (canServeExisting) {
      return {
        source: 'database',
        discovered: 0,
        processed: 0,
        failed: 0,
        hourly_match_counts: existing,
      };
    }

    // Not in DB — fetch from API
    await guardVendorFallback(req, reply, {
      scope: 'manual-match-discovery',
      entity: `${queueNum}:${dateStr}:${hourNum}`,
    });
    const { discover } = await import('../workers/active-match-discovery.js');
    const discovered = await discover(
      queueNum,
      dateStr,
      hourNum,
      hasOpenDebt
        ? {
            // Operator-triggered discovery of a partial hour should not burn a
            // fresh getmatchidsbyqueue call. The discovered IDs already live in
            // hourly_ingest_match_debt; discovery only needs to resume the
            // ordered blocker/refill detail/recovery path. `force` is optional
            // and deliberately manual-only: cron/gap-checker still obey
            // next_retry_at as the no-loop brake.
            debtOnly: true,
            forceDebt: Boolean(force),
          }
        : {},
    );

    // Process every staged payload before returning. The raw buffer is a
    // crash-safe handoff, not a second queue that manual discovery may leave
    // partially drained.
    const { drainRawIngestBuffer } = await import('../workers/buffer-processor.js');
    const drainResult = await drainRawIngestBuffer({ batchSize: 50, reason: 'manual /matches/discover' });

    // Return the hourly_match_counts row for this date/hour/queue
    const statDate = dateStr.replace(/^\d{4}(\d{2})(\d{2})$/, (m, mm, dd) => `${m.slice(0,4)}-${mm}-${dd}`);
    const row = await oneOrNone(
      `SELECT * FROM hourly_match_counts WHERE date = $1 AND hour = $2 AND queue_id = $3`,
      [statDate, hourNum, queueNum]
    );

    return {
      discovered,
      processed: drainResult.processed,
      failed: drainResult.failed,
      revivedDebt,
      hourly_match_counts: row || null,
    };
  });

  // Live match: check if player is in a live match
  fastify.get('/live/:playerId', async (req: any, reply: any) => {
    const playerId = parseInt(req.params.playerId as string);
    if (!Number.isInteger(playerId) || playerId <= 0) {
      return { error: 'Invalid player ID' };
    }
    const { getPlayerLiveMatch } = await import('../workers/live-match-tracker.js');
    const result = await getPlayerLiveMatch(
      playerId,
      (stage, entity) => guardVendorFallback(req, reply, {
        scope: `live-${stage}`,
        entity,
        entityWindowMs: stage === 'player-status' ? 30_000 : 10_000,
      }),
    );
    if (!result) {
      return { message: 'Player not in a live match', player_id: playerId };
    }
    return result;
  });

  // Drop hack suspects
  fastify.get('/live/drop-hack-suspects', async (req: any) => {
    const limit = parseInt(req.query.limit as string) || 50;
    const { getDropHackSuspects } = await import('../workers/live-match-tracker.js');
    return getDropHackSuspects(limit);
  });

  // Raw Hi-Rez API pass-through: getmatchidsbyqueue
  // GET /matches/raw/discover?queueId=486&date=20260528&hour=10
  // Returns the raw Hi-Rez API response without normalization. The exact
  // backend-observed payload is first copied to hirez_raw_api_responses so this
  // debug endpoint cannot lose evidence when raw_ingest_buffer retention runs.
  fastify.get('/raw/discover', async (req: any, reply: any) => {
    const queueId = parseInt(req.query.queueId as string);
    const date = req.query.date as string;
    const hour = parseInt(req.query.hour as string);

    if (!queueId || !date || isNaN(hour)) {
      return { error: 'Missing required query params: queueId, date, hour' };
    }

    await guardVendorFallback(req, reply, {
      scope: 'raw-match-discovery',
      entity: `${queueId}:${date}:${hour}`,
    });
    const raw = await getMatchIdsByQueue(queueId, date, hour, 'operator_raw_audit');
    const audit = await recordRawHirezResponse({
      endpoint: 'getmatchidsbyqueue',
      operation: 'getMatchIdsByQueue',
      entityType: 'match_discovery',
      entityId: `${queueId}:${date}:${hour}`,
      params: { queueId, date, hour },
      rawResponse: raw,
    });
    return {
      queue_id: queueId,
      date,
      hour,
      count: Array.isArray(raw) ? raw.length : null,
      audit,
      data: raw,
    };
  });

  // Raw Hi-Rez API pass-through: getmatchdetailsbatch
  // GET /matches/raw/matchdetails?ids=1279879096,1279879095
  // Returns the raw Hi-Rez API response without normalization, after durable
  // audit storage succeeds.
  fastify.get('/raw/matchdetails', async (req: any, reply: any) => {
    const idsRaw = req.query.ids as string;
    if (!idsRaw) {
      return { error: 'Missing required query param: ids (comma-separated match IDs)' };
    }
    const matchIds = [...new Set(
      idsRaw
        .split(',')
        .map(value => Number(value.trim()))
        .filter(value => Number.isSafeInteger(value) && value > 0),
    )];
    if (matchIds.length === 0) {
      return { error: 'No valid match IDs provided' };
    }
    if (matchIds.length > 10) {
      return reply.status(400).send({ error: 'Maximum 10 match IDs per raw request' });
    }

    await guardVendorFallback(req, reply, {
      scope: 'raw-match-details',
      entity: matchIds.join(','),
    });
    const raw = await getMatchDetailsBatchRaw(matchIds, 'operator_raw_audit');
    const audit = await recordRawHirezResponse({
      endpoint: 'getmatchdetailsbatch',
      operation: 'getMatchDetailsBatchRaw',
      entityType: 'match_batch',
      entityId: matchIds.join(','),
      params: { matchIds },
      rawResponse: raw,
    });
    return {
      match_ids: matchIds,
      count: Array.isArray(raw) ? raw.length : null,
      audit,
      data: raw,
    };
  });

  // Raw Hi-Rez API pass-through: getdemodetails. Broken-match recovery may use
  // non-score shell fields such as duration/replay/bans, but its score and
  // winner fields are diagnostic only and are never recovery authority.
  fastify.get('/raw/demo', async (req: any, reply: any) => {
    const matchId = parseInt(req.query.matchId as string, 10);
    if (!Number.isInteger(matchId) || matchId <= 0) {
      return { error: 'Missing or invalid matchId' };
    }
    await guardVendorFallback(req, reply, {
      scope: 'raw-match-demo',
      entity: matchId,
    });
    const raw = await getDemoDetails(matchId, 'operator_raw_audit');
    const audit = await recordRawHirezResponse({
      endpoint: 'getdemodetails',
      operation: 'getDemoDetails',
      entityType: 'match_demo',
      entityId: matchId,
      params: { matchId },
      rawResponse: raw,
    });
    return { match_id: matchId, audit, data: raw };
  });

  // Raw Hi-Rez API pass-through: getplayerbatchfrommatch
  // GET /matches/raw/playerbatch?matchId=1279879096
  // Returns the raw Hi-Rez API response without normalization, after durable
  // audit storage succeeds.
  fastify.get('/raw/playerbatch', async (req: any, reply: any) => {
    const matchId = parseInt(req.query.matchId as string);

    if (!matchId || isNaN(matchId)) {
      return { error: 'Missing required query param: matchId' };
    }

    await guardVendorFallback(req, reply, {
      scope: 'raw-match-player-batch',
      entity: matchId,
    });
    const raw = await getPlayerBatchFromMatch(matchId, 'operator_raw_audit');
    const audit = await recordRawHirezResponse({
      endpoint: 'getplayerbatchfrommatch',
      operation: 'getPlayerBatchFromMatch',
      entityType: 'match',
      entityId: matchId,
      params: { matchId },
      rawResponse: raw,
    });
    return {
      match_id: matchId,
      count: Array.isArray(raw) ? raw.length : null,
      audit,
      data: raw,
    };
  });

  /**
   * GET /matches/search — Advanced match search.
   *
   * Query params:
   *   ?championId=   — Filter by champion played
   *   ?queueId=      — Filter by queue (e.g. 486 for ranked)
   *   ?region=       — Filter by region
   *   ?date=         — Calendar date in timeZone (YYYY-MM-DD)
   *   ?hour=         — Optional local one-hour window (0–23; requires date)
   *   ?timeZone=     — IANA time zone used with date/hour (default: UTC)
   *   ?from=         — ISO 8601 start date
   *   ?to=           — ISO 8601 end date
   *   ?afkMax=       — Max AFK score filter
   *   ?minPlayers=   — Minimum player count
   *   ?page=         — Page number (default: 1)
   *   ?perPage=      — Results per page (default: 20, max: 100)
   *
   * Returns: Array of { match_id, entry_datetime, map, queue_id, duration_seconds, region,
   *   champion_id, champion_name, win_status, kills, deaths, assists, player_count }
   */
  fastify.get('/search', async (req: any, reply: any) => {
    const requestedPage = parseInt(req.query.page, 10);
    const requestedPerPage = parseInt(req.query.perPage, 10);
    const page = Number.isInteger(requestedPage) && requestedPage > 0 ? requestedPage : 1;
    const perPage = Number.isInteger(requestedPerPage) && requestedPerPage > 0
      ? Math.min(requestedPerPage, 100)
      : 20;
    const calculatedOffset = (page - 1) * perPage;
    const cursor = parseMatchCursor(req.query.cursor);
    if (req.query.cursor && !cursor) return reply.status(400).send({ error: 'Invalid cursor' });
    const cursorMode = Boolean(cursor)
      || ['1','true'].includes(String(req.query.cursorMode ?? '').toLowerCase());

    const conditions: string[] = [];
    const params: any[] = [];
    const addParam = (value: any) => {
      params.push(value);
      return `$${params.length}`;
    };
    const parsePositiveInt = (value: unknown) => {
      const parsed = parseInt(String(value), 10);
      return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
    };

    let championParam: string | null = null;
    if (req.query.championId !== undefined) {
      const championId = parsePositiveInt(req.query.championId);
      if (championId === null) return reply.status(400).send({ error: 'Invalid championId' });
      championParam = addParam(championId);
      // Match-level existence prevents a player-row join from duplicating each match.
      conditions.push(`EXISTS (SELECT 1 FROM match_players mp_filter WHERE mp_filter.match_id = m.match_id AND mp_filter.champion_id = ${championParam})`);
    }
    if (req.query.queueId !== undefined) {
      const queueId = parsePositiveInt(req.query.queueId);
      if (queueId === null) return reply.status(400).send({ error: 'Invalid queueId' });
      conditions.push(`m.queue_id = ${addParam(queueId)}`);
    }
    if (req.query.region !== undefined && String(req.query.region).trim()) {
      conditions.push(`m.region = ${addParam(String(req.query.region).trim().toUpperCase())}`);
    }

    const date = req.query.date === undefined ? '' : String(req.query.date);
    if (date) {
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return reply.status(400).send({ error: 'Invalid date; expected YYYY-MM-DD' });
      const requestedHour = req.query.hour === undefined || req.query.hour === '' ? null : Number(req.query.hour);
      if (requestedHour !== null && (!Number.isInteger(requestedHour) || requestedHour < 0 || requestedHour > 23)) {
        return reply.status(400).send({ error: 'Invalid hour; expected 0 through 23' });
      }
      const timeZone = req.query.timeZone === undefined ? 'UTC' : String(req.query.timeZone);
      if (!isValidTimeZone(timeZone)) return reply.status(400).send({ error: 'Invalid timeZone' });
      const start = zonedDateHourToUtc(date, requestedHour ?? 0, timeZone);
      if (Number.isNaN(start.getTime())) return reply.status(400).send({ error: 'Invalid date' });
      const end = requestedHour === null
        ? zonedDateHourToUtc(nextDate(date), 0, timeZone)
        : new Date(start.getTime() + 60 * 60 * 1000);
      conditions.push(`m.entry_datetime >= ${addParam(start)}`);
      conditions.push(`m.entry_datetime < ${addParam(end)}`);
    } else {
      if (req.query.hour !== undefined && req.query.hour !== '') return reply.status(400).send({ error: 'hour requires date' });
      if (req.query.from) {
        const from = new Date(String(req.query.from));
        if (Number.isNaN(from.getTime())) return reply.status(400).send({ error: 'Invalid from date' });
        conditions.push(`m.entry_datetime >= ${addParam(from)}`);
      }
      if (req.query.to) {
        const to = new Date(String(req.query.to));
        if (Number.isNaN(to.getTime())) return reply.status(400).send({ error: 'Invalid to date' });
        conditions.push(`m.entry_datetime <= ${addParam(to)}`);
      }
    }

    if (cursor) {
      const atParam = addParam(cursor.at);
      const idParam = addParam(cursor.id);
      conditions.push(`(m.entry_datetime,m.match_id) < (${atParam}::TIMESTAMPTZ,${idParam}::BIGINT)`);
    }
    const clause = conditions.length > 0 ? ` WHERE ${conditions.join(' AND ')}` : '';

    // Cursor requests deliberately skip COUNT(*) and OFFSET: both grow with
    // table size. Numbered pages remain backward compatible for existing UI.
    const countRow = cursorMode ? null : await one(`SELECT COUNT(*) as total FROM matches m${clause}`, params);

    const selectedChampionCondition = championParam ? `AND mp.champion_id = ${championParam}` : '';
    const dataSql = `SELECT m.match_id, m.entry_datetime, m.map, m.queue_id, m.duration_seconds, m.region,
      COALESCE(mp.champion_id, 0) AS champion_id, COALESCE(c.name, '') AS champion_name,
      COALESCE(mp.win_status, '') AS win_status, COALESCE(mp.kills, 0) AS kills,
      COALESCE(mp.deaths, 0) AS deaths, COALESCE(mp.assists, 0) AS assists,
      (SELECT COUNT(*) FROM match_players mp2 WHERE mp2.match_id = m.match_id) as player_count
      FROM matches m
      LEFT JOIN LATERAL (
        SELECT champion_id, win_status, kills, deaths, assists
        FROM match_players mp
        WHERE mp.match_id = m.match_id ${selectedChampionCondition}
        ORDER BY mp.entry_datetime DESC, mp.player_id, mp.private_slot
        LIMIT 1
      ) mp ON true
      LEFT JOIN champions c ON c.id = mp.champion_id${clause}
      ORDER BY m.entry_datetime DESC,m.match_id DESC LIMIT $${params.length + 1}${cursorMode ? '' : ` OFFSET $${params.length + 2}`}`;
    const rows = await query(dataSql, cursorMode ? [...params,perPage+1] : [...params,perPage,calculatedOffset]);

    if (cursorMode) {
      const data = rows.slice(0,perPage);
      const nextCursor = rows.length > perPage && data.length > 0 ? encodeMatchCursor(data[data.length-1]) : null;
      if (nextCursor) reply.header('X-Next-Cursor',nextCursor);
      return { data,total:null,next_cursor:nextCursor,page:{ current:null,size:perPage,totalPages:null } };
    }

    return { data: rows, total: countRow?.total ?? 0, page: { current: page, size: perPage, totalPages: Math.ceil((countRow?.total ?? 0) / perPage) } };
  });

  /**
   * GET /matches/bans — Ban statistics across matches.
   *
   * Query params:
   *   ?championId=   — Filter by champion
   *   ?sort=         — "count" or "ban_rate" (default: count)
   *   ?order=        — "asc" or "desc" (default: desc)
   *   ?limit=        — Max results (default: 50)
   *
   * Returns: Array of { champion_id, champion_name, total_bans, total_matches, ban_rate, ban_count }
   */
  fastify.get('/bans', async (req: any) => {
    const limit = Math.min(parseInt(req.query.limit as string) || 50, 200);
    const sort = req.query.sort === 'ban_rate' ? 'ban_rate' : 'ban_count';
    const order = req.query.order === 'asc' ? 'ASC' : 'DESC';

    const params: any[] = [RANKED_STATS_QUEUE_ID];
    const where = ['sba.queue_id = $1'];
    if (req.query.championId) {
      params.push(parseInt(req.query.championId,10));
      where.push(`sba.champion_id = $${params.length}`);
    }
    params.push(limit);
    const rows = await query(
      `WITH champion_bans AS (
         SELECT sba.champion_id,SUM(sba.bans)::BIGINT AS ban_count
         FROM stats_ban_aggregate sba WHERE ${where.join(' AND ')} GROUP BY sba.champion_id
       ), totals AS (
         SELECT SUM(bans)::BIGINT AS all_bans FROM stats_ban_aggregate WHERE queue_id=$1
       ), match_total AS (
         SELECT SUM(match_count)::BIGINT AS total_matches FROM stats_match_aggregate WHERE queue_id=$1
       )
       SELECT c.id AS champion_id,c.name AS champion_name,cb.ban_count,
         cb.ban_count AS total_bans,mt.total_matches,
         ROUND(100.0*cb.ban_count::NUMERIC/NULLIF(t.all_bans,0),2) AS ban_rate
       FROM champion_bans cb JOIN champions c ON c.id=cb.champion_id
       CROSS JOIN totals t CROSS JOIN match_total mt
       ORDER BY ${sort} ${order}
       LIMIT $${params.length}`,
      params
    );
    return rows;
  });

  /**
   * GET /matches/fact/:matchId — Fact tables for a match (items + cards + talents combined).
   *
   * Returns: { match_id, players: [{ player_id, player_name, items: [...], cards: [...], talents: [...] }] }
   */
  fastify.get('/fact/:matchId', async (req: any, reply: any) => {
    const matchId = parseInt((req.params as any).matchId, 10);
    if (!Number.isInteger(matchId) || matchId <= 0) {
      return reply.status(400).send({ error: 'Invalid match ID' });
    }

    const match = await one(
      `SELECT match_id, 'ranked'::text AS storage_kind
       FROM matches
       WHERE match_id = $1
       UNION ALL
       SELECT match_id, 'casual'::text AS storage_kind
       FROM casual_matches
       WHERE match_id = $1
       UNION ALL
       SELECT match_id, 'special'::text AS storage_kind
       FROM special_matches
       WHERE match_id = $1
       LIMIT 1`,
      [matchId],
    );
    if (!match) {
      return reply.status(404).send({ error: 'Match not found' });
    }

    // Bulk-load the three per-player fact tables instead of doing 30 small
    // queries per match. These rows are display facts, not recovery input:
    // they must reflect exactly what ingest persisted in match_player_items,
    // match_player_cards, and match_player_talents, including item/card levels.
    const players = await query(
      `WITH stored_players AS (
         SELECT 'ranked'::text AS storage_kind, mp.player_id, mp.private_slot,
                mp.player_name, mp.champion_id, NULL::text AS stored_champion_name,
                mp.task_force, 0::int AS roster_slot, NULL::jsonb AS raw_player
         FROM match_players mp
         WHERE mp.match_id = $1
         UNION ALL
         SELECT 'casual'::text AS storage_kind, cmp.player_id, cmp.private_slot,
                cmp.player_name, cmp.champion_id, cmp.champion_name AS stored_champion_name,
                cmp.task_force, cmp.roster_slot, cmp.raw_player
         FROM casual_match_players cmp
         WHERE cmp.match_id = $1
         UNION ALL
         SELECT 'special'::text AS storage_kind, smp.player_id, smp.private_slot,
                smp.player_name, smp.champion_id, smp.champion_name AS stored_champion_name,
                smp.task_force, smp.roster_slot, smp.raw_player
         FROM special_match_players smp
         WHERE smp.match_id = $1
       )
       SELECT stored.player_id, stored.player_name, stored.champion_id,
              COALESCE(c.name, stored.stored_champion_name) AS champion_name,
              stored.raw_player
       FROM stored_players stored
       LEFT JOIN champions c ON c.id = stored.champion_id
       WHERE stored.storage_kind = $2
       ORDER BY stored.task_force, stored.roster_slot, stored.player_id, stored.private_slot`,
      [matchId, match.storage_kind],
    );

    const result: any = {
      match_id: matchId,
      players: players.map((player: any) => ({
        player_id: player.player_id,
        player_name: player.player_name,
        champion_id: player.champion_id,
        champion_name: player.champion_name,
        items: [],
        cards: [],
        talents: [],
      })),
    };
    const byPlayer = new Map<string, any>(
      result.players.map((player: any) => [String(player.player_id), player] as [string, any]),
    );

    const [items, cards, talents] = await Promise.all([
      query(
        `SELECT mpi.player_id, mpi.item_id, mpi.slot, mpi.item_level,
                i.item_name, i.description, i.item_type, i.cost, i.icon_url AS db_icon_url
         FROM match_player_items mpi
         LEFT JOIN items i ON i.item_id = mpi.item_id
         WHERE mpi.match_id = $1
         ORDER BY mpi.player_id, mpi.slot`,
        [matchId],
      ),
      query(
        `SELECT mpc.player_id, mpc.card_id, mpc.card_level,
                COALESCE(c.card_name, i.item_name) AS card_name,
                COALESCE(c.champion_id, i.champion_id) AS champion_id,
                i.description, i.icon_url AS db_icon_url
         FROM match_player_cards mpc
         LEFT JOIN cards c ON c.card_id = mpc.card_id
         LEFT JOIN items i ON i.item_id = mpc.card_id
         WHERE mpc.match_id = $1
         ORDER BY mpc.player_id, mpc.card_id`,
        [matchId],
      ),
      query(
        `SELECT mpt.player_id, mpt.talent_id,
                COALESCE(t.talent_name, i.item_name) AS talent_name,
                COALESCE(t.champion_id, i.champion_id) AS champion_id,
                c.name AS champion_name,
                i.description,
                i.icon_url AS db_icon_url
         FROM match_player_talents mpt
         JOIN match_players mp
           ON mp.match_id = mpt.match_id
          AND mp.player_id = mpt.player_id
         LEFT JOIN talents t ON t.talent_id = mpt.talent_id
         LEFT JOIN items i ON i.item_id = mpt.talent_id
         LEFT JOIN champions c ON c.id = COALESCE(t.champion_id, i.champion_id)
         WHERE mpt.match_id = $1
           AND COALESCE(t.champion_id, i.champion_id, mp.champion_id) = mp.champion_id
         ORDER BY mpt.player_id, mpt.talent_id`,
        [matchId],
      ),
    ]);

    for (const item of items) {
      const player = byPlayer.get(String(item.player_id));
      if (!player) continue;
      player.items.push({
        item_id: item.item_id,
        slot: item.slot,
        item_level: item.item_level,
        item_name: item.item_name,
        description: item.description,
        item_type: item.item_type,
        cost: item.cost,
        icon_url: item.db_icon_url || itemIconUrl(item.item_name),
        fallback_icon_url: itemFallbackIconUrl(item.item_name),
      });
    }

    for (const card of cards) {
      const player = byPlayer.get(String(card.player_id));
      if (!player) continue;
      player.cards.push({
        card_id: card.card_id,
        card_level: card.card_level,
        card_name: card.card_name,
        champion_id: card.champion_id,
        description: card.description,
        icon_url: card.db_icon_url || cardIconUrl(card.card_name),
        fallback_icon_url: cardFallbackIconUrl(card.card_name),
      });
    }

    for (const talent of talents) {
      const player = byPlayer.get(String(talent.player_id));
      if (!player) continue;
      player.talents.push({
        talent_id: talent.talent_id,
        talent_name: talent.talent_name,
        champion_id: talent.champion_id,
        champion_name: talent.champion_name || player.champion_name,
        description: talent.description,
        icon_url: talent.db_icon_url || talentIconUrl(talent.champion_name || player.champion_name, talent.talent_name),
        fallback_icon_url: talentFallbackIconUrl(talent.champion_name || player.champion_name, talent.talent_name),
      });
    }

    // Casual and special acquisition deliberately keep their full normalized
    // Hi-Rez player row in raw_player rather than duplicating ranked-only fact
    // projections. Reconstruct display facts from that durable row so every
    // persisted match class serves the same public facts contract.
    for (let index = 0; index < players.length; index += 1) {
      const stored = players[index];
      const player = result.players[index];
      const raw = stored?.raw_player && typeof stored.raw_player === 'object'
        ? stored.raw_player as Record<string, unknown>
        : null;
      if (!raw || !player) continue;

      const positiveId = (value: unknown): number | null => {
        const parsed = Number(value);
        return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : null;
      };
      const text = (value: unknown): string => typeof value === 'string' ? value.trim() : '';

      if (player.items.length === 0) {
        for (let slot = 1; slot <= 4; slot += 1) {
          const itemId = positiveId(raw[`active_id_${slot}`]);
          if (itemId == null) continue;
          const itemName = text(raw[`item_active_${slot}`]);
          const rawLevel = Math.max(0, Math.trunc(Number(raw[`active_level_${slot}`]) || 0));
          const itemLevel = rawLevel > 2 ? Math.floor(rawLevel / 4) : rawLevel;
          player.items.push({
            item_id: itemId,
            slot,
            item_level: itemLevel,
            item_name: itemName || null,
            description: null,
            item_type: null,
            cost: null,
            icon_url: itemIconUrl(itemName),
            fallback_icon_url: itemFallbackIconUrl(itemName),
          });
        }
      }

      if (player.cards.length === 0) {
        for (let slot = 1; slot <= 5; slot += 1) {
          const cardId = positiveId(raw[`item_id_${slot}`]);
          if (cardId == null) continue;
          const cardName = text(raw[`item_purch_${slot}`]);
          player.cards.push({
            card_id: cardId,
            card_level: Math.max(0, Math.trunc(Number(raw[`item_level_${slot}`]) || 0)),
            card_name: cardName || null,
            champion_id: player.champion_id,
            description: null,
            icon_url: cardIconUrl(cardName),
            fallback_icon_url: cardFallbackIconUrl(cardName),
          });
        }
      }

      if (player.talents.length === 0) {
        const talentId = positiveId(raw.item_id_6);
        const talentName = text(raw.item_purch_6);
        if (talentId != null) {
          player.talents.push({
            talent_id: talentId,
            talent_name: talentName || null,
            champion_id: player.champion_id,
            champion_name: player.champion_name,
            description: null,
            icon_url: talentIconUrl(player.champion_name, talentName),
            fallback_icon_url: talentFallbackIconUrl(player.champion_name, talentName),
          });
        }
      }
    }

    return result;
  });

  /**
   * GET /matches/hourly-stats — Live ranked match activity by region.
   *
   * Returns today's ranked match counts broken down by region (from
   * hourly_match_counts), plus per-region hourly rates averaged over the
   * hours that have data today.
   *
   * Response shape:
   *   { totalToday, rankedToday, regions: [{ region, matchesPerHour, totalToday }] }
   */
  fastify.get('/hourly-stats', async (req: any, reply: any) => {
    const RANKED_QUEUE = RANKED_STATS_QUEUE_ID;
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const includePlayerTrend = String(req.query?.includePlayers ?? '').toLowerCase() === 'true';

    const now = new Date();
    const currentHour = now.getUTCHours();
    const todayStr = now.toISOString().slice(0, 10);
    const yesterday = new Date(now.getTime() - 86400000);
    const yesterdayStr = yesterday.toISOString().slice(0, 10);
    const weekStart = new Date(now.getTime() - (6 * 86400000));
    const weekStartStr = weekStart.toISOString().slice(0, 10);

    // The all-lobbies path uses the compact hourly projection. A selected
    // scope computes each recent match's exact average from known real-player
    // tiers. This preserves numeric boundaries: Diamond begins at 21.0, so a
    // 20.9 average can never enter the Diamond 5+ tracker through rounding.
    let rows: any[];
    if (lobbyTier.active) {
      const params: any[] = [yesterdayStr, todayStr, RANKED_QUEUE];
      const tierWhere = ['average_tier IS NOT NULL'];
      if (lobbyTier.min != null) {
        params.push(lobbyTier.min);
        tierWhere.push(`average_tier >= $${params.length}`);
      }
      if (lobbyTier.max != null) {
        // Integer tier bands represent the full tier interval. For example,
        // Bronze–Gold (max 15) includes averages below 16.0 instead of leaving
        // fractional averages such as 15.4 outside every scope.
        params.push(lobbyTier.max + 1);
        tierWhere.push(`average_tier < $${params.length}`);
      }
      rows = await query(
        `WITH ranked_lobbies AS (
           SELECT
             m.match_id,
             m.entry_datetime,
             m.region,
             AVG(mp.league_tier::NUMERIC) FILTER (
               WHERE mp.player_id > 0
                 AND mp.champion_id > 0
                 AND mp.league_tier BETWEEN 1 AND 26
             ) AS average_tier
           FROM matches m
           JOIN match_players mp
             ON mp.match_id = m.match_id
            AND mp.entry_datetime = m.entry_datetime
           WHERE m.entry_datetime >= $1::DATE
             AND m.entry_datetime < ($2::DATE + INTERVAL '1 day')
             AND m.queue_id = $3
           GROUP BY m.match_id, m.entry_datetime, m.region
         ), scoped_lobbies AS (
           SELECT * FROM ranked_lobbies WHERE ${tierWhere.join(' AND ')}
         )
         SELECT
           (entry_datetime AT TIME ZONE 'UTC')::DATE::TEXT AS date,
           EXTRACT(HOUR FROM entry_datetime AT TIME ZONE 'UTC')::INT AS hour,
           COUNT(*) FILTER (WHERE region = 'NA')::INT AS matches_na,
           COUNT(*) FILTER (WHERE region = 'EU')::INT AS matches_eu,
           COUNT(*) FILTER (WHERE region IN ('ASIA', 'Asia'))::INT AS matches_asia,
           COUNT(*) FILTER (WHERE region = 'SEA')::INT AS matches_sea,
           COUNT(*) FILTER (WHERE region = 'JPN')::INT AS matches_jpn,
           COUNT(*) FILTER (WHERE region = 'RUS')::INT AS matches_rus,
           COUNT(*) FILTER (WHERE region = 'BR')::INT AS matches_br,
           COUNT(*) FILTER (WHERE region = 'OCE')::INT AS matches_oce,
           COUNT(*) FILTER (WHERE region IN ('SA', 'LATAM'))::INT AS matches_sa,
           COUNT(*) FILTER (WHERE region IS NULL OR region NOT IN ('NA','EU','ASIA','Asia','SEA','JPN','RUS','BR','OCE','SA','LATAM'))::INT AS matches_unknown,
           COUNT(*)::INT AS total_matches
         FROM scoped_lobbies
         GROUP BY 1, 2
         ORDER BY 1, 2`,
        params,
      );
    } else {
      rows = await query(
        `SELECT date::text as date, hour,
                 matches_na, matches_eu, matches_asia, matches_sea,
                 matches_jpn, matches_rus, matches_br, matches_oce,
                 matches_sa, matches_unknown, total_matches
         FROM hourly_match_counts
         WHERE date::TEXT IN ($1, $2) AND queue_id = $3
         ORDER BY date, hour`,
        [todayStr, yesterdayStr, RANKED_QUEUE],
      );
    }

    const regionDefs = [
      { key: 'matches_na',   region: 'NA' },
      { key: 'matches_eu',   region: 'EU' },
      { key: 'matches_asia', region: 'Asia' },
      { key: 'matches_br',   region: 'BR' },
      { key: 'matches_oce',  region: 'OCE' },
      { key: 'matches_sa',   region: 'LATAM' },
    ];

    await ensureMatchCountDiscoveryTables();
    const [
      casualRows,
      observedQueueRows,
      rankedDailyRows,
      nonrankedDailyRows,
      dailyPlayerRows,
    ] = await Promise.all([
      query(
        `SELECT date::text AS date,
                hour,
                queue_id,
                region,
                match_count AS total_matches
         FROM match_count_discovery_region_hours
         WHERE date::text IN ($1, $2)
           AND queue_id <> $3
         ORDER BY date, hour, queue_id, region`,
        [todayStr, yesterdayStr, RANKED_QUEUE],
      ),
      query(`SELECT DISTINCT queue_id FROM match_count_discovery_region_hours WHERE match_count > 0`),
      query(
        `SELECT date::text AS date, SUM(total_matches)::int AS total
         FROM hourly_match_counts
         WHERE date >= $1::date
           AND date <= $2::date
           AND queue_id = $3
         GROUP BY date
         ORDER BY date`,
        [weekStartStr, todayStr, RANKED_QUEUE],
      ),
      query(
        `SELECT date::text AS date, queue_id, SUM(match_count)::int AS total
         FROM match_count_discovery_region_hours
         WHERE date >= $1::date
           AND date <= $2::date
           AND queue_id <> $3
         GROUP BY date, queue_id
         ORDER BY date, queue_id`,
        [weekStartStr, todayStr, RANKED_QUEUE],
      ),
      includePlayerTrend
        ? query(
          `WITH observations AS MATERIALIZED (
             SELECT
               (mp.entry_datetime AT TIME ZONE 'UTC')::date AS activity_date,
               $3::int AS queue_id,
               mp.player_id
             FROM match_players mp
             WHERE mp.entry_datetime >= $1::date
               AND mp.entry_datetime < ($2::date + interval '1 day')
               AND mp.player_id > 0
               AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
             UNION ALL
             SELECT
               (casual.entry_datetime AT TIME ZONE 'UTC')::date,
               casual.queue_id,
               fact.player_id
             FROM casual_matches casual
             JOIN queue_types queue
               ON queue.queue_id = casual.queue_id
              AND queue.track_presence = TRUE
             JOIN casual_match_players fact ON fact.match_id = casual.match_id
             WHERE casual.entry_datetime >= $1::date
               AND casual.entry_datetime < ($2::date + interval '1 day')
               AND fact.player_id > 0
               AND fact.participant_kind = 'human'
             UNION ALL
             SELECT
               (special.entry_datetime AT TIME ZONE 'UTC')::date,
               special.queue_id,
               fact.player_id
             FROM special_matches special
             JOIN queue_types queue
               ON queue.queue_id = special.queue_id
              AND queue.track_presence = TRUE
             JOIN special_match_players fact ON fact.match_id = special.match_id
             WHERE special.entry_datetime >= $1::date
               AND special.entry_datetime < ($2::date + interval '1 day')
               AND fact.player_id > 0
               AND fact.participant_kind = 'human'
           )
           SELECT
             activity_date::text AS date,
             queue_id,
             COUNT(DISTINCT player_id)::int AS players
           FROM observations
           GROUP BY GROUPING SETS (
             (activity_date),
             (activity_date, queue_id)
           )
           ORDER BY activity_date, queue_id NULLS FIRST`,
          [weekStartStr, todayStr, RANKED_QUEUE],
        )
        : Promise.resolve([]),
    ]);

    // Build a map of date+hour -> row data
    const rowMap = new Map<string, any>();
    for (const row of rows) {
      const key = `${row.date}|${String(row.hour).padStart(2, '0')}`;
      rowMap.set(key, row);
    }

    // Generate rolling 24 hours: (currentHour - 23) to currentHour
    const hourly: any[] = [];
    let grandTotal = 0;
    const regionTotals: Record<string, number> = {};
    for (const def of regionDefs) regionTotals[def.region] = 0;

    for (let i = 23; i >= 0; i--) {
      let h = currentHour - i;
      let dateStr = todayStr;
      if (h < 0) { h += 24; dateStr = yesterdayStr; }
      const key = `${dateStr}|${String(h).padStart(2, '0')}`;
      const row = rowMap.get(key);

      const entry: any = { hour: h, date: dateStr };
      for (const def of regionDefs) {
        entry[def.region] = row ? (Number(row[def.key]) || 0) : 0;
      }
      entry.total = row ? (Number(row.total_matches) || 0) : 0;
      hourly.push(entry);
      grandTotal += entry.total;
      for (const def of regionDefs) regionTotals[def.region] += entry[def.region];
    }

    const hoursWithData = rows.length || 1;

    const detailedRegionDefs = [
      { region: 'NA', key: 'matches_na' },
      { region: 'EU', key: 'matches_eu' },
      { region: 'SEA', key: 'matches_sea' },
      { region: 'JPN', key: 'matches_jpn' },
      { region: 'RUS', key: 'matches_rus' },
      { region: 'BR', key: 'matches_br' },
      { region: 'OCE', key: 'matches_oce' },
      { region: 'LATAM', key: 'matches_sa' },
      { region: 'Unknown', key: 'matches_unknown' },
    ];
    const casualByWindow = new Map<string, Record<string, number>>();
    for (const row of casualRows) {
      const key = `${row.queue_id}|${row.date}|${Number(row.hour)}`;
      const counts = casualByWindow.get(key) ?? {};
      const rawRegion = String(row.region || 'Unknown');
      const region = rawRegion === 'SA' ? 'LATAM' : rawRegion;
      counts[region] = (counts[region] ?? 0) + Number(row.total_matches || 0);
      casualByWindow.set(key, counts);
    }
    const visibleQueueIds = new Set<number>([
      RANKED_QUEUE,
      ...observedQueueRows.map((row: any) => Number(row.queue_id)),
    ]);
    const queueActivities = MATCH_COUNT_QUEUE_DEFINITIONS
      .filter(queue => visibleQueueIds.has(queue.queueId))
      .map(queue => {
        const queueHourly: any[] = [];
        const totals: Record<string, number> = Object.fromEntries(detailedRegionDefs.map(def => [def.region, 0]));
        let total24h = 0;
        for (let i = 23; i >= 0; i--) {
          let h = currentHour - i;
          let dateStr = todayStr;
          if (h < 0) { h += 24; dateStr = yesterdayStr; }
          const counts: Record<string, number> = {};
          if (queue.queueId === RANKED_QUEUE) {
            const rankedRow = rowMap.get(`${dateStr}|${String(h).padStart(2, '0')}`);
            for (const def of detailedRegionDefs) {
              counts[def.region] = Number(rankedRow?.[def.key] || 0);
            }
            // Older projection rows kept undifferentiated Asia in a legacy
            // column. Surface it under SEA instead of dropping it from totals.
            counts.SEA += Number(rankedRow?.matches_asia || 0);
          } else {
            const observed = casualByWindow.get(`${queue.queueId}|${dateStr}|${h}`) ?? {};
            for (const def of detailedRegionDefs) counts[def.region] = Number(observed[def.region] || 0);
          }
          const total = Object.values(counts).reduce((sum, value) => sum + Number(value || 0), 0);
          for (const [region, value] of Object.entries(counts)) totals[region] = (totals[region] ?? 0) + value;
          total24h += total;
          queueHourly.push({ date: dateStr, hour: h, total, regions: counts });
        }
        return {
          queueId: queue.queueId,
          queueName: queue.name,
          ranked: queue.ranked,
          total24h,
          regions: detailedRegionDefs.map(def => ({
            region: def.region,
            total24h: totals[def.region] || 0,
            matchesPerHour: Math.round((totals[def.region] || 0) / 24),
          })),
          hourly: queueHourly,
        };
      });

    const weeklyByDate = new Map<string, {
      ranked: number;
      queues: Record<string, number>;
      players: number;
      playerQueues: Record<string, number>;
    }>();
    for (let offset = 0; offset < 7; offset += 1) {
      const date = new Date(weekStart.getTime() + (offset * 86400000)).toISOString().slice(0, 10);
      weeklyByDate.set(date, {
        ranked: 0,
        queues: {},
        players: 0,
        playerQueues: {},
      });
    }
    for (const row of rankedDailyRows) {
      const entry = weeklyByDate.get(String(row.date));
      if (!entry) continue;
      entry.ranked = Number(row.total || 0);
      entry.queues[String(RANKED_QUEUE)] = entry.ranked;
    }
    for (const row of nonrankedDailyRows) {
      const entry = weeklyByDate.get(String(row.date));
      if (!entry) continue;
      entry.queues[String(row.queue_id)] = Number(row.total || 0);
    }
    for (const row of dailyPlayerRows) {
      const entry = weeklyByDate.get(String(row.date));
      if (!entry) continue;
      if (row.queue_id == null) {
        entry.players = Number(row.players || 0);
      } else {
        entry.playerQueues[String(row.queue_id)] = Number(row.players || 0);
      }
    }
    const weekly = [...weeklyByDate.entries()].map(([date, entry]) => ({
      date,
      total: Object.values(entry.queues).reduce((sum, value) => sum + value, 0),
      ranked: entry.ranked,
      queues: entry.queues,
      players: entry.players,
      playerQueues: entry.playerQueues,
    }));

    return {
      totalToday: grandTotal,
      rankedToday: grandTotal,
      regions: regionDefs.map((def) => ({
        region: def.region,
        matchesPerHour: Math.round(regionTotals[def.region] / hoursWithData),
        totalToday: regionTotals[def.region],
      })),
      hourly,
      currentHour,
      allQueuesTotal24h: queueActivities.reduce((sum, queue) => sum + queue.total24h, 0),
      queues: queueActivities,
      weekly,
    };
  });

  // GET /matches/compositions — ranked team compositions with winrate
  // Optional: ?sortBy=count|winrate|wins&order=desc|asc&limit=20
  fastify.get('/compositions', async (req: any, reply: any) => {
    const sortBy = req.query.sortBy || 'count';
    const order = req.query.order === 'asc' ? 'ASC' : 'DESC';
    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);

    const validSort = ['count', 'winrate', 'wins', 'frontline', 'damage', 'flank', 'support'];
    const sort = validSort.includes(sortBy) ? sortBy : 'count';

    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });

    const params: any[] = [RANKED_STATS_QUEUE_ID];
    const where: string[] = ['sca.queue_id = $1'];
    appendLobbyTierPredicate(lobbyTier, params, where, 'sca');
    params.push(limit);

    // Composition rows are maintained at ingest time by shared match lobby
    // tier. Reading a selected scope is therefore a small projection rollup,
    // not a scan and regroup of every historical match_player row.
    const rows = await query(
      `SELECT
         sca.comp_id,
         sca.frontline,
         sca.damage,
         sca.flank,
         sca.support,
         SUM(sca.uses)::BIGINT AS count,
         SUM(sca.wins)::BIGINT AS wins,
         SUM(sca.losses)::BIGINT AS losses,
         ROUND(
           100.0 * SUM(sca.wins)::NUMERIC
           / NULLIF((SUM(sca.wins) + SUM(sca.losses))::NUMERIC, 0),
           2
         ) AS winrate
       FROM stats_composition_aggregate sca
       WHERE ${where.join(' AND ')}
       GROUP BY sca.comp_id, sca.frontline, sca.damage, sca.flank, sca.support
       ORDER BY ${sort} ${order}, sca.comp_id
       LIMIT $${params.length}`,
      params,
    );

    return {
      total: rows.length,
      data: rows,
    };
  });
}
