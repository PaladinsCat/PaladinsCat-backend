import { query, one, transaction } from '../config/db';
import { dumpRawPayloads, getMatchPlayerDetails, getPlayerStatus } from '../services/hirez';
import { get as cacheGet, set as cacheSet } from '../services/cache';
import { normalizeLiveMatchPlayer, normalizePlayerStatus } from '../services/normalizer';

const LIVE_LOOKUP_TTL_SECONDS = 30;
const LIVE_PENDING_TTL_SECONDS = 10;
const liveLookupInFlight = new Map<number, Promise<any | null>>();

type LiveLookupCache = {
  state: 'live' | 'not_live' | 'pending';
  matchId?: number;
};

/** Read the latest locally persisted live lobby for a player. */
async function readStoredLiveMatch(playerId: number, matchId?: number): Promise<any | null> {
  const params: number[] = [playerId];
  const matchFilter = matchId ? ' AND lm.match_id = $2' : '';
  if (matchId) params.push(matchId);
  const matchRow = await one(`
    SELECT lm.* FROM live_matches lm
    JOIN live_match_players lmp ON lmp.match_id = lm.match_id
    WHERE lmp.player_id = $1 AND lm.status = 'active'${matchFilter}
    ORDER BY lm.detected_at DESC
    LIMIT 1
  `, params);
  if (!matchRow) return null;
  const players = await query(
    'SELECT * FROM live_match_players WHERE match_id = $1 ORDER BY task_force, id',
    [matchRow.match_id],
  );
  return { match: matchRow, players };
}

export async function getPlayerLiveMatch(
  playerId: number,
  beforeVendor?: (stage: 'player-status' | 'match-players', entity: number) => Promise<void>,
): Promise<any | null> {
  const cacheKey = `live_match_lookup:${playerId}`;
  const cached = await cacheGet<LiveLookupCache>(cacheKey);
  if (cached?.state === 'not_live') return null;
  if (cached?.state === 'pending') return { match: null, players: [], pending: true };
  if (cached?.state === 'live' && cached.matchId) {
    const stored = await readStoredLiveMatch(playerId, cached.matchId);
    if (stored) return stored;
  }

  const existing = liveLookupInFlight.get(playerId);
  if (existing) return existing;
  const lookup = refreshPlayerLiveMatch(playerId, cacheKey, beforeVendor)
    .finally(() => liveLookupInFlight.delete(playerId));
  liveLookupInFlight.set(playerId, lookup);
  return lookup;
}

async function refreshPlayerLiveMatch(
  playerId: number,
  cacheKey: string,
  beforeVendor?: (stage: 'player-status' | 'match-players', entity: number) => Promise<void>,
): Promise<any | null> {
  let rawStatuses: any[];
  try {
    await beforeVendor?.('player-status', playerId);
    rawStatuses = await getPlayerStatus(playerId, 'live_match_lookup');
  } catch (error) {
    await cacheSet(cacheKey, { state: 'pending' } satisfies LiveLookupCache, LIVE_PENDING_TTL_SECONDS);
    throw error;
  }

  await dumpRawPayloads([{
    endpoint: 'getplayerstatus',
    entity_type: 'player_status',
    entity_id: playerId,
    raw_data: rawStatuses,
    source: 'player-current-match',
  }]);

  const rawStatus = rawStatuses.find((row: any) => !(row?.ret_msg || '').trim()) ?? rawStatuses[0];
  if (!rawStatus || (rawStatus.ret_msg || '').trim()) {
    await cacheSet(cacheKey, { state: 'not_live' } satisfies LiveLookupCache, LIVE_LOOKUP_TTL_SECONDS);
    return null;
  }

  const status = normalizePlayerStatus({ ...rawStatus, player_id: rawStatus.player_id || playerId });
  await one(`
    INSERT INTO player_status (
      player_id, status, status_string, current_match_id, queue_id,
      privacy_flag, personal_status_message, updated_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,now())
    ON CONFLICT (player_id) DO UPDATE SET
      status = EXCLUDED.status,
      status_string = EXCLUDED.status_string,
      current_match_id = EXCLUDED.current_match_id,
      queue_id = EXCLUDED.queue_id,
      privacy_flag = EXCLUDED.privacy_flag,
      personal_status_message = EXCLUDED.personal_status_message,
      updated_at = now()
  `, [
    playerId, status.status, status.status_string, status.current_match_id,
    status.queue_id, status.privacy_flag, status.personal_status_message,
  ]);

  const matchId = Number(status.current_match_id || 0);
  if (matchId <= 0) {
    await cacheSet(cacheKey, { state: 'not_live' } satisfies LiveLookupCache, LIVE_LOOKUP_TTL_SECONDS);
    return null;
  }

  let rawPlayers: any[];
  try {
    await beforeVendor?.('match-players', matchId);
    rawPlayers = (await getMatchPlayerDetails(matchId, 'live_match_lookup'))
      .filter((row: any) => !(row?.ret_msg || '').trim());
  } catch (error) {
    await cacheSet(cacheKey, { state: 'pending', matchId } satisfies LiveLookupCache, LIVE_PENDING_TTL_SECONDS);
    throw error;
  }
  if (rawPlayers.length === 0) {
    await cacheSet(cacheKey, { state: 'pending', matchId } satisfies LiveLookupCache, LIVE_PENDING_TTL_SECONDS);
    return { match: null, players: [], pending: true };
  }

  await dumpRawPayloads([{
    endpoint: 'getmatchplayerdetails',
    entity_type: 'live_match',
    entity_id: matchId,
    raw_data: rawPlayers,
    source: 'player-current-match',
  }]);

  await persistLiveMatchSnapshot(matchId, playerId, status.queue_id, rawPlayers);
  await cacheSet(cacheKey, { state: 'live', matchId } satisfies LiveLookupCache, LIVE_LOOKUP_TTL_SECONDS);
  return readStoredLiveMatch(playerId, matchId);
}

/**
 * Fetch live match from Hi-Rez API and store.
 */
export async function fetchLiveMatch(matchId: number, _sourcePlayerId: number): Promise<any | null> {
  try {
    const rawPlayers = (await getMatchPlayerDetails(matchId, 'live_match_lookup'))
      .filter((row: any) => !(row?.ret_msg || '').trim());
    if (rawPlayers.length === 0) return null;
    await dumpRawPayloads([{
      endpoint: 'getmatchplayerdetails',
      entity_type: 'live_match',
      entity_id: matchId,
      raw_data: rawPlayers,
      source: 'live',
    }]);
    await persistLiveMatchSnapshot(matchId, _sourcePlayerId, null, rawPlayers);
    return readStoredLiveMatch(_sourcePlayerId, matchId);
  } catch (err) {
    console.error(`[LIVE-MATCH] Failed to fetch live match ${matchId}: ${err}`);
    return null;
  }
}

async function persistLiveMatchSnapshot(
  matchId: number,
  sourcePlayerId: number,
  statusQueueId: number | null,
  rawPlayers: any[],
): Promise<void> {
  const first = rawPlayers[0] || {};
  const queueId = Number(
    statusQueueId
    || first.match_queue_id
    || first.Match_Queue_Id
    || first.Queue
    || first.queue_id
    || 0,
  );
  const map = String(first.mapGame || first.Map_Game || first.map || '');
  const region = String(first.playerRegion || first.Region || first.region || 'Unknown');
  const normalized = rawPlayers.map((raw, index) => {
    const player = normalizeLiveMatchPlayer(raw);
    return {
      ...player,
      player_id: player.player_id > 0 ? player.player_id : -(index + 1),
      player_name: player.player_name || 'Private Account',
    };
  });

  await transaction(async (client) => {
    await client.query(`
      INSERT INTO live_matches (match_id, queue_id, region, map, detected_at, source_player_id, status, ended_at, dropped)
      VALUES ($1,$2,$3,$4,now(),$5,'active',NULL,false)
      ON CONFLICT (match_id) DO UPDATE SET
        queue_id = EXCLUDED.queue_id,
        region = EXCLUDED.region,
        map = EXCLUDED.map,
        detected_at = now(),
        source_player_id = EXCLUDED.source_player_id,
        status = 'active',
        ended_at = NULL,
        dropped = false
    `, [matchId, queueId, region, map, sourcePlayerId]);

    await client.query('DELETE FROM live_match_players WHERE match_id = $1', [matchId]);
    for (const player of normalized) {
      await client.query(`
        INSERT INTO live_match_players (
          match_id, player_id, player_name, champion_id, champion_name,
          skin_id, skin_name, account_level, mastery_level, tier,
          tier_wins, tier_losses, task_force, platform
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      `, [
        matchId, player.player_id, player.player_name, player.champion_id,
        player.champion_name, player.skin_id, player.skin_name,
        player.account_level, player.mastery_level, player.tier,
        player.tier_wins, player.tier_losses, player.task_force, player.portal_id,
      ]);
    }
  });
}

/**
 * Drop Detection - runs every 15 minutes.
 */
export async function detectDroppedMatches(): Promise<{ ended: number; dropped: number; suspects: number }> {
  let ended = 0;
  let dropped = 0;
  let suspects = 0;
  const staleMatches = await query(`
    SELECT match_id FROM live_matches
    WHERE status = 'active' AND detected_at < now() - INTERVAL '30 minutes'
  `);
  for (const row of staleMatches) {
    const matchId = row.match_id;
    const existsInMatches = await one(`SELECT 1 FROM matches WHERE match_id = $1`, [matchId]);
    if (existsInMatches) {
      await one(`UPDATE live_matches SET status = 'ended', ended_at = now() WHERE match_id = $1`, [matchId]);
      await one(`DELETE FROM live_match_players WHERE match_id = $1`, [matchId]);
      ended++;
    } else {
      await one(`UPDATE live_matches SET status = 'dropped', dropped = true, ended_at = now() WHERE match_id = $1`, [matchId]);
      const players = await query(`SELECT * FROM live_match_players WHERE match_id = $1`, [matchId]);
      for (const p of players) {
        const existing = await one(`
          SELECT incident_count FROM drop_hack_suspects
          WHERE player_id = $1 AND match_id = $2
        `, [p.player_id, matchId]);
        const isCassie = p.champion_id === 67;
        if (existing) {
          await one(`
            UPDATE drop_hack_suspects SET incident_count = incident_count + 1, dropped_at = now()
            WHERE player_id = $1 AND match_id = $2
          `, [p.player_id, matchId]);
        } else {
          await one(`
            INSERT INTO drop_hack_suspects (player_id, player_name, match_id, champion_id, champion_name, is_cassie, dropped_at, incident_count)
            VALUES ($1, $2, $3, $4, $5, $6, now(), 1)
          `, [p.player_id, p.player_name, matchId, p.champion_id, p.champion_name, isCassie]);
        }
        suspects++;
      }
      dropped++;
    }
  }
  console.log(`[DROP-DETECT] Ended: ${ended}, Dropped: ${dropped}, Suspect entries: ${suspects}`);
  return { ended, dropped, suspects };
}

/**
 * Get drop hack suspects with high incident count.
 */
export async function getDropHackSuspects(limit = 50): Promise<any[]> {
  return await query(`
    SELECT * FROM drop_hack_suspects
    WHERE incident_count > 1
    ORDER BY incident_count DESC
    LIMIT $1
  `, [limit]);
}
