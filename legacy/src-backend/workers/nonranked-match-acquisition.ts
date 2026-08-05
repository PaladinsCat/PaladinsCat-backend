import type { PoolClient } from 'pg';
import { query, transaction } from '../config/db';
import type { MatchDetails } from '../contracts/hirez-relay';
import { getMatchDetailsBatch } from '../services/hirez';
import { normalizePlayerProfile } from '../services/normalizer';
import { compactNonrankedRawPlayer } from '../services/nonranked-raw-json';
import { upsertPlayerProfile } from '../services/player-profile-store';
import {
  getMatchQueueDefinition,
  type MatchCountQueueDefinition,
  type MatchStatScope,
} from './match-count-discovery-policy';
import {
  recordPrivateAccountObservation,
  resolvePrivateAccountsForMatch,
} from '../services/private-account-resolver';
import { jsonForPostgresJsonb } from '../utils/postgres-json';
import { getApiHeadroomSnapshot } from './api-headroom';
import {
  calculateBackgroundMatchAllowance,
  getRankedPriorityReserveSnapshot,
  type RankedPriorityReserveSnapshot,
} from './ranked-priority-budget';
import {
  buildContinuousFetchLanes,
  fetchNonrankedMatchesContinuously,
  isCompleteNonrankedMatchDetail,
  orderUniquePresenceFacts,
  usableMatchPlayers,
  type HybridFetchResult,
} from './nonranked-acquisition-batching';

// This is a transaction/lease page size, not an hourly throughput ceiling.
// The worker continuously claims more pages while work and API headroom remain.
const CLAIM_PAGE_SIZE = Math.min(1000, Math.max(10, Number(process.env.NONRANKED_ACQUISITION_CLAIM_LIMIT || 500)));
const MAX_MATCHES_PER_RUN = Math.min(
  20_000,
  Math.max(CLAIM_PAGE_SIZE, Number(process.env.NONRANKED_ACQUISITION_MAX_MATCHES_PER_RUN || 2_000)),
);
const MAX_RUN_MS = Math.min(
  20 * 60_000,
  Math.max(30_000, Number(process.env.NONRANKED_ACQUISITION_MAX_RUN_MS || 50_000)),
);
const ACTIVE_MATCH_GRACE_MINUTES = Math.min(
  360,
  Math.max(10, Number(process.env.NONRANKED_ACTIVE_MATCH_GRACE_MINUTES || 30)),
);
// A non-ranked match spends one detail call in the worst ordered-blocker case
// and at most one roster fallback. Budget claims conservatively even though
// healthy detail batches normally amortize one call across ten matches.
const WORST_CASE_CALLS_PER_MATCH = 2;
const LEASE_MINUTES = 30;
const BATCH_SIZE = 10;
const FETCH_CONCURRENCY = Math.min(
  8,
  Math.max(1, Number(process.env.NONRANKED_ACQUISITION_FETCH_CONCURRENCY || 8)),
);
const PRIVATE_NAME = 'PRIVATEACCOUNT';
const ACQUISITION_REPAIR_LOOKBACK_HOURS = Math.max(
  24,
  Number(process.env.NONRANKED_ACQUISITION_REPAIR_LOOKBACK_HOURS || 48),
);

type AcquisitionRow = {
  match_id: string | number;
  queue_id: number;
  source_date: string;
  source_hour: number;
  region: string;
  discovered_entry_datetime: string | null;
  active_flag: boolean;
};

export type NonrankedAcquisitionOptions = {
  lookbackHours?: number;
  seedLedger?: boolean;
  /** Operator-only UTC day filter for a bounded historical repair. */
  sourceDate?: string;
};

function acquisitionLookbackHours(value?: number): number {
  if (value === undefined) return ACQUISITION_REPAIR_LOOKBACK_HOURS;
  return Math.min(168, Math.max(1, Math.trunc(value)));
}

function acquisitionSourceDate(value?: string): string | undefined {
  if (value === undefined) return undefined;
  const date = value.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error('Non-ranked acquisition sourceDate must be a UTC YYYY-MM-DD date');
  }
  return date;
}

function finiteInt(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function clean(value: unknown): string {
  return String(value ?? '')
    .replace(/\u0000/g, '')
    .replace(/\\u0000/g, '')
    .trim();
}

function firstValue(row: any, ...keys: string[]): unknown {
  for (const key of keys) {
    if (row?.[key] !== undefined && row?.[key] !== null) return row[key];
  }
  return undefined;
}

async function seedAcquisitionLedger(lookbackHours: number): Promise<void> {
  // Normal discovery inserts its ledger row in the same transaction. This
  // bounded anti-join is a safety repair for a deployment interrupted between
  // old-version discovery and the migration/backfill boundary.
  await query(
    `INSERT INTO nonranked_match_acquisition (
       match_id, queue_id, stats_scope, source_date, source_hour, region,
       discovered_entry_datetime, active_flag, status,
       first_discovered_at, last_observed_at
     )
     SELECT d.match_id, d.queue_id, COALESCE(q.stats_scope, 'other'),
            d.source_date, d.source_hour, d.region, d.entry_datetime, d.active_flag,
            CASE WHEN d.active_flag THEN 'waiting_for_completion' ELSE 'discovered' END,
            d.first_seen_at, d.last_seen_at
     FROM match_count_discoveries d
     JOIN queue_types q ON q.queue_id = d.queue_id
     LEFT JOIN nonranked_match_acquisition existing ON existing.match_id = d.match_id
     WHERE d.queue_id <> 486
       AND existing.match_id IS NULL
       AND d.last_seen_at >= now() - ($1::int * interval '1 hour')
     ON CONFLICT (match_id) DO NOTHING`,
    [lookbackHours],
  );

  // Refresh recent discovery metadata separately. In particular, an active
  // observation can become non-active if its source hour is deliberately
  // rechecked, allowing it to skip the grace wait.
  await query(
    `UPDATE nonranked_match_acquisition acquisition SET
       queue_id = discovery.queue_id,
       stats_scope = COALESCE(queue.stats_scope, acquisition.stats_scope),
       last_observed_at = GREATEST(acquisition.last_observed_at, discovery.last_seen_at),
       region = CASE
         WHEN discovery.region <> 'Unknown' THEN discovery.region
         ELSE acquisition.region
       END,
       discovered_entry_datetime = COALESCE(
         acquisition.discovered_entry_datetime,
         discovery.entry_datetime
       ),
       active_flag = discovery.active_flag,
       status = CASE
         WHEN acquisition.status IN ('discovered', 'waiting_for_completion')
              AND discovery.active_flag
           THEN 'waiting_for_completion'
         WHEN acquisition.status = 'waiting_for_completion'
              AND NOT discovery.active_flag
           THEN 'discovered'
         ELSE acquisition.status
       END,
       updated_at = now()
     FROM match_count_discoveries discovery
     JOIN queue_types queue ON queue.queue_id = discovery.queue_id
     WHERE acquisition.match_id = discovery.match_id
       AND discovery.queue_id <> 486
       AND discovery.last_seen_at >= now() - ($1::int * interval '1 hour')
       -- This job runs every five minutes. The discovery worker already
       -- upserts normal metadata transitions, so the repair pass must not
       -- rewrite the entire 48-hour ledger merely to advance updated_at.
       -- Avoiding unchanged tuple versions removes tens of thousands of
       -- recurring writes, index churn, and deadlock opportunities.
       AND (
         acquisition.queue_id IS DISTINCT FROM discovery.queue_id
         OR acquisition.stats_scope IS DISTINCT FROM COALESCE(
              queue.stats_scope,
              acquisition.stats_scope
            )
         OR acquisition.last_observed_at < discovery.last_seen_at
         OR acquisition.region IS DISTINCT FROM CASE
              WHEN discovery.region <> 'Unknown' THEN discovery.region
              ELSE acquisition.region
            END
         OR acquisition.discovered_entry_datetime IS DISTINCT FROM COALESCE(
              acquisition.discovered_entry_datetime,
              discovery.entry_datetime
            )
         OR acquisition.active_flag IS DISTINCT FROM discovery.active_flag
         OR acquisition.status IS DISTINCT FROM CASE
              WHEN acquisition.status IN ('discovered', 'waiting_for_completion')
                   AND discovery.active_flag
                THEN 'waiting_for_completion'
              WHEN acquisition.status = 'waiting_for_completion'
                   AND NOT discovery.active_flag
                THEN 'discovered'
              ELSE acquisition.status
            END
       )`,
    [lookbackHours],
  );
}

async function claimMatches(limit: number, sourceDate?: string): Promise<AcquisitionRow[]> {
  return transaction(async client => {
    const claimed = await client.query<AcquisitionRow>(
      `WITH due AS (
         SELECT match_id,
                source_date + (source_hour * interval '1 hour') AS source_bucket
         FROM nonranked_match_acquisition
         WHERE (
             status = 'discovered'
             OR (
               status = 'waiting_for_completion'
               AND last_observed_at
                   <= now() - ($3::int * interval '1 minute')
             )
           )
           AND (lease_until IS NULL OR lease_until <= now())
           AND ($4::text IS NULL OR source_date = $4::text::date)
         ORDER BY
           CASE
             WHEN source_date + (source_hour * interval '1 hour')
                    >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
               THEN 0
             ELSE 1
           END,
           CASE
             WHEN source_date + (source_hour * interval '1 hour')
                    >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
               THEN source_date + (source_hour * interval '1 hour')
           END DESC,
           CASE
             WHEN source_date + (source_hour * interval '1 hour')
                    < (now() AT TIME ZONE 'UTC') - interval '24 hours'
               THEN source_date + (source_hour * interval '1 hour')
           END ASC,
           match_id
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       )
       UPDATE nonranked_match_acquisition a SET
         status = 'fetching',
         detail_attempts = detail_attempts + 1,
         last_attempt_at = now(),
         lease_until = now() + ($2::int * interval '1 minute'),
         error_message = NULL,
         updated_at = now()
       FROM due
       WHERE a.match_id = due.match_id
       RETURNING a.match_id, a.queue_id, a.source_date::text, a.source_hour,
                 a.region, a.discovered_entry_datetime::text, a.active_flag`,
      [limit, LEASE_MINUTES, ACTIVE_MATCH_GRACE_MINUTES, sourceDate ?? null],
    );
    return claimed.rows;
  });
}

async function terminalizeInterruptedClaims(): Promise<number> {
  const rows = await query(
    `UPDATE nonranked_match_acquisition SET
       status='dropped',
       quality='unavailable',
       lease_until=NULL,
       terminal_reason=COALESCE(
         terminal_reason,
         'worker_interrupted_single_pass_not_retried'
       ),
       error_message=COALESCE(error_message, 'Acquisition lease expired before persistence'),
       completed_at=COALESCE(completed_at, now()),
       updated_at=now()
     WHERE status IN ('fetching', 'service_deferred')
       AND (lease_until IS NULL OR lease_until <= now())
     RETURNING match_id`,
  );
  return rows.length;
}

function matchEntry(detail: MatchDetails | undefined, row: AcquisitionRow): string {
  const candidate = detail?.entry_datetime || row.discovered_entry_datetime;
  const parsed = candidate ? new Date(candidate) : new Date(`${row.source_date}T${String(row.source_hour).padStart(2, '0')}:00:00.000Z`);
  return Number.isNaN(parsed.getTime())
    ? new Date(`${row.source_date}T${String(row.source_hour).padStart(2, '0')}:00:00.000Z`).toISOString()
    : parsed.toISOString();
}

function rosterIdentity(row: any): { playerId: number; playerName: string } {
  return {
    playerId: finiteInt(firstValue(row, 'player_id', 'playerId', 'Id', 'ActivePlayerId')),
    playerName: clean(firstValue(row, 'player_name', 'playerName', 'Name', 'hz_player_name', 'hz_gamer_tag')),
  };
}

function participantKind(player: any, definition: MatchCountQueueDefinition): 'human' | 'private' | 'bot' | 'unknown' {
  const identity = rosterIdentity(player);
  if (identity.playerName.toUpperCase() === PRIVATE_NAME) return 'private';
  if (identity.playerId > 0) return 'human';
  if (definition.participantModel === 'bots') return 'bot';
  if (/\bbot\b/i.test(identity.playerName)) return 'bot';
  return 'unknown';
}

function mergedPlayers(detail: MatchDetails | undefined, roster: any[] | undefined): any[] {
  const direct = usableMatchPlayers(detail);
  if (!roster?.length) return direct;
  const byId = new Map<number, any>();
  const byName = new Map<string, any>();
  for (const player of direct) {
    const identity = rosterIdentity(player);
    if (identity.playerId > 0) byId.set(identity.playerId, player);
    if (identity.playerName) byName.set(identity.playerName.toLowerCase(), player);
  }
  const merged = [...direct];
  for (const profile of roster) {
    const identity = rosterIdentity(profile);
    if (
      (identity.playerId > 0 && byId.has(identity.playerId))
      || (identity.playerName && byName.has(identity.playerName.toLowerCase()))
    ) continue;
    merged.push({ ...profile, source: 'roster' });
  }
  return merged;
}

async function persistNewRosterProfiles(client: PoolClient, roster: any[] | undefined): Promise<void> {
  if (!roster?.length) return;
  const normalized = roster
    .map(raw => normalizePlayerProfile(raw))
    .filter(profile => Number.isInteger(profile.player_id) && profile.player_id > 0);
  if (normalized.length === 0) return;
  const ids = [...new Set(normalized.flatMap(profile => [
    profile.player_id,
    profile.active_player_id,
  ].filter(id => Number.isInteger(id) && id > 0)))];
  const existing = await client.query<{ id: string | number }>(
    `SELECT id
     FROM players
     WHERE id = ANY($1::bigint[])
        OR active_player_id = ANY($1::bigint[])`,
    [ids],
  );
  const existingIds = new Set(existing.rows.map(row => Number(row.id)));
  for (const profile of normalized) {
    if (existingIds.has(profile.player_id) || existingIds.has(profile.active_player_id)) continue;
    await upsertPlayerProfile(profile, client);
    existingIds.add(profile.player_id);
    if (profile.active_player_id > 0) existingIds.add(profile.active_player_id);
  }
}

function playerFact(player: any, definition: MatchCountQueueDefinition, direct: boolean) {
  const identity = rosterIdentity(player);
  const kind = participantKind(player, definition);
  return {
    playerId: identity.playerId,
    playerName: identity.playerName || null,
    championId: finiteInt(firstValue(player, 'champion_id', 'ChampionId', 'Champion_ID')),
    championName: clean(firstValue(player, 'champion_name', 'Reference_Name', 'ChampionName')) || null,
    taskForce: finiteInt(firstValue(player, 'task_force', 'TaskForce')),
    winStatus: clean(firstValue(player, 'win_status', 'Win_Status')) || null,
    kills: finiteInt(firstValue(player, 'kills', 'Kills_Player', 'Kills')),
    deaths: finiteInt(firstValue(player, 'deaths', 'Deaths')),
    assists: finiteInt(firstValue(player, 'assists', 'Assists')),
    // Prefer the vendor's combined player-damage field when a raw detail row
    // reaches this adapter. Normalized rows store the same total in the
    // historical `damage_done_physical` key.
    damage: finiteInt(firstValue(player, 'Damage_Player', 'damage_done_physical', 'Damage', 'Damage_Done_Physical')),
    damageTaken: finiteInt(firstValue(player, 'damage_taken', 'Damage_Taken')),
    healing: finiteInt(firstValue(player, 'healing', 'Healing')),
    mitigation: finiteInt(firstValue(player, 'damage_mitigated', 'Damage_Mitigated')),
    credits: finiteInt(firstValue(player, 'gold_earned', 'Gold_Earned', 'Gold')),
    objectiveTime: finiteInt(firstValue(
      player,
      'objective_time',
      'objective_assists',
      'Objective_Assists',
      'Objective_Time',
    )),
    accountLevel: finiteInt(firstValue(player, 'account_level', 'Account_Level', 'Level')),
    masteryLevel: finiteInt(firstValue(player, 'mastery_level', 'Mastery_Level')),
    partyId: finiteInt(firstValue(player, 'party_id', 'PartyId')),
    portalId: finiteInt(firstValue(player, 'portal_id', 'PortalId', 'Portal_ID')),
    portalUserId: clean(firstValue(player, 'portal_user_id', 'PortalUserId', 'hz_player_name')) || null,
    platform: clean(firstValue(player, 'platform', 'Platform')) || null,
    kind,
    source: direct ? clean(player?.source) || 'direct' : 'roster',
  };
}

async function writePlayerFacts(
  client: PoolClient,
  table: 'casual_match_players' | 'special_match_players',
  matchId: number,
  players: any[],
  definition: MatchCountQueueDefinition,
  complete: boolean,
): Promise<void> {
  await client.query(`DELETE FROM ${table} WHERE match_id = $1`, [matchId]);
  let privateSlot = 0;
  for (let index = 0; index < players.length; index++) {
    const raw = players[index];
    const fact = playerFact(raw, definition, clean(raw?.source).toLowerCase() !== 'roster');
    const eligible = complete && fact.kind === 'human' && fact.championId > 0;
    if (fact.kind === 'private') privateSlot += 1;
    await client.query(
      `INSERT INTO ${table} (
         match_id, roster_slot, private_slot, player_id, player_name, champion_id, champion_name,
         task_force, win_status, kills, deaths, assists, damage, damage_taken,
         healing, mitigation, credits, objective_time, account_level, mastery_level,
         party_id, portal_id, portal_user_id, platform, participant_kind,
         source, stats_eligible, raw_player
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
         $19,$20,$21,$22,$23,$24,$25,$26,$27,$28::jsonb
       )`,
      [
        matchId, index + 1, fact.kind === 'private' ? privateSlot : 0,
        fact.playerId, fact.playerName, fact.championId || null,
        fact.championName, fact.taskForce || null, fact.winStatus, fact.kills,
        fact.deaths, fact.assists, fact.damage, fact.damageTaken, fact.healing,
        fact.mitigation, fact.credits, fact.objectiveTime, fact.accountLevel,
        fact.masteryLevel, fact.partyId, fact.portalId, fact.portalUserId,
        fact.platform, fact.kind, fact.source, eligible,
        jsonForPostgresJsonb(compactNonrankedRawPlayer(raw)),
      ],
    );
  }
}

async function replaceProjectionForMatch(
  client: PoolClient,
  matchId: number,
  definition: MatchCountQueueDefinition,
  entry: string,
  region: string,
  map: string,
  duration: number,
  players: any[],
): Promise<void> {
  // Acquisition is exactly-once terminal, but the timestamp guard also makes
  // manual repair/replay safe.
  const ledger = await client.query<{ stats_projected_at: string | null }>(
    `SELECT stats_projected_at::text FROM nonranked_match_acquisition WHERE match_id = $1 FOR UPDATE`,
    [matchId],
  );
  if (ledger.rows[0]?.stats_projected_at) return;

  await client.query(
    `INSERT INTO nonranked_map_stats_daily (
       stats_date, stats_scope, queue_id, region, map, matches, duration_sum
     ) VALUES ($1::timestamptz::date,$2,$3,$4,$5,1,$6)
     ON CONFLICT (stats_date, stats_scope, queue_id, region, map) DO UPDATE SET
       matches = nonranked_map_stats_daily.matches + 1,
       duration_sum = nonranked_map_stats_daily.duration_sum + EXCLUDED.duration_sum,
       updated_at = now()`,
    [entry, definition.scope, definition.queueId, region, map, duration],
  );

  for (const raw of players) {
    const fact = playerFact(raw, definition, true);
    if (fact.kind !== 'human' || fact.championId <= 0) continue;
    const normalizedWin = /^(winner|win)$/i.test(fact.winStatus || '');
    const normalizedLoss = /^(loser|loss)$/i.test(fact.winStatus || '');
    await client.query(
      `INSERT INTO nonranked_champion_stats_daily (
         stats_date, stats_scope, queue_id, region, map, champion_id,
         plays, wins, losses, kills_sum, deaths_sum, assists_sum, damage_sum,
         healing_sum, mitigation_sum, credits_sum, duration_sum
       ) VALUES (
         $1::timestamptz::date,$2,$3,$4,$5,$6,1,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16
       )
       ON CONFLICT (stats_date, stats_scope, queue_id, region, map, champion_id) DO UPDATE SET
         plays = nonranked_champion_stats_daily.plays + 1,
         wins = nonranked_champion_stats_daily.wins + EXCLUDED.wins,
         losses = nonranked_champion_stats_daily.losses + EXCLUDED.losses,
         kills_sum = nonranked_champion_stats_daily.kills_sum + EXCLUDED.kills_sum,
         deaths_sum = nonranked_champion_stats_daily.deaths_sum + EXCLUDED.deaths_sum,
         assists_sum = nonranked_champion_stats_daily.assists_sum + EXCLUDED.assists_sum,
         damage_sum = nonranked_champion_stats_daily.damage_sum + EXCLUDED.damage_sum,
         healing_sum = nonranked_champion_stats_daily.healing_sum + EXCLUDED.healing_sum,
         mitigation_sum = nonranked_champion_stats_daily.mitigation_sum + EXCLUDED.mitigation_sum,
         credits_sum = nonranked_champion_stats_daily.credits_sum + EXCLUDED.credits_sum,
         duration_sum = nonranked_champion_stats_daily.duration_sum + EXCLUDED.duration_sum,
         updated_at = now()`,
      [
        entry, definition.scope, definition.queueId, region, map, fact.championId,
        normalizedWin ? 1 : 0, normalizedLoss ? 1 : 0, fact.kills, fact.deaths,
        fact.assists, fact.damage, fact.healing, fact.mitigation, fact.credits, duration,
      ],
    );
  }
  await client.query(
    `UPDATE nonranked_match_acquisition SET stats_projected_at = now(), updated_at = now() WHERE match_id = $1`,
    [matchId],
  );
}

async function writePresence(
  client: PoolClient,
  matchId: number,
  entry: string,
  definition: MatchCountQueueDefinition,
  players: any[],
): Promise<void> {
  if (!definition.trackPresence) return;
  const presenceFacts = orderUniquePresenceFacts(
    players
      .map(raw => playerFact(raw, definition, true))
      .filter(fact => fact.kind === 'human' && fact.playerId > 0),
  );
  for (const fact of presenceFacts) {
    await client.query(
      `INSERT INTO player_presence_24h (
         player_id, first_observed_at, last_observed_at, last_match_id,
         last_queue_id, last_stats_scope
       ) VALUES ($1,$2,$2,$3,$4,$5)
       ON CONFLICT (player_id) DO UPDATE SET
         first_observed_at = LEAST(player_presence_24h.first_observed_at, EXCLUDED.first_observed_at),
         last_observed_at = GREATEST(player_presence_24h.last_observed_at, EXCLUDED.last_observed_at),
         last_match_id = CASE WHEN EXCLUDED.last_observed_at >= player_presence_24h.last_observed_at THEN EXCLUDED.last_match_id ELSE player_presence_24h.last_match_id END,
         last_queue_id = CASE WHEN EXCLUDED.last_observed_at >= player_presence_24h.last_observed_at THEN EXCLUDED.last_queue_id ELSE player_presence_24h.last_queue_id END,
         last_stats_scope = CASE WHEN EXCLUDED.last_observed_at >= player_presence_24h.last_observed_at THEN EXCLUDED.last_stats_scope ELSE player_presence_24h.last_stats_scope END,
         updated_at = now()`,
      [fact.playerId, entry, matchId, definition.queueId, definition.scope],
    );
    await client.query(
      `INSERT INTO player_queue_presence_24h (
         player_id, queue_id, stats_scope, first_observed_at,
         last_observed_at, last_match_id
       ) VALUES ($1,$2,$3,$4,$4,$5)
       ON CONFLICT (player_id,queue_id) DO UPDATE SET
         first_observed_at = LEAST(player_queue_presence_24h.first_observed_at, EXCLUDED.first_observed_at),
         last_observed_at = GREATEST(player_queue_presence_24h.last_observed_at, EXCLUDED.last_observed_at),
         last_match_id = CASE
           WHEN EXCLUDED.last_observed_at >= player_queue_presence_24h.last_observed_at
             THEN EXCLUDED.last_match_id
           ELSE player_queue_presence_24h.last_match_id
         END,
         stats_scope = EXCLUDED.stats_scope,
         updated_at = now()`,
      [fact.playerId, definition.queueId, definition.scope, entry, matchId],
    );
  }
}

async function persistResult(row: AcquisitionRow, result: HybridFetchResult): Promise<void> {
  const definition = getMatchQueueDefinition(row.queue_id);
  const detail = result.detail;
  const complete = result.state === 'complete_direct'
    && isCompleteNonrankedMatchDetail(detail, definition.participantModel);
  const players = mergedPlayers(detail, result.roster);
  const entry = matchEntry(detail, row);
  const region = clean(detail?.region) || row.region || 'Unknown';
  const map = clean(detail?.map) || 'Unknown';
  const duration = Math.max(0, finiteInt(detail?.duration_seconds));
  const quality = complete ? 'complete' : players.length > 0 ? (detail ? 'partial' : 'limited') : 'unavailable';
  const statsEligible = complete && definition.statsEnabled;
  const table = definition.scope === 'casual' ? 'casual_matches' : 'special_matches';
  const playerTable = definition.scope === 'casual' ? 'casual_match_players' : 'special_match_players';

  await transaction(async client => {
    // The roster response already contains account fields. Store it only for
    // accounts absent from the local player table; never refresh an existing
    // profile as a side effect of activity counting.
    await persistNewRosterProfiles(client, result.roster);
    if (players.length > 0) {
      if (table === 'casual_matches') {
        await client.query(
          `INSERT INTO casual_matches (
             match_id, queue_id, entry_datetime, region, map, duration_seconds,
             team1_score, team2_score, winning_task_force, quality, stats_eligible,
             player_count, source, raw_match
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)
           ON CONFLICT (match_id) DO UPDATE SET
             entry_datetime=EXCLUDED.entry_datetime, region=EXCLUDED.region, map=EXCLUDED.map,
             duration_seconds=EXCLUDED.duration_seconds, team1_score=EXCLUDED.team1_score,
             team2_score=EXCLUDED.team2_score, winning_task_force=EXCLUDED.winning_task_force,
             quality=EXCLUDED.quality, stats_eligible=EXCLUDED.stats_eligible,
             player_count=EXCLUDED.player_count, source=EXCLUDED.source,
             raw_match=EXCLUDED.raw_match, updated_at=now()`,
          [
            result.matchId, definition.queueId, entry, region, map, duration,
            detail?.team1_score ?? null, detail?.team2_score ?? null,
            detail?.winning_task_force ?? null, quality, statsEligible, players.length,
            complete
              ? (detail?.recovery_attempted === true ? 'relay_recovered' : 'direct')
              : result.state,
            null,
          ],
        );
      } else {
        await client.query(
          `INSERT INTO special_matches (
             match_id, queue_id, stats_scope, participant_model, entry_datetime,
             region, map, duration_seconds, team1_score, team2_score,
             winning_task_force, quality, stats_eligible, player_count, source, raw_match
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb)
           ON CONFLICT (match_id) DO UPDATE SET
             queue_id=EXCLUDED.queue_id, stats_scope=EXCLUDED.stats_scope,
             participant_model=EXCLUDED.participant_model,
             entry_datetime=EXCLUDED.entry_datetime, region=EXCLUDED.region,
             map=EXCLUDED.map, duration_seconds=EXCLUDED.duration_seconds,
             team1_score=EXCLUDED.team1_score, team2_score=EXCLUDED.team2_score,
             winning_task_force=EXCLUDED.winning_task_force, quality=EXCLUDED.quality,
             stats_eligible=EXCLUDED.stats_eligible, player_count=EXCLUDED.player_count,
             source=EXCLUDED.source, raw_match=EXCLUDED.raw_match, updated_at=now()`,
          [
            result.matchId, definition.queueId, definition.scope, definition.participantModel,
            entry, region, map, duration, detail?.team1_score ?? null,
            detail?.team2_score ?? null, detail?.winning_task_force ?? null,
            quality, statsEligible, players.length,
            complete
              ? (detail?.recovery_attempted === true ? 'relay_recovered' : 'direct')
              : result.state,
            null,
          ],
        );
      }
      await writePlayerFacts(client, playerTable, result.matchId, players, definition, complete);
      await writePresence(client, result.matchId, entry, definition, players);
      if (statsEligible) {
        await replaceProjectionForMatch(client, result.matchId, definition, entry, region, map, duration, players);
      }
    }

    await client.query(
      `UPDATE nonranked_match_acquisition SET
         status=$2, quality=$3, direct_player_count=$4, roster_player_count=$5,
         roster_attempts=CASE WHEN $6 THEN roster_attempts + 1 ELSE roster_attempts END,
         terminal_reason=$7, error_message=NULL, lease_until=NULL,
         completed_at=now(), updated_at=now()
       WHERE match_id=$1`,
      [
        result.matchId, result.state, quality, usableMatchPlayers(detail).length,
        result.roster?.length ?? 0, result.state !== 'complete_direct',
        result.terminalReason ?? null,
      ],
    );
  });

  // Private identity enrichment is intentionally outside the acquisition
  // transaction. A resolver failure must not strand a successfully persisted
  // match or prevent later claimed IDs from being released.
  try {
    let privateSlot = 0;
    for (const raw of players) {
      if (participantKind(raw, definition) !== 'private') continue;
      privateSlot += 1;
      const fact = playerFact(raw, definition, clean(raw?.source).toLowerCase() !== 'roster');
      // Roster fallback uses Hi-Rez's PascalCase field names. Normalize it to
      // the same observation contract as direct detail rows so private roster
      // slots are not silently skipped.
      const observationPlayer = {
        player_id: fact.playerId,
        player_name: fact.playerName,
        party_id: fact.partyId,
        account_level: fact.accountLevel,
        mastery_level: fact.masteryLevel,
        league_tier: finiteInt(firstValue(raw, 'league_tier', 'League_Tier', 'Tier')),
        league_points: finiteInt(firstValue(raw, 'league_points', 'League_Points', 'Points')),
        champion_id: fact.championId,
        task_force: fact.taskForce,
        win_status: fact.winStatus,
        portal_id: fact.portalId,
        portal_user_id: fact.portalUserId,
        platform: fact.platform,
        source: fact.source,
      };
      await recordPrivateAccountObservation(result.matchId, privateSlot, observationPlayer, entry, {
        queueId: definition.queueId,
        statsScope: definition.scope,
        map,
        matchEndDatetime: new Date(new Date(entry).getTime() + duration * 1000).toISOString(),
        observationQuality: quality,
      });
    }
    if (privateSlot > 0) await resolvePrivateAccountsForMatch(result.matchId);
  } catch (error) {
    console.error(`[nonranked-acquisition] private identity enrichment failed for ${result.matchId}`, error);
  }
}

async function terminalizeClaims(
  rows: AcquisitionRow[],
  error: unknown,
): Promise<void> {
  const ids = rows.map(row => finiteInt(row.match_id));
  if (ids.length === 0) return;
  await query(
    `UPDATE nonranked_match_acquisition SET
       status='dropped',
       quality='unavailable',
       lease_until=NULL,
       terminal_reason='single_pass_worker_failure',
       error_message=$2,
       completed_at=now(),
       updated_at=now()
     WHERE match_id = ANY($1::bigint[])
       AND status = 'fetching'`,
    [
      ids,
      error instanceof Error ? error.message : String(error),
    ],
  );
}

async function readBacklogSnapshot(): Promise<{
  open24h: number;
  waitingForCompletion24h: number;
  historicalOpen: number;
  oldestOpenHour: string | null;
}> {
  const rows = await query<{
    open_24h: string | number;
    waiting_24h: string | number;
    historical_open: string | number;
    oldest_open_hour: string | null;
  }>(
    `SELECT
       COUNT(*) FILTER (
         WHERE source_date + source_hour * interval '1 hour'
                 >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
       ) AS open_24h,
       COUNT(*) FILTER (
         WHERE status = 'waiting_for_completion'
           AND source_date + source_hour * interval '1 hour'
                 >= (now() AT TIME ZONE 'UTC') - interval '24 hours'
       ) AS waiting_24h,
       COUNT(*) FILTER (
         WHERE source_date + source_hour * interval '1 hour'
                 < (now() AT TIME ZONE 'UTC') - interval '24 hours'
       ) AS historical_open,
       MIN(source_date + source_hour * interval '1 hour')::text AS oldest_open_hour
     FROM nonranked_match_acquisition
     WHERE status IN (
       'discovered', 'waiting_for_completion', 'fetching'
     )`,
  );
  const row = rows[0];
  return {
    open24h: Number(row?.open_24h || 0),
    waitingForCompletion24h: Number(row?.waiting_24h || 0),
    historicalOpen: Number(row?.historical_open || 0),
    oldestOpenHour: row?.oldest_open_hour ?? null,
  };
}

export async function runNonrankedMatchAcquisition(
  reason = 'manual',
  options: NonrankedAcquisitionOptions = {},
): Promise<{
  claimed: number;
  complete: number;
  partial: number;
  dropped: number;
}> {
  const startedAt = Date.now();
  const lookbackHours = acquisitionLookbackHours(options.lookbackHours);
  const sourceDate = acquisitionSourceDate(options.sourceDate);
  if (options.seedLedger !== false) {
    await seedAcquisitionLedger(lookbackHours);
  }
  const interrupted = await terminalizeInterruptedClaims();
  if (interrupted > 0) {
    console.warn(
      `[nonranked-acquisition] ${reason}: terminalized ${interrupted} interrupted legacy claim(s) without vendor retry`,
    );
  }
  const totals = { claimed: 0, complete: 0, partial: 0, dropped: 0 };
  let rankedPriorityReserve: RankedPriorityReserveSnapshot | null = null;

  while (
    totals.claimed < MAX_MATCHES_PER_RUN
    && Date.now() - startedAt < MAX_RUN_MS
  ) {
    const headroom = await getApiHeadroomSnapshot();
    rankedPriorityReserve = await getRankedPriorityReserveSnapshot();
    if (!headroom.hasUsableKeys) {
      console.warn(`[nonranked-acquisition] ${reason}: paused; no usable Hi-Rez key headroom`);
      break;
    }
    const remainingRunCapacity = MAX_MATCHES_PER_RUN - totals.claimed;
    let claimLimit = Math.min(CLAIM_PAGE_SIZE, remainingRunCapacity);
    if (headroom.totalKeys > 0) {
      claimLimit = Math.min(
        claimLimit,
        calculateBackgroundMatchAllowance({
          usableCalls: headroom.totalUsableBeforeReserve,
          rankedPriorityReserveCalls: rankedPriorityReserve.reservedCalls,
          worstCaseCallsPerMatch: WORST_CASE_CALLS_PER_MATCH,
        }),
      );
    }
    if (claimLimit <= 0) {
      console.warn(
        `[nonranked-acquisition] ${reason}: paused at protected headroom ` +
        `(usableBeforeReserve=${headroom.totalUsableBeforeReserve}, ` +
        `rankedPriorityReserve=${rankedPriorityReserve.reservedCalls}, ` +
        `protectedRankedMatches=${rankedPriorityReserve.protectedRankedMatches})`,
      );
      break;
    }

    const rows = await claimMatches(claimLimit, sourceDate);
    if (rows.length === 0) break;
    totals.claimed += rows.length;
    const byId = new Map(rows.map(row => [finiteInt(row.match_id), row]));
    const persistedIds = new Set<number>();
    const lanes = buildContinuousFetchLanes([...byId.keys()], FETCH_CONCURRENCY);
    const laneFailures: Array<{ error: unknown; dropped: number }> = [];

    await Promise.all(lanes.map(async laneIds => {
      try {
        await fetchNonrankedMatchesContinuously(
          laneIds.map(matchId => ({
            matchId,
            queueId: byId.get(matchId)?.queue_id,
          })),
          {
            getMatchDetailsBatch: requests => (
              getMatchDetailsBatch(requests, 'presence_acquisition')
            ),
          },
          {
            onResult: async result => {
              const row = byId.get(result.matchId);
              if (!row) throw new Error(`Missing acquisition claim for match ${result.matchId}`);
              await persistResult(row, result);
              persistedIds.add(result.matchId);
              if (result.state === 'complete_direct') totals.complete += 1;
              else if (result.state === 'dropped') totals.dropped += 1;
              else totals.partial += 1;
            },
          },
        );
      } catch (error) {
        // This is a worker/persistence failure, not a reason to repeat vendor
        // calls. Terminalize every unpersisted claim in this lane.
        const unpersisted = laneIds
          .map(matchId => byId.get(matchId))
          .filter((row): row is AcquisitionRow => Boolean(
            row && !persistedIds.has(finiteInt(row.match_id)),
          ));
        await terminalizeClaims(unpersisted, error);
        totals.dropped += unpersisted.length;
        laneFailures.push({ error, dropped: unpersisted.length });
      }
    }));

    if (laneFailures.length > 0) {
      const dropped = laneFailures.reduce((sum, failure) => sum + failure.dropped, 0);
      console.warn(
        `[nonranked-acquisition] ${reason}: ${laneFailures.length}/${lanes.length} lane(s) ` +
        `stopped; terminalized ${dropped} uncommitted match(es) after preserving ` +
        `${persistedIds.size} result(s) (${laneFailures[0].error})`,
      );
    }

    if (rows.length < claimLimit) break;
  }

  const backlog = await readBacklogSnapshot();
  const summary =
    `[nonranked-acquisition] ${reason}: claimed=${totals.claimed}, ` +
    `complete=${totals.complete}, limited=${totals.partial}, dropped=${totals.dropped}, ` +
    `rankedPriorityReserve=${rankedPriorityReserve?.reservedCalls ?? 'unavailable'}, ` +
    `open24h=${backlog.open24h}, waitingActive24h=${backlog.waitingForCompletion24h}, ` +
    `historicalOpen=${backlog.historicalOpen}, oldestOpen=${backlog.oldestOpenHour || 'none'}, ` +
    `sourceDate=${sourceDate || 'all'}, ` +
    `elapsedMs=${Date.now() - startedAt}`;
  if (backlog.open24h > backlog.waitingForCompletion24h) console.warn(summary);
  else console.log(summary);
  return totals;
}
