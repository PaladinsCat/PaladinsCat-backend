import type { PoolClient, QueryResultRow } from 'pg';
import { one, pool, query, transaction } from '../config/db';
import {
  PRIVATE_IDENTITY_LINK_THRESHOLD,
  PRIVATE_IDENTITY_MARGIN,
  PRIVATE_IDENTITY_VERSION,
  hasPrivateIdentityEvidence,
  privateObservationFromPlayer,
  scorePrivateIdentity,
  type IdentityScore,
  type PrivateAccountObservation,
} from '../utils/private-account-identity';

export {
  PRIVATE_IDENTITY_LINK_THRESHOLD,
  PRIVATE_IDENTITY_MARGIN,
  PRIVATE_IDENTITY_VERSION,
  scorePrivateIdentity,
} from '../utils/private-account-identity';
export type { IdentityScore, PrivateAccountObservation } from '../utils/private-account-identity';

const PRIVATE_ACCOUNT_NAME = 'PRIVATEACCOUNT';
const PRIVATE_RESOLVER_LOCK = 812_240_583;

interface ObservationRow {
  match_id: string | number;
  private_slot: number;
  entry_datetime: string | Date;
  party_id: number;
  account_level: number;
  mastery_level: number;
  league_tier: number;
  league_points: number;
  champion_id: number | null;
  task_force: number | null;
  win_status: string | null;
  portal_id: number | null;
  portal_user_id: string | null;
  platform: string | null;
  source: string;
  party_member_ids: Array<string | number> | null;
  private_player_id: number | null;
  resolution_status: string;
  verified_name?: string | null;
  queue_id?: number | null;
  stats_scope?: string | null;
  map?: string | null;
  match_end_datetime?: string | Date | null;
  observation_quality?: string | null;
}

interface CandidateObservationRow extends ObservationRow {
  identity_id: number;
}

export interface PrivateBackfillReport {
  apply: boolean;
  sourceRows: number;
  observationRows: number;
  detailedUnresolved: number;
  minimalRows: number;
  currentIdentities: number;
  linkedMatchRows: number;
  legacyActive: number;
  outdatedActive: number;
  unlinkedMatchRows: number;
  mergedDuringRun: number;
  processedMatches: number;
}

function finiteInt(value: unknown): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

function cleanString(value: unknown): string {
  return String(value ?? '').trim();
}

function sourcePriority(source: unknown): number {
  switch (cleanString(source).toLowerCase()) {
    case 'direct': return 30;
    case 'recovered': return 20;
    default: return 10;
  }
}

function toIso(value: string | Date): string {
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? new Date(0).toISOString() : date.toISOString();
}

function uniquePositive(values: unknown): number[] {
  if (!Array.isArray(values)) return [];
  return [...new Set(values.map(finiteInt).filter(value => value > 0))].sort((a, b) => a - b);
}

function rowToObservation(row: ObservationRow): PrivateAccountObservation {
  return {
    matchId: finiteInt(row.match_id),
    privateSlot: finiteInt(row.private_slot),
    entryDatetime: toIso(row.entry_datetime),
    partyId: finiteInt(row.party_id),
    accountLevel: finiteInt(row.account_level),
    masteryLevel: finiteInt(row.mastery_level),
    leagueTier: finiteInt(row.league_tier),
    leaguePoints: finiteInt(row.league_points),
    championId: finiteInt(row.champion_id),
    taskForce: finiteInt(row.task_force),
    winStatus: cleanString(row.win_status).toLowerCase(),
    portalId: finiteInt(row.portal_id),
    portalUserId: cleanString(row.portal_user_id),
    platform: cleanString(row.platform).toLowerCase(),
    source: cleanString(row.source).toLowerCase() || 'direct',
    partyMemberIds: uniquePositive(row.party_member_ids),
    queueId: finiteInt(row.queue_id),
    statsScope: cleanString(row.stats_scope).toLowerCase(),
    map: cleanString(row.map),
    matchEndDatetime: row.match_end_datetime ? toIso(row.match_end_datetime) : undefined,
    observationQuality: cleanString(row.observation_quality).toLowerCase() || 'complete',
  };
}

export interface PrivateObservationContext {
  queueId?: number;
  statsScope?: string;
  map?: string;
  matchEndDatetime?: string;
  observationQuality?: string;
}

async function runQuery<T extends QueryResultRow = any>(client: PoolClient | null, text: string, params: any[] = []): Promise<T[]> {
  if (client) return (await client.query<T>(text, params)).rows;
  return query<T>(text, params);
}

async function runOne<T extends QueryResultRow = any>(client: PoolClient | null, text: string, params: any[] = []): Promise<T | null> {
  if (client) return (await client.query<T>(text, params)).rows[0] || null;
  return one<T>(text, params);
}

export async function recordPrivateAccountObservation(
  matchId: number,
  privateSlot: number,
  player: any,
  entryDatetime: string,
  context: PrivateObservationContext = {},
): Promise<number | null> {
  if (finiteInt(player?.player_id) !== 0 || cleanString(player?.player_name).toUpperCase() !== PRIVATE_ACCOUNT_NAME) {
    return null;
  }
  if (!Number.isInteger(matchId) || matchId <= 0 || !Number.isInteger(privateSlot) || privateSlot <= 0) {
    throw new Error(`Invalid private observation key ${matchId}:${privateSlot}`);
  }

  const observation = privateObservationFromPlayer(matchId, privateSlot, player, entryDatetime);
  await query(
    `INSERT INTO private_account_observations (
       match_id, private_slot, entry_datetime, party_id, account_level,
       mastery_level, league_tier, league_points, champion_id, task_force,
       win_status, portal_id, portal_user_id, platform, source, source_priority,
       resolution_status, queue_id, stats_scope, map, match_end_datetime,
       observation_quality, updated_at
     ) VALUES (
       $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,
       $18,$19,$20,$21,$22,now()
     )
     ON CONFLICT (match_id, private_slot) DO UPDATE SET
       entry_datetime = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.entry_datetime ELSE private_account_observations.entry_datetime END,
       party_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.party_id ELSE private_account_observations.party_id END,
       account_level = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.account_level ELSE private_account_observations.account_level END,
       mastery_level = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.mastery_level ELSE private_account_observations.mastery_level END,
       league_tier = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.league_tier ELSE private_account_observations.league_tier END,
       league_points = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.league_points ELSE private_account_observations.league_points END,
       champion_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.champion_id ELSE private_account_observations.champion_id END,
       task_force = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.task_force ELSE private_account_observations.task_force END,
       win_status = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.win_status ELSE private_account_observations.win_status END,
       portal_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.portal_id ELSE private_account_observations.portal_id END,
       portal_user_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.portal_user_id ELSE private_account_observations.portal_user_id END,
       platform = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.platform ELSE private_account_observations.platform END,
       source = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.source ELSE private_account_observations.source END,
       source_priority = GREATEST(EXCLUDED.source_priority, private_account_observations.source_priority),
       resolution_status = CASE
         WHEN private_account_observations.private_player_id IS NOT NULL THEN private_account_observations.resolution_status
         ELSE EXCLUDED.resolution_status
       END,
       queue_id = COALESCE(EXCLUDED.queue_id, private_account_observations.queue_id),
       stats_scope = COALESCE(NULLIF(EXCLUDED.stats_scope, ''), private_account_observations.stats_scope),
       map = COALESCE(NULLIF(EXCLUDED.map, ''), private_account_observations.map),
       match_end_datetime = COALESCE(EXCLUDED.match_end_datetime, private_account_observations.match_end_datetime),
       observation_quality = EXCLUDED.observation_quality,
       updated_at = now()`,
    [
      observation.matchId, observation.privateSlot, observation.entryDatetime,
      observation.partyId, observation.accountLevel, observation.masteryLevel,
      observation.leagueTier, observation.leaguePoints,
      observation.championId || null, observation.taskForce || null,
      observation.winStatus || null, observation.portalId || null, observation.portalUserId || null,
      observation.platform || null, observation.source, sourcePriority(observation.source),
      hasPrivateIdentityEvidence(observation) ? 'unresolved' : 'minimal',
      context.queueId || null, cleanString(context.statsScope) || null,
      cleanString(context.map) || null, context.matchEndDatetime || null,
      cleanString(context.observationQuality) || 'complete',
    ],
  );

  const row = await one<{ private_player_id: number | null }>(
    `SELECT private_player_id FROM private_account_observations
     WHERE match_id = $1 AND private_slot = $2`,
    [matchId, privateSlot],
  );
  return row?.private_player_id ? finiteInt(row.private_player_id) : null;
}

async function refreshIdentity(client: PoolClient, privatePlayerId: number): Promise<void> {
  await client.query(
    `WITH aggregate AS (
       SELECT
         min(entry_datetime) AS first_seen,
         max(entry_datetime) AS last_seen,
         count(*)::INT AS match_count,
         min(CASE WHEN resolution_status = 'new_identity' THEN 100 ELSE resolution_confidence END)::SMALLINT AS confidence
       FROM private_account_observations
       WHERE private_player_id = $1
     ), latest AS (
       SELECT party_id, account_level, mastery_level, league_tier, league_points, entry_datetime
       FROM private_account_observations
       WHERE private_player_id = $1
       ORDER BY entry_datetime DESC, match_id DESC, private_slot DESC
       LIMIT 1
     )
     UPDATE players_private pp SET
       party_id = latest.party_id,
       account_level = latest.account_level,
       mastery_level = latest.mastery_level,
       league_tier = latest.league_tier,
       league_points = latest.league_points,
       last_known_level = latest.account_level,
       last_known_mastery = latest.mastery_level,
       last_known_league_tier = latest.league_tier,
       last_known_league_points = latest.league_points,
       first_seen = aggregate.first_seen,
       last_seen = aggregate.last_seen,
       state_observed_at = latest.entry_datetime,
       match_count = aggregate.match_count,
       identity_confidence = CASE WHEN aggregate.match_count <= 1 THEN 0 ELSE COALESCE(aggregate.confidence, 0) END,
       identity_status = CASE WHEN pp.verified_name IS NOT NULL THEN 'verified' ELSE 'inferred' END,
       updated_at = now()
     FROM aggregate, latest
     WHERE pp.id = $1`,
    [privatePlayerId],
  );
}

async function createIdentity(
  client: PoolClient,
  observation: PrivateAccountObservation,
): Promise<number> {
  const row = await runOne<{ id: number }>(client,
    `INSERT INTO players_private (
       party_id, account_level, mastery_level, league_tier, league_points,
       last_known_level, last_known_mastery, last_known_league_tier,
       last_known_league_points, first_seen, last_seen, state_observed_at,
       match_count, tracking_version, identity_status, identity_confidence,
       is_active
     ) VALUES ($1,$2,$3,$4,$5,$2,$3,$4,$5,$6,$6,$6,0,$7,'inferred',0,TRUE)
     RETURNING id`,
    [
      observation.partyId, observation.accountLevel, observation.masteryLevel,
      observation.leagueTier, observation.leaguePoints,
      observation.entryDatetime, PRIVATE_IDENTITY_VERSION,
    ],
  );
  if (!row?.id) throw new Error('Failed to create private identity');
  const id = finiteInt(row.id);
  await client.query(
    `UPDATE players_private SET alias = COALESCE(NULLIF(btrim(alias), ''), 'P-' || lpad(id::text, 6, '0')) WHERE id = $1`,
    [id],
  );
  return id;
}

async function linkObservation(
  client: PoolClient,
  observation: PrivateAccountObservation,
  privatePlayerId: number,
  status: 'new_identity' | 'linked',
  confidence: number,
  reasons: string[],
): Promise<void> {
  await client.query(
    `UPDATE private_account_observations SET
       private_player_id = $3,
       resolution_status = $4,
       resolution_confidence = $5,
       resolution_reasons = $6::jsonb,
       resolved_at = now(),
       updated_at = now()
     WHERE match_id = $1 AND private_slot = $2`,
    [observation.matchId, observation.privateSlot, privatePlayerId, status, confidence, JSON.stringify(reasons)],
  );
  await client.query(
    `UPDATE match_players
     SET private_player_id = $3
     WHERE match_id = $1 AND player_id = 0 AND private_slot = $2`,
    [observation.matchId, observation.privateSlot, privatePlayerId],
  );
  await client.query(
     `UPDATE casual_match_players
     SET private_player_id = $3
     WHERE match_id = $1 AND participant_kind = 'private' AND private_slot = $2`,
    [observation.matchId, observation.privateSlot, privatePlayerId],
  );
  await client.query(
     `UPDATE special_match_players
     SET private_player_id = $3
     WHERE match_id = $1 AND participant_kind = 'private' AND private_slot = $2`,
    [observation.matchId, observation.privateSlot, privatePlayerId],
  );
  await client.query(
    `INSERT INTO players_private_history (
       player_private_id, party_id, account_level, mastery_level,
       league_tier, league_points, match_id, private_slot, recorded_at,
       resolution_confidence, resolution_reasons
     ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
     ON CONFLICT (match_id, private_slot) WHERE match_id IS NOT NULL DO UPDATE SET
       player_private_id = EXCLUDED.player_private_id,
       party_id = EXCLUDED.party_id,
       account_level = EXCLUDED.account_level,
       mastery_level = EXCLUDED.mastery_level,
       league_tier = EXCLUDED.league_tier,
       league_points = EXCLUDED.league_points,
       recorded_at = EXCLUDED.recorded_at,
       resolution_confidence = EXCLUDED.resolution_confidence,
       resolution_reasons = EXCLUDED.resolution_reasons`,
    [
      privatePlayerId, observation.partyId, observation.accountLevel,
      observation.masteryLevel, observation.leagueTier, observation.leaguePoints,
      observation.matchId, observation.privateSlot, observation.entryDatetime,
      confidence, JSON.stringify(reasons),
    ],
  );
  await refreshIdentity(client, privatePlayerId);
  if (observation.queueId && observation.statsScope) {
    await client.query(
      `INSERT INTO private_player_presence_24h (
         private_player_id, first_observed_at, last_observed_at, last_match_id,
         last_queue_id, last_stats_scope, identity_confidence
       ) VALUES ($1,$2,$2,$3,$4,$5,$6)
       ON CONFLICT (private_player_id) DO UPDATE SET
         first_observed_at = LEAST(private_player_presence_24h.first_observed_at, EXCLUDED.first_observed_at),
         last_observed_at = GREATEST(private_player_presence_24h.last_observed_at, EXCLUDED.last_observed_at),
         last_match_id = CASE WHEN EXCLUDED.last_observed_at >= private_player_presence_24h.last_observed_at THEN EXCLUDED.last_match_id ELSE private_player_presence_24h.last_match_id END,
         last_queue_id = CASE WHEN EXCLUDED.last_observed_at >= private_player_presence_24h.last_observed_at THEN EXCLUDED.last_queue_id ELSE private_player_presence_24h.last_queue_id END,
         last_stats_scope = CASE WHEN EXCLUDED.last_observed_at >= private_player_presence_24h.last_observed_at THEN EXCLUDED.last_stats_scope ELSE private_player_presence_24h.last_stats_scope END,
         identity_confidence = GREATEST(private_player_presence_24h.identity_confidence, EXCLUDED.identity_confidence),
         updated_at = now()`,
      [
        privatePlayerId, observation.entryDatetime, observation.matchId,
        observation.queueId, observation.statsScope, confidence,
      ],
    );
    await client.query(
      `DELETE FROM unresolved_private_presence WHERE match_id = $1 AND private_slot = $2`,
      [observation.matchId, observation.privateSlot],
    );
  }
}

function groupCandidateObservations(rows: CandidateObservationRow[]): Map<number, PrivateAccountObservation[]> {
  const groups = new Map<number, PrivateAccountObservation[]>();
  for (const row of rows) {
    const list = groups.get(finiteInt(row.identity_id)) || [];
    list.push(rowToObservation(row));
    groups.set(finiteInt(row.identity_id), list);
  }
  for (const list of groups.values()) {
    list.sort((left, right) => new Date(left.entryDatetime).getTime() - new Date(right.entryDatetime).getTime());
  }
  return groups;
}

function scoreCandidate(
  incoming: PrivateAccountObservation,
  observations: PrivateAccountObservation[],
): IdentityScore {
  const incomingTime = new Date(incoming.entryDatetime).getTime();
  let before: PrivateAccountObservation | undefined;
  let after: PrivateAccountObservation | undefined;
  for (const observation of observations) {
    const time = new Date(observation.entryDatetime).getTime();
    if (time <= incomingTime) before = observation;
    else if (!after) after = observation;
  }

  for (const neighbor of [before, after]) {
    if (!neighbor) continue;
    const compatibility = scorePrivateIdentity(incoming, neighbor);
    if (compatibility.hardConflict) return compatibility;
  }

  let best: IdentityScore = { score: 0, reasons: [], hardConflict: false };
  for (const observation of observations) {
    const result = scorePrivateIdentity(incoming, observation);
    if (!result.hardConflict && result.score > best.score) best = result;
  }
  return best;
}

async function candidateRows(
  client: PoolClient,
  observation: PrivateAccountObservation,
): Promise<CandidateObservationRow[]> {
  return runQuery<CandidateObservationRow>(client,
    `SELECT o.*, o.private_player_id AS identity_id
     FROM private_account_observations o
     JOIN players_private pp ON pp.id = o.private_player_id
     WHERE o.private_player_id IS NOT NULL
       AND pp.tracking_version = $2
       AND pp.is_active
       AND o.match_id <> $1
       AND NOT EXISTS (
         SELECT 1 FROM private_account_observations used
         WHERE used.match_id = $1
           AND used.private_player_id = o.private_player_id
       )
       AND (
         ($3 <> '' AND o.portal_user_id = $3)
         OR (cardinality($4::bigint[]) > 0 AND o.party_member_ids && $4::bigint[])
         OR ($5 <> 0 AND o.party_id = $5 AND abs(extract(epoch FROM (o.entry_datetime - $6::timestamptz))) <= 43200)
         OR (
           $7 > 0 AND o.account_level BETWEEN GREATEST(1, $7 - 2) AND $7 + 2
           AND $8 > 0 AND o.champion_id = $8
           AND $9 > 0 AND o.mastery_level BETWEEN GREATEST(1, $9 - 2) AND $9 + 2
         )
       )
     ORDER BY o.private_player_id, o.entry_datetime`,
    [
      observation.matchId, PRIVATE_IDENTITY_VERSION, observation.portalUserId,
      observation.partyMemberIds, observation.partyId, observation.entryDatetime,
      observation.accountLevel, observation.championId, observation.masteryLevel,
    ],
  );
}

async function resolveObservation(client: PoolClient, row: ObservationRow): Promise<number | null> {
  const observation = rowToObservation(row);
  if (!hasPrivateIdentityEvidence(observation)) {
    await client.query(
      `UPDATE private_account_observations SET
         resolution_status = 'minimal', resolution_confidence = 0,
         resolution_reasons = '["no_identity_evidence"]'::jsonb,
         updated_at = now()
       WHERE match_id = $1 AND private_slot = $2`,
      [observation.matchId, observation.privateSlot],
    );
    return null;
  }

  if (row.private_player_id) {
    const id = finiteInt(row.private_player_id);
    await client.query(
      `UPDATE match_players SET private_player_id = $3
       WHERE match_id = $1 AND player_id = 0 AND private_slot = $2`,
      [observation.matchId, observation.privateSlot, id],
    );
    await refreshIdentity(client, id);
    return id;
  }

  const grouped = groupCandidateObservations(await candidateRows(client, observation));
  const ranked = [...grouped.entries()]
    .map(([id, observations]) => ({ id, result: scoreCandidate(observation, observations) }))
    .filter(candidate => !candidate.result.hardConflict)
    .sort((left, right) => right.result.score - left.result.score || left.id - right.id);

  const best = ranked[0];
  const runnerUp = ranked[1];
  const hasThreshold = best && best.result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD;
  const hasMargin = best && (!runnerUp || best.result.score - runnerUp.result.score >= PRIVATE_IDENTITY_MARGIN);

  if (hasThreshold && hasMargin && best) {
    await linkObservation(client, observation, best.id, 'linked', best.result.score, best.result.reasons);
    return best.id;
  }

  // Ranked observations retain the legacy behavior because tier/TP provides a
  // strong evolving anchor. Non-ranked observations do not. If a casual or
  // special observation has a plausible-but-insufficient existing candidate,
  // leave it unresolved instead of minting a second identity and inflating the
  // rolling private population. A first observation with no candidate still
  // seeds an identity so future strong evidence has something to link to.
  if (observation.statsScope && observation.statsScope !== 'ranked' && best) {
    await client.query(
      `UPDATE private_account_observations SET
         resolution_status='ambiguous',
         resolution_confidence=$3,
         resolution_reasons=$4::jsonb,
         resolved_at=NULL,
         updated_at=now()
       WHERE match_id=$1 AND private_slot=$2`,
      [
        observation.matchId, observation.privateSlot, best.result.score,
        JSON.stringify([
          best.result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD ? 'ambiguous_candidate_margin' : 'candidate_below_threshold',
          `best_candidate:${best.id}:${best.result.score}`,
        ]),
      ],
    );
    return null;
  }

  const id = await createIdentity(client, observation);
  const reasons = best
    ? [
        best.result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD ? 'ambiguous_candidate_margin' : 'candidate_below_threshold',
        `best_candidate:${best.id}:${best.result.score}`,
      ]
    : ['no_candidate'];
  await linkObservation(client, observation, id, 'new_identity', 100, reasons);
  return id;
}

export async function resolvePrivateAccountsForMatch(matchId: number): Promise<number[]> {
  if (!Number.isInteger(matchId) || matchId <= 0) throw new Error(`Invalid match id ${matchId}`);
  return transaction(async client => {
    await client.query('SELECT pg_advisory_xact_lock($1)', [PRIVATE_RESOLVER_LOCK]);

    await client.query(
      `UPDATE private_account_observations o SET
         party_member_ids = COALESCE((
           SELECT array_agg(DISTINCT companions.player_id ORDER BY companions.player_id)
           FROM (
             SELECT mp.player_id, mp.party_id FROM match_players mp WHERE mp.match_id = o.match_id
             UNION ALL
             SELECT cmp.player_id, cmp.party_id FROM casual_match_players cmp WHERE cmp.match_id = o.match_id
             UNION ALL
             SELECT smp.player_id, smp.party_id FROM special_match_players smp WHERE smp.match_id = o.match_id
           ) companions
           WHERE companions.player_id > 0
             AND o.party_id > 0
             AND companions.party_id = o.party_id
         ), '{}'::bigint[]),
         updated_at = now()
       WHERE o.match_id = $1`,
      [matchId],
    );

    const rows = await runQuery<ObservationRow>(client,
      `SELECT * FROM private_account_observations
       WHERE match_id = $1
       ORDER BY private_slot
       FOR UPDATE`,
      [matchId],
    );
    const ids: number[] = [];
    for (const row of rows) {
      const id = await resolveObservation(client, row);
      if (id) {
        ids.push(id);
      } else if (row.queue_id && row.stats_scope) {
        await client.query(
          `INSERT INTO unresolved_private_presence (
             match_id, private_slot, observed_at, queue_id, stats_scope, reason
           ) VALUES ($1,$2,$3,$4,$5,$6)
           ON CONFLICT (match_id, private_slot) DO UPDATE SET
             observed_at=EXCLUDED.observed_at, queue_id=EXCLUDED.queue_id,
             stats_scope=EXCLUDED.stats_scope, reason=EXCLUDED.reason, updated_at=now()`,
          [
            finiteInt(row.match_id), finiteInt(row.private_slot), toIso(row.entry_datetime),
            finiteInt(row.queue_id), cleanString(row.stats_scope),
            hasPrivateIdentityEvidence(rowToObservation(row))
              ? 'ambiguous_identity'
              : 'no_identity_evidence',
          ],
        );
      }
    }
    return ids;
  });
}

export async function seedHistoricalPrivateObservations(): Promise<void> {
  await query(
    `INSERT INTO private_account_observations (
       match_id, private_slot, entry_datetime, party_id, account_level,
       mastery_level, league_tier, league_points, champion_id, task_force,
       win_status, portal_id, portal_user_id, platform, source, source_priority
     )
     SELECT
       mp.match_id,
       CASE WHEN mp.private_slot > 0 THEN mp.private_slot ELSE 1 END,
       mp.entry_datetime,
       COALESCE(mp.party_id, 0), COALESCE(mp.account_level, 0),
       COALESCE(mp.mastery_level, 0), COALESCE(mp.league_tier, 0),
       COALESCE(mp.league_points, 0), mp.champion_id, mp.task_force,
       mp.win_status, mp.portal_id, NULLIF(mp.portal_user_id, ''), NULLIF(mp.platform, ''),
       COALESCE(mp.source, 'direct'),
       CASE COALESCE(mp.source, 'direct') WHEN 'direct' THEN 30 WHEN 'recovered' THEN 20 ELSE 10 END
     FROM match_players mp
     WHERE mp.player_id = 0
       AND upper(COALESCE(mp.player_name, '')) = $1
     ON CONFLICT (match_id, private_slot) DO UPDATE SET
       entry_datetime = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.entry_datetime ELSE private_account_observations.entry_datetime END,
       party_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.party_id ELSE private_account_observations.party_id END,
       account_level = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.account_level ELSE private_account_observations.account_level END,
       mastery_level = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.mastery_level ELSE private_account_observations.mastery_level END,
       league_tier = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.league_tier ELSE private_account_observations.league_tier END,
       league_points = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.league_points ELSE private_account_observations.league_points END,
       champion_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.champion_id ELSE private_account_observations.champion_id END,
       task_force = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.task_force ELSE private_account_observations.task_force END,
       win_status = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.win_status ELSE private_account_observations.win_status END,
       portal_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.portal_id ELSE private_account_observations.portal_id END,
       portal_user_id = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.portal_user_id ELSE private_account_observations.portal_user_id END,
       platform = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.platform ELSE private_account_observations.platform END,
       source = CASE WHEN EXCLUDED.source_priority >= private_account_observations.source_priority THEN EXCLUDED.source ELSE private_account_observations.source END,
       source_priority = GREATEST(EXCLUDED.source_priority, private_account_observations.source_priority),
       updated_at = now()`,
    [PRIVATE_ACCOUNT_NAME],
  );
}

async function repairPrivateMatchLinks(): Promise<void> {
  // Historical v1 rows used slot 0. Observation v2 assigned the only legacy
  // private participant slot 1, but originally failed to copy that key back to
  // match_players. Keep this repair idempotent for restored/older databases.
  await query(
    `UPDATE match_players mp
     SET private_slot = o.private_slot,
         private_player_id = o.private_player_id
     FROM private_account_observations o
     WHERE mp.match_id = o.match_id
       AND mp.player_id = 0
       AND upper(COALESCE(mp.player_name, '')) = $1
       AND mp.private_slot = 0
       AND o.private_slot = 1
       AND NOT EXISTS (
         SELECT 1 FROM match_players existing
         WHERE existing.match_id = mp.match_id
           AND existing.player_id = 0
           AND existing.private_slot = o.private_slot
           AND existing.entry_datetime = mp.entry_datetime
       )`,
    [PRIVATE_ACCOUNT_NAME],
  );
  await query(
    `UPDATE match_players mp
     SET private_player_id = o.private_player_id
     FROM private_account_observations o
     WHERE mp.match_id = o.match_id
       AND mp.player_id = 0
       AND mp.private_slot = o.private_slot
       AND o.private_player_id IS NOT NULL
       AND mp.private_player_id IS DISTINCT FROM o.private_player_id`,
  );
}

async function reconcileSplitPrivateIdentities(): Promise<number> {
  return transaction(async client => {
    await client.query('SELECT pg_advisory_xact_lock($1)', [PRIVATE_RESOLVER_LOCK]);
    const sources = (await client.query<{ id: number }>(
      `SELECT id
       FROM players_private
       WHERE tracking_version = $1
         AND is_active
         AND verified_name IS NULL
       ORDER BY first_seen, id`,
      [PRIVATE_IDENTITY_VERSION],
    )).rows;
    let merged = 0;

    for (const sourceRow of sources) {
      const sourceId = finiteInt(sourceRow.id);
      const sourceIdentity = (await client.query<{ id: number }>(
        `SELECT id FROM players_private
         WHERE id = $1 AND tracking_version = $2 AND is_active AND verified_name IS NULL
         FOR UPDATE`,
        [sourceId, PRIVATE_IDENTITY_VERSION],
      )).rows[0];
      if (!sourceIdentity) continue;

      const sourceObservations = (await client.query<ObservationRow>(
        `SELECT * FROM private_account_observations
         WHERE private_player_id = $1
         ORDER BY entry_datetime, match_id, private_slot`,
        [sourceId],
      )).rows;
      if (sourceObservations.length === 0) continue;
      const incoming = rowToObservation(sourceObservations[0]!);
      const incomingTime = new Date(incoming.entryDatetime).getTime();
      const grouped = groupCandidateObservations(await candidateRows(client, incoming));
      const ranked = [...grouped.entries()]
        .filter(([id, observations]) => id !== sourceId && observations.some(observation => new Date(observation.entryDatetime).getTime() <= incomingTime))
        .map(([id, observations]) => ({ id, result: scoreCandidate(incoming, observations) }))
        .filter(candidate => !candidate.result.hardConflict)
        .sort((left, right) => right.result.score - left.result.score || left.id - right.id);
      const best = ranked[0];
      const runnerUp = ranked[1];
      if (!best || best.result.score < PRIVATE_IDENTITY_LINK_THRESHOLD) continue;
      if (runnerUp && best.result.score - runnerUp.result.score < PRIVATE_IDENTITY_MARGIN) continue;

      // The earliest observation proposes a historical merge, but every fact
      // already grouped under that source must remain chronologically sane
      // against the target timeline. One later level/mastery regression is
      // enough to keep both identities separate for manual review.
      const targetObservations = grouped.get(best.id) || [];
      const hasTimelineConflict = sourceObservations.some(sourceObservation =>
        scoreCandidate(rowToObservation(sourceObservation), targetObservations).hardConflict,
      );
      if (hasTimelineConflict) continue;

      const collision = (await client.query<{ exists: boolean }>(
        `SELECT EXISTS (
           SELECT 1
           FROM private_account_observations source
           JOIN private_account_observations target ON target.match_id = source.match_id
           WHERE source.private_player_id = $1 AND target.private_player_id = $2
         ) AS exists`,
        [sourceId, best.id],
      )).rows[0]?.exists;
      if (collision) continue;

      const mergeReasons = [...best.result.reasons, `merged_identity:${sourceId}`];
      await client.query(
        `UPDATE match_players mp SET private_player_id = $2
         WHERE mp.player_id = 0
           AND (
             mp.private_player_id = $1
             OR EXISTS (
               SELECT 1 FROM private_account_observations o
               WHERE o.private_player_id = $1
                 AND o.match_id = mp.match_id
                 AND o.private_slot = mp.private_slot
             )
           )`,
        [sourceId, best.id],
      );
      await client.query(
        `UPDATE players_private_history
         SET player_private_id = $2,
             resolution_confidence = GREATEST(COALESCE(resolution_confidence, 0), $3),
             resolution_reasons = $4::jsonb
         WHERE player_private_id = $1`,
        [sourceId, best.id, best.result.score, JSON.stringify(mergeReasons)],
      );
      await client.query(
        `UPDATE private_account_observations SET
           private_player_id = $2,
           resolution_status = 'linked',
           resolution_confidence = GREATEST(resolution_confidence, $3),
           resolution_reasons = $4::jsonb,
           resolved_at = now(),
           updated_at = now()
         WHERE private_player_id = $1`,
        [sourceId, best.id, best.result.score, JSON.stringify(mergeReasons)],
      );
      // Community moderation follows the canonical private identity. Preserve
      // each user's unique vote while collapsing any vote already submitted
      // against both sides of the identity merge.
      await client.query(
        `INSERT INTO private_account_community_votes (
           private_player_id, user_id, vote_type, reason, created_at
         )
         SELECT $2, user_id, vote_type, reason, created_at
         FROM private_account_community_votes
         WHERE private_player_id = $1
         ON CONFLICT (private_player_id, user_id, vote_type) DO NOTHING`,
        [sourceId, best.id],
      );
      await client.query(
        'DELETE FROM private_account_community_votes WHERE private_player_id = $1',
        [sourceId],
      );
      await client.query(
        `UPDATE players_private target
         SET cheater = target.cheater OR source.cheater,
             sus_count = (
               SELECT COUNT(*)::INT
               FROM private_account_community_votes vote
               WHERE vote.private_player_id = target.id
                 AND vote.vote_type = 'suspicious'
             ),
             cheater_reason = CASE
               WHEN source.cheater AND NOT target.cheater THEN source.cheater_reason
               ELSE target.cheater_reason
             END,
             cheater_marked_at = CASE
               WHEN source.cheater AND NOT target.cheater THEN source.cheater_marked_at
               ELSE target.cheater_marked_at
             END,
             updated_at = now()
         FROM players_private source
         WHERE target.id = $2 AND source.id = $1`,
        [sourceId, best.id],
      );
      await client.query(
        `UPDATE players_private SET
           is_active = FALSE,
           merged_into_id = $2,
           identity_status = 'merged',
           updated_at = now()
         WHERE id = $1`,
        [sourceId, best.id],
      );
      await refreshIdentity(client, best.id);
      merged += 1;
    }
    return merged;
  });
}

export async function getPrivateBackfillReport(apply = false, processedMatches = 0, mergedDuringRun = 0): Promise<PrivateBackfillReport> {
  const row = await one<any>(
    `SELECT
       (SELECT count(*)::INT FROM match_players WHERE player_id = 0 AND upper(COALESCE(player_name, '')) = $1) AS source_rows,
       (SELECT count(*)::INT FROM private_account_observations) AS observation_rows,
       (SELECT count(*)::INT FROM private_account_observations WHERE private_player_id IS NULL AND resolution_status IN ('unresolved', 'ambiguous')) AS detailed_unresolved,
       (SELECT count(*)::INT FROM private_account_observations WHERE resolution_status = 'minimal') AS minimal_rows,
       (SELECT count(*)::INT FROM players_private WHERE tracking_version = $2 AND is_active) AS current_identities,
       (SELECT count(*)::INT FROM match_players mp JOIN players_private pp ON pp.id = mp.private_player_id WHERE mp.player_id = 0 AND pp.tracking_version = $2) AS linked_match_rows,
       (SELECT count(*)::INT FROM players_private WHERE tracking_version = 1 AND is_active AND verified_name IS NULL) AS legacy_active,
       (SELECT count(*)::INT FROM players_private WHERE tracking_version > 1 AND tracking_version < $2 AND is_active) AS outdated_active,
       (SELECT count(*)::INT
        FROM private_account_observations o
        LEFT JOIN match_players mp
          ON mp.match_id = o.match_id AND mp.player_id = 0 AND mp.private_slot = o.private_slot
        WHERE o.private_player_id IS NOT NULL
          AND (mp.private_player_id IS NULL OR mp.private_player_id IS DISTINCT FROM o.private_player_id)) AS unlinked_match_rows`,
    [PRIVATE_ACCOUNT_NAME, PRIVATE_IDENTITY_VERSION],
  );
  return {
    apply,
    sourceRows: finiteInt(row?.source_rows),
    observationRows: finiteInt(row?.observation_rows),
    detailedUnresolved: finiteInt(row?.detailed_unresolved),
    minimalRows: finiteInt(row?.minimal_rows),
    currentIdentities: finiteInt(row?.current_identities),
    linkedMatchRows: finiteInt(row?.linked_match_rows),
    legacyActive: finiteInt(row?.legacy_active),
    outdatedActive: finiteInt(row?.outdated_active),
    unlinkedMatchRows: finiteInt(row?.unlinked_match_rows),
    mergedDuringRun,
    processedMatches,
  };
}

/**
 * Idempotent historical reconciliation.  Existing v2 links are retained and
 * only unlinked match observations are processed. Legacy PartyId identities
 * remain in the database for audit, then become inactive only after every
 * detailed observation has a current evidence-backed identity.
 */
export async function backfillPrivateAccountIdentities(apply = false): Promise<PrivateBackfillReport> {
  if (!apply) return getPrivateBackfillReport(false);
  await query(
    `UPDATE players_private SET tracking_version = $1, updated_at = now()
     WHERE tracking_version = 2 AND is_active`,
    [PRIVATE_IDENTITY_VERSION],
  );
  await seedHistoricalPrivateObservations();
  await repairPrivateMatchLinks();

  const matchRows = await query<{ match_id: string | number }>(
    `SELECT match_id
     FROM private_account_observations
     WHERE private_player_id IS NULL AND resolution_status IN ('unresolved', 'ambiguous')
     GROUP BY match_id
     ORDER BY min(entry_datetime), match_id`,
  );
  for (const row of matchRows) await resolvePrivateAccountsForMatch(finiteInt(row.match_id));

  const mergedDuringRun = await reconcileSplitPrivateIdentities();
  await repairPrivateMatchLinks();

  const unresolved = await one<{ count: number }>(
    `SELECT count(*)::INT AS count
     FROM private_account_observations
     WHERE private_player_id IS NULL AND resolution_status IN ('unresolved', 'ambiguous')`,
  );
  if (finiteInt(unresolved?.count) === 0) {
    await query(
      `UPDATE players_private SET
         is_active = FALSE,
         identity_status = 'legacy',
         updated_at = now()
       WHERE tracking_version = 1
         AND is_active
         AND verified_name IS NULL`,
    );
  }

  return getPrivateBackfillReport(true, matchRows.length, mergedDuringRun);
}

export async function closePrivateResolverPool(): Promise<void> {
  await pool.end();
}
