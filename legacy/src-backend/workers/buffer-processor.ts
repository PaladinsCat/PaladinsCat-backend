import { pool, query, one } from '../config/db';
import { PlayerDetails, getMatchDetailsBatch, cleanupFetchedPlayersCache } from '../services/hirez';
import { matchPayloadRequiresRecovery } from '../services/batch-int16';
import { getApiHeadroomSnapshot } from './api-headroom';
import { normalizeMatchPlayer, normalizeMatchHistoryPlayer, normalizeChampion, normalizeItem, normalizeEsportsLeague, normalizeEsportsTeam, extractMatchMetadata, normalizePlayerProfile, normalizeLeaderboardEntry, normalizeLoadout, normalizePlayerStatus, normalizePlayerChampion, normalizePlayerAchievements, normalizeSkin, normalizeBountyItem, normalizeLeagueLeaderboardEntry, normalizeLiveMatchPlayer, roundTo2, NormalizedChampion, NormalizedItem, normalizeRegion, hasPlayerChampionCombatStats } from '../services/normalizer';
import { calculateAndApplyRatingChanges } from '../services/rating-calculator';
import { syncPlayer, syncMatch, bulkSyncPlayers } from '../services/meilisearch';
import { markHourlyIngestComplete } from './hourly-ingest-state';
import {
  markHourlyIngestMatchDebtComplete,
  markHourlyIngestMatchDebtRetryable,
} from './hourly-ingest-match-debt';
import { runExclusive } from './worker-lock';
import { updatePlayerAveragesForMatches } from '../services/player-performance-rollups';
import { upsertPlayerProfile as persistPlayerProfile } from '../services/player-profile-store';
import { isRankedStatsQueue } from './ranked-stats-policy';
import { calculateKda } from '../utils/kda';
import { calculateAfkRate, calculateCreditRates, calculatePerMinute, resolveGameplayDuration } from '../utils/credit-rates';
import { isStagedRecoveryRoster, matchBanEntries } from '../utils/match-bans';
import {
  recordPrivateAccountObservation,
  resolvePrivateAccountsForMatch,
} from '../services/private-account-resolver';
import { recordRankedPartyGroups } from '../services/party-tracking';
import {
  isValidCompletedMatchScore,
  reconcileSiegeMatchScore,
  resolvePlayerOutcomeConsensus,
} from '../services/ranked-score';
import {
  refreshPerformanceMetricStats,
  upsertPerformanceProjectionsForMatch,
  upsertPerformanceProjectionsForMatches,
} from '../services/performance-projections';
import {
  upsertScalableStatsProjectionsForMatch,
  upsertScalableStatsProjectionsForMatches,
} from '../services/scalable-stats-projections';
import { upsertCasualItemProjectionForMatch } from '../services/casual-mechanics-projections';
import { limitedMatchReason } from './limited-match-policy';


const PRIVATE_ACCOUNT_NAME = 'PRIVATEACCOUNT';

function isPrivateAccountParticipant(player: any): boolean {
  return Number(player?.player_id || 0) === 0
    && String(player?.player_name || '').toUpperCase() === PRIVATE_ACCOUNT_NAME;
}

function isDetailedMatchPlayer(player: any): boolean {
  const source = String(player?.source || 'direct').toLowerCase();
  return Number(player?.champion_id || 0) > 0
    && ['direct', 'recovered'].includes(source)
    && [1, 2].includes(Number(player?.task_force || 0))
    && ['winner', 'win', 'loser', 'loss'].includes(String(player?.win_status || '').toLowerCase());
}

function isAuthoritativeMetricPlayer(player: any): boolean {
  return Number(player?.player_id || 0) > 0 && isDetailedMatchPlayer(player);
}

function isPrivateAccountPlaceholder(player: any): boolean {
  return isPrivateAccountParticipant(player)
    && String(player?.source || '').toLowerCase() === 'minimal'
    && Number(player?.champion_id || 0) <= 0;
}

function privateParticipantFingerprint(player: any): string {
  return [
    Number(player?.task_force || 0), Number(player?.champion_id || 0),
    Number(player?.skin_id || 0), Number(player?.kills || 0),
    Number(player?.deaths || 0), Number(player?.assists || 0),
    String(player?.source || ''),
  ].join(':');
}

function assignPrivateParticipantSlots(players: any[]): Map<any, number> {
  const privatePlayers = players.filter(isPrivateAccountParticipant);
  privatePlayers.sort((left: any, right: any) => {
    const leftKey = [
      Number(left?.task_force || 0), Number(left?.party_id || 0),
      Number(left?.champion_id || 0), Number(left?.account_level || 0),
      Number(left?.mastery_level || 0), Number(left?.league_tier || 0),
      Number(left?.league_points || 0), String(left?.portal_user_id || ''),
      privateParticipantFingerprint(left),
    ].join(':');
    const rightKey = [
      Number(right?.task_force || 0), Number(right?.party_id || 0),
      Number(right?.champion_id || 0), Number(right?.account_level || 0),
      Number(right?.mastery_level || 0), Number(right?.league_tier || 0),
      Number(right?.league_points || 0), String(right?.portal_user_id || ''),
      privateParticipantFingerprint(right),
    ].join(':');
    return leftKey.localeCompare(rightKey);
  });
  return new Map(privatePlayers.map((player, index) => [player, index + 1]));
}

/**
 * A private account with player_id=0 has no stable player/champion identity.
 * Keep a logical roster row for every private participant so all detailed
 * players remain usable, but
 * erase every player/champion metric that could otherwise be attributed to the
 * shared zero identity. Team/outcome are match facts and are retained so the
 * roster can still be validated as 5v5 and its real players can receive W/L.
 */
function toPrivateAccountPlaceholder(
  player: any,
  meta: any,
  matchId: number,
  teamOneCount: number,
  teamTwoCount: number,
): any {
  // Recovery-private profiles do not carry trustworthy TaskForce. Fill the
  // open logical roster slot from the detailed rows already known. This works
  // for more than one placeholder and never assigns more than five to a team.
  let taskForce = 0;
  if (teamOneCount < 5 && (teamOneCount <= teamTwoCount || teamTwoCount >= 5)) taskForce = 1;
  else if (teamTwoCount < 5) taskForce = 2;

  const winner = Number(meta?.winning_task_force || 0);
  const winStatus = taskForce > 0 && (winner === 1 || winner === 2)
    ? (taskForce === winner ? 'Winner' : 'Loser')
    : '';
  const placeholder: any = {
    ...player,
    player_id: 0,
    player_name: PRIVATE_ACCOUNT_NAME,
    match_id: matchId,
    entry_datetime: meta?.entry_datetime || player?.entry_datetime || '',
    queue_id: Number(meta?.queue_id || player?.queue_id || 0),
    champion_id: 0,
    champion_name: '',
    skin_id: 0,
    skin_name: '',
    win_status: winStatus,
    task_force: taskForce,
    source: 'minimal',
    has_ret_msg: false,
    portal_id: 0,
    portal_user_id: '',
    platform: '',
    merged_players: null,
  };

  const zeroFields = [
    'kills', 'deaths', 'assists', 'damage_done_in_hand', 'damage_done_physical',
    'damage_done_magical', 'damage_taken', 'damage_taken_physical',
    'damage_taken_magical', 'damage_mitigated', 'healing', 'healing_self',
    'healing_bot', 'healing_player_self', 'gold_earned', 'gold_per_minute',
    'objective_assists', 'camps_cleared', 'structure_damage', 'wards_placed',
    'towers_destroyed', 'distance_traveled', 'multi_kill_max', 'killing_spree',
    'kills_first_blood', 'kills_double', 'kills_triple', 'kills_quadra',
    'kills_penta', 'kills_fire_giant', 'kills_gold_fury', 'kills_phoenix',
    'kills_siege_jugg', 'kills_wild_jugg', 'kills_player', 'kills_bot',
    'kills_single', 'league_tier', 'league_points', 'league_wins',
    'league_losses', 'account_level', 'mastery_level', 'party_id', 'team_id',
    'final_match_level', 'rank_stat_league', 'surrendered', 'time_in_match',
    'match_duration',
  ];
  for (let index = 1; index <= 6; index++) {
    zeroFields.push(`item_id_${index}`, `item_level_${index}`);
    placeholder[`item_purch_${index}`] = '';
  }
  for (let index = 1; index <= 4; index++) {
    zeroFields.push(`active_id_${index}`, `active_level_${index}`);
    placeholder[`item_active_${index}`] = '';
  }
  for (let index = 1; index <= 8; index++) zeroFields.push(`ban_id_${index}`);
  placeholder.team_name = '';
  for (const field of zeroFields) placeholder[field] = 0;
  return placeholder;
}

function validateLogicalMatchRoster(players: any[], matchId: number, winningTaskForce: number): string | null {
  if (players.length !== 10) return `expected 10 logical roster rows, got ${players.length}`;
  const placeholders = players.filter(isPrivateAccountPlaceholder);
  const detailedPlayers = players.filter(isDetailedMatchPlayer);
  if (detailedPlayers.length + placeholders.length !== 10) {
    return `expected every roster row to be detailed or a private placeholder, got detailed=${detailedPlayers.length}, placeholders=${placeholders.length}`;
  }
  const metricPlayers = players.filter(isAuthoritativeMetricPlayer);
  if (new Set(metricPlayers.map((player: any) => Number(player.player_id))).size !== metricPlayers.length) {
    return 'authoritative metric player IDs are not unique';
  }

  const teamOne = players.filter((player: any) => Number(player?.task_force || 0) === 1).length;
  const teamTwo = players.filter((player: any) => Number(player?.task_force || 0) === 2).length;
  if (teamOne !== 5 || teamTwo !== 5) return `expected a logical 5v5 roster, got ${teamOne}v${teamTwo}`;

  const invalidOutcome = players.find((player: any) => !['winner', 'win', 'loser', 'loss'].includes(String(player?.win_status || '').toLowerCase()));
  if (invalidOutcome) return `roster row ${invalidOutcome.player_id} has no usable outcome`;
  const inconsistentOutcome = players.find((player: any) => {
    const normalizedOutcome = String(player?.win_status || '').toLowerCase();
    const isWinner = normalizedOutcome === 'winner' || normalizedOutcome === 'win';
    return isWinner !== (Number(player?.task_force || 0) === winningTaskForce);
  });
  if (inconsistentOutcome) {
    return `roster row ${inconsistentOutcome.player_id} outcome conflicts with winning_task_force ${winningTaskForce}`;
  }
  return null;
}

type MatchIngestStatus = {
  status: 'processing' | 'partial' | 'complete' | 'limited' | 'failed';
  completed_stages: string[];
};

const TERMINAL_MATCH_INGEST_STATUSES = new Set<MatchIngestStatus['status']>(['complete', 'limited']);

const MATCH_INGEST_STAGES = {
  core: 'core',
  playerFacts: 'player_facts',
  matchBans: 'match_bans',
  opponentFacts: 'opponent_facts',
  skinFacts: 'skin_facts',
  playerProfiles: 'player_profiles',
  playerSearch: 'player_search',
  playerAverages: 'player_averages',
  teamAggregates: 'team_aggregates',
  bans: 'bans',
  partyRelationships: 'party_relationships',
  ratings: 'ratings',
  countProjections: 'count_projections',
  rankedStats: 'ranked_stats',
  performanceProjections: 'performance_projections',
  scalableStats: 'scalable_stats',
  matchSearch: 'match_search',
} as const;

const PERFORMANCE_STATS_REFRESH_MIN_INTERVAL_MS = 5 * 60 * 1000;
let lastPerformanceStatsRefreshAt = 0;

// raw_ingest_buffer is a handoff, not an audit archive. Permanent operator
// evidence belongs in hirez_raw_api_responses/match_ingest_status and compact
// delete summaries belong in raw_ingest_buffer_retention_audit. Keep terminal
// payloads for one hour of immediate debugging, then reclaim their JSON.
const RAW_BUFFER_PROCESSED_RETENTION_HOURS = positiveIntFromEnv('RAW_BUFFER_PROCESSED_RETENTION_HOURS', 1);
const RAW_BUFFER_FAILED_RETENTION_HOURS = positiveIntFromEnv('RAW_BUFFER_FAILED_RETENTION_HOURS', 1);
const RAW_BUFFER_RETENTION_BATCH_SIZE = positiveIntFromEnv('RAW_BUFFER_RETENTION_BATCH_SIZE', 5000);

function positiveIntFromEnv(name: string, fallback: number): number {
  const parsed = Number(process.env[name]);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

async function maybeRefreshPerformanceMetricStatsAfterBatch(processed: number): Promise<void> {
  if (processed < 1) return;

  const now = Date.now();
  if (now - lastPerformanceStatsRefreshAt < PERFORMANCE_STATS_REFRESH_MIN_INTERVAL_MS) {
    return;
  }

  // Histogram rows are maintained per match. Recompute the tiny public summary
  // projection from those buckets, never from historical match_players here.
  lastPerformanceStatsRefreshAt = now;
  await runExclusive('baseline:refresh', async () => {
    console.log('[buffer-processor] Refreshing performance summaries from histogram buckets...');
    await refreshPerformanceMetricStats();
  });
}

let ingestControlTablesReady = false;
let rawBufferRetentionTablesReady = false;

async function ensureRawBufferRetentionTables(): Promise<void> {
  if (rawBufferRetentionTablesReady) return;

  // raw_ingest_buffer is a short-term staging/audit table, not a permanent raw
  // API archive. Deleting old processed/failed payloads keeps indexes and
  // VACUUM pressure bounded, but operators still need to know what was removed
  // after incidents. This summary table retains the retention event, endpoint,
  // entity type, row count, and timestamp ranges without preserving bulky raw
  // JSON payloads forever.
  await one(`
    CREATE TABLE IF NOT EXISTS raw_ingest_buffer_retention_audit (
      id BIGSERIAL PRIMARY KEY,
      reason TEXT NOT NULL,
      status VARCHAR(20) NOT NULL,
      endpoint VARCHAR NOT NULL DEFAULT '',
      entity_type VARCHAR NOT NULL DEFAULT '',
      retention_seconds INT NOT NULL,
      deleted_count INT NOT NULL,
      oldest_created_at TIMESTAMPTZ,
      newest_created_at TIMESTAMPTZ,
      oldest_processed_at TIMESTAMPTZ,
      newest_processed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`);
  await one(`
    CREATE INDEX IF NOT EXISTS idx_rib_retention_audit_created
    ON raw_ingest_buffer_retention_audit (created_at DESC)`);
  rawBufferRetentionTablesReady = true;
}

async function ensureIngestControlTables(): Promise<void> {
  if (ingestControlTablesReady) return;

  // Multiple private participants all arrive with Hi-Rez player_id=0. The
  // historical primary key collapsed them into one row. Add a per-match ordinal
  // to the durable identity before any new payload is processed. The partition
  // time remains in the key, so this is compatible with the Timescale shape.
  await one(`ALTER TABLE match_players ADD COLUMN IF NOT EXISTS private_slot SMALLINT NOT NULL DEFAULT 0`);
  await one(`
    DO $$
    DECLARE
      current_pk TEXT;
      current_pk_def TEXT;
    BEGIN
      SELECT c.conname, pg_get_constraintdef(c.oid)
      INTO current_pk, current_pk_def
      FROM pg_constraint c
      WHERE c.conrelid = 'match_players'::regclass AND c.contype = 'p'
      LIMIT 1;

      IF current_pk_def IS NULL OR position('private_slot' in current_pk_def) = 0 THEN
        IF current_pk IS NOT NULL THEN
          EXECUTE format('ALTER TABLE match_players DROP CONSTRAINT %I', current_pk);
        END IF;
        ALTER TABLE match_players
          ADD PRIMARY KEY (match_id, player_id, private_slot, entry_datetime);
      END IF;
    END $$
  `);

  // The buffer worker is the place where raw API payloads become durable match
  // facts. A `matches` row alone is not enough to prove that the whole ingest
  // finished: the worker still has to write match_players, item/card/talent
  // facts, opponent facts, ratings, count projections, and search documents.
  //
  // `match_ingest_status` is the explicit completion boundary for that long
  // flow. Workers and relay staging checks treat status='complete' and the
  // lookup-only status='limited' as terminal, while status='processing' or
  // status='partial' means a later run may safely retry. Keeping this table
  // small and match-keyed lets us recover from crashes without using the final
  // `matches` table as a misleading proxy for full completion.
  await one(`
    CREATE TABLE IF NOT EXISTS match_ingest_status (
      match_id BIGINT PRIMARY KEY,
      status VARCHAR(20) NOT NULL DEFAULT 'processing'
        CHECK (status IN ('processing', 'partial', 'complete', 'limited', 'failed')),
      completed_stages TEXT[] NOT NULL DEFAULT '{}',
      source VARCHAR(50),
      attempts INT NOT NULL DEFAULT 0,
      error_message TEXT,
      started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      completed_at TIMESTAMPTZ
    )`);
  await one(`CREATE INDEX IF NOT EXISTS idx_mis_status_updated ON match_ingest_status (status, updated_at)`);

  // `match_opponents` is a cumulative projection keyed by
  // (player, player champion, opponent champion), so it cannot tell whether a
  // particular match has already contributed to the counters. The fact table
  // below provides that missing per-match idempotency key. insertMatchOpponents()
  // first inserts a fact row with ON CONFLICT DO NOTHING, then increments the
  // cumulative projection only when the fact row was newly created.
  await one(`
    CREATE TABLE IF NOT EXISTS match_opponent_facts (
      match_id BIGINT NOT NULL,
      player_id BIGINT NOT NULL,
      player_champion_id INT NOT NULL,
      opponent_champion_id INT NOT NULL,
      wins INT NOT NULL DEFAULT 0,
      losses INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (match_id, player_id, player_champion_id, opponent_champion_id)
    )`);
  await one(`CREATE INDEX IF NOT EXISTS idx_mof_player ON match_opponent_facts (player_id, player_champion_id)`);
  ingestControlTablesReady = true;
}

async function getMatchIngestStatus(matchId: number): Promise<MatchIngestStatus | null> {
  await ensureIngestControlTables();
  return one<MatchIngestStatus>(
    `SELECT status, completed_stages FROM match_ingest_status WHERE match_id = $1`,
    [matchId],
  );
}

async function markLegacyMatchComplete(matchId: number): Promise<void> {
  await ensureIngestControlTables();
  await one(
    `INSERT INTO match_ingest_status (match_id, status, completed_stages, source, attempts, completed_at, updated_at)
     VALUES ($1, 'complete', $2, 'legacy-existing-match', 0, now(), now())
     ON CONFLICT (match_id) DO NOTHING`,
    [matchId, Object.values(MATCH_INGEST_STAGES)],
  );
}

async function beginMatchIngest(matchId: number, source: string): Promise<Set<string>> {
  await ensureIngestControlTables();
  const row = await one<MatchIngestStatus>(
    `INSERT INTO match_ingest_status (match_id, status, source, attempts, started_at, updated_at)
     VALUES ($1, 'processing', $2, 1, now(), now())
     ON CONFLICT (match_id) DO UPDATE SET
       status = CASE
         WHEN match_ingest_status.status IN ('complete', 'limited') THEN match_ingest_status.status
         ELSE 'processing'
       END,
       source = EXCLUDED.source,
       attempts = match_ingest_status.attempts + 1,
       error_message = NULL,
       updated_at = now()
     RETURNING status, completed_stages`,
    [matchId, source],
  );
  return new Set(row?.completed_stages || []);
}

async function markMatchIngestStage(matchId: number, stage: string, stages: Set<string>): Promise<void> {
  stages.add(stage);
  await one(
    `UPDATE match_ingest_status
     SET completed_stages = (
           SELECT array_agg(DISTINCT stage_name)
           FROM unnest(completed_stages || $2::text[]) AS stages(stage_name)
         ),
         updated_at = now()
     WHERE match_id = $1`,
    [matchId, [stage]],
  );
}

async function markMatchIngestComplete(matchId: number, stages: Set<string>): Promise<void> {
  for (const stage of Object.values(MATCH_INGEST_STAGES)) stages.add(stage);
  await one(
    `UPDATE match_ingest_status
     SET status = 'complete',
         completed_stages = $2,
         completed_at = now(),
         updated_at = now(),
         error_message = NULL
     WHERE match_id = $1`,
    [matchId, [...stages]],
  );
}

async function markMatchIngestLimited(matchId: number): Promise<void> {
  await one(
    `UPDATE match_ingest_status
     SET status = 'limited',
         completed_at = now(),
         updated_at = now(),
         error_message = NULL
     WHERE match_id = $1`,
    [matchId],
  );
}

async function markMatchFactsDurable(
  matchId: number,
  queueId: number,
  entryDatetime: Date,
): Promise<void> {
  // `player_facts` plus the per-match ban rows are the public match-detail
  // boundary. Hourly discovery debt closes here; ratings, relationships,
  // aggregates, profile snapshots, and search are independently resumable
  // background projections and must never delay match availability.
  await one(
    `UPDATE match_ingest_status
     SET status = CASE WHEN status IN ('complete', 'limited') THEN status ELSE 'partial' END,
         updated_at = now(),
         error_message = NULL
     WHERE match_id = $1`,
    [matchId],
  );
  await markHourlyIngestMatchDebtComplete(matchId);
  await markHourlyWindowCompleteIfReady(queueId, entryDatetime);
}

async function markMatchIngestFailed(matchId: number | null, error: unknown, status: 'partial' | 'failed' = 'failed'): Promise<void> {
  if (!matchId || matchId <= 0) return;
  await ensureIngestControlTables();
  const row = await one<{ facts_durable: boolean }>(
    `INSERT INTO match_ingest_status (match_id, status, error_message, attempts, updated_at)
     VALUES ($1, $2, $3, 1, now())
     ON CONFLICT (match_id) DO UPDATE SET
       status = CASE
         WHEN match_ingest_status.status = 'limited' THEN 'limited'
         WHEN match_ingest_status.completed_stages @> ARRAY['player_facts', 'match_bans']::text[]
           THEN 'partial'
         ELSE EXCLUDED.status
       END,
       error_message = EXCLUDED.error_message,
       attempts = match_ingest_status.attempts + 1,
       updated_at = now()
     RETURNING completed_stages @> ARRAY['player_facts', 'match_bans']::text[] AS facts_durable`,
    [matchId, status, error instanceof Error ? error.message : String(error)],
  );
  if (row?.facts_durable) {
    // Projection failures are background debt. They must never reopen hourly
    // match acquisition or make an already-readable match disappear.
    await markHourlyIngestMatchDebtComplete(matchId);
  } else {
    await markHourlyIngestMatchDebtRetryable(
      matchId,
      `buffer-processor marked ${status}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

async function matchFactsAreDurable(matchId: number): Promise<boolean> {
  const row = await one<{ facts_durable: boolean }>(
    `SELECT completed_stages @> ARRAY['player_facts', 'match_bans']::text[] AS facts_durable
     FROM match_ingest_status
     WHERE match_id = $1`,
    [matchId],
  );
  return row?.facts_durable === true;
}

function isValidTimestamp(value: unknown): boolean {
  if (!value) return false;
  const timestamp = new Date(String(value)).getTime();
  return Number.isFinite(timestamp);
}

// Note: cleanupFetchedPlayersCache() is called inside processBufferBatch()
// at the start of each cycle. It cannot be a module-level call because it
// accesses globalFetchedPlayers which lives in hirez.ts — calling it during
// import triggers a temporal dead zone (ReferenceError).

type ClaimedBufferRow = {
  id: number;
  raw_data: any;
  endpoint: string;
  entity_type: string;
  entity_id: string | null;
  retry_count: number;
  priority: number;
};

type CumulativeProjectionBatchResult = {
  completedMatchIds: Set<number>;
  fallbackMatchIds: Set<number>;
};

const CUMULATIVE_PROJECTION_BATCH_SIZE = Math.min(
  positiveIntFromEnv('CUMULATIVE_PROJECTION_BATCH_SIZE', 8),
  25,
);
const MATCH_FACT_PROCESSING_CONCURRENCY = Math.min(
  positiveIntFromEnv('MATCH_FACT_PROCESSING_CONCURRENCY', 8),
  8,
);

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Apply one exactly-once projection in bounded chunks. A failed chunk is
 * bisected until the healthy matches commit and only the irreducible singleton
 * is returned to the ordinary per-match path. This turns a database timeout
 * into smaller useful commits instead of replaying the same 50-row statement
 * forever.
 */
async function applyAdaptiveProjectionBatch(
  stage: string,
  matchIds: number[],
  apply: (ids: number[]) => Promise<unknown>,
): Promise<CumulativeProjectionBatchResult> {
  const completedMatchIds = new Set<number>();
  const fallbackMatchIds = new Set<number>();

  const applyChunk = async (ids: number[]): Promise<void> => {
    if (ids.length === 0) return;
    const startedAt = Date.now();
    try {
      await apply(ids);
      await markMatchIngestStageForMatches(ids, stage);
      for (const matchId of ids) completedMatchIds.add(matchId);
      console.log(`[buffer-processor] Projection ${stage}: committed ${ids.length} match delta(s) in ${Date.now() - startedAt}ms`);
    } catch (error) {
      if (ids.length === 1) {
        fallbackMatchIds.add(ids[0]);
        console.error(
          `[buffer-processor] Projection ${stage}: batch path failed for match ${ids[0]} after `
          + `${Date.now() - startedAt}ms; using per-match fallback: ${errorMessage(error)}`,
        );
        return;
      }

      const midpoint = Math.ceil(ids.length / 2);
      console.warn(
        `[buffer-processor] Projection ${stage}: ${ids.length}-match delta failed after `
        + `${Date.now() - startedAt}ms; splitting into ${midpoint} + ${ids.length - midpoint}: ${errorMessage(error)}`,
      );
      await applyChunk(ids.slice(0, midpoint));
      await applyChunk(ids.slice(midpoint));
    }
  };

  for (let offset = 0; offset < matchIds.length; offset += CUMULATIVE_PROJECTION_BATCH_SIZE) {
    await applyChunk(matchIds.slice(offset, offset + CUMULATIVE_PROJECTION_BATCH_SIZE));
  }
  return { completedMatchIds, fallbackMatchIds };
}

async function markMatchIngestStageForMatches(matchIds: number[], stage: string): Promise<void> {
  if (matchIds.length === 0) return;
  await query(
    `UPDATE match_ingest_status
     SET completed_stages = array_append(completed_stages, $2),
         updated_at = now()
     WHERE match_id = ANY($1::bigint[])
       AND NOT completed_stages @> ARRAY[$2]::text[]`,
    [matchIds, stage],
  );
}

/**
 * Collapse the two hottest cumulative stages across the claimed rows. The
 * ordinary row processor remains the source of stage ordering and retries; it
 * simply observes these markers and skips work already committed here.
 */
async function applyBatchedCumulativeProjections(rows: ClaimedBufferRow[]): Promise<Set<number>> {
  const matchIds = [...new Set(rows
    .filter((row) => row.entity_type === 'match')
    .map((row) => Number(row.entity_id))
    .filter((id) => Number.isFinite(id) && id > 0))];
  if (matchIds.length < 2) return new Set<number>();

  const candidates = await query<{
    match_id: string;
    completed_stages: string[];
    is_ranked: boolean;
  }>(`
    SELECT mis.match_id, mis.completed_stages,
      COALESCE(m.is_ranked, m.queue_id = 486) AS is_ranked
    FROM match_ingest_status mis
    JOIN matches m ON m.match_id = mis.match_id
    WHERE mis.match_id = ANY($1::bigint[])
  `, [matchIds]);

  const averages = candidates
    .filter((row) => row.is_ranked
      && row.completed_stages.includes(MATCH_INGEST_STAGES.playerProfiles)
      && !row.completed_stages.includes(MATCH_INGEST_STAGES.playerAverages))
    .map((row) => Number(row.match_id));
  const fallbackMatchIds = new Set<number>();
  if (averages.length > 0) {
    const result = await applyAdaptiveProjectionBatch(
      MATCH_INGEST_STAGES.playerAverages,
      averages,
      updatePlayerAveragesForMatches,
    );
    for (const matchId of result.fallbackMatchIds) fallbackMatchIds.add(matchId);
  }

  const performance = candidates
    .filter((row) => row.is_ranked
      && row.completed_stages.includes(MATCH_INGEST_STAGES.rankedStats)
      && !row.completed_stages.includes(MATCH_INGEST_STAGES.performanceProjections))
    .map((row) => Number(row.match_id));
  const performanceResult = performance.length > 0
    ? await applyAdaptiveProjectionBatch(
      MATCH_INGEST_STAGES.performanceProjections,
      performance,
      upsertPerformanceProjectionsForMatches,
    )
    : { completedMatchIds: new Set<number>(), fallbackMatchIds: new Set<number>() };
  for (const matchId of performanceResult.fallbackMatchIds) fallbackMatchIds.add(matchId);

  const scalable = candidates
    .filter((row) => row.is_ranked
      && (row.completed_stages.includes(MATCH_INGEST_STAGES.performanceProjections)
        || performanceResult.completedMatchIds.has(Number(row.match_id)))
      && !row.completed_stages.includes(MATCH_INGEST_STAGES.scalableStats))
    .map((row) => Number(row.match_id));
  if (scalable.length > 0) {
    const result = await applyAdaptiveProjectionBatch(
      MATCH_INGEST_STAGES.scalableStats,
      scalable,
      upsertScalableStatsProjectionsForMatches,
    );
    for (const matchId of result.fallbackMatchIds) fallbackMatchIds.add(matchId);
  }
  return fallbackMatchIds;
}

async function requeueRecentQuotaPausedRecoveryRows(): Promise<number> {
  const rows = await query<{ count: number }>(
    `WITH revived AS (
       UPDATE raw_ingest_buffer rib
       SET status = 'pending',
           retry_count = 0,
           error_message = 'quota pause: recovery-required match retained pending',
           processed_at = NULL
       WHERE rib.status = 'failed'
         AND rib.entity_type = 'match'
         AND rib.endpoint = 'getmatchdetailsbatch'
         AND rib.created_at >= now() - interval '6 hours'
         AND jsonb_typeof(rib.raw_data) = 'array'
         AND (
           EXISTS (
             SELECT 1 FROM jsonb_array_elements(rib.raw_data) player
             WHERE btrim(COALESCE(player->>'ret_msg', '')) <> ''
           )
           OR (
             SELECT COUNT(*) FROM jsonb_array_elements(rib.raw_data) player
             WHERE btrim(COALESCE(player->>'ret_msg', '')) = ''
           ) < 10
         )
         AND EXISTS (
           SELECT 1 FROM hourly_ingest_match_debt debt
           WHERE debt.match_id::text = rib.entity_id
             AND debt.status IN ('pending', 'staged')
         )
       RETURNING 1
     )
     SELECT COUNT(*)::int AS count FROM revived`,
  );
  return Number(rows[0]?.count || 0);
}

type BufferRowProcessResult = {
  processed: number;
  failed: number;
  paused: number;
  deferred: number;
};

function emptyBufferRowProcessResult(): BufferRowProcessResult {
  return { processed: 0, failed: 0, paused: 0, deferred: 0 };
}

function addBufferRowProcessResult(
  target: BufferRowProcessResult,
  source: BufferRowProcessResult,
): void {
  target.processed += source.processed;
  target.failed += source.failed;
  target.paused += source.paused;
  target.deferred += source.deferred;
}

async function returnClaimedRowsToPending(rows: ClaimedBufferRow[], reason: string): Promise<void> {
  if (rows.length === 0) return;
  const requeued = await query<{ count: number }>(
    `WITH returned AS (
       UPDATE raw_ingest_buffer
       SET status = 'pending',
           processed_at = NULL
       WHERE id = ANY($1::bigint[])
         AND status = 'processing'
       RETURNING 1
     )
     SELECT COUNT(*)::int AS count FROM returned`,
    [rows.map((row) => row.id)],
  );
  console.log(
    `[buffer-processor] ${reason}; returned ${Number(requeued[0]?.count || 0)} `
    + `unstarted claimed row(s) to pending`,
  );
}

async function processClaimedBufferRow(
  row: ClaimedBufferRow,
  hasApiHeadroom: boolean,
  deferCumulativeProjectionWork: boolean,
  cumulativeProjectionFallbacks: Set<number>,
): Promise<BufferRowProcessResult> {
  const result = emptyBufferRowProcessResult();
  const lease = await one<{ id: number }>(
    `UPDATE raw_ingest_buffer
     SET processed_at = now()
     WHERE id = $1 AND status = 'processing'
     RETURNING id`,
    [row.id],
  );
  if (!lease) return result;

  if (
    !hasApiHeadroom
    && row.entity_type === 'match'
    && matchPayloadRequiresRecovery(row.raw_data)
  ) {
    // This payload is already durable. Keep it pending without consuming its
    // retry budget until a recovery endpoint can actually run. Healthy/full
    // match payloads in the same claimed batch remain DB-local and continue.
    await one(
      `UPDATE raw_ingest_buffer
       SET status = 'pending',
           error_message = 'quota pause: recovery-required match retained pending',
           processed_at = NULL
       WHERE id = $1`,
      [row.id],
    );
    result.paused++;
    return result;
  }

  try {
    const outcome = await processRawPayload(
      row.raw_data,
      row.endpoint,
      row.entity_type,
      row.id,
      true,
      deferCumulativeProjectionWork
        && !cumulativeProjectionFallbacks.has(Number(row.entity_id || 0)),
    );
    if (outcome === 'match-facts-persisted' || outcome === 'cumulative-projections-pending') {
      await one(
        `UPDATE raw_ingest_buffer
         SET status = 'pending',
             error_message = $2,
             processed_at = NULL
         WHERE id = $1`,
        [
          row.id,
          outcome === 'match-facts-persisted'
            ? 'match facts durable; background projections pending'
            : 'cumulative projections queued for batched delta',
        ],
      );
      result.deferred++;
    } else {
      await one(`UPDATE raw_ingest_buffer SET status = 'processed', processed_at = now() WHERE id = $1`, [row.id]);
      result.processed++;
    }
  } catch (err) {
    const matchId = row.entity_type === 'match' ? Number(row.entity_id || 0) : null;
    if (matchId && matchId > 0) {
      try {
        await markMatchIngestFailed(matchId, err);
      } catch (statusErr) {
        // Failure bookkeeping must never strand this row and the rest of its
        // claimed batch in processing. The raw row remains the retry source.
        console.error(`[buffer-processor] Failed to record ingest status for match ${matchId}: ${statusErr}`);
      }
    }
    const retry = (row.retry_count || 0) + 1;
    const factsDurable = Boolean(matchId && matchId > 0 && await matchFactsAreDurable(matchId));
    if (factsDurable) {
      // Projection debt remains retryable for as long as its canonical match
      // facts exist. Backoff prevents a poison/slow singleton from consuming
      // every drain cycle while other matches continue through the queue.
      const retryDelaySeconds = Math.min(300, 2 ** Math.min(retry, 8));
      await one(
        `UPDATE raw_ingest_buffer
         SET status = 'pending', retry_count = $1, error_message = $2,
             processed_at = NULL,
             available_at = now() + ($3::int * interval '1 second')
         WHERE id = $4`,
        [retry, String(err), retryDelaySeconds, row.id],
      );
      result.deferred++;
    } else if (retry >= 3) {
      await one(`UPDATE raw_ingest_buffer SET status = 'failed', retry_count = $1, error_message = $2, processed_at = now() WHERE id = $3`, [retry, String(err), row.id]);
    } else {
      await one(`UPDATE raw_ingest_buffer SET status = 'pending', retry_count = $1, error_message = $2, processed_at = NULL WHERE id = $3`, [retry, String(err), row.id]);
    }
    if (!factsDurable) result.failed++;
  }

  return result;
}

async function processClaimedBufferRows(
  rows: ClaimedBufferRow[],
  hasApiHeadroom: boolean,
  shouldStop?: () => boolean,
): Promise<{ processed: number; failed: number; paused: number; deferred: number }> {
  const totals = emptyBufferRowProcessResult();

  // The public availability boundary (match, ten player rows, and bans) is
  // independent between matches. Run only this facts-first prefix in parallel:
  // each lane owns one whole match and uses at most one pool connection at a
  // time, leaving ample room in the default 20-connection pool for web traffic.
  // Projection rows remain below in strict order because ratings and several
  // incremental aggregates are chronology-sensitive.
  const factRows = rows.filter((row) => row.entity_type === 'match' && row.priority <= 1);
  const factRowIds = new Set(factRows.map((row) => row.id));
  const derivedRows = rows.filter((row) => !factRowIds.has(row.id));
  if (factRows.length > 0) {
    const factStartedAt = Date.now();
    let nextFactIndex = 0;
    let startedFactCount = 0;
    const laneCount = Math.min(MATCH_FACT_PROCESSING_CONCURRENCY, factRows.length);
    const laneResults = await Promise.all(
      Array.from({ length: laneCount }, async () => {
        const laneTotal = emptyBufferRowProcessResult();
        while (!shouldStop?.()) {
          const index = nextFactIndex++;
          if (index >= factRows.length) break;
          const row = factRows[index];
          startedFactCount++;
          try {
            addBufferRowProcessResult(
              laneTotal,
              await processClaimedBufferRow(row, hasApiHeadroom, false, new Set<number>()),
            );
          } catch (error) {
            // An outer database/lease error is different from a payload failure
            // handled inside processClaimedBufferRow. Return this claim to the
            // durable queue when possible and let the other lanes continue.
            console.error(
              `[buffer-processor] Fact lane failed outside row retry handling for ${row.entity_id}: `
              + errorMessage(error),
            );
            await returnClaimedRowsToPending([row], 'Fact lane failure').catch((requeueError) => {
              console.error(
                `[buffer-processor] Could not return fact row ${row.id} after lane failure: `
                + errorMessage(requeueError),
              );
            });
            laneTotal.failed++;
          }
        }
        return laneTotal;
      }),
    );
    for (const laneResult of laneResults) addBufferRowProcessResult(totals, laneResult);
    console.log(
      `[buffer-processor] Fact lanes processed ${startedFactCount}/${factRows.length} row(s) with `
      + `${laneCount} worker(s) in ${Date.now() - factStartedAt}ms`,
    );

    if (startedFactCount < factRows.length) {
      await returnClaimedRowsToPending(factRows.slice(startedFactCount), 'Quiesce requested');
    }
    if (shouldStop?.()) {
      await returnClaimedRowsToPending(derivedRows, 'Quiesce requested');
      return totals;
    }
  }

  const deferCumulativeProjectionWork = derivedRows.filter((row) => row.entity_type === 'match').length > 1;
  let cumulativeProjectionFallbacks = new Set<number>();
  if (deferCumulativeProjectionWork) {
    try {
      cumulativeProjectionFallbacks = await applyBatchedCumulativeProjections(derivedRows);
    } catch (error) {
      // Even candidate discovery can fail during a transient database event.
      // Preserve forward progress by routing every claimed match through the
      // independently retryable single-row path instead of abandoning leases.
      cumulativeProjectionFallbacks = new Set(derivedRows
        .filter((row) => row.entity_type === 'match')
        .map((row) => Number(row.entity_id || 0))
        .filter((matchId) => matchId > 0));
      console.error(
        `[buffer-processor] Projection batch setup failed; using per-match fallback for `
        + `${cumulativeProjectionFallbacks.size} match(es): ${errorMessage(error)}`,
      );
    }
  }

  for (let index = 0; index < derivedRows.length; index++) {
    const factWorkPreemptedDerivedBatch = derivedRows[index].priority > 1
      && await hasPendingMatchFactsWaiting();
    if (shouldStop?.() || factWorkPreemptedDerivedBatch) {
      await returnClaimedRowsToPending(
        derivedRows.slice(index),
        factWorkPreemptedDerivedBatch ? 'Fact work arrived' : 'Quiesce requested',
      );
      break;
    }

    addBufferRowProcessResult(
      totals,
      await processClaimedBufferRow(
        derivedRows[index],
        hasApiHeadroom,
        deferCumulativeProjectionWork,
        cumulativeProjectionFallbacks,
      ),
    );
  }

  return totals;
}

export async function processBufferBatch(
  batchSize = 50,
  shouldStop?: () => boolean,
  _flushPlayerProfiles = false,
): Promise<{ processed: number; failed: number; deferred: number }> {
  if (shouldStop?.()) return { processed: 0, failed: 0, deferred: 0 };
  await ensureIngestControlTables();
  const headroom = await getApiHeadroomSnapshot();
  if (!headroom.hasUsableKeys) {
    const revived = await requeueRecentQuotaPausedRecoveryRows();
    if (revived > 0) {
      console.warn(`[buffer-processor] Requeued ${revived} recent quota-failed recovery row(s) as pending`);
    }
  }
  // Reset the relay-side fetched-player cache at the start of each
  // buffer-processor cycle. This is awaited intentionally: the cache lives in
  // HirezRelay, not this backend process, and recovery work below depends on
  // the cleanup having actually completed before the next batch starts.
  try {
    await cleanupFetchedPlayersCache();
  } catch (err) {
    console.warn(`[buffer-processor] Relay cache cleanup failed before batch: ${err}`);
  }
  // ----------------------------------------------------------------
  // Crash recovery: `processed_at` is used as the processing lease timestamp
  // while a row is status='processing'. This avoids the old created_at bug,
  // where old-but-active backlog rows were reset mid-process. Rows stuck in
  // processing past the lease are requeued or failed after the normal retry
  // budget.
  // ----------------------------------------------------------------
  const staleRows = await query(
    `WITH stale AS (
       UPDATE raw_ingest_buffer rib
       SET status = CASE
             WHEN EXISTS (
               SELECT 1 FROM match_ingest_status mis
               WHERE rib.entity_type = 'match'
                 AND rib.entity_id ~ '^[0-9]+$'
                 AND mis.match_id = rib.entity_id::BIGINT
                 AND mis.completed_stages @> ARRAY['player_facts', 'match_bans']::text[]
             )
               THEN 'pending'
             WHEN rib.retry_count >= 2 THEN 'failed'
             ELSE 'pending'
           END,
           retry_count = rib.retry_count + 1,
           error_message = concat_ws(' | ', nullif(rib.error_message, ''), 'stale processing lease reset'),
           processed_at = NULL,
           available_at = CASE
             WHEN EXISTS (
               SELECT 1 FROM match_ingest_status mis
               WHERE rib.entity_type = 'match'
                 AND rib.entity_id ~ '^[0-9]+$'
                 AND mis.match_id = rib.entity_id::BIGINT
                 AND mis.completed_stages @> ARRAY['player_facts', 'match_bans']::text[]
             )
               THEN now() + interval '5 minutes'
             ELSE rib.available_at
           END
       WHERE rib.status = 'processing'
         AND (
           (rib.processed_at IS NOT NULL AND rib.processed_at < now() - interval '15 minutes')
           OR (rib.processed_at IS NULL AND rib.created_at < now() - interval '30 minutes')
         )
       RETURNING rib.status
     )
     SELECT status, count(*)::int AS count FROM stale GROUP BY status`
  );

  for (const row of staleRows) {
    console.warn(`[buffer-processor] Recovered ${row.count} stale processing rows as ${row.status}`);
  }

  // Recovery-required matches are the highest-priority work, followed by
  // ordinary matches that have not crossed the durable player-facts boundary.
  // Derived rows are claimed together so cumulative stages can fold their
  // deltas in one set-based batch. processClaimedBufferRows checks for newly
  // staged fact work before every derived row and returns unstarted claims,
  // preserving the facts-first preemption boundary.
  const hasPendingMatchFacts = await hasPendingMatchFactsWaiting();
  const claimSize = batchSize;
  const rows = await query<ClaimedBufferRow>(
    `WITH eligible AS MATERIALIZED (
       SELECT rib.id, rib.created_at,
         CASE
           WHEN rib.entity_type = 'match'
             AND NOT COALESCE(mis.completed_stages @> ARRAY['player_facts', 'match_bans']::text[], false)
             AND CASE WHEN jsonb_typeof(rib.raw_data) = 'array' THEN (
               jsonb_array_length(rib.raw_data) < 10
               OR EXISTS (
                 SELECT 1 FROM jsonb_array_elements(rib.raw_data) player
                 WHERE btrim(COALESCE(player->>'ret_msg', '')) <> ''
                    OR lower(COALESCE(player->>'source', '')) = 'recovered'
               )
             ) ELSE false END
             THEN 0
           WHEN rib.entity_type = 'match'
             AND NOT COALESCE(mis.completed_stages @> ARRAY['player_facts', 'match_bans']::text[], false)
             THEN 1
           WHEN rib.endpoint IN ('getmatchhistory', 'getplayermatchhistory', 'getplayermatchhistoryafterdatetime')
             OR rib.entity_type IN ('match_history', 'prefetch_match')
             THEN 4
           WHEN rib.entity_type = 'match' THEN 3
           ELSE 2
         END AS priority,
         CASE
           WHEN rib.entity_type = 'match'
             THEN 'match:' || COALESCE(rib.entity_id, rib.id::text)
           ELSE 'row:' || rib.id::text
         END AS entity_claim_key
       FROM raw_ingest_buffer rib
       LEFT JOIN match_ingest_status mis
         ON rib.entity_type = 'match'
        AND mis.match_id = CASE
           WHEN rib.entity_id ~ '^[0-9]+$' THEN rib.entity_id::bigint
           ELSE NULL
        END
       WHERE rib.status = 'pending'
         AND rib.available_at <= now()
         AND (
           NOT $2::boolean
           OR (
             rib.entity_type = 'match'
             AND NOT COALESCE(mis.completed_stages @> ARRAY['player_facts', 'match_bans']::text[], false)
           )
         )
         AND (
           rib.entity_type <> 'match'
           OR NOT EXISTS (
             SELECT 1
             FROM raw_ingest_buffer in_flight
             WHERE in_flight.entity_type = 'match'
               AND in_flight.entity_id IS NOT DISTINCT FROM rib.entity_id
               AND in_flight.status = 'processing'
               AND in_flight.id <> rib.id
           )
         )
     ), deduplicated AS MATERIALIZED (
       SELECT DISTINCT ON (entity_claim_key)
         id, created_at, priority
       FROM eligible
       ORDER BY entity_claim_key, priority, created_at, id
     ), candidates AS (
       SELECT rib.id, rib.created_at, deduplicated.priority
       FROM deduplicated
       JOIN raw_ingest_buffer rib ON rib.id = deduplicated.id
       ORDER BY deduplicated.priority,
         rib.created_at ASC,
         rib.id ASC
       LIMIT $1
       FOR UPDATE OF rib SKIP LOCKED
     ), claimed AS (
       UPDATE raw_ingest_buffer rib
       SET status = 'processing', processed_at = now()
       FROM candidates candidate
       WHERE rib.id = candidate.id
       RETURNING rib.id, rib.raw_data, rib.endpoint, rib.entity_type, rib.entity_id,
         rib.retry_count, candidate.priority, rib.created_at
     )
     SELECT id, raw_data, endpoint, entity_type, entity_id, retry_count, priority
     FROM claimed ORDER BY priority, created_at, id`,
    [claimSize, hasPendingMatchFacts]
  );
  if (rows.length === 0) return { processed: 0, failed: 0, deferred: 0 };

  const { processed, failed, paused, deferred } = await processClaimedBufferRows(
    rows,
    headroom.hasUsableKeys,
    shouldStop,
  );
  if (paused > 0) {
    console.warn(`[buffer-processor] Quota pause retained ${paused} recovery-required match row(s) as pending`);
  }
  if (shouldStop?.()) return { processed, failed, deferred };

  await cleanupRawIngestBufferRetention('post-batch');

  // Publish exact percentiles from the incrementally maintained histogram.
  // This is bounded by distinct metric values rather than historical matches.
  try {
    await maybeRefreshPerformanceMetricStatsAfterBatch(processed);
  } catch (err) {
    console.error(`[buffer-processor] Post-ingest performance summary refresh failed: ${err}`);
  }

  // ----------------------------------------------------------------
  // REMOVED: Materialized view refresh from the hot path.
  // Old code refreshed MVs after every batch of 10+ matches. Processing
  // a backlog of 5,000 matches = 100 batches = 100 MV refreshes back-to-back.
  // Each refresh consumes massive I/O, acquires locks, and pegs the CPU.
  // This completely collapsed PostgreSQL performance during catch-up runs.
  // New: MV refresh moved to auto-ingester-scheduler as a standalone hourly
  // cron job (see fix below). Decouples MV freshness from queue volume.
  // Source: User report 2026-06-01 — "materialized view thrashing:
  //   buffer processor refreshes MVs on every batch, collapsing PG perf."
  // ----------------------------------------------------------------

  return { processed, failed, deferred };
}

/**
 * Claim and process pending match buffer rows for an interactive lookup.
 * This uses the exact same row processor and retry/status transitions as the
 * background batch worker, while avoiding an unrelated FIFO backlog delaying
 * the requested match response.
 */
export async function processPendingMatchBufferRows(matchIds: number[]): Promise<{ processed: number; failed: number; deferred: number }> {
  const ids = [...new Set(matchIds.map(Number).filter(id => Number.isFinite(id) && id > 0))].map(String);
  if (ids.length === 0) return { processed: 0, failed: 0, deferred: 0 };

  await ensureIngestControlTables();
  try {
    await cleanupFetchedPlayersCache();
  } catch (err) {
    console.warn(`[buffer-processor] Relay cache cleanup failed before requested-match batch: ${err}`);
  }

  const rows = await query<ClaimedBufferRow>(
    `WITH eligible AS MATERIALIZED (
       SELECT rib.id, rib.created_at, rib.entity_id,
         CASE WHEN jsonb_typeof(raw_data) = 'array' THEN
           CASE WHEN jsonb_array_length(raw_data) < 10
             OR EXISTS (
               SELECT 1 FROM jsonb_array_elements(raw_data) player
               WHERE btrim(COALESCE(player->>'ret_msg', '')) <> ''
                  OR lower(COALESCE(player->>'source', '')) = 'recovered'
             )
           THEN 0 ELSE 1 END
         ELSE 1 END AS priority
       FROM raw_ingest_buffer rib
       WHERE rib.status = 'pending'
         AND rib.available_at <= now()
         AND rib.entity_type = 'match'
         AND rib.entity_id = ANY($1::text[])
         AND NOT EXISTS (
           SELECT 1
           FROM raw_ingest_buffer in_flight
           WHERE in_flight.entity_type = 'match'
             AND in_flight.entity_id IS NOT DISTINCT FROM rib.entity_id
             AND in_flight.status = 'processing'
             AND in_flight.id <> rib.id
         )
     ), deduplicated AS MATERIALIZED (
       SELECT DISTINCT ON (entity_id)
         id, created_at, priority
       FROM eligible
       ORDER BY entity_id, priority, created_at, id
     ), candidates AS (
       SELECT rib.id, rib.created_at, deduplicated.priority
       FROM deduplicated
       JOIN raw_ingest_buffer rib ON rib.id = deduplicated.id
       ORDER BY deduplicated.priority, rib.created_at ASC, rib.id ASC
       FOR UPDATE OF rib SKIP LOCKED
     ), claimed AS (
       UPDATE raw_ingest_buffer rib
       SET status = 'processing', processed_at = now()
       FROM candidates candidate
       WHERE rib.id = candidate.id
       RETURNING rib.id, rib.raw_data, rib.endpoint, rib.entity_type, rib.entity_id,
         rib.retry_count, candidate.priority, rib.created_at
     )
     SELECT id, raw_data, endpoint, entity_type, entity_id, retry_count, priority
     FROM claimed ORDER BY priority, created_at, id`,
    [ids],
  );

  const headroom = await getApiHeadroomSnapshot();
  const { processed, failed, paused, deferred } = await processClaimedBufferRows(rows, headroom.hasUsableKeys);
  if (paused > 0) {
    console.warn(`[buffer-processor] Quota pause retained ${paused} requested match row(s) as pending`);
  }
  // Match lookup returns at the durable fact boundary. Statistic/search
  // projections remain queued for the background drain.
  await cleanupRawIngestBufferRetention('post-requested-match-batch');
  return { processed, failed, deferred };
}

export interface BufferDrainOptions {
  batchSize?: number;
  reason?: string;
  shouldStop?: () => boolean;
}

async function hasPendingMatchFactsWaiting(): Promise<boolean> {
  const row = await one<{ has_match_facts: boolean }>(
    `SELECT EXISTS (
       SELECT 1
       FROM raw_ingest_buffer rib
       LEFT JOIN match_ingest_status mis
         ON mis.match_id = CASE
           WHEN rib.entity_id ~ '^[0-9]+$' THEN rib.entity_id::bigint
           ELSE NULL
         END
       WHERE rib.entity_type = 'match'
         AND rib.status = 'pending'
         AND NOT COALESCE(mis.completed_stages @> ARRAY['player_facts', 'match_bans']::text[], false)
     ) AS has_match_facts`,
  );
  return row?.has_match_facts === true;
}

/**
 * Drain raw_ingest_buffer by repeatedly running processBufferBatch().
 *
 * Ownership:
 * - processBufferBatch() owns a single locked batch.
 * - drainRawIngestBuffer() owns the common "keep going until empty"
 *   loop used by cron, startup catch-up, and manual discovery routes.
 *
 * Why this exists:
 * The same loop had been copied into auto-ingester-scheduler.ts and
 * routes/matches.ts. Small differences in max-batch count or logging make it
 * easy for one trigger to behave differently from another during backlog
 * recovery. Keeping the loop here means all buffer drains share the invariant
 * that discovered work is fully ingested before the owner releases the drain.
 */
export async function drainRawIngestBuffer(options: BufferDrainOptions = {}): Promise<{ processed: number; failed: number; deferred: number; batches: number }> {
  const batchSize = options.batchSize ?? 50;
  const reason = options.reason ? ` (${options.reason})` : '';
  let totalProcessed = 0;
  let totalFailed = 0;
  let totalDeferred = 0;
  let batches = 0;

  while (true) {
    if (options.shouldStop?.()) break;
    const result = await processBufferBatch(batchSize, options.shouldStop, false);
    if (result.processed + result.failed + result.deferred === 0) break;
    batches++;
    totalProcessed += result.processed;
    totalFailed += result.failed;
    totalDeferred += result.deferred;
    if (options.shouldStop?.()) break;
  }

  console.log(
    `[buffer-processor] Drain complete${reason}: ${totalProcessed} complete, ` +
    `${totalDeferred} facts-first, ${totalFailed} failed, ${batches} batch(es)`,
  );
  return { processed: totalProcessed, failed: totalFailed, deferred: totalDeferred, batches };
}

export type RawBufferRetentionResult = {
  processedDeleted: number;
  failedDeleted: number;
  totalDeleted: number;
};

async function deleteRetainedRawBufferRows(status: 'processed' | 'failed', retentionHours: number, reason: string): Promise<number> {
  const rows = await query<{ deleted: number }>(
    `WITH doomed AS (
       SELECT id
       FROM raw_ingest_buffer
       WHERE status = $1
         AND COALESCE(processed_at, created_at) < now() - ($2::int * interval '1 hour')
       ORDER BY COALESCE(processed_at, created_at) ASC, id ASC
       LIMIT $3
     ),
     deleted AS (
       DELETE FROM raw_ingest_buffer rib
       USING doomed d
       WHERE rib.id = d.id
       RETURNING rib.status, rib.endpoint, rib.entity_type, rib.created_at, rib.processed_at
     ),
     inserted AS (
       INSERT INTO raw_ingest_buffer_retention_audit (
         reason,
         status,
         endpoint,
         entity_type,
         retention_seconds,
         deleted_count,
         oldest_created_at,
         newest_created_at,
         oldest_processed_at,
         newest_processed_at
       )
       SELECT
         $4,
         status,
         COALESCE(endpoint, ''),
         COALESCE(entity_type, ''),
         $2::int * 3600,
         count(*)::int,
         min(created_at),
         max(created_at),
         min(processed_at),
         max(processed_at)
       FROM deleted
       GROUP BY status, endpoint, entity_type
       RETURNING deleted_count
     )
     SELECT COALESCE(sum(deleted_count), 0)::int AS deleted
     FROM inserted`,
    [status, retentionHours, RAW_BUFFER_RETENTION_BATCH_SIZE, reason],
  );

  return Number(rows[0]?.deleted || 0);
}

/**
 * Periodic retention for raw_ingest_buffer.
 *
 * The table has two jobs that pull in opposite directions:
 * - It is the durable queue that prevents worker restarts from losing API
 *   payloads while ingest is active.
 * - It is also the fastest short-term audit trail when something goes wrong.
 *
 * To keep both properties, this cleanup only deletes terminal rows
 * (`processed`/`failed`) after configurable retention windows. Active rows are
 * never deleted here. Each delete pass is capped by
 * RAW_BUFFER_RETENTION_BATCH_SIZE and summarized into
 * raw_ingest_buffer_retention_audit before raw JSON leaves the table.
 */
export async function cleanupRawIngestBufferRetention(reason = 'manual'): Promise<RawBufferRetentionResult> {
  await ensureRawBufferRetentionTables();

  const result = await runExclusive('raw-buffer:retention', async () => {
    const processedDeleted = await deleteRetainedRawBufferRows(
      'processed',
      RAW_BUFFER_PROCESSED_RETENTION_HOURS,
      reason,
    );
    const failedDeleted = await deleteRetainedRawBufferRows(
      'failed',
      RAW_BUFFER_FAILED_RETENTION_HOURS,
      reason,
    );
    const totalDeleted = processedDeleted + failedDeleted;

    if (totalDeleted > 0) {
      console.log(
        `[buffer-retention] ${reason}: deleted ${totalDeleted} old raw buffer rows ` +
        `(processed=${processedDeleted}, failed=${failedDeleted})`,
      );
    }

    return { processedDeleted, failedDeleted, totalDeleted };
  });

  return result ?? { processedDeleted: 0, failedDeleted: 0, totalDeleted: 0 };
}

type RawPayloadProcessOutcome = 'complete' | 'match-facts-persisted' | 'cumulative-projections-pending';

async function processRawPayload(
  rawData: any[],
  endpoint: string,
  entityType: string,
  bufferRowId?: number,
  deferDerivedMatchWork = true,
  deferCumulativeProjectionWork = false,
): Promise<RawPayloadProcessOutcome> {
  void bufferRowId; // Retained for older tests/importers; batch status is updated by processBufferBatch.
  const rows = Array.isArray(rawData) ? rawData : [rawData];
  // CRITICAL QUOTA GUARD:
  // `getmatchhistory` rows are prefetch/history observations: usually one
  // player's view of one historical match. They are NOT complete
  // `getmatchdetailsbatch` payloads. If a mislabeled history row is routed
  // through processMatchPayload(), the match looks "broken" because it has
  // fewer than 10 players, and the worker recalls the canonical relay lookup. During
  // the 2026-06-18 backfill this converted harmless prefetch rows into tens of
  // thousands of outbound recovery calls (`getplayerbatchfrommatch`,
  // `getdemodetails`, `getplayerbatch`, and `getmatchhistory`) and limited all
  // Hi-Rez keys. Route by endpoint first so both old mislabeled rows
  // (`entity_type='match'`) and correct rows (`entity_type='match_history'` or
  // `prefetch_match`) take the cheap DB-only history insert path.
  if (
    endpoint === 'getmatchhistory' ||
    endpoint === 'getplayermatchhistory' ||
    endpoint === 'getplayermatchhistoryafterdatetime' ||
    entityType === 'match_history' ||
    entityType === 'prefetch_match' ||
    isHistoryOnlyPayload(rows)
    ) {
      await processMatchHistoryPayload(rows, endpoint);
      return 'complete';
    }

  switch (entityType) {
    case 'match':
      return processMatchPayload(rawData, endpoint, deferDerivedMatchWork, deferCumulativeProjectionWork);
    case 'player':
      await processPlayerPayload(rawData, endpoint);
      break;
    case 'champion':
      await processChampionPayload(rawData, endpoint);
      break;
    case 'item':
      await processItemPayload(rawData, endpoint);
      break;
    case 'leaderboard':
      await processLeaderboardPayload(rawData, endpoint);
      break;
    case 'loadout':
      await processLoadoutPayload(rawData, endpoint);
      break;
    case 'match_history':
      await processMatchHistoryPayload(rawData, endpoint);
      break;
    case 'esports':
      await processEsportsPayload(rawData, endpoint);
      break;
    case 'player_status':
      await processPlayerStatusPayload(rawData, endpoint);
      break;
    case 'player_champions':
      await processPlayerChampionsPayload(rawData, endpoint);
      break;
    case 'player_achievements':
      await processPlayerAchievementsPayload(rawData, endpoint);
      break;
    case 'champion_skins':
      await processChampionSkinsPayload(rawData, endpoint);
      break;
    case 'bounty_items':
      await processBountyItemsPayload(rawData, endpoint);
      break;
    case 'league_leaderboard':
      await processLeagueLeaderboardPayload(rawData, endpoint);
      break;
    case 'live_match':
      await processLiveMatchPayload(rawData, endpoint);
      break;
    case 'esports_team':
      await processEsportsTeamPayload(rawData, endpoint);
      break;
    default:
      // Unknown entity_type — raw data already stored in raw_ingest_buffer by hirez.ts.
      // Mark as processed so it doesn't get retried.
      console.log(`[BUFFER] No processor for entity_type=${entityType} (endpoint=${endpoint}). Skipping normalization.`);
      break;
  }
  return 'complete';
}

// â"€â"€â"€ Match Payload Processor â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€

async function skipPreviouslyCompletedMatch(matchId: number): Promise<boolean> {
  if (!Number.isFinite(matchId) || matchId <= 0) return false;

  const existingStatus = await getMatchIngestStatus(matchId);
  if (existingStatus && TERMINAL_MATCH_INGEST_STATUSES.has(existingStatus.status)) {
    console.log(`[buffer-processor] Match ${matchId} already has terminal ${existingStatus.status} ingest status; skipping staged duplicate`);
    return true;
  }

  const existingMatch = await one(`SELECT 1 FROM matches WHERE match_id = $1`, [matchId]);
  if (existingMatch && !existingStatus) {
    await markLegacyMatchComplete(matchId);
    console.log(`[buffer-processor] Match ${matchId} exists without ingest status; marked legacy-complete and skipped duplicate`);
    return true;
  }

  return false;
}

async function loadDurableMatchResumePayload(matchId: number): Promise<any[]> {
  // match_players is a time-partitioned hypertable. Resolve the match timestamp
  // first so the roster read prunes old compressed chunks instead of scanning
  // the entire table for one match ID.
  const match = await one(
    `SELECT entry_datetime
     FROM matches
     WHERE match_id = $1
     ORDER BY entry_datetime DESC
     LIMIT 1`,
    [matchId],
  );
  if (!match?.entry_datetime) return [];

  return query(
    `SELECT
       mp.*,
       m.entry_datetime AS "Entry_Datetime",
       m.map AS "Map_Game",
       m.queue_id AS match_queue_id,
       m.duration_seconds AS "Match_Duration",
       floor(COALESCE(m.duration_seconds, 0) / 60.0)::int AS "Minutes",
       m.region AS "Region",
       m.team1_score AS "Team1Score",
       m.team2_score AS "Team2Score",
       m.winning_task_force AS "Winning_TaskForce",
       CASE WHEN m.has_replay THEN 'y' ELSE 'n' END AS "hasReplay",
       true AS recovery_attempted,
       COALESCE(NULLIF(m.source, ''), 'durable_match_facts') AS recovery_source,
       COALESCE(rs.api_calls, 0) AS recovery_api_calls,
       COALESCE(m.limited, false) AS limited
     FROM match_players mp
     JOIN matches m
       ON m.match_id = mp.match_id
      AND m.entry_datetime = mp.entry_datetime
     LEFT JOIN recovery_stats rs ON rs.match_id = m.match_id
     WHERE mp.match_id = $1
       AND mp.entry_datetime = $2::timestamptz
     ORDER BY mp.task_force, mp.private_slot, mp.player_id`,
    [matchId, match.entry_datetime],
  );
}

async function processMatchPayload(
  rawData: any[],
  endpoint: string,
  deferDerivedWork: boolean,
  deferCumulativeProjectionWork: boolean,
): Promise<RawPayloadProcessOutcome> {
  if (!Array.isArray(rawData)) {
    throw new Error(`Invalid match payload from ${endpoint}: raw_data must be an array`);
  }
  // Most staged payloads carry the match ID even when their roster is broken.
  // Check the durable completion boundary before recovery so stale legacy
  // duplicates cannot spend relay quota rebuilding facts already in Postgres.
  const bufferedMatchId = rawData
    .map((row: any) => Number(row?.Match || row?.match_id || row?.MatchId || 0))
    .find((matchId: number) => Number.isFinite(matchId) && matchId > 0) || 0;
  if (await skipPreviouslyCompletedMatch(bufferedMatchId)) return 'complete';

  // A facts-first pass deliberately requeues the original raw row while
  // projections continue in the background. If that original payload was a
  // broken/partial vendor response, parsing it again would call the canonical
  // relay a second time even though the recovered roster is already durable.
  // Resume from matches + match_players instead: this makes projection retries
  // DB-only and preserves the one relay-recovery attempt invariant.
  if (bufferedMatchId > 0 && matchPayloadRequiresRecovery(rawData)) {
    const existingStatus = await getMatchIngestStatus(bufferedMatchId);
    const factsDurable = Boolean(
      existingStatus?.completed_stages?.includes(MATCH_INGEST_STAGES.playerFacts)
      && existingStatus.completed_stages.includes(MATCH_INGEST_STAGES.matchBans),
    );
    if (factsDurable) {
      const durablePayload = await loadDurableMatchResumePayload(bufferedMatchId);
      if (durablePayload.length > 0) {
        rawData = durablePayload;
        console.log(
          `[buffer-processor] Match ${bufferedMatchId} resumed from durable facts; ` +
          `skipped duplicate relay recovery`,
        );
      }
    }
  }

  // getmatchdetailsbatch returns a flat list of player objects with Kills_Player, Gold_Earned, etc.
  // array of 10 player objects with Kills_Player, Gold_Earned, etc.
  // Single match lookup is just a batch call with 1 parameter.
  const players = rawData.filter((p: any) => !(p.ret_msg || '').trim());
  // Separate valid players from broken ones (ret_msg = broken skin / Int16 overflow)
  // Broken players have NO stats — they are NOT private accounts
  // Only valid players (no ret_msg) with player_id=0 are real private accounts
  const validPlayers = players.map(normalizeMatchPlayer);
  const knownReal = validPlayers.filter((p: any) => p.player_id > 0);
  const knownPrivate = validPlayers.filter((p: any) => p.player_id === 0 && !p.has_ret_msg && !(p.ret_msg || '').trim());

  let normalized: any[] = validPlayers;
  let recoveredPlayers: any[] = [];
  const hasExplicitApiReturn = rawData.some((p: any) => Boolean((p.ret_msg || '').trim()));
  // Broken-skin recovery should be triggered by Hi-Rez returning an explicit
  // ret_msg sentinel, not by seeing a high skin_id on an otherwise valid row.
  // The skin reference tracker still records high IDs, but treating valid high
  // IDs as broken caused healthy 10-player matches to be marked broken and
  // pulled into unnecessary recovery.
  const hasBrokenSkin = hasExplicitApiReturn;
  // Discovery and requested-match recovery stage a synthetic, complete
  // getmatchdetailsbatch-shaped payload whose player facts already carry
  // source='recovered'. Treat that as recovered work without recursively
  // recalling the relay a second time. Older code saw ten usable rows,
  // mislabeled the match direct, omitted recovery_stats, and lost demo bans.
  const relayRecoveryAttempted = rawData.some((player: any) => player?.recovery_attempted === true);
  const isStagedRecoveryPayload = relayRecoveryAttempted
    || isStagedRecoveryRoster(normalized, hasExplicitApiReturn);
  let isBroken = normalized.length < 10 || hasBrokenSkin || isStagedRecoveryPayload;
  let meta: any;
  let actualMatchId: number;
  let entryDatetime: string;
  const stagedRecoveryApiCalls = rawData
    .map((player: any) => Number(player?.recovery_api_calls))
    .find((value: number) => Number.isFinite(value) && value >= 0);
  let recoveryApiCalls: number | null = stagedRecoveryApiCalls ?? null;

  if (players.length === 0) {
    // 0 valid players from getmatchdetails — all players have broken skins.
    const rawFirst = rawData[0];
    const matchId = Number(rawFirst?.Match || rawFirst?.match_id || rawFirst?.MatchId || 0);
    meta = {
      match_id: matchId,
      entry_datetime: rawFirst?.Entry_Datetime || new Date().toISOString(),
      map: null,
      queue_id: null,
      duration_seconds: null,
      region: null,
      team1_score: null,
      team2_score: null,
      winning_task_force: null,
      has_replay: null,
      is_ranked: null,
      surrendered: null,
      match_level: null,
    };

    if (matchId > 0) {
      try {
        const rawQueueId = Number(
          rawFirst?.match_queue_id || rawFirst?.Match_Queue_Id || rawFirst?.Queue || 0,
        );
        const [outcome] = await getMatchDetailsBatch(
          [{
            matchId,
            queueId: rawQueueId > 0 ? rawQueueId : undefined,
          }],
          'ranked_recovery',
        );
        const result = outcome?.status === 'recovery_pending' ? undefined : outcome?.match;
        recoveredPlayers = result?.players || [];
        recoveryApiCalls = Number.isFinite(Number(result?.recovery_api_calls))
          ? Number(result?.recovery_api_calls)
          : recoveryApiCalls;
        normalized = recoveredPlayers;
        if (result) {
          meta = { ...meta, ...result, match_id: matchId };
        }
      } catch (err) {
        console.log('[RECOVERY] Failed for 0-player match ' + matchId + ': ' + err);
      }
    }

    actualMatchId = meta.match_id;
    entryDatetime = meta.entry_datetime;
  } else {
    meta = extractMatchMetadata(players);
    if (relayRecoveryAttempted) {
      const relayRow = rawData.find((player: any) => player?.recovery_attempted === true) || {};
      meta = {
        ...meta,
        recovery_attempted: true,
        recovery_source: relayRow.recovery_source || 'relay_recovery',
        recovery_terminal: relayRow.recovery_terminal === true,
        limited: relayRow.limited === true,
        recovery_api_calls: Number(relayRow.recovery_api_calls || 0),
      };
    }
    actualMatchId = meta.match_id;
    entryDatetime = meta.entry_datetime;

    if (isBroken && !isStagedRecoveryPayload) {
      try {
        const queueId = Number(meta.queue_id || 0);
        const [outcome] = await getMatchDetailsBatch(
          [{
            matchId: actualMatchId,
            queueId: queueId > 0 ? queueId : undefined,
          }],
          'ranked_recovery',
        );
        const result = outcome?.status === 'recovery_pending' ? undefined : outcome?.match;
        recoveredPlayers = result?.players || [];
        recoveryApiCalls = Number.isFinite(Number(result?.recovery_api_calls))
          ? Number(result?.recovery_api_calls)
          : recoveryApiCalls;
        const knownIds = new Set(normalized.map((p: any) => Number(p.player_id)).filter((id: number) => id > 0));
        const knownPrivateFingerprints = new Map<string, number>();
        for (const player of normalized.filter(isPrivateAccountParticipant)) {
          const fingerprint = privateParticipantFingerprint(player);
          knownPrivateFingerprints.set(fingerprint, (knownPrivateFingerprints.get(fingerprint) || 0) + 1);
        }
        for (const player of recoveredPlayers) {
          const playerId = Number(player.player_id || 0);
          if (playerId > 0) {
            if (!knownIds.has(playerId)) {
              normalized.push(player);
              knownIds.add(playerId);
            }
            continue;
          }

          // player_id=0 is not an identity. Recovery may return the direct
          // private rows we already supplied plus additional private accounts
          // whose detail was dropped. Consume matching direct fingerprints once
          // and retain every unmatched private participant, including multiple
          // zero placeholders.
          const fingerprint = privateParticipantFingerprint(player);
          const duplicateCount = knownPrivateFingerprints.get(fingerprint) || 0;
          if (duplicateCount > 0) {
            knownPrivateFingerprints.set(fingerprint, duplicateCount - 1);
          } else {
            normalized.push(player);
          }
        }
        if (result) {
          meta = { ...meta, ...result, match_id: actualMatchId };
          entryDatetime = meta.entry_datetime || entryDatetime;
        }
      } catch (err) {
        console.log('[RECOVERY] Failed for match ' + actualMatchId + ': ' + err);
      }

      // Recovery ran but didn't actually recover any players — reclassify.
      // This happens when hasBrokenSkin=true (ret_msg sentinel) but all 10
      // players are already present and authoritative. The match is not broken.
      if (!isStagedRecoveryPayload && recoveredPlayers.length === 0 && normalized.length >= 10) {
        const hasRetMsg = normalized.some((p: any) => p.has_ret_msg);
        const hasMinimal = normalized.some((p: any) => p.source === 'minimal');
        if (!hasRetMsg && !hasMinimal) {
          isBroken = false;
          console.log(`[RECOVERY] Match ${actualMatchId} had all 10 players — clearing broken flag (false positive)`);
        }
      }
    }
  }

  normalized.sort((a: any, b: any) => a.task_force - b.task_force);
  if (!Number.isFinite(Number(actualMatchId)) || Number(actualMatchId) <= 0) {
    // A real match must have a stable positive match_id before any final-table
    // write. Allowing match_id=0 creates a poison record that every future
    // guard treats as "handled" while it is not tied to any Hi-Rez match.
    throw new Error(`Invalid match payload from ${endpoint}: missing positive match_id`);
  }
  actualMatchId = Number(actualMatchId);

  // Retain the completion guard for unusual payloads whose authoritative ID
  // is only established by normalization or recovery metadata.
  if (actualMatchId !== bufferedMatchId && await skipPreviouslyCompletedMatch(actualMatchId)) return 'complete';

  if (!isValidTimestamp(entryDatetime)) {
    await markMatchIngestFailed(actualMatchId, `invalid entry_datetime: ${entryDatetime}`, 'partial');
    throw new Error(`Invalid match payload for ${actualMatchId}: invalid entry_datetime ${entryDatetime}`);
  }

  const queueId = Number(meta?.queue_id || 0);
  if (!Number.isFinite(queueId) || queueId <= 0) {
    await markMatchIngestFailed(actualMatchId, `invalid queue_id: ${meta?.queue_id ?? 'missing'}`, 'partial');
    throw new Error(
      `Invalid match payload for ${actualMatchId}: missing positive queue_id. ` +
      `Full match facts must come from getmatchdetailsbatch/recovery metadata, not history prefetch rows.`,
    );
  }
  meta.queue_id = queueId;
  const isRankedMatch = isRankedStatsQueue(queueId);

  const mapName = String(meta?.map || '').trim();
  if (!mapName || mapName.toLowerCase() === 'unknown') {
    await markMatchIngestFailed(actualMatchId, `invalid map: ${meta?.map ?? 'missing'}`, 'partial');
    throw new Error(
      `Invalid match payload for ${actualMatchId}: missing authoritative map metadata. ` +
      `History/profile observations can seed recovery, but they must not create Unknown-map match rows.`,
    );
  }
  meta.map = mapName;

  const playerOutcomeConsensus = resolvePlayerOutcomeConsensus(normalized);
  if (!playerOutcomeConsensus.coherent) {
    await markMatchIngestFailed(actualMatchId, 'contradictory player win/loss outcomes', 'partial');
    throw new Error(
      `Invalid match payload for ${actualMatchId}: player win/loss outcomes do not identify one winning task force.`,
    );
  }

  // Queue 424 is casual Siege and 486 is ranked Siege. Historical Hi-Rez
  // responses sometimes reversed Team1Score/Team2Score relative to the
  // TaskForce carried by every player row. Canonicalize that one proven shape
  // before applying the general completed-result boundary.
  if (queueId === 424 || queueId === 486) {
    const reconciledScore = reconcileSiegeMatchScore({
      team1: meta.team1_score,
      team2: meta.team2_score,
      winner: meta.winning_task_force,
    }, playerOutcomeConsensus);
    if (reconciledScore) {
      meta.team1_score = reconciledScore.team1;
      meta.team2_score = reconciledScore.team2;
      meta.winning_task_force = reconciledScore.winner;
      meta.score_canonicalized = reconciledScore.canonicalized;
    }
  }

  if (!isValidCompletedMatchScore(meta.team1_score, meta.team2_score, meta.winning_task_force)) {
    await markMatchIngestFailed(actualMatchId, 'missing or contradictory completed-match score', 'partial');
    throw new Error(
      `Invalid match payload for ${actualMatchId}: completed score/winner is not coherent. ` +
      `Recovery requires repeated direct or target-history score observations and never uses demo.`,
    );
  }

  if (
    playerOutcomeConsensus.winner !== null
    && playerOutcomeConsensus.winner !== Number(meta.winning_task_force)
  ) {
    await markMatchIngestFailed(actualMatchId, 'score winner contradicts player outcomes', 'partial');
    throw new Error(
      `Invalid match payload for ${actualMatchId}: completed score winner contradicts unanimous player outcomes.`,
    );
  }

  // A player_id=0 private account has no durable identity and cannot safely
  // contribute champion/player metrics. Retain one zero-valued logical roster
  // row per recovery-only private account, with only team/outcome metadata
  // populated, so every detailed row remains eligible even when recovered=false.
  let detailedTeamOne = normalized.filter((player: any) => isDetailedMatchPlayer(player) && Number(player.task_force) === 1).length;
  let detailedTeamTwo = normalized.filter((player: any) => isDetailedMatchPlayer(player) && Number(player.task_force) === 2).length;
  normalized = normalized.map((player: any) => {
    if (!isPrivateAccountParticipant(player)) return player;
    const source = String(player?.source || 'direct').toLowerCase();
    const hasDetailedChampionFacts = Number(player?.champion_id || 0) > 0;
    // Direct private rows can arrive before the broken response and retain full
    // match facts. Only recovery/minimal rows whose detail was dropped become
    // zero placeholders.
    if (source === 'minimal' || !hasDetailedChampionFacts) {
      const placeholder = toPrivateAccountPlaceholder(
        player,
        meta,
        actualMatchId,
        detailedTeamOne,
        detailedTeamTwo,
      );
      if (placeholder.task_force === 1) detailedTeamOne++;
      if (placeholder.task_force === 2) detailedTeamTwo++;
      return placeholder;
    }
    return { ...player, player_name: PRIVATE_ACCOUNT_NAME };
  });
  const privatePlaceholders = normalized.filter(isPrivateAccountPlaceholder);
  const privateParticipants = normalized.filter(isPrivateAccountParticipant);
  if (privatePlaceholders.length > 0) isBroken = true;

  const rosterIssue = validateLogicalMatchRoster(normalized, actualMatchId, Number(meta.winning_task_force));
  const teamOneCount = normalized.filter((player: any) => Number(player?.task_force || 0) === 1).length;
  const teamTwoCount = normalized.filter((player: any) => Number(player?.task_force || 0) === 2).length;
  const limitedReason = rosterIssue ? limitedMatchReason({
    playerCount: normalized.length,
    teamOneCount,
    teamTwoCount,
    allRowsAuthoritative: normalized.every(isDetailedMatchPlayer),
    recoverySource: meta?.recovery_source,
    recoveryTerminal: meta?.recovery_terminal,
    recoveryApiCalls,
    anchorPlayerCount: meta?.anchor_player_count,
  }) : null;
  const isLimited = limitedReason !== null;
  if (rosterIssue && !isLimited) {
    await markMatchIngestFailed(actualMatchId, `non-authoritative roster: ${rosterIssue}`, 'partial');
    throw new Error(`Invalid match payload for ${actualMatchId}: ${rosterIssue}`);
  }

  // ----------------------------------------------------------------
  // Completion boundary:
  // - Old code used `SELECT 1 FROM matches` as the duplicate guard.
  // - That prevented API flood loops, but it also meant a crash after the
  //   match row insert could hide missing players/ratings/projections forever.
  // - New code treats `match_ingest_status.status='complete'` as the durable
  //   completion signal for matches processed by this worker version.
  // - A legacy match row with no status record is treated as complete to avoid
  //   re-incrementing historical aggregate tables when old staged duplicates
  //   surface. New partial/crashed ingests always have a status row before the
  //   first final-table write, so retries can continue instead of fossilizing.
  // ----------------------------------------------------------------
  const ingestStages = await beginMatchIngest(actualMatchId, endpoint);
  const factsNeededPersistence = !ingestStages.has(MATCH_INGEST_STAGES.core)
    || !ingestStages.has(MATCH_INGEST_STAGES.playerFacts)
    || !ingestStages.has(MATCH_INGEST_STAGES.matchBans);

  // Insert match — idempotent via ON CONFLICT DO UPDATE.
  const hasPrivatePlaceholder = privatePlaceholders.length > 0;
  const isRecovered = !isLimited && isBroken
    && !hasPrivatePlaceholder
    && normalized.every((player: any) => isDetailedMatchPlayer(player));
  const source = hasPrivatePlaceholder ? 'minimal' : (isRecovered ? 'recovery' : 'direct');
  const representativePlayer = normalized.find((player: any) => !isPrivateAccountPlaceholder(player)) || normalized[0];
  if (!ingestStages.has(MATCH_INGEST_STAGES.core)) {
    await one(`INSERT INTO matches (match_id, entry_datetime, map, queue_id, duration_seconds, region,
      team1_score, team2_score, winning_task_force, has_replay, is_ranked, recovered, broken, private,
      limited, limited_reason, surrendered, match_level, source, ingested_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, now())
    ON CONFLICT (match_id, entry_datetime) DO UPDATE SET
      duration_seconds = CASE
        WHEN COALESCE(matches.duration_seconds, 0) <= 0 AND EXCLUDED.duration_seconds > 0
          THEN EXCLUDED.duration_seconds
        ELSE matches.duration_seconds
      END,
      broken = EXCLUDED.broken,
      recovered = EXCLUDED.recovered,
      private = EXCLUDED.private,
      limited = EXCLUDED.limited,
      limited_reason = EXCLUDED.limited_reason,
      source = EXCLUDED.source,
      team1_score = EXCLUDED.team1_score,
      team2_score = EXCLUDED.team2_score,
      winning_task_force = EXCLUDED.winning_task_force,
      ingested_at = now()`,
    [actualMatchId, entryDatetime, meta.map, meta.queue_id, meta.duration_seconds,
      meta.region, meta.team1_score, meta.team2_score,
      meta.winning_task_force, meta.has_replay,
      isRankedMatch, isRecovered, isBroken,
      privateParticipants.length > 0,
      isLimited, limitedReason,
      Boolean(representativePlayer?.surrendered), representativePlayer?.final_match_level || 0,
      source]);

    // Record recovery_stats if this match needed recovery
    if (isBroken || hasBrokenSkin) {
      const apiCalls = recoveryApiCalls ?? (recoveredPlayers.length > 0 ? 1 + recoveredPlayers.length : 0);
      const directCount = normalized.filter((p: any) => String(p?.source || 'direct').toLowerCase() === 'direct').length;
      const recoveredCount = normalized.filter((p: any) => String(p?.source || '').toLowerCase() === 'recovered').length;
      await one(`INSERT INTO recovery_stats (match_id, players, direct_count, recovered_count, missing_count, api_calls, total_calls)
        VALUES ($1, 10, $2, $3, $4, $5, $5)
        ON CONFLICT (match_id) DO UPDATE SET players = EXCLUDED.players, direct_count = EXCLUDED.direct_count, recovered_count = EXCLUDED.recovered_count, missing_count = EXCLUDED.missing_count, api_calls = EXCLUDED.api_calls, total_calls = EXCLUDED.total_calls`, 
        [actualMatchId, directCount, recoveredCount,
         Math.max(0, 10 - directCount), apiCalls]);
    }

    // Auto-calc: upsert hourly_match_counts for this match. The helper
    // recomputes the count from `matches`, so this stage is idempotent and safe
    // to repeat if a crash happens before the stage marker is saved.
    await upsertHourlyCount(meta.region, meta.queue_id, new Date(entryDatetime));
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.core, ingestStages);
  }

// Process each player
  if (!ingestStages.has(MATCH_INGEST_STAGES.playerFacts)) {
    const privateSlots = assignPrivateParticipantSlots(normalized);
    // Concurrent fact lanes can contain back-to-back matches with several of
    // the same players. Acquire shared player-row locks in a deterministic
    // order across every match so parallel upserts cannot form a lock cycle.
    // Keep the normalized roster order untouched for all later projections.
    const factPersistencePlayers = [...normalized].sort((left: any, right: any) => {
      const leftId = Number(left?.player_id || 0);
      const rightId = Number(right?.player_id || 0);
      const leftKey = leftId > 0 ? leftId : Number.MAX_SAFE_INTEGER;
      const rightKey = rightId > 0 ? rightId : Number.MAX_SAFE_INTEGER;
      if (leftKey !== rightKey) return leftKey - rightKey;
      return (privateSlots.get(left) || 0) - (privateSlots.get(right) || 0);
    });
    for (const player of factPersistencePlayers) {
      const isPrivateParticipant = isPrivateAccountParticipant(player);
      const isPrivatePlaceholder = isPrivateAccountPlaceholder(player);
      const playerPrivateSlot = isPrivateParticipant ? (privateSlots.get(player) || 0) : 0;
      // A private identity starts with the immutable match/slot observation.
      // PartyId is stored as evidence, never used directly as the person key.
      // Resolution runs after all ten match rows exist so known public party
      // companions can contribute stable cross-match evidence.
      const privatePlayerId = isPrivateParticipant
        ? await handlePrivatePlayer(actualMatchId, playerPrivateSlot, player, entryDatetime, isRankedMatch ? {
            queueId,
            statsScope: 'ranked',
            map: mapName,
            matchEndDatetime: new Date(
              new Date(entryDatetime).getTime() + Math.max(0, Number(meta.duration_seconds) || 0) * 1000,
            ).toISOString(),
            observationQuality: isLimited ? 'limited' : 'complete',
          } : {})
        : null;
      if (!isPrivateParticipant) {
        await upsertPlayer(player, { includeRankedSeed: isRankedMatch && !isLimited });
      }
      await insertMatchPlayer(
        actualMatchId,
        player,
        entryDatetime,
        meta.duration_seconds,
        privatePlayerId,
        playerPrivateSlot,
      );
      if (!isPrivateParticipant && !isPrivatePlaceholder) {
        await insertMatchPlayerItems(actualMatchId, player);
        await insertMatchPlayerCards(actualMatchId, player);
        await insertMatchPlayerTalents(actualMatchId, player);
      }
    }
    if (!isLimited && privateSlots.size > 0) await resolvePrivateAccountsForMatch(actualMatchId);
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.playerFacts, ingestStages);
  }

  // Presence is a small idempotent read model, not a ranked performance
  // projection. Keep it outside the playerFacts condition so an older
  // completed-stage replay can backfill the rolling table safely.
  if (isRankedMatch) await upsertRankedPlayerPresence(actualMatchId, entryDatetime);

  // Bans are part of the match-detail read model, not an aggregate. Persist
  // them before exposing the fact boundary; ranked ban counters remain in the
  // later background `bans` stage.
  if (!ingestStages.has(MATCH_INGEST_STAGES.matchBans)) {
    await insertMatchBans(actualMatchId, normalized);
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.matchBans, ingestStages);
  }

  await markMatchFactsDurable(actualMatchId, queueId, new Date(entryDatetime));

  if (isLimited) {
    await markMatchIngestLimited(actualMatchId);
    console.warn(
      `[buffer-processor] Match ${actualMatchId} retained as limited (${limitedReason}) with `
      + `${normalized.length}/10 authoritative rows after one roster-anchor attempt; all projections skipped`,
    );
    return 'complete';
  }

  // This is the hard hourly-ingest boundary. Once core, roster, per-player
  // item/card/talent facts, and bans are durable, the match page must be able
  // to read PostgreSQL immediately. Everything below is background projection
  // work and cannot hold the next match's facts behind it.
  if (deferDerivedWork && factsNeededPersistence) {
    return 'match-facts-persisted';
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.playerProfiles)) {
    // Automatic profile snapshots were removed to preserve the Hi-Rez budget.
    // Match ingest already records player identity and last-seen facts; profile
    // fields are refreshed only through the explicit player-profile action.
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.playerProfiles, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.playerAverages)) {
    if (isRankedMatch) {
      if (deferCumulativeProjectionWork) return 'cumulative-projections-pending';
      // `players.avg_*` powers the /players performance leaderboard. It is a
      // denormalized rollup, not a source fact, so it must be derived only after
      // authoritative match_player rows exist. The rollup service normally
      // requires match_ingest_status='complete'; this call passes the current
      // match id as an explicit exception because the worker has just written
      // the ten player facts but has not reached the final completion marker
      // yet. No other processing/partial match can leak into the calculation.
      await updatePlayerAveragesForMatches([actualMatchId]);
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.playerAverages, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.opponentFacts)) {
    if (isRankedMatch) {
      for (const player of normalized) {
        await insertMatchOpponents(actualMatchId, player, normalized);
      }
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.opponentFacts, ingestStages);
  }

  // Sync players to MeiliSearch for full-text search. Search sync is tracked as
  // a stage because it is an external side effect. A failure should retry, but a
  // later retry should not also re-run already completed DB aggregate stages.
  if (!ingestStages.has(MATCH_INGEST_STAGES.playerSearch)) {
    try {
      const playerIds = normalized.map((p: any) => p.player_id).filter((id: number) => id > 0);
      if (playerIds.length > 0) {
        const playerDocs = await query(`SELECT id, name, level, wins, losses, mastery_level, region, platform,
            kbm_tier, kbm_points, cheater, sus_count, portal_id, portal_user_id, first_seen, last_seen, last_updated
          FROM players WHERE id = ANY($1)`, [playerIds]);
        if (playerDocs.length > 0) {
          await bulkSyncPlayers(playerDocs);
        }
      }
      await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.playerSearch, ingestStages);
    } catch (err) {
      console.error(`[buffer-processor] MeiliSearch sync failed for match ${actualMatchId} players: ${err}`);
    }
  }

  // ----------------------------------------------------------------
  // Auto-populate skins and broken_skins from this batch.
  // Idempotent via INSERT ... ON CONFLICT DO NOTHING — safe to call
  // on every match regardless of whether it already exists. Removed
  // accidentally when wrapping the aggregate idempotency guard above;
  // restored here so skins continue populating from the live pipeline.
  // Source: User report 2026-06-01 — "autoPopulateSkins never called,
  //   skins and broken_skins tables not populated from live pipeline."
  // ----------------------------------------------------------------
  if (!ingestStages.has(MATCH_INGEST_STAGES.skinFacts)) {
    await autoPopulateSkins(normalized);
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.skinFacts, ingestStages);
  }

  // ----------------------------------------------------------------
  // Stage-based idempotency guard:
  // The tables below use incremental arithmetic:
  //   - computeTeamAggregates() → team_stats with count-based rollups
  //   - updatePlayerAveragesForMatches() → cumulative players.avg_* performance rollups
  //   - insertMatchBans() → bans_ranked via INSERT ... ON CONFLICT DO UPDATE SET count = count + 1
  //   - assignPartyNumbers() → player_relationships via count = count + 1
  //   - calculateRatingChanges()/applyRatingChanges() → rating tables with additive delta logic
  //   - upsertMatchCompositions() → match_compositions via count = count + 1, wins = wins + $X
  //   - upsertItemCounts() → item_counts_ranked via same pattern
  //   - upsertMapItemCounts() → map_item_counts_ranked via same pattern
  //   - upsertTalentCounts() → talent_counts_ranked via same pattern
  //   - upsertCardCounts() → card_counts_ranked via same pattern
  //   - upsertTalentCardCounts() → talent_card_counts_ranked via same pattern
  //   - upsertBansRanked() → bans_ranked via same pattern
  // Running these twice on the same match permanently inflates stats, but a
  // crash can happen after one stage and before the full match is complete.
  // `match_ingest_status.completed_stages` lets a retry resume after the last
  // completed stage instead of either replaying everything or skipping the
  // unfinished match forever.
  // autoPopulateSkins is safe (idempotent INSERT ... ON CONFLICT DO NOTHING).
  // ----------------------------------------------------------------
  if (!ingestStages.has(MATCH_INGEST_STAGES.teamAggregates)) {
    await computeTeamAggregates(actualMatchId);
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.teamAggregates, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.bans)) {
    // Ban fields are repeated on normal detail rows but may be present only on
    // recovery metadata after a broken getmatchdetailsbatch response. Scan the
    // complete normalized roster so a zero-filled first recovered player can
    // never hide valid ban slots carried by another row.
    if (isRankedMatch) {
      await upsertBansRanked(actualMatchId);
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.bans, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.partyRelationships)) {
    await assignPartyNumbers(actualMatchId, entryDatetime);
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.partyRelationships, ingestStages);
  }

  // Compute and apply ratings for ranked matches ONLY when we have a full match
  // (>=10 players). Ratings are a separate stage because replaying them changes
  // player/champion rating state. applyRatingChanges() also guards against an
  // existing snapshot for the same match as a second line of defense.
  if (!ingestStages.has(MATCH_INGEST_STAGES.ratings)) {
    if (isRankedMatch && normalized.length >= 10) {
      try {
        const ratingResult = await calculateAndApplyRatingChanges(actualMatchId);
        if (ratingResult === 'deferred') {
          console.warn(`[buffer-processor] Rating update deferred for match ${actualMatchId}; a chronological rebuild was requested`);
        } else if (ratingResult === 'busy') {
          throw new Error(
            `Rating stream busy for match ${actualMatchId}; durable projection retry scheduled`,
          );
        }
      } catch (err) {
        console.error(`[buffer-processor] Rating calc failed for match ${actualMatchId}: ${err}`);
        // Never mark the stage complete after a timeout or database failure.
        // The durable-row retry path applies bounded backoff and will resume
        // this exact stage without replaying already committed match facts.
        throw err;
      }
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.ratings, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.countProjections)) {
    if (isRankedMatch) {
      // Lobby tier is a ranked-only dimension. Casual mechanics projections
      // never write tier, Elo, ban, or ranked relationship state.
      await upsertMatchLobbyTier(actualMatchId);
      // This is the shared filter dimension for every aggregate. Do not mark
      // count projections complete if it cannot be written; a retry is safer
      // than permanently classifying downstream rows as unknown tier.
      try {
        await upsertMatchCompositions(actualMatchId);
      } catch (err) {
        console.error(`[buffer-processor] Composition update failed for match ${actualMatchId}: ${err}`);
      }
    }

    // Per-match item/card/talent facts are retained for every queue so lookup
    // pages can render the match. Their cumulative projections are queue-486
    // statistics and must never be mutated by a requested casual match.
    if (isRankedMatch) {
      // Unlike the older count tables, this projection has its own per-match
      // ledger. Let failures retry the stage; the atomic claim prevents a
      // replay from incrementing the map totals twice.
      await upsertMapItemCounts(actualMatchId);
      try {
        await upsertItemCounts(actualMatchId);
      } catch (err) {
        console.error(`[buffer-processor] Item counts update failed for match ${actualMatchId}: ${err}`);
      }
      try {
        await upsertTalentCounts(actualMatchId);
      } catch (err) {
        console.error(`[buffer-processor] Talent counts update failed for match ${actualMatchId}: ${err}`);
      }
      try {
        await upsertCardCounts(actualMatchId);
      } catch (err) {
        console.error(`[buffer-processor] Card counts update failed for match ${actualMatchId}: ${err}`);
      }
      try {
        await upsertTalentCardCounts(actualMatchId);
      } catch (err) {
        console.error(`[buffer-processor] Talent/card counts update failed for match ${actualMatchId}: ${err}`);
      }
      try {
        await upsertSkinCounts(actualMatchId);
      } catch (err) {
        console.error(`[buffer-processor] Skin counts update failed for match ${actualMatchId}: ${err}`);
      }
    } else if (!isLimited) {
      // The item pilot reads the same canonical per-match facts as Ranked but
      // owns a physically separate, classified aggregate and idempotency
      // ledger. A failure aborts this stage so a retry cannot lose the match.
      await upsertCasualItemProjectionForMatch(actualMatchId);
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.countProjections, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.rankedStats)) {
    if (isRankedMatch) {
      try { await upsertChampionStatsRanked(actualMatchId); } catch (err) { console.error(`[buffer-processor] Champion stats update failed for match ${actualMatchId}: ${err}`); }
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.rankedStats, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.performanceProjections)) {
    if (isRankedMatch) {
      if (deferCumulativeProjectionWork) return 'cumulative-projections-pending';
      await upsertPerformanceProjectionsForMatch(actualMatchId);
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.performanceProjections, ingestStages);
  }

  if (!ingestStages.has(MATCH_INGEST_STAGES.scalableStats)) {
    if (isRankedMatch) {
      if (deferCumulativeProjectionWork) return 'cumulative-projections-pending';
      // Match facts are queue-neutral, but aggregate statistics are not:
      // queue 486 is the only PaladinsCat statistical population. Custom and
      // casual matches remain visible on match/player pages without entering
      // any global champion, item, talent, card, composition, or metric model.
      await upsertScalableStatsProjectionsForMatch(actualMatchId);
    }
    await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.scalableStats, ingestStages);
  }

  // Sync match to MeiliSearch for full-text search
  if (!ingestStages.has(MATCH_INGEST_STAGES.matchSearch)) {
    try {
      const matchDoc = await one(`SELECT m.match_id, m.entry_datetime, m.map, m.queue_id, m.duration_seconds, m.region,
          m.team1_score, m.team2_score, m.winning_task_force, m.is_ranked, m.recovered, m.broken, m.surrendered,
          m.source, m.match_level,
          (SELECT json_agg(json_build_object('player_id', mp.player_id, 'player_name', mp.player_name,
            -- champions stores the display label as name; several lookup
            -- tables expose derived champion_name columns, but the canonical
            -- reference table does not. Use the canonical column here so match
            -- search sync works on clean databases seeded only with champions.
            'champion_id', mp.champion_id, 'champion_name', c.name,
            'win_status', mp.win_status, 'kills', mp.kills, 'deaths', mp.deaths, 'assists', mp.assists,
            'task_force', mp.task_force))
          FROM match_players mp LEFT JOIN champions c ON c.id = mp.champion_id
          WHERE mp.match_id = m.match_id) as players
        FROM matches m WHERE m.match_id = $1`, [actualMatchId]);
      if (matchDoc) {
        await syncMatch(actualMatchId, matchDoc);
      }
      await markMatchIngestStage(actualMatchId, MATCH_INGEST_STAGES.matchSearch, ingestStages);
    } catch (err) {
      console.error(`[buffer-processor] MeiliSearch sync failed for match ${actualMatchId}: ${err}`);
    }
  }

  await markMatchIngestComplete(actualMatchId, ingestStages);
  return 'complete';
}

// â"€â"€â"€ Player Profile Payload Processor â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€

async function processPlayerPayload(rawData: any[], endpoint: string): Promise<void> {
  // getplayer / getplayerbatch: array of player profile objects
  const profiles = rawData.filter((p: any) => !(p.ret_msg || '').trim());
  if (profiles.length === 0) return;

  const profileIds: number[] = [];
  for (const raw of profiles) {
    const profile = normalizePlayerProfile(raw);
    await upsertPlayerProfile(profile);
    if (profile.player_id > 0) profileIds.push(profile.player_id);
  }

  // Sync players to MeiliSearch for full-text search
  try {
    const playerDocs = await query(`SELECT id, name, level, wins, losses, mastery_level, region, platform,
        kbm_tier, kbm_points, cheater, sus_count, portal_id, portal_user_id, first_seen, last_seen, last_updated
      FROM players WHERE id = ANY($1)`, [profileIds]);
    if (playerDocs.length > 0) {
      await bulkSyncPlayers(playerDocs);
    }
  } catch (err) {
    console.error(`[buffer-processor] MeiliSearch sync failed for players: ${err}`);
  }
}

// â"€â"€â"€ Champion Payload Processor â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€

async function processChampionPayload(rawData: any[], endpoint: string): Promise<void> {
  // getchampions / getgods: array of champion objects
  const champions = rawData.filter((c: any) => !(c.ret_msg || '').trim());
  if (champions.length === 0) return;

  for (const raw of champions) {
    const champion = normalizeChampion(raw);
      await upsertChampion(champion);
  }
}

// â"€â"€â"€ Item Payload Processor â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€

async function processItemPayload(rawData: any[], endpoint: string): Promise<void> {
  // getitems: array of item objects
  const items = rawData.filter((i: any) => !(i.ret_msg || '').trim());
  if (items.length === 0) return;

  for (const raw of items) {
    const item = normalizeItem(raw);
      await upsertItem(item);
  }
}

// â"€â"€â"€ Leaderboard Payload Processor â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€

async function processLeaderboardPayload(rawData: any[], endpoint: string): Promise<void> {
  // getchampionleaderboard: array of leaderboard entries
  const entries = rawData.filter((e: any) => !(e.ret_msg || '').trim());
  if (entries.length === 0) return;

  for (const raw of entries) {
    const entry = normalizeLeaderboardEntry(raw);
    await upsertLeaderboardEntry(entry);
  }
}

// â"€â"€â"€ Loadout Payload Processor â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€

async function processLoadoutPayload(rawData: any[], endpoint: string): Promise<void> {
  // getplayerloadouts: array of loadout objects
  const loadouts = rawData.filter((l: any) => !(l.ret_msg || '').trim());
  if (loadouts.length === 0) return;

  const playerIds = new Set<number>();
  for (const raw of loadouts) {
    const loadout = normalizeLoadout(raw);
    if (loadout.player_id <= 0 || loadout.champion_id <= 0 || !loadout.deck_name) continue;
    playerIds.add(loadout.player_id);
    await upsertLoadout(loadout);
  }
  // Background fetches should satisfy the same 24-hour web cache as a direct
  // page visit. (Empty successful responses are recorded by the synchronous
  // player route, which has the requested player id outside the payload.)
  for (const playerId of playerIds) {
    await one(`INSERT INTO player_loadout_fetches (player_id, fetched_at)
      VALUES ($1, now())
      ON CONFLICT (player_id) DO UPDATE SET fetched_at = now()`, [playerId]);
  }
}

/**
 * Normalize a player object from the flat API response.
 * Uses endpoint-aware detection to apply correct field mapping.
 */
function normalizePlayer(raw: any): PlayerDetails {
  return normalizeMatchPlayer(raw);
}

export async function upsertPlayer(
  player: any,
  options: { includeRankedSeed?: boolean } = {},
): Promise<void> {
  // Match ingest is not the authoritative profile refresh path, but ranked
  // match detail payloads do carry the player's tier at match time. Without
  // this narrow seed, players discovered only through hourly ingest remain
  // kbm_tier=0 until someone explicitly looks up their profile, and public
  // tier distribution pages look much worse than the match facts we already
  // paid to fetch. The upsert below only fills a missing profile tier; it does
  // not overwrite a tier already supplied by getplayer/getplayerbatch.
  const observedLeagueTier = Number(player.league_tier || 0);
  const observedLeaguePoints = Number(player.league_points || 0);
  const kbmTierFromMatchFact =
    options.includeRankedSeed === true
    && Number.isFinite(observedLeagueTier) && observedLeagueTier >= 1 && observedLeagueTier <= 26
      ? observedLeagueTier
      : 0;
  const kbmPointsFromMatchFact = options.includeRankedSeed === true
    && Number.isFinite(observedLeaguePoints) && observedLeaguePoints > 0
    ? observedLeaguePoints
    : 0;

  await one(`INSERT INTO players (id, name, level, wins, losses, mastery_level, region, platform, portal_id, portal_user_id,
      kbm_tier, kbm_points, first_seen, last_seen, last_updated, name_source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(), now(), now(), 'match_player')
    ON CONFLICT (id) DO UPDATE SET
      name = $2,
      name_source = 'match_player',
      kbm_tier = CASE
        WHEN COALESCE(players.kbm_tier, 0) = 0 AND EXCLUDED.kbm_tier BETWEEN 1 AND 26
          THEN EXCLUDED.kbm_tier
        ELSE players.kbm_tier
      END,
      kbm_points = CASE
        WHEN COALESCE(players.kbm_tier, 0) = 0 AND EXCLUDED.kbm_tier BETWEEN 1 AND 26
          THEN EXCLUDED.kbm_points
        ELSE players.kbm_points
      END,
      last_seen = now()`,
    [player.player_id, player.player_name, player.account_level, 0, 0, player.mastery_level,
      player.region, player.platform || null, player.portal_id, player.portal_user_id || null,
      kbmTierFromMatchFact, kbmPointsFromMatchFact]);

  if (player.merged_players && Array.isArray(player.merged_players) && player.merged_players.length > 0) {
    for (const merged of player.merged_players) {
      const mergedFromId = Number(merged.player_id ?? merged.playerId ?? 0);
      if (!mergedFromId) continue;
      await one(`INSERT INTO player_account_merges (player_id, merged_from_id, merged_from_portal, merge_datetime)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (player_id, merged_from_id) DO NOTHING`,
        [
          player.player_id,
          mergedFromId,
          Number(merged.portal_id ?? merged.portalId) || null,
          merged.merge_datetime || new Date().toISOString(),
        ]);
    }
  }
}

export async function insertMatchPlayer(
  matchId: number,
  player: any,
  entryDatetime: string,
  matchDurationSeconds: number = 0,
  privatePlayerId: number | null = null,
  privateSlot = 0,
): Promise<void> {
  await ensureIngestControlTables();
  const normalizedChampionId = Number(player.champion_id);
  const championIdForInsert = Number.isFinite(normalizedChampionId) && normalizedChampionId > 0
    ? normalizedChampionId
    : null;
  await ensureChampionReference(championIdForInsert, player.champion_name || player.Champion || player.ChampionName);
  const rawSource = String(player.source || '').trim().toLowerCase();
  if (['prefetch', 'match_history', 'history_observation', 'legacy_prefetch'].includes(rawSource)) {
    throw new Error(
      `History observation ${matchId}:${player.player_id} cannot be inserted into match_players; ` +
      `route it through player_match_history_entries instead.`,
    );
  }
  const playerSource = normalizeMatchPlayerSource(player.source, 'direct');
  const incomingPriority = matchPlayerSourcePriority(playerSource);

  // match_players is keyed by (match_id, player_id, entry_datetime) for the
  // Timescale-compatible table shape, but gameplay truth is one row per
  // match/player. A getmatchhistory prefetch row can carry a slightly different
  // timestamp than a later getmatchdetailsbatch/recovery row; without this
  // authority check both rows can survive and casual/ranked lookups see mixed
  // partial/full data. Higher-authority rows always win across timestamp
  // variants, while same-authority full-detail rows may refresh themselves.
  if (await hasHigherAuthorityMatchPlayerRow(matchId, player.player_id, privateSlot, incomingPriority)) {
    console.log(`[buffer-processor] Skipping ${playerSource} match_players row for ${matchId}:${player.player_id}; higher-authority row already exists`);
    return;
  }

  const rawKda = calculateKda(player.kills, player.deaths, player.assists);
  const kda = rawKda != null && !Number.isNaN(rawKda) ? Math.max(0, roundTo2(rawKda)) : 0;
  // Match_Duration/getdemodetails Match_Time measures actual gameplay. The
  // player API timer can include loading/waiting overhead, so it remains raw
  // evidence and is never used as the denominator.
  const duration = resolveGameplayDuration(matchDurationSeconds || player.match_duration);
  const dpm = calculatePerMinute(player.damage_done_physical, duration);
  const hpm = calculatePerMinute(player.healing, duration);
  const shpm = calculatePerMinute(player.healing_self, duration);
  const mpm = calculatePerMinute(player.damage_mitigated || 0, duration);
  const { cpm, ecpm } = calculateCreditRates(player.gold_earned, duration);

  await one(`INSERT INTO match_players (match_id, player_id, private_slot, player_name, region, champion_id, skin_id, skin_name,
      kills, deaths, assists, damage_done_in_hand, damage_done_physical, damage_done_magical, damage_taken,
      damage_taken_physical, damage_taken_magical, damage_mitigated, healing, healing_self, healing_bot, healing_player_self,
      gold_earned, gold_per_minute, objective_assists, camps_cleared, structure_damage, wards_placed, towers_destroyed,
      distance_traveled, multi_kill_max, killing_spree, kills_first_blood, kills_double, kills_triple, kills_quadra, kills_penta,
      kills_fire_giant, kills_gold_fury, kills_phoenix, kills_siege_jugg, kills_wild_jugg,
      win_status, task_force, league_tier, league_points, league_wins, league_losses, account_level, mastery_level, party_id,
      kda, damage_per_minute, healing_per_minute, healing_self_per_minute, time_in_match, entry_datetime, source, portal_id, is_ranked,
      afk_rate, egpm, mitigation_per_minute, private_player_id, portal_user_id, kills_player, created_at,
      platform, damage_bot, kills_single, kills_bot, final_match_level, rank_stat_league, team_id, surrendered, has_ret_msg)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49,$50,$51,$52,$53,$54,$55,$56,$57,$58,$59,$60,$61,$62,$63,$64,$65,$66,$67,$68,$69,$70,$71,$72,$73,$74,$75,$76)
    -- Match-player idempotency must target the actual table key. The schema's
    -- primary key includes entry_datetime so it can remain compatible with the
    -- documented time-partition/hypertable shape. Using only
    -- (match_id, player_id) has no matching unique constraint on fresh/local
    -- DBs and fails before item/card/talent math can run.
    ON CONFLICT (match_id, player_id, private_slot, entry_datetime) DO UPDATE SET
      player_name = EXCLUDED.player_name, region = EXCLUDED.region,
      champion_id = EXCLUDED.champion_id, skin_id = EXCLUDED.skin_id, skin_name = EXCLUDED.skin_name,
      kills = EXCLUDED.kills, deaths = EXCLUDED.deaths, assists = EXCLUDED.assists,
      damage_done_in_hand = EXCLUDED.damage_done_in_hand,
      damage_done_physical = EXCLUDED.damage_done_physical,
      damage_done_magical = EXCLUDED.damage_done_magical,
      damage_taken = EXCLUDED.damage_taken,
      damage_taken_physical = EXCLUDED.damage_taken_physical,
      damage_taken_magical = EXCLUDED.damage_taken_magical,
      damage_mitigated = EXCLUDED.damage_mitigated,
      healing = EXCLUDED.healing, healing_self = EXCLUDED.healing_self,
      healing_bot = EXCLUDED.healing_bot, healing_player_self = EXCLUDED.healing_player_self,
      gold_earned = EXCLUDED.gold_earned, gold_per_minute = EXCLUDED.gold_per_minute,
      objective_assists = EXCLUDED.objective_assists, camps_cleared = EXCLUDED.camps_cleared,
      structure_damage = EXCLUDED.structure_damage, wards_placed = EXCLUDED.wards_placed,
      towers_destroyed = EXCLUDED.towers_destroyed, distance_traveled = EXCLUDED.distance_traveled,
      multi_kill_max = EXCLUDED.multi_kill_max, killing_spree = EXCLUDED.killing_spree,
      kills_first_blood = EXCLUDED.kills_first_blood, kills_double = EXCLUDED.kills_double,
      kills_triple = EXCLUDED.kills_triple, kills_quadra = EXCLUDED.kills_quadra,
      kills_penta = EXCLUDED.kills_penta, kills_fire_giant = EXCLUDED.kills_fire_giant,
      kills_gold_fury = EXCLUDED.kills_gold_fury, kills_phoenix = EXCLUDED.kills_phoenix,
      kills_siege_jugg = EXCLUDED.kills_siege_jugg, kills_wild_jugg = EXCLUDED.kills_wild_jugg,
      win_status = EXCLUDED.win_status, task_force = EXCLUDED.task_force,
      league_tier = EXCLUDED.league_tier, league_points = EXCLUDED.league_points,
      league_wins = EXCLUDED.league_wins, league_losses = EXCLUDED.league_losses,
      account_level = EXCLUDED.account_level, mastery_level = EXCLUDED.mastery_level,
      party_id = EXCLUDED.party_id, time_in_match = EXCLUDED.time_in_match,
      portal_id = EXCLUDED.portal_id, portal_user_id = EXCLUDED.portal_user_id,
      platform = EXCLUDED.platform, private_player_id = COALESCE(EXCLUDED.private_player_id, match_players.private_player_id),
      afk_rate = EXCLUDED.afk_rate, egpm = EXCLUDED.egpm, mitigation_per_minute = EXCLUDED.mitigation_per_minute,
      kda = EXCLUDED.kda, damage_per_minute = EXCLUDED.damage_per_minute,
      healing_per_minute = EXCLUDED.healing_per_minute, healing_self_per_minute = EXCLUDED.healing_self_per_minute,
      source = EXCLUDED.source, created_at = EXCLUDED.created_at
    WHERE ${matchPlayerSourcePrioritySql('EXCLUDED.source')} >= ${matchPlayerSourcePrioritySql('match_players.source')}`,
    [matchId, player.player_id, privateSlot, player.player_name, normalizeRegion(player.region || ''), championIdForInsert, player.skin_id, player.skin_name,
      player.kills, player.deaths, player.assists, player.damage_done_in_hand, player.damage_done_physical, player.damage_done_magical,
      player.damage_taken, player.damage_taken_physical, player.damage_taken_magical, player.damage_mitigated,
      player.healing, player.healing_self, player.healing_bot, player.healing_player_self,
      player.gold_earned, cpm, player.objective_assists, player.camps_cleared, player.structure_damage,
      player.wards_placed, player.towers_destroyed, player.distance_traveled, player.multi_kill_max, player.killing_spree,
      player.kills_first_blood, player.kills_double, player.kills_triple, player.kills_quadra, player.kills_penta,
      player.kills_fire_giant, player.kills_gold_fury, player.kills_phoenix, player.kills_siege_jugg, player.kills_wild_jugg,
      player.win_status, player.task_force, player.league_tier, player.league_points, player.league_wins,
      player.league_losses, player.account_level, player.mastery_level, player.party_id, kda, dpm, hpm, shpm, player.time_in_match,
      entryDatetime, playerSource, player.portal_id, isRankedStatsQueue(player.queue_id),
      calculateAfkRate(player.gold_earned, duration), ecpm, mpm, privatePlayerId,
      player.portal_user_id || null, player.kills_player, new Date().toISOString(),
      player.platform || null, player.damage_bot || 0, player.kills_single || 0,
      player.kills_bot || 0, player.final_match_level || 0, player.rank_stat_league || 0, player.team_id || null, Boolean(player.surrendered), Boolean(player.has_ret_msg)]);

  await pruneSupersededMatchPlayerRows(matchId, player.player_id, privateSlot, entryDatetime, incomingPriority);
}

function normalizeMatchPlayerSource(source: any, fallback: string): string {
  const normalized = String(source || fallback || '').trim().toLowerCase();
  // match_players now stores authoritative match facts only. getmatchhistory
  // rows are observations in player_match_history_entries, so any history-ish
  // source that reaches this function is a legacy/mislabeled payload and must
  // not gain authority inside match_players.
  if (['prefetch', 'match_history', 'history_observation', 'legacy_prefetch'].includes(normalized)) return 'minimal';
  if (['direct', 'recovered', 'minimal'].includes(normalized)) return normalized;
  return fallback;
}

function matchPlayerSourcePriority(source: any): number {
  switch (normalizeMatchPlayerSource(source, 'minimal')) {
    case 'direct': return 4;
    case 'recovered': return 3;
    case 'minimal': return 1;
    default: return 0;
  }
}

function matchPlayerSourcePrioritySql(sourceExpression: string): string {
  return `(CASE COALESCE(${sourceExpression}, '')
    WHEN 'direct' THEN 4
    WHEN 'recovered' THEN 3
    WHEN 'minimal' THEN 1
    ELSE 0
  END)`;
}

async function hasHigherAuthorityMatchPlayerRow(matchId: number, playerId: any, privateSlot: number, incomingPriority: number): Promise<boolean> {
  const normalizedPlayerId = Number(playerId || 0);
  if (!matchId || (normalizedPlayerId === 0 && privateSlot <= 0) || incomingPriority <= 0) return false;
  const rows = await query(
    `SELECT 1
     FROM match_players
     WHERE match_id = $1
       AND player_id = $2
        AND private_slot = $3
        AND ${matchPlayerSourcePrioritySql('source')} > $4
      LIMIT 1`,
    [matchId, normalizedPlayerId, privateSlot, incomingPriority],
  );
  return rows.length > 0;
}

async function pruneSupersededMatchPlayerRows(
  matchId: number,
  playerId: any,
  privateSlot: number,
  entryDatetime: string,
  incomingPriority: number,
): Promise<void> {
  const normalizedPlayerId = Number(playerId || 0);
  if (!matchId || (normalizedPlayerId === 0 && privateSlot <= 0) || !entryDatetime || incomingPriority <= 0) return;

  await query(
    `DELETE FROM match_players
     WHERE match_id = $1
       AND player_id = $2
        AND private_slot = $3
        AND entry_datetime IS DISTINCT FROM $4::timestamptz
        AND ${matchPlayerSourcePrioritySql('source')} <= $5`,
    [matchId, normalizedPlayerId, privateSlot, entryDatetime, incomingPriority],
  );
}

const championReferenceCache = new Set<number>();
const itemReferenceCache = new Set<number>();
const talentReferenceCache = new Set<number>();

async function ensureChampionReference(championId: any, championName?: any): Promise<void> {
  const id = Number(championId);
  if (!Number.isFinite(id) || id <= 0 || championReferenceCache.has(id)) return;

  const suppliedName = String(championName || '').trim();
  const name = suppliedName || `Champion ${id}`;

  // Real match payloads can arrive before a full getchampions reference refresh.
  // Since match_players, bans, opponents, talents, and projections FK into
  // champions, a clean real-mode DB must create a small placeholder instead of
  // depending on dummy seeds. Later getchampions payloads overwrite the static
  // metadata through upsertChampion(); this helper only prevents FK failures on
  // the ingest hot path.
  await one(`INSERT INTO champions (id, name, title, health, speed, roles)
    VALUES ($1, $2, 'Reference placeholder from match ingest', 0, 0, 'Unknown')
    ON CONFLICT (id) DO UPDATE SET
      name = CASE
        WHEN champions.title = 'Reference placeholder from match ingest'
         AND EXCLUDED.name NOT LIKE 'Champion %'
        THEN EXCLUDED.name
        ELSE champions.name
      END`,
    [id, name]);
  championReferenceCache.add(id);
}

async function ensureItemReference(itemId: any, itemName?: any): Promise<void> {
  const id = Number(itemId);
  if (!Number.isFinite(id) || id <= 0 || itemReferenceCache.has(id)) return;

  const suppliedName = String(itemName || '').trim();
  const name = suppliedName || `Item ${id}`;

  await one(`INSERT INTO items (item_id, item_name, description, item_type, cost)
    VALUES ($1, $2, 'Reference placeholder from match ingest', 'Match item placeholder', 0)
    ON CONFLICT (item_id) DO UPDATE SET
      item_name = CASE
        WHEN items.description = 'Reference placeholder from match ingest'
         AND EXCLUDED.item_name NOT LIKE 'Item %'
        THEN EXCLUDED.item_name
        ELSE items.item_name
      END`,
    [id, name]);
  itemReferenceCache.add(id);
}

async function ensureTalentReference(talentId: any, talentName: any, championId: any): Promise<void> {
  const id = Number(talentId);
  if (!Number.isFinite(id) || id <= 0 || talentReferenceCache.has(id)) return;

  const champId = Number(championId);
  if (Number.isFinite(champId) && champId > 0) {
    await ensureChampionReference(champId);
  }

  const suppliedName = String(talentName || '').trim();
  const name = suppliedName || `Talent ${id}`;

  await one(`INSERT INTO talents (talent_id, talent_name, champion_id)
    VALUES ($1, $2, $3)
    ON CONFLICT (talent_id) DO UPDATE SET
      talent_name = CASE
        WHEN talents.talent_name LIKE 'Talent %'
         AND EXCLUDED.talent_name NOT LIKE 'Talent %'
        THEN EXCLUDED.talent_name
        ELSE talents.talent_name
      END,
      champion_id = COALESCE(talents.champion_id, EXCLUDED.champion_id)`,
    [id, name, Number.isFinite(champId) && champId > 0 ? champId : null]);
  talentReferenceCache.add(id);
}

export async function insertMatchPlayerItems(matchId: number, player: any): Promise<void> {
  for (let i = 1; i <= 4; i++) {
    const itemId = player[`active_id_${i}`];
    if (itemId && itemId > 0) {
      let level = player[`active_level_${i}`] || 0;
      if (level > 2) {
        level = Math.floor(level / 4);
      }
      await ensureItemReference(itemId, player[`item_active_${i}`]);
      await one(`INSERT INTO match_player_items (match_id, player_id, item_id, slot, item_level)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (match_id, player_id, item_id) DO NOTHING`,
        [matchId, player.player_id, itemId, i, level]);
    }
  }
}

export async function insertMatchPlayerCards(matchId: number, player: any): Promise<void> {
  for (let i = 1; i <= 5; i++) {
    const cardId = player[`item_id_${i}`];
    if (cardId && cardId > 0) {
      await one(`INSERT INTO match_player_cards (match_id, player_id, card_id, card_level)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (match_id, player_id, card_id) DO NOTHING`,
        [matchId, player.player_id, cardId, player[`item_level_${i}`] || 0]);
    }
  }
}

export async function insertMatchPlayerTalents(matchId: number, player: any): Promise<void> {
  const talentId = player.item_id_6;
  if (talentId && talentId > 0) {
    await ensureTalentReference(talentId, player.item_purch_6, player.champion_id);
    await one(`INSERT INTO match_player_talents (match_id, player_id, talent_id)
      SELECT $1, $2, $3
      WHERE EXISTS (
        SELECT 1 FROM talents
        WHERE talent_id = $3 AND champion_id = $4
      )
      ON CONFLICT (match_id, player_id, talent_id) DO NOTHING`,
      [matchId, player.player_id, talentId, player.champion_id]);
  }
}

export async function insertMatchOpponents(matchId: number, player: any, allPlayers: any[]): Promise<void> {
  // Opponent history is identity-based. Private participants can still count as
  // an opponent champion for identified players, but must not create a
  // cumulative anonymous player_id=0 profile of their own.
  if (Number(player.player_id || 0) <= 0 || !Number.isFinite(Number(player.champion_id)) || Number(player.champion_id) <= 0) return;

  const opponents = allPlayers.filter((p: any) => p.task_force !== player.task_force);
  const playerWon = isNormalizedWinStatus(player.win_status);

  // Group opponents by champion to aggregate per-champion wins/losses
  const championCounts = new Map<number, number>();
  for (const opp of opponents) {
    if (opp.champion_id) {
      championCounts.set(opp.champion_id, (championCounts.get(opp.champion_id) || 0) + 1);
    }
  }

  for (const [oppChampionId, count] of championCounts) {
    await ensureChampionReference(player.champion_id, player.champion_name);
    await ensureChampionReference(oppChampionId);
    await one(`
      WITH inserted_fact AS (
        INSERT INTO match_opponent_facts (
          match_id, player_id, player_champion_id, opponent_champion_id, wins, losses
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (match_id, player_id, player_champion_id, opponent_champion_id) DO NOTHING
        RETURNING player_id, player_champion_id, opponent_champion_id, wins, losses
      )
      INSERT INTO match_opponents (player_id, player_champion_id, opponent_champion_id, wins, losses)
      SELECT player_id, player_champion_id, opponent_champion_id, wins, losses
      FROM inserted_fact
      ON CONFLICT (player_id, player_champion_id, opponent_champion_id)
      DO UPDATE SET wins = match_opponents.wins + EXCLUDED.wins,
                    losses = match_opponents.losses + EXCLUDED.losses
    `, [
      matchId,
      player.player_id,
      player.champion_id,
      oppChampionId,
      playerWon ? count : 0,
      playerWon ? 0 : count
    ]);
  }
}

export async function insertMatchBans(matchId: number, playerOrPlayers: any | any[]): Promise<void> {
  for (const { banSlot, championId } of matchBanEntries(playerOrPlayers)) {
    await ensureChampionReference(championId);
    await one(`INSERT INTO match_bans (match_id, ban_slot, champion_id)
      VALUES ($1, $2, $3)
      ON CONFLICT (match_id, ban_slot) DO UPDATE
      SET champion_id = EXCLUDED.champion_id`,
      [matchId, banSlot, championId]);
  }
}

export async function handlePrivatePlayer(
  matchId: number,
  privateSlot: number,
  player: any,
  entryDatetime: string,
  context: Parameters<typeof recordPrivateAccountObservation>[4] = {},
): Promise<number | null> {
  if (!isPrivateAccountParticipant(player)) return null;
  return recordPrivateAccountObservation(matchId, privateSlot, player, entryDatetime, context);
}

async function upsertRankedPlayerPresence(
  matchId: number,
  entryDatetime: string,
): Promise<void> {
  await one(
    `INSERT INTO player_presence_24h (
       player_id, first_observed_at, last_observed_at, last_match_id,
       last_queue_id, last_stats_scope
     )
     SELECT DISTINCT ON (mp.player_id)
       mp.player_id,$2::timestamptz,$2::timestamptz,$1,486,'ranked'
     FROM match_players mp
     WHERE mp.match_id=$1
       AND mp.player_id>0
       AND COALESCE(mp.source,'direct') IN ('direct','recovered')
     ORDER BY mp.player_id
     ON CONFLICT (player_id) DO UPDATE SET
       first_observed_at=LEAST(player_presence_24h.first_observed_at,EXCLUDED.first_observed_at),
       last_observed_at=GREATEST(player_presence_24h.last_observed_at,EXCLUDED.last_observed_at),
       last_match_id=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_match_id ELSE player_presence_24h.last_match_id END,
       last_queue_id=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_queue_id ELSE player_presence_24h.last_queue_id END,
       last_stats_scope=CASE WHEN EXCLUDED.last_observed_at>=player_presence_24h.last_observed_at THEN EXCLUDED.last_stats_scope ELSE player_presence_24h.last_stats_scope END,
       updated_at=now()`,
    [matchId, entryDatetime],
  );
  await one(
    `INSERT INTO player_queue_presence_24h (
       player_id,queue_id,stats_scope,first_observed_at,last_observed_at,last_match_id
     )
     SELECT DISTINCT ON (mp.player_id)
       mp.player_id,486,'ranked',$2::timestamptz,$2::timestamptz,$1
     FROM match_players mp
     WHERE mp.match_id=$1
       AND mp.player_id>0
       AND COALESCE(mp.source,'direct') IN ('direct','recovered')
     ORDER BY mp.player_id
     ON CONFLICT (player_id,queue_id) DO UPDATE SET
       first_observed_at=LEAST(player_queue_presence_24h.first_observed_at,EXCLUDED.first_observed_at),
       last_observed_at=GREATEST(player_queue_presence_24h.last_observed_at,EXCLUDED.last_observed_at),
       last_match_id=CASE
         WHEN EXCLUDED.last_observed_at>=player_queue_presence_24h.last_observed_at
           THEN EXCLUDED.last_match_id
         ELSE player_queue_presence_24h.last_match_id
       END,
       stats_scope=EXCLUDED.stats_scope,
       updated_at=now()`,
    [matchId, entryDatetime],
  );
}

export async function computeTeamAggregates(matchId: number): Promise<void> {
  // Private placeholders preserve roster completeness but have deliberately
  // zeroed stats. Exclude them from sums and average denominators so the known
  // players' performance is aggregated exactly as observed.
  const players = await query(`
    SELECT *
    FROM match_players
    WHERE match_id = $1
      AND champion_id > 0
      AND COALESCE(source, 'direct') IN ('direct', 'recovered')
  `, [matchId]);
  const team1 = players.filter((p: any) => p.task_force === 1);
  const team2 = players.filter((p: any) => p.task_force === 2);

  const calcTeam = (team: any[]) => ({
    gold: team.reduce((s, p) => s + (p.gold_earned || 0), 0),
    // The column name `damage_done_physical` is historical and misleading:
    // it contains the API's total player damage. In-hand and magical values
    // are optional breakdown fields and must not be added to that total.
    damage: team.reduce((s, p) => s + (p.damage_done_physical || 0), 0),
    healing: team.reduce((s, p) => s + (p.healing || 0) + (p.healing_self || 0), 0),
    avgKills: team.length ? team.reduce((s, p) => s + (p.kills || 0), 0) / team.length : 0,
    avgDeaths: team.length ? team.reduce((s, p) => s + (p.deaths || 0), 0) / team.length : 0,
  });

  const t1 = calcTeam(team1);
  const t2 = calcTeam(team2);
  const winningTeam = players.find((p: any) => isNormalizedWinStatus(p.win_status))?.task_force;

  await one(`UPDATE matches SET team1_total_gold = $1, team2_total_gold = $2, team1_total_damage = $3, team2_total_damage = $4,
    team1_total_healing = $5, team2_total_healing = $6, team1_avg_kills = $7, team2_avg_kills = $8,
    team1_avg_deaths = $9, team2_avg_deaths = $10,
    winning_task_force = COALESCE(winning_task_force, $11)
    WHERE match_id = $12`,
    [t1.gold, t2.gold, t1.damage, t2.damage, t1.healing, t2.healing,
      Math.round(t1.avgKills * 10) / 10, Math.round(t2.avgKills * 10) / 10,
      Math.round(t1.avgDeaths * 10) / 10, Math.round(t2.avgDeaths * 10) / 10,
      winningTeam, matchId]);
}

// â"€â"€â"€ Non-match upsert helpers â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€â"€

export async function upsertPlayerProfile(profile: any): Promise<void> {
  await persistPlayerProfile(profile);
}

async function upsertChampion(champion: NormalizedChampion): Promise<void> {
  await one(`INSERT INTO champions (id, name, title, health, speed, roles,
      ability1_id, ability1_name, ability1_type, ability1_description,
      ability2_id, ability2_name, ability2_type, ability2_description,
      ability3_id, ability3_name, ability3_type, ability3_description,
      ability4_id, ability4_name, ability4_type, ability4_description,
      ability5_id, ability5_name, ability5_type, ability5_description)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)
    ON CONFLICT (id) DO UPDATE SET
      name = $2, title = $3, health = $4, speed = $5, roles = $6,
      ability1_id = $7, ability1_name = $8, ability1_type = $9, ability1_description = $10,
      ability2_id = $11, ability2_name = $12, ability2_type = $13, ability2_description = $14,
      ability3_id = $15, ability3_name = $16, ability3_type = $17, ability3_description = $18,
      ability4_id = $19, ability4_name = $20, ability4_type = $21, ability4_description = $22,
      ability5_id = $23, ability5_name = $24, ability5_type = $25, ability5_description = $26`,
    [champion.id, champion.name, champion.title, champion.health, champion.speed, champion.roles,
      champion.ability1_id, champion.ability1_name, champion.ability1_type, champion.ability1_description,
      champion.ability2_id, champion.ability2_name, champion.ability2_type, champion.ability2_description,
      champion.ability3_id, champion.ability3_name, champion.ability3_type, champion.ability3_description,
      champion.ability4_id, champion.ability4_name, champion.ability4_type, champion.ability4_description,
      champion.ability5_id, champion.ability5_name, champion.ability5_type, champion.ability5_description]);
}

async function upsertItem(item: NormalizedItem): Promise<void> {
  await one(`INSERT INTO items (item_id, item_name, description, item_type, cost, icon_url)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (item_id) DO UPDATE SET item_name = $2, description = $3, item_type = $4, cost = $5, icon_url = $6`,
    [item.id, item.name, item.description, item.type, item.cost, item.icon_url]);
}

async function upsertLeaderboardEntry(entry: any): Promise<void> {
  await one(`INSERT INTO champion_match_ratings (champion_id, player_id, rank, player_ranking, wins, losses)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (champion_id, player_id) DO UPDATE SET rank = $3, player_ranking = $4, wins = $5, losses = $6`,
    [entry.champion_id, entry.player_id, entry.rank, entry.player_ranking, entry.wins, entry.losses]);
}

async function upsertLoadout(loadout: any): Promise<void> {
  // Store each saved deck independently. A champion can have up to nine decks,
  // so using player_id + champion_id here would overwrite the previous one.
  // cards table is a static game-card reference table — do NOT write player data there.
  const cardIds = loadout.cards.map((c: any) => c.item_id);
  const cardLevels = loadout.cards.map((c: any) => c.points);
  const deckId = Number(loadout.deck_id) || 0;
  const deckKey = deckId > 0
    ? `id:${deckId}`
    : `legacy:${loadout.champion_id}:${String(loadout.deck_name ?? '').toLowerCase().replace(/\s+/g, ' ').slice(0, 80)}`;

  await one(`INSERT INTO player_loadouts (player_id, champion_id, deck_id, deck_key, loadout_name, card_ids, card_levels, talent_id, fetched_at, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, now(), now())
    ON CONFLICT (player_id, deck_key) DO UPDATE SET
      champion_id = $2, deck_id = $3, loadout_name = $5, card_ids = $6, card_levels = $7, fetched_at = now(), updated_at = now()`,
    [loadout.player_id, loadout.champion_id, deckId || null, deckKey, loadout.deck_name, cardIds, cardLevels]);
}

function isHistoryOnlyPayload(rows: any[]): boolean {
  if (!Array.isArray(rows) || rows.length === 0) return false;

  // Source fence for the 2026-06-18 queue-0 incident:
  // Normalized getmatchhistory prefetch rows can arrive with a misleading
  // entity_type from an old buffer row or a future staging bug. These rows are
  // partial one-player observations and must never enter processMatchPayload().
  // Recovered rows are intentionally excluded because a reconstructed full
  // match can contain source='recovered' players after DB/API recovery.
  return rows.every((row: any) => {
    const source = String(row?.source || '').toLowerCase();
    return ['prefetch', 'match_history', 'history_observation', 'legacy_prefetch'].includes(source);
  });
}

/**
 * Process match history payload from getmatchhistory / getplayermatchhistory.
 * Each entry is one player's partial view of one historical match. It is NOT a
 * full match payload and must never be routed back through broken-match
 * recovery. These observations live in player_match_history_entries so they can
 * serve player-history UI and DB-first recovery without polluting matches,
 * match_players, ranked projections, or raw_ingest_buffer backlog.
 */
function normalizeBufferedHistoryEntry(entry: any): any {
  // Older relay versions stored history rows after normalizing the raw Hi-Rez
  // history entry and marking `source='prefetch'`. Re-normalizing that object through
  // normalizeMatchHistoryPlayer() would drop most lower-case fields because the
  // Hi-Rez and normalized schemas use different names (`Kills` vs `kills`,
  // `ChampionId` vs `champion_id`, `Match_Queue_Id` vs `queue_id`). Preserve
  // already-normalized rows as-is, with light numeric/default coercion for the
  // insert path below. Raw Hi-Rez rows still go through the normalizer.
  const source = String(entry?.source || '');
  const looksAlreadyNormalized =
    Boolean(entry?.match_id || entry?.player_id) &&
    ['prefetch', 'recovered', 'match_history', 'history_observation', 'legacy_prefetch'].includes(source);

  if (!looksAlreadyNormalized) {
    return normalizeMatchHistoryPlayer(entry);
  }

  return {
    ...entry,
    source: source || 'match_history',
    match_id: Number(entry.match_id || 0),
    player_id: Number(entry.player_id || 0),
    queue_id: Number(entry.queue_id || entry.match_queue_id || 0),
    champion_id: Number(entry.champion_id || 0),
    skin_id: Number(entry.skin_id || 0),
    kills: Number(entry.kills || 0),
    deaths: Number(entry.deaths || 0),
    assists: Number(entry.assists || 0),
    time_in_match: Number(entry.time_in_match || 0),
    match_duration: Number(entry.match_duration || 0),
  };
}

async function processMatchHistoryPayload(rawData: any[] | any, endpoint: string): Promise<void> {
  const payload = Array.isArray(rawData) ? rawData : [rawData];
  const entries = payload.filter((p: any) => !(p?.ret_msg || '').trim());
  if (entries.length === 0) return;

  for (const entry of entries) {
    const player = normalizeBufferedHistoryEntry(entry);
    const matchIdFromPlayer = Number(player.match_id || 0);
    const playerId = Number(player.player_id || 0);
    if (!matchIdFromPlayer || !playerId) continue;

    const entryDatetime = isValidTimestamp(player.entry_datetime) ? new Date(String(player.entry_datetime)).toISOString() : null;
    const damage = Number(player.damage_done_physical || 0);
    const normalizedJson = JSON.stringify({ ...player, source: 'match_history' }).replace(/\u0000/g, '');
    const rawJson = JSON.stringify(entry ?? {}).replace(/\u0000/g, '');

    await one(
      `INSERT INTO player_match_history_entries (
         match_id, player_id, fetched_player_id, entry_datetime, queue_id, region, map,
         champion_id, champion_name, skin_id, skin_name, win_status,
         kills, deaths, assists, damage, healing, gold_earned, time_in_match,
         task_force, league_tier, source, raw_data, normalized_data, observed_at, expires_at
       )
       VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,
         $23::jsonb,$24::jsonb,now(),now() + interval '6 hours'
       )
       ON CONFLICT (match_id, player_id) DO UPDATE SET
         fetched_player_id = COALESCE(EXCLUDED.fetched_player_id, player_match_history_entries.fetched_player_id),
         entry_datetime = COALESCE(EXCLUDED.entry_datetime, player_match_history_entries.entry_datetime),
         queue_id = COALESCE(EXCLUDED.queue_id, player_match_history_entries.queue_id),
         region = COALESCE(EXCLUDED.region, player_match_history_entries.region),
         map = COALESCE(NULLIF(EXCLUDED.map, ''), player_match_history_entries.map),
         champion_id = COALESCE(EXCLUDED.champion_id, player_match_history_entries.champion_id),
         champion_name = COALESCE(NULLIF(EXCLUDED.champion_name, ''), player_match_history_entries.champion_name),
         skin_id = COALESCE(EXCLUDED.skin_id, player_match_history_entries.skin_id),
         skin_name = COALESCE(NULLIF(EXCLUDED.skin_name, ''), player_match_history_entries.skin_name),
         win_status = COALESCE(NULLIF(EXCLUDED.win_status, ''), player_match_history_entries.win_status),
         kills = EXCLUDED.kills,
         deaths = EXCLUDED.deaths,
         assists = EXCLUDED.assists,
         damage = EXCLUDED.damage,
         healing = EXCLUDED.healing,
         gold_earned = EXCLUDED.gold_earned,
         time_in_match = EXCLUDED.time_in_match,
         task_force = EXCLUDED.task_force,
         league_tier = EXCLUDED.league_tier,
         source = EXCLUDED.source,
         raw_data = EXCLUDED.raw_data,
         normalized_data = EXCLUDED.normalized_data,
         observed_at = EXCLUDED.observed_at,
         expires_at = EXCLUDED.expires_at`,
      [
        matchIdFromPlayer,
        playerId,
        playerId,
        entryDatetime,
        Number(player.queue_id || 0) || null,
        normalizeRegion(player.region || '') || null,
        player.map || null,
        Number(player.champion_id || 0) || null,
        player.champion_name || null,
        Number(player.skin_id || 0) || null,
        player.skin_name || null,
        player.win_status || null,
        Number(player.kills || 0),
        Number(player.deaths || 0),
        Number(player.assists || 0),
        damage,
        Number(player.healing || 0),
        Number(player.gold_earned || 0),
        Number(player.time_in_match || player.match_duration || 0),
        Number(player.task_force || 0),
        Number(player.league_tier || 0),
        endpoint || 'getmatchhistory',
        rawJson,
        normalizedJson,
      ],
    );
  }
}

async function upsertEsportsLeague(league: any): Promise<void> {
  await one(`INSERT INTO esports_leagues (league_id, league_name, league_description, league_image_url, league_start_date, league_end_date, updated_at)
    VALUES ($1, $2, $3, $4, $5, $6, now())
    ON CONFLICT (league_id) DO UPDATE SET
      league_name = $2, league_description = $3, league_image_url = $4,
      league_start_date = $5, league_end_date = $6, updated_at = now()`,
    [league.league_id, league.league_name, league.league_description, league.league_image_url,
      league.league_start_date || null, league.league_end_date || null]);

  for (const team of (league.teams || [])) {
    await upsertEsportsTeam(team, league.league_id);
  }
}

async function upsertEsportsTeam(team: any, leagueId?: number): Promise<void> {
  await one(`INSERT INTO esports_teams (team_id, team_name, team_description, team_image_url, league_id, updated_at)
    VALUES ($1, $2, $3, $4, $5, now())
    ON CONFLICT (team_id) DO UPDATE SET
      team_name = $2, team_description = $3, team_image_url = $4, updated_at = now()`,
    [team.team_id, team.team_name, team.team_description, team.team_image_url, leagueId || null]);

  for (const player of (team.players || [])) {
    await one(`INSERT INTO esports_team_players (player_id, team_id, player_name)
      VALUES ($1, $2, $3)
      ON CONFLICT (player_id, team_id) DO UPDATE SET player_name = $3`,
      [player.player_id, team.team_id, player.player_name]);
  }
}

/**
 * Process esports payload from getesportsproleaguedetails / getteamdetails / searchteams.
 */
async function processEsportsPayload(rawData: any[], endpoint: string): Promise<void> {
  const entries = rawData.filter((e: any) => !(e.ret_msg || '').trim());
  if (entries.length === 0) return;

  for (const raw of entries) {
    const league = normalizeEsportsLeague(raw);
    await upsertEsportsLeague(league);
  }
}

// ── Player Status ────────────────────────────────────────────────────────────

async function processPlayerStatusPayload(rawData: any[], endpoint: string): Promise<void> {
  const statuses = rawData.filter((s: any) => !(s.ret_msg || '').trim());
  if (statuses.length === 0) return;
  for (const raw of statuses) {
    const status = normalizePlayerStatus(raw);
    await upsertPlayerStatus(status);
  }
}

async function upsertPlayerStatus(status: any): Promise<void> {
  await one(`INSERT INTO player_status (player_id, status, status_string, current_match_id, queue_id, privacy_flag, personal_status_message, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,now())
    ON CONFLICT (player_id) DO UPDATE SET
      status = $2, status_string = $3, current_match_id = $4, queue_id = $5,
      privacy_flag = $6, personal_status_message = $7, updated_at = now()`,
    [status.player_id, status.status, status.status_string, status.current_match_id,
      status.queue_id, status.privacy_flag, status.personal_status_message]);
}

// ── Player Champions ─────────────────────────────────────────────────────────

async function processPlayerChampionsPayload(rawData: any[], endpoint: string): Promise<void> {
  const champList = rawData.filter((c: any) => !(c.ret_msg || '').trim());
  if (champList.length === 0) return;
  const storesCombatStats = endpoint.trim().toLowerCase() === 'getchampionranks';
  for (const raw of champList) {
    if (storesCombatStats && !hasPlayerChampionCombatStats(raw)) {
      console.warn(`[PLAYER-CHAMPIONS] Skipping incomplete combat-stat row from ${endpoint}.`);
      continue;
    }
    const champ = normalizePlayerChampion(raw);
    await upsertPlayerChampion(champ, storesCombatStats);
  }
}

async function upsertPlayerChampion(champ: any, storesCombatStats: boolean): Promise<void> {
  if (!storesCombatStats) {
    await one(`INSERT INTO player_champions (player_id, champion_id, champion_name, xp, ownership_type, stats_populated)
      VALUES ($1,$2,$3,$4,$5,false)
      ON CONFLICT (player_id, champion_id) DO UPDATE SET
        champion_name = COALESCE(NULLIF(EXCLUDED.champion_name, ''), player_champions.champion_name),
        xp = CASE WHEN EXCLUDED.xp > 0 THEN EXCLUDED.xp ELSE player_champions.xp END,
        ownership_type = COALESCE(NULLIF(EXCLUDED.ownership_type, ''), player_champions.ownership_type)`,
      [champ.player_id, champ.champion_id, champ.champion_name, champ.xp, champ.ownership_type]);
    return;
  }

  await one(`INSERT INTO player_champions (player_id, champion_id, champion_name, xp, ownership_type, wins, losses, kills, deaths, assists, minutes_played, stats_populated)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,true)
    ON CONFLICT (player_id, champion_id) DO UPDATE SET
      champion_name = COALESCE(NULLIF(EXCLUDED.champion_name, ''), player_champions.champion_name),
      xp = CASE WHEN EXCLUDED.xp > 0 THEN EXCLUDED.xp ELSE player_champions.xp END,
      ownership_type = COALESCE(NULLIF(EXCLUDED.ownership_type, ''), player_champions.ownership_type),
      wins = EXCLUDED.wins, losses = EXCLUDED.losses, kills = EXCLUDED.kills, deaths = EXCLUDED.deaths,
      assists = EXCLUDED.assists, minutes_played = EXCLUDED.minutes_played,
      stats_populated = true, last_updated = now()`,
    [champ.player_id, champ.champion_id, champ.champion_name, champ.xp, champ.ownership_type,
      champ.wins, champ.losses, champ.kills, champ.deaths, champ.assists, champ.minutes_played]);
}

// ── Player Achievements ──────────────────────────────────────────────────────

async function processPlayerAchievementsPayload(rawData: any[], endpoint: string): Promise<void> {
  const achievements = rawData.filter((a: any) => !(a.ret_msg || '').trim());
  if (achievements.length === 0) return;
  for (const raw of achievements) {
    const ach = normalizePlayerAchievements(raw);
    await upsertPlayerAchievements(ach);
  }
}

async function upsertPlayerAchievements(ach: any): Promise<void> {
  await one(`INSERT INTO player_achievements (player_id, player_name, assisted_kills, camps_cleared, divine_spree,
      double_kills, fire_giant_kills, first_bloods, god_like_spree, gold_fury_kills, immortal_spree, killing_spree,
      minion_kills, penta_kills, phoenix_kills, player_kills, quadra_kills, rampage_spree, shutdown_spree,
      siege_juggernaut_kills, tower_kills, triple_kills, unstoppable_spree, wild_juggernaut_kills, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,now())
    ON CONFLICT (player_id) DO UPDATE SET
      player_name = $2, assisted_kills = $3, camps_cleared = $4, divine_spree = $5,
      double_kills = $6, fire_giant_kills = $7, first_bloods = $8, god_like_spree = $9,
      gold_fury_kills = $10, immortal_spree = $11, killing_spree = $12, minion_kills = $13,
      penta_kills = $14, phoenix_kills = $15, player_kills = $16, quadra_kills = $17,
      rampage_spree = $18, shutdown_spree = $19, siege_juggernaut_kills = $20,
      tower_kills = $21, triple_kills = $22, unstoppable_spree = $23,
      wild_juggernaut_kills = $24, updated_at = now()`,
    [ach.player_id, ach.player_name, ach.assisted_kills, ach.camps_cleared, ach.divine_spree,
      ach.double_kills, ach.fire_giant_kills, ach.first_bloods, ach.god_like_spree, ach.gold_fury_kills,
      ach.immortal_spree, ach.killing_spree, ach.minion_kills, ach.penta_kills, ach.phoenix_kills,
      ach.player_kills, ach.quadra_kills, ach.rampage_spree, ach.shutdown_spree,
      ach.siege_juggernaut_kills, ach.tower_kills, ach.triple_kills, ach.unstoppable_spree,
      ach.wild_juggernaut_kills]);
}

// ── Champion Skins ───────────────────────────────────────────────────────────

async function processChampionSkinsPayload(rawData: any[], endpoint: string): Promise<void> {
  const skins = rawData.filter((s: any) => !(s.ret_msg || '').trim());
  if (skins.length === 0) return;
  for (const raw of skins) {
    const skin = normalizeSkin(raw);
    await upsertSkin(skin);
  }
}

async function upsertSkin(skin: any): Promise<void> {
  // 3NF: only champion_id, skin_id, skin_name
  await one(`INSERT INTO skins (skin_id, champion_id, skin_name)
    VALUES ($1,$2,$3)
    ON CONFLICT (skin_id) DO UPDATE SET
      champion_id = $2, skin_name = $3`,
    [skin.skin_id, skin.champion_id, skin.skin_name]);
}

// ── Bounty Items ─────────────────────────────────────────────────────────────

async function processBountyItemsPayload(rawData: any[], endpoint: string): Promise<void> {
  const items = rawData.filter((i: any) => !(i.ret_msg || '').trim());
  if (items.length === 0) return;
  for (const raw of items) {
    const item = normalizeBountyItem(raw);
    await upsertBountyItem(item);
  }
}

async function upsertBountyItem(item: any): Promise<void> {
  await one(`INSERT INTO bounty_items (item_id, item_name, champion_id, champion_name,
      sale_type, initial_price, final_price, sale_end_datetime, is_active)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (item_id) DO UPDATE SET
      item_name = $2, champion_name = $4, sale_type = $5,
      initial_price = $6, final_price = $7, sale_end_datetime = $8, is_active = $9`,
    [item.item_id, item.item_name, item.champion_id, item.champion_name,
      item.sale_type, item.initial_price, item.final_price, item.sale_end_date, item.active]);
}

// ── League Leaderboard ───────────────────────────────────────────────────────

async function processLeagueLeaderboardPayload(rawData: any[], endpoint: string): Promise<void> {
  const entries = rawData.filter((e: any) => !(e.ret_msg || '').trim());
  if (entries.length === 0) return;
  for (const raw of entries) {
    const entry = normalizeLeagueLeaderboardEntry(raw);
    await upsertLeagueLeaderboardEntry(entry);
  }
}

async function upsertLeagueLeaderboardEntry(entry: any): Promise<void> {
  await one(`INSERT INTO league_leaderboard (player_id, player_name, rank, tier, points, wins, losses, queue_id, season)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
    ON CONFLICT (player_id, queue_id) DO UPDATE SET
      player_name = $2, rank = $3, tier = $4, points = $5, wins = $6, losses = $7, season = $9`,
    [entry.player_id, entry.player_name, entry.rank, entry.tier, entry.points,
      entry.wins, entry.losses, entry.queue_id, entry.season]);
}

// ── Live Match ───────────────────────────────────────────────────────────────

async function processLiveMatchPayload(rawData: any[], endpoint: string): Promise<void> {
  // getmatchplayerdetails returns flat list of 10 player objects
  const players = rawData.filter((p: any) => !(p.ret_msg || '').trim());
  if (players.length === 0) return;

  const p0 = players[0];
  const matchId = Number(p0.Match || p0.match_id || 0);
  if (!matchId) return;

  const queueId = Number(p0.Queue || p0.queue_id || 0);
  const map = p0.mapGame || p0.map || '';
  const region = p0.playerRegion || p0.region || '';
  const sourcePlayerId = Number(p0.playerId || p0.player_id || 0);

  // Upsert live_matches
  await one(`
    INSERT INTO live_matches (match_id, queue_id, region, map, detected_at, source_player_id)
    VALUES ($1, $2, $3, $4, now(), $5)
    ON CONFLICT (match_id) DO UPDATE SET
      queue_id = $2, region = $3, map = $4, detected_at = now(), source_player_id = $5, status = 'active'
  `, [matchId, queueId, region, map, sourcePlayerId]);

  // Insert all 10 players into live_match_players
  for (const raw of players) {
    const p = normalizeLiveMatchPlayer(raw);
    await one(`
      INSERT INTO live_match_players (match_id, player_id, player_name, champion_id, champion_name,
        skin_id, skin_name, account_level, mastery_level, tier, tier_wins, tier_losses, task_force, platform)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
      ON CONFLICT (match_id, player_id) DO UPDATE SET
        player_name = $3, champion_id = $4, champion_name = $5,
        skin_id = $6, skin_name = $7, account_level = $8, mastery_level = $9,
        tier = $10, tier_wins = $11, tier_losses = $12, task_force = $13, platform = $14
    `, [
      matchId, p.player_id, p.player_name, p.champion_id, p.champion_name,
      p.skin_id, p.skin_name, p.account_level, p.mastery_level,
      p.tier, p.tier_wins, p.tier_losses, p.task_force, p.portal_id
    ]);
  }
}

// ── Esports Team (standalone) ────────────────────────────────────────────────

async function processEsportsTeamPayload(rawData: any[], endpoint: string): Promise<void> {
  const entries = rawData.filter((e: any) => !(e.ret_msg || '').trim());
  if (entries.length === 0) return;
  for (const raw of entries) {
    const team = normalizeEsportsTeam(raw);
    await upsertEsportsTeam(team);
  }
}

// ── Auto-populate skins from match_players ───────────────────────────────────

/**
 * After each batch of match_players is inserted, auto-populate the skins
 * and broken_skins tables. Normal skins (skin_id <= 32767) go to `skins`;
 * Int16 overflow skins (skin_id > 32767) go to `broken_skins`.
 * ON CONFLICT DO NOTHING skips on overlap.
 */
export async function autoPopulateSkins(players: any[]): Promise<void> {
  const withSkins = players.filter((p: any) => {
    const skinId = Number(p.skin_id);
    const championId = Number(p.champion_id);
    return Number.isFinite(skinId)
      && skinId > 0
      && Number.isFinite(championId)
      && championId > 0;
  });
  if (withSkins.length === 0) return;

  // Normal skins: skin_id <= 32767
  const normal = withSkins.filter((p: any) => p.skin_id <= 32767);
  if (normal.length > 0) {
    for (const p of normal) {
      await ensureChampionReference(p.champion_id, p.champion_name);
      await one(`INSERT INTO skins (champion_id, skin_id, skin_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (skin_id) DO NOTHING`,
        [p.champion_id, p.skin_id, p.skin_name]);
    }
  }

  // Broken skins: skin_id > 32767 (Int16 overflow)
  const broken = withSkins.filter((p: any) => p.skin_id > 32767);
  if (broken.length > 0) {
    for (const p of broken) {
      await ensureChampionReference(p.champion_id, p.champion_name);
      await one(`INSERT INTO broken_skins (champion_id, champion_name, skin_id, skin_name)
        VALUES ($1, (SELECT name FROM champions WHERE id = $1), $2, $3)
        ON CONFLICT ON CONSTRAINT uq_broken_skins DO NOTHING`,
        [p.champion_id, p.skin_id, p.skin_name]);
    }
  }
}

/**
 * Assign sequential party numbers within a match based on PartyId groups.
 * Insert normalized player relationships (teammate + opponent) — ranked only.
 *
 * player_relationships intentionally stores one row per unordered pair with
 * source_player_id < target_player_id. Readers must query both columns and
 * derive the other player; inserting A->B and B->A would double-count co-play.
 */
export async function assignPartyNumbers(
  matchId: number,
  entryDatetime: string | Date,
): Promise<void> {
  // Reset party to 0 for all players in this match, then assign
  await one(
    `UPDATE match_players
     SET party = 0
     WHERE match_id = $1
       AND entry_datetime = $2::timestamptz`,
    [matchId, entryDatetime],
  );

  // Assign sequential party numbers to groups with 2+ players
  await one(`
    WITH party_groups AS (
      SELECT match_id, party_id
      FROM match_players
      WHERE match_id = $1
        AND entry_datetime = $2::timestamptz
        AND party_id IS NOT NULL AND party_id != 0
      GROUP BY match_id, party_id
      HAVING COUNT(*) > 1
    ),
    party_numbered AS (
      SELECT match_id, party_id,
        ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY party_id) AS party_num
      FROM party_groups
    )
    UPDATE match_players mp
    SET party = pn.party_num
    FROM party_numbered pn
    WHERE mp.match_id = pn.match_id
      AND mp.entry_datetime = $2::timestamptz
      AND mp.party_id = pn.party_id
  `, [matchId, entryDatetime]);

  // Advanced metrics (co-play tracking) — ranked only (queue_id = 486)
  const isRanked = await one(
    `SELECT is_ranked
     FROM matches
     WHERE match_id = $1
       AND entry_datetime = $2::timestamptz`,
    [matchId, entryDatetime],
  );
  if (!isRanked.is_ranked) return;

  // Store exact 2-5 member stacks and every canonical unordered pair. The
  // match-level fact tables make this retry-safe even if the ingest stage
  // marker is written after this transaction commits.
  await recordRankedPartyGroups(matchId, entryDatetime);

  // Upsert teammate pairs (single direction: source < target)
  await one(`
    INSERT INTO player_relationships (source_player_id, target_player_id, same_team, same_party, count, first_seen, last_seen)
    SELECT
      a.player_id, b.player_id,
      true,
      (a.party > 0 AND a.party = b.party),
      1,
      m.entry_datetime, m.entry_datetime
    FROM match_players a
    JOIN match_players b
      ON a.match_id = b.match_id
     AND a.entry_datetime = b.entry_datetime
     AND a.task_force = b.task_force
     AND a.player_id < b.player_id
    JOIN matches m
      ON m.match_id = a.match_id
     AND m.entry_datetime = a.entry_datetime
    WHERE a.match_id = $1
      AND a.entry_datetime = $2::timestamptz
      AND a.player_id > 0 AND b.player_id > 0
      AND a.champion_id > 0 AND b.champion_id > 0
      AND COALESCE(a.source, 'direct') IN ('direct', 'recovered')
      AND COALESCE(b.source, 'direct') IN ('direct', 'recovered')
    ON CONFLICT (source_player_id, target_player_id, same_team)
    DO UPDATE SET count = player_relationships.count + 1,
                  last_seen = EXCLUDED.last_seen,
                  same_party = player_relationships.same_party OR EXCLUDED.same_party
  `, [matchId, entryDatetime]);

  // Upsert opponent pairs (single direction: source < target)
  await one(`
    INSERT INTO player_relationships (source_player_id, target_player_id, same_team, same_party, count, first_seen, last_seen)
    SELECT
      a.player_id, b.player_id,
      false, false,
      1,
      m.entry_datetime, m.entry_datetime
    FROM match_players a
    JOIN match_players b
      ON a.match_id = b.match_id
     AND a.entry_datetime = b.entry_datetime
     AND a.task_force != b.task_force
     AND a.player_id < b.player_id
    JOIN matches m
      ON m.match_id = a.match_id
     AND m.entry_datetime = a.entry_datetime
    WHERE a.match_id = $1
      AND a.entry_datetime = $2::timestamptz
      AND a.player_id > 0 AND b.player_id > 0
      AND a.champion_id > 0 AND b.champion_id > 0
      AND COALESCE(a.source, 'direct') IN ('direct', 'recovered')
      AND COALESCE(b.source, 'direct') IN ('direct', 'recovered')
    ON CONFLICT (source_player_id, target_player_id, same_team)
    DO UPDATE SET count = player_relationships.count + 1,
                  last_seen = EXCLUDED.last_seen
  `, [matchId, entryDatetime]);
}

export { processRawPayload };

// ── Auto-calc: Refresh Materialized Views ────────────────────────────────────

const MATERIALIZED_VIEW_REFRESH_TIMEOUT_MS = Math.min(
  Math.max(Number(process.env.MATERIALIZED_VIEW_REFRESH_TIMEOUT_MS || 120_000), 30_000),
  300_000,
);

async function refreshMaterializedViewConcurrently(view: string): Promise<void> {
  const client = await pool.connect();
  const startedAt = Date.now();
  try {
    // Maintenance reads are intentionally allowed to outlive the generic
    // 30-second web-query boundary. CONCURRENTLY preserves the prior snapshot
    // for readers while rebuilding. The per-query client timeout and server
    // timeout are both widened so neither side cancels first.
    await client.query({
      text: `SET statement_timeout = '${MATERIALIZED_VIEW_REFRESH_TIMEOUT_MS}ms'`,
      query_timeout: 10_000,
    } as any);
    await client.query({
      text: `REFRESH MATERIALIZED VIEW CONCURRENTLY ${view}`,
      query_timeout: MATERIALIZED_VIEW_REFRESH_TIMEOUT_MS,
    } as any);
    console.log(
      `[buffer-processor] Refreshed MV ${view} concurrently in ${Date.now() - startedAt}ms`,
    );
  } finally {
    try {
      await client.query({
        text: 'RESET statement_timeout',
        query_timeout: 10_000,
      } as any);
    } finally {
      client.release();
    }
  }
}

export async function refreshMaterializedViews(): Promise<void> {
  // champion_meta_stats removed — replaced by incremental champion_stats_ranked table for ranked data.
  // The MV still exists but is no longer refreshed automatically. If needed, manual refresh via admin endpoint.
  // tier_population_stats kept — still used for leaderboard/tier analytics and not yet replaced.
  // mv_player_coplay_stats is derived from player_relationships, which is
  // updated during ranked match ingest. Refreshing it hourly makes /coplay/stats
  // current without re-fetching any Hi-Rez payloads, while avoiding the old
  // per-batch MV refresh problem that caused database pressure.
  // Source: User request 2026-06-03 — "disable hourly MV refresh for champion_meta_stats"
  const views = [
    'tier_population_stats',
    'mv_player_coplay_stats',
  ];

  for (const view of views) {
    try {
      const exists = await one('SELECT to_regclass($1) AS relation_oid', [`public.${view}`]);
      if (!exists?.relation_oid) {
        continue;
      }
      await refreshMaterializedViewConcurrently(view);
    } catch (err) {
      // Never follow a timed-out concurrent rebuild with a non-concurrent
      // refresh. The fallback takes an exclusive lock and turned one timeout
      // into a predictable second period of blocked/contending web queries.
      // Keep serving the previous materialized snapshot and retry next hour.
      console.error(`[buffer-processor] Concurrent refresh MV ${view} failed: ${err}`);
    }
  }
}

// ── Auto-calc: Hourly Match Counts ───────────────────────────────────────────

/**
 * Map region string from matches table to hourly_match_counts column name.
 */
/**
 * Whitelist of valid column names for hourly_match_counts region columns.
 * Used by upsertHourlyCount to prevent SQL injection via region string.
 * Source: Fault #2 — "SQL injection via col variable"
 */
const REGION_COL: Record<string, string> = {
  'NA': 'matches_na',
  'EU': 'matches_eu',
  'SEA': 'matches_sea',
  'JPN': 'matches_jpn',
  'RUS': 'matches_rus',
  'BR': 'matches_br',
  'OCE': 'matches_oce',
  'SA': 'matches_sa',
  '': 'matches_unknown',
  'Unknown': 'matches_unknown',
};

/**
 * Aggregate loadout projections are ranked-only. Keeping the table names
 * constant makes it impossible for a future caller to route casual match facts
 * into a second aggregate family by passing a boolean.
 */
const ITEM_COUNT_TABLE = 'item_counts_ranked';
const TALENT_COUNT_TABLE = 'talent_counts_ranked';
const CARD_COUNT_TABLE = 'card_counts_ranked';

/**
 * After each match is persisted, upsert the region count into hourly_match_counts.
 * Increments the region column by 1 and total_matches by 1.
 */
async function upsertHourlyCount(region: string | null, queueId: number, entryDatetime: Date): Promise<void> {
  if (!isRankedStatsQueue(queueId)) return;
  if (!Number.isFinite(Number(queueId)) || Number(queueId) <= 0) {
    console.warn(`[buffer-processor] Refusing hourly_match_counts write for invalid queue_id=${queueId}`);
    return;
  }

  const normalized = region || 'Unknown';
  const col = REGION_COL[normalized];
  // CRITICAL: Validate column name against whitelist. The REGION_COL map
  // only contains known-safe values, but defensive check prevents injection
  // if the map is ever extended with untrusted input.
  // Source: Fault #2 — "SQL injection via col variable"
  const validCols = Object.values(REGION_COL);
  if (!col || !validCols.includes(col)) return;

  // ----------------------------------------------------------------
  // Use native Date arithmetic for hour bounds instead of string concat.
  // Old code: `hour + 1` produced `'2026-06-01T24:00:00'` at midnight.
  // PG 18 happens to accept this and rolls it over, but it's technically
  // invalid ISO-8601. Other engines (MySQL, SQLite) reject it outright.
  // Date arithmetic handles day/month/year rollover correctly every time.
  // Source: User report 2026-06-01 — "midnight crash: hour+1=24 creates
  //   invalid timestamp string at 23:xx UTC."
  // ----------------------------------------------------------------
  const startObj = new Date(Date.UTC(
    entryDatetime.getUTCFullYear(),
    entryDatetime.getUTCMonth(),
    entryDatetime.getUTCDate(),
    entryDatetime.getUTCHours()
  ));
  const endObj = new Date(startObj.getTime() + 3600000);
  const start = startObj.toISOString();
  const end = endObj.toISOString();

  // Derive dateStr and hour from the Date object for downstream INSERT.
  const dateStr = startObj.toISOString().slice(0, 10);
  const hour = startObj.getUTCHours();

  // Compute actual counts from matches table (idempotent — safe on re-run)
  const result = await one(`
    SELECT COUNT(*) FILTER (WHERE region = $4) AS region_count,
           COUNT(*) AS total_count
    FROM matches
    WHERE queue_id = $3
      AND entry_datetime >= $1
      AND entry_datetime < $2
      AND COALESCE(limited, false) = false
  `, [start, end, queueId, region]);

  // CRITICAL: Use Number() with fallback to 0 instead of parseInt(undefined, 10).
  // parseInt(undefined, 10) returns NaN, which PostgreSQL rejects. Number()
  // returns 0 for undefined, which is the correct default.
  // Source: Fault #9 — "NaN from parseInt on undefined region_count"
  const regionCount = Number(result.region_count) || 0;
  const totalCount = Number(result.total_count) || 0;

  await one(`
    INSERT INTO hourly_match_counts (date, hour, queue_id, ${col}, total_matches, fetched_at)
    VALUES ($1, $2, $3, $4, $5, now())
    ON CONFLICT (date, hour, queue_id)
    DO UPDATE SET
      ${col} = $4,
      total_matches = $5,
      fetched_at = now()
  `, [
    dateStr,
    hour,
    queueId,
    regionCount,
    totalCount,
  ]);

  await markHourlyIngestComplete(dateStr, hour, queueId, totalCount);
}

async function markHourlyWindowCompleteIfReady(queueId: number, entryDatetime: Date): Promise<void> {
  if (!isRankedStatsQueue(queueId)) return;
  if (!Number.isFinite(Number(queueId)) || Number(queueId) <= 0 || Number.isNaN(entryDatetime.getTime())) {
    return;
  }

  const hourStart = new Date(Date.UTC(
    entryDatetime.getUTCFullYear(),
    entryDatetime.getUTCMonth(),
    entryDatetime.getUTCDate(),
    entryDatetime.getUTCHours(),
  ));
  const dateStr = hourStart.toISOString().slice(0, 10);
  const hour = hourStart.getUTCHours();
  // hourly_match_counts is a public/statistical projection and deliberately
  // excludes limited matches. Hour completion is an operational question, so
  // count every readable terminal/fact-durable match here instead of using the
  // smaller statistical count as the acquisition boundary.
  const row = await one(
    `SELECT COUNT(*)::int AS total_matches
     FROM matches m
     LEFT JOIN match_ingest_status mis ON mis.match_id = m.match_id
     WHERE m.queue_id = $1
       AND m.entry_datetime >= $2
       AND m.entry_datetime < $3
       AND (
         mis.status IN ('complete', 'limited')
         OR mis.completed_stages @> ARRAY['player_facts', 'match_bans']::text[]
         OR mis.status IS NULL
       )`,
    [queueId, hourStart.toISOString(), new Date(hourStart.getTime() + 3600000).toISOString()],
  );
  const totalCount = Number(row?.total_matches) || 0;
  if (totalCount <= 0) return;

  // `upsertHourlyCount()` runs before the match has completed every downstream
  // stage, so hourly_ingest_state may still be blocked by this match's
  // `hourly_ingest_match_debt.status='staged'`. After markMatchIngestComplete()
  // flips the per-match debt to complete, try the hour close again. The state
  // helper still refuses to close if any sibling match debt remains open.
  await markHourlyIngestComplete(dateStr, hour, queueId, totalCount);
}

// ── Match Compositions ──────────────────────────────────────────────────────

// FIX (2026-05-31): PostgreSQL 18 rejects $N used both bare (INTEGER) and as
// $N::NUMERIC (NUMERIC) in the same INSERT statement — "inconsistent types deduced for parameter $N".
// Solution: pre-compute winrate in JS and pass it as a separate parameter ($8),
// so $6 is only used in integer contexts (wins column + DO UPDATE arithmetic).
// The derived item/talent/card count tables had the same bug.
async function upsertMatchCompositions(matchId: number): Promise<void> {
  const comps = await query(`
    SELECT
      mp.task_force,
      COUNT(*) FILTER (WHERE c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%') AS frontline,
      COUNT(*) FILTER (WHERE c.roles ILIKE '%Damage%') AS damage,
      COUNT(*) FILTER (WHERE c.roles ILIKE '%Flank%') AS flank,
      COUNT(*) FILTER (WHERE c.roles ILIKE '%Support%') AS support,
      m.winning_task_force,
      mlt.lobby_tier
    FROM match_players mp
    JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
    JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
    JOIN champions c ON c.id = mp.champion_id
    WHERE mp.match_id = $1
      AND mp.task_force IS NOT NULL
      AND mp.task_force != 0
      AND mp.champion_id > 0
      AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    GROUP BY mp.task_force, m.winning_task_force, mlt.lobby_tier
    HAVING COUNT(*) = 5
  `, [matchId]);

  for (const comp of comps) {
    const compId = `${comp.frontline}-${comp.damage}-${comp.flank}-${comp.support}`;
    const roleCount = Number(comp.frontline) + Number(comp.damage) + Number(comp.flank) + Number(comp.support);
    // Never persist malformed compositions when reference role metadata is
    // absent. A composition identifier must always describe all five players.
    if (roleCount !== 5) continue;
    const taskForce = Number(comp.task_force);
    const winningTaskForce = Number(comp.winning_task_force);
    const hasResolvedWinner = winningTaskForce === 1 || winningTaskForce === 2;
    const isWin = hasResolvedWinner && taskForce === winningTaskForce;
    const isLoss = hasResolvedWinner && taskForce !== winningTaskForce;

    const winsVal = isWin ? 1 : 0;
    const lossesVal = isLoss ? 1 : 0;
    await one(`
      INSERT INTO match_compositions_ranked (
        comp_id, lobby_tier, frontline, damage, flank, support, count, wins, losses, updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, 1, $7, $8, now())
      ON CONFLICT (comp_id, lobby_tier) DO UPDATE SET
        count = match_compositions_ranked.count + 1,
        wins = match_compositions_ranked.wins + $7,
        losses = match_compositions_ranked.losses + $8,
        updated_at = now()
    `, [
      compId,
      Number(comp.lobby_tier) || 0,
      parseInt(comp.frontline, 10),
      parseInt(comp.damage, 10),
      parseInt(comp.flank, 10),
      parseInt(comp.support, 10),
      winsVal,
      lossesVal,
    ]);

    // Maintain the original all-lobbies table for older API consumers. New
    // public reads use match_compositions_ranked so lobby filtering is cheap.
    await one(`
      INSERT INTO match_compositions (comp_id, frontline, damage, flank, support, count, wins, losses, winrate, updated_at)
      VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, now())
      ON CONFLICT (comp_id)
      DO UPDATE SET
        count = match_compositions.count + 1,
        wins = match_compositions.wins + $6,
        losses = match_compositions.losses + $7,
        winrate = ROUND(
          ((match_compositions.wins + $6)::NUMERIC
            / NULLIF((match_compositions.wins + $6 + match_compositions.losses + $7)::NUMERIC, 0)) * 100,
          2
        ),
        updated_at = now()
    `, [
      compId,
      parseInt(comp.frontline, 10),
      parseInt(comp.damage, 10),
      parseInt(comp.flank, 10),
      parseInt(comp.support, 10),
      winsVal,
      lossesVal,
      winsVal * 100,
    ]);
  }
}

function isNormalizedWinStatus(value: unknown): boolean {
  const normalized = String(value || '').toLowerCase();
  return normalized === 'winner' || normalized === 'win';
}

function isNormalizedLossStatus(value: unknown): boolean {
  const normalized = String(value || '').toLowerCase();
  return normalized === 'loser' || normalized === 'loss';
}

// ── Item Counts ──────────────────────────────────────────────────────────────
// FIX (2026-05-31): Same PostgreSQL 18 type conflict as match_compositions.
// $5 (wins) was used bare in VALUES and as $5::NUMERIC in winrate CASE expression.
// Resolved by pre-computing winrate in JS and passing it as $7.
async function upsertItemCounts(matchId: number): Promise<void> {
  const tableName = ITEM_COUNT_TABLE;

  const items = await query(`
    SELECT
      mpi.item_id,
      i.item_name,
      mpi.slot,
      COALESCE(mpi.item_level, 0) AS item_level,
      mp.win_status
    FROM match_player_items mpi
    JOIN match_players mp ON mp.match_id = mpi.match_id AND mp.player_id = mpi.player_id
    JOIN items i ON i.item_id = mpi.item_id
    WHERE mpi.match_id = $1
  `, [matchId]);

  for (const item of items) {
    const isWin = isNormalizedWinStatus(item.win_status);
    const isLoss = isNormalizedLossStatus(item.win_status);

    const winsVal = isWin ? 1 : 0;
    const lossesVal = isLoss ? 1 : 0;
    await one(`
      INSERT INTO ${tableName} (item_id, item_name, slot, item_level, count, wins, losses, winrate, updated_at)
      VALUES ($1, $2, $3, $4, 1, $5, $6, $7, now())
      ON CONFLICT (item_id, slot, item_level)
      DO UPDATE SET
        item_name = EXCLUDED.item_name,
        count = ${tableName}.count + 1,
        wins = ${tableName}.wins + $5,
        losses = ${tableName}.losses + $6,
        winrate = ROUND(((${tableName}.wins + $5)::NUMERIC / NULLIF((${tableName}.wins + $5 + ${tableName}.losses + $6)::NUMERIC, 0)) * 100, 2),
        updated_at = now()
    `, [
      parseInt(item.item_id, 10),
      item.item_name,
      parseInt(item.slot, 10),
      parseInt(item.item_level, 10),
      winsVal,
      lossesVal,
      winsVal * 100,
    ]);
  }
}

/**
 * Maintain the request-time projection used by every ranked map detail page.
 * The surrounding countProjections ingest stage is the idempotency boundary,
 * exactly like item_counts_ranked and the other cumulative projections.
 */
async function upsertMapItemCounts(matchId: number): Promise<void> {
  await one(`
    WITH claimed_match AS (
      INSERT INTO map_item_counts_ranked_matches (match_id)
      VALUES ($1)
      ON CONFLICT (match_id) DO NOTHING
      RETURNING match_id
    )
    INSERT INTO map_item_counts_ranked (
      map_name, lobby_tier, item_id, count, wins, losses, updated_at
    )
    SELECT
      m.map,
      COALESCE(mlt.lobby_tier, 0)::SMALLINT,
      mpi.item_id,
      COUNT(*)::INT,
      COUNT(*) FILTER (
        WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')
      )::INT,
      COUNT(*) FILTER (
        WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')
      )::INT,
      now()
    FROM match_player_items mpi
    JOIN claimed_match claimed
      ON claimed.match_id = mpi.match_id
    JOIN match_players mp
      ON mp.match_id = mpi.match_id
     AND mp.player_id = mpi.player_id
    JOIN matches m
      ON m.match_id = mp.match_id
     AND m.entry_datetime = mp.entry_datetime
    JOIN match_lobby_tiers mlt
      ON mlt.match_id = m.match_id
     AND mlt.entry_datetime = m.entry_datetime
    WHERE mpi.match_id = $1
      AND m.queue_id = 486
      AND NULLIF(m.map, '') IS NOT NULL
    GROUP BY m.map, COALESCE(mlt.lobby_tier, 0), mpi.item_id
    ON CONFLICT (map_name, lobby_tier, item_id) DO UPDATE SET
      count = map_item_counts_ranked.count + EXCLUDED.count,
      wins = map_item_counts_ranked.wins + EXCLUDED.wins,
      losses = map_item_counts_ranked.losses + EXCLUDED.losses,
      updated_at = EXCLUDED.updated_at
  `, [matchId]);
}

// ── Talent Counts ────────────────────────────────────────────────────────────
// FIX (2026-05-31): Same PostgreSQL 18 type conflict. $4 (wins) used bare and
// as $4::NUMERIC in winrate CASE expression. Resolved by pre-computing winrate
// in JS and passing it as $6.
async function upsertTalentCounts(matchId: number): Promise<void> {
  const tableName = TALENT_COUNT_TABLE;

  const talents = await query(`
    SELECT
      mpt.talent_id,
      c.name AS champion_name,
      t.talent_name,
      mp.win_status
    FROM match_player_talents mpt
    JOIN match_players mp ON mp.match_id = mpt.match_id AND mp.player_id = mpt.player_id
    JOIN talents t ON t.talent_id = mpt.talent_id
    JOIN champions c ON c.id = t.champion_id
    WHERE mpt.match_id = $1
      AND t.champion_id = mp.champion_id
  `, [matchId]);

  for (const talent of talents) {
    const isWin = isNormalizedWinStatus(talent.win_status);
    const isLoss = isNormalizedLossStatus(talent.win_status);

    const winsVal = isWin ? 1 : 0;
    const lossesVal = isLoss ? 1 : 0;
    await one(`
      INSERT INTO ${tableName} (talent_id, champion_name, talent_name, count, wins, losses, winrate, updated_at)
      VALUES ($1, $2, $3, 1, $4, $5, $6, now())
      ON CONFLICT (talent_id)
      DO UPDATE SET
        champion_name = EXCLUDED.champion_name,
        talent_name = EXCLUDED.talent_name,
        count = ${tableName}.count + 1,
        wins = ${tableName}.wins + $4,
        losses = ${tableName}.losses + $5,
        winrate = ROUND(((${tableName}.wins + $4)::NUMERIC / NULLIF((${tableName}.wins + $4 + ${tableName}.losses + $5)::NUMERIC, 0)) * 100, 2),
        updated_at = now()
    `, [
      parseInt(talent.talent_id, 10),
      talent.champion_name,
      talent.talent_name,
      winsVal,
      lossesVal,
      winsVal * 100,
    ]);
  }
}

// ── Card Counts ──────────────────────────────────────────────────────────────
// FIX (2026-05-31): Same PostgreSQL 18 type conflict. $5 (wins) used bare and
// as $5::NUMERIC in winrate CASE expression. Resolved by pre-computing winrate
// in JS and passing it as $7.
async function upsertCardCounts(matchId: number): Promise<void> {
  const tableName = CARD_COUNT_TABLE;

  const cards = await query(`
    SELECT
      mpc.card_id,
      COALESCE(c_ref.name, c_player.name, 'Champion ' || mp.champion_id::TEXT) AS champion_name,
      COALESCE(cd.card_name, 'Card ' || mpc.card_id::TEXT) AS card_name,
      COALESCE(mpc.card_level, 0) AS card_level,
      mp.win_status
    FROM match_player_cards mpc
    JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
    LEFT JOIN cards cd ON cd.card_id = mpc.card_id
    LEFT JOIN champions c_ref ON c_ref.id = cd.champion_id
    LEFT JOIN champions c_player ON c_player.id = mp.champion_id
    WHERE mpc.match_id = $1
  `, [matchId]);

  for (const card of cards) {
    const isWin = isNormalizedWinStatus(card.win_status);
    const isLoss = isNormalizedLossStatus(card.win_status);

    const winsVal = isWin ? 1 : 0;
    const lossesVal = isLoss ? 1 : 0;
    await one(`
      INSERT INTO ${tableName} (card_id, champion_name, card_name, card_level, count, wins, losses, winrate, updated_at)
      VALUES ($1, $2, $3, $4, 1, $5, $6, $7, now())
      ON CONFLICT (card_id, card_level)
      DO UPDATE SET
        champion_name = EXCLUDED.champion_name,
        card_name = EXCLUDED.card_name,
        count = ${tableName}.count + 1,
        wins = ${tableName}.wins + $5,
        losses = ${tableName}.losses + $6,
        winrate = ROUND(((${tableName}.wins + $5)::NUMERIC / NULLIF((${tableName}.wins + $5 + ${tableName}.losses + $6)::NUMERIC, 0)) * 100, 2),
        updated_at = now()
    `, [
      parseInt(card.card_id, 10),
      card.champion_name,
      card.card_name,
      parseInt(card.card_level, 10),
      winsVal,
      lossesVal,
      winsVal * 100,
    ]);
  }
}

// ── Skin Counts ─────────────────────────────────────────────────────────────
async function upsertTalentCardCounts(matchId: number): Promise<void> {
  await one(`
    INSERT INTO talent_card_counts_ranked (
      talent_id, card_id, card_level, count, wins, losses, updated_at
    )
    SELECT
      mpt.talent_id,
      mpc.card_id,
      COALESCE(mpc.card_level, 0)::SMALLINT,
      COUNT(*)::INT,
      COUNT(*) FILTER (
        WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')
      )::INT,
      COUNT(*) FILTER (
        WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')
      )::INT,
      now()
    FROM match_player_talents mpt
    JOIN match_player_cards mpc
      ON mpc.match_id = mpt.match_id
     AND mpc.player_id = mpt.player_id
    JOIN match_players mp
      ON mp.match_id = mpt.match_id
     AND mp.player_id = mpt.player_id
    JOIN talents t
      ON t.talent_id = mpt.talent_id
     AND t.champion_id = mp.champion_id
    WHERE mpt.match_id = $1
    GROUP BY mpt.talent_id, mpc.card_id, COALESCE(mpc.card_level, 0)
    ON CONFLICT (talent_id, card_id, card_level) DO UPDATE SET
      count = talent_card_counts_ranked.count + EXCLUDED.count,
      wins = talent_card_counts_ranked.wins + EXCLUDED.wins,
      losses = talent_card_counts_ranked.losses + EXCLUDED.losses,
      updated_at = EXCLUDED.updated_at
  `, [matchId]);
}

// One compact row per skin/tier bucket keeps global lobby filtering cheap while
// retaining tier 0 as an explicit unknown-coverage bucket.
async function upsertSkinCounts(matchId: number): Promise<void> {
  await one(`
    INSERT INTO skin_counts_ranked (
      champion_id, skin_id, league_tier, skin_name, count, wins, losses, updated_at
    )
    SELECT
      mp.champion_id,
      mp.skin_id,
      COALESCE(mlt.lobby_tier, 0)::SMALLINT,
      MAX(COALESCE(NULLIF(mp.skin_name, ''), s.skin_name, 'Unknown Skin')),
      COUNT(*)::INT,
      COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::INT,
      COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::INT,
      now()
    FROM match_players mp
    LEFT JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
    LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
    LEFT JOIN skins s ON s.skin_id = mp.skin_id
    WHERE mp.match_id = $1
      AND mp.champion_id > 0
      AND mp.skin_id IS NOT NULL
      AND mp.skin_id > 0
      AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    GROUP BY
      mp.champion_id,
      mp.skin_id,
      COALESCE(mlt.lobby_tier, 0)
    ON CONFLICT (champion_id, skin_id, league_tier) DO UPDATE SET
      skin_name = EXCLUDED.skin_name,
      count = skin_counts_ranked.count + EXCLUDED.count,
      wins = skin_counts_ranked.wins + EXCLUDED.wins,
      losses = skin_counts_ranked.losses + EXCLUDED.losses,
      updated_at = now()
  `, [matchId]);
}

async function upsertMatchLobbyTier(matchId: number): Promise<void> {
  await one(`
    INSERT INTO match_lobby_tiers (match_id, entry_datetime, lobby_tier, known_players, updated_at)
    SELECT m.match_id, m.entry_datetime,
      COALESCE(ROUND(AVG(mp.league_tier) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)), 0)::SMALLINT,
      COUNT(*) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)::SMALLINT,
      now()
    FROM matches m
    LEFT JOIN match_players mp ON mp.match_id = m.match_id AND mp.entry_datetime = m.entry_datetime
      AND mp.player_id > 0 AND mp.champion_id > 0
    WHERE m.match_id = $1
    GROUP BY m.match_id, m.entry_datetime
    ON CONFLICT (match_id, entry_datetime) DO UPDATE SET
      lobby_tier = EXCLUDED.lobby_tier,
      known_players = EXCLUDED.known_players,
      updated_at = EXCLUDED.updated_at
  `, [matchId]);
}

async function upsertBansRanked(matchId: number): Promise<void> {
  const bans = await query(`
    SELECT
      mb.champion_id,
      COALESCE(c.name, 'Champion ' || mb.champion_id::TEXT) AS champion_name,
      COUNT(*)::INTEGER AS ban_count,
      COUNT(*) FILTER (WHERE mb.ban_slot = 1)::INTEGER AS s1,
      COUNT(*) FILTER (WHERE mb.ban_slot = 2)::INTEGER AS s2,
      COUNT(*) FILTER (WHERE mb.ban_slot = 3)::INTEGER AS s3,
      COUNT(*) FILTER (WHERE mb.ban_slot = 4)::INTEGER AS s4,
      COUNT(*) FILTER (WHERE mb.ban_slot = 5)::INTEGER AS s5,
      COUNT(*) FILTER (WHERE mb.ban_slot = 6)::INTEGER AS s6,
      COUNT(*) FILTER (WHERE mb.ban_slot = 7)::INTEGER AS s7,
      COUNT(*) FILTER (WHERE mb.ban_slot = 8)::INTEGER AS s8
    FROM match_bans mb
    LEFT JOIN champions c ON c.id = mb.champion_id
    WHERE mb.match_id = $1
      AND mb.champion_id > 0
    GROUP BY mb.champion_id, COALESCE(c.name, 'Champion ' || mb.champion_id::TEXT)
  `, [matchId]);

  for (const ban of bans) {
    await one(`
      INSERT INTO bans_ranked (
        champion_id, champion_name, ban_total,
        slot1, slot2, slot3, slot4, slot5, slot6, slot7, slot8, updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
      ON CONFLICT (champion_id) DO UPDATE SET
        champion_name = EXCLUDED.champion_name,
        ban_total = bans_ranked.ban_total + $3,
        slot1 = bans_ranked.slot1 + $4,
        slot2 = bans_ranked.slot2 + $5,
        slot3 = bans_ranked.slot3 + $6,
        slot4 = bans_ranked.slot4 + $7,
        slot5 = bans_ranked.slot5 + $8,
        slot6 = bans_ranked.slot6 + $9,
        slot7 = bans_ranked.slot7 + $10,
        slot8 = bans_ranked.slot8 + $11,
        updated_at = now()
    `, [
      Number(ban.champion_id),
      ban.champion_name,
      Number(ban.ban_count || 0),
      Number(ban.s1 || 0),
      Number(ban.s2 || 0),
      Number(ban.s3 || 0),
      Number(ban.s4 || 0),
      Number(ban.s5 || 0),
      Number(ban.s6 || 0),
      Number(ban.s7 || 0),
      Number(ban.s8 || 0),
    ]);
  }
}

/**
 * upsertChampionStatsRanked - Consolidated incremental ranked stats update (stats + bans merged).
 *
 * PURPOSE: Maintains live win_rate, pick_rate, ban_rate, KDA, and per-stat averages for ranked matches.
 * Each ranked match ingest adds player-level data to running totals plus any bans from this match,
 * then recalculates all rates in one pass. Replaces former two-function design (upsertBansRanked + upsertChampionStatsRanked).
 *
 * SOURCE DATA:
 * - Stats: match_players rows for this match_id (champion plays, wins/losses, KDA components, damage/gold/heal/mitigation/tier).
 * - Bans: match_bans rows for this match_id (ban_total increments per champion banned, slot tracking).
 *
 * LOGIC:
 * 1. Query match_players for this match where champion_id > 0. Group by champion_id, aggregate plays/wins/losses/sum stats.
 * 2. Query match_bans for this match where champion_id > 0. Group by champion_id, aggregate ban_total + slots.
 * 3. Merge player and ban data by champion_id into a single map.
 * 4. For each champion: INSERT ... ON CONFLICT DO UPDATE with additive increments for both stats AND bans.
 * 5. After ALL champions updated, recalculate win_rate/pick_rate/ban_rate/kda in one UPDATE statement.
 *
 * AFFECTED AREAS: champion_stats_ranked table (consolidated), /stats/champions endpoint reads directly from this table.
 * CALLED FROM: processRawPayload() inside ranked-match block (queue_id === 486).
 * Source: User request 2026-06-03.
 */
async function upsertChampionStatsRanked(matchId: number): Promise<void> {
  // Query player stats for this ranked match (champion_id > 0 filters out PRIVATEACCOUNT)
  const players = await query(`
    SELECT mp.champion_id, c.name AS champion_name, mp.win_status,
           mp.kills, mp.deaths, mp.assists, mp.damage_done_physical, mp.gold_earned,
           mp.healing, mp.damage_mitigated, mp.league_tier
    FROM match_players mp JOIN champions c ON c.id = mp.champion_id
    WHERE mp.match_id = $1
      AND mp.champion_id > 0
      AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
  `, [matchId]);

  // Query ban data for this ranked match with per-slot aggregation
  const bans = await query(`
    SELECT mb.champion_id, c.name AS champion_name, COUNT(*)::INTEGER AS ban_count,
           count(CASE WHEN mb.ban_slot = 1 THEN 1 END)::INTEGER AS s1,
           count(CASE WHEN mb.ban_slot = 2 THEN 1 END)::INTEGER AS s2,
           count(CASE WHEN mb.ban_slot = 3 THEN 1 END)::INTEGER AS s3,
           count(CASE WHEN mb.ban_slot = 4 THEN 1 END)::INTEGER AS s4,
           count(CASE WHEN mb.ban_slot = 5 THEN 1 END)::INTEGER AS s5,
           count(CASE WHEN mb.ban_slot = 6 THEN 1 END)::INTEGER AS s6,
           count(CASE WHEN mb.ban_slot = 7 THEN 1 END)::INTEGER AS s7,
           count(CASE WHEN mb.ban_slot = 8 THEN 1 END)::INTEGER AS s8
    FROM match_bans mb JOIN champions c ON c.id = mb.champion_id
    WHERE mb.match_id = $1 AND mb.champion_id > 0 GROUP BY mb.champion_id, c.name
  `, [matchId]);

  // Merge player stats and ban data into a single map keyed by champion_id
  const allChamps = new Map<number, {
    name: string; plays: number; winsVal: number; lossesVal: number;
    kills: number; deaths: number; assists: number; damage: number;
    gold: number; heal: number; mitigation: number; leagueTier: number; leagueTierCount: number;
    banCount: number; s1: number; s2: number; s3: number; s4: number;
    s5: number; s6: number; s7: number; s8: number;
  }>();

  // Aggregate player stats per champion
  for (const p of players) {
    const cid = parseInt(p.champion_id, 10);
    if (!allChamps.has(cid)) {
      allChamps.set(cid, { name: p.champion_name, plays: 0, winsVal: 0, lossesVal: 0, kills: 0, deaths: 0, assists: 0, damage: 0, gold: 0, heal: 0, mitigation: 0, leagueTier: 0, leagueTierCount: 0, banCount: 0, s1: 0, s2: 0, s3: 0, s4: 0, s5: 0, s6: 0, s7: 0, s8: 0 });
    }
    const e = allChamps.get(cid)!;
    e.plays++;
    // Hi-Rez-derived rows can arrive with either long-form outcomes
    // (`Winner`/`Loser`) or compact history-style outcomes (`Win`/`Loss`).
    // Champion rollups are the source for public win-rate pages, so treating
    // only one spelling as valid makes every champion look artificially below
    // 50% while the ignored outcomes still inflate total_matches.
    const normalizedOutcome = String(p.win_status || '').toLowerCase();
    if (normalizedOutcome === 'winner' || normalizedOutcome === 'win') e.winsVal++;
    else if (normalizedOutcome === 'loser' || normalizedOutcome === 'loss') e.lossesVal++;
    e.kills += p.kills || 0; e.deaths += p.deaths || 0; e.assists += p.assists || 0;
    e.damage += p.damage_done_physical || 0; e.gold += p.gold_earned || 0;
    e.heal += p.healing || 0; e.mitigation += p.damage_mitigated || 0;
    const leagueTier = Number(p.league_tier || 0);
    // Tier 0 is a coverage/unknown signal, not a real rating bucket. Keep it
    // out of champion average-rating denominators while tier distribution
    // endpoints still expose tier 0 for data-quality visibility.
    if (leagueTier >= 1 && leagueTier <= 26) {
      e.leagueTier += leagueTier;
      e.leagueTierCount++;
    }
  }

  // Aggregate ban data per champion (merge with existing entries or create new)
  for (const b of bans) {
    const cid = parseInt(b.champion_id, 10);
    const entry = allChamps.get(cid);
    if (!entry) {
      allChamps.set(cid, { name: b.champion_name, plays: 0, winsVal: 0, lossesVal: 0, kills: 0, deaths: 0, assists: 0, damage: 0, gold: 0, heal: 0, mitigation: 0, leagueTier: 0, leagueTierCount: 0, banCount: parseInt(b.ban_count, 10), s1: parseInt(b.s1, 10), s2: parseInt(b.s2, 10), s3: parseInt(b.s3, 10), s4: parseInt(b.s4, 10), s5: parseInt(b.s5, 10), s6: parseInt(b.s6, 10), s7: parseInt(b.s7, 10), s8: parseInt(b.s8, 10) });
    } else {
      entry.banCount += parseInt(b.ban_count, 10);
      entry.s1 += parseInt(b.s1, 10); entry.s2 += parseInt(b.s2, 10); entry.s3 += parseInt(b.s3, 10); entry.s4 += parseInt(b.s4, 10);
      entry.s5 += parseInt(b.s5, 10); entry.s6 += parseInt(b.s6, 10); entry.s7 += parseInt(b.s7, 10); entry.s8 += parseInt(b.s8, 10);
    }
  }

  if (allChamps.size === 0) return; // No data for this match — skip

  // Upsert each champion with additive increments for both stats and bans.
  // In ranked: one player per champion per match. Increment by +1 per match occurrence.
  // The "plays" counter in JS tracks how many players played this champ in THIS match
  // (normally 1, but recovery logic may drop some). We use DISTINCT match_id for total_matches,
  // so we increment by 1 if the champion was played in this match at all.
  for (const [cid, info] of allChamps) {
    const matchesInc = info.plays > 0 ? 1 : 0; // champion appeared in this match?
    await one(`
      INSERT INTO champion_stats_ranked (champion_id, champion_name, total_matches, wins, losses, sum_kills, sum_deaths, sum_assists, sum_damage, sum_gold, sum_heal, sum_mitigation, sum_league_tier, league_tier_count, ban_total, slot1, slot2, slot3, slot4, slot5, slot6, slot7, slot8, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, now())
      ON CONFLICT (champion_id) DO UPDATE SET
        champion_name = EXCLUDED.champion_name,
        total_matches = champion_stats_ranked.total_matches + $3,
        wins = champion_stats_ranked.wins + $4,
        losses = champion_stats_ranked.losses + $5,
        sum_kills = champion_stats_ranked.sum_kills + $6,
        sum_deaths = champion_stats_ranked.sum_deaths + $7,
        sum_assists = champion_stats_ranked.sum_assists + $8,
        sum_damage = champion_stats_ranked.sum_damage + $9,
        sum_gold = champion_stats_ranked.sum_gold + $10,
        sum_heal = champion_stats_ranked.sum_heal + $11,
        sum_mitigation = champion_stats_ranked.sum_mitigation + $12,
        sum_league_tier = champion_stats_ranked.sum_league_tier + $13,
        league_tier_count = champion_stats_ranked.league_tier_count + $14,
        ban_total = champion_stats_ranked.ban_total + $15,
        slot1 = champion_stats_ranked.slot1 + $16,
        slot2 = champion_stats_ranked.slot2 + $17,
        slot3 = champion_stats_ranked.slot3 + $18,
        slot4 = champion_stats_ranked.slot4 + $19,
        slot5 = champion_stats_ranked.slot5 + $20,
        slot6 = champion_stats_ranked.slot6 + $21,
        slot7 = champion_stats_ranked.slot7 + $22,
        slot8 = champion_stats_ranked.slot8 + $23,
        updated_at = now()
    `, [cid, info.name, matchesInc, info.winsVal, info.lossesVal, info.kills, info.deaths, info.assists, info.damage, info.gold, info.heal, info.mitigation, info.leagueTier, info.leagueTierCount, info.banCount, info.s1, info.s2, info.s3, info.s4, info.s5, info.s6, info.s7, info.s8]);
  }

  // Recalculate all rates (win_rate, pick_rate, ban_rate, kda) for every row in one pass.
  // total_matches is the denominator — each ranked match = one play per champion.
  await one(`
    UPDATE champion_stats_ranked SET
      win_rate = CASE WHEN (wins + losses) > 0 THEN ROUND(wins::NUMERIC / (wins + losses)::NUMERIC * 100, 2) ELSE NULL END,
      pick_rate = CASE WHEN matches_sub.total > 0 THEN ROUND(total_matches::NUMERIC / matches_sub.total, 4) ELSE NULL END,
      ban_rate = CASE WHEN bans_sub.total > 0 THEN ROUND(ban_total::NUMERIC / bans_sub.total, 4) ELSE NULL END,
      kda = ROUND((sum_kills + sum_assists / 2.0)::NUMERIC / GREATEST(sum_deaths, 1), 2)
    FROM (SELECT SUM(total_matches) AS total FROM champion_stats_ranked) AS matches_sub,
         (SELECT SUM(ban_total) AS total FROM champion_stats_ranked) AS bans_sub
  `);
}
