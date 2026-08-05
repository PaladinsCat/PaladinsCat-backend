import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';

const readBackendSource = (relativePath: string) => readFileSync(
  join(__dirname, '..', relativePath),
  'utf8',
);

test('buffer persists every match read model before running background projections', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  assert.match(source, /lower\(COALESCE\(player->>'source', ''\)\) = 'recovered'/);
  assert.match(source, /completed_stages @> ARRAY\['player_facts', 'match_bans'\]/);
  assert.match(source, /match-facts-persisted/);
  assert.match(source, /match facts durable; background projections pending/);
  assert.match(source, /const claimSize = batchSize/);
  assert.match(source, /applyBatchedCumulativeProjections\(derivedRows\)/);
  assert.match(source, /cumulative-projections-pending/);
  assert.match(source, /cumulative projections queued for batched delta/);
  assert.match(source, /deferCumulativeProjectionWork[\s\S]*derivedRows\.filter/);
  assert.match(source, /derivedRows\[index\]\.priority > 1[\s\S]*hasPendingMatchFactsWaiting\(\)/);
  assert.match(source, /unstarted claimed row\(s\) to pending/);
  assert.match(source, /NOT \$2::boolean[\s\S]*rib\.entity_type = 'match'[\s\S]*ARRAY\['player_facts', 'match_bans'\]/);
  assert.match(source, /while \(true\)/);
  assert.doesNotMatch(source, /maxBatches/);
  assert.match(source, /result\.processed \+ result\.failed \+ result\.deferred === 0/);
  assert.match(source, /processBufferBatch\(batchSize, options\.shouldStop, false\)/);
  assert.doesNotMatch(source, /queuePlayerProfileSnapshotsForMatch|getPlayerBatch\(playerIds\)/);
  assert.match(source, /fields are refreshed only through the explicit player-profile action/);
  assert.match(source, /RAW_BUFFER_PROCESSED_RETENTION_HOURS', 1/);
  assert.match(source, /RAW_BUFFER_FAILED_RETENTION_HOURS', 1/);
});

test('match facts use bounded parallel lanes while projections stay ordered', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  const processorStart = source.indexOf('async function processClaimedBufferRows');
  const processorEnd = source.indexOf('export async function processBufferBatch', processorStart);
  const processor = source.slice(processorStart, processorEnd);
  const factLoop = processor.indexOf('Array.from({ length: laneCount }');
  const derivedLoop = processor.indexOf('for (let index = 0; index < derivedRows.length; index++)');

  assert.match(source, /MATCH_FACT_PROCESSING_CONCURRENCY', 8/);
  assert.match(source, /Math\.min\([\s\S]*MATCH_FACT_PROCESSING_CONCURRENCY[\s\S]*factRows\.length/);
  assert.match(processor, /Promise\.all\(/);
  assert.ok(factLoop >= 0, 'expected bounded fact lanes');
  assert.ok(derivedLoop > factLoop, 'ordered projections must run after parallel fact lanes');
  assert.doesNotMatch(
    processor.slice(derivedLoop),
    /Promise\.all\(/,
    'chronology-sensitive derived rows must not run in parallel',
  );

  const matchProcessor = source.slice(source.indexOf('async function processMatchPayload'));
  assert.match(matchProcessor, /factPersistencePlayers = \[\.\.\.normalized\]\.sort/);
  assert.match(matchProcessor, /leftId > 0 \? leftId : Number\.MAX_SAFE_INTEGER/);
});

test('buffer claims allow only one in-flight row per match entity', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  assert.match(source, /DISTINCT ON \(entity_claim_key\)/);
  assert.match(source, /DISTINCT ON \(entity_id\)/);
  assert.match(source, /in_flight\.status = 'processing'/);
  assert.match(source, /in_flight\.entity_id IS NOT DISTINCT FROM rib\.entity_id/);
});

test('database pools are named and report real application waiters', () => {
  const database = readBackendSource('config/db.ts');
  const system = readBackendSource('routes/system.ts');

  assert.match(database, /application_name: DB_APPLICATION_NAME/);
  assert.match(system, /current_setting\('max_connections'\)/);
  assert.match(system, /state <> 'idle' AND wait_event IS NOT NULL/);
  assert.match(system, /waiting_requests: pool\.waitingCount/);
});

test('hourly debt closes at the PostgreSQL match-detail boundary', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  const processor = source.slice(source.indexOf('async function processMatchPayload'));
  const playerFacts = processor.indexOf('MATCH_INGEST_STAGES.playerFacts');
  const matchBans = processor.indexOf('MATCH_INGEST_STAGES.matchBans');
  const factsDurable = processor.indexOf('markMatchFactsDurable(actualMatchId');
  const yieldToBackground = processor.indexOf("return 'match-facts-persisted'");
  const playerAverages = processor.indexOf('MATCH_INGEST_STAGES.playerAverages');

  assert.ok(playerFacts >= 0);
  assert.ok(matchBans > playerFacts);
  assert.ok(factsDurable > matchBans);
  assert.ok(yieldToBackground > factsDurable);
  assert.ok(playerAverages > yieldToBackground);

  const debtSource = readBackendSource('workers/hourly-ingest-match-debt.ts');
  assert.match(debtSource, /match facts durable and readable/);
});

test('requested match lookup returns at facts without flushing background profiles', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  const start = source.indexOf('export async function processPendingMatchBufferRows');
  const end = source.indexOf('export interface BufferDrainOptions', start);
  const requestedProcessor = source.slice(start, end);

  assert.match(requestedProcessor, /processClaimedBufferRows/);
  assert.doesNotMatch(requestedProcessor, /flushQueuedPlayerProfileBackfills/);
});

test('HH:30 discovery targets exactly the preceding UTC hour', () => {
  const scheduler = readBackendSource('workers/auto-ingester-scheduler.ts');
  const discovery = readBackendSource('workers/active-match-discovery.ts');

  assert.match(scheduler, /'30 \* \* \* \*'/);
  assert.match(discovery, /const hoursBehind = now\.getUTCMinutes\(\) < 30 \? 2 : 1/);
  assert.match(discovery, /new Date\(now\.getTime\(\) - hoursBehind \* 3600000\)/);
});

test('buffer skips completed and legacy match duplicates before canonical relay lookup', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  const processor = source.slice(source.indexOf('async function processMatchPayload'));
  const completionGuard = processor.indexOf('skipPreviouslyCompletedMatch(bufferedMatchId)');
  const durableResume = processor.indexOf('loadDurableMatchResumePayload(bufferedMatchId)');
  const recovery = processor.indexOf('getMatchDetailsBatch(');

  assert.ok(completionGuard >= 0, 'expected an early durable-completion guard');
  assert.ok(durableResume > completionGuard, 'expected a durable-facts resume path after the completion guard');
  assert.ok(recovery >= 0, 'expected the canonical relay lookup path');
  assert.ok(durableResume < recovery, 'durable facts must replace partial raw data before relay lookup');
  assert.match(processor, /resumed from durable facts;[\s\S]*skipped duplicate relay recovery/);
});

test('all completed-match workers share the canonical relay lookup boundary', () => {
  const workerFiles = readdirSync(join(__dirname, '..', 'workers'))
    .filter(file => file.endsWith('.ts'))
    .map(file => `workers/${file}`);
  for (const file of workerFiles) {
    const source = readBackendSource(file);
    assert.doesNotMatch(source, /\brecoverBrokenMatch\b/, `${file} must not orchestrate recovery`);
    assert.doesNotMatch(source, /\bgetPlayerBatchFromMatch\b/, `${file} must not orchestrate roster fallback`);
  }

  const facade = readBackendSource('services/hirez.ts');
  const dispatcher = readBackendSource('hirez-relay/dispatcher.ts');
  const relay = readBackendSource('hirez-relay/core.ts');
  assert.doesNotMatch(facade, /export async function recoverBrokenMatch/);
  assert.doesNotMatch(facade, /export async function resolveCompletedMatches/);
  assert.doesNotMatch(facade, /export async function getPresenceMatchDetailsBatch/);
  assert.doesNotMatch(dispatcher, /recoverBrokenMatch:/);
  assert.doesNotMatch(dispatcher, /resolveCompletedMatches|getPresenceMatchDetailsBatch/);
  assert.match(relay, /export async function getMatchDetailsBatch[\s\S]*recoverBrokenMatch/);
  assert.doesNotMatch(relay, /export async function (?:resolveCompletedMatches|getPresenceMatchDetailsBatch)/);

  const genericIngest = readBackendSource('workers/match-ingestion.ts');
  assert.match(genericIngest, /fetchCompletedMatchesContinuously/);

  const rankedDiscovery = readBackendSource('workers/active-match-discovery.ts');
  assert.match(rankedDiscovery, /fetchCompletedMatchesContinuously/);
  assert.match(rankedDiscovery, /case 'recovery_pending'/);

  const nonranked = readBackendSource('workers/nonranked-acquisition-batching.ts');
  assert.match(nonranked, /fetchCompletedMatchesContinuously/);
  assert.match(nonranked, /dependencies\.getMatchDetailsBatch/);
});

test('limited matches become terminal before every background projection', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  const processor = source.slice(source.indexOf('async function processMatchPayload'));
  const limitedMarker = processor.indexOf('markMatchIngestLimited(actualMatchId)');
  const limitedReturn = processor.indexOf("return 'complete';", limitedMarker);
  const playerProfiles = processor.indexOf('MATCH_INGEST_STAGES.playerProfiles');

  assert.ok(limitedMarker >= 0, 'expected a durable limited-state marker');
  assert.ok(limitedReturn > limitedMarker, 'expected limited processing to stop successfully');
  assert.ok(playerProfiles > limitedReturn, 'limited processing must stop before profiles and projections');

  const scalable = readBackendSource('services/scalable-stats-projections.ts');
  const performance = readBackendSource('services/performance-projections.ts');
  const ratings = readBackendSource('services/rating-calculator.ts');
  const rebuilds = readBackendSource('workers/derived-projection-tracker.ts');
  assert.match(scalable, /COALESCE\(m\.limited, false\) = false/);
  assert.match(performance, /COALESCE\(m\.limited, false\) = false/);
  assert.match(ratings, /COALESCE\(m\.limited, false\) = false/);
  assert.match(processor, /COALESCE\(limited, false\) = false/);
  assert.match(rebuilds, /rebuildMatchLobbyTiers[\s\S]*COALESCE\(m\.limited, false\) = false/);
});

test('limited matches are terminal across discovery, lookup, and debt monitors', () => {
  const guards = readBackendSource('workers/ingest-guards.ts');
  const hourlyDebt = readBackendSource('workers/hourly-ingest-match-debt.ts');
  const requested = readBackendSource('workers/requested-match-ingestion.ts');
  const matchesRoute = readBackendSource('routes/matches.ts');
  const dropped = readBackendSource('services/dropped-matches.ts');
  const relay = readBackendSource('hirez-relay/core.ts');

  assert.match(guards, /status === 'complete' \|\| status === 'limited'/);
  assert.match(hourlyDebt, /mis\.status IN \('complete', 'limited'\)/);
  assert.match(requested, /\['complete', 'limited'\]\.includes/);
  assert.match(matchesRoute, /NOT IN \('complete', 'limited'\)/);
  assert.match(dropped, /ingest_status IN \('complete', 'limited'\)/);
  assert.match(relay, /mis\.status IN \('complete', 'limited'\)/);
});

test('all scheduled buffer drains share one exclusive owner', () => {
  const source = readBackendSource('workers/auto-ingester-scheduler.ts');
  const discoveryStart = source.indexOf('async function runDiscoveryCycle');
  const drainStart = source.indexOf('async function runBufferDrain');
  const discovery = source.slice(discoveryStart, drainStart);
  const drain = source.slice(drainStart, source.indexOf('async function runBufferRetention'));

  assert.doesNotMatch(discovery, /drainRawIngestBuffer\(/);
  assert.match(discovery, /runBufferDrain\(`\$\{reason\} post-discovery`\)/);
  assert.match(drain, /runExclusive\('auto-ingester:buffer-drain'/);
  assert.match(drain, /drainRawIngestBuffer\(/);
});

test('composition projection collapses identical team keys before upsert', () => {
  const source = readBackendSource('services/scalable-stats-projections.ts');
  assert.match(source, /collapsed AS \(/);
  assert.match(source, /SUM\(uses\)::BIGINT AS uses/);
  assert.match(source, /INSERT INTO stats_composition_aggregate SELECT \*,now\(\) FROM collapsed/);
});

test('player averages scope roster validation to target-player matches', () => {
  const source = readBackendSource('services/player-performance-rollups.ts');
  assert.match(source, /candidate_matches AS \(/);
  assert.match(source, /JOIN match_players target_mp ON target_mp\.player_id = tp\.player_id/);
  assert.match(source, /FROM candidate_matches cm\s+JOIN match_players mp_check/);
  assert.doesNotMatch(source, /eligible_matches AS \(\s+SELECT m\.match_id[\s\S]*?FROM matches m/);
});

test('hot performance projections use exactly-once cumulative match deltas', () => {
  const rollups = readBackendSource('services/player-performance-rollups.ts');
  const performance = readBackendSource('services/performance-projections.ts');
  const scalable = readBackendSource('services/scalable-stats-projections.ts');
  const migration = readBackendSource('db/migrations/091_cumulative_player_performance.sql');

  assert.match(rollups, /updatePlayerAveragesForMatches/);
  assert.match(rollups, /player_performance_projection_matches/);
  assert.match(rollups, /player_performance_aggregate\.dpm_sum \+ EXCLUDED\.dpm_sum/);
  assert.match(rollups, /a\.dpm_sum \/ NULLIF\(a\.sample_count, 0\)/);
  assert.match(performance, /upsertPerformanceProjectionsForMatches/);
  assert.match(performance, /unnest\(\$1::bigint\[\]\)/);
  assert.match(performance, /m\.match_id = ANY\(\$1::bigint\[\]\)/);
  assert.match(scalable, /projectMatchesWithClient/);
  assert.match(scalable, /unnest\(\$2::bigint\[\]\)/);
  assert.match(scalable, /m\.match_id=ANY\(\$1::bigint\[\]\)/);
  assert.match(migration, /CREATE TABLE IF NOT EXISTS player_performance_aggregate/);
  assert.match(migration, /COALESCE\(SUM\(mp\.damage_per_minute\), 0\)/);
});

test('projection timeouts split batches, preserve fallback, and back off durable debt', () => {
  const source = readBackendSource('workers/buffer-processor.ts');
  const migration = readBackendSource('db/migrations/092_resilient_delta_workers.sql');

  assert.match(source, /applyAdaptiveProjectionBatch/);
  assert.match(source, /splitting into/);
  assert.match(source, /using per-match fallback/);
  assert.match(source, /cumulativeProjectionFallbacks\.has/);
  assert.match(source, /available_at = now\(\) \+ \(\$3::int \* interval '1 second'\)/);
  assert.match(source, /AND rib\.available_at <= now\(\)/);
  assert.match(migration, /ADD COLUMN IF NOT EXISTS available_at/);
});

test('rating and outcome history use cumulative indexed state', () => {
  const ratings = readBackendSource('services/rating-calculator.ts');
  const performance = readBackendSource('services/performance-projections.ts');
  const migration = readBackendSource('db/migrations/092_resilient_delta_workers.sql');

  assert.match(ratings, /JOIN rating_player_cursors cursor/);
  assert.doesNotMatch(ratings, /JOIN match_rating_snapshots later_snapshot/);
  assert.match(ratings, /INSERT INTO rating_late_match_applications/);
  assert.match(ratings, /arrival_order_delta/);
  assert.match(performance, /INSERT INTO player_champion_outcome_summary/);
  assert.match(performance, /outcomes\.total_matches AS matches_played/);
  assert.doesNotMatch(performance, /FROM match_players qualification_mp/);
  assert.match(migration, /CREATE TABLE IF NOT EXISTS player_champion_outcome_summary/);
  assert.match(migration, /CREATE TABLE IF NOT EXISTS rating_player_cursors/);
  assert.match(migration, /CREATE TABLE IF NOT EXISTS rating_late_match_applications/);
});

test('ratings accept the facts-durable partial state used by the worker', () => {
  const ratings = readBackendSource('services/rating-calculator.ts');
  assert.match(
    ratings,
    /COALESCE\(mis\.status,\s*'complete'\)\s+IN\s+\('processing',\s*'partial',\s*'complete'\)/,
  );
});

test('parallel fact lanes never block on the serialized rating stream', () => {
  const ratings = readBackendSource('services/rating-calculator.ts');
  const buffer = readBackendSource('workers/buffer-processor.ts');

  assert.match(ratings, /pg_try_advisory_xact_lock\(4860001\)/);
  assert.match(ratings, /return 'busy'/);
  assert.match(buffer, /ratingResult === 'busy'/);
  assert.match(buffer, /durable projection retry scheduled/);
});

test('hourly materialized view maintenance never falls back to a blocking refresh', () => {
  const buffer = readBackendSource('workers/buffer-processor.ts');
  const refreshStart = buffer.indexOf('async function refreshMaterializedViewConcurrently');
  const refreshEnd = buffer.indexOf('// ── Auto-calc: Hourly Match Counts', refreshStart);
  const refresh = buffer.slice(refreshStart, refreshEnd);

  assert.match(refresh, /MATERIALIZED_VIEW_REFRESH_TIMEOUT_MS/);
  assert.match(refresh, /REFRESH MATERIALIZED VIEW CONCURRENTLY/);
  assert.match(refresh, /RESET statement_timeout/);
  assert.doesNotMatch(refresh, /await one\(`REFRESH MATERIALIZED VIEW \$\{view\}`\)/);
});

test('requested matches become readable as soon as player facts are durable', () => {
  const source = readBackendSource('workers/requested-match-ingestion.ts');
  const route = readBackendSource('routes/matches.ts');
  assert.match(source, /completed_stages\?\.includes\('player_facts'\)/);
  assert.match(source, /completed_stages\.includes\('match_bans'\)/);
  assert.match(route, /requestedOutcome && requestedOutcome\.status !== 'ready'/);
  assert.match(route, /Never expose that shell as a[\s\S]*successful read-through response/);
});
