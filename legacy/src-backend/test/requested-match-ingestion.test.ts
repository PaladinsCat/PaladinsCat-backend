import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import type { MatchDetails, PlayerDetails } from '../contracts/hirez-relay';
import {
  fetchRequestedMatchPayload,
  RequestedMatchRecoveryError,
  type RequestedMatchDependencies,
} from '../workers/requested-match-fetch';
import { isRankedStatsQueue } from '../workers/ranked-stats-policy';

function players(matchId: number, count = 10, queueId = 424): PlayerDetails[] {
  return Array.from({ length: count }, (_, index) => ({
    player_id: index + 1,
    player_name: `Player ${index + 1}`,
    match_id: matchId,
    entry_datetime: '7/11/2026 4:00:00 PM',
    queue_id: queueId,
    champion_id: index + 1,
    skin_id: 1,
    skin_name: 'Default',
    source: 'recovered',
    task_force: index < 5 ? 1 : 2,
    win_status: index < 5 ? 'Winner' : 'Loser',
  } as PlayerDetails));
}

function match(matchId: number, playerCount = 10, queueId = 424): MatchDetails {
  return {
    match_id: matchId,
    entry_datetime: '2026-07-11T20:00:00.000Z',
    map: 'Stone Keep',
    queue_id: queueId,
    duration_seconds: 900,
    minutes: 15,
    region: 'North America',
    team1_score: 4,
    team2_score: 2,
    winning_task_force: 1,
    has_replay: false,
    players: players(matchId, playerCount, queueId),
  };
}

test('manual lookup stages the relay-recovered casual match payload', async () => {
  const matchId = 1280696346;
  let calls = 0;
  const dependencies: RequestedMatchDependencies = {
    getMatchDetailsBatch: async () => {
      calls++;
      return [{ ...match(matchId), recovery_attempted: true, recovery_source: 'target_history' }];
    },
  };

  const payload = await fetchRequestedMatchPayload(matchId, dependencies);
  assert.ok(payload);
  assert.equal(calls, 1);
  assert.equal(payload.entity_type, 'match');
  assert.equal(payload.endpoint, 'getmatchdetailsbatch');
  assert.equal(payload.entity_id, matchId);
  assert.equal(payload.raw_data.length, 10);
  assert.ok(payload.raw_data.every(row => row.match_queue_id === 424));
  assert.ok(payload.raw_data.every(row => !isRankedStatsQueue(row.match_queue_id)));
});

test('full recovery carries getdemodetails bans into every staged match row', async () => {
  const matchId = 1280775331;
  const recoveredPlayers = players(matchId).map(player => ({
    ...player,
    ban_id_1: 0,
    ban_id_2: 0,
    ban_id_3: 0,
    ban_id_4: 0,
  }));
  const dependencies: RequestedMatchDependencies = {
    getMatchDetailsBatch: async () => [{
        ...match(matchId),
        players: recoveredPlayers,
        recovery_attempted: true,
        recovery_source: 'batch_only_history',
        recovery_api_calls: 12,
        ban_id_1: 2092,
        ban_id_2: 2479,
        ban_id_3: 2094,
        ban_id_4: 2477,
    }],
  };

  const payload = await fetchRequestedMatchPayload(matchId, dependencies);
  assert.ok(payload);
  assert.ok(payload.raw_data.every(row => row.ban_id_1 === 2092));
  assert.ok(payload.raw_data.every(row => row.ban_id_2 === 2479));
  assert.ok(payload.raw_data.every(row => row.ban_id_3 === 2094));
  assert.ok(payload.raw_data.every(row => row.ban_id_4 === 2477));
  assert.ok(payload.raw_data.every(row => row.recovery_source === 'batch_only_history'));
  assert.ok(payload.raw_data.every(row => row.recovery_api_calls === 12));
});

test('partial direct payload is staged for the normal processor to recover', async () => {
  const matchId = 1280696347;
  const dependencies: RequestedMatchDependencies = {
    getMatchDetailsBatch: async () => [{
      ...match(matchId, 7),
      recovery_attempted: true,
      limited: true,
      recovery_terminal: true,
      recovery_api_calls: 1,
      recovery_source: 'getplayerbatchfrommatch_failed',
    }],
  };

  const payload = await fetchRequestedMatchPayload(matchId, dependencies);
  assert.equal(payload?.raw_data.length, 7);
  assert.ok(payload?.raw_data.every(row => row.recovery_attempted === true));
});

test('non-recoverable batch failures are not disguised as broken-skin recovery', async () => {
  const dependencies: RequestedMatchDependencies = {
    getMatchDetailsBatch: async () => {
      throw new Error('Daily request limit reached');
    },
  };

  await assert.rejects(() => fetchRequestedMatchPayload(1280696348, dependencies), /Daily request limit/);
});

test('recoverable detail errors must reconstruct before a requested match can succeed', async () => {
  const matchId = 1280696349;
  const dependencies: RequestedMatchDependencies = {
    getMatchDetailsBatch: async () => {
      throw new Error('Value was either too large or too small for an Int16. Failing Field = skin_id');
    },
  };

  await assert.rejects(
    () => fetchRequestedMatchPayload(matchId, dependencies),
    (error: unknown) => (
      error instanceof RequestedMatchRecoveryError
      && error.matchId === matchId
      && /could not reconstruct a durable response/.test(error.message)
    ),
  );
});

test('requested lookup never stages a relay recovery-pending sentinel', async () => {
  const matchId = 1280696350;
  await assert.rejects(
    () => fetchRequestedMatchPayload(matchId, {
      getMatchDetailsBatch: async () => [{
        ...match(matchId, 7),
        recovery_attempted: true,
        recovery_pending: true,
        recovery_source: 'target_history_unresolved',
      }],
    }),
    (error: unknown) => (
      error instanceof RequestedMatchRecoveryError
      && error.matchId === matchId
      && /recovery remains pending/.test(error.message)
    ),
  );
});

test('only queue 486 is eligible for aggregate statistics', () => {
  assert.equal(isRankedStatsQueue(486), true);
  assert.equal(isRankedStatsQueue('486'), true);
  assert.equal(isRankedStatsQueue(424), false);
  assert.equal(isRankedStatsQueue(0), false);
});

test('directly requested custom matches remain complete queue-neutral fact payloads', async () => {
  const examples = [
    { matchId: 1280977354, queueId: 458 },
    { matchId: 1280936847, queueId: 10210 },
    { matchId: 1280976776, queueId: 440 },
  ];

  for (const { matchId, queueId } of examples) {
    const dependencies: RequestedMatchDependencies = {
      getMatchDetailsBatch: async () => [match(matchId, 10, queueId)],
    };

    const payload = await fetchRequestedMatchPayload(matchId, dependencies);
    assert.ok(payload);
    assert.equal(payload.entity_id, matchId);
    assert.equal(payload.raw_data.length, 10);
    assert.ok(payload.raw_data.every(row => row.match_queue_id === queueId));
    assert.ok(payload.raw_data.every(row => !isRankedStatsQueue(row.match_queue_id)));
  }
});

test('exact match-id search persists missing queues while aggregate projections stay ranked-only', () => {
  const search = readFileSync(join(__dirname, '..', 'routes/search.ts'), 'utf8');
  const matches = readFileSync(join(__dirname, '..', 'routes/matches.ts'), 'utf8');
  const buffer = readFileSync(join(__dirname, '..', 'workers/buffer-processor.ts'), 'utf8');
  const scalable = readFileSync(join(__dirname, '..', 'services/scalable-stats-projections.ts'), 'utf8');
  const playerRoutes = readFileSync(join(__dirname, '..', 'routes/players.ts'), 'utf8');
  const migration = readFileSync(
    join(__dirname, '..', 'db/migrations/098_ranked_only_scalable_stats.sql'),
    'utf8',
  );

  assert.match(search, /const fetched = await fetchMatches\(\[matchId\],[\s\S]*allowHirezFallback: true/);
  assert.match(search, /beforeHirezFallback: options\.beforeRemote/);
  assert.match(search, /scope: `search-\$\{remoteTarget\}`/);
  assert.match(
    search,
    /const autoMatchLookup = isLikelyMatchId\(q\) && !isDeveloperApiRequest\(req\)/,
  );
  const exactMatchRoute = matches.slice(
    matches.indexOf("fastify.get('/:id'"),
    matches.indexOf("fastify.get('/dropped/nonranked'"),
  );
  assert.match(exactMatchRoute, /allowHirezFallback: String\(req\.method\)\.toUpperCase\(\) === 'GET'/);
  assert.match(exactMatchRoute, /strictReadThrough: String\(req\.method\)\.toUpperCase\(\) === 'GET'/);
  assert.match(exactMatchRoute, /MATCH_RECOVERY_FAILED/);
  assert.doesNotMatch(exactMatchRoute, /!isDeveloperApiRequest/);

  const scalableStage = buffer.slice(
    buffer.indexOf('MATCH_INGEST_STAGES.scalableStats'),
    buffer.indexOf('// Sync match to MeiliSearch'),
  );
  assert.match(scalableStage, /if \(isRankedMatch\)[\s\S]*upsertScalableStatsProjectionsForMatch/);
  assert.match(scalable, /WHERE m\.queue_id = 486[\s\S]*COALESCE\(m\.limited, false\) = false/);
  assert.match(scalable, /AND m\.queue_id=486/);
  assert.match(migration, /DELETE FROM stats_match_aggregate WHERE queue_id IS DISTINCT FROM 486/);
  assert.match(migration, /DELETE FROM stats_champion_metric_histogram WHERE queue_id IS DISTINCT FROM 486/);

  const playerHistoryRoute = playerRoutes.slice(
    playerRoutes.indexOf("fastify.get('/:id/matches'"),
    playerRoutes.indexOf("fastify.get('/:id/champions'"),
  );
  assert.match(playerHistoryRoute, /JOIN matches m ON m\.match_id = mp\.match_id/);
  assert.doesNotMatch(playerHistoryRoute, /m\.queue_id = 486/);
});

test('public match reads cannot inherit a process-wide force-refresh flag', () => {
  const matches = readFileSync(join(__dirname, '..', 'routes/matches.ts'), 'utf8');
  const admin = readFileSync(join(__dirname, '..', 'routes/admin.ts'), 'utf8');

  assert.doesNotMatch(matches, /import \{ API_CONFIG \} from '\.\.\/config\/api'/);
  assert.match(matches, /const forceRefresh = options\.forceRefresh === true/);

  const exactMatchRoute = matches.slice(
    matches.indexOf("fastify.get('/:id'"),
    matches.indexOf("fastify.get('/dropped/nonranked'"),
  );
  assert.doesNotMatch(exactMatchRoute, /forceRefresh/);
  assert.match(admin, /forceRefresh: true/);
  assert.doesNotMatch(admin, /API_CONFIG\.FORCE_REFRESH\s*=/);
});

test('single-match hot paths prune match_players by entry timestamp', () => {
  const matches = readFileSync(join(__dirname, '..', 'routes/matches.ts'), 'utf8');
  const buffer = readFileSync(join(__dirname, '..', 'workers/buffer-processor.ts'), 'utf8');
  const parties = readFileSync(join(__dirname, '..', 'services/party-tracking.ts'), 'utf8');

  const formatter = matches.slice(
    matches.indexOf('async function formatMatchResult'),
    matches.indexOf('async function formatNonrankedMatchResult'),
  );
  const durableResume = buffer.slice(
    buffer.indexOf('async function loadDurableMatchResumePayload'),
    buffer.indexOf('async function processMatchPayload'),
  );
  const partyAssignment = buffer.slice(
    buffer.indexOf('export async function assignPartyNumbers'),
    buffer.indexOf('export { processRawPayload }'),
  );

  assert.match(formatter, /mp\.entry_datetime = \$2::timestamptz/);
  assert.match(durableResume, /mp\.entry_datetime = \$2::timestamptz/);
  assert.match(durableResume, /m\.entry_datetime = mp\.entry_datetime/);
  assert.match(partyAssignment, /entry_datetime = \$2::timestamptz/);
  assert.match(partyAssignment, /recordRankedPartyGroups\(matchId, entryDatetime\)/);
  assert.match(parties, /mp\.entry_datetime = \$3::timestamptz/);
});
