import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  MATCH_COUNT_QUEUE_DEFINITIONS,
} from '../workers/match-count-discovery-policy';
import { isVariableHumanRosterQueue } from '../services/match-participant-policy';
import {
  calculateBackgroundMatchAllowance,
  calculateRankedPriorityReserveCalls,
  RANKED_PRIORITY_CALLS_PER_MATCH,
  RANKED_PRIORITY_MAX_MATCHES_PER_HOUR,
} from '../workers/ranked-priority-budget';

test('scheduled match-count discovery carries the complete queue taxonomy', () => {
  const expectedQueues = [
    { queueId: 424, name: 'Casual Siege', ranked: false },
    { queueId: 425, name: 'Siege Training', ranked: false },
    { queueId: 452, name: 'Casual Onslaught', ranked: false },
    { queueId: 453, name: 'Onslaught Training', ranked: false },
    { queueId: 486, name: 'Ranked Siege', ranked: true },
    { queueId: 10297, name: 'Team Deathmatch Training', ranked: false },
    { queueId: 10332, name: 'Arcade', ranked: false },
    { queueId: 10348, name: 'Wave Defense Party Beta', ranked: false },
    { queueId: 10362, name: 'Wave Defense Public Beta', ranked: false },
    { queueId: 10367, name: 'Newcomer', ranked: false },
    { queueId: 10369, name: 'Experiment: Subclasses', ranked: false },
  ];

  assert.deepEqual(
    MATCH_COUNT_QUEUE_DEFINITIONS.map(({ queueId, name, ranked }) => ({ queueId, name, ranked })),
    [
      ...expectedQueues.slice(0, 4),
      { queueId: 469, name: 'Team Deathmatch', ranked: false },
      ...expectedQueues.slice(4),
    ],
  );
  assert.deepEqual(
    MATCH_COUNT_QUEUE_DEFINITIONS.filter(queue => !queue.ranked).map(queue => queue.queueId),
    [424, 425, 452, 453, 469, 10297, 10332, 10348, 10362, 10367, 10369],
  );
  assert.deepEqual(
    MATCH_COUNT_QUEUE_DEFINITIONS.filter(queue => queue.ranked).map(queue => queue.queueId),
    [486],
  );
  assert.equal(
    new Set(MATCH_COUNT_QUEUE_DEFINITIONS.map(queue => queue.queueId)).size,
    MATCH_COUNT_QUEUE_DEFINITIONS.length,
  );

  const migration = readFileSync(
    join(__dirname, '../db/migrations/097_register_hourly_match_count_queues.sql'),
    'utf8',
  );
  for (const queue of expectedQueues) {
    assert.match(migration, new RegExp(`\\(${queue.queueId}, '${queue.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}',`));
  }
  assert.equal(MATCH_COUNT_QUEUE_DEFINITIONS.find(queue => queue.queueId === 424)?.scope, 'casual');
  assert.equal(MATCH_COUNT_QUEUE_DEFINITIONS.find(queue => queue.queueId === 425)?.participantModel, 'bots');
  assert.equal(isVariableHumanRosterQueue(425), true);
  assert.equal(isVariableHumanRosterQueue(10362), true);
  assert.equal(isVariableHumanRosterQueue(424), false);
  assert.equal(isVariableHumanRosterQueue(486), false);
  assert.equal(MATCH_COUNT_QUEUE_DEFINITIONS.find(queue => queue.queueId === 10348)?.scope, 'wave_defense');
});

test('all queue discovery uses the ranked HH:30 scheduler and one state table', () => {
  const autoIngesterSource = readFileSync(
    join(__dirname, '../workers/auto-ingester-scheduler.ts'),
    'utf8',
  );
  const activeDiscoverySource = readFileSync(
    join(__dirname, '../workers/active-match-discovery.ts'),
    'utf8',
  );
  const matchCountSource = readFileSync(
    join(__dirname, '../workers/match-count-discovery.ts'),
    'utf8',
  );
  const schedulerRegistrySource = readFileSync(
    join(__dirname, '../workers/scheduler-registry.ts'),
    'utf8',
  );
  assert.match(autoIngesterSource, /cron\.createTask\(\s*'30 \* \* \* \*'/);
  assert.match(autoIngesterSource, /await discover\(\)\.catch[\s\S]*await discoverPresenceQueues/);
  assert.match(activeDiscoverySource, /claimHourlyIngestHour/);
  assert.match(activeDiscoverySource, /getMatchIdsByQueueDetails/);
  assert.match(activeDiscoverySource, /markHourlyIngestComplete\(resolved\.statDate/);
  assert.match(
    readFileSync(join(__dirname, '../hirez-relay/core.ts'), 'utf8'),
    /currentRelayConsumer\(\) === 'presence_discovery'[\s\S]*SINGLE_ATTEMPT_LOOKUP/,
  );
  assert.doesNotMatch(matchCountSource, /cron\.createTask|getMatchIdsByQueueDetails|match_count_discovery_hours/);
  assert.doesNotMatch(schedulerRegistrySource, /key: 'match_count_discovery'/);
  assert.match(matchCountSource, /INSERT INTO nonranked_match_acquisition/);
  assert.match(matchCountSource, /waiting_for_completion/);
});

test('ID-only discovery is storage-only and backfill only repairs missed hourly state', () => {
  const source = readFileSync(
    join(__dirname, '../workers/match-count-discovery.ts'),
    'utf8',
  );
  const gapSource = readFileSync(
    join(__dirname, '../workers/hourly-gap-checker.ts'),
    'utf8',
  );
  assert.doesNotMatch(source, /startup-catch-up|PROBE_LOOKBACK_HOURS|setTimeout|next_retry_at/);
  assert.match(gapSource, /findMissingPresenceHours/);
  assert.match(gapSource, /state\.status === 'complete' \|\| state\.status === 'empty'/);
  assert.match(gapSource, /discoverPresenceQueue/);
});

test('non-ranked acquisition keeps durable claims while bounding metadata refresh', () => {
  const source = readFileSync(
    join(__dirname, '../workers/nonranked-match-acquisition.ts'),
    'utf8',
  );
  const autoIngester = readFileSync(
    join(__dirname, '../workers/auto-ingester-scheduler.ts'),
    'utf8',
  );
  const schedulerRegistry = readFileSync(
    join(__dirname, '../workers/scheduler-registry.ts'),
    'utf8',
  );
  assert.match(source, /LEFT JOIN nonranked_match_acquisition existing/);
  assert.match(source, /existing\.match_id IS NULL/);
  assert.doesNotMatch(
    source.match(/async function claimMatches[\s\S]*?return claimed\.rows;/)?.[0] ?? '',
    /MATCH_COUNT_DISCOVERY_LOOKBACK_HOURS|lookbackHours/,
    'existing queue debt must not become unclaimable because it aged out',
  );
  assert.match(source, /waiting_for_completion/);
  assert.match(source, /ACTIVE_MATCH_GRACE_MINUTES/);
  assert.match(source, /NONRANKED_ACQUISITION_MAX_MATCHES_PER_RUN/);
  assert.match(source, /sourceDate\?: string/);
  assert.match(source, /source_date = \$4::text::date/);
  assert.match(source, /getApiHeadroomSnapshot/);
  assert.match(source, /getRankedPriorityReserveSnapshot/);
  assert.match(source, /rankedPriorityReserve\.reservedCalls/);
  assert.doesNotMatch(source, /NONRANKED_ACQUISITION_MIN_HEADROOM_CALLS/);
  assert.match(source, /NONRANKED_ACQUISITION_FETCH_CONCURRENCY \|\| 8/);
  assert.doesNotMatch(source, /cron\.createTask|NONRANKED_ACQUISITION_CRON/);
  assert.match(autoIngester, /runNonrankedMatchAcquisition\(`auto-ingester \$\{reason\}`/);
  assert.doesNotMatch(schedulerRegistry, /key: 'nonranked_match_acquisition'/);
  assert.doesNotMatch(source, /getMatchHistory|getDemoDetails|deferClaims/);
  assert.match(source, /getMatchDetailsBatch/);
  assert.doesNotMatch(source, /getPresenceMatchDetailsBatch|getPlayerBatchFromMatch/);
  assert.match(source, /terminalizeClaims/);
  assert.match(source, /runNonrankedMatchAcquisition\([\s\S]*options: NonrankedAcquisitionOptions/);
  assert.match(source, /if \(options\.seedLedger !== false\)[\s\S]*seedAcquisitionLedger\(lookbackHours\)/);
  assert.match(source, /acquisition\.last_observed_at < discovery\.last_seen_at/);
  assert.match(source, /acquisition\.status IS DISTINCT FROM CASE/);
});

test('non-ranked API budget permanently protects one peak ranked recovery hour', () => {
  assert.equal(RANKED_PRIORITY_MAX_MATCHES_PER_HOUR, 75);
  assert.equal(RANKED_PRIORITY_CALLS_PER_MATCH, 13);
  assert.deepEqual(
    calculateRankedPriorityReserveCalls({ dueRankedMatches: 0 }),
    {
      dueRankedMatches: 0,
      configuredHourlyFloor: 75,
      observedHourlyPeak: 0,
      protectedRankedMatches: 75,
      callsPerMatch: 13,
      discoveryCalls: 1,
      reservedCalls: 976,
    },
  );
  assert.equal(
    calculateRankedPriorityReserveCalls({ dueRankedMatches: 78 }).reservedCalls,
    1015,
  );
  assert.equal(
    calculateRankedPriorityReserveCalls({
      dueRankedMatches: 10,
      observedHourlyPeak: 92,
    }).reservedCalls,
    1197,
  );
  assert.equal(
    calculateBackgroundMatchAllowance({
      usableCalls: 4043,
      rankedPriorityReserveCalls: 1028,
      worstCaseCallsPerMatch: 2,
    }),
    1507,
  );
  assert.equal(
    calculateBackgroundMatchAllowance({
      usableCalls: 900,
      rankedPriorityReserveCalls: 976,
      worstCaseCallsPerMatch: 2,
    }),
    0,
  );
});

test('Hi-Rez call logs retain feature attribution', () => {
  const migration = readFileSync(
    join(__dirname, '../db/migrations/104_api_log_consumer_attribution.sql'),
    'utf8',
  );
  const keyPool = readFileSync(join(__dirname, '../services/api-key-pool.ts'), 'utf8');
  const relayServer = readFileSync(join(__dirname, '../hirez-relay/server.ts'), 'utf8');
  assert.match(migration, /PRIMARY KEY \(dev_id, endpoint, consumer, hour\)/);
  assert.match(keyPool, /INSERT INTO api_log \(dev_id, endpoint, consumer, hour/);
  assert.match(relayServer, /runWithRelayAttribution/);
});

test('weekly activity player totals deduplicate every tracked match fact store', () => {
  const source = readFileSync(
    join(__dirname, '../routes/matches.ts'),
    'utf8',
  );
  assert.match(source, /view === 'activity-v3' \? 'includePlayers=true'/);
  assert.match(source, /Activity overview hourly source unavailable/);
  assert.match(source, /hourly\.hourly\.length !== 24/);
  assert.match(source, /FROM match_players mp/);
  assert.match(source, /JOIN casual_match_players fact ON fact\.match_id = casual\.match_id/);
  assert.match(source, /JOIN special_match_players fact ON fact\.match_id = special\.match_id/);
  assert.match(source, /COUNT\(DISTINCT player_id\)::int AS players/);
  assert.match(source, /GROUP BY GROUPING SETS/);
  assert.match(source, /playerQueues: entry\.playerQueues/);
});
