import assert from 'node:assert/strict';
import test from 'node:test';
import type {
  CompletedMatchResolution,
  MatchDetails,
} from '../contracts/hirez-relay';
import { jsonForPostgresJsonb } from '../utils/postgres-json';
import {
  buildContinuousFetchLanes,
  fetchNonrankedMatchesContinuously,
  isCompleteNonrankedMatchDetail,
  orderUniquePresenceFacts,
} from '../workers/nonranked-acquisition-batching';

function detail(matchId: number, playerCount = 10): MatchDetails {
  return {
    match_id: matchId,
    entry_datetime: '2026-07-24T10:00:00.000Z',
    map: 'Stone Keep',
    queue_id: 424,
    duration_seconds: 600,
    minutes: 10,
    region: 'North America',
    team1_score: 4,
    team2_score: 2,
    winning_task_force: 1,
    has_replay: false,
    players: Array.from({ length: playerCount }, (_, index) => ({
      player_id: matchId * 10 + index,
      player_name: `Player ${matchId}-${index}`,
      champion_id: index + 1,
      source: 'direct',
    } as any)),
  };
}

function directResolution(row: MatchDetails): CompletedMatchResolution {
  return {
    matchId: row.match_id,
    queueId: row.queue_id,
    status: 'complete_direct',
    match: row,
  };
}

function requests(ids: number[], queueId = 424) {
  return ids.map(matchId => ({ matchId, queueId }));
}

test('presence acquisition isolates one omitted blocker through the canonical operation', async () => {
  const ids = Array.from({ length: 12 }, (_, index) => index + 1);
  const detailCalls: number[][] = [];
  const result = await fetchNonrankedMatchesContinuously(requests(ids), {
    getMatchDetailsBatch: async window => {
      detailCalls.push(window.map(request => request.matchId));
      if (window.length === 1 && window[0].matchId === 4) {
        return [{
          matchId: 4,
          queueId: 424,
          status: 'roster_only',
          roster: [{ Id: 40, Name: 'Roster 4' }],
        }];
      }
      return window
        .filter(request => request.matchId !== 4)
        .map(request => directResolution(detail(request.matchId)));
    },
  });

  assert.deepEqual(detailCalls, [ids.slice(0, 10), [4], ids.slice(10)]);
  assert.equal(result.length, 12);
  assert.equal(result.find(row => row.matchId === 4)?.state, 'roster_only');
  assert.equal(result.filter(row => row.state === 'complete_direct').length, 11);
});

test('service-wide detail failure escapes and does not create singleton retries', async () => {
  let detailCalls = 0;
  await assert.rejects(
    fetchNonrankedMatchesContinuously(requests([1, 2, 3]), {
      getMatchDetailsBatch: async () => {
        detailCalls += 1;
        throw new Error('Daily request limit reached');
      },
    }),
    /Daily request limit reached/,
  );
  assert.equal(detailCalls, 1);
});

test('partial detail plus roster is retained without recovery/history state', async () => {
  const partial = detail(40, 7);
  const result = await fetchNonrankedMatchesContinuously(requests([40]), {
    getMatchDetailsBatch: async () => [{
      matchId: 40,
      queueId: 424,
      status: 'limited',
      match: partial,
      roster: [{ Id: 407, Name: 'Roster player' }],
      reason: 'single_pass_presence_only',
    }],
  });
  assert.equal(result[0]?.state, 'partial_roster');
  assert.equal(result[0]?.terminalReason, 'single_pass_presence_only');
});

test('missing detail and roster is dropped once', async () => {
  let detailCalls = 0;
  const result = await fetchNonrankedMatchesContinuously(requests([100]), {
    getMatchDetailsBatch: async () => {
      detailCalls += 1;
      return [{
        matchId: 100,
        queueId: 424,
        status: 'dropped',
        reason: 'single_pass_no_match_facts',
      }];
    },
  });
  assert.equal(detailCalls, 1);
  assert.equal(result[0]?.state, 'dropped');
});

test('bot and PvE direct outcomes retain variable-sized human rosters', async () => {
  const botDetail = { ...detail(4250, 1), queue_id: 425 };
  const pveDetail = { ...detail(103620, 4), queue_id: 10362 };
  const result = await fetchNonrankedMatchesContinuously(
    [
      { matchId: 4250, queueId: 425 },
      { matchId: 103620, queueId: 10362 },
    ],
    {
      getMatchDetailsBatch: async () => [
        directResolution(botDetail),
        directResolution(pveDetail),
      ],
    },
  );
  assert.deepEqual(result.map(row => row.state), ['complete_direct', 'complete_direct']);
});

test('claim pages use bounded full-window lanes', () => {
  const ids = Array.from({ length: 47 }, (_, index) => index + 1);
  assert.deepEqual(
    buildContinuousFetchLanes(ids, 3),
    [
      [...ids.slice(0, 10), ...ids.slice(30, 40)],
      [...ids.slice(10, 20), ...ids.slice(40, 47)],
      ids.slice(20, 30),
    ],
  );
  assert.equal(buildContinuousFetchLanes(ids, 20).length, 5);
  assert.deepEqual(buildContinuousFetchLanes([1, 1, -2, 0, 2], 5), [[1, 2]]);
});

test('PvP queues still require ten usable direct rows', () => {
  assert.equal(isCompleteNonrankedMatchDetail(detail(1, 9), 'pvp'), false);
  assert.equal(isCompleteNonrankedMatchDetail(detail(1, 10), 'pvp'), true);
});

test('non-ranked raw detail JSON strips PostgreSQL-hostile NUL escapes', () => {
  const encoded = jsonForPostgresJsonb({
    player_name: 'snøw\u0000cat',
    literal_escape: 'before\\u0000after',
  });
  assert.equal(encoded.includes('snøw\\u0000cat'), false);
  assert.deepEqual(JSON.parse(encoded), {
    player_name: 'snøwcat',
    literal_escape: 'before\\u0000after',
  });
});

test('presence facts use one stable player-ID lock order across roster orderings', () => {
  const forward = [
    { playerId: 40, marker: 'first-40' },
    { playerId: 10, marker: 'first-10' },
    { playerId: 40, marker: 'duplicate-40' },
    { playerId: 30, marker: 'first-30' },
  ];
  const reverse = [...forward].reverse();

  assert.deepEqual(
    orderUniquePresenceFacts(forward).map(fact => [fact.playerId, fact.marker]),
    [[10, 'first-10'], [30, 'first-30'], [40, 'first-40']],
  );
  assert.deepEqual(
    orderUniquePresenceFacts(reverse).map(fact => fact.playerId),
    [10, 30, 40],
  );
});
