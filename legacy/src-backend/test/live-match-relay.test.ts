import test from 'node:test';
import assert from 'node:assert/strict';
import { dispatchRelayOperation } from '../hirez-relay/dispatcher';
import { getDummyApiCallCounts, resetDummyApiCallCounts } from '../hirez-relay/dummy-data';

test('live match relay exposes player status and ten-player lobby details', async () => {
  resetDummyApiCallCounts();

  const status = await dispatchRelayOperation('getPlayerStatus', [16706730], 'dummy') as any[];
  assert.equal(status.length, 1);
  assert.equal(Number(status[0].player_id), 16706730);
  assert.equal(Number(status[0].Match), 990000001);

  const players = await dispatchRelayOperation('getMatchPlayerDetails', [990000001], 'dummy') as any[];
  assert.equal(players.length, 10);
  assert.deepEqual(new Set(players.map(player => Number(player.taskForce))), new Set([1, 2]));

  const calls = getDummyApiCallCounts();
  assert.equal(calls.getplayerstatus, 1);
  assert.equal(calls.getmatchplayerdetails, 1);
});

test('live match relay validates player and match IDs', async () => {
  await assert.rejects(
    dispatchRelayOperation('getPlayerStatus', ['not-a-player'], 'dummy'),
    /playerId must be a finite number/,
  );
  await assert.rejects(
    dispatchRelayOperation('getMatchPlayerDetails', [Number.NaN], 'dummy'),
    /matchId must be a finite number/,
  );
});
