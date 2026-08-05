import assert from 'node:assert/strict';
import {
  getDummyApiCallCounts,
  getMatchDetailsBatch,
  resetDummyApiCallCounts,
  resetDummyMatchScenarios,
  setDummyMatchScenario,
} from '../services/hirez';
import { fetchCompletedMatchesContinuously } from '../workers/completed-match-batching';
import { fetchRequestedMatchPayload } from '../workers/requested-match-fetch';

type ScenarioEvidence = {
  scenario: string;
  outcome: string;
  calls: Record<string, number>;
};

function syntheticIds(count: number): number[] {
  const seed = Date.now() % 10_000;
  const first = 800_000_000 + seed * 10_000;
  // Dummy getmatchhistory exposes a 50-match window. Keep scenario IDs more
  // than 50 apart so observations cached by one recovery cannot satisfy the
  // local-preflight branch of the next scenario.
  return Array.from({ length: count }, (_, index) => first + (index + 1) * 100);
}

function totalCalls(calls: Record<string, number>): number {
  return Object.values(calls).reduce((sum, value) => sum + Number(value || 0), 0);
}

function assertCalls(
  calls: Record<string, number>,
  expected: Record<string, number>,
  expectedTotal: number,
): void {
  for (const [endpoint, count] of Object.entries(expected)) {
    assert.equal(Number(calls[endpoint] || 0), count, `${endpoint} call count`);
  }
  assert.equal(totalCalls(calls), expectedTotal, 'total synthetic Hi-Rez call count');
}

async function resetScenarioState(): Promise<void> {
  await resetDummyMatchScenarios();
  await resetDummyApiCallCounts();
}

async function main(): Promise<void> {
  const ids = syntheticIds(40);
  const evidence: ScenarioEvidence[] = [];

  await resetScenarioState();
  const complete = await getMatchDetailsBatch([{ matchId: ids[0], queueId: 486 }]);
  assert.equal(complete.length, 1);
  assert.equal(complete[0].status, 'complete_direct');
  assert.equal(complete[0].match?.players.length, 10);
  assert.equal(complete[0].match?.recovery_attempted, undefined);
  let calls = await getDummyApiCallCounts();
  assertCalls(calls, { getmatchdetailsbatch: 1 }, 1);
  evidence.push({ scenario: 'healthy direct singleton', outcome: '10 direct rows; no recovery fan-out', calls });

  await resetScenarioState();
  await setDummyMatchScenario(ids[1], 'broken_skin');
  const recovered = await getMatchDetailsBatch([{ matchId: ids[1], queueId: 486 }]);
  assert.equal(recovered.length, 1);
  assert.equal(recovered[0].status, 'complete_recovered');
  assert.equal(recovered[0].match?.players.length, 10);
  assert.equal(recovered[0].match?.recovery_attempted, true);
  assert.equal(recovered[0].match?.recovery_pending, false);
  assert.equal(recovered[0].match?.limited, false);
  assert.equal(recovered[0].match?.players.filter(player => player.source === 'recovered').length, 3);
  calls = await getDummyApiCallCounts();
  assertCalls(calls, {
    getmatchdetailsbatch: 1,
    getplayerbatchfrommatch: 1,
    getmatchhistory: 3,
  }, 5);
  evidence.push({ scenario: 'broken SkinId/Int16 partial roster', outcome: 'relay reconstructed all 10 players automatically', calls });

  await resetScenarioState();
  const continuousIds = ids.slice(2, 14);
  await setDummyMatchScenario(continuousIds[3], 'omit_from_multi');
  const continuous = await fetchCompletedMatchesContinuously(
    continuousIds.map(matchId => ({ matchId, queueId: 486 })),
    requests => getMatchDetailsBatch(requests, 'pipeline_mock_e2e'),
  );
  assert.deepEqual(continuous.map(result => result.matchId), continuousIds);
  assert.ok(continuous.every(result => result.status === 'complete_direct'));
  calls = await getDummyApiCallCounts();
  assertCalls(calls, { getmatchdetailsbatch: 3 }, 3);
  evidence.push({
    scenario: 'ordered batch omission in a 12-match stream',
    outcome: 'healthy prefix released, blocker isolated once, suffix re-batched',
    calls,
  });

  await resetScenarioState();
  await setDummyMatchScenario(ids[14], 'history_missing');
  const [historyUnavailable] = await getMatchDetailsBatch([
    { matchId: ids[14], queueId: 486 },
  ]);
  assert.equal(historyUnavailable.status, 'recovery_pending');
  assert.equal(historyUnavailable.match?.recovery_pending, true);
  assert.equal(historyUnavailable.match?.recovery_terminal, false);
  calls = await getDummyApiCallCounts();
  assertCalls(calls, {
    getmatchdetailsbatch: 1,
    getplayerbatchfrommatch: 1,
    getmatchhistory: 3,
  }, 5);
  evidence.push({
    scenario: 'roster known but target absent from player history',
    outcome: 'retryable recovery_pending response; no false terminal match',
    calls,
  });

  await resetScenarioState();
  await setDummyMatchScenario(ids[15], 'roster_failure');
  const limited = await fetchRequestedMatchPayload(ids[15], {
    getMatchDetailsBatch: async matchIds => {
      const outcomes = await getMatchDetailsBatch(
        matchIds.map(matchId => ({ matchId, queueId: 486 })),
        'pipeline_mock_requested_match',
      );
      return outcomes.flatMap(outcome => outcome.match ? [outcome.match] : []);
    },
  });
  assert.ok(limited);
  assert.equal(limited.raw_data.length, 7);
  assert.ok(limited.raw_data.every(row => row.recovery_attempted === true));
  assert.ok(limited.raw_data.every(row => row.limited === true));
  assert.ok(limited.raw_data.every(row => row.recovery_source === 'getplayerbatchfrommatch_failed'));
  calls = await getDummyApiCallCounts();
  assertCalls(calls, {
    getmatchdetailsbatch: 1,
    getplayerbatchfrommatch: 1,
  }, 2);
  evidence.push({
    scenario: 'partial direct response plus roster endpoint failure',
    outcome: 'seven facts retained and explicitly marked limited',
    calls,
  });

  await resetScenarioState();
  await setDummyMatchScenario(ids[16], 'no_player_anchors');
  const dropped = await getMatchDetailsBatch([{ matchId: ids[16], queueId: 486 }]);
  assert.equal(dropped[0]?.matchId, ids[16]);
  assert.equal(dropped[0]?.status, 'dropped');
  calls = await getDummyApiCallCounts();
  assertCalls(calls, {
    getmatchdetailsbatch: 1,
    getplayerbatchfrommatch: 1,
  }, 2);
  evidence.push({
    scenario: 'no direct rows and no roster anchors',
    outcome: 'one terminal recovery attempt, then dropped',
    calls,
  });

  await resetScenarioState();
  await setDummyMatchScenario(ids[17], 'pve_single_human');
  const pve = await getMatchDetailsBatch([{ matchId: ids[17], queueId: 425 }]);
  assert.equal(pve[0]?.status, 'complete_direct');
  assert.equal(pve[0]?.match?.players.length, 1);
  assert.equal(pve[0]?.match?.queue_id, 425);
  calls = await getDummyApiCallCounts();
  assertCalls(calls, { getmatchdetailsbatch: 1 }, 1);
  evidence.push({
    scenario: 'bot/PvE response with one human row',
    outcome: 'accepted as complete without ten-player recovery',
    calls,
  });

  await resetScenarioState();
  await setDummyMatchScenario(ids[18], 'vendor_failure');
  await assert.rejects(
    getMatchDetailsBatch(
      ids.slice(18, 21).map(matchId => ({ matchId, queueId: 486 })),
    ),
    /Synthetic Hi-Rez service-wide failure/,
  );
  calls = await getDummyApiCallCounts();
  assertCalls(calls, { getmatchdetailsbatch: 1 }, 1);
  evidence.push({
    scenario: 'service-wide vendor failure',
    outcome: 'escaped after one batch call; no singleton fan-out',
    calls,
  });

  console.log(JSON.stringify({
    ok: true,
    scenariosPassed: evidence.length,
    evidence,
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error instanceof Error ? error.stack || error.message : error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await resetDummyMatchScenarios().catch(() => false);
  });
