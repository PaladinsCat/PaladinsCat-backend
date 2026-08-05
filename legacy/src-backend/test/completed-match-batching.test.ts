import assert from 'node:assert/strict';
import test from 'node:test';
import type {
  CompletedMatchRequest,
  CompletedMatchResolution,
} from '../contracts/hirez-relay';
import {
  fetchCompletedMatchesContinuously,
  isRecoverableCompletedMatchBatchError,
} from '../workers/completed-match-batching';

function outcome(
  request: CompletedMatchRequest,
  status: CompletedMatchResolution['status'] = 'complete_direct',
): CompletedMatchResolution {
  return {
    matchId: request.matchId,
    queueId: request.queueId ?? 0,
    status,
  };
}

test('worker isolates one omitted blocker and refills the continuous batch', async () => {
  const requests = Array.from({ length: 12 }, (_, index) => ({
    matchId: index + 1,
    queueId: 486,
  }));
  const calls: number[][] = [];
  const emitted: number[] = [];
  const results = await fetchCompletedMatchesContinuously(
    requests,
    async window => {
      calls.push(window.map(request => request.matchId));
      return window
        .filter(request => window.length === 1 || request.matchId !== 4)
        .map(request => outcome(request));
    },
    {
      onResult: async result => {
        emitted.push(result.matchId);
      },
    },
  );

  assert.deepEqual(calls, [
    requests.slice(0, 10).map(request => request.matchId),
    [4],
    [11, 12],
  ]);
  assert.deepEqual(results.map(result => result.matchId), requests.map(request => request.matchId));
  assert.equal(new Set(emitted).size, requests.length);
});

test('relay-recovered row is accepted without a worker singleton recall', async () => {
  const calls: number[][] = [];
  const requests = [
    { matchId: 20, queueId: 486 },
    { matchId: 21, queueId: 486 },
  ];
  const results = await fetchCompletedMatchesContinuously(requests, async window => {
    calls.push(window.map(request => request.matchId));
    return window.map(request => outcome(
      request,
      request.matchId === 20 ? 'complete_recovered' : 'complete_direct',
    ));
  });
  assert.deepEqual(calls, [[20, 21]]);
  assert.deepEqual(results.map(result => result.status), [
    'complete_recovered',
    'complete_direct',
  ]);
});

test('recovery-pending outcome is accounted once without durable-match guessing', async () => {
  let calls = 0;
  const [result] = await fetchCompletedMatchesContinuously(
    [{ matchId: 30, queueId: 486 }],
    async ([request]) => {
      calls += 1;
      return [outcome(request, 'recovery_pending')];
    },
  );
  assert.equal(calls, 1);
  assert.equal(result.status, 'recovery_pending');
});

test('service-wide failure escapes after one batch with no singleton fan-out', async () => {
  let calls = 0;
  await assert.rejects(
    fetchCompletedMatchesContinuously(
      Array.from({ length: 10 }, (_, index) => ({
        matchId: 40 + index,
        queueId: 486,
      })),
      async () => {
        calls += 1;
        throw new Error('Daily request limit reached');
      },
    ),
    /Daily request limit reached/,
  );
  assert.equal(calls, 1);
});

test('classified no-prefix parser failure is bisected by the worker', async () => {
  const calls: number[][] = [];
  const requests = Array.from({ length: 6 }, (_, index) => ({
    matchId: 60 + index,
    queueId: 486,
  }));
  const results = await fetchCompletedMatchesContinuously(
    requests,
    async window => {
      calls.push(window.map(request => request.matchId));
      if (window.length > 1 && window.some(request => request.matchId === 63)) {
        throw new Error('Value was too large for Int16 SkinId');
      }
      return window.map(request => outcome(
        request,
        request.matchId === 63 ? 'complete_recovered' : 'complete_direct',
      ));
    },
    { isRecoverableBatchError: isRecoverableCompletedMatchBatchError },
  );
  assert.equal(results.length, requests.length);
  assert.equal(results.find(result => result.matchId === 63)?.status, 'complete_recovered');
  assert.ok(calls.some(call => call.length === 1 && call[0] === 63));
});

test('queue context is preserved through mixed continuous requests', async () => {
  const requests = [
    { matchId: 70, queueId: 486 },
    { matchId: 71, queueId: 424 },
    { matchId: 72, queueId: 425 },
  ];
  const results = await fetchCompletedMatchesContinuously(
    requests,
    async window => window.map(request => outcome(
      request,
      request.queueId === 424 ? 'roster_only' : 'complete_direct',
    )),
  );
  assert.deepEqual(results.map(result => [result.matchId, result.queueId, result.status]), [
    [70, 486, 'complete_direct'],
    [71, 424, 'roster_only'],
    [72, 425, 'complete_direct'],
  ]);
});
