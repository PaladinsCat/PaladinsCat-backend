import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { startFixtureRelayCapture } from './local-relay-trace-capture.mjs';

test('fixture relay is loopback-only, fixture-hashed, and records full request/response', async context => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), 'pc-relay-trace-'));
  context.after(() => fs.rm(temporary, { recursive: true, force: true }));
  const fixturePath = path.join(temporary, 'fixture.json');
  const fixture = JSON.stringify({ id: 'fixture-a', responses: { getMatchDetailsBatch: { ok: true, result: [] } } });
  await fs.writeFile(fixturePath, fixture);
  await assert.rejects(
    startFixtureRelayCapture({ fixturePath, fixtureSha256: '0'.repeat(64), tracePath: path.join(temporary, 'x'), runtime: 'typescript', scenario: 'normal', runMarker: 'a'.repeat(64) }),
    /fixture hash mismatch/,
  );
  const tracePath = path.join(temporary, 'trace.jsonl');
  const capture = await startFixtureRelayCapture({
    fixturePath, fixtureSha256: sha256(fixture), tracePath, runtime: 'typescript', scenario: 'normal', runMarker: 'a'.repeat(64),
  });
  context.after(() => capture.close());
  assert.match(capture.url, /^http:\/\/127\.0\.0\.1:/);
  const response = await fetch(`${capture.url}/v1/call`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ operation: 'getMatchDetailsBatch', args: [[{ matchId: 10 }]] }),
  });
  assert.equal(response.status, 200);
  const events = (await fs.readFile(tracePath, 'utf8')).trim().split('\n').map(JSON.parse);
  assert.deepEqual(events.map(event => event.type), ['relay.request', 'relay.response']);
  assert.equal(events[1].payload.fixtureId, 'fixture-a');
  assert.equal(events[1].payload.fixtureResponseSha256, sha256(fixture));
});

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}
