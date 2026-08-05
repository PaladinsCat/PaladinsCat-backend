import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  compareWorkerTraceFiles,
  normalizeTrace,
  parseTrace,
} from './worker-trace-compat.mjs';

test('worker trace parser requires auditable worker and event fields', () => {
  assert.throws(() => parseTrace('{"worker":"buffer"}\n'), /requires string field event/);
  assert.throws(() => parseTrace('not-json\n'), /not valid JSON/);
});
test('worker trace normalizers are explicit and must match every selected event', () => {
  const normalized = normalizeTrace(
    [{ worker: 'buffer', event: 'claim', at: 'left', data: { id: 1 } }],
    [{ worker: 'buffer', event: 'claim', operation: 'replace', pointer: '/at', value: '<clock>' }],
  );
  assert.equal(normalized[0].at, '<clock>');
  assert.throws(
    () => normalizeTrace(
      [{ worker: 'buffer', event: 'claim' }],
      [{ operation: 'omit', pointer: '/missing' }],
    ),
    /did not match/,
  );
});

test('worker trace comparator passes exact ordered behavior and fails reordered calls', async (context) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), 'pc-worker-trace-'));
  context.after(() => fs.rm(temporary, { recursive: true, force: true }));
  const manifestPath = path.join(temporary, 'manifest.json');
  const typescriptPath = path.join(temporary, 'typescript.jsonl');
  const rustPath = path.join(temporary, 'rust.jsonl');
  await fs.writeFile(manifestPath, JSON.stringify({
    schemaVersion: 1,
    normalize: [{ operation: 'replace', pointer: '/at', value: '<clock>' }],
  }));
  await fs.writeFile(
    typescriptPath,
    [
      JSON.stringify({ worker: 'buffer', event: 'claim', at: '2026-01-01', id: 1 }),
      JSON.stringify({ worker: 'buffer', event: 'relay.call', at: '2026-01-01', operation: 'resolveMatches' }),
    ].join('\n'),
  );
  await fs.writeFile(
    rustPath,
    [
      JSON.stringify({ worker: 'buffer', event: 'claim', at: '2030-01-01', id: 1 }),
      JSON.stringify({ worker: 'buffer', event: 'relay.call', at: '2030-01-01', operation: 'resolveMatches' }),
    ].join('\n'),
  );
  const passing = await compareWorkerTraceFiles({
    typescriptTracePath: typescriptPath,
    rustTracePath: rustPath,
    manifestPath,
  });
  assert.equal(passing.summary.passed, true);

  await fs.writeFile(
    rustPath,
    [
      JSON.stringify({ worker: 'buffer', event: 'relay.call', at: '2030-01-01', operation: 'resolveMatches' }),
      JSON.stringify({ worker: 'buffer', event: 'claim', at: '2030-01-01', id: 1 }),
    ].join('\n'),
  );
  const failing = await compareWorkerTraceFiles({
    typescriptTracePath: typescriptPath,
    rustTracePath: rustPath,
    manifestPath,
  });
  assert.equal(failing.summary.passed, false);
  assert.ok(failing.differences.some((difference) => difference.pointer === '/0/event'));
});
