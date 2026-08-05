import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { validateExecutionEvidence, verifySchedulerAcquisitionParity } from './scheduler-acquisition-parity.mjs';

test('scheduler runtime parity CLI fails closed on a missing report', () => {
  const verifier = fileURLToPath(new URL('./scheduler-acquisition-parity.mjs', import.meta.url));
  const result = spawnSync(process.execPath, [verifier, path.join(os.tmpdir(), 'missing-scheduler-parity-report.json')], {
    encoding: 'utf8',
  });
  assert.notEqual(result.status, 0);
  assert.match(`${result.stdout}${result.stderr}`, /ENOENT/);
});

test('scheduler runtime parity fails closed without an evidence report', async () => {
  await assert.rejects(
    verifySchedulerAcquisitionParity({ reportPath: 'missing.json', repositoryRoot: os.tmpdir() }),
    /ENOENT/,
  );
});

test('scheduler runtime parity rejects synthetic lease/run booleans', () => {
  assert.throws(() => validateExecutionEvidence({
    capture: { execution: { jobKey: 'auto-ingester:discovery', leaseAcquired: true, runRecorded: true } },
    runtime: 'typescript',
    scenario: 'normal',
    runMarker: 'a'.repeat(64),
    executionEvidence: {
      marker: 'a'.repeat(64), runtime: 'typescript', jobKey: 'auto-ingester:discovery',
      lock: { leaseAcquired: true }, run: { runRecorded: true },
    },
  }), /lacks actual advisory lock evidence/);
});

test('scheduler runtime parity rejects self-declared semantic events and unmarked DBs', async context => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'pc-scheduler-parity-'));
  context.after(() => fs.rm(root, { recursive: true, force: true }));
  await fs.writeFile(path.join(root, 'report.json'), JSON.stringify({
    schemaVersion: 3,
    kind: 'scheduler-acquisition-runtime-evidence',
    scenarios: { normal: {}, debt: {}, outage: {}, nonranked: {} },
    provenance: { database: { createdBy: 'forged-runner', disposable: false, destroyed: false, host: 'production', runMarker: 'complete' } },
  }));
  await assert.rejects(
    verifySchedulerAcquisitionParity({ reportPath: 'report.json', repositoryRoot: root }),
    /DB must be created by the local capture runner/,
  );
});
