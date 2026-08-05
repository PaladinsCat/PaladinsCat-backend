import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { schedulerParitySourceHash } from './scheduler-parity-source-hash.mjs';
import { execFileSync } from 'node:child_process';

// This gate intentionally consumes evidence only. It never seeds, resets, or
// starts a worker. Evidence must be captured by the real due-job runner against
// a runner-created disposable DB and a fixture-only loopback relay.
const RUNTIMES = ['typescript', 'rust'];
const SCENARIOS = ['normal', 'debt', 'outage', 'nonranked'];
const EVENT_TYPES = new Set(['scheduler.lock', 'scheduler.run', 'db.pre', 'relay.request', 'relay.response', 'db.post']);

export async function verifySchedulerAcquisitionParity({ reportPath, repositoryRoot }) {
  const absoluteReport = path.resolve(repositoryRoot, reportPath);
  const report = JSON.parse(await fs.readFile(absoluteReport, 'utf8'));
  assert.equal(report.schemaVersion, 3, 'runtime evidence schemaVersion must be 3');
  assert.equal(report.kind, 'scheduler-acquisition-runtime-evidence');
  assert.deepEqual(Object.keys(report.scenarios ?? {}).sort(), [...SCENARIOS].sort(), 'all required scenarios need evidence');
  const expectedRelayTranscript = await verifyProvenance(report.provenance, repositoryRoot);
  const relayTranscript = [];

  for (const scenario of SCENARIOS) {
    const captures = report.scenarios[scenario];
    assert.deepEqual(
      Object.keys(captures ?? {}).sort(),
      [...RUNTIMES].sort(),
      `${scenario} requires TS and Rust captures`,
    );
    const normalized = {};
    for (const runtime of RUNTIMES) {
      normalized[runtime] = await verifyCapture({
        capture: captures[runtime], runtime, scenario, repositoryRoot, provenance: report.provenance,
      });
      relayTranscript.push(...normalized[runtime].relayEvents);
    }
    assert.deepEqual(normalized.rust, normalized.typescript, `${scenario} observable due-job behavior differs`);
  }
  assert.equal(sha256(JSON.stringify(relayTranscript)), expectedRelayTranscript, 'relay transcript hash mismatch');
  return { passed: true, reportPath: absoluteReport, scenarios: SCENARIOS };
}

async function verifyProvenance(provenance, repositoryRoot) {
  assert.ok(provenance && typeof provenance === 'object', 'provenance is required');
  const database = provenance.database;
  assert.equal(database?.createdBy, 'scheduler-acquisition-capture-runner', 'DB must be created by the local capture runner');
  assert.equal(database?.disposable, true, 'DB must be disposable');
  assert.equal(database?.destroyed, true, 'DB must be destroyed after capture');
  assert.equal(database?.host, '127.0.0.1', 'DB must be loopback-only');
  assert.match(database?.runMarker ?? '', /^[a-f0-9]{64}$/i, 'DB needs a cryptographic run marker');
  assert.equal(database.markerSha256, sha256(database.runMarker), 'DB marker hash mismatch');
  assert.match(database.schemaSha256 ?? '', /^[a-f0-9]{64}$/i, 'DB schema hash is required');
  assert.match(provenance.source?.commit ?? '', /^[a-f0-9]{40}$/i, 'source commit is required');
  const currentCommit = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repositoryRoot, encoding: 'utf8' }).trim();
  assert.equal(provenance.source.commit, currentCommit, 'evidence source commit is not the current source');
  assert.match(provenance.source.worktreeSha256 ?? '', /^[a-f0-9]{64}$/i, 'source worktree hash is required');
  assert.equal(
    provenance.source.worktreeSha256,
    schedulerParitySourceHash(repositoryRoot),
    'evidence source worktree does not match',
  );
  for (const runtime of RUNTIMES) {
    const build = provenance.builds?.[runtime];
    assert.ok(typeof build?.path === 'string' && build.path.length > 0, `${runtime} build path is required`);
    assert.match(build.sha256 ?? '', /^[a-f0-9]{64}$/i, `${runtime} build hash is required`);
    const content = await fs.readFile(path.resolve(repositoryRoot, build.path));
    assert.equal(sha256(content), build.sha256, `${runtime} build hash mismatch`);
  }
  for (const scenario of SCENARIOS) {
    const fixture = provenance.fixtures?.[scenario];
    assert.ok(typeof fixture?.id === 'string' && fixture.id.length > 0, `${scenario} fixture identity is required`);
    assert.match(fixture?.responseSha256 ?? '', /^[a-f0-9]{64}$/i, `${scenario} fixture response hash is required`);
  }
  assert.match(provenance.relay?.transcriptSha256 ?? '', /^[a-f0-9]{64}$/i, 'relay transcript hash is required');
  return provenance.relay.transcriptSha256;
}

async function verifyCapture({ capture, runtime, scenario, repositoryRoot, provenance }) {
  const fixture = provenance.fixtures[scenario];
  assert.equal(capture?.execution?.entrypoint, 'scheduled-due-job', `${scenario}/${runtime} must execute a due job`);
  assert.ok(typeof capture.execution.jobKey === 'string' && capture.execution.jobKey.includes(':'), `${scenario}/${runtime} job key is required`);
  const evidencePath = capture.execution.evidence?.path;
  assert.ok(typeof evidencePath === 'string' && evidencePath.length > 0, `${scenario}/${runtime} execution evidence path is required`);
  const evidenceRaw = await fs.readFile(path.resolve(repositoryRoot, evidencePath), 'utf8');
  assert.equal(sha256(evidenceRaw), capture.execution.evidence.sha256, `${scenario}/${runtime} execution evidence hash mismatch`);
  const executionEvidence = JSON.parse(evidenceRaw);
  validateExecutionEvidence({ capture, runtime, scenario, runMarker: provenance.database.runMarker, executionEvidence });
  assert.equal(capture.pre?.runMarker, provenance.database.runMarker, `${scenario}/${runtime} pre marker mismatch`);
  assert.equal(capture.post?.runMarker, provenance.database.runMarker, `${scenario}/${runtime} post marker mismatch`);
  assert.match(capture.pre?.stateSha256 ?? '', /^[a-f0-9]{64}$/i, `${scenario}/${runtime} pre-state hash is required`);
  assert.match(capture.post?.stateSha256 ?? '', /^[a-f0-9]{64}$/i, `${scenario}/${runtime} post-state hash is required`);
  assert.notEqual(capture.pre.stateSha256, capture.post.stateSha256, `${scenario}/${runtime} must prove a state transition`);

  const tracePath = capture.trace?.path;
  assert.ok(typeof tracePath === 'string' && tracePath.length > 0, `${scenario}/${runtime} trace path is required`);
  const raw = await fs.readFile(path.resolve(repositoryRoot, tracePath), 'utf8');
  assert.equal(sha256(raw), capture.trace.sha256, `${scenario}/${runtime} trace hash mismatch`);
  const events = raw.split(/\r?\n/).filter(Boolean).map(JSON.parse);
  assert.ok(events.length >= 4, `${scenario}/${runtime} trace is incomplete`);
  let sequence = 0;
  for (const event of events) {
    assert.equal(event.runtime, runtime, `${scenario}/${runtime} runtime mismatch`);
    assert.equal(event.scenario, scenario, `${scenario}/${runtime} scenario mismatch`);
    assert.equal(event.runMarker, provenance.database.runMarker, `${scenario}/${runtime} event marker mismatch`);
    assert.equal(event.sequence, ++sequence, `${scenario}/${runtime} event sequence is not contiguous`);
    assert.ok(EVENT_TYPES.has(event.type), `${scenario}/${runtime} unknown normalized event type`);
    if (event.type === 'relay.request' || event.type === 'relay.response') {
      assert.ok(event.payload && typeof event.payload === 'object', `${scenario}/${runtime} relay payload is required`);
      assert.match(event.payload.sha256 ?? '', /^[a-f0-9]{64}$/i, `${scenario}/${runtime} relay payload hash is required`);
    }
    if (event.type === 'relay.response') {
      assert.equal(event.payload.fixtureId, fixture.id, `${scenario}/${runtime} relay response fixture mismatch`);
      assert.equal(event.payload.fixtureResponseSha256, fixture.responseSha256, `${scenario}/${runtime} relay response hash mismatch`);
    }
  }
  assert.equal(events[0]?.type, 'db.pre', `${scenario}/${runtime} trace must begin with pre-state`);
  assert.equal(events[1]?.type, 'scheduler.lock', `${scenario}/${runtime} trace lacks semantic lock evidence`);
  assert.equal(events[2]?.type, 'scheduler.run', `${scenario}/${runtime} trace lacks semantic run evidence`);
  for (const event of events.slice(1, 3)) {
    assert.equal(event.payload?.evidenceSha256, capture.execution.evidence.sha256, `${scenario}/${runtime} semantic scheduler evidence hash mismatch`);
  }
  assert.deepEqual(events.map(event => event.type), ['db.pre', 'scheduler.lock', 'scheduler.run', ...events.slice(3, -1).map(event => event.type), 'db.post'], `${scenario}/${runtime} trace ordering is invalid`);
  assert.equal(events[events.length - 1].type, 'db.post', `${scenario}/${runtime} must record post-state`);
  assert.equal(events.find(event => event.type === 'db.pre')?.payload?.runMarker, provenance.database.runMarker, `${scenario}/${runtime} pre DB marker was not read`);
  assert.equal(events.find(event => event.type === 'db.post')?.payload?.runMarker, provenance.database.runMarker, `${scenario}/${runtime} post DB marker was not read`);
  assert.ok(events.some(event => event.type === 'relay.request'), `${scenario}/${runtime} needs real relay request evidence`);
  assert.ok(events.some(event => event.type === 'relay.response'), `${scenario}/${runtime} needs real relay response evidence`);

  const normalizedEvents = events.map(({ runtime: _runtime, scenario: _scenario, ...event }) => {
    if (event.type === 'scheduler.lock' || event.type === 'scheduler.run') {
      return { ...event, payload: { jobKey: capture.execution.jobKey } };
    }
    if (event.type === 'relay.request') {
      const request = JSON.parse(event.payload.body);
      delete request.requestId;
      const body = JSON.stringify(request);
      return { ...event, payload: { ...event.payload, body, sha256: sha256(body) } };
    }
    return event;
  });
  return {
    execution: { jobKey: capture.execution.jobKey },
    pre: capture.pre.stateSha256,
    post: capture.post.stateSha256,
    events: normalizedEvents,
    relayEvents: normalizedEvents.filter(event => event.type.startsWith('relay.')),
  };
}

export function validateExecutionEvidence({ capture, runtime, scenario, runMarker, executionEvidence }) {
  assert.equal(executionEvidence.marker, runMarker, `${scenario}/${runtime} execution marker mismatch`);
  assert.equal(executionEvidence.runtime, runtime, `${scenario}/${runtime} execution runtime mismatch`);
  assert.equal(executionEvidence.jobKey, capture.execution.jobKey, `${scenario}/${runtime} execution job mismatch`);
  if (runtime === 'typescript') {
    assert.equal(executionEvidence.lock?.event_type, 'lock-acquired', `${scenario}/typescript lacks actual advisory lock evidence`);
    assert.equal(executionEvidence.run?.event_type, 'run-completed', `${scenario}/typescript lacks actual completed run evidence`);
  } else {
    assert.equal(executionEvidence.run?.status, 'completed', `${scenario}/rust lacks completed worker_job_run_log evidence`);
    assert.equal(executionEvidence.run?.trigger, 'capture-once', `${scenario}/rust did not use the guarded capture path`);
    assert.match(executionEvidence.run?.owner_id ?? '', /^rust:/, `${scenario}/rust owner evidence is invalid`);
  }
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

if (process.argv[1] && pathToFileURL(path.resolve(process.argv[1])).href === import.meta.url) {
  const [reportPath] = process.argv.slice(2);
  if (!reportPath) throw new Error('Usage: scheduler-acquisition-parity.mjs <runtime-evidence-report>');
  await verifySchedulerAcquisitionParity({ reportPath, repositoryRoot: process.cwd() });
}
