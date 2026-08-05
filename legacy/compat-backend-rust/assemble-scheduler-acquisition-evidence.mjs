import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import { verifySchedulerAcquisitionParity } from './scheduler-acquisition-parity.mjs';
import { schedulerParitySourceHash } from './scheduler-parity-source-hash.mjs';

const [manifestPath, reportPath] = process.argv.slice(2);
if (!manifestPath || !reportPath) throw new Error('usage: assemble-scheduler-acquisition-evidence.mjs <manifest> <report>');
const manifest = JSON.parse(await fs.readFile(manifestPath, 'utf8'));
const root = path.resolve(manifest.repositoryRoot);
const sha256 = value => crypto.createHash('sha256').update(value).digest('hex');
const readHash = async relative => sha256(await fs.readFile(path.resolve(root, relative)));
const scenarios = {};
const transcript = [];
for (const [scenario, runtimes] of Object.entries(manifest.captures)) {
  scenarios[scenario] = {};
  for (const [runtime, capture] of Object.entries(runtimes)) {
    const tracePath = path.resolve(root, capture.trace.path);
    const raw = await fs.readFile(tracePath, 'utf8');
    const events = raw.split(/\r?\n/).filter(Boolean).map(JSON.parse);
    transcript.push(...events.filter(event => event.type.startsWith('relay.')).map(({ runtime: _runtime, scenario: _scenario, ...event }) => {
      if (event.type !== 'relay.request') return event;
      const request = JSON.parse(event.payload.body);
      delete request.requestId;
      const body = JSON.stringify(request);
      return { ...event, payload: { ...event.payload, body, sha256: sha256(body) } };
    }));
    scenarios[scenario][runtime] = {
      ...capture,
      trace: { ...capture.trace, sha256: sha256(raw) },
    };
  }
}
const fixtures = {};
for (const [scenario, fixturePath] of Object.entries(manifest.fixtures)) {
  const raw = await fs.readFile(path.resolve(root, fixturePath));
  const fixture = JSON.parse(raw.toString('utf8'));
  fixtures[scenario] = { id: fixture.id, responseSha256: sha256(raw) };
}
const report = {
  schemaVersion: 3,
  kind: 'scheduler-acquisition-runtime-evidence',
  provenance: {
    database: { ...manifest.database, destroyed: true },
    source: {
      commit: execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim(),
      worktreeSha256: schedulerParitySourceHash(root),
    },
    builds: {
      typescript: { path: manifest.builds.typescript, sha256: await readHash(manifest.builds.typescript) },
      rust: { path: manifest.builds.rust, sha256: await readHash(manifest.builds.rust) },
    },
    fixtures,
    relay: { transcriptSha256: sha256(JSON.stringify(transcript)) },
  },
  scenarios,
};
const absoluteReport = path.resolve(reportPath);
const pending = `${absoluteReport}.${manifest.database.runMarker}.pending`;
await fs.mkdir(path.dirname(absoluteReport), { recursive: true });
await fs.writeFile(pending, `${JSON.stringify(report, null, 2)}\n`);
try {
  await verifySchedulerAcquisitionParity({ reportPath: pending, repositoryRoot: root });
  await fs.rename(pending, absoluteReport);
} catch (error) {
  await fs.rm(pending, { force: true });
  throw error;
}
