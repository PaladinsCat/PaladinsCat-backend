import fs from 'node:fs/promises';
import { startFixtureRelayCapture } from './local-relay-trace-capture.mjs';

const values = Object.fromEntries(process.argv.slice(2).map((value, index, all) =>
  value.startsWith('--') ? [value.slice(2), all[index + 1]] : null,
).filter(Boolean));
for (const key of ['fixture', 'fixture-sha256', 'trace', 'runtime', 'scenario', 'marker', 'ready']) {
  if (!values[key] || values[key].startsWith('--')) throw new Error(`missing --${key}`);
}
const capture = await startFixtureRelayCapture({
  fixturePath: values.fixture,
  fixtureSha256: values['fixture-sha256'],
  tracePath: values.trace,
  runtime: values.runtime,
  scenario: values.scenario,
  runMarker: values.marker,
});
await fs.writeFile(values.ready, `${capture.url}\n`, 'utf8');
const stop = async () => { await capture.close(); process.exit(0); };
process.once('SIGINT', stop);
process.once('SIGTERM', stop);
