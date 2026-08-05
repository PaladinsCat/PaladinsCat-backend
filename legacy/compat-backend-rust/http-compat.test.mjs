import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { compareHttpFixtures, loadCompatibilityInputs } from './http-compat.mjs';

const repositoryRoot = path.resolve(import.meta.dirname, '../../..');
const inventoryPath = path.join(
  repositoryRoot,
  'documents/02-technical/migration/backend-rust-inventory.json',
);

test('fixed recovery fixtures are tied to inventoried routes', async () => {
  const inputs = await loadCompatibilityInputs({
    inventoryPath,
    fixturePath: path.join(import.meta.dirname, 'recovery-fixtures.json'),
  });
  assert.equal(inputs.manifest.fixtures.length, 21);
  assert.equal(inputs.inventory.totals.routes, 268);
});

test('comparator passes exact compatible responses and normalizes timing duration only', async (context) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), 'pc-http-compat-'));
  context.after(() => fs.rm(temporary, { recursive: true, force: true }));
  const fixturePath = path.join(temporary, 'fixtures.json');
  await fs.writeFile(fixturePath, JSON.stringify(manifest()));
  const left = await listen((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.setHeader('server-timing', 'app;dur=1.2');
    response.end(JSON.stringify({ value: 1, requestId: 'left' }));
  });
  const right = await listen((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.setHeader('server-timing', 'app;dur=9.8');
    response.end(JSON.stringify({ value: 1, requestId: 'right' }));
  });
  context.after(() => Promise.all([close(left.server), close(right.server)]));

  const report = await compareHttpFixtures({
    typescriptBaseUrl: left.url,
    rustBaseUrl: right.url,
    fixturePath,
    inventoryPath,
  });
  assert.equal(report.summary.failedFixtures, 0);
  assert.deepEqual(report.passingRouteIds, ['GET /recovery/pending']);
});

test('comparator reports an exact body mismatch', async (context) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), 'pc-http-compat-'));
  context.after(() => fs.rm(temporary, { recursive: true, force: true }));
  const fixturePath = path.join(temporary, 'fixtures.json');
  const input = manifest();
  input.fixtures[0].normalize = [];
  await fs.writeFile(fixturePath, JSON.stringify(input));
  const left = await listen((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ value: 1 }));
  });
  const right = await listen((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ value: 2 }));
  });
  context.after(() => Promise.all([close(left.server), close(right.server)]));

  const report = await compareHttpFixtures({
    typescriptBaseUrl: left.url,
    rustBaseUrl: right.url,
    fixturePath,
    inventoryPath,
  });
  assert.equal(report.summary.failedFixtures, 1);
  assert.equal(report.results[0].differences[0].pointer, '/body/value');
});

test('fixture assertions prevent two equally wrong implementations from passing', async (context) => {
  const temporary = await fs.mkdtemp(path.join(os.tmpdir(), 'pc-http-compat-'));
  context.after(() => fs.rm(temporary, { recursive: true, force: true }));
  const fixturePath = path.join(temporary, 'fixtures.json');
  const input = manifest();
  input.fixtures[0].normalize = [];
  input.fixtures[0].assertions = [
    { pointer: '/status', equals: 200 },
    { pointer: '/body/items', length: 2 },
  ];
  await fs.writeFile(fixturePath, JSON.stringify(input));
  const left = await listen((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ items: [1] }));
  });
  const right = await listen((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ items: [1] }));
  });
  context.after(() => Promise.all([close(left.server), close(right.server)]));

  const report = await compareHttpFixtures({
    typescriptBaseUrl: left.url,
    rustBaseUrl: right.url,
    fixturePath,
    inventoryPath,
  });
  assert.equal(report.summary.failedFixtures, 1);
  const assertionDifferences = report.results[0].differences.filter(
    difference => difference.kind === 'fixture-assertion',
  );
  assert.equal(assertionDifferences.length, 2);
  assert.deepEqual(
    assertionDifferences.map(difference => difference.runtime).sort(),
    ['rust', 'typescript'],
  );
});

function manifest() {
  return {
    schemaVersion: 1,
    fixtures: [{
      id: 'pending',
      routeId: 'GET /recovery/pending',
      request: { method: 'GET', path: '/recovery/pending' },
      normalize: [{ operation: 'omit', pointer: '/body/requestId' }],
    }],
  };
}

async function listen(handler) {
  const server = http.createServer(handler);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  return { server, url: `http://127.0.0.1:${address.port}` };
}

async function close(server) {
  await new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
}
