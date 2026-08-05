#!/usr/bin/env node
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { compareHttpFixtures } from './http-compat.mjs';

const argumentsByName = parseArguments(process.argv.slice(2));
const required = ['typescript-base-url', 'rust-base-url'];
for (const name of required) {
  if (!argumentsByName[name]) {
    throw new Error(`Missing required --${name}`);
  }
}

const repositoryRoot = path.resolve(import.meta.dirname, '../../..');
const fixturePath = path.resolve(
  argumentsByName.fixtures
    || path.join(import.meta.dirname, 'recovery-fixtures.json'),
);
const inventoryPath = path.resolve(
  argumentsByName.inventory
    || path.join(repositoryRoot, 'documents/02-technical/migration/backend-rust-inventory.json'),
);
const report = await compareHttpFixtures({
  typescriptBaseUrl: argumentsByName['typescript-base-url'],
  rustBaseUrl: argumentsByName['rust-base-url'],
  fixturePath,
  inventoryPath,
  timeoutMs: Number(argumentsByName['timeout-ms'] || 10_000),
});

if (argumentsByName.report) {
  const reportPath = path.resolve(argumentsByName.report);
  await fs.mkdir(path.dirname(reportPath), { recursive: true });
  await fs.writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);
}
const output = argumentsByName.output === 'summary'
  ? { summary: report.summary, passingRouteIds: report.passingRouteIds }
  : report;
process.stdout.write(`${JSON.stringify(output, null, 2)}\n`);
if (report.summary.failedFixtures > 0) process.exitCode = 1;

function parseArguments(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!value.startsWith('--')) throw new Error(`Unexpected argument ${value}`);
    const name = value.slice(2);
    const next = values[index + 1];
    if (!next || next.startsWith('--')) throw new Error(`Missing value for --${name}`);
    parsed[name] = next;
    index += 1;
  }
  return parsed;
}
