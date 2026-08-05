import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';

export async function compareWorkerTraceFiles({
  typescriptTracePath,
  rustTracePath,
  manifestPath,
}) {
  const [typescriptSource, rustSource, manifestSource] = await Promise.all([
    fs.readFile(typescriptTracePath, 'utf8'),
    fs.readFile(rustTracePath, 'utf8'),
    fs.readFile(manifestPath, 'utf8'),
  ]);
  const manifest = JSON.parse(manifestSource);
  validateManifest(manifest);
  const typescript = parseTrace(typescriptSource, 'TypeScript');
  const rust = parseTrace(rustSource, 'Rust');
  const normalizedTypescript = normalizeTrace(typescript, manifest.normalize || []);
  const normalizedRust = normalizeTrace(rust, manifest.normalize || []);
  const differences = deepDifferences(normalizedTypescript, normalizedRust);
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    manifestSha256: sha256(manifestSource),
    traces: {
      typescriptSha256: sha256(typescriptSource),
      rustSha256: sha256(rustSource),
    },
    summary: {
      typescriptEvents: typescript.length,
      rustEvents: rust.length,
      differences: differences.length,
      passed: differences.length === 0,
    },
    differences: differences.slice(0, 100),
  };
}
export function parseTrace(source, label = 'trace') {
  const events = [];
  for (const [index, rawLine] of source.split(/\r?\n/).entries()) {
    const line = rawLine.trim();
    if (!line) continue;
    let event;
    try {
      event = JSON.parse(line);
    } catch (error) {
      throw new Error(`${label} line ${index + 1} is not valid JSON: ${error.message}`);
    }
    if (!event || typeof event !== 'object' || Array.isArray(event)) {
      throw new Error(`${label} line ${index + 1} must be a JSON object`);
    }
    for (const field of ['worker', 'event']) {
      if (typeof event[field] !== 'string' || event[field] === '') {
        throw new Error(`${label} line ${index + 1} requires string field ${field}`);
      }
    }
    events.push(event);
  }
  return events;
}

export function normalizeTrace(events, rules) {
  const normalized = structuredClone(events);
  for (const [index, event] of normalized.entries()) {
    for (const rule of rules) {
      if (rule.worker && rule.worker !== event.worker) continue;
      if (rule.event && rule.event !== event.event) continue;
      if (!['omit', 'replace'].includes(rule.operation) || !rule.pointer) {
        throw new Error(`Invalid worker trace normalization rule ${JSON.stringify(rule)}`);
      }
      const found = rule.operation === 'omit'
        ? removeJsonPointer(event, rule.pointer)
        : setJsonPointer(event, rule.pointer, rule.value);
      if (!found) {
        throw new Error(
          `Worker trace normalization ${rule.pointer} did not match event ${index} `
          + `${event.worker}:${event.event}`,
        );
      }
    }
  }
  return normalized;
}

export function validateManifest(manifest) {
  if (manifest.schemaVersion !== 1) {
    throw new Error('Worker trace manifest must use schemaVersion 1');
  }
  if (manifest.normalize !== undefined && !Array.isArray(manifest.normalize)) {
    throw new Error('Worker trace normalize must be an array');
  }
}

function deepDifferences(left, right, pointer = '') {
  if (Object.is(left, right)) return [];
  if (typeof left !== typeof right || left === null || right === null) {
    return [{ pointer: pointer || '/', typescript: left, rust: right }];
  }
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right)) {
      return [{ pointer: pointer || '/', typescript: left, rust: right }];
    }
    const differences = [];
    const length = Math.max(left.length, right.length);
    for (let index = 0; index < length; index += 1) {
      differences.push(...deepDifferences(left[index], right[index], `${pointer}/${index}`));
    }
    return differences;
  }
  if (typeof left === 'object') {
    const differences = [];
    const keys = [...new Set([...Object.keys(left), ...Object.keys(right)])].sort();
    for (const key of keys) {
      differences.push(
        ...deepDifferences(left[key], right[key], `${pointer}/${escapePointerSegment(key)}`),
      );
    }
    return differences;
  }
  return [{ pointer: pointer || '/', typescript: left, rust: right }];
}

function removeJsonPointer(value, pointer) {
  const resolved = resolveParent(value, pointer);
  if (!resolved || !(resolved.key in resolved.parent)) return false;
  if (Array.isArray(resolved.parent)) {
    const index = Number(resolved.key);
    if (!Number.isInteger(index) || index < 0 || index >= resolved.parent.length) return false;
    resolved.parent.splice(index, 1);
    return true;
  }
  delete resolved.parent[resolved.key];
  return true;
}

function setJsonPointer(value, pointer, replacement) {
  const resolved = resolveParent(value, pointer);
  if (!resolved || !(resolved.key in resolved.parent)) return false;
  resolved.parent[resolved.key] = replacement;
  return true;
}

function resolveParent(value, pointer) {
  if (!pointer.startsWith('/') || pointer === '/') {
    throw new Error(`Normalization pointer must target a child: ${pointer}`);
  }
  const segments = pointer.slice(1).split('/').map(unescapePointerSegment);
  let parent = value;
  for (const segment of segments.slice(0, -1)) {
    if (parent === null || typeof parent !== 'object' || !(segment in parent)) return null;
    parent = parent[segment];
  }
  if (parent === null || typeof parent !== 'object') return null;
  return { parent, key: segments.at(-1) };
}

function escapePointerSegment(value) {
  return value.replaceAll('~', '~0').replaceAll('/', '~1');
}

function unescapePointerSegment(value) {
  return value.replaceAll('~1', '/').replaceAll('~0', '~');
}

function sha256(value) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

function parseArguments(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const argument = values[index];
    if (!argument.startsWith('--')) throw new Error(`Unexpected argument ${argument}`);
    const name = argument.slice(2);
    const value = values[index + 1];
    if (!value || value.startsWith('--')) throw new Error(`Missing value for --${name}`);
    parsed[name] = value;
    index += 1;
  }
  return parsed;
}

async function main() {
  const args = parseArguments(process.argv.slice(2));
  for (const required of ['typescript-trace', 'rust-trace', 'manifest']) {
    if (!args[required]) throw new Error(`Missing required --${required}`);
  }
  const report = await compareWorkerTraceFiles({
    typescriptTracePath: path.resolve(args['typescript-trace']),
    rustTracePath: path.resolve(args['rust-trace']),
    manifestPath: path.resolve(args.manifest),
  });
  if (args.report) {
    const reportPath = path.resolve(args.report);
    await fs.mkdir(path.dirname(reportPath), { recursive: true });
    await fs.writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  }
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (!report.summary.passed) process.exitCode = 1;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  void main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
