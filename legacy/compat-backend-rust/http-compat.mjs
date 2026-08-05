import crypto from 'node:crypto';
import fs from 'node:fs/promises';

const TRANSPORT_HEADERS = new Set([
  'connection',
  'content-length',
  'date',
  'keep-alive',
  'transfer-encoding',
]);

export async function loadCompatibilityInputs({ inventoryPath, fixturePath }) {
  const [inventorySource, fixtureSource] = await Promise.all([
    fs.readFile(inventoryPath, 'utf8'),
    fs.readFile(fixturePath, 'utf8'),
  ]);
  const inventory = JSON.parse(inventorySource);
  const manifest = JSON.parse(fixtureSource);
  if (inventory.schemaVersion !== 1) {
    throw new Error(`Unsupported route inventory schema: ${inventory.schemaVersion}`);
  }
  if (manifest.schemaVersion !== 1 || !Array.isArray(manifest.fixtures)) {
    throw new Error('Compatibility fixture manifest must use schemaVersion 1');
  }

  const inventoryRoutes = new Map(inventory.routes.map((route) => [route.id, route]));
  const fixtureIds = new Set();
  for (const fixture of manifest.fixtures) {
    if (!fixture.id || !fixture.routeId || !fixture.request?.path) {
      throw new Error('Each compatibility fixture needs id, routeId, and request.path');
    }
    if (fixtureIds.has(fixture.id)) {
      throw new Error(`Duplicate compatibility fixture id: ${fixture.id}`);
    }
    fixtureIds.add(fixture.id);
    const inventoried = inventoryRoutes.get(fixture.routeId);
    if (!inventoried) {
      throw new Error(`Fixture ${fixture.id} references unknown route ${fixture.routeId}`);
    }
    const method = String(fixture.request.method || 'GET').toUpperCase();
    const middlewareProbe = fixture.middlewareProbe === true && method === 'OPTIONS';
    if (method !== inventoried.method && !middlewareProbe) {
      throw new Error(
        `Fixture ${fixture.id} method ${method} does not match inventory ${inventoried.method}`,
      );
    }
    const inventoryProbePath = fixture.developerProbe === true
      ? stripDeveloperApiPrefix(fixture.request.path)
      : fixture.request.path;
    if (!pathMatchesInventory(inventoryProbePath, inventoried.path)) {
      throw new Error(
        `Fixture ${fixture.id} path ${fixture.request.path} does not match ${inventoried.path}`,
      );
    }
    for (const assertion of fixture.assertions || []) {
      const operators = ['equals', 'length'].filter((name) =>
        Object.hasOwn(assertion, name));
      if (!assertion.pointer || operators.length !== 1) {
        throw new Error(
          `Fixture ${fixture.id} assertions need pointer and exactly one of equals or length`,
        );
      }
      if (
        Object.hasOwn(assertion, 'length')
        && (!Number.isInteger(assertion.length) || assertion.length < 0)
      ) {
        throw new Error(`Fixture ${fixture.id} assertion length must be a non-negative integer`);
      }
    }
  }

  return {
    inventory,
    manifest,
    inventorySha256: sha256(inventorySource),
    fixtureSha256: sha256(fixtureSource),
  };
}

export async function compareHttpFixtures({
  typescriptBaseUrl,
  rustBaseUrl,
  inventoryPath,
  fixturePath,
  timeoutMs = 10_000,
}) {
  const inputs = await loadCompatibilityInputs({ inventoryPath, fixturePath });
  const results = [];
  for (const fixture of inputs.manifest.fixtures) {
    const [typescript, rust] = await Promise.all([
      issueRequest(
        typescriptBaseUrl,
        fixture,
        timeoutMs,
        inputs.manifest.defaultHeaders || {},
      ),
      issueRequest(
        rustBaseUrl,
        fixture,
        timeoutMs,
        inputs.manifest.defaultHeaders || {},
      ),
    ]);
    const differences = [
      ...compareSnapshots(typescript, rust, fixture.normalize || []),
      ...compareAssertions(typescript, fixture.assertions || [], 'typescript'),
      ...compareAssertions(rust, fixture.assertions || [], 'rust'),
    ];
    results.push({
      id: fixture.id,
      routeId: fixture.routeId,
      passed: differences.length === 0,
      differences,
      typescript,
      rust,
    });
  }

  const routes = [...new Set(results.map((result) => result.routeId))];
  const passingRoutes = routes.filter((routeId) => {
    const routeResults = results.filter((result) => result.routeId === routeId);
    return routeResults.length > 0 && routeResults.every((result) => result.passed);
  });
  return {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    inventorySha256: inputs.inventorySha256,
    fixtureSha256: inputs.fixtureSha256,
    endpoints: {
      typescript: typescriptBaseUrl,
      rust: rustBaseUrl,
    },
    summary: {
      fixtures: results.length,
      passedFixtures: results.filter((result) => result.passed).length,
      failedFixtures: results.filter((result) => !result.passed).length,
      coveredRoutes: routes.length,
      passingRoutes: passingRoutes.length,
    },
    passingRouteIds: passingRoutes,
    results,
  };
}

async function issueRequest(baseUrl, fixture, timeoutMs, defaultHeaders) {
  const url = new URL(fixture.request.path, ensureTrailingSlash(baseUrl));
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), fixture.timeoutMs || timeoutMs);
  try {
    const response = await fetch(url, {
      method: fixture.request.method || 'GET',
      headers: {
        ...defaultHeaders,
        ...fixture.request.headers,
      },
      body: encodeBody(fixture.request),
      redirect: 'manual',
      signal: controller.signal,
    });
    const text = await response.text();
    const contentType = response.headers.get('content-type') || '';
    let body = text;
    if (contentType.toLowerCase().includes('application/json') && text !== '') {
      try {
        body = JSON.parse(text);
      } catch {
        body = { $invalidJson: text };
      }
    }
    return {
      status: response.status,
      headers: snapshotHeaders(response.headers),
      body,
    };
  } catch (error) {
    return {
      transportError: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
    };
  } finally {
    clearTimeout(timer);
  }
}

function encodeBody(request) {
  if (request.body === undefined) return undefined;
  if (typeof request.body === 'string') return request.body;
  return JSON.stringify(request.body);
}

function snapshotHeaders(headers) {
  const snapshot = {};
  for (const [rawName, rawValue] of headers) {
    const name = rawName.toLowerCase();
    if (TRANSPORT_HEADERS.has(name)) continue;
    snapshot[name] = name === 'server-timing'
      ? normalizeServerTiming(rawValue)
      : name.endsWith('-ratelimit-reset')
        ? '<reset>'
        : name.endsWith('-ratelimit-remaining')
          ? '<remaining>'
        : rawValue;
  }
  return snapshot;
}

function normalizeServerTiming(value) {
  return value
    .split(',')
    .map((metric) => metric.trim().replace(/;dur=-?\d+(?:\.\d+)?/g, ';dur=<number>'))
    .join(',');
}

function compareSnapshots(leftInput, rightInput, normalizationRules) {
  const left = structuredClone(leftInput);
  const right = structuredClone(rightInput);
  const normalizationDifferences = [];
  for (const rule of normalizationRules) {
    if (!['omit', 'replace'].includes(rule.operation) || !rule.pointer) {
      throw new Error(`Invalid normalization rule ${JSON.stringify(rule)}`);
    }
    if (rule.operation === 'omit') {
      const leftFound = removeJsonPointer(left, rule.pointer);
      const rightFound = removeJsonPointer(right, rule.pointer);
      if (leftFound !== rightFound) {
        normalizationDifferences.push({
          pointer: rule.pointer,
          typescript: leftFound ? '<present>' : '<missing>',
          rust: rightFound ? '<present>' : '<missing>',
        });
      }
    } else {
      const leftFound = setJsonPointer(left, rule.pointer, rule.value);
      const rightFound = setJsonPointer(right, rule.pointer, rule.value);
      if (leftFound !== rightFound) {
        normalizationDifferences.push({
          pointer: rule.pointer,
          typescript: leftFound ? '<present>' : '<missing>',
          rust: rightFound ? '<present>' : '<missing>',
        });
      }
    }
  }
  return [...normalizationDifferences, ...deepDifferences(left, right)];
}

function compareAssertions(snapshot, assertions, runtime) {
  const differences = [];
  for (const assertion of assertions) {
    const resolved = getJsonPointer(snapshot, assertion.pointer);
    if (!resolved.found) {
      differences.push({
        kind: 'fixture-assertion',
        runtime,
        pointer: assertion.pointer,
        actual: '<missing>',
        expected: Object.hasOwn(assertion, 'equals')
          ? assertion.equals
          : { length: assertion.length },
      });
      continue;
    }
    if (Object.hasOwn(assertion, 'equals')) {
      if (deepDifferences(resolved.value, assertion.equals).length > 0) {
        differences.push({
          kind: 'fixture-assertion',
          runtime,
          pointer: assertion.pointer,
          actual: resolved.value,
          expected: assertion.equals,
        });
      }
      continue;
    }
    const actualLength = resolved.value?.length;
    if (actualLength !== assertion.length) {
      differences.push({
        kind: 'fixture-assertion',
        runtime,
        pointer: assertion.pointer,
        actual: actualLength ?? '<not-sized>',
        expected: assertion.length,
      });
    }
  }
  return differences;
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

function getJsonPointer(value, pointer) {
  if (pointer === '') return { found: true, value };
  if (!pointer.startsWith('/')) {
    throw new Error(`Assertion pointer must start with /: ${pointer}`);
  }
  const segments = pointer.slice(1).split('/').map(unescapePointerSegment);
  let current = value;
  for (const segment of segments) {
    if (current === null || typeof current !== 'object' || !(segment in current)) {
      return { found: false };
    }
    current = current[segment];
  }
  return { found: true, value: current };
}

function removeJsonPointer(value, pointer) {
  const resolved = resolveParent(value, pointer);
  if (!resolved || !(resolved.key in resolved.parent)) return false;
  if (Array.isArray(resolved.parent)) {
    const index = Number(resolved.key);
    if (Number.isInteger(index) && index >= 0 && index < resolved.parent.length) {
      resolved.parent.splice(index, 1);
      return true;
    }
    return false;
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

function pathMatchesInventory(actualPath, inventoryPath) {
  const actual = new URL(actualPath, 'http://compat.invalid').pathname.split('/').filter(Boolean);
  const expected = inventoryPath.split('/').filter(Boolean);
  if (actual.length !== expected.length) return false;
  return expected.every((segment, index) => segment.startsWith(':') || segment === actual[index]);
}

function stripDeveloperApiPrefix(actualPath) {
  const url = new URL(actualPath, 'http://compat.invalid');
  if (!url.pathname.startsWith('/v1/')) return actualPath;
  url.pathname = url.pathname.slice(3);
  return `${url.pathname}${url.search}`;
}

function ensureTrailingSlash(baseUrl) {
  return baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
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
