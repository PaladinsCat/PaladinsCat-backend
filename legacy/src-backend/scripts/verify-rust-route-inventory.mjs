import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const backendRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repositoryRoot = path.resolve(backendRoot, '..', '..');
const rustRoot = path.join(repositoryRoot, 'src', 'backend-rust', 'src');
const inventoryPath = path.join(
  repositoryRoot,
  'documents',
  '02-technical',
  'migration',
  'backend-rust-inventory.json',
);
const methods = new Set(['get', 'post', 'put', 'delete']);

function canonicalPath(value) {
  const normalized = value.replace(/\{[^/}]+\}|:[^/]+/g, ':parameter');
  return normalized.length > 1 ? normalized.replace(/\/+$/, '') : normalized;
}

function routeId(method, routePath) {
  return `${method.toUpperCase()} ${canonicalPath(routePath)}`;
}

function balancedCall(source, start) {
  let depth = 0;
  let quote = null;
  let escaped = false;
  for (let index = start; index < source.length; index += 1) {
    const character = source[index];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (character === '\\') {
        escaped = true;
      } else if (character === quote) {
        quote = null;
      }
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
    } else if (character === '(') {
      depth += 1;
    } else if (character === ')') {
      depth -= 1;
      if (depth === 0) return source.slice(start, index + 1);
    }
  }
  throw new Error('Unterminated Axum .route(...) call');
}

function routesInSource(source, relativePath) {
  const routes = [];
  const referenceSpecRoutes = relativePath.endsWith('/routes/reference.rs')
    ? [...source.matchAll(/\broute:\s*"([^"]+)"/g)].map((match) => match[1])
    : [];
  const marker = '.route(';
  let offset = 0;
  while (true) {
    const markerIndex = source.indexOf(marker, offset);
    if (markerIndex < 0) break;
    const call = balancedCall(source, markerIndex + marker.length - 1);
    offset = markerIndex + marker.length + call.length;
    const pathMatch = call.match(/^\(\s*"([^"]+)"/);
    const dynamicReference = !pathMatch && referenceSpecRoutes.length > 0
      ? call.match(/format!\(\s*"\/reference\/\{\}(?:\/\{\{id\}\})?"/)
      : null;
    if (!pathMatch && !dynamicReference) {
      throw new Error(`Dynamic Axum route path is not inventory-safe in ${relativePath}`);
    }
    const methodMatches = [...call.matchAll(/\b(get|post|put|delete)\s*\(/g)]
      .map((match) => match[1])
      .filter((method) => methods.has(method));
    if (methodMatches.length === 0) {
      throw new Error(
        `No supported HTTP method found for ${pathMatch?.[1] ?? 'dynamic route'} in ${relativePath}`,
      );
    }
    const paths = pathMatch
      ? [pathMatch[1]]
      : referenceSpecRoutes.map((route) => (
          call.includes('{{id}}') ? `/reference/${route}/{id}` : `/reference/${route}`
        ));
    for (const routePath of paths) {
      for (const method of new Set(methodMatches)) {
        routes.push({
          id: routeId(method, routePath),
          method: method.toUpperCase(),
          path: routePath,
          source: relativePath,
        });
      }
    }
  }
  return routes;
}

async function rustFiles(directory) {
  const files = [];
  for (const entry of await fs.readdir(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...await rustFiles(absolute));
    } else if (entry.isFile() && entry.name.endsWith('.rs')) {
      files.push(absolute);
    }
  }
  return files;
}

const inventory = JSON.parse(await fs.readFile(inventoryPath, 'utf8'));
const expected = new Map(
  inventory.routes.map((route) => [
    routeId(route.method.toLowerCase(), route.path),
    route,
  ]),
);
const productionSources = [
  path.join(rustRoot, 'lib.rs'),
  ...await rustFiles(path.join(rustRoot, 'routes')),
];
const actualRoutes = [];
for (const absolute of productionSources) {
  const relative = path.relative(repositoryRoot, absolute).replaceAll('\\', '/');
  actualRoutes.push(...routesInSource(await fs.readFile(absolute, 'utf8'), relative));
}

const actual = new Map();
for (const route of actualRoutes) {
  const existing = actual.get(route.id) ?? [];
  existing.push(route);
  actual.set(route.id, existing);
}

const missing = [...expected.keys()].filter((id) => !actual.has(id));
const aliases = [...actual.entries()]
  .filter(([id, routes]) => expected.has(id) && routes.length > 1)
  .map(([id, routes]) => ({
    id,
    paths: [...new Set(routes.map((route) => route.path))],
    sources: [...new Set(routes.map((route) => route.source))],
  }));
const duplicateRegistrations = actualRoutes
  .reduce((groups, route) => {
    const key = `${route.method} ${route.path}`;
    const rows = groups.get(key) ?? [];
    rows.push(route);
    groups.set(key, rows);
    return groups;
  }, new Map());
const duplicates = [...duplicateRegistrations.entries()]
  .filter(([, routes]) => routes.length > 1)
  .map(([id, routes]) => ({ id, sources: routes.map((route) => route.source) }));
const extras = [...actual.entries()]
  .filter(([id]) => !expected.has(id))
  .flatMap(([, routes]) => routes);

const report = {
  inventoryRoutes: expected.size,
  coveredInventoryRoutes: expected.size - missing.length,
  parsedRustRoutes: actualRoutes.length,
  missing,
  duplicates,
  aliases,
  extras,
};
process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
if (missing.length > 0 || duplicates.length > 0) process.exitCode = 1;
