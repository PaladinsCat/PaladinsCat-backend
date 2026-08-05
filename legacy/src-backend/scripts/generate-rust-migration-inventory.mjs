import {
  mkdirSync,
  readFileSync,
  readdirSync,
  statSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const backendRoot = resolve(scriptDirectory, '..');
const repositoryRoot = resolve(backendRoot, '..', '..');
const outputDirectory = join(
  repositoryRoot,
  'documents',
  '02-technical',
  'migration',
);
const routeMethods = new Set([
  'get',
  'post',
  'put',
  'patch',
  'delete',
  'head',
  'options',
]);

function normalizedRelative(path) {
  return relative(repositoryRoot, path).split(sep).join('/');
}

function sourceFile(path) {
  const source = readFileSync(path, 'utf8');
  return {
    source,
    ast: ts.createSourceFile(
      path,
      source,
      ts.ScriptTarget.Latest,
      true,
      ts.ScriptKind.TS,
    ),
  };
}

function walk(node, visitor) {
  visitor(node);
  ts.forEachChild(node, child => walk(child, visitor));
}

function literalText(node) {
  if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
    return node.text;
  }
  return null;
}

function findVariableArray(ast, name) {
  let array = null;
  walk(ast, node => {
    if (
      ts.isVariableDeclaration(node)
      && node.name.getText(ast) === name
      && node.initializer
      && ts.isArrayLiteralExpression(node.initializer)
    ) {
      array = node.initializer;
    }
  });
  return array;
}

function enclosingForOf(node) {
  let current = node.parent;
  while (current) {
    if (ts.isForOfStatement(current)) return current;
    current = current.parent;
  }
  return null;
}

function routePathVariants(node, ast) {
  const literal = literalText(node);
  if (literal !== null) return [literal];
  if (!ts.isTemplateExpression(node)) return [];

  const loop = enclosingForOf(node);
  const declaration = loop?.initializer
    && ts.isVariableDeclarationList(loop.initializer)
    ? loop.initializer.declarations[0]
    : null;
  const loopVariable = declaration?.name.getText(ast);
  const collectionName = loop?.expression && ts.isIdentifier(loop.expression)
    ? loop.expression.text
    : null;
  const collection = collectionName ? findVariableArray(ast, collectionName) : null;
  if (!loopVariable || !collection) return [];

  const rows = collection.elements
    .filter(ts.isObjectLiteralExpression)
    .map(element => Object.fromEntries(
      element.properties
        .filter(ts.isPropertyAssignment)
        .map(property => [
          property.name.getText(ast).replace(/^['"]|['"]$/g, ''),
          literalText(property.initializer),
        ]),
    ));

  const variants = [];
  for (const row of rows) {
    let value = node.head.text;
    let resolvable = true;
    for (const span of node.templateSpans) {
      const expression = span.expression;
      if (
        !ts.isPropertyAccessExpression(expression)
        || expression.expression.getText(ast) !== loopVariable
      ) {
        resolvable = false;
        break;
      }
      const replacement = row[expression.name.text];
      if (typeof replacement !== 'string') {
        resolvable = false;
        break;
      }
      value += replacement + span.literal.text;
    }
    if (resolvable) variants.push(value);
  }
  return variants;
}

function objectProperty(object, name) {
  if (!object || !ts.isObjectLiteralExpression(object)) return null;
  for (const property of object.properties) {
    if (!ts.isPropertyAssignment(property)) continue;
    const propertyName = property.name.getText().replace(/^['"]|['"]$/g, '');
    if (propertyName === name) return property.initializer;
  }
  return null;
}

function joinRoute(prefix, path) {
  const joined = `${prefix === '/' ? '' : prefix}/${path === '/' ? '' : path}`
    .replace(/\/+/g, '/');
  return joined || '/';
}

function findFunctionArgument(call) {
  return [...call.arguments]
    .reverse()
    .find(argument => ts.isArrowFunction(argument) || ts.isFunctionExpression(argument));
}

function callSignals(text, source) {
  const names = [
    'requireAuth',
    'requireAdminSession',
    'requireUserSession',
    'getAuthenticatedUser',
    'guardDeveloperApi',
    'guardVendorFallback',
    'verifyApiKey',
  ];
  return names.filter(name => new RegExp(`\\b${name}\\b`).test(text || source));
}

function behaviorSignals(handlerText, relayImports) {
  const sqlMutation = /\b(INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|TRUNCATE)\b/i
    .test(handlerText);
  const database = /\b(query|one|transaction)\s*\(/.test(handlerText);
  const cache = /\b(cache|getCache|setCache|redis|invalidateCache|CACHE_)\b/i
    .test(handlerText);
  return {
    database: database ? (sqlMutation ? 'write-signal' : 'read-signal') : 'none-detected',
    cache: cache ? 'cache-signal' : 'none-detected',
    relayOperations: relayImports.filter(name =>
      new RegExp(`\\b${name}\\s*\\(`).test(handlerText),
    ),
  };
}

function routeRegistry() {
  const indexPath = join(backendRoot, 'index.ts');
  const { ast } = sourceFile(indexPath);
  const imports = new Map();
  const registrations = [];

  for (const statement of ast.statements) {
    if (!ts.isImportDeclaration(statement)) continue;
    const specifier = literalText(statement.moduleSpecifier);
    if (!specifier?.startsWith('./routes/')) continue;
    const defaultIdentifier = statement.importClause?.name?.text;
    if (defaultIdentifier) imports.set(defaultIdentifier, specifier);
    const bindings = statement.importClause?.namedBindings;
    if (bindings && ts.isNamedImports(bindings)) {
      for (const element of bindings.elements) {
        imports.set(element.name.text, specifier);
      }
    }
  }

  walk(ast, node => {
    if (!ts.isCallExpression(node) || !ts.isPropertyAccessExpression(node.expression)) return;
    if (node.expression.expression.getText(ast) !== 'fastify') return;
    if (node.expression.name.text !== 'register') return;
    const plugin = node.arguments[0];
    if (!plugin || !ts.isIdentifier(plugin)) return;
    const specifier = imports.get(plugin.text);
    if (!specifier) return;
    const prefixNode = objectProperty(node.arguments[1], 'prefix');
    registrations.push({
      plugin: plugin.text,
      specifier,
      prefix: literalText(prefixNode) ?? '/',
      file: resolve(backendRoot, `${specifier}.ts`),
    });
  });
  return registrations;
}

function collectRoutes() {
  const routes = [];
  for (const registration of routeRegistry()) {
    const { source, ast } = sourceFile(registration.file);
    const relayImports = [];
    for (const statement of ast.statements) {
      if (!ts.isImportDeclaration(statement)) continue;
      if (literalText(statement.moduleSpecifier) !== '../services/hirez') continue;
      const bindings = statement.importClause?.namedBindings;
      if (!bindings || !ts.isNamedImports(bindings)) continue;
      relayImports.push(...bindings.elements.map(element => element.name.text));
    }

    const moduleGuardHandlers = [];
    walk(ast, node => {
      if (!ts.isCallExpression(node) || !ts.isPropertyAccessExpression(node.expression)) return;
      if (node.expression.expression.getText(ast) !== 'fastify') return;
      if (node.expression.name.text !== 'addHook') return;
      if (literalText(node.arguments[0]) !== 'preHandler') return;
      const handler = findFunctionArgument(node);
      if (handler) moduleGuardHandlers.push(handler.getText(ast));
    });
    const moduleAuthSignals = callSignals(moduleGuardHandlers.join('\n'), '');

    walk(ast, node => {
      if (!ts.isCallExpression(node) || !ts.isPropertyAccessExpression(node.expression)) return;
      if (node.expression.expression.getText(ast) !== 'fastify') return;
      const method = node.expression.name.text.toLowerCase();
      if (!routeMethods.has(method)) return;
      const localPaths = routePathVariants(node.arguments[0], ast);
      if (localPaths.length === 0) {
        throw new Error(
          `Dynamic route path is not inventory-safe: ${normalizedRelative(registration.file)}:${ast.getLineAndCharacterOfPosition(node.getStart(ast)).line + 1}`,
        );
      }
      const handler = findFunctionArgument(node);
      const handlerText = handler?.getText(ast) ?? '';
      const line = ast.getLineAndCharacterOfPosition(node.getStart(ast)).line + 1;
      const authSignals = [
        ...new Set([
          ...moduleAuthSignals,
          ...callSignals(handlerText, source),
        ]),
      ];
      for (const localPath of localPaths) {
        const fullPath = joinRoute(registration.prefix, localPath);
        routes.push({
          id: `${method.toUpperCase()} ${fullPath}`,
          method: method.toUpperCase(),
          path: fullPath,
          prefix: registration.prefix,
          localPath,
          module: registration.specifier.replace('./routes/', ''),
          source: normalizedRelative(registration.file),
          line,
          auth: authSignals.length > 0 ? 'guard-signal' : 'public-or-manual-review',
          authSignals,
          ...behaviorSignals(handlerText, relayImports),
          fixture: 'required',
          migrationStatus: 'typescript',
        });
      }
    });
  }

  routes.sort((a, b) =>
    a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
  const duplicateIds = routes
    .map(route => route.id)
    .filter((id, index, all) => all.indexOf(id) !== index);
  if (duplicateIds.length > 0) {
    throw new Error(`Duplicate route identities: ${[...new Set(duplicateIds)].join(', ')}`);
  }
  if (routes.length !== 268) {
    throw new Error(`Expected the audited 268 concrete Fastify routes, found ${routes.length}`);
  }
  return routes;
}

function schedulerInventory() {
  const registryPath = join(backendRoot, 'workers', 'scheduler-registry.ts');
  const { ast } = sourceFile(registryPath);
  const imports = new Map();
  for (const statement of ast.statements) {
    if (!ts.isImportDeclaration(statement)) continue;
    const specifier = literalText(statement.moduleSpecifier);
    const bindings = statement.importClause?.namedBindings;
    if (!specifier || !bindings || !ts.isNamedImports(bindings)) continue;
    for (const element of bindings.elements) {
      imports.set(element.name.text, specifier);
    }
  }

  let schedulerArray = null;
  walk(ast, node => {
    if (
      ts.isVariableDeclaration(node)
      && node.name.getText(ast) === 'BACKEND_SCHEDULERS'
      && node.initializer
      && ts.isArrayLiteralExpression(node.initializer)
    ) {
      schedulerArray = node.initializer;
    }
  });
  if (!schedulerArray) throw new Error('BACKEND_SCHEDULERS was not statically discoverable');

  return schedulerArray.elements.map(element => {
    if (!ts.isObjectLiteralExpression(element)) {
      throw new Error('Scheduler registry entry must be an object literal');
    }
    const key = literalText(objectProperty(element, 'key'));
    const description = literalText(objectProperty(element, 'description'));
    const enableNode = objectProperty(element, 'enable');
    const disableNode = objectProperty(element, 'disable');
    const jobTypesNode = objectProperty(element, 'jobTypes');
    const jobTypes = jobTypesNode && ts.isArrayLiteralExpression(jobTypesNode)
      ? jobTypesNode.elements.map(literalText)
      : [];
    const enable = enableNode?.getText(ast) ?? '';
    const disable = disableNode?.getText(ast) ?? '';
    return {
      key,
      jobTypes,
      description,
      module: imports.get(enable)?.replace('./', '') ?? 'unknown',
      enable,
      disable,
      owner: 'typescript-backend',
      rustOwner: 'not-migrated',
      compatibilityFixture: 'required',
    };
  });
}

function exportedNames(ast) {
  const names = [];
  for (const statement of ast.statements) {
    const exported = statement.modifiers?.some(
      modifier => modifier.kind === ts.SyntaxKind.ExportKeyword,
    );
    if (!exported) continue;
    if (
      (ts.isFunctionDeclaration(statement) || ts.isClassDeclaration(statement))
      && statement.name
    ) {
      names.push(statement.name.text);
    } else if (ts.isVariableStatement(statement)) {
      for (const declaration of statement.declarationList.declarations) {
        names.push(declaration.name.getText(ast));
      }
    }
  }
  return names.sort();
}

function workerInventory(schedulers) {
  const workerDirectory = join(backendRoot, 'workers');
  const schedulerModules = new Map(
    schedulers.map(scheduler => [scheduler.module, scheduler.key]),
  );
  return readdirSync(workerDirectory)
    .filter(name => name.endsWith('.ts'))
    .sort()
    .map(name => {
      const path = join(workerDirectory, name);
      const { source, ast } = sourceFile(path);
      const module = `workers/${name.replace(/\.ts$/, '')}`;
      const relayOperations = [];
      for (const statement of ast.statements) {
        if (!ts.isImportDeclaration(statement)) continue;
        if (literalText(statement.moduleSpecifier) !== '../services/hirez') continue;
        const bindings = statement.importClause?.namedBindings;
        if (bindings && ts.isNamedImports(bindings)) {
          relayOperations.push(...bindings.elements.map(element => element.name.text));
        }
      }
      return {
        module,
        source: normalizedRelative(path),
        lines: source.split(/\r?\n/).length,
        exports: exportedNames(ast),
        schedulerKeys: schedulerModules.has(module)
          ? [schedulerModules.get(module)]
          : [],
        relayOperationImports: relayOperations.sort(),
        migrationStatus: 'typescript',
        compatibilityFixture: 'required',
      };
    });
}

function recursiveTypeScriptFiles(directory) {
  return readdirSync(directory).flatMap(name => {
    const path = join(directory, name);
    const stats = statSync(path);
    if (stats.isDirectory()) {
      if (['dist', 'node_modules'].includes(name)) return [];
      return recursiveTypeScriptFiles(path);
    }
    return name.endsWith('.ts') ? [path] : [];
  });
}

function environmentInventory() {
  const variables = new Map();
  for (const path of recursiveTypeScriptFiles(backendRoot)) {
    const source = readFileSync(path, 'utf8');
    const patterns = [
      /process\.env\.([A-Z][A-Z0-9_]*)/g,
      /process\.env\[['"]([A-Z][A-Z0-9_]*)['"]\]/g,
    ];
    for (const pattern of patterns) {
      for (const match of source.matchAll(pattern)) {
        const line = source.slice(0, match.index).split(/\r?\n/).length;
        const locations = variables.get(match[1]) ?? [];
        locations.push(`${normalizedRelative(path)}:${line}`);
        variables.set(match[1], locations);
      }
    }
  }
  return [...variables.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([name, locations]) => ({
      name,
      locations: [...new Set(locations)],
      rustMapping: 'required',
      defaultCompatibility: 'required',
    }));
}

function groupCounts(items, key) {
  return Object.fromEntries(
    [...items.reduce((groups, item) => {
      groups.set(item[key], (groups.get(item[key]) ?? 0) + 1);
      return groups;
    }, new Map()).entries()].sort(([a], [b]) => a.localeCompare(b)),
  );
}

const routes = collectRoutes();
const schedulers = schedulerInventory();
const workers = workerInventory(schedulers);
const environment = environmentInventory();
const inventory = {
  schemaVersion: 1,
  generatedFrom: 'repository source; regenerate with npm run migration:inventory',
  completionRule:
    'Every item must have a passing Rust compatibility fixture before TypeScript retirement.',
  totals: {
    routes: routes.length,
    routeModules: new Set(routes.map(route => route.module)).size,
    schedulerOwners: schedulers.length,
    workerModules: workers.length,
    environmentVariables: environment.length,
  },
  routeCountsByModule: groupCounts(routes, 'module'),
  routes,
  schedulers,
  workers,
  environment,
};

mkdirSync(outputDirectory, { recursive: true });
writeFileSync(
  join(outputDirectory, 'backend-rust-inventory.json'),
  `${JSON.stringify(inventory, null, 2)}\n`,
);

const summary = `# Rust backend migration inventory

Generated from source with:

\`\`\`powershell
cd src/backend
npm run migration:inventory
\`\`\`

Do not edit the JSON inventory by hand. Classification fields are conservative
signals; the compatibility fixtures named in the migration plan are the
authoritative behavior contract.

| Surface | Count |
| --- | ---: |
| HTTP routes | ${inventory.totals.routes} |
| Route modules | ${inventory.totals.routeModules} |
| Scheduler owners | ${inventory.totals.schedulerOwners} |
| Worker modules | ${inventory.totals.workerModules} |
| Environment variables | ${inventory.totals.environmentVariables} |

The machine-readable inventory is
\`documents/02-technical/migration/backend-rust-inventory.json\`.

## Route modules

| Module | Routes |
| --- | ---: |
${Object.entries(inventory.routeCountsByModule)
  .sort(([, a], [, b]) => b - a)
  .map(([module, count]) => `| \`${module}\` | ${count} |`)
  .join('\n')}

## Scheduler ownership

| Key | TypeScript module | Current owner | Rust state |
| --- | --- | --- | --- |
${schedulers
  .map(scheduler =>
    `| \`${scheduler.key}\` | \`${scheduler.module}\` | ${scheduler.owner} | ${scheduler.rustOwner} |`)
  .join('\n')}
`;
writeFileSync(
  join(outputDirectory, 'backend-rust-inventory.md'),
  summary,
);

console.log(JSON.stringify(inventory.totals));
