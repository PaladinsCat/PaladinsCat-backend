import { execFileSync, spawnSync } from 'node:child_process';
import {
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname, extname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

const scriptDirectory = dirname(fileURLToPath(import.meta.url));
const backendRoot = resolve(scriptDirectory, '..');
const repositoryRoot = resolve(backendRoot, '..', '..');
const mapRoot = join(repositoryRoot, 'documents', '02-technical', 'code-map', 'backend');
const databaseWriter = join(repositoryRoot, 'scripts', 'code-map', 'write-code-map-db.py');
const generatedModulesDirectory = join(mapRoot, 'modules');
const generatedAreasDirectory = join(mapRoot, 'areas');
const databasePath = join(mapRoot, 'code-map.sqlite');

const sourceRoots = [
  {
    id: 'typescript-backend',
    label: 'Legacy TypeScript backend',
    path: join(repositoryRoot, 'src', 'backend'),
    language: 'typescript',
    extensions: new Set(['.ts']),
    tsConfig: join(repositoryRoot, 'src', 'backend', 'tsconfig.json'),
    excludedDirectories: new Set(['dist', 'node_modules', 'test', 'tests']),
    excludedFileSuffixes: [],
  },
  {
    id: 'frontend',
    label: 'Next.js frontend',
    path: join(repositoryRoot, 'src', 'frontend'),
    language: 'typescript',
    extensions: new Set(['.ts', '.tsx']),
    tsConfig: join(repositoryRoot, 'src', 'frontend', 'tsconfig.json'),
    excludedDirectories: new Set(['.next', 'node_modules', 'out', 'public']),
    excludedFileSuffixes: ['.test.ts', '.test.tsx'],
  },
  {
    id: 'discord-bot',
    label: 'Discord bot',
    path: join(repositoryRoot, 'src', 'discord-bot'),
    language: 'typescript',
    extensions: new Set(['.ts']),
    tsConfig: join(repositoryRoot, 'src', 'discord-bot', 'tsconfig.json'),
    excludedDirectories: new Set(['dist', 'node_modules', 'test']),
    excludedFileSuffixes: ['.test.ts'],
  },
  {
    id: 'rust-api',
    label: 'Rust API and worker candidate',
    path: join(repositoryRoot, 'src', 'backend-rust', 'src'),
    language: 'rust',
    extensions: new Set(['.rs']),
    excludedDirectories: new Set(['target']),
    excludedFileSuffixes: [],
  },
  {
    id: 'rust-relay',
    label: 'Rust HirezRelay',
    path: join(repositoryRoot, 'src', 'hirez-relay-rust', 'src'),
    language: 'rust',
    extensions: new Set(['.rs']),
    excludedDirectories: new Set(['target']),
    excludedFileSuffixes: [],
  },
  {
    id: 'rust-core',
    label: 'Shared Rust core',
    path: join(repositoryRoot, 'src', 'paladinscat-core', 'src'),
    language: 'rust',
    extensions: new Set(['.rs']),
    excludedDirectories: new Set(['target']),
    excludedFileSuffixes: [],
  },
];

const rustCrates = new Map([
  ['paladinscat_backend', 'rust-api'],
  ['paladinscat_hirez_relay', 'rust-relay'],
  ['paladinscat_core', 'rust-core'],
]);

const modules = [];
const symbols = [];
const relations = [];
const moduleByPath = new Map();
const symbolByDeclaration = new Map();
let nextModuleId = 1;
let nextSymbolId = 1;
let nextRelationId = 1;

function repositoryPath(path) {
  return relative(repositoryRoot, path).split(sep).join('/');
}

function lineAt(text, position) {
  return text.slice(0, position).split(/\r?\n/).length;
}

function lineRange(sourceFile, start, end) {
  return {
    startLine: sourceFile.getLineAndCharacterOfPosition(start).line + 1,
    endLine: sourceFile.getLineAndCharacterOfPosition(end).line + 1,
  };
}

function condensed(text, maximum = 220) {
  const value = text.replace(/\s+/g, ' ').trim();
  return value.length > maximum ? `${value.slice(0, maximum - 1)}…` : value;
}

function markdown(text) {
  return String(text).replace(/\|/g, '\\|').replace(/`/g, '\\`').replace(/\r?\n/g, ' ');
}

function contractMarkdown(text) {
  return String(text).replace(/\|/g, '\\|').replace(/\r?\n/g, ' ');
}

function slug(text) {
  return text.replace(/[^a-zA-Z0-9]+/g, '-').replace(/^-|-$/g, '').toLowerCase();
}

function sorted(items, selector) {
  return [...items].sort((left, right) => selector(left).localeCompare(selector(right)));
}

function walkFiles(directory, extensions, excludedDirectories, excludedFileSuffixes) {
  return readdirSync(directory, { withFileTypes: true })
    .flatMap(entry => {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) {
        return excludedDirectories.has(entry.name)
          ? []
          : walkFiles(path, extensions, excludedDirectories, excludedFileSuffixes);
      }
      return entry.isFile()
        && extensions.has(extname(entry.name))
        && !excludedFileSuffixes.some(suffix => entry.name.endsWith(suffix))
        ? [path]
        : [];
    });
}

function areaFor(root, path) {
  const parts = relative(root.path, path).split(sep);
  const branch = parts.length > 1 ? parts[0] : 'root';
  return `${root.id}/${branch.replace(/\.[^.]+$/, '')}`;
}

function addModule(root, path) {
  const text = readFileSync(path, 'utf8');
  const module = {
    id: nextModuleId++,
    path: repositoryPath(path),
    absolutePath: path,
    sourceRootId: root.id,
    area: areaFor(root, path),
    language: root.language,
    lineCount: text.split(/\r?\n/).length,
    text,
  };
  modules.push(module);
  moduleByPath.set(resolve(path), module);
  return module;
}

function addSymbol(module, values, declarationPath = null) {
  const symbol = {
    id: nextSymbolId++,
    moduleId: module.id,
    ...values,
  };
  symbols.push(symbol);
  if (declarationPath) symbolByDeclaration.set(declarationPath, symbol);
  return symbol;
}

function addRelation(values) {
  const sourceModule = modules.find(module => module.id === values.sourceModuleId);
  const targetModule = values.targetModuleId
    ? modules.find(module => module.id === values.targetModuleId)
    : null;
  if (!sourceModule || (!targetModule && !values.targetSymbolId)) return;
  const duplicate = relations.some(relation =>
    relation.kind === values.kind
    && relation.sourceModuleId === values.sourceModuleId
    && relation.sourceSymbolId === (values.sourceSymbolId ?? null)
    && relation.targetModuleId === (values.targetModuleId ?? null)
    && relation.targetSymbolId === (values.targetSymbolId ?? null)
    && relation.referenceLine === values.referenceLine,
  );
  if (duplicate) return;
  relations.push({ id: nextRelationId++, contractPath: null, ...values });
}

function declarationKey(sourceFile, node) {
  return `${resolve(sourceFile.fileName)}:${node.getStart(sourceFile)}`;
}

function hasModifier(node, kind) {
  return node.modifiers?.some(modifier => modifier.kind === kind) ?? false;
}

function typescriptSymbols(module, sourceFile) {
  function addNodeSymbol(node, name, qualifiedName, kind, signatureNode = node) {
    const range = lineRange(sourceFile, node.getStart(sourceFile), node.getEnd());
    const signatureEnd = 'body' in signatureNode && signatureNode.body
      ? signatureNode.body.getStart(sourceFile)
      : signatureNode.getEnd();
    return addSymbol(module, {
      name,
      qualifiedName,
      kind,
      visibility: hasModifier(node, ts.SyntaxKind.ExportKeyword) ? 'exported' : 'internal',
      ...range,
      signature: condensed(sourceFile.text.slice(node.getStart(sourceFile), signatureEnd)),
    }, declarationKey(sourceFile, node));
  }

  function visit(node, className = null) {
    if (ts.isClassDeclaration(node) && node.name) {
      addNodeSymbol(node, node.name.text, node.name.text, 'class');
      ts.forEachChild(node, child => visit(child, node.name.text));
      return;
    }
    if (ts.isInterfaceDeclaration(node)) {
      addNodeSymbol(node, node.name.text, node.name.text, 'interface');
    } else if (ts.isEnumDeclaration(node)) {
      addNodeSymbol(node, node.name.text, node.name.text, 'enum');
    } else if (ts.isTypeAliasDeclaration(node)) {
      addNodeSymbol(node, node.name.text, node.name.text, 'type');
    } else if (ts.isFunctionDeclaration(node) && node.name) {
      addNodeSymbol(node, node.name.text, node.name.text, 'function');
    } else if (
      (ts.isMethodDeclaration(node) || ts.isGetAccessorDeclaration(node) || ts.isSetAccessorDeclaration(node))
      && node.name
    ) {
      const name = node.name.getText(sourceFile);
      addNodeSymbol(node, name, className ? `${className}.${name}` : name, 'method');
    } else if (ts.isConstructorDeclaration(node)) {
      addNodeSymbol(node, 'constructor', className ? `${className}.constructor` : 'constructor', 'constructor');
    } else if (ts.isVariableDeclaration(node) && node.initializer
      && (ts.isArrowFunction(node.initializer) || ts.isFunctionExpression(node.initializer))
      && ts.isIdentifier(node.name)) {
      const name = node.name.text;
      addNodeSymbol(node, name, className ? `${className}.${name}` : name, 'function');
    }
    ts.forEachChild(node, child => visit(child, className));
  }

  visit(sourceFile);
}

function moduleFromTypeScriptSpecifier(containingFile, specifier, compilerOptions) {
  const resolved = ts.resolveModuleName(specifier, containingFile, compilerOptions, ts.sys)
    .resolvedModule?.resolvedFileName;
  if (!resolved) return null;
  return moduleByPath.get(resolve(resolved)) ?? null;
}

function enclosingTypeScriptSymbol(sourceFile, node) {
  let current = node.parent;
  while (current) {
    const found = symbolByDeclaration.get(declarationKey(sourceFile, current));
    if (found) return found;
    current = current.parent;
  }
  return null;
}

function resolveTypeScriptCall(checker, node) {
  const location = ts.isPropertyAccessExpression(node.expression)
    ? node.expression.name
    : node.expression;
  let resolved = checker.getSymbolAtLocation(location);
  if (resolved?.flags & ts.SymbolFlags.Alias) {
    resolved = checker.getAliasedSymbol(resolved);
  }
  const declaration = resolved?.getDeclarations()
    ?.find(candidate => symbolByDeclaration.has(declarationKey(candidate.getSourceFile(), candidate)));
  return declaration
    ? symbolByDeclaration.get(declarationKey(declaration.getSourceFile(), declaration))
    : null;
}

function typescriptRelations(module, sourceFile, checker, compilerOptions) {
  function visit(node) {
    if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) {
      const moduleSpecifier = node.moduleSpecifier;
      if (moduleSpecifier && ts.isStringLiteral(moduleSpecifier)) {
        const target = moduleFromTypeScriptSpecifier(sourceFile.fileName, moduleSpecifier.text, compilerOptions);
        if (target) {
          addRelation({
            kind: 'imports',
            sourceModuleId: module.id,
            sourceSymbolId: null,
            targetModuleId: target.id,
            targetSymbolId: null,
            referenceLine: lineRange(sourceFile, node.getStart(sourceFile), node.getEnd()).startLine,
            resolution: 'typescript-module-resolution',
          });
        }
      }
    } else if (ts.isCallExpression(node)) {
      const source = enclosingTypeScriptSymbol(sourceFile, node);
      const target = source ? resolveTypeScriptCall(checker, node) : null;
      if (source && target && source.id !== target.id) {
        addRelation({
          kind: 'calls',
          sourceModuleId: module.id,
          sourceSymbolId: source.id,
          targetModuleId: target.moduleId,
          targetSymbolId: target.id,
          referenceLine: lineRange(sourceFile, node.getStart(sourceFile), node.getEnd()).startLine,
          resolution: 'typescript-type-checker',
        });
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
}

function typeScriptCompilerOptions(root) {
  const configuration = ts.readConfigFile(root.tsConfig, ts.sys.readFile);
  if (configuration.error) {
    throw new Error(ts.flattenDiagnosticMessageText(configuration.error.messageText, '\n'));
  }
  const parsed = ts.parseJsonConfigFileContent(
    configuration.config,
    ts.sys,
    dirname(root.tsConfig),
    { noEmit: true, incremental: false },
    root.tsConfig,
  );
  if (parsed.errors.length > 0) {
    throw new Error(parsed.errors.map(error => ts.flattenDiagnosticMessageText(error.messageText, '\n')).join('\n'));
  }
  return parsed.options;
}

function sourceHttpTarget(node, sourceFile) {
  if (!node) return null;
  const text = node.getText(sourceFile);
  if (!/(?:\b(?:NEXT_(?:PUBLIC|SERVER)_API_URL|PALADINSCAT_API_URL|API_BASE|apiUrl|baseUrl)\b|\/api(?:\/|['"`])|\/_pc(?:\/|['"`])|\bbackend\b)/i.test(text)) {
    return null;
  }
  return condensed(text, 180);
}

function addHttpContract(module, sourceSymbol, targetModule, sourceFile, node, contractPath, resolution) {
  addRelation({
    kind: 'http_contract',
    sourceModuleId: module.id,
    sourceSymbolId: sourceSymbol?.id ?? null,
    targetModuleId: targetModule.id,
    targetSymbolId: null,
    referenceLine: lineRange(sourceFile, node.getStart(sourceFile), node.getEnd()).startLine,
    contractPath,
    resolution,
  });
}

function crossStackContracts(module, sourceFile) {
  if (!['frontend', 'discord-bot'].includes(module.sourceRootId)) return;
  const backendEntry = moduleByPath.get(resolve(repositoryRoot, 'src', 'backend', 'index.ts'));
  const discordRenderer = moduleByPath.get(resolve(repositoryRoot, 'src', 'discord-bot', 'src', 'health.ts'));
  if (!backendEntry) throw new Error('Expected the TypeScript backend entry point for stack contract edges');
  if (!discordRenderer) throw new Error('Expected the Discord renderer boundary for stack contract edges');
  if (module.sourceRootId === 'frontend' && module.path === 'src/frontend/next.config.ts') {
    addHttpContract(
      module,
      null,
      backendEntry,
      sourceFile,
      sourceFile,
      '/api/:path* and /_pc/:path* -> backend/:path*',
      'next-rewrite-proxy',
    );
  }
  if (
    module.sourceRootId === 'frontend'
    && module.path === 'src/frontend/lib/types.gen.ts'
    && /Auto-generated TypeScript types from Fastify backend API spec/.test(module.text)
  ) {
    addHttpContract(
      module,
      null,
      backendEntry,
      sourceFile,
      sourceFile,
      'Fastify OpenAPI-generated TypeScript types',
      'openapi-generated-contract',
    );
  }
  function visit(node) {
    if (!ts.isCallExpression(node)) {
      ts.forEachChild(node, visit);
      return;
    }
    const isFetchCall = ts.isIdentifier(node.expression)
      ? node.expression.text === 'fetch'
      : ts.isPropertyAccessExpression(node.expression)
        && ['fetch', 'fetchImpl'].includes(node.expression.name.text);
    const requestText = node.arguments[0]?.getText(sourceFile) ?? '';
    const rendererRequest = /\b(?:PALADINSCAT_RENDER_URL|rendererUrl)\b/.test(requestText);
    const targetText = isFetchCall
      ? rendererRequest ? condensed(requestText, 180) : sourceHttpTarget(node.arguments[0], sourceFile)
      : null;
    if (targetText) {
      const sourceSymbol = enclosingTypeScriptSymbol(sourceFile, node);
      addHttpContract(
        module,
        sourceSymbol,
        rendererRequest ? discordRenderer : backendEntry,
        sourceFile,
        node,
        targetText,
        rendererRequest ? 'frontend-discord-renderer-client'
          : module.sourceRootId === 'frontend' ? 'frontend-http-client' : 'discord-http-client',
      );
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
}

function findRustBlockEnd(text, start) {
  let depth = 0;
  let inString = false;
  let inCharacter = false;
  let lineComment = false;
  let blockCommentDepth = 0;
  let escaped = false;
  for (let index = start; index < text.length; index += 1) {
    const current = text[index];
    const next = text[index + 1];
    if (lineComment) {
      if (current === '\n') lineComment = false;
      continue;
    }
    if (blockCommentDepth > 0) {
      if (current === '/' && next === '*') {
        blockCommentDepth += 1;
        index += 1;
      } else if (current === '*' && next === '/') {
        blockCommentDepth -= 1;
        index += 1;
      }
      continue;
    }
    if (inString) {
      if (!escaped && current === '"') inString = false;
      escaped = !escaped && current === '\\';
      if (current !== '\\') escaped = false;
      continue;
    }
    if (inCharacter) {
      if (!escaped && current === "'") inCharacter = false;
      escaped = !escaped && current === '\\';
      if (current !== '\\') escaped = false;
      continue;
    }
    if (current === '/' && next === '/') {
      lineComment = true;
      index += 1;
      continue;
    }
    if (current === '/' && next === '*') {
      blockCommentDepth = 1;
      index += 1;
      continue;
    }
    if (current === '"') {
      inString = true;
      continue;
    }
    if (current === "'") {
      inCharacter = true;
      continue;
    }
    if (current === '{') depth += 1;
    if (current === '}') {
      depth -= 1;
      if (depth === 0) return index + 1;
    }
  }
  return text.length;
}

function rustImplBlocks(text) {
  const blocks = [];
  const pattern = /\bimpl(?:\s*<[^>{}]*>)?\s+([^\s{]+)(?:\s+for\s+([^\s{]+))?[^\n{]*\{/g;
  for (const match of text.matchAll(pattern)) {
    const brace = text.indexOf('{', match.index);
    blocks.push({
      start: match.index,
      end: findRustBlockEnd(text, brace),
      name: (match[2] ?? match[1]).replace(/<.*$/, ''),
    });
  }
  return blocks;
}

function rustSymbols(module) {
  const implBlocks = rustImplBlocks(module.text);
  const typePattern = /\b(?:pub(?:\([^)]*\))?\s+)?(struct|enum|trait|type)\s+([A-Za-z_][A-Za-z0-9_]*)[^\n{;]*(?:\{|;)/g;
  for (const match of module.text.matchAll(typePattern)) {
    const start = match.index;
    const hasBlock = match[0].endsWith('{');
    const end = hasBlock ? findRustBlockEnd(module.text, module.text.indexOf('{', start)) : start + match[0].length;
    addSymbol(module, {
      name: match[2],
      qualifiedName: match[2],
      kind: match[1],
      visibility: /\bpub\b/.test(match[0]) ? 'public' : 'internal',
      startLine: lineAt(module.text, start),
      endLine: lineAt(module.text, end),
      signature: condensed(match[0]),
    }, `${module.absolutePath}:${start}`);
  }

  const functionPattern = /\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\s*(?:<[^>{}]*>)?\s*\(/g;
  for (const match of module.text.matchAll(functionPattern)) {
    const start = match.index;
    const bodyStart = module.text.indexOf('{', start);
    const declarationEnd = module.text.indexOf(';', start);
    const isTraitSignature = declarationEnd >= 0 && (bodyStart < 0 || declarationEnd < bodyStart);
    if (bodyStart < 0 && !isTraitSignature) continue;
    const end = isTraitSignature ? declarationEnd + 1 : findRustBlockEnd(module.text, bodyStart);
    const implementation = implBlocks.find(block => start > block.start && start < block.end);
    const name = match[1];
    const signature = module.text.slice(start, isTraitSignature ? declarationEnd : bodyStart).trim();
    addSymbol(module, {
      name,
      qualifiedName: implementation ? `${implementation.name}.${name}` : name,
      kind: implementation ? 'method' : 'function',
      visibility: /\bpub\b/.test(signature) ? 'public' : 'internal',
      startLine: lineAt(module.text, start),
      endLine: lineAt(module.text, end),
      signature: condensed(signature),
      ...(isTraitSignature ? {} : { bodyStart, bodyEnd: end }),
    }, `${module.absolutePath}:${start}`);
  }
}

function rustModuleTarget(rootId, modulePath) {
  const root = sourceRoots.find(candidate => candidate.id === rootId);
  if (!root || !modulePath) return null;
  const segments = modulePath.split('::').filter(Boolean);
  for (let length = segments.length; length > 0; length -= 1) {
    const base = join(root.path, ...segments.slice(0, length));
    const direct = moduleByPath.get(resolve(`${base}.rs`));
    const nested = moduleByPath.get(resolve(base, 'mod.rs'));
    if (direct || nested) return direct ?? nested;
  }
  return null;
}

function rustImportTargets(module) {
  const targets = [];
  const statements = module.text.matchAll(/\b(?:pub\s+)?use\s+([\s\S]*?);|\b(?:pub\s+)?mod\s+([A-Za-z_][A-Za-z0-9_]*)\s*;/g);
  for (const match of statements) {
    const statement = match[0].replace(/\s+/g, ' ');
    const line = lineAt(module.text, match.index);
    if (match[2]) {
      const target = rustModuleTarget(module.sourceRootId, match[2]);
      if (target) targets.push({ target, line, resolution: 'rust-mod-declaration' });
      continue;
    }
    const rootMatch = statement.match(/\b(crate|paladinscat_core|paladinscat_backend|paladinscat_hirez_relay)\s*::\s*(.*)$/);
    if (!rootMatch) continue;
    const targetRoot = rootMatch[1] === 'crate'
      ? module.sourceRootId
      : rustCrates.get(rootMatch[1]);
    if (!targetRoot) continue;
    const tail = rootMatch[2].replace(/;$/, '').trim();
    const braces = tail.match(/^\{(.+)\}$/);
    if (braces) {
      for (const child of braces[1].matchAll(/([A-Za-z_][A-Za-z0-9_]*)/g)) {
        const target = rustModuleTarget(targetRoot, child[1]);
        if (target) targets.push({ target, line, resolution: 'rust-use-resolution' });
      }
    } else {
      const target = rustModuleTarget(targetRoot, tail.replace(/[\s,{].*$/, ''));
      if (target) targets.push({ target, line, resolution: 'rust-use-resolution' });
    }
  }
  return targets;
}

function hasBodyRange(symbol) {
  return Number.isInteger(symbol.bodyStart) && Number.isInteger(symbol.bodyEnd);
}

function rustRelations(module) {
  for (const { target, line, resolution } of rustImportTargets(module)) {
    addRelation({
      kind: 'imports',
      sourceModuleId: module.id,
      sourceSymbolId: null,
      targetModuleId: target.id,
      targetSymbolId: null,
      referenceLine: line,
      resolution,
    });
  }
  const localFunctions = new Map();
  for (const symbol of symbols.filter(symbol => {
    const owner = modules.find(moduleCandidate => moduleCandidate.id === symbol.moduleId);
    return owner?.sourceRootId === module.sourceRootId && ['function', 'method'].includes(symbol.kind);
  })) {
    const candidates = localFunctions.get(symbol.name) ?? [];
    candidates.push(symbol);
    localFunctions.set(symbol.name, candidates);
  }
  for (const source of symbols.filter(symbol => symbol.moduleId === module.id && hasBodyRange(symbol))) {
    const body = module.text.slice(source.bodyStart, source.bodyEnd);
    for (const match of body.matchAll(/\b([A-Za-z_][A-Za-z0-9_]*)\s*\(/g)) {
      const candidates = localFunctions.get(match[1]) ?? [];
      if (candidates.length !== 1 || candidates[0].id === source.id) continue;
      addRelation({
        kind: 'calls',
        sourceModuleId: module.id,
        sourceSymbolId: source.id,
        targetModuleId: candidates[0].moduleId,
        targetSymbolId: candidates[0].id,
        referenceLine: lineAt(module.text, source.bodyStart + match.index),
        resolution: 'rust-unique-lexical-call',
      });
    }
  }
}

function moduleCardPath(module) {
  return `modules/${slug(module.path)}.md`;
}

function areaCardPath(area) {
  return `areas/${slug(area)}.md`;
}

function moduleById(id) {
  return modules.find(module => module.id === id);
}

function symbolById(id) {
  return symbols.find(symbol => symbol.id === id);
}

function writeModuleCards() {
  rmSync(generatedModulesDirectory, { recursive: true, force: true });
  mkdirSync(generatedModulesDirectory, { recursive: true });
  for (const module of sorted(modules, item => item.path)) {
    const moduleSymbols = symbols
      .filter(symbol => symbol.moduleId === module.id)
      .sort((left, right) => left.startLine - right.startLine || left.qualifiedName.localeCompare(right.qualifiedName));
    const imports = relations
      .filter(relation => relation.kind === 'imports' && relation.sourceModuleId === module.id)
      .map(relation => ({ relation, target: moduleById(relation.targetModuleId) }))
      .filter(({ target }) => target)
      .sort(({ target: left }, { target: right }) => left.path.localeCompare(right.path));
    const calls = relations
      .filter(relation => relation.kind === 'calls' && relation.sourceModuleId === module.id)
      .map(relation => ({ relation, source: symbolById(relation.sourceSymbolId), target: symbolById(relation.targetSymbolId) }))
      .filter(({ source, target }) => source && target)
      .sort(({ source: left }, { source: right }) => left.startLine - right.startLine);
    const contracts = relations
      .filter(relation => relation.kind === 'http_contract' && relation.sourceModuleId === module.id)
      .map(relation => ({
        relation,
        source: relation.sourceSymbolId ? symbolById(relation.sourceSymbolId) : null,
        target: moduleById(relation.targetModuleId),
      }))
      .filter(({ target }) => target)
      .sort(({ relation: left }, { relation: right }) => left.referenceLine - right.referenceLine);
    const dependents = relations
      .filter(relation => relation.kind === 'imports' && relation.targetModuleId === module.id)
      .map(relation => ({ relation, source: moduleById(relation.sourceModuleId) }))
      .filter(({ source }) => source)
      .sort(({ source: left }, { source: right }) => left.path.localeCompare(right.path));
    const contractConsumers = relations
      .filter(relation => relation.kind === 'http_contract' && relation.targetModuleId === module.id)
      .map(relation => ({
        relation,
        source: moduleById(relation.sourceModuleId),
        symbol: relation.sourceSymbolId ? symbolById(relation.sourceSymbolId) : null,
      }))
      .filter(({ source }) => source)
      .sort(({ source: left }, { source: right }) => left.path.localeCompare(right.path));
    const document = `---
tags: [code-map, generated, ${module.language}]
type: code-map-module
source: ${module.path}
---

# Code map: \`${module.path}\`

| Field | Value |
| --- | --- |
| Area | \`${module.area}\` |
| Language | ${module.language} |
| Source lines | ${module.lineCount} |
| Symbols | ${moduleSymbols.length} |

Source location: \`${module.path}:1-${module.lineCount}\`.

## Symbols

| Symbol | Kind | Visibility | Lines | Declaration |
| --- | --- | --- | --- | --- |
${moduleSymbols.length === 0 ? '| _No structural symbols were extracted._ | - | - | - | - |' : moduleSymbols.map(symbol => `| \`${markdown(symbol.qualifiedName)}\` | ${symbol.kind} | ${symbol.visibility} | \`${module.path}:${symbol.startLine}-${symbol.endLine}\` | \`${markdown(symbol.signature)}\` |`).join('\n')}

## Internal module dependencies

| Target module | Import line | Resolution |
| --- | ---: | --- |
${imports.length === 0 ? '| _None resolved_ | - | - |' : imports.map(({ relation, target }) => `| [\`${target.path}\`](../${moduleCardPath(target)}) | ${relation.referenceLine} | ${relation.resolution} |`).join('\n')}

## Resolved function calls

Only statically resolvable calls are listed. Dynamic dispatch, trait objects,
reflection, and unresolved/ambiguous names remain intentionally absent rather
than being guessed.

| From | To | Call line | Resolution |
| --- | --- | ---: | --- |
${calls.length === 0 ? '| _None resolved_ | - | - | - |' : calls.map(({ relation, source, target }) => `| \`${source.qualifiedName}\` | [\`${target.qualifiedName}\`](../${moduleCardPath(moduleById(target.moduleId))}) (${moduleById(target.moduleId).path}:${target.startLine}-${target.endLine}) | ${relation.referenceLine} | ${relation.resolution} |`).join('\n')}

## Cross-stack contracts

These edges cross a process boundary. They are generated only for statically
identified PaladinsCat API and Discord-renderer traffic; they are not inferred
from arbitrary internet requests.

| Client symbol | Server boundary | Request or contract | Source line | Resolution |
| --- | --- | --- | ---: | --- |
${contracts.length === 0 ? '| _None from this module_ | - | - | - | - |' : contracts.map(({ relation, source, target }) => `| \`${source?.qualifiedName ?? '(module boundary)'}\` | [\`${target.path}\`](../${moduleCardPath(target)}) | ${contractMarkdown(relation.contractPath)} | ${relation.referenceLine} | ${relation.resolution} |`).join('\n')}

## Direct dependents

| Module | Import line |
| --- | ---: |
${dependents.length === 0 ? '| _None resolved_ | - |' : dependents.map(({ relation, source }) => `| [\`${source.path}\`](../${moduleCardPath(source)}) | ${relation.referenceLine} |`).join('\n')}

## Cross-stack consumers

| Client module | Client symbol | Contract | Source line |
| --- | --- | --- | ---: |
${contractConsumers.length === 0 ? '| _None resolved_ | - | - | - |' : contractConsumers.map(({ relation, source, symbol }) => `| [\`${source.path}\`](../${moduleCardPath(source)}) | \`${symbol?.qualifiedName ?? '(module boundary)'}\` | ${contractMarkdown(relation.contractPath)} | ${relation.referenceLine} |`).join('\n')}
`;
    writeFileSync(join(mapRoot, moduleCardPath(module)), document);
  }
}

function writeAreaCards() {
  rmSync(generatedAreasDirectory, { recursive: true, force: true });
  mkdirSync(generatedAreasDirectory, { recursive: true });
  const areas = new Map();
  for (const module of modules) {
    const members = areas.get(module.area) ?? [];
    members.push(module);
    areas.set(module.area, members);
  }
  for (const [area, members] of sorted(areas.entries(), ([name]) => name)) {
    const ordered = sorted(members, module => module.path);
    const document = `---
tags: [code-map, generated]
type: code-map-area
area: ${area}
---

# Code-map area: \`${area}\`

| Module | Lines | Symbols | Internal imports | Resolved calls |
| --- | ---: | ---: | ---: | ---: |
${ordered.map(module => {
  const moduleSymbols = symbols.filter(symbol => symbol.moduleId === module.id).length;
  const importCount = relations.filter(relation => relation.kind === 'imports' && relation.sourceModuleId === module.id).length;
  const callCount = relations.filter(relation => relation.kind === 'calls' && relation.sourceModuleId === module.id).length;
  return `| [\`${module.path}\`](../${moduleCardPath(module)}) | ${module.lineCount} | ${moduleSymbols} | ${importCount} | ${callCount} |`;
}).join('\n')}
`;
    writeFileSync(join(mapRoot, areaCardPath(area)), document);
  }
  return areas;
}

function writeMoc(areas) {
  const entryPointPaths = [
    'src/backend/index.ts',
    'src/backend/hirez-relay/server.ts',
    'src/backend-rust/src/bin/api.rs',
    'src/backend-rust/src/bin/worker.rs',
    'src/backend-rust/src/bin/admin.rs',
    'src/hirez-relay-rust/src/main.rs',
    'src/paladinscat-core/src/lib.rs',
    'src/frontend/app/layout.tsx',
    'src/frontend/lib/api-client.ts',
    'src/discord-bot/src/index.ts',
    'src/discord-bot/src/api-client.ts',
  ];
  const entries = entryPointPaths
    .map(path => moduleByPath.get(resolve(repositoryRoot, path)))
    .filter(Boolean);
  const roots = sourceRoots.map(root => ({
    ...root,
    moduleCount: modules.filter(module => module.sourceRootId === root.id).length,
    symbolCount: symbols.filter(symbol => moduleById(symbol.moduleId).sourceRootId === root.id).length,
  }));
  const contractBoundaries = new Map();
  for (const relation of relations.filter(relation => relation.kind === 'http_contract')) {
    const source = moduleById(relation.sourceModuleId);
    const target = moduleById(relation.targetModuleId);
    const key = `${source.sourceRootId}->${target.sourceRootId}`;
    const boundary = contractBoundaries.get(key) ?? {
      sourceRootId: source.sourceRootId,
      targetRootId: target.sourceRootId,
      clientModules: new Set(),
      contracts: 0,
    };
    boundary.clientModules.add(source.id);
    boundary.contracts += 1;
    contractBoundaries.set(key, boundary);
  }
  const document = `---
tags: [moc, code-map, generated, full-stack, backend, frontend, discord, rust-migration]
type: index
related:
  - [[documents/02-technical/code-map/README]]
  - [[documents/02-technical/backend-rust-migration]]
  - [[documents/02-technical/migration/backend-rust-incremental-progress]]
---

# Full-Stack Rebuild Code Map

This is the generated map of content (MOC) for the PaladinsCat rebuild. It is
the agent entry point: choose an area, then a module card, then open only the
exact source range listed for the needed symbol.

Read [[documents/02-technical/code-map/README]] for the durable generation,
query, edge-semantics, and impact-analysis instructions.

It maps the Next.js frontend, Discord bot, production TypeScript backend, and
its Rust replacement candidates. Cross-stack HTTP contract edges point frontend
and bot clients to the current backend and Discord-renderer boundaries, so
changing a client helper, API rewrite, renderer path, or backend route surface
has an inspectable impact path.

Regenerate after a source-structure change:

\`\`\`powershell
cd src/backend
npm run code-map:generate
\`\`\`

The generated relational graph is
\`documents/02-technical/code-map/backend/code-map.sqlite\`. It is local and
ignored by Git so it never becomes a stale binary merge conflict; the generator
and all Markdown navigation are versioned source. Query it directly with
\`sqlite3\`, or use the read-only helper:

\`\`\`powershell
python scripts/code-map/query-code-map.py symbol dispatchRelayOperation
python scripts/code-map/query-code-map.py calls dispatchRelayOperation
python scripts/code-map/query-code-map.py callers dumpRawPayloads
python scripts/code-map/query-code-map.py contracts frontend
python scripts/code-map/query-code-map.py contracts discord-bot
python scripts/code-map/query-code-map.py dependencies src/backend/hirez-relay/dispatcher.ts
python scripts/code-map/query-code-map.py dependents src/paladinscat-core/src/database.rs
\`\`\`

## Scope and extraction guarantees

- Exact source-file and symbol line ranges are generated from the TypeScript
  compiler and a conservative Rust structural parser.
- TypeScript call edges use the compiler type checker. Rust call edges are only
  emitted when a lexical name resolves to exactly one function in its crate.
- Import edges are internal-only. External package references, dynamic imports,
  trait-object dispatch, reflection, and unresolved names are intentionally not
  guessed.
- HTTP contract edges cover known PaladinsCat frontend/Discord API calls, the
  frontend-to-Discord renderer calls, the Next proxy rewrite, and generated
  OpenAPI types. They do not claim to map arbitrary external fetches or dynamic
  URL construction.
- The graph is a navigation aid, not a behavior/parity or migration-readiness
  assertion. Existing compatibility and ownership gates remain authoritative.

## Source roots

| Source root | Role | Language | Modules | Symbols |
| --- | --- | --- | ---: | ---: |
${roots.map(root => `| \`${repositoryPath(root.path)}\` | ${root.label} | ${root.language} | ${root.moduleCount} | ${root.symbolCount} |`).join('\n')}

## Runtime entry points

| Entry point | Purpose | Map |
| --- | --- | --- |
${entries.map(module => `| \`${module.path}\` | ${module.sourceRootId === 'typescript-backend' ? 'Current TypeScript runtime boundary' : module.sourceRootId === 'frontend' ? 'Next.js frontend entry or API client boundary' : module.sourceRootId === 'discord-bot' ? 'Discord bot runtime or API client boundary' : module.sourceRootId === 'rust-relay' ? 'Rust HirezRelay runtime' : module.sourceRootId === 'rust-api' ? 'Rust backend candidate binary' : 'Shared Rust crate root'} | [module card](${moduleCardPath(module)}) |`).join('\n')}

## Cross-stack API boundaries

| Client surface | Current server surface | Client modules | Contract edges |
| --- | --- | ---: | ---: |
${sorted(contractBoundaries.values(), boundary => `${boundary.sourceRootId}->${boundary.targetRootId}`).map(boundary => `| ${sourceRoots.find(root => root.id === boundary.sourceRootId).label} | ${sourceRoots.find(root => root.id === boundary.targetRootId).label} | ${boundary.clientModules.size} | ${boundary.contracts} |`).join('\n') || '| _No static client contracts resolved_ | - | - | - |'}

## Areas

| Area | Modules | Symbols | Map |
| --- | ---: | ---: | --- |
${sorted(areas.entries(), ([area]) => area).map(([area, members]) => `| \`${area}\` | ${members.length} | ${members.reduce((total, module) => total + symbols.filter(symbol => symbol.moduleId === module.id).length, 0)} | [area card](${areaCardPath(area)}) |`).join('\n')}

## Database shape

\`source_roots\` → \`modules\` → \`symbols\`, with \`relations\` for
\`imports\`, resolved \`calls\`, and cross-process \`http_contract\` edges.
The foreign keys let an agent move either direction: symbol → dependencies, or
symbol/module → dependents and clients.

Useful direct query:

\`\`\`sql
SELECT source.qualified_name AS caller, target.qualified_name AS callee,
       source_module.path AS caller_file, relation.reference_line AS call_line
FROM relations AS relation
JOIN symbols AS source ON source.id = relation.source_symbol_id
JOIN symbols AS target ON target.id = relation.target_symbol_id
JOIN modules AS source_module ON source_module.id = source.module_id
WHERE relation.kind = 'calls'
  AND target.qualified_name = 'dispatchRelayOperation';
\`\`\`
`;
  writeFileSync(join(mapRoot, 'MOC.md'), document);
}

function gitRevision() {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], { cwd: repositoryRoot, encoding: 'utf8' }).trim();
  } catch {
    return 'unavailable';
  }
}

function main() {
  for (const root of sourceRoots) {
    for (const path of walkFiles(
      root.path,
      root.extensions,
      root.excludedDirectories,
      root.excludedFileSuffixes,
    )) {
      addModule(root, path);
    }
  }
  const typescriptModules = modules.filter(module => module.language === 'typescript');
  const typescriptProjects = new Map();
  for (const root of sourceRoots.filter(root => root.language === 'typescript')) {
    const projectModules = typescriptModules.filter(module => module.sourceRootId === root.id);
    const compilerOptions = typeScriptCompilerOptions(root);
    const program = ts.createProgram(projectModules.map(module => module.absolutePath), compilerOptions);
    typescriptProjects.set(root.id, { compilerOptions, program, checker: program.getTypeChecker() });
    for (const module of projectModules) {
      const sourceFile = program.getSourceFile(module.absolutePath);
      if (!sourceFile) throw new Error(`TypeScript compiler did not load ${module.path}`);
      typescriptSymbols(module, sourceFile);
    }
  }
  for (const module of modules.filter(module => module.language === 'rust')) rustSymbols(module);
  for (const module of typescriptModules) {
    const project = typescriptProjects.get(module.sourceRootId);
    const sourceFile = project?.program.getSourceFile(module.absolutePath);
    if (!project || !sourceFile) throw new Error(`Missing TypeScript project data for ${module.path}`);
    typescriptRelations(module, sourceFile, project.checker, project.compilerOptions);
    crossStackContracts(module, sourceFile);
  }
  for (const module of modules.filter(module => module.language === 'rust')) rustRelations(module);

  mkdirSync(mapRoot, { recursive: true });
  writeModuleCards();
  const areas = writeAreaCards();
  writeMoc(areas);

  const graph = {
    metadata: {
      schema_version: 1,
      generator: 'src/backend/scripts/generate-code-map.mjs',
      revision: gitRevision(),
      source_scope: 'src/backend (without test), src/backend-rust/src, src/hirez-relay-rust/src, src/paladinscat-core/src',
    },
    sourceRoots: sourceRoots.map(root => ({
      id: root.id,
      path: repositoryPath(root.path),
      label: root.label,
      language: root.language,
    })),
    modules: modules.map(({ absolutePath, text, ...module }) => module),
    symbols: symbols.map(({ bodyStart, bodyEnd, ...symbol }) => symbol),
    relations,
  };
  const python = process.env.PYTHON ?? 'python';
  const result = spawnSync(python, [databaseWriter, '--output', databasePath], {
    cwd: repositoryRoot,
    input: JSON.stringify(graph),
    stdio: ['pipe', 'inherit', 'inherit'],
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`SQLite writer exited with ${result.status}`);
  console.log(JSON.stringify({
    modules: modules.length,
    symbols: symbols.length,
    imports: relations.filter(relation => relation.kind === 'imports').length,
    calls: relations.filter(relation => relation.kind === 'calls').length,
    contracts: relations.filter(relation => relation.kind === 'http_contract').length,
    markdownRoot: repositoryPath(mapRoot),
    database: repositoryPath(databasePath),
  }));
}

main();
