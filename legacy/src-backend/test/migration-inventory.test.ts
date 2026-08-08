import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

interface MigrationInventory {
  totals: {
    routes: number;
    routeModules: number;
    schedulerOwners: number;
    workerModules: number;
    environmentVariables: number;
  };
  routes: Array<{
    id: string;
    method: string;
    module: string;
    database: string;
    fixture: string;
    migrationStatus: string;
  }>;
  schedulers: Array<{
    key: string;
    compatibilityFixture: string;
  }>;
  workers: Array<{
    module: string;
    compatibilityFixture: string;
  }>;
  routeCountsByModule: Record<string, number>;
}

interface MigrationWorkPackages {
  schemaVersion: number;
  goal: string;
  completionState: string;
  targetBinaries: string[];
  retainedNonRustClients: string[];
  legacyRuntimePolicy: {
    productionCutoverRequiresExplicitAuthorization: boolean;
    allowedAsRollbackUntil: string;
    forbiddenAtCompletion: string[];
  };
  incrementalRolloutPolicy: {
    componentStateTransitions: string[];
    productionRustRequires: string[];
    soakCompleteRequires: string[];
    rules: string[];
  };
  incrementalRolloutWaves: Array<{
    id: string;
    component: string;
    implementationPhase: string;
    productionMutation: boolean;
    requiresExplicitAuthorization: boolean;
    cutoverMode: string;
    minimumObservationHours: number;
    acceptanceCheckpoints: Array<{
      id: string;
      window: string;
      requires: string[];
    }>;
    legacyRetirementRequires: string;
    entryGates: string[];
    postflightGates: string[];
    doesNotAdvance: string[];
    rollbackArtifact: string;
    rollbackGate: string;
  }>;
  retirementVerification: {
    requiredAbsentPaths: string[];
    retainedArtifactRelocations: Array<{
      legacy: string;
      target: string;
    }>;
    productionControlFiles: string[];
    forbiddenProductionPatterns: Array<{
      id: string;
      pattern: string;
    }>;
    requiredEvidence: string[];
  };
  states: string[];
  progressAccounting: {
    baselineInventory: string;
    evidenceRoot: string;
    allowedEvidence: string[];
    rules: string[];
  };
  resourceAcceptance: {
    comparisonMode: string;
    requiredMetrics: string[];
    hardGates: string[];
  };
  migrationPhases: Array<{
    id: string;
    name: string;
    dependsOn: string[];
    exitGates: string[];
    productionMutation: boolean;
  }>;
  routePackages: Array<{
    id: string;
    expectedRoutes: number;
    modules: string[];
  }>;
  workerPackages: Array<{
    id: string;
    modules: string[];
  }>;
  operatorCommands: Array<{
    id: string;
    legacy: string;
    target: string;
  }>;
  runtimeSelectors: Array<{
    id: string;
    source: string;
    retirementCondition: string;
  }>;
}

const repositoryRoot = join(__dirname, '..', '..', '..');
const inventory = JSON.parse(
  readFileSync(
    join(
      repositoryRoot,
      'documents',
      '02-technical',
      'migration',
      'backend-rust-inventory.json',
    ),
    'utf8',
  ),
) as MigrationInventory;
const workPackages = JSON.parse(
  readFileSync(
    join(
      repositoryRoot,
      'documents',
      '02-technical',
      'migration',
      'backend-rust-work-packages.json',
    ),
    'utf8',
  ),
) as MigrationWorkPackages;

test('full Rust migration inventory fixes the complete backend denominator', () => {
  assert.deepEqual(inventory.totals, {
    routes: 268,
    routeModules: 26,
    schedulerOwners: 6,
    workerModules: 41,
    environmentVariables: 117,
  });
  assert.equal(new Set(inventory.routes.map(route => route.id)).size, 268);
  assert.equal(new Set(inventory.schedulers.map(scheduler => scheduler.key)).size, 6);
  assert.equal(new Set(inventory.workers.map(worker => worker.module)).size, 41);
});

test('nothing in the inventory may be declared migrated without a fixture gate', () => {
  assert.ok(inventory.routes.every(route => route.fixture === 'required'));
  assert.ok(inventory.routes.every(route => route.migrationStatus === 'typescript'));
  assert.ok(
    inventory.schedulers.every(
      scheduler => scheduler.compatibilityFixture === 'required',
    ),
  );
  assert.ok(
    inventory.workers.every(worker => worker.compatibilityFixture === 'required'),
  );
});

test('non-ranked mechanics preserve physically isolated facts and aggregates', () => {
  const isolation = readFileSync(
    join(
      repositoryRoot,
      'migrations',
      'tracked',
      '115_isolate_nonranked_match_facts.sql',
    ),
    'utf8',
  );
  const workerModules = readFileSync(
    join(repositoryRoot, 'src', 'backend-rust', 'src', 'workers', 'mod.rs'),
    'utf8',
  );

  for (const casualFactTable of [
    'nonranked_match_items',
    'nonranked_match_cards',
    'nonranked_match_talents',
  ]) {
    assert.match(isolation, new RegExp(`CREATE TABLE IF NOT EXISTS ${casualFactTable}`));
    assert.match(isolation, new RegExp(`INSERT INTO ${casualFactTable}`));
  }
  for (const rankedFactTable of [
    'match_player_items',
    'match_player_cards',
    'match_player_talents',
  ]) {
    assert.match(isolation, new RegExp(`DELETE FROM ${rankedFactTable}`));
  }
  assert.match(isolation, /DELETE FROM match_players/);
  assert.match(isolation, /DELETE FROM matches/);
  assert.match(isolation, /raw_player\s*=\s*NULL/);
  assert.match(isolation, /raw_match\s*=\s*NULL/);
  assert.doesNotMatch(
    workerModules,
    /^\s*pub mod nonranked_mechanics\s*;/m,
    'the superseded duplicate mechanics repository must stay outside the runtime graph',
  );
  assert.match(
    isolation.split(/\r?\n/).slice(0, 8).join('\n'),
    /paladinscat:requires-full-backup/,
    'the population rewrite must require an explicit full backup',
  );
});

test('casual item projection is classified, ranked-isolated, and idempotent', () => {
  const projection = readFileSync(
    join(
      repositoryRoot,
      'src',
      'backend-rust',
      'src',
      'workers',
      'casual_mechanics.rs',
    ),
    'utf8',
  );
  const ledger = readFileSync(
    join(
      repositoryRoot,
      'src',
      'backend',
      'db',
      'migrations',
      '113_casual_item_projection_ledger.sql',
    ),
    'utf8',
  );

  assert.match(projection, /FROM nonranked_match_items item_fact/);
  assert.doesNotMatch(projection, /FROM match_player_items item_fact/);
  assert.match(projection, /INSERT INTO item_counts_casual_matches/);
  assert.match(projection, /ON CONFLICT \(match_id\) DO NOTHING/);
  assert.match(
    projection,
    /ON CONFLICT \(stats_scope, queue_id, item_id, slot, item_level\)/,
  );
  assert.match(projection, /ingest\.population IN\('casual','special'\)/);
  assert.doesNotMatch(projection, /\blobby_tier\b/);
  assert.match(ledger, /stats_scope VARCHAR\(32\) NOT NULL/);
  assert.match(ledger, /CHECK \(stats_scope <> 'ranked'\)/);
  assert.match(ledger, /eligible_players SMALLINT NOT NULL/);
});

test('items API keeps ranked default and gives casual its own classified read model', () => {
  const statsRoute = readFileSync(
    join(repositoryRoot, 'src', 'backend', 'routes', 'stats.ts'),
    'utf8',
  );
  const itemsRoute = statsRoute.slice(
    statsRoute.indexOf("fastify.get('/items'"),
    statsRoute.indexOf("fastify.get('/items/:itemId'"),
  );

  assert.match(itemsRoute, /mode \?\? 'ranked'/);
  assert.match(itemsRoute, /FROM item_counts_casual casual/);
  assert.match(itemsRoute, /FROM item_counts_casual_matches ledger/);
  assert.match(itemsRoute, /SUM\(ledger\.eligible_players\)/);
  assert.match(itemsRoute, /allowedCasualScopes/);
  assert.match(itemsRoute, /queueId must identify a positive non-ranked queue/);
  assert.doesNotMatch(
    itemsRoute.slice(
      itemsRoute.indexOf("if (mode === 'casual')"),
      itemsRoute.indexOf('const lobbyTier = parseLobbyTierBounds'),
    ),
    /\blobby_tier\b/,
  );
});

test('route work packages cover every inventoried module and route exactly once', () => {
  const packagedModules = workPackages.routePackages.flatMap(
    migrationPackage => migrationPackage.modules,
  );
  const inventoriedModules = Object.keys(inventory.routeCountsByModule);

  assert.equal(
    new Set(packagedModules).size,
    packagedModules.length,
    'route modules may not appear in more than one work package',
  );
  assert.deepEqual(
    [...packagedModules].sort(),
    [...inventoriedModules].sort(),
    'route packages must cover the fixed route-module inventory',
  );

  let totalRoutes = 0;
  for (const migrationPackage of workPackages.routePackages) {
    const actualRoutes = migrationPackage.modules.reduce(
      (sum, moduleName) => sum + inventory.routeCountsByModule[moduleName],
      0,
    );
    assert.equal(
      actualRoutes,
      migrationPackage.expectedRoutes,
      `route package ${migrationPackage.id} count drifted`,
    );
    totalRoutes += actualRoutes;
  }
  assert.equal(totalRoutes, inventory.totals.routes);
});

test('read-only route packages contain no mutating method or database write signal', () => {
  for (const packageId of ['A', 'B']) {
    const migrationPackage = workPackages.routePackages.find(
      candidate => candidate.id === packageId,
    );
    assert.ok(migrationPackage, `missing route package ${packageId}`);
    const routes = inventory.routes.filter(route =>
      migrationPackage.modules.includes(route.module),
    );
    const unsafe = routes.filter(
      route => route.method !== 'GET' || route.database === 'write-signal',
    );
    assert.deepEqual(
      unsafe.map(route => route.id),
      [],
      `read-only package ${packageId} contains mutation-capable routes`,
    );
  }
});

test('read-only route packages cannot execute runtime schema migrations', () => {
  const readOnlyModules = workPackages.routePackages
    .filter(migrationPackage => ['A', 'B'].includes(migrationPackage.id))
    .flatMap(migrationPackage => migrationPackage.modules);
  const runtimeDdl =
    /\b(?:CREATE|ALTER|DROP|TRUNCATE)\s+(?:TABLE|INDEX|MATERIALIZED|VIEW)\b/i;

  const violations = readOnlyModules.flatMap(moduleName => {
    const source = readFileSync(
      join(repositoryRoot, 'src', 'backend', 'routes', `${moduleName}.ts`),
      'utf8',
    );
    return runtimeDdl.test(source) ? [moduleName] : [];
  });
  assert.deepEqual(
    violations,
    [],
    'read-only routes must rely on tracked migrations instead of request-time DDL',
  );
});

test('worker work packages cover every inventoried worker exactly once', () => {
  const packagedWorkers = workPackages.workerPackages.flatMap(
    migrationPackage => migrationPackage.modules,
  );
  const inventoriedWorkers = inventory.workers.map(worker => worker.module);

  assert.equal(
    new Set(packagedWorkers).size,
    packagedWorkers.length,
    'worker modules may not appear in more than one work package',
  );
  assert.deepEqual(
    [...packagedWorkers].sort(),
    [...inventoriedWorkers].sort(),
    'worker packages must cover the fixed worker inventory',
  );
  assert.equal(packagedWorkers.length, inventory.totals.workerModules);
});

test('the work-package state machine ends only after legacy removal', () => {
  assert.equal(workPackages.schemaVersion, 4);
  assert.equal(
    workPackages.goal,
    'Replace every production TypeScript backend, worker, scheduler, relay, and operator execution path with Rust.',
  );
  assert.deepEqual(workPackages.states, [
    'typescript',
    'rust-implemented',
    'fixture-parity',
    'full-stack-parity',
    'shadow-verified',
    'production-rust',
    'legacy-removed',
  ]);
  assert.equal(workPackages.completionState, 'legacy-removed');
});

test('progress is evidence-derived and cannot be claimed by editing the baseline inventory', () => {
  assert.equal(
    workPackages.progressAccounting.baselineInventory,
    'documents/02-technical/migration/backend-rust-inventory.json',
  );
  assert.equal(
    workPackages.progressAccounting.evidenceRoot,
    'dev/compat/backend-rust',
  );
  assert.ok(
    workPackages.progressAccounting.allowedEvidence.includes(
      'inventory-bound-route-report',
    ),
  );
  assert.ok(
    workPackages.progressAccounting.allowedEvidence.includes(
      'worker-trace-report',
    ),
  );
  assert.ok(
    workPackages.progressAccounting.allowedEvidence.includes(
      'production-soak-report',
    ),
  );
  assert.ok(
    workPackages.progressAccounting.allowedEvidence.includes(
      'retirement-report',
    ),
  );
  assert.ok(
    workPackages.progressAccounting.rules.some(rule =>
      rule.includes('baseline inventory always describes the TypeScript source'),
    ),
  );
  assert.ok(
    workPackages.progressAccounting.rules.some(rule =>
      rule.includes('Only production evidence'),
    ),
  );
  assert.ok(
    workPackages.progressAccounting.rules.some(rule =>
      rule.includes('Only the terminal retirement report'),
    ),
  );
});

test('the full candidate must prove reduced overhead without trading away compatibility', () => {
  assert.equal(
    workPackages.resourceAcceptance.comparisonMode,
    'same-fixture-same-data-same-concurrency',
  );
  const requiredMetrics = new Set(
    workPackages.resourceAcceptance.requiredMetrics,
  );
  for (const metric of [
    'http-p95-latency',
    'http-p99-latency',
    'worker-end-to-end-latency',
    'cpu-seconds-per-1000-http-requests',
    'cpu-seconds-per-1000-matches',
    'process-rss-p95',
    'postgres-connections-peak',
    'hirez-quota-consumed',
  ]) {
    assert.ok(requiredMetrics.has(metric), `missing resource metric ${metric}`);
  }
  assert.ok(
    workPackages.resourceAcceptance.hardGates.some(gate =>
      gate.includes('No measured p95 or p99'),
    ),
  );
  assert.ok(
    workPackages.resourceAcceptance.hardGates.some(gate =>
      gate.includes('improve at least one of CPU-seconds or RSS'),
    ),
  );
  assert.ok(
    workPackages.resourceAcceptance.hardGates.some(gate =>
      gate.includes('cannot waive HTTP, database, worker, recovery, or scheduler compatibility'),
    ),
  );
});

test('the full migration graph joins every implementation lane before cutover', () => {
  const expectedPhaseIds = Array.from({ length: 12 }, (_, index) => `R${index}`);
  const phaseIds = workPackages.migrationPhases.map(phase => phase.id);
  assert.deepEqual(phaseIds, expectedPhaseIds);
  assert.equal(new Set(phaseIds).size, expectedPhaseIds.length);

  const knownPhases = new Set(phaseIds);
  for (const phase of workPackages.migrationPhases) {
    assert.ok(phase.name.length > 0);
    assert.ok(phase.exitGates.length > 0);
    assert.equal(
      new Set(phase.dependsOn).size,
      phase.dependsOn.length,
      `${phase.id} repeats a dependency`,
    );
    assert.ok(
      phase.dependsOn.every(dependency => knownPhases.has(dependency)),
      `${phase.id} depends on an unknown phase`,
    );
  }

  const fullCandidate = workPackages.migrationPhases.find(phase => phase.id === 'R9');
  assert.ok(fullCandidate);
  assert.deepEqual(
    [...fullCandidate.dependsOn].sort(),
    ['R4', 'R5', 'R6', 'R7', 'R8'],
  );

  const productionPhases = workPackages.migrationPhases
    .filter(phase => phase.productionMutation)
    .map(phase => phase.id);
  assert.deepEqual(productionPhases, ['R10', 'R11']);
  assert.ok(
    workPackages.migrationPhases
      .find(phase => phase.id === 'R10')
      ?.exitGates.includes('explicit-cutover-authorization'),
  );
  assert.ok(
    workPackages.migrationPhases
      .find(phase => phase.id === 'R11')
      ?.exitGates.includes('typescript-runtime-retirement'),
  );
});

test('the target runtime is Rust-only while frontend clients remain out of scope', () => {
  assert.deepEqual(workPackages.targetBinaries, [
    'paladinscat-api',
    'paladinscat-worker',
    'paladinscat-admin',
    'paladinscat-hirez-relay',
  ]);
  assert.deepEqual(workPackages.retainedNonRustClients, [
    'nextjs-frontend',
    'discord-presentation-process',
  ]);
  assert.equal(
    workPackages.legacyRuntimePolicy.productionCutoverRequiresExplicitAuthorization,
    true,
  );
  assert.equal(
    workPackages.legacyRuntimePolicy.allowedAsRollbackUntil,
    '72-hour-soak-complete',
  );
  assert.deepEqual(workPackages.legacyRuntimePolicy.forbiddenAtCompletion, [
    'typescript-backend-runtime',
    'typescript-worker-runtime',
    'typescript-scheduler-runtime',
    'typescript-hirez-relay-runtime',
    'node-backed-operator-command',
    'node-backend-container-image',
  ]);
});

test('the relay has an independently gated rollout wave that cannot advance unrelated migration state', () => {
  assert.equal(workPackages.schemaVersion, 4);
  assert.deepEqual(
    workPackages.incrementalRolloutPolicy.componentStateTransitions,
    ['implementation-verified', 'production-rust', 'soak-complete'],
  );
  assert.ok(
    workPackages.incrementalRolloutPolicy.productionRustRequires.includes(
      'explicit-deployment-authorization',
    ),
  );
  assert.ok(
    workPackages.incrementalRolloutPolicy.soakCompleteRequires.includes(
      'rollback-exercise-passed',
    ),
  );

  const relayWave = workPackages.incrementalRolloutWaves.find(
    wave => wave.id === 'W1-hirez-relay',
  );
  assert.ok(relayWave);
  assert.equal(relayWave.component, 'paladinscat-hirez-relay');
  assert.equal(relayWave.implementationPhase, 'R0');
  assert.equal(relayWave.productionMutation, true);
  assert.equal(relayWave.requiresExplicitAuthorization, true);
  assert.equal(relayWave.cutoverMode, 'immediate-exclusive');
  assert.equal(relayWave.minimumObservationHours, 72);
  assert.deepEqual(
    relayWave.acceptanceCheckpoints.map(checkpoint => checkpoint.id),
    ['initial-check', 'middle-check', 'final-check'],
  );
  assert.equal(
    relayWave.acceptanceCheckpoints[0]?.window,
    'immediate-through-15-minutes',
  );
  assert.equal(
    relayWave.acceptanceCheckpoints[1]?.window,
    '24-hours-after-cutover',
  );
  assert.equal(
    relayWave.acceptanceCheckpoints[2]?.window,
    '72-hours-after-cutover',
  );
  assert.ok(
    relayWave.acceptanceCheckpoints[2]?.requires.includes(
      'typescript-relay-retirement-authorized',
    ),
  );
  assert.equal(relayWave.legacyRetirementRequires, 'final-check-passed');
  assert.ok(relayWave.entryGates.includes('code-map-impact-review'));
  assert.ok(relayWave.entryGates.includes('37-operation-contract-coverage'));
  assert.ok(relayWave.entryGates.includes('rebuildable-typescript-rollback-image'));
  assert.ok(relayWave.postflightGates.includes('rust-engine-and-single-owner'));
  assert.deepEqual(relayWave.doesNotAdvance, [
    'http-routes',
    'worker-modules',
    'scheduler-owners',
    'operator-commands',
    'backend-runtime-selectors',
    'typescript-runtime-retirement',
  ]);
  assert.match(relayWave.rollbackArtifact, /^paladinscat-rollback\/hirezrelay:/);
  assert.ok(relayWave.rollbackGate.length > 0);
});

test('operator and runtime retirement ledgers have fixed unique denominators', () => {
  assert.equal(workPackages.operatorCommands.length, 20);
  assert.equal(
    new Set(workPackages.operatorCommands.map(command => command.id)).size,
    20,
  );
  assert.ok(
    workPackages.operatorCommands.every(
      command => command.legacy.length > 0 && command.target.startsWith('paladinscat-admin '),
    ),
  );

  assert.equal(workPackages.runtimeSelectors.length, 14);
  assert.equal(
    new Set(workPackages.runtimeSelectors.map(selector => selector.id)).size,
    14,
  );
  assert.ok(
    workPackages.runtimeSelectors.every(
      selector => selector.source.length > 0 && selector.retirementCondition.length > 0,
    ),
  );
});

test('the 100 percent retirement gate rejects every legacy backend runtime path', () => {
  assert.deepEqual(workPackages.retirementVerification.requiredAbsentPaths, [
    'legacy/src-backend',
    'docker/Dockerfile.backend',
    'docker/Dockerfile.relay',
    'docker/backend-entrypoint.sh',
  ]);

  assert.deepEqual(workPackages.retirementVerification.retainedArtifactRelocations, [
    {
      legacy: 'legacy/src-backend/db',
      target: 'migrations',
    },
    {
      legacy: 'legacy/src-backend/contracts/hirez-relay-operation-contract.json',
      target: 'src/paladinscat-core/resources/hirez-relay-operation-contract.json',
    },
    {
      legacy: 'legacy/src-backend/config/broken-skins.json',
      target: 'src/paladinscat-core/resources/broken-skins.json',
    },
  ]);

  const controlFiles = workPackages.retirementVerification.productionControlFiles;
  assert.equal(new Set(controlFiles).size, controlFiles.length);
  assert.ok(controlFiles.includes('docker-compose.yml'));
  assert.ok(controlFiles.includes('docker-compose.vps.yml'));
  assert.ok(controlFiles.includes('Update-PaladinsCatVps.ps1'));
  assert.ok(
    controlFiles.includes('scripts/migration/Deploy-PaladinsCatVps.ps1'),
  );
  assert.ok(
    controlFiles.includes('scripts/migration/Test-PaladinsCatVpsHealth.ps1'),
  );
  assert.ok(
    controlFiles.includes('scripts/migration/Test-PaladinsCatVpsFileSync.ps1'),
  );

  const patterns = workPackages.retirementVerification.forbiddenProductionPatterns;
  assert.equal(new Set(patterns.map(pattern => pattern.id)).size, patterns.length);
  assert.ok(patterns.every(pattern => pattern.id.length > 0 && pattern.pattern.length > 0));
  for (const pattern of patterns) {
    assert.doesNotThrow(() => new RegExp(pattern.pattern));
  }

  assert.deepEqual(workPackages.retirementVerification.requiredEvidence, [
    'all-fixed-ledger-items-legacy-removed',
    'full-native-compatibility-report',
    'rust-only-backend-and-relay-image-digests',
    'single-rust-scheduler-ownership',
    '72-hour-production-soak',
    'rollback-selector-removal',
    'retired-gate',
  ]);
});
