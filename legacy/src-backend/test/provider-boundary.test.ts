import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { join, relative, sep } from 'node:path';
import test from 'node:test';

const backendRoot = join(__dirname, '..');
const repositoryRoot = join(backendRoot, '..', '..');

function typescriptFiles(directory: string): string[] {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const absolute = join(directory, entry.name);
    if (entry.isDirectory()) {
      if (['dist', 'node_modules', 'test', 'tests', 'hirez-relay'].includes(entry.name)) {
        return [];
      }
      return typescriptFiles(absolute);
    }
    return entry.isFile() && entry.name.endsWith('.ts') ? [absolute] : [];
  });
}

test('all active TypeScript Hi-Rez consumers cross the relay transport boundary', () => {
  const legacyExceptions = new Set([
    join(backendRoot, 'services', 'api-key-pool.ts'),
    join(backendRoot, 'services', 'session-manager.ts'),
    join(backendRoot, 'config', 'api.ts'),
  ]);
  const forbidden = [
    /paladinsapi\.svc/i,
    /HIREZ_API_BASE_URL/,
    /API_CONFIG\.BASE_URL/,
    /sessionManager\.(?:sign|acquireSession|getActiveSession)/,
    /apiKeyPool\.(?:getNext|getActive|getKeyForMonitoring)/,
    /from ['"].*services\/api-key-pool/,
    /from ['"].*services\/session-manager/,
  ];

  const violations: string[] = [];
  for (const file of typescriptFiles(backendRoot)) {
    if (legacyExceptions.has(file)) continue;
    const source = readFileSync(file, 'utf8');
    for (const pattern of forbidden) {
      if (pattern.test(source)) {
        violations.push(`${relative(backendRoot, file).split(sep).join('/')}: ${pattern}`);
      }
    }
  }
  assert.deepEqual(violations, []);
});

test('operator pipeline static ingestion uses the relay instead of signing requests', () => {
  const source = readFileSync(join(backendRoot, 'scripts', 'run-pipeline.ts'), 'utf8');
  assert.match(source, /from ['"]\.\.\/services\/hirez['"]/);
  assert.match(source, /getChampions\('operator_static_ingest'\)/);
  assert.match(source, /getItems\('operator_static_ingest'\)/);
  assert.match(source, /getEsportsProLeagueDetails\('operator_static_ingest'\)/);
  assert.doesNotMatch(
    source,
    /sessionManager|apiKeyPool|API_CONFIG\.BASE_URL|(?<![.\w])fetch\s*\(/,
  );
});

test('normal Compose and package scripts cannot accidentally start the legacy relay', () => {
  const compose = readFileSync(join(repositoryRoot, 'docker-compose.yml'), 'utf8');
  assert.match(compose, /dockerfile:\s+docker\/Dockerfile\.hirez-relay-rust/);
  assert.doesNotMatch(compose, /dockerfile:\s+docker\/Dockerfile\.relay(?:\s|$)/);

  const packageJson = JSON.parse(
    readFileSync(join(backendRoot, 'package.json'), 'utf8'),
  ) as { scripts?: Record<string, string> };
  assert.equal(packageJson.scripts?.relay, undefined);
  assert.equal(packageJson.scripts?.['start:relay'], undefined);
  assert.match(packageJson.scripts?.['relay:legacy'] ?? '', /hirez-relay\/server\.ts/);
  assert.match(packageJson.scripts?.['start:relay:legacy'] ?? '', /dist\/hirez-relay\/server\.js/);
});

test('shared relay contract is neutral backend data, not legacy relay source', () => {
  const rustContract = readFileSync(
    join(repositoryRoot, 'src', 'hirez-relay-rust', 'src', 'contract.rs'),
    'utf8',
  );
  const dockerfile = readFileSync(
    join(repositoryRoot, 'docker', 'Dockerfile.hirez-relay-rust'),
    'utf8',
  );
  assert.match(rustContract, /backend\/contracts\/hirez-relay-operation-contract\.json/);
  assert.doesNotMatch(rustContract, /backend\/hirez-relay/);
  assert.match(dockerfile, /backend\/contracts\/hirez-relay-operation-contract\.json/);
  assert.doesNotMatch(dockerfile, /backend\/hirez-relay/);
});

test('native relay preserves the production secret-file identity boundary', () => {
  const dockerfile = readFileSync(
    join(repositoryRoot, 'docker', 'Dockerfile.hirez-relay-rust'),
    'utf8',
  );
  const deployHelper = readFileSync(
    join(repositoryRoot, 'scripts', 'migration', 'Deploy-PaladinsCatVps.ps1'),
    'utf8',
  );

  assert.match(dockerfile, /^USER 1000:1000$/m);
  assert.match(
    deployHelper,
    /chown 1000:1000 secrets\/mek\.txt secrets\/api_keys\.json/,
  );
  assert.match(
    deployHelper,
    /chmod 400 secrets\/mek\.txt secrets\/api_keys\.json/,
  );
});
