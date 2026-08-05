import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import test from 'node:test';
import Fastify from 'fastify';
import { close as closeRedis } from '../services/cache';
import {
  installDeveloperApiSecurity,
  isAuthenticatedDeveloperApiRequest,
  isDeveloperApiRequest,
  resolveDeveloperApiRoute,
  rewriteDeveloperApiUrl,
} from '../services/developer-api';
import type { RateLimitResult } from '../services/rate-limit';

const API_KEY = `pc_test_${Buffer.alloc(32, 7).toString('base64url')}`;
const API_KEY_HASH = crypto.createHash('sha256').update(API_KEY).digest('hex');

test.after(async () => {
  await closeRedis();
});

function allowedRate(): Promise<RateLimitResult> {
  return Promise.resolve({
    remaining: 119,
    total: 120,
    resetAt: Date.now() + 60_000,
    allowed: true,
    backendAvailable: true,
  });
}

function buildApp(keyHash: string | null = API_KEY_HASH) {
  const app = Fastify({ rewriteUrl: rewriteDeveloperApiUrl });
  installDeveloperApiSecurity(app, { keyHash, rateLimitCheck: allowedRate });
  app.get('/health', async () => ({ status: 'healthy' }));
  app.get('/meta/version', async () => ({ version: 'test' }));
  app.get('/stats/champions', async () => [{ champion_id: 1 }]);
  app.get('/matches/:id', async req => ({
    id: (req.params as any).id,
    developerApi: isDeveloperApiRequest(req),
    authenticatedDeveloper: isAuthenticatedDeveloperApiRequest(req),
  }));
  app.post('/players/:id/refresh', async req => ({
    id: (req.params as any).id,
    developerApi: isDeveloperApiRequest(req),
    authenticatedDeveloper: isAuthenticatedDeveloperApiRequest(req),
  }));
  app.get('/search/universal', async () => ({ results: [] }));
  app.get('/admin/database', async () => ({ private: true }));
  return app;
}

test('v1 route resolver admits safe reads and the guarded exact-player refresh', () => {
  assert.deepEqual(
    resolveDeveloperApiRoute('GET', '/v1/stats/champions?limit=5'),
    {
      attempted: true,
      supported: true,
      anonymous: false,
      targetUrl: '/stats/champions?limit=5',
    },
  );
  assert.equal(resolveDeveloperApiRoute('GET', '/v1/health').targetUrl, '/health');
  assert.equal(resolveDeveloperApiRoute('GET', '/v1/version').targetUrl, '/meta/version');
  assert.equal(resolveDeveloperApiRoute('GET', '/stats/champions').attempted, false);
  assert.equal(resolveDeveloperApiRoute('GET', '/v1/admin/database').statusCode, 404);
  assert.equal(resolveDeveloperApiRoute('POST', '/v1/stats/champions').statusCode, 405);
  assert.deepEqual(
    resolveDeveloperApiRoute('POST', '/v1/players/716515038/refresh'),
    {
      attempted: true,
      supported: true,
      anonymous: false,
      targetUrl: '/players/716515038/refresh',
    },
  );
  assert.equal(
    resolveDeveloperApiRoute('GET', '/v1/search/universal?q=name&remote=true').code,
    'REMOTE_LOOKUP_NOT_AVAILABLE',
  );
});

test('documented v1 endpoint families remain in the explicit allowlist', () => {
  const documentedPaths = [
    '/notifications',
    '/operations/stats',
    '/system/hirez-status',
    '/search/universal?q=Androxus',
    '/search/players?q=player',
    '/search/matches?q=1280',
    '/reference/champions',
    '/reference/champions/2205',
    '/reference/lookup?type=champions&id=2205',
    '/champions',
    '/champions/androxus',
    '/champions/androxus/page-data',
    '/players/716515038',
    '/players/716515038/matches',
    '/players/716515038/champions',
    '/players/716515038/loadouts',
    '/players/leaderboard/class?role=Flank',
    '/player-ext/status/716515038',
    '/matches/1280340959',
    '/matches/batch?ids=1280340959',
    '/matches/recent',
    '/matches/queue/486',
    '/matches/search',
    '/matches/fact/1280340959',
    '/live/matches',
    '/live/matches/1280340959',
    '/live/ended',
    '/stats/champions',
    '/stats/maps/Stone%20Keep',
    '/ratings/queue/716515038',
    '/coplay/teammates/716515038',
    '/meta/changelog',
    '/meta/items',
    '/meta/items/1',
  ];
  const rejected = documentedPaths.filter(
    path => !resolveDeveloperApiRoute('GET', `/v1${path}`).supported,
  );
  assert.deepEqual(rejected, []);
});

test('account, service, upstream, mutation, and operator families never enter v1', () => {
  const forbiddenPaths = [
    '/admin/database',
    '/recovery/pending',
    '/matches/raw/demo',
    '/matches/dropped',
    '/players/discord',
    '/player-ext/private',
    '/live/players/716515038',
    '/live/drop-hack-suspects',
    '/auth/me',
    '/builds',
    '/community/posts',
    '/api/raw-responses',
    '/esports/leagues',
  ];
  const unexpectedlyAllowed = forbiddenPaths.filter(
    path => resolveDeveloperApiRoute('GET', `/v1${path}`).supported,
  );
  assert.deepEqual(unexpectedlyAllowed, []);
});

test('legacy routes remain anonymous while v1 data routes require the static key', async () => {
  const app = buildApp();
  const legacy = await app.inject({ method: 'GET', url: '/stats/champions' });
  const missing = await app.inject({ method: 'GET', url: '/v1/stats/champions' });
  const invalid = await app.inject({
    method: 'GET',
    url: '/v1/stats/champions',
    headers: { authorization: `Bearer pc_test_${Buffer.alloc(32, 8).toString('base64url')}` },
  });
  const allowed = await app.inject({
    method: 'GET',
    url: '/v1/stats/champions',
    headers: { authorization: `Bearer ${API_KEY}` },
  });

  assert.equal(legacy.statusCode, 200);
  assert.equal(missing.statusCode, 401);
  assert.equal(missing.json().error.code, 'INVALID_API_KEY');
  assert.equal(invalid.statusCode, 401);
  assert.equal(allowed.statusCode, 200);
  assert.equal(allowed.headers['x-paladinscat-api-version'], 'v1');
  assert.equal(allowed.headers['x-developer-ratelimit-limit'], '120');
  assert.equal(allowed.headers['cache-control'], 'private, no-store');
  assert.match(String(allowed.headers.vary), /Authorization/i);

  const versionedMatch = await app.inject({
    method: 'GET',
    url: '/v1/matches/1280340959',
    headers: { authorization: `Bearer ${API_KEY}` },
  });
  const legacyMatch = await app.inject({ method: 'GET', url: '/matches/1280340959' });
  assert.equal(versionedMatch.json().developerApi, true);
  assert.equal(versionedMatch.json().authenticatedDeveloper, true);
  assert.equal(legacyMatch.json().developerApi, false);
  assert.equal(legacyMatch.json().authenticatedDeveloper, false);

  const refresh = await app.inject({
    method: 'POST',
    url: '/v1/players/716515038/refresh',
    headers: { authorization: `Bearer ${API_KEY}` },
  });
  const refreshWithoutKey = await app.inject({
    method: 'POST',
    url: '/v1/players/716515038/refresh',
  });
  assert.equal(refresh.statusCode, 200);
  assert.equal(refresh.json().developerApi, true);
  assert.equal(refresh.json().authenticatedDeveloper, true);
  assert.equal(refreshWithoutKey.statusCode, 401);
  assert.equal(refreshWithoutKey.json().error.code, 'INVALID_API_KEY');
  await app.close();
});

test('v1 health and version are anonymous but unsupported or unsafe operations stay closed', async () => {
  const app = buildApp();
  const health = await app.inject({ method: 'GET', url: '/v1/health' });
  const version = await app.inject({ method: 'GET', url: '/v1/version' });
  const privateRoute = await app.inject({
    method: 'GET',
    url: '/v1/admin/database',
    headers: { authorization: `Bearer ${API_KEY}` },
  });
  const mutation = await app.inject({
    method: 'POST',
    url: '/v1/stats/champions',
    headers: { authorization: `Bearer ${API_KEY}` },
  });
  const remoteSearch = await app.inject({
    method: 'GET',
    url: '/v1/search/universal?q=test&remote=true',
    headers: { authorization: `Bearer ${API_KEY}` },
  });

  assert.equal(health.statusCode, 200);
  assert.equal(version.statusCode, 200);
  assert.equal(privateRoute.statusCode, 404);
  assert.equal(privateRoute.json().error.code, 'API_ROUTE_NOT_FOUND');
  assert.equal(mutation.statusCode, 405);
  assert.equal(remoteSearch.statusCode, 400);
  assert.equal(remoteSearch.json().error.code, 'REMOTE_LOOKUP_NOT_AVAILABLE');
  await app.close();
});

test('configured hash is required before an authenticated v1 route can open', async () => {
  const app = buildApp(null);
  const response = await app.inject({
    method: 'GET',
    url: '/v1/matches/1280340959',
    headers: { authorization: `Bearer ${API_KEY}` },
  });
  assert.equal(response.statusCode, 503);
  assert.equal(response.json().error.code, 'DEVELOPER_API_NOT_CONFIGURED');
  await app.close();
});

test('developer-key limiter is per key and does not impose a daily counter', async () => {
  let calls = 0;
  const app = Fastify({ rewriteUrl: rewriteDeveloperApiUrl });
  installDeveloperApiSecurity(app, {
    keyHash: API_KEY_HASH,
    rateLimitCheck: async options => {
      calls += 1;
      assert.equal(options.key, `developer-api:${API_KEY_HASH.slice(0, 16)}`);
      assert.equal(options.windowMs, 60_000);
      return {
        remaining: calls === 1 ? 0 : 0,
        total: 1,
        resetAt: Date.now() + 60_000,
        allowed: calls === 1,
        backendAvailable: true,
      };
    },
  });
  app.get('/stats/champions', async () => []);

  const headers = { authorization: `Bearer ${API_KEY}` };
  const first = await app.inject({ method: 'GET', url: '/v1/stats/champions', headers });
  const second = await app.inject({ method: 'GET', url: '/v1/stats/champions', headers });
  assert.equal(first.statusCode, 200);
  assert.equal(second.statusCode, 429);
  assert.equal(second.json().error.code, 'DEVELOPER_RATE_LIMITED');
  assert.equal(calls, 2);
  await app.close();
});

test('developer-key concurrency is bounded and released after the response', async () => {
  const savedLimit = process.env.DEVELOPER_API_CONCURRENCY_LIMIT;
  process.env.DEVELOPER_API_CONCURRENCY_LIMIT = '1';
  try {
    let releaseHandler!: () => void;
    let markStarted!: () => void;
    const handlerStarted = new Promise<void>(resolve => { markStarted = resolve; });
    const handlerRelease = new Promise<void>(resolve => { releaseHandler = resolve; });
    const app = Fastify({ rewriteUrl: rewriteDeveloperApiUrl });
    installDeveloperApiSecurity(app, {
      keyHash: API_KEY_HASH,
      rateLimitCheck: allowedRate,
    });
    app.get('/stats/champions', async () => {
      markStarted();
      await handlerRelease;
      return [];
    });

    const headers = { authorization: `Bearer ${API_KEY}` };
    const firstPromise = app.inject({ method: 'GET', url: '/v1/stats/champions', headers });
    await handlerStarted;
    const blocked = await app.inject({ method: 'GET', url: '/v1/stats/champions', headers });
    assert.equal(blocked.statusCode, 429);
    assert.equal(blocked.json().error.code, 'DEVELOPER_CONCURRENCY_LIMITED');

    releaseHandler();
    const first = await firstPromise;
    assert.equal(first.statusCode, 200);

    const afterRelease = await app.inject({ method: 'GET', url: '/v1/stats/champions', headers });
    assert.equal(afterRelease.statusCode, 200);
    await app.close();
  } finally {
    if (savedLimit === undefined) delete process.env.DEVELOPER_API_CONCURRENCY_LIMIT;
    else process.env.DEVELOPER_API_CONCURRENCY_LIMIT = savedLimit;
  }
});
