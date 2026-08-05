import assert from 'node:assert/strict';
import test from 'node:test';
import Fastify from 'fastify';
import { close as closeRedis } from '../services/cache';
import { installRateLimitHook, type RateLimitResult } from '../services/rate-limit';
import {
  guardPlayerRefreshAttempt,
  installRequestSecurity,
  installSensitiveRouteAuth,
  resolveClientAddress,
  sendRequestSecurityError,
  RequestSecurityError,
} from '../services/request-security';

test.after(async () => {
  await closeRedis();
});

test('a root-installed limiter covers routes registered by sibling plugins', async () => {
  const app = Fastify();
  let calls = 0;
  installRateLimitHook(app, {
    limit: 1,
    windowMs: 60_000,
    check: async (): Promise<RateLimitResult> => {
      calls += 1;
      return {
        remaining: calls <= 1 ? 0 : 0,
        total: 1,
        resetAt: Date.now() + 60_000,
        allowed: calls <= 1,
        backendAvailable: true,
      };
    },
  });
  app.register(async (sibling) => {
    sibling.get('/sibling', async () => ({ ok: true }));
  });

  const first = await app.inject({ method: 'GET', url: '/sibling' });
  const second = await app.inject({ method: 'GET', url: '/sibling' });

  assert.equal(first.statusCode, 200);
  assert.equal(first.headers['x-ratelimit-limit'], '1');
  assert.equal(second.statusCode, 429);
  assert.equal(calls, 2);
  await app.close();
});

test('a blocked client does not consume the aggregate counter', async () => {
  const app = Fastify();
  const checkedKeys: string[] = [];
  const check = async (options: any): Promise<RateLimitResult> => {
    checkedKeys.push(options.key);
    const clientAllowed = options.key !== 'blocked-client';
    return {
      remaining: clientAllowed ? 99 : 0,
      total: 100,
      resetAt: Date.now() + 60_000,
      allowed: clientAllowed,
      backendAvailable: true,
    };
  };
  installRateLimitHook(app, {
    keyFn: () => 'blocked-client',
    check,
  });
  installRateLimitHook(app, {
    keyFn: () => 'aggregate',
    check,
    headerPrefix: 'Global-RateLimit',
  });
  app.get('/limited', async () => ({ ok: true }));

  const response = await app.inject({ method: 'GET', url: '/limited' });
  assert.equal(response.statusCode, 429);
  assert.deepEqual(checkedKeys, ['blocked-client']);
  assert.equal(response.headers['x-global-ratelimit-limit'], undefined);
  await app.close();
});

test('profile refresh allows five attempts before returning the 10-minute cooldown', async () => {
  const app = Fastify();
  let calls = 0;
  const resetAt = Date.now() + 10 * 60_000;
  const check = async (): Promise<RateLimitResult> => {
    calls += 1;
    return {
      remaining: Math.max(0, 5 - calls),
      total: 5,
      resetAt,
      allowed: calls <= 5,
      backendAvailable: true,
    };
  };
  app.post('/players/:id/refresh', async (req, reply) => {
    const quota = await guardPlayerRefreshAttempt(
      req,
      reply,
      Number((req.params as any).id),
      check,
    );
    return quota;
  });
  app.setErrorHandler((error, _req, reply) => {
    if (error instanceof RequestSecurityError) return sendRequestSecurityError(error, reply);
    return reply.status(500).send({ error: error instanceof Error ? error.message : String(error) });
  });

  for (let attempt = 1; attempt <= 5; attempt += 1) {
    const response = await app.inject({ method: 'POST', url: '/players/42/refresh' });
    assert.equal(response.statusCode, 200);
    assert.equal(response.headers['x-player-refresh-limit'], '5');
    assert.equal(response.json().remaining, 5 - attempt);
  }
  const blocked = await app.inject({ method: 'POST', url: '/players/42/refresh' });
  assert.equal(blocked.statusCode, 429);
  assert.equal(blocked.json().error.code, 'PROFILE_REFRESH_COOLDOWN');
  assert.ok(Number(blocked.headers['retry-after']) > 0);
  await app.close();
});

test('client resolution takes the proxy-appended address instead of a forged XFF prefix', () => {
  const address = resolveClientAddress({
    ip: '172.21.0.4',
    headers: {
      'x-forwarded-for': '203.0.113.99, 198.51.100.24',
    },
  } as any);
  assert.equal(address, '198.51.100.24');
});

test('operator and raw routes require credentials while public database reads remain open', async () => {
  const previousAdminSecret = process.env.ADMIN_SECRET;
  process.env.ADMIN_SECRET = 'operator-secret';
  const app = Fastify();
  installSensitiveRouteAuth(app);
  app.get('/matches/raw/demo', async () => ({ raw: true }));
  app.get('/champions', async () => ({ data: [] }));

  const denied = await app.inject({ method: 'GET', url: '/matches/raw/demo' });
  const allowed = await app.inject({
    method: 'GET',
    url: '/matches/raw/demo',
    headers: { authorization: 'Bearer operator-secret' },
  });
  const publicRead = await app.inject({ method: 'GET', url: '/champions' });

  assert.equal(denied.statusCode, 401);
  assert.equal(allowed.statusCode, 200);
  assert.equal(publicRead.statusCode, 200);
  await app.close();
  if (previousAdminSecret === undefined) delete process.env.ADMIN_SECRET;
  else process.env.ADMIN_SECRET = previousAdminSecret;
});

test('Discord service auth activates only after the shared token is configured', async () => {
  const previousServiceToken = process.env.PALADINSCAT_SERVICE_TOKEN;
  const previousGraceToken = process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS;
  process.env.PALADINSCAT_SERVICE_TOKEN = 's'.repeat(64);
  process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS = 'p'.repeat(64);
  const app = Fastify();
  installSensitiveRouteAuth(app);
  app.get('/players/discord', async () => ({ player: null }));
  app.get('/players/discord/saved-player', async () => ({ player: null }));

  const denied = await app.inject({ method: 'GET', url: '/players/discord?player=1' });
  const allowed = await app.inject({
    method: 'GET',
    url: '/players/discord?player=1',
    headers: { 'x-paladinscat-service-token': 's'.repeat(64) },
  });
  const previousAllowed = await app.inject({
    method: 'GET',
    url: '/players/discord?player=1',
    headers: { 'x-paladinscat-service-token': 'p'.repeat(64) },
  });
  const unknownDenied = await app.inject({
    method: 'GET',
    url: '/players/discord?player=1',
    headers: { 'x-paladinscat-service-token': 'x'.repeat(64) },
  });
  const savedPlayerDenied = await app.inject({
    method: 'GET',
    url: '/players/discord/saved-player?discordUserId=123',
  });
  const savedPlayerAllowed = await app.inject({
    method: 'GET',
    url: '/players/discord/saved-player?discordUserId=123',
    headers: { 'x-paladinscat-service-token': 's'.repeat(64) },
  });

  assert.equal(denied.statusCode, 401);
  assert.equal(allowed.statusCode, 200);
  assert.equal(previousAllowed.statusCode, 200);
  assert.equal(unknownDenied.statusCode, 401);
  assert.equal(savedPlayerDenied.statusCode, 401);
  assert.equal(savedPlayerAllowed.statusCode, 200);
  await app.close();
  if (previousServiceToken === undefined) delete process.env.PALADINSCAT_SERVICE_TOKEN;
  else process.env.PALADINSCAT_SERVICE_TOKEN = previousServiceToken;
  if (previousGraceToken === undefined) delete process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS;
  else process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS = previousGraceToken;
});

test('Discord saved-player mappings stay closed when no service token is configured', async () => {
  const previousServiceToken = process.env.PALADINSCAT_SERVICE_TOKEN;
  const previousGraceToken = process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS;
  delete process.env.PALADINSCAT_SERVICE_TOKEN;
  delete process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS;
  const app = Fastify();
  installSensitiveRouteAuth(app);
  app.get('/players/discord', async () => ({ player: null }));
  app.get('/players/discord/saved-player', async () => ({ player: null }));

  const legacyLookup = await app.inject({ method: 'GET', url: '/players/discord?player=1' });
  const privateMapping = await app.inject({
    method: 'GET',
    url: '/players/discord/saved-player?discordUserId=123',
  });

  assert.equal(legacyLookup.statusCode, 200);
  assert.equal(privateMapping.statusCode, 401);
  await app.close();
  if (previousServiceToken === undefined) delete process.env.PALADINSCAT_SERVICE_TOKEN;
  else process.env.PALADINSCAT_SERVICE_TOKEN = previousServiceToken;
  if (previousGraceToken === undefined) delete process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS;
  else process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS = previousGraceToken;
});

test('service-token rotation configuration rejects unsafe grace states', async () => {
  const savedCurrent = process.env.PALADINSCAT_SERVICE_TOKEN;
  const savedPrevious = process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS;
  try {
    delete process.env.PALADINSCAT_SERVICE_TOKEN;
    process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS = 'p'.repeat(64);
    const missingCurrentApp = Fastify();
    assert.throws(
      () => installRequestSecurity(missingCurrentApp),
      /PREVIOUS requires PALADINSCAT_SERVICE_TOKEN/,
    );
    await missingCurrentApp.close();

    process.env.PALADINSCAT_SERVICE_TOKEN = 's'.repeat(64);
    process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS = 's'.repeat(64);
    const duplicateTokenApp = Fastify();
    assert.throws(
      () => installRequestSecurity(duplicateTokenApp),
      /PREVIOUS must differ/,
    );
    await duplicateTokenApp.close();
  } finally {
    if (savedCurrent === undefined) delete process.env.PALADINSCAT_SERVICE_TOKEN;
    else process.env.PALADINSCAT_SERVICE_TOKEN = savedCurrent;
    if (savedPrevious === undefined) delete process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS;
    else process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS = savedPrevious;
  }
});
