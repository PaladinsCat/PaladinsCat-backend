import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import Fastify from 'fastify';
import { close as closeRedis, redis, waitForRedisReady } from '../services/cache';
import {
  DEPLOYMENT_STATE_KEY,
  configureDeploymentExpiryHandler,
  dropActivePublicRequests,
  getActivePublicRequestCount,
  initializeDeploymentControl,
  registerDeploymentGate,
  setDeploymentState,
  waitForActivePublicRequests,
} from '../services/deployment-control';

after(async () => {
  try {
    if (await waitForRedisReady(1_000)) {
      await redis.del(DEPLOYMENT_STATE_KEY);
    }
  } finally {
    await closeRedis();
  }
});

test('deployment drain can drop public requests left after the grace period', async () => {
  assert.equal(await waitForRedisReady(1_000), true);
  await redis.del(DEPLOYMENT_STATE_KEY);
  await initializeDeploymentControl();

  const app = Fastify();
  registerDeploymentGate(app);
  let releaseRequest!: () => void;
  const requestStarted = new Promise<void>((resolve) => {
    app.get('/stuck', async () => {
      resolve();
      await new Promise<void>((release) => { releaseRequest = release; });
      return { ok: true };
    });
  });
  await app.ready();
  await setDeploymentState({ id: 'drop-test', phase: 'announced', ttlSeconds: 60 });

  // Fastify's inject result is a lazy thenable. Assimilate it into a real
  // Promise so the request starts before we wait for the handler signal.
  const request = Promise.resolve(app.inject('/stuck'));
  await requestStarted;
  assert.equal(getActivePublicRequestCount(), 1);
  const dropped = dropActivePublicRequests();
  assert.equal(dropped.droppedRequests, 1);
  assert.equal(getActivePublicRequestCount(), 0);

  releaseRequest();
  await request.catch(() => undefined);
  await app.close();
});

test('deployment gate drains existing requests and rejects new public work', async () => {
  assert.equal(await waitForRedisReady(1_000), true);
  await redis.del(DEPLOYMENT_STATE_KEY);
  await initializeDeploymentControl();

  const app = Fastify();
  registerDeploymentGate(app);
  let releaseSlowRequest!: () => void;
  const slowRequestStarted = new Promise<void>((resolve) => {
    app.get('/slow', async () => {
      resolve();
      await new Promise<void>((release) => { releaseSlowRequest = release; });
      return { ok: true };
    });
  });
  app.get('/public', async () => ({ ok: true }));
  app.get('/health', async () => ({ ok: true }));
  app.get('/schedulers', async () => ({ ok: true }));
  await app.ready();

  await setDeploymentState({ id: 'test-deploy', phase: 'announced', ttlSeconds: 60 });
  assert.equal((await app.inject('/public')).statusCode, 200);

  const existingRequest = Promise.resolve(app.inject('/slow'));
  await slowRequestStarted;
  assert.equal(getActivePublicRequestCount(), 1);

  await setDeploymentState({ id: 'test-deploy', phase: 'draining', ttlSeconds: 60 });
  const rejected = await app.inject('/public');
  assert.equal(rejected.statusCode, 503);
  assert.equal(rejected.json().error.code, 'DEPLOYMENT_DRAIN');
  assert.equal((await app.inject('/health')).statusCode, 200);
  assert.equal((await app.inject('/schedulers')).statusCode, 200);

  releaseSlowRequest();
  assert.equal((await existingRequest).statusCode, 200);
  assert.deepEqual(await waitForActivePublicRequests(1_000), { drained: true, activeRequests: 0 });

  let resumed = 0;
  configureDeploymentExpiryHandler(() => { resumed += 1; });
  await setDeploymentState({ id: 'test-deploy', phase: 'complete', ttlSeconds: 60 });
  assert.equal(resumed, 1);
  assert.equal((await app.inject('/public')).statusCode, 200);
  await app.close();
});
