import assert from 'node:assert/strict';
import { test } from 'node:test';
import Fastify from 'fastify';
import { installApplicationErrorHandler } from '../services/application-foundation';
import { close as closeRedis } from '../services/cache';

test('error handler does not write after a route already sent its response', async () => {
  const app = Fastify({ logger: false });
  installApplicationErrorHandler(app);
  app.get('/already-sent', async (_request, reply) => {
    reply.status(400).send({ error: 'validation' });
    throw new Error('later failure');
  });

  try {
    const response = await app.inject('/already-sent');
    assert.equal(response.statusCode, 400);
    assert.deepEqual(response.json(), { error: 'validation' });
  } finally {
    await app.close();
    await closeRedis();
  }
});
