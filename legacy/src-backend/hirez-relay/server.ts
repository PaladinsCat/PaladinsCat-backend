import Fastify from 'fastify';
import { randomUUID } from 'crypto';
import { dispatchRelayOperation, getRelayMode, RelayValidationError } from './dispatcher';
import { failTrace, finishTrace, getMetrics, startTrace } from './observability';
import { RelayCallRequest, RelayCallResponse } from '../contracts/hirez-relay';
import { shutdown as closeDatabase } from '../config/db';
import { close as closeRedis } from '../services/cache';
import { waitForActiveWorkerJobs } from '../workers/worker-lock';
import { runWithRelayAttribution } from './request-context';
import {
  acquireLiveOwnerLease,
  liveOwnerHealthy,
  releaseLiveOwnerLease,
} from './live-owner-lease';

const HOST = process.env.HIREZ_RELAY_HOST || '127.0.0.1';
const PORT = Number(process.env.HIREZ_RELAY_PORT || 3015);
const BODY_LIMIT_BYTES = Math.max(
  1024 * 1024,
  Number(process.env.HIREZ_RELAY_BODY_LIMIT_BYTES || 10 * 1024 * 1024),
);

async function initializeRealMode(): Promise<void> {
  if (getRelayMode() !== 'real') return;
  const { apiKeyPool } = await import('../services/api-key-pool.js');
  const { enableApiKeySync } = await import('./api-key-sync.js');
  await apiKeyPool.init();
  if (process.env.HIREZ_RELAY_START_QUIESCED === 'true') {
    console.log('[HirezRelay] real mode initialized quiesced for deployment candidate validation');
    return;
  }
  await acquireLiveOwnerLease();
  enableApiKeySync();
  console.log('[HirezRelay] real mode initialized API key pool and usage sync');
}

export function buildRelayServer() {
  // Explicit body limit for local backend -> relay calls. Discovery now dumps
  // one match payload per call by default, but raw Hi-Rez player rows are wide
  // and can exceed Fastify's small default during recovery. This limit protects
  // against accidental hour-sized payloads while allowing a legitimate single
  // match handoff to reach raw_ingest_buffer.
  const app = Fastify({ logger: false, bodyLimit: BODY_LIMIT_BYTES });

  app.get('/health', async () => {
    const mode = getRelayMode();
    const quiesced = process.env.HIREZ_RELAY_START_QUIESCED === 'true';
    const owner = mode === 'real' && !quiesced ? await liveOwnerHealthy() : false;
    return {
      service: 'HirezRelay',
      engine: 'typescript',
      mode,
      status: mode !== 'real' || quiesced || owner ? 'ok' : 'degraded',
      ready: mode !== 'real' || quiesced || owner,
      quiesced,
      owner,
      keysEnabled: mode === 'real',
      timestamp: new Date().toISOString(),
    };
  });

  app.get('/metrics', async () => ({
    service: 'HirezRelay',
    mode: getRelayMode(),
    ...getMetrics(),
  }));

  app.post('/v1/call', async (request, reply) => {
    const body = request.body as RelayCallRequest;
    if (!body?.operation || typeof body.operation !== 'string') {
      return reply.status(400).send({
        ok: false,
        mode: getRelayMode(),
        operation: body?.operation ?? 'unknown',
        requestId: body?.requestId ?? randomUUID(),
        latencyMs: 0,
        error: 'operation is required',
        errorCode: 'VALIDATION_ERROR',
      } satisfies RelayCallResponse);
    }

    const mode = getRelayMode();
    const requestId = body.requestId || randomUUID();
    const started = Date.now();
    const trace = startTrace(requestId, body.operation, mode, body.args ?? []);

    try {
      const result = await runWithRelayAttribution(
        body.attribution,
        () => dispatchRelayOperation(body.operation, body.args ?? [], mode),
      );
      const latencyMs = Date.now() - started;
      finishTrace(trace, result, latencyMs);
      return {
        ok: true,
        mode,
        operation: body.operation,
        requestId,
        latencyMs,
        result,
      } satisfies RelayCallResponse;
    } catch (error) {
      const latencyMs = Date.now() - started;
      failTrace(trace, error, latencyMs);
      const statusCode = error instanceof RelayValidationError ? error.statusCode : 502;
      const errorCode = error instanceof RelayValidationError ? error.code : 'RELAY_OPERATION_FAILED';
      return reply.status(statusCode).send({
        ok: false,
        mode,
        operation: body.operation,
        requestId,
        latencyMs,
        error: error instanceof Error ? error.message : String(error),
        errorCode,
      } satisfies RelayCallResponse);
    }
  });

  return app;
}

if (require.main === module) {
  const app = buildRelayServer();
  let shuttingDown = false;

  const shutdown = async (signal: string) => {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[HirezRelay] ${signal} received; draining in-flight work`);
    try {
      const { disableApiKeySync } = await import('./api-key-sync.js');
      disableApiKeySync();
      await Promise.allSettled([
        app.close(),
        waitForActiveWorkerJobs(Number(process.env.SHUTDOWN_DRAIN_TIMEOUT_MS || 60_000)),
      ]);
      await releaseLiveOwnerLease();
      await Promise.allSettled([closeRedis(), closeDatabase()]);
    } finally {
      process.exit(0);
    }
  };

  process.on('SIGTERM', () => void shutdown('SIGTERM'));
  process.on('SIGINT', () => void shutdown('SIGINT'));

  initializeRealMode()
    .then(() => app.listen({ host: HOST, port: PORT }))
    .then(() => {
      console.log(`[HirezRelay] listening on http://${HOST}:${PORT} mode=${getRelayMode()}`);
    })
    .catch((error) => {
      console.error('[HirezRelay] failed to start', error);
      process.exitCode = 1;
    });
}
