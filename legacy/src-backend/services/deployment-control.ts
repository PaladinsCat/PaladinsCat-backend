import { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { redis, waitForRedisReady } from './cache';
import { isInternalRequest } from './internal-request';

export const DEPLOYMENT_STATE_KEY = 'paladinscat:deployment:state';
export const DEPLOYMENT_BLOCKING_PHASES = ['draining', 'switching', 'warming'] as const;
export const DEPLOYMENT_PHASES = [
  'idle',
  'announced',
  ...DEPLOYMENT_BLOCKING_PHASES,
  'complete',
  'failed',
] as const;

export type DeploymentPhase = typeof DEPLOYMENT_PHASES[number];

export interface DeploymentState {
  id: string;
  phase: DeploymentPhase;
  message: string | null;
  startedAt: string | null;
  updatedAt: string;
  expiresAt: string | null;
}

const IDLE_STATE: DeploymentState = {
  id: '',
  phase: 'idle',
  message: null,
  startedAt: null,
  updatedAt: new Date(0).toISOString(),
  expiresAt: null,
};

let localState: DeploymentState = { ...IDLE_STATE };
let expiryTimer: NodeJS.Timeout | null = null;
let expiryHandler: (() => void | Promise<void>) | null = null;
const activePublicRequests = new Map<FastifyRequest, {
  reply: FastifyReply;
  startedAt: number;
}>();

export function isDeploymentPhase(value: unknown): value is DeploymentPhase {
  return typeof value === 'string' && (DEPLOYMENT_PHASES as readonly string[]).includes(value);
}

export function isDeploymentBlockingPhase(phase: DeploymentPhase): boolean {
  return (DEPLOYMENT_BLOCKING_PHASES as readonly string[]).includes(phase);
}

export function shouldBypassDeploymentGate(req: Pick<FastifyRequest, 'url' | 'headers'>): boolean {
  return isInternalRequest(req)
    || req.url.startsWith('/health')
    || req.url.startsWith('/schedulers')
    || req.url.startsWith('/deployment/status')
    || req.url.startsWith('/admin/deployment');
}

function isExpired(state: DeploymentState): boolean {
  return Boolean(state.expiresAt && Date.parse(state.expiresAt) <= Date.now());
}

function clearExpiryTimer(): void {
  if (expiryTimer) clearTimeout(expiryTimer);
  expiryTimer = null;
}

function notifyExpiry(): void {
  if (!expiryHandler) return;
  Promise.resolve(expiryHandler()).catch((error) => {
    console.error(`[deployment] Failed to resume after deployment state expiry: ${error}`);
  });
}

function applyLocalState(state: DeploymentState): DeploymentState {
  const wasBlocking = isDeploymentBlockingPhase(localState.phase);
  clearExpiryTimer();
  localState = state;

  if (state.expiresAt) {
    const delayMs = Math.max(0, Date.parse(state.expiresAt) - Date.now());
    expiryTimer = setTimeout(() => {
      expiryTimer = null;
      const blockingAtExpiry = isDeploymentBlockingPhase(localState.phase);
      localState = { ...IDLE_STATE, updatedAt: new Date().toISOString() };
      if (blockingAtExpiry) notifyExpiry();
    }, Math.min(delayMs, 2_147_483_647));
    expiryTimer.unref();
  }

  if (wasBlocking && !isDeploymentBlockingPhase(state.phase)) notifyExpiry();
  return localState;
}

function parseState(value: string | null): DeploymentState | null {
  if (!value) return null;
  try {
    const parsed = JSON.parse(value) as Partial<DeploymentState>;
    if (!isDeploymentPhase(parsed.phase) || typeof parsed.id !== 'string') return null;
    return {
      id: parsed.id,
      phase: parsed.phase,
      message: typeof parsed.message === 'string' ? parsed.message : null,
      startedAt: typeof parsed.startedAt === 'string' ? parsed.startedAt : null,
      updatedAt: typeof parsed.updatedAt === 'string' ? parsed.updatedAt : new Date().toISOString(),
      expiresAt: typeof parsed.expiresAt === 'string' ? parsed.expiresAt : null,
    };
  } catch {
    return null;
  }
}

export function configureDeploymentExpiryHandler(handler: () => void | Promise<void>): void {
  expiryHandler = handler;
}

export async function initializeDeploymentControl(): Promise<DeploymentState> {
  // `enableOfflineQueue: false` deliberately makes ordinary cache requests
  // fail fast, including commands issued during ioredis' initial handshake.
  // Deployment state is different: reading it a few milliseconds too early
  // can start schedulers in a replacement container while the stack is meant
  // to remain quiesced. Wait only at process startup, then retain the normal
  // fail-fast behaviour everywhere else.
  const redisReady = await waitForRedisReady(
    Math.max(250, Number(process.env.DEPLOYMENT_REDIS_STARTUP_TIMEOUT_MS) || 5_000),
  );
  if (!redisReady) {
    console.warn('[deployment] Redis was not ready before the startup coordination timeout');
  }
  try {
    const stored = parseState(await redis.get(DEPLOYMENT_STATE_KEY));
    if (!stored || isExpired(stored)) return applyLocalState({ ...IDLE_STATE, updatedAt: new Date().toISOString() });
    return applyLocalState(stored);
  } catch (error) {
    console.warn(`[deployment] Could not load deployment state from Redis: ${error}`);
    return localState;
  }
}

export function getLocalDeploymentState(): DeploymentState {
  if (isExpired(localState)) {
    applyLocalState({ ...IDLE_STATE, updatedAt: new Date().toISOString() });
  }
  return { ...localState };
}

export async function getDeploymentState(): Promise<DeploymentState> {
  try {
    const stored = parseState(await redis.get(DEPLOYMENT_STATE_KEY));
    if (!stored || isExpired(stored)) {
      return applyLocalState({ ...IDLE_STATE, updatedAt: new Date().toISOString() });
    }
    return applyLocalState(stored);
  } catch (error) {
    console.warn(`[deployment] Could not refresh deployment state from Redis: ${error}`);
    return getLocalDeploymentState();
  }
}

export async function setDeploymentState(input: {
  id: string;
  phase: DeploymentPhase;
  message?: string | null;
  ttlSeconds?: number;
}): Promise<DeploymentState> {
  const now = new Date();
  const previous = getLocalDeploymentState();
  const ttlSeconds = Math.min(7200, Math.max(30, Math.trunc(input.ttlSeconds || 1800)));
  const state: DeploymentState = {
    id: input.id.trim().slice(0, 128),
    phase: input.phase,
    message: input.message?.trim().slice(0, 500) || null,
    startedAt: previous.id === input.id && previous.startedAt
      ? previous.startedAt
      : now.toISOString(),
    updatedAt: now.toISOString(),
    expiresAt: new Date(now.getTime() + ttlSeconds * 1000).toISOString(),
  };
  if (!state.id && state.phase !== 'idle') throw new Error('Deployment id is required');

  await redis.setex(DEPLOYMENT_STATE_KEY, ttlSeconds, JSON.stringify(state));
  return applyLocalState(state);
}

function finishCountedRequest(req: FastifyRequest): void {
  activePublicRequests.delete(req);
}

export function registerDeploymentGate(fastify: FastifyInstance): void {
  fastify.addHook('onRequest', async (req: FastifyRequest, reply: FastifyReply) => {
    if (shouldBypassDeploymentGate(req)) return;
    const state = getLocalDeploymentState();
    if (isDeploymentBlockingPhase(state.phase)) {
      reply.header('Cache-Control', 'no-store');
      reply.header('Retry-After', '5');
      return reply.status(503).send({
        error: {
          code: 'DEPLOYMENT_DRAIN',
          message: state.message || 'PaladinsCat is applying an update. Please retry shortly.',
          details: { deploymentId: state.id, phase: state.phase, retryAfterSeconds: 5 },
        },
      });
    }
    activePublicRequests.set(req, { reply, startedAt: Date.now() });
    // Fastify's onError hook is not guaranteed for a client disconnect. The
    // ServerResponse close event covers that path without decrementing early
    // while a handler is still processing a fully-received request.
    reply.raw.once('close', () => finishCountedRequest(req));
  });

  fastify.addHook('onResponse', async (req: FastifyRequest) => finishCountedRequest(req));
  fastify.addHook('onError', async (req: FastifyRequest) => finishCountedRequest(req));
}

export function getActivePublicRequestCount(): number {
  return activePublicRequests.size;
}

export function dropActivePublicRequests(): {
  droppedRequests: number;
  oldestRequestMs: number;
} {
  const now = Date.now();
  const tracked = [...activePublicRequests.entries()];
  let oldestRequestMs = 0;
  for (const [req, entry] of tracked) {
    oldestRequestMs = Math.max(oldestRequestMs, now - entry.startedAt);
    try {
      entry.reply.raw.destroy(new Error('Deployment drain grace period expired'));
    } finally {
      finishCountedRequest(req);
    }
  }
  return { droppedRequests: tracked.length, oldestRequestMs };
}

export async function waitForActivePublicRequests(
  timeoutMs: number,
  pollIntervalMs = 100,
): Promise<{ drained: boolean; activeRequests: number }> {
  const deadline = Date.now() + Math.max(0, timeoutMs);
  while (activePublicRequests.size > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
  }
  return {
    drained: activePublicRequests.size === 0,
    activeRequests: activePublicRequests.size,
  };
}
