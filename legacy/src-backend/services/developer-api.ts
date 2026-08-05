import crypto from 'node:crypto';
import { readFileSync } from 'node:fs';
import type { IncomingMessage } from 'node:http';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import {
  installRateLimitHook,
  type RateLimitOptions,
  type RateLimitResult,
} from './rate-limit';

const VERSION_PREFIX = '/v1';
const KEY_HASH_PATTERN = /^[a-f0-9]{64}$/i;
const KEY_PATTERN = /^pc_(?:live|test)_[A-Za-z0-9_-]{43,}$/;
const authenticatedRequests = new WeakSet<object>();
const concurrencyCountedRequests = new WeakSet<object>();

export interface DeveloperApiDecision {
  attempted: boolean;
  supported: boolean;
  anonymous: boolean;
  targetUrl: string;
  statusCode?: number;
  code?: string;
  message?: string;
}

export interface DeveloperApiSecurityOptions {
  keyHash?: string | null;
  rateLimitCheck?: (options: RateLimitOptions) => Promise<RateLimitResult>;
}

const SAFE_PATHS: RegExp[] = [
  /^\/notifications$/,
  /^\/operations\/stats$/,
  /^\/system\/hirez-status$/,
  /^\/hirez-status$/,
  /^\/search\/(?:universal|players|matches)$/,
  /^\/reference\/(?:champions|items|bounty-items|maps|tiers|regions|talents|queues|patches|cards|skins|abilities)(?:\/\d+)?$/,
  /^\/reference\/lookup$/,
  /^\/champions$/,
  /^\/champions\/(?:overview|tiers|top-winrate)$/,
  /^\/champions\/[^/]+$/,
  /^\/champions\/[^/]+\/(?:page-data|patch-history|counters)$/,
  /^\/champions\/[^/]+\/talents\/\d+\/page-data$/,
  /^\/players\/(?:overview|search|bulk)$/,
  /^\/players\/leaderboard\/(?:class|champion-elo|performance)$/,
  /^\/players\/\d+$/,
  /^\/players\/\d+\/(?:matches|champions|charts|loadouts|card-winrates)$/,
  /^\/players\/\d+\/loadouts\/decks\/\d+$/,
  /^\/player-ext\/(?:name-history|merges|status|achievements)\/\d+$/,
  /^\/matches\/(?:overview|batch|recent|search|bans|hourly-stats|compositions)$/,
  /^\/matches\/queue\/\d+$/,
  /^\/matches\/fact\/\d+$/,
  /^\/matches\/\d+$/,
  /^\/live\/matches$/,
  /^\/live\/matches\/\d+$/,
  /^\/live\/ended$/,
  /^\/stats(?:\/.*)?$/,
  /^\/ratings(?:\/.*)?$/,
  /^\/coplay(?:\/.*)?$/,
  /^\/meta\/changelog$/,
  /^\/meta\/(?:items|talents|cards|compositions|top)(?:\/\d+)?$/,
];

const GUARDED_WRITE_PATHS: RegExp[] = [
  /^\/players\/\d+\/refresh$/,
];

function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function originalRequestUrl(req: FastifyRequest): string {
  return String(req.originalUrl || (req.raw as any).originalUrl || req.raw.url || req.url);
}

function parseUrl(rawUrl: string): URL | null {
  try {
    return new URL(rawUrl, 'http://paladinscat.internal');
  } catch {
    return null;
  }
}

function isSafePath(pathname: string): boolean {
  return SAFE_PATHS.some(pattern => pattern.test(pathname));
}

function isGuardedWritePath(pathname: string): boolean {
  return GUARDED_WRITE_PATHS.some(pattern => pattern.test(pathname));
}

export function resolveDeveloperApiRoute(method: string | undefined, rawUrl: string): DeveloperApiDecision {
  const parsed = parseUrl(rawUrl);
  if (!parsed || (parsed.pathname !== VERSION_PREFIX && !parsed.pathname.startsWith(`${VERSION_PREFIX}/`))) {
    return { attempted: false, supported: false, anonymous: false, targetUrl: rawUrl };
  }

  const path = parsed.pathname.slice(VERSION_PREFIX.length) || '/';
  const normalizedMethod = String(method || 'GET').toUpperCase();
  const anonymousTarget = path === '/health'
    ? '/health'
    : path === '/version'
      ? '/meta/version'
      : null;
  const guardedWrite = isGuardedWritePath(path);
  const safePath = anonymousTarget !== null || isSafePath(path) || guardedWrite;

  if (!safePath) {
    return {
      attempted: true,
      supported: false,
      anonymous: false,
      targetUrl: rawUrl,
      statusCode: 404,
      code: 'API_ROUTE_NOT_FOUND',
      message: 'This endpoint is not part of the PaladinsCat v1 API.',
    };
  }

  const methodSupported = ['GET', 'HEAD'].includes(normalizedMethod)
    || (normalizedMethod === 'POST' && guardedWrite);
  if (!methodSupported) {
    return {
      attempted: true,
      supported: false,
      anonymous: false,
      targetUrl: rawUrl,
      statusCode: 405,
      code: 'METHOD_NOT_ALLOWED',
      message: 'This method is not available for the requested PaladinsCat v1 endpoint.',
    };
  }

  if (
    path === '/search/universal'
    && ['true', '1'].includes(String(parsed.searchParams.get('remote') || '').toLowerCase())
  ) {
    return {
      attempted: true,
      supported: false,
      anonymous: false,
      targetUrl: rawUrl,
      statusCode: 400,
      code: 'REMOTE_LOOKUP_NOT_AVAILABLE',
      message: 'Developer API searches are database-only and cannot request an upstream lookup.',
    };
  }

  return {
    attempted: true,
    supported: true,
    anonymous: anonymousTarget !== null,
    targetUrl: `${anonymousTarget || path}${parsed.search}`,
  };
}

export function rewriteDeveloperApiUrl(req: IncomingMessage): string {
  const rawUrl = String(req.url || '/');
  const decision = resolveDeveloperApiRoute(req.method, rawUrl);
  return decision.supported ? decision.targetUrl : rawUrl;
}

export function isDeveloperApiRequest(req: FastifyRequest): boolean {
  return resolveDeveloperApiRoute(req.method, originalRequestUrl(req)).attempted;
}

/**
 * True only after the root v1 security hook authenticated the configured
 * developer key for this exact request. Request-driven Hi-Rez fallbacks use
 * this to select the bounded trusted bucket without treating an arbitrary
 * request carrying a `/v1` path as authorized.
 */
export function isAuthenticatedDeveloperApiRequest(req: FastifyRequest): boolean {
  return authenticatedRequests.has(req.raw);
}

function readConfiguredHash(): string | null {
  const direct = process.env.PALADINSCAT_DEVELOPER_API_KEY_SHA256?.trim();
  if (direct) return direct;

  const file = process.env.PALADINSCAT_DEVELOPER_API_KEY_SHA256_FILE?.trim();
  if (!file) return null;
  try {
    return readFileSync(file, 'utf8').trim();
  } catch (error) {
    throw new Error(
      `Unable to read PALADINSCAT_DEVELOPER_API_KEY_SHA256_FILE: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }
}

function normalizeConfiguredHash(value: string | null | undefined): Buffer | null {
  const normalized = value?.trim();
  if (!normalized) return null;
  if (!KEY_HASH_PATTERN.test(normalized)) {
    throw new Error('PALADINSCAT_DEVELOPER_API_KEY_SHA256 must contain exactly 64 hexadecimal characters');
  }
  return Buffer.from(normalized, 'hex');
}

function bearerToken(req: FastifyRequest): string {
  const header = req.headers.authorization;
  if (typeof header !== 'string') return '';
  const match = header.match(/^Bearer\s+(.+)$/i);
  return match?.[1]?.trim() || '';
}

function keyMatches(candidate: string, expectedHash: Buffer): boolean {
  if (!KEY_PATTERN.test(candidate)) return false;
  const candidateHash = crypto.createHash('sha256').update(candidate, 'utf8').digest();
  return crypto.timingSafeEqual(candidateHash, expectedHash);
}

function appendVary(reply: any, value: string): void {
  const existing = reply.getHeader('Vary');
  const values = String(existing || '')
    .split(',')
    .map((item: string) => item.trim())
    .filter(Boolean);
  if (!values.some((item: string) => item.toLowerCase() === value.toLowerCase())) values.push(value);
  reply.header('Vary', values.join(', '));
}

export function installDeveloperApiSecurity(
  fastify: FastifyInstance,
  options: DeveloperApiSecurityOptions = {},
): void {
  const expectedHash = normalizeConfiguredHash(
    options.keyHash === undefined ? readConfiguredHash() : options.keyHash,
  );
  const keyIdentity = expectedHash?.toString('hex').slice(0, 16) || 'unconfigured';
  const concurrencyLimit = positiveInteger(process.env.DEVELOPER_API_CONCURRENCY_LIMIT, 10);
  let activeRequests = 0;

  fastify.addHook('onRequest', async (req, reply) => {
    const decision = resolveDeveloperApiRoute(req.method, originalRequestUrl(req));
    if (!decision.attempted) return;

    reply.header('X-PaladinsCat-Api-Version', 'v1');
    reply.header('X-Request-Id', req.id);

    if (!decision.supported) {
      return reply.status(decision.statusCode || 404).send({
        error: {
          code: decision.code || 'API_ROUTE_NOT_FOUND',
          message: decision.message || 'This endpoint is not part of the PaladinsCat v1 API.',
          requestId: req.id,
        },
      });
    }

    if (decision.anonymous) return;

    appendVary(reply, 'Authorization');
    reply.header('Cache-Control', 'private, no-store');
    if (!expectedHash) {
      return reply.status(503).send({
        error: {
          code: 'DEVELOPER_API_NOT_CONFIGURED',
          message: 'The PaladinsCat developer API is not configured.',
          requestId: req.id,
        },
      });
    }

    const candidate = bearerToken(req);
    if (!candidate || !keyMatches(candidate, expectedHash)) {
      reply.header('WWW-Authenticate', 'Bearer realm="PaladinsCat API", error="invalid_token"');
      return reply.status(401).send({
        error: {
          code: 'INVALID_API_KEY',
          message: 'A valid PaladinsCat developer API key is required.',
          requestId: req.id,
        },
      });
    }

    authenticatedRequests.add(req.raw);
  });

  installRateLimitHook(fastify, {
    limit: positiveInteger(process.env.DEVELOPER_API_RATE_LIMIT_PER_MINUTE, 120),
    windowMs: 60_000,
    keyFn: () => `developer-api:${keyIdentity}`,
    skip: req => !authenticatedRequests.has(req.raw),
    failOpen: true,
    headerPrefix: 'Developer-RateLimit',
    check: options.rateLimitCheck,
    errorCode: 'DEVELOPER_RATE_LIMITED',
    errorMessage: 'The developer key has reached its per-minute request limit.',
  });

  fastify.addHook('preHandler', async (req, reply) => {
    if (!authenticatedRequests.has(req.raw)) return;
    if (activeRequests >= concurrencyLimit) {
      return reply.status(429).send({
        error: {
          code: 'DEVELOPER_CONCURRENCY_LIMITED',
          message: 'Too many concurrent requests for this developer key.',
          requestId: req.id,
        },
      });
    }
    activeRequests += 1;
    concurrencyCountedRequests.add(req.raw);
  });

  const releaseConcurrency = (req: FastifyRequest) => {
    if (!concurrencyCountedRequests.delete(req.raw)) return;
    activeRequests = Math.max(0, activeRequests - 1);
  };
  fastify.addHook('onError', async req => releaseConcurrency(req));
  fastify.addHook('onResponse', async req => releaseConcurrency(req));
}
