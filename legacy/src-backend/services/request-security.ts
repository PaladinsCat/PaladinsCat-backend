import crypto from 'node:crypto';
import net from 'node:net';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { checkRateLimit, installRateLimitHook, type RateLimitResult } from './rate-limit';
import { isInternalRequest } from './internal-request';
import { isAuthenticatedDeveloperApiRequest } from './developer-api';

const SERVICE_TOKEN_HEADER = 'x-paladinscat-service-token';
export const PLAYER_REFRESH_ATTEMPT_LIMIT = 5;
export const PLAYER_REFRESH_WINDOW_MS = 10 * 60_000;

export class RequestSecurityError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string,
    public readonly retryAfter?: number,
  ) {
    super(message);
    this.name = 'RequestSecurityError';
  }
}

function positiveInteger(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function constantTimeEqual(candidate: string, expected: string): boolean {
  const candidateDigest = crypto.createHash('sha256').update(candidate).digest();
  const expectedDigest = crypto.createHash('sha256').update(expected).digest();
  return crypto.timingSafeEqual(candidateDigest, expectedDigest);
}

function bearerToken(req: FastifyRequest): string {
  const auth = req.headers.authorization;
  return typeof auth === 'string' && auth.startsWith('Bearer ') ? auth.slice(7).trim() : '';
}

function serviceToken(req: FastifyRequest): string {
  const header = req.headers[SERVICE_TOKEN_HEADER];
  if (typeof header === 'string' && header.trim()) return header.trim();
  return bearerToken(req);
}

export function isServiceRequest(req: FastifyRequest): boolean {
  const current = process.env.PALADINSCAT_SERVICE_TOKEN?.trim();
  const previous = process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS?.trim();
  const candidate = serviceToken(req);
  if (!current || !candidate) return false;

  // Always evaluate both configured comparisons. The previous credential is a
  // bounded deployment grace mechanism, not a second permanent service key.
  const currentMatches = constantTimeEqual(candidate, current);
  const previousMatches = previous
    ? constantTimeEqual(candidate, previous)
    : false;
  return currentMatches || previousMatches;
}

function isOperatorRequest(req: FastifyRequest): boolean {
  if (
    isInternalRequest(req)
    || isServiceRequest(req)
    || isAuthenticatedDeveloperApiRequest(req)
  ) return true;
  const expected = process.env.ADMIN_SECRET?.trim();
  const candidate = bearerToken(req);
  return Boolean(expected && candidate && constantTimeEqual(candidate, expected));
}

function normalizeIp(value: string): string {
  const trimmed = value.trim().replace(/^\[|\]$/g, '');
  if (net.isIP(trimmed)) return trimmed;
  const ipv4WithPort = trimmed.match(/^(\d{1,3}(?:\.\d{1,3}){3}):\d+$/);
  return ipv4WithPort && net.isIP(ipv4WithPort[1]) ? ipv4WithPort[1] : '';
}

/**
 * Resolve a useful client identity without blindly trusting the left-most XFF
 * value. Caddy appends the peer it observed, so scanning from the right keeps
 * an attacker-supplied prefix from becoming the limiter key.
 *
 * CF-Connecting-IP is only trusted when explicitly enabled. Enabling that flag
 * before the origin firewall accepts traffic exclusively from Cloudflare would
 * let a direct-origin caller choose its own limiter identity.
 */
export function resolveClientAddress(req: FastifyRequest): string {
  if (process.env.TRUST_CLOUDFLARE_HEADERS === 'true') {
    const cloudflare = req.headers['cf-connecting-ip'];
    if (typeof cloudflare === 'string') {
      const normalized = normalizeIp(cloudflare);
      if (normalized) return normalized;
    }
  }

  const forwarded = req.headers['x-forwarded-for'];
  if (typeof forwarded === 'string') {
    const addresses = forwarded.split(',').map(normalizeIp).filter(Boolean);
    if (addresses.length > 0) return addresses[addresses.length - 1];
  }

  return normalizeIp(req.ip) || String(req.ip || 'unknown');
}

export function clientRateLimitIdentity(req: FastifyRequest): string {
  const digest = crypto
    .createHash('sha256')
    .update(resolveClientAddress(req))
    .digest('hex')
    .slice(0, 32);
  return `client:${digest}`;
}

function retryAfter(result: RateLimitResult): number {
  return Math.max(1, Math.ceil((result.resetAt - Date.now()) / 1000));
}

function setLimitHeaders(reply: FastifyReply, prefix: string, result: RateLimitResult): void {
  reply.header(`X-${prefix}-Limit`, result.total);
  reply.header(`X-${prefix}-Remaining`, result.remaining);
  reply.header(`X-${prefix}-Reset`, result.resetAt);
}

async function strictLimit(
  key: string,
  limit: number,
  windowMs: number,
): Promise<RateLimitResult> {
  const result = await checkRateLimit({ key, limit, windowMs, failOpen: false });
  if (!result.backendAvailable) {
    throw new RequestSecurityError(
      503,
      'PROTECTION_UNAVAILABLE',
      'The upstream protection boundary is temporarily unavailable. Cached data remains available.',
      retryAfter(result),
    );
  }
  return result;
}

export interface VendorFallbackGuardOptions {
  scope: string;
  entity?: string | number;
  entityLimit?: number;
  entityWindowMs?: number;
}

export interface PlayerRefreshQuota {
  limit: number;
  remaining: number;
  resetAt: number;
  remainingSeconds: number;
}

/**
 * Limit explicit profile-page refresh actions without coupling the button to
 * the profile row's freshness timestamp. The key is scoped to both the client
 * and player, so one visitor cannot spend another visitor's five attempts.
 */
export async function guardPlayerRefreshAttempt(
  req: FastifyRequest,
  reply: FastifyReply,
  playerId: number,
  check: typeof checkRateLimit = checkRateLimit,
): Promise<PlayerRefreshQuota> {
  const limit = PLAYER_REFRESH_ATTEMPT_LIMIT;
  const windowMs = PLAYER_REFRESH_WINDOW_MS;
  const identity = crypto
    .createHash('sha256')
    .update(`${clientRateLimitIdentity(req)}:${playerId}`)
    .digest('hex')
    .slice(0, 32);
  const result = await check({
    key: `player-refresh:${identity}`,
    limit,
    windowMs,
    failOpen: false,
  });
  setLimitHeaders(reply, 'Player-Refresh', result);

  const remainingSeconds = retryAfter(result);
  if (!result.backendAvailable) {
    throw new RequestSecurityError(
      503,
      'PROTECTION_UNAVAILABLE',
      'Profile refresh protection is temporarily unavailable. Cached player data remains available.',
      remainingSeconds,
    );
  }
  if (!result.allowed) {
    throw new RequestSecurityError(
      429,
      'PROFILE_REFRESH_COOLDOWN',
      'Refresh limit reached. Up to five refreshes are allowed every 10 minutes.',
      remainingSeconds,
    );
  }

  return {
    limit: result.total,
    remaining: result.remaining,
    resetAt: result.resetAt,
    remainingSeconds,
  };
}

/**
 * Shared, fail-closed quota boundary for a request that is about to leave the
 * PaladinsCat database/cache buffer and call Hi-Rez.
 */
export async function guardVendorFallback(
  req: FastifyRequest,
  reply: FastifyReply,
  options: VendorFallbackGuardOptions,
): Promise<void> {
  // Authenticated operators, service callers, and trusted internal requests
  // share the larger trusted bucket. Public callers keep the deliberately
  // small fallback allowance.
  const service = isOperatorRequest(req);
  const windowMs = 60_000;
  const clientLimit = service
    ? positiveInteger(process.env.VENDOR_SERVICE_LIMIT_PER_MINUTE, 60)
    : positiveInteger(process.env.VENDOR_PUBLIC_LIMIT_PER_MINUTE, 8);
  const globalLimit = positiveInteger(process.env.VENDOR_GLOBAL_LIMIT_PER_MINUTE, 180);

  const client = await strictLimit(
    `vendor-fallback:${options.scope}:${clientRateLimitIdentity(req)}:${service ? 'service' : 'public'}`,
    clientLimit,
    windowMs,
  );
  setLimitHeaders(reply, 'Vendor-RateLimit', client);
  if (!client.allowed) {
    throw new RequestSecurityError(
      429,
      'VENDOR_RATE_LIMITED',
      'Too many live-data fallbacks. Please wait for the database buffer to refresh.',
      retryAfter(client),
    );
  }

  // Only a request that passed its client bucket may consume shared capacity.
  // This prevents one already-blocked client from incrementing the global
  // counter until every other visitor is denied.
  const global = await strictLimit('vendor-fallback:global', globalLimit, windowMs);
  setLimitHeaders(reply, 'Vendor-Global-RateLimit', global);
  if (!global.allowed) {
    throw new RequestSecurityError(
      429,
      'VENDOR_GLOBAL_RATE_LIMITED',
      'The live-data fallback is busy. Cached database data remains available.',
      retryAfter(global),
    );
  }

  if (options.entity !== undefined) {
    const entityDigest = crypto
      .createHash('sha256')
      .update(`${options.scope}:${String(options.entity)}`)
      .digest('hex')
      .slice(0, 32);
    const entity = await strictLimit(
      `vendor-fallback:entity:${entityDigest}`,
      options.entityLimit ?? (service ? 2 : 1),
      options.entityWindowMs
        ?? positiveInteger(process.env.VENDOR_ENTITY_RETRY_WINDOW_MS, 120_000),
    );
    if (!entity.allowed) {
      throw new RequestSecurityError(
        429,
        'VENDOR_ENTITY_COOLDOWN',
        'A live-data attempt for this record already ran recently. Cached database data remains available.',
        retryAfter(entity),
      );
    }
  }
}

function isSensitiveOperatorRoute(req: FastifyRequest): boolean {
  const path = req.url.split('?')[0];
  const method = req.method.toUpperCase();
  return path.startsWith('/recovery/')
    || path === '/recovery'
    || path.startsWith('/api/hirez-raw-responses')
    || path.startsWith('/api/raw-responses')
    || path.startsWith('/matches/raw/')
    || path.startsWith('/players/raw/')
    || path === '/api-keys/status'
    || (method === 'POST' && (path === '/matches/pull' || path === '/matches/discover'));
}

function isServiceOnlyRoute(req: FastifyRequest): boolean {
  const path = req.url.split('?')[0];
  return path === '/players/discord' || path.startsWith('/players/discord/');
}

function requiresConfiguredServiceRoute(req: FastifyRequest): boolean {
  return req.url.split('?')[0].startsWith('/players/discord/');
}

export function installSensitiveRouteAuth(fastify: FastifyInstance): void {
  fastify.addHook('preHandler', async (req, reply) => {
    if (isSensitiveOperatorRoute(req) && !isOperatorRequest(req)) {
      return reply.status(401).send({
        error: {
          code: 'OPERATOR_AUTH_REQUIRED',
          message: 'Operator credentials are required for this endpoint.',
        },
      });
    }

    // Compatibility-first service auth: deploy the same token to backend and
    // bot first. The route becomes private automatically once configured.
    if (
      isServiceOnlyRoute(req)
      && (
        requiresConfiguredServiceRoute(req)
        || process.env.PALADINSCAT_SERVICE_TOKEN?.trim()
      )
      && !isServiceRequest(req)
    ) {
      return reply.status(401).send({
        error: {
          code: 'SERVICE_AUTH_REQUIRED',
          message: 'Service credentials are required for this endpoint.',
        },
      });
    }
  });
}

function installAuthenticationAbuseGuard(fastify: FastifyInstance): void {
  fastify.addHook('preHandler', async (req, reply) => {
    const path = req.url.split('?')[0];
    if (req.method !== 'POST' || !['/auth/login', '/auth/register'].includes(path)) return;

    const result = await strictLimit(
      `account-auth:${path}:${clientRateLimitIdentity(req)}`,
      positiveInteger(process.env.ACCOUNT_AUTH_ATTEMPTS_PER_WINDOW, 10),
      positiveInteger(process.env.ACCOUNT_AUTH_WINDOW_MS, 15 * 60_000),
    );
    setLimitHeaders(reply, 'Auth-RateLimit', result);
    if (!result.allowed) {
      throw new RequestSecurityError(
        429,
        'AUTH_RATE_LIMITED',
        'Too many account authentication attempts. Please try again later.',
        retryAfter(result),
      );
    }
  });
}

/**
 * Install security hooks directly on the root Fastify instance. Registering
 * these through fastify.register() would encapsulate them and leave sibling
 * route plugins unprotected.
 */
export function installRequestSecurity(fastify: FastifyInstance): void {
  const configuredServiceToken = process.env.PALADINSCAT_SERVICE_TOKEN?.trim();
  const configuredPreviousServiceToken = process.env.PALADINSCAT_SERVICE_TOKEN_PREVIOUS?.trim();
  for (const [name, token] of [
    ['PALADINSCAT_SERVICE_TOKEN', configuredServiceToken],
    ['PALADINSCAT_SERVICE_TOKEN_PREVIOUS', configuredPreviousServiceToken],
  ] as const) {
    if (token && Buffer.byteLength(token, 'utf8') < 32) {
      throw new Error(`${name} must contain at least 32 bytes when configured`);
    }
  }
  if (configuredPreviousServiceToken && !configuredServiceToken) {
    throw new Error('PALADINSCAT_SERVICE_TOKEN_PREVIOUS requires PALADINSCAT_SERVICE_TOKEN');
  }
  if (
    configuredServiceToken
    && configuredPreviousServiceToken
    && constantTimeEqual(configuredServiceToken, configuredPreviousServiceToken)
  ) {
    throw new Error('PALADINSCAT_SERVICE_TOKEN_PREVIOUS must differ from the current token');
  }

  const skip = (req: FastifyRequest) => isInternalRequest(req)
    || req.url.startsWith('/health')
    || req.url.startsWith('/deployment/status');

  // Reject a saturated client before it can consume the aggregate counter.
  // Otherwise one caller could keep incrementing the global bucket even after
  // its own allowance was exhausted.
  installRateLimitHook(fastify, {
    limit: positiveInteger(process.env.PUBLIC_API_RATE_LIMIT_PER_MINUTE, 300),
    windowMs: 60_000,
    keyFn: clientRateLimitIdentity,
    skip,
    failOpen: true,
  });
  // Independent service-wide ceiling: per-client identity can become less
  // precise during a proxy misconfiguration, but it can never remove this
  // aggregate backstop.
  installRateLimitHook(fastify, {
    limit: positiveInteger(process.env.PUBLIC_API_GLOBAL_LIMIT_PER_MINUTE, 6000),
    windowMs: 60_000,
    keyFn: () => 'public-api:global',
    skip,
    failOpen: true,
    headerPrefix: 'Global-RateLimit',
  });
  installSensitiveRouteAuth(fastify);
  installAuthenticationAbuseGuard(fastify);
}

export function sendRequestSecurityError(
  error: RequestSecurityError,
  reply: FastifyReply,
): FastifyReply {
  if (error.retryAfter) reply.header('Retry-After', error.retryAfter);
  return reply.status(error.statusCode).send({
    error: {
      code: error.code,
      message: error.message,
      ...(error.retryAfter ? { details: { retry_after_seconds: error.retryAfter } } : {}),
    },
  });
}
