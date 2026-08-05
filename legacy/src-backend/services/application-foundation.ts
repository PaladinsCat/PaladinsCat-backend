import Fastify, { type FastifyInstance } from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import {
  installRequestSecurity,
  RequestSecurityError,
  sendRequestSecurityError,
} from './request-security';
import { registerDeploymentGate } from './deployment-control';
import {
  installDeveloperApiSecurity,
  rewriteDeveloperApiUrl,
} from './developer-api';

const requestStartedAt = Symbol('requestStartedAt');

function configuredCorsOrigins(): Set<string> {
  return new Set(
    (process.env.CORS_ORIGINS
      || 'https://paladinscat.com,https://www.paladinscat.com')
      .split(',')
      .map(origin => origin.trim())
      .filter(Boolean),
  );
}

function isAllowedCorsOrigin(origin: string, configured: Set<string>): boolean {
  if (configured.has(origin)) return true;
  if (process.env.NODE_ENV === 'production') return false;
  try {
    const parsed = new URL(origin);
    return ['localhost', '127.0.0.1', '::1'].includes(parsed.hostname);
  } catch {
    return false;
  }
}

/**
 * Construct the production Fastify boundary. Compatibility harnesses use this
 * same constructor so URL rewriting cannot silently drift from production.
 */
export function createApplicationServer(logger = true): FastifyInstance {
  return Fastify({
    logger,
    rewriteUrl: rewriteDeveloperApiUrl,
  });
}

/**
 * Install the root application middleware in production order.
 *
 * These hooks must remain on the root instance. Registering them through an
 * encapsulated Fastify plugin would leave sibling route plugins unprotected.
 */
export function installApplicationFoundation(fastify: FastifyInstance): void {
  const allowedOrigins = configuredCorsOrigins();
  fastify.addHook('onRequest', async req => {
    (req as any)[requestStartedAt] = performance.now();
  });
  fastify.addHook('onSend', async (req, reply, payload) => {
    const started = (req as any)[requestStartedAt] as number | undefined;
    if (started != null) {
      reply.header('Server-Timing', `app;dur=${(performance.now() - started).toFixed(1)}`);
    }
    return payload;
  });
  fastify.register(cors, {
    origin: (origin, callback) => {
      if (!origin || isAllowedCorsOrigin(origin, allowedOrigins)) {
        return callback(null, true);
      }
      return callback(null, false);
    },
  });
  fastify.register(helmet);
  registerDeploymentGate(fastify);
  installRequestSecurity(fastify);
  installDeveloperApiSecurity(fastify);
}

export function installApplicationErrorHandler(fastify: FastifyInstance): void {
  fastify.setErrorHandler((err, req, reply) => {
    // A route can have already sent a client error before a later hook or
    // promise continuation fails. Writing a second response crashes Node with
    // ERR_HTTP_HEADERS_SENT and takes the whole legacy backend down.
    if (reply.sent) return;
    if (err instanceof RequestSecurityError) {
      return sendRequestSecurityError(err, reply);
    }
    req.log.error(err);
    const statusCode = Number((err as any)?.statusCode);
    const safeStatus = Number.isInteger(statusCode) && statusCode >= 400 && statusCode < 600
      ? statusCode
      : 500;
    const production = process.env.NODE_ENV === 'production';
    reply.status(safeStatus).send({
      error: {
        code: safeStatus >= 500 ? 'INTERNAL_ERROR' : 'REQUEST_ERROR',
        message: production && safeStatus >= 500
          ? 'The request could not be completed.'
          : (err as Error).message,
        requestId: req.id,
      },
    });
  });
}
