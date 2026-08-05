import crypto from 'crypto';

// Kept only in this backend process. It lets composed Fastify routes opt out
// of public-request accounting without trusting a client-controlled header.
const internalRequestToken = crypto.randomBytes(32).toString('hex');
const INTERNAL_REQUEST_HEADER = 'x-pc-internal-request';

export function internalRequestHeaders(): Record<string, string> {
  return { [INTERNAL_REQUEST_HEADER]: internalRequestToken };
}

export function isInternalRequest(req: { headers?: Record<string, unknown> }): boolean {
  const value = req.headers?.[INTERNAL_REQUEST_HEADER];
  return typeof value === 'string' && value === internalRequestToken;
}
