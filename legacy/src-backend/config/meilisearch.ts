import { MeiliSearch } from 'meilisearch';

// CRITICAL: Do NOT throw on missing MEILISEARCH_URL. MeiliSearch is an optional
// search layer — the server should start with degraded functionality (PostgreSQL
// only) rather than crash entirely. The old code threw here, preventing the
// backend from starting if the env var was unset. This is unacceptable for an
// optional service.
// Source: Debug 2026-05-31 — "Startup crash on missing optional service"
const host = process.env.MEILISEARCH_URL;
if (!host) {
  console.warn('[meilisearch] MEILISEARCH_URL not set — search functionality disabled');
}

// CRITICAL: Request timeout (ms). Guard against NaN from invalid env var.
// parseInt('abc', 10) → NaN → MeiliSearch client rejects NaN timeout
// and silently disables it → requests hang indefinitely.
// Source: Fault #5 — "NaN from parseInt on MEILISEARCH_TIMEOUT_MS"
const rawTimeout = process.env.MEILISEARCH_TIMEOUT_MS;
const requestTimeout = rawTimeout ? (isNaN(parseInt(rawTimeout, 10)) ? 5000 : parseInt(rawTimeout, 10)) : 5000;

// CRITICAL: Do NOT default to 'masterKey'. If MEILISEARCH_API_KEY is unset,
// the client should refuse to connect rather than use a well-known default
// that grants full admin access. In production with the default MeiliSearch
// config, 'masterKey' is the actual admin key — using it by default exposes
// the entire search index to unauthorized access.
// Source: Fault #2 — "Hardcoded 'masterKey' default"
const apiKey = process.env.MEILISEARCH_API_KEY;

export const meilisearch = host ? new MeiliSearch({
  host,
  apiKey: apiKey || undefined,
  timeout: requestTimeout,
}) : undefined;

export async function healthCheck(): Promise<boolean> {
  if (!meilisearch) return false;
  // CRITICAL: Add timeout to prevent indefinite hang on unreachable MeiliSearch.
  // Without it, getStats() blocks forever -> /health endpoint never responds
  // -> process health checks fail -> container orchestrator restarts the service.
  // Source: Fault #6 — "No timeout on healthCheck()"
  try {
    await Promise.race([
      meilisearch.getStats(),
      new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), 5000)),
    ]);
    return true;
  } catch {
    return false;
  }
}
