/**
 * =====================================================================
 * redis-queue.ts — Deprecated Redis Queue (No-Op)
 * =====================================================================
 * DEPRECATED 2026-05-31: This module is now a no-op. Processing happens
 * directly via processBufferBatch() reading from the PostgreSQL
 * raw_ingest_buffer. The Redis queue was never consumed — messages
 * accumulated indefinitely, causing an infinite memory leak in Redis.
 *
 * These functions are retained as empty shims for API compatibility
 * with workers/match-ingestion.ts, which still calls them. They return
 * immediately without touching Redis.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */

/**
 * DEPRECATED: No-op. Processing uses the DB buffer directly.
 * Retained for API compatibility with workers/match-ingestion.ts.
 */
export async function enqueueProcess(_matchId: number): Promise<void> {
  return Promise.resolve();
}

/**
 * DEPRECATED: No-op. Processing uses the DB buffer directly.
 * Retained for API compatibility with workers/match-ingestion.ts.
 */
export async function enqueueProcessBatch(_matchIds: number[]): Promise<void> {
  return Promise.resolve();
}
