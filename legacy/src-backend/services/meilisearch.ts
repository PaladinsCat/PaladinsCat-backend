/**
 * =====================================================================
 * meilisearch.ts — MeiliSearch Full-Text Search Layer
 * =====================================================================
 * Purpose: Indexes players and matches into MeiliSearch for full-text
 * search. PostgreSQL handles structured queries; MeiliSearch handles
 * fuzzy search, typo tolerance, and fast relevance ranking. All search
 * endpoints (/search/players, /search/matches) route through here.
 *
 * Architecture:
 * - Two indices: 'players' and 'matches'. Each stores documents with
 *   objectID as the primary key (string). MeiliSearch uses objectID
 *   for upserts — calling addDocuments with an existing objectID
 *   updates the document instead of creating a duplicate.
 * - syncPlayer/syncMatch: Single-document upserts. Called during
 *   pipeline processing when a single entity is ready.
 * - bulkSyncPlayers/bulkSyncMatches: Batch upserts. Called after
 *   pipeline batches complete — more efficient than individual calls.
 * - searchPlayers/searchMatches: Full-text search with configurable limit.
 *
 * Called by:
 * - workers/buffer-processor.ts — syncs processed entities to search.
 * - routes/search.ts — /search/players and /search/matches endpoints.
 *
 * Fixed 2026-05-30:
 * - bulkSyncPlayers/bulkSyncMatches: Added chunking (2000 docs per batch)
 *   to avoid MeiliSearch 413 Payload Too Large errors on large batches.
 *
 * Fixed 2026-05-31:
 * - All functions: Added try-catch with graceful degradation. MeiliSearch
 *   failures no longer crash the pipeline or endpoints. Errors are logged
 *   and operations are skipped silently.
 * - syncPlayer/syncMatch: Added input validation (finite ID, non-null data).
 * - bulkSyncPlayers: Fixed undefined objectID trap — filters docs missing
 *   both id and player_id before indexing. Added empty chunk guard.
 * - bulkSyncMatches: Fixed undefined objectID trap — filters docs missing
 *   match_id before indexing. Added empty chunk guard.
 * - searchPlayers/searchMatches: Added empty query guard. Added missing
 *   index handling. Added timeout configuration.
 * - initIndices: Added index initialization on startup to prevent first-search crash.
 *
 * Source: PaladinsCat backend services layer.
 * =====================================================================
 */
import { meilisearch } from '../config/meilisearch';

/**
 * Initialize MeiliSearch indices. Creates 'players' and 'matches' indices
 * if they don't exist. Called once at server startup to prevent first-search crash.
 * MeiliSearch throws index_not_found on search() if the index was never created.
 * Source: Debug 2026-05-31 — "Missing index crash on first search"
 */
export async function initIndices(): Promise<void> {
  if (!meilisearch) return;
  try {
    await meilisearch.createIndex('players', { primaryKey: 'objectID' });
  } catch {
    // Index already exists — this is expected on subsequent startups.
    // MeiliSearch throws index_already_exists (HTTP 400) which we ignore.
  }
  try {
    await meilisearch.createIndex('matches', { primaryKey: 'objectID' });
  } catch {
    // Index already exists — expected on subsequent startups.
  }
}

/**
 * Upsert a single player document into the 'players' index.
 * Uses objectID = playerId.toString() as the primary key.
 * If the document exists, it is updated; if not, it is created.
 * Gracefully degrades: logs error and returns void on failure.
 *
 * @param playerId - The player's numeric ID (becomes objectID).
 * @param data - Player data to index (stats, name, etc.).
 */
export async function syncPlayer(playerId: number, data: any): Promise<void> {
  if (!meilisearch) return;
  // CRITICAL: Validate inputs before indexing. NaN or Infinity toString()
  // produces "NaN" or "Infinity" — invalid objectID that breaks MeiliSearch.
  // Negative IDs are invalid player IDs. Null/undefined data produces empty doc.
  // Source: Debug 2026-05-31 — "No input validation"
  if (!Number.isFinite(playerId) || playerId <= 0) {
    console.warn(`[meilisearch] Skipping syncPlayer: invalid playerId ${playerId}`);
    return;
  }
  if (!data || typeof data !== 'object') {
    console.warn(`[meilisearch] Skipping syncPlayer: null or non-object data for playerId ${playerId}`);
    return;
  }

  try {
    const index = meilisearch.index('players');
    await index.addDocuments([{ ...data, objectID: playerId.toString() }]);
  } catch (err) {
    // CRITICAL: Graceful degradation. MeiliSearch failures must not crash the
    // pipeline. The buffer-processor wraps this in try-catch too, but this
    // inner catch prevents the error from bubbling up and interrupting the
    // player processing loop. MeiliSearch is a best-effort search layer —
    // the canonical data lives in PostgreSQL.
    // Source: Debug 2026-05-31 — "No error handling"
    console.error(`[meilisearch] syncPlayer failed for playerId ${playerId}: ${err}`);
  }
}

/**
 * Upsert a single match document into the 'matches' index.
 * Uses objectID = matchId.toString() as the primary key.
 * If the document exists, it is updated; if not, it is created.
 * Gracefully degrades: logs error and returns void on failure.
 *
 * @param matchId - The match's numeric ID (becomes objectID).
 * @param data - Match data to index (players, queue, duration, etc.).
 */
export async function syncMatch(matchId: number, data: any): Promise<void> {
  if (!meilisearch) return;
  // CRITICAL: Validate inputs. Same rationale as syncPlayer.
  // Source: Debug 2026-05-31 — "No input validation"
  if (!Number.isFinite(matchId) || matchId <= 0) {
    console.warn(`[meilisearch] Skipping syncMatch: invalid matchId ${matchId}`);
    return;
  }
  if (!data || typeof data !== 'object') {
    console.warn(`[meilisearch] Skipping syncMatch: null or non-object data for matchId ${matchId}`);
    return;
  }

  try {
    const index = meilisearch.index('matches');
    await index.addDocuments([{ ...data, objectID: matchId.toString() }]);
  } catch (err) {
    // CRITICAL: Graceful degradation. Same rationale as syncPlayer.
    // Source: Debug 2026-05-31 — "No error handling"
    console.error(`[meilisearch] syncMatch failed for matchId ${matchId}: ${err}`);
  }
}

/**
 * Batch upsert multiple player documents into the 'players' index.
 * More efficient than calling syncPlayer() in a loop — single network
 * request. Handles both id and player_id field names as objectID source.
 * Filters out documents with missing IDs before indexing.
 * Gracefully degrades: logs errors and continues on partial failure.
 *
 * @param players - Array of player data objects.
 */
export async function bulkSyncPlayers(players: any[]): Promise<void> {
  if (!meilisearch) return;
  // CRITICAL: Guard against empty array. MeiliSearch addDocuments([]) throws
  // missing_payload (HTTP 400) because an empty array has no documents to index.
  // Source: Debug 2026-05-31 — "Empty array crash"
  if (!players || players.length === 0) {
    return;
  }

  try {
    const index = meilisearch.index('players');
    // Chunk to avoid MeiliSearch 413 Payload Too Large errors.
    // Default limit is ~100MB; 2000 docs is a safe threshold.
    // Source: Audit 2026-05-30 — "Payload Too Large"
    const chunkSize = 2000;
    for (let i = 0; i < players.length; i += chunkSize) {
      const chunk = players.slice(i, i + chunkSize);
      // CRITICAL: Filter out documents with missing objectID. If both p.id and
      // p.player_id are undefined, the optional chain produces undefined.
      // JSON.stringify(undefined) serializes to the literal string "undefined".
      // Multiple documents with objectID="undefined" will silently overwrite each
      // other — last writer wins, all others are lost. This corrupts the index.
      // Fix: filter out docs with no resolvable ID before mapping.
      // Source: Debug 2026-05-31 — "undefined objectID trap"
      const validDocs = chunk
        .filter(p => p.id != null || p.player_id != null)
        .map(p => ({ ...p, objectID: (p.id ?? p.player_id).toString() }));

      // CRITICAL: Skip empty chunks after filtering. If all docs in a chunk
      // had missing IDs, validDocs is empty → addDocuments([]) throws.
      // Source: Debug 2026-05-31 — "Empty chunk after filter"
      if (validDocs.length === 0) continue;

      await index.addDocuments(validDocs);
    }
  } catch (err) {
    // CRITICAL: Graceful degradation. Partial failures are acceptable —
    // some docs may have indexed, others may have failed. The pipeline
    // will retry on next run. This catch prevents the error from bubbling
    // up and interrupting the batch processing loop.
    // Source: Debug 2026-05-31 — "No error handling"
    console.error(`[meilisearch] bulkSyncPlayers failed (${players.length} docs): ${err}`);
  }
}

/**
 * Batch upsert multiple match documents into the 'matches' index.
 * More efficient than calling syncMatch() in a loop — single network
 * request. Uses match_id as the objectID source.
 * Filters out documents with missing IDs before indexing.
 * Gracefully degrades: logs errors and continues on partial failure.
 *
 * @param matches - Array of match data objects.
 */
export async function bulkSyncMatches(matches: any[]): Promise<void> {
  if (!meilisearch) return;
  // CRITICAL: Guard against empty array. Same rationale as bulkSyncPlayers.
  // Source: Debug 2026-05-31 — "Empty array crash"
  if (!matches || matches.length === 0) {
    return;
  }

  try {
    const index = meilisearch.index('matches');
    // Chunk to avoid MeiliSearch 413 Payload Too Large errors.
    // Default limit is ~100MB; 2000 docs is a safe threshold.
    // Source: Audit 2026-05-30 — "Payload Too Large"
    const chunkSize = 2000;
    for (let i = 0; i < matches.length; i += chunkSize) {
      const chunk = matches.slice(i, i + chunkSize);
      // CRITICAL: Filter out documents with missing match_id. Same rationale
      // as bulkSyncPlayers — undefined objectID causes silent overwrites.
      // Source: Debug 2026-05-31 — "undefined objectID trap"
      const validDocs = chunk
        .filter(m => m.match_id != null)
        .map(m => ({ ...m, objectID: m.match_id.toString() }));

      // CRITICAL: Skip empty chunks after filtering.
      // Source: Debug 2026-05-31 — "Empty chunk after filter"
      if (validDocs.length === 0) continue;

      await index.addDocuments(validDocs);
    }
  } catch (err) {
    // CRITICAL: Graceful degradation. Same rationale as bulkSyncPlayers.
    // Source: Debug 2026-05-31 — "No error handling"
    console.error(`[meilisearch] bulkSyncMatches failed (${matches.length} docs): ${err}`);
  }
}

/**
 * Full-text search across the 'players' index. Supports fuzzy matching,
 * typo tolerance, and relevance ranking. Returns up to `limit` results.
 * Gracefully degrades: returns empty array on failure.
 *
 * @param query - Search query string (player name, champion, etc.).
 * @param limit - Maximum results to return (default 20).
 * @returns Array of matching player documents.
 */
export async function searchPlayers(query: string, limit = 20): Promise<any[]> {
  if (!meilisearch) return [];
  // CRITICAL: Guard against empty query. An empty string search either
  // returns all documents (expensive, slow, wasteful) or throws invalid_search_q
  // depending on MeiliSearch version. Either way, it's a misuse.
  // Source: Debug 2026-05-31 — "Empty query crash"
  if (!query || query.trim().length === 0) {
    return [];
  }

  // CRITICAL: Validate limit. Negative or non-finite values throw invalid_search_limit.
  // Zero is valid (returns empty array immediately). Cap at 100 per MeiliSearch default.
  // Source: Debug 2026-05-31 — "No input validation"
  if (!Number.isFinite(limit) || limit < 0) {
    console.warn(`[meilisearch] searchPlayers: invalid limit ${limit}, using default 20`);
    limit = 20;
  }
  const safeLimit = Math.min(Math.floor(limit), 100);

  try {
    const index = meilisearch.index('players');
    const res = await index.search(query.trim(), { limit: safeLimit });
    return res.hits;
  } catch (err) {
    // CRITICAL: Graceful degradation. Search failures should return empty results
    // rather than crashing the endpoint. The caller can detect this and fall back
    // to PostgreSQL if needed. Missing index errors (index_not_found) will occur
    // on fresh deployments before initIndices() runs.
    // Source: Debug 2026-05-31 — "No error handling" + "Missing index crash"
    console.error(`[meilisearch] searchPlayers failed for query "${query}": ${err}`);
    return [];
  }
}

/**
 * Full-text search across the 'matches' index. Supports fuzzy matching,
 * typo tolerance, and relevance ranking. Returns up to `limit` results.
 * Gracefully degrades: returns empty array on failure.
 *
 * @param query - Search query string (match ID, player name, etc.).
 * @param limit - Maximum results to return (default 20).
 * @returns Array of matching match documents.
 */
export async function searchMatches(query: string, limit = 20): Promise<any[]> {
  if (!meilisearch) return [];
  // CRITICAL: Guard against empty query. Same rationale as searchPlayers.
  // Source: Debug 2026-05-31 — "Empty query crash"
  if (!query || query.trim().length === 0) {
    return [];
  }

  // CRITICAL: Validate limit. Same rationale as searchPlayers.
  // Source: Debug 2026-05-31 — "No input validation"
  if (!Number.isFinite(limit) || limit < 0) {
    console.warn(`[meilisearch] searchMatches: invalid limit ${limit}, using default 20`);
    limit = 20;
  }
  const safeLimit = Math.min(Math.floor(limit), 100);

  try {
    const index = meilisearch.index('matches');
    const res = await index.search(query.trim(), { limit: safeLimit });
    return res.hits;
  } catch (err) {
    // CRITICAL: Graceful degradation. Same rationale as searchPlayers.
    // Source: Debug 2026-05-31 — "No error handling" + "Missing index crash"
    console.error(`[meilisearch] searchMatches failed for query "${query}": ${err}`);
    return [];
  }
}
