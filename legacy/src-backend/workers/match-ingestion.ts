import { query, one } from '../config/db';
import { getMatchDetailsBatch, dumpRawPayloads } from '../services/hirez';
import { filterAlreadyHandledMatchIds } from './ingest-guards';
import { fetchCompletedMatchesContinuously } from './completed-match-batching';

/**
 * ELT Ingest: Fetch raw data → dump to buffer table → enqueue for async processing.
 * This decouples I/O-bound API fetching from CPU-bound normalization.
 */
export async function ingestBatch(batchSize = 10): Promise<{ ingested: number; failed: number }> {
  const pending = await query(
    `WITH claimed AS (
       SELECT match_id
       FROM match_pull_list
       WHERE status = 'pending'
       ORDER BY match_id
       LIMIT $1
       FOR UPDATE SKIP LOCKED
     )
     UPDATE match_pull_list
     SET status = 'pulling'
     WHERE match_id IN (SELECT match_id FROM claimed)
     RETURNING match_id`,
    [batchSize],
  );

  if (pending.length === 0) {
    console.log('No pending matches to ingest');
    return { ingested: 0, failed: 0 };
  }

  const matchIds = pending.map((r: any) => r.match_id);
  const guard = await filterAlreadyHandledMatchIds(matchIds, { includePullList: false });
  const fetchIds = guard.fetchIds;

  let ingested = 0;
  let failed = 0;

  if (guard.skippedIds.length > 0) {
    await one(`UPDATE match_pull_list SET status = 'completed' WHERE match_id = ANY($1)`, [guard.skippedIds]);
    await one(`DELETE FROM match_pull_list WHERE status = 'completed'`);
    console.log(
      `[match-ingestion] Skipped ${guard.skipped.totalUnique} already-handled matches ` +
      `(matches=${guard.skipped.matches}, players=${guard.skipped.matchPlayers}, buffer=${guard.skipped.rawBuffer})`
    );
  }

  if (fetchIds.length === 0) {
    console.log(`Batch complete: 0 buffered, 0 failed`);
    return { ingested: 0, failed: 0 };
  }

  try {
    // Fetch raw data from Hi-Rez API
    const rawPayloads = await fetchRawBatch(fetchIds);

    // Dump to buffer table (fast, single INSERT)
    if (rawPayloads.length > 0) {
      await dumpRawPayloads(rawPayloads);
    }

    // ----------------------------------------------------------------
    // Only mark as completed + delete if fetch AND dump succeeded.
    // Previously these ran outside the try block, so a network error in
    // fetchRawBatch() would still mark matches 'completed' and delete them
    // from match_pull_list — permanently losing un-ingested matches on any
    // transient Hi-Rez outage. Now: completed/delete live inside try.
    // On failure, revert to 'pending' so the next run retries them.
    // Source: User report 2026-06-01 — "silent data deletion: failed batches
    //   marked completed and instantly deleted without ever being ingested."
    // ----------------------------------------------------------------
    const bufferedIds = rawPayloads.map(p => Number(p.entity_id)).filter(id => Number.isFinite(id) && id > 0);
    const bufferedSet = new Set(bufferedIds);
    const missingIds = fetchIds.filter(id => !bufferedSet.has(id));

    if (bufferedIds.length > 0) {
      await one(`UPDATE match_pull_list SET status = 'completed' WHERE match_id = ANY($1) AND status = 'pulling'`, [bufferedIds]);
    }
    if (missingIds.length > 0) {
      await one(`UPDATE match_pull_list SET status = 'pending' WHERE match_id = ANY($1) AND status = 'pulling'`, [missingIds]);
      failed += missingIds.length;
    }
    await one(`DELETE FROM match_pull_list WHERE status = 'completed'`);
    ingested = rawPayloads.length;
  } catch (err) {
    console.error(`Batch fetch failed:`, err);
    // Revert to pending so the next run retries these matches.
    await one(`UPDATE match_pull_list SET status = 'pending' WHERE match_id = ANY($1)`, [fetchIds]);
    failed += fetchIds.length;
  }

  console.log(`Batch complete: ${ingested} buffered, ${failed} failed`);
  return { ingested, failed };
}

/**
 * Fetch raw match data and return un-normalized payloads.
 * Each payload carries the raw JSON array for one match.
 */
async function fetchRawBatch(matchIds: number[]): Promise<{ endpoint: string; entity_type: string; entity_id?: number; raw_data: any[]; source: string; dev_id?: string }[]> {
  const payloads: { endpoint: string; entity_type: string; entity_id?: number; raw_data: any[]; source: string; dev_id?: string }[] = [];
  const requests = [...new Set(
    matchIds.map(Number).filter(id => Number.isInteger(id) && id > 0),
  )].map(matchId => ({ matchId }));
  const outcomes = await fetchCompletedMatchesContinuously(
    requests,
    window => getMatchDetailsBatch(window, 'legacy_match_ingestion'),
  );
  const matches = outcomes.flatMap(outcome => (
    outcome.status !== 'recovery_pending' && outcome.match
      ? [outcome.match]
      : []
  ));

  for (const match of matches) {
    payloads.push({
      endpoint: 'getmatchdetailsbatch',
      entity_type: 'match',
      entity_id: match.match_id,
      raw_data: match.players.map((p: any) => ({
        ...p,
        Match: match.match_id,
        Entry_Datetime: match.entry_datetime,
        Map_Game: match.map,
        match_queue_id: match.queue_id,
        Match_Duration: match.duration_seconds,
        Minutes: match.minutes,
        Region: match.region,
        Team1Score: match.team1_score,
        Team2Score: match.team2_score,
        Winning_TaskForce: match.winning_task_force,
        hasReplay: match.has_replay ? 'y' : 'n',
        recovery_source: match.recovery_source,
        recovery_api_calls: match.recovery_api_calls,
        recovery_attempted: match.recovery_attempted === true,
        recovery_terminal: match.recovery_terminal === true,
        limited: match.limited === true,
      })),
      source: 'batch',
    });
  }

  return payloads;
}


