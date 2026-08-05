import { query, one } from '../config/db';
import { getMatchIdsByQueue } from '../services/hirez';
import { RANKED_QUEUES } from '../config/api';
import { filterAlreadyHandledMatchIds } from './ingest-guards';

export interface PullEntry {
  matchId: number;
  queueId: number;
  entryDatetime: string;
  status: string;
}

/**
 * Populate match_pull_list for a specific queue and hour window.
 *
 * Hi-Rez API: getmatchidsbyqueue/{queueId}/{date}/{hour}
 * Returns ALL matches for that queue/hour across all regions.
 *
 * @param queueId - Queue ID (e.g., 486 for ranked)
 * @param date - Date string in YYYYMMDD format
 * @param hour - Hour in UTC (0-23, -1 for entire day)
 */
export async function populate(queueId: number, date: string, hour: number): Promise<number> {
  const matchIds = await getMatchIdsByQueue(queueId, date, hour, 'legacy_match_discovery');
  console.log(`Fetched ${matchIds.length} match IDs for queue ${queueId}, date ${date}, hour ${hour}`);

  const guard = await filterAlreadyHandledMatchIds(matchIds);
  const trulyNew = guard.fetchIds;

  if (guard.skipped.totalUnique > 0) {
    console.log(
      `Skipped ${guard.skipped.totalUnique} already-handled match IDs ` +
      `(matches=${guard.skipped.matches}, players=${guard.skipped.matchPlayers}, ` +
      `buffer=${guard.skipped.rawBuffer}, pull_list=${guard.skipped.pullList})`
    );
  }

  if (trulyNew.length === 0) {
    console.log('No new matches to add to pull list');
    return 0;
  }

  // ----------------------------------------------------------------
  // Batch insert to prevent N+1 query lockup.
  // Previously used a sequential for...of loop with await one() per ID,
  // forcing Node.js to wait for each individual database round-trip.
  // With thousands of missing matches (e.g., gap-checker backfill), this
  // locked the PostgreSQL connection pool and stalled the entire pipeline.
  // Now: single INSERT ... VALUES ($1,$2,...),($3,$4,...) query handles
  // all IDs in one round-trip. ON CONFLICT (match_id) DO NOTHING prevents
  // duplicates if two callers race on the same match.
  // Source: User report 2026-06-01 — "N+1 database choke in populate()"
  // ----------------------------------------------------------------
  const values: string[] = [];
  const params: any[] = [];
  let paramIdx = 1;

  for (const id of trulyNew) {
    values.push(`($${paramIdx++}, $${paramIdx++}, now(), 'pending')`);
    params.push(id, queueId);
  }

  await query(
    `INSERT INTO match_pull_list (match_id, queue_id, entry_datetime, status)
     VALUES ${values.join(', ')}
     ON CONFLICT (match_id) DO NOTHING`,
    params
  );

  console.log(`Added ${trulyNew.length} new matches to pull list (batch insert)`);
  return trulyNew.length;
}

/**
 * Populate all queues for a given date/hour.
 *
 * @param date - Date string in YYYYMMDD format
 * @param hour - Hour in UTC (0-23, -1 for entire day)
 */
export async function populateAll(date: string, hour: number): Promise<number> {
  let total = 0;
  for (const queueId of RANKED_QUEUES) {
    const count = await populate(queueId, date, hour);
    total += count;
  }
  return total;
}

/**
 * Get current pull list status
 */
export async function getStatus(): Promise<any> {
  const pending = await one('SELECT COUNT(*) as count FROM match_pull_list WHERE status = \'pending\'');
  const pulling = await one('SELECT COUNT(*) as count FROM match_pull_list WHERE status = \'pulling\'');
  const total = await one('SELECT COUNT(*) as count FROM match_pull_list');
  return {
    pending: pending?.count || 0,
    pulling: pulling?.count || 0,
    total: total?.count || 0,
  };
}
