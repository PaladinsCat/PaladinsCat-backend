import { one, query, shutdown } from '../config/db';
import { getDummyApiCallCounts, resetDummyApiCallCounts } from '../services/hirez';
import { discover } from '../workers/active-match-discovery';
import { processBufferBatch } from '../workers/buffer-processor';

const QUEUE_ID = 486;

function dummyMatchIdsFor(queueId: number, apiDate: string, hour: number): number[] {
  const seed = Number(`${String(queueId).slice(-2)}${apiDate.slice(-2)}${String(hour).padStart(2, '0')}`);
  return Array.from({ length: 6 }, (_, index) => seed * 100 + index + 1);
}

async function findUnusedSyntheticHour(): Promise<{ date: string; apiDate: string; hour: number; ids: number[] }> {
  for (let day = 1; day <= 31; day++) {
    for (let hour = 0; hour < 24; hour++) {
      const date = `2031-01-${String(day).padStart(2, '0')}`;
      const apiDate = date.replace(/-/g, '');
      const ids = dummyMatchIdsFor(QUEUE_ID, apiDate, hour);
      const existingMatches = await one<{ count: string }>(
        `SELECT count(*)::text AS count FROM matches WHERE match_id = ANY($1)`,
        [ids],
      );
      if (Number(existingMatches?.count ?? 0) === 0) {
        // Previous interrupted test runs can leave the synthetic hour in
        // raw_ingest_buffer or match_ingest_status before final `matches` rows
        // exist. Clean only this high-ID dummy target so the assertion below
        // verifies worker behavior, not leftover test staging.
        await one(`DELETE FROM raw_ingest_buffer WHERE entity_type = 'match' AND entity_id = ANY($1)`, [ids.map(String)]);
        await one(`DELETE FROM match_ingest_status WHERE match_id = ANY($1)`, [ids]);
        await one(`DELETE FROM match_players WHERE match_id = ANY($1)`, [ids]);
        await one(`DELETE FROM hourly_ingest_state WHERE date = $1::date AND hour = $2 AND queue_id = $3`, [date, hour, QUEUE_ID]);
        await one(`DELETE FROM hourly_match_counts WHERE date = $1::date AND hour = $2 AND queue_id = $3`, [date, hour, QUEUE_ID]);
        return { date, apiDate, hour, ids };
      }
    }
  }
  throw new Error('No unused synthetic dummy hour is available for hourly ingest state test');
}

async function drainBufferForHour(ids: number[]): Promise<Array<{ processed: number; failed: number }>> {
  const batches: Array<{ processed: number; failed: number }> = [];
  const idSet = new Set(ids.map(String));

  for (let attempt = 0; attempt < 12; attempt++) {
    const result = await processBufferBatch(20);
    batches.push(result);
    const activeRows = await query(
      `SELECT entity_id, status
       FROM raw_ingest_buffer
       WHERE entity_type = 'match'
         AND entity_id = ANY($1)
         AND status IN ('pending', 'processing')`,
      [[...idSet]],
    );
    if (activeRows.length === 0) return batches;
  }

  throw new Error('Timed out draining synthetic hourly ingest buffer rows');
}

async function main(): Promise<void> {
  const target = await findUnusedSyntheticHour();

  await resetDummyApiCallCounts();
  const staged = await discover(QUEUE_ID, target.apiDate, target.hour);
  if (staged !== target.ids.length) {
    throw new Error(`Expected ${target.ids.length} staged dummy payloads, got ${staged}`);
  }

  const firstCalls = await getDummyApiCallCounts();
  const firstTotalCalls = Object.values(firstCalls).reduce((sum, count) => sum + Number(count), 0);
  if (firstTotalCalls === 0) throw new Error('First discovery did not call dummy relay endpoints');

  const batches = await drainBufferForHour(target.ids);
  const stateAfterDrain = await one<any>(
    `SELECT status, raw_match_count, staged_match_count, fetched, fetch_succeeded
     FROM hourly_ingest_state
     WHERE date = $1::date AND hour = $2 AND queue_id = $3`,
    [target.date, target.hour, QUEUE_ID],
  );
  if (stateAfterDrain?.status !== 'complete') {
    throw new Error(`Expected hourly_ingest_state complete after drain, got ${stateAfterDrain?.status}`);
  }

  await resetDummyApiCallCounts();
  const secondStaged = await discover(QUEUE_ID, target.apiDate, target.hour);
  const secondCalls = await getDummyApiCallCounts();
  const secondTotalCalls = Object.values(secondCalls).reduce((sum, count) => sum + Number(count), 0);
  if (secondStaged !== 0) throw new Error(`Expected second discovery to stage 0, got ${secondStaged}`);
  if (secondTotalCalls !== 0) throw new Error(`Expected second discovery to make 0 dummy API calls, got ${secondTotalCalls}`);

  console.log(JSON.stringify({
    ok: true,
    target,
    firstCalls,
    secondCalls,
    stateAfterDrain,
    batches,
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await shutdown();
  });
