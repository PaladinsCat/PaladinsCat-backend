import { PoolClient } from 'pg';
import { one, transaction } from '../config/db';
import type { MatchIdObservation } from '../contracts/hirez-relay';
import { normalizeRegion } from '../services/normalizer';

let tablesReady = false;

export async function ensureMatchCountDiscoveryTables(): Promise<void> {
  if (tablesReady) return;
  await one(`
    CREATE TABLE IF NOT EXISTS match_count_discoveries (
      match_id BIGINT NOT NULL,
      queue_id INT NOT NULL,
      region VARCHAR(20) NOT NULL DEFAULT 'Unknown',
      entry_datetime TIMESTAMPTZ,
      active_flag BOOLEAN NOT NULL DEFAULT FALSE,
      source_date DATE NOT NULL,
      source_hour INT NOT NULL CHECK (source_hour BETWEEN 0 AND 23),
      first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (match_id, queue_id)
    );
    CREATE INDEX IF NOT EXISTS idx_mcd_window_queue
      ON match_count_discoveries (source_date DESC, source_hour, queue_id);
    CREATE INDEX IF NOT EXISTS idx_mcd_queue_region_window
      ON match_count_discoveries (queue_id, region, source_date DESC, source_hour);

    CREATE TABLE IF NOT EXISTS match_count_discovery_region_hours (
      date DATE NOT NULL,
      hour INT NOT NULL CHECK (hour BETWEEN 0 AND 23),
      queue_id INT NOT NULL,
      region VARCHAR(20) NOT NULL,
      match_count INT NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      PRIMARY KEY (date, hour, queue_id, region)
    );
    CREATE INDEX IF NOT EXISTS idx_mcdrh_window_queue
      ON match_count_discovery_region_hours (date DESC, hour, queue_id);

  `);
  tablesReady = true;
}

function sanitizeObservation(observation: MatchIdObservation): MatchIdObservation | null {
  const matchId = Number(observation.matchId);
  if (!Number.isFinite(matchId) || matchId <= 0) return null;
  const rawEntry = observation.entryDatetime ? new Date(observation.entryDatetime) : null;
  return {
    matchId,
    entryDatetime: rawEntry && Number.isFinite(rawEntry.getTime()) ? rawEntry.toISOString() : null,
    region: normalizeRegion(observation.region),
    activeFlag: observation.activeFlag === true,
  };
}

async function insertObservationChunk(
  client: PoolClient,
  date: string,
  hour: number,
  queueId: number,
  observations: MatchIdObservation[],
): Promise<void> {
  if (observations.length === 0) return;
  const values: string[] = [];
  const params: any[] = [];
  for (const observation of observations) {
    const base = params.length;
    values.push(`($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}::timestamptz, $${base + 5}, $${base + 6}::date, $${base + 7})`);
    params.push(
      observation.matchId,
      queueId,
      observation.region,
      observation.entryDatetime,
      observation.activeFlag,
      date,
      hour,
    );
  }
  await client.query(
    `INSERT INTO match_count_discoveries (
       match_id, queue_id, region, entry_datetime, active_flag, source_date, source_hour
     ) VALUES ${values.join(', ')}
     ON CONFLICT (match_id, queue_id) DO UPDATE
     SET region = CASE
           WHEN EXCLUDED.region <> 'Unknown' THEN EXCLUDED.region
           ELSE match_count_discoveries.region
         END,
         entry_datetime = COALESCE(match_count_discoveries.entry_datetime, EXCLUDED.entry_datetime),
         active_flag = EXCLUDED.active_flag,
         last_seen_at = now()`,
    params,
  );
}

/**
 * Shared storage boundary used by both workers. Queue 486 calls this immediately
 * after its existing getmatchidsbyqueue request, so casual expansion never
 * duplicates ranked discovery calls.
 */
export async function recordMatchCountDiscoveryResult(
  date: string,
  hour: number,
  queueId: number,
  rawObservations: MatchIdObservation[],
  source: string,
): Promise<number> {
  await ensureMatchCountDiscoveryTables();
  const byMatchId = new Map<number, MatchIdObservation>();
  for (const raw of rawObservations) {
    const observation = sanitizeObservation(raw);
    if (observation) byMatchId.set(observation.matchId, observation);
  }
  const observations = [...byMatchId.values()];

  await transaction(async client => {
    for (let offset = 0; offset < observations.length; offset += 1000) {
      await insertObservationChunk(client, date, hour, queueId, observations.slice(offset, offset + 1000));
    }

    // Discovery and acquisition share one durable commit boundary. This avoids
    // a polling race where IDs were visible to the activity count but absent
    // from the detail queue until a later seeding pass. Ranked queue 486 keeps
    // its existing ingestion/debt pipeline.
    if (queueId !== 486 && observations.length > 0) {
      await client.query(
        `INSERT INTO nonranked_match_acquisition (
           match_id, queue_id, stats_scope, source_date, source_hour, region,
           discovered_entry_datetime, active_flag, status,
           first_discovered_at, last_observed_at
         )
         SELECT
           discovery.match_id,
           discovery.queue_id,
           COALESCE(queue.stats_scope, 'other'),
           discovery.source_date,
           discovery.source_hour,
           discovery.region,
           discovery.entry_datetime,
           discovery.active_flag,
           CASE
             WHEN discovery.active_flag THEN 'waiting_for_completion'
             ELSE 'discovered'
           END,
           discovery.first_seen_at,
           discovery.last_seen_at
         FROM match_count_discoveries discovery
         JOIN queue_types queue ON queue.queue_id = discovery.queue_id
         WHERE discovery.queue_id = $3
           AND discovery.source_date = $1::date
           AND discovery.source_hour = $2
         ON CONFLICT (match_id) DO UPDATE SET
           queue_id = EXCLUDED.queue_id,
           stats_scope = EXCLUDED.stats_scope,
           last_observed_at = GREATEST(
             nonranked_match_acquisition.last_observed_at,
             EXCLUDED.last_observed_at
           ),
           region = CASE
             WHEN EXCLUDED.region <> 'Unknown' THEN EXCLUDED.region
             ELSE nonranked_match_acquisition.region
           END,
           discovered_entry_datetime = COALESCE(
             nonranked_match_acquisition.discovered_entry_datetime,
             EXCLUDED.discovered_entry_datetime
           ),
           active_flag = EXCLUDED.active_flag,
           status = CASE
             WHEN nonranked_match_acquisition.status IN (
                    'discovered', 'waiting_for_completion'
                  )
                  AND EXCLUDED.active_flag
               THEN 'waiting_for_completion'
             WHEN nonranked_match_acquisition.status = 'waiting_for_completion'
                  AND NOT EXCLUDED.active_flag
               THEN 'discovered'
             ELSE nonranked_match_acquisition.status
           END,
           updated_at = now()`,
        [date, hour, queueId],
      );
    }

    await client.query(
      `DELETE FROM match_count_discovery_region_hours
       WHERE date = $1::date AND hour = $2 AND queue_id = $3`,
      [date, hour, queueId],
    );
    const regionCounts = new Map<string, number>();
    for (const observation of observations) {
      regionCounts.set(observation.region, (regionCounts.get(observation.region) ?? 0) + 1);
    }
    if (regionCounts.size > 0) {
      const regionValues: string[] = [];
      const regionParams: any[] = [];
      for (const [region, matchCount] of regionCounts) {
        const base = regionParams.length;
        regionValues.push(`($${base + 1}::date, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`);
        regionParams.push(date, hour, queueId, region, matchCount);
      }
      await client.query(
        `INSERT INTO match_count_discovery_region_hours (date, hour, queue_id, region, match_count)
         VALUES ${regionValues.join(', ')}`,
        regionParams,
      );
    }

  });
  return observations.length;
}
