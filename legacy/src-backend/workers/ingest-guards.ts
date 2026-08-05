import { query } from '../config/db';

export type MatchIngestGuardResult = {
  fetchIds: number[];
  skippedIds: number[];
  skipped: {
    matches: number;
    matchPlayers: number;
    rawBuffer: number;
    pullList: number;
    totalUnique: number;
  };
};

type MatchIngestGuardOptions = {
  includeRawBuffer?: boolean;
  includePullList?: boolean;
};

function normalizeMatchIds(matchIds: number[]): number[] {
  return [...new Set(matchIds.map(id => Number(id)).filter(id => Number.isFinite(id) && id > 0))];
}

/**
 * Filters out matches that are already final, staged, or claimed elsewhere.
 *
 * This is the API quota guard: workers must call it before any expensive
 * getmatchdetailsbatch/getdemodetails fetch. During a backlog, a match can be
 * absent from `matches` because it is still waiting in raw_ingest_buffer; that
 * is not permission to fetch it again.
 */
export async function filterAlreadyHandledMatchIds(
  matchIds: number[],
  options: MatchIngestGuardOptions = {},
): Promise<MatchIngestGuardResult> {
  const ids = normalizeMatchIds(matchIds);
  const includeRawBuffer = options.includeRawBuffer ?? true;
  const includePullList = options.includePullList ?? true;

  if (ids.length === 0) {
    return {
      fetchIds: [],
      skippedIds: [],
      skipped: { matches: 0, matchPlayers: 0, rawBuffer: 0, pullList: 0, totalUnique: 0 },
    };
  }

  let statusRows: Array<{ match_id: number; status: string }> = [];
  try {
    statusRows = await query(
      `SELECT match_id, status FROM match_ingest_status WHERE match_id = ANY($1)`,
      [ids],
    );
  } catch (error) {
    // Fresh or older databases may not have match_ingest_status until the
    // guardrail migration or buffer worker has created it. In that case, keep
    // legacy behavior: a row in matches/match_players means "handled." Once the
    // table exists, terminal status ('complete' or lookup-only 'limited') becomes the stronger signal and
    // status='processing'/'partial' allows repair fetches instead of hiding
    // half-ingested matches.
    statusRows = [];
  }
  const statusByMatchId = new Map(statusRows.map(row => [Number(row.match_id), row.status]));

  const matchesRows = await query(
    `SELECT match_id FROM matches WHERE match_id = ANY($1)`,
    [ids],
  );
  const matchPlayerRows = await query(
    `SELECT match_id
     FROM match_players
     WHERE match_id = ANY($1)
     GROUP BY match_id
     HAVING count(*) >= 10`,
    [ids],
  );

  const rawBufferRows = includeRawBuffer
    ? await query(
        `SELECT DISTINCT entity_id
         FROM raw_ingest_buffer
         WHERE entity_type = 'match'
           AND entity_id = ANY($1)
           AND status IN ('pending', 'processing')`,
        [ids.map(String)],
      )
    : [];

  const pullListRows = includePullList
    ? await query(
        `SELECT match_id
         FROM match_pull_list
         WHERE match_id = ANY($1)
           AND status IN ('pending', 'pulling', 'completed')`,
        [ids],
      )
    : [];

  const isCompleteOrLegacy = (matchId: number): boolean => {
    const status = statusByMatchId.get(matchId);
    return status === 'complete' || status === 'limited' || status === undefined;
  };

  const matchesSet = new Set(
    matchesRows
      .map((row: any) => Number(row.match_id))
      .filter(isCompleteOrLegacy),
  );
  const matchPlayersSet = new Set(
    matchPlayerRows
      .map((row: any) => Number(row.match_id))
      .filter(isCompleteOrLegacy),
  );
  const rawBufferSet = new Set(rawBufferRows.map((row: any) => Number(row.entity_id)));
  const pullListSet = new Set(pullListRows.map((row: any) => Number(row.match_id)));

  const skippedSet = new Set<number>();
  for (const id of ids) {
    if (matchesSet.has(id) || matchPlayersSet.has(id) || rawBufferSet.has(id) || pullListSet.has(id)) {
      skippedSet.add(id);
    }
  }

  return {
    fetchIds: ids.filter(id => !skippedSet.has(id)),
    skippedIds: [...skippedSet],
    skipped: {
      matches: matchesSet.size,
      matchPlayers: matchPlayersSet.size,
      rawBuffer: rawBufferSet.size,
      pullList: pullListSet.size,
      totalUnique: skippedSet.size,
    },
  };
}
