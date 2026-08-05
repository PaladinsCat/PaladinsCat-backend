import { one, query, shutdown } from '../config/db';
import {
  dumpRawPayloads,
  getDummyApiCallCounts,
  getMatchDetailsBatch as getCompletedMatchDetailsBatch,
  MatchDetails,
  resetDummyMatchScenarios,
  resetDummyApiCallCounts,
  setDummyMatchScenario,
} from '../services/hirez';
import { processBufferBatch } from '../workers/buffer-processor';
import { filterAlreadyHandledMatchIds } from '../workers/ingest-guards';

type ScenarioResult = {
  label: string;
  matchId: number;
  expectedScore: { team1: number; team2: number; winner: number };
  batches: Array<{ processed: number; failed: number }>;
  apiCalls: Record<string, number>;
  match: any;
  status: any;
  sources: any[];
  recoveryStats: any;
};

function nextSyntheticMatchId(offset: number): number {
  const seed = Math.floor(Date.now() / 1000) % 100000;
  return 980000000 + (seed * 10) + offset;
}

async function getMatchDetailsBatch(matchIds: number[]): Promise<MatchDetails[]> {
  const outcomes = await getCompletedMatchDetailsBatch(
    matchIds.map(matchId => ({ matchId })),
    'broken_skin_regression',
  );
  return outcomes.flatMap(outcome => outcome.match ? [outcome.match] : []);
}

async function buildBrokenSkinPayload(matchId: number) {
  const [match] = await getMatchDetailsBatch([matchId]);
  if (!match || !Array.isArray(match.players) || match.players.length !== 10) {
    throw new Error(`Dummy match ${matchId} did not return 10 players`);
  }

  // Keep seven real player rows, then append the same kind of terminal ret_msg
  // sentinel Hi-Rez returns when getmatchdetailsbatch hits a broken skin/Int16
  // response. processMatchPayload filters this sentinel out, sees <10 usable
  // players plus a real ret_msg, and must call the recovery pipeline instead of
  // treating the payload as a private-account or transient-empty response.
  const validPlayers = match.players.slice(0, 7).map(player => ({
    ...player,
    // A surviving direct prefix retains the repeated completed score. The
    // missing players still require targeted history for their match facts,
    // but history must not replace a coherent multi-row direct result.
    Team1Score: match.team1_score,
    Team2Score: match.team2_score,
    Winning_TaskForce: match.winning_task_force,
    ret_msg: null,
    has_ret_msg: false,
  }));
  const brokenSkinSentinel = {
    Match: matchId,
    match_id: matchId,
    Entry_Datetime: match.entry_datetime,
    Map_Game: match.map,
    Match_Queue_Id: match.queue_id,
    match_queue_id: match.queue_id,
    Match_Duration: match.duration_seconds,
    Minutes: match.minutes,
    Region: match.region,
    Team1Score: match.team1_score,
    Team2Score: match.team2_score,
    Winning_TaskForce: match.winning_task_force,
    hasReplay: 'y',
    SkinId: 32768,
    playerId: 0,
    playerName: 'BROKEN_SKIN_SENTINEL',
    ChampionId: 0,
    ret_msg: 'Value was either too large or too small for an Int16 while reading SkinId',
  };

  return {
    match,
    rawData: [...validPlayers, brokenSkinSentinel],
    missingPlayers: match.players.slice(7, 10),
  };
}

async function stageBrokenPayload(matchId: number, rawData: any[]): Promise<void> {
  const inserted = await dumpRawPayloads([{
    endpoint: 'getmatchdetailsbatch',
    entity_type: 'match',
    entity_id: matchId,
    raw_data: rawData,
    source: 'dummy-broken-skin-regression',
  }]);
  if (inserted !== 1) {
    throw new Error(`Expected to stage one broken payload for ${matchId}, staged ${inserted}`);
  }

  // Put the test row at the front of the FIFO buffer without touching unrelated
  // rows. This keeps the script deterministic even if local dummy prefetch rows
  // are still pending from a previous manual experiment.
  await one(
    `UPDATE raw_ingest_buffer
     SET created_at = now() - interval '2 hours'
     WHERE entity_type = 'match' AND entity_id = $1 AND status = 'pending'`,
    [String(matchId)],
  );
}

async function seedHistoryPlayers(matchId: number, players: any[], entryDatetime: string): Promise<void> {
  // These rows simulate target getmatchhistory observations retained from an
  // earlier fetch. Recovery must reuse their player facts and exact score
  // without spending another per-player history call.
  for (const player of players) {
    const rawHistory = {
      ...player,
      Match: matchId,
      Match_Time: entryDatetime,
      playerId: player.player_id,
      Team1Score: player.Team1Score ?? player.team1_score,
      Team2Score: player.Team2Score ?? player.team2_score,
      Winning_TaskForce: player.Winning_TaskForce ?? player.winning_task_force,
      Win_Status: player.win_status === 'Winner' ? 'Win' : 'Loss',
      TaskForce: player.task_force,
      ret_msg: null,
    };
    await one(
      `INSERT INTO player_match_history_entries (
         match_id, player_id, fetched_player_id, entry_datetime, queue_id,
         region, map, champion_id, champion_name, skin_id, skin_name,
         win_status, task_force, source, raw_data, normalized_data,
         observed_at, expires_at
       ) VALUES (
         $1,$2,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,
         'getmatchhistory',$13::jsonb,$14::jsonb,now(),now() + interval '6 hours'
       )
       ON CONFLICT (match_id, player_id) DO UPDATE SET
         raw_data = EXCLUDED.raw_data,
         normalized_data = EXCLUDED.normalized_data,
         observed_at = EXCLUDED.observed_at,
         expires_at = EXCLUDED.expires_at`,
      [
        matchId,
        player.player_id,
        entryDatetime,
        player.queue_id,
        player.region,
        player.map || 'LIVE Jaguar Falls',
        player.champion_id,
        player.champion_name,
        player.skin_id,
        player.skin_name,
        player.win_status,
        player.task_force,
        JSON.stringify(rawHistory),
        JSON.stringify({ ...player, source: 'match_history' }),
      ],
    );
  }
}

async function processUntilComplete(matchId: number): Promise<Array<{ processed: number; failed: number }>> {
  const batches: Array<{ processed: number; failed: number }> = [];
  for (let attempt = 0; attempt < 8; attempt++) {
    batches.push(await processBufferBatch(1));
    const status = await one<{ status: string }>(
      `SELECT status FROM match_ingest_status WHERE match_id = $1`,
      [matchId],
    );
    if (status?.status === 'complete') return batches;
    if (status?.status === 'failed') throw new Error(`Match ${matchId} failed during buffer processing`);
  }
  throw new Error(`Match ${matchId} did not reach complete status after ${batches.length} batches`);
}

async function retireSyntheticPrefetchRows(matchId: number): Promise<void> {
  const prefetchIds = Array.from({ length: 49 }, (_, index) => String(matchId + index + 1));
  await one(
    `UPDATE raw_ingest_buffer
     SET status = 'processed',
         processed_at = now(),
         error_message = 'retired by dummy broken-skin recovery regression test'
     WHERE status = 'pending'
       AND endpoint = 'getmatchhistory'
       AND (
         entity_id = ANY($1)
         OR (entity_id ~ '^\\d+:\\d+$' AND split_part(entity_id, ':', 1) = ANY($1))
       )`,
    [prefetchIds],
  );
}

async function collectScenarioResult(
  label: string,
  matchId: number,
  batches: Array<{ processed: number; failed: number }>,
): Promise<ScenarioResult> {
  const [apiCalls, match, status, sources, recoveryStats] = await Promise.all([
    getDummyApiCallCounts(),
    one(`SELECT match_id, team1_score, team2_score, winning_task_force, broken, recovered, source FROM matches WHERE match_id = $1`, [matchId]),
    one(`SELECT status, completed_stages FROM match_ingest_status WHERE match_id = $1`, [matchId]),
    query(
      `SELECT source, count(*)::int AS count
       FROM match_players
       WHERE match_id = $1
       GROUP BY source
       ORDER BY source`,
      [matchId],
    ),
    one(`SELECT direct_count, recovered_count, missing_count, api_calls, total_calls FROM recovery_stats WHERE match_id = $1`, [matchId]),
  ]);

  return { label, matchId, batches, apiCalls, match, status, sources, recoveryStats } as ScenarioResult;
}

function assertScenario(
  result: ScenarioResult,
  expectedHistoryCalls: number,
  expectedPrefetchRows: number,
  expectedTotalApiCalls: number,
  expectedRecoveryApiCalls = expectedTotalApiCalls,
): void {
  const totalPlayers = result.sources.reduce((sum, row) => sum + Number(row.count), 0);
  const prefetchRows = result.sources
    .filter(row => row.source === 'prefetch')
    .reduce((sum, row) => sum + Number(row.count), 0);
  const recoveredRows = result.sources
    .filter(row => row.source === 'recovered')
    .reduce((sum, row) => sum + Number(row.count), 0);
  const historyCalls = Number(result.apiCalls.getmatchhistory ?? 0);
  const countedApiCalls = Object.values(result.apiCalls).reduce((sum, count) => sum + Number(count), 0);

  if (result.status?.status !== 'complete') throw new Error(`${result.label}: match_ingest_status did not complete`);
  if (!result.match?.broken) throw new Error(`${result.label}: match was not marked broken`);
  if (!result.match?.recovered) throw new Error(`${result.label}: match was not marked recovered`);
  if (Number(result.match?.team1_score) !== result.expectedScore.team1 ||
      Number(result.match?.team2_score) !== result.expectedScore.team2 ||
      Number(result.match?.winning_task_force) !== result.expectedScore.winner) {
    throw new Error(`${result.label}: persisted score did not match authoritative getmatchhistory result`);
  }
  if (totalPlayers !== 10) throw new Error(`${result.label}: expected 10 match_players rows, found ${totalPlayers}`);
  if (prefetchRows !== expectedPrefetchRows) {
    throw new Error(`${result.label}: expected ${expectedPrefetchRows} prefetch rows, found ${prefetchRows}`);
  }
  if (recoveredRows !== 3) {
    throw new Error(`${result.label}: expected all 3 broken-skin players to retain source=recovered, found ${recoveredRows}`);
  }
  if (historyCalls !== expectedHistoryCalls) {
    throw new Error(`${result.label}: expected ${expectedHistoryCalls} getmatchhistory calls, found ${historyCalls}`);
  }
  if (countedApiCalls !== expectedTotalApiCalls) {
    throw new Error(`${result.label}: expected ${expectedTotalApiCalls} total dummy API calls, found ${countedApiCalls}`);
  }
  if (Number(result.recoveryStats?.api_calls ?? -1) !== expectedRecoveryApiCalls) {
    throw new Error(
      `${result.label}: expected ${expectedRecoveryApiCalls} recovery calls, ` +
      `recovery_stats.api_calls=${result.recoveryStats?.api_calls}`,
    );
  }
}

async function runScenario(label: string, matchId: number, withPrefetch: boolean): Promise<ScenarioResult> {
  const { match, rawData, missingPlayers } = await buildBrokenSkinPayload(matchId);
  if (withPrefetch) await seedHistoryPlayers(matchId, missingPlayers, match.entry_datetime);

  await resetDummyApiCallCounts();
  await setDummyMatchScenario(matchId, 'broken_skin');
  await stageBrokenPayload(matchId, rawData);
  const batches = await processUntilComplete(matchId);
  const result = await collectScenarioResult(label, matchId, batches);
  result.expectedScore = {
    team1: Number(match.team1_score),
    team2: Number(match.team2_score),
    winner: Number(match.winning_task_force),
  };
  await retireSyntheticPrefetchRows(matchId);
  await setDummyMatchScenario(matchId, 'complete');
  return result;
}

async function runPrivatePlaceholderScenario(matchId: number): Promise<any> {
  const [match] = await getMatchDetailsBatch([matchId]);
  if (!match || match.players.length !== 10) throw new Error('private placeholder fixture did not return 10 players');

  const rawData = match.players.map((player: any, index: number) => ({
    ...player,
    Match: matchId,
    Entry_Datetime: match.entry_datetime,
    Map_Game: match.map,
    match_queue_id: match.queue_id,
    Match_Duration: match.duration_seconds,
    Minutes: match.minutes,
    Region: match.region,
    Team1Score: match.team1_score,
    Team2Score: match.team2_score,
    Winning_TaskForce: match.winning_task_force,
    ...(index >= 7 ? {
      player_id: 0,
      playerId: 0,
      player_name: 'PRIVATEACCOUNT',
      playerName: 'PRIVATEACCOUNT',
      // Two private rows retain full direct details. The final private row
      // models recovery-only identity loss and must become a zero placeholder.
      ...(index === 9 ? { source: 'minimal', champion_id: 0, ChampionId: 0 } : {}),
    } : {}),
  }));

  await resetDummyApiCallCounts();
  await stageBrokenPayload(matchId, rawData);
  const batches = await processUntilComplete(matchId);
  const [storedMatch, roster, ratings, ingestGuard] = await Promise.all([
    one(`SELECT broken, recovered, private, source FROM matches WHERE match_id = $1`, [matchId]),
    one(`SELECT
           COUNT(*)::INT AS logical_rows,
           COUNT(*) FILTER (
             WHERE player_id > 0 AND champion_id > 0
               AND COALESCE(source, 'direct') IN ('direct', 'recovered')
           )::INT AS metric_rows,
           COUNT(*) FILTER (
             WHERE player_id = 0 AND champion_id > 0 AND source = 'direct'
               AND damage_done_in_hand > 0
           )::INT AS detailed_private_rows,
           COUNT(*) FILTER (
             WHERE player_id = 0 AND COALESCE(champion_id, 0) = 0
               AND player_name = 'PRIVATEACCOUNT' AND source = 'minimal'
               AND kills = 0 AND deaths = 0 AND assists = 0
               AND damage_done_physical = 0 AND healing = 0 AND gold_earned = 0
           )::INT AS private_placeholders,
           COUNT(DISTINCT private_slot) FILTER (WHERE player_id = 0)::INT AS private_slots
         FROM match_players WHERE match_id = $1`, [matchId]),
    one(`SELECT COUNT(*)::INT AS count FROM match_rating_snapshots WHERE match_id = $1`, [matchId]),
    filterAlreadyHandledMatchIds([matchId]),
  ]);

  if (!storedMatch?.broken || storedMatch?.recovered || !storedMatch?.private || storedMatch?.source !== 'minimal') {
    throw new Error(`private placeholder: unexpected match flags ${JSON.stringify(storedMatch)}`);
  }
  if (Number(roster?.logical_rows) !== 10 || Number(roster?.metric_rows) !== 7 ||
      Number(roster?.detailed_private_rows) !== 2 || Number(roster?.private_placeholders) !== 1 ||
      Number(roster?.private_slots) !== 3) {
    throw new Error(`private placeholder: unexpected roster ${JSON.stringify(roster)}`);
  }
  if (Number(ratings?.count) !== 7) {
    throw new Error(`private placeholder: expected 7 rating snapshots, found ${ratings?.count}`);
  }
  if (ingestGuard.fetchIds.length !== 0 || !ingestGuard.skippedIds.includes(matchId)) {
    throw new Error(`private placeholder: completed lookup-first match was not skipped by hourly ingest guard`);
  }
  return { label: 'multiple private rows preserve direct facts and exclude only identities/placeholders', matchId, batches, storedMatch, roster, ratings, ingestGuard };
}

async function main(): Promise<void> {
  await resetDummyMatchScenarios();
  // Keep scenarios more than the dummy 50-match history window apart so a
  // previous scenario's non-target history rows cannot masquerade as roster
  // observations for the next scenario.
  const noPrefetchMatchId = nextSyntheticMatchId(1);
  const prefetchMatchId = nextSyntheticMatchId(101);
  const privatePlaceholderMatchId = nextSyntheticMatchId(201);

  const noPrefetch = await runScenario('broken skin recovery without prefetch', noPrefetchMatchId, false);
  // One canonical getmatchdetailsbatch call + four relay-owned recovery calls
  // (roster + three histories). The roster response already persisted fresh
  // profiles, so the snapshot stage reuses them without getplayerbatch.
  assertScenario(noPrefetch, 3, 0, 5, 4);

  const withPrefetch = await runScenario('broken skin recovery with existing history rows', prefetchMatchId, true);
  // DB-first history recovery spends no recovery calls and carries the exact
  // score through the same metadata handoff used by freshly fetched histories.
  // The downstream profile-snapshot stage still uses one getplayerbatch call to
  // refresh all ten profiles because this branch skipped getplayerbatchfrommatch.
  // The direct canonical lookup and downstream profile snapshot remain; the
  // relay recovery itself is satisfied entirely by durable history rows.
  assertScenario(withPrefetch, 0, 0, 2, 0);

  const privatePlaceholder = await runPrivatePlaceholderScenario(privatePlaceholderMatchId);

  console.log(JSON.stringify({
    ok: true,
    scenarios: [noPrefetch, withPrefetch].map(result => ({
      label: result.label,
      matchId: result.matchId,
      apiCalls: result.apiCalls,
      sources: result.sources,
      recoveryStats: result.recoveryStats,
      batches: result.batches,
    })).concat(privatePlaceholder),
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await resetDummyMatchScenarios().catch(() => false);
    await shutdown();
  });
