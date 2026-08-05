import assert from 'node:assert/strict';
import test from 'node:test';
import {
  isIncompleteDirectMatch,
  matchPayloadRequiresRecovery,
  shouldPreserveBrokenSkinBatchResponse,
} from '../services/batch-int16';
import {
  isValidCompletedMatchScore,
  reconcileSiegeMatchScore,
  resolveDirectMatchScore,
  resolveRecoveredMatchScoreSources,
  resolveCompletedMatchScore,
  resolveHistoryMatchScore,
  resolvePlayerOutcomeConsensus,
} from '../services/ranked-score';
import { extractMatchMetadata, normalizeMatchHistoryPlayer } from '../services/normalizer';

function playerRow(matchId: number, playerId: number) {
  return {
    Match: matchId,
    Entry_Datetime: '07/11/2026 01:00:00 PM',
    Map_Game: 'LIVE Jaguar Falls',
    Match_Queue_Id: 486,
    Match_Duration: 900,
    Team1Score: 4,
    Team2Score: 2,
    Winning_TaskForce: 1,
    playerId,
    playerName: `Player${playerId}`,
    ChampionId: 2000 + playerId,
    SkinId: 1000 + playerId,
    TaskForce: playerId <= 5 ? 1 : 2,
    ret_msg: null,
  };
}

test('only preserves getmatchdetailsbatch broken-skin Int16 sentinels', () => {
  const int16 = 'Value was either too large or too small for an Int16. Failing Field = skin_id';

  assert.equal(shouldPreserveBrokenSkinBatchResponse('getmatchdetailsbatch', [int16]), true);
  assert.equal(shouldPreserveBrokenSkinBatchResponse('getmatchdetails', [int16]), false);
  assert.equal(
    shouldPreserveBrokenSkinBatchResponse('getmatchdetailsbatch', ['Invalid session id.']),
    false,
  );
  assert.equal(
    shouldPreserveBrokenSkinBatchResponse('getmatchdetailsbatch', [int16, 'Invalid session id.']),
    false,
  );
});

test('keeps healthy matches and exposes only the sentinel match as incomplete', () => {
  const healthyRows = Array.from({ length: 10 }, (_, index) => playerRow(1001, index + 1));
  const brokenPrefix = Array.from({ length: 7 }, (_, index) => playerRow(1002, index + 11));
  const sentinel = {
    ...playerRow(1002, 0),
    ChampionId: 0,
    SkinId: 32768,
    ret_msg: 'Value was either too large or too small for an Int16. Failing Field = skin_id',
  };
  const healthyMatch = { match_id: 1001, players: healthyRows };
  const brokenMatch = { match_id: 1002, players: [...brokenPrefix, sentinel] };

  assert.equal(isIncompleteDirectMatch(healthyMatch), false);
  assert.equal(isIncompleteDirectMatch(brokenMatch), true);
  assert.equal(healthyMatch.players.length, 10);
  assert.equal(brokenMatch.players.length, 8);
});

test('preserves a coherent direct result for every queue over a contradictory demo snapshot', () => {
  assert.equal(isValidCompletedMatchScore(4, 3, 1), true);
  assert.equal(isValidCompletedMatchScore(400, 397, 1), true);
  assert.equal(isValidCompletedMatchScore(1, 0, 2), false);
  assert.deepEqual(
    resolveCompletedMatchScore({ team1: 4, team2: 3, winner: 1 }),
    { team1: 4, team2: 3, winner: 1, canonicalized: false, source: 'direct' },
  );
});

test('derives one winner from repeated player win and loss outcomes', () => {
  assert.deepEqual(
    resolvePlayerOutcomeConsensus([
      { task_force: 1, win_status: 'Win' },
      { task_force: 1, win_status: 'Winner' },
      { task_force: 2, win_status: 'Loss' },
      { task_force: 2, win_status: 'Loser' },
    ]),
    { coherent: true, observations: 4, winner: 1 },
  );
  assert.deepEqual(
    resolvePlayerOutcomeConsensus([
      { task_force: 1, win_status: 'Win' },
      { task_force: 2, win_status: 'Win' },
    ]),
    { coherent: false, observations: 2, winner: null },
  );
});

test('repairs a strictly reversed Siege score using unanimous player outcomes', () => {
  const consensus = resolvePlayerOutcomeConsensus([
    { task_force: 1, win_status: 'Win' },
    { task_force: 2, win_status: 'Loss' },
  ]);

  assert.deepEqual(
    reconcileSiegeMatchScore({ team1: 0, team2: 4, winner: 1 }, consensus),
    { team1: 4, team2: 0, winner: 1, canonicalized: true, source: 'direct' },
  );
  assert.deepEqual(
    reconcileSiegeMatchScore({ team1: 0, team2: 4, winner: 2 }, consensus),
    { team1: 4, team2: 0, winner: 1, canonicalized: true, source: 'direct' },
  );
});

test('does not rewrite an ambiguous or unsupported Siege result', () => {
  assert.equal(
    reconcileSiegeMatchScore(
      { team1: 0, team2: 4, winner: 2 },
      { coherent: true, observations: 1, winner: null },
    )?.canonicalized,
    false,
  );
  assert.equal(
    reconcileSiegeMatchScore(
      { team1: 0, team2: 0, winner: 1 },
      { coherent: true, observations: 5, winner: 1 },
    ),
    null,
  );
  assert.equal(
    reconcileSiegeMatchScore(
      { team1: 0, team2: 4, winner: 2 },
      { coherent: false, observations: 2, winner: null },
    ),
    null,
  );
});

test('requires repeated coherent direct observations for recovered matches', () => {
  assert.equal(resolveDirectMatchScore([{ team1: 4, team2: 1, winner: 1 }]), null);
  assert.deepEqual(
    resolveDirectMatchScore([
      { team1: 4, team2: 1, winner: 1 },
      { team1: '4', team2: '1', winner: '1' },
    ]),
    { team1: 4, team2: 1, winner: 1, canonicalized: false, source: 'direct' },
  );
});

test('uses unanimous getmatchhistory score with no demo input', () => {
  const history = [
    { team1: 1, team2: 4, winner: 2 },
    { team1: '1', team2: '4', winner: '2' },
  ];

  assert.deepEqual(
    resolveHistoryMatchScore(history),
    { team1: 1, team2: 4, winner: 2, canonicalized: false, source: 'history' },
  );
  assert.deepEqual(
    resolveRecoveredMatchScoreSources(
      [],
      history,
    ),
    { team1: 1, team2: 4, winner: 2, canonicalized: false, source: 'history' },
  );
});

test('rejects conflicting or malformed getmatchhistory score observations', () => {
  assert.equal(
    resolveHistoryMatchScore([
      { team1: 4, team2: 1, winner: 1 },
      { team1: 4, team2: 3, winner: 1 },
    ]),
    null,
  );
  assert.equal(
    resolveHistoryMatchScore([{ team1: 0, team2: 1, winner: 1 }]),
    null,
  );
  assert.equal(
    resolveHistoryMatchScore([{ team1: 4, team2: 1, winner: 1 }]),
    null,
  );
});

test('retains match score fields when normalizing getmatchhistory players', () => {
  const normalized = normalizeMatchHistoryPlayer({
    Match: 1005,
    playerId: 42,
    Team1Score: 4,
    Team2Score: 3,
    Winning_TaskForce: 1,
    TaskForce: 1,
    Win_Status: 'Win',
  });

  assert.equal(normalized.history_team1_score, 4);
  assert.equal(normalized.history_team2_score, 3);
  assert.equal(normalized.history_winning_task_force, 1);
});

test('normalization preserves null until the final score boundary rejects it', () => {
  const recoveredWinner = {
    ...playerRow(1003, 1),
    Team1Score: 4,
    Team2Score: null,
    Winning_TaskForce: 1,
    source: 'recovered',
  };
  const recoveredLoser = {
    ...playerRow(1004, 1),
    Team1Score: null,
    Team2Score: 4,
    Winning_TaskForce: 2,
    source: 'recovered',
  };

  assert.equal(extractMatchMetadata([recoveredWinner]).team2_score, null);
  assert.equal(extractMatchMetadata([recoveredLoser]).team1_score, null);
  assert.equal(extractMatchMetadata([{ ...recoveredWinner, Team2Score: 0 }]).team2_score, 0);
});

test('identifies only match payloads that need outbound recovery', () => {
  const healthy = Array.from({ length: 10 }, (_, index) => playerRow(2001, index + 1));
  const partial = healthy.slice(0, 7);
  const sentinel = {
    ...playerRow(2001, 0),
    ret_msg: 'Value was either too large or too small for an Int16. Failing Field = skin_id',
  };

  assert.equal(matchPayloadRequiresRecovery(healthy), false);
  assert.equal(matchPayloadRequiresRecovery(partial), true);
  assert.equal(matchPayloadRequiresRecovery([...partial, sentinel]), true);
  assert.equal(matchPayloadRequiresRecovery({ invalid: true }), false);
});
