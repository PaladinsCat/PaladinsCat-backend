import assert from 'node:assert/strict';
import test from 'node:test';
import {
  LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE,
  limitedMatchReason,
} from '../workers/limited-match-policy';

test('retains an authoritative partial roster after one empty anchor response', () => {
  assert.equal(limitedMatchReason({
    playerCount: 7,
    teamOneCount: 2,
    teamTwoCount: 5,
    allRowsAuthoritative: true,
    recoverySource: 'no_player_anchors',
    recoveryTerminal: true,
    recoveryApiCalls: 1,
    anchorPlayerCount: 0,
  }), LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE);
});

test('zero public anchors is terminal even when a direct private row survived', () => {
  assert.equal(limitedMatchReason({
    playerCount: 7,
    teamOneCount: 2,
    teamTwoCount: 5,
    allRowsAuthoritative: true,
    recoveryApiCalls: 1,
    anchorPlayerCount: 0,
  }), LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE);
});

test('retains an authoritative partial roster after the single anchor request fails', () => {
  assert.equal(limitedMatchReason({
    playerCount: 7,
    teamOneCount: 5,
    teamTwoCount: 2,
    allRowsAuthoritative: true,
    recoverySource: 'getplayerbatchfrommatch_failed',
    recoveryApiCalls: 1,
  }), LIMITED_MATCH_REASON_ROSTER_UNAVAILABLE);
});

test('does not terminally limit retryable or non-authoritative payloads', () => {
  const base = {
    playerCount: 7,
    teamOneCount: 5,
    teamTwoCount: 2,
    allRowsAuthoritative: true,
    recoverySource: 'no_player_anchors',
    recoveryTerminal: true,
    recoveryApiCalls: 1,
    anchorPlayerCount: 0,
  };

  assert.equal(limitedMatchReason({ ...base, recoveryApiCalls: 0 }), null);
  assert.equal(limitedMatchReason({ ...base, recoveryApiCalls: 2 }), null);
  assert.equal(limitedMatchReason({ ...base, allRowsAuthoritative: false }), null);
  assert.equal(limitedMatchReason({ ...base, teamOneCount: 6, teamTwoCount: 1 }), null);
  assert.equal(limitedMatchReason({ ...base, playerCount: 10, teamOneCount: 5, teamTwoCount: 5 }), null);
  assert.equal(limitedMatchReason({ ...base, recoverySource: 'target_history_unresolved', recoveryTerminal: false, anchorPlayerCount: 10 }), null);
});
