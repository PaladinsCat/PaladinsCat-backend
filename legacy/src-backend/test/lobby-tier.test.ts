import assert from 'node:assert/strict';
import test from 'node:test';
import { appendLobbyTierPredicate, lobbyTierQueryString, parseLobbyTierBounds } from '../utils/lobby-tier';

test('lobby tier bounds default to an inactive all-lobbies scope', () => {
  assert.deepEqual(parseLobbyTierBounds({}), { active: false, min: null, max: null });
  assert.equal(lobbyTierQueryString(parseLobbyTierBounds({})!), '');
});

test('lobby tier bounds validate and preserve configured groups', () => {
  const scope = parseLobbyTierBounds({ tierMin: '16', tierMax: '26' });
  assert.deepEqual(scope, { active: true, min: 16, max: 26 });
  assert.equal(lobbyTierQueryString(scope!), 'tierMin=16&tierMax=26');
  assert.equal(parseLobbyTierBounds({ tierMin: '0' }), null);
  assert.equal(parseLobbyTierBounds({ tierMax: '27' }), null);
  assert.equal(parseLobbyTierBounds({ tierMin: '21', tierMax: '15' }), null);
});

test('lobby tier SQL predicates continue the caller parameter sequence', () => {
  const params: unknown[] = [486];
  const where = ['m.queue_id = $1'];
  appendLobbyTierPredicate({ active: true, min: 21, max: 26 }, params, where, 'scope');
  assert.deepEqual(params, [486, 21, 26]);
  assert.deepEqual(where, ['m.queue_id = $1', 'scope.lobby_tier >= $2', 'scope.lobby_tier <= $3']);
});
