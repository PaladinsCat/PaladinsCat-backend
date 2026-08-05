import assert from 'node:assert/strict';
import test from 'node:test';
import { overlayCurrentPlayerModeration } from '../services/match-moderation';

test('cached match facts receive the current canonical player moderation', () => {
  const cached = {
    match: { match_id: '1280310401' },
    players: [
      { player_id: '721400977', profile_snapshot: { level: 479, cheater: false, sus_count: 2 } },
      { player_id: '123', profile_snapshot: { level: 50, cheater: false, sus_count: 0 } },
      { player_id: '0', private_player_id: '123', profile_snapshot: { level: 9999, cheater: false, sus_count: 0 } },
    ],
  };

  const current = overlayCurrentPlayerModeration(cached, [
    { id: 721400977, cheater: true, sus_count: 0, verified: true },
    { id: 123, cheater: false, sus_count: 4, verified: false },
  ], [
    { id: 123, cheater: true, sus_count: 3 },
  ]);

  assert.equal(current.players[0].profile_snapshot.cheater, true);
  assert.equal(current.players[0].profile_snapshot.sus_count, 0);
  assert.equal(current.players[0].profile_snapshot.verified, true);
  assert.equal(current.players[0].profile_snapshot.level, 479);
  assert.equal(current.players[1].profile_snapshot.cheater, false);
  assert.equal(current.players[1].profile_snapshot.sus_count, 4);
  assert.equal(current.players[1].profile_snapshot.verified, false);
  assert.equal(current.players[2].profile_snapshot.cheater, true);
  assert.equal(current.players[2].profile_snapshot.sus_count, 3);
  assert.equal(current.players[2].profile_snapshot.level, 9999);
  assert.equal(cached.players[0].profile_snapshot.cheater, false, 'the cached object remains immutable');
});
