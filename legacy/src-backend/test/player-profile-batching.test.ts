import assert from 'node:assert/strict';
import test from 'node:test';
import { API_CONFIG } from '../config/api';
import {
  PLAYER_PROFILE_BATCH_MAX_SIZE,
  PLAYER_PROFILE_BATCH_MIN_SIZE,
  takeReadyPlayerProfileBatch,
  uniquePositivePlayerIds,
} from '../services/player-profile-batching';

test('uses the player endpoint 20-profile cap independently from match batches', () => {
  assert.equal(API_CONFIG.BATCH_SIZE, 10);
  assert.equal(API_CONFIG.PLAYER_BATCH_SIZE, 20);
  assert.equal(PLAYER_PROFILE_BATCH_MIN_SIZE, 10);
  assert.equal(PLAYER_PROFILE_BATCH_MAX_SIZE, 20);
});

test('does not release a partial or single-player background profile batch', () => {
  const pending = new Set(Array.from({ length: 9 }, (_, index) => index + 1));

  assert.deepEqual(takeReadyPlayerProfileBatch(pending), []);
  assert.equal(pending.size, 9);
});

test('releases complete profile batches between 10 and 20 IDs', () => {
  const exactMatch = new Set(Array.from({ length: 10 }, (_, index) => index + 1));
  assert.equal(takeReadyPlayerProfileBatch(exactMatch).length, 10);
  assert.equal(exactMatch.size, 0);

  const twoRostersAndTail = new Set(Array.from({ length: 29 }, (_, index) => index + 1));
  assert.deepEqual(
    takeReadyPlayerProfileBatch(twoRostersAndTail),
    Array.from({ length: 20 }, (_, index) => index + 1),
  );
  assert.equal(twoRostersAndTail.size, 9);
  assert.deepEqual(takeReadyPlayerProfileBatch(twoRostersAndTail), []);
});

test('normalizes and deduplicates profile IDs before batching', () => {
  assert.deepEqual(uniquePositivePlayerIds([1, '2', 2, 0, -1, 'nope', 3]), [1, 2, 3]);
});
