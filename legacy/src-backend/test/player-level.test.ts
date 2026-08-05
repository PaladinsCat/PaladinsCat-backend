import assert from 'node:assert/strict';
import test from 'node:test';
import { normalizePlayerProfile } from '../services/normalizer';
import { calculatePlayerLevel, resolvePlayerLevel } from '../services/player-level';

test('calculates account levels from total XP past the Hi-Rez level cap', () => {
  assert.equal(calculatePlayerLevel(0), 1);
  assert.equal(calculatePlayerLevel(40_000), 2);
  assert.equal(calculatePlayerLevel(25_479_999), 49);
  assert.equal(calculatePlayerLevel(25_480_000), 50);
  assert.equal(calculatePlayerLevel(26_480_000), 51);
  assert.equal(calculatePlayerLevel(1_134_204_368), 1158);
});

test('prefers total XP and retains the API level separately during profile normalization', () => {
  const profile = normalizePlayerProfile({
    Id: 721555989,
    Name: 'bosshog27170',
    Level: 999,
    Total_XP: 1_134_204_368,
  });

  assert.equal(profile.level, 1158);
  assert.equal(profile.api_level, 999);
  assert.equal(resolvePlayerLevel(undefined, 42), 42);
});
