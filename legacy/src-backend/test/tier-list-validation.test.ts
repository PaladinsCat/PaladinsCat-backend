import assert from 'node:assert/strict';
import test from 'node:test';
import { parseTierListEntries, validateTierListEntries } from '../utils/tier-list-validation';

const champions = new Set([1, 2, 3]);

test('accepts a partial tier list', () => {
  assert.equal(validateTierListEntries([
    { championId: 2, tier: 'S', position: 0 },
  ], champions), null);
});

test('requires at least one placed champion', () => {
  assert.match(validateTierListEntries([], champions) ?? '', /at least one/i);
});

test('rejects duplicate champions, positions, and unknown champions', () => {
  assert.match(validateTierListEntries([
    { championId: 1, tier: 'S', position: 0 },
    { championId: 1, tier: 'A', position: 0 },
  ], champions) ?? '', /only once/i);
  assert.match(validateTierListEntries([
    { championId: 1, tier: 'S', position: 0 },
    { championId: 2, tier: 'S', position: 0 },
  ], champions) ?? '', /position/i);
  assert.match(validateTierListEntries([
    { championId: 99, tier: 'S', position: 0 },
  ], champions) ?? '', /unknown/i);
});

test('rejects malformed entry payloads before catalog validation', () => {
  assert.equal(parseTierListEntries({}), null);
  assert.equal(parseTierListEntries([{ championId: 1, tier: 'Z', position: 0 }]), null);
  assert.equal(parseTierListEntries([{ championId: 1, tier: 'S', position: -1 }]), null);
});
