import assert from 'node:assert/strict';
import test from 'node:test';
import { calculateKda } from '../utils/kda';

test('calculateKda counts assists at half value', () => {
  assert.equal(calculateKda(4, 8, 15), 1.4375);
  assert.equal(calculateKda(21, 6, 20), 31 / 6);
});

test('calculateKda uses a denominator of one for zero deaths', () => {
  assert.equal(calculateKda(3, 0, 11), 8.5);
  assert.equal(calculateKda(0, 0, 0), 0);
});
