import assert from 'node:assert/strict';
import test from 'node:test';
import { glickoUpdate, isValidGlickoState } from '../services/glicko2';

const initial = { rating: 1500, deviation: 350, volatility: 0.06 };
const equalOpponent = { rating: 1500, deviation: 350 };

test('a 5v5 outcome has one rating event worth of opponent weight', () => {
  const oneOpponent = glickoUpdate(initial, [equalOpponent], 'win');
  const fiveWeightedOpponents = glickoUpdate(
    initial,
    Array.from({ length: 5 }, () => ({ ...equalOpponent, weight: 0.2 })),
    'win',
  );

  assert.deepEqual(fiveWeightedOpponents, oneOpponent);
});

test('unweighted opponents retain independent-game semantics for callers that need them', () => {
  const result = glickoUpdate(initial, Array.from({ length: 5 }, () => equalOpponent), 'win');
  assert.ok(result.rating > 1800);
  assert.ok(result.deviation < 200);
});

test('finite but unsafe persisted state is rejected before it can propagate', () => {
  assert.equal(isValidGlickoState({ rating: 11982276276700926000, deviation: 25169578517.77, volatility: 28126979.1773 }), false);
  assert.throws(
    () => glickoUpdate({ rating: 1500, deviation: 100, volatility: 0.21 }, [equalOpponent], 'win'),
    /outside safe bounds/,
  );
});
