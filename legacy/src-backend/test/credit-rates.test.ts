import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';
import {
  calculateAfkRate,
  calculateCreditRates,
  resolveGameplayDuration,
} from '../utils/credit-rates';

test('credit rates use gameplay duration instead of the API player timer', () => {
  // Match 1280863382 has 765 seconds of gameplay and a 785-second player API
  // timer. Loading/waiting overhead must not dilute activity metrics.
  const duration = resolveGameplayDuration(765);
  assert.equal(duration, 765);
  assert.deepEqual(calculateCreditRates(1899, duration), { cpm: 148.94, ecpm: 109.73 });
  assert.deepEqual(calculateCreditRates(3208, duration), { cpm: 251.61, ecpm: 212.39 });
});

test('eCPM removes the 500 starting-credit bias across match lengths', () => {
  assert.deepEqual(calculateCreditRates(850, 300), { cpm: 170, ecpm: 70 });
  assert.deepEqual(calculateCreditRates(1200, 600), { cpm: 120, ecpm: 70 });
  assert.deepEqual(calculateCreditRates(1900, 1200), { cpm: 95, ecpm: 70 });
});

test('gameplay duration and review bands avoid automatic false-positive AFK flags', () => {
  const creditsAtSeventyGameplayEcpm = 1200;
  assert.equal(calculateAfkRate(creditsAtSeventyGameplayEcpm, 600), 0);
  assert.equal(calculateAfkRate(creditsAtSeventyGameplayEcpm, 620), 3);
  assert.equal(calculateAfkRate(1500, 600), 0, '100 eCPM is review-only');
  assert.equal(calculateAfkRate(1200, 600), 0, '70 eCPM is review-only');
  assert.equal(calculateAfkRate(1100, 600), 3, '60 eCPM passive-only pace is an automatic AFK signal');
  assert.equal(calculateAfkRate(800, 600), 3, '30 eCPM remains an automatic AFK signal');
});

test('credit rates remain safe when match time is unavailable', () => {
  assert.equal(resolveGameplayDuration(0), 0);
  assert.deepEqual(calculateCreditRates(1899, 0), { cpm: 0, ecpm: 0 });
  assert.deepEqual(calculateCreditRates(undefined, undefined), { cpm: 0, ecpm: 0 });
});

test('the database derives and backfills gameplay-duration metrics', () => {
  const migration = readFileSync(resolve(__dirname, '../db/migrations/085_match_player_credit_rates.sql'), 'utf8');
  assert.match(migration, /SELECT NULLIF\(m\.duration_seconds, 0\)/);
  assert.match(migration, /COALESCE\(NEW\.gold_earned, 0\)::NUMERIC \* 60 \/ duration_seconds/);
  assert.match(migration, /SET gold_per_minute = ROUND/);
  assert.match(migration, /afk_rate = CASE/);
});

test('the passive-floor calibration preserves review bands without auto-flagging them', () => {
  const migration = readFileSync(resolve(__dirname, '../db/migrations/089_recalibrate_ecpm_passive_floor.sql'), 'utf8');
  assert.match(migration, /effective_cpm >= 70 THEN 0/);
  assert.match(migration, /ELSE 3/);
  assert.doesNotMatch(migration, /effective_cpm >= 90 THEN [123]/);
});
