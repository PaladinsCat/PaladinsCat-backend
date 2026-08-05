import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';
import { normalizeMatchHistoryPlayer, normalizeMatchPlayer } from '../services/normalizer';

test('direct match details use Damage_Player as authoritative total damage', () => {
  const player = normalizeMatchPlayer({
    Damage_Player: 31_310,
    Damage_Done_Physical: 28_979,
    Damage_Done_Magical: 1_039,
    Damage_Done_In_Hand: 22_162,
  });

  assert.equal(player.damage_done_physical, 31_310);
  assert.equal(player.damage_done_magical, 1_039);
  assert.equal(player.damage_done_in_hand, 22_162);
});

test('recovered history keeps combined Damage as the DPM total', () => {
  const player = normalizeMatchHistoryPlayer({
    Damage: 31_310,
    Damage_Done_Physical: 28_979,
    Damage_Done_Magical: 1_039,
  });

  assert.equal(player.damage_done_physical, 31_310);
  assert.equal(player.damage_done_in_hand, 0);
});

test('match display uses total damage and always hides recovered WPM/APM', () => {
  const formatSource = readFileSync(
    resolve(__dirname, '../../frontend/components/match-result/format.ts'),
    'utf8',
  );
  const tableSource = readFileSync(
    resolve(__dirname, '../../frontend/components/match-result/stat-table.tsx'),
    'utf8',
  );

  assert.match(formatSource, /const totalDamage = p\.damage_done_physical;/);
  assert.match(formatSource, /const hasWeaponBreakdown = p\.source !== ["']recovered["'];/);
  assert.doesNotMatch(
    formatSource,
    /p\.damage_done_physical\s*\+\s*p\.damage_done_magical/,
  );
  assert.doesNotMatch(
    formatSource,
    /p\.source !== ["']recovered["']\s*\|\|/,
  );
  assert.doesNotMatch(tableSource, /generated\.matches\.physicalDmg/);
  assert.doesNotMatch(tableSource, /generated\.matches\.magicalDmg/);
});

test('all live WPM/APM writers strictly exclude recovered rows', () => {
  for (const relativePath of [
    '../workers/baseline-tracker.ts',
    '../services/performance-projections.ts',
    '../services/scalable-stats-projections.ts',
    '../routes/stats.ts',
  ]) {
    const source = readFileSync(resolve(__dirname, relativePath), 'utf8');
    assert.doesNotMatch(
      source,
      /source\s*,?\s*'direct'\)\s*<>\s*'recovered'\s+OR/i,
      `${relativePath} must not restore recovered WPM/APM eligibility from partial fields`,
    );
    assert.doesNotMatch(
      source,
      /damage_done_physical\s*,\s*0\)\s*\+\s*COALESCE\(mp\.damage_done_magical/i,
      `${relativePath} must derive APM from authoritative total damage only`,
    );
  }
});

test('forward migration rebuilds WPM/APM from direct total damage only', () => {
  const migration = readFileSync(
    resolve(__dirname, '../db/migrations/106_authoritative_damage_metrics.sql'),
    'utf8',
  );

  assert.match(migration, /COALESCE\(mp\.source, 'direct'\) = 'direct'/);
  assert.match(
    migration,
    /COALESCE\(mp\.damage_done_physical, 0\) - COALESCE\(mp\.damage_done_in_hand, 0\)/,
  );
  assert.doesNotMatch(
    migration,
    /damage_done_physical\s*,\s*0\)\s*\+\s*COALESCE\(mp\.damage_done_magical/i,
  );
  assert.match(migration, /DELETE FROM stats_metric_histogram WHERE metric IN \('wpm', 'apm'\)/);
  assert.match(migration, /DELETE FROM performance_metric_histogram WHERE metric IN \('wpm', 'apm'\)/);
});
