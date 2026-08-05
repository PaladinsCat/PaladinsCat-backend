import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';

process.env.DATABASE_URL ||= 'postgresql://test:test@127.0.0.1:5432/test';

async function calculator() {
  return (await import('../services/performance-projections.js')).calculateWeightedMetricStats;
}

test('frontend eCPM activity bands match backend AFK severity boundaries', () => {
  const source = readFileSync(resolve(__dirname, '../../frontend/lib/ecpm-activity.ts'), 'utf8');
  assert.match(source, /fullAfk: 70/);
  assert.match(source, /partialAfk: 90/);
  assert.match(source, /disconnected: 110/);
  assert.match(source, /engaged: 120/);
  assert.match(source, /possible-disconnect/);
  assert.match(source, /Math\.max\(160/);
});

test('zero eCPM observations affect weighted activity distributions', async () => {
  const calculateWeightedMetricStats = await calculator();
  const stats = calculateWeightedMetricStats([
    { queue_id: 486, role_id: 0, role_name: 'Global', metric: 'egpm', value: 0, sample_count: 2 },
    { queue_id: 486, role_id: 0, role_name: 'Global', metric: 'egpm', value: 80, sample_count: 2 },
  ]);

  assert.equal(stats.length, 1);
  assert.equal(stats[0].min, 0);
  assert.equal(stats[0].mean, 40);
  assert.equal(stats[0].p25, 0);
});

test('all live eCPM projection writers retain zero-value full-AFK samples', () => {
  for (const relativePath of [
    '../workers/baseline-tracker.ts',
    '../services/performance-projections.ts',
    '../services/scalable-stats-projections.ts',
    '../routes/stats.ts',
  ]) {
    const source = readFileSync(resolve(__dirname, relativePath), 'utf8');
    assert.match(source, /['"]egpm['"]/i, `${relativePath} should project eCPM`);
    assert.match(source, /(?:wpm['"],\s*['"]apm['"],\s*['"]egpm|egpm['"]\) AND value = 0|mp\.egpm >= 0)/i,
      `${relativePath} should retain zero eCPM observations`);
  }

  const migration = readFileSync(resolve(__dirname, '../db/migrations/086_include_zero_ecpm_activity.sql'), 'utf8');
  assert.match(migration, /DELETE FROM stats_metric_histogram WHERE metric = 'egpm'/);
  assert.match(migration, /DELETE FROM performance_metric_histogram WHERE metric = 'egpm'/);
  assert.match(migration, /mp\.egpm >= 0/);
});

test('candidate pages preserve review policy and use stable indexed pagination', () => {
  const route = readFileSync(resolve(__dirname, '../routes/stats.ts'), 'utf8');
  assert.match(route, /'possible-disconnect': \{ minimum: 110, maximum: 120,[^}]+automaticFlag: false/);
  assert.match(route, /disconnected: \{ minimum: 90, maximum: 110,[^}]+automaticFlag: false/);
  assert.match(route, /'partial-afk': \{ minimum: 70, maximum: 90,[^}]+automaticFlag: false/);
  assert.match(route, /'full-afk': \{ minimum: 0, maximum: 70,[^}]+automaticFlag: true/);
  assert.match(route, /performance_metric_histogram/);
  assert.match(route, /bracket_counts/);
  assert.match(route, /\(mp\.entry_datetime, mp\.match_id, mp\.player_id\)[\s\S]+< \(\$/);
  assert.match(route, /COALESCE\(mis\.status, 'complete'\) = 'complete'/);
  assert.match(route, /COALESCE\(mp\.source, 'direct'\) IN \('direct', 'recovered'\)/);

  const migration = readFileSync(resolve(__dirname, '../db/migrations/088_ranked_ecpm_candidate_index.sql'), 'utf8');
  assert.match(migration, /entry_datetime DESC, match_id DESC, player_id DESC/);
  assert.match(migration, /egpm >= 0/);
  assert.match(migration, /egpm < 120/);
});
