import assert from 'node:assert/strict';
import test from 'node:test';

process.env.DATABASE_URL ||= 'postgresql://test:test@127.0.0.1:5432/test';

async function calculator() {
  return (await import('../services/performance-projections.js')).calculateWeightedMetricStats;
}

test('weighted performance stats match percentile_cont semantics without expanding rows', async () => {
  const calculateWeightedMetricStats = await calculator();
  const [stats] = calculateWeightedMetricStats([
    { queue_id: 486, role_id: 0, role_name: 'Global', metric: 'gpm', value: 100, sample_count: 1 },
    { queue_id: 486, role_id: 0, role_name: 'Global', metric: 'gpm', value: 200, sample_count: 2 },
    { queue_id: 486, role_id: 0, role_name: 'Global', metric: 'gpm', value: 400, sample_count: 1 },
  ]);

  assert.deepEqual(stats, {
    queueId: 486,
    roleId: 0,
    roleName: 'Global',
    metric: 'gpm',
    min: 100,
    max: 400,
    mean: 225,
    median: 200,
    mode: 200,
    p10: 130,
    p25: 175,
    p75: 250,
    p90: 340,
    sampleSize: 4,
  });
});

test('KDA mode uses the public one-decimal grouping rule', async () => {
  const calculateWeightedMetricStats = await calculator();
  const [stats] = calculateWeightedMetricStats([
    { queue_id: 486, role_id: 3, role_name: 'Support', metric: 'kda', value: 1.04, sample_count: 2 },
    { queue_id: 486, role_id: 3, role_name: 'Support', metric: 'kda', value: 1.06, sample_count: 3 },
  ]);

  assert.equal(stats.mode, 1.1);
  assert.equal(stats.sampleSize, 5);
});
