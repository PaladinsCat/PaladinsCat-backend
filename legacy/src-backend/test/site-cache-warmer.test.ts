import assert from 'node:assert/strict';
import { after, test } from 'node:test';
import Fastify from 'fastify';
import { close as closeRedis } from '../services/cache';
import {
  ACTIVITY_API_WARM_URLS,
  DEPLOYMENT_CRITICAL_API_WARM_URLS,
  MAIN_API_WARM_URLS,
  mainPageWarmPaths,
} from '../workers/site-cache-warm-targets';
import { warmDeploymentCriticalCaches } from '../workers/site-cache-warmer';
import { canonicalRouteCacheUrl } from '../utils/route-cache';

after(async () => {
  await closeRedis();
});

test('discovers main pages from sitemap priority without retaining public origins', () => {
  const xml = `<?xml version="1.0"?><urlset>
    <url><loc>https://paladinscat.com/</loc><priority>1</priority></url>
    <url><loc>https://paladinscat.com/game/maps?mode=ranked&amp;view=all</loc><priority>0.85</priority></url>
    <url><loc>https://paladinscat.com/champions/ash</loc><priority>0.78</priority></url>
    <url><loc>broken</loc><priority>1</priority></url>
    <url><loc>https://paladinscat.com/</loc><priority>1</priority></url>
  </urlset>`;
  assert.deepEqual(mainPageWarmPaths(xml), ['/', '/game/maps?mode=ranked&view=all']);
});

test('route cache canonicalizes query order and drops tracking-only parameters', () => {
  assert.equal(
    canonicalRouteCacheUrl('/stats/items?tierMax=26&utm_source=discord&tierMin=16&limit=50'),
    '/stats/items?limit=50&tierMax=26&tierMin=16',
  );
  assert.equal(
    canonicalRouteCacheUrl('/stats/items?tierMin=16&limit=50&tierMax=26'),
    '/stats/items?limit=50&tierMax=26&tierMin=16',
  );
});

test('warms deployment-critical bundles and every first-view performance section key', () => {
  for (const url of DEPLOYMENT_CRITICAL_API_WARM_URLS) assert.ok(MAIN_API_WARM_URLS.includes(url));
  const sections = [
    { metric: 'gpm' },
    { metric: 'hpm', role: 'Support' },
    { metric: 'dpm', role: 'Damage' },
    { metric: 'mpm', role: 'Frontline' },
  ];
  for (const { metric, role } of sections) {
    const roleQuery = role ? `&role=${role}` : '';
    assert.ok(MAIN_API_WARM_URLS.includes(
      `/players/leaderboard/performance?metric=${metric}&limit=100${roleQuery}&queueId=486&scope=ranked`,
    ));
    assert.ok(MAIN_API_WARM_URLS.includes(
      `/stats/performance-metrics?metric=${metric}${roleQuery}&queueId=486&scope=ranked`,
    ));
    assert.ok(MAIN_API_WARM_URLS.includes(
      `/players/leaderboard/performance?metric=${metric}&limit=100${roleQuery}&scope=casual`,
    ));
    assert.ok(MAIN_API_WARM_URLS.includes(
      `/stats/performance-metrics?metric=${metric}${roleQuery}&scope=casual`,
    ));
  }
  assert.ok(
    MAIN_API_WARM_URLS.indexOf('/stats/page-data')
      > MAIN_API_WARM_URLS.indexOf('/stats/overview'),
    'composite stats page data must be rebuilt after its overview dependency',
  );
  assert.equal(new Set(MAIN_API_WARM_URLS).size, MAIN_API_WARM_URLS.length);
  assert.ok(MAIN_API_WARM_URLS.includes('/matches/overview?view=activity-v3'));
  assert.ok(MAIN_API_WARM_URLS.includes('/stats/presence?view=activity-v4'));
  assert.deepEqual(
    MAIN_API_WARM_URLS.slice(0, ACTIVITY_API_WARM_URLS.length),
    [...ACTIVITY_API_WARM_URLS],
    'activity aggregates must warm before lower-priority statistics',
  );
  for (const url of ACTIVITY_API_WARM_URLS) {
    assert.ok(DEPLOYMENT_CRITICAL_API_WARM_URLS.includes(url));
  }
  for (const scope of ['tierMin=1&tierMax=15', 'tierMin=16&tierMax=26', 'tierMin=21&tierMax=26']) {
    assert.ok(MAIN_API_WARM_URLS.includes(`/stats/page-data?${scope}`));
    assert.ok(MAIN_API_WARM_URLS.includes(`/stats/items?mode=ranked&limit=200&${scope}`));
  }
});

test('deployment warming visits every critical route with forced revalidation', async () => {
  const app = Fastify({ logger: false });
  const visited: Array<{ url: string; revalidate: string | undefined }> = [];
  const routePaths = new Set(DEPLOYMENT_CRITICAL_API_WARM_URLS.map((url) => (
    new URL(url, 'http://paladinscat.local').pathname
  )));
  for (const path of routePaths) {
    app.get(path, async (request) => {
      visited.push({
        url: request.url,
        revalidate: request.headers['x-pc-route-cache-revalidate'] as string | undefined,
      });
      return { ok: true };
    });
  }

  await app.ready();
  await warmDeploymentCriticalCaches(app);
  assert.deepEqual(visited.map((entry) => entry.url), [...DEPLOYMENT_CRITICAL_API_WARM_URLS]);
  assert.ok(visited.every((entry) => entry.revalidate === '1'));
  await app.close();
});

test('deployment warming reports a failed critical route', async () => {
  const app = Fastify({ logger: false });
  const routePaths = new Set(DEPLOYMENT_CRITICAL_API_WARM_URLS.map((url) => (
    new URL(url, 'http://paladinscat.local').pathname
  )));
  for (const path of routePaths) {
    app.get(path, async (request, reply) => {
      if (request.url === '/matches/overview') return reply.code(503).send({ ok: false });
      return { ok: true };
    });
  }

  await app.ready();
  await assert.rejects(() => warmDeploymentCriticalCaches(app), /1 critical API route/);
  await app.close();
});
