import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  ACTIVITY_PROFILE_BATCH_SIZE,
  ACTIVITY_PROFILE_TTL_HOURS,
  chunkActivityProfileIds,
  requestedIdsSatisfiedByProfiles,
  uniquePlayerIds,
} from '../workers/player-activity-profile-policy';

test('activity profile refresh uses one-day TTL and getplayerbatch maximum', () => {
  assert.equal(ACTIVITY_PROFILE_TTL_HOURS, 24);
  assert.equal(ACTIVITY_PROFILE_BATCH_SIZE, 20);
});

test('active-player profiles are refreshed by the owned background scheduler', () => {
  const registry = readFileSync(
    join(__dirname, '../workers/scheduler-registry.ts'),
    'utf8',
  );
  assert.match(
    registry,
    /key: 'auto_ingester'[\s\S]*?enable:\s*\(\)\s*=>\s*\{[\s\S]*?enableAutoIngest\(\);[\s\S]*?enablePlayerActivityProfile\(\);/,
  );
  assert.doesNotMatch(registry, /key: 'player_activity_profile_enrichment'/);
});

test('profile candidates are positive, unique, and chunked at twenty', () => {
  const ids = [0, -1, 1, 1, ...Array.from({ length: 44 }, (_, index) => index + 2)];
  assert.equal(uniquePlayerIds(ids).length, 45);
  assert.deepEqual(chunkActivityProfileIds(ids).map(batch => batch.length), [20, 20, 5]);
  assert.ok(chunkActivityProfileIds(ids).every(batch => batch.length <= 20));
});

test('merged profile responses satisfy both requested aliases', () => {
  const satisfied = requestedIdsSatisfiedByProfiles(
    [10, 11, 12, 13],
    [
      { Id: 10, ActivePlayerId: 11 },
      { Id: 12, ret_msg: 'Player is private.' },
      { player_id: 13 },
    ],
  );
  assert.deepEqual([...satisfied].sort((left, right) => left - right), [10, 11, 13]);
});

test('profile refresh state update gives every PostgreSQL parameter an explicit type', () => {
  const source = readFileSync(
    join(__dirname, '../workers/player-activity-profile-enrichment.ts'),
    'utf8',
  );
  assert.match(source, /next_retry_at = CASE \$8::text/);
  assert.match(source, /WHEN 'ttl' THEN now\(\) \+ \(\$3::int \* interval '1 hour'\)/);
  assert.match(source, /WHEN 'failed' THEN now\(\) \+ \(\$4::int \* interval '1 minute'\)/);
  assert.match(source, /WHEN 'never' THEN NULL/);
  assert.doesNotMatch(source, /status IN \('success', 'unavailable'/);
  assert.doesNotMatch(source, /const retryExpression/);
});

test('profile freshness lookup keeps primary and active IDs on separate index paths', () => {
  const source = readFileSync(
    join(__dirname, '../workers/player-activity-profile-enrichment.ts'),
    'utf8',
  );
  assert.match(source, /WHERE profile\.id = refresh\.player_id[\s\S]*UNION ALL/);
  assert.match(source, /WHERE profile\.active_player_id = refresh\.player_id/);
  assert.match(
    source,
    /WHERE profile\.active_player_id = refresh\.player_id\s+AND profile\.active_player_id > 0/,
  );
  assert.doesNotMatch(
    source,
    /profile\.id = refresh\.player_id\s+OR profile\.active_player_id = refresh\.player_id/,
  );
});

test('public activity platform coverage uses the same indexed alias lookup', () => {
  const source = readFileSync(join(__dirname, '../routes/stats.ts'), 'utf8');
  const activityRoute = source.slice(
    source.indexOf("fastify.get('/presence'"),
    source.indexOf("fastify.get('/skins'"),
  );
  assert.match(activityRoute, /UNION ALL/);
  assert.match(activityRoute, /profile\.active_player_id > 0/g);
  assert.doesNotMatch(
    activityRoute,
    /profile\.id = identity\.player_id\s+OR profile\.active_player_id = identity\.player_id/,
  );
});

test('profile enrichment reconciles its cache from the shared evidence facts first', () => {
  const source = readFileSync(
    join(__dirname, '../workers/player-activity-profile-enrichment.ts'),
    'utf8',
  );
  assert.match(source, /PUBLIC_PLAYER_EVIDENCE_CTES_SQL/);
  assert.match(source, /export async function reconcilePlayerPresenceCache/);
  assert.match(source, /SELECT DISTINCT\s+player_id, match_id, queue_id, stats_scope, observed_at/);
  assert.match(source, /INSERT INTO player_presence_24h/);
  assert.match(source, /INSERT INTO player_queue_presence_24h/);
  assert.match(
    source,
    /await reconcilePlayerPresenceCache\(\);\s+await seedRefreshLedger\(\);/,
  );
});
