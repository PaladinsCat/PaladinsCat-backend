import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { test } from 'node:test';
import {
  decodePresenceDetailCursor,
  encodePresenceDetailCursor,
  parsePresenceDetailLimit,
  parsePresenceEvidenceLimit,
  parsePresenceEvidencePage,
  parsePresenceDetailQueueId,
  parsePresencePlayerSort,
} from '../workers/presence-detail-policy';

const statsRouteSource = readFileSync(join(__dirname, '../routes/stats.ts'), 'utf8');
const playerPresenceEvidenceSource = readFileSync(
  join(__dirname, '../workers/player-presence-evidence.ts'),
  'utf8',
);

test('presence detail pages stay within a bounded public page size', () => {
  assert.equal(parsePresenceDetailLimit(undefined), 25);
  assert.equal(parsePresenceDetailLimit('5'), 10);
  assert.equal(parsePresenceDetailLimit('30'), 30);
  assert.equal(parsePresenceDetailLimit('500'), 50);
});

test('compact evidence pages support larger but bounded text-only payloads', () => {
  assert.equal(parsePresenceEvidenceLimit(undefined), 250);
  assert.equal(parsePresenceEvidenceLimit('10'), 50);
  assert.equal(parsePresenceEvidenceLimit('300'), 300);
  assert.equal(parsePresenceEvidenceLimit('1000'), 500);
});

test('compact evidence page numbers are positive and bounded', () => {
  assert.equal(parsePresenceEvidencePage(undefined), 1);
  assert.equal(parsePresenceEvidencePage('-4'), 1);
  assert.equal(parsePresenceEvidencePage('12'), 12);
  assert.equal(parsePresenceEvidencePage('2000000'), 1_000_000);
});

test('player evidence defaults to most matches and accepts alphabetical sorting', () => {
  assert.equal(parsePresencePlayerSort(undefined), 'matches');
  assert.equal(parsePresencePlayerSort('matches'), 'matches');
  assert.equal(parsePresencePlayerSort('alphabetical'), 'alphabetical');
  assert.equal(parsePresencePlayerSort('unknown'), 'matches');
});

test('presence detail queue IDs reject values outside PostgreSQL integer range', () => {
  assert.equal(parsePresenceDetailQueueId('486'), 486);
  assert.equal(parsePresenceDetailQueueId(''), null);
  assert.equal(parsePresenceDetailQueueId('-1'), null);
  assert.equal(parsePresenceDetailQueueId('2147483648'), null);
});

test('presence detail cursor round-trips the complete stable sort key', () => {
  const encoded = encodePresenceDetailCursor({
    source_date: '2026-07-25',
    source_hour: 8,
    match_id: '1281000000',
    queue_id: 424,
  });
  assert.deepEqual(decodePresenceDetailCursor(encoded), {
    date: '2026-07-25',
    hour: 8,
    matchId: '1281000000',
    queueId: 424,
  });
});

test('presence detail cursor rejects malformed or incomplete keys', () => {
  assert.equal(decodePresenceDetailCursor('not-base64-json'), null);
  assert.equal(
    decodePresenceDetailCursor(
      Buffer.from(JSON.stringify({
        date: '2026-07-25',
        hour: 25,
        matchId: '1281000000',
        queueId: 424,
      })).toString('base64url'),
    ),
    null,
  );
  assert.equal(
    decodePresenceDetailCursor(
      Buffer.from(JSON.stringify({
        date: '2026-99-99',
        hour: 1,
        matchId: '1281000000',
        queueId: 424,
      })).toString('base64url'),
    ),
    null,
  );
  assert.equal(
    decodePresenceDetailCursor(
      Buffer.from(JSON.stringify({
        date: '2026-07-25',
        hour: 1,
        matchId: '9223372036854775808',
        queueId: 424,
      })).toString('base64url'),
    ),
    null,
  );
});

test('presence detail evidence stays anchored to discovery and all match fact stores', () => {
  const routeStart = statsRouteSource.indexOf("fastify.get('/presence/details'");
  const routeEnd = statsRouteSource.indexOf("fastify.get('/skins'", routeStart);
  const route = statsRouteSource.slice(routeStart, routeEnd);

  assert.ok(routeStart >= 0);
  assert.match(route, /FROM match_count_discoveries d/);
  assert.match(route, /q\.track_presence = TRUE/);
  assert.match(route, /FROM match_players mp/);
  assert.match(route, /FROM casual_match_players cmp/);
  assert.match(route, /FROM special_match_players smp/);
  assert.match(route, /playersByMatch\.get\(String\(row\.match_id\)\) \?\? \[\]/);
});

test('compact evidence feeds preserve full match and cross-queue player authority', () => {
  assert.match(statsRouteSource, /fastify\.get\('\/presence\/match-ids'/);
  assert.match(statsRouteSource, /fastify\.get\('\/presence\/players'/);
  assert.match(statsRouteSource, /WITH \$\{PUBLIC_PLAYER_EVIDENCE_CTES_SQL\}/);
  assert.match(playerPresenceEvidenceSource, /FROM match_count_discoveries d/);
  assert.match(playerPresenceEvidenceSource, /COALESCE\(\s+d\.entry_datetime AT TIME ZONE 'UTC',\s+d\.source_date \+ \(d\.source_hour \* interval '1 hour'\)\s+\)/);
  assert.match(playerPresenceEvidenceSource, /JOIN match_players mp ON mp\.match_id = discovery\.match_id/);
  assert.match(playerPresenceEvidenceSource, /JOIN casual_match_players cmp ON cmp\.match_id = discovery\.match_id/);
  assert.match(playerPresenceEvidenceSource, /JOIN special_match_players smp ON smp\.match_id = discovery\.match_id/);
  assert.match(statsRouteSource, /FROM participation_counts/);
  assert.match(statsRouteSource, /COUNT\(DISTINCT match_id\)::int AS matches_played/);
  assert.match(statsRouteSource, /COUNT\(DISTINCT match_id\) FROM participation/);
  assert.match(statsRouteSource, /SUM\(matches_played\) FROM participation_counts/);
  assert.doesNotMatch(statsRouteSource, /COALESCE\(participation_counts\.matches_played, 0\)/);
  assert.match(statsRouteSource, /matches_played DESC, player_id/);
  assert.match(statsRouteSource, /LOWER\(player_name\), player_id/);
});

test('presence overview uses the same player facts as the evidence feed', () => {
  const routeStart = statsRouteSource.indexOf("fastify.get('/presence'");
  const routeEnd = statsRouteSource.indexOf("fastify.get('/presence/match-ids'", routeStart);
  const route = statsRouteSource.slice(routeStart, routeEnd);

  assert.ok(routeStart >= 0);
  assert.match(route, /WITH \$\{PUBLIC_PLAYER_EVIDENCE_CTES_SQL\}/);
  assert.match(route, /COUNT\(DISTINCT player_id\)::int AS players/);
  assert.match(route, /FROM public_identities/);
  assert.doesNotMatch(route, /FROM player_queue_presence_24h/);
  assert.doesNotMatch(route, /FROM player_presence_24h/);
});

test('presence uncertainty never infers occupied training or PvE slots', () => {
  assert.match(playerPresenceEvidenceSource, /match_uncertainty AS MATERIALIZED/);
  assert.match(
    playerPresenceEvidenceSource,
    /participant_model IN \('bots', 'pve'\)\s+THEN COALESCE\(roster\.observed_unresolved_slots, 0\)/,
  );
  assert.match(
    playerPresenceEvidenceSource,
    /ELSE GREATEST\(\s*10 - COALESCE\(roster\.known_public_players, 0\)/,
  );
  assert.match(playerPresenceEvidenceSource, /observed_unresolved_slots/);
  assert.match(
    statsRouteSource,
    /unresolved_player_slots_upper: Number\(\s*publicPresence\?\.unresolved_player_slots_upper/,
  );
  assert.match(
    statsRouteSource,
    /public_players_upper_bound: Number\(publicPresence\?\.public_players \?\? 0\)\s*\+ Number\(publicPresence\?\.unresolved_player_slots_upper/,
  );
  assert.match(statsRouteSource, /summary\.unresolved_player_slots_upper/);
});

test('presence demographics group unique public identities by resolved profile region', () => {
  const routeStart = statsRouteSource.indexOf("fastify.get('/presence'");
  const routeEnd = statsRouteSource.indexOf("fastify.get('/presence/match-ids'", routeStart);
  const route = statsRouteSource.slice(routeStart, routeEnd);

  assert.ok(routeStart >= 0);
  assert.match(route, /candidate\.region/);
  assert.match(route, /profile\.active_player_id = identity\.player_id/);
  assert.match(route, /COALESCE\(NULLIF\(BTRIM\(region\), ''\), 'Unknown'\) AS region/);
  assert.match(route, /public_by_region: Array\.isArray\(publicPresence\?\.public_by_region\)/);
});
