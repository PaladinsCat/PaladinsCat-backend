import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';
import { dispatchDummy } from '../hirez-relay/dummy-data';
import { hasPlayerChampionCombatStats, normalizePlayerChampion } from '../services/normalizer';

test('dummy player-champion endpoints preserve the real vendor contract split', async () => {
  const roster = await dispatchDummy('getPlayerChampions', [16706730]) as any[];
  const ranks = await dispatchDummy('getChampionRanks', [16706730]) as any[];

  assert.ok(roster.length > 0);
  assert.ok(ranks.length > 0);
  assert.equal(hasPlayerChampionCombatStats(roster[0]), false);
  assert.equal(hasPlayerChampionCombatStats(ranks[0]), true);
  assert.equal(Object.hasOwn(roster[0], 'XP'), true);
  assert.equal(Object.hasOwn(ranks[0], 'XP'), false);
  assert.equal(Object.hasOwn(ranks[0], 'Worshippers'), true);
  assert.equal(normalizePlayerChampion(ranks[0]).xp, ranks[0].Worshippers);
  assert.equal(normalizePlayerChampion(roster[0]).xp, roster[0].XP);
});

test('player champion normalization prefers XP when both mastery fields are present', () => {
  assert.equal(normalizePlayerChampion({ XP: 2_000, Worshippers: 1_000 }).xp, 2_000);
});

test('combat-stat detection accepts legitimate zero totals but rejects partial rows', () => {
  const zeroStats = {
    PlayerId: 16706730,
    ChampionId: 2404,
    Champion: 'Ash',
    Wins: 0,
    Losses: 0,
    Kills: 0,
    Deaths: 0,
    Assists: 0,
    Minutes: 0,
  };

  assert.equal(hasPlayerChampionCombatStats(zeroStats), true);
  assert.equal(hasPlayerChampionCombatStats({ ...zeroStats, Minutes: '0' }), true);
  assert.equal(hasPlayerChampionCombatStats({ ...zeroStats, Minutes: undefined }), false);
  const { Minutes: _minutes, ...partialStats } = zeroStats;
  assert.equal(hasPlayerChampionCombatStats(partialStats), false);
  assert.deepEqual(normalizePlayerChampion(zeroStats), {
    player_id: 16706730,
    champion_id: 2404,
    champion_name: 'Ash',
    xp: 0,
    ownership_type: '',
    wins: 0,
    losses: 0,
    kills: 0,
    deaths: 0,
    assists: 0,
    minutes_played: 0,
  });
});

test('player champion stats refresh is wired to getchampionranks', () => {
  const routeSource = readFileSync(resolve(__dirname, '../routes/players.ts'), 'utf8');

  assert.match(routeSource, /const raw = await getChampionRanks\([\s\S]*?playerId,[\s\S]*?'manual_profile_refresh'/);
  assert.match(routeSource, /endpoint: 'getchampionranks'/);
  assert.doesNotMatch(routeSource, /const raw = await getPlayerChampions\(playerId\)/);
});

test('Discord player profiles populate cached career totals and never expose placeholder rows', () => {
  const routeSource = readFileSync(resolve(__dirname, '../routes/players.ts'), 'utf8');

  assert.match(routeSource, /DISCORD_PLAYER_CHAMPION_STATS_TTL_MS = 24 \* 60 \* 60 \* 1000/);
  assert.match(routeSource, /await refreshDiscordPlayerChampionStatsIfExpired\([\s\S]*?playerId,[\s\S]*?discord-player-champions/);
  assert.match(routeSource, /WHERE player_id = \$1 AND stats_populated/);
  assert.match(routeSource, /const globalStats = await readPlayerGlobalStats\(playerId\)/);
});
