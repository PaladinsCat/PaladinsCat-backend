import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';
import {
  PRIVATE_IDENTITY_LINK_THRESHOLD,
  scorePrivateIdentity,
  type PrivateAccountObservation,
} from '../utils/private-account-identity';

function observation(overrides: Partial<PrivateAccountObservation> = {}): PrivateAccountObservation {
  return {
    matchId: 100,
    privateSlot: 1,
    entryDatetime: '2026-07-14T12:00:00.000Z',
    partyId: 12345,
    accountLevel: 500,
    masteryLevel: 42,
    leagueTier: 20,
    leaguePoints: 65,
    championId: 2205,
    taskForce: 1,
    winStatus: 'Winner',
    portalId: 0,
    portalUserId: '',
    platform: 'pc',
    source: 'direct',
    partyMemberIds: [],
    ...overrides,
  };
}

test('PartyId alone is contextual evidence and cannot identify a person', () => {
  const result = scorePrivateIdentity(
    observation({ accountLevel: 0, masteryLevel: 0, leagueTier: 0, leaguePoints: 0, championId: 0, platform: '' }),
    observation({ matchId: 99, entryDatetime: '2026-07-14T08:00:00.000Z', accountLevel: 0, masteryLevel: 0, leagueTier: 0, leaguePoints: 0, championId: 0, platform: '' }),
  );
  assert.equal(result.hardConflict, false);
  assert.ok(result.score < PRIVATE_IDENTITY_LINK_THRESHOLD);
  assert.ok(result.reasons.includes('party_session_exact'));
});

test('PartyId plus level, same-champion mastery, rank, and platform can form solid session evidence', () => {
  const result = scorePrivateIdentity(
    observation(),
    observation({ matchId: 99, entryDatetime: '2026-07-14T08:00:00.000Z' }),
  );
  assert.equal(result.hardConflict, false);
  assert.ok(result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
  assert.ok(result.reasons.includes('account_level_exact'));
  assert.ok(result.reasons.includes('mastery_exact'));
  assert.ok(result.reasons.includes('league_tier_exact'));
});

test('two private slots in the same match can never resolve to one identity', () => {
  const result = scorePrivateIdentity(
    observation({ privateSlot: 1 }),
    observation({ privateSlot: 2 }),
  );
  assert.equal(result.hardConflict, true);
  assert.equal(result.score, 0);
  assert.deepEqual(result.reasons, ['same_match_conflict']);
});

test('material account-level regression vetoes an otherwise similar candidate', () => {
  const result = scorePrivateIdentity(
    observation({ accountLevel: 360, entryDatetime: '2026-07-14T12:00:00.000Z' }),
    observation({ matchId: 99, accountLevel: 361, entryDatetime: '2026-07-13T12:00:00.000Z' }),
  );
  assert.equal(result.hardConflict, false, 'one-level API jitter is tolerated');

  const material = scorePrivateIdentity(
    observation({ accountLevel: 359, entryDatetime: '2026-07-14T12:00:00.000Z' }),
    observation({ matchId: 99, accountLevel: 361, entryDatetime: '2026-07-13T12:00:00.000Z' }),
  );
  assert.equal(material.hardConflict, true);
  assert.ok(material.reasons.includes('account_level_regression'));
});

test('known public party companion overlap is stronger than a random PartyId', () => {
  const result = scorePrivateIdentity(
    observation({ partyId: 70001, partyMemberIds: [8717218] }),
    observation({ matchId: 99, partyId: 90002, partyMemberIds: [8717218], entryDatetime: '2026-07-13T12:00:00.000Z' }),
  );
  assert.equal(result.hardConflict, false);
  assert.ok(result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
  assert.ok(result.reasons.includes('party_companion_overlap:1'));
});

test('PartyId zero and capped level do not force a cross-match merge', () => {
  const result = scorePrivateIdentity(
    observation({ partyId: 0, accountLevel: 999 }),
    observation({ matchId: 99, partyId: 0, accountLevel: 999, entryDatetime: '2026-07-13T12:00:00.000Z' }),
  );
  assert.equal(result.hardConflict, false);
  assert.ok(result.score < PRIVATE_IDENTITY_LINK_THRESHOLD);
  assert.ok(result.reasons.includes('account_level_cap_exact'));
});

test('different non-empty portal user ids are a hard conflict', () => {
  const result = scorePrivateIdentity(
    observation({ portalId: 1, portalUserId: 'private-a' }),
    observation({ matchId: 99, portalId: 1, portalUserId: 'private-b' }),
  );
  assert.equal(result.hardConflict, true);
  assert.ok(result.reasons.includes('portal_user_conflict'));
});

test('TP progression follows the previous observed result without becoming a fixed identity field', () => {
  const priorWin = observation({
    matchId: 99,
    entryDatetime: '2026-07-14T11:30:00.000Z',
    leaguePoints: 33,
    championId: 2493,
    winStatus: 'Winner',
  });
  const nextMatch = observation({
    leaguePoints: 48,
    championId: 2205,
    winStatus: 'Winner',
  });
  const result = scorePrivateIdentity(nextMatch, priorWin);

  assert.equal(result.hardConflict, false);
  assert.ok(result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
  assert.ok(result.reasons.includes('tp_win_progression'));
  assert.ok(result.reasons.includes('ranked_session_progression'));
});

test('same-session stable TP remains compatible after a loss', () => {
  const priorLoss = observation({
    matchId: 99,
    entryDatetime: '2026-07-14T11:30:00.000Z',
    leaguePoints: 33,
    championId: 2493,
    winStatus: 'Loser',
  });
  const nextMatch = observation({ leaguePoints: 33, championId: 2472 });
  const result = scorePrivateIdentity(nextMatch, priorLoss);

  assert.equal(result.hardConflict, false);
  assert.ok(result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
  assert.ok(result.reasons.includes('tp_stable'));
});

test('an unexpected TP direction is uncertain evidence, not a hard conflict', () => {
  const priorLoss = observation({
    matchId: 99,
    entryDatetime: '2026-07-14T11:30:00.000Z',
    leaguePoints: 50,
    winStatus: 'Loser',
  });
  const later = observation({ leaguePoints: 80 });
  const result = scorePrivateIdentity(later, priorLoss);

  assert.equal(result.hardConflict, false);
  assert.ok(result.reasons.includes('tp_progression_uncertain'));
});

test('casual account and mastery trajectory can link without ranked tier anchors', () => {
  const first = observation({
    matchId: 9001,
    entryDatetime: '2026-07-24T10:00:00Z',
    accountLevel: 220,
    masteryLevel: 31,
    partyId: 0,
    leagueTier: 0,
    leaguePoints: 0,
    championId: 2071,
    platform: 'steam',
    statsScope: 'casual',
  });
  const second = observation({
    matchId: 9002,
    entryDatetime: '2026-07-24T11:00:00Z',
    accountLevel: 220,
    masteryLevel: 31,
    partyId: 0,
    leagueTier: 0,
    leaguePoints: 0,
    championId: 2071,
    platform: 'steam',
    statsScope: 'casual',
  });
  const result = scorePrivateIdentity(second, first);
  assert.equal(result.hardConflict, false);
  assert.ok(result.score >= PRIVATE_IDENTITY_LINK_THRESHOLD);
  assert.ok(result.reasons.includes('casual_progression_bundle'));
});

test('level-capped casual accounts do not use the casual progression bundle', () => {
  const first = observation({
    matchId: 9101,
    accountLevel: 999,
    masteryLevel: 31,
    partyId: 0,
    leagueTier: 0,
    leaguePoints: 0,
    championId: 2071,
    platform: 'steam',
    statsScope: 'casual',
  });
  const second = observation({
    matchId: 9102,
    accountLevel: 999,
    masteryLevel: 31,
    partyId: 0,
    leagueTier: 0,
    leaguePoints: 0,
    championId: 2071,
    platform: 'steam',
    statsScope: 'casual',
  });
  const result = scorePrivateIdentity(second, first);
  assert.equal(result.reasons.includes('casual_progression_bundle'), false);
});

test('database constraint permits unresolved non-ranked identity ambiguity', () => {
  const migration = readFileSync(
    join(__dirname, '../db/migrations/101_private_observation_ambiguity.sql'),
    'utf8',
  );
  assert.match(migration, /resolution_status IN \(\s*'unresolved', 'ambiguous'/);
  assert.match(migration, /private_player_id IS NULL[\s\S]*'unresolved', 'ambiguous'/);
});
