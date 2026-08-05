import assert from 'node:assert/strict';
import test from 'node:test';
import {
  normalizeChampionCardStatsPayload,
  normalizeChampionTalentStatsPayload,
} from '../services/champion-page-contract';

test('normalizes PostgreSQL aggregate strings in champion talent bundles', () => {
  const result = normalizeChampionTalentStatsPayload({
    totalMatches: '5351',
    talentCoveredMatches: '5326',
    disconnectedPlayers: '25',
    disconnectedWins: '10',
    disconnectedLosses: '15',
    disconnectedWinRate: '40.00',
    talentCoverageRate: '99.53',
    talents: [{
      talentId: '20085', talentName: 'Defiant Fist', totalPlays: '1000',
      wins: '507', losses: '493', winRate: '50.70',
    }],
  });

  assert.deepEqual(result, {
    totalMatches: 5351,
    talentCoveredMatches: 5326,
    disconnectedPlayers: 25,
    disconnectedWins: 10,
    disconnectedLosses: 15,
    disconnectedWinRate: 40,
    talentCoverageRate: 99.53,
    talents: [{
      talentId: 20085, talentName: 'Defiant Fist', totalPlays: 1000,
      wins: 507, losses: 493, winRate: 50.7,
    }],
  });
});

test('normalizes nested loadout card and level aggregates', () => {
  const result = normalizeChampionCardStatsPayload({
    totalMatches: '1000',
    talentId: '20085',
    cards: [{
      cardId: '13291', cardName: 'Marksman', totalPlays: '793',
      wins: '403', losses: '390', winRate: '50.82',
      levels: [{ level: '2', plays: '629', wins: '339', losses: '290', winRate: '53.90' }],
    }],
  });

  assert.deepEqual(result, {
    totalMatches: 1000,
    talentId: 20085,
    cards: [{
      cardId: 13291, cardName: 'Marksman', totalPlays: 793,
      wins: 403, losses: 390, winRate: 50.82,
      levels: [{ level: 2, plays: 629, wins: 339, losses: 290, winRate: 53.9 }],
    }],
  });
});
