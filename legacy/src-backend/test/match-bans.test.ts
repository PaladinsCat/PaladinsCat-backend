import assert from 'node:assert/strict';
import test from 'node:test';
import { extractMatchBanFields, isStagedRecoveryRoster, matchBanEntries } from '../utils/match-bans';

test('normalizes getdemodetails BanId fields by draft slot', () => {
  const fields = extractMatchBanFields({
    BanId1: 2092,
    BanId2: 2479,
    BanId3: 2094,
    BanId4: 2477,
  });

  assert.deepEqual(matchBanEntries(fields), [
    { banSlot: 1, championId: 2092 },
    { banSlot: 2, championId: 2479 },
    { banSlot: 3, championId: 2094 },
    { banSlot: 4, championId: 2477 },
  ]);
});

test('scans the whole roster and ignores zero-filled recovery placeholders', () => {
  const fields = extractMatchBanFields(
    { ban_id_1: 0, ban_id_2: 0 },
    [
      { ban_id_1: 0, ban_id_2: 2479 },
      { ban_id_1: 0, BanId1: 2092, Ban_1: 0, Ban_2: 0 },
    ],
  );

  assert.equal(fields.ban_id_1, 2092);
  assert.equal(fields.ban_id_2, 2479);
});

test('recognizes only complete synthetic recovery rosters', () => {
  const recovered = Array.from({ length: 10 }, () => ({ source: 'recovered' }));
  const direct = Array.from({ length: 10 }, () => ({ source: 'direct' }));

  assert.equal(isStagedRecoveryRoster(recovered, false), true);
  assert.equal(isStagedRecoveryRoster(recovered.slice(0, 9), false), false);
  assert.equal(isStagedRecoveryRoster(recovered, true), false);
  assert.equal(isStagedRecoveryRoster(direct, false), false);
});
