import assert from 'node:assert/strict';
import test from 'node:test';
import { compactNonrankedRawPlayer } from '../services/nonranked-raw-json';

test('non-ranked raw player storage retains only public equipment fallback fields', () => {
  const compact = compactNonrankedRawPlayer({
    player_id: 7,
    player_name: 'discard me',
    active_id_1: 101,
    item_active_1: 'Chronos',
    active_level_1: 8,
    item_id_1: 201,
    item_purch_1: 'Card One',
    item_level_1: 5,
    item_id_6: 301,
    item_purch_6: 'Talent One',
    unrelated_payload: { large: true },
  });

  assert.deepEqual(compact, {
    _storage: 'compact-equipment-v1',
    active_id_1: 101,
    item_active_1: 'Chronos',
    active_level_1: 8,
    item_id_1: 201,
    item_purch_1: 'Card One',
    item_level_1: 5,
    item_id_6: 301,
    item_purch_6: 'Talent One',
  });
});

test('non-ranked raw player compaction is idempotent', () => {
  const once = compactNonrankedRawPlayer({ active_id_2: 102 });
  assert.deepEqual(compactNonrankedRawPlayer(once), once);
});
