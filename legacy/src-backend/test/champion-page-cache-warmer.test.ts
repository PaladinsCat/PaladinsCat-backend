import assert from 'node:assert/strict';
import test from 'node:test';
import { championPageWarmUrls } from '../workers/champion-page-cache-urls';

test('warms one champion bundle and every talent bundle without duplicates', () => {
  const urls = championPageWarmUrls([
    { name: 'Androxus', talent_id: 16368 },
    { name: 'Androxus', talent_id: 19292 },
    { name: 'Androxus', talent_id: 20085 },
    { name: 'Androxus', talent_id: 20085 },
    { name: "Mal'Damba", talent_id: 13189 },
    { name: "Mal'Damba", talent_id: null },
  ]);

  assert.deepEqual(urls, [
    '/champions/androxus/page-data',
    '/champions/androxus/talents/16368/page-data',
    '/champions/androxus/talents/19292/page-data',
    '/champions/androxus/talents/20085/page-data',
    '/champions/maldamba/page-data',
    '/champions/maldamba/talents/13189/page-data',
  ]);
});
