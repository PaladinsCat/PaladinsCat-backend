import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';
import {
  getMatchHistoryRequestParams,
  getPlayerLoadoutRequestParams,
  HIREZ_LANGUAGE_ID,
} from '../contracts/hirez-request-params';
import { normalizePlayerLoadoutDeck } from '../services/player-loadout-normalizer';
import { PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES } from '../services/player-history-policy';

test('getplayerloadouts includes the required language ID', () => {
  assert.equal(HIREZ_LANGUAGE_ID, 1);
  assert.deepEqual(getPlayerLoadoutRequestParams(9573956), ['9573956', '1']);
});

test('getmatchhistory omits the unsupported limit path segment', () => {
  assert.deepEqual(getMatchHistoryRequestParams(721203321), ['721203321']);
});

test('player reads are database-only and explicit refresh owns the long cache bypass', () => {
  const routeSource = readFileSync(resolve(__dirname, '../routes/players.ts'), 'utf8');
  const relaySource = readFileSync(resolve(__dirname, '../hirez-relay/core.ts'), 'utf8');

  assert.equal(PUBLIC_PLAYER_HISTORY_CACHE_TTL_MINUTES, 1440);
  assert.doesNotMatch(routeSource, /developer_api_history_profile_read_through|Fresh-profile history prime failed/);
  assert.match(routeSource, /fastify\.post\('\/:id\/refresh'[\s\S]*?await getMatchHistory\(id, 50, true, 'manual_profile_refresh'\)/);
  assert.match(routeSource, /refreshPlayerProfileIfExpired\([\s\S]*?'manual_profile_refresh'[\s\S]*?true,/);
  assert.match(routeSource, /fetched_at >= now\(\) - \(\$2::int \* interval '1 minute'\)/);
  assert.match(relaySource, /fetched_at >= now\(\) - \(\$2::int \* interval '1 minute'\)/);
});

test('unnamed player loadouts retain their deck ID and cards', () => {
  assert.deepEqual(normalizePlayerLoadoutDeck({
    DeckId: 991857303,
    DeckName: '',
    ChampionId: 2493,
    LoadoutItems: [
      { ItemId: 1001, Points: 5 },
      { ItemId: 1002, Points: 4 },
      { ItemId: 1003, Points: 3 },
      { ItemId: 1004, Points: 2 },
      { ItemId: 1005, Points: 1 },
    ],
  }), {
    deckId: 991857303,
    deckKey: 'id:991857303',
    championId: 2493,
    deckName: 'Unnamed Loadout',
    cardIds: [1001, 1002, 1003, 1004, 1005],
    cardLevels: [5, 4, 3, 2, 1],
  });
});
