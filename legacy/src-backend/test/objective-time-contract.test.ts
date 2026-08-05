import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';

test('non-ranked ingestion accepts the normalized objective-assists key', () => {
  const source = readFileSync(
    resolve(__dirname, '../workers/nonranked-match-acquisition.ts'),
    'utf8',
  );

  assert.match(
    source,
    /firstValue\(\s*player,\s*'objective_time',\s*'objective_assists',\s*'Objective_Assists',\s*'Objective_Time'/,
  );
});

test('objective-time migration keeps rollback evidence and repairs both scopes locally', () => {
  const migration = readFileSync(
    resolve(__dirname, '../db/migrations/107_repair_nonranked_objective_time.sql'),
    'utf8',
  );

  assert.match(migration, /CREATE TABLE IF NOT EXISTS nonranked_objective_time_backfill_audit/);
  assert.match(migration, /FROM casual_match_players/);
  assert.match(migration, /FROM special_match_players/);
  assert.match(migration, /UPDATE casual_match_players fact/);
  assert.match(migration, /UPDATE special_match_players fact/);
  assert.match(migration, /raw_player->>'objective_assists'/);
  assert.doesNotMatch(migration, /getmatchdetails|getmatchhistory|api_log/i);
});

test('match-detail cache version advances past objective-time-zero responses', () => {
  const source = readFileSync(resolve(__dirname, '../routes/matches.ts'), 'utf8');
  assert.match(source, /const MATCH_DETAIL_CACHE_VERSION = 13;/);
});
