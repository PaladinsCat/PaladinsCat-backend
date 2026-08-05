import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const source = fs.readFileSync(
  path.resolve(process.cwd(), 'routes/matches.ts'),
  'utf8',
);

test('nonranked match reads expose compact party numbers only for real groups', () => {
  assert.match(source, /party_groups AS \([\s\S]*HAVING COUNT\(\*\) > 1/);
  assert.match(source, /ROW_NUMBER\(\) OVER \(ORDER BY party_id\) AS party_num/);
  assert.match(source, /COALESCE\(party_numbered\.party_num, 0\) AS party/);
});

test('match facts cover every persisted match class from durable player rows', () => {
  assert.match(source, /FROM casual_matches[\s\S]*FROM special_matches[\s\S]*LIMIT 1/);
  assert.match(source, /cmp\.raw_player[\s\S]*FROM casual_match_players cmp/);
  assert.match(source, /smp\.raw_player[\s\S]*FROM special_match_players smp/);
  assert.match(source, /raw\.item_id_6/);
  assert.match(source, /raw\.item_purch_6/);
  assert.match(source, /talentIconUrl\(player\.champion_name, talentName\)/);
});
