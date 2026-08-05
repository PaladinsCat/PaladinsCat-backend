import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import test from 'node:test';
import { extractMatchMetadata, normalizeMatchHistoryPlayer } from '../services/normalizer';

test('match duration and participant duration remain distinct upstream facts', () => {
  const match = extractMatchMetadata([{
    Match: 1280863382,
    Entry_Datetime: '7/18/2026 6:31:11 PM',
    Match_Duration: 765,
    Match_Queue_Id: 486,
  }]);
  const player = normalizeMatchHistoryPlayer({
    Match: 1280863382,
    playerId: 11296626,
    Match_Time: '7/18/2026 6:31:11 PM',
    Time_In_Match_Seconds: 785,
    Gold: 2361,
  });

  assert.equal(match.duration_seconds, 765);
  assert.equal(player.time_in_match, 785);
});

test('the distinct player timer remains evidence rather than a metric denominator', () => {
  const recoveryCore = readFileSync(resolve(__dirname, '../hirez-relay/core.ts'), 'utf8');
  const bufferProcessor = readFileSync(resolve(__dirname, '../workers/buffer-processor.ts'), 'utf8');
  assert.doesNotMatch(recoveryCore, /normalized\.time_in_match\s*=\s*matchDuration/);
  assert.match(bufferProcessor, /resolveGameplayDuration\(matchDurationSeconds \|\| player\.match_duration\)/);
});

test('broken-match recovery does not replace participant time with demo duration', () => {
  const recoveryCore = readFileSync(resolve(__dirname, '../hirez-relay/core.ts'), 'utf8');
  assert.doesNotMatch(recoveryCore, /normalized\.time_in_match\s*=\s*matchDuration/);
});

test('performance read models never divide by the API player timer', () => {
  for (const relativePath of [
    '../routes/stats.ts',
    '../services/performance-projections.ts',
    '../services/scalable-stats-projections.ts',
    '../workers/baseline-tracker.ts',
  ]) {
    const source = readFileSync(resolve(__dirname, relativePath), 'utf8');
    assert.doesNotMatch(source, /time_in_match\s*\/\s*60/);
  }
});
