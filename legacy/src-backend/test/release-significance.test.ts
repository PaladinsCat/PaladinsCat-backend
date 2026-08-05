import assert from 'node:assert/strict';
import test from 'node:test';
import { classifyRelease, countChangelogChanges, releaseSignificance } from '../utils/release-significance';

test('release significance follows the deployment change thresholds', () => {
  assert.equal(classifyRelease(0), 'patch');
  assert.equal(classifyRelease(4), 'patch');
  assert.equal(classifyRelease(5), 'minor');
  assert.equal(classifyRelease(9), 'minor');
  assert.equal(classifyRelease(10), 'major');
});

test('automated changelog rows count one Git commit per change', () => {
  const changelog = ['abc1234 fix: one', 'def5678 feat: two', '', '987abcd refactor: three'].join('\n');
  assert.equal(countChangelogChanges(changelog), 3);
});

test('stored deployment count wins while release type is derived consistently', () => {
  assert.deepEqual(releaseSignificance({ changeCount: 7, releaseType: 'major' }, 'abc1234 fix: one'), {
    changeCount: 7,
    releaseType: 'minor',
  });
});
