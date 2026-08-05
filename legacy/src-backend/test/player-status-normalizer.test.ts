import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizePlayerStatus } from '../services/normalizer';

test('normalizePlayerStatus strips PostgreSQL-incompatible NUL characters', () => {
  const status = normalizePlayerStatus({
    player_id: 16706730,
    status: 3,
    status_string: 'In\u0000 Game\u0000',
    Match: 990000001,
    match_queue_id: 486,
    privacy_flag: 'n',
    personal_status_message: '\u0000Ready\u0000 to play',
  });

  assert.equal(status.status_string, 'In Game');
  assert.equal(status.personal_status_message, 'Ready to play');
  assert.equal(status.status_string.includes('\u0000'), false);
  assert.equal(status.personal_status_message?.includes('\u0000'), false);
});

test('normalizePlayerStatus preserves empty personal status as null', () => {
  const status = normalizePlayerStatus({
    status_string: '\u0000',
    personal_status_message: '\u0000',
  });

  assert.equal(status.status_string, '');
  assert.equal(status.personal_status_message, null);
});
