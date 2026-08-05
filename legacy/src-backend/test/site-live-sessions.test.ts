import assert from 'node:assert/strict';
import test from 'node:test';
import {
  ACTIVE_USER_WINDOW_SECONDS,
  LIVE_SESSION_HEARTBEAT_SECONDS,
  anonymousVisitorIdentity,
  isValidAnonymousVisitorId,
} from '../services/site-live-session-policy';

test('live visitor identifiers retain the existing privacy-safe input contract', () => {
  assert.equal(isValidAnonymousVisitorId('123e4567-e89b-12d3-a456-426614174000'), true);
  assert.equal(isValidAnonymousVisitorId('short'), false);
  assert.equal(isValidAnonymousVisitorId('visitor-id-with-spaces is invalid'), false);
});

test('live session identity is stable within a UTC day and rotates across days', () => {
  const visitorId = '123e4567-e89b-12d3-a456-426614174000';
  const today = anonymousVisitorIdentity(visitorId, '2026-07-17', 'test-salt');
  const repeat = anonymousVisitorIdentity(visitorId, '2026-07-17', 'test-salt');
  const tomorrow = anonymousVisitorIdentity(visitorId, '2026-07-18', 'test-salt');
  assert.equal(today.visitorHash, repeat.visitorHash);
  assert.notEqual(today.visitorHash, tomorrow.visitorHash);
  assert.equal(today.visitDate, '2026-07-17');
});

test('active window tolerates multiple missed heartbeats without treating expired sessions as live', () => {
  assert.equal(LIVE_SESSION_HEARTBEAT_SECONDS, 60);
  assert.equal(ACTIVE_USER_WINDOW_SECONDS, 300);
  assert.ok(ACTIVE_USER_WINDOW_SECONDS >= LIVE_SESSION_HEARTBEAT_SECONDS * 3);
});
