import assert from 'node:assert/strict';
import test from 'node:test';
import { getRelayMode } from '../hirez-relay/dispatcher';

test('production relay requires explicit real mode', () => {
  assert.equal(getRelayMode({ NODE_ENV: 'production', HIREZ_RELAY_MODE: 'real' }), 'real');

  assert.throws(
    () => getRelayMode({ NODE_ENV: 'production' }),
    /HIREZ_RELAY_MODE must be set to "real" in production/,
  );
  assert.throws(
    () => getRelayMode({ NODE_ENV: 'production', HIREZ_RELAY_MODE: 'dummy' }),
    /Refusing to start with dummy data/,
  );
});
