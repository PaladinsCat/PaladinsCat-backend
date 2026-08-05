import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import test from 'node:test';

import {
  relayOperationManifest,
  validateRelayOperationFromManifest,
} from '../contracts/hirez-relay-operation-contract';
import {
  RelayValidationError,
  validateRelayOperation,
} from '../hirez-relay/dispatcher';

function manifestFailure(message: string): never {
  throw new RelayValidationError(message);
}

test('shared relay manifest has one unique 38-operation inventory', () => {
  assert.equal(relayOperationManifest.schemaVersion, 1);
  assert.equal(relayOperationManifest.operations.length, 38);
  const names = relayOperationManifest.operations.map(operation => operation.name);
  assert.equal(new Set(names).size, names.length);
  assert.equal(relayOperationManifest.transport.validationStatus, 400);
  assert.equal(relayOperationManifest.transport.validationErrorCode, 'VALIDATION_ERROR');
  assert.equal(relayOperationManifest.transport.runtimeStatus, 502);
  assert.equal(relayOperationManifest.transport.runtimeErrorCode, 'RELAY_OPERATION_FAILED');
});

test('every manifest valid fixture passes every declared mode', () => {
  for (const operation of relayOperationManifest.operations) {
    for (const mode of operation.modes) {
      assert.doesNotThrow(
        () => validateRelayOperation(operation.name, operation.validArgs, mode),
        `${operation.name} should validate in ${mode}`,
      );
    }
  }
});

test('dummy-only manifest operations cannot enter real dispatch', () => {
  for (const operation of relayOperationManifest.operations.filter(
    operation => !operation.modes.includes('real'),
  )) {
    assert.throws(
      () => validateRelayOperation(operation.name, operation.validArgs, 'real'),
      (error: unknown) => (
        error instanceof RelayValidationError
        && error.statusCode === 400
        && error.code === 'VALIDATION_ERROR'
        && error.message === `Unsupported HirezRelay operation: ${operation.name}`
      ),
    );
  }
});

test('manifest drives exact TypeScript boundary errors', () => {
  const cases: Array<{
    operation: string;
    args: unknown[];
    error: string;
  }> = [
    {
      operation: 'getMatchDetailsBatch',
      args: [[]],
      error: 'requests must contain between 1 and 10 matches',
    },
    {
      operation: 'getMatchDetailsBatch',
      args: [[{ matchId: 12 }, { matchId: '12' }]],
      error: 'requests contains duplicate matchId 12',
    },
    {
      operation: 'getMatchHistory',
      args: [12, 50, 'false'],
      error: 'forceRefresh must be a boolean',
    },
    {
      operation: 'dumpRawPayloads',
      args: [[{ endpoint: '', entity_type: 'match', raw_data: [] }]],
      error: 'payloads[0].endpoint must be a non-empty string',
    },
    {
      operation: 'resetDummyApiCallCounts',
      args: [true],
      error: 'resetDummyApiCallCounts takes no args',
    },
  ];

  for (const fixture of cases) {
    assert.throws(
      () => validateRelayOperationFromManifest(
        fixture.operation,
        fixture.args,
        'dummy',
        manifestFailure,
      ),
      (error: unknown) => (
        error instanceof RelayValidationError
        && error.message === fixture.error
      ),
      fixture.operation,
    );
  }
});

test('real TypeScript handler registry covers every manifest real operation', () => {
  const dispatcher = readFileSync(
    join(__dirname, '../hirez-relay/dispatcher.ts'),
    'utf8',
  );
  const handlerBlock = dispatcher
    .split('const handlers: Record<string, (...innerArgs: any[]) => unknown> = {')[1]
    ?.split('\n  };')[0];
  assert.ok(handlerBlock, 'real handler registry was not found');
  const handlerNames = Array.from(
    handlerBlock.matchAll(/^\s{4}([A-Za-z0-9_]+):/gm),
    match => match[1],
  ).sort();
  const manifestRealNames = relayOperationManifest.operations
    .filter(operation => operation.modes.includes('real'))
    .map(operation => operation.name)
    .sort();
  assert.deepEqual(handlerNames, manifestRealNames);
});

test('dummy TypeScript implementation covers every manifest dummy operation', () => {
  const dummy = readFileSync(
    join(__dirname, '../hirez-relay/dummy-data.ts'),
    'utf8',
  );
  const caseNames = new Set(
    Array.from(dummy.matchAll(/case '([A-Za-z0-9_]+)'/g), match => match[1]),
  );
  const missing = relayOperationManifest.operations
    .filter(operation => operation.modes.includes('dummy'))
    .map(operation => operation.name)
    .filter(operation => !caseNames.has(operation));
  assert.deepEqual(missing, []);
});
