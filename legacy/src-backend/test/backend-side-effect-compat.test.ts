import assert from 'node:assert/strict';
import test from 'node:test';
import {
  compareDatabaseSnapshots,
  validateManifest,
  type DatabaseSnapshot,
} from '../scripts/backend-side-effect-compat';

test('side-effect comparator accepts identical canonical row snapshots', () => {
  const snapshot: DatabaseSnapshot = {
    tables: [{
      name: 'matches',
      rowCount: 1,
      sha256: 'same',
      rows: ['{"match_id":1280000001}'],
    }],
  };
  assert.deepEqual(compareDatabaseSnapshots(snapshot, structuredClone(snapshot)), []);
});
test('side-effect comparator reports exact first differing rows', () => {
  const typescript: DatabaseSnapshot = {
    tables: [{
      name: 'matches',
      rowCount: 1,
      sha256: 'left',
      rows: ['{"match_id":1280000001,"status":"complete"}'],
    }],
  };
  const rust: DatabaseSnapshot = {
    tables: [{
      name: 'matches',
      rowCount: 1,
      sha256: 'right',
      rows: ['{"match_id":1280000001,"status":"limited"}'],
    }],
  };
  const differences = compareDatabaseSnapshots(typescript, rust);
  assert.equal(differences.length, 1);
  assert.equal(differences[0].table, 'matches');
  assert.deepEqual((differences[0].firstRows as any[])[0], {
    index: 0,
    typescript: '{"match_id":1280000001,"status":"complete"}',
    rust: '{"match_id":1280000001,"status":"limited"}',
  });
});

test('side-effect manifest rejects unsafe or nondeterministic table definitions', () => {
  assert.throws(
    () => validateManifest({
      schemaVersion: 1,
      tables: [{ name: 'matches; DROP TABLE players', orderBy: ['match_id'] }],
    }),
    /Invalid table name/,
  );
  assert.throws(
    () => validateManifest({
      schemaVersion: 1,
      tables: [{ name: 'matches', orderBy: [] }],
    }),
    /deterministic orderBy/,
  );
});
