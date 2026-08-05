import crypto from 'crypto';
import fs from 'fs';
import path from 'path';
import type { PoolClient } from 'pg';
import { pool } from '../config/db';

const MIGRATION_PATTERN = /^(\d{3,})_([a-z0-9_]+)\.sql$/;
const MIGRATION_LOCK = 'paladinscat-schema-migrations';
const MIGRATION_QUERY_TIMEOUT_MS = 10 * 60 * 1000;

function findMigrationDirectory(): string {
  const candidates = [
    path.resolve(process.cwd(), 'migrations', 'tracked'),
    path.resolve(__dirname, '..', 'db', 'migrations'),
    path.resolve(__dirname, '..', '..', 'db', 'migrations'),
    path.resolve(process.cwd(), 'dist', 'db', 'migrations'),
    path.resolve(process.cwd(), 'db', 'migrations'),
  ];
  const directory = candidates.find((candidate) => fs.existsSync(candidate));
  if (!directory) throw new Error('Migration directory not found in the backend image.');
  return directory;
}

function sha256(contents: string): string {
  return crypto.createHash('sha256').update(contents, 'utf8').digest('hex');
}

async function main(): Promise<void> {
  const directory = findMigrationDirectory();
  const files = fs.readdirSync(directory)
    .filter((fileName) => MIGRATION_PATTERN.test(fileName))
    .sort((left, right) => left.localeCompare(right));

  const versions = new Set<string>();
  for (const fileName of files) {
    const version = fileName.match(MIGRATION_PATTERN)?.[1] ?? '';
    if (versions.has(version)) throw new Error(`Duplicate migration version ${version}.`);
    versions.add(version);
  }

  const client = await pool.connect();
  let locked = false;
  try {
    await client.query('SELECT pg_advisory_lock(hashtext($1))', [MIGRATION_LOCK]);
    locked = true;
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version TEXT PRIMARY KEY,
        file_name TEXT NOT NULL UNIQUE,
        checksum_sha256 TEXT NOT NULL,
        git_commit TEXT,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        execution_ms INTEGER NOT NULL
      )
    `);

    const appliedResult = await client.query<{
      version: string;
      file_name: string;
      checksum_sha256: string;
    }>('SELECT version, file_name, checksum_sha256 FROM schema_migrations ORDER BY version');
    const applied = new Map(appliedResult.rows.map((row) => [row.version, row]));
    let appliedCount = 0;

    for (const fileName of files) {
      const match = fileName.match(MIGRATION_PATTERN);
      if (!match) continue;
      const version = match[1];
      const contents = fs.readFileSync(path.join(directory, fileName), 'utf8');
      const checksum = sha256(contents);
      const existing = applied.get(version);
      if (existing) {
        if (existing.file_name !== fileName || existing.checksum_sha256 !== checksum) {
          throw new Error(`Applied migration ${version} no longer matches ${fileName}; migrations are immutable.`);
        }
        console.log(`[migrations] ${fileName} already applied`);
        continue;
      }

      const transactionOff = contents.split(/\r?\n/, 8)
        .some((line) => line.trim().toLowerCase() === '-- paladinscat:transaction=off');
      const requiresFullBackup = contents.split(/\r?\n/, 8)
        .some((line) => line.trim().toLowerCase() === '-- paladinscat:requires-full-backup');
      if (requiresFullBackup && process.env.PALADINSCAT_DESTRUCTIVE_MIGRATIONS_CONFIRMED !== 'yes') {
        throw new Error(`${fileName} requires a verified full backup; deploy again with explicit destructive-migration confirmation.`);
      }
      const startedAt = Date.now();
      console.log(`[migrations] applying ${fileName}${transactionOff ? ' (non-transactional)' : ''}`);

      if (!transactionOff) await client.query('BEGIN');
      try {
        await client.query(transactionOff ? "SET lock_timeout = '5s'" : "SET LOCAL lock_timeout = '5s'");
        await client.query(transactionOff ? "SET statement_timeout = '10min'" : "SET LOCAL statement_timeout = '10min'");
        // The shared application pool has a 30-second client read timeout.
        // node-postgres supports a per-query override at runtime, but the
        // installed TypeScript definitions omit it from QueryConfig. Override
        // this checked-out client's connection setting temporarily instead.
        const migrationClient = client as PoolClient & {
          connectionParameters: { query_timeout?: number | false };
        };
        const originalQueryTimeout = migrationClient.connectionParameters.query_timeout;
        migrationClient.connectionParameters.query_timeout = MIGRATION_QUERY_TIMEOUT_MS;
        try {
          await client.query(contents);
        } finally {
          migrationClient.connectionParameters.query_timeout = originalQueryTimeout;
        }
        const executionMs = Date.now() - startedAt;
        await client.query(
          `INSERT INTO schema_migrations
             (version, file_name, checksum_sha256, git_commit, execution_ms)
           VALUES ($1, $2, $3, $4, $5)`,
          [version, fileName, checksum, process.env.PALADINSCAT_GIT_COMMIT || null, executionMs],
        );
        if (!transactionOff) await client.query('COMMIT');
        console.log(`[migrations] applied ${fileName} in ${executionMs}ms`);
        appliedCount += 1;
      } catch (error) {
        if (!transactionOff) {
          try { await client.query('ROLLBACK'); } catch { /* preserve the migration error */ }
        }
        throw error;
      } finally {
        if (transactionOff) {
          try { await client.query('RESET lock_timeout'); } catch { /* preserve the migration result */ }
          try { await client.query('RESET statement_timeout'); } catch { /* preserve the migration result */ }
        }
      }
    }

    console.log(`[migrations] complete; ${appliedCount} migration(s) applied`);
  } finally {
    if (locked) {
      try { await client.query('SELECT pg_advisory_unlock(hashtext($1))', [MIGRATION_LOCK]); } catch { /* connection cleanup releases it */ }
    }
    client.release();
    await pool.end();
  }
}

main().catch(async (error: unknown) => {
  console.error('[migrations] failed:', error instanceof Error ? error.message : String(error));
  try { await pool.end(); } catch { /* process is already failing */ }
  process.exit(1);
});
