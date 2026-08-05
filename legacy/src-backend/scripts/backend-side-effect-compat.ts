import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { Pool } from 'pg';

export type SideEffectTable = {
  name: string;
  orderBy: string[];
  omitColumns?: string[];
};

export type SideEffectManifest = {
  schemaVersion: 1;
  tables: SideEffectTable[];
};

export type TableSnapshot = {
  name: string;
  rowCount: number;
  sha256: string;
  rows: string[];
};

export type DatabaseSnapshot = {
  tables: TableSnapshot[];
};

const IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

export async function snapshotDatabase(
  connectionString: string,
  manifest: SideEffectManifest,
): Promise<DatabaseSnapshot> {
  validateManifest(manifest);
  const pool = new Pool({
    connectionString,
    application_name: 'paladinscat-rust-side-effect-compat',
    max: 2,
    statement_timeout: 30_000,
  });
  try {
    const tables: TableSnapshot[] = [];
    for (const table of manifest.tables) {
      const orderBy = table.orderBy
        .map((column) => `t.${quoteIdentifier(column)}`)
        .join(', ');
      const result = await pool.query<{ canonical_row: string }>(
        `SELECT encode(
           convert_to((to_jsonb(t) - $1::text[])::text, 'UTF8'),
           'base64'
         ) AS canonical_row
         FROM public.${quoteIdentifier(table.name)} AS t
         ORDER BY ${orderBy}`,
        [table.omitColumns ?? []],
      );
      const rows = result.rows.map(({ canonical_row }) =>
        Buffer.from(canonical_row, 'base64').toString('utf8')
      );
      tables.push({
        name: table.name,
        rowCount: rows.length,
        sha256: sha256(rows.join('\n')),
        rows,
      });
    }
    return { tables };
  } finally {
    await pool.end();
  }
}

export function compareDatabaseSnapshots(
  typescript: DatabaseSnapshot,
  rust: DatabaseSnapshot,
) {
  const typescriptTables = new Map(typescript.tables.map((table) => [table.name, table]));
  const rustTables = new Map(rust.tables.map((table) => [table.name, table]));
  const tableNames = [...new Set([...typescriptTables.keys(), ...rustTables.keys()])].sort();
  const differences: Array<Record<string, unknown>> = [];

  for (const tableName of tableNames) {
    const left = typescriptTables.get(tableName);
    const right = rustTables.get(tableName);
    if (!left || !right) {
      differences.push({
        table: tableName,
        kind: 'missing-table-snapshot',
        typescript: Boolean(left),
        rust: Boolean(right),
      });
      continue;
    }
    if (left.sha256 === right.sha256 && left.rowCount === right.rowCount) continue;
    const length = Math.max(left.rows.length, right.rows.length);
    const rowDifferences = [];
    for (let index = 0; index < length && rowDifferences.length < 20; index += 1) {
      if (left.rows[index] !== right.rows[index]) {
        rowDifferences.push({
          index,
          typescript: left.rows[index] ?? '<missing>',
          rust: right.rows[index] ?? '<missing>',
        });
      }
    }
    differences.push({
      table: tableName,
      kind: 'row-mismatch',
      typescript: { rowCount: left.rowCount, sha256: left.sha256 },
      rust: { rowCount: right.rowCount, sha256: right.sha256 },
      firstRows: rowDifferences,
    });
  }
  return differences;
}

export function validateManifest(manifest: SideEffectManifest) {
  if (manifest.schemaVersion !== 1 || !Array.isArray(manifest.tables)) {
    throw new Error('Side-effect manifest must use schemaVersion 1');
  }
  const names = new Set<string>();
  for (const table of manifest.tables) {
    if (!IDENTIFIER.test(table.name)) throw new Error(`Invalid table name: ${table.name}`);
    if (names.has(table.name)) throw new Error(`Duplicate table: ${table.name}`);
    names.add(table.name);
    if (!Array.isArray(table.orderBy) || table.orderBy.length === 0) {
      throw new Error(`Table ${table.name} requires deterministic orderBy columns`);
    }
    for (const column of [...table.orderBy, ...(table.omitColumns ?? [])]) {
      if (!IDENTIFIER.test(column)) {
        throw new Error(`Invalid column ${column} for table ${table.name}`);
      }
    }
  }
}

async function main() {
  const args = parseArguments(process.argv.slice(2));
  for (const required of ['typescript-db', 'rust-db', 'manifest']) {
    if (!args[required]) throw new Error(`Missing required --${required}`);
  }
  const manifestSource = await fs.readFile(path.resolve(args.manifest), 'utf8');
  const manifest = JSON.parse(manifestSource) as SideEffectManifest;
  validateManifest(manifest);
  const [typescript, rust] = await Promise.all([
    snapshotDatabase(args['typescript-db'], manifest),
    snapshotDatabase(args['rust-db'], manifest),
  ]);
  const differences = compareDatabaseSnapshots(typescript, rust);
  const report = {
    schemaVersion: 1,
    generatedAt: new Date().toISOString(),
    manifestSha256: sha256(manifestSource),
    databases: {
      typescript: redactDatabaseUrl(args['typescript-db']),
      rust: redactDatabaseUrl(args['rust-db']),
    },
    summary: {
      tables: manifest.tables.length,
      passingTables: manifest.tables.length - differences.length,
      failingTables: differences.length,
    },
    differences,
    snapshots: {
      typescript: stripRows(typescript),
      rust: stripRows(rust),
    },
  };
  if (args.report) {
    const reportPath = path.resolve(args.report);
    await fs.mkdir(path.dirname(reportPath), { recursive: true });
    await fs.writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`);
  }
  process.stdout.write(`${JSON.stringify(report, null, 2)}\n`);
  if (differences.length > 0) process.exitCode = 1;
}

function quoteIdentifier(value: string) {
  if (!IDENTIFIER.test(value)) throw new Error(`Invalid SQL identifier: ${value}`);
  return `"${value}"`;
}

function stripRows(snapshot: DatabaseSnapshot) {
  return {
    tables: snapshot.tables.map(({ rows: _rows, ...summary }) => summary),
  };
}

function redactDatabaseUrl(value: string) {
  const url = new URL(value);
  url.username = '<redacted>';
  url.password = '<redacted>';
  return url.toString();
}

function parseArguments(values: string[]) {
  const parsed: Record<string, string> = {};
  for (let index = 0; index < values.length; index += 1) {
    const argument = values[index];
    if (!argument.startsWith('--')) throw new Error(`Unexpected argument ${argument}`);
    const name = argument.slice(2);
    const value = values[index + 1];
    if (!value || value.startsWith('--')) throw new Error(`Missing value for --${name}`);
    parsed[name] = value;
    index += 1;
  }
  return parsed;
}

function sha256(value: string) {
  return crypto.createHash('sha256').update(value).digest('hex');
}

if (require.main === module) {
  void main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}
