import { Pool } from 'pg';

type TableSummary = {
  table: string;
  exists: boolean;
  count?: string;
  maxCreatedAt?: string | null;
  maxUpdatedAt?: string | null;
  maxId?: string | null;
  maxMatchId?: string | null;
  distinctMatchIds?: string | null;
  maxPlayerId?: string | null;
  distinctPlayerIds?: string | null;
  statuses?: Record<string, string>;
};

const CRITICAL_TABLES = [
  // Facts and lookup rows that must move as-is for user-facing continuity.
  'players',
  'matches',
  'match_players',
  'bans',
  'bans_ranked',
  'card_counts_ranked',
  'talent_card_counts_ranked',
  'item_counts_ranked',
  'talent_counts_ranked',
  'champion_performance_baselines',
  'performance_records_ranked',
  'performance_metric_histogram',
  'performance_metric_stats',
  'player_best_champion_ratings',
  'champion_stats',
  'class_stats',
  'global_stats',
  'baselines',
  'player_ratings',
  'champion_player_ratings',

  // Worker control plane. These are what let the VPS continue from the local
  // ingest position instead of rediscovering windows and spending quota twice.
  'raw_ingest_buffer',
  'match_ingest_status',
  'hourly_ingest_state',
  'hourly_ingest_match_debt',
  'match_pull_list',
  'sync_jobs',

  // API accounting and cache state. Migrating these avoids key-budget drift and
  // repeated profile/history calls immediately after cutover.
  'api_keys',
  'api_log',
  'api_key_hourly_usage',
  'player_match_history_cache',
  'player_match_history_entries',
  'raw_ingest_buffer_retention_audit',
  'player_history_retention_audit',
] as const;

function requiredUrl(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} is required. Example: ${name}=postgresql://user:password@host:port/db`);
  }
  return value;
}

function quoteIdent(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

async function getColumns(pool: Pool, table: string): Promise<Set<string> | null> {
  const exists = await pool.query<{ exists: string | null }>('SELECT to_regclass($1) AS exists', [`public.${table}`]);
  if (!exists.rows[0]?.exists) {
    return null;
  }

  const columns = await pool.query<{ column_name: string }>(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'public' AND table_name = $1`,
    [table],
  );
  return new Set(columns.rows.map((row) => row.column_name));
}

async function summarizeTable(pool: Pool, table: string): Promise<TableSummary> {
  const columns = await getColumns(pool, table);
  if (!columns) {
    return { table, exists: false };
  }

  const selectParts = ['count(*)::text AS count'];
  if (columns.has('created_at')) selectParts.push('max(created_at)::text AS "maxCreatedAt"');
  if (columns.has('updated_at')) selectParts.push('max(updated_at)::text AS "maxUpdatedAt"');
  if (columns.has('id')) selectParts.push('max(id)::text AS "maxId"');
  if (columns.has('match_id')) {
    selectParts.push('max(match_id)::text AS "maxMatchId"');
    selectParts.push('count(DISTINCT match_id)::text AS "distinctMatchIds"');
  }
  if (columns.has('player_id')) {
    selectParts.push('max(player_id)::text AS "maxPlayerId"');
    selectParts.push('count(DISTINCT player_id)::text AS "distinctPlayerIds"');
  }

  const summary = await pool.query<Omit<TableSummary, 'table' | 'exists'>>(
    `SELECT ${selectParts.join(', ')} FROM public.${quoteIdent(table)}`,
  );

  const result: TableSummary = {
    table,
    exists: true,
    ...summary.rows[0],
  };

  if (columns.has('status')) {
    const statuses = await pool.query<{ status: string | null; count: string }>(
      `SELECT COALESCE(status::text, '<null>') AS status, count(*)::text AS count
       FROM public.${quoteIdent(table)}
       GROUP BY 1
       ORDER BY 1`,
    );
    result.statuses = Object.fromEntries(statuses.rows.map((row) => [row.status ?? '<null>', row.count]));
  }

  return result;
}

function stable(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(stable);
  }
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([key, nested]) => [key, stable(nested)]),
    );
  }
  return value;
}

function normalize(summary: TableSummary): string {
  return JSON.stringify(stable(summary));
}

async function summarizeDatabase(label: string, connectionString: string): Promise<TableSummary[]> {
  const pool = new Pool({ connectionString });
  try {
    console.log(`[migration-compare] collecting ${label} summaries`);
    const summaries: TableSummary[] = [];
    for (const table of CRITICAL_TABLES) {
      summaries.push(await summarizeTable(pool, table));
    }
    return summaries;
  } finally {
    await pool.end();
  }
}

async function main() {
  const localUrl = requiredUrl('LOCAL_DATABASE_URL');
  const remoteUrl = requiredUrl('REMOTE_DATABASE_URL');

  const [local, remote] = await Promise.all([
    summarizeDatabase('local', localUrl),
    summarizeDatabase('remote', remoteUrl),
  ]);

  const remoteByTable = new Map(remote.map((summary) => [summary.table, summary]));
  const differences: Array<{ table: string; local: TableSummary; remote: TableSummary }> = [];

  for (const localSummary of local) {
    const remoteSummary = remoteByTable.get(localSummary.table);
    if (!remoteSummary || normalize(localSummary) !== normalize(remoteSummary)) {
      differences.push({
        table: localSummary.table,
        local: localSummary,
        remote: remoteSummary ?? { table: localSummary.table, exists: false },
      });
    }
  }

  if (differences.length === 0) {
    console.log('[migration-compare] local and remote summaries match');
    return;
  }

  console.log(`[migration-compare] ${differences.length} table(s) differ`);
  for (const diff of differences) {
    console.log(JSON.stringify(diff, null, 2));
  }

  process.exitCode = 2;
}

main().catch((error) => {
  console.error('[migration-compare] failed:', error instanceof Error ? error.message : error);
  process.exit(1);
});
