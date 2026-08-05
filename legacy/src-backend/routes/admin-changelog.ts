import { FastifyInstance } from 'fastify';
import { one, query } from '../config/db';
import { requireAdminSession } from '../utils/query-helpers';
import { invalidateRouteCache } from '../utils/route-cache';
import { releaseSignificance } from '../utils/release-significance';

type StackChangelogRow = {
  id: number;
  version: string;
  git_commit: string | null;
  git_commit_short: string | null;
  git_branch: string | null;
  deployed_at: string | null;
  source: string | null;
  metadata: Record<string, unknown> | null;
  changelog: string | null;
};

const MAX_CHANGELOG_LENGTH = 12_000;

function mapChangelogEntry(row: StackChangelogRow) {
  const changelog = row.changelog ?? '';
  const significance = releaseSignificance(row.metadata, changelog);
  return {
    id: row.id,
    version: row.version,
    gitCommit: row.git_commit ?? '',
    gitCommitShort: row.git_commit_short ?? row.git_commit?.slice(0, 7) ?? '',
    gitBranch: row.git_branch ?? '',
    deployedAt: row.deployed_at,
    source: row.source ?? '',
    changelog,
    changeCount: significance.changeCount,
    releaseType: significance.releaseType,
  };
}

function normalizeChangelog(value: unknown): string | null {
  if (typeof value !== 'string') throw new Error('changelog must be a string');
  const normalized = value.replace(/\r\n?/g, '\n').trim();
  if (normalized.length > MAX_CHANGELOG_LENGTH) {
    throw new Error(`changelog must be ${MAX_CHANGELOG_LENGTH.toLocaleString()} characters or fewer`);
  }
  return normalized || null;
}

/** Admin-only editor for public deployment release notes. */
export default async function adminChangelogRoutes(fastify: FastifyInstance) {
  fastify.addHook('preHandler', async (req: any, reply: any) => {
    try {
      await requireAdminSession(req);
    } catch {
      return reply.status(401).send({ error: { code: 'UNAUTHORIZED', message: 'Admin access required' } });
    }
  });

  fastify.get('/changelog', async (req: any) => {
    const requestedLimit = Number.parseInt(String(req.query?.limit ?? '100'), 10);
    const limit = Number.isInteger(requestedLimit) ? Math.min(Math.max(requestedLimit, 1), 100) : 100;
    const rows = await query<StackChangelogRow>(
      `SELECT id, version, git_commit, git_commit_short, git_branch, deployed_at, source, metadata, changelog
       FROM stack_versions
       WHERE component = 'stack'
       ORDER BY deployed_at DESC, id DESC
       LIMIT $1`,
      [limit],
    );
    return rows.map(mapChangelogEntry);
  });

  fastify.put('/changelog/:id', async (req: any, reply: any) => {
    const id = Number.parseInt(String(req.params?.id), 10);
    if (!Number.isInteger(id) || id <= 0) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid changelog entry id' } });
    }

    let changelog: string | null;
    try {
      changelog = normalizeChangelog(req.body?.changelog);
    } catch (error: any) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: error.message } });
    }

    const row = await one<StackChangelogRow>(
      `UPDATE stack_versions
       SET changelog = $2
       WHERE id = $1 AND component = 'stack'
       RETURNING id, version, git_commit, git_commit_short, git_branch, deployed_at, source, metadata, changelog`,
      [id, changelog],
    );
    if (!row) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Changelog entry not found' } });
    }

    await invalidateRouteCache('route:meta:changelog');
    return mapChangelogEntry(row);
  });
}
