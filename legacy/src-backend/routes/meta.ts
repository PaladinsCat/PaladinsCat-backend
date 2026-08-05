import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { paginate, sorting } from '../utils/query-helpers';
import { FilterBuilder } from '../utils/filter-builder';
import { releaseSignificance } from '../utils/release-significance';
import { appendLobbyTierPredicate, parseLobbyTierBounds } from '../utils/lobby-tier';
import { registerReadThroughCache } from '../utils/route-cache';
type StackVersionRow = {
  id: number;
  component: string;
  environment: string;
  version: string;
  git_commit: string | null;
  git_commit_short: string | null;
  git_branch: string | null;
  git_dirty: boolean | null;
  build_timestamp: string | null;
  deployed_at: string | null;
  db_schema_version: string | null;
  source: string | null;
  notes: string | null;
  metadata: Record<string, unknown> | null;
  changelog: string | null;
};

function normalizeCommitShort(full: string | null | undefined, short: string | null | undefined): string {
  if (short) return short;
  return full ? full.slice(0, 7) : '';
}

function mapStackVersion(row: StackVersionRow, components: StackVersionRow[] = []) {
  const gitCommitShort = normalizeCommitShort(row.git_commit, row.git_commit_short);
  return {
    id: row.id,
    timestamp: row.deployed_at,
    version: row.version,
    component: row.component,
    environment: row.environment,
    gitCommit: row.git_commit ?? '',
    gitCommitShort,
    gitBranch: row.git_branch ?? '',
    gitDirty: Boolean(row.git_dirty),
    buildTimestamp: row.build_timestamp,
    deployedAt: row.deployed_at,
    dbSchemaVersion: row.db_schema_version ?? '',
    source: row.source ?? '',
    notes: row.notes ?? '',
    metadata: row.metadata ?? {},
    components: components.map((component) => ({
      id: component.id,
      component: component.component,
      environment: component.environment,
      version: component.version,
      gitCommit: component.git_commit ?? '',
      gitCommitShort: normalizeCommitShort(component.git_commit, component.git_commit_short),
      gitBranch: component.git_branch ?? '',
      gitDirty: Boolean(component.git_dirty),
      buildTimestamp: component.build_timestamp,
      deployedAt: component.deployed_at,
      dbSchemaVersion: component.db_schema_version ?? '',
      source: component.source ?? '',
      metadata: component.metadata ?? {},
    })),
  };
}

/**
 * Meta Stats Routes.
 * Exposes aggregate count tables (ranked/casual split) + match compositions.
 * Tables: item_counts_ranked/casual, talent_counts_ranked/casual,
 *         card_counts_ranked/casual, match_compositions.
 */
export default async function metaRoutes(fastify: FastifyInstance) {
  // The changelog preview is requested by the high-traffic homepage. Keep the
  // public feed cache-first; admin changelog writes already invalidate this
  // namespace so published edits are visible immediately.
  registerReadThroughCache(fastify, {
    namespace: 'route:meta:changelog',
    shouldCache: (req) => (
      req.url.startsWith('/changelog')
      || req.url.startsWith('/meta/changelog')
    ),
    ttlSeconds: () => 300,
  });

  /**
   * GET /meta/version — Latest public deployment/version metadata.
   *
   * Backward compatibility: callers that only know about the old site_versions
   * contract can still read { id, timestamp, version }. New deploy tooling and
   * the footer use the extra Git fields to compare local HEAD against the VPS
   * without SSH or database access.
   */
  fastify.get('/version', async (_req, reply) => {
    reply.header('Cache-Control', 'no-store, max-age=0');

    const row = await one<StackVersionRow>(
      `SELECT id, component, environment, version, git_commit, git_commit_short,
              git_branch, git_dirty, build_timestamp, deployed_at,
              db_schema_version, source, notes, metadata
       FROM stack_versions
       WHERE component = 'stack'
       ORDER BY deployed_at DESC, id DESC
       LIMIT 1`
    );

    if (row) {
      const components = await query<StackVersionRow>(
        `SELECT DISTINCT ON (component)
                id, component, environment, version, git_commit, git_commit_short,
                git_branch, git_dirty, build_timestamp, deployed_at,
                db_schema_version, source, notes, metadata
         FROM stack_versions
         WHERE component <> 'stack'
         ORDER BY component, deployed_at DESC, id DESC`
      );
      return mapStackVersion(row, components);
    }

    const legacy = await one<{
      id: number;
      timestamp: string | null;
      version: string;
    }>(
      `SELECT id, timestamp, version
       FROM site_versions
       ORDER BY timestamp DESC, id DESC
       LIMIT 1`
    );

    return {
      id: legacy?.id ?? 0,
      timestamp: legacy?.timestamp ?? null,
      version: legacy?.version ?? process.env.PALADINSCAT_VERSION ?? '',
      component: 'stack',
      environment: process.env.NODE_ENV ?? 'unknown',
      gitCommit: process.env.PALADINSCAT_GIT_COMMIT ?? '',
      gitCommitShort: normalizeCommitShort(process.env.PALADINSCAT_GIT_COMMIT, process.env.PALADINSCAT_GIT_COMMIT_SHORT),
      gitBranch: process.env.PALADINSCAT_GIT_BRANCH ?? '',
      gitDirty: process.env.PALADINSCAT_GIT_DIRTY === 'true',
      buildTimestamp: process.env.PALADINSCAT_BUILD_TIMESTAMP ?? null,
      deployedAt: legacy?.timestamp ?? null,
      dbSchemaVersion: '036_stack_versions',
      source: legacy ? 'site_versions_legacy' : 'runtime_env_fallback',
      metadata: {},
      components: [],
    };
  });

  /**
   * GET /meta/changelog — Paginated changelog feed.
   *
   * Returns ALL stack_versions rows (stack component only), ordered by
   * deployed_at DESC. Entries without changelog text are included so the
   * page shows a complete deployment history. Used by the public /changelog
   * page and the homepage changelog preview card.
   *
   * Query params:
   *   ?page= — Page number (default 1)
   *   ?perPage= — Items per page (default 10, max 50)
   *   ?preview=true — Return only the first entry with changelog (homepage card)
   */
  fastify.get('/changelog', async (req: any) => {
    const isPreview = req.query.preview === 'true';
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    if (isPreview) {
      const row = await one<StackVersionRow>(
        `SELECT id, version, git_commit, git_commit_short, git_branch, deployed_at, source, metadata, changelog
         FROM stack_versions
         WHERE component = 'stack' AND changelog IS NOT NULL AND changelog <> ''
         ORDER BY deployed_at DESC, id DESC
         LIMIT 1`
      );
      return row ? mapChangelogEntry(row) : null;
    }

    // A redeploy can create another stack-version row for the same Git commit.
    // The public history should show the useful record once, preferring a row
    // with a changelog over a later operational duplicate with an empty range.
    const publicVersionRows = `
      SELECT DISTINCT ON (COALESCE(NULLIF(git_commit, ''), 'row:' || id::text))
        id, version, git_commit, git_commit_short, git_branch, deployed_at, source, metadata, changelog
      FROM stack_versions
      WHERE component = 'stack'
      ORDER BY
        COALESCE(NULLIF(git_commit, ''), 'row:' || id::text),
        (changelog IS NOT NULL AND changelog <> '') DESC,
        deployed_at DESC,
        id DESC
    `;

    const totalResult = await one<{ total: number }>(
      `SELECT COUNT(*)::INT AS total FROM (${publicVersionRows}) AS public_versions`
    );
    const total = totalResult?.total ?? 0;

    const rows = await query<StackVersionRow>(
      `SELECT id, version, git_commit, git_commit_short, git_branch, deployed_at, source, metadata, changelog
       FROM (${publicVersionRows}) AS public_versions
       ORDER BY deployed_at DESC, id DESC
       LIMIT $1 OFFSET $2`,
      [perPage, offset]
    );

    return {
      data: rows.map(mapChangelogEntry),
      total,
      page,
      perPage,
      totalPages: Math.ceil(total / perPage),
    };
  });

function mapChangelogEntry(row: StackVersionRow) {
  const significance = releaseSignificance(row.metadata, row.changelog);
  return {
    id: row.id,
    version: row.version,
    gitCommit: row.git_commit ?? '',
    gitCommitShort: normalizeCommitShort(row.git_commit, row.git_commit_short),
    gitBranch: row.git_branch ?? '',
    deployedAt: row.deployed_at,
    source: row.source ?? '',
    changelog: row.changelog ?? '',
    changeCount: significance.changeCount,
    releaseType: significance.releaseType,
  };
}

  /**
   * GET /meta/items — Item usage stats
   * ?mode=ranked|casual (default: ranked)
   * ?slot=, ?itemLevel=, ?sort=count|winrate&wins, ?order=, ?limit=
   */
  fastify.get('/items', async (req: any) => {
    const mode = req.query.mode === 'casual' ? 'casual' : 'ranked';
    const table = `item_counts_${mode}`;
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const fb = new FilterBuilder();
    if (req.query.slot) fb.eq('slot', parseInt(req.query.slot, 10));
    if (req.query.itemLevel) fb.eq('item_level', parseInt(req.query.itemLevel, 10));

    const { clause, params } = fb.build();
    const sort = sorting(req.query.sort, req.query.order, ['count', 'winrate', 'wins', 'losses', 'item_name', 'slot', 'item_level']);

    const rows = await query(
      `SELECT * FROM ${table}${clause}${sort} LIMIT $${params.length + 1} OFFSET $${params.length + 2}`,
      [...params, perPage, offset]
    );
    return rows;
  });

  /**
   * GET /meta/talents — Talent usage stats
   * ?mode=ranked|casual, ?sort=count|winrate&wins, ?order=, ?limit=
   */
  fastify.get('/talents', async (req: any) => {
    const mode = req.query.mode === 'casual' ? 'casual' : 'ranked';
    const table = `talent_counts_${mode}`;
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const sort = sorting(req.query.sort, req.query.order, ['count', 'winrate', 'wins']);

    const rows = await query(
      `SELECT * FROM ${table}${sort} LIMIT $1 OFFSET $2`,
      [perPage, offset]
    );
    return rows;
  });

  /**
   * GET /meta/cards — Card usage stats
   * ?mode=ranked|casual, ?sort=count|winrate&wins, ?order=, ?limit=
   */
  fastify.get('/cards', async (req: any) => {
    const mode = req.query.mode === 'casual' ? 'casual' : 'ranked';
    const table = `card_counts_${mode}`;
    const { page, perPage, offset } = paginate({ page: req.query.page, perPage: req.query.perPage });

    const sort = sorting(req.query.sort, req.query.order, ['count', 'winrate', 'wins']);

    const rows = await query(
      `SELECT * FROM ${table}${sort} LIMIT $1 OFFSET $2`,
      [perPage, offset]
    );
    return rows;
  });

  /**
   * GET /meta/compositions — Ranked team compositions
   * ?sortBy=count|winrate|wins|frontline|damage|flank|support, ?order=, ?limit=
   */
  fastify.get('/compositions', async (req: any, reply: any) => {
    const sortBy = req.query.sortBy || 'count';
    const order = req.query.order === 'asc' ? 'ASC' : 'DESC';
    const limit = Math.min(parseInt(req.query.limit, 10) || 50, 200);

    const validSort = ['count', 'winrate', 'wins', 'frontline', 'damage', 'flank', 'support'];
    const sort = validSort.includes(sortBy) ? sortBy : 'count';

    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const params: any[] = [];
    const where: string[] = [];
    appendLobbyTierPredicate(lobbyTier, params, where, 'mcr');
    params.push(limit);

    const rows = await query(
      `SELECT
         mcr.comp_id,
         mcr.frontline,
         mcr.damage,
         mcr.flank,
         mcr.support,
         SUM(mcr.count)::INT AS count,
         SUM(mcr.wins)::INT AS wins,
         SUM(mcr.losses)::INT AS losses,
         ROUND(
           100.0 * SUM(mcr.wins)::NUMERIC
           / NULLIF((SUM(mcr.wins) + SUM(mcr.losses))::NUMERIC, 0),
           2
         ) AS winrate
       FROM match_compositions_ranked mcr
       ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       GROUP BY mcr.comp_id, mcr.frontline, mcr.damage, mcr.flank, mcr.support
       ORDER BY ${sort} ${order}, mcr.comp_id
       LIMIT $${params.length}`,
      params,
    );
    return { total: rows.length, data: rows };
  });

  /**
   * GET /meta/items/:itemId — Item detail with winrate breakdown
   * ?mode=ranked|casual
   */
  fastify.get('/items/:itemId', async (req: any, reply: any) => {
    const itemId = parseInt(req.params.itemId, 10);
    if (!Number.isInteger(itemId)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid item ID' } });
    }

    const mode = req.query.mode === 'casual' ? 'casual' : 'ranked';
    const table = `item_counts_${mode}`;

    const row = await query(`SELECT * FROM ${table} WHERE item_id = $1`, [itemId]);
    if (row.length === 0) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Item stats not found', details: { itemId, mode } } });
    }
    return row[0];
  });

  /**
   * GET /meta/talents/:talentId — Talent detail
   * ?mode=ranked|casual
   */
  fastify.get('/talents/:talentId', async (req: any, reply: any) => {
    const talentId = parseInt(req.params.talentId, 10);
    if (!Number.isInteger(talentId)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid talent ID' } });
    }

    const mode = req.query.mode === 'casual' ? 'casual' : 'ranked';
    const table = `talent_counts_${mode}`;

    const row = await query(`SELECT * FROM ${table} WHERE talent_id = $1`, [talentId]);
    if (row.length === 0) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Talent stats not found', details: { talentId, mode } } });
    }
    return row[0];
  });

  /**
   * GET /meta/cards/:cardId — Card detail
   * ?mode=ranked|casual
   */
  fastify.get('/cards/:cardId', async (req: any, reply: any) => {
    const cardId = parseInt(req.params.cardId, 10);
    if (!Number.isInteger(cardId)) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Invalid card ID' } });
    }

    const mode = req.query.mode === 'casual' ? 'casual' : 'ranked';
    const table = `card_counts_${mode}`;

    const row = await query(`SELECT * FROM ${table} WHERE card_id = $1`, [cardId]);
    if (row.length === 0) {
      return reply.status(404).send({ error: { code: 'NOT_FOUND', message: 'Card stats not found', details: { cardId, mode } } });
    }
    return row[0];
  });

  /**
   * GET /meta/top — Aggregated top items/talents/cards
   * ?mode=ranked|casual, ?limit=
   */
  fastify.get('/top', async (req: any) => {
    const mode = req.query.mode === 'casual' ? 'casual' : 'ranked';
    const limit = Math.min(parseInt(req.query.limit, 10) || 10, 50);

    const [topItems, topTalents, topCards] = await Promise.all([
      query(`SELECT item_id, item_name AS name, count, winrate FROM item_counts_${mode} ORDER BY count DESC LIMIT $1`, [limit]),
      query(`SELECT talent_id, talent_name AS name, count, winrate FROM talent_counts_${mode} ORDER BY count DESC LIMIT $1`, [limit]),
      query(`SELECT card_id, card_name AS name, count, winrate FROM card_counts_${mode} ORDER BY count DESC LIMIT $1`, [limit]),
    ]);

    return { mode, topItems, topTalents, topCards };
  });
}
