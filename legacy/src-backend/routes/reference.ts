import { FastifyInstance } from 'fastify';
import { query } from '../config/db';
import { get, set } from '../services/cache';

const REFERENCE_CACHE_TTL = 3600; // 1 hour — static data

/**
 * Reference data route file.
 * Exposes all static lookup tables: champions, items, bounty_items, maps,
 * ranked_tiers, regions, talents, queue_types, patches, cards, skins, championsquick.
 */

interface ReferenceTable {
  table: string;
  route: string;
  cacheKey: string;
  idColumn?: string;
}

const REFERENCE_TABLES: ReferenceTable[] = [
  { table: 'champions', route: 'champions', cacheKey: 'ref:champions', idColumn: 'id' },
  { table: 'items', route: 'items', cacheKey: 'ref:items', idColumn: 'item_id' },
  { table: 'bounty_items', route: 'bounty-items', cacheKey: 'ref:bounty_items', idColumn: 'bounty_item_id' },
  { table: 'maps', route: 'maps', cacheKey: 'ref:maps', idColumn: 'map_id' },
  { table: 'ranked_tiers', route: 'tiers', cacheKey: 'ref:ranked_tiers', idColumn: 'tier_id' },
  // regions uses region_code/region_name, not a generic `region` column. This
  // route is hit by the match dashboard on startup, so keeping the canonical
  // key here prevents harmless reference lookups from surfacing as 500s.
  { table: 'regions', route: 'regions', cacheKey: 'ref:regions', idColumn: 'region_code' },
  { table: 'talents', route: 'talents', cacheKey: 'ref:talents', idColumn: 'talent_id' },
  { table: 'queue_types', route: 'queues', cacheKey: 'ref:queue_types', idColumn: 'queue_id' },
  { table: 'patches', route: 'patches', cacheKey: 'ref:patches', idColumn: 'id' },
  { table: 'cards', route: 'cards', cacheKey: 'ref:cards', idColumn: 'card_id' },
  { table: 'skins', route: 'skins', cacheKey: 'ref:skins', idColumn: 'skin_id' },
  { table: 'championsquick', route: 'abilities', cacheKey: 'ref:championsquick', idColumn: 'id' },
];

export default async function referenceRoutes(fastify: FastifyInstance) {
  // Register individual routes: GET /reference/:type
  for (const ref of REFERENCE_TABLES) {
    fastify.get(`/${ref.route}`, async () => {
      const cached = await get(ref.cacheKey);
      if (cached) return cached;

      const rows = await query(`SELECT * FROM ${ref.table} ORDER BY ${ref.idColumn ?? 'id'}`);
      await set(ref.cacheKey, rows, REFERENCE_CACHE_TTL);
      return rows;
    });

    // Single lookup: GET /reference/:type/:id
    if (ref.idColumn) {
      fastify.get(`/${ref.route}/:id`, async (req: any, reply: any) => {
        const id = req.params.id;
        const cached = await get(`${ref.cacheKey}:${id}`);
        if (cached) return cached;

        const row = await query(`SELECT * FROM ${ref.table} WHERE ${ref.idColumn} = $1`, [id]);
        if (row.length === 0) {
          return reply.status(404).send({ error: { code: 'NOT_FOUND', message: `${ref.route} not found`, details: { id } } });
        }
        await set(`${ref.cacheKey}:${id}`, row[0], REFERENCE_CACHE_TTL);
        return row[0];
      });
    }
  }

  // Generic lookup: GET /reference/lookup?type=items&id=123
  fastify.get('/lookup', async (req: any, reply: any) => {
    const type = req.query.type as string;
    const id = req.query.id as string;

    if (!type) {
      return reply.status(400).send({ error: { code: 'VALIDATION', message: 'Missing required query param: type' } });
    }

    // Map route name to table name
    const ref = REFERENCE_TABLES.find(r => r.route === type);
    if (!ref) {
      return reply.status(400).send({
        error: { code: 'VALIDATION', message: `Unknown reference type: ${type}. Valid types: ${REFERENCE_TABLES.map(r => r.route).join(', ')}` },
      });
    }

    if (id) {
      const row = await query(`SELECT * FROM ${ref.table} WHERE ${ref.idColumn ?? 'id'} = $1`, [id]);
      if (row.length === 0) {
        return reply.status(404).send({ error: { code: 'NOT_FOUND', message: `${type} not found`, details: { id } } });
      }
      return row[0];
    }

    const cached = await get(ref.cacheKey);
    if (cached) return cached;

    const rows = await query(`SELECT * FROM ${ref.table} ORDER BY ${ref.idColumn ?? 'id'}`);
    await set(ref.cacheKey, rows, REFERENCE_CACHE_TTL);
    return rows;
  });
}
