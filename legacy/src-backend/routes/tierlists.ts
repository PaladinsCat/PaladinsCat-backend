import crypto from 'crypto';
import { FastifyInstance } from 'fastify';
import { one, query, transaction } from '../config/db';
import {
  parseTierListEntries,
  validateTierListEntries,
  type TierListEntryInput,
} from '../utils/tier-list-validation';

type Session = {
  user_id: number;
  username: string;
  is_admin: boolean;
};

function hashSessionToken(token: string): string {
  return crypto.createHash('sha256').update(token).digest('hex');
}

async function getSession(req: any): Promise<Session | null> {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return null;
  return one<Session>(
    `SELECT s.user_id, u.username, u.is_admin
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token = $1 AND s.expires_at > now()`,
    [hashSessionToken(token)],
  );
}

function parseId(value: unknown): number | null {
  const id = Number.parseInt(String(value), 10);
  return Number.isInteger(id) && id > 0 ? id : null;
}

function cleanText(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

async function validateEntriesAgainstCatalog(entries: TierListEntryInput[]): Promise<string | null> {
  const championRows = await query<{ id: number }>('SELECT id FROM champions ORDER BY id');
  return validateTierListEntries(entries, new Set(championRows.map((row) => Number(row.id))));
}

const TIER_LIST_SELECT = `
  SELECT p.id, p.user_id, p.title, p.content, p.likes, p.view_count, p.created_at,
         u.username, u.linked_player_id,
         (SELECT COUNT(*)::int FROM comments cmt WHERE cmt.post_id = p.id) AS comment_count,
         COALESCE(
           jsonb_agg(
             jsonb_build_object(
               'championId', e.champion_id,
               'championName', c.name,
               'tier', e.tier,
               'position', e.position
             )
             ORDER BY CASE e.tier
               WHEN 'S' THEN 0 WHEN 'A' THEN 1 WHEN 'B' THEN 2
               WHEN 'C' THEN 3 WHEN 'D' THEN 4 ELSE 5 END,
               e.position
           ) FILTER (WHERE e.champion_id IS NOT NULL),
           '[]'::jsonb
         ) AS entries
  FROM tier_lists tl
  JOIN posts p ON p.id = tl.post_id
  JOIN users u ON u.id = p.user_id
  LEFT JOIN tier_list_entries e ON e.post_id = p.id
  LEFT JOIN champions c ON c.id = e.champion_id
`;

export default async function tierListRoutes(fastify: FastifyInstance) {
  fastify.get('/', async (req: any, reply: any) => {
    const requestedLimit = parseId(req.query?.limit) ?? 20;
    const limit = Math.min(requestedLimit, 100);
    reply.header('Cache-Control', 'public, max-age=30, s-maxage=60');
    return query(
      `${TIER_LIST_SELECT}
       GROUP BY p.id, u.username, u.linked_player_id
       ORDER BY p.created_at DESC
       LIMIT $1`,
      [limit],
    );
  });

  fastify.get('/:id', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid tier-list id' });
    const result = await query(
      `${TIER_LIST_SELECT}
       WHERE p.id = $1
       GROUP BY p.id, u.username, u.linked_player_id`,
      [id],
    );
    if (result.length === 0) return reply.status(404).send({ error: 'Tier list not found' });
    return result[0];
  });

  fastify.post('/', async (req: any, reply: any) => {
    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const title = cleanText(req.body?.title);
    const description = cleanText(req.body?.description);
    const entries = parseTierListEntries(req.body?.entries);
    if (!title || title.length > 160) return reply.status(400).send({ error: 'Title is required and must be 160 characters or fewer' });
    if (description.length > 4000) return reply.status(400).send({ error: 'Description must be 4000 characters or fewer' });
    if (!entries) return reply.status(400).send({ error: 'Tier-list entries are invalid' });

    const entryError = await validateEntriesAgainstCatalog(entries);
    if (entryError) return reply.status(400).send({ error: entryError });

    const result = await transaction(async (client) => {
      const postResult = await client.query<{ id: number }>(
        `INSERT INTO posts (user_id, title, content)
         VALUES ($1, $2, $3)
         RETURNING id`,
        [session.user_id, title, description],
      );
      const postId = postResult.rows[0].id;
      await client.query(
        `INSERT INTO tier_lists (post_id, user_id)
         VALUES ($1, $2)`,
        [postId, session.user_id],
      );
      await client.query(
        `INSERT INTO tier_list_entries (post_id, champion_id, tier, position)
         SELECT $1, entry."championId", entry.tier, entry.position
         FROM jsonb_to_recordset($2::jsonb) AS entry("championId" integer, tier text, position integer)`,
        [postId, JSON.stringify(entries)],
      );
      return { postId };
    });

    return reply.status(201).send(result);
  });

  fastify.put('/:id', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid tier-list id' });

    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const existing = await one<{ user_id: number }>(
      'SELECT user_id FROM tier_lists WHERE post_id = $1',
      [id],
    );
    if (!existing) return reply.status(404).send({ error: 'Tier list not found' });
    if (Number(existing.user_id) !== session.user_id && !session.is_admin) {
      return reply.status(403).send({ error: 'Not allowed to edit this tier list' });
    }

    const title = cleanText(req.body?.title);
    const description = cleanText(req.body?.description);
    const entries = parseTierListEntries(req.body?.entries);
    if (!title || title.length > 160) return reply.status(400).send({ error: 'Title is required and must be 160 characters or fewer' });
    if (description.length > 4000) return reply.status(400).send({ error: 'Description must be 4000 characters or fewer' });
    if (!entries) return reply.status(400).send({ error: 'Tier-list entries are invalid' });
    const entryError = await validateEntriesAgainstCatalog(entries);
    if (entryError) return reply.status(400).send({ error: entryError });

    await transaction(async (client) => {
      await client.query(
        `UPDATE posts
         SET title = $2, content = $3, updated_at = now()
         WHERE id = $1`,
        [id, title, description],
      );
      await client.query(
        'UPDATE tier_lists SET updated_at = now() WHERE post_id = $1',
        [id],
      );
      await client.query('DELETE FROM tier_list_entries WHERE post_id = $1', [id]);
      await client.query(
        `INSERT INTO tier_list_entries (post_id, champion_id, tier, position)
         SELECT $1, entry."championId", entry.tier, entry.position
         FROM jsonb_to_recordset($2::jsonb) AS entry("championId" integer, tier text, position integer)`,
        [id, JSON.stringify(entries)],
      );
    });

    return { postId: id };
  });
}
