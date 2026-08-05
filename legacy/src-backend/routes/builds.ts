import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import crypto from 'crypto';

function hashSessionToken(token: string): string {
  return crypto.createHash('sha256').update(token).digest('hex');
}

function normalizeIntArray(input: unknown, maxLength: number): number[] | null {
  if (!Array.isArray(input)) return [];
  if (input.length > maxLength) return null;
  const values = input.map((value) => Number(value));
  if (values.some((value) => !Number.isInteger(value) || value <= 0)) return null;
  return Array.from(new Set(values));
}

function normalizeBuildCards(input: unknown): Array<{ card_id: number; level: number }> | null {
  if (!Array.isArray(input)) return [];
  if (input.length > 5) return null;

  const seen = new Set<number>();
  const cards: Array<{ card_id: number; level: number }> = [];
  for (const value of input) {
    if (!value || typeof value !== 'object') return null;
    const row = value as { card_id?: unknown; cardId?: unknown; level?: unknown; card_level?: unknown };
    const cardId = Number(row.card_id ?? row.cardId);
    const level = Number(row.level ?? row.card_level);
    if (!Number.isInteger(cardId) || cardId <= 0 || !Number.isInteger(level) || level < 1 || level > 5) return null;
    if (seen.has(cardId)) return null;
    seen.add(cardId);
    cards.push({ card_id: cardId, level });
  }
  return cards;
}

async function selectBuild(buildId: number) {
  return one(
    `SELECT b.id, b.user_id, b.champion_id, COALESCE(c.name, 'Champion ' || b.champion_id::TEXT) AS champion_name,
        b.name, b.items, b.cards, b.actives, b.talents, b.notes, b.visibility, b.likes, b.view_count, b.created_at, u.username
       FROM builds b
       JOIN users u ON u.id = b.user_id
       LEFT JOIN champions c ON c.id = b.champion_id
       WHERE b.id = $1`,
    [buildId]
  );
}

export default async function buildsRoutes(fastify: FastifyInstance) {
  fastify.get('/', async (req: any) => {
    const championId = req.query.championId ? parseInt(req.query.championId as string, 10) : undefined;
    const q = championId ? 'WHERE b.champion_id = $1 AND b.visibility = \'public\'' : 'WHERE b.visibility = \'public\'';
    const params = championId ? [championId] : [];
    return query(
      `SELECT b.id, b.user_id, b.champion_id, COALESCE(c.name, 'Champion ' || b.champion_id::TEXT) AS champion_name,
        b.name, b.items, b.cards, b.actives, b.talents, b.notes, b.visibility, b.likes, b.view_count, b.created_at, u.username
       FROM builds b
       JOIN users u ON u.id = b.user_id
       LEFT JOIN champions c ON c.id = b.champion_id
       ${q}
       ORDER BY b.likes DESC`,
      params
    );
  });

  fastify.post('/', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    const session = token
      ? await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [hashSessionToken(token)])
      : null;
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const { champion_id, name, items, cards, actives, talents, notes, visibility } = req.body ?? {};
    const championId = Number(champion_id);
    const buildName = String(name ?? '').trim();
    const normalizedItems = normalizeIntArray(items, 4);
    const normalizedCards = normalizeBuildCards(cards);
    const normalizedActives = normalizeIntArray(actives, 4);
    const normalizedTalents = normalizeIntArray(talents, 1);
    const normalizedVisibility = visibility === 'private' ? 'private' : 'public';

    if (!Number.isInteger(championId) || championId <= 0) {
      return reply.status(400).send({ error: 'Invalid champion_id' });
    }
    if (!buildName) {
      return reply.status(400).send({ error: 'Build name is required' });
    }
    if (!normalizedItems) {
      return reply.status(400).send({ error: 'Builds can include up to 4 valid item IDs' });
    }
    if (!normalizedCards) {
      return reply.status(400).send({ error: 'Builds can include up to 5 cards with levels from 1 to 5' });
    }
    if (!normalizedActives) {
      return reply.status(400).send({ error: 'Invalid legacy active item IDs' });
    }
    if (!normalizedTalents) {
      return reply.status(400).send({ error: 'Builds can include only 1 valid talent ID' });
    }

    // The UI enforces the full Paladins deck rule (5 cards totaling 15 points).
    // The API keeps validation slightly looser so older or imported community
    // builds can still be represented, but it always stores card levels in a
    // lossless JSONB shape instead of overloading the legacy `actives` array.
    const inserted = await one(
      `INSERT INTO builds (user_id, champion_id, name, items, cards, actives, talents, notes, visibility)
       VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9)
       RETURNING id`,
      [
        session.user_id,
        championId,
        buildName,
        normalizedItems,
        JSON.stringify(normalizedCards),
        normalizedActives,
        normalizedTalents,
        typeof notes === 'string' && notes.trim() ? notes.trim() : null,
        normalizedVisibility,
      ]
    );

    return selectBuild(Number(inserted.id));
  });

  fastify.get('/:id', async (req, reply) => {
    const id = parseInt((req.params as any).id, 10);
    const build = await selectBuild(id);
    if (!build) return reply.status(404).send({ error: 'Build not found' });
    await one('UPDATE builds SET view_count = view_count + 1 WHERE id = $1', [id]);
    return build;
  });

  fastify.post('/:id/like', async (req: any, reply: any) => {
    const id = parseInt(req.params.id as string, 10);
    const token = req.headers.authorization?.replace('Bearer ', '');
    const session = token
      ? await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [hashSessionToken(token)])
      : null;
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });
    const existing = await one('SELECT * FROM user_build_likes WHERE user_id = $1 AND build_id = $2', [session.user_id, id]);
    if (existing) {
      await one('DELETE FROM user_build_likes WHERE user_id = $1 AND build_id = $2', [session.user_id, id]);
      const row = await one('UPDATE builds SET likes = GREATEST(likes - 1, 0) WHERE id = $1 RETURNING likes', [id]);
      return { liked: false, likes: Number(row?.likes ?? 0) };
    } else {
      await one('INSERT INTO user_build_likes (user_id, build_id) VALUES ($1, $2)', [session.user_id, id]);
      const row = await one('UPDATE builds SET likes = likes + 1 WHERE id = $1 RETURNING likes', [id]);
      return { liked: true, likes: Number(row?.likes ?? 0) };
    }
  });
}
