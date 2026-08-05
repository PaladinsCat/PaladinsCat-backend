import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import crypto from 'crypto';
import { getPaladinsTwitchStreams } from '../services/twitch.js';

function hashSessionToken(token: string): string {
  return crypto.createHash('sha256').update(token).digest('hex');
}

type Session = {
  user_id: number;
  username: string;
  is_admin: boolean;
};

async function getSession(req: any): Promise<Session | null> {
  const token = req.headers.authorization?.replace('Bearer ', '');
  if (!token) return null;
  return one<Session>(
    `SELECT s.user_id, u.username, u.is_admin
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token = $1 AND s.expires_at > now()`,
    [hashSessionToken(token)]
  );
}

function parseId(value: unknown): number | null {
  const id = Number.parseInt(String(value), 10);
  return Number.isInteger(id) && id > 0 ? id : null;
}

function cleanText(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

async function canEditPost(postId: number, session: Session) {
  const post = await one<{ user_id: number }>('SELECT user_id FROM posts WHERE id = $1', [postId]);
  if (!post) return { ok: false as const, status: 404, error: 'Post not found' };
  if (post.user_id !== session.user_id && !session.is_admin) {
    return { ok: false as const, status: 403, error: 'Not allowed to edit this post' };
  }
  return { ok: true as const };
}

async function canEditComment(commentId: number, session: Session) {
  const comment = await one<{ user_id: number }>('SELECT user_id FROM comments WHERE id = $1', [commentId]);
  if (!comment) return { ok: false as const, status: 404, error: 'Comment not found' };
  if (comment.user_id !== session.user_id && !session.is_admin) {
    return { ok: false as const, status: 403, error: 'Not allowed to edit this comment' };
  }
  return { ok: true as const };
}

export default async function communityRoutes(fastify: FastifyInstance) {
  fastify.get('/streams', async (_req: any, reply: any) => {
    reply.header('Cache-Control', 'public, max-age=30, s-maxage=60');
    return getPaladinsTwitchStreams();
  });

  fastify.get('/posts', async (req: any) => {
    const limit = parseId(req.query.limit) ?? 20;
    return query(
      `SELECT p.id, p.user_id, p.title, p.content, p.build_id, p.likes, p.view_count, p.created_at,
              u.username, u.linked_player_id, tl.post_id AS tier_list_id
       FROM posts p
       JOIN users u ON u.id = p.user_id
       LEFT JOIN tier_lists tl ON tl.post_id = p.id
       ORDER BY p.created_at DESC
       LIMIT $1`,
      [Math.min(limit, 100)]
    );
  });

  fastify.post('/posts', async (req: any, reply: any) => {
    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const title = cleanText(req.body?.title);
    const content = cleanText(req.body?.content);
    const buildId = req.body?.build_id == null ? null : parseId(req.body.build_id);
    if (!title || !content) return reply.status(400).send({ error: 'Title and content are required' });

    const post = await one(
      `INSERT INTO posts (user_id, title, content, build_id)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [session.user_id, title, content, buildId]
    );
    const author = await one('SELECT linked_player_id FROM users WHERE id = $1', [session.user_id]);
    return { ...post, username: session.username, linked_player_id: author?.linked_player_id ?? null };
  });

  fastify.get('/posts/:id', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid post id' });

    const post = await one(
      `UPDATE posts p
       SET view_count = p.view_count + 1
       FROM users u
       WHERE p.id = $1 AND u.id = p.user_id
       RETURNING p.*, u.username, u.linked_player_id,
                 (SELECT tl.post_id FROM tier_lists tl WHERE tl.post_id = p.id) AS tier_list_id`,
      [id]
    );
    if (!post) return reply.status(404).send({ error: 'Post not found' });

    const comments = await query(
      `SELECT c.*, u.username, u.linked_player_id
       FROM comments c
       JOIN users u ON u.id = c.user_id
       WHERE c.post_id = $1
       ORDER BY c.created_at`,
      [id]
    );
    return { post, comments };
  });

  fastify.put('/posts/:id', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid post id' });

    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const permission = await canEditPost(id, session);
    if (!permission.ok) return reply.status(permission.status).send({ error: permission.error });

    const title = cleanText(req.body?.title);
    const content = cleanText(req.body?.content);
    if (!title || !content) return reply.status(400).send({ error: 'Title and content are required' });

    const post = await one(
      `UPDATE posts p
       SET title = $2, content = $3
       FROM users u
       WHERE p.id = $1 AND u.id = p.user_id
       RETURNING p.*, u.username`,
      [id, title, content]
    );
    return post;
  });

  fastify.delete('/posts/:id', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid post id' });

    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const permission = await canEditPost(id, session);
    if (!permission.ok) return reply.status(permission.status).send({ error: permission.error });

    await one('DELETE FROM posts WHERE id = $1', [id]);
    return { deleted: true, id };
  });

  fastify.post('/posts/:id/comments', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid post id' });

    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const content = cleanText(req.body?.content);
    const parentId = req.body?.parent_id == null ? null : parseId(req.body.parent_id);
    if (!content) return reply.status(400).send({ error: 'Comment content is required' });

    const post = await one<{ id: number; user_id: number }>('SELECT id, user_id FROM posts WHERE id = $1', [id]);
    if (!post) return reply.status(404).send({ error: 'Post not found' });

    const comment = await one(
      `INSERT INTO comments (post_id, user_id, parent_id, content)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [id, session.user_id, parentId, content]
    );
    if (post.user_id !== session.user_id) {
      await query(
        `INSERT INTO user_notifications (user_id, actor_user_id, type, post_id, comment_id)
         VALUES ($1, $2, 'community_comment', $3, $4)
         ON CONFLICT (user_id, comment_id) DO NOTHING`,
        [post.user_id, session.user_id, id, comment.id],
      );
    }
    const author = await one('SELECT linked_player_id FROM users WHERE id = $1', [session.user_id]);
    return { ...comment, username: session.username, linked_player_id: author?.linked_player_id ?? null };
  });

  fastify.put('/comments/:id', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid comment id' });

    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const permission = await canEditComment(id, session);
    if (!permission.ok) return reply.status(permission.status).send({ error: permission.error });

    const content = cleanText(req.body?.content);
    if (!content) return reply.status(400).send({ error: 'Comment content is required' });

    const comment = await one(
      `UPDATE comments c
       SET content = $2
       FROM users u
       WHERE c.id = $1 AND u.id = c.user_id
       RETURNING c.*, u.username, u.linked_player_id`,
      [id, content]
    );
    return comment;
  });

  fastify.delete('/comments/:id', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid comment id' });

    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const permission = await canEditComment(id, session);
    if (!permission.ok) return reply.status(permission.status).send({ error: permission.error });

    await one('DELETE FROM comments WHERE id = $1', [id]);
    return { deleted: true, id };
  });

  fastify.post('/posts/:id/like', async (req: any, reply: any) => {
    const id = parseId(req.params.id);
    if (!id) return reply.status(400).send({ error: 'Invalid post id' });

    const session = await getSession(req);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const post = await one('SELECT id FROM posts WHERE id = $1', [id]);
    if (!post) return reply.status(404).send({ error: 'Post not found' });

    const existing = await one('SELECT 1 FROM user_post_likes WHERE user_id = $1 AND post_id = $2', [session.user_id, id]);
    if (existing) {
      await one('DELETE FROM user_post_likes WHERE user_id = $1 AND post_id = $2', [session.user_id, id]);
      const row = await one('UPDATE posts SET likes = GREATEST(likes - 1, 0) WHERE id = $1 RETURNING likes', [id]);
      return { liked: false, likes: Number(row?.likes ?? 0) };
    }

    const inserted = await one(
      `INSERT INTO user_post_likes (user_id, post_id)
       VALUES ($1, $2)
       ON CONFLICT DO NOTHING
       RETURNING post_id`,
      [session.user_id, id]
    );
    if (inserted) {
      const row = await one('UPDATE posts SET likes = likes + 1 WHERE id = $1 RETURNING likes', [id]);
      return { liked: true, likes: Number(row?.likes ?? 0) };
    }

    const row = await one('SELECT likes FROM posts WHERE id = $1', [id]);
    return { liked: true, likes: Number(row?.likes ?? 0) };
  });
}
