import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import crypto from 'crypto';
import { getPlayerLoadouts } from '../services/hirez';
import { recordRawHirezResponse } from '../services/raw-hirez-response-audit';
import { guardVendorFallback, RequestSecurityError } from '../services/request-security';

const SESSION_TTL_HOURS = 72;
const USERNAME_RE = /^[A-Za-z0-9_-]{3,32}$/;
const PLAYER_LINK_ATTEMPT_COOLDOWN_SECONDS = 30;
const PLAYER_LINK_MAX_ATTEMPTS = 5;
const PLAYER_LINK_LOCKOUT_MINUTES = 10;

function hashPassword(password: string, salt: string): string {
  return crypto.createHash('sha256').update(password + salt).digest('hex');
}

function hashSessionToken(token: string): string {
  return crypto.createHash('sha256').update(token).digest('hex');
}

function normalizeEmail(email: string): string {
  return String(email || '').trim().toLowerCase();
}

function normalizeUsername(username: string): string {
  return String(username || '').trim();
}

function isValidEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function isDuplicateKeyError(err: unknown): boolean {
  return Boolean(err && typeof err === 'object' && (err as any).code === '23505');
}

let authSchemaReady: Promise<void> | null = null;

async function ensureAuthSchema(): Promise<void> {
  if (!authSchemaReady) {
    authSchemaReady = (async () => {
      // Runtime schema alignment is intentionally local to the auth route.
      // Existing Docker volumes only run /docker-entrypoint-initdb.d SQL on the
      // first boot, so a VPS restored from an older volume can have `users`
      // without the newer `salt`, `is_admin`, and `is_approved` columns. Account
      // creation must not crash in that state. The SQL below is idempotent and
      // mirrors db/035_auth_runtime_alignment.sql so normal fresh installs and
      // already-running production databases converge to the same contract.
      await query(`
        CREATE TABLE IF NOT EXISTS users (
          id            SERIAL PRIMARY KEY,
          username      VARCHAR(64) NOT NULL,
          email         VARCHAR(255) NOT NULL,
          password_hash VARCHAR(64) NOT NULL,
          salt          VARCHAR(64) NOT NULL DEFAULT '',
          avatar_url    TEXT,
          bio           TEXT,
          is_active     BOOLEAN NOT NULL DEFAULT TRUE,
          is_admin      BOOLEAN NOT NULL DEFAULT FALSE,
          is_approved   BOOLEAN NOT NULL DEFAULT FALSE,
          created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
          last_login    TIMESTAMPTZ,
          updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS salt VARCHAR(64)`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_approved BOOLEAN`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS is_active BOOLEAN`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS time_zone VARCHAR(64)`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login TIMESTAMPTZ`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ`);
      await query(`
        UPDATE users
        SET
          salt = COALESCE(salt, ''),
          is_admin = COALESCE(is_admin, FALSE),
          is_approved = COALESCE(is_approved, FALSE),
          is_active = COALESCE(is_active, TRUE),
          updated_at = COALESCE(updated_at, now())
      `);
      await query(`ALTER TABLE users ALTER COLUMN salt SET DEFAULT ''`);
      await query(`ALTER TABLE users ALTER COLUMN salt SET NOT NULL`);
      await query(`ALTER TABLE users ALTER COLUMN is_admin SET DEFAULT FALSE`);
      await query(`ALTER TABLE users ALTER COLUMN is_admin SET NOT NULL`);
      await query(`ALTER TABLE users ALTER COLUMN is_approved SET DEFAULT FALSE`);
      await query(`ALTER TABLE users ALTER COLUMN is_approved SET NOT NULL`);
      await query(`ALTER TABLE users ALTER COLUMN is_active SET DEFAULT TRUE`);
      await query(`ALTER TABLE users ALTER COLUMN is_active SET NOT NULL`);
      await query(`ALTER TABLE users ALTER COLUMN updated_at SET DEFAULT now()`);
      await query(`ALTER TABLE users ALTER COLUMN updated_at SET NOT NULL`);
      await query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS linked_player_id BIGINT`);
      await query(`CREATE INDEX IF NOT EXISTS idx_users_linked_player ON users (linked_player_id) WHERE linked_player_id IS NOT NULL`);
      await query(`
        CREATE TABLE IF NOT EXISTS player_link_verifications (
          user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
          player_id BIGINT NOT NULL,
          code VARCHAR(24) NOT NULL UNIQUE,
          expires_at TIMESTAMPTZ NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          last_attempt_at TIMESTAMPTZ,
          next_attempt_at TIMESTAMPTZ,
          locked_until TIMESTAMPTZ,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);
      await query(`ALTER TABLE player_link_verifications ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0`);
      await query(`ALTER TABLE player_link_verifications ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMPTZ`);
      await query(`ALTER TABLE player_link_verifications ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ`);
      await query(`ALTER TABLE player_link_verifications ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ`);
      await query(`CREATE INDEX IF NOT EXISTS idx_player_link_verifications_expires ON player_link_verifications (expires_at)`);
      await query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username ON users (username)`);
      await query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users (email)`);
      await query(`CREATE INDEX IF NOT EXISTS idx_users_username_lower ON users ((lower(username)))`);
      await query(`CREATE INDEX IF NOT EXISTS idx_users_email_lower ON users ((lower(email)))`);

      await query(`
        CREATE TABLE IF NOT EXISTS sessions (
          id          SERIAL PRIMARY KEY,
          user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
          token       VARCHAR(64) NOT NULL,
          device      VARCHAR(128),
          ip_address  VARCHAR(64),
          expires_at  TIMESTAMPTZ NOT NULL,
          created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);
      await query(`ALTER TABLE sessions ADD COLUMN IF NOT EXISTS device VARCHAR(128)`);
      await query(`ALTER TABLE sessions ADD COLUMN IF NOT EXISTS ip_address VARCHAR(64)`);
      await query(`ALTER TABLE sessions ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()`);
      await query(`CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions (token)`);
      await query(`CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions (user_id)`);
      await query(`CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions (expires_at)`);
    })().catch((err) => {
      authSchemaReady = null;
      throw err;
    });
  }

  return authSchemaReady;
}

function toAuthUser(row: any) {
  return {
    id: row.id,
    username: row.username,
    email: row.email,
    avatar_url: row.avatar_url ?? null,
    bio: row.bio ?? null,
    time_zone: row.time_zone ?? null,
    is_admin: Boolean(row.is_admin),
    is_approved: Boolean(row.is_approved),
    created_at: row.created_at,
    last_login: row.last_login ?? null,
    linked_player_id: row.linked_player_id ?? null,
    linked_player_name: row.linked_player_name ?? null,
  };
}

async function lockPlayerLinkVerification(userId: number): Promise<any> {
  return one(
    `UPDATE player_link_verifications
     SET locked_until = now() + make_interval(mins => $2),
         expires_at = GREATEST(expires_at, now() + make_interval(mins => $2))
     WHERE user_id = $1
     RETURNING locked_until`,
    [userId, PLAYER_LINK_LOCKOUT_MINUTES],
  );
}

async function createSession(userId: number) {
  const token = crypto.randomBytes(32).toString('hex');
  const tokenHash = hashSessionToken(token);
  const expiresAt = new Date(Date.now() + SESSION_TTL_HOURS * 60 * 60 * 1000).toISOString();
  await one(`INSERT INTO sessions (user_id, token, expires_at) VALUES ($1, $2, $3)`, [userId, tokenHash, expiresAt]);
  return { token, expires_at: expiresAt };
}

export default async function authRoutes(fastify: FastifyInstance) {
  await ensureAuthSchema();

  fastify.post('/register', async (req: any, reply: any) => {
    const username = normalizeUsername(req.body?.username);
    const email = normalizeEmail(req.body?.email);
    const password = String(req.body?.password || '');

    // CRITICAL: Validate required fields. Missing fields cause DB errors or
    // store invalid data (NULL username, empty password hash).
    // Source: Debug 2026-05-31 - "No input validation"
    if (!username || !email || !password) {
      return reply.status(400).send({ error: 'Missing required fields: username, email, password' });
    }
    if (!USERNAME_RE.test(username)) {
      return reply.status(400).send({ error: 'Username must be 3-32 characters and use only letters, numbers, underscore, or dash' });
    }
    if (!isValidEmail(email)) {
      return reply.status(400).send({ error: 'Invalid email address' });
    }
    if (password.length < 6) {
      return reply.status(400).send({ error: 'Password must be at least 6 characters' });
    }

    const existing = await one(
      `SELECT username, email FROM users WHERE lower(username) = lower($1) OR lower(email) = lower($2) LIMIT 1`,
      [username, email],
    );
    if (existing) {
      const field = String(existing.username).toLowerCase() === username.toLowerCase() ? 'username' : 'email';
      return reply.status(409).send({ error: `That ${field} is already registered` });
    }

    const salt = crypto.randomBytes(16).toString('hex');
    const hash = hashPassword(password, salt);
    try {
      const user = await one(
        `INSERT INTO users (username, email, password_hash, salt, updated_at)
         VALUES ($1, $2, $3, $4, now())
        RETURNING id, username, email, avatar_url, bio, time_zone, is_admin, is_approved, created_at, last_login`,
        [username, email, hash, salt],
      );
      const session = await createSession(user.id);
      return { message: 'User registered', token: session.token, expires_at: session.expires_at, user: toAuthUser(user) };
    } catch (err) {
      if (isDuplicateKeyError(err)) {
        return reply.status(409).send({ error: 'That username or email is already registered' });
      }
      throw err;
    }
  });

  fastify.post('/login', async (req: any, reply: any) => {
    const identifier = String(req.body?.username || '').trim();
    const password = String(req.body?.password || '');

    // CRITICAL: Validate required fields.
    // Source: Debug 2026-05-31 - "No input validation"
    if (!identifier || !password) {
      return reply.status(400).send({ error: 'Missing required fields: username, password' });
    }

    // CRITICAL: Timing attack prevention. The old code returned immediately on
    // `if (!user) return`, leaking whether a username exists via response time.
    // An attacker sends requests with known-invalid passwords for candidate usernames:
    // fast response = username doesn't exist, slow response (hash computed) = exists.
    // Fix: always compute the hash, even for non-existent users. Use a dummy hash
    // that takes the same time to compare. Return 401 for both cases.
    // Source: Debug 2026-05-31 - "Timing attack on login"
    const user = await one(
      `SELECT u.id, u.username, u.email, u.password_hash, u.salt, u.avatar_url, u.bio, u.time_zone,
              u.is_admin, u.is_approved, u.created_at, u.last_login, u.linked_player_id,
              linked_player.name AS linked_player_name
       FROM users u
       LEFT JOIN players linked_player ON linked_player.id = u.linked_player_id
       WHERE lower(u.username) = lower($1) OR lower(u.email) = lower($1)
       LIMIT 1`,
      [identifier],
    );
    const storedHash = user ? user.password_hash : crypto.createHash('sha256').update('dummy_password' + 'dummy_salt').digest('hex');
    const storedSalt = user ? user.salt : 'dummy_salt';
    const hash = hashPassword(password, storedSalt);
    if (!user || hash !== storedHash) {
      return reply.status(401).send({ error: 'Invalid credentials' });
    }

    // CRITICAL: Token stored as plain text in DB. If the DB is compromised,
    // all tokens are immediately usable. Hash the token before storing.
    // Source: Debug 2026-05-31 - "Plain text tokens"
    const session = await createSession(user.id);
    const lastLogin = new Date().toISOString();
    await one('UPDATE users SET last_login = $2, updated_at = now() WHERE id = $1', [user.id, lastLogin]);
    return { token: session.token, expires_at: session.expires_at, user: toAuthUser({ ...user, last_login: lastLogin }) };
  });

  fastify.post('/logout', async (req: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (token) {
      // Token is stored as hash - hash the incoming token for lookup.
      const tokenHash = hashSessionToken(token);
      await one('DELETE FROM sessions WHERE token = $1', [tokenHash]);
    }
    return { message: 'Logged out' };
  });

  fastify.get('/me', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) {
      return reply.status(401).send({ error: 'No token' });
    }
    // Token is stored as hash - hash the incoming token for lookup.
    const tokenHash = hashSessionToken(token);
    const session = await one(
      `SELECT s.user_id, s.expires_at, u.username, u.email, u.avatar_url, u.bio, u.time_zone,
              u.is_admin, u.is_approved, u.linked_player_id, linked_player.name AS linked_player_name
       FROM sessions s
       JOIN users u ON u.id = s.user_id
       LEFT JOIN players linked_player ON linked_player.id = u.linked_player_id
       WHERE s.token = $1 AND s.expires_at > now()`,
      [tokenHash],
    );
    if (!session) {
      return reply.status(401).send({ error: 'Invalid session' });
    }
    return session;
  });

  fastify.put('/profile', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) {
      return reply.status(401).send({ error: 'Not authenticated' });
    }
    const tokenHash = hashSessionToken(token);
    const session = await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [tokenHash]);
    if (!session) {
      return reply.status(401).send({ error: 'Not authenticated' });
    }
    const body = req.body ?? {};
    const timeZone = body.time_zone === undefined ? undefined : String(body.time_zone).trim();
    if (timeZone !== undefined) {
      try {
        new Intl.DateTimeFormat('en-US', { timeZone });
      } catch {
        return reply.status(400).send({ error: 'Invalid time zone' });
      }
    }
    await one(
      `UPDATE users
       SET avatar_url = CASE WHEN $1 THEN $2 ELSE avatar_url END,
           bio = CASE WHEN $3 THEN $4 ELSE bio END,
           time_zone = CASE WHEN $5 THEN $6 ELSE time_zone END,
           updated_at = now()
       WHERE id = $7`,
      [body.avatar_url !== undefined, body.avatar_url ?? null, body.bio !== undefined, body.bio ?? null, timeZone !== undefined, timeZone ?? null, session.user_id],
    );
    return { message: 'Profile updated' };
  });

  // ── Get full account details (including linked_player_id) ──
  fastify.get('/account', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) {
      return reply.status(401).send({ error: 'No token' });
    }
    const tokenHash = hashSessionToken(token);
    const session = await one(
      `SELECT s.user_id, s.expires_at, u.username, u.email, u.avatar_url, u.bio, u.time_zone, u.is_admin, u.is_approved, u.linked_player_id, u.created_at, u.last_login
       FROM sessions s JOIN users u ON u.id = s.user_id
       WHERE s.token = $1 AND s.expires_at > now()`,
      [tokenHash],
    );
    if (!session) {
      return reply.status(401).send({ error: 'Invalid session' });
    }
    // If linked_player_id is set, fetch player profile details
    let linkedPlayer = null;
    if (session.linked_player_id) {
      linkedPlayer = await one(
        `SELECT id, name, platform_name, level, wins, losses, kbm_tier, kbm_points, kbm_player_id, controller_player_id, conquest_player_id
         FROM players WHERE id = $1`,
        [session.linked_player_id],
      );
    }
    return {
      user: {
        id: session.user_id,
        username: session.username,
        email: session.email,
        avatar_url: session.avatar_url,
        bio: session.bio,
        time_zone: session.time_zone,
        is_admin: Boolean(session.is_admin),
        is_approved: Boolean(session.is_approved),
        linked_player_id: session.linked_player_id,
        created_at: session.created_at,
        last_login: session.last_login,
      },
      linkedPlayer,
    };
  });

  // ── Account-scoped notifications ──
  fastify.get('/account/notifications', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const session = await one<{ user_id: number }>(
      'SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()',
      [hashSessionToken(token)],
    );
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });
    const limit = Math.min(Math.max(parseInt(req.query.limit as string, 10) || 25, 1), 100);
    const rows = await query(
      `SELECT
         notification.id,
         notification.type,
         notification.post_id,
         notification.comment_id,
         notification.read_at,
         notification.created_at,
         COALESCE(actor.username, 'A community member') AS actor_username,
         post.title AS post_title,
         comment.content AS comment_content
       FROM user_notifications notification
       LEFT JOIN users actor ON actor.id = notification.actor_user_id
       LEFT JOIN posts post ON post.id = notification.post_id
       LEFT JOIN comments comment ON comment.id = notification.comment_id
       WHERE notification.user_id = $1
       ORDER BY notification.read_at NULLS FIRST, notification.created_at DESC
       LIMIT $2`,
      [session.user_id, limit],
    );
    return { data: rows };
  });

  fastify.post('/account/notifications/:id/read', async (req: any, reply: any) => {
    const notificationId = Number(req.params.id);
    if (!Number.isSafeInteger(notificationId) || notificationId <= 0) {
      return reply.status(400).send({ error: 'Invalid notification id' });
    }
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const session = await one<{ user_id: number }>(
      'SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()',
      [hashSessionToken(token)],
    );
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });
    const updated = await one(
      `UPDATE user_notifications
       SET read_at = COALESCE(read_at, now())
       WHERE id = $1 AND user_id = $2
       RETURNING id, read_at`,
      [notificationId, session.user_id],
    );
    if (!updated) return reply.status(404).send({ error: 'Notification not found' });
    return updated;
  });

  // ── Account-scoped read state for global site notifications ──
  fastify.get('/account/site-notifications', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const session = await one<{ user_id: number }>(
      'SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()',
      [hashSessionToken(token)],
    );
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });
    const limit = Math.min(Math.max(parseInt(req.query.limit as string, 10) || 8, 1), 20);
    const rows = await query(
      `SELECT
         notification.id,
         notification.timestamp,
         notification.importance,
         notification.message,
         notification_read.read_at
       FROM notifications notification
       LEFT JOIN site_notification_reads notification_read
         ON notification_read.notification_id = notification.id
        AND notification_read.user_id = $1
       ORDER BY
         (notification_read.read_at IS NULL) DESC,
         notification.importance DESC,
         notification.timestamp DESC,
         notification.id DESC
       LIMIT $2`,
      [session.user_id, limit],
    );
    return { data: rows };
  });

  fastify.post('/account/site-notifications/:id/read', async (req: any, reply: any) => {
    const notificationId = Number(req.params.id);
    if (!Number.isSafeInteger(notificationId) || notificationId <= 0) {
      return reply.status(400).send({ error: 'Invalid notification id' });
    }
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const session = await one<{ user_id: number }>(
      'SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()',
      [hashSessionToken(token)],
    );
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });
    const notification = await one<{ id: number }>('SELECT id FROM notifications WHERE id = $1', [notificationId]);
    if (!notification) return reply.status(404).send({ error: 'Notification not found' });
    const read = await one(
      `INSERT INTO site_notification_reads (user_id, notification_id)
       VALUES ($1, $2)
       ON CONFLICT (user_id, notification_id)
       DO UPDATE SET read_at = site_notification_reads.read_at
       RETURNING notification_id AS id, read_at`,
      [session.user_id, notificationId],
    );
    return read;
  });

  fastify.post('/account/site-notifications/read-all', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const session = await one<{ user_id: number }>(
      'SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()',
      [hashSessionToken(token)],
    );
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });
    await query(
      `INSERT INTO site_notification_reads (user_id, notification_id)
       SELECT $1, notification.id
       FROM notifications notification
       ON CONFLICT (user_id, notification_id) DO NOTHING`,
      [session.user_id],
    );
    return { read: true };
  });

  // ── Link/unlink a Paladins player profile ──
  fastify.post('/account/player-link', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) {
      return reply.status(401).send({ error: 'Not authenticated' });
    }
    const tokenHash = hashSessionToken(token);
    const session = await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [tokenHash]);
    if (!session) {
      return reply.status(401).send({ error: 'Not authenticated' });
    }

    const action = req.body?.action; // 'link' or 'unlink'
    if (action === 'unlink') {
      await one('UPDATE users SET linked_player_id = NULL, updated_at = now() WHERE id = $1', [session.user_id]);
      return { message: 'Player link removed' };
    }

    return reply.status(400).send({ error: 'Use loadout verification to link a player.' });
  });

  // ── Loadout-code player ownership verification ──
  fastify.get('/account/player-link/verification', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const tokenHash = hashSessionToken(token);
    const session = await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [tokenHash]);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    await query('DELETE FROM player_link_verifications WHERE expires_at <= now()');
    const verification = await one(
      `SELECT verification.player_id, verification.code, verification.expires_at, player.name AS player_name
       FROM player_link_verifications verification
       JOIN players player ON player.id = verification.player_id
       WHERE verification.user_id = $1`,
      [session.user_id],
    );
    return { verification: verification ? {
      player: { id: verification.player_id, name: verification.player_name },
      code: verification.code,
      expiresAt: verification.expires_at,
    } : null };
  });

  fastify.post('/account/player-link/verification', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const tokenHash = hashSessionToken(token);
    const session = await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [tokenHash]);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    const lockedVerification = await one(
      'SELECT locked_until FROM player_link_verifications WHERE user_id = $1 AND locked_until > now()',
      [session.user_id],
    );
    if (lockedVerification) {
      return reply.status(429).send({
        error: `Too many verification attempts. Try again after ${new Date(lockedVerification.locked_until).toISOString()}.`,
        retry_at: lockedVerification.locked_until,
      });
    }

    const playerId = Number(req.body?.playerId);
    if (!Number.isSafeInteger(playerId) || playerId <= 0) {
      return reply.status(400).send({ error: 'Valid player ID required' });
    }
    const player = await one('SELECT id, name FROM players WHERE id = $1', [playerId]);
    if (!player) return reply.status(404).send({ error: 'Player not found. Search for the player name first.' });

    const owner = await one('SELECT username FROM users WHERE linked_player_id = $1 AND id != $2', [playerId, session.user_id]);
    if (owner) return reply.status(409).send({ error: 'This player is already linked to another account.' });

    // A short numeric PIN is much easier to enter as a temporary loadout name.
    // Retry an extremely unlikely collision with another active verification.
    let verification: any = null;
    for (let attempt = 0; attempt < 5; attempt++) {
      const code = crypto.randomInt(100_000, 1_000_000).toString();
      try {
        verification = await one(
          `INSERT INTO player_link_verifications (user_id, player_id, code, expires_at)
           VALUES ($1, $2, $3, now() + interval '10 minutes')
           ON CONFLICT (user_id) DO UPDATE SET
             player_id = EXCLUDED.player_id,
             code = EXCLUDED.code,
             expires_at = EXCLUDED.expires_at,
             attempt_count = 0,
             last_attempt_at = NULL,
             next_attempt_at = NULL,
             locked_until = NULL,
             created_at = now()
           RETURNING code, expires_at`,
          [session.user_id, playerId, code],
        );
        break;
      } catch (error) {
        if (!isDuplicateKeyError(error) || attempt === 4) throw error;
      }
    }
    return {
      verification: {
        player: { id: player.id, name: player.name },
        code: verification.code,
        expiresAt: verification.expires_at,
      },
    };
  });

  fastify.post('/account/player-link/verification/check', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const tokenHash = hashSessionToken(token);
    const session = await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [tokenHash]);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });

    // Claim an attempt before calling Hi-Rez. This prevents concurrent clicks
    // or requests from burning multiple vendor calls for the same PIN check.
    const verification = await one(
      `UPDATE player_link_verifications verification
       SET attempt_count = verification.attempt_count + 1,
           last_attempt_at = now(),
           next_attempt_at = now() + make_interval(secs => $2)
       FROM players player
       WHERE verification.user_id = $1
         AND player.id = verification.player_id
         AND verification.expires_at > now()
         AND verification.attempt_count < $3
         AND (verification.next_attempt_at IS NULL OR verification.next_attempt_at <= now())
         AND (verification.locked_until IS NULL OR verification.locked_until <= now())
       RETURNING verification.player_id, verification.code, verification.expires_at,
                 verification.attempt_count, verification.next_attempt_at, player.name AS player_name`,
      [session.user_id, PLAYER_LINK_ATTEMPT_COOLDOWN_SECONDS, PLAYER_LINK_MAX_ATTEMPTS],
    );

    if (!verification) {
      const state = await one(
        `SELECT expires_at, attempt_count, next_attempt_at, locked_until
         FROM player_link_verifications WHERE user_id = $1`,
        [session.user_id],
      );
      if (!state) return reply.status(404).send({ error: 'No active player-link verification.' });
      if (new Date(state.expires_at).getTime() <= Date.now()) {
        await one('DELETE FROM player_link_verifications WHERE user_id = $1', [session.user_id]);
        return reply.status(410).send({ error: 'Verification code expired. Generate a new one.' });
      }
      if (state.locked_until && new Date(state.locked_until).getTime() > Date.now()) {
        return reply.status(429).send({ error: `Maximum verification attempts reached. Try again after ${new Date(state.locked_until).toISOString()}.`, retry_at: state.locked_until });
      }
      if (state.next_attempt_at && new Date(state.next_attempt_at).getTime() > Date.now()) {
        return reply.status(429).send({ error: `Please wait before checking again. Next check is available after ${new Date(state.next_attempt_at).toISOString()}.`, retry_at: state.next_attempt_at });
      }
      return reply.status(429).send({ error: 'Maximum verification attempts reached. Generate a new code later.' });
    }

    let loadouts: any[];
    try {
      // This is intentionally a fresh direct Hi-Rez request, rather than the
      // stored loadout cache: the player may have just renamed their loadout.
      await guardVendorFallback(req, reply, {
        scope: 'player-link-verification',
        entity: verification.player_id,
        entityWindowMs: PLAYER_LINK_ATTEMPT_COOLDOWN_SECONDS * 1000,
      });
      loadouts = await getPlayerLoadouts(Number(verification.player_id), 'account_verification');
      await recordRawHirezResponse({
        endpoint: 'getplayerloadouts',
        operation: 'player-link-verification',
        entityType: 'player_loadout',
        entityId: Number(verification.player_id),
        params: { playerId: Number(verification.player_id), reason: 'player_link_verification' },
        rawResponse: loadouts,
        source: 'player-link-verification',
      });
    } catch (error) {
      if (error instanceof RequestSecurityError) throw error;
      if (Number(verification.attempt_count) >= PLAYER_LINK_MAX_ATTEMPTS) {
        const locked = await lockPlayerLinkVerification(session.user_id);
        return reply.status(429).send({ error: `The verification attempt limit was reached. Try again after ${new Date(locked.locked_until).toISOString()}.`, retry_at: locked.locked_until });
      }
      return reply.status(502).send({ error: 'Hi-Rez could not refresh this player\'s loadouts. Please wait for the cooldown and try again.' });
    }

    const verified = Array.isArray(loadouts) && loadouts.some((loadout: any) =>
      String(loadout.DeckName ?? loadout.deck_name ?? '').trim().toUpperCase() === String(verification.code).toUpperCase(),
    );
    if (!verified) {
      if (Number(verification.attempt_count) >= PLAYER_LINK_MAX_ATTEMPTS) {
        const locked = await lockPlayerLinkVerification(session.user_id);
        return reply.status(429).send({ error: `The verification attempt limit was reached. Try again after ${new Date(locked.locked_until).toISOString()}.`, retry_at: locked.locked_until });
      }
      const attemptsRemaining = PLAYER_LINK_MAX_ATTEMPTS - Number(verification.attempt_count);
      return reply.status(409).send({ error: `Code not found in this player\'s freshly refreshed loadouts. Save the renamed loadout, wait ${PLAYER_LINK_ATTEMPT_COOLDOWN_SECONDS} seconds, then try again. ${attemptsRemaining} attempt${attemptsRemaining === 1 ? '' : 's'} remaining.` });
    }

    const owner = await one('SELECT username FROM users WHERE linked_player_id = $1 AND id != $2', [verification.player_id, session.user_id]);
    if (owner) return reply.status(409).send({ error: 'This player is already linked to another account.' });

    await one('UPDATE users SET linked_player_id = $1, updated_at = now() WHERE id = $2', [verification.player_id, session.user_id]);
    await one('DELETE FROM player_link_verifications WHERE user_id = $1', [session.user_id]);
    return { message: 'Player linked', player: { id: verification.player_id, name: verification.player_name } };
  });

  fastify.delete('/account/player-link/verification', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) return reply.status(401).send({ error: 'No token' });
    const tokenHash = hashSessionToken(token);
    const session = await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [tokenHash]);
    if (!session) return reply.status(401).send({ error: 'Not authenticated' });
    await one('DELETE FROM player_link_verifications WHERE user_id = $1', [session.user_id]);
    return { message: 'Verification cancelled' };
  });

  // ── Change password ──
  fastify.post('/account/password', async (req: any, reply: any) => {
    const token = req.headers.authorization?.replace('Bearer ', '');
    if (!token) {
      return reply.status(401).send({ error: 'Not authenticated' });
    }
    const tokenHash = hashSessionToken(token);
    const session = await one('SELECT user_id FROM sessions WHERE token = $1 AND expires_at > now()', [tokenHash]);
    if (!session) {
      return reply.status(401).send({ error: 'Not authenticated' });
    }

    const currentPassword = String(req.body?.currentPassword || '');
    const newPassword = String(req.body?.newPassword || '');

    if (!currentPassword || !newPassword) {
      return reply.status(400).send({ error: 'Both current and new password required' });
    }
    if (newPassword.length < 6) {
      return reply.status(400).send({ error: 'Password must be at least 6 characters' });
    }

    // Verify current password
    const user = await one('SELECT id, password_hash, salt FROM users WHERE id = $1', [session.user_id]);
    if (!user || hashPassword(currentPassword, user.salt) !== user.password_hash) {
      return reply.status(403).send({ error: 'Current password is incorrect' });
    }

    // Set new password with new salt (prevent reuse attack)
    const newSalt = crypto.randomBytes(16).toString('hex');
    const newHash = hashPassword(newPassword, newSalt);
    await one('UPDATE users SET password_hash = $1, salt = $2, updated_at = now() WHERE id = $3', [newHash, newSalt, user.id]);
    return { message: 'Password changed successfully' };
  });
}
