import crypto from 'node:crypto';
import { query, one } from '../config/db';

/**
 * SQL expression for resolving the best player display name.
 *
 * Priority matches the normalizer: hz_player_name > hz_gamer_tag > name,
 * but every candidate is screened for values that are known to be transport
 * artifacts rather than real player-facing names.
 *
 * Why this exists:
 * - Epic-linked profile `Name` can be an obfuscated platform identity like
 *   `70f0...User-1bb...`.
 * - HirezRelay dummy mode returns profile-like rows named `DummyPlayer####`.
 *   Those rows are useful in dummy-mode tests, but if they ever land in a live
 *   database they must be treated as contaminated audit data, not display data.
 *
 * The API routes use this expression for leaderboards, ratings, esports teams,
 * and champion pages. Keeping the guard here prevents every route from needing
 * its own "do not show dummy/mock names" patch.
 *
 * Usage: `SELECT ${DISPLAY_NAME_SQL} AS player_name FROM players p ...`
 */
export const DISPLAY_NAME_SQL = `COALESCE(
  CASE
    WHEN NULLIF(p.hz_player_name, '') IS NOT NULL
      AND p.hz_player_name !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.hz_player_name
  END,
  CASE
    WHEN NULLIF(p.hz_gamer_tag, '') IS NOT NULL
      AND p.hz_gamer_tag !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.hz_gamer_tag
  END,
  CASE
    WHEN NULLIF(p.name, '') IS NOT NULL
      AND p.name !~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
    THEN p.name
  END,
  'Player ' || p.id::text
)`;

/**
 * Standard error envelope for API responses.
 * Usage: reply.status(code).send(error(code, message, details));
 */
export function err(code: string, message: string, details?: Record<string, unknown>) {
  return { error: { code, message, ...(details ? { details } : {}) } };
}

/**
 * Standard success envelope for list endpoints.
 */
export function list<T>(data: T[], total: number, page: number, perPage: number) {
  const totalPages = Math.ceil(total / perPage);
  return { data, total, page: { current: page, size: perPage, totalPages } };
}

/**
 * Paginated query builder.
 * Returns { rows, total } with LIMIT/OFFSET applied.
 * Defaults: page=1, perPage=20, max perPage=100.
 */
export interface PaginationParams {
  page?: number | string;
  perPage?: number | string;
}

export function paginate(params: PaginationParams) {
  const page = Math.max(1, parseInt(String(params.page ?? '1'), 10) || 1);
  const perPage = Math.min(100, Math.max(1, parseInt(String(params.perPage ?? '20'), 10) || 20));
  return { page, perPage, offset: (page - 1) * perPage };
}

/**
 * Apply LIMIT/OFFSET to a query string.
 * Accepts the existing param count to avoid fragile $ counting.
 * The old sql.split('$').length approach broke on string literals
 * containing $ (e.g., LIKE patterns with '$test').
 * Source: Fault #9 — "Fragile $ counting in applyPagination"
 */
export function applyPagination(sql: string, { offset, perPage, paramCount = 0 }: { offset: number; perPage: number; paramCount?: number }) {
  const limitIdx = paramCount + 1;
  const offsetIdx = paramCount + 2;
  return `${sql} LIMIT $${limitIdx} OFFSET $${offsetIdx}`;
}

/**
 * Apply COUNT wrapper to a query string for total row count.
 * Only accepts SELECT queries — rejects mutations and injection payloads.
 * Source: Fault #10 — "No query validation on countQuery"
 */
export function countQuery(sql: string) {
  const trimmed = sql.trim();
  if (!trimmed.toUpperCase().startsWith('SELECT')) {
    throw new Error('countQuery only accepts SELECT queries');
  }
  return `SELECT COUNT(*) as total FROM (${sql}) _count`;
}

/**
 * Field selection: wrap SELECT with specific columns.
 * Only works if the query starts with 'SELECT'.
 * Returns the modified query string.
 */
export function selectFields(sql: string, fields?: string) {
  if (!fields) return sql;
  const cols = fields.split(',').map((f: string) => f.trim()).filter(Boolean);
  if (cols.length === 0) return sql;
  // Replace 'SELECT ... FROM' with 'SELECT col1, col2 FROM'
  const match = sql.match(/^(SELECT\s+)(.+?)(\s+FROM\s+)/i);
  if (!match) return sql; // can't parse, return as-is
  return `${match[1]}${cols.join(', ')}${match[3]}`;
}

/**
 * Date range filter: parses ?from= and ?to= as ISO8601.
 * Returns { clauses: string[], params: any[] } to append to query.
 */
export interface DateRangeParams {
  from?: string;
  to?: string;
}

export function dateRange(params: DateRangeParams) {
  const clauses: string[] = [];
  const paramsList: any[] = [];

  if (params.from) {
    const d = new Date(params.from);
    if (!isNaN(d.getTime())) {
      clauses.push('>= $1');
      paramsList.push(d);
    }
  }
  if (params.to) {
    const d = new Date(params.to);
    if (!isNaN(d.getTime())) {
      clauses.push('<= $1');
      paramsList.push(d);
    }
  }

  return {
    clause: clauses.length > 0 ? ' AND ' + clauses.join(' AND ') : '',
    params: paramsList,
  };
}

/**
 * Sorting: parses ?sort= and ?order= with validation.
 * Returns ORDER BY clause string and param offset.
 * Fixed 2026-05-31: validates order against whitelist ['asc', 'desc']
 * instead of accepting any non-'asc' value as DESC.
 * Source: Fault #11 — "Unvalidated order parameter"
 */
export function sorting(sort?: string, order?: string, allowedFields?: string[]) {
  if (!sort || (allowedFields && !allowedFields.includes(sort))) return '';
  const dir = order === 'asc' ? 'ASC' : order === 'desc' ? 'DESC' : 'DESC';
  return ` ORDER BY ${sort} ${dir}`;
}

/**
 * Bulk ID lookup: parses ?ids= comma-separated string.
 * Returns array of integers (max 50).
 */
export function bulkIds(ids?: string, max: number = 50) {
  if (!ids) return [];
  return ids
    .split(',')
    .map((s: string) => parseInt(s.trim(), 10))
    .filter((n: number) => !isNaN(n) && n > 0)
    .slice(0, max);
}

/**
 * Run a paginated query: executes both the data query and count query.
 * Returns the standard list envelope.
 */
export async function runPaginated<T>(
  dataSql: string,
  dataParams: any[],
  { page, perPage, offset }: { page: number; perPage: number; offset: number }
): Promise<{ data: T[]; total: number; page: { current: number; size: number; totalPages: number } }> {
  const countSql = countQuery(dataSql);
  const [dataRows, countRow] = await Promise.all([
    query<T>(applyPagination(dataSql, { offset, perPage, paramCount: dataParams.length }), dataParams),
    one<{ total: number }>(countSql, dataParams),
  ]);
  return list(dataRows, countRow?.total ?? 0, page, perPage);
}

/**
 * Check if a request has valid Bearer token (admin routes).
 * Compares token against ADMIN_SECRET env var using constant-time comparison.
 * Returns { authorized: true } or throws UNAUTHORIZED.
 *
 * Fixed 2026-05-31: replaced short-circuit string comparison (token !== adminSecret)
 * with crypto.timingSafeEqual(). The old code allowed byte-by-byte timing attacks:
 * attacker sends candidate tokens and measures response time — longer time = more
 * matching prefix bytes. The secret could be brute-forced in ~64 hex chars × 256
 * guesses. timingSafeEqual compares all bytes regardless of mismatch position.
 * Source: Fault #8 — "Timing attack on requireAuth"
 */
export async function requireAuth(req: any) {
  const auth = req.headers?.authorization;
  if (!auth || !auth.startsWith('Bearer ')) {
    throw new Error('UNAUTHORIZED');
  }
  const token = auth.slice(7);
  const adminSecret = process.env.ADMIN_SECRET;
  if (!adminSecret) {
    throw new Error('UNAUTHORIZED');
  }
  // Constant-time comparison prevents timing attacks
  const tokenBuf = Buffer.from(token);
  const secretBuf = Buffer.from(adminSecret);
  if (tokenBuf.length !== secretBuf.length || !crypto.timingSafeEqual(tokenBuf, secretBuf)) {
    throw new Error('UNAUTHORIZED');
  }
  return { authorized: true };
}

/**
 * Check if a request has a valid user session with admin privileges.
 * Looks up the Bearer token as a hashed session, joins to users, and verifies is_admin.
 * Returns { authorized: true, user } or throws UNAUTHORIZED.
 */
export async function requireAdminSession(req: any) {
  const auth = req.headers?.authorization;
  if (!auth || !auth.startsWith('Bearer ')) {
    throw new Error('UNAUTHORIZED');
  }
  const token = auth.slice(7);
  const tokenHash = crypto.createHash('sha256').update(token).digest('hex');
  const session = await one(
    `SELECT s.user_id, u.username, u.email, u.is_admin, u.is_approved
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token = $1 AND s.expires_at > now()`,
    [tokenHash]
  );
  if (!session || !session.is_admin) {
    throw new Error('UNAUTHORIZED');
  }
  return { authorized: true, user: { id: session.user_id, username: session.username, email: session.email, is_approved: session.is_approved } };
}

/** Resolve an authenticated account without granting any administrative role. */
export async function requireUserSession(req: any) {
  const auth = req.headers?.authorization;
  if (!auth || !auth.startsWith('Bearer ')) throw new Error('UNAUTHORIZED');
  const tokenHash = crypto.createHash('sha256').update(auth.slice(7)).digest('hex');
  const session = await one(
    `SELECT s.user_id, u.username, u.email, u.is_admin, u.is_approved, u.is_active
     FROM sessions s
     JOIN users u ON u.id = s.user_id
     WHERE s.token = $1 AND s.expires_at > now()`,
    [tokenHash],
  );
  if (!session || session.is_active === false) throw new Error('UNAUTHORIZED');
  return {
    user: {
      id: session.user_id,
      username: session.username,
      email: session.email,
      isAdmin: Boolean(session.is_admin),
      isApproved: Boolean(session.is_approved),
    },
  };
}

/**
 * Parse common query params from a Fastify request.
 * Returns { pagination, dateRange, sort, fields, ids }.
 */
export function parseQuery(req: any) {
  const pagination = paginate({ page: req.query.page, perPage: req.query.perPage });
  const dateRangeParsed = dateRange({ from: req.query.from, to: req.query.to });
  const sort = req.query.sort as string | undefined;
  const order = req.query.order as string | undefined;
  const fields = req.query.fields as string | undefined;
  const ids = bulkIds(req.query.ids as string | undefined);
  return { pagination, dateRange: dateRangeParsed, sort, order, fields, ids };
}
