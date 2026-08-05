/**
 * Dynamic WHERE clause builder for SQL queries.
 * Adds conditions with parameterized values to prevent SQL injection.
 * Usage:
 *   const fb = new FilterBuilder();
 *   fb.eq('queue_id', queueId);
 *   fb.eq('region', region);
 *   fb.gte('entry_datetime', new Date(from));
 *   fb.lte('entry_datetime', new Date(to));
 *   fb.like('name', `%${name}%`);
 *   fb.in('match_id', [1,2,3]);
 *   fb.gt('phi', 0);
 *   fb.notNull('player_id');
 *   fb.notNull('champion_id');
 *
 *   const { clause, params } = fb.build();
 *   // clause: " WHERE queue_id = $1 AND region = $2 AND ..."
 *   // params: [queueId, region, ...]
 */
/**
 * Validate a column name against the SQL identifier whitelist.
 * Allows: alphanumeric, underscore, dot (for table prefixes like "m.queue_id").
 * Rejects everything else to prevent SQL injection via column parameter.
 * Source: Fault #5 — "Column name SQL injection"
 */
function validColumn(column: string): boolean {
  return /^[a-zA-Z_][a-zA-Z0-9_.]*$/.test(column);
}

export class FilterBuilder {
  private conditions: string[] = [];
  private params: any[] = [];
  private paramIndex = 0;

  private nextParam(value: any): number {
    this.params.push(value);
    this.paramIndex++;
    return this.paramIndex;
  }

  /**
   * Internal: add a condition after validating the column name.
   * All comparison methods (eq, ne, gt, etc.) call this instead of
   * interpolating column names directly.
   * Source: Fault #5 — "Column name SQL injection"
   */
  private addCondition(column: string, condition: string): this {
    if (!validColumn(column)) {
      throw new Error(`Invalid column name: ${column}`);
    }
    this.conditions.push(condition);
    return this;
  }

  eq(column: string, value: any): this {
    if (value !== undefined && value !== null) {
      const idx = this.nextParam(value);
      this.addCondition(column, `${column} = $${idx}`);
    }
    return this;
  }

  ne(column: string, value: any): this {
    if (value !== undefined && value !== null) {
      const idx = this.nextParam(value);
      this.addCondition(column, `${column} != $${idx}`);
    }
    return this;
  }

  gt(column: string, value: any): this {
    if (value !== undefined && value !== null) {
      const idx = this.nextParam(value);
      this.addCondition(column, `${column} > $${idx}`);
    }
    return this;
  }

  gte(column: string, value: any): this {
    if (value !== undefined && value !== null) {
      const idx = this.nextParam(value);
      this.addCondition(column, `${column} >= $${idx}`);
    }
    return this;
  }

  lt(column: string, value: any): this {
    if (value !== undefined && value !== null) {
      const idx = this.nextParam(value);
      this.addCondition(column, `${column} < $${idx}`);
    }
    return this;
  }

  lte(column: string, value: any): this {
    if (value !== undefined && value !== null) {
      const idx = this.nextParam(value);
      this.addCondition(column, `${column} <= $${idx}`);
    }
    return this;
  }

  like(column: string, pattern: string): this {
    if (pattern !== undefined && pattern !== '') {
      const idx = this.nextParam(pattern);
      this.addCondition(column, `${column} ILIKE $${idx}`);
    }
    return this;
  }

  in(column: string, values: any[]): this {
    if (values && values.length > 0) {
      const idx = this.nextParam(values);
      this.addCondition(column, `${column} = ANY($${idx})`);
    }
    return this;
  }

  notIn(column: string, values: any[]): this {
    if (values && values.length > 0) {
      const idx = this.nextParam(values);
      this.addCondition(column, `${column} != ALL($${idx})`);
    }
    return this;
  }

  notNull(column: string): this {
    this.addCondition(column, `${column} IS NOT NULL`);
    return this;
  }

  isNull(column: string): this {
    this.addCondition(column, `${column} IS NULL`);
    return this;
  }

  range(column: string, min?: any, max?: any): this {
    this.gte(column, min).lte(column, max);
    return this;
  }

  /**
   * Build the WHERE clause and params array.
   * Returns { clause: string, params: any[] }.
   * clause includes " WHERE " prefix only if conditions exist.
   */
  build(): { clause: string; params: any[] } {
    const clause = this.conditions.length > 0
      ? ` WHERE ${this.conditions.join(' AND ')}`
      : '';
    return { clause, params: this.params };
  }

  /**
   * Merge conditions from another FilterBuilder.
   * Increments param index so $ references stay valid.
   */
  merge(other: FilterBuilder): this {
    if (other.conditions.length === 0) return this;
    // Re-index the other builder's conditions
    const offset = this.paramIndex;
    for (const cond of other.conditions) {
      const reindexed = cond.replace(/\$(\d+)/g, (_, num) => `$${parseInt(num) + offset}`);
      this.conditions.push(reindexed);
    }
    this.params.push(...other.params);
    this.paramIndex += other.params.length;
    return this;
  }

  /**
   * Reset the builder (for reuse).
   */
  reset(): this {
    this.conditions = [];
    this.params = [];
    this.paramIndex = 0;
    return this;
  }

  /**
   * Check if any conditions have been added.
   */
  hasConditions(): boolean {
    return this.conditions.length > 0;
  }
}

/**
 * Shorthand: create a FilterBuilder and immediately apply date range.
 */
export function dateRangeFilter(column: string = 'entry_datetime', from?: string, to?: string) {
  const fb = new FilterBuilder();
  if (from) {
    const d = new Date(from);
    if (!isNaN(d.getTime())) fb.gte(column, d);
  }
  if (to) {
    const d = new Date(to);
    if (!isNaN(d.getTime())) fb.lte(column, d);
  }
  return fb;
}

/**
 * Escape SQL LIKE wildcards in user input.
 * Escapes % and _ so they match literally rather than as wildcards.
 * Source: Fault #7 — "LIKE injection via user input"
 */
function escapeLike(s: string): string {
  return s.replace(/\\/g, '\\\\').replace(/%/g, '\\%').replace(/_/g, '\\_');
}

/**
 * Safely parse an integer query param — returns undefined if NaN.
 * Prevents NaN from being passed to FilterBuilder which produces
 * silent zero results (column >= NaN is always false).
 * Source: Fault #6 — "NaN from unvalidated parseInt"
 */
function safeInt(s: string | undefined): number | undefined {
  if (!s) return undefined;
  const n = parseInt(s, 10);
  return isNaN(n) ? undefined : n;
}

/**
 * Safely parse a float query param — returns undefined if NaN.
 */
function safeFloat(s: string | undefined): number | undefined {
  if (!s) return undefined;
  const n = parseFloat(s);
  return isNaN(n) ? undefined : n;
}

/**
 * Shorthand: create a FilterBuilder for common player filters.
 */
export function playerFilter(req: any) {
  const fb = new FilterBuilder();
  if (req.query.region) fb.eq('region', req.query.region);
  if (req.query.platform) fb.eq('platform', req.query.platform);
  if (req.query.tierMin) fb.gte('kbm_tier', safeInt(req.query.tierMin));
  if (req.query.tierMax) fb.lte('kbm_tier', safeInt(req.query.tierMax));
  if (req.query.cheater === 'true') fb.eq('cheater', true);
  if (req.query.cheater === 'false') fb.eq('cheater', false);
  if (req.query.q) fb.like('name', `%${escapeLike(req.query.q)}%`);
  return fb;
}

/**
 * Shorthand: create a FilterBuilder for common match filters.
 */
export function matchFilter(req: any) {
  const fb = new FilterBuilder();
  if (req.query.queueId) fb.eq('m.queue_id', safeInt(req.query.queueId));
  if (req.query.region) fb.eq('m.region', req.query.region);
  if (req.query.championId) fb.eq('mp.champion_id', safeInt(req.query.championId));
  if (req.query.winStatus) fb.eq('mp.win_status', req.query.winStatus);
  if (req.query.afkMax) fb.lte('ai.afk_score', safeFloat(req.query.afkMax));
  if (req.query.minPlayers) fb.gte('_player_count', safeInt(req.query.minPlayers));
  if (req.query.from) fb.gte('m.entry_datetime', new Date(req.query.from));
  if (req.query.to) fb.lte('m.entry_datetime', new Date(req.query.to));
  return fb;
}
