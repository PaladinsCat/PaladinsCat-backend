/**
 * Encode a value for a PostgreSQL JSON/JSONB parameter.
 *
 * PostgreSQL rejects U+0000 even when JSON.stringify emits it as "\u0000".
 * Removing actual NUL characters before encoding keeps literal backslash-u
 * text intact and prevents one vendor string from aborting a whole batch.
 */
export function jsonForPostgresJsonb(value: unknown): string {
  return JSON.stringify(value ?? null, (_key, nested) => (
    typeof nested === 'string' ? nested.replace(/\u0000/g, '') : nested
  ));
}
