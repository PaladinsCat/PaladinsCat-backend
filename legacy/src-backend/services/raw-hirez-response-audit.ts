/**
 * Durable raw Hi-Rez response audit storage.
 *
 * raw_ingest_buffer is intentionally a short-lived work queue: processed rows
 * are pruned so the buffer does not become the next bottleneck. Operator raw
 * pass-through endpoints have a different contract. When an endpoint is used
 * to inspect "what Hi-Rez actually sent", the exact payload must remain
 * queryable after the staging queue has drained. This helper writes that
 * permanent copy and returns the audit id before the HTTP route responds.
 */
import { createHash } from 'crypto';
import type { PoolClient } from 'pg';
import { query } from '../config/db';
import { jsonForPostgresJsonb } from '../utils/postgres-json';

export interface RawHirezAuditInput {
  endpoint: string;
  operation: string;
  entityType: string;
  entityId?: string | number | null;
  params?: Record<string, unknown> | unknown[];
  rawResponse: unknown;
  statusCode?: number;
  success?: boolean;
  errorMessage?: string | null;
  source?: string;
}

export interface RawHirezAuditRecord {
  id: string;
  response_sha256: string;
  response_shape: string;
  response_count: number | null;
  created_at: string;
}

function responseShape(rawResponse: unknown): string {
  if (Array.isArray(rawResponse)) return 'array';
  if (rawResponse === null) return 'null';
  return typeof rawResponse;
}

function responseCount(rawResponse: unknown): number | null {
  if (Array.isArray(rawResponse)) return rawResponse.length;
  if (rawResponse && typeof rawResponse === 'object') return 1;
  return null;
}

export async function recordRawHirezResponse(
  input: RawHirezAuditInput,
  client?: PoolClient,
): Promise<RawHirezAuditRecord> {
  const rawResponseText = jsonForPostgresJsonb(input.rawResponse);
  const responseSha256 = createHash('sha256').update(rawResponseText).digest('hex');
  const sql = `
      INSERT INTO hirez_raw_api_responses (
        endpoint,
        operation,
        entity_type,
        entity_id,
        params,
        raw_response,
        raw_response_text,
        response_sha256,
        response_shape,
        response_count,
        status_code,
        success,
        error_message,
        source
      )
      VALUES (
        $1, $2, $3, $4,
        $5::jsonb,
        $6::jsonb,
        $7,
        $8,
        $9,
        $10,
        $11,
        $12,
        $13,
        $14
      )
      RETURNING id::text, response_sha256, response_shape, response_count, created_at::text
    `;
  const params = [
    input.endpoint,
    input.operation,
    input.entityType,
    input.entityId == null ? null : String(input.entityId),
    jsonForPostgresJsonb(input.params ?? {}),
    rawResponseText,
    rawResponseText,
    responseSha256,
    responseShape(input.rawResponse),
    responseCount(input.rawResponse),
    input.statusCode ?? 200,
    input.success ?? true,
    input.errorMessage ?? null,
    input.source ?? 'paladinscat-api-raw-pass-through',
  ];
  const rows = client
    ? (await client.query<RawHirezAuditRecord>(sql, params)).rows
    : await query<RawHirezAuditRecord>(sql, params);

  if (!rows[0]) {
    throw new Error('Failed to persist raw Hi-Rez API response audit row');
  }
  return rows[0];
}
