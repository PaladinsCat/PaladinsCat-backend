interface TracePoint {
  requestId: string;
  operation: string;
  mode: string;
  timestamp: string;
  payload: unknown;
  response?: unknown;
  latencyMs?: number;
  error?: string;
}

interface MetricPoint {
  count: number;
  errors: number;
  totalLatencyMs: number;
  lastCalledAt?: string;
}

const traces: TracePoint[] = [];
const metrics = new Map<string, MetricPoint>();
const TRACE_LIMIT = Number(process.env.HIREZ_RELAY_TRACE_LIMIT || 250);
const TRACE_ARRAY_SAMPLE_LIMIT = Math.max(
  0,
  Number(process.env.HIREZ_RELAY_TRACE_ARRAY_SAMPLE_LIMIT || 3),
);

function summarizeArray(items: unknown[]): unknown {
  const sample = items.slice(0, TRACE_ARRAY_SAMPLE_LIMIT).map(item => summarizeForTrace(item, 1));
  return {
    type: 'array',
    count: items.length,
    sample,
  };
}

function summarizeObject(value: Record<string, unknown>, depth: number): unknown {
  const keys = Object.keys(value);
  const summary: Record<string, unknown> = {
    type: 'object',
    keys,
  };

  // Keep the identifiers and sizes that matter for debugging the ingest flow,
  // but avoid embedding full Hi-Rez player/match payloads in every trace line.
  // The full payload is already persisted where required (`raw_ingest_buffer`
  // for queued ingest or `hirez_raw_api_responses` for explicit raw endpoints).
  for (const key of [
    'operation',
    'requestId',
    'endpoint',
    'entity_type',
    'entity_id',
    'match_id',
    'player_id',
    'queue_id',
    'date',
    'hour',
    'error',
    'errorCode',
    'ok',
    'mode',
    'latencyMs',
  ]) {
    if (key in value) summary[key] = summarizeForTrace(value[key], depth + 1);
  }

  for (const key of ['args', 'raw_data', 'players', 'recovered', 'payloads', 'result']) {
    const inner = value[key];
    if (Array.isArray(inner)) {
      summary[`${key}_count`] = inner.length;
      if (TRACE_ARRAY_SAMPLE_LIMIT > 0) summary[`${key}_sample`] = inner.slice(0, TRACE_ARRAY_SAMPLE_LIMIT).map(item => summarizeForTrace(item, depth + 1));
    } else if (inner && typeof inner === 'object') {
      summary[key] = summarizeForTrace(inner, depth + 1);
    }
  }

  return summary;
}

function summarizeForTrace(value: unknown, depth = 0): unknown {
  if (value == null) return value;
  if (typeof value === 'bigint') return value.toString();
  if (typeof value === 'string') {
    return value.length > 500 ? `${value.slice(0, 500)}...<truncated>` : value;
  }
  if (typeof value !== 'object') return value;
  if (Array.isArray(value)) return summarizeArray(value);
  if (depth >= 2) {
    return {
      type: 'object',
      keys: Object.keys(value as Record<string, unknown>),
    };
  }
  return summarizeObject(value as Record<string, unknown>, depth);
}

function scrub(value: unknown): unknown {
  if (value == null) return value;
  try {
    const text = JSON.stringify(summarizeForTrace(value));
    return JSON.parse(text);
  } catch (error) {
    // Observability must never be able to fail the operation it is observing.
    // Relay payloads are normally plain JSON, but circular objects, BigInt
    // values, or unexpected framework objects can throw during serialization.
    // Returning a compact placeholder preserves the request path and still
    // leaves an audit breadcrumb that tracing was degraded for this call.
    return {
      unserializable: true,
      reason: error instanceof Error ? error.message : String(error),
    };
  }
}

export function startTrace(requestId: string, operation: string, mode: string, payload: unknown): TracePoint {
  const trace = {
    requestId,
    operation,
    mode,
    timestamp: new Date().toISOString(),
    payload: scrub(payload),
  };
  traces.push(trace);
  if (traces.length > TRACE_LIMIT) traces.shift();
  console.log(`[HirezRelay] -> ${operation} requestId=${requestId} mode=${mode} payload=${JSON.stringify(trace.payload)}`);
  return trace;
}

export function finishTrace(trace: TracePoint, response: unknown, latencyMs: number): void {
  trace.response = scrub(response);
  trace.latencyMs = latencyMs;
  const metric = metrics.get(trace.operation) ?? { count: 0, errors: 0, totalLatencyMs: 0 };
  metric.count += 1;
  metric.totalLatencyMs += latencyMs;
  metric.lastCalledAt = new Date().toISOString();
  metrics.set(trace.operation, metric);
  console.log(`[HirezRelay] <- ${trace.operation} requestId=${trace.requestId} latencyMs=${latencyMs} response=${JSON.stringify(trace.response)}`);
}

export function failTrace(trace: TracePoint, error: unknown, latencyMs: number): void {
  const message = error instanceof Error ? error.message : String(error);
  trace.error = message;
  trace.latencyMs = latencyMs;
  const metric = metrics.get(trace.operation) ?? { count: 0, errors: 0, totalLatencyMs: 0 };
  metric.count += 1;
  metric.errors += 1;
  metric.totalLatencyMs += latencyMs;
  metric.lastCalledAt = new Date().toISOString();
  metrics.set(trace.operation, metric);
  console.warn(`[HirezRelay] !! ${trace.operation} requestId=${trace.requestId} latencyMs=${latencyMs} error=${message}`);
}

export function getMetrics() {
  return {
    metrics: Array.from(metrics.entries()).map(([operation, value]) => ({
      operation,
      count: value.count,
      errors: value.errors,
      avgLatencyMs: value.count > 0 ? Math.round(value.totalLatencyMs / value.count) : 0,
      lastCalledAt: value.lastCalledAt,
    })),
    traces: traces.slice(-50),
  };
}
