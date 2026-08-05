import { AsyncLocalStorage } from 'node:async_hooks';
import type { RelayCallAttribution } from '../contracts/hirez-relay';

const attributionStorage = new AsyncLocalStorage<RelayCallAttribution>();

function sanitizeConsumer(value: unknown): string {
  const normalized = String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80);
  return normalized || 'unattributed';
}

export function runWithRelayAttribution<T>(
  attribution: RelayCallAttribution | undefined,
  work: () => Promise<T>,
): Promise<T> {
  return attributionStorage.run(
    {
      consumer: sanitizeConsumer(attribution?.consumer),
      reason: attribution?.reason ? String(attribution.reason).slice(0, 160) : undefined,
    },
    work,
  );
}

export function currentRelayConsumer(): string {
  return sanitizeConsumer(attributionStorage.getStore()?.consumer);
}
