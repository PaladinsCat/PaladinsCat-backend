import manifestJson from './hirez-relay-operation-contract.json';

export type RelayContractMode = 'dummy' | 'real';

export type RelayValidationRuleKind =
  | 'boolean'
  | 'completedMatchRequests'
  | 'enum'
  | 'finiteNumber'
  | 'finiteNumberArray'
  | 'nonEmptyString'
  | 'nonEmptyStringArray'
  | 'positiveInteger'
  | 'rawPayloadArray';

export interface RelayValidationRule {
  index: number;
  kind: RelayValidationRuleKind;
  label: string;
  optional?: boolean;
  minItems?: number;
  maxItems?: number;
  valuesFrom?: 'dummyMatchScenarios';
  errorTemplate?: string;
}

export interface RelayOperationDefinition {
  name: string;
  modes: RelayContractMode[];
  kind: string;
  validation: {
    maxArgs?: number;
    maxArgsError?: string;
    rules: RelayValidationRule[];
  };
  validArgs: unknown[];
  result: string;
  upstream: string[];
  storage: string[];
  cache: string;
  maxOutboundCalls: number | string;
}

export interface RelayOperationManifest {
  schemaVersion: number;
  transport: {
    callPath: string;
    healthPath: string;
    metricsPath: string;
    validationStatus: number;
    validationErrorCode: string;
    runtimeStatus: number;
    runtimeErrorCode: string;
  };
  dummyMatchScenarios: string[];
  operations: RelayOperationDefinition[];
}

export const relayOperationManifest = manifestJson as RelayOperationManifest;

const operationsByName = new Map(
  relayOperationManifest.operations.map(operation => [operation.name, operation]),
);

export function relayOperationDefinition(operation: string): RelayOperationDefinition | undefined {
  return operationsByName.get(operation);
}

type ValidationFailure = (message: string) => never;

function isFiniteNumber(value: unknown): boolean {
  return Number.isFinite(Number(value));
}

function validateCompletedMatchRequests(
  value: unknown,
  rule: RelayValidationRule,
  fail: ValidationFailure,
): void {
  if (
    !Array.isArray(value)
    || value.length < (rule.minItems ?? 0)
    || value.length > (rule.maxItems ?? Number.MAX_SAFE_INTEGER)
  ) {
    fail(
      `${rule.label} must contain between ${rule.minItems ?? 0} and ${rule.maxItems ?? 'unbounded'} matches`,
    );
  }

  const seen = new Set<number>();
  for (const [index, request] of value.entries()) {
    if (!request || typeof request !== 'object' || Array.isArray(request)) {
      fail(`${rule.label}[${index}] must be an object`);
    }
    const record = request as Record<string, unknown>;
    const matchId = Number(record.matchId);
    if (!Number.isInteger(matchId) || matchId <= 0) {
      fail(`${rule.label}[${index}].matchId must be a positive integer`);
    }
    if (seen.has(matchId)) {
      fail(`${rule.label} contains duplicate matchId ${matchId}`);
    }
    seen.add(matchId);
    if (
      record.queueId !== undefined
      && (!Number.isInteger(Number(record.queueId)) || Number(record.queueId) <= 0)
    ) {
      fail(`${rule.label}[${index}].queueId must be a positive integer`);
    }
  }
}

function validateRawPayloadArray(value: unknown, label: string, fail: ValidationFailure): void {
  if (!Array.isArray(value)) fail(`${label} must be an array`);
  for (const [index, payload] of value.entries()) {
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
      fail(`${label}[${index}] must be an object`);
    }
    const record = payload as Record<string, unknown>;
    if (typeof record.endpoint !== 'string' || record.endpoint.trim() === '') {
      fail(`${label}[${index}].endpoint must be a non-empty string`);
    }
    if (typeof record.entity_type !== 'string' || record.entity_type.trim() === '') {
      fail(`${label}[${index}].entity_type must be a non-empty string`);
    }
    if (!Array.isArray(record.raw_data)) {
      fail(`${label}[${index}].raw_data must be an array`);
    }
  }
}

function validateRule(
  args: unknown[],
  rule: RelayValidationRule,
  fail: ValidationFailure,
): void {
  const value = args[rule.index];
  if (rule.optional && value === undefined) return;

  switch (rule.kind) {
    case 'boolean':
      if (typeof value !== 'boolean') fail(`${rule.label} must be a boolean`);
      return;
    case 'completedMatchRequests':
      validateCompletedMatchRequests(value, rule, fail);
      return;
    case 'enum': {
      const values = rule.valuesFrom === 'dummyMatchScenarios'
        ? relayOperationManifest.dummyMatchScenarios
        : [];
      if (!values.includes(String(value))) {
        fail(
          (rule.errorTemplate ?? `${rule.label} must be one of: {values}`)
            .replace('{values}', values.join(', ')),
        );
      }
      return;
    }
    case 'finiteNumber':
      if (!isFiniteNumber(value)) fail(`${rule.label} must be a finite number`);
      return;
    case 'finiteNumberArray':
      if (!Array.isArray(value) || value.some(item => !isFiniteNumber(item))) {
        fail(`${rule.label} must be an array of finite numbers`);
      }
      return;
    case 'nonEmptyString':
      if (typeof value !== 'string' || value.trim() === '') {
        fail(`${rule.label} must be a non-empty string`);
      }
      return;
    case 'nonEmptyStringArray':
      if (
        !Array.isArray(value)
        || value.some(item => typeof item !== 'string' || item.trim() === '')
      ) {
        fail(`${rule.label} must be an array of non-empty strings`);
      }
      return;
    case 'positiveInteger':
      if (!Number.isInteger(Number(value)) || Number(value) <= 0) {
        fail(`${rule.label} must be a positive integer`);
      }
      return;
    case 'rawPayloadArray':
      validateRawPayloadArray(value, rule.label, fail);
      return;
  }
}

export function validateRelayOperationFromManifest(
  operation: string,
  args: unknown[],
  mode: RelayContractMode | undefined,
  fail: ValidationFailure,
): RelayOperationDefinition {
  const definition = relayOperationDefinition(operation);
  if (!definition || (mode !== undefined && !definition.modes.includes(mode))) {
    fail(`Unsupported HirezRelay operation: ${operation}`);
  }

  if (
    definition.validation.maxArgs !== undefined
    && args.length > definition.validation.maxArgs
  ) {
    fail(
      definition.validation.maxArgsError
      ?? `${operation} accepts at most ${definition.validation.maxArgs} args`,
    );
  }

  for (const rule of definition.validation.rules) {
    validateRule(args, rule, fail);
  }
  return definition;
}
