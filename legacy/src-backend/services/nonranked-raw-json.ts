const EQUIPMENT_KEYS = [
  'active_id_1', 'item_active_1', 'active_level_1',
  'active_id_2', 'item_active_2', 'active_level_2',
  'active_id_3', 'item_active_3', 'active_level_3',
  'active_id_4', 'item_active_4', 'active_level_4',
  'item_id_1', 'item_purch_1', 'item_level_1',
  'item_id_2', 'item_purch_2', 'item_level_2',
  'item_id_3', 'item_purch_3', 'item_level_3',
  'item_id_4', 'item_purch_4', 'item_level_4',
  'item_id_5', 'item_purch_5', 'item_level_5',
  'item_id_6', 'item_purch_6',
] as const;

export function compactNonrankedRawPlayer(value: unknown): Record<string, unknown> {
  const source = value && typeof value === 'object'
    ? value as Record<string, unknown>
    : {};
  const compact: Record<string, unknown> = { _storage: 'compact-equipment-v1' };
  for (const key of EQUIPMENT_KEYS) {
    const field = source[key];
    if (field !== undefined && field !== null) compact[key] = field;
  }
  return compact;
}
