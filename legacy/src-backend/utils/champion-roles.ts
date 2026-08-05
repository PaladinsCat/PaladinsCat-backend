const CHAMPION_ROLES: Record<string, string[]> = {
  Frontline: [
    'Ash', 'Atlas', 'Azaan', 'Barik', 'Fernando', 'Inara', 'Khan', 'Makoa',
    'Nyx', 'Raum', 'Ruckus', 'Terminus', 'Torvald', 'Yagorath',
  ],
  Damage: [
    'Betty La Bomba', 'Betty la Bomba', 'Bomb King', 'Cassie', 'Dredge', 'Drogoz', 'Imani',
    'Kinessa', 'Lian', 'Octavia', 'Omen', 'Saati', 'Sha Lin', 'Strix',
    'Tiberius', 'Tyra', 'Viktor', 'Vivian', 'Willo',
  ],
  Flank: [
    'Androxus', 'Buck', 'Caspian', 'Evie', 'Kasumi', 'Koga', 'Lex', 'Maeve',
    'Skye', 'Talus', 'Vatu', 'VII', 'Vora', 'Zhin',
  ],
  Support: [
    'Corvus', 'Furia', 'Grohk', 'Grover', 'Io', 'Jenos', 'Lillith',
    'Mal Damba', "Mal'Damba", 'Moji', 'Pip', 'Rei', 'Seris', 'Ying',
  ],
};

function sqlString(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

function sqlList(values: string[]): string {
  return values.map(sqlString).join(', ');
}

/**
 * SQL expression that normalizes champion class/role.
 *
 * The Hi-Rez reference refresh can temporarily leave champions.roles as
 * "Unknown" while match ingest already has valid champion IDs/names. Player
 * and stats leaderboards should not go dark just because the reference row is
 * incomplete, so every endpoint that needs a role should use this expression:
 * first trust a populated DB role, then fall back to the canonical champion
 * name map, then expose the raw role/Unknown for debugging.
 */
export function championRoleSql(alias = 'c'): string {
  const roles = `${alias}.roles`;
  const name = `${alias}.name`;
  return `CASE
    WHEN ${roles} ILIKE '%Frontline%' OR ${roles} ILIKE '%Front Line%' OR ${name} IN (${sqlList(CHAMPION_ROLES.Frontline)}) THEN 'Frontline'
    WHEN ${roles} ILIKE '%Damage%' OR ${name} IN (${sqlList(CHAMPION_ROLES.Damage)}) THEN 'Damage'
    WHEN ${roles} ILIKE '%Flank%' OR ${name} IN (${sqlList(CHAMPION_ROLES.Flank)}) THEN 'Flank'
    WHEN ${roles} ILIKE '%Support%' OR ${name} IN (${sqlList(CHAMPION_ROLES.Support)}) THEN 'Support'
    ELSE COALESCE(NULLIF(${roles}, ''), 'Unknown')
  END`;
}
