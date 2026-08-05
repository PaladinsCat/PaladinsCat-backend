import type { PoolClient } from 'pg';
import { transaction } from '../config/db';

type QueryClient = Pick<PoolClient, 'query'>;

export const SCALABLE_STATS_PROJECTION_VERSION = 1;

const scopeSql = `
  SELECT m.match_id, m.entry_datetime, m.queue_id, COALESCE(mlt.lobby_tier, 0)::SMALLINT AS lobby_tier,
    COALESCE(NULLIF(m.map, ''), 'Unknown') AS map_name, m.duration_seconds
  FROM matches m
  LEFT JOIN match_lobby_tiers mlt
    ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
  WHERE m.match_id = ANY($1::bigint[])
    AND m.queue_id = 486
    AND COALESCE(m.limited, false) = false
`;

function normalizedMatchIds(matchIds: number[]): number[] {
  return [...new Set(matchIds
    .map(Number)
    .filter((id) => Number.isFinite(id) && id > 0)
    .map((id) => Math.trunc(id)))];
}

export async function projectMatchesWithClient(client: QueryClient, matchIds: number[]): Promise<number> {
  const ids = normalizedMatchIds(matchIds);
  if (ids.length === 0) return 0;

  const claim = await client.query<{ match_id: string }>(
    `INSERT INTO stats_projection_matches (projection_version, match_id)
     SELECT $1, requested.match_id
     FROM unnest($2::bigint[]) AS requested(match_id)
     JOIN matches m ON m.match_id = requested.match_id
     WHERE m.queue_id = 486
       AND COALESCE(m.limited, false) = false
     ON CONFLICT DO NOTHING RETURNING match_id`,
    [SCALABLE_STATS_PROJECTION_VERSION, ids],
  );
  const claimedIds = claim.rows.map((row) => Number(row.match_id));
  if (claimedIds.length === 0) return 0;

  await client.query(`
    INSERT INTO stats_match_aggregate (
      queue_id,lobby_tier,stat_date,region,map_name,match_count,duration_sum,updated_at
    )
    SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT,m.entry_datetime::DATE,
      COALESCE(NULLIF(m.region,''),'Unknown'),COALESCE(NULLIF(m.map,''),'Unknown'),COUNT(*)::bigint,
      COALESCE(SUM(m.duration_seconds),0)::bigint,now()
    FROM matches m LEFT JOIN match_lobby_tiers mlt
      ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
    WHERE m.match_id=ANY($1::bigint[])
    GROUP BY 1,2,3,4,5
    ON CONFLICT (queue_id,lobby_tier,stat_date,region,map_name) DO UPDATE SET
      match_count=stats_match_aggregate.match_count+EXCLUDED.match_count,
      duration_sum=stats_match_aggregate.duration_sum+EXCLUDED.duration_sum,updated_at=now()
  `, [claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), source AS (
      SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,
        CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
          WHEN c.roles ILIKE '%Support%' THEN 3
          WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT,
        scope.map_name,COALESCE(NULLIF(mp.platform,''),'Unknown'),COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,
        COALESCE(SUM(mp.kills),0)::BIGINT,COALESCE(SUM(mp.deaths),0)::BIGINT,COALESCE(SUM(mp.assists),0)::BIGINT,
        COALESCE(SUM(mp.damage_done_physical),0)::BIGINT,COALESCE(SUM(mp.gold_earned),0)::BIGINT,
        COALESCE(SUM(mp.healing),0)::BIGINT,COALESCE(SUM(mp.damage_mitigated),0)::BIGINT,
        COALESCE(SUM(mp.damage_per_minute),0)::DOUBLE PRECISION,
        COALESCE(SUM(mp.healing_per_minute),0)::DOUBLE PRECISION,
        COALESCE(SUM(mp.gold_per_minute),0)::DOUBLE PRECISION,
        COALESCE(SUM(mp.mitigation_per_minute),0)::DOUBLE PRECISION,
        COALESCE(SUM(mp.egpm),0)::DOUBLE PRECISION,
        COUNT(*) FILTER (WHERE mp.time_in_match>0)::BIGINT
      FROM scope JOIN match_players mp
        ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
      LEFT JOIN champions c ON c.id=mp.champion_id
      WHERE mp.champion_id>0 AND COALESCE(mp.source,'direct') IN ('direct','recovered')
      GROUP BY 1,2,3,4,5,6
    )
    INSERT INTO stats_player_aggregate
    SELECT *,now() FROM source
    ON CONFLICT (queue_id,lobby_tier,champion_id,map_name,platform) DO UPDATE SET
      role_id=EXCLUDED.role_id,plays=stats_player_aggregate.plays+EXCLUDED.plays,
      wins=stats_player_aggregate.wins+EXCLUDED.wins,losses=stats_player_aggregate.losses+EXCLUDED.losses,
      kills_sum=stats_player_aggregate.kills_sum+EXCLUDED.kills_sum,
      deaths_sum=stats_player_aggregate.deaths_sum+EXCLUDED.deaths_sum,
      assists_sum=stats_player_aggregate.assists_sum+EXCLUDED.assists_sum,
      damage_sum=stats_player_aggregate.damage_sum+EXCLUDED.damage_sum,
      gold_sum=stats_player_aggregate.gold_sum+EXCLUDED.gold_sum,
      healing_sum=stats_player_aggregate.healing_sum+EXCLUDED.healing_sum,
      mitigation_sum=stats_player_aggregate.mitigation_sum+EXCLUDED.mitigation_sum,
      dpm_sum=stats_player_aggregate.dpm_sum+EXCLUDED.dpm_sum,
      hpm_sum=stats_player_aggregate.hpm_sum+EXCLUDED.hpm_sum,
      gpm_sum=stats_player_aggregate.gpm_sum+EXCLUDED.gpm_sum,
      mpm_sum=stats_player_aggregate.mpm_sum+EXCLUDED.mpm_sum,
      egpm_sum=stats_player_aggregate.egpm_sum+EXCLUDED.egpm_sum,
      metric_samples=stats_player_aggregate.metric_samples+EXCLUDED.metric_samples,updated_at=now()
  `, [claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), source AS (
      SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,mpt.talent_id,mpc.card_id,
        COALESCE(mpc.card_level,0)::SMALLINT,COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT
      FROM scope JOIN match_players mp ON mp.match_id=scope.match_id
      JOIN match_player_talents mpt ON mpt.match_id=mp.match_id AND mpt.player_id=mp.player_id
      JOIN talents t ON t.talent_id=mpt.talent_id AND t.champion_id=mp.champion_id
      JOIN match_player_cards mpc ON mpc.match_id=mp.match_id AND mpc.player_id=mp.player_id
      WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5,6
    ) INSERT INTO stats_talent_card_aggregate SELECT *,now() FROM source
    ON CONFLICT (queue_id,lobby_tier,champion_id,talent_id,card_id,card_level) DO UPDATE SET
      uses=stats_talent_card_aggregate.uses+EXCLUDED.uses,wins=stats_talent_card_aggregate.wins+EXCLUDED.wins,
      losses=stats_talent_card_aggregate.losses+EXCLUDED.losses,updated_at=now()
  `,[claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), team_row AS (
      SELECT scope.queue_id,scope.lobby_tier,scope.map_name,mp.task_force,m.winning_task_force,
        COUNT(*) FILTER (WHERE c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%')::SMALLINT AS frontline,
        COUNT(*) FILTER (WHERE c.roles ILIKE '%Damage%')::SMALLINT AS damage,
        COUNT(*) FILTER (WHERE c.roles ILIKE '%Flank%')::SMALLINT AS flank,
        COUNT(*) FILTER (WHERE c.roles ILIKE '%Support%')::SMALLINT AS support
      FROM scope JOIN matches m ON m.match_id=scope.match_id AND m.entry_datetime=scope.entry_datetime
      JOIN match_players mp ON mp.match_id=m.match_id AND mp.entry_datetime=m.entry_datetime
      JOIN champions c ON c.id=mp.champion_id
      WHERE mp.task_force IN (1,2) AND mp.champion_id>0 AND COALESCE(mp.source,'direct') IN ('direct','recovered')
      GROUP BY 1,2,3,4,5 HAVING COUNT(*)=5
    ), source AS (
      SELECT queue_id,lobby_tier,map_name,frontline||'-'||damage||'-'||flank||'-'||support AS comp_id,
        frontline,damage,flank,support,1::BIGINT AS uses,
        (task_force=winning_task_force)::INT::BIGINT AS wins,
        (task_force<>winning_task_force)::INT::BIGINT AS losses
      FROM team_row WHERE frontline+damage+flank+support=5
    ), collapsed AS (
      -- Both teams can have the same composition. Collapse them before the
      -- upsert so one INSERT never targets the same aggregate key twice.
      SELECT queue_id,lobby_tier,map_name,comp_id,frontline,damage,flank,support,
        SUM(uses)::BIGINT AS uses,SUM(wins)::BIGINT AS wins,SUM(losses)::BIGINT AS losses
      FROM source GROUP BY 1,2,3,4,5,6,7,8
    ) INSERT INTO stats_composition_aggregate SELECT *,now() FROM collapsed
    ON CONFLICT (queue_id,lobby_tier,map_name,comp_id) DO UPDATE SET
      uses=stats_composition_aggregate.uses+EXCLUDED.uses,wins=stats_composition_aggregate.wins+EXCLUDED.wins,
      losses=stats_composition_aggregate.losses+EXCLUDED.losses,updated_at=now()
  `,[claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), source AS (
      SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,scope.map_name,mpi.item_id,
        COALESCE(mpi.slot,0)::SMALLINT,COALESCE(mpi.item_level,0)::SMALLINT,COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT
      FROM scope JOIN match_players mp ON mp.match_id=scope.match_id
      JOIN match_player_items mpi ON mpi.match_id=mp.match_id AND mpi.player_id=mp.player_id
      WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5,6,7
    ) INSERT INTO stats_item_aggregate SELECT *,now() FROM source
    ON CONFLICT (queue_id,lobby_tier,champion_id,map_name,item_id,slot,item_level) DO UPDATE SET
      uses=stats_item_aggregate.uses+EXCLUDED.uses,wins=stats_item_aggregate.wins+EXCLUDED.wins,
      losses=stats_item_aggregate.losses+EXCLUDED.losses,updated_at=now()
  `, [claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), source AS (
      SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,scope.map_name,mpt.talent_id,COUNT(*)::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,
        COALESCE(SUM(mp.kills),0)::BIGINT,COALESCE(SUM(mp.deaths),0)::BIGINT,COALESCE(SUM(mp.assists),0)::BIGINT
      FROM scope JOIN match_players mp ON mp.match_id=scope.match_id
      JOIN match_player_talents mpt ON mpt.match_id=mp.match_id AND mpt.player_id=mp.player_id
      JOIN talents t ON t.talent_id=mpt.talent_id AND t.champion_id=mp.champion_id
      WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5
    ) INSERT INTO stats_talent_aggregate SELECT *,now() FROM source
    ON CONFLICT (queue_id,lobby_tier,champion_id,map_name,talent_id) DO UPDATE SET
      uses=stats_talent_aggregate.uses+EXCLUDED.uses,wins=stats_talent_aggregate.wins+EXCLUDED.wins,
      losses=stats_talent_aggregate.losses+EXCLUDED.losses,kills_sum=stats_talent_aggregate.kills_sum+EXCLUDED.kills_sum,
      deaths_sum=stats_talent_aggregate.deaths_sum+EXCLUDED.deaths_sum,
      assists_sum=stats_talent_aggregate.assists_sum+EXCLUDED.assists_sum,updated_at=now()
  `, [claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), source AS (
      SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,mpc.card_id,COALESCE(mpc.card_level,0)::SMALLINT,
        COUNT(*)::BIGINT,COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,
        COALESCE(SUM(mp.kills),0)::BIGINT,COALESCE(SUM(mp.deaths),0)::BIGINT,COALESCE(SUM(mp.assists),0)::BIGINT
      FROM scope JOIN match_players mp ON mp.match_id=scope.match_id
      JOIN match_player_cards mpc ON mpc.match_id=mp.match_id AND mpc.player_id=mp.player_id
      WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5
    ) INSERT INTO stats_card_aggregate SELECT *,now() FROM source
    ON CONFLICT (queue_id,lobby_tier,champion_id,card_id,card_level) DO UPDATE SET
      uses=stats_card_aggregate.uses+EXCLUDED.uses,wins=stats_card_aggregate.wins+EXCLUDED.wins,
      losses=stats_card_aggregate.losses+EXCLUDED.losses,kills_sum=stats_card_aggregate.kills_sum+EXCLUDED.kills_sum,
      deaths_sum=stats_card_aggregate.deaths_sum+EXCLUDED.deaths_sum,
      assists_sum=stats_card_aggregate.assists_sum+EXCLUDED.assists_sum,updated_at=now()
  `, [claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), source AS (
      SELECT scope.queue_id,scope.lobby_tier,scope.map_name,mb.champion_id,
        COALESCE(mb.ban_slot,0)::SMALLINT,COUNT(*)::BIGINT
      FROM scope JOIN match_bans mb ON mb.match_id=scope.match_id
      WHERE mb.champion_id>0 GROUP BY 1,2,3,4,5
    ) INSERT INTO stats_ban_aggregate SELECT *,now() FROM source
    ON CONFLICT (queue_id,lobby_tier,map_name,champion_id,ban_slot) DO UPDATE SET
      bans=stats_ban_aggregate.bans+EXCLUDED.bans,updated_at=now()
  `, [claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), eligible AS (
      SELECT scope.queue_id,scope.lobby_tier,
        CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
          WHEN c.roles ILIKE '%Support%' THEN 3
          WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT AS role_id,
        mp.damage_per_minute,mp.healing_per_minute,mp.gold_per_minute,mp.egpm,mp.mitigation_per_minute,mp.kda,
        CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
          THEN COALESCE(mp.damage_done_in_hand,0)/(scope.duration_seconds/60.0) END AS weapon_per_minute,
        CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
          THEN GREATEST(COALESCE(mp.damage_done_physical,0)-COALESCE(mp.damage_done_in_hand,0),0)
            /(scope.duration_seconds/60.0) END AS ability_per_minute
      FROM scope JOIN match_players mp
        ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
      LEFT JOIN champions c ON c.id=mp.champion_id
      WHERE mp.champion_id>0 AND scope.duration_seconds>120 AND COALESCE(mp.source,'direct') IN ('direct','recovered')
    ), metric_values AS (
      SELECT e.queue_id,e.lobby_tier,roles.role_id,metric.metric,
        CASE WHEN metric.metric='kda' THEN ROUND(metric.value::NUMERIC,1)::DOUBLE PRECISION
          ELSE ROUND(metric.value::NUMERIC,0)::DOUBLE PRECISION END AS value
      FROM eligible e
      CROSS JOIN LATERAL (SELECT DISTINCT role_id FROM (VALUES (0::SMALLINT),(e.role_id)) r(role_id)) roles
      CROSS JOIN LATERAL (VALUES ('dpm',e.damage_per_minute::DOUBLE PRECISION),
        ('wpm',e.weapon_per_minute::DOUBLE PRECISION),('apm',e.ability_per_minute::DOUBLE PRECISION),
        ('hpm',e.healing_per_minute::DOUBLE PRECISION),('gpm',e.gold_per_minute::DOUBLE PRECISION),
        ('egpm',e.egpm::DOUBLE PRECISION),('mpm',e.mitigation_per_minute::DOUBLE PRECISION),
        ('kda',e.kda::DOUBLE PRECISION)) metric(metric,value)
      WHERE metric.value IS NOT NULL
        AND (metric.value>0 OR (metric.metric IN ('wpm','apm','egpm') AND metric.value=0))
    ) INSERT INTO stats_metric_histogram
    SELECT queue_id,lobby_tier,role_id,metric,value,COUNT(*)::BIGINT,now()
    FROM metric_values GROUP BY 1,2,3,4,5
    ON CONFLICT (queue_id,lobby_tier,role_id,metric,value) DO UPDATE SET
      sample_count=stats_metric_histogram.sample_count+EXCLUDED.sample_count,updated_at=now()
  `, [claimedIds]);

  await client.query(`
    WITH scope AS (${scopeSql}), eligible AS (
      SELECT scope.queue_id,scope.lobby_tier,mp.champion_id,
        mp.damage_per_minute,mp.healing_per_minute,mp.gold_per_minute,mp.egpm,mp.mitigation_per_minute,mp.kda,
        CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
          THEN COALESCE(mp.damage_done_in_hand,0)/(scope.duration_seconds/60.0) END AS weapon_per_minute,
        CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
          THEN GREATEST(COALESCE(mp.damage_done_physical,0)-COALESCE(mp.damage_done_in_hand,0),0)
            /(scope.duration_seconds/60.0) END AS ability_per_minute
      FROM scope JOIN match_players mp
        ON mp.match_id=scope.match_id AND mp.entry_datetime=scope.entry_datetime
      WHERE mp.champion_id>0 AND scope.duration_seconds>120 AND COALESCE(mp.source,'direct') IN ('direct','recovered')
    ), metric_values AS (
      SELECT e.queue_id,e.lobby_tier,e.champion_id,metric.metric,
        CASE WHEN metric.metric='kda' THEN ROUND(metric.value::NUMERIC,1)::DOUBLE PRECISION
          ELSE ROUND(metric.value::NUMERIC,0)::DOUBLE PRECISION END AS value
      FROM eligible e CROSS JOIN LATERAL (VALUES
        ('dpm',e.damage_per_minute::DOUBLE PRECISION),('wpm',e.weapon_per_minute::DOUBLE PRECISION),
        ('apm',e.ability_per_minute::DOUBLE PRECISION),('hpm',e.healing_per_minute::DOUBLE PRECISION),
        ('gpm',e.gold_per_minute::DOUBLE PRECISION),('egpm',e.egpm::DOUBLE PRECISION),
        ('mpm',e.mitigation_per_minute::DOUBLE PRECISION),('kda',e.kda::DOUBLE PRECISION)
      ) metric(metric,value) WHERE metric.value IS NOT NULL
        AND (metric.value>0 OR (metric.metric IN ('wpm','apm','egpm') AND metric.value=0))
    ) INSERT INTO stats_champion_metric_histogram
    SELECT queue_id,lobby_tier,champion_id,metric,value,COUNT(*)::BIGINT,now()
    FROM metric_values GROUP BY 1,2,3,4,5
    ON CONFLICT (queue_id,lobby_tier,champion_id,metric,value) DO UPDATE SET
      sample_count=stats_champion_metric_histogram.sample_count+EXCLUDED.sample_count,updated_at=now()
  `, [claimedIds]);

  return claimedIds.length;
}

export async function projectMatchWithClient(client: QueryClient, matchId: number): Promise<boolean> {
  return (await projectMatchesWithClient(client, [matchId])) > 0;
}

export async function upsertScalableStatsProjectionsForMatch(matchId: number): Promise<boolean> {
  return transaction((client) => projectMatchWithClient(client, matchId));
}

export async function upsertScalableStatsProjectionsForMatches(matchIds: number[]): Promise<number> {
  return transaction((client) => projectMatchesWithClient(client, matchIds));
}

/** Rebuild the scalable tables whose values depend on canonical per-minute rates. */
export async function rebuildScalablePerformanceReadModelsWithClient(
  client: QueryClient,
): Promise<Record<string, number>> {
  await client.query('DELETE FROM stats_player_aggregate');
  await client.query(`
    INSERT INTO stats_player_aggregate (
      queue_id,lobby_tier,champion_id,role_id,map_name,platform,
      plays,wins,losses,kills_sum,deaths_sum,assists_sum,damage_sum,gold_sum,
      healing_sum,mitigation_sum,dpm_sum,hpm_sum,gpm_sum,mpm_sum,egpm_sum,
      metric_samples,updated_at
    )
    SELECT
      m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT,mp.champion_id,
      CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
        WHEN c.roles ILIKE '%Support%' THEN 3
        WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT,
      COALESCE(NULLIF(m.map,''),'Unknown'),COALESCE(NULLIF(mp.platform,''),'Unknown'),
      COUNT(*)::BIGINT,
      COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
      COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,
      COALESCE(SUM(mp.kills),0)::BIGINT,COALESCE(SUM(mp.deaths),0)::BIGINT,
      COALESCE(SUM(mp.assists),0)::BIGINT,COALESCE(SUM(mp.damage_done_physical),0)::BIGINT,
      COALESCE(SUM(mp.gold_earned),0)::BIGINT,COALESCE(SUM(mp.healing),0)::BIGINT,
      COALESCE(SUM(mp.damage_mitigated),0)::BIGINT,
      COALESCE(SUM(mp.damage_per_minute),0)::DOUBLE PRECISION,
      COALESCE(SUM(mp.healing_per_minute),0)::DOUBLE PRECISION,
      COALESCE(SUM(mp.gold_per_minute),0)::DOUBLE PRECISION,
      COALESCE(SUM(mp.mitigation_per_minute),0)::DOUBLE PRECISION,
      COALESCE(SUM(mp.egpm),0)::DOUBLE PRECISION,
      COUNT(*) FILTER (WHERE m.duration_seconds>0)::BIGINT,now()
    FROM match_players mp
    JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
    LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
    LEFT JOIN champions c ON c.id=mp.champion_id
    WHERE m.queue_id=486
      AND mp.champion_id>0 AND COALESCE(mp.source,'direct') IN ('direct','recovered')
    GROUP BY 1,2,3,4,5,6
  `);

  const metricValuesSql = `
    SELECT
      m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT AS lobby_tier,mp.champion_id,
      CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
        WHEN c.roles ILIKE '%Support%' THEN 3
        WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT AS role_id,
      metric.metric,
      CASE WHEN metric.metric='kda' THEN ROUND(metric.value::NUMERIC,1)::DOUBLE PRECISION
        ELSE ROUND(metric.value::NUMERIC,0)::DOUBLE PRECISION END AS value
    FROM match_players mp
    JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
    LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
    LEFT JOIN champions c ON c.id=mp.champion_id
    CROSS JOIN LATERAL (VALUES
      ('dpm'::TEXT,mp.damage_per_minute::DOUBLE PRECISION),
      ('wpm'::TEXT,CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
        THEN COALESCE(mp.damage_done_in_hand,0)/(m.duration_seconds/60.0) END::DOUBLE PRECISION),
      ('apm'::TEXT,CASE WHEN COALESCE(mp.source,'direct')<>'recovered'
        THEN GREATEST(COALESCE(mp.damage_done_physical,0)-COALESCE(mp.damage_done_in_hand,0),0)
          /(m.duration_seconds/60.0) END::DOUBLE PRECISION),
      ('hpm'::TEXT,mp.healing_per_minute::DOUBLE PRECISION),
      ('gpm'::TEXT,mp.gold_per_minute::DOUBLE PRECISION),
      ('egpm'::TEXT,mp.egpm::DOUBLE PRECISION),
      ('mpm'::TEXT,mp.mitigation_per_minute::DOUBLE PRECISION),
      ('kda'::TEXT,mp.kda::DOUBLE PRECISION)
    ) metric(metric,value)
    WHERE m.queue_id=486
      AND mp.champion_id>0 AND m.duration_seconds>120
      AND COALESCE(mp.source,'direct') IN ('direct','recovered')
      AND metric.value IS NOT NULL
      AND (metric.value>0 OR (metric.metric IN ('wpm','apm','egpm') AND metric.value=0))
  `;

  await client.query('DELETE FROM stats_metric_histogram');
  await client.query(`
    WITH metric_values AS (${metricValuesSql}), scoped AS (
      SELECT metric_values.queue_id,metric_values.lobby_tier,scope.role_id,
        metric_values.metric,metric_values.value
      FROM metric_values CROSS JOIN LATERAL (
        SELECT DISTINCT role_id FROM (VALUES (0::SMALLINT),(metric_values.role_id)) roles(role_id)
      ) scope
    )
    INSERT INTO stats_metric_histogram
    SELECT queue_id,lobby_tier,role_id,metric,value,COUNT(*)::BIGINT,now()
    FROM scoped GROUP BY 1,2,3,4,5
  `);

  await client.query('DELETE FROM stats_champion_metric_histogram');
  await client.query(`
    WITH metric_values AS (${metricValuesSql})
    INSERT INTO stats_champion_metric_histogram
    SELECT queue_id,lobby_tier,champion_id,metric,value,COUNT(*)::BIGINT,now()
    FROM metric_values GROUP BY 1,2,3,4,5
  `);

  const counts: Record<string, number> = {};
  for (const table of ['stats_player_aggregate','stats_metric_histogram','stats_champion_metric_histogram']) {
    const result = await client.query<{ count: number }>(`SELECT COUNT(*)::INT AS count FROM ${table}`);
    counts[table] = Number(result.rows[0]?.count ?? 0);
  }
  return counts;
}

/** Repair bounded projection gaps without turning the daily job into a fact-table rebuild. */
export async function repairScalableStatsProjectionGapsWithClient(
  client: QueryClient,
  limit = 250,
): Promise<number> {
  const missing = await client.query<{ match_id: string }>(`
    SELECT m.match_id
    FROM matches m
    JOIN match_ingest_status mis ON mis.match_id=m.match_id AND mis.status='complete'
    LEFT JOIN stats_projection_matches spm
      ON spm.projection_version=$1 AND spm.match_id=m.match_id
    WHERE spm.match_id IS NULL
      AND m.queue_id=486
      AND COALESCE(m.limited, false)=false
    ORDER BY m.entry_datetime,m.match_id
    LIMIT $2
  `,[SCALABLE_STATS_PROJECTION_VERSION,limit]);
  return projectMatchesWithClient(client, missing.rows.map((row) => Number(row.match_id)));
}
