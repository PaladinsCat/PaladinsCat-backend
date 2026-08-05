import { one, transaction } from '../config/db';
import { rebuildPlayerAverages } from '../services/player-performance-rollups';
import { rebuildPartyTrackingProjections } from '../services/party-tracking';
import { rebuildPerformanceReadModelsWithClient } from '../services/performance-projections';
import {
  rebuildScalablePerformanceReadModelsWithClient,
  repairScalableStatsProjectionGapsWithClient,
} from '../services/scalable-stats-projections';

type ProjectionCounts = Record<string, number>;
type RefreshSource = 'scheduler' | 'manual' | 'post_ingest';

const NORMALIZED_WIN_SQL = `lower(COALESCE(mp.win_status, '')) IN ('winner', 'win')`;
const NORMALIZED_LOSS_SQL = `lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss')`;
const NORMALIZED_RATE_SQL = `ROUND(
        100.0 * COUNT(*) FILTER (WHERE ${NORMALIZED_WIN_SQL})::NUMERIC
        / NULLIF(
          (
            COUNT(*) FILTER (WHERE ${NORMALIZED_WIN_SQL})
            + COUNT(*) FILTER (WHERE ${NORMALIZED_LOSS_SQL})
          )::NUMERIC,
          0
        ),
        2
      )`;

async function tableCount(client: { query: (sql: string, params?: unknown[]) => Promise<{ rows: any[] }> }, tableName: string): Promise<number> {
  const result = await client.query(`SELECT COUNT(*)::INT AS count FROM ${tableName}`);
  return Number(result.rows[0]?.count || 0);
}

async function relationExists(
  client: { query: (sql: string, params?: unknown[]) => Promise<{ rows: any[] }> },
  relationName: string,
): Promise<boolean> {
  const result = await client.query('SELECT to_regclass($1) AS relation_oid', [`public.${relationName}`]);
  return Boolean(result.rows[0]?.relation_oid);
}

/**
 * Rebuild derived projection tables from durable source facts.
 *
 * Source-of-truth tables:
 * - `matches`
 * - `match_players`
 * - `match_bans`
 * - `match_player_items`
 * - `match_player_talents`
 * - `match_player_cards`
 *
 * Projection tables rebuilt here:
 * - `hourly_match_counts`
 * - `match_compositions`
 * - `match_compositions_ranked`
 * - `bans_ranked`
 * - `item_counts_ranked`
 * - `talent_counts_ranked`
 * - `card_counts_ranked`
 * - `talent_card_counts_ranked`
 * - `skin_counts_ranked`
 * - `match_lobby_tiers`
 * - `champion_stats_ranked`
 * - `players.avg_*` rolling performance columns
 * - `party_stack_stats` and `party_pair_stats` from immutable match party facts
 * - `mv_player_coplay_stats` when that materialized view exists
 *
 * Why this exists:
 * The buffer processor maintains these tables incrementally for new matches,
 * but the tables are derived data. If a reference table is missing, a deploy
 * changes projection SQL, or a legacy row was marked complete before a
 * projection existed, incremental replay will not repair old rows. This worker
 * treats facts as the source of truth and rebuilds projections in one
 * transaction so dashboards can be corrected without re-fetching Hi-Rez.
 */
export async function rebuildDerivedProjections(): Promise<ProjectionCounts> {
  return transaction(async (client) => {
    const counts: ProjectionCounts = {};

    // hourly_match_counts is an analytics projection. Preserve explicit zero
    // rows, because the hourly ingest state table, not this projection, controls
    // retries. Positive rows are rebuilt from `matches`.
    await client.query('DELETE FROM hourly_match_counts WHERE total_matches > 0');
    await client.query(`
      INSERT INTO hourly_match_counts (
        date, hour, queue_id,
        matches_na, matches_eu, matches_asia, matches_sea, matches_jpn,
        matches_rus, matches_br, matches_oce, matches_sa, matches_unknown,
        total_matches, fetched_at
      )
      SELECT
        (entry_datetime AT TIME ZONE 'UTC')::DATE AS date,
        EXTRACT(HOUR FROM entry_datetime AT TIME ZONE 'UTC')::INT AS hour,
        queue_id,
        COUNT(*) FILTER (WHERE region = 'NA')::INT AS matches_na,
        COUNT(*) FILTER (WHERE region = 'EU')::INT AS matches_eu,
        COUNT(*) FILTER (WHERE region IN ('ASIA', 'Asia'))::INT AS matches_asia,
        COUNT(*) FILTER (WHERE region = 'SEA')::INT AS matches_sea,
        COUNT(*) FILTER (WHERE region = 'JPN')::INT AS matches_jpn,
        COUNT(*) FILTER (WHERE region = 'RUS')::INT AS matches_rus,
        COUNT(*) FILTER (WHERE region = 'BR')::INT AS matches_br,
        COUNT(*) FILTER (WHERE region = 'OCE')::INT AS matches_oce,
        COUNT(*) FILTER (WHERE region = 'SA')::INT AS matches_sa,
        COUNT(*) FILTER (
          WHERE COALESCE(region, '') NOT IN ('NA','EU','ASIA','Asia','SEA','JPN','RUS','BR','OCE','SA')
        )::INT AS matches_unknown,
        COUNT(*)::INT AS total_matches,
        now() AS fetched_at
      FROM matches
      WHERE queue_id = 486
        AND COALESCE(limited, false) = false
      GROUP BY 1, 2, queue_id
      ON CONFLICT (date, hour, queue_id) DO UPDATE SET
        matches_na = EXCLUDED.matches_na,
        matches_eu = EXCLUDED.matches_eu,
        matches_asia = EXCLUDED.matches_asia,
        matches_sea = EXCLUDED.matches_sea,
        matches_jpn = EXCLUDED.matches_jpn,
        matches_rus = EXCLUDED.matches_rus,
        matches_br = EXCLUDED.matches_br,
        matches_oce = EXCLUDED.matches_oce,
        matches_sa = EXCLUDED.matches_sa,
        matches_unknown = EXCLUDED.matches_unknown,
        total_matches = EXCLUDED.total_matches,
        fetched_at = now()
    `);

    await rebuildMatchLobbyTiers(client);
    await client.query('DELETE FROM match_compositions_ranked');
    await client.query(`
      WITH team_comps AS (
        SELECT
          mp.match_id,
          mp.entry_datetime,
          mp.task_force,
          m.winning_task_force,
          mlt.lobby_tier,
          COUNT(*) FILTER (WHERE c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%')::INT AS frontline,
          COUNT(*) FILTER (WHERE c.roles ILIKE '%Damage%')::INT AS damage,
          COUNT(*) FILTER (WHERE c.roles ILIKE '%Flank%')::INT AS flank,
          COUNT(*) FILTER (WHERE c.roles ILIKE '%Support%')::INT AS support
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
        JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        JOIN champions c ON c.id = mp.champion_id
        WHERE m.queue_id = 486
          AND COALESCE(m.limited, false) = false
          AND mp.task_force IS NOT NULL
          AND mp.task_force != 0
          AND mp.champion_id > 0
          AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
        GROUP BY mp.match_id, mp.entry_datetime, mp.task_force, m.winning_task_force, mlt.lobby_tier
        HAVING COUNT(*) = 5
      )
      INSERT INTO match_compositions_ranked (
        comp_id, lobby_tier, frontline, damage, flank, support, count, wins, losses, updated_at
      )
      SELECT
        frontline || '-' || damage || '-' || flank || '-' || support AS comp_id,
        lobby_tier,
        frontline,
        damage,
        flank,
        support,
        COUNT(*)::INT AS count,
        COUNT(*) FILTER (WHERE task_force = winning_task_force)::INT AS wins,
        COUNT(*) FILTER (WHERE task_force != winning_task_force)::INT AS losses,
        now()
      FROM team_comps
      WHERE frontline + damage + flank + support = 5
      GROUP BY lobby_tier, frontline, damage, flank, support
    `);

    // Keep the legacy all-lobbies projection synchronized for older internal
    // consumers while the public endpoint reads the tier-bucketed table.
    await client.query('DELETE FROM match_compositions');
    await client.query(`
      INSERT INTO match_compositions (
        comp_id, frontline, damage, flank, support, count, wins, losses, winrate, updated_at
      )
      SELECT
        comp_id,
        frontline,
        damage,
        flank,
        support,
        SUM(count)::INT,
        SUM(wins)::INT,
        SUM(losses)::INT,
        ROUND(100.0 * SUM(wins)::NUMERIC / NULLIF((SUM(wins) + SUM(losses))::NUMERIC, 0), 2),
        now()
      FROM match_compositions_ranked
      GROUP BY comp_id, frontline, damage, flank, support
    `);

    await client.query('DELETE FROM bans_ranked');
    await client.query(`
      INSERT INTO bans_ranked (
        champion_id, champion_name, ban_total,
        slot1, slot2, slot3, slot4, slot5, slot6, slot7, slot8, updated_at
      )
      SELECT
        mb.champion_id,
        COALESCE(c.name, 'Champion ' || mb.champion_id::TEXT) AS champion_name,
        COUNT(*)::INT AS ban_total,
        COUNT(*) FILTER (WHERE mb.ban_slot = 1)::INT AS slot1,
        COUNT(*) FILTER (WHERE mb.ban_slot = 2)::INT AS slot2,
        COUNT(*) FILTER (WHERE mb.ban_slot = 3)::INT AS slot3,
        COUNT(*) FILTER (WHERE mb.ban_slot = 4)::INT AS slot4,
        COUNT(*) FILTER (WHERE mb.ban_slot = 5)::INT AS slot5,
        COUNT(*) FILTER (WHERE mb.ban_slot = 6)::INT AS slot6,
        COUNT(*) FILTER (WHERE mb.ban_slot = 7)::INT AS slot7,
        COUNT(*) FILTER (WHERE mb.ban_slot = 8)::INT AS slot8,
        now()
      FROM match_bans mb
      JOIN matches m ON m.match_id = mb.match_id
      LEFT JOIN champions c ON c.id = mb.champion_id
      WHERE m.queue_id = 486
        AND COALESCE(m.limited, false) = false
        AND mb.champion_id > 0
      GROUP BY mb.champion_id, c.name
    `);

    await rebuildItemCounts(client);
    await rebuildTalentCounts(client);
    await rebuildCardCounts(client);
    await rebuildTalentCardCounts(client);
    await rebuildSkinCounts(client);
    // These tables remain in the schema for compatibility with older builds,
    // but casual performance aggregates are no longer a supported projection.
    // Purging them during repair also removes data written before the queue-486
    // authority fence was enforced.
    await client.query('DELETE FROM item_counts_casual');
    await client.query('DELETE FROM talent_counts_casual');
    await client.query('DELETE FROM card_counts_casual');
    await rebuildChampionStatsRanked(client);
    counts.player_performance_averages = await rebuildPlayerAverages(client);
    Object.assign(counts, await rebuildPerformanceReadModelsWithClient(client));
    if (await relationExists(client,'stats_projection_matches')) {
      Object.assign(counts, await rebuildScalablePerformanceReadModelsWithClient(client));
      counts.scalable_stats_repaired = await repairScalableStatsProjectionGapsWithClient(client);
    }

    if (
      await relationExists(client, 'match_party_groups')
      && await relationExists(client, 'match_party_pairs')
      && await relationExists(client, 'party_stack_stats')
      && await relationExists(client, 'party_pair_stats')
    ) {
      await rebuildPartyTrackingProjections(client);
    }

    const countTargets = [
      'hourly_match_counts',
      'match_compositions',
      'match_compositions_ranked',
      'bans_ranked',
      'item_counts_ranked',
      'item_counts_casual',
      'talent_counts_ranked',
      'talent_counts_casual',
      'card_counts_ranked',
      'talent_card_counts_ranked',
      'skin_counts_ranked',
      'match_lobby_tiers',
      'card_counts_casual',
      'champion_stats_ranked',
    ];

    for (const table of [
      'stats_match_aggregate','stats_player_aggregate','stats_item_aggregate',
      'stats_talent_aggregate','stats_card_aggregate','stats_talent_card_aggregate','stats_ban_aggregate',
      'stats_composition_aggregate','stats_metric_histogram','player_queue_rating_summary',
    ]) {
      if (await relationExists(client,table)) countTargets.push(table);
    }
    if (await relationExists(client,'stats_champion_metric_histogram')) {
      countTargets.push('stats_champion_metric_histogram');
    }

    if (await relationExists(client, 'party_stack_stats')) countTargets.push('party_stack_stats');
    if (await relationExists(client, 'party_pair_stats')) countTargets.push('party_pair_stats');

    // Co-play stats are derived from player_relationships, not fetched from
    // Hi-Rez. Keep this optional so older local databases can still repair the
    // table projections before migration 014 has been applied; once the view
    // exists, the same repair pass refreshes /coplay/stats as well.
    if (await relationExists(client, 'mv_player_coplay_stats')) {
      await client.query('REFRESH MATERIALIZED VIEW mv_player_coplay_stats');
      countTargets.push('mv_player_coplay_stats');
    }

    for (const table of countTargets) {
      counts[table] = await tableCount(client, table);
    }

    return counts;
  });
}

async function rebuildItemCounts(client: { query: (sql: string, params?: unknown[]) => Promise<unknown> }): Promise<void> {
  const tableName = 'item_counts_ranked';
  await client.query(`DELETE FROM ${tableName}`);
  await client.query(`
    INSERT INTO ${tableName} (item_id, item_name, slot, item_level, count, wins, losses, winrate, updated_at)
    SELECT
      mpi.item_id,
      COALESCE(i.item_name, 'Item ' || mpi.item_id::TEXT) AS item_name,
      mpi.slot,
      COALESCE(mpi.item_level, 0)::SMALLINT AS item_level,
      COUNT(*)::INT AS count,
      -- Outcome labels are not fully canonical in historical facts: direct
      -- match rows use Winner/Loser, while recovered/history rows can use
      -- Win/Loss. Normalize here so derived talent/card/item win rates match
      -- champion_stats_ranked and do not treat valid recovered wins as losses.
      COUNT(*) FILTER (WHERE ${NORMALIZED_WIN_SQL})::INT AS wins,
      COUNT(*) FILTER (WHERE ${NORMALIZED_LOSS_SQL})::INT AS losses,
      ${NORMALIZED_RATE_SQL} AS winrate,
      now()
    FROM match_player_items mpi
    JOIN match_players mp ON mp.match_id = mpi.match_id AND mp.player_id = mpi.player_id
    JOIN matches m ON m.match_id = mpi.match_id
    LEFT JOIN items i ON i.item_id = mpi.item_id
    WHERE m.queue_id = 486
      AND COALESCE(m.limited, false) = false
    GROUP BY mpi.item_id, COALESCE(i.item_name, 'Item ' || mpi.item_id::TEXT), mpi.slot, COALESCE(mpi.item_level, 0)
  `);
}

async function rebuildTalentCounts(client: { query: (sql: string, params?: unknown[]) => Promise<unknown> }): Promise<void> {
  const tableName = 'talent_counts_ranked';
  await client.query(`DELETE FROM ${tableName}`);
  await client.query(`
    INSERT INTO ${tableName} (talent_id, champion_name, talent_name, count, wins, losses, winrate, updated_at)
    SELECT
      mpt.talent_id,
      COALESCE(ch_talent.name, ch_player.name, 'Champion ' || mp.champion_id::TEXT) AS champion_name,
      COALESCE(t.talent_name, 'Talent ' || mpt.talent_id::TEXT) AS talent_name,
      COUNT(*)::INT AS count,
      -- Outcome labels are not fully canonical in historical facts: direct
      -- match rows use Winner/Loser, while recovered/history rows can use
      -- Win/Loss. Normalize here so derived talent/card/item win rates match
      -- champion_stats_ranked and do not treat valid recovered wins as losses.
      COUNT(*) FILTER (WHERE ${NORMALIZED_WIN_SQL})::INT AS wins,
      COUNT(*) FILTER (WHERE ${NORMALIZED_LOSS_SQL})::INT AS losses,
      ${NORMALIZED_RATE_SQL} AS winrate,
      now()
    FROM match_player_talents mpt
    JOIN match_players mp ON mp.match_id = mpt.match_id AND mp.player_id = mpt.player_id
    JOIN matches m ON m.match_id = mpt.match_id
    JOIN talents t ON t.talent_id = mpt.talent_id AND t.champion_id = mp.champion_id
    LEFT JOIN champions ch_talent ON ch_talent.id = t.champion_id
    LEFT JOIN champions ch_player ON ch_player.id = mp.champion_id
    WHERE m.queue_id = 486
      AND COALESCE(m.limited, false) = false
    GROUP BY mpt.talent_id, COALESCE(ch_talent.name, ch_player.name, 'Champion ' || mp.champion_id::TEXT), COALESCE(t.talent_name, 'Talent ' || mpt.talent_id::TEXT)
  `);
}

async function rebuildCardCounts(client: { query: (sql: string, params?: unknown[]) => Promise<unknown> }): Promise<void> {
  const tableName = 'card_counts_ranked';
  await client.query(`DELETE FROM ${tableName}`);
  await client.query(`
    INSERT INTO ${tableName} (card_id, champion_name, card_name, card_level, count, wins, losses, winrate, updated_at)
    SELECT
      mpc.card_id,
      COALESCE(ch_card.name, ch_player.name, 'Champion ' || mp.champion_id::TEXT) AS champion_name,
      COALESCE(cd.card_name, 'Card ' || mpc.card_id::TEXT) AS card_name,
      COALESCE(mpc.card_level, 0)::SMALLINT AS card_level,
      COUNT(*)::INT AS count,
      -- Outcome labels are not fully canonical in historical facts: direct
      -- match rows use Winner/Loser, while recovered/history rows can use
      -- Win/Loss. Normalize here so derived talent/card/item win rates match
      -- champion_stats_ranked and do not treat valid recovered wins as losses.
      COUNT(*) FILTER (WHERE ${NORMALIZED_WIN_SQL})::INT AS wins,
      COUNT(*) FILTER (WHERE ${NORMALIZED_LOSS_SQL})::INT AS losses,
      ${NORMALIZED_RATE_SQL} AS winrate,
      now()
    FROM match_player_cards mpc
    JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id
    JOIN matches m ON m.match_id = mpc.match_id
    LEFT JOIN cards cd ON cd.card_id = mpc.card_id
    LEFT JOIN champions ch_card ON ch_card.id = cd.champion_id
    LEFT JOIN champions ch_player ON ch_player.id = mp.champion_id
    WHERE m.queue_id = 486
      AND COALESCE(m.limited, false) = false
    GROUP BY
      mpc.card_id,
      COALESCE(ch_card.name, ch_player.name, 'Champion ' || mp.champion_id::TEXT),
      COALESCE(cd.card_name, 'Card ' || mpc.card_id::TEXT),
      COALESCE(mpc.card_level, 0)
  `);
}

async function rebuildTalentCardCounts(client: { query: (sql: string, params?: unknown[]) => Promise<unknown> }): Promise<void> {
  await client.query('DELETE FROM talent_card_counts_ranked');
  await client.query(`
    INSERT INTO talent_card_counts_ranked (
      talent_id, card_id, card_level, count, wins, losses, updated_at
    )
    SELECT
      mpt.talent_id,
      mpc.card_id,
      COALESCE(mpc.card_level, 0)::SMALLINT,
      COUNT(*)::INT,
      COUNT(*) FILTER (WHERE ${NORMALIZED_WIN_SQL})::INT,
      COUNT(*) FILTER (WHERE ${NORMALIZED_LOSS_SQL})::INT,
      now()
    FROM match_player_talents mpt
    JOIN match_player_cards mpc
      ON mpc.match_id = mpt.match_id
     AND mpc.player_id = mpt.player_id
    JOIN match_players mp
      ON mp.match_id = mpt.match_id
     AND mp.player_id = mpt.player_id
    JOIN talents t
      ON t.talent_id = mpt.talent_id
     AND t.champion_id = mp.champion_id
    JOIN matches m ON m.match_id = mp.match_id
    WHERE m.queue_id = 486
      AND COALESCE(m.limited, false) = false
    GROUP BY mpt.talent_id, mpc.card_id, COALESCE(mpc.card_level, 0)
  `);
}

async function rebuildSkinCounts(client: { query: (sql: string, params?: unknown[]) => Promise<unknown> }): Promise<void> {
  await client.query('DELETE FROM skin_counts_ranked');
  await client.query(`
    INSERT INTO skin_counts_ranked (
      champion_id, skin_id, league_tier, skin_name, count, wins, losses, updated_at
    )
    SELECT
      mp.champion_id,
      mp.skin_id,
      mlt.lobby_tier,
      MAX(COALESCE(NULLIF(mp.skin_name, ''), s.skin_name, 'Unknown Skin')),
      COUNT(*)::INT,
      COUNT(*) FILTER (WHERE ${NORMALIZED_WIN_SQL})::INT,
      COUNT(*) FILTER (WHERE ${NORMALIZED_LOSS_SQL})::INT,
      now()
    FROM match_players mp
    JOIN matches m
      ON m.match_id = mp.match_id
     AND m.entry_datetime = mp.entry_datetime
    JOIN match_lobby_tiers mlt
      ON mlt.match_id = m.match_id
     AND mlt.entry_datetime = m.entry_datetime
    LEFT JOIN skins s ON s.skin_id = mp.skin_id
    WHERE m.queue_id = 486
      AND COALESCE(m.limited, false) = false
      AND mp.champion_id > 0
      AND mp.skin_id IS NOT NULL
      AND mp.skin_id > 0
      AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
    GROUP BY
      mp.champion_id,
      mp.skin_id,
      mlt.lobby_tier
  `);
}

async function rebuildMatchLobbyTiers(client: { query: (sql: string, params?: unknown[]) => Promise<unknown> }): Promise<void> {
  await client.query('DELETE FROM match_lobby_tiers');
  await client.query(`
    INSERT INTO match_lobby_tiers (match_id, entry_datetime, lobby_tier, known_players, updated_at)
    SELECT m.match_id, m.entry_datetime,
      COALESCE(ROUND(AVG(mp.league_tier) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)), 0)::SMALLINT,
      COUNT(*) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)::SMALLINT,
      now()
    FROM matches m
    LEFT JOIN match_players mp ON mp.match_id = m.match_id AND mp.entry_datetime = m.entry_datetime
      AND mp.player_id > 0 AND mp.champion_id > 0
    WHERE m.queue_id = 486
      AND COALESCE(m.limited, false) = false
    GROUP BY m.match_id, m.entry_datetime
  `);
}

async function rebuildChampionStatsRanked(client: { query: (sql: string, params?: unknown[]) => Promise<unknown> }): Promise<void> {
  await client.query('DELETE FROM champion_stats_ranked');
  await client.query(`
    WITH player_agg AS (
      SELECT
        mp.champion_id,
        COALESCE(c.name, 'Champion ' || mp.champion_id::TEXT) AS champion_name,
        COUNT(*)::INT AS total_matches,
        -- Match facts have historically used both Winner/Loser and
        -- Win/Loss depending on whether the row came from direct match
        -- detail or match-history recovery. Champion-rate projections must
        -- normalize both spellings; otherwise valid outcomes are ignored while
        -- total_matches still grows, pushing every displayed win rate below
        -- reality.
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::INT AS wins,
        COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::INT AS losses,
        COALESCE(SUM(mp.kills), 0)::INT AS sum_kills,
        COALESCE(SUM(mp.deaths), 0)::INT AS sum_deaths,
        COALESCE(SUM(mp.assists), 0)::INT AS sum_assists,
        COALESCE(SUM(mp.damage_done_physical), 0)::INT AS sum_damage,
        COALESCE(SUM(mp.gold_earned), 0)::INT AS sum_gold,
        COALESCE(SUM(mp.healing), 0)::INT AS sum_heal,
        COALESCE(SUM(mp.damage_mitigated), 0)::INT AS sum_mitigation,
        COALESCE(SUM(mp.league_tier) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26), 0)::INT AS sum_league_tier,
        COUNT(*) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)::INT AS league_tier_count
      FROM match_players mp
      JOIN matches m ON m.match_id = mp.match_id
      LEFT JOIN champions c ON c.id = mp.champion_id
      WHERE m.queue_id = 486
        AND COALESCE(m.limited, false) = false
        AND mp.champion_id > 0
        AND COALESCE(mp.source, 'direct') IN ('direct', 'recovered')
      GROUP BY mp.champion_id, COALESCE(c.name, 'Champion ' || mp.champion_id::TEXT)
    ),
    ban_agg AS (
      SELECT
        mb.champion_id,
        COALESCE(c.name, 'Champion ' || mb.champion_id::TEXT) AS champion_name,
        COUNT(*)::INT AS ban_total,
        COUNT(*) FILTER (WHERE mb.ban_slot = 1)::INT AS slot1,
        COUNT(*) FILTER (WHERE mb.ban_slot = 2)::INT AS slot2,
        COUNT(*) FILTER (WHERE mb.ban_slot = 3)::INT AS slot3,
        COUNT(*) FILTER (WHERE mb.ban_slot = 4)::INT AS slot4,
        COUNT(*) FILTER (WHERE mb.ban_slot = 5)::INT AS slot5,
        COUNT(*) FILTER (WHERE mb.ban_slot = 6)::INT AS slot6,
        COUNT(*) FILTER (WHERE mb.ban_slot = 7)::INT AS slot7,
        COUNT(*) FILTER (WHERE mb.ban_slot = 8)::INT AS slot8
      FROM match_bans mb
      JOIN matches m ON m.match_id = mb.match_id
      LEFT JOIN champions c ON c.id = mb.champion_id
      WHERE m.queue_id = 486
        AND COALESCE(m.limited, false) = false
        AND mb.champion_id > 0
      GROUP BY mb.champion_id, COALESCE(c.name, 'Champion ' || mb.champion_id::TEXT)
    ),
    merged AS (
      SELECT
        COALESCE(p.champion_id, b.champion_id) AS champion_id,
        COALESCE(p.champion_name, b.champion_name) AS champion_name,
        COALESCE(p.total_matches, 0) AS total_matches,
        COALESCE(p.wins, 0) AS wins,
        COALESCE(p.losses, 0) AS losses,
        COALESCE(p.sum_kills, 0) AS sum_kills,
        COALESCE(p.sum_deaths, 0) AS sum_deaths,
        COALESCE(p.sum_assists, 0) AS sum_assists,
        COALESCE(p.sum_damage, 0) AS sum_damage,
        COALESCE(p.sum_gold, 0) AS sum_gold,
        COALESCE(p.sum_heal, 0) AS sum_heal,
        COALESCE(p.sum_mitigation, 0) AS sum_mitigation,
        COALESCE(p.sum_league_tier, 0) AS sum_league_tier,
        COALESCE(p.league_tier_count, 0) AS league_tier_count,
        COALESCE(b.ban_total, 0) AS ban_total,
        COALESCE(b.slot1, 0) AS slot1,
        COALESCE(b.slot2, 0) AS slot2,
        COALESCE(b.slot3, 0) AS slot3,
        COALESCE(b.slot4, 0) AS slot4,
        COALESCE(b.slot5, 0) AS slot5,
        COALESCE(b.slot6, 0) AS slot6,
        COALESCE(b.slot7, 0) AS slot7,
        COALESCE(b.slot8, 0) AS slot8
      FROM player_agg p
      FULL OUTER JOIN ban_agg b ON b.champion_id = p.champion_id
    )
    INSERT INTO champion_stats_ranked (
      champion_id, champion_name, total_matches, wins, losses,
      sum_kills, sum_deaths, sum_assists, sum_damage, sum_gold, sum_heal,
      sum_mitigation, sum_league_tier, league_tier_count, ban_total,
      slot1, slot2, slot3, slot4, slot5, slot6, slot7, slot8,
      win_rate, pick_rate, ban_rate, kda, updated_at
    )
    SELECT
      champion_id,
      champion_name,
      total_matches,
      wins,
      losses,
      sum_kills,
      sum_deaths,
      sum_assists,
      sum_damage,
      sum_gold,
      sum_heal,
      sum_mitigation,
      sum_league_tier,
      league_tier_count,
      ban_total,
      slot1, slot2, slot3, slot4, slot5, slot6, slot7, slot8,
      CASE WHEN (wins + losses) > 0 THEN ROUND(wins::NUMERIC / (wins + losses)::NUMERIC * 100, 2) ELSE NULL END AS win_rate,
      CASE WHEN SUM(total_matches) OVER () > 0 THEN ROUND(total_matches::NUMERIC / SUM(total_matches) OVER (), 4) ELSE NULL END AS pick_rate,
      CASE WHEN SUM(ban_total) OVER () > 0 THEN ROUND(ban_total::NUMERIC / SUM(ban_total) OVER (), 4) ELSE NULL END AS ban_rate,
      ROUND((sum_kills + sum_assists / 2.0)::NUMERIC / GREATEST(sum_deaths, 1), 2) AS kda,
      now()
    FROM merged
  `);
}

export async function refreshDerivedProjectionsWithJob(source: RefreshSource = 'manual'): Promise<{ jobId: number; counts: ProjectionCounts }> {
  const job = await one(`
    INSERT INTO sync_jobs (job_type, status, started_at)
    VALUES ('derived_projection_tracker', 'running', now())
    RETURNING id
  `);

  try {
    console.log(`[derived-projection-tracker] Rebuilding derived projections (${source})...`);
    const counts = await rebuildDerivedProjections();
    await one(`
      UPDATE sync_jobs
      SET status = 'completed',
          completed_at = now(),
          matches_processed = $1,
          players_processed = $2
      WHERE id = $3
    `, [counts.hourly_match_counts || 0, counts.champion_stats_ranked || 0, job.id]);
    return { jobId: Number(job.id), counts };
  } catch (err) {
    await one(`
      UPDATE sync_jobs
      SET status = 'failed',
          completed_at = now(),
          error_message = $1
      WHERE id = $2
    `, [String(err), job.id]);
    throw err;
  }
}
