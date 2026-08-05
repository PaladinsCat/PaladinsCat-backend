import { FastifyInstance } from 'fastify';
import { query, one } from '../config/db';
import { registerReadThroughCache } from '../utils/route-cache';
import { internalRequestHeaders } from '../services/internal-request';
import { appendLobbyTierPredicate, lobbyTierQueryString, parseLobbyTierBounds } from '../utils/lobby-tier';
import {
  normalizeChampionCardStatsPayload,
  normalizeChampionTalentStatsPayload,
} from '../services/champion-page-contract';

async function resolveChampionId(value: unknown): Promise<number | null> {
  const text = String(value ?? '').trim();
  if (/^\d+$/.test(text)) return Number(text);

  // Public routes use URL slugs/reference names while the database keeps the
  // display spelling (for example "Mal'Damba"). Compare normalized names so
  // spaces, apostrophes, and hyphens do not require frontend ID duplication.
  const row = await one(`SELECT id FROM champions
    WHERE REGEXP_REPLACE(LOWER(name), '[^a-z0-9]+', '', 'g')
      = REGEXP_REPLACE(LOWER($1), '[^a-z0-9]+', '', 'g')`, [text]);
  return row ? Number(row.id) : null;
}

export default async function championsRoutes(fastify: FastifyInstance) {
  registerReadThroughCache(fastify, {
    namespace: 'route:champions:v3',
    // Public champion detail responses include several aggregate lookups and
    // are requested by every champion page. Cache the catalog, composition,
    // and one-segment detail route, but not mutable/admin subroutes.
    shouldCache: (req) => {
      const path = req.url.split('?')[0];
      return path === '/champions'
        || path === '/champions/overview'
        || /^\/champions\/[^/]+$/.test(path)
        || /^\/champions\/[^/]+\/page-data$/.test(path)
        || /^\/champions\/[^/]+\/talents\/\d+\/page-data$/.test(path);
    },
    ttlSeconds: () => 300,
  });

  /**
   * GET /champions/overview — Catalog and ranked aggregates for the main grid.
   * Keeping this composition server-side avoids two dependent browser requests
   * every time a visitor returns to the Champions top-level page.
   */
  fastify.get('/overview', async (req: any, reply: any) => {
    reply.header('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const scope = lobbyTierQueryString(lobbyTier);
    const statsScope = String(req.query.scope || 'ranked').trim().toLowerCase();
    const [catalogResponse, statsResponse] = await Promise.all([
      fastify.inject({ method: 'GET', url: '/champions', headers: internalRequestHeaders() }),
      fastify.inject({
        method: 'GET',
        url: `/stats/champions?limit=200&scope=${encodeURIComponent(statsScope)}${statsScope === 'ranked' && scope ? `&${scope}` : ''}`,
        headers: internalRequestHeaders(),
      }),
    ]);
    if (catalogResponse.statusCode >= 400) {
      return reply.status(catalogResponse.statusCode).send(catalogResponse.json());
    }
    return {
      champions: catalogResponse.json(),
      stats: statsResponse.statusCode < 400 ? statsResponse.json() : [],
    };
  });

  fastify.get('/', async () => {
    // Exclude Unknown/PRIVATEACCOUNT placeholder (id=0) from public champion list.
    // It exists internally for tracking matches with dropped/unrecognized player data.
    return query('SELECT id, name, title, health, speed, roles FROM champions WHERE id > 0 ORDER BY name');
  });

  fastify.get('/:id', async (req: any, reply) => {
    const id = await resolveChampionId((req.params as any).id);
    if (id == null) return reply.status(404).send({ error: 'Champion not found' });
    const champion = await one('SELECT * FROM champions WHERE id = $1', [id]);
    if (!champion) return reply.status(404).send({ error: 'Champion not found' });

    // Champion aggregate stats are now maintained in champion_stats_ranked by
    // buffer-processor.ts and can be rebuilt by derived-projection-tracker.ts.
    // Do not read champion_meta_stats here: that old MV is no longer refreshed
    // in the ingest path, so it can be missing or stale on upgraded databases.
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const stats = lobbyTier.active ? await (async () => {
      const params: any[] = [id];
      const where = ['mp.champion_id = $1', 'm.queue_id = 486', `COALESCE(mp.source, 'direct') IN ('direct', 'recovered')`];
      appendLobbyTierPredicate(lobbyTier, params, where);
      return one(`SELECT COUNT(*)::INT AS total_matches, COUNT(*)::INT AS total_plays,
          COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::INT AS wins,
          COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser', 'loss'))::INT AS losses,
          ROUND(100.0 * COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win'))::NUMERIC
            / NULLIF(COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner', 'win', 'loser', 'loss'))::NUMERIC, 0), 2) AS win_rate,
          ROUND(AVG(mp.kills)::NUMERIC, 2) AS avg_kills,
          ROUND(AVG(mp.deaths)::NUMERIC, 2) AS avg_deaths,
          ROUND(AVG(mp.assists)::NUMERIC, 2) AS avg_assists,
          ROUND(AVG(mp.damage_done_physical)::NUMERIC, 2) AS avg_damage,
          ROUND(AVG(mp.gold_earned)::NUMERIC, 2) AS avg_gold,
          ROUND(AVG(mp.league_tier) FILTER (WHERE mp.league_tier BETWEEN 1 AND 26)::NUMERIC, 2) AS avg_league_tier
        FROM match_players mp
        JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
        JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        WHERE ${where.join(' AND ')}`, params);
    })() : await one(`
      SELECT
        total_matches,
        total_matches AS total_plays,
        wins,
        losses,
        win_rate,
        CASE WHEN total_matches > 0 THEN ROUND(sum_kills::NUMERIC / total_matches, 2) ELSE NULL END AS avg_kills,
        CASE WHEN total_matches > 0 THEN ROUND(sum_deaths::NUMERIC / total_matches, 2) ELSE NULL END AS avg_deaths,
        CASE WHEN total_matches > 0 THEN ROUND(sum_assists::NUMERIC / total_matches, 2) ELSE NULL END AS avg_assists,
        CASE WHEN total_matches > 0 THEN ROUND(sum_damage::NUMERIC / total_matches, 2) ELSE NULL END AS avg_damage,
        CASE WHEN total_matches > 0 THEN ROUND(sum_gold::NUMERIC / total_matches, 2) ELSE NULL END AS avg_gold,
        CASE WHEN league_tier_count > 0 THEN ROUND(sum_league_tier::NUMERIC / league_tier_count, 2) ELSE NULL END AS avg_league_tier
      FROM champion_stats_ranked
      WHERE champion_id = $1
    `, [id]);

    const tierRatings = await query('SELECT tier, rating, deviation, matches_played FROM champion_tier_ratings WHERE champion_id = $1 ORDER BY tier', [id]);

    return { champion, stats, tierRatings };
  });

  /**
   * GET /champions/:id/page-data — Cached data bundle for the champion page.
   *
   * The old client-side page fanned one visit into the champion record, talent
   * data, item data, map data, global performance data, and seven independent
   * metric distributions. Compose those existing public contracts here so a
   * warm champion page becomes one Redis-served request instead of repeatedly
   * reaching the API for every card on the page.
   */
  fastify.get('/:id/page-data', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const resolvedId = await resolveChampionId(req.params.id);
    // Frontend roster IDs are deliberately lightweight list keys, not Hi-Rez
    // champion IDs. Accept the canonical champion name here and resolve it
    // before composing numeric stats routes so every consumer uses one source
    // of truth for the real ID.
    if (resolvedId == null) return reply.status(404).send({ error: 'Champion not found' });
    const id = String(resolvedId);
    const scope = lobbyTierQueryString(lobbyTier);
    const scoped = (url: string) => scope ? `${url}${url.includes('?') ? '&' : '?'}${scope}` : url;
    const metricKeys = ['dpm', 'wpm', 'apm', 'gpm', 'hpm', 'mpm', 'kda'] as const;
    const routes = {
      detail: scoped(`/champions/${id}`),
      talents: scoped(`/stats/talents/${id}?mode=ranked`),
      items: scoped(`/stats/items?mode=ranked&championId=${id}&limit=200`),
      maps: scoped(`/stats/champions/${id}/maps`),
      performance: scoped('/stats/performance-metrics?queueId=486'),
      ...Object.fromEntries(metricKeys.map((metric) => [
        metric,
        scoped(`/stats/performance-metrics/by-champion?metric=${metric}&championId=${id}&queueId=486`),
      ])),
    } as Record<string, string>;

    const entries = await Promise.all(Object.entries(routes).map(async ([key, url]) => {
      const response = await fastify.inject({ method: 'GET', url, headers: internalRequestHeaders() });
      return [key, response.statusCode < 400 ? response.json() : null] as const;
    }));
    const source = Object.fromEntries(entries) as Record<string, any>;
    if (!source.detail) return reply.status(404).send({ error: 'Champion not found' });

    const number = (value: unknown) => Number(value ?? 0);
    const metricSummary = (row: any) => ({
      min: number(row?.min), max: number(row?.max), mean: number(row?.mean), median: number(row?.median),
      mode: number(row?.mode), p10: number(row?.p10), p25: number(row?.p25), p75: number(row?.p75),
      p90: number(row?.p90), sampleSize: number(row?.sample_size ?? row?.sampleSize),
    });
    const items = Array.isArray(source.items) ? source.items.map((row: any) => ({
      itemId: number(row.item_id), itemName: String(row.item_name ?? ''),
      totalUsage: number(row.total_uses ?? row.total_usage), winRate: number(row.win_rate),
      pickRate: row.pick_rate == null ? undefined : number(row.pick_rate),
      slots: (row.slots ?? []).map((slot: any) => ({ slot: number(slot.slot), totalUses: number(slot.total_uses), wins: 0, losses: 0, winRate: number(slot.win_rate) })),
      levels: (row.levels ?? []).map((level: any) => ({ level: number(level.item_level), totalUses: number(level.total_uses), wins: 0, losses: 0, winRate: number(level.win_rate) })),
      breakdown: (row.breakdown ?? []).map((entry: any) => ({ slot: number(entry.slot), level: number(entry.item_level), totalUses: number(entry.total_uses), wins: 0, losses: 0, winRate: number(entry.win_rate), pickRate: entry.pick_rate == null ? undefined : number(entry.pick_rate) })),
    })) : [];
    const maps = Array.isArray(source.maps) ? source.maps.map((row: any) => ({
      name: String(row.map ?? ''), totalPlays: number(row.total_plays), wins: number(row.wins), losses: number(row.losses),
      winRate: number(row.win_rate), pickRate: number(row.pick_rate),
    })) : [];
    const performance = Object.fromEntries(Object.entries(source.performance ?? {}).map(([metric, row]) => [metric, metricSummary(row)]));
    const championPerformance = Object.fromEntries(metricKeys.flatMap((metric) => {
      const row = Array.isArray(source[metric]?.data) ? source[metric].data[0] : null;
      return row ? [[metric, {
        championId: number(row.champion_id), championName: String(row.champion_name ?? ''), className: String(row.class ?? ''),
        min: number(row.min), max: number(row.max), mean: number(row.mean), median: number(row.median), mode: number(row.mode),
        p10: number(row.p10), p90: number(row.p90), avgValue: number(row.avg_value), totalMatches: number(row.total_matches),
      }]] : [];
    }));

    reply.header('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
    return {
      champion: source.detail.champion ?? null,
      stats: source.detail.stats ?? null,
      talentStats: source.talents ? normalizeChampionTalentStatsPayload(source.talents) : null,
      items,
      maps,
      performance,
      championPerformance,
    };
  });

  /**
   * GET /champions/:id/talents/:talentId/page-data — Cached talent-page bundle.
   *
   * Talent detail pages previously made independent roster, talent-stat, and
   * card-stat requests in the browser. A cold card aggregate can take several
   * seconds, and either stats helper used to turn a timeout into an empty page.
   * Compose the two database-backed contracts atomically so the route cache and
   * champion page warmer can serve the complete page with the champion TTL.
   */
  fastify.get('/:id/talents/:talentId/page-data', async (req: any, reply: any) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const championId = await resolveChampionId(req.params.id);
    if (championId == null) return reply.status(404).send({ error: 'Champion not found' });

    const talentId = Number(req.params.talentId);
    if (!Number.isInteger(talentId) || talentId <= 0) {
      return reply.status(400).send({ error: 'Invalid talentId.' });
    }
    const talent = await one<{ talent_id: number }>(
      'SELECT talent_id FROM talents WHERE talent_id = $1 AND champion_id = $2',
      [talentId, championId],
    );
    if (!talent) return reply.status(404).send({ error: 'Talent not found for champion' });

    const scope = lobbyTierQueryString(lobbyTier);
    const scoped = (url: string) => scope ? `${url}${url.includes('?') ? '&' : '?'}${scope}` : url;
    const [talentsResponse, cardsResponse] = await Promise.all([
      fastify.inject({
        method: 'GET',
        url: scoped(`/stats/talents/${championId}?mode=ranked`),
        headers: internalRequestHeaders(),
      }),
      fastify.inject({
        method: 'GET',
        url: scoped(`/stats/cards/${championId}?mode=ranked&talentId=${talentId}`),
        headers: internalRequestHeaders(),
      }),
    ]);
    if (talentsResponse.statusCode >= 400 || cardsResponse.statusCode >= 400) {
      req.log.warn({
        championId,
        talentId,
        talentsStatusCode: talentsResponse.statusCode,
        cardsStatusCode: cardsResponse.statusCode,
      }, 'Talent page data composition failed');
      return reply.status(502).send({ error: 'Talent page data is temporarily unavailable' });
    }

    const talentStats = normalizeChampionTalentStatsPayload(talentsResponse.json());
    const cardStats = normalizeChampionCardStatsPayload(cardsResponse.json());
    const talentStat = (talentStats.talents ?? []).find(
      (entry: any) => Number(entry.talentId) === talentId,
    );
    if (!talentStat) return reply.status(404).send({ error: 'Talent statistics not found' });

    reply.header('Cache-Control', 'public, max-age=300, stale-while-revalidate=900');
    return {
      championId,
      talentId,
      totalMatches: Number(talentStats.totalMatches ?? 0),
      talentStat,
      cardStats,
    };
  });

  fastify.get('/tiers', async () => {
    return query('SELECT * FROM champion_tier_ratings ORDER BY rating DESC LIMIT 50');
  });

  fastify.get('/:id/patch-history', async (req, reply) => {
    const id = parseInt((req.params as any).id);
    const patches = await query('SELECT * FROM patches ORDER BY release_date DESC');
    return { championId: id, patches };
  });

  fastify.get('/:id/counters', async (req: any, reply) => {
    const id = parseInt((req.params as any).id);
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    if (lobbyTier.active) {
      const params: any[] = [id];
      const where = ['mof.player_champion_id = $1', 'm.queue_id = 486'];
      appendLobbyTierPredicate(lobbyTier, params, where);
      return query(`SELECT mof.opponent_champion_id, c.name AS opponent_champion_name,
          (SUM(mof.wins) + SUM(mof.losses))::INT AS total_matchups,
          (SUM(mof.wins) + SUM(mof.losses))::INT AS total_encounters,
          SUM(mof.wins)::INT AS wins, SUM(mof.losses)::INT AS losses,
          ROUND(100.0 * SUM(mof.wins)::NUMERIC / NULLIF((SUM(mof.wins) + SUM(mof.losses))::NUMERIC, 0), 2) AS win_rate,
          NULL::NUMERIC AS avg_kills, NULL::NUMERIC AS avg_deaths, NULL::NUMERIC AS avg_dpm
        FROM match_opponent_facts mof
        JOIN matches m ON m.match_id = mof.match_id
        JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
        JOIN champions c ON c.id = mof.opponent_champion_id
        WHERE ${where.join(' AND ')}
        GROUP BY mof.opponent_champion_id, c.name ORDER BY win_rate DESC`, params);
    }
    const counters = await query(`SELECT opponent_champion_id, opponent_champion_name, total_matchups, total_encounters, wins, losses, win_rate, avg_kills, avg_deaths, avg_dpm
      FROM counter_pick_stats WHERE attacker_champion_id = $1 ORDER BY win_rate DESC`, [id]);
    return counters;
  });

  // Top 3 per class (roles) by win rate, minimum 50 plays
  fastify.get('/top-winrate', async (req: any, reply) => {
    const lobbyTier = parseLobbyTierBounds(req.query);
    if (!lobbyTier) return reply.status(400).send({ error: 'Tier bounds must be between 1 and 26.' });
    const scope = lobbyTierQueryString(lobbyTier);
    const statsResponse = await fastify.inject({ method: 'GET', url: `/stats/champions?limit=200${scope ? `&${scope}` : ''}`, headers: internalRequestHeaders() });
    if (statsResponse.statusCode >= 400) return reply.status(statsResponse.statusCode).send(statsResponse.json());
    const stats = statsResponse.json() as any[];
    const catalog = await query('SELECT id, name, roles FROM champions WHERE id > 0');
    const byId = new Map(stats.map((row) => [Number(row.champion_id), row]));
    const rows = catalog.map((champion: any) => ({
      ...champion,
      winRate: Number(byId.get(Number(champion.id))?.win_rate ?? 0),
      totalPlays: Number(byId.get(Number(champion.id))?.total_matches ?? 0),
    })).filter((row: any) => row.totalPlays >= 50)
      .sort((a: any, b: any) => b.winRate - a.winRate);

    // Normalize roles to consolidate fragments ('Paladins Flanker' + 'Paladins Flank' -> 'Flank')
    const normalizeRole = (r: string) => {
      if (!r) return 'Other';
      const m = r.match(/paladins?\s+(.*)/i);
      return m ? m[1].replace(/\s*er\s*$/, '').trim() : r;
    };

    // Keep only top 3 per normalized class group
    const grouped: Record<string, any[]> = {};
    for (const row of rows) {
      const key = normalizeRole(row.roles);
      if (!grouped[key]) grouped[key] = [];
      if (grouped[key].length < 3) grouped[key].push(row);
    }
    return Object.values(grouped).flat();
  });
}
