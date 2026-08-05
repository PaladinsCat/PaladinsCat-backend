export const ACTIVITY_API_WARM_URLS = [
  '/matches/overview?view=activity-v3',
  '/stats/presence?view=activity-v4',
] as const;

export const DEPLOYMENT_CRITICAL_API_WARM_URLS = [
  '/champions/overview',
  '/players/overview',
  '/matches/overview',
  '/stats/overview',
  ...ACTIVITY_API_WARM_URLS,
] as const;

const PERFORMANCE_LEADERBOARD_SECTIONS = [
  { metric: 'gpm' },
  { metric: 'hpm', role: 'Support' },
  { metric: 'dpm', role: 'Damage' },
  { metric: 'mpm', role: 'Frontline' },
] as const;
const PERFORMANCE_DASHBOARD_METRICS = ['dpm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda'] as const;
const LOBBY_TIER_SCOPES = ['tierMin=1&tierMax=15', 'tierMin=16&tierMax=26', 'tierMin=21&tierMax=26'] as const;
const TIER_SCOPED_FIRST_VIEWS = [
  '/stats/overview',
  '/stats/page-data',
  '/stats/champions?sort=winrate&limit=100',
  '/stats/items?mode=ranked&limit=200',
  '/stats/maps?queueId=486&limit=100',
  '/stats/platforms',
  '/stats/baselines?queueId=486',
] as const;

/**
 * Canonical first-view API keys for public directory and statistics pages.
 * These are backend route-cache keys, not rendered-page paths. Rendered page
 * discovery is intentionally sitemap-driven so new main pages need no worker
 * change as long as their sitemap priority meets the configured threshold.
 */
export const MAIN_API_WARM_URLS = [...new Set<string>([
  // Activity is a real-time operational surface. Warm its exact browser cache
  // identities first so the worker cannot defer them behind lower-priority
  // statistics when public traffic arrives during a warm cycle.
  ...ACTIVITY_API_WARM_URLS,
  // Keep these URLs byte-for-byte aligned with the /players/performance
  // client requests. Route-cache keys include the query string, so warming a
  // global HPM/DPM/MPM response does not warm the role-scoped page sections.
  ...PERFORMANCE_LEADERBOARD_SECTIONS.flatMap((section) => {
    const { metric } = section;
    const role = 'role' in section ? section.role : undefined;
    const roleQuery = role ? `&role=${role}` : '';
    return [
      `/players/leaderboard/performance?metric=${metric}&limit=100${roleQuery}&queueId=486&scope=ranked`,
      `/stats/performance-metrics?metric=${metric}${roleQuery}&queueId=486&scope=ranked`,
      `/players/leaderboard/performance?metric=${metric}&limit=100${roleQuery}&scope=casual`,
      `/stats/performance-metrics?metric=${metric}${roleQuery}&scope=casual`,
    ];
  }),
  '/players/leaderboard/class?role=Frontline&limit=100&queueId=486&mode=account',
  '/players/leaderboard/champion-elo?limit=100&queueId=486',
  '/stats/ranked-leaderboard?tier=26&top=100',
  '/players/boosted?limit=100',
  '/matches/recent?limit=20',
  '/matches/compositions?limit=200',
  ...PERFORMANCE_DASHBOARD_METRICS.flatMap((metric) => [
    `/stats/performance-metrics?metric=${metric}&includeRoles=1`,
    `/stats/performance-metrics/by-champion?metric=${metric}`,
  ]),
  '/stats/champions?sort=winrate&limit=100',
  '/stats/regions',
  '/stats/platforms',
  '/stats/loadouts',
  '/stats/items?mode=ranked&limit=200',
  '/stats/maps?queueId=486&limit=100',
  '/stats/skins?limit=200',
  '/stats/broken-skins',
  '/stats/talents',
  '/stats/cards?limit=200',
  '/stats/tiers?source=profiles',
  '/stats/tiers?source=matches',
  '/stats/tiers/summary',
  '/stats/baselines?queueId=486',
  ...LOBBY_TIER_SCOPES.flatMap((scope) => TIER_SCOPED_FIRST_VIEWS.map((url) => (
    `${url}${url.includes('?') ? '&' : '?'}${scope}`
  ))),
  '/meta/changelog?page=1&perPage=10',
  '/notifications?limit=5',
  // Refresh composite landing-page bundles after their leaf routes. This
  // ensures each bundle is rebuilt from the newly warmed first-view data.
  ...DEPLOYMENT_CRITICAL_API_WARM_URLS,
  '/stats/page-data',
])];

function decodeXml(value: string): string {
  return value
    .replaceAll('&amp;', '&')
    .replaceAll('&apos;', "'")
    .replaceAll('&quot;', '"')
    .replaceAll('&lt;', '<')
    .replaceAll('&gt;', '>');
}

/**
 * Return same-site paths whose sitemap priority marks them as primary pages.
 * Absolute origins are discarded; callers remap the path to the private
 * frontend service so background warming never traverses public ingress.
 */
export function mainPageWarmPaths(sitemapXml: string, minimumPriority = 0.8): string[] {
  const paths: string[] = [];
  const seen = new Set<string>();
  for (const match of sitemapXml.matchAll(/<url>([\s\S]*?)<\/url>/gi)) {
    const entry = match[1];
    const location = entry.match(/<loc>([\s\S]*?)<\/loc>/i)?.[1]?.trim();
    const priorityText = entry.match(/<priority>([\s\S]*?)<\/priority>/i)?.[1]?.trim();
    const priority = Number(priorityText);
    if (!location || !Number.isFinite(priority) || priority < minimumPriority) continue;
    try {
      const url = new URL(decodeXml(location));
      const path = `${url.pathname}${url.search}`;
      if (!path.startsWith('/') || seen.has(path)) continue;
      seen.add(path);
      paths.push(path);
    } catch {
      // Ignore malformed sitemap entries; the remaining canonical pages can
      // still be warmed and the aggregate worker log reports the final count.
    }
  }
  return paths;
}
