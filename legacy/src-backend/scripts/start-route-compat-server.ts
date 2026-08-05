import { shutdown as closeDatabase } from '../config/db';
import coplayRoutes from '../routes/coplay';
import championsRoutes from '../routes/champions';
import metaRoutes from '../routes/meta';
import esportsRoutes from '../routes/esports';
import notificationRoutes from '../routes/notifications';
import playerExtRoutes from '../routes/player-ext';
import publicOperationsRoutes from '../routes/public-operations';
import ratingsRoutes from '../routes/ratings';
import { rawApiResponsesRoutes } from '../routes/raw-api-responses';
import recoveryRoutes from '../routes/recovery';
import referenceRoutes from '../routes/reference';
import searchRoutes from '../routes/search';
import liveRoutes from '../routes/live';
import matchesRoutes from '../routes/matches';
import { close as closeRedis } from '../services/cache';
import systemRoutes from '../routes/system';
import statsRoutes from '../routes/stats';
import {
  createApplicationServer,
  installApplicationErrorHandler,
  installApplicationFoundation,
} from '../services/application-foundation';
import { initializeDeploymentControl } from '../services/deployment-control';

/**
 * Private migration harness server.
 *
 * This intentionally registers only the route plugin under comparison. It
 * avoids production startup migrations, schedulers, cache warming, MeiliSearch,
 * and Hi-Rez calls while exercising the exact current TypeScript route and
 * PostgreSQL adapter. Full-app middleware parity is a separate cutover gate.
 */
const group = process.env.PALADINSCAT_ROUTE_COMPAT_GROUP || 'recovery';
const app = createApplicationServer(false);
installApplicationFoundation(app);

const port = Number(process.env.PALADINSCAT_ROUTE_COMPAT_PORT || 13305);

async function shutdown() {
  await app.close();
  await closeDatabase();
  await closeRedis();
}

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    void shutdown().finally(() => process.exit(0));
  });
}

async function main() {
  if (group === 'recovery') {
    app.register(recoveryRoutes, { prefix: '/recovery' });
  } else if (group === 'notifications') {
    app.register(notificationRoutes, { prefix: '/notifications' });
  } else if (group === 'esports') {
    app.register(esportsRoutes, { prefix: '/esports' });
  } else if (group === 'ratings') {
    app.register(ratingsRoutes, { prefix: '/ratings' });
  } else if (group === 'reference') {
    app.register(referenceRoutes, { prefix: '/reference' });
  } else if (group === 'public-operations') {
    app.register(publicOperationsRoutes, { prefix: '/operations' });
  } else if (group === 'coplay') {
    app.register(coplayRoutes, { prefix: '/coplay' });
  } else if (group === 'meta') {
    app.register(metaRoutes, { prefix: '/meta' });
  } else if (group === 'package-c-edge') {
    app.register(searchRoutes, { prefix: '/search' });
    app.register(liveRoutes, { prefix: '/live' });
    app.register(rawApiResponsesRoutes, { prefix: '/api' });
  } else if (group === 'package-c-player-ext') {
    app.register(playerExtRoutes, { prefix: '/player-ext' });
  } else if (group === 'package-c-matches') {
    app.register(matchesRoutes, { prefix: '/matches' });
  } else if (
    group === 'stats-foundation'
    || group === 'stats-summaries'
    || group === 'stats-items'
    || group === 'package-b-remaining'
  ) {
    app.register(statsRoutes, { prefix: '/stats' });
    if (group === 'package-b-remaining') {
      app.register(championsRoutes, { prefix: '/champions' });
    }
  } else if (group === 'foundation') {
    app.register(systemRoutes, { prefix: '/' });
    app.register(referenceRoutes, { prefix: '/reference' });
    app.register(recoveryRoutes, { prefix: '/recovery' });
  } else {
    throw new Error(`Unsupported route compatibility group: ${group}`);
  }
  installApplicationErrorHandler(app);
  await initializeDeploymentControl();
  await app.listen({ host: '127.0.0.1', port });
  process.stdout.write(`route compatibility server listening on 127.0.0.1:${port}\n`);
}

void main().catch((error) => {
  console.error(error);
  process.exit(1);
});
