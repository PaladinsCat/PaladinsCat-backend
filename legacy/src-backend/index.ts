import dotenv from 'dotenv';
import path from 'path';
// Load production .env from project root, NOT from legacy/src-backend/
dotenv.config({ path: path.resolve(__dirname, '../../.env') });
import { healthCheck, one, shutdown as closeDatabase } from './config/db';
import { close as closeRedis } from './services/cache';
import systemRoutes from './routes/system';
import championsRoutes from './routes/champions';
import playersRoutes from './routes/players';
import matchesRoutes from './routes/matches';
import statsRoutes from './routes/stats';
import authRoutes from './routes/auth';
import buildsRoutes from './routes/builds';
import communityRoutes from './routes/community';
import tierListRoutes from './routes/tierlists';
import { rawApiResponsesRoutes } from './routes/raw-api-responses';
import referenceRoutes from './routes/reference';
import ratingsRoutes from './routes/ratings';
import coplayRoutes from './routes/coplay';
import esportsRoutes from './routes/esports';
import metaRoutes from './routes/meta';
import recoveryRoutes from './routes/recovery';
import adminRoutes from './routes/admin';
import adminChangelogRoutes from './routes/admin-changelog';
import adminNotificationRoutes from './routes/admin-notifications';
import adminDashboardRoutes from './routes/admin-dashboard';
import liveRoutes from './routes/live';
import playerExtRoutes from './routes/player-ext';
import searchRoutes from './routes/search';
import notificationsRoutes from './routes/notifications';
import siteAnalyticsRoutes from './routes/site-analytics';
import publicOperationsRoutes from './routes/public-operations';
import { initIndices } from './services/meilisearch';
import { applyRuntimeMigrations } from './utils/runtime-migrations';
import {
  backfillPrivateAccountIdentities,
  getPrivateBackfillReport,
} from './services/private-account-resolver';
import {
  configureDeploymentExpiryHandler,
  getLocalDeploymentState,
  initializeDeploymentControl,
  isDeploymentBlockingPhase,
} from './services/deployment-control';
import { quiesceBackendWork, startBackendWork } from './services/backend-lifecycle';
import {
  createApplicationServer,
  installApplicationErrorHandler,
  installApplicationFoundation,
} from './services/application-foundation';

const fastify = createApplicationServer(true);
installApplicationFoundation(fastify);

fastify.register(systemRoutes, { prefix: '/' });
fastify.register(championsRoutes, { prefix: '/champions' });
fastify.register(playersRoutes, { prefix: '/players' });
fastify.register(matchesRoutes, { prefix: '/matches' });
fastify.register(statsRoutes, { prefix: '/stats' });
fastify.register(authRoutes, { prefix: '/auth' });
fastify.register(buildsRoutes, { prefix: '/builds' });
fastify.register(communityRoutes, { prefix: '/community' });
fastify.register(tierListRoutes, { prefix: '/tierlists' });
fastify.register(rawApiResponsesRoutes, { prefix: '/api' });

// New routes
fastify.register(referenceRoutes, { prefix: '/reference' });
fastify.register(ratingsRoutes, { prefix: '/ratings' });
fastify.register(coplayRoutes, { prefix: '/coplay' });
fastify.register(esportsRoutes, { prefix: '/esports' });
fastify.register(metaRoutes, { prefix: '/meta' });
fastify.register(recoveryRoutes, { prefix: '/recovery' });
fastify.register(adminRoutes, { prefix: '/admin' });
fastify.register(adminChangelogRoutes, { prefix: '/admin' });
fastify.register(adminNotificationRoutes, { prefix: '/admin' });
fastify.register(adminDashboardRoutes, { prefix: '/admin' });
fastify.register(liveRoutes, { prefix: '/live' });
fastify.register(playerExtRoutes, { prefix: '/player-ext' });
fastify.register(searchRoutes, { prefix: '/search' });
fastify.register(notificationsRoutes, { prefix: '/notifications' });
fastify.register(siteAnalyticsRoutes, { prefix: '/analytics' });
fastify.register(publicOperationsRoutes, { prefix: '/operations' });

installApplicationErrorHandler(fastify);

async function start() {
  try {
    await initializeDeploymentControl();
    // Initialize MeiliSearch indices (players, matches) if they don't exist.
    // This prevents first-search crash on fresh deployments. The init function
    // handles the "index already exists" error gracefully.
    await initIndices();
    await applyRuntimeMigrations();
    const privateIdentitySchema = await one<{ table_name: string | null }>(
      `SELECT to_regclass('public.private_account_observations')::text AS table_name`,
    );
    // The serving API runs with schedulers disabled during the incremental
    // migration. Do not make it mutate private-identity facts before opening
    // its listener; the standalone scheduler owns that background work.
    if (process.env.BACKEND_SCHEDULERS_ENABLED !== 'false' && privateIdentitySchema?.table_name) {
      const privateIdentityReport = await getPrivateBackfillReport(false);
      if (
        privateIdentityReport.detailedUnresolved > 0
        || privateIdentityReport.legacyActive > 0
        || privateIdentityReport.outdatedActive > 0
        || privateIdentityReport.unlinkedMatchRows > 0
      ) {
        const repaired = await backfillPrivateAccountIdentities(true);
        fastify.log.info({ privateAccounts: repaired }, 'Private-account identity reconciliation complete');
      }
    }
    const dbOk = await healthCheck();
    if (!dbOk) {
      console.warn('Database health check failed');
    }
    const port = parseInt(process.env.PORT || '3005');
    await fastify.listen({ port, host: '0.0.0.0' });
    console.log(`Server running on port ${port}`);

    configureDeploymentExpiryHandler(() => startBackendWork(fastify));
    const deploymentState = getLocalDeploymentState();
    if (isDeploymentBlockingPhase(deploymentState.phase)) {
      fastify.log.info(
        { deploymentId: deploymentState.id, phase: deploymentState.phase },
        'Backend started quiesced for an in-progress deployment',
      );
    } else {
      await startBackendWork(fastify);
    }
  } catch (err) {
    fastify.log.error(err as Error);
    process.exit(1);
  }
}

let shuttingDown = false;
async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`${signal} received, shutting down gracefully`);
  try {
    // Stop accepting new sockets while the lifecycle drain waits for requests
    // already inside Fastify and for tracked workers to finish.
    await Promise.allSettled([
      fastify.close(),
      quiesceBackendWork(Number(process.env.SHUTDOWN_DRAIN_TIMEOUT_MS || 60_000)),
    ]);
    await Promise.allSettled([closeRedis(), closeDatabase()]);
  } finally {
    process.exit(0);
  }
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

start();
