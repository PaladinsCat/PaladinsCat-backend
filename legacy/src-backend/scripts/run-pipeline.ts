import { query, one } from '../config/db';
import { populate, populateAll, getStatus } from '../workers/match-discovery';
import { ingestBatch } from '../workers/match-ingestion';
import { processBufferBatch } from '../workers/buffer-processor';
import {
  getChampions,
  getEsportsProLeagueDetails,
  getItems,
} from '../services/hirez';

const args = process.argv.slice(2);
const cmd = args[0];

function parseArgs() {
  const opts: any = {};
  for (let i = 1; i < args.length; i++) {
    if (args[i].startsWith('--')) {
      opts[args[i].replace('--', '')] = args[i + 1];
      i++;
    }
  }
  return opts;
}

/**
 * Ingest static data (champions, items, esports) from Hi-Rez API.
 * The operator script is a relay consumer; it never signs or sends a Hi-Rez
 * request itself. Fetches raw data through the native relay, writes to the
 * durable buffer, then processes it through the normal backend path.
 */
async function ingestStaticData(type: 'champions' | 'items' | 'esports'): Promise<void> {
  const endpointMap: Record<string, {
    endpoint: string;
    entityType: string;
    fetch: () => Promise<any[]>;
  }> = {
    champions: {
      endpoint: 'getchampions',
      entityType: 'champion',
      fetch: () => getChampions('operator_static_ingest'),
    },
    items: {
      endpoint: 'getitems',
      entityType: 'item',
      fetch: () => getItems('operator_static_ingest'),
    },
    esports: {
      endpoint: 'getesportsproleaguedetails',
      entityType: 'esports',
      fetch: () => getEsportsProLeagueDetails('operator_static_ingest'),
    },
  };

  const config = endpointMap[type];
  if (!config) throw new Error(`Unknown ingest type: ${type}`);

  console.log(`[INGEST] Fetching ${type} from ${config.endpoint}...`);
  const rawData = await config.fetch();

  if (!rawData || (Array.isArray(rawData) && rawData.length === 0)) {
    console.log(`[INGEST] No data returned for ${type}`);
    return;
  }

  const data = Array.isArray(rawData) ? rawData : [rawData];
  console.log(`[INGEST] Received ${data.length} raw ${type} entries`);

  // Write raw data to buffer
  for (const item of data) {
    const entityId = item.id || item.ItemId || item.LeagueId || null;
    await one(`INSERT INTO raw_ingest_buffer (raw_data, endpoint, entity_type, entity_id)
      VALUES ($1, $2, $3, $4)`,
      [JSON.stringify(item), config.endpoint, config.entityType, entityId]);
  }
  console.log(`[INGEST] Wrote ${data.length} entries to buffer`);

  // Process buffer entries
  const result = await processBufferBatch(data.length);
  console.log(`[INGEST] Processed: ${result.processed}, Failed: ${result.failed}`);
}

async function main() {
  const opts = parseArgs();

  switch (cmd) {
    case 'populate':
      if (opts.queue && opts.from && opts.to) {
        const date = opts.from.replace(/-/g, '');
        const hour = parseInt(opts.from.split('T')[1]?.split(':')[0] || '0');
        const count = await populate(parseInt(opts.queue), date, hour);
        console.log(`Populated ${count} matches`);
      } else {
        const date = (opts.from || '2026-05-01').replace(/-/g, '');
        const hour = parseInt((opts.from || '2026-05-01').split('T')[1]?.split(':')[0] || '0');
        const total = await populateAll(date, hour);
        console.log(`Populated ${total} matches across all queues`);
      }
      break;

    case 'ingest': {
      const ingestType = opts.type || 'matches';
      if (ingestType === 'champions' || ingestType === 'items' || ingestType === 'esports') {
        await ingestStaticData(ingestType);
      } else {
        const batchSize = parseInt(opts.batchSize || '10');
        const limit = parseInt(opts.limit || '100');
        let ingested = 0;
        let failed = 0;
        for (let i = 0; i < limit; i++) {
          const result = await ingestBatch(batchSize);
          ingested += result.ingested;
          failed += result.failed;
          if (result.ingested === 0) break;
        }
        console.log(`Buffered: ${ingested}, Failed: ${failed}`);
      }
      break;
    }

    case 'ingest-champions': {
      await ingestStaticData('champions');
      break;
    }

    case 'ingest-items': {
      await ingestStaticData('items');
      break;
    }

    case 'ingest-esports': {
      await ingestStaticData('esports');
      break;
    }

    case 'ingest-matches': {
      const batchSize = parseInt(opts.batchSize || '10');
      const limit = parseInt(opts.limit || '100');
      let ingested = 0;
      let failed = 0;
      for (let i = 0; i < limit; i++) {
        const result = await ingestBatch(batchSize);
        ingested += result.ingested;
        failed += result.failed;
        if (result.ingested === 0) break;
      }
      console.log(`Buffered: ${ingested}, Failed: ${failed}`);
      break;
    }

    case 'process': {
      const batchSize = parseInt(opts.batchSize || '50');
      const limit = parseInt(opts.limit || '100');
      let processed = 0;
      let failed = 0;
      for (let i = 0; i < limit; i++) {
        const result = await processBufferBatch(batchSize);
        processed += result.processed;
        failed += result.failed;
        if (result.processed + result.failed + result.deferred === 0) break;
      }
      console.log(`Processed: ${processed}, Failed: ${failed}`);
      break;
    }

    case 'pipeline': {
      // Full pipeline: ingest → process → repeat
      const ingestBatchSize = parseInt(opts.ingestBatchSize || '10');
      const processBatchSize = parseInt(opts.processBatchSize || '50');
      const cycles = parseInt(opts.cycles || '10');

      console.log(`Running full pipeline: ${cycles} cycles`);
      for (let i = 0; i < cycles; i++) {
        const ingestResult = await ingestBatch(ingestBatchSize);
        const processResult = await processBufferBatch(processBatchSize);
        console.log(`Cycle ${i + 1}: buffered ${ingestResult.ingested}, processed ${processResult.processed}`);
        if (
          ingestResult.ingested === 0
          && processResult.processed + processResult.failed + processResult.deferred === 0
        ) break;
      }
      break;
    }

    case 'check':
      const status = await getStatus();
      console.log('Pull list status:', status);
      break;

    case 'fix':
      await one(`UPDATE match_pull_list SET status = 'pending' WHERE status = 'pulling'`);
      console.log('Reset all pulling matches to pending');
      break;

    case 'buffer-status': {
      const pending = await one('SELECT COUNT(*) as count FROM raw_ingest_buffer WHERE status = \'pending\'');
      const processing = await one('SELECT COUNT(*) as count FROM raw_ingest_buffer WHERE status = \'processing\'');
      const processed = await one('SELECT COUNT(*) as count FROM raw_ingest_buffer WHERE status = \'processed\'');
      const failed = await one('SELECT COUNT(*) as count FROM raw_ingest_buffer WHERE status = \'failed\'');
      const byType = await query('SELECT entity_type, endpoint, COUNT(*) as count FROM raw_ingest_buffer GROUP BY entity_type, endpoint ORDER BY entity_type, endpoint');
      console.log({ pending: pending?.count, processing: processing?.count, processed: processed?.count, failed: failed?.count, byType });
      break;
    }

    case 'status': {
      const matches = await one('SELECT COUNT(*) as count FROM matches');
      const players = await one('SELECT COUNT(*) as count FROM players');
      const pullList = await getStatus();
      const buffer = await one('SELECT status, COUNT(*) as count FROM raw_ingest_buffer GROUP BY status');
      const bufferByType = await query('SELECT entity_type, COUNT(*) as count FROM raw_ingest_buffer GROUP BY entity_type');
      console.log({ matches: matches?.count, players: players?.count, pullList, buffer, bufferByType });
      break;
    }

    default:
      console.log('Usage: run-pipeline.ts <command> [options]');
      console.log('Commands:');
      console.log('  populate        — Fetch match IDs and populate pull list');
      console.log('  ingest          — Fetch match data and dump to buffer table');
      console.log('  ingest-champions — Ingest champion data from Hi-Rez API');
      console.log('  ingest-items     — Ingest item data from Hi-Rez API');
      console.log('  ingest-esports   — Ingest esports data from Hi-Rez API');
      console.log('  process         — Process buffer table → normalized tables');
      console.log('  pipeline        — Full pipeline: ingest + process (cycles)');
      console.log('  check           — Check pull list status');
      console.log('  fix             — Reset stuck pulling matches to pending');
      console.log('  buffer-status   — Show buffer table stats');
      console.log('  status          — Full system status');
      console.log('Options: --queue, --from, --to, --region, --batch-size, --limit, --cycles, --type');
      process.exit(1);
  }
}

main().catch(console.error);
