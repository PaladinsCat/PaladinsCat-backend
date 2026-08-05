/**
 * Manual buffer processor — runs directly against PostgreSQL to process pre-fetched payloads
 * from raw_ingest_buffer into match_players. No external API calls involved; all data is already
 * in the buffer's raw_data arrays (previously fetched by discovery/gap-checker).
 * 
 * This script bypasses the Fastify worker and processes entries directly via pg client:
 * - Reads pending rows with FOR UPDATE SKIP LOCKED for concurrency safety
 * - Inserts player data into match_players using ON CONFLICT (match_id, player_id) DO UPDATE SET
 * - Marks buffer rows as processed and deletes them after completion
 */

import { Client } from 'pg';

const client = new Client({
  host: process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.DB_PORT || '5432'),
  database: process.env.DB_NAME || 'paladinscat',
  user: process.env.DB_USER || 'paladins',
  password: 'paladinsdev',
});

async function main() {
  await client.connect();
  console.log('[buffer-manual] Connected to PostgreSQL');

  let processed = 0;
  let failed = 0;

  // Process in batches of 100 rows at a time until buffer is empty or no more pending rows exist
  while (true) {
    const result = await client.query(`
      SELECT id, raw_data, endpoint, entity_type, entity_id 
      FROM raw_ingest_buffer 
      WHERE status = 'pending' ORDER BY created_at ASC LIMIT 100 FOR UPDATE SKIP LOCKED
    `);

    if (result.rows.length === 0) {
      console.log(`[buffer-manual] No more pending rows. Total processed: ${processed}, failed: ${failed}`);
      break;
    }

    for (const row of result.rows) {
      try {
        await client.query('UPDATE raw_ingest_buffer SET status = \'processing\' WHERE id = $1', [row.id]);

        // Only process match-type payloads — ignore other entity types
        if (row.entity_type === 'match') {
          const players = row.raw_data;
          const matchId = parseInt(row.entity_id);

          for (const player of players) {
            await client.query(`
              INSERT INTO match_players (
                match_id, player_id, player_name, region, champion_id, skin_id, skin_name,
                team_number, is_victory, kills, deaths, assists, gold_earned, damage_done_physical,
                healing_done_to_teammates, damage_mitigated_by_player, total_healing_received_from_teammates,
                entry_datetime, source, created_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, NOW(), 'direct', NOW())
              ON CONFLICT (match_id, player_id) DO UPDATE SET
                afk_rate = EXCLUDED.afk_rate, egpm = EXCLUDED.egpm, mitigation_per_minute = EXCLUDED.mitigation_per_minute,
                kda = EXCLUDED.kda, damage_per_minute = EXCLUDED.damage_per_minute,
                healing_per_minute = EXCLUDED.healing_per_minute, healing_self_per_minute = EXCLUDED.healing_self_per_minute,
                source = EXCLUDED.source, created_at = EXCLUDED.created_at
            `, [
              matchId, player.player_id, player.player_name, normalizeRegion(player.region || ''), 
              player.champion_id, player.skin_id, player.skin_name,
              player.team_number, player.is_victory, player.kills, player.deaths, player.assists,
              player.gold_earned, player.damage_done_physical, player.healing_done_to_teammates,
              player.damage_mitigated_by_player, player.total_healing_received_from_teammates,
            ]);
          }
        }

        await client.query('UPDATE raw_ingest_buffer SET status = \'processed\', processed_at = now() WHERE id = $1', [row.id]);
        processed++;
      } catch (err) {
        const retry = (row.retry_count || 0) + 1;
        if (retry >= 3) {
          await client.query('UPDATE raw_ingest_buffer SET status = \'failed\', retry_count = $1, error_message = $2 WHERE id = $3', [retry, String(err), row.id]);
        } else {
          await client.query('UPDATE raw_ingest_buffer SET status = \'pending\', retry_count = $1, error_message = $2 WHERE id = $3', [retry, String(err), row.id]);
        }
        failed++;
      }
    }

    // Delete processed/failed rows after each batch to keep buffer small
    await client.query("DELETE FROM raw_ingest_buffer WHERE status IN ('processed', 'failed')");
  }

  await client.end();
}

function normalizeRegion(region: string): string {
  if (region === 'Unknown' || region === '') return '';
  const normalized = region.toLowerCase().replace(/[^a-z]/g, '');
  return ['na', 'eu', 'kr', 'br'].includes(normalized) ? normalized : '';
}

main().catch((err) => { console.error('[buffer-manual] Fatal:', err); process.exit(1); });