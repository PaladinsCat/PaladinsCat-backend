const pg = require('pg');

// Connect directly to PostgreSQL (same credentials as Docker container)
const client = new pg.Client({
  host: 'localhost',
  port: 5432,
  database: 'paladinscat',
  user: 'paladins',
  password: 'paladinsdev'
});

async function main() {
  await client.connect();
  console.log('[buffer-manual] Connected');

  let processed = 0;
  let failed = 0;

  while (true) {
    const result = await client.query(
      "SELECT id, raw_data, entity_type, entity_id FROM raw_ingest_buffer WHERE status = 'pending' ORDER BY created_at ASC LIMIT 100 FOR UPDATE SKIP LOCKED"
    );

    if (result.rows.length === 0) break;

    for (const row of result.rows) {
      try {
        // Mark as processing to prevent duplicate work
        await client.query("UPDATE raw_ingest_buffer SET status = 'processing' WHERE id = $1", [row.id]);

        if (row.entity_type === 'match') {
          const players = JSON.parse(row.raw_data);
          const matchId = parseInt(row.entity_id);

          for (const p of players) {
            // Insert player data using existing ON CONFLICT logic from buffer-processor.ts
            await client.query(`
              INSERT INTO match_players (
                match_id, player_id, player_name, region, champion_id, skin_id, skin_name,
                team_number, is_victory, kills, deaths, assists, gold_earned, damage_done_physical,
                healing_done_to_teammates, damage_mitigated_by_player, total_healing_received_from_teammates,
                entry_datetime, source, created_at
              ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,NOW(),'direct',NOW())
              ON CONFLICT (match_id, player_id) DO UPDATE SET
                afk_rate=EXCLUDED.afk_rate, egpm=EXCLUDED.egpm, mitigation_per_minute=EXCLUDED.mitigation_per_minute,
                kda=EXCLUDED.kda, damage_per_minute=EXCLUDED.damage_per_minute,
                healing_per_minute=EXCLUDED.healing_per_minute, healing_self_per_minute=EXCLUDED.healing_self_per_minute,
                source=EXCLUDED.source, created_at=EXCLUDED.created_at
            `, [
              matchId, p.player_id, p.player_name, 
              (p.region || '').toLowerCase().replace(/[^a-z]/g, ''),
              p.champion_id, p.skin_id, p.skin_name,
              p.team_number, p.is_victory, p.kills, p.deaths, p.assists,
              p.gold_earned, p.damage_done_physical, p.healing_done_to_teammates,
              p.damage_mitigated_by_player, p.total_healing_received_from_teammates
            ]);
          }
        }

        // Mark as processed and increment counter
        await client.query("UPDATE raw_ingest_buffer SET status = 'processed', processed_at = now() WHERE id = $1", [row.id]);
        processed++;

      } catch (err) {
        console.error(`[buffer-manual] Failed row ${row.id}: ${err.message}`);
        await client.query("UPDATE raw_ingest_buffer SET status = 'failed', error_message = $2 WHERE id = $1", [row.id, String(err)]);
        failed++;
      }
    }

    // Clean up processed/failed rows to keep buffer small
    await client.query("DELETE FROM raw_ingest_buffer WHERE status IN ('processed', 'failed')");
  }

  console.log(`[buffer-manual] Done! Processed: ${processed}, Failed: ${failed}`);
  await client.end();
}

main().catch(e => { console.error('[buffer-manual] Fatal:', e.message); process.exit(1); });