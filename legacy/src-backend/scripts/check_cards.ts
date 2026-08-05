require('dotenv').config();
const { pool } = require('./dist/config/db');

async function main() {
  // Check cards table
  const cardsResult = await pool.query('SELECT COUNT(*) as total, COUNT(CASE WHEN champion_id IS NOT NULL THEN 1 END) as has_champ FROM cards');
  console.log('Cards table:', cardsResult.rows[0]);
  
  // Check for Androxus cards
  const androxus = await pool.query('SELECT * FROM cards WHERE champion_id = 2205 LIMIT 5');
  console.log('Androxus cards:', androxus.rows);
  
  // Check some random card IDs from match_player_cards
  const randomCards = await pool.query('SELECT DISTINCT card_id, COUNT(*) as cnt FROM match_player_cards WHERE card_id IN (SELECT card_id FROM match_player_cards JOIN match_players mp ON mp.match_id = mpc.match_id AND mp.player_id = mpc.player_id WHERE mp.champion_id = 2205) GROUP BY card_id ORDER BY cnt DESC LIMIT 10');
  console.log('Cards from Androxus matches:', randomCards.rows);
  
  // Check if cards table is populated at all
  const sample = await pool.query('SELECT * FROM cards LIMIT 5');
  console.log('Sample cards rows:', sample.rows);
  
  // Check items table for Androxus cards
  const items = await pool.query('SELECT item_id, item_name, champion_id FROM items WHERE champion_id = 2205 AND item_type = $1 LIMIT 10', ['burn_card']);
  console.log('Items for Androxus:', items.rows);
  
  await pool.end();
}

main().catch(e => { console.error(e); process.exit(1); });
