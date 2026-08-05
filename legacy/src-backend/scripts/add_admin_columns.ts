import { Pool } from 'pg';

async function main() {
  const pool = new Pool({
    connectionString: 'postgresql://paladins:***@localhost:5433/paladinscat',
    max: 1,
  });

  try {
    console.log('Adding missing columns to users table...');
    await pool.query("ALTER TABLE users ADD COLUMN IF NOT EXISTS salt VARCHAR(64) NOT NULL DEFAULT ''");
    await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE');
    await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS is_approved BOOLEAN NOT NULL DEFAULT FALSE');
    console.log('Columns added successfully.');

    const result = await pool.query(`SELECT column_name, data_type, column_default FROM information_schema.columns WHERE table_name = 'users' AND column_name IN ('salt', 'is_admin', 'is_approved') ORDER BY column_name`);
    console.log('Verification:', JSON.stringify(result.rows, null, 2));
  } catch (err) {
    console.error('Error:', err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
