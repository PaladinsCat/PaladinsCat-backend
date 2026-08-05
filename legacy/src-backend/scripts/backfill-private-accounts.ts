import { pool } from '../config/db';
import { backfillPrivateAccountIdentities } from '../services/private-account-resolver';

async function main(): Promise<void> {
  const apply = process.argv.includes('--apply');
  const report = await backfillPrivateAccountIdentities(apply);
  console.log(JSON.stringify(report, null, 2));
  if (!apply) {
    console.log('[private-accounts] dry run only; pass --apply to resolve observations and switch the public directory to current identities');
  }
}

main()
  .catch(error => {
    console.error('[private-accounts] backfill failed:', error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
