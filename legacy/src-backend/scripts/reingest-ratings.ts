import { reingestRatings } from '../services/rating-calculator';

async function main() {
  console.log('Starting Glicko-2 re-ingest...');
  const result = await reingestRatings();
  console.log(`\nDone. Processed: ${result.matchesProcessed}, Broken: ${result.brokenMatches.length}`);
  if (result.brokenMatches.length > 0) {
    console.log('\nBroken matches:');
    for (const b of result.brokenMatches) {
      console.log(`  Match ${b.matchId}: ${b.reason}`);
    }
  }
  process.exit(0);
}

main().catch(err => {
  console.error('Re-ingest failed:', err);
  process.exit(1);
});
