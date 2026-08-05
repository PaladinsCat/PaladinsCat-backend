import dotenv from 'dotenv';
import path from 'path';
dotenv.config({ path: path.resolve(__dirname, '../../.env') });

async function main() {
  const { getDataUsed } = await import('../services/hirez.js');
  const result = await getDataUsed('3693');
  console.log('=== Raw Hi-Rez Response (getdatausedJson) ===');
  console.log(JSON.stringify(result, null, 2));

  if (result && result.Total_Requests_Today != null && result.Request_Limit_Daily != null) {
    const remaining = result.Request_Limit_Daily - result.Total_Requests_Today;
    console.log(`\n=== Summary ===`);
    console.log(`Daily limit:     ${result.Request_Limit_Daily}`);
    console.log(`Used today:      ${result.Total_Requests_Today}`);
    console.log(`Remaining calls: ${remaining}`);
  }
}

main().catch(err => {
  console.error('Error:', err instanceof Error ? err.message : String(err));
  process.exit(1);
});
