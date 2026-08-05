import { dispatchRelayOperation } from '../hirez-relay/dispatcher';

async function main() {
  const ids = await dispatchRelayOperation('getMatchIdsByQueue', [486, '20260615', 12], 'dummy') as number[];
  if (!Array.isArray(ids) || ids.length === 0) throw new Error('dummy getMatchIdsByQueue returned no IDs');

  const matches = await dispatchRelayOperation('getMatchDetailsBatch', [ids.slice(0, 2)], 'dummy') as any[];
  if (!Array.isArray(matches) || matches.length !== 2) throw new Error('dummy getMatchDetailsBatch returned wrong match count');
  if (!Array.isArray(matches[0].players) || matches[0].players.length !== 10) {
    throw new Error('dummy match does not contain 10 players');
  }

  const usage = await dispatchRelayOperation('getDataUsed', ['dummy'], 'dummy') as any;
  if (usage.Total_Requests_Today !== 0 || usage.Request_Limit_Daily <= 0 || usage.dummy !== true) {
    throw new Error('dummy getDataUsed should report a healthy synthetic quota with zero real usage');
  }

  console.log(JSON.stringify({
    ok: true,
    mode: 'dummy',
    ids: ids.length,
    matches: matches.length,
    playersPerMatch: matches[0].players.length,
    keysEnabled: false,
  }, null, 2));
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exit(1);
});
