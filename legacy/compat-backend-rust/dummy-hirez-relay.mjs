import http from 'node:http';

const port = Number(process.env.PALADINSCAT_DUMMY_RELAY_PORT || 0);
if (!Number.isInteger(port) || port <= 0) {
  throw new Error('PALADINSCAT_DUMMY_RELAY_PORT must be a positive integer');
}

const results = {
  getMatchIdsByQueue: [
    { Match: 1281115901, Active_Flag: 'n', Entry_Datetime: '7/30/2026 7:00:00 PM' },
  ],
  getMatchDetailsBatchRaw: [
    {
      Match: 1281115795,
      playerId: 101,
      playerName: 'Alpha',
      ChampionId: 2491,
      Champion: 'Furia',
      Queue: 452,
      Region: 'NA',
      ret_msg: null,
    },
  ],
  getDemoDetails: {
    Match: 1281115795,
    Map_Game: 'LIVE Snowfall Junction (Onslaught)',
    Duration: 448,
    ret_msg: null,
  },
  getPlayerBatchFromMatch: [
    { playerId: 101, playerName: 'Alpha', portalId: 28, ret_msg: null },
    { playerId: 102, playerName: 'Bravo', portalId: 5, ret_msg: null },
  ],
  getPlayerStatus: [
    {
      player_id: 101,
      status: 3,
      status_string: 'In Game',
      Match: 1281115910,
      match_queue_id: 424,
      privacy_flag: 'n',
      personal_status_message: '',
      ret_msg: null,
    },
  ],
  getMatchPlayerDetails: [
    {
      playerId: 101,
      playerName: 'Alpha',
      playerPortalId: 28,
      ChampionId: 2491,
      ChampionName: 'Furia',
      SkinId: 1,
      Skin: 'Default',
      Tier: 20,
      tierWins: 12,
      tierLosses: 8,
      Account_Level: 573,
      Mastery_Level: 36,
      taskForce: 1,
      match_queue_id: 424,
      mapGame: 'LIVE Stone Keep',
      playerRegion: 'NA',
      ret_msg: null,
    },
    {
      playerId: 102,
      playerName: 'Bravo',
      playerPortalId: 5,
      ChampionId: 2288,
      ChampionName: 'Makoa',
      SkinId: 2,
      Skin: 'Default',
      Tier: 21,
      tierWins: 10,
      tierLosses: 10,
      Account_Level: 999,
      Mastery_Level: 59,
      taskForce: 2,
      match_queue_id: 424,
      mapGame: 'LIVE Stone Keep',
      playerRegion: 'NA',
      ret_msg: null,
    },
  ],
  dumpRawPayloads: { inserted: 1 },
};

const server = http.createServer((request, response) => {
  if (request.method !== 'POST' || request.url !== '/v1/call') {
    response.writeHead(404, { 'content-type': 'application/json' });
    response.end(JSON.stringify({ ok: false, error: 'Not found' }));
    return;
  }
  let body = '';
  request.setEncoding('utf8');
  request.on('data', chunk => {
    body += chunk;
  });
  request.on('end', () => {
    try {
      const payload = JSON.parse(body);
      if (!Object.hasOwn(results, payload.operation)) {
        response.writeHead(400, { 'content-type': 'application/json' });
        response.end(JSON.stringify({
          ok: false,
          error: `Unsupported dummy relay operation: ${payload.operation}`,
        }));
        return;
      }
      response.writeHead(200, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ ok: true, result: results[payload.operation] }));
    } catch (error) {
      response.writeHead(400, { 'content-type': 'application/json' });
      response.end(JSON.stringify({ ok: false, error: String(error) }));
    }
  });
});

server.listen(port, '127.0.0.1', () => {
  process.stdout.write(`dummy-hirez-relay:${port}\n`);
});
