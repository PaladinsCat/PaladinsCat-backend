import { MatchDetails, PlayerDetails, RawPayload } from '../contracts/hirez-relay';

const dummyApiCallCounts: Record<string, number> = {};
const dummyMatchWindows = new Map<number, { date: string; hour: number }>();
const dummyMatchScenarios = new Map<number, DummyMatchScenario>();

export const DUMMY_MATCH_SCENARIOS = [
  'complete',
  'broken_skin',
  'omit_from_multi',
  'roster_failure',
  'history_missing',
  'no_player_anchors',
  'pve_single_human',
  'vendor_failure',
] as const;

export type DummyMatchScenario = typeof DUMMY_MATCH_SCENARIOS[number];

export function setDummyMatchScenario(matchId: number, scenario: DummyMatchScenario): boolean {
  if (!Number.isInteger(matchId) || matchId <= 0) {
    throw new Error('dummy match scenario requires a positive integer match ID');
  }
  if (!DUMMY_MATCH_SCENARIOS.includes(scenario)) {
    throw new Error(`unsupported dummy match scenario: ${scenario}`);
  }
  if (scenario === 'complete') dummyMatchScenarios.delete(matchId);
  else dummyMatchScenarios.set(matchId, scenario);
  return true;
}

export function resetDummyMatchScenarios(): boolean {
  dummyMatchScenarios.clear();
  return true;
}

function dummyMatchScenario(matchId: number): DummyMatchScenario {
  return dummyMatchScenarios.get(matchId) ?? 'complete';
}

const champions = [
  { id: 2288, name: 'Cassie' },
  { id: 2285, name: 'Fernando' },
  { id: 2493, name: 'Koga' },
  { id: 2362, name: 'Jenos' },
  { id: 2314, name: 'Inara' },
  { id: 2431, name: 'Lian' },
  { id: 2512, name: 'Vora' },
  { id: 2557, name: 'Nyx' },
  { id: 2404, name: 'Furia' },
  { id: 2548, name: 'Betty la Bomba' },
];

function isoForMatch(matchId: number): string {
  const base = Date.UTC(2026, 0, 1, 0, 0, 0);
  return new Date(base + (matchId % 100000) * 1000).toISOString();
}

function isoForWindowedMatch(matchId: number): string {
  const window = dummyMatchWindows.get(matchId);
  if (!window || !/^\d{8}$/.test(window.date)) return isoForMatch(matchId);

  const year = Number(window.date.slice(0, 4));
  const monthIndex = Number(window.date.slice(4, 6)) - 1;
  const day = Number(window.date.slice(6, 8));
  const minute = Math.abs(matchId) % 60;
  return new Date(Date.UTC(year, monthIndex, day, window.hour, minute, 0)).toISOString();
}

export function dummyMatchIdsByQueue(queueId: number, date: string, hour: number): number[] {
  const seed = Number(`${String(queueId).slice(-2)}${date.slice(-2)}${String(hour).padStart(2, '0')}`);
  const ids = Array.from({ length: 6 }, (_, index) => seed * 100 + index + 1);
  for (const id of ids) {
    dummyMatchWindows.set(id, { date, hour });
  }
  return ids;
}

export function dummyMatchDetailsBatch(matchIds: number[]): MatchDetails[] {
  return matchIds.filter(id => id > 0).map((matchId) => {
    const entry = isoForWindowedMatch(matchId);
    const duration = 900 + (matchId % 420);
    const players = Array.from({ length: 10 }, (_, index) => dummyPlayer(matchId, index, entry, duration));
    return {
      match_id: matchId,
      entry_datetime: entry,
      map: 'LIVE Jaguar Falls',
      queue_id: 486,
      duration_seconds: duration,
      minutes: Math.round(duration / 60),
      region: matchId % 2 === 0 ? 'NA' : 'EU',
      team1_score: 4,
      team2_score: matchId % 2,
      winning_task_force: 1,
      has_replay: true,
      players,
    };
  });
}

function dummyBrokenSkinRows(matchId: number): any[] {
  const match = dummyMatchDetailsBatch([matchId])[0];
  if (!match) return [];
  const directPrefix = match.players.slice(0, 7);
  const first = directPrefix[0] as any;
  return [
    ...directPrefix,
    {
      ...first,
      player_id: 0,
      playerId: 0,
      player_name: 'BROKEN_SKIN_SENTINEL',
      playerName: 'BROKEN_SKIN_SENTINEL',
      champion_id: 0,
      ChampionId: 0,
      skin_id: 32768,
      SkinId: 32768,
      has_ret_msg: true,
      ret_msg: 'Value was either too large or too small for an Int16 while reading SkinId',
    },
  ];
}

function dummyPveSingleHumanRows(matchId: number): any[] {
  const match = dummyMatchDetailsBatch([matchId])[0];
  const player = match?.players[0] as any;
  if (!player) return [];
  return [{
    ...player,
    queue_id: 425,
    match_queue_id: 425,
    Match_Queue_Id: 425,
  }];
}

function dummyPlayerStatus(playerId: number): any[] {
  return [{
    player_id: playerId,
    status: 3,
    status_string: 'In Game',
    Match: 990000001,
    match_queue_id: 486,
    privacy_flag: 'n',
    ret_msg: null,
  }];
}

function dummyMatchPlayerDetails(matchId: number): any[] {
  return dummyMatchDetailsBatch([matchId])[0]?.players.map((player, index) => ({
    ...player,
    playerId: player.player_id,
    playerName: player.player_name,
    playerPortalId: player.portal_id,
    ChampionId: player.champion_id,
    ChampionName: player.champion_name,
    SkinId: player.skin_id,
    Skin: player.skin_name,
    Tier: player.league_tier,
    tierWins: player.league_wins,
    tierLosses: player.league_losses,
    Account_Level: player.account_level,
    Mastery_Level: player.mastery_level,
    taskForce: index < 5 ? 1 : 2,
    mapGame: 'LIVE Jaguar Falls',
    playerRegion: player.region,
    match_queue_id: 486,
    ret_msg: null,
  })) ?? [];
}

export function dummyPlayer(matchId: number, index: number, entry: string, duration: number): PlayerDetails {
  const champion = champions[index % champions.length];
  const deaths = 1 + ((matchId + index) % 8);
  const kills = 2 + ((matchId + index * 3) % 15);
  const assists = 4 + ((matchId + index * 5) % 18);
  const gold = 1400 + ((matchId + index * 97) % 2200);
  const taskForce = index < 5 ? 1 : 2;
  const winStatus = taskForce === 1 ? 'Winner' : 'Loser';
  const region = matchId % 2 === 0 ? 'NA' : 'EU';
  const map = 'LIVE Jaguar Falls';
  const team1Score = 4;
  const team2Score = matchId % 2;
  const itemIds = [30000, 30010, 30020, 30030, 30040].map(base => base + index);
  const activeIds = [23453, 23454, 23455, 23456];
  const activeLevels = [1, 1, 0, 0];
  const cardLevels = [5, 4, 3, 2, 1];
  const banIds = [2288, 2493, 2362, 2314, 0, 0, 0, 0];

  return {
    player_id: matchId * 100 + index + 1,
    player_name: `DummyPlayer${index + 1}`,
    match_id: matchId,
    entry_datetime: entry,
    queue_id: 486,
    champion_id: champion.id,
    champion_name: champion.name,
    skin_id: 10000 + index,
    skin_name: 'Default',
    kills,
    deaths,
    assists,
    damage_done_in_hand: 9000 + index * 500,
    damage_done_physical: 12000 + index * 600,
    damage_done_magical: 0,
    damage_taken: 16000 + index * 800,
    damage_mitigated: index % 2 === 0 ? 12000 : 2000,
    healing: index === 3 || index === 8 ? 18000 : 1200,
    healing_self: 800 + index * 10,
    gold_earned: gold,
    gold_per_minute: Math.round(gold / (duration / 60)),
    objective_assists: index % 4,
    killing_spree: Math.max(0, kills - deaths),
    multi_kill_max: index % 3,
    win_status: winStatus,
    task_force: taskForce,
    league_tier: 18 + (index % 5),
    league_points: 40 + index * 10,
    account_level: 100 + index,
    mastery_level: 20 + index,
    party_id: index % 5,
    time_in_match: duration,
    distance_traveled: 120000 + index * 1000,
    structure_damage: 0,
    camps_cleared: 0,
    source: 'direct',
    portal_id: 5,
    portal_user_id: `dummy-${matchId}-${index}`,
    kills_player: kills,
    region,
    healing_player_self: 800 + index * 10,
    damage_taken_physical: 12000 + index * 500,
    damage_taken_magical: 4000 + index * 300,
    kills_fire_giant: 0,
    kills_gold_fury: 0,
    kills_phoenix: 0,
    kills_siege_jugg: 0,
    kills_wild_jugg: 0,
    kills_bot: 0,
    kills_single: kills,
    kills_double: index % 2,
    kills_triple: 0,
    kills_quadra: 0,
    kills_penta: 0,
    kills_first_blood: index === 0 ? 1 : 0,
    wards_placed: 0,
    towers_destroyed: 0,
    league_wins: 50 + index,
    league_losses: 45 + index,
    healing_bot: 0,
    damage_bot: 0,
    platform: 'Steam',
    surrendered: 0,
    team_id: taskForce,
    team_name: taskForce === 1 ? 'Blue' : 'Red',
    rank_stat_league: 0,
    final_match_level: 15,
    match_duration: duration,
    active_id_1: activeIds[0],
    active_id_2: activeIds[1],
    active_id_3: activeIds[2],
    active_id_4: activeIds[3],
    active_level_1: activeLevels[0],
    active_level_2: activeLevels[1],
    active_level_3: activeLevels[2],
    active_level_4: activeLevels[3],
    item_active_1: 'Haven',
    item_active_2: 'Nimble',
    item_active_3: '',
    item_active_4: '',
    item_id_1: itemIds[0],
    item_id_2: itemIds[1],
    item_id_3: itemIds[2],
    item_id_4: itemIds[3],
    item_id_5: itemIds[4],
    item_id_6: 40000 + index,
    item_level_1: cardLevels[0],
    item_level_2: cardLevels[1],
    item_level_3: cardLevels[2],
    item_level_4: cardLevels[3],
    item_level_5: cardLevels[4],
    item_level_6: 0,
    item_purch_1: 'Dummy Card 1',
    item_purch_2: 'Dummy Card 2',
    item_purch_3: 'Dummy Card 3',
    item_purch_4: 'Dummy Card 4',
    item_purch_5: 'Dummy Card 5',
    item_purch_6: 'Dummy Talent',
    ban_id_1: banIds[0],
    ban_id_2: banIds[1],
    ban_id_3: banIds[2],
    ban_id_4: banIds[3],
    ban_id_5: banIds[4],
    ban_id_6: banIds[5],
    ban_id_7: banIds[6],
    ban_id_8: banIds[7],
    merged_players: null,
    has_ret_msg: false,
    ret_msg: null,

    // Hi-Rez shape aliases.
    //
    // Dummy mode must test the same parser branches as real Hi-Rez payloads.
    // The backend stores normalized snake_case fields, but the raw
    // `getmatchdetailsbatch` endpoint returns a flat player array with mixed
    // PascalCase/underscore names such as Match, ChampionId, Kills_Player,
    // ActiveId1, ItemId1, and BanId1. Keep both shapes on dummy rows so:
    // - `getMatchDetailsBatch` remains a drop-in backend facade result.
    // - `getMatchDetailsBatchRaw` can validate normalizer/math/sorting logic
    //   against realistic Hi-Rez-style field names without spending quota.
    Match: matchId,
    Entry_Datetime: entry,
    Map_Game: map,
    match_queue_id: 486,
    Match_Queue_Id: 486,
    Match_Duration: duration,
    Minutes: Math.round(duration / 60),
    Region: region,
    Team1Score: team1Score,
    Team2Score: team2Score,
    Winning_TaskForce: 1,
    hasReplay: 'y',
    playerName: `DummyPlayer${index + 1}`,
    ChampionId: champion.id,
    Champion: champion.name,
    SkinId: 10000 + index,
    Skin: 'Default',
    Kills_Player: kills,
    Deaths: deaths,
    Assists: assists,
    Damage: 12000 + index * 600,
    Damage_Done_In_Hand: 9000 + index * 500,
    Damage_Mitigated: index % 2 === 0 ? 12000 : 2000,
    Damage_Taken: 16000 + index * 800,
    Damage_Taken_Physical: 12000 + index * 500,
    Damage_Taken_Magical: 4000 + index * 300,
    Healing: index === 3 || index === 8 ? 18000 : 1200,
    Healing_Player_Self: 800 + index * 10,
    Gold_Earned: gold,
    Objective_Assists: index % 4,
    Killing_Spree: Math.max(0, kills - deaths),
    Multi_kill_Max: index % 3,
    Win_Status: winStatus,
    TaskForce: taskForce,
    League_Tier: 18 + (index % 5),
    League_Points: 40 + index * 10,
    Account_Level: 100 + index,
    Mastery_Level: 20 + index,
    PartyId: index % 5,
    Time_In_Match_Seconds: duration,
    Distance_Traveled: 120000 + index * 1000,
    Structure_Damage: 0,
    ActiveId1: activeIds[0],
    ActiveId2: activeIds[1],
    ActiveId3: activeIds[2],
    ActiveId4: activeIds[3],
    ActiveLevel1: activeLevels[0],
    ActiveLevel2: activeLevels[1],
    ActiveLevel3: activeLevels[2],
    ActiveLevel4: activeLevels[3],
    Item_Active_1: 'Haven',
    Item_Active_2: 'Nimble',
    Item_Active_3: '',
    Item_Active_4: '',
    ItemId1: itemIds[0],
    ItemId2: itemIds[1],
    ItemId3: itemIds[2],
    ItemId4: itemIds[3],
    ItemId5: itemIds[4],
    ItemId6: 40000 + index,
    ItemLevel1: cardLevels[0],
    ItemLevel2: cardLevels[1],
    ItemLevel3: cardLevels[2],
    ItemLevel4: cardLevels[3],
    ItemLevel5: cardLevels[4],
    ItemLevel6: 0,
    Item_Purch_1: 'Dummy Card 1',
    Item_Purch_2: 'Dummy Card 2',
    Item_Purch_3: 'Dummy Card 3',
    Item_Purch_4: 'Dummy Card 4',
    Item_Purch_5: 'Dummy Card 5',
    Item_Purch_6: 'Dummy Talent',
    BanId1: banIds[0],
    BanId2: banIds[1],
    BanId3: banIds[2],
    BanId4: banIds[3],
    BanId5: banIds[4],
    BanId6: banIds[5],
    BanId7: banIds[6],
    BanId8: banIds[7],
  };
}

export function dummyMatchHistory(playerId: number, limit = 50): any[] {
  const capped = Math.max(1, Math.min(limit, 50));
  const encodedMatchId = Math.floor(playerId / 100);
  const encodedSlot = playerId - (encodedMatchId * 100);
  const targetMatchId = encodedMatchId > 0 && encodedSlot >= 1 && encodedSlot <= 10
    ? encodedMatchId
    : Math.floor(playerId / 10);
  const targetSlot = encodedSlot >= 1 && encodedSlot <= 10 ? encodedSlot - 1 : 0;

  return Array.from({ length: capped }, (_, index) => {
    const matchId = targetMatchId + index;
    const player = dummyPlayer(matchId, index === 0 ? targetSlot : index % 10, isoForMatch(matchId), 1000);
    return {
      ...player,
      Match: matchId,
      Match_Time: isoForMatch(matchId),
      Match_Duration: player.match_duration,
      Time_In_Match_Seconds: player.time_in_match,
      playerId,
      playerName: `DummyPlayer${playerId}`,
      Champion: player.champion_name,
      ChampionId: player.champion_id,
      Kills: player.kills,
      Deaths: player.deaths,
      Assists: player.assists,
      Damage: player.damage_done_physical,
      Gold: player.gold_earned,
      Match_Queue_Id: player.queue_id,
      Map_Game: 'LIVE Jaguar Falls',
      ret_msg: null,
    };
  });
}

function bumpDummyApiCall(method: string): void {
  const key = method.toLowerCase();
  dummyApiCallCounts[key] = (dummyApiCallCounts[key] ?? 0) + 1;
}

export function resetDummyApiCallCounts(): Record<string, number> {
  for (const key of Object.keys(dummyApiCallCounts)) delete dummyApiCallCounts[key];
  return {};
}

export function getDummyApiCallCounts(): Record<string, number> {
  return { ...dummyApiCallCounts };
}

function dummyOperationToEndpoint(operation: string): string | null {
  const endpoints: Record<string, string> = {
    getMatchIdsByQueue: 'getmatchidsbyqueue',
    getMatchIdsByQueueDetails: 'getmatchidsbyqueue',
    getMatchDetailsBatch: 'getmatchdetailsbatch',
    getMatchDetailsBatchRaw: 'getmatchdetailsbatch',
    getMatchDetailsRaw: 'getmatchdetails',
    getPlayerBatchFromMatch: 'getplayerbatchfrommatch',
    getDemoDetails: 'getdemodetails',
    getPlayerBatch: 'getplayerbatch',
    getPlayerBatchLookup: 'getplayerbatch',
    getMatchHistory: 'getmatchhistory',
    getPlayers: 'getplayers',
    getPlayerIdByName: 'getplayeridbyname',
    searchPlayers: 'searchplayers',
    getPlayerIdsByGamerTag: 'getplayeridsbygamertag',
    getPlayerIdByPortalUserId: 'getplayeridbyportaluserid',
    getPlayerChampions: 'getplayerchampions',
    getChampionRanks: 'getchampionranks',
    getChampions: 'getchampions',
    getItems: 'getitems',
    getEsportsProLeagueDetails: 'getesportsproleaguedetails',
    getPlayerLoadouts: 'getplayerloadouts',
    getPlayerStatus: 'getplayerstatus',
    getMatchPlayerDetails: 'getmatchplayerdetails',
    getLeagueLeaderboard: 'getleagueleaderboard',
    getMatchLeaderboard: 'getmatchleaderboard',
    getLeagueSeasons: 'getleagueseasons',
    getDataUsed: 'getdataused',
  };
  return endpoints[operation] ?? null;
}

/**
 * Synthetic Hi-Rez endpoint provider used by core.apiRequest() in dummy mode.
 *
 * `dispatchDummy()` below is the public relay facade and returns already-shaped
 * backend DTOs. Recovery testing needs something lower level: the real
 * `recoverBrokenMatch()` algorithm must still call `apiRequest()` and receive
 * Hi-Rez-like raw endpoint responses, so DB-first/prefetch/cache branches are
 * exercised exactly as production would exercise them. This function provides
 * those raw responses and records per-endpoint call counts so regression tests
 * can prove that prefetch rows suppress extra `getmatchhistory` calls.
 */
export async function dispatchDummyApiRequest(method: string, params: string[] = []): Promise<unknown> {
  const normalizedMethod = method.toLowerCase();
  bumpDummyApiCall(normalizedMethod);

  switch (normalizedMethod) {
    case 'getmatchidsbyqueue':
      return dummyMatchIdsByQueue(Number(params[0]), String(params[1]), Number(params[2]))
        .map(matchId => ({ Match: matchId, ret_msg: null }));
    case 'getmatchdetailsbatch': {
      const matchIds = String(params[0] ?? '')
        .split(',')
        .map(Number)
        .filter(matchId => Number.isFinite(matchId) && matchId > 0);
      if (matchIds.some(matchId => dummyMatchScenario(matchId) === 'vendor_failure')) {
        throw new Error('Synthetic Hi-Rez service-wide failure');
      }
      return matchIds.flatMap(matchId => {
        const scenario = dummyMatchScenario(matchId);
        if (scenario === 'omit_from_multi' && matchIds.length > 1) return [];
        if (scenario === 'broken_skin' || scenario === 'roster_failure' || scenario === 'history_missing') {
          return dummyBrokenSkinRows(matchId);
        }
        if (scenario === 'no_player_anchors') return [];
        if (scenario === 'pve_single_human') return dummyPveSingleHumanRows(matchId);
        return dummyMatchDetailsBatch([matchId]).flatMap(match => match.players);
      });
    }
    case 'getmatchdetails': {
      const matchId = Number(params[0]);
      return dummyMatchDetailsBatch([matchId]).flatMap(match => match.players);
    }
    case 'getplayerstatus': {
      return dummyPlayerStatus(Number(params[0]));
    }
    case 'getmatchplayerdetails': {
      return dummyMatchPlayerDetails(Number(params[0]));
    }
    case 'getplayerbatchfrommatch': {
      const matchId = Number(params[0]);
      const scenario = dummyMatchScenario(matchId);
      if (scenario === 'roster_failure') {
        throw new Error('Synthetic getplayerbatchfrommatch failure');
      }
      if (scenario === 'no_player_anchors') return [];
      return dummyMatchDetailsBatch([matchId])[0]?.players.map(player => ({
        ...player,
        Id: player.player_id,
        ActivePlayerId: player.player_id,
        Name: player.player_name,
        hz_player_name: player.player_name,
        Level: player.account_level || 100,
        Platform: player.platform,
        RankedConquest: { Tier: player.league_tier || 0, Points: player.league_points || 0 },
        ret_msg: null,
      })) ?? [];
    }
    case 'getdemodetails': {
      const matchId = Number(params[0]);
      return {
        Match: matchId,
        Queue: 486,
        Entry_Datetime: isoForMatch(matchId),
        Map_Game: 'LIVE Jaguar Falls',
        Match_Time: 1000,
        Match_Duration: 1000,
        Minutes: 17,
        Team1_Score: 4,
        Team2_Score: matchId % 2,
        Winning_Team: 1,
        hasReplay: 'y',
        ret_msg: null,
      };
    }
    case 'getplayerbatch': {
      return String(params[0] ?? '')
        .split(',')
        .map(Number)
        .filter(playerId => Number.isFinite(playerId) && playerId > 0)
        .map((playerId, index) => ({
          Id: playerId,
          ActivePlayerId: playerId,
          Name: `DummyPlayer${playerId}`,
          hz_player_name: `DummyPlayer${playerId}`,
          Level: 100 + index,
          Platform: 'Steam',
          Region: 'NA',
          ret_msg: null,
        }));
    }
    case 'getplayeridbyname': {
      const name = String(params[0] ?? '').trim();
      if (!name) return [];
      return [{
        player_id: 900000,
        Id: 900000,
        ActivePlayerId: 900000,
        Name: name,
        hz_player_name: name,
        Platform: 'Steam',
        portal_id: 1,
        ret_msg: null,
      }];
    }
    case 'searchplayers': {
      const name = String(params[0] ?? '').trim();
      if (!name) return [];
      return [{
        player_id: 900000,
        Id: 900000,
        Name: name,
        hz_player_name: name,
        Platform: 'Steam',
        portal_id: 1,
        privacy_flag: 'n',
        ret_msg: null,
      }];
    }
    case 'getplayeridsbygamertag': {
      const name = String(params[1] ?? '').trim();
      if (!name) return [];
      return [{
        player_id: 900001,
        Id: 900001,
        Name: name,
        hz_gamer_tag: name,
        portal_id: Number(params[0] ?? 0),
        ret_msg: null,
      }];
    }
    case 'getplayeridbyportaluserid': {
      const portalUserId = String(params[1] ?? '').trim();
      if (!portalUserId) return [];
      return [{
        player_id: 900002,
        Id: 900002,
        ActivePlayerId: 900002,
        portal_user_id: portalUserId,
        portal_id: Number(params[0] ?? 0),
        ret_msg: null,
      }];
    }
    case 'getmatchhistory': {
      const playerId = Number(params[0]);
      const encodedMatchId = Math.floor(playerId / 100);
      const history = dummyMatchHistory(playerId, Number(params[1] ?? 50));
      return {
        matches: dummyMatchScenario(encodedMatchId) === 'history_missing'
          ? history.filter(row => Number(row.Match || row.match_id || 0) !== encodedMatchId)
          : history,
        ret_msg: null,
      };
    }
    case 'getchampions':
      return champions.map(champion => ({
        id: champion.id,
        Name: champion.name,
        Roles: 'Paladins Champion',
        ret_msg: null,
      }));
    case 'getitems':
      return [{
        ItemId: 30000,
        DeviceName: 'Dummy Item',
        Description: 'Synthetic item for relay parity tests',
        ret_msg: null,
      }];
    case 'getesportsproleaguedetails':
      return [{
        LeagueId: 1,
        LeagueName: 'Dummy League',
        ret_msg: null,
      }];
    default:
      return dispatchDummy(normalizedMethod, params);
  }
}

export async function dispatchDummy(operation: string, args: any[] = []): Promise<unknown> {
  // Count dummy dispatcher calls that stand in for real Hi-Rez endpoints.
  // `dumpRawPayloads`, cache cleanup, and the counter helpers are intentionally
  // excluded: those are local relay/backend control operations, not API quota
  // equivalents. Recovery calls routed through core.apiRequest() are counted
  // at the lower endpoint layer by dispatchDummyApiRequest().
  const countedEndpoint = dummyOperationToEndpoint(operation);
  if (countedEndpoint) bumpDummyApiCall(countedEndpoint);

  switch (operation) {
    case 'getMatchIdsByQueue':
      return dummyMatchIdsByQueue(Number(args[0]), String(args[1]), Number(args[2]));
    case 'getMatchIdsByQueueDetails':
      return dummyMatchIdsByQueue(Number(args[0]), String(args[1]), Number(args[2])).map(matchId => ({
        matchId,
        entryDatetime: isoForMatch(matchId),
        region: matchId % 2 === 0 ? 'NA' : 'EU',
        activeFlag: false,
      }));
    case 'getMatchDetailsBatch':
      return dummyMatchDetailsBatch((args[0] ?? []).map(Number));
    case 'resumeMatchRecovery': {
      const request = args[0]?.[0] ?? {};
      const match = dummyMatchDetailsBatch([Number(request.matchId)])[0];
      return match ? [{
        matchId: Number(request.matchId),
        queueId: Number(request.queueId || match.queue_id || 0),
        status: 'complete_recovered',
        match: {
          ...match,
          recovery_source: 'local_resume',
          recovery_api_calls: 1,
          recovery_attempted: true,
          recovery_terminal: false,
          recovery_pending: false,
          limited: false,
        },
      }] : [];
    }
    case 'getMatchDetailsBatchRaw':
      return dummyMatchDetailsBatch((args[0] ?? []).map(Number)).flatMap(match => match.players);
    case 'getMatchDetailsRaw':
      return dummyMatchDetailsBatch([Number(args[0])]).flatMap(match => match.players);
    case 'getPlayerBatchFromMatch':
      return dummyMatchDetailsBatch([Number(args[0])])[0]?.players.map(player => ({
        Id: player.player_id,
        Name: player.player_name,
        Platform: player.platform,
        ret_msg: null,
      })) ?? [];
    case 'getDemoDetails':
      return {
        Match: Number(args[0]),
        Queue: '486',
        Entry_Datetime: isoForMatch(Number(args[0])),
        Match_Time: 1000,
        Team1_Score: 4,
        Team2_Score: 1,
        Winning_Team: 1,
        ret_msg: null,
      };
    case 'getPlayerBatch':
    case 'getPlayerBatchLookup':
      return (args[0] ?? []).map((playerId: number, index: number) => ({
        Id: Number(playerId),
        ActivePlayerId: Number(playerId),
        Name: `DummyPlayer${playerId}`,
        hz_player_name: `DummyPlayer${playerId}`,
        Level: 100 + index,
        Platform: 'Steam',
        Region: 'NA',
        ret_msg: null,
      }));
    case 'getMatchHistory':
      return dummyMatchHistory(Number(args[0]), Number(args[1] ?? 50));
    case 'getPlayers':
      return (args[0] ?? []).map((name: string, index: number) => ({
        Id: 900000 + index,
        Name: name,
        Level: 100 + index,
        Platform: 'Steam',
        Region: 'NA',
        ret_msg: null,
      }));
    case 'getPlayerIdByName':
      return [{
        player_id: 900000,
        Id: 900000,
        ActivePlayerId: 900000,
        Name: String(args[0] ?? 'DummyPlayer900000'),
        hz_player_name: String(args[0] ?? 'DummyPlayer900000'),
        Platform: 'Steam',
        portal_id: 1,
        ret_msg: null,
      }];
    case 'searchPlayers':
      return [{
        player_id: 900000,
        Id: 900000,
        Name: String(args[0] ?? 'DummyPlayer900000'),
        hz_player_name: String(args[0] ?? 'DummyPlayer900000'),
        Platform: 'Steam',
        portal_id: 1,
        privacy_flag: 'n',
        ret_msg: null,
      }];
    case 'getPlayerIdsByGamerTag':
      return [{
        player_id: 900001,
        Id: 900001,
        Name: String(args[1] ?? 'DummyConsolePlayer'),
        hz_gamer_tag: String(args[1] ?? 'DummyConsolePlayer'),
        portal_id: Number(args[0] ?? 0),
        ret_msg: null,
      }];
    case 'getPlayerIdByPortalUserId':
      return [{
        player_id: 900002,
        Id: 900002,
        ActivePlayerId: 900002,
        portal_user_id: String(args[1] ?? ''),
        portal_id: Number(args[0] ?? 0),
        ret_msg: null,
      }];
    case 'getPlayerChampions':
      return champions.map((champion, index) => ({
        PlayerId: Number(args[0]), ChampionId: champion.id, Champion: champion.name,
        XP: 1_000 + index,
        OwnershipType: 'Purchased', ret_msg: null,
      }));
    case 'getChampionRanks':
      return champions.map((champion, index) => ({
        PlayerId: Number(args[0]), ChampionId: champion.id, Champion: champion.name,
        Worshippers: 1_000 + index,
        Wins: 20 + index, Losses: 10 + index, Kills: 300 + index, Deaths: 100 + index,
        Assists: 200 + index, Minutes: 600 + index, ret_msg: null,
      }));
    case 'getChampions':
      return champions.map(champion => ({
        id: champion.id,
        Name: champion.name,
        Roles: 'Paladins Champion',
        ret_msg: null,
      }));
    case 'getItems':
      return [{
        ItemId: 30000,
        DeviceName: 'Dummy Item',
        Description: 'Synthetic item for relay parity tests',
        ret_msg: null,
      }];
    case 'getEsportsProLeagueDetails':
      return [{
        LeagueId: 1,
        LeagueName: 'Dummy League',
        ret_msg: null,
      }];
    case 'getPlayerLoadouts':
      return [{ DeckId: 1, playerId: Number(args[0]), ChampionId: champions[0].id, LoadoutItems: [], ret_msg: null }];
    case 'getPlayerStatus':
      return dummyPlayerStatus(Number(args[0]));
    case 'getMatchPlayerDetails':
      return dummyMatchPlayerDetails(Number(args[0]));
    case 'getLeagueLeaderboard':
    case 'getMatchLeaderboard':
      return [{ player_id: 900001, name: 'DummyPlayer1', tier: Number(args[1] ?? args[0] ?? 21), rank: 1, points: 100, ret_msg: null }];
    case 'getLeagueSeasons':
      return [{ id: 12, name: 'Dummy Season', ret_msg: null }];
    case 'getDataUsed':
      return {
        Active_Sessions: 0,
        Concurrent_Sessions: 50,
        // Dummy mode should look like a healthy synthetic key, not an exhausted
        // key. A zero daily limit is a valid-looking number that can poison
        // dashboards or usage-sync code if someone points admin tooling at the
        // dummy relay. Keep the shape identical to Hi-Rez while making the
        // values obviously safe for quota-free local testing.
        Request_Limit_Daily: 10000,
        Session_Cap: 0,
        Session_Time_Limit: 15,
        Total_Requests_Today: 0,
        Total_Sessions_Today: 0,
        dummy: true,
      };
    case 'syncApiKeyUsage':
      return true;
    case 'resetDummyApiCallCounts':
      return resetDummyApiCallCounts();
    case 'getDummyApiCallCounts':
      return getDummyApiCallCounts();
    case 'setDummyMatchScenario':
      return setDummyMatchScenario(Number(args[0]), String(args[1]) as DummyMatchScenario);
    case 'resetDummyMatchScenarios':
      return resetDummyMatchScenarios();
    case 'reloadApiKeyPool':
      return true;
    case 'getApiKeyStatus':
      return [];
    case 'dumpRawPayloads':
      return (args[0] as RawPayload[] | undefined)?.length ?? 0;
    case 'cleanupFetchedPlayersCache':
    case 'clearMatchHistoryCache':
      return true;
    default:
      throw new Error(`Unsupported dummy HirezRelay operation: ${operation}`);
  }
}
