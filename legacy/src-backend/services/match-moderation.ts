export type StoredPlayerModeration = {
  id: string | number;
  cheater: boolean;
  sus_count: number;
  verified: boolean;
};

export type StoredPrivateModeration = {
  id: string | number;
  cheater: boolean;
  sus_count: number;
};

/** Overlay mutable player moderation onto otherwise cacheable match facts. */
export function overlayCurrentPlayerModeration(
  result: any,
  rows: StoredPlayerModeration[],
  privateRows: StoredPrivateModeration[] = [],
): any {
  const publicModeration = new Map(rows.map((row) => [Number(row.id), row]));
  const privateModeration = new Map(privateRows.map((row) => [Number(row.id), row]));
  return {
    ...result,
    players: (Array.isArray(result?.players) ? result.players : []).map((player: any) => {
      const playerId = Number(player.player_id);
      const privateId = Number(player.private_player_id);
      const current = playerId > 0
        ? publicModeration.get(playerId)
        : privateId > 0
          ? privateModeration.get(privateId)
          : undefined;
      if (!current || !player.profile_snapshot) return player;
      return {
        ...player,
        profile_snapshot: {
          ...player.profile_snapshot,
          cheater: Boolean(current.cheater),
          sus_count: 'sus_count' in current ? Number(current.sus_count ?? 0) : 0,
          verified: 'verified' in current ? Boolean(current.verified) : false,
        },
      };
    }),
  };
}
