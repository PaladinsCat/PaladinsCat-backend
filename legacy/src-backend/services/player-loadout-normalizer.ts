export type StoredPlayerLoadout = {
  deckId: number;
  deckKey: string;
  championId: number;
  deckName: string;
  cardIds: number[];
  cardLevels: number[];
};

/** Normalize one getplayerloadouts deck without discarding unnamed decks. */
export function normalizePlayerLoadoutDeck(raw: any): StoredPlayerLoadout | null {
  const championId = Number(raw?.ChampionId ?? raw?.champion_id ?? raw?.championId ?? 0);
  const deckId = Number(raw?.DeckId ?? raw?.deck_id ?? raw?.deckId ?? 0);
  const suppliedDeckName = String(raw?.DeckName ?? raw?.deck_name ?? raw?.deckName ?? '').trim();
  if (!Number.isInteger(championId) || championId <= 0) return null;

  const rawCards = Array.isArray(raw?.LoadoutItems)
    ? raw.LoadoutItems
    : Array.isArray(raw?.loadout_items)
      ? raw.loadout_items
      : [];
  const cards: Array<{ id: number; level: number }> = rawCards
    .map((card: any): { id: number; level: number } => ({
      id: Number(card?.ItemId ?? card?.item_id ?? card?.id ?? 0),
      level: Number(card?.Points ?? card?.points ?? card?.level ?? 0),
    }))
    .filter((card: { id: number }) => Number.isInteger(card.id) && card.id > 0);
  const cardIds = cards.map((card) => card.id);
  const cardLevels = cards.map((card) => (
    Number.isFinite(card.level) ? Math.max(0, Math.min(5, Math.round(card.level))) : 0
  ));
  const deckName = suppliedDeckName || 'Unnamed Loadout';
  const deckKey = Number.isInteger(deckId) && deckId > 0
    ? `id:${deckId}`
    : `legacy:${championId}:${suppliedDeckName.toLowerCase().replace(/\s+/g, ' ').slice(0, 80) || 'unnamed'}:${cardIds.join('-')}`;

  return {
    deckId: Number.isInteger(deckId) && deckId > 0 ? deckId : 0,
    deckKey,
    championId,
    deckName,
    cardIds,
    cardLevels,
  };
}
