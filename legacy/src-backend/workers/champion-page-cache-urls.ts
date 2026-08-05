export type ChampionTalentCacheRow = {
  name: string;
  talent_id: number | null;
};

export function championPageSlug(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9]/g, '');
}

export function championPageWarmUrls(rows: ChampionTalentCacheRow[]): string[] {
  const urls: string[] = [];
  const seen = new Set<string>();

  for (const row of rows) {
    const slug = championPageSlug(row.name);
    if (!slug) continue;

    const championUrl = `/champions/${slug}/page-data`;
    if (!seen.has(championUrl)) {
      seen.add(championUrl);
      urls.push(championUrl);
    }

    const talentId = Number(row.talent_id);
    if (!Number.isInteger(talentId) || talentId <= 0) continue;
    const talentUrl = `/champions/${slug}/talents/${talentId}/page-data`;
    if (!seen.has(talentUrl)) {
      seen.add(talentUrl);
      urls.push(talentUrl);
    }
  }

  return urls;
}
