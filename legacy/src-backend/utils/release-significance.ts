export type ReleaseType = 'major' | 'minor' | 'patch';

export type ReleaseSignificance = {
  changeCount: number;
  releaseType: ReleaseType;
};

export function classifyRelease(changeCount: number): ReleaseType {
  if (changeCount >= 10) return 'major';
  if (changeCount >= 5) return 'minor';
  return 'patch';
}

export function countChangelogChanges(changelog: string | null | undefined): number {
  const lines = String(changelog ?? '').split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (lines.length === 0) return 0;

  // Automated deploy changelogs use one abbreviated Git hash + subject per
  // commit. Prefer those rows so wrapped/structured text cannot inflate the
  // significance of historical releases.
  const commitLines = lines.filter((line) => /^[0-9a-f]{7,40}\s+\S/i.test(line));
  if (commitLines.length > 0) return commitLines.length;

  // Legacy/manual entries may use Keep-a-Changelog headings and bullets.
  // Section headings are labels, not individual changes.
  return lines.filter((line) => !/^[-*]?\s*\*\*(added|changed|fixed|removed|refactored|improved|security)\*\*/i.test(line)).length;
}

export function releaseSignificance(
  metadata: Record<string, unknown> | null | undefined,
  changelog: string | null | undefined,
): ReleaseSignificance {
  const storedCount = Number(metadata?.changeCount);
  const changeCount = Number.isInteger(storedCount) && storedCount >= 0
    ? storedCount
    : countChangelogChanges(changelog);
  return { changeCount, releaseType: classifyRelease(changeCount) };
}
