/**
 * Player profile read-model persistence.
 *
 * `players` is intentionally a hybrid table:
 * - Hi-Rez profile/account fields from getplayer/getplayerbatch.
 * - PaladinsCat derived stats such as rolling ranked averages, match totals,
 *   moderation flags, and ELO/Glicko tables reachable from the profile route.
 *
 * This helper updates only the Hi-Rez profile side. It deliberately avoids
 * touching derived columns like total_matches, total_wins, avg_dpm, avg_hpm,
 * avg_egpm, avg_mpm, cheater, and sus_count. Those are owned by ingest and
 * derived-projection workers.
 *
 * Important storage rule:
 * - Do not make `players` depend on a raw JSON blob for profile data.
 * - Every getplayer/getplayerbatch field has a typed column here, except the
 *   repeatable MergedPlayers array, which is stored in
 *   player_profile_merged_players.
 * - hirez_raw_api_responses remains the operator inspection/audit trail for
 *   exact raw payloads.
 */
import type { PoolClient } from 'pg';
import { transaction } from '../config/db';
import { NormalizedPlayerProfile, NormalizedRankedQueue } from './normalizer';

function mergedPlayerIds(profile: NormalizedPlayerProfile): string[] | null {
  if (!profile.merged_players || profile.merged_players.length === 0) return null;
  return profile.merged_players
    .map((merged) => sanitizeDbString(String(merged.player_id || '').trim()))
    .filter(Boolean);
}

function sanitizeDbString(value: string): string {
  // Hi-Rez profile/history strings can contain embedded NUL bytes. PostgreSQL
  // rejects those even for plain text columns with "invalid byte sequence for
  // encoding UTF8: 0x00". Profile enrichment is a recovery-side bonus, so a bad
  // display string must not poison the whole recovery pass or hide the useful
  // match/player facts already recovered.
  return value.replace(/\u0000/g, '').replace(/\\u0000/g, '');
}

function sanitizeDbValue(value: unknown): unknown {
  if (typeof value === 'string') return sanitizeDbString(value);
  if (Array.isArray(value)) return value.map(sanitizeDbValue);
  return value;
}

function hasUsableProfileDisplayName(profile: NormalizedPlayerProfile): boolean {
  return profile.name_source !== 'none' && Boolean(profile.player_name && profile.player_name.trim());
}

function nullableTimestamp(value: unknown): string | null {
  if (value == null) return null;
  const parsed = new Date(String(value));
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function rankedFields(prefix: 'kbm' | 'controller' | 'conquest', ranked: NormalizedRankedQueue, tierFallback: number): [string, unknown][] {
  return [
    [`${prefix}_name`, ranked.name],
    [`${prefix}_points`, ranked.points],
    [`${prefix}_tier`, ranked.tier || tierFallback],
    [`${prefix}_rank`, ranked.rank],
    [`${prefix}_wins`, ranked.wins],
    [`${prefix}_losses`, ranked.losses],
    [`${prefix}_leaves`, ranked.leaves],
    [`${prefix}_trend`, ranked.trend],
    [`${prefix}_prev_rank`, ranked.prev_rank],
    [`${prefix}_season`, ranked.season],
    [`${prefix}_player_id`, ranked.player_id],
    [`${prefix}_ret_msg`, ranked.ret_msg],
  ];
}

function playerProfileColumns(profile: NormalizedPlayerProfile): [string, unknown][] {
  // If the normalizer rejected every profile name candidate, keep a neutral
  // placeholder for new rows only. On conflict, the SQL below preserves any
  // already-known match-detail name and suppresses old synthetic placeholders.
  const displayName = hasUsableProfileDisplayName(profile) ? profile.player_name : `Player ${profile.player_id}`;
  return [
    ['id', profile.player_id],
    ['active_player_id', profile.active_player_id || profile.player_id],
    ['name', displayName],
    ['level', profile.level],
    ['api_level', profile.api_level],
    ['wins', profile.wins],
    ['losses', profile.losses],
    ['leaves', profile.leaves],
    ['hours_played', profile.hours_played],
    ['minutes_played', profile.minutes_played],
    ['mastery_level', profile.mastery_level],
    ['region', profile.region],
    ['platform', profile.platform],
    ['ret_msg', profile.ret_msg],
    ['total_xp', profile.total_xp],
    ['total_worshippers', profile.total_worshippers],
    ['total_achievements', profile.total_achievements],
    ['avatar_id', profile.avatar_id],
    ['avatar_url', profile.avatar_url],
    ['title', profile.title],
    ['loading_frame', profile.loading_frame],
    ['created_datetime', profile.created_at],
    ['last_login_datetime', profile.last_login],
    ['personal_status_message', profile.personal_status_message],
    ['team_id', profile.team_id],
    ['team_name', profile.team_name],
    ['merged_players', mergedPlayerIds(profile)],
    ['privacy_flag', profile.privacy_flag ? 'y' : 'n'],
    ...rankedFields('kbm', profile.ranked_kbm, profile.tier_ranked_kbm),
    ...rankedFields('controller', profile.ranked_controller, profile.tier_ranked_controller),
    ...rankedFields('conquest', profile.ranked_conquest, profile.tier_conquest),
    ['platform_name', profile.platform_name],
    ['hz_player_name', profile.hz_player_name],
    ['hz_gamer_tag', profile.hz_gamer_tag],
    ['name_source', profile.name_source],
    ['name_anomaly', Boolean(profile.name_anomaly)],
    ['name_anomaly_reason', profile.name_anomaly_reason],
    ['name_anomaly_detected_at', profile.name_anomaly ? new Date().toISOString() : null],
  ];
}

export async function upsertPlayerProfile(profile: NormalizedPlayerProfile, existingClient?: PoolClient): Promise<void> {
  const fields = playerProfileColumns(profile);
  const columns = fields.map(([column]) => column);
  const values = fields.map(([, value]) => sanitizeDbValue(value));
  const placeholders = fields.map((_, index) => `$${index + 1}`);
  const regularUpdateColumns = columns.filter((column) => ![
    'id',
    'name',
    'region',
    'platform',
    'name_source',
    'name_anomaly_reason',
    'name_anomaly_detected_at',
  ].includes(column));

  const updateAssignments = regularUpdateColumns
    .map((column) => `${column} = EXCLUDED.${column}`)
    .join(',\n      ');

  const persist = async (client: PoolClient): Promise<void> => {
    await client.query(
      `INSERT INTO players (
        ${columns.join(', ')},
        first_seen,
        last_seen,
        last_updated,
        hirez_profile_refreshed_at
      )
      VALUES (
        ${placeholders.join(', ')},
        now(),
        now(),
        now(),
        now()
      )
      ON CONFLICT (id) DO UPDATE SET
        -- Do not let an unusable profile name replace a better local name.
        -- Epic can put an obfuscated platform identity in raw Name, and the
        -- relay dummy mode can put DummyPlayer#### into profile-like fields.
        -- The normalizer stores those raw values for audit and sets
        -- name_source='none' when no clean display fallback exists.
        name = CASE
          WHEN EXCLUDED.name_source <> 'none' AND NULLIF(EXCLUDED.name, '') IS NOT NULL THEN EXCLUDED.name
          WHEN players.name ~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$' THEN 'Player ' || players.id::text
          ELSE players.name
        END,
        region = CASE
          WHEN NULLIF(BTRIM(EXCLUDED.region), '') IS NOT NULL
               AND UPPER(EXCLUDED.region) <> 'UNKNOWN'
            THEN EXCLUDED.region
          ELSE players.region
        END,
        platform = CASE
          WHEN NULLIF(BTRIM(EXCLUDED.platform), '') IS NOT NULL
               AND UPPER(EXCLUDED.platform) <> 'UNKNOWN'
            THEN EXCLUDED.platform
          ELSE players.platform
        END,
        ${updateAssignments},
        name_source = CASE
          WHEN EXCLUDED.name_source <> 'none' THEN EXCLUDED.name_source
          ELSE players.name_source
        END,
        name_anomaly_reason = CASE
          WHEN EXCLUDED.name_anomaly THEN EXCLUDED.name_anomaly_reason
          ELSE players.name_anomaly_reason
        END,
        name_anomaly_detected_at = CASE
          WHEN EXCLUDED.name_anomaly THEN COALESCE(players.name_anomaly_detected_at, now())
          ELSE players.name_anomaly_detected_at
        END,
        hirez_profile_refreshed_at = now(),
        last_seen = now(),
        last_updated = now()`,
      values,
    );

    // MergedPlayers is the one repeatable object in the profile response. Keep
    // the quick array of merged ids on players.merged_players, and store the
    // full typed object rows here so no portal/merge timestamp data is trapped
    // in a JSON column.
    await client.query('DELETE FROM player_profile_merged_players WHERE player_id = $1', [profile.player_id]);
    if (profile.merged_players && profile.merged_players.length > 0) {
      for (const merged of profile.merged_players) {
        if (!merged.player_id) continue;
        await client.query(
          `INSERT INTO player_profile_merged_players (
            player_id,
            merged_player_id,
            portal_id,
            merge_datetime,
            profile_refreshed_at
          )
          VALUES ($1, $2, $3, $4, now())
          ON CONFLICT (player_id, merged_player_id) DO UPDATE SET
            portal_id = EXCLUDED.portal_id,
            merge_datetime = EXCLUDED.merge_datetime,
            profile_refreshed_at = EXCLUDED.profile_refreshed_at`,
          [
            profile.player_id,
            merged.player_id,
            merged.portal_id,
            nullableTimestamp(sanitizeDbValue(merged.merge_datetime)),
          ],
        );
      }
    }
  };

  if (existingClient) {
    await persist(existingClient);
  } else {
    await transaction(persist);
  }
}
