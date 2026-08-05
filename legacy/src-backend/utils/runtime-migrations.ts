import fs from 'fs';
import path from 'path';
import { query } from '../config/db';
function findSeedFile(fileName: string): string | null {
  const candidates = [
    path.resolve(__dirname, '..', 'db', fileName),
    path.resolve(__dirname, '..', '..', 'db', fileName),
    path.resolve(process.cwd(), 'dist', 'db', fileName),
    path.resolve(process.cwd(), 'db', fileName),
  ];
  return candidates.find((candidate) => fs.existsSync(candidate)) ?? null;
}

async function ensureCardReferenceSeed(): Promise<void> {
  const before = await query<{ count: number }>(`SELECT COUNT(*)::INT AS count FROM cards`);
  const currentCount = Number(before[0]?.count ?? 0);
  if (currentCount >= 400) return;

  const seedPath = findSeedFile('004_seed_data.sql');
  if (!seedPath) {
    console.warn('[runtime-migrations] card seed skipped: 004_seed_data.sql not found');
    return;
  }

  // Existing Docker volumes never replay /docker-entrypoint-initdb.d seed files.
  // The champion card stats endpoint LEFT JOINs from cards so every card can
  // render, including zero-play cards. If this reference table is empty after a
  // restore or old deploy, all champion card stats disappear even though the raw
  // match_player_cards facts are present. Replaying the idempotent seed restores
  // the reference surface without touching match facts.
  await query(fs.readFileSync(seedPath, 'utf8'));
  const after = await query<{ count: number }>(`SELECT COUNT(*)::INT AS count FROM cards`);
  console.log(`[runtime-migrations] card reference seed checked: ${currentCount} -> ${Number(after[0]?.count ?? 0)} cards`);
}

/**
 * Runtime migrations that need to be applied on startup.
 * These are for data fixes that can't be handled by Docker init scripts
 * (which only run on empty data volumes).
 *
 * Each migration is idempotent and guarded by a WHERE clause.
 */
async function applyRuntimeMigrations(): Promise<void> {
  await ensureCardReferenceSeed();

  // Migration: Update notification message to remove v1.0 version reference
  // and simplify to just "PaladinsCat - Open Beta"
  await query(
    `UPDATE notifications
     SET message = 'PaladinsCat - Open Beta.'
     WHERE id = 1 AND message LIKE 'PaladinsCat v%' OR message LIKE 'PaladinsCat - Open Beta. WORK IN PROGRESS%'`,
  );

  // Migration: Add card-level storage for community builds.
  // Build items and talents are simple ID arrays, but champion loadout cards
  // need both a card ID and a 1-5 investment level. Keeping that in a JSONB
  // column preserves existing builds while allowing the create/detail pages to
  // render the actual Paladins deck shape: 4 items, 5 leveled cards, 1 talent.
  await query(`ALTER TABLE builds ADD COLUMN IF NOT EXISTS cards JSONB NOT NULL DEFAULT '[]'::jsonb`);
  // Community player labels are backed by a per-user vote table, while their
  // materialized counts/flags remain on `players` for directory/profile reads.
  // Keep this bootstrap migration here because established Docker volumes do
  // not replay db/002_extended_schema.sql.
  await query(`ALTER TABLE players
    ADD COLUMN IF NOT EXISTS weirdo_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hall_of_fame_count INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS dropper BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS afk_wintrade BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS alt_account BOOLEAN NOT NULL DEFAULT FALSE`);
  await query(`CREATE TABLE IF NOT EXISTS player_community_votes (
    id BIGSERIAL PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    vote_type VARCHAR(20) NOT NULL CHECK (vote_type IN ('suspicious', 'weirdo', 'hall_of_fame', 'cheater')),
    reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_player_community_vote UNIQUE (player_id, user_id, vote_type)
  )`);
  // Earlier deployments created this table for the first community labels.
  // Some moderation reports store reasons; lightweight community votes use ''.
  await query(`ALTER TABLE player_community_votes ADD COLUMN IF NOT EXISTS reason TEXT NOT NULL DEFAULT ''`);
  await query(`ALTER TABLE player_community_votes DROP CONSTRAINT IF EXISTS player_community_votes_vote_type_check`);
  await query(`ALTER TABLE player_community_votes
    ADD CONSTRAINT player_community_votes_vote_type_check
    CHECK (vote_type IN ('suspicious', 'weirdo', 'hall_of_fame', 'cheater', 'dropper', 'afk_wintrade', 'alt_account'))`);
  await query(`CREATE INDEX IF NOT EXISTS idx_player_community_votes_type_created
    ON player_community_votes (vote_type, created_at DESC)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_players_dropper ON players (id) WHERE dropper = TRUE`);
  await query(`CREATE INDEX IF NOT EXISTS idx_players_afk_wintrade ON players (id) WHERE afk_wintrade = TRUE`);
  await query(`CREATE INDEX IF NOT EXISTS idx_players_alt_account ON players (id) WHERE alt_account = TRUE`);
  await query(`ALTER TABLE players_private ADD COLUMN IF NOT EXISTS sus_count INT NOT NULL DEFAULT 0`);
  await query(`CREATE INDEX IF NOT EXISTS idx_players_private_suspicious
    ON players_private (sus_count DESC, last_seen DESC, id DESC) WHERE sus_count > 0`);
  await query(`CREATE TABLE IF NOT EXISTS private_account_community_votes (
    id BIGSERIAL PRIMARY KEY,
    private_player_id INTEGER NOT NULL REFERENCES players_private(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    vote_type VARCHAR(20) NOT NULL CHECK (vote_type IN ('suspicious', 'cheater')),
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_private_account_community_vote UNIQUE (private_player_id, user_id, vote_type)
  )`);
  await query(`CREATE INDEX IF NOT EXISTS idx_private_account_votes_target
    ON private_account_community_votes (private_player_id, vote_type, created_at DESC)`);
  await query(`CREATE TABLE IF NOT EXISTS player_alt_account_votes (
    id BIGSERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    main_player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    alt_player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT player_alt_account_votes_distinct_players CHECK (main_player_id <> alt_player_id),
    CONSTRAINT player_alt_account_votes_direction_unique UNIQUE (user_id, main_player_id, alt_player_id)
  )`);
  await query(`CREATE UNIQUE INDEX IF NOT EXISTS uq_player_alt_account_votes_user_pair
    ON player_alt_account_votes (user_id, LEAST(main_player_id, alt_player_id), GREATEST(main_player_id, alt_player_id))`);
  await query(`CREATE INDEX IF NOT EXISTS idx_player_alt_account_votes_main
    ON player_alt_account_votes (main_player_id, updated_at DESC)`);
  await query(`CREATE INDEX IF NOT EXISTS idx_player_alt_account_votes_alt
    ON player_alt_account_votes (alt_player_id, updated_at DESC)`);

  // Community replies are the first account-scoped notification type. Keep the
  // target post/comment and actor as foreign keys so the account page can show
  // a useful link without denormalized message text going stale after edits.
  await query(`CREATE TABLE IF NOT EXISTS user_notifications (
    id BIGSERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    actor_user_id INT REFERENCES users(id) ON DELETE SET NULL,
    type VARCHAR(32) NOT NULL CHECK (type IN ('community_comment')),
    post_id INT REFERENCES posts(id) ON DELETE CASCADE,
    comment_id INT REFERENCES comments(id) ON DELETE CASCADE,
    read_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_user_notification_comment UNIQUE (user_id, comment_id)
  )`);
  await query(`CREATE INDEX IF NOT EXISTS idx_user_notifications_inbox
    ON user_notifications (user_id, read_at, created_at DESC)`);

  // Site announcements are shared globally, but read state belongs to the
  // signed-in account. Keep the relationship sparse: a row only exists after
  // that account has read a notification.
  await query(`CREATE TABLE IF NOT EXISTS site_notification_reads (
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    notification_id INT NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    read_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, notification_id)
  )`);
  await query(`CREATE INDEX IF NOT EXISTS idx_site_notification_reads_user
    ON site_notification_reads (user_id, read_at DESC)`);

  // Authoritative match facts already store skin IDs. This index powers the
  // ranked skin detail route without a duplicate counter projection.
  await query(`CREATE INDEX IF NOT EXISTS idx_match_players_skin_stats
    ON match_players (champion_id, skin_id, league_tier)
    WHERE skin_id IS NOT NULL AND skin_id > 0`);

  // Saved decks are one-to-many per player/champion (up to nine in-game).
  // Older builds used one row per champion, silently replacing all but the
  // last deck. Upgrade the cache key before the player-loadout routes read it.
  await query(`ALTER TABLE player_loadouts ADD COLUMN IF NOT EXISTS deck_id BIGINT`);
  await query(`ALTER TABLE player_loadouts ADD COLUMN IF NOT EXISTS deck_key TEXT`);
  await query(`UPDATE player_loadouts
    SET deck_key = 'legacy:' || player_id::TEXT || ':' || id::TEXT
    WHERE deck_key IS NULL OR deck_key = ''`);
  await query(`ALTER TABLE player_loadouts ALTER COLUMN deck_key SET NOT NULL`);
  await query(`ALTER TABLE player_loadouts DROP CONSTRAINT IF EXISTS player_loadouts_player_id_champion_id_key`);
  await query(`DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_player_loadouts_deck'
          AND conrelid = 'player_loadouts'::regclass
      ) THEN
        ALTER TABLE player_loadouts
          ADD CONSTRAINT uq_player_loadouts_deck UNIQUE (player_id, deck_key);
      END IF;
    END $$`);
  await query(`CREATE INDEX IF NOT EXISTS idx_pl_player_champion ON player_loadouts (player_id, champion_id)`);
  await query(`CREATE TABLE IF NOT EXISTS player_loadout_fetches (
    player_id BIGINT PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_manual_refresh_at TIMESTAMPTZ
  )`);

}

export { applyRuntimeMigrations };
