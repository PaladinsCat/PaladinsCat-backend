-- Durable negative/positive lookup cache for Discord's on-demand player
-- resolver. This bounds remote name lookups for missing accounts to once per
-- five minutes even when the bot is restarted or horizontally scaled.
CREATE TABLE IF NOT EXISTS discord_player_lookup_cache (
    lookup_key TEXT PRIMARY KEY,
    player_id BIGINT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_discord_player_lookup_cache_expires
    ON discord_player_lookup_cache (expires_at);
