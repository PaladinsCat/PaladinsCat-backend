-- Durable Discord-to-Paladins defaults. The bot stores the resolved player ID,
-- not the entered name, so renamed Paladins accounts keep working.
CREATE TABLE IF NOT EXISTS discord_saved_players (
    discord_user_id VARCHAR(32) PRIMARY KEY,
    player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    saved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT discord_saved_players_user_id_digits
        CHECK (discord_user_id ~ '^[0-9]{1,32}$')
);

CREATE INDEX IF NOT EXISTS idx_discord_saved_players_player
    ON discord_saved_players (player_id);
