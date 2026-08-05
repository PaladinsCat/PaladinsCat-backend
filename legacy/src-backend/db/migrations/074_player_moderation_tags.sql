-- Stored moderation decisions used by player badges and directories.
ALTER TABLE players
    ADD COLUMN IF NOT EXISTS dropper BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS afk_wintrade BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS alt_account BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_players_dropper ON players (id) WHERE dropper = TRUE;
CREATE INDEX IF NOT EXISTS idx_players_afk_wintrade ON players (id) WHERE afk_wintrade = TRUE;
CREATE INDEX IF NOT EXISTS idx_players_alt_account ON players (id) WHERE alt_account = TRUE;

ALTER TABLE player_community_votes
    DROP CONSTRAINT IF EXISTS player_community_votes_vote_type_check;
ALTER TABLE player_community_votes
    ADD CONSTRAINT player_community_votes_vote_type_check
    CHECK (vote_type IN (
        'suspicious', 'weirdo', 'hall_of_fame', 'cheater',
        'dropper', 'afk_wintrade', 'alt_account'
    ));

COMMENT ON COLUMN players.dropper IS 'Confirmed match dropper moderation flag.';
COMMENT ON COLUMN players.afk_wintrade IS 'Confirmed AFK or win-trading moderation flag.';
COMMENT ON COLUMN players.alt_account IS 'Confirmed alternate-account moderation flag.';
