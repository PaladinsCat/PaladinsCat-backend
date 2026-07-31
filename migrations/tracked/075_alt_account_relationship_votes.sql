CREATE TABLE IF NOT EXISTS player_alt_account_votes (
    id              BIGSERIAL PRIMARY KEY,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    main_player_id  BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    alt_player_id   BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT player_alt_account_votes_distinct_players CHECK (main_player_id <> alt_player_id),
    CONSTRAINT player_alt_account_votes_direction_unique UNIQUE (user_id, main_player_id, alt_player_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_player_alt_account_votes_user_pair
    ON player_alt_account_votes (
        user_id,
        LEAST(main_player_id, alt_player_id),
        GREATEST(main_player_id, alt_player_id)
    );

CREATE INDEX IF NOT EXISTS idx_player_alt_account_votes_main
    ON player_alt_account_votes (main_player_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_player_alt_account_votes_alt
    ON player_alt_account_votes (alt_player_id, updated_at DESC);

COMMENT ON TABLE player_alt_account_votes IS
    'Directional, reason-free community votes linking one main Paladins account to one alternate account; each site user owns one vote per unordered player pair.';

COMMENT ON COLUMN players.alt_account IS
    'Materialized community relationship flag; true while at least one directional vote identifies this player as an alternate account.';
