CREATE TABLE IF NOT EXISTS tier_lists (
    post_id         INTEGER PRIMARY KEY REFERENCES posts(id) ON DELETE CASCADE,
    user_id         INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tier_lists_user_id ON tier_lists (user_id);
CREATE INDEX IF NOT EXISTS idx_tier_lists_created_at ON tier_lists (created_at DESC);

CREATE TABLE IF NOT EXISTS tier_list_entries (
    post_id         INTEGER NOT NULL REFERENCES tier_lists(post_id) ON DELETE CASCADE,
    champion_id     INTEGER NOT NULL REFERENCES champions(id),
    tier            VARCHAR(1) NOT NULL CHECK (tier IN ('S', 'A', 'B', 'C', 'D', 'F')),
    position        INTEGER NOT NULL CHECK (position >= 0),
    PRIMARY KEY (post_id, champion_id),
    UNIQUE (post_id, tier, position)
);

CREATE INDEX IF NOT EXISTS idx_tier_list_entries_tier ON tier_list_entries (post_id, tier, position);

COMMENT ON TABLE tier_lists IS 'Community champion tier lists backed by posts so likes, comments, notifications, and moderation use the existing social pipeline.';
COMMENT ON TABLE tier_list_entries IS 'Ordered champion placement for each S-through-F community tier list.';
