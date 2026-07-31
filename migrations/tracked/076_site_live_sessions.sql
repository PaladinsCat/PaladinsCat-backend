CREATE INDEX IF NOT EXISTS idx_site_daily_visitors_live_sessions
    ON site_daily_visitors (visit_date, last_seen DESC);

COMMENT ON INDEX idx_site_daily_visitors_live_sessions IS
    'Supports privacy-preserving active-user counts from recently seen anonymous browser sessions.';
