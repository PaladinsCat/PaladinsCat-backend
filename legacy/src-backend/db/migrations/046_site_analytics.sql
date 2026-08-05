-- Privacy-minimal first-party traffic counters. The browser identifier is
-- hashed before storage; raw identifiers and IP addresses are never retained.
CREATE TABLE IF NOT EXISTS site_daily_visitors (
  visit_date DATE NOT NULL,
  visitor_hash TEXT NOT NULL,
  page_views INT NOT NULL DEFAULT 1,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (visit_date, visitor_hash)
);

CREATE INDEX IF NOT EXISTS idx_site_daily_visitors_date
  ON site_daily_visitors (visit_date DESC);

CREATE TABLE IF NOT EXISTS site_daily_page_views (
  visit_date DATE NOT NULL,
  path TEXT NOT NULL,
  page_views INT NOT NULL DEFAULT 1,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (visit_date, path)
);

CREATE INDEX IF NOT EXISTS idx_site_daily_page_views_date_views
  ON site_daily_page_views (visit_date DESC, page_views DESC);

COMMENT ON TABLE site_daily_visitors IS
  'Daily unique anonymous browser hashes and page-view totals; no raw visitor identifiers or IP addresses.';
COMMENT ON TABLE site_daily_page_views IS
  'Daily normalized public route view totals for the private admin dashboard.';
