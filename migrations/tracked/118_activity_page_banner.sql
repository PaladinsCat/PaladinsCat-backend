CREATE TABLE IF NOT EXISTS site_banners (
  id TEXT PRIMARY KEY,
  enabled BOOLEAN NOT NULL DEFAULT FALSE,
  message TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT site_banners_message_length CHECK (char_length(message) <= 500)
);

COMMENT ON TABLE site_banners IS 'Operator-managed single-instance banners for public page surfaces.';

INSERT INTO site_banners (id, enabled, message)
VALUES ('activity', FALSE, '')
ON CONFLICT (id) DO NOTHING;
