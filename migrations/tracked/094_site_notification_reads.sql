CREATE TABLE IF NOT EXISTS site_notification_reads (
  user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  notification_id INT NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
  read_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, notification_id)
);

CREATE INDEX IF NOT EXISTS idx_site_notification_reads_user
  ON site_notification_reads (user_id, read_at DESC);
