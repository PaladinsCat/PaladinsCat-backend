CREATE TABLE IF NOT EXISTS user_notifications (
  id BIGSERIAL PRIMARY KEY,
  user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  actor_user_id INT REFERENCES users(id) ON DELETE SET NULL,
  type VARCHAR(32) NOT NULL CHECK (type IN ('community_comment')),
  post_id INT REFERENCES posts(id) ON DELETE CASCADE,
  comment_id INT REFERENCES comments(id) ON DELETE CASCADE,
  read_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_user_notification_comment UNIQUE (user_id, comment_id)
);

CREATE INDEX IF NOT EXISTS idx_user_notifications_inbox
  ON user_notifications (user_id, read_at, created_at DESC);
