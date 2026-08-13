ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user'
  CHECK (role IN ('user','moderator','developer','admin'));
UPDATE users SET role='admin' WHERE is_admin IS TRUE AND role='user';
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);

CREATE TABLE IF NOT EXISTS tickets (
  id BIGSERIAL PRIMARY KEY,
  requester_user_id INT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  kind TEXT NOT NULL CHECK (kind IN ('bug','feature')),
  status TEXT NOT NULL DEFAULT 'received' CHECK (status IN ('received','under_review','planned','closed')),
  title VARCHAR(180) NOT NULL,
  details TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_tickets_created ON tickets(created_at DESC,id DESC);

CREATE TABLE IF NOT EXISTS ticket_comments (
  id BIGSERIAL PRIMARY KEY,
  ticket_id BIGINT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
  author_user_id INT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  body TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ticket_comments_ticket ON ticket_comments(ticket_id,created_at,id);

CREATE TABLE IF NOT EXISTS project_work_items (
  id BIGSERIAL PRIMARY KEY,
  title VARCHAR(180) NOT NULL,
  component VARCHAR(120) NOT NULL,
  column_name TEXT NOT NULL DEFAULT 'backlog' CHECK (column_name IN ('backlog','building','review','done')),
  priority TEXT NOT NULL DEFAULT 'normal' CHECK (priority IN ('low','normal','high')),
  assignee VARCHAR(120),
  details TEXT NOT NULL DEFAULT '',
  created_by_user_id INT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  updated_by_user_id INT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_project_work_items_column ON project_work_items(column_name,updated_at DESC,id DESC);

ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS ticket_id BIGINT REFERENCES tickets(id) ON DELETE CASCADE;
ALTER TABLE user_notifications ADD COLUMN IF NOT EXISTS ticket_comment_id BIGINT REFERENCES ticket_comments(id) ON DELETE CASCADE;
ALTER TABLE user_notifications DROP CONSTRAINT IF EXISTS user_notifications_type_check;
ALTER TABLE user_notifications ADD CONSTRAINT user_notifications_type_check
  CHECK (type IN ('community_comment','ticket_created','ticket_comment','ticket_status'));
CREATE UNIQUE INDEX IF NOT EXISTS uq_user_ticket_comment_notification
  ON user_notifications(user_id,ticket_comment_id) WHERE ticket_comment_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_user_notifications_ticket
  ON user_notifications(user_id,ticket_id,created_at DESC) WHERE ticket_id IS NOT NULL;
