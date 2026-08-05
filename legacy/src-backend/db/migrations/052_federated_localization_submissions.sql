CREATE TABLE IF NOT EXISTS localization_api_tokens (
  id BIGSERIAL PRIMARY KEY,
  user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  name VARCHAR(80) NOT NULL,
  token_hash CHAR(64) NOT NULL UNIQUE,
  token_prefix VARCHAR(24) NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  last_used_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_localization_api_tokens_user
  ON localization_api_tokens (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_localization_api_tokens_active
  ON localization_api_tokens (token_hash, expires_at)
  WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS localization_submissions (
  id BIGSERIAL PRIMARY KEY,
  public_id UUID NOT NULL UNIQUE,
  user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  catalog_id VARCHAR(32) NOT NULL CHECK (catalog_id IN ('website', 'game-client')),
  locale VARCHAR(16) NOT NULL,
  base_revision TEXT NOT NULL,
  payload_sha256 CHAR(64) NOT NULL,
  translations JSONB NOT NULL,
  key_count INT NOT NULL CHECK (key_count > 0),
  status VARCHAR(24) NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'needs_rebase', 'approving', 'approved', 'rejected')),
  validation JSONB NOT NULL DEFAULT '{}'::jsonb,
  reviewed_by INT REFERENCES users(id) ON DELETE SET NULL,
  reviewed_at TIMESTAMPTZ,
  review_notes TEXT,
  github_pr_number INT,
  github_pr_url TEXT,
  github_branch TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_localization_submissions_review_queue
  ON localization_submissions (status, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_localization_submissions_user
  ON localization_submissions (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_localization_submissions_locale
  ON localization_submissions (catalog_id, locale, status);
