-- paladinscat:requires-full-backup
-- OIDC identity is immutable issuer+subject.  Email is deliberately not a key.
CREATE TABLE IF NOT EXISTS user_identities (
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  issuer TEXT NOT NULL,
  subject TEXT NOT NULL,
  linked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ,
  migration_state TEXT NOT NULL DEFAULT 'linked'
    CHECK (migration_state IN ('legacy','linked','reset_required','disabled')),
  PRIMARY KEY (issuer, subject),
  UNIQUE (user_id, issuer)
);
CREATE INDEX IF NOT EXISTS idx_user_identities_user_id ON user_identities(user_id);
