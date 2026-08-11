CREATE TABLE IF NOT EXISTS public.legacy_password_migration_gate (
  legacy_user_id INTEGER PRIMARY KEY REFERENCES public.users(id),
  keycloak_subject UUID NOT NULL UNIQUE,
  username TEXT NOT NULL UNIQUE,
  client_id TEXT NOT NULL CHECK (client_id = 'paladinscat-web'),
  capability_sha256 CHAR(64) NOT NULL UNIQUE CHECK (capability_sha256 ~ '^[0-9a-f]{64}$'),
  enabled BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  activated_at TIMESTAMPTZ,
  migrated_at TIMESTAMPTZ,
  retired_at TIMESTAMPTZ,
  CHECK ((enabled AND activated_at IS NOT NULL AND migrated_at IS NULL AND retired_at IS NULL)
      OR (NOT enabled AND migrated_at IS NULL AND retired_at IS NULL)
      OR (NOT enabled AND migrated_at IS NOT NULL AND retired_at IS NOT NULL))
);
REVOKE ALL ON TABLE public.legacy_password_migration_gate FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.paladinscat_fetch_legacy_password_migration(
  p_capability TEXT,
  p_subject UUID
)
RETURNS TABLE(legacy_user_id INTEGER, username TEXT, password_hash TEXT, salt TEXT)
LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, public AS $$
  SELECT g.legacy_user_id, g.username, u.password_hash, u.salt
  FROM public.legacy_password_migration_gate g
  JOIN public.users u ON u.id = g.legacy_user_id AND u.username = g.username
  WHERE g.enabled
    AND g.activated_at IS NOT NULL
    AND g.migrated_at IS NULL
    AND g.retired_at IS NULL
    AND u.is_active
    AND g.keycloak_subject = p_subject
    AND g.capability_sha256 = encode(sha256(convert_to(p_capability, 'UTF8')), 'hex');
$$;
REVOKE ALL ON FUNCTION public.paladinscat_fetch_legacy_password_migration(TEXT, UUID) FROM PUBLIC;

CREATE OR REPLACE FUNCTION public.paladinscat_retire_legacy_password_migration(
  p_capability TEXT,
  p_subject UUID
)
RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public AS $$
DECLARE changed INTEGER;
BEGIN
  UPDATE public.legacy_password_migration_gate
  SET enabled=false, migrated_at=COALESCE(migrated_at, now()), retired_at=COALESCE(retired_at, now())
  WHERE enabled
    AND migrated_at IS NULL
    AND retired_at IS NULL
    AND keycloak_subject=p_subject
    AND capability_sha256=encode(sha256(convert_to(p_capability, 'UTF8')), 'hex');
  GET DIAGNOSTICS changed = ROW_COUNT;
  RETURN changed = 1;
END;
$$;
REVOKE ALL ON FUNCTION public.paladinscat_retire_legacy_password_migration(TEXT, UUID) FROM PUBLIC;
