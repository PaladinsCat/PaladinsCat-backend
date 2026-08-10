-- The deploy wrapper inserts exactly one immutable gate row for the explicit
-- canary. The Keycloak role can fetch only that one row: it accepts no account
-- parameters, so the role cannot enumerate legacy identities.
CREATE TABLE IF NOT EXISTS public.legacy_password_canary_gate (
  legacy_user_id INTEGER PRIMARY KEY REFERENCES public.users(id),
  username TEXT NOT NULL UNIQUE,
  enabled BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  retired_at TIMESTAMPTZ
);
REVOKE ALL ON TABLE public.legacy_password_canary_gate FROM PUBLIC;
CREATE OR REPLACE FUNCTION public.paladinscat_fetch_legacy_canary()
RETURNS TABLE(legacy_user_id INTEGER, username TEXT, password_hash TEXT, salt TEXT)
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
  WITH gated AS (
    SELECT g.legacy_user_id, g.username, u.password_hash, u.salt
    FROM public.legacy_password_canary_gate g
    JOIN public.users u ON u.id=g.legacy_user_id AND u.username=g.username
    WHERE g.enabled=true AND g.retired_at IS NULL AND u.is_active=true
  )
  SELECT * FROM gated WHERE (SELECT count(*) FROM gated)=1;
$$;
REVOKE ALL ON FUNCTION public.paladinscat_fetch_legacy_canary() FROM PUBLIC;
-- The one-time LOGIN role does not exist at governed migration time.  The
-- explicit canary transaction creates it remotely and grants this function
-- only after the ledger/checksum prerequisite has been verified.
