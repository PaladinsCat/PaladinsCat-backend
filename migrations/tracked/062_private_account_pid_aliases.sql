-- Use a compact, stable public identifier for inferred private accounts.
-- Verified player names remain separate evidence-backed metadata.

UPDATE players_private
SET alias = 'P-' || lpad(id::text, 6, '0'),
    updated_at = now()
WHERE tracking_version >= 2
  AND is_active
  AND (
    alias IS NULL
    OR btrim(alias) = ''
    OR alias ~* '^private-[0-9]+$'
  );

COMMENT ON COLUMN players_private.alias IS
  'Stable public private-account identifier in P-000000 format; not a Hi-Rez player ID or verified player name.';
