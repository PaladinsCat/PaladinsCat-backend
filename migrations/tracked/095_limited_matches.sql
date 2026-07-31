-- Preserve authoritative partial detail rows when Hi-Rez cannot return a
-- roster anchor after the single permitted recovery attempt. Limited matches
-- remain readable, are terminal for quota guards, and never enter projections.

ALTER TABLE matches
  ADD COLUMN IF NOT EXISTS limited BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS limited_reason TEXT;

COMMENT ON COLUMN matches.limited IS
  'TRUE when authoritative direct match rows were retained without a complete logical roster. Limited matches are lookup-only and excluded from aggregate/rating/stat projections.';
COMMENT ON COLUMN matches.limited_reason IS
  'Stable machine-readable reason for lookup-only limited match quality.';

ALTER TABLE match_ingest_status
  DROP CONSTRAINT IF EXISTS match_ingest_status_status_check;

ALTER TABLE match_ingest_status
  ADD CONSTRAINT match_ingest_status_status_check
  CHECK (status IN ('processing', 'partial', 'complete', 'limited', 'failed'));

COMMENT ON TABLE match_ingest_status IS
  'Durable match ingest state. complete and limited are terminal; only complete matches are eligible for aggregate projections.';
