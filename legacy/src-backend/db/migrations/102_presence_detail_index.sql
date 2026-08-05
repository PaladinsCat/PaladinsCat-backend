-- Public 24-hour transparency detail pages walk the complete discovery ledger
-- in a stable newest-first order. Keep cursor pagination bounded without
-- sorting the rolling window for every request.
CREATE INDEX IF NOT EXISTS idx_mcd_presence_detail_window
  ON match_count_discoveries (
    source_date DESC,
    source_hour DESC,
    match_id DESC,
    queue_id DESC
  );
