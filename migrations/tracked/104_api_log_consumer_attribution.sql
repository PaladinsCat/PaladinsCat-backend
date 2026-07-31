-- Attribute every Hi-Rez request to the backend feature that consumed it.
ALTER TABLE api_log
  ADD COLUMN IF NOT EXISTS consumer VARCHAR(80) NOT NULL DEFAULT 'legacy';

ALTER TABLE api_log
  DROP CONSTRAINT IF EXISTS api_log_pkey;

ALTER TABLE api_log
  ADD CONSTRAINT api_log_pkey PRIMARY KEY (dev_id, endpoint, consumer, hour);

CREATE INDEX IF NOT EXISTS idx_api_log_consumer_hour
  ON api_log (consumer, hour DESC);
