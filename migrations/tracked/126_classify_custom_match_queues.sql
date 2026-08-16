-- Confirmed custom-match queues remain fully ingestible but are excluded from
-- ranked and casual aggregate projections.

INSERT INTO queue_types (
  queue_id,
  queue_name,
  is_ranked,
  stats_scope,
  participant_model,
  stats_enabled,
  track_presence
)
VALUES
  (440, 'Custom Match', FALSE, 'custom', 'custom', FALSE, FALSE),
  (458, 'Custom Match', FALSE, 'custom', 'custom', FALSE, FALSE),
  (10210, 'Custom Match', FALSE, 'custom', 'custom', FALSE, FALSE)
ON CONFLICT (queue_id) DO UPDATE SET
  queue_name = EXCLUDED.queue_name,
  is_ranked = EXCLUDED.is_ranked,
  stats_scope = EXCLUDED.stats_scope,
  participant_model = EXCLUDED.participant_model,
  stats_enabled = EXCLUDED.stats_enabled,
  track_presence = EXCLUDED.track_presence;
