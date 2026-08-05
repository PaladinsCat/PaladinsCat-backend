SELECT jsonb_build_object(
  'hourly_ingest_state', COALESCE((
    SELECT jsonb_agg(to_jsonb(t) - ARRAY[
      'last_attempt_at','next_retry_at','lease_until','completed_at','created_at','updated_at'
    ] ORDER BY date,hour,queue_id) FROM hourly_ingest_state t
  ), '[]'::jsonb),
  'hourly_ingest_match_debt', COALESCE((
    SELECT jsonb_agg(to_jsonb(t) - ARRAY[
      'first_seen_at','last_attempt_at','next_retry_at','staged_at','completed_at','updated_at'
    ] ORDER BY match_id) FROM hourly_ingest_match_debt t
  ), '[]'::jsonb),
  'match_count_discoveries', COALESCE((
    SELECT jsonb_agg(to_jsonb(t) - ARRAY['first_seen_at','last_seen_at'] ORDER BY match_id,queue_id)
    FROM match_count_discoveries t
  ), '[]'::jsonb),
  'nonranked_match_acquisition', COALESCE((
    SELECT jsonb_agg(to_jsonb(t) - ARRAY[
      'first_discovered_at','last_observed_at','last_attempt_at','lease_until',
      'completed_at','stats_projected_at','updated_at'
    ] ORDER BY match_id) FROM nonranked_match_acquisition t
  ), '[]'::jsonb),
  'raw_ingest_buffer', COALESCE((
    SELECT jsonb_agg(to_jsonb(t) - ARRAY['created_at','available_at','processed_at'] ORDER BY id)
    FROM raw_ingest_buffer t
  ), '[]'::jsonb),
  'matches', COALESCE((
    SELECT jsonb_agg(to_jsonb(t) - 'ingested_at' ORDER BY match_id,entry_datetime) FROM matches t
  ), '[]'::jsonb),
  'match_players', COALESCE((
    SELECT jsonb_agg(to_jsonb(t) - 'created_at' ORDER BY match_id,entry_datetime,player_id) FROM match_players t
  ), '[]'::jsonb)
)::text;
