-- paladinscat:requires-full-backup

-- Match facts are intentionally queue-neutral, but PaladinsCat public
-- statistics are queue-486 ranked data only. Earlier scalable projections
-- admitted casual queues because their schema was queue-aware. Remove those
-- derived rows without touching matches, match_players, or player histories.
DELETE FROM stats_match_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_player_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_item_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_talent_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_card_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_talent_card_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_ban_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_composition_aggregate WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_metric_histogram WHERE queue_id IS DISTINCT FROM 486;
DELETE FROM stats_champion_metric_histogram WHERE queue_id IS DISTINCT FROM 486;

-- The ledger is a projection idempotency record, not match ownership. Clear
-- non-ranked claims so it also represents only the supported stats population.
DELETE FROM stats_projection_matches projection
USING matches match
WHERE projection.match_id = match.match_id
  AND match.queue_id IS DISTINCT FROM 486;
