-- Install the lifecycle refresh trigger after migration 130 has committed its
-- historical projection backfill. Keeping this table-level trigger DDL out of
-- the long backfill transaction avoids competing with match ingestion updates.

CREATE OR REPLACE FUNCTION paladinscat_refresh_automatic_player_metric_flags()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  DELETE FROM automatic_player_metric_flags WHERE match_id=NEW.match_id;
  IF NEW.population='ranked' AND NEW.status='complete' THEN
    INSERT INTO automatic_player_metric_flags(metric,match_id,entry_datetime,player_id)
    SELECT metric,match_id,entry_datetime,player_id
    FROM paladinscat_automatic_player_metric_flags(NEW.match_id)
    ON CONFLICT (metric,match_id,entry_datetime) DO UPDATE SET player_id=EXCLUDED.player_id,flagged_at=now();
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_refresh_automatic_player_metric_flags ON match_ingest_status;
CREATE TRIGGER trg_refresh_automatic_player_metric_flags
AFTER INSERT OR UPDATE OF status,population ON match_ingest_status
FOR EACH ROW EXECUTE FUNCTION paladinscat_refresh_automatic_player_metric_flags();
