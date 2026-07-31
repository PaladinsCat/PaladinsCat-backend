-- `players.cheater` is the sole application moderation state. Older releases
-- also wrote `cheater_status`, which could drift and made different endpoints
-- disagree. Preserve every confirmed legacy decision in the boolean before
-- the application stops reading the compatibility column.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'players'
      AND column_name = 'cheater_status'
  ) THEN
    UPDATE players
    SET cheater = TRUE
    WHERE cheater_status = 'confirmed'
      AND cheater IS DISTINCT FROM TRUE;

    UPDATE players
    SET cheater_status = CASE WHEN cheater THEN 'confirmed' ELSE NULL END
    WHERE cheater_status IS DISTINCT FROM CASE WHEN cheater THEN 'confirmed' ELSE NULL END;

    COMMENT ON COLUMN players.cheater_status IS
      'Deprecated deployment-compatibility column. players.cheater is the sole canonical moderation flag.';

    -- Keep the old column only as a rolling-deployment compatibility mirror.
    -- It is no longer an independent state and can be dropped after every
    -- rollback image predates the former cheater_status contract.
    EXECUTE $function$
      CREATE OR REPLACE FUNCTION mirror_legacy_cheater_status()
      RETURNS trigger
      LANGUAGE plpgsql
      AS $body$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          NEW.cheater := COALESCE(NEW.cheater, FALSE)
            OR COALESCE(NEW.cheater_status = 'confirmed', FALSE);
        ELSIF NEW.cheater IS NOT DISTINCT FROM OLD.cheater
          AND NEW.cheater_status IS DISTINCT FROM OLD.cheater_status THEN
          NEW.cheater := COALESCE(NEW.cheater_status = 'confirmed', FALSE);
        END IF;

        NEW.cheater_status := CASE WHEN NEW.cheater THEN 'confirmed' ELSE NULL END;
        RETURN NEW;
      END
      $body$
    $function$;
    EXECUTE 'DROP TRIGGER IF EXISTS trg_mirror_legacy_cheater_status ON players';
    EXECUTE 'CREATE TRIGGER trg_mirror_legacy_cheater_status
      BEFORE INSERT OR UPDATE OF cheater, cheater_status ON players
      FOR EACH ROW EXECUTE FUNCTION mirror_legacy_cheater_status()';
  END IF;
END
$$;
