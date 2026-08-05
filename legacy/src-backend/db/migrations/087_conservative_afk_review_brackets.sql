-- eCPM from 60 through 119 is a review signal, not proof of AFK. Keep those
-- observations visible in the UI confidence bands while reserving the stored
-- automatic AFK severity for stronger evidence below 60 eCPM.

CREATE OR REPLACE FUNCTION derive_match_player_gameplay_rates()
RETURNS TRIGGER AS $$
DECLARE
  duration_seconds INTEGER;
  effective_cpm NUMERIC;
BEGIN
  SELECT NULLIF(m.duration_seconds, 0)
    INTO duration_seconds
    FROM matches m
   WHERE m.match_id = NEW.match_id
     AND m.entry_datetime = NEW.entry_datetime
   LIMIT 1;

  IF duration_seconds IS NOT NULL AND duration_seconds > 0 THEN
    NEW.gold_per_minute := ROUND(
      COALESCE(NEW.gold_earned, 0)::NUMERIC * 60 / duration_seconds,
      2
    )::DOUBLE PRECISION;
    effective_cpm := ROUND(
      (COALESCE(NEW.gold_earned, 0) - 500)::NUMERIC * 60 / duration_seconds,
      2
    );
    NEW.egpm := effective_cpm::DOUBLE PRECISION;
    NEW.damage_per_minute := ROUND(
      COALESCE(NEW.damage_done_physical, 0)::NUMERIC * 60 / duration_seconds,
      2
    )::DOUBLE PRECISION;
    NEW.healing_per_minute := ROUND(
      COALESCE(NEW.healing, 0)::NUMERIC * 60 / duration_seconds,
      2
    )::DOUBLE PRECISION;
    NEW.healing_self_per_minute := ROUND(
      COALESCE(NEW.healing_self, 0)::NUMERIC * 60 / duration_seconds,
      2
    )::DOUBLE PRECISION;
    NEW.mitigation_per_minute := ROUND(
      COALESCE(NEW.damage_mitigated, 0)::NUMERIC * 60 / duration_seconds,
      2
    )::DOUBLE PRECISION;
    NEW.afk_rate := CASE
      WHEN effective_cpm >= 60 THEN 0
      WHEN effective_cpm >= 40 THEN 2
      ELSE 3
    END;
  ELSE
    NEW.gold_per_minute := 0;
    NEW.egpm := 0;
    NEW.damage_per_minute := 0;
    NEW.healing_per_minute := 0;
    NEW.healing_self_per_minute := 0;
    NEW.mitigation_per_minute := 0;
    NEW.afk_rate := 0;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

UPDATE match_players
SET afk_rate = CASE
  WHEN egpm >= 60 THEN 0
  WHEN egpm >= 40 THEN 2
  ELSE 3
END
WHERE egpm IS NOT NULL
  AND afk_rate IS DISTINCT FROM CASE
    WHEN egpm >= 60 THEN 0
    WHEN egpm >= 40 THEN 2
    ELSE 3
  END;

COMMENT ON COLUMN match_players.afk_rate IS
  'Conservative automatic AFK severity: 0=not auto-flagged (including 60-119 eCPM review bands), 2=partial AFK, 3=full AFK.';

COMMENT ON FUNCTION derive_match_player_gameplay_rates() IS
  'Derives gameplay-duration rates and conservatively auto-flags AFK only below 60 eCPM.';
