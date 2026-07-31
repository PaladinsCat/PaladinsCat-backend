-- Canonical per-minute metrics use actual gameplay duration from matches.
-- Hi-Rez Time_In_Match_Seconds is retained on match_players as raw evidence,
-- because it can include loading/waiting overhead. Siege eCPM removes the 500
-- starting credits before division.

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
      WHEN effective_cpm >= 80 THEN 0
      WHEN effective_cpm >= 60 THEN 1
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

DROP TRIGGER IF EXISTS trg_match_player_credit_rates ON match_players;
DROP TRIGGER IF EXISTS trg_match_player_gameplay_rates ON match_players;
CREATE TRIGGER trg_match_player_gameplay_rates
  BEFORE INSERT OR UPDATE OF
    gold_earned, damage_done_physical, healing, healing_self,
    damage_mitigated, time_in_match, match_id, entry_datetime
  ON match_players
  FOR EACH ROW
  EXECUTE FUNCTION derive_match_player_gameplay_rates();

-- Recompute every historical fact, not only missing CPM rows. Existing direct
-- API values used Time_In_Match_Seconds and therefore include the same overhead
-- this migration removes from recovered rows.
UPDATE match_players mp
   SET gold_per_minute = ROUND(mp.gold_earned::NUMERIC * 60 / m.duration_seconds, 2)::DOUBLE PRECISION,
       egpm = ROUND((mp.gold_earned - 500)::NUMERIC * 60 / m.duration_seconds, 2)::DOUBLE PRECISION,
       damage_per_minute = ROUND(COALESCE(mp.damage_done_physical, 0)::NUMERIC * 60 / m.duration_seconds, 2)::DOUBLE PRECISION,
       healing_per_minute = ROUND(COALESCE(mp.healing, 0)::NUMERIC * 60 / m.duration_seconds, 2)::DOUBLE PRECISION,
       healing_self_per_minute = ROUND(COALESCE(mp.healing_self, 0)::NUMERIC * 60 / m.duration_seconds, 2)::DOUBLE PRECISION,
       mitigation_per_minute = ROUND(COALESCE(mp.damage_mitigated, 0)::NUMERIC * 60 / m.duration_seconds, 2)::DOUBLE PRECISION,
       afk_rate = CASE
         WHEN (mp.gold_earned - 500)::NUMERIC * 60 / m.duration_seconds >= 80 THEN 0
         WHEN (mp.gold_earned - 500)::NUMERIC * 60 / m.duration_seconds >= 60 THEN 1
         WHEN (mp.gold_earned - 500)::NUMERIC * 60 / m.duration_seconds >= 40 THEN 2
         ELSE 3
       END
  FROM matches m
 WHERE m.match_id = mp.match_id
   AND m.entry_datetime = mp.entry_datetime
   AND m.duration_seconds > 0;

UPDATE match_players mp
   SET gold_per_minute = 0,
       egpm = 0,
       damage_per_minute = 0,
       healing_per_minute = 0,
       healing_self_per_minute = 0,
       mitigation_per_minute = 0,
       afk_rate = 0
  FROM matches m
 WHERE m.match_id = mp.match_id
   AND m.entry_datetime = mp.entry_datetime
   AND COALESCE(m.duration_seconds, 0) <= 0;

-- A corrected match duration must immediately propagate to its player facts.
CREATE OR REPLACE FUNCTION refresh_match_player_gameplay_rates_on_duration_change()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.duration_seconds IS DISTINCT FROM OLD.duration_seconds THEN
    UPDATE match_players
       SET gold_earned = gold_earned
     WHERE match_id = NEW.match_id
       AND entry_datetime = NEW.entry_datetime;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_match_duration_gameplay_rates ON matches;
CREATE TRIGGER trg_match_duration_gameplay_rates
  AFTER UPDATE OF duration_seconds ON matches
  FOR EACH ROW
  EXECUTE FUNCTION refresh_match_player_gameplay_rates_on_duration_change();

DROP FUNCTION IF EXISTS derive_match_player_credit_rates();

COMMENT ON FUNCTION derive_match_player_gameplay_rates() IS
  'Derives CPM, eCPM, AFK severity, DPM, HPM, SHPM, and MPM strictly from match gameplay duration.';
