-- Reconcile community-vote projections and keep them synchronized at the
-- database boundary. Administrative cheater decisions remain independent of
-- community votes and are intentionally excluded from this projection.

CREATE OR REPLACE FUNCTION sync_player_community_vote_projection(target_player_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  suspicious_votes INTEGER;
  weirdo_votes INTEGER;
  hall_of_fame_votes INTEGER;
  has_dropper_vote BOOLEAN;
  has_afk_wintrade_vote BOOLEAN;
BEGIN
  SELECT
    count(*) FILTER (WHERE vote_type = 'suspicious')::INTEGER,
    count(*) FILTER (WHERE vote_type = 'weirdo')::INTEGER,
    count(*) FILTER (WHERE vote_type = 'hall_of_fame')::INTEGER,
    count(*) FILTER (WHERE vote_type = 'dropper') > 0,
    count(*) FILTER (WHERE vote_type = 'afk_wintrade') > 0
  INTO suspicious_votes, weirdo_votes, hall_of_fame_votes,
       has_dropper_vote, has_afk_wintrade_vote
  FROM player_community_votes
  WHERE player_id = target_player_id;

  UPDATE players
  SET sus_count = suspicious_votes,
      weirdo_count = weirdo_votes,
      hall_of_fame_count = hall_of_fame_votes,
      dropper = has_dropper_vote,
      afk_wintrade = has_afk_wintrade_vote
  WHERE id = target_player_id;
END;
$$;

CREATE OR REPLACE FUNCTION sync_player_community_vote_projection_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP <> 'INSERT' THEN
    PERFORM sync_player_community_vote_projection(OLD.player_id);
  END IF;
  IF TG_OP <> 'DELETE' AND (TG_OP = 'INSERT' OR NEW.player_id <> OLD.player_id) THEN
    PERFORM sync_player_community_vote_projection(NEW.player_id);
  END IF;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_player_community_vote_projection
  ON player_community_votes;
CREATE TRIGGER trg_sync_player_community_vote_projection
AFTER INSERT OR UPDATE OR DELETE ON player_community_votes
FOR EACH ROW EXECUTE FUNCTION sync_player_community_vote_projection_trigger();

WITH vote_counts AS (
  SELECT
    player_id,
    count(*) FILTER (WHERE vote_type = 'suspicious')::INTEGER AS suspicious_votes,
    count(*) FILTER (WHERE vote_type = 'weirdo')::INTEGER AS weirdo_votes,
    count(*) FILTER (WHERE vote_type = 'hall_of_fame')::INTEGER AS hall_of_fame_votes,
    count(*) FILTER (WHERE vote_type = 'dropper') > 0 AS has_dropper_vote,
    count(*) FILTER (WHERE vote_type = 'afk_wintrade') > 0 AS has_afk_wintrade_vote
  FROM player_community_votes
  GROUP BY player_id
)
UPDATE players player
SET sus_count = COALESCE(vote.suspicious_votes, 0),
    weirdo_count = COALESCE(vote.weirdo_votes, 0),
    hall_of_fame_count = COALESCE(vote.hall_of_fame_votes, 0),
    dropper = COALESCE(vote.has_dropper_vote, FALSE),
    afk_wintrade = COALESCE(vote.has_afk_wintrade_vote, FALSE)
FROM players current_player
LEFT JOIN vote_counts vote ON vote.player_id = current_player.id
WHERE player.id = current_player.id
  AND (
    player.sus_count <> COALESCE(vote.suspicious_votes, 0)
    OR player.weirdo_count <> COALESCE(vote.weirdo_votes, 0)
    OR player.hall_of_fame_count <> COALESCE(vote.hall_of_fame_votes, 0)
    OR player.dropper <> COALESCE(vote.has_dropper_vote, FALSE)
    OR player.afk_wintrade <> COALESCE(vote.has_afk_wintrade_vote, FALSE)
  );

CREATE OR REPLACE FUNCTION sync_player_alt_account_projection(target_player_id BIGINT)
RETURNS VOID
LANGUAGE sql
AS $$
  UPDATE players
  SET alt_account = EXISTS (
    SELECT 1
    FROM player_alt_account_votes
    WHERE alt_player_id = target_player_id
  )
  WHERE id = target_player_id;
$$;

CREATE OR REPLACE FUNCTION sync_player_alt_account_projection_trigger()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP <> 'INSERT' THEN
    PERFORM sync_player_alt_account_projection(OLD.alt_player_id);
  END IF;
  IF TG_OP <> 'DELETE' AND (TG_OP = 'INSERT' OR NEW.alt_player_id <> OLD.alt_player_id) THEN
    PERFORM sync_player_alt_account_projection(NEW.alt_player_id);
  END IF;
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_player_alt_account_projection
  ON player_alt_account_votes;
CREATE TRIGGER trg_sync_player_alt_account_projection
AFTER INSERT OR UPDATE OR DELETE ON player_alt_account_votes
FOR EACH ROW EXECUTE FUNCTION sync_player_alt_account_projection_trigger();

UPDATE players player
SET alt_account = EXISTS (
  SELECT 1 FROM player_alt_account_votes vote WHERE vote.alt_player_id = player.id
)
WHERE player.alt_account <> EXISTS (
  SELECT 1 FROM player_alt_account_votes vote WHERE vote.alt_player_id = player.id
);
