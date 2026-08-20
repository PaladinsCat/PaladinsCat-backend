-- Public automatic-player directories must not scan the complete match fact
-- table per request. Store one deterministic recipient for each metric/match
-- and refresh the small projection whenever ranked lifecycle completion lands.

CREATE TABLE IF NOT EXISTS automatic_player_metric_flags (
  metric TEXT NOT NULL CHECK (metric IN ('wall_shooter','master_feeding','tank_diff','support_diff','dps_diff','flank_diff','noob','hypercarry')),
  match_id BIGINT NOT NULL,
  entry_datetime TIMESTAMPTZ NOT NULL,
  player_id BIGINT NOT NULL,
  flagged_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (metric, match_id, entry_datetime)
);

CREATE INDEX IF NOT EXISTS idx_automatic_player_metric_flags_directory
  ON automatic_player_metric_flags (metric, player_id, entry_datetime DESC);

CREATE OR REPLACE FUNCTION paladinscat_automatic_player_metric_flags(target_match_id BIGINT DEFAULT NULL)
RETURNS TABLE(metric TEXT, match_id BIGINT, entry_datetime TIMESTAMPTZ, player_id BIGINT)
LANGUAGE sql STABLE AS $$
WITH metric_lobbies AS MATERIALIZED (
  SELECT m.match_id,m.entry_datetime
  FROM matches m
  JOIN match_ingest_status status ON status.match_id=m.match_id
  JOIN match_players lobby_player
    ON lobby_player.match_id=m.match_id AND lobby_player.entry_datetime=m.entry_datetime
  WHERE m.queue_id=486
    AND status.population='ranked' AND status.status='complete'
    AND m.duration_seconds>=480 AND NOT COALESCE(m.surrendered,false)
    AND (NOT COALESCE(m.broken,false) OR COALESCE(m.recovered,false))
    AND (target_match_id IS NULL OR m.match_id=target_match_id)
    AND COALESCE(lobby_player.source,'direct') IN ('direct','recovered')
    AND lobby_player.is_ranked=true AND lobby_player.player_id>0 AND lobby_player.champion_id>0
    AND lobby_player.task_force IN (1,2)
    AND LOWER(BTRIM(COALESCE(lobby_player.win_status,''))) IN ('winner','loser','win','loss')
  GROUP BY m.match_id,m.entry_datetime
  HAVING COUNT(*)=10
    AND COUNT(*) FILTER (WHERE lobby_player.egpm>=0 AND lobby_player.egpm<70)=0
), metric_players AS MATERIALIZED (
  SELECT mp.player_id,mp.match_id,mp.entry_datetime,mp.task_force,mp.win_status,
    COALESCE(mp.damage_done_physical,0) AS damage_done_physical,
    COALESCE(mp.kills,0) AS kills,COALESCE(mp.assists,0) AS assists,
    COALESCE(mp.deaths,0) AS deaths,COALESCE(mp.healing,0) AS healing,
    CASE
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 'Frontline'
      WHEN c.roles ILIKE '%Damage%' THEN 'Damage'
      WHEN c.roles ILIKE '%Flank%' THEN 'Flank'
      WHEN c.roles ILIKE '%Support%' THEN 'Support'
      ELSE COALESCE(NULLIF(c.roles,''),'Unknown')
    END AS champion_role
  FROM match_players mp
  JOIN metric_lobbies lobby ON lobby.match_id=mp.match_id AND lobby.entry_datetime=mp.entry_datetime
  JOIN champions c ON c.id=mp.champion_id
  WHERE COALESCE(mp.source,'direct') IN ('direct','recovered')
), team_stats AS MATERIALIZED (
  SELECT match_id,entry_datetime,task_force,MAX(damage_done_physical) AS team_max_damage,
    MAX(kills) AS team_max_kills,
    MAX(damage_done_physical) FILTER (WHERE champion_role IN ('Damage','Flank')) AS damage_flank_max_damage,
    MAX(damage_done_physical) FILTER (WHERE champion_role='Frontline') AS frontline_max_damage,
    MAX(kills) FILTER (WHERE champion_role='Frontline') AS frontline_max_kills,
    MAX(healing) FILTER (WHERE champion_role='Support') AS support_max_healing,
    MAX(damage_done_physical) FILTER (WHERE champion_role='Damage') AS damage_max_damage,
    MAX(kills) FILTER (WHERE champion_role='Damage') AS damage_max_kills,
    MAX(damage_done_physical) FILTER (WHERE champion_role='Flank') AS flank_max_damage,
    MAX(kills) FILTER (WHERE champion_role='Flank') AS flank_max_kills
  FROM metric_players GROUP BY match_id,entry_datetime,task_force
), match_stats AS MATERIALIZED (
  SELECT match_id,entry_datetime,MAX(damage_done_physical) AS match_max_damage,
    MAX(kills) AS match_max_kills,MAX(healing) AS match_max_healing
  FROM metric_players GROUP BY match_id,entry_datetime
), role_players AS (
  SELECT metric_player.*,ROW_NUMBER() OVER (
    PARTITION BY match_id,entry_datetime,task_force ORDER BY damage_done_physical ASC,player_id ASC
  ) AS role_rank
  FROM metric_players metric_player WHERE champion_role IN ('Damage','Flank')
), death_ranked AS (
  SELECT metric_player.*,ROW_NUMBER() OVER (
    PARTITION BY match_id,entry_datetime ORDER BY deaths DESC,player_id ASC
  ) AS death_rank,LEAD(deaths) OVER (
    PARTITION BY match_id,entry_datetime ORDER BY deaths DESC,player_id ASC
  ) AS next_highest_deaths
  FROM metric_players metric_player
), ranked_players AS MATERIALIZED (
  SELECT metric_player.*,
    ROW_NUMBER() OVER (PARTITION BY match_id,entry_datetime ORDER BY damage_done_physical DESC,player_id ASC) AS damage_rank,
    LEAD(damage_done_physical) OVER (PARTITION BY match_id,entry_datetime ORDER BY damage_done_physical DESC,player_id ASC) AS next_highest_damage,
    ROW_NUMBER() OVER (PARTITION BY match_id,entry_datetime ORDER BY kills DESC,player_id ASC) AS kills_rank,
    LEAD(kills) OVER (PARTITION BY match_id,entry_datetime ORDER BY kills DESC,player_id ASC) AS next_highest_kills,
    ROW_NUMBER() OVER (
      PARTITION BY match_id,entry_datetime,task_force
      ORDER BY CASE WHEN champion_role IN ('Damage','Flank') THEN damage_done_physical END ASC NULLS LAST,player_id ASC
    ) AS role_low_damage_rank
  FROM metric_players metric_player
), wall_flags AS (
  SELECT 'wall_shooter'::TEXT AS metric,candidate.match_id,candidate.entry_datetime,candidate.player_id
  FROM role_players candidate
  JOIN team_stats own_team ON own_team.match_id=candidate.match_id AND own_team.entry_datetime=candidate.entry_datetime AND own_team.task_force=candidate.task_force
  JOIN team_stats enemy_team ON enemy_team.match_id=candidate.match_id AND enemy_team.entry_datetime=candidate.entry_datetime AND enemy_team.task_force<>candidate.task_force
  WHERE LOWER(BTRIM(COALESCE(candidate.win_status,''))) IN ('loser','loss')
    AND candidate.role_rank=1
    AND own_team.damage_flank_max_damage>=candidate.damage_done_physical*1.6666667
    AND enemy_team.team_max_damage>candidate.damage_done_physical
), feeding_flags AS (
  SELECT 'master_feeding'::TEXT AS metric,feed_player.match_id,feed_player.entry_datetime,feed_player.player_id
  FROM death_ranked feed_player
  WHERE feed_player.death_rank=1
    AND LOWER(BTRIM(COALESCE(feed_player.win_status,''))) IN ('loser','loss')
    AND feed_player.deaths>=8
    AND (feed_player.kills+feed_player.assists)::NUMERIC/NULLIF(feed_player.deaths,0)<=1.0
    AND feed_player.deaths>=1.25*COALESCE(feed_player.next_highest_deaths,0)
    AND EXISTS(SELECT 1 FROM match_player_items purchased_item
      JOIN items item ON item.item_id=purchased_item.item_id
      WHERE purchased_item.match_id=feed_player.match_id AND purchased_item.player_id=feed_player.player_id
        AND LOWER(BTRIM(COALESCE(item.item_name,'')))='master riding')
), tank_flags AS (
  SELECT 'tank_diff'::TEXT AS metric,candidate.match_id,candidate.entry_datetime,candidate.player_id,
    ROW_NUMBER() OVER (PARTITION BY candidate.match_id,candidate.entry_datetime ORDER BY candidate.damage_done_physical DESC,candidate.kills DESC,candidate.player_id ASC) AS rank
  FROM ranked_players candidate JOIN match_stats ON match_stats.match_id=candidate.match_id AND match_stats.entry_datetime=candidate.entry_datetime
  JOIN team_stats enemy_team ON enemy_team.match_id=candidate.match_id AND enemy_team.entry_datetime=candidate.entry_datetime AND enemy_team.task_force<>candidate.task_force
  WHERE LOWER(BTRIM(COALESCE(candidate.win_status,''))) IN ('winner','win') AND candidate.champion_role='Frontline'
    AND ((candidate.damage_done_physical=match_stats.match_max_damage AND candidate.damage_done_physical>=1.25*COALESCE(enemy_team.frontline_max_damage,0))
      OR (candidate.kills=match_stats.match_max_kills AND candidate.kills>=1.25*COALESCE(enemy_team.frontline_max_kills,0)))
), support_flags AS (
  SELECT 'support_diff'::TEXT AS metric,candidate.match_id,candidate.entry_datetime,candidate.player_id,
    ROW_NUMBER() OVER (PARTITION BY candidate.match_id,candidate.entry_datetime ORDER BY candidate.healing DESC,candidate.player_id ASC) AS rank
  FROM ranked_players candidate JOIN match_stats ON match_stats.match_id=candidate.match_id AND match_stats.entry_datetime=candidate.entry_datetime
  JOIN team_stats enemy_team ON enemy_team.match_id=candidate.match_id AND enemy_team.entry_datetime=candidate.entry_datetime AND enemy_team.task_force<>candidate.task_force
  WHERE LOWER(BTRIM(COALESCE(candidate.win_status,''))) IN ('winner','win') AND candidate.champion_role='Support'
    AND candidate.healing=match_stats.match_max_healing AND candidate.healing>=1.25*COALESCE(enemy_team.support_max_healing,0)
    AND (enemy_team.team_max_damage=match_stats.match_max_damage OR enemy_team.team_max_kills=match_stats.match_max_kills)
), dps_flags AS (
  SELECT 'dps_diff'::TEXT AS metric,candidate.match_id,candidate.entry_datetime,candidate.player_id,
    ROW_NUMBER() OVER (PARTITION BY candidate.match_id,candidate.entry_datetime ORDER BY candidate.damage_done_physical DESC,candidate.kills DESC,candidate.player_id ASC) AS rank
  FROM ranked_players candidate JOIN match_stats ON match_stats.match_id=candidate.match_id AND match_stats.entry_datetime=candidate.entry_datetime
  JOIN team_stats enemy_team ON enemy_team.match_id=candidate.match_id AND enemy_team.entry_datetime=candidate.entry_datetime AND enemy_team.task_force<>candidate.task_force
  WHERE LOWER(BTRIM(COALESCE(candidate.win_status,''))) IN ('winner','win') AND candidate.champion_role='Damage'
    AND ((candidate.damage_done_physical=match_stats.match_max_damage AND candidate.damage_done_physical>=1.25*COALESCE(enemy_team.damage_max_damage,0))
      OR (candidate.kills=match_stats.match_max_kills AND candidate.kills>=1.25*COALESCE(enemy_team.damage_max_kills,0)))
    AND (enemy_team.team_max_damage=match_stats.match_max_damage OR enemy_team.team_max_kills=match_stats.match_max_kills)
), flank_flags AS (
  SELECT 'flank_diff'::TEXT AS metric,candidate.match_id,candidate.entry_datetime,candidate.player_id,
    ROW_NUMBER() OVER (PARTITION BY candidate.match_id,candidate.entry_datetime ORDER BY candidate.damage_done_physical DESC,candidate.kills DESC,candidate.player_id ASC) AS rank
  FROM ranked_players candidate JOIN match_stats ON match_stats.match_id=candidate.match_id AND match_stats.entry_datetime=candidate.entry_datetime
  JOIN team_stats enemy_team ON enemy_team.match_id=candidate.match_id AND enemy_team.entry_datetime=candidate.entry_datetime AND enemy_team.task_force<>candidate.task_force
  WHERE LOWER(BTRIM(COALESCE(candidate.win_status,''))) IN ('winner','win') AND candidate.champion_role='Flank'
    AND ((candidate.damage_done_physical=match_stats.match_max_damage AND candidate.damage_done_physical>=1.25*COALESCE(enemy_team.flank_max_damage,0))
      OR (candidate.kills=match_stats.match_max_kills AND candidate.kills>=1.25*COALESCE(enemy_team.flank_max_kills,0)))
    AND (enemy_team.team_max_damage=match_stats.match_max_damage OR enemy_team.team_max_kills=match_stats.match_max_kills)
), noob_flags AS (
  SELECT 'noob'::TEXT AS metric,candidate.match_id,candidate.entry_datetime,candidate.player_id,
    ROW_NUMBER() OVER (PARTITION BY candidate.match_id,candidate.entry_datetime ORDER BY candidate.damage_done_physical ASC,candidate.kills ASC,candidate.player_id ASC) AS rank
  FROM ranked_players candidate JOIN team_stats own_team ON own_team.match_id=candidate.match_id AND own_team.entry_datetime=candidate.entry_datetime AND own_team.task_force=candidate.task_force
  WHERE LOWER(BTRIM(COALESCE(candidate.win_status,''))) IN ('winner','win') AND candidate.champion_role IN ('Damage','Flank')
    AND candidate.role_low_damage_rank=1 AND candidate.damage_done_physical<=own_team.team_max_damage*.5 AND candidate.kills<=own_team.team_max_kills*.5
), carry_flags AS (
  SELECT 'hypercarry'::TEXT AS metric,candidate.match_id,candidate.entry_datetime,candidate.player_id,
    ROW_NUMBER() OVER (PARTITION BY candidate.match_id,candidate.entry_datetime ORDER BY candidate.damage_done_physical DESC,candidate.kills DESC,candidate.player_id ASC) AS rank
  FROM ranked_players candidate
  WHERE LOWER(BTRIM(COALESCE(candidate.win_status,''))) IN ('winner','win')
    AND candidate.damage_rank=1 AND candidate.kills_rank=1
    AND candidate.damage_done_physical>=1.2*COALESCE(candidate.next_highest_damage,0)
    AND candidate.kills>=1.2*COALESCE(candidate.next_highest_kills,0)
)
SELECT metric,match_id,entry_datetime,player_id FROM wall_flags
UNION ALL SELECT metric,match_id,entry_datetime,player_id FROM feeding_flags
UNION ALL SELECT metric,match_id,entry_datetime,player_id FROM tank_flags WHERE rank=1
UNION ALL SELECT metric,match_id,entry_datetime,player_id FROM support_flags WHERE rank=1
UNION ALL SELECT metric,match_id,entry_datetime,player_id FROM dps_flags WHERE rank=1
UNION ALL SELECT metric,match_id,entry_datetime,player_id FROM flank_flags WHERE rank=1
UNION ALL SELECT metric,match_id,entry_datetime,player_id FROM noob_flags WHERE rank=1
UNION ALL SELECT metric,match_id,entry_datetime,player_id FROM carry_flags WHERE rank=1;
$$;

INSERT INTO automatic_player_metric_flags(metric,match_id,entry_datetime,player_id)
SELECT metric,match_id,entry_datetime,player_id
FROM paladinscat_automatic_player_metric_flags()
ON CONFLICT (metric,match_id,entry_datetime) DO UPDATE SET player_id=EXCLUDED.player_id,flagged_at=now();

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
