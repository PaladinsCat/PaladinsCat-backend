-- Queue-aware, exact-tier read models for the public analytics surface.
-- paladinscat:requires-full-backup
-- Canonical match fact tables remain the source of truth. These tables are
-- additive, versioned projections which can be rebuilt without vendor calls.

CREATE TABLE IF NOT EXISTS stats_projection_matches (
  projection_version SMALLINT NOT NULL,
  match_id BIGINT NOT NULL,
  projected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (projection_version, match_id)
);

CREATE TABLE IF NOT EXISTS stats_match_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  stat_date DATE NOT NULL,
  region TEXT NOT NULL,
  map_name TEXT NOT NULL,
  match_count BIGINT NOT NULL DEFAULT 0,
  duration_sum BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, lobby_tier, stat_date, region, map_name)
);

CREATE TABLE IF NOT EXISTS stats_player_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  champion_id INT NOT NULL,
  role_id SMALLINT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
  map_name TEXT NOT NULL,
  platform TEXT NOT NULL,
  plays BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  kills_sum BIGINT NOT NULL DEFAULT 0,
  deaths_sum BIGINT NOT NULL DEFAULT 0,
  assists_sum BIGINT NOT NULL DEFAULT 0,
  damage_sum BIGINT NOT NULL DEFAULT 0,
  gold_sum BIGINT NOT NULL DEFAULT 0,
  healing_sum BIGINT NOT NULL DEFAULT 0,
  mitigation_sum BIGINT NOT NULL DEFAULT 0,
  dpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  hpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  gpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  mpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  egpm_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
  metric_samples BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, lobby_tier, champion_id, map_name, platform)
);

CREATE TABLE IF NOT EXISTS stats_item_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  champion_id INT NOT NULL,
  map_name TEXT NOT NULL,
  item_id INT NOT NULL,
  slot SMALLINT NOT NULL,
  item_level SMALLINT NOT NULL,
  uses BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, lobby_tier, champion_id, map_name, item_id, slot, item_level)
);

CREATE TABLE IF NOT EXISTS stats_talent_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  champion_id INT NOT NULL,
  map_name TEXT NOT NULL,
  talent_id INT NOT NULL,
  uses BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  kills_sum BIGINT NOT NULL DEFAULT 0,
  deaths_sum BIGINT NOT NULL DEFAULT 0,
  assists_sum BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, lobby_tier, champion_id, map_name, talent_id)
);

CREATE TABLE IF NOT EXISTS stats_card_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  champion_id INT NOT NULL,
  card_id INT NOT NULL,
  card_level SMALLINT NOT NULL,
  uses BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  kills_sum BIGINT NOT NULL DEFAULT 0,
  deaths_sum BIGINT NOT NULL DEFAULT 0,
  assists_sum BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, lobby_tier, champion_id, card_id, card_level)
);

CREATE TABLE IF NOT EXISTS stats_talent_card_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  champion_id INT NOT NULL,
  talent_id INT NOT NULL,
  card_id INT NOT NULL,
  card_level SMALLINT NOT NULL,
  uses BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id,lobby_tier,champion_id,talent_id,card_id,card_level)
);

CREATE TABLE IF NOT EXISTS stats_ban_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  map_name TEXT NOT NULL,
  champion_id INT NOT NULL,
  ban_slot SMALLINT NOT NULL,
  bans BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, lobby_tier, map_name, champion_id, ban_slot)
);

CREATE TABLE IF NOT EXISTS stats_composition_aggregate (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  map_name TEXT NOT NULL,
  comp_id TEXT NOT NULL,
  frontline SMALLINT NOT NULL,
  damage SMALLINT NOT NULL,
  flank SMALLINT NOT NULL,
  support SMALLINT NOT NULL,
  uses BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id,lobby_tier,map_name,comp_id)
);

CREATE TABLE IF NOT EXISTS stats_metric_histogram (
  queue_id INT NOT NULL,
  lobby_tier SMALLINT NOT NULL CHECK (lobby_tier BETWEEN 0 AND 26),
  role_id SMALLINT NOT NULL CHECK (role_id BETWEEN 0 AND 4),
  metric TEXT NOT NULL CHECK (metric IN ('dpm', 'hpm', 'gpm', 'egpm', 'mpm', 'kda')),
  value DOUBLE PRECISION NOT NULL,
  sample_count BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, lobby_tier, role_id, metric, value)
);

CREATE TABLE IF NOT EXISTS player_queue_rating_summary (
  queue_id INT NOT NULL,
  player_id BIGINT NOT NULL,
  total_matches BIGINT NOT NULL DEFAULT 0,
  total_wins BIGINT NOT NULL DEFAULT 0,
  total_losses BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (queue_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_stats_match_scope
  ON stats_match_aggregate (queue_id, lobby_tier, stat_date DESC);
CREATE INDEX IF NOT EXISTS idx_stats_match_map
  ON stats_match_aggregate (queue_id, lobby_tier, map_name);
CREATE INDEX IF NOT EXISTS idx_stats_player_scope
  ON stats_player_aggregate (queue_id, lobby_tier, role_id, plays DESC);
CREATE INDEX IF NOT EXISTS idx_stats_player_map
  ON stats_player_aggregate (queue_id, lobby_tier, map_name, champion_id);
CREATE INDEX IF NOT EXISTS idx_stats_player_platform
  ON stats_player_aggregate (queue_id, lobby_tier, platform, champion_id);
CREATE INDEX IF NOT EXISTS idx_stats_item_scope
  ON stats_item_aggregate (queue_id, lobby_tier, champion_id, uses DESC);
CREATE INDEX IF NOT EXISTS idx_stats_item_map
  ON stats_item_aggregate (queue_id, lobby_tier, map_name, item_id);
CREATE INDEX IF NOT EXISTS idx_stats_talent_scope
  ON stats_talent_aggregate (queue_id, lobby_tier, champion_id, uses DESC);
CREATE INDEX IF NOT EXISTS idx_stats_talent_map
  ON stats_talent_aggregate (queue_id, lobby_tier, map_name, talent_id);
CREATE INDEX IF NOT EXISTS idx_stats_card_scope
  ON stats_card_aggregate (queue_id, lobby_tier, champion_id, uses DESC);
CREATE INDEX IF NOT EXISTS idx_stats_talent_card_scope
  ON stats_talent_card_aggregate (queue_id,lobby_tier,champion_id,talent_id,uses DESC);
CREATE INDEX IF NOT EXISTS idx_stats_ban_scope
  ON stats_ban_aggregate (queue_id, lobby_tier, champion_id);
CREATE INDEX IF NOT EXISTS idx_stats_composition_scope
  ON stats_composition_aggregate (queue_id,lobby_tier,map_name,uses DESC);
CREATE INDEX IF NOT EXISTS idx_stats_metric_scope
  ON stats_metric_histogram (queue_id, lobby_tier, role_id, metric, value);
CREATE INDEX IF NOT EXISTS idx_player_queue_rating_summary_rank
  ON player_queue_rating_summary (queue_id, total_matches DESC, player_id);

-- Fact joins most often use match identity plus timestamp. The existing
-- hypertable primary key puts player_id before entry_datetime, which prevents
-- these joins from using a tight lookup path.
CREATE INDEX IF NOT EXISTS idx_match_players_match_time_player
  ON match_players (match_id, entry_datetime, player_id);

WITH source AS (
  SELECT m.queue_id, COALESCE(mlt.lobby_tier, 0)::SMALLINT AS lobby_tier,
    m.entry_datetime::DATE AS stat_date, COALESCE(NULLIF(m.region, ''), 'Unknown') AS region,
    COALESCE(NULLIF(m.map, ''), 'Unknown') AS map_name,
    COUNT(*)::BIGINT AS match_count, COALESCE(SUM(m.duration_seconds), 0)::BIGINT AS duration_sum
  FROM matches m
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
  GROUP BY 1, 2, 3, 4, 5
)
INSERT INTO stats_match_aggregate
SELECT *, now() FROM source
ON CONFLICT (queue_id, lobby_tier, stat_date, region, map_name) DO UPDATE SET
  match_count = EXCLUDED.match_count, duration_sum = EXCLUDED.duration_sum, updated_at = now();

WITH source AS (
  SELECT m.queue_id, COALESCE(mlt.lobby_tier, 0)::SMALLINT AS lobby_tier, mp.champion_id,
    CASE
      WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0
    END::SMALLINT AS role_id,
    COALESCE(NULLIF(m.map, ''), 'Unknown') AS map_name,
    COALESCE(NULLIF(mp.platform, ''), 'Unknown') AS platform,
    COUNT(*)::BIGINT AS plays,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('winner','win'))::BIGINT AS wins,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status, '')) IN ('loser','loss'))::BIGINT AS losses,
    COALESCE(SUM(mp.kills),0)::BIGINT, COALESCE(SUM(mp.deaths),0)::BIGINT,
    COALESCE(SUM(mp.assists),0)::BIGINT, COALESCE(SUM(mp.damage_done_physical),0)::BIGINT,
    COALESCE(SUM(mp.gold_earned),0)::BIGINT, COALESCE(SUM(mp.healing),0)::BIGINT,
    COALESCE(SUM(mp.damage_mitigated),0)::BIGINT, COALESCE(SUM(mp.damage_per_minute),0)::DOUBLE PRECISION,
    COALESCE(SUM(mp.healing_per_minute),0)::DOUBLE PRECISION, COALESCE(SUM(mp.gold_per_minute),0)::DOUBLE PRECISION,
    COALESCE(SUM(mp.mitigation_per_minute),0)::DOUBLE PRECISION, COALESCE(SUM(mp.egpm),0)::DOUBLE PRECISION,
    COUNT(*) FILTER (WHERE mp.time_in_match > 0)::BIGINT AS metric_samples
  FROM match_players mp
  JOIN matches m ON m.match_id = mp.match_id AND m.entry_datetime = mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id = m.match_id AND mlt.entry_datetime = m.entry_datetime
  LEFT JOIN champions c ON c.id = mp.champion_id
  WHERE mp.champion_id > 0 AND COALESCE(mp.source, 'direct') IN ('direct','recovered')
  GROUP BY 1,2,3,4,5,6
)
INSERT INTO stats_player_aggregate
SELECT *, now() FROM source
ON CONFLICT (queue_id, lobby_tier, champion_id, map_name, platform) DO UPDATE SET
  role_id=EXCLUDED.role_id, plays=EXCLUDED.plays, wins=EXCLUDED.wins, losses=EXCLUDED.losses,
  kills_sum=EXCLUDED.kills_sum, deaths_sum=EXCLUDED.deaths_sum, assists_sum=EXCLUDED.assists_sum,
  damage_sum=EXCLUDED.damage_sum, gold_sum=EXCLUDED.gold_sum,
  healing_sum=EXCLUDED.healing_sum, mitigation_sum=EXCLUDED.mitigation_sum,
  dpm_sum=EXCLUDED.dpm_sum, hpm_sum=EXCLUDED.hpm_sum, gpm_sum=EXCLUDED.gpm_sum,
  mpm_sum=EXCLUDED.mpm_sum, egpm_sum=EXCLUDED.egpm_sum, metric_samples=EXCLUDED.metric_samples, updated_at=now();

WITH source AS (
  SELECT m.queue_id, COALESCE(mlt.lobby_tier,0)::SMALLINT, mp.champion_id,
    COALESCE(NULLIF(m.map,''),'Unknown'), mpi.item_id, COALESCE(mpi.slot,0)::SMALLINT,
    COALESCE(mpi.item_level,0)::SMALLINT, COUNT(*)::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT
  FROM match_player_items mpi
  JOIN match_players mp ON mp.match_id=mpi.match_id AND mp.player_id=mpi.player_id
  JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  WHERE mp.champion_id > 0
  GROUP BY 1,2,3,4,5,6,7
)
INSERT INTO stats_item_aggregate SELECT *, now() FROM source
ON CONFLICT (queue_id,lobby_tier,champion_id,map_name,item_id,slot,item_level) DO UPDATE SET
  uses=EXCLUDED.uses,wins=EXCLUDED.wins,losses=EXCLUDED.losses,updated_at=now();

WITH source AS (
  SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT,mp.champion_id,
    COALESCE(NULLIF(m.map,''),'Unknown'),mpt.talent_id,COUNT(*)::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,
    COALESCE(SUM(mp.kills),0)::BIGINT,COALESCE(SUM(mp.deaths),0)::BIGINT,COALESCE(SUM(mp.assists),0)::BIGINT
  FROM match_player_talents mpt
  JOIN match_players mp ON mp.match_id=mpt.match_id AND mp.player_id=mpt.player_id
  JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  WHERE mp.champion_id > 0
  GROUP BY 1,2,3,4,5
)
INSERT INTO stats_talent_aggregate SELECT *,now() FROM source
ON CONFLICT (queue_id,lobby_tier,champion_id,map_name,talent_id) DO UPDATE SET
  uses=EXCLUDED.uses,wins=EXCLUDED.wins,losses=EXCLUDED.losses,kills_sum=EXCLUDED.kills_sum,
  deaths_sum=EXCLUDED.deaths_sum,assists_sum=EXCLUDED.assists_sum,updated_at=now();

WITH source AS (
  SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT,mp.champion_id,mpc.card_id,
    COALESCE(mpc.card_level,0)::SMALLINT,COUNT(*)::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,
    COALESCE(SUM(mp.kills),0)::BIGINT,COALESCE(SUM(mp.deaths),0)::BIGINT,COALESCE(SUM(mp.assists),0)::BIGINT
  FROM match_player_cards mpc
  JOIN match_players mp ON mp.match_id=mpc.match_id AND mp.player_id=mpc.player_id
  JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  WHERE mp.champion_id > 0
  GROUP BY 1,2,3,4,5
)
INSERT INTO stats_card_aggregate SELECT *,now() FROM source
ON CONFLICT (queue_id,lobby_tier,champion_id,card_id,card_level) DO UPDATE SET
  uses=EXCLUDED.uses,wins=EXCLUDED.wins,losses=EXCLUDED.losses,kills_sum=EXCLUDED.kills_sum,
  deaths_sum=EXCLUDED.deaths_sum,assists_sum=EXCLUDED.assists_sum,updated_at=now();

WITH source AS (
  SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT,mp.champion_id,mpt.talent_id,mpc.card_id,
    COALESCE(mpc.card_level,0)::SMALLINT,COUNT(*)::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
    COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT
  FROM match_player_talents mpt JOIN match_player_cards mpc ON mpc.match_id=mpt.match_id AND mpc.player_id=mpt.player_id
  JOIN match_players mp ON mp.match_id=mpt.match_id AND mp.player_id=mpt.player_id
  JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  WHERE mp.champion_id>0 GROUP BY 1,2,3,4,5,6
)
INSERT INTO stats_talent_card_aggregate SELECT *,now() FROM source
ON CONFLICT (queue_id,lobby_tier,champion_id,talent_id,card_id,card_level) DO UPDATE SET
  uses=EXCLUDED.uses,wins=EXCLUDED.wins,losses=EXCLUDED.losses,updated_at=now();

WITH source AS (
  SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT,COALESCE(NULLIF(m.map,''),'Unknown'),
    mb.champion_id,COALESCE(mb.ban_slot,0)::SMALLINT,COUNT(*)::BIGINT
  FROM match_bans mb JOIN matches m ON m.match_id=mb.match_id
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  WHERE mb.champion_id > 0 GROUP BY 1,2,3,4,5
)
INSERT INTO stats_ban_aggregate SELECT *,now() FROM source
ON CONFLICT (queue_id,lobby_tier,map_name,champion_id,ban_slot) DO UPDATE SET bans=EXCLUDED.bans,updated_at=now();

WITH team_rows AS (
  SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT AS lobby_tier,
    COALESCE(NULLIF(m.map,''),'Unknown') AS map_name,mp.match_id,mp.task_force,m.winning_task_force,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%')::SMALLINT AS frontline,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Damage%')::SMALLINT AS damage,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Flank%')::SMALLINT AS flank,
    COUNT(*) FILTER (WHERE c.roles ILIKE '%Support%')::SMALLINT AS support
  FROM match_players mp JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  JOIN champions c ON c.id=mp.champion_id
  WHERE mp.task_force IN (1,2) AND mp.champion_id>0 AND COALESCE(mp.source,'direct') IN ('direct','recovered')
  GROUP BY 1,2,3,4,5,6 HAVING COUNT(*)=5
), source AS (
  SELECT queue_id,lobby_tier,map_name,
    frontline||'-'||damage||'-'||flank||'-'||support AS comp_id,
    frontline,damage,flank,support,COUNT(*)::BIGINT,
    COUNT(*) FILTER (WHERE task_force=winning_task_force)::BIGINT,
    COUNT(*) FILTER (WHERE task_force<>winning_task_force)::BIGINT
  FROM team_rows WHERE frontline+damage+flank+support=5
  GROUP BY 1,2,3,4,5,6,7,8
)
INSERT INTO stats_composition_aggregate SELECT *,now() FROM source
ON CONFLICT (queue_id,lobby_tier,map_name,comp_id) DO UPDATE SET
  uses=EXCLUDED.uses,wins=EXCLUDED.wins,losses=EXCLUDED.losses,updated_at=now();

WITH eligible AS (
  SELECT m.queue_id,COALESCE(mlt.lobby_tier,0)::SMALLINT AS lobby_tier,
    CASE WHEN c.roles ILIKE '%Damage%' THEN 1 WHEN c.roles ILIKE '%Flank%' THEN 2
      WHEN c.roles ILIKE '%Support%' THEN 3
      WHEN c.roles ILIKE '%Frontline%' OR c.roles ILIKE '%Front Line%' THEN 4 ELSE 0 END::SMALLINT AS role_id,
    mp.damage_per_minute,mp.healing_per_minute,mp.gold_per_minute,mp.egpm,mp.mitigation_per_minute,mp.kda
  FROM match_players mp JOIN matches m ON m.match_id=mp.match_id AND m.entry_datetime=mp.entry_datetime
  LEFT JOIN match_lobby_tiers mlt ON mlt.match_id=m.match_id AND mlt.entry_datetime=m.entry_datetime
  LEFT JOIN champions c ON c.id=mp.champion_id
  WHERE mp.champion_id>0 AND mp.time_in_match>120 AND COALESCE(mp.source,'direct') IN ('direct','recovered')
), values_by_scope AS (
  SELECT e.queue_id,e.lobby_tier,scope.role_id,metric.metric,metric.value
  FROM eligible e
  CROSS JOIN LATERAL (
    SELECT DISTINCT role_id FROM (VALUES (0::SMALLINT),(e.role_id)) roles(role_id)
  ) scope
  CROSS JOIN LATERAL (VALUES ('dpm',e.damage_per_minute::DOUBLE PRECISION),('hpm',e.healing_per_minute::DOUBLE PRECISION),
    ('gpm',e.gold_per_minute::DOUBLE PRECISION),('egpm',e.egpm::DOUBLE PRECISION),
    ('mpm',e.mitigation_per_minute::DOUBLE PRECISION),('kda',e.kda::DOUBLE PRECISION)) metric(metric,value)
  WHERE metric.value IS NOT NULL AND metric.value>0
)
INSERT INTO stats_metric_histogram
SELECT queue_id,lobby_tier,role_id,metric,value,COUNT(*)::BIGINT,now()
FROM values_by_scope GROUP BY 1,2,3,4,5
ON CONFLICT (queue_id,lobby_tier,role_id,metric,value) DO UPDATE SET sample_count=EXCLUDED.sample_count,updated_at=now();

INSERT INTO player_queue_rating_summary (queue_id,player_id,total_matches,total_wins,total_losses,updated_at)
SELECT m.queue_id,mrs.player_id,COUNT(*)::BIGINT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('winner','win'))::BIGINT,
  COUNT(*) FILTER (WHERE lower(COALESCE(mp.win_status,'')) IN ('loser','loss'))::BIGINT,now()
FROM match_rating_snapshots mrs JOIN matches m ON m.match_id=mrs.match_id
LEFT JOIN match_players mp ON mp.match_id=mrs.match_id AND mp.player_id=mrs.player_id AND mp.champion_id=mrs.champion_id
GROUP BY m.queue_id,mrs.player_id
ON CONFLICT (queue_id,player_id) DO UPDATE SET total_matches=EXCLUDED.total_matches,
  total_wins=EXCLUDED.total_wins,total_losses=EXCLUDED.total_losses,updated_at=now();

INSERT INTO stats_projection_matches (projection_version,match_id)
SELECT 1,m.match_id FROM matches m
JOIN match_ingest_status mis ON mis.match_id=m.match_id AND mis.status='complete'
ON CONFLICT DO NOTHING;

ANALYZE matches;
ANALYZE match_players;
ANALYZE match_player_items;
ANALYZE match_player_talents;
ANALYZE match_player_cards;
ANALYZE match_bans;
ANALYZE match_rating_snapshots;
ANALYZE stats_match_aggregate;
ANALYZE stats_player_aggregate;
ANALYZE stats_item_aggregate;
ANALYZE stats_talent_aggregate;
ANALYZE stats_card_aggregate;
ANALYZE stats_talent_card_aggregate;
ANALYZE stats_ban_aggregate;
ANALYZE stats_composition_aggregate;
ANALYZE stats_metric_histogram;

COMMENT ON TABLE stats_projection_matches IS 'Versioned idempotency ledger for queue-aware public statistics read models.';
COMMENT ON TABLE stats_player_aggregate IS 'Exact queue/tier/champion/map/platform player aggregates; summed for arbitrary public scopes.';
COMMENT ON TABLE stats_metric_histogram IS 'Exact queue/tier/role metric histogram used for weighted percentiles without historical fact scans.';

-- Older production databases enabled compression on the hypertables but never
-- received the policies embedded in the consolidated bootstrap schema. Run
-- them once daily during the low-traffic UTC window; old immutable chunks are
-- then compressed incrementally by Timescale rather than during deployment.
DO $$
BEGIN
  PERFORM add_compression_policy(
    'matches',INTERVAL '7 days',
    schedule_interval => INTERVAL '1 day',
    initial_start => date_trunc('day',now())+INTERVAL '1 day 08 hours',
    if_not_exists => TRUE
  );
  PERFORM add_compression_policy(
    'match_players',INTERVAL '30 days',
    schedule_interval => INTERVAL '1 day',
    initial_start => date_trunc('day',now())+INTERVAL '1 day 08 hours',
    if_not_exists => TRUE
  );
END $$;
