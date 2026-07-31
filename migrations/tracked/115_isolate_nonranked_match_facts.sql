-- paladinscat:requires-full-backup
-- Complete the population boundary introduced by migration 109.
-- Ranked match/player/mechanics facts never retain casual or special rows.

CREATE TABLE IF NOT EXISTS nonranked_match_items (
  match_id BIGINT NOT NULL,
  population VARCHAR(16) NOT NULL CHECK (population IN ('casual','special')),
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  roster_slot SMALLINT NOT NULL CHECK (roster_slot > 0),
  player_id BIGINT NOT NULL DEFAULT 0,
  slot SMALLINT NOT NULL CHECK (slot BETWEEN 1 AND 4),
  item_id INT NOT NULL REFERENCES items(item_id),
  item_level SMALLINT NOT NULL DEFAULT 0 CHECK (item_level BETWEEN 0 AND 3),
  PRIMARY KEY(match_id,roster_slot,item_id),
  FOREIGN KEY(match_id,population)
    REFERENCES match_ingest_status(match_id,population) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_items_item
  ON nonranked_match_items(stats_scope,queue_id,item_id,match_id);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_items_player
  ON nonranked_match_items(player_id,match_id) WHERE player_id>0;

CREATE TABLE IF NOT EXISTS nonranked_match_talents (
  match_id BIGINT NOT NULL,
  population VARCHAR(16) NOT NULL CHECK (population IN ('casual','special')),
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  roster_slot SMALLINT NOT NULL CHECK (roster_slot > 0),
  player_id BIGINT NOT NULL DEFAULT 0,
  champion_id INT NOT NULL,
  talent_id INT NOT NULL REFERENCES talents(talent_id),
  PRIMARY KEY(match_id,roster_slot,talent_id),
  FOREIGN KEY(match_id,population)
    REFERENCES match_ingest_status(match_id,population) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_talents_talent
  ON nonranked_match_talents(stats_scope,queue_id,talent_id,match_id);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_talents_player
  ON nonranked_match_talents(player_id,match_id) WHERE player_id>0;

CREATE TABLE IF NOT EXISTS nonranked_match_cards (
  match_id BIGINT NOT NULL,
  population VARCHAR(16) NOT NULL CHECK (population IN ('casual','special')),
  stats_scope VARCHAR(32) NOT NULL CHECK (stats_scope <> 'ranked'),
  queue_id INT NOT NULL,
  roster_slot SMALLINT NOT NULL CHECK (roster_slot > 0),
  player_id BIGINT NOT NULL DEFAULT 0,
  champion_id INT NOT NULL,
  talent_id INT NOT NULL DEFAULT 0,
  card_id INT NOT NULL REFERENCES cards(card_id),
  card_level SMALLINT NOT NULL DEFAULT 0 CHECK (card_level BETWEEN 0 AND 5),
  PRIMARY KEY(match_id,roster_slot,card_id),
  FOREIGN KEY(match_id,population)
    REFERENCES match_ingest_status(match_id,population) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_cards_card
  ON nonranked_match_cards(stats_scope,queue_id,card_id,match_id);
CREATE INDEX IF NOT EXISTS idx_nonranked_match_cards_talent
  ON nonranked_match_cards(stats_scope,queue_id,talent_id,card_id)
  WHERE talent_id>0;
CREATE INDEX IF NOT EXISTS idx_nonranked_match_cards_player
  ON nonranked_match_cards(player_id,match_id) WHERE player_id>0;

INSERT INTO casual_matches(
  match_id,queue_id,entry_datetime,region,map,duration_seconds,team1_score,
  team2_score,winning_task_force,quality,stats_eligible,player_count,source,
  raw_match,ingested_at,updated_at
)
SELECT m.match_id,m.queue_id,m.entry_datetime,COALESCE(NULLIF(m.region,''),'Unknown'),
  COALESCE(NULLIF(m.map,''),'Unknown'),COALESCE(m.duration_seconds,0),
  m.team1_score,m.team2_score,m.winning_task_force,
  CASE WHEN COALESCE(m.limited,FALSE) THEN 'limited'
    WHEN COALESCE(m.broken,FALSE) AND NOT COALESCE(m.recovered,FALSE) THEN 'partial'
    ELSE 'complete' END,
  NOT COALESCE(m.limited,FALSE)
    AND(NOT COALESCE(m.broken,FALSE) OR COALESCE(m.recovered,FALSE)),
  (SELECT count(*)::SMALLINT FROM match_players p WHERE p.match_id=m.match_id),
  COALESCE(NULLIF(m.source,''),'direct'),NULL,COALESCE(m.ingested_at,now()),now()
FROM matches m JOIN match_ingest_status mis ON mis.match_id=m.match_id
WHERE mis.population='casual'
ON CONFLICT(match_id) DO UPDATE SET
  queue_id=EXCLUDED.queue_id,entry_datetime=EXCLUDED.entry_datetime,
  region=EXCLUDED.region,map=EXCLUDED.map,duration_seconds=EXCLUDED.duration_seconds,
  team1_score=EXCLUDED.team1_score,team2_score=EXCLUDED.team2_score,
  winning_task_force=EXCLUDED.winning_task_force,quality=EXCLUDED.quality,
  stats_eligible=EXCLUDED.stats_eligible,player_count=EXCLUDED.player_count,
  source=EXCLUDED.source,raw_match=NULL,updated_at=now();

INSERT INTO special_matches(
  match_id,queue_id,stats_scope,participant_model,entry_datetime,region,map,
  duration_seconds,team1_score,team2_score,winning_task_force,quality,
  stats_eligible,player_count,source,raw_match,ingested_at,updated_at
)
SELECT m.match_id,m.queue_id,COALESCE(NULLIF(q.stats_scope,'ranked'),'other'),
  COALESCE(NULLIF(q.participant_model,''),'unknown'),m.entry_datetime,
  COALESCE(NULLIF(m.region,''),'Unknown'),COALESCE(NULLIF(m.map,''),'Unknown'),
  COALESCE(m.duration_seconds,0),m.team1_score,m.team2_score,m.winning_task_force,
  CASE WHEN COALESCE(m.limited,FALSE) THEN 'limited'
    WHEN COALESCE(m.broken,FALSE) AND NOT COALESCE(m.recovered,FALSE) THEN 'partial'
    ELSE 'complete' END,
  NOT COALESCE(m.limited,FALSE)
    AND(NOT COALESCE(m.broken,FALSE) OR COALESCE(m.recovered,FALSE)),
  (SELECT count(*)::SMALLINT FROM match_players p WHERE p.match_id=m.match_id),
  COALESCE(NULLIF(m.source,''),'direct'),NULL,COALESCE(m.ingested_at,now()),now()
FROM matches m JOIN match_ingest_status mis ON mis.match_id=m.match_id
LEFT JOIN queue_types q ON q.queue_id=m.queue_id
WHERE mis.population='special'
ON CONFLICT(match_id) DO UPDATE SET
  queue_id=EXCLUDED.queue_id,stats_scope=EXCLUDED.stats_scope,
  participant_model=EXCLUDED.participant_model,entry_datetime=EXCLUDED.entry_datetime,
  region=EXCLUDED.region,map=EXCLUDED.map,duration_seconds=EXCLUDED.duration_seconds,
  team1_score=EXCLUDED.team1_score,team2_score=EXCLUDED.team2_score,
  winning_task_force=EXCLUDED.winning_task_force,quality=EXCLUDED.quality,
  stats_eligible=EXCLUDED.stats_eligible,player_count=EXCLUDED.player_count,
  source=EXCLUDED.source,raw_match=NULL,updated_at=now();

WITH source AS(
  SELECT mp.*,row_number() OVER(
    PARTITION BY mp.match_id ORDER BY mp.task_force,mp.player_id,mp.private_slot,mp.champion_id
  )::SMALLINT roster_slot
  FROM match_players mp JOIN match_ingest_status mis ON mis.match_id=mp.match_id
  WHERE mis.population='casual'
)
INSERT INTO casual_match_players(
  match_id,roster_slot,private_slot,player_id,private_player_id,player_name,
  champion_id,champion_name,task_force,win_status,kills,deaths,assists,damage,
  damage_taken,healing,mitigation,credits,objective_time,account_level,
  mastery_level,party_id,portal_id,portal_user_id,platform,participant_kind,
  source,stats_eligible,raw_player
)
SELECT s.match_id,s.roster_slot,COALESCE(s.private_slot,0),s.player_id,s.private_player_id,
  s.player_name,s.champion_id,c.name,s.task_force,s.win_status,s.kills,s.deaths,
  s.assists,COALESCE(s.damage_done_physical,0),COALESCE(s.damage_taken,0),
  COALESCE(s.healing,0),COALESCE(s.damage_mitigated,0),COALESCE(s.gold_earned,0),
  COALESCE(s.objective_assists,0),COALESCE(s.account_level,0),
  COALESCE(s.mastery_level,0),COALESCE(s.party_id,0),COALESCE(s.portal_id,0),
  NULLIF(s.portal_user_id,''),NULLIF(s.platform,''),
  CASE WHEN s.player_id>0 THEN 'human'
    WHEN upper(COALESCE(s.player_name,''))='PRIVATEACCOUNT' THEN 'private'
    WHEN s.champion_id>0 THEN 'bot' ELSE 'unknown' END,
  COALESCE(NULLIF(s.source,''),'direct'),
  cm.stats_eligible AND s.champion_id>0
    AND lower(COALESCE(s.win_status,'')) IN('winner','win','loser','loss'),
  NULL
FROM source s JOIN casual_matches cm ON cm.match_id=s.match_id
LEFT JOIN champions c ON c.id=s.champion_id
ON CONFLICT(match_id,roster_slot) DO UPDATE SET
  private_slot=EXCLUDED.private_slot,player_id=EXCLUDED.player_id,
  private_player_id=EXCLUDED.private_player_id,player_name=EXCLUDED.player_name,
  champion_id=EXCLUDED.champion_id,champion_name=EXCLUDED.champion_name,
  task_force=EXCLUDED.task_force,win_status=EXCLUDED.win_status,kills=EXCLUDED.kills,
  deaths=EXCLUDED.deaths,assists=EXCLUDED.assists,damage=EXCLUDED.damage,
  damage_taken=EXCLUDED.damage_taken,healing=EXCLUDED.healing,
  mitigation=EXCLUDED.mitigation,credits=EXCLUDED.credits,
  objective_time=EXCLUDED.objective_time,account_level=EXCLUDED.account_level,
  mastery_level=EXCLUDED.mastery_level,party_id=EXCLUDED.party_id,
  portal_id=EXCLUDED.portal_id,portal_user_id=EXCLUDED.portal_user_id,
  platform=EXCLUDED.platform,participant_kind=EXCLUDED.participant_kind,
  source=EXCLUDED.source,stats_eligible=EXCLUDED.stats_eligible,raw_player=NULL;

WITH source AS(
  SELECT mp.*,row_number() OVER(
    PARTITION BY mp.match_id ORDER BY mp.task_force,mp.player_id,mp.private_slot,mp.champion_id
  )::SMALLINT roster_slot
  FROM match_players mp JOIN match_ingest_status mis ON mis.match_id=mp.match_id
  WHERE mis.population='special'
)
INSERT INTO special_match_players(
  match_id,roster_slot,private_slot,player_id,private_player_id,player_name,
  champion_id,champion_name,task_force,win_status,kills,deaths,assists,damage,
  damage_taken,healing,mitigation,credits,objective_time,account_level,
  mastery_level,party_id,portal_id,portal_user_id,platform,participant_kind,
  source,stats_eligible,raw_player
)
SELECT s.match_id,s.roster_slot,COALESCE(s.private_slot,0),s.player_id,s.private_player_id,
  s.player_name,s.champion_id,c.name,s.task_force,s.win_status,s.kills,s.deaths,
  s.assists,COALESCE(s.damage_done_physical,0),COALESCE(s.damage_taken,0),
  COALESCE(s.healing,0),COALESCE(s.damage_mitigated,0),COALESCE(s.gold_earned,0),
  COALESCE(s.objective_assists,0),COALESCE(s.account_level,0),
  COALESCE(s.mastery_level,0),COALESCE(s.party_id,0),COALESCE(s.portal_id,0),
  NULLIF(s.portal_user_id,''),NULLIF(s.platform,''),
  CASE WHEN s.player_id>0 THEN 'human'
    WHEN upper(COALESCE(s.player_name,''))='PRIVATEACCOUNT' THEN 'private'
    WHEN s.champion_id>0 THEN 'bot' ELSE 'unknown' END,
  COALESCE(NULLIF(s.source,''),'direct'),
  sm.stats_eligible AND s.champion_id>0
    AND lower(COALESCE(s.win_status,'')) IN('winner','win','loser','loss'),
  NULL
FROM source s JOIN special_matches sm ON sm.match_id=s.match_id
LEFT JOIN champions c ON c.id=s.champion_id
ON CONFLICT(match_id,roster_slot) DO UPDATE SET
  private_slot=EXCLUDED.private_slot,player_id=EXCLUDED.player_id,
  private_player_id=EXCLUDED.private_player_id,player_name=EXCLUDED.player_name,
  champion_id=EXCLUDED.champion_id,champion_name=EXCLUDED.champion_name,
  task_force=EXCLUDED.task_force,win_status=EXCLUDED.win_status,kills=EXCLUDED.kills,
  deaths=EXCLUDED.deaths,assists=EXCLUDED.assists,damage=EXCLUDED.damage,
  damage_taken=EXCLUDED.damage_taken,healing=EXCLUDED.healing,
  mitigation=EXCLUDED.mitigation,credits=EXCLUDED.credits,
  objective_time=EXCLUDED.objective_time,account_level=EXCLUDED.account_level,
  mastery_level=EXCLUDED.mastery_level,party_id=EXCLUDED.party_id,
  portal_id=EXCLUDED.portal_id,portal_user_id=EXCLUDED.portal_user_id,
  platform=EXCLUDED.platform,participant_kind=EXCLUDED.participant_kind,
  source=EXCLUDED.source,stats_eligible=EXCLUDED.stats_eligible,raw_player=NULL;

INSERT INTO nonranked_match_items(
  match_id,population,stats_scope,queue_id,roster_slot,player_id,slot,item_id,item_level
)
SELECT f.match_id,mis.population,
  CASE WHEN mis.population='casual' THEN 'casual' ELSE sm.stats_scope END,
  COALESCE(cm.queue_id,sm.queue_id),COALESCE(cp.roster_slot,sp.roster_slot),
  f.player_id,f.slot,f.item_id,COALESCE(f.item_level,0)
FROM match_player_items f JOIN match_ingest_status mis ON mis.match_id=f.match_id
LEFT JOIN casual_matches cm ON cm.match_id=f.match_id AND mis.population='casual'
LEFT JOIN casual_match_players cp ON cp.match_id=f.match_id AND cp.player_id=f.player_id AND f.player_id>0
LEFT JOIN special_matches sm ON sm.match_id=f.match_id AND mis.population='special'
LEFT JOIN special_match_players sp ON sp.match_id=f.match_id AND sp.player_id=f.player_id AND f.player_id>0
WHERE mis.population IN('casual','special') AND COALESCE(cp.roster_slot,sp.roster_slot) IS NOT NULL
ON CONFLICT(match_id,roster_slot,item_id) DO UPDATE SET
  population=EXCLUDED.population,stats_scope=EXCLUDED.stats_scope,
  queue_id=EXCLUDED.queue_id,player_id=EXCLUDED.player_id,slot=EXCLUDED.slot,
  item_level=EXCLUDED.item_level;

INSERT INTO nonranked_match_talents(
  match_id,population,stats_scope,queue_id,roster_slot,player_id,champion_id,talent_id
)
SELECT f.match_id,mis.population,
  CASE WHEN mis.population='casual' THEN 'casual' ELSE sm.stats_scope END,
  COALESCE(cm.queue_id,sm.queue_id),COALESCE(cp.roster_slot,sp.roster_slot),
  f.player_id,COALESCE(cp.champion_id,sp.champion_id),f.talent_id
FROM match_player_talents f JOIN match_ingest_status mis ON mis.match_id=f.match_id
LEFT JOIN casual_matches cm ON cm.match_id=f.match_id AND mis.population='casual'
LEFT JOIN casual_match_players cp ON cp.match_id=f.match_id AND cp.player_id=f.player_id AND f.player_id>0
LEFT JOIN special_matches sm ON sm.match_id=f.match_id AND mis.population='special'
LEFT JOIN special_match_players sp ON sp.match_id=f.match_id AND sp.player_id=f.player_id AND f.player_id>0
WHERE mis.population IN('casual','special')
  AND COALESCE(cp.roster_slot,sp.roster_slot) IS NOT NULL
  AND COALESCE(cp.champion_id,sp.champion_id)>0
ON CONFLICT(match_id,roster_slot,talent_id) DO UPDATE SET
  population=EXCLUDED.population,stats_scope=EXCLUDED.stats_scope,
  queue_id=EXCLUDED.queue_id,player_id=EXCLUDED.player_id,
  champion_id=EXCLUDED.champion_id;

INSERT INTO nonranked_match_cards(
  match_id,population,stats_scope,queue_id,roster_slot,player_id,champion_id,
  talent_id,card_id,card_level
)
SELECT f.match_id,mis.population,
  CASE WHEN mis.population='casual' THEN 'casual' ELSE sm.stats_scope END,
  COALESCE(cm.queue_id,sm.queue_id),COALESCE(cp.roster_slot,sp.roster_slot),
  f.player_id,COALESCE(cp.champion_id,sp.champion_id),
  COALESCE(t.talent_id,0),f.card_id,COALESCE(f.card_level,0)
FROM match_player_cards f JOIN match_ingest_status mis ON mis.match_id=f.match_id
LEFT JOIN casual_matches cm ON cm.match_id=f.match_id AND mis.population='casual'
LEFT JOIN casual_match_players cp ON cp.match_id=f.match_id AND cp.player_id=f.player_id AND f.player_id>0
LEFT JOIN special_matches sm ON sm.match_id=f.match_id AND mis.population='special'
LEFT JOIN special_match_players sp ON sp.match_id=f.match_id AND sp.player_id=f.player_id AND f.player_id>0
LEFT JOIN nonranked_match_talents t ON t.match_id=f.match_id
  AND t.roster_slot=COALESCE(cp.roster_slot,sp.roster_slot)
WHERE mis.population IN('casual','special')
  AND COALESCE(cp.roster_slot,sp.roster_slot) IS NOT NULL
  AND COALESCE(cp.champion_id,sp.champion_id)>0
ON CONFLICT(match_id,roster_slot,card_id) DO UPDATE SET
  population=EXCLUDED.population,stats_scope=EXCLUDED.stats_scope,
  queue_id=EXCLUDED.queue_id,player_id=EXCLUDED.player_id,
  champion_id=EXCLUDED.champion_id,talent_id=EXCLUDED.talent_id,
  card_level=EXCLUDED.card_level;

DELETE FROM match_player_items f USING match_ingest_status mis
WHERE mis.match_id=f.match_id AND mis.population IN('casual','special');
DELETE FROM match_player_talents f USING match_ingest_status mis
WHERE mis.match_id=f.match_id AND mis.population IN('casual','special');
DELETE FROM match_player_cards f USING match_ingest_status mis
WHERE mis.match_id=f.match_id AND mis.population IN('casual','special');
DELETE FROM match_players p USING match_ingest_status mis
WHERE mis.match_id=p.match_id AND mis.population IN('casual','special');
DELETE FROM matches m USING match_ingest_status mis
WHERE mis.match_id=m.match_id AND mis.population IN('casual','special');

TRUNCATE item_counts_casual,talent_counts_casual,card_counts_casual,
  item_counts_casual_matches,talent_counts_casual_matches,
  card_counts_casual_matches;

COMMENT ON TABLE item_counts_casual IS
  'Non-ranked item statistics projected only from nonranked_match_items.';
COMMENT ON TABLE talent_counts_casual IS
  'Non-ranked talent statistics projected only from nonranked_match_talents.';
COMMENT ON TABLE card_counts_casual IS
  'Non-ranked card statistics projected only from nonranked_match_cards.';
