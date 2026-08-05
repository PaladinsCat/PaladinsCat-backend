CREATE TABLE queue_types (
  queue_id INT PRIMARY KEY,
  queue_name TEXT NOT NULL,
  is_ranked BOOLEAN NOT NULL DEFAULT FALSE,
  stats_scope TEXT NOT NULL,
  participant_model TEXT NOT NULL DEFAULT 'pvp',
  stats_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  track_presence BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO queue_types (
  queue_id,queue_name,is_ranked,stats_scope,participant_model
) VALUES
  (486,'Ranked Siege',TRUE,'ranked','pvp'),
  (424,'Casual Siege',FALSE,'casual','pvp'),
  (10332,'Arcade',FALSE,'arcade','pvp');

CREATE TABLE champions (
  id INT PRIMARY KEY,
  name TEXT NOT NULL,
  title TEXT,
  health INT,
  speed INT,
  roles TEXT
);

CREATE TABLE items (
  item_id INT PRIMARY KEY,
  item_name TEXT,
  description TEXT,
  item_type TEXT,
  cost INT
);

CREATE TABLE talents (
  talent_id INT PRIMARY KEY,
  talent_name TEXT NOT NULL,
  champion_id INT REFERENCES champions(id)
);

CREATE TABLE players (
  id BIGINT PRIMARY KEY,
  name TEXT NOT NULL,
  name_source TEXT NOT NULL DEFAULT 'unknown',
  level INT DEFAULT 0,
  wins INT DEFAULT 0,
  losses INT DEFAULT 0,
  mastery_level INT DEFAULT 0,
  region TEXT,
  platform TEXT,
  portal_id SMALLINT,
  portal_user_id TEXT,
  kbm_tier INT DEFAULT 0,
  kbm_points INT DEFAULT 0,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE player_account_merges (
  player_id BIGINT NOT NULL REFERENCES players(id),
  merged_from_id BIGINT NOT NULL,
  merged_from_portal SMALLINT,
  merge_datetime TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (player_id,merged_from_id)
);

CREATE TABLE matches (
  match_id BIGINT NOT NULL,
  entry_datetime TIMESTAMPTZ NOT NULL,
  map TEXT,
  queue_id INT REFERENCES queue_types(queue_id),
  duration_seconds INT,
  region TEXT,
  team1_score INT,
  team2_score INT,
  winning_task_force INT,
  has_replay BOOLEAN DEFAULT FALSE,
  is_ranked BOOLEAN DEFAULT FALSE,
  recovered BOOLEAN DEFAULT FALSE,
  broken BOOLEAN DEFAULT FALSE,
  private BOOLEAN DEFAULT FALSE,
  limited BOOLEAN NOT NULL DEFAULT FALSE,
  limited_reason TEXT,
  surrendered BOOLEAN DEFAULT FALSE,
  match_level INT,
  source TEXT,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id,entry_datetime)
);

CREATE TABLE match_ingest_status (
  match_id BIGINT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'processing',
  completed_stages TEXT[] NOT NULL DEFAULT '{}',
  source TEXT,
  attempts INT NOT NULL DEFAULT 0,
  error_message TEXT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at TIMESTAMPTZ,
  queue_id INT,
  population TEXT NOT NULL DEFAULT 'unknown'
    CHECK (population IN ('ranked','casual','special','unknown')),
  acquisition_state TEXT NOT NULL DEFAULT 'discovered'
    CHECK (acquisition_state IN (
      'discovered','detail_pending','detail_complete','recovery_pending',
      'facts_ready','complete','limited','unavailable'
    )),
  detail_attempted_at TIMESTAMPTZ,
  roster_resolved_at TIMESTAMPTZ,
  demo_resolved_at TIMESTAMPTZ,
  direct_player_count SMALLINT NOT NULL DEFAULT 0,
  roster_player_count SMALLINT NOT NULL DEFAULT 0,
  unresolved_player_ids BIGINT[] NOT NULL DEFAULT '{}',
  lease_owner TEXT,
  lease_until TIMESTAMPTZ,
  UNIQUE (match_id,population)
);

CREATE TABLE match_ingest_participants (
  match_id BIGINT NOT NULL REFERENCES match_ingest_status(match_id) ON DELETE CASCADE,
  roster_slot SMALLINT NOT NULL,
  player_id BIGINT NOT NULL DEFAULT 0,
  participant_kind TEXT NOT NULL DEFAULT 'human',
  source TEXT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id,roster_slot)
);

CREATE TABLE player_match_history_entries (
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL,
  PRIMARY KEY (match_id,player_id)
);

CREATE TABLE match_players (
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL,
  private_slot SMALLINT NOT NULL DEFAULT 0,
  player_name TEXT,
  region TEXT,
  champion_id INT REFERENCES champions(id),
  skin_id INT,
  skin_name TEXT,
  kills INT DEFAULT 0,
  deaths INT DEFAULT 0,
  assists INT DEFAULT 0,
  damage_done_in_hand INT DEFAULT 0,
  damage_done_physical INT DEFAULT 0,
  damage_done_magical INT DEFAULT 0,
  damage_taken INT DEFAULT 0,
  damage_taken_physical INT DEFAULT 0,
  damage_taken_magical INT DEFAULT 0,
  damage_mitigated INT DEFAULT 0,
  healing INT DEFAULT 0,
  healing_self INT DEFAULT 0,
  healing_bot INT DEFAULT 0,
  healing_player_self INT DEFAULT 0,
  gold_earned INT DEFAULT 0,
  gold_per_minute DOUBLE PRECISION DEFAULT 0,
  objective_assists INT DEFAULT 0,
  camps_cleared INT DEFAULT 0,
  structure_damage INT DEFAULT 0,
  wards_placed INT DEFAULT 0,
  towers_destroyed INT DEFAULT 0,
  distance_traveled INT DEFAULT 0,
  multi_kill_max INT DEFAULT 0,
  killing_spree INT DEFAULT 0,
  kills_first_blood BOOLEAN DEFAULT FALSE,
  kills_double INT DEFAULT 0,
  kills_triple INT DEFAULT 0,
  kills_quadra INT DEFAULT 0,
  kills_penta INT DEFAULT 0,
  kills_fire_giant INT DEFAULT 0,
  kills_gold_fury INT DEFAULT 0,
  kills_phoenix INT DEFAULT 0,
  kills_siege_jugg INT DEFAULT 0,
  kills_wild_jugg INT DEFAULT 0,
  win_status TEXT,
  task_force SMALLINT,
  league_tier INT,
  league_points INT,
  league_wins INT,
  league_losses INT,
  account_level INT,
  mastery_level INT,
  party_id INT,
  party SMALLINT NOT NULL DEFAULT 0,
  kda DOUBLE PRECISION,
  damage_per_minute DOUBLE PRECISION DEFAULT 0,
  healing_per_minute DOUBLE PRECISION DEFAULT 0,
  healing_self_per_minute DOUBLE PRECISION DEFAULT 0,
  time_in_match INT,
  entry_datetime TIMESTAMPTZ NOT NULL,
  source TEXT,
  portal_id SMALLINT,
  is_ranked BOOLEAN DEFAULT FALSE,
  afk_rate DOUBLE PRECISION DEFAULT 0,
  egpm DOUBLE PRECISION DEFAULT 0,
  mitigation_per_minute DOUBLE PRECISION DEFAULT 0,
  private_player_id INT DEFAULT 0,
  portal_user_id TEXT,
  kills_player INT,
  created_at TIMESTAMPTZ,
  platform TEXT,
  damage_bot INT DEFAULT 0,
  kills_single INT DEFAULT 0,
  kills_bot INT DEFAULT 0,
  final_match_level INT DEFAULT 0,
  rank_stat_league INT DEFAULT 0,
  team_id INT,
  surrendered BOOLEAN DEFAULT FALSE,
  has_ret_msg BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (match_id,player_id,private_slot,entry_datetime)
);

CREATE OR REPLACE FUNCTION derive_match_player_gameplay_rates()
RETURNS TRIGGER AS $$
DECLARE
  duration_seconds INTEGER;
  effective_cpm NUMERIC;
BEGIN
  SELECT NULLIF(m.duration_seconds,0)
    INTO duration_seconds
    FROM matches m
   WHERE m.match_id=NEW.match_id
     AND m.entry_datetime=NEW.entry_datetime
   LIMIT 1;
  IF duration_seconds IS NOT NULL AND duration_seconds>0 THEN
    NEW.gold_per_minute := ROUND(COALESCE(NEW.gold_earned,0)::NUMERIC*60/duration_seconds,2);
    effective_cpm := ROUND((COALESCE(NEW.gold_earned,0)-500)::NUMERIC*60/duration_seconds,2);
    NEW.egpm := effective_cpm;
    NEW.damage_per_minute := ROUND(COALESCE(NEW.damage_done_physical,0)::NUMERIC*60/duration_seconds,2);
    NEW.healing_per_minute := ROUND(COALESCE(NEW.healing,0)::NUMERIC*60/duration_seconds,2);
    NEW.healing_self_per_minute := ROUND(COALESCE(NEW.healing_self,0)::NUMERIC*60/duration_seconds,2);
    NEW.mitigation_per_minute := ROUND(COALESCE(NEW.damage_mitigated,0)::NUMERIC*60/duration_seconds,2);
    NEW.afk_rate := CASE WHEN effective_cpm>=70 THEN 0 ELSE 3 END;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_match_player_gameplay_rates
BEFORE INSERT OR UPDATE OF
  gold_earned,damage_done_physical,healing,healing_self,damage_mitigated,
  time_in_match,match_id,entry_datetime
ON match_players
FOR EACH ROW EXECUTE FUNCTION derive_match_player_gameplay_rates();

CREATE TABLE match_player_items (
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL,
  item_id INT NOT NULL REFERENCES items(item_id),
  slot SMALLINT NOT NULL,
  item_level SMALLINT DEFAULT 0,
  PRIMARY KEY (match_id,player_id,item_id)
);

CREATE TABLE match_player_cards (
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL,
  card_id INT NOT NULL,
  card_level SMALLINT DEFAULT 0,
  PRIMARY KEY (match_id,player_id,card_id)
);

CREATE TABLE match_player_talents (
  match_id BIGINT NOT NULL,
  player_id BIGINT NOT NULL,
  talent_id INT NOT NULL REFERENCES talents(talent_id),
  PRIMARY KEY (match_id,player_id,talent_id)
);

CREATE TABLE match_bans (
  match_id BIGINT NOT NULL,
  ban_slot SMALLINT NOT NULL,
  champion_id INT REFERENCES champions(id),
  PRIMARY KEY (match_id,ban_slot)
);

CREATE TABLE hourly_ingest_match_debt (
  match_id BIGINT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'pending',
  reason TEXT,
  next_retry_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE item_counts_ranked (
  item_id INT PRIMARY KEY,
  uses BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE item_counts_casual (
  stats_scope TEXT NOT NULL,
  queue_id INT NOT NULL,
  item_id INT NOT NULL,
  uses BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (stats_scope,queue_id,item_id)
);

CREATE TABLE players_private (
  id SERIAL PRIMARY KEY,
  party_id INT NOT NULL DEFAULT 0,
  account_level INT NOT NULL DEFAULT 0,
  mastery_level INT NOT NULL DEFAULT 0,
  league_tier INT NOT NULL DEFAULT 0,
  league_points INT NOT NULL DEFAULT 0,
  last_known_level INT,
  last_known_mastery INT,
  last_known_league_tier INT,
  last_known_league_points INT,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  match_count INT NOT NULL DEFAULT 0,
  alias VARCHAR(50),
  tracking_version SMALLINT NOT NULL DEFAULT 1,
  identity_status VARCHAR(20) NOT NULL DEFAULT 'inferred',
  identity_confidence SMALLINT NOT NULL DEFAULT 0,
  state_observed_at TIMESTAMPTZ,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  merged_into_id INT REFERENCES players_private(id),
  verified_name VARCHAR(100),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE match_players
  ALTER COLUMN private_player_id DROP DEFAULT,
  ADD CONSTRAINT match_players_private_player_id_fkey
    FOREIGN KEY (private_player_id) REFERENCES players_private(id);

CREATE TABLE players_private_history (
  id SERIAL PRIMARY KEY,
  player_private_id INT NOT NULL REFERENCES players_private(id),
  party_id INT NOT NULL,
  account_level INT NOT NULL,
  mastery_level INT NOT NULL,
  league_tier INT NOT NULL,
  league_points INT NOT NULL,
  match_id BIGINT,
  private_slot SMALLINT NOT NULL DEFAULT 0,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolution_confidence SMALLINT,
  resolution_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX uq_private_history_match_slot
  ON players_private_history (match_id,private_slot)
  WHERE match_id IS NOT NULL;

CREATE TABLE private_account_observations (
  match_id BIGINT NOT NULL,
  private_slot SMALLINT NOT NULL CHECK (private_slot>0),
  entry_datetime TIMESTAMPTZ NOT NULL,
  party_id INT NOT NULL DEFAULT 0,
  account_level INT NOT NULL DEFAULT 0,
  mastery_level INT NOT NULL DEFAULT 0,
  league_tier INT NOT NULL DEFAULT 0,
  league_points INT NOT NULL DEFAULT 0,
  champion_id INT,
  task_force SMALLINT,
  win_status VARCHAR(20),
  portal_id SMALLINT,
  portal_user_id TEXT,
  platform VARCHAR(20),
  source VARCHAR(20) NOT NULL DEFAULT 'direct',
  source_priority SMALLINT NOT NULL DEFAULT 0,
  party_member_ids BIGINT[] NOT NULL DEFAULT '{}',
  private_player_id INT REFERENCES players_private(id) ON DELETE SET NULL,
  resolution_status VARCHAR(24) NOT NULL DEFAULT 'unresolved'
    CHECK (resolution_status IN (
      'unresolved','ambiguous','minimal','new_identity','linked','verified'
    )),
  resolution_confidence SMALLINT NOT NULL DEFAULT 0
    CHECK (resolution_confidence BETWEEN 0 AND 100),
  resolution_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  resolved_at TIMESTAMPTZ,
  queue_id INT,
  stats_scope VARCHAR(32),
  map VARCHAR(200),
  match_end_datetime TIMESTAMPTZ,
  observation_quality VARCHAR(24) NOT NULL DEFAULT 'complete',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id,private_slot)
);

CREATE TABLE private_player_presence_24h (
  private_player_id INT PRIMARY KEY REFERENCES players_private(id) ON DELETE CASCADE,
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at TIMESTAMPTZ NOT NULL,
  last_match_id BIGINT NOT NULL,
  last_queue_id INT NOT NULL,
  last_stats_scope VARCHAR(32) NOT NULL,
  identity_confidence SMALLINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE unresolved_private_presence (
  match_id BIGINT NOT NULL,
  private_slot SMALLINT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  queue_id INT NOT NULL,
  stats_scope VARCHAR(32) NOT NULL,
  reason VARCHAR(80) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id,private_slot)
);
