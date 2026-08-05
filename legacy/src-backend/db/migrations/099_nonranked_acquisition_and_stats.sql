-- Non-ranked acquisition, quality ledger, compact statistics, and rolling
-- presence. Ranked remains on its existing authoritative pipeline.

ALTER TABLE queue_types
  ADD COLUMN IF NOT EXISTS stats_scope VARCHAR(32) NOT NULL DEFAULT 'other',
  ADD COLUMN IF NOT EXISTS participant_model VARCHAR(16) NOT NULL DEFAULT 'unknown',
  ADD COLUMN IF NOT EXISTS stats_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS track_presence BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE queue_types DROP CONSTRAINT IF EXISTS queue_types_stats_scope_check;
ALTER TABLE queue_types ADD CONSTRAINT queue_types_stats_scope_check CHECK (
  stats_scope IN (
    'ranked', 'casual', 'bot', 'team_deathmatch', 'arcade',
    'wave_defense', 'experiment', 'newcomer', 'custom', 'other'
  )
);
ALTER TABLE queue_types DROP CONSTRAINT IF EXISTS queue_types_participant_model_check;
ALTER TABLE queue_types ADD CONSTRAINT queue_types_participant_model_check CHECK (
  participant_model IN ('pvp', 'pve', 'bots', 'custom', 'unknown')
);

INSERT INTO queue_types (
  queue_id, queue_name, is_ranked, stats_scope, participant_model,
  stats_enabled, track_presence
) VALUES
  (0, 'Unknown', false, 'other', 'unknown', false, false),
  (424, 'Casual Siege', false, 'casual', 'pvp', true, true),
  (425, 'Siege Training', false, 'bot', 'bots', true, true),
  (452, 'Casual Onslaught', false, 'casual', 'pvp', true, true),
  (453, 'Onslaught Training', false, 'bot', 'bots', true, true),
  (469, 'Team Deathmatch', false, 'team_deathmatch', 'pvp', true, true),
  (486, 'Ranked Siege', true, 'ranked', 'pvp', true, true),
  (10297, 'Team Deathmatch Training', false, 'bot', 'bots', true, true),
  (10332, 'Arcade', false, 'arcade', 'pvp', true, true),
  (10348, 'Wave Defense Party Beta', false, 'wave_defense', 'pve', true, true),
  (10362, 'Wave Defense Public Beta', false, 'wave_defense', 'pve', true, true),
  (10367, 'Newcomer', false, 'newcomer', 'pvp', true, true),
  (10369, 'Experiment: Subclasses', false, 'experiment', 'pvp', true, true)
ON CONFLICT (queue_id) DO UPDATE SET
  queue_name = EXCLUDED.queue_name,
  is_ranked = EXCLUDED.is_ranked,
  stats_scope = EXCLUDED.stats_scope,
  participant_model = EXCLUDED.participant_model,
  stats_enabled = EXCLUDED.stats_enabled,
  track_presence = EXCLUDED.track_presence;

CREATE TABLE IF NOT EXISTS nonranked_match_acquisition (
  match_id BIGINT PRIMARY KEY,
  queue_id INT NOT NULL REFERENCES queue_types(queue_id),
  stats_scope VARCHAR(32) NOT NULL,
  source_date DATE NOT NULL,
  source_hour SMALLINT NOT NULL CHECK (source_hour BETWEEN 0 AND 23),
  region VARCHAR(20) NOT NULL DEFAULT 'Unknown',
  discovered_entry_datetime TIMESTAMPTZ,
  status VARCHAR(24) NOT NULL DEFAULT 'discovered' CHECK (
    status IN (
      'discovered', 'fetching', 'complete_direct', 'partial_roster',
      'roster_only', 'service_deferred', 'dropped'
    )
  ),
  quality VARCHAR(24) NOT NULL DEFAULT 'unknown' CHECK (
    quality IN ('unknown', 'complete', 'partial', 'limited', 'unavailable')
  ),
  direct_player_count SMALLINT NOT NULL DEFAULT 0,
  roster_player_count SMALLINT NOT NULL DEFAULT 0,
  detail_attempts SMALLINT NOT NULL DEFAULT 0,
  roster_attempts SMALLINT NOT NULL DEFAULT 0,
  first_discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_attempt_at TIMESTAMPTZ,
  lease_until TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  stats_projected_at TIMESTAMPTZ,
  terminal_reason VARCHAR(80),
  error_message TEXT,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_nrma_claim
  ON nonranked_match_acquisition (status, lease_until, source_date, source_hour, match_id);
CREATE INDEX IF NOT EXISTS idx_nrma_dropped_window
  ON nonranked_match_acquisition (source_date DESC, source_hour DESC, queue_id, match_id)
  WHERE status = 'dropped';
CREATE INDEX IF NOT EXISTS idx_nrma_scope_window
  ON nonranked_match_acquisition (stats_scope, source_date DESC, source_hour DESC);

-- Casual Siege and Casual Onslaught are deliberately isolated from Ranked and
-- from rotating/special queues. Casual has no ban fact table.
CREATE TABLE IF NOT EXISTS casual_matches (
  match_id BIGINT PRIMARY KEY,
  queue_id INT NOT NULL,
  entry_datetime TIMESTAMPTZ NOT NULL,
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  duration_seconds INT NOT NULL DEFAULT 0,
  team1_score INT,
  team2_score INT,
  winning_task_force SMALLINT,
  quality VARCHAR(24) NOT NULL CHECK (quality IN ('complete', 'partial', 'limited')),
  stats_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  player_count SMALLINT NOT NULL DEFAULT 0,
  source VARCHAR(32) NOT NULL DEFAULT 'direct',
  raw_match JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_casual_matches_time ON casual_matches (entry_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_casual_matches_map ON casual_matches (map, entry_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_casual_matches_queue ON casual_matches (queue_id, entry_datetime DESC);

CREATE TABLE IF NOT EXISTS casual_match_players (
  match_id BIGINT NOT NULL REFERENCES casual_matches(match_id) ON DELETE CASCADE,
  roster_slot SMALLINT NOT NULL,
  private_slot SMALLINT NOT NULL DEFAULT 0,
  player_id BIGINT NOT NULL DEFAULT 0,
  private_player_id INT,
  player_name VARCHAR(100),
  champion_id INT,
  champion_name VARCHAR(100),
  task_force SMALLINT,
  win_status VARCHAR(20),
  kills INT NOT NULL DEFAULT 0,
  deaths INT NOT NULL DEFAULT 0,
  assists INT NOT NULL DEFAULT 0,
  damage INT NOT NULL DEFAULT 0,
  damage_taken INT NOT NULL DEFAULT 0,
  healing INT NOT NULL DEFAULT 0,
  mitigation INT NOT NULL DEFAULT 0,
  credits INT NOT NULL DEFAULT 0,
  objective_time INT NOT NULL DEFAULT 0,
  account_level INT NOT NULL DEFAULT 0,
  mastery_level INT NOT NULL DEFAULT 0,
  party_id INT NOT NULL DEFAULT 0,
  portal_id INT NOT NULL DEFAULT 0,
  portal_user_id TEXT,
  platform VARCHAR(30),
  participant_kind VARCHAR(16) NOT NULL DEFAULT 'human' CHECK (
    participant_kind IN ('human', 'private', 'bot', 'unknown')
  ),
  source VARCHAR(24) NOT NULL DEFAULT 'direct',
  stats_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  raw_player JSONB,
  PRIMARY KEY (match_id, roster_slot)
);
CREATE INDEX IF NOT EXISTS idx_cmp_player ON casual_match_players (player_id, match_id) WHERE player_id > 0;
CREATE INDEX IF NOT EXISTS idx_cmp_private ON casual_match_players (private_player_id, match_id) WHERE private_player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cmp_champion ON casual_match_players (champion_id, match_id) WHERE stats_eligible;

CREATE TABLE IF NOT EXISTS special_matches (
  match_id BIGINT PRIMARY KEY,
  queue_id INT NOT NULL,
  stats_scope VARCHAR(32) NOT NULL,
  participant_model VARCHAR(16) NOT NULL,
  entry_datetime TIMESTAMPTZ NOT NULL,
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  duration_seconds INT NOT NULL DEFAULT 0,
  team1_score INT,
  team2_score INT,
  winning_task_force SMALLINT,
  quality VARCHAR(24) NOT NULL CHECK (quality IN ('complete', 'partial', 'limited')),
  stats_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  player_count SMALLINT NOT NULL DEFAULT 0,
  source VARCHAR(32) NOT NULL DEFAULT 'direct',
  raw_match JSONB,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_special_matches_scope_time
  ON special_matches (stats_scope, entry_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_special_matches_map
  ON special_matches (stats_scope, map, entry_datetime DESC);

CREATE TABLE IF NOT EXISTS special_match_players (
  match_id BIGINT NOT NULL REFERENCES special_matches(match_id) ON DELETE CASCADE,
  roster_slot SMALLINT NOT NULL,
  private_slot SMALLINT NOT NULL DEFAULT 0,
  player_id BIGINT NOT NULL DEFAULT 0,
  private_player_id INT,
  player_name VARCHAR(100),
  champion_id INT,
  champion_name VARCHAR(100),
  task_force SMALLINT,
  win_status VARCHAR(20),
  kills INT NOT NULL DEFAULT 0,
  deaths INT NOT NULL DEFAULT 0,
  assists INT NOT NULL DEFAULT 0,
  damage INT NOT NULL DEFAULT 0,
  damage_taken INT NOT NULL DEFAULT 0,
  healing INT NOT NULL DEFAULT 0,
  mitigation INT NOT NULL DEFAULT 0,
  credits INT NOT NULL DEFAULT 0,
  objective_time INT NOT NULL DEFAULT 0,
  account_level INT NOT NULL DEFAULT 0,
  mastery_level INT NOT NULL DEFAULT 0,
  party_id INT NOT NULL DEFAULT 0,
  portal_id INT NOT NULL DEFAULT 0,
  portal_user_id TEXT,
  platform VARCHAR(30),
  participant_kind VARCHAR(16) NOT NULL DEFAULT 'human' CHECK (
    participant_kind IN ('human', 'private', 'bot', 'unknown')
  ),
  source VARCHAR(24) NOT NULL DEFAULT 'direct',
  stats_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  raw_player JSONB,
  PRIMARY KEY (match_id, roster_slot)
);
CREATE INDEX IF NOT EXISTS idx_smp_player ON special_match_players (player_id, match_id) WHERE player_id > 0;
CREATE INDEX IF NOT EXISTS idx_smp_private ON special_match_players (private_player_id, match_id) WHERE private_player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_smp_scope_champion
  ON special_match_players (champion_id, match_id) WHERE stats_eligible;

-- Compact durable projections. These contain no ranked tier, ban, Elo, or
-- relationship dimensions.
CREATE TABLE IF NOT EXISTS nonranked_champion_stats_daily (
  stats_date DATE NOT NULL,
  stats_scope VARCHAR(32) NOT NULL,
  queue_id INT NOT NULL,
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  champion_id INT NOT NULL,
  plays BIGINT NOT NULL DEFAULT 0,
  wins BIGINT NOT NULL DEFAULT 0,
  losses BIGINT NOT NULL DEFAULT 0,
  kills_sum BIGINT NOT NULL DEFAULT 0,
  deaths_sum BIGINT NOT NULL DEFAULT 0,
  assists_sum BIGINT NOT NULL DEFAULT 0,
  damage_sum BIGINT NOT NULL DEFAULT 0,
  healing_sum BIGINT NOT NULL DEFAULT 0,
  mitigation_sum BIGINT NOT NULL DEFAULT 0,
  credits_sum BIGINT NOT NULL DEFAULT 0,
  duration_sum BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (stats_date, stats_scope, queue_id, region, map, champion_id)
);
CREATE INDEX IF NOT EXISTS idx_ncsd_scope_champion
  ON nonranked_champion_stats_daily (stats_scope, champion_id, stats_date DESC);
CREATE INDEX IF NOT EXISTS idx_ncsd_scope_map
  ON nonranked_champion_stats_daily (stats_scope, map, stats_date DESC);

CREATE TABLE IF NOT EXISTS nonranked_map_stats_daily (
  stats_date DATE NOT NULL,
  stats_scope VARCHAR(32) NOT NULL,
  queue_id INT NOT NULL,
  region VARCHAR(50) NOT NULL DEFAULT 'Unknown',
  map VARCHAR(200) NOT NULL DEFAULT 'Unknown',
  matches BIGINT NOT NULL DEFAULT 0,
  duration_sum BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (stats_date, stats_scope, queue_id, region, map)
);
CREATE INDEX IF NOT EXISTS idx_nmsd_scope_date
  ON nonranked_map_stats_daily (stats_scope, stats_date DESC, map);

-- A public player appears once in the rolling window regardless of how many
-- matches they play. Bot rows are never inserted here.
CREATE TABLE IF NOT EXISTS player_presence_24h (
  player_id BIGINT PRIMARY KEY,
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at TIMESTAMPTZ NOT NULL,
  last_match_id BIGINT NOT NULL,
  last_queue_id INT NOT NULL,
  last_stats_scope VARCHAR(32) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_player_presence_24h_last
  ON player_presence_24h (last_observed_at DESC);

-- Private presence shares the global players_private identity. Ambiguous
-- observations remain separate and are never force-merged.
CREATE TABLE IF NOT EXISTS private_player_presence_24h (
  private_player_id INT PRIMARY KEY REFERENCES players_private(id) ON DELETE CASCADE,
  first_observed_at TIMESTAMPTZ NOT NULL,
  last_observed_at TIMESTAMPTZ NOT NULL,
  last_match_id BIGINT NOT NULL,
  last_queue_id INT NOT NULL,
  last_stats_scope VARCHAR(32) NOT NULL,
  identity_confidence SMALLINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_private_presence_24h_last
  ON private_player_presence_24h (last_observed_at DESC);

CREATE TABLE IF NOT EXISTS unresolved_private_presence (
  match_id BIGINT NOT NULL,
  private_slot SMALLINT NOT NULL,
  observed_at TIMESTAMPTZ NOT NULL,
  queue_id INT NOT NULL,
  stats_scope VARCHAR(32) NOT NULL,
  reason VARCHAR(80) NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (match_id, private_slot)
);
CREATE INDEX IF NOT EXISTS idx_unresolved_private_presence_window
  ON unresolved_private_presence (observed_at DESC, stats_scope);

ALTER TABLE private_account_observations
  ADD COLUMN IF NOT EXISTS queue_id INT,
  ADD COLUMN IF NOT EXISTS stats_scope VARCHAR(32),
  ADD COLUMN IF NOT EXISTS map VARCHAR(200),
  ADD COLUMN IF NOT EXISTS match_end_datetime TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS observation_quality VARCHAR(24) NOT NULL DEFAULT 'complete';

COMMENT ON TABLE nonranked_match_acquisition IS
  'One terminal ledger row per discovered non-ranked match. One detail batch attempt and at most one roster lookup per blocking match.';
COMMENT ON TABLE casual_matches IS
  'Casual Siege and Casual Onslaught facts only. No ban, tier, Elo, or relationship projections.';
COMMENT ON TABLE special_matches IS
  'Bot, TDM, Arcade, Wave Defense, Experiment, Newcomer, custom, and other display facts separated by stats_scope.';
COMMENT ON TABLE unresolved_private_presence IS
  'Private observations lacking enough stable evidence for the shared private identity; intentionally not counted as unique people.';
