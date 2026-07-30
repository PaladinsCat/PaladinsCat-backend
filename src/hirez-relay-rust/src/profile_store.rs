use std::sync::Arc;

use serde_json::{Map, Value, json};
use thiserror::Error;
use time::OffsetDateTime;

use crate::database::Database;

#[derive(Clone, Debug, Default)]
pub struct RankedQueue {
    pub name: String,
    pub rank: i32,
    pub tier: i32,
    pub points: i32,
    pub wins: i32,
    pub losses: i32,
    pub leaves: i32,
    pub trend: i32,
    pub prev_rank: i32,
    pub season: i32,
    pub ret_msg: Option<String>,
    pub player_id: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct MergedPlayer {
    pub player_id: i64,
    pub portal_id: Option<i32>,
    pub merge_datetime: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub struct PlayerProfile {
    pub player_id: i64,
    pub active_player_id: i64,
    pub player_name: String,
    pub platform_name: Option<String>,
    pub level: i32,
    pub api_level: i32,
    pub wins: i32,
    pub losses: i32,
    pub leaves: i32,
    pub mastery_level: i32,
    pub region: String,
    pub platform: Option<String>,
    pub hours_played: i32,
    pub minutes_played: i32,
    pub total_xp: i64,
    pub total_worshippers: i64,
    pub total_achievements: i32,
    pub title: String,
    pub avatar_id: i32,
    pub avatar_url: Option<String>,
    pub team_id: i32,
    pub team_name: String,
    pub hz_gamer_tag: Option<String>,
    pub hz_player_name: Option<String>,
    pub name_source: String,
    pub name_anomaly: bool,
    pub name_anomaly_reason: Option<String>,
    pub ret_msg: Option<String>,
    pub privacy_flag: bool,
    pub created_at: Option<OffsetDateTime>,
    pub last_login: Option<OffsetDateTime>,
    pub loading_frame: String,
    pub personal_status_message: String,
    pub ranked_kbm: RankedQueue,
    pub ranked_controller: RankedQueue,
    pub ranked_conquest: RankedQueue,
    pub tier_ranked_kbm: i32,
    pub tier_ranked_controller: i32,
    pub tier_conquest: i32,
    pub merged_players: Vec<MergedPlayer>,
}

#[derive(Debug, Error)]
pub enum ProfileStoreError {
    #[error("PostgreSQL player-profile operation failed: {0}")]
    Database(String),
    #[error("player profile has an invalid player ID")]
    InvalidPlayerId,
}

#[derive(Clone)]
pub struct ProfileStore {
    database: Arc<Database>,
}

impl ProfileStore {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    pub async fn upsert(&self, profile: &PlayerProfile) -> Result<(), ProfileStoreError> {
        if profile.player_id <= 0 {
            return Err(ProfileStoreError::InvalidPlayerId);
        }
        let document = profile_document(profile);
        let mut client = self.database.connection().await.map_err(database_error)?;
        let transaction = client.transaction().await.map_err(database_error)?;
        transaction
            .execute(UPSERT_PROFILE_SQL, &[&document])
            .await
            .map_err(database_error)?;
        transaction
            .execute(
                "DELETE FROM player_profile_merged_players WHERE player_id = $1",
                &[&profile.player_id],
            )
            .await
            .map_err(database_error)?;
        for merged in &profile.merged_players {
            if merged.player_id <= 0 {
                continue;
            }
            transaction
                .execute(
                    r#"
                    INSERT INTO player_profile_merged_players (
                      player_id, merged_player_id, portal_id, merge_datetime,
                      profile_refreshed_at
                    )
                    VALUES ($1, $2, $3, $4, now())
                    ON CONFLICT (player_id, merged_player_id) DO UPDATE SET
                      portal_id = EXCLUDED.portal_id,
                      merge_datetime = EXCLUDED.merge_datetime,
                      profile_refreshed_at = EXCLUDED.profile_refreshed_at
                    "#,
                    &[
                        &profile.player_id,
                        &merged.player_id,
                        &merged.portal_id,
                        &merged.merge_datetime,
                    ],
                )
                .await
                .map_err(database_error)?;
        }
        transaction.commit().await.map_err(database_error)
    }
}

const UPSERT_PROFILE_SQL: &str = r#"
    WITH profile AS (
      SELECT *
      FROM jsonb_to_record($1::jsonb) AS value (
        id BIGINT,
        active_player_id BIGINT,
        name VARCHAR(100),
        platform_name VARCHAR(200),
        level INT,
        api_level INT,
        wins INT,
        losses INT,
        leaves INT,
        hours_played INT,
        minutes_played INT,
        mastery_level INT,
        region VARCHAR(50),
        platform VARCHAR(20),
        ret_msg TEXT,
        total_xp BIGINT,
        total_worshippers BIGINT,
        total_achievements INT,
        avatar_id INT,
        avatar_url VARCHAR(500),
        title VARCHAR(200),
        loading_frame VARCHAR(200),
        created_datetime TIMESTAMPTZ,
        last_login_datetime TIMESTAMPTZ,
        personal_status_message VARCHAR(500),
        team_id INT,
        team_name VARCHAR(200),
        merged_players TEXT[],
        privacy_flag VARCHAR(1),
        kbm_name VARCHAR(100),
        kbm_points INT,
        kbm_tier INT,
        kbm_rank INT,
        kbm_wins INT,
        kbm_losses INT,
        kbm_leaves INT,
        kbm_trend INT,
        kbm_prev_rank INT,
        kbm_season INT,
        kbm_player_id BIGINT,
        kbm_ret_msg TEXT,
        controller_name VARCHAR(100),
        controller_points INT,
        controller_tier INT,
        controller_rank INT,
        controller_wins INT,
        controller_losses INT,
        controller_leaves INT,
        controller_trend INT,
        controller_prev_rank INT,
        controller_season INT,
        controller_player_id BIGINT,
        controller_ret_msg TEXT,
        conquest_name VARCHAR(100),
        conquest_points INT,
        conquest_tier INT,
        conquest_rank INT,
        conquest_wins INT,
        conquest_losses INT,
        conquest_leaves INT,
        conquest_trend INT,
        conquest_prev_rank INT,
        conquest_season INT,
        conquest_player_id BIGINT,
        conquest_ret_msg TEXT,
        hz_player_name VARCHAR(100),
        hz_gamer_tag VARCHAR(100),
        name_source VARCHAR(30),
        name_anomaly BOOLEAN,
        name_anomaly_reason TEXT,
        name_anomaly_detected_at TIMESTAMPTZ
      )
    )
    INSERT INTO players (
      id, active_player_id, name, platform_name, level, api_level, wins, losses,
      leaves, hours_played, minutes_played, mastery_level, region, platform,
      ret_msg, total_xp, total_worshippers, total_achievements, avatar_id,
      avatar_url, title, loading_frame, created_datetime, last_login_datetime,
      personal_status_message, team_id, team_name, merged_players, privacy_flag,
      kbm_name, kbm_points, kbm_tier, kbm_rank, kbm_wins, kbm_losses, kbm_leaves,
      kbm_trend, kbm_prev_rank, kbm_season, kbm_player_id, kbm_ret_msg,
      controller_name, controller_points, controller_tier, controller_rank,
      controller_wins, controller_losses, controller_leaves, controller_trend,
      controller_prev_rank, controller_season, controller_player_id,
      controller_ret_msg, conquest_name, conquest_points, conquest_tier,
      conquest_rank, conquest_wins, conquest_losses, conquest_leaves,
      conquest_trend, conquest_prev_rank, conquest_season, conquest_player_id,
      conquest_ret_msg, hz_player_name, hz_gamer_tag, name_source, name_anomaly,
      name_anomaly_reason, name_anomaly_detected_at, first_seen, last_seen,
      last_updated, hirez_profile_refreshed_at
    )
    SELECT
      id, active_player_id, name, platform_name, level, api_level, wins, losses,
      leaves, hours_played, minutes_played, mastery_level, region, platform,
      ret_msg, total_xp, total_worshippers, total_achievements, avatar_id,
      avatar_url, title, loading_frame, created_datetime, last_login_datetime,
      personal_status_message, team_id, team_name, merged_players, privacy_flag,
      kbm_name, kbm_points, kbm_tier, kbm_rank, kbm_wins, kbm_losses, kbm_leaves,
      kbm_trend, kbm_prev_rank, kbm_season, kbm_player_id, kbm_ret_msg,
      controller_name, controller_points, controller_tier, controller_rank,
      controller_wins, controller_losses, controller_leaves, controller_trend,
      controller_prev_rank, controller_season, controller_player_id,
      controller_ret_msg, conquest_name, conquest_points, conquest_tier,
      conquest_rank, conquest_wins, conquest_losses, conquest_leaves,
      conquest_trend, conquest_prev_rank, conquest_season, conquest_player_id,
      conquest_ret_msg, hz_player_name, hz_gamer_tag, name_source, name_anomaly,
      name_anomaly_reason, name_anomaly_detected_at, now(), now(), now(), now()
    FROM profile
    ON CONFLICT (id) DO UPDATE SET
      name = CASE
        WHEN EXCLUDED.name_source <> 'none' AND NULLIF(EXCLUDED.name, '') IS NOT NULL
          THEN EXCLUDED.name
        WHEN players.name ~* '^(DummyPlayer[0-9]+|[0-9a-f]{20,}User-[0-9a-f]{6,})$'
          THEN 'Player ' || players.id::text
        ELSE players.name
      END,
      region = CASE
        WHEN NULLIF(BTRIM(EXCLUDED.region), '') IS NOT NULL
             AND UPPER(EXCLUDED.region) <> 'UNKNOWN'
          THEN EXCLUDED.region
        ELSE players.region
      END,
      platform = CASE
        WHEN NULLIF(BTRIM(EXCLUDED.platform), '') IS NOT NULL
             AND UPPER(EXCLUDED.platform) <> 'UNKNOWN'
          THEN EXCLUDED.platform
        ELSE players.platform
      END,
      active_player_id = EXCLUDED.active_player_id,
      platform_name = EXCLUDED.platform_name,
      level = EXCLUDED.level,
      api_level = EXCLUDED.api_level,
      wins = EXCLUDED.wins,
      losses = EXCLUDED.losses,
      leaves = EXCLUDED.leaves,
      hours_played = EXCLUDED.hours_played,
      minutes_played = EXCLUDED.minutes_played,
      mastery_level = EXCLUDED.mastery_level,
      ret_msg = EXCLUDED.ret_msg,
      total_xp = EXCLUDED.total_xp,
      total_worshippers = EXCLUDED.total_worshippers,
      total_achievements = EXCLUDED.total_achievements,
      avatar_id = EXCLUDED.avatar_id,
      avatar_url = EXCLUDED.avatar_url,
      title = EXCLUDED.title,
      loading_frame = EXCLUDED.loading_frame,
      created_datetime = EXCLUDED.created_datetime,
      last_login_datetime = EXCLUDED.last_login_datetime,
      personal_status_message = EXCLUDED.personal_status_message,
      team_id = EXCLUDED.team_id,
      team_name = EXCLUDED.team_name,
      merged_players = EXCLUDED.merged_players,
      privacy_flag = EXCLUDED.privacy_flag,
      kbm_name = EXCLUDED.kbm_name,
      kbm_points = EXCLUDED.kbm_points,
      kbm_tier = EXCLUDED.kbm_tier,
      kbm_rank = EXCLUDED.kbm_rank,
      kbm_wins = EXCLUDED.kbm_wins,
      kbm_losses = EXCLUDED.kbm_losses,
      kbm_leaves = EXCLUDED.kbm_leaves,
      kbm_trend = EXCLUDED.kbm_trend,
      kbm_prev_rank = EXCLUDED.kbm_prev_rank,
      kbm_season = EXCLUDED.kbm_season,
      kbm_player_id = EXCLUDED.kbm_player_id,
      kbm_ret_msg = EXCLUDED.kbm_ret_msg,
      controller_name = EXCLUDED.controller_name,
      controller_points = EXCLUDED.controller_points,
      controller_tier = EXCLUDED.controller_tier,
      controller_rank = EXCLUDED.controller_rank,
      controller_wins = EXCLUDED.controller_wins,
      controller_losses = EXCLUDED.controller_losses,
      controller_leaves = EXCLUDED.controller_leaves,
      controller_trend = EXCLUDED.controller_trend,
      controller_prev_rank = EXCLUDED.controller_prev_rank,
      controller_season = EXCLUDED.controller_season,
      controller_player_id = EXCLUDED.controller_player_id,
      controller_ret_msg = EXCLUDED.controller_ret_msg,
      conquest_name = EXCLUDED.conquest_name,
      conquest_points = EXCLUDED.conquest_points,
      conquest_tier = EXCLUDED.conquest_tier,
      conquest_rank = EXCLUDED.conquest_rank,
      conquest_wins = EXCLUDED.conquest_wins,
      conquest_losses = EXCLUDED.conquest_losses,
      conquest_leaves = EXCLUDED.conquest_leaves,
      conquest_trend = EXCLUDED.conquest_trend,
      conquest_prev_rank = EXCLUDED.conquest_prev_rank,
      conquest_season = EXCLUDED.conquest_season,
      conquest_player_id = EXCLUDED.conquest_player_id,
      conquest_ret_msg = EXCLUDED.conquest_ret_msg,
      hz_player_name = EXCLUDED.hz_player_name,
      hz_gamer_tag = EXCLUDED.hz_gamer_tag,
      name_source = CASE
        WHEN EXCLUDED.name_source <> 'none' THEN EXCLUDED.name_source
        ELSE players.name_source
      END,
      name_anomaly = EXCLUDED.name_anomaly,
      name_anomaly_reason = CASE
        WHEN EXCLUDED.name_anomaly THEN EXCLUDED.name_anomaly_reason
        ELSE players.name_anomaly_reason
      END,
      name_anomaly_detected_at = CASE
        WHEN EXCLUDED.name_anomaly
          THEN COALESCE(players.name_anomaly_detected_at, now())
        ELSE players.name_anomaly_detected_at
      END,
      hirez_profile_refreshed_at = now(),
      last_seen = now(),
      last_updated = now()
"#;

fn profile_document(profile: &PlayerProfile) -> Value {
    let mut document = Map::new();
    document.insert("id".to_owned(), json!(profile.player_id));
    document.insert(
        "active_player_id".to_owned(),
        json!(profile.active_player_id),
    );
    let usable_name = profile.name_source != "none" && !profile.player_name.trim().is_empty();
    let display_name = if usable_name {
        sanitize(&profile.player_name)
    } else {
        format!("Player {}", profile.player_id)
    };
    document.insert("name".to_owned(), json!(display_name));
    macro_rules! field {
        ($name:literal, $value:expr) => {
            document.insert($name.to_owned(), json!($value));
        };
    }
    field!("platform_name", sanitize_optional(&profile.platform_name));
    field!("level", profile.level);
    field!("api_level", profile.api_level);
    field!("wins", profile.wins);
    field!("losses", profile.losses);
    field!("leaves", profile.leaves);
    field!("hours_played", profile.hours_played);
    field!("minutes_played", profile.minutes_played);
    field!("mastery_level", profile.mastery_level);
    field!("region", sanitize(&profile.region));
    field!("platform", sanitize_optional(&profile.platform));
    field!("ret_msg", sanitize_optional(&profile.ret_msg));
    field!("total_xp", profile.total_xp);
    field!("total_worshippers", profile.total_worshippers);
    field!("total_achievements", profile.total_achievements);
    field!("avatar_id", profile.avatar_id);
    field!("avatar_url", sanitize_optional(&profile.avatar_url));
    field!("title", sanitize(&profile.title));
    field!("loading_frame", sanitize(&profile.loading_frame));
    field!("created_datetime", profile.created_at);
    field!("last_login_datetime", profile.last_login);
    field!(
        "personal_status_message",
        sanitize(&profile.personal_status_message)
    );
    field!("team_id", profile.team_id);
    field!("team_name", sanitize(&profile.team_name));
    let merged_player_ids = profile
        .merged_players
        .iter()
        .filter(|merged| merged.player_id > 0)
        .map(|merged| merged.player_id.to_string())
        .collect::<Vec<_>>();
    document.insert(
        "merged_players".to_owned(),
        if merged_player_ids.is_empty() {
            Value::Null
        } else {
            json!(merged_player_ids)
        },
    );
    field!("privacy_flag", if profile.privacy_flag { "y" } else { "n" });
    ranked_document(
        &mut document,
        "kbm",
        &profile.ranked_kbm,
        profile.tier_ranked_kbm,
    );
    ranked_document(
        &mut document,
        "controller",
        &profile.ranked_controller,
        profile.tier_ranked_controller,
    );
    ranked_document(
        &mut document,
        "conquest",
        &profile.ranked_conquest,
        profile.tier_conquest,
    );
    field!("hz_player_name", sanitize_optional(&profile.hz_player_name));
    field!("hz_gamer_tag", sanitize_optional(&profile.hz_gamer_tag));
    field!("name_source", sanitize(&profile.name_source));
    field!("name_anomaly", profile.name_anomaly);
    field!(
        "name_anomaly_reason",
        sanitize_optional(&profile.name_anomaly_reason)
    );
    field!(
        "name_anomaly_detected_at",
        profile.name_anomaly.then(OffsetDateTime::now_utc)
    );
    Value::Object(document)
}

fn ranked_document(
    document: &mut Map<String, Value>,
    prefix: &str,
    queue: &RankedQueue,
    tier_fallback: i32,
) {
    let values = [
        ("name", json!(sanitize(&queue.name))),
        ("points", json!(queue.points)),
        (
            "tier",
            json!(if queue.tier != 0 {
                queue.tier
            } else {
                tier_fallback
            }),
        ),
        ("rank", json!(queue.rank)),
        ("wins", json!(queue.wins)),
        ("losses", json!(queue.losses)),
        ("leaves", json!(queue.leaves)),
        ("trend", json!(queue.trend)),
        ("prev_rank", json!(queue.prev_rank)),
        ("season", json!(queue.season)),
        ("player_id", json!(queue.player_id)),
        ("ret_msg", json!(sanitize_optional(&queue.ret_msg))),
    ];
    for (suffix, value) in values {
        document.insert(format!("{prefix}_{suffix}"), value);
    }
}

fn sanitize(value: &str) -> String {
    value.replace('\0', "").replace("\\u0000", "")
}

fn sanitize_optional(value: &Option<String>) -> Option<String> {
    value.as_deref().map(sanitize)
}

fn database_error(error: impl std::fmt::Display + std::fmt::Debug) -> ProfileStoreError {
    ProfileStoreError::Database(format!("{error}: {error:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> PlayerProfile {
        PlayerProfile {
            player_id: 7,
            active_player_id: 7,
            player_name: "Rust Player".to_owned(),
            platform_name: Some("raw-name".to_owned()),
            level: 1001,
            api_level: 999,
            wins: 10,
            losses: 2,
            leaves: 1,
            mastery_level: 3,
            region: "North America".to_owned(),
            platform: Some("Steam".to_owned()),
            hours_played: 4,
            minutes_played: 5,
            total_xp: 6,
            total_worshippers: 7,
            total_achievements: 8,
            title: "Title".to_owned(),
            avatar_id: 9,
            avatar_url: Some("https://example.invalid/avatar".to_owned()),
            team_id: 10,
            team_name: "Team".to_owned(),
            hz_gamer_tag: Some("gamer".to_owned()),
            hz_player_name: Some("Rust Player".to_owned()),
            name_source: "hz_player_name".to_owned(),
            name_anomaly: false,
            name_anomaly_reason: None,
            ret_msg: None,
            privacy_flag: false,
            created_at: None,
            last_login: None,
            loading_frame: "Frame".to_owned(),
            personal_status_message: "Hello".to_owned(),
            ranked_kbm: RankedQueue {
                tier: 12,
                points: 34,
                ..RankedQueue::default()
            },
            ranked_controller: RankedQueue::default(),
            ranked_conquest: RankedQueue::default(),
            tier_ranked_kbm: 11,
            tier_ranked_controller: 0,
            tier_conquest: 0,
            merged_players: vec![MergedPlayer {
                player_id: 8,
                portal_id: Some(1),
                merge_datetime: None,
            }],
        }
    }

    #[tokio::test]
    #[ignore = "requires PALADINSCAT_TEST_DATABASE_URL"]
    async fn live_profile_upsert_preserves_derived_fields_and_name_authority() {
        let database_url =
            std::env::var("PALADINSCAT_TEST_DATABASE_URL").expect("PALADINSCAT_TEST_DATABASE_URL");
        let database =
            Arc::new(Database::new(&database_url, "rust-profile-test", 4, 50).expect("db"));
        let client = database.connection().await.expect("connection");
        client
            .batch_execute(
                r#"
                DROP TABLE IF EXISTS player_profile_merged_players;
                DROP TABLE IF EXISTS players;
                CREATE TABLE players (
                  id BIGINT PRIMARY KEY,
                  active_player_id BIGINT,
                  name VARCHAR(100) NOT NULL,
                  platform_name VARCHAR(200),
                  hz_player_name VARCHAR(100),
                  hz_gamer_tag VARCHAR(100),
                  name_source VARCHAR(30) NOT NULL DEFAULT 'unknown',
                  name_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
                  name_anomaly_reason TEXT,
                  name_anomaly_detected_at TIMESTAMPTZ,
                  level INT DEFAULT 0,
                  api_level INT NOT NULL DEFAULT 0,
                  wins INT DEFAULT 0,
                  losses INT DEFAULT 0,
                  hours_played INT DEFAULT 0,
                  minutes_played INT DEFAULT 0,
                  mastery_level INT DEFAULT 0,
                  region VARCHAR(50),
                  platform VARCHAR(20),
                  ret_msg TEXT,
                  total_xp BIGINT DEFAULT 0,
                  total_worshippers BIGINT DEFAULT 0,
                  total_achievements INT DEFAULT 0,
                  avatar_id INT,
                  avatar_url VARCHAR(500),
                  title VARCHAR(200),
                  loading_frame VARCHAR(200),
                  created_datetime TIMESTAMPTZ,
                  last_login_datetime TIMESTAMPTZ,
                  personal_status_message VARCHAR(500),
                  team_id INT DEFAULT 0,
                  team_name VARCHAR(200),
                  leaves INT DEFAULT 0,
                  merged_players TEXT[],
                  privacy_flag VARCHAR(1) DEFAULT 'n',
                  kbm_name VARCHAR(100),
                  kbm_points INT DEFAULT 0,
                  kbm_tier INT DEFAULT 0,
                  kbm_season INT DEFAULT 0,
                  kbm_wins INT DEFAULT 0,
                  kbm_losses INT DEFAULT 0,
                  kbm_rank INT DEFAULT 0,
                  kbm_leaves INT DEFAULT 0,
                  kbm_trend INT DEFAULT 0,
                  kbm_prev_rank INT DEFAULT 0,
                  kbm_player_id BIGINT,
                  kbm_ret_msg TEXT,
                  controller_name VARCHAR(100),
                  controller_points INT DEFAULT 0,
                  controller_tier INT DEFAULT 0,
                  controller_rank INT DEFAULT 0,
                  controller_wins INT DEFAULT 0,
                  controller_losses INT DEFAULT 0,
                  controller_leaves INT DEFAULT 0,
                  controller_trend INT DEFAULT 0,
                  controller_prev_rank INT DEFAULT 0,
                  controller_season INT DEFAULT 0,
                  controller_player_id BIGINT,
                  controller_ret_msg TEXT,
                  conquest_name VARCHAR(100),
                  conquest_points INT DEFAULT 0,
                  conquest_tier INT DEFAULT 0,
                  conquest_rank INT DEFAULT 0,
                  conquest_wins INT DEFAULT 0,
                  conquest_losses INT DEFAULT 0,
                  conquest_leaves INT DEFAULT 0,
                  conquest_trend INT DEFAULT 0,
                  conquest_prev_rank INT DEFAULT 0,
                  conquest_season INT DEFAULT 0,
                  conquest_player_id BIGINT,
                  conquest_ret_msg TEXT,
                  hirez_profile_refreshed_at TIMESTAMPTZ,
                  last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
                  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
                  last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
                  total_matches INT DEFAULT 0,
                  cheater BOOLEAN NOT NULL DEFAULT FALSE
                );
                CREATE TABLE player_profile_merged_players (
                  player_id BIGINT NOT NULL REFERENCES players(id) ON DELETE CASCADE,
                  merged_player_id BIGINT NOT NULL,
                  portal_id INT,
                  merge_datetime TIMESTAMPTZ,
                  profile_refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  PRIMARY KEY (player_id, merged_player_id)
                );
                "#,
            )
            .await
            .expect("project schema");
        drop(client);

        let store = ProfileStore::new(database.clone());
        let profile = fixture();
        store.upsert(&profile).await.expect("insert");
        let client = database.connection().await.expect("derived seed");
        client
            .execute(
                "UPDATE players SET total_matches = 99, cheater = TRUE WHERE id = 7",
                &[],
            )
            .await
            .expect("derived fields");
        drop(client);

        let mut fallback = profile.clone();
        fallback.player_name.clear();
        fallback.name_source = "none".to_owned();
        fallback.region = "Unknown".to_owned();
        fallback.platform = Some("UNKNOWN".to_owned());
        fallback.wins = 11;
        fallback.merged_players.clear();
        store.upsert(&fallback).await.expect("update");

        let client = database.connection().await.expect("verify");
        let row = client
            .query_one(
                "SELECT name, region, platform, wins, total_matches, cheater, kbm_tier, merged_players FROM players WHERE id = 7",
                &[],
            )
            .await
            .expect("player");
        assert_eq!(row.get::<_, String>("name"), "Rust Player");
        assert_eq!(
            row.get::<_, Option<String>>("region").as_deref(),
            Some("North America")
        );
        assert_eq!(
            row.get::<_, Option<String>>("platform").as_deref(),
            Some("Steam")
        );
        assert_eq!(row.get::<_, i32>("wins"), 11);
        assert_eq!(row.get::<_, i32>("total_matches"), 99);
        assert!(row.get::<_, bool>("cheater"));
        assert_eq!(row.get::<_, i32>("kbm_tier"), 12);
        assert_eq!(row.get::<_, Option<Vec<String>>>("merged_players"), None);
        let merged: i64 = client
            .query_one(
                "SELECT COUNT(*)::bigint AS count FROM player_profile_merged_players WHERE player_id = 7",
                &[],
            )
            .await
            .expect("merged")
            .get("count");
        assert_eq!(merged, 0);
    }
}
